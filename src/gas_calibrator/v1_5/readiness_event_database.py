from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from ..storage.database import DatabaseManager, resolve_run_uuid, stable_uuid
from ..storage.models import DeviceEventRecord, RunRecord


EVENT_TYPE = "v1_5_post_initialization_pre_route_readiness"


def _readiness_gates(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    gates_container = payload.get("gates") if isinstance(payload.get("gates"), Mapping) else {}
    gates = gates_container.get("gates") if isinstance(gates_container, Mapping) else None
    if not isinstance(gates, list):
        raise ValueError("readiness payload is missing gates.gates")
    return [dict(gate) for gate in gates if isinstance(gate, Mapping)]


def _device_scope(payload: Mapping[str, Any], gates: Sequence[Mapping[str, Any]]) -> list[str]:
    devices: list[str] = []
    seen: set[str] = set()
    for gate in gates:
        for key in ("expected_device_scope", "device_scope", "devices"):
            values = gate.get(key)
            if not isinstance(values, list):
                continue
            for value in values:
                token = str(value or "").strip()
                if token and token not in seen:
                    seen.add(token)
                    devices.append(token)
    if devices:
        return devices
    configured = payload.get("device_scope")
    if isinstance(configured, list):
        for value in configured:
            token = str(value or "").strip()
            if token and token not in seen:
                seen.add(token)
                devices.append(token)
    if not devices:
        raise ValueError("readiness payload does not identify any device scope")
    return devices


def _gate_statuses(gates: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    return {str(gate.get("name") or ""): str(gate.get("status") or "") for gate in gates if gate.get("name")}


def build_v1_5_readiness_event_preview(
    *,
    readiness_payload: Mapping[str, Any],
    source_run_id: str,
) -> dict[str, Any]:
    gates = _readiness_gates(readiness_payload)
    devices = _device_scope(readiness_payload, gates)
    gate_statuses = _gate_statuses(gates)
    return {
        "schema_version": "v1_5_readiness_event_import_preview_v0",
        "source_run_id": source_run_id,
        "source_run_uuid": str(resolve_run_uuid(source_run_id)),
        "readiness_run_id": str(readiness_payload.get("run_id") or ""),
        "readiness_status": str(readiness_payload.get("status") or ""),
        "device_scope": devices,
        "event_type": EVENT_TYPE,
        "event_count": len(devices),
        "gate_statuses": gate_statuses,
        "database_written": False,
        "boundary": {
            "opens_com": False,
            "writes_device": False,
            "controls_pressure": False,
            "controls_temperature": False,
            "controls_gas_route": False,
            "controls_water_route": False,
            "runs_sampling": False,
            "runs_fitting": False,
            "not_calibration_acceptance_result": True,
        },
    }


def import_v1_5_readiness_events(
    database: DatabaseManager,
    *,
    readiness_payload: Mapping[str, Any],
    source_run_id: str,
    dry_run: bool = True,
    allow_write: bool = False,
    operator: str | None = None,
) -> dict[str, Any]:
    preview = build_v1_5_readiness_event_preview(
        readiness_payload=readiness_payload,
        source_run_id=source_run_id,
    )
    if dry_run:
        return preview
    if not allow_write:
        raise PermissionError("formal database readiness event write requires allow_write=True")

    run_uuid = resolve_run_uuid(source_run_id)
    readiness_run_id = str(readiness_payload.get("run_id") or "v1_5_post_initialization_readiness")
    generated_at = str(readiness_payload.get("generated_at") or "")
    timestamp = datetime.now(timezone.utc)
    gates_payload = readiness_payload.get("gates") if isinstance(readiness_payload.get("gates"), Mapping) else {}
    event_payload = {
        "readiness_schema_version": readiness_payload.get("schema_version"),
        "readiness_run_id": readiness_run_id,
        "readiness_status": readiness_payload.get("status"),
        "generated_at": generated_at,
        "gate_statuses": preview["gate_statuses"],
        "gates": gates_payload,
        "source_paths": dict(readiness_payload.get("source_paths") or {}),
        "evidence_paths": dict(readiness_payload.get("evidence_paths") or {}),
        "operator": operator,
        "meaning": "V1.5 post-initialization pressure/temperature readiness and pre-route gate state.",
        "boundary": {
            **dict(readiness_payload.get("boundary") or {}),
            "opens_com": False,
            "writes_device": False,
            "controls_gas_route": False,
            "controls_water_route": False,
            "runs_sampling": False,
            "runs_fitting": False,
            "not_calibration_acceptance_result": True,
        },
    }

    with database.session_scope() as session:
        if session.get(RunRecord, run_uuid) is None:
            raise ValueError(f"source run is missing from storage: {source_run_id}")
        for device in preview["device_scope"]:
            event_id = stable_uuid("device_event", run_uuid, device, EVENT_TYPE, readiness_run_id)
            session.merge(
                DeviceEventRecord(
                    id=event_id,
                    run_id=run_uuid,
                    device_name=device,
                    event_type=EVENT_TYPE,
                    event_data={**event_payload, "device_name": device},
                    timestamp=timestamp,
                )
            )
    return {
        **preview,
        "dry_run": False,
        "database_written": True,
        "events_written": len(preview["device_scope"]),
    }
