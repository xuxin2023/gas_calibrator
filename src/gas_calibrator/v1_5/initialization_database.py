from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
from pathlib import Path
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from ..v2.storage.database import DatabaseManager, stable_uuid
from ..v2.storage.models import DeviceEventRecord, RunRecord, SensorIdentityAliasRecord, SensorRecord


V1_5_INITIALIZATION_SCHEMA_PREFIX = "v1_5_formal_db_upsert_dry_run"
V1_5_FORMAL_INITIALIZATION_DB_BUNDLE_SCHEMA = "v1_5_formal_initialization_db_bundle_v0"
V1_5_RUNTIME_SETUP_SCHEMA_PREFIX = "v1_5_analyzer_runtime_setup_result"
V1_5_SENSOR_CHANNEL_TYPE = "co2_h2o_dual"
V1_5_INITIALIZATION_PROFILE_VERSION = "v1_5_formal_initialization_v0"
_V1_5_SN_CODE_RE = re.compile(r"\d{8}")


@dataclass(slots=True)
class V15InitializationImportResult:
    run_id: str
    run_uuid: str
    dry_run: bool
    sensors: int
    run_devices: int
    device_events: int
    validation_status: str
    database_written: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_uuid": self.run_uuid,
            "dry_run": self.dry_run,
            "sensors": self.sensors,
            "run_devices": self.run_devices,
            "device_events": self.device_events,
            "validation_status": self.validation_status,
            "database_written": self.database_written,
        }


@dataclass(slots=True)
class V15RuntimeSetupImportResult:
    run_id: str
    run_uuid: str
    dry_run: bool
    sensors: int
    run_devices: int
    device_events: int
    runtime_status: str
    database_written: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_uuid": self.run_uuid,
            "dry_run": self.dry_run,
            "sensors": self.sensors,
            "run_devices": self.run_devices,
            "device_events": self.device_events,
            "runtime_status": self.runtime_status,
            "database_written": self.database_written,
        }


def load_v1_5_initialization_bundle(path: str | Path) -> dict[str, Any]:
    bundle_path = Path(path)
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    schema = str(payload.get("schema") or "")
    if schema == V1_5_FORMAL_INITIALIZATION_DB_BUNDLE_SCHEMA:
        payload = _initialization_bundle_from_formal_db_bundle(payload)
        schema = str(payload.get("schema") or "")
    if not schema.startswith(V1_5_INITIALIZATION_SCHEMA_PREFIX):
        raise ValueError(f"unsupported V1.5 initialization bundle schema: {schema!r}")
    validation = payload.get("validation")
    if not isinstance(validation, dict):
        raise ValueError("V1.5 initialization bundle is missing validation")
    return payload


def subset_v1_5_initialization_bundle(
    bundle: dict[str, Any],
    *,
    include_sn_codes: list[str] | tuple[str, ...] | set[str],
    run_id: str | None = None,
    source_note: str | None = None,
) -> dict[str, Any]:
    """Return a provenance-preserving SN subset of a ready V1.5 initialization bundle."""

    include = _normalize_include_sn_codes(include_sn_codes)
    if not include:
        raise ValueError("include_sn_codes must contain at least one valid 8-digit SN")

    source_run_id = _bundle_run_id(bundle)
    derived = copy.deepcopy(bundle)
    available_devices = {str(item.get("sn_code") or "") for item in derived.get("devices") or [] if isinstance(item, dict)}
    missing = [sn for sn in include if sn not in available_devices]
    if missing:
        raise ValueError(f"requested SN codes are not present in source bundle: {', '.join(missing)}")

    def keep_sn(row: Any) -> bool:
        return isinstance(row, dict) and str(row.get("sn_code") or "").strip() in include

    derived_run_id = str(run_id or "").strip() or _derived_subset_run_id(source_run_id, include)
    derived["run_id"] = derived_run_id
    derived["devices"] = [item for item in derived.get("devices") or [] if keep_sn(item)]
    derived["run_devices"] = [
        {**item, "run_id": derived_run_id}
        for item in derived.get("run_devices") or []
        if keep_sn(item)
    ]
    derived["identity_lookup"] = [item for item in derived.get("identity_lookup") or [] if keep_sn(item)]
    derived["coefficient_snapshots"] = [
        item for item in derived.get("coefficient_snapshots") or [] if keep_sn(item)
    ]

    if len(derived["devices"]) != len(include):
        raise ValueError("derived subset did not preserve all requested device identity rows")
    if len(derived["run_devices"]) != len(include):
        raise ValueError("derived subset did not preserve all requested run device rows")
    if len(derived["coefficient_snapshots"]) != len(include):
        raise ValueError("derived subset did not preserve all requested GETCO snapshots")

    validation = dict(derived.get("validation") or {})
    validation.update(
        {
            "subset_of_source_run_id": source_run_id,
            "subset_sn_codes": include,
            "subset_device_count": len(include),
            "subset_source_note": source_note or "derived SN subset from source initialization bundle",
        }
    )
    derived["validation"] = validation
    identity_rules = dict(derived.get("identity_rules") or {})
    identity_rules.setdefault("subset_preserves_source_provenance", True)
    identity_rules["source_run_id"] = source_run_id
    derived["identity_rules"] = identity_rules
    evidence_manifest = list(derived.get("evidence_manifest") or [])
    evidence_manifest.append(
        {
            "role": "subset_provenance",
            "source_run_id": source_run_id,
            "derived_run_id": derived_run_id,
            "include_sn_codes": include,
            "note": source_note or "current subset preserves source GETCO evidence provenance",
        }
    )
    derived["evidence_manifest"] = evidence_manifest
    return derived


def _normalize_include_sn_codes(values: list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        for part in str(value or "").replace(";", ",").split(","):
            sn_code = _valid_sn(part.strip())
            if not sn_code:
                raise ValueError(f"invalid include SN code: {part!r}")
            if sn_code in seen:
                continue
            normalized.append(sn_code)
            seen.add(sn_code)
    return normalized


def _derived_subset_run_id(source_run_id: str, include_sn_codes: list[str]) -> str:
    suffix = "_".join(include_sn_codes)
    return f"{source_run_id}_subset_{suffix}"


def _initialization_bundle_from_formal_db_bundle(payload: dict[str, Any]) -> dict[str, Any]:
    tables = payload.get("tables") or {}
    if not isinstance(tables, dict):
        raise ValueError("V1.5 formal initialization DB bundle is missing tables")
    run_row = next(iter(tables.get("runs") or []), {})
    devices = [_canonical_identity_device(row) for row in tables.get("devices") or [] if isinstance(row, dict)]
    devices = [row for row in devices if row.get("sn_code")]
    run_devices = [
        _canonical_run_device(row, devices=devices, run_id=str(payload.get("run_id") or ""))
        for row in tables.get("run_devices") or []
        if isinstance(row, dict)
    ]
    run_devices = [row for row in run_devices if row.get("sn_code")]
    if not run_devices:
        run_devices = [
            {
                "run_id": str(payload.get("run_id") or ""),
                "slot_id": row.get("slot_id") or "",
                "port": (row.get("metadata_json") or {}).get("port_at_initialization", ""),
                "sn_code": row["sn_code"],
                "device_code": row.get("device_code") or row["sn_code"],
                "protocol_device_id_at_run": (row.get("metadata_json") or {}).get("protocol_device_id_current"),
                "mode_at_run": "2",
                "status": "formal_initialization_planned_identity_bound",
            }
            for row in devices
        ]
    snapshots = _canonical_coefficient_snapshots(tables.get("coefficient_snapshots") or [], devices=devices)
    all_getco_complete = bool(devices) and len(snapshots) >= len(devices)
    return {
        "schema": f"{V1_5_INITIALIZATION_SCHEMA_PREFIX}_from_formal_initialization_db_bundle_v0",
        "created_at": payload.get("generated_at"),
        "run_id": payload.get("run_id"),
        "validation": {
            "status": "ready" if all_getco_complete else "planned_identity_index",
            "formal_database_written": False,
            "protocol_device_id_is_primary_identity": False,
            "all_final_mode2_ready": all(bool(row.get("mode_at_run")) for row in run_devices),
            "all_getco_epoch0_complete": all_getco_complete,
            "all_identity_bound_to_sn": bool(devices) and len(devices) == len(run_devices),
            "planned_only_no_live_getco": not all_getco_complete,
            "not_calibration_acceptance_result": True,
        },
        "identity_rules": {
            "primary_identity": "sn_code/device_code",
            "protocol_device_id_role": "compatibility query field only",
            "source_schema": payload.get("schema"),
        },
        "devices": devices,
        "run_devices": run_devices,
        "identity_lookup": [],
        "coefficient_snapshots": snapshots,
        "evidence_manifest": tables.get("sample_files") or [],
        "evidence_manifest_sha256": (run_row.get("metadata") or {}).get("config_hash") if isinstance(run_row, dict) else None,
    }


def _canonical_identity_device(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_object(row.get("metadata_json") or row.get("metadata"))
    sn_code = _valid_sn(row.get("sn_code") or row.get("serial_number"))
    device_code = _valid_sn(row.get("device_code") or sn_code)
    if not sn_code:
        return {}
    metadata.setdefault("sn_code", sn_code)
    metadata.setdefault("device_code", device_code or sn_code)
    metadata.setdefault("protocol_device_id_current", row.get("protocol_device_id_current"))
    return {
        "device_key": row.get("device_key") or f"gas_analyzer:sn:{sn_code}",
        "sn_code": sn_code,
        "device_code": device_code or sn_code,
        "metadata_json": metadata,
    }


def _canonical_run_device(
    row: dict[str, Any],
    *,
    devices: list[dict[str, Any]],
    run_id: str,
) -> dict[str, Any]:
    metadata = _json_object(row.get("metadata"))
    sn_code = _valid_sn(row.get("sn_code") or metadata.get("sn_code"))
    if not sn_code:
        return {}
    device_by_sn = {item.get("sn_code"): item for item in devices}
    device = device_by_sn.get(sn_code) or {}
    device_metadata = _json_object(device.get("metadata_json"))
    return {
        "run_id": run_id,
        "slot_id": row.get("slot_id") or metadata.get("slot_id") or "",
        "port": row.get("port") or device_metadata.get("port_at_initialization") or "",
        "sn_code": sn_code,
        "device_code": _valid_sn(row.get("device_code") or metadata.get("device_code")) or sn_code,
        "protocol_device_id_at_run": row.get("protocol_device_id_at_run")
        or metadata.get("planned_device_id")
        or device_metadata.get("protocol_device_id_current"),
        "mode_at_run": row.get("mode_at_run") or "2",
        "status": row.get("status") or "formal_initialization_planned_identity_bound",
    }


def _canonical_coefficient_snapshots(rows: list[Any], *, devices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    protocol_to_sn = {}
    for device in devices:
        metadata = _json_object(device.get("metadata_json"))
        protocol_id = str(metadata.get("protocol_device_id_current") or "").strip()
        if protocol_id:
            protocol_to_sn[protocol_id] = device.get("sn_code")
    snapshots: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sn_code = _valid_sn(row.get("sn_code")) or protocol_to_sn.get(str(row.get("analyzer_id") or "").strip())
        if not sn_code:
            continue
        snapshots.append(
            {
                "sn_code": sn_code,
                "snapshot_type": row.get("snapshot_type") or "initialization_epoch0_getco1_9",
                "getco_complete": "True",
                "source_artifact_id": row.get("source_artifact_id"),
                "coefficients_hash": row.get("coefficients_hash"),
            }
        )
    return snapshots


def _valid_sn(value: Any) -> str:
    text = str(value or "").strip()
    return text if _V1_5_SN_CODE_RE.fullmatch(text) and text != "00000000" else ""


def load_v1_5_runtime_setup_result(path: str | Path) -> dict[str, Any]:
    result_path = Path(path)
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    schema = str(payload.get("schema_version") or "")
    if not schema.startswith(V1_5_RUNTIME_SETUP_SCHEMA_PREFIX):
        raise ValueError(f"unsupported V1.5 runtime setup result schema: {schema!r}")
    if not isinstance(payload.get("plan"), dict):
        raise ValueError("V1.5 runtime setup result is missing plan")
    if not isinstance(payload.get("results"), list):
        raise ValueError("V1.5 runtime setup result is missing results")
    return payload


def build_v1_5_initialization_storage_preview(bundle: dict[str, Any]) -> dict[str, Any]:
    _ensure_bundle_ready(bundle)
    run_id = _bundle_run_id(bundle)
    run_uuid = stable_uuid("run", run_id)
    sensors = [_sensor_payload(device) for device in bundle.get("devices") or []]
    run_devices = [dict(item) for item in bundle.get("run_devices") or []]
    coefficient_snapshots = [dict(item) for item in bundle.get("coefficient_snapshots") or []]
    device_events = _device_event_payloads(
        run_uuid=run_uuid,
        run_devices=run_devices,
        coefficient_snapshots=coefficient_snapshots,
    )
    return {
        "run_id": run_id,
        "run_uuid": str(run_uuid),
        "validation_status": bundle.get("validation", {}).get("status"),
        "database_written": False,
        "target_tables": ["sensors", "runs", "device_events"],
        "sensors": sensors,
        "run_devices": run_devices,
        "device_events": device_events,
        "notes": {
            "devices_table_not_required_for_current_v2_storage": True,
            "run_devices_preserved_in_runs_notes": True,
            "coefficient_snapshots_preserved_as_device_events": True,
            "protocol_device_id_is_primary_identity": False,
        },
    }


def build_v1_5_runtime_setup_storage_preview(
    result: dict[str, Any],
    *,
    result_path: str | Path | None = None,
) -> dict[str, Any]:
    _ensure_runtime_setup_result_usable(result)
    run_id = _runtime_setup_run_id(result)
    run_uuid = stable_uuid("run", run_id)
    run_devices = _runtime_setup_run_devices(result)
    sensors = [_sensor_payload(_runtime_setup_sensor_device(row)) for row in run_devices]
    device_events = _runtime_setup_device_event_payloads(
        run_uuid=run_uuid,
        result=result,
        run_devices=run_devices,
        result_path=str(Path(result_path).resolve()) if result_path else None,
    )
    return {
        "run_id": run_id,
        "run_uuid": str(run_uuid),
        "runtime_status": result.get("status"),
        "database_written": False,
        "target_tables": ["sensors", "runs", "device_events"],
        "sensors": sensors,
        "run_devices": run_devices,
        "device_events": device_events,
        "notes": {
            "runtime_setup_is_sampling": False,
            "runtime_setup_writes_senco": False,
            "runtime_setup_writes_device_id": False,
            "runtime_setup_writes_sn": False,
            "protocol_device_id_is_primary_identity": False,
        },
    }


def import_v1_5_initialization_bundle(
    database: DatabaseManager,
    bundle_path: str | Path,
    *,
    dry_run: bool = True,
    allow_write: bool = False,
    operator: str | None = None,
) -> dict[str, Any]:
    bundle = load_v1_5_initialization_bundle(bundle_path)
    return import_v1_5_initialization_payload(
        database,
        bundle,
        dry_run=dry_run,
        allow_write=allow_write,
        operator=operator,
    )


def import_v1_5_initialization_payload(
    database: DatabaseManager,
    bundle: dict[str, Any],
    *,
    dry_run: bool = True,
    allow_write: bool = False,
    operator: str | None = None,
) -> dict[str, Any]:
    preview = build_v1_5_initialization_storage_preview(bundle)
    if dry_run:
        return {
            **preview,
            "dry_run": True,
            "database_written": False,
        }
    if not allow_write:
        raise PermissionError("formal database write requires allow_write=True")
    with database.session_scope() as session:
        result = _apply_v1_5_initialization_bundle(
            session,
            bundle=bundle,
            preview=preview,
            operator=operator,
        )
    return result.as_dict()


def import_v1_5_runtime_setup_result(
    database: DatabaseManager,
    result_path: str | Path,
    *,
    dry_run: bool = True,
    allow_write: bool = False,
    operator: str | None = None,
) -> dict[str, Any]:
    result_payload = load_v1_5_runtime_setup_result(result_path)
    preview = build_v1_5_runtime_setup_storage_preview(result_payload, result_path=result_path)
    if dry_run:
        return {
            **preview,
            "dry_run": True,
            "database_written": False,
        }
    if not allow_write:
        raise PermissionError("V1.5 runtime setup database write requires allow_write=True")
    with database.session_scope() as session:
        result = _apply_v1_5_runtime_setup_result(
            session,
            runtime_result=result_payload,
            preview=preview,
            result_path=result_path,
            operator=operator,
        )
    return result.as_dict()


def _ensure_bundle_ready(bundle: dict[str, Any]) -> None:
    validation = bundle.get("validation") or {}
    status = validation.get("status")
    planned_identity_index = status == "planned_identity_index" and validation.get("planned_only_no_live_getco") is True
    if status != "ready" and not planned_identity_index:
        raise ValueError(f"V1.5 initialization bundle is not ready: {validation.get('status')!r}")
    if validation.get("formal_database_written") is not False:
        raise ValueError("bundle must be a dry-run package before formal database import")
    if validation.get("protocol_device_id_is_primary_identity") is not False:
        raise ValueError("protocol_device_id must not be treated as the primary identity")
    if validation.get("all_final_mode2_ready") is not True:
        raise ValueError("all selected analyzers must be final MODE2 ready")
    if not planned_identity_index and validation.get("all_getco_epoch0_complete") is not True:
        raise ValueError("all selected analyzers must have complete GETCO epoch0")
    if validation.get("all_identity_bound_to_sn") is not True:
        raise ValueError("all selected analyzers must be bound to valid SN identity")
    _ensure_bundle_identity_ready(bundle)


def _ensure_bundle_identity_ready(bundle: dict[str, Any]) -> None:
    devices = bundle.get("devices")
    if not isinstance(devices, list) or not devices:
        raise ValueError("V1.5 initialization bundle has no device identity rows")

    seen_sn: dict[str, str] = {}
    seen_device_code: dict[str, str] = {}
    for index, device in enumerate(devices):
        if not isinstance(device, dict):
            raise ValueError(f"device identity row {index} is not an object")
        slot = str(device.get("slot_id") or device.get("slot") or f"row_{index}").strip()
        sn_code = str(device.get("sn_code") or "").strip()
        device_code = str(device.get("device_code") or sn_code).strip()
        if not _V1_5_SN_CODE_RE.fullmatch(sn_code):
            raise ValueError(f"{slot}: sn_code must be 8 numeric digits")
        if sn_code == "00000000":
            raise ValueError(f"{slot}: sn_code is uninitialized 00000000")
        if not _V1_5_SN_CODE_RE.fullmatch(device_code):
            raise ValueError(f"{slot}: device_code must be 8 numeric digits")
        if device_code != sn_code:
            raise ValueError(f"{slot}: device_code must match sn_code for V1.5 formal initialization")
        if sn_code in seen_sn:
            raise ValueError(f"duplicate sn_code in initialization bundle: {sn_code}")
        if device_code in seen_device_code:
            raise ValueError(f"duplicate device_code in initialization bundle: {device_code}")
        seen_sn[sn_code] = slot
        seen_device_code[device_code] = slot

    run_devices = bundle.get("run_devices") or []
    for index, run_device in enumerate(run_devices):
        if not isinstance(run_device, dict):
            continue
        slot = str(run_device.get("slot_id") or f"run_device_{index}").strip()
        sn_code = str(run_device.get("sn_code") or "").strip()
        device_code = str(run_device.get("device_code") or sn_code).strip()
        if sn_code not in seen_sn:
            raise ValueError(f"{slot}: run_device sn_code is not present in devices identity table")
        if device_code != sn_code:
            raise ValueError(f"{slot}: run_device device_code must match sn_code")


def _ensure_runtime_setup_result_usable(result: dict[str, Any]) -> None:
    schema = str(result.get("schema_version") or "")
    if not schema.startswith(V1_5_RUNTIME_SETUP_SCHEMA_PREFIX):
        raise ValueError(f"unsupported V1.5 runtime setup result schema: {schema!r}")
    plan = result.get("plan") or {}
    safety = plan.get("safety") or {}
    contract = plan.get("contract") or {}
    if safety.get("writes_senco") is not False:
        raise ValueError("runtime setup result must declare writes_senco=false")
    if safety.get("writes_device_id") is not False:
        raise ValueError("runtime setup result must declare writes_device_id=false")
    if safety.get("writes_sn") is not False:
        raise ValueError("runtime setup result must declare writes_sn=false")
    if safety.get("runs_sampling") is not False:
        raise ValueError("runtime setup result must declare runs_sampling=false")
    if safety.get("runs_fitting") is not False:
        raise ValueError("runtime setup result must declare runs_fitting=false")
    if safety.get("controls_gas_route") is not False or safety.get("controls_water_route") is not False:
        raise ValueError("runtime setup result must not control gas or water routes")
    if not isinstance(result.get("results"), list) or not result["results"]:
        raise ValueError("runtime setup result has no analyzer rows")
    if result.get("status") != "ready":
        raise ValueError(f"runtime setup result must be ready before database import: {result.get('status')!r}")
    if int(contract.get("mode") or 0) != 2:
        raise ValueError("runtime setup result must target MODE2")
    if bool(contract.get("active_send")) is not True:
        raise ValueError("runtime setup result must enable active upload")
    if int(contract.get("ftd_hz") or 0) != 1:
        raise ValueError("runtime setup result must use mature V1.5 FTD=1Hz")
    if int(contract.get("average1_target") or 0) != 49 or int(contract.get("average2_target") or 0) != 49:
        raise ValueError("runtime setup result must use mature V1.5 AVERAGE1/2=49")
    _ensure_runtime_setup_rows_ready(result["results"])


def _ensure_runtime_setup_rows_ready(rows: list[Any]) -> None:
    seen_sn: dict[str, str] = {}
    seen_protocol_id: dict[str, str] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"runtime setup row {index} is not an object")
        slot = str(row.get("slot") or f"row_{index}").strip()
        status = str(row.get("status") or "").strip()
        if status != "ready":
            raise ValueError(f"{slot}: runtime setup row must be ready, got {status!r}")
        sn_code = _valid_sn(row.get("sn_code"))
        if not sn_code:
            raise ValueError(f"{slot}: runtime setup row is missing valid 8-digit sn_code")
        sn_readback = _valid_sn(row.get("sn_readback"))
        if sn_readback != sn_code:
            raise ValueError(f"{slot}: SN readback {sn_readback!r} does not match sn_code {sn_code!r}")
        device_code = _valid_sn(row.get("device_code") or sn_code)
        if device_code != sn_code:
            raise ValueError(f"{slot}: device_code must match sn_code for V1.5 runtime setup")
        protocol_id = str(row.get("protocol_device_id") or row.get("protocol_device_id_at_run") or "").strip()
        if not re.fullmatch(r"\d{3}", protocol_id):
            raise ValueError(f"{slot}: protocol_device_id must be 3 digits")
        identity = row.get("identity_before") if isinstance(row.get("identity_before"), dict) else {}
        if str(identity.get("id") or "").strip() != protocol_id:
            raise ValueError(f"{slot}: identity_before id must match protocol_device_id")
        if str(identity.get("mode") or "").strip() != "2":
            raise ValueError(f"{slot}: identity_before must be MODE2")
        if _runtime_setup_mode_from_row(row) != "2":
            raise ValueError(f"{slot}: final MODE2 evidence is missing")
        _ensure_active_upload_rate_ready(row, slot=slot)
        attempt_count = int(row.get("runtime_setup_attempt_count") or 0)
        attempts = row.get("runtime_setup_attempts") if isinstance(row.get("runtime_setup_attempts"), list) else []
        if attempt_count < 1 or len(attempts) != attempt_count:
            raise ValueError(f"{slot}: runtime_setup_attempts must match runtime_setup_attempt_count")
        final_attempt = attempts[-1] if attempts and isinstance(attempts[-1], dict) else {}
        if final_attempt.get("status") != "ready":
            raise ValueError(f"{slot}: final runtime setup attempt must be ready")
        if sn_code in seen_sn:
            raise ValueError(f"duplicate sn_code in runtime setup result: {sn_code}")
        if protocol_id in seen_protocol_id:
            raise ValueError(f"duplicate protocol_device_id in runtime setup result: {protocol_id}")
        seen_sn[sn_code] = slot
        seen_protocol_id[protocol_id] = slot


def _ensure_active_upload_rate_ready(row: dict[str, Any], *, slot: str) -> None:
    active_rate = row.get("active_upload_rate")
    if not isinstance(active_rate, dict):
        raise ValueError(f"{slot}: runtime setup row is missing active_upload_rate evidence")
    if active_rate.get("enabled") is not True:
        raise ValueError(f"{slot}: active upload rate verification must be enabled")
    if active_rate.get("ok") is not True:
        raise ValueError(f"{slot}: active upload rate verification did not pass")
    if int(active_rate.get("target_hz") or 0) != 1:
        raise ValueError(f"{slot}: active upload rate target must be 1Hz")
    approx_hz = float(active_rate.get("approx_hz") or 0.0)
    min_hz = float(active_rate.get("min_hz") or 0.0)
    max_hz = float(active_rate.get("max_hz") or 0.0)
    if not (min_hz <= approx_hz <= max_hz):
        raise ValueError(f"{slot}: active upload rate {approx_hz}Hz is outside {min_hz}-{max_hz}Hz")


def _bundle_run_id(bundle: dict[str, Any]) -> str:
    run_id = str(bundle.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("V1.5 initialization bundle is missing run_id")
    return run_id


def _runtime_setup_run_id(result: dict[str, Any]) -> str:
    run_id = str(result.get("run_id") or "").strip()
    if run_id:
        return run_id
    generated_at = str(result.get("generated_at") or "").strip().replace(":", "").replace("-", "")
    if generated_at:
        return f"v1_5_analyzer_runtime_setup_{generated_at}"
    raise ValueError("V1.5 runtime setup result is missing run_id")


def _sensor_device_key(sn_code: str) -> str:
    return f"{V1_5_SENSOR_CHANNEL_TYPE}:{sn_code.lower()}"


def _runtime_setup_sensor_device(row: dict[str, Any]) -> dict[str, Any]:
    sn_code = str(row.get("sn_code") or "").strip()
    device_code = str(row.get("device_code") or sn_code).strip()
    active_rate = row.get("active_upload_rate") if isinstance(row.get("active_upload_rate"), dict) else {}
    metadata = {
        "sn_code": sn_code,
        "device_code": device_code,
        "protocol_device_id_current": row.get("protocol_device_id") or row.get("protocol_device_id_at_run"),
        "port_at_runtime_setup": row.get("port"),
        "slot_id": row.get("slot") or row.get("slot_id"),
        "runtime_setup_status": row.get("status"),
        "runtime_setup_ready": row.get("status") == "ready",
        "runtime_setup_attempt_count": row.get("runtime_setup_attempt_count"),
        "active_upload_rate": active_rate,
    }
    return {
        "device_key": f"gas_analyzer:sn:{sn_code}",
        "sn_code": sn_code,
        "device_code": device_code,
        "metadata_json": metadata,
    }


def _sensor_payload(device: dict[str, Any]) -> dict[str, Any]:
    sn_code = str(device.get("sn_code") or "").strip()
    if not sn_code:
        raise ValueError("device row is missing sn_code")
    metadata = _json_object(device.get("metadata_json"))
    metadata["formal_device_key"] = device.get("device_key") or f"gas_analyzer:sn:{sn_code}"
    metadata["storage_bridge"] = {
        "table": "sensors",
        "device_key": _sensor_device_key(sn_code),
        "sn_code": sn_code,
        "device_code": device.get("device_code") or sn_code,
    }
    sensor_id = stable_uuid("sensor", _sensor_device_key(sn_code))
    return {
        "sensor_id": str(sensor_id),
        "device_key": _sensor_device_key(sn_code),
        "sn_code": sn_code,
        "device_code": device.get("device_code") or sn_code,
        "analyzer_id": sn_code,
        "analyzer_serial": sn_code,
        "software_version": "v1.5",
        "model": "gas_analyzer",
        "channel_type": V1_5_SENSOR_CHANNEL_TYPE,
        "metadata": metadata,
    }


def _device_event_payloads(
    *,
    run_uuid,
    run_devices: list[dict[str, Any]],
    coefficient_snapshots: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    snapshot_by_sn = {str(item.get("sn_code") or ""): item for item in coefficient_snapshots}
    events: list[dict[str, Any]] = []
    for run_device in run_devices:
        sn_code = str(run_device.get("sn_code") or "").strip()
        if not sn_code:
            continue
        snapshot = snapshot_by_sn.get(sn_code)
        snapshot_summary = {
            "sn_code": sn_code,
            "device_code": run_device.get("device_code") or sn_code,
            "slot_id": run_device.get("slot_id"),
            "port": run_device.get("port"),
            "protocol_device_id_at_run": run_device.get("protocol_device_id_at_run"),
            "getco_complete": bool(snapshot and snapshot.get("getco_complete") in (True, "True", "true", 1, "1")),
            "snapshot_type": snapshot.get("snapshot_type") if isinstance(snapshot, dict) else None,
        }
        meaning = (
            "V1.5 formal initialization identity binding and GETCO epoch0 reference"
            if snapshot
            else "V1.5 formal initialization planned identity index; GETCO epoch0 remains pending live evidence"
        )
        identity_payload = {
            "summary": snapshot_summary,
            "run_device": run_device,
            "snapshot": snapshot,
            "meaning": meaning,
            "not_calibration_acceptance_result": True,
            "protocol_device_id_is_primary_identity": False,
        }
        events.append(
            {
                "event_id": str(stable_uuid("device_event", run_uuid, sn_code, "v1_5_initialization_identity_bound")),
                "device_name": sn_code,
                "event_type": "v1_5_initialization_identity_bound",
                "event_data": identity_payload,
            }
        )
    return events


def _runtime_setup_run_devices(result: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in result.get("results") or []:
        if not isinstance(item, dict):
            continue
        sn_code = str(item.get("sn_code") or "").strip()
        if not sn_code:
            raise ValueError("runtime setup result row is missing sn_code")
        rows.append(
            {
                "run_id": _runtime_setup_run_id(result),
                "slot_id": item.get("slot"),
                "port": item.get("port"),
                "sn_code": sn_code,
                "device_code": item.get("device_code") or sn_code,
                "protocol_device_id_at_run": item.get("protocol_device_id"),
                "sn_readback": item.get("sn_readback"),
                "mode_at_run": _runtime_setup_mode_from_row(item),
                "status": item.get("status"),
                "runtime_setup_attempt_count": item.get("runtime_setup_attempt_count"),
                "active_upload_rate": item.get("active_upload_rate") if isinstance(item.get("active_upload_rate"), dict) else {},
            }
        )
    return rows


def _runtime_setup_mode_from_row(row: dict[str, Any]) -> str | None:
    identity = row.get("identity_before")
    if isinstance(identity, dict) and identity.get("mode") is not None:
        return str(identity.get("mode"))
    for frame in row.get("mode2_frames") or []:
        parsed = frame.get("parsed") if isinstance(frame, dict) else None
        if isinstance(parsed, dict) and parsed.get("mode") is not None:
            return str(parsed.get("mode"))
    return None


def _runtime_setup_device_event_payloads(
    *,
    run_uuid,
    result: dict[str, Any],
    run_devices: list[dict[str, Any]],
    result_path: str | None,
) -> list[dict[str, Any]]:
    result_by_sn = {str(item.get("sn_code") or ""): item for item in result.get("results") or [] if isinstance(item, dict)}
    event_paths = dict((result.get("evidence_paths") or {}))
    if result_path:
        event_paths["result_json"] = result_path
    events: list[dict[str, Any]] = []
    for run_device in run_devices:
        sn_code = str(run_device.get("sn_code") or "").strip()
        row = result_by_sn.get(sn_code, {})
        event_payload = {
            "summary": {
                "sn_code": sn_code,
                "device_code": run_device.get("device_code") or sn_code,
                "slot_id": run_device.get("slot_id"),
                "port": run_device.get("port"),
                "protocol_device_id_at_run": run_device.get("protocol_device_id_at_run"),
                "runtime_setup_ready": run_device.get("status") == "ready",
                "runtime_setup_attempt_count": run_device.get("runtime_setup_attempt_count"),
                "active_upload_rate_ok": (run_device.get("active_upload_rate") or {}).get("ok")
                if isinstance(run_device.get("active_upload_rate"), dict)
                else None,
            },
            "run_device": run_device,
            "runtime_setup_result": row,
            "plan_contract": (result.get("plan") or {}).get("contract"),
            "evidence_paths": event_paths,
            "meaning": "V1.5 analyzer runtime setup readiness event after identity binding",
            "not_calibration_acceptance_result": True,
            "protocol_device_id_is_primary_identity": False,
            "writes_senco": False,
            "writes_device_id": False,
            "writes_sn": False,
            "controls_route": False,
            "runs_sampling": False,
            "runs_fitting": False,
        }
        events.append(
            {
                "event_id": str(stable_uuid("device_event", run_uuid, sn_code, "v1_5_analyzer_runtime_setup")),
                "device_name": sn_code,
                "event_type": "v1_5_analyzer_runtime_setup",
                "event_data": event_payload,
            }
        )
    return events


def _apply_v1_5_initialization_bundle(
    session: Session,
    *,
    bundle: dict[str, Any],
    preview: dict[str, Any],
    operator: str | None,
) -> V15InitializationImportResult:
    run_id = preview["run_id"]
    run_uuid = stable_uuid("run", run_id)
    sensors = preview["sensors"]
    run_devices = preview["run_devices"]
    device_events = preview["device_events"]

    created_at = _parse_datetime(bundle.get("created_at")) or datetime.now(timezone.utc)
    for sensor_payload in sensors:
        sensor = _upsert_sensor(session, sensor_payload)
        _upsert_identity_aliases(
            session,
            sensor=sensor,
            payload=sensor_payload,
            run_uuid=run_uuid,
            observed_at=created_at,
            source="v1_5_initialization_identity_bound",
        )

    _upsert_initialization_run(
        session,
        run_uuid=run_uuid,
        run_id=run_id,
        bundle=bundle,
        preview=preview,
        operator=operator,
    )

    for event_payload in device_events:
        session.merge(
            DeviceEventRecord(
                id=event_payload["event_id"],
                run_id=run_uuid,
                device_name=event_payload["device_name"],
                event_type=event_payload["event_type"],
                event_data=event_payload["event_data"],
                timestamp=_parse_datetime(bundle.get("created_at")),
            )
        )
    session.flush()
    return V15InitializationImportResult(
        run_id=run_id,
        run_uuid=str(run_uuid),
        dry_run=False,
        sensors=len(sensors),
        run_devices=len(run_devices),
        device_events=len(device_events),
        validation_status=str(bundle.get("validation", {}).get("status") or ""),
        database_written=True,
    )


def _apply_v1_5_runtime_setup_result(
    session: Session,
    *,
    runtime_result: dict[str, Any],
    preview: dict[str, Any],
    result_path: str | Path,
    operator: str | None,
) -> V15RuntimeSetupImportResult:
    run_id = preview["run_id"]
    run_uuid = stable_uuid("run", run_id)
    sensors = preview["sensors"]
    run_devices = preview["run_devices"]
    device_events = preview["device_events"]

    timestamp = _parse_datetime(runtime_result.get("generated_at")) or datetime.now(timezone.utc)
    for sensor_payload in sensors:
        sensor = _upsert_sensor(session, sensor_payload)
        _upsert_identity_aliases(
            session,
            sensor=sensor,
            payload=sensor_payload,
            run_uuid=run_uuid,
            observed_at=timestamp,
            source="v1_5_analyzer_runtime_setup",
        )

    _upsert_runtime_setup_run(
        session,
        run_uuid=run_uuid,
        run_id=run_id,
        runtime_result=runtime_result,
        preview=preview,
        result_path=result_path,
        operator=operator,
    )

    for event_payload in device_events:
        session.merge(
            DeviceEventRecord(
                id=event_payload["event_id"],
                run_id=run_uuid,
                device_name=event_payload["device_name"],
                event_type=event_payload["event_type"],
                event_data=event_payload["event_data"],
                timestamp=timestamp,
            )
        )
    session.flush()
    return V15RuntimeSetupImportResult(
        run_id=run_id,
        run_uuid=str(run_uuid),
        dry_run=False,
        sensors=len(sensors),
        run_devices=len(run_devices),
        device_events=len(device_events),
        runtime_status=str(runtime_result.get("status") or ""),
        database_written=True,
    )


def _upsert_sensor(session: Session, payload: dict[str, Any]) -> SensorRecord:
    lookup_conditions = [
        SensorRecord.sensor_id == payload["sensor_id"],
        SensorRecord.device_key == payload["device_key"],
    ]
    if payload.get("sn_code"):
        lookup_conditions.append(SensorRecord.sn_code == payload["sn_code"])
    if payload.get("device_code"):
        lookup_conditions.append(SensorRecord.device_code == payload["device_code"])
    existing = session.execute(
        select(SensorRecord).where(or_(*lookup_conditions))
    ).scalars().first()
    if existing is None:
        existing = SensorRecord(
            sensor_id=payload["sensor_id"],
            device_key=payload["device_key"],
            sn_code=payload.get("sn_code"),
            device_code=payload.get("device_code"),
            analyzer_id=payload["analyzer_id"],
            analyzer_serial=payload["analyzer_serial"],
            software_version=payload["software_version"],
            model=payload["model"],
            channel_type=payload["channel_type"],
            metadata_json=payload["metadata"],
        )
        session.add(existing)
        session.flush()
        return existing
    existing.device_key = payload["device_key"]
    existing.sn_code = payload.get("sn_code")
    existing.device_code = payload.get("device_code")
    existing.analyzer_id = payload["analyzer_id"]
    existing.analyzer_serial = payload["analyzer_serial"]
    existing.software_version = payload["software_version"]
    existing.model = payload["model"]
    existing.channel_type = payload["channel_type"]
    existing.metadata_json = _merge_dicts(dict(existing.metadata_json or {}), dict(payload["metadata"] or {}))
    session.flush()
    return existing


def _identity_alias_values(payload: dict[str, Any]) -> list[tuple[str, str]]:
    metadata = dict(payload.get("metadata") or {})
    bridge = metadata.get("storage_bridge") if isinstance(metadata.get("storage_bridge"), dict) else {}
    values = [
        ("sn_code", payload.get("sn_code") or metadata.get("sn_code") or bridge.get("sn_code")),
        ("device_code", payload.get("device_code") or metadata.get("device_code") or bridge.get("device_code")),
        ("protocol_device_id_current", metadata.get("protocol_device_id_current")),
        ("protocol_device_id_at_run", metadata.get("protocol_device_id_at_run")),
    ]
    aliases: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for alias_type, value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = (alias_type, text)
        if key in seen:
            continue
        seen.add(key)
        aliases.append(key)
    return aliases


def _upsert_identity_aliases(
    session: Session,
    *,
    sensor: SensorRecord,
    payload: dict[str, Any],
    run_uuid,
    observed_at,
    source: str,
) -> None:
    for alias_type, alias_value in _identity_alias_values(payload):
        alias_id = stable_uuid("sensor_identity_alias", sensor.sensor_id, alias_type, alias_value, run_uuid)
        session.merge(
            SensorIdentityAliasRecord(
                id=alias_id,
                sensor_id=sensor.sensor_id,
                alias_type=alias_type,
                alias_value=alias_value,
                source_run_id=run_uuid,
                observed_at=observed_at,
                valid_from=observed_at,
                valid_to=None,
                metadata_json={
                    "source": source,
                    "formal_device_key": (payload.get("metadata") or {}).get("formal_device_key"),
                    "storage_bridge": (payload.get("metadata") or {}).get("storage_bridge"),
                },
            )
        )


def _upsert_initialization_run(
    session: Session,
    *,
    run_uuid,
    run_id: str,
    bundle: dict[str, Any],
    preview: dict[str, Any],
    operator: str | None,
) -> None:
    existing = session.get(RunRecord, run_uuid)
    validation = bundle.get("validation") or {}
    validation_status = str(validation.get("status") or "")
    is_planned_identity_index = validation_status == "planned_identity_index"
    notes = _merge_dicts(
        _json_object(None if existing is None else existing.notes),
        {
            "source_run_id": run_id,
            "v1_5_initialization": {
                "bundle_schema": bundle.get("schema"),
                "validation": bundle.get("validation"),
                "identity_rules": bundle.get("identity_rules"),
                "evidence_manifest": bundle.get("evidence_manifest"),
                "evidence_manifest_sha256": bundle.get("evidence_manifest_sha256"),
                "run_devices": preview.get("run_devices"),
                "device_events": preview.get("device_events"),
                "target_tables": preview.get("target_tables"),
            },
        },
    )
    created_at = _parse_datetime(bundle.get("created_at")) or datetime.now(timezone.utc)
    record = RunRecord(
        id=run_uuid,
        start_time=created_at,
        end_time=created_at,
        status="completed",
        config_hash=bundle.get("evidence_manifest_sha256"),
        software_version="v1.5",
        run_mode="v1_5_formal_initialization",
        route_mode="identity_planned" if is_planned_identity_index else "identity_getco_epoch0",
        profile_name="v1_5_initialization",
        profile_version=V1_5_INITIALIZATION_PROFILE_VERSION,
        report_family="v1_5_initialization_identity",
        report_templates={},
        analyzer_setup={"run_devices": preview.get("run_devices"), "identity_lookup": bundle.get("identity_lookup")},
        operator=operator,
        total_points=0,
        successful_points=0,
        failed_points=0,
        warnings=1 if is_planned_identity_index else 0,
        errors=0,
        notes=json.dumps(notes, ensure_ascii=False, sort_keys=True),
    )
    session.merge(record)


def _upsert_runtime_setup_run(
    session: Session,
    *,
    run_uuid,
    run_id: str,
    runtime_result: dict[str, Any],
    preview: dict[str, Any],
    result_path: str | Path,
    operator: str | None,
) -> None:
    existing = session.get(RunRecord, run_uuid)
    runtime_status = str(runtime_result.get("status") or "")
    notes = _merge_dicts(
        _json_object(None if existing is None else existing.notes),
        {
            "source_run_id": run_id,
            "v1_5_analyzer_runtime_setup": {
                "result_schema": runtime_result.get("schema_version"),
                "runtime_status": runtime_status,
                "result_path": str(Path(result_path).resolve()),
                "evidence_paths": runtime_result.get("evidence_paths"),
                "run_devices": preview.get("run_devices"),
                "device_events": preview.get("device_events"),
                "target_tables": preview.get("target_tables"),
                "boundary": preview.get("notes"),
            },
        },
    )
    created_at = _parse_datetime(runtime_result.get("generated_at")) or datetime.now(timezone.utc)
    record = RunRecord(
        id=run_uuid,
        start_time=created_at,
        end_time=created_at,
        status="completed" if runtime_status == "ready" else "failed",
        config_hash=None,
        software_version="v1.5",
        run_mode="v1_5_analyzer_runtime_setup",
        route_mode="analyzer_runtime_setup_only",
        profile_name="v1_5_analyzer_runtime_setup",
        profile_version=str(runtime_result.get("schema_version") or "unknown"),
        report_family="v1_5_initialization_identity",
        report_templates={},
        analyzer_setup={"run_devices": preview.get("run_devices"), "plan_contract": (runtime_result.get("plan") or {}).get("contract")},
        operator=operator,
        total_points=0,
        successful_points=0,
        failed_points=0,
        warnings=0,
        errors=0 if runtime_status == "ready" else 1,
        notes=json.dumps(notes, ensure_ascii=False, sort_keys=True),
    )
    session.merge(record)


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        payload = json.loads(str(value))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _merge_dicts(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, "", "null", "None"):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text_value = str(value).strip()
    if not text_value:
        return None
    if text_value.endswith("Z"):
        text_value = f"{text_value[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text_value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
