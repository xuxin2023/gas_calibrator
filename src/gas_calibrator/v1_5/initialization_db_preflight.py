"""Read-only database preflight for V1.5 initialization readiness.

This module checks already-imported initialization evidence only. It does not
open COM ports, control gas/water routes, sample data, fit coefficients, or
write analyzer state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from sqlalchemy import or_, select, text
from sqlalchemy.orm import Session

from ..v2.storage.database import DatabaseManager, StorageSettings
from ..v2.storage.models import DeviceEventRecord, SensorIdentityAliasRecord, SensorRecord


INIT_EVENT_TYPE = "v1_5_initialization_identity_bound"
RUNTIME_EVENT_TYPE = "v1_5_analyzer_runtime_setup"
SN_RE = re.compile(r"\d{8}")
PROTOCOL_ID_RE = re.compile(r"\d{3}")


@dataclass(frozen=True, slots=True)
class ExpectedAnalyzerIdentity:
    sn_code: str
    protocol_device_id: str = ""

    @classmethod
    def from_value(cls, value: str | Mapping[str, Any]) -> "ExpectedAnalyzerIdentity":
        if isinstance(value, Mapping):
            sn_code = _valid_sn(value.get("sn_code") or value.get("device_code"))
            protocol_id = _valid_protocol_id(
                value.get("protocol_device_id") or value.get("protocol_device_id_at_run")
            )
        else:
            sn_code, protocol_id = _parse_expected_device_token(str(value or ""))
        if not sn_code:
            raise ValueError(f"invalid expected SN code: {value!r}")
        return cls(sn_code=sn_code, protocol_device_id=protocol_id)

    def as_dict(self) -> dict[str, str]:
        return {"sn_code": self.sn_code, "protocol_device_id": self.protocol_device_id}


def build_v1_5_initialization_db_preflight(
    database: DatabaseManager,
    *,
    expected_devices: Sequence[str | Mapping[str, Any] | ExpectedAnalyzerIdentity],
    require_postgresql_major: int | None = None,
) -> dict[str, Any]:
    identities = _normalize_expected_devices(expected_devices)
    db_status = _probe_database(database, require_postgresql_major=require_postgresql_major)
    if not db_status["ok"]:
        return _blocked_report(identities, db_status, [db_status["reason"]])

    with database.session_scope() as session:
        device_rows = [_assess_device(session, identity) for identity in identities]

    failed = [row for row in device_rows if row["status"] != "ready"]
    status = "ready" if not failed else "blocked"
    return {
        "schema_version": "v1_5_initialization_db_preflight_v0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "device_count": len(identities),
        "ready_count": len(device_rows) - len(failed),
        "blocked_count": len(failed),
        "expected_devices": [identity.as_dict() for identity in identities],
        "database": db_status,
        "devices": device_rows,
        "boundary": _boundary(),
        "next_gate": "gas_route_allowed" if status == "ready" else "blocked_before_gas_route",
    }


def write_v1_5_initialization_db_preflight_report(
    report: Mapping[str, Any],
    *,
    output_json: str | Path | None = None,
    output_md: str | Path | None = None,
) -> dict[str, Path]:
    outputs: dict[str, Path] = {}
    if output_json:
        path = Path(output_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(dict(report), ensure_ascii=False, indent=2), encoding="utf-8")
        outputs["json"] = path
    if output_md:
        path = Path(output_md)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_markdown_report(report), encoding="utf-8")
        outputs["md"] = path
    return outputs


def _normalize_expected_devices(
    values: Sequence[str | Mapping[str, Any] | ExpectedAnalyzerIdentity],
) -> list[ExpectedAnalyzerIdentity]:
    identities: list[ExpectedAnalyzerIdentity] = []
    seen: set[str] = set()
    for value in values:
        identity = value if isinstance(value, ExpectedAnalyzerIdentity) else ExpectedAnalyzerIdentity.from_value(value)
        if identity.sn_code in seen:
            raise ValueError(f"duplicate expected SN code: {identity.sn_code}")
        identities.append(identity)
        seen.add(identity.sn_code)
    if not identities:
        raise ValueError("expected_devices must contain 1 to 6 analyzers")
    if len(identities) > 6:
        raise ValueError("V1.5 initialization preflight supports at most 6 analyzers per round")
    return identities


def _parse_expected_device_token(value: str) -> tuple[str, str]:
    token = value.strip()
    if not token:
        return "", ""
    for delimiter in ("=", ":", ","):
        if delimiter in token:
            left, right = token.split(delimiter, 1)
            return _valid_sn(left), _valid_protocol_id(right)
    return _valid_sn(token), ""


def _valid_sn(value: Any) -> str:
    text_value = str(value or "").strip()
    return text_value if SN_RE.fullmatch(text_value) and text_value != "00000000" else ""


def _valid_protocol_id(value: Any) -> str:
    text_value = str(value or "").strip()
    return text_value if PROTOCOL_ID_RE.fullmatch(text_value) else ""


def _probe_database(database: DatabaseManager, *, require_postgresql_major: int | None) -> dict[str, Any]:
    settings = database.settings
    backend = settings.normalized_backend
    status: dict[str, Any] = {
        "ok": False,
        "backend": backend,
        "host": settings.host,
        "port": settings.port,
        "database": settings.database,
        "require_postgresql_major": require_postgresql_major,
        "sidecar_query_only": True,
    }
    if require_postgresql_major is not None and backend != "postgresql":
        return {
            **status,
            "reason": f"postgresql_{require_postgresql_major}_required",
        }
    try:
        with database.engine.connect() as conn:
            if backend == "postgresql":
                version = str(conn.execute(text("SHOW server_version")).scalar() or "")
                major = _parse_postgresql_major(version)
                ok = require_postgresql_major is None or major == int(require_postgresql_major)
                return {
                    **status,
                    "ok": ok,
                    "reason": "" if ok else f"postgresql_major_{major}_does_not_match_required_{require_postgresql_major}",
                    "server_version": version,
                    "postgresql_major": major,
                }
            if backend == "sqlite":
                version = str(conn.execute(text("SELECT sqlite_version()")).scalar() or "")
                return {**status, "ok": True, "reason": "", "server_version": version}
            conn.execute(text("SELECT 1"))
            return {**status, "ok": True, "reason": "", "server_version": ""}
    except Exception as exc:
        return {**status, "reason": str(exc)}


def _parse_postgresql_major(version: str) -> int | None:
    match = re.match(r"\s*(\d+)", str(version or ""))
    return int(match.group(1)) if match else None


def _blocked_report(
    identities: Sequence[ExpectedAnalyzerIdentity],
    db_status: Mapping[str, Any],
    blockers: Sequence[str],
) -> dict[str, Any]:
    return {
        "schema_version": "v1_5_initialization_db_preflight_v0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "blocked",
        "device_count": len(identities),
        "ready_count": 0,
        "blocked_count": len(identities),
        "expected_devices": [identity.as_dict() for identity in identities],
        "database": dict(db_status),
        "devices": [],
        "blockers": list(blockers),
        "boundary": _boundary(),
        "next_gate": "blocked_before_gas_route",
    }


def _assess_device(session: Session, identity: ExpectedAnalyzerIdentity) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    sensor = _find_single_sensor(session, identity.sn_code)
    if sensor is None:
        return _device_result(identity, None, checks + [_fail("sensor_identity", "sensor_not_found")])

    sensor_metadata = _json_object(sensor.metadata_json)
    checks.extend(_sensor_identity_checks(session, sensor, sensor_metadata, identity))

    init_event = _best_event(session, identity.sn_code, INIT_EVENT_TYPE)
    runtime_event = _best_event(session, identity.sn_code, RUNTIME_EVENT_TYPE)
    checks.extend(_initialization_event_checks(init_event))
    checks.extend(_runtime_event_checks(runtime_event))

    status = "ready" if all(item["status"] != "fail" for item in checks) else "blocked"
    protocol_id = identity.protocol_device_id or _protocol_from_sensor(sensor, sensor_metadata)
    return {
        "status": status,
        "sn_code": identity.sn_code,
        "device_code": sensor.device_code,
        "protocol_device_id": protocol_id,
        "sensor_id": str(sensor.sensor_id),
        "device_key": sensor.device_key,
        "initialization_run_uuid": "" if init_event is None else str(init_event.run_id),
        "runtime_setup_run_uuid": "" if runtime_event is None else str(runtime_event.run_id),
        "checks": checks,
    }


def _find_single_sensor(session: Session, sn_code: str) -> SensorRecord | None:
    rows = session.execute(
        select(SensorRecord).where(
            or_(
                SensorRecord.sn_code == sn_code,
                SensorRecord.device_code == sn_code,
                SensorRecord.analyzer_id == sn_code,
                SensorRecord.analyzer_serial == sn_code,
            )
        )
    ).scalars().all()
    unique = {str(row.sensor_id): row for row in rows}
    if len(unique) != 1:
        return None
    return next(iter(unique.values()))


def _sensor_identity_checks(
    session: Session,
    sensor: SensorRecord,
    sensor_metadata: Mapping[str, Any],
    identity: ExpectedAnalyzerIdentity,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    checks.append(
        _pass("sensor_identity", "sn_device_code_match")
        if sensor.sn_code == identity.sn_code and sensor.device_code == identity.sn_code
        else _fail("sensor_identity", "sn_code_or_device_code_mismatch")
    )
    checks.append(
        _pass("sensor_identity", "sn_lookup_resolves_sensor")
        if _identity_resolves_to_sensor(session, identity.sn_code, str(sensor.sensor_id))
        else _fail("sensor_identity", "sn_lookup_does_not_resolve_sensor")
    )
    checks.append(
        _pass("sensor_identity", "device_code_lookup_resolves_sensor")
        if _identity_resolves_to_sensor(session, str(sensor.device_code or ""), str(sensor.sensor_id))
        else _fail("sensor_identity", "device_code_lookup_does_not_resolve_sensor")
    )

    protocol_id = identity.protocol_device_id or _protocol_from_sensor(sensor, sensor_metadata)
    if protocol_id:
        protocol_matches = _protocol_from_sensor(sensor, sensor_metadata) == protocol_id
        protocol_resolved_ids = _identity_resolution_sensor_ids(session, protocol_id)
        protocol_resolves = str(sensor.sensor_id) in protocol_resolved_ids
        checks.append(
            _pass("sensor_identity", "protocol_id_matches_sensor_metadata")
            if protocol_matches
            else _fail("sensor_identity", "protocol_id_metadata_mismatch")
        )
        checks.append(
            _pass("sensor_identity", "protocol_id_lookup_resolves_sensor")
            if protocol_resolves
            else _fail("sensor_identity", "protocol_id_lookup_does_not_resolve_sensor")
        )
        if len(protocol_resolved_ids) > 1:
            checks.append(_warn("sensor_identity", "protocol_id_lookup_not_unique_compatibility_only"))
    else:
        checks.append(_fail("sensor_identity", "protocol_device_id_missing"))
    return checks


def _identity_resolves_to_sensor(session: Session, token: str, expected_sensor_id: str) -> bool:
    return _identity_resolution_sensor_ids(session, token) == {expected_sensor_id}


def _identity_resolution_sensor_ids(session: Session, token: str) -> set[str]:
    if not token:
        return set()
    direct_rows = session.execute(
        select(SensorRecord).where(
            or_(
                SensorRecord.sn_code == token,
                SensorRecord.device_code == token,
                SensorRecord.analyzer_id == token,
                SensorRecord.analyzer_serial == token,
                SensorRecord.device_key == token,
            )
        )
    ).scalars().all()
    alias_rows = session.execute(
        select(SensorIdentityAliasRecord).where(SensorIdentityAliasRecord.alias_value == token)
    ).scalars().all()
    resolved = {str(row.sensor_id) for row in direct_rows}
    resolved.update(str(row.sensor_id) for row in alias_rows)
    return resolved


def _protocol_from_sensor(sensor: SensorRecord, metadata: Mapping[str, Any]) -> str:
    for value in (
        metadata.get("protocol_device_id_current"),
        metadata.get("protocol_device_id"),
        metadata.get("protocol_device_id_at_run"),
    ):
        protocol_id = _valid_protocol_id(value)
        if protocol_id:
            return protocol_id
    return _valid_protocol_id(sensor.analyzer_id)


def _best_event(session: Session, sn_code: str, event_type: str) -> DeviceEventRecord | None:
    rows = session.execute(
        select(DeviceEventRecord).where(
            DeviceEventRecord.device_name == sn_code,
            DeviceEventRecord.event_type == event_type,
        )
    ).scalars().all()
    if not rows:
        return None
    return sorted(rows, key=lambda row: (_event_readiness_score(row), _event_sort_key(row)))[-1]


def _event_sort_key(row: DeviceEventRecord) -> str:
    timestamp = row.timestamp
    if timestamp is None:
        return ""
    if hasattr(timestamp, "isoformat"):
        return str(timestamp.isoformat())
    return str(timestamp)


def _event_readiness_score(row: DeviceEventRecord) -> int:
    if row.event_type == INIT_EVENT_TYPE:
        event_data = _json_object(row.event_data)
        summary = _json_object(event_data.get("summary"))
        return int(
            _truthy(summary.get("getco_complete"))
            and summary.get("snapshot_type") == "initialization_epoch0_getco1_9"
        )
    if row.event_type == RUNTIME_EVENT_TYPE:
        event_data = _json_object(row.event_data)
        summary = _json_object(event_data.get("summary"))
        run_device = _json_object(event_data.get("run_device"))
        runtime_result = _json_object(event_data.get("runtime_setup_result"))
        active_rate = _json_object(runtime_result.get("active_upload_rate")) or _json_object(
            run_device.get("active_upload_rate")
        )
        runtime_ready = (
            summary.get("runtime_setup_ready") is True
            or run_device.get("status") == "ready"
            or runtime_result.get("status") == "ready"
        )
        return int(runtime_ready and active_rate.get("ok") is True and int(active_rate.get("target_hz") or 0) == 1)
    return 0


def _initialization_event_checks(event: DeviceEventRecord | None) -> list[dict[str, Any]]:
    if event is None:
        return [_fail("initialization_getco_epoch0", "initialization_event_missing")]
    event_data = _json_object(event.event_data)
    summary = _json_object(event_data.get("summary"))
    return [
        _pass("initialization_getco_epoch0", "event_present"),
        _pass("initialization_getco_epoch0", "getco_complete")
        if _truthy(summary.get("getco_complete"))
        else _fail("initialization_getco_epoch0", "getco_not_complete"),
        _pass("initialization_getco_epoch0", "snapshot_type_valid")
        if summary.get("snapshot_type") == "initialization_epoch0_getco1_9"
        else _fail("initialization_getco_epoch0", "snapshot_type_not_initialization_epoch0_getco1_9"),
        _pass("initialization_getco_epoch0", "not_acceptance_result")
        if event_data.get("not_calibration_acceptance_result") is True
        else _fail("initialization_getco_epoch0", "acceptance_boundary_missing"),
    ]


def _runtime_event_checks(event: DeviceEventRecord | None) -> list[dict[str, Any]]:
    if event is None:
        return [_fail("runtime_setup", "runtime_setup_event_missing")]
    event_data = _json_object(event.event_data)
    summary = _json_object(event_data.get("summary"))
    run_device = _json_object(event_data.get("run_device"))
    runtime_result = _json_object(event_data.get("runtime_setup_result"))
    active_rate = _json_object(runtime_result.get("active_upload_rate")) or _json_object(
        run_device.get("active_upload_rate")
    )
    runtime_ready = (
        summary.get("runtime_setup_ready") is True
        or run_device.get("status") == "ready"
        or runtime_result.get("status") == "ready"
    )
    active_rate_ok = (
        summary.get("active_upload_rate_ok") is True
        or active_rate.get("ok") is True
    )
    return [
        _pass("runtime_setup", "event_present"),
        _pass("runtime_setup", "runtime_setup_ready")
        if runtime_ready
        else _fail("runtime_setup", "runtime_setup_not_ready"),
        _pass("runtime_setup", "active_upload_rate_ok")
        if active_rate_ok
        else _fail("runtime_setup", "active_upload_rate_not_ok"),
        _pass("runtime_setup", "target_hz_1")
        if int(active_rate.get("target_hz") or 0) == 1
        else _fail("runtime_setup", "target_hz_not_1"),
        _pass("runtime_setup", "no_senco_or_identity_writes")
        if event_data.get("writes_senco") is False
        and event_data.get("writes_device_id") is False
        and event_data.get("writes_sn") is False
        else _fail("runtime_setup", "runtime_setup_write_boundary_missing"),
        _pass("runtime_setup", "no_route_sampling_or_fitting")
        if event_data.get("controls_route") is False
        and event_data.get("runs_sampling") is False
        and event_data.get("runs_fitting") is False
        else _fail("runtime_setup", "runtime_setup_activity_boundary_missing"),
    ]


def _device_result(
    identity: ExpectedAnalyzerIdentity,
    sensor: SensorRecord | None,
    checks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "status": "blocked",
        "sn_code": identity.sn_code,
        "device_code": "" if sensor is None else str(sensor.device_code or ""),
        "protocol_device_id": identity.protocol_device_id,
        "sensor_id": "" if sensor is None else str(sensor.sensor_id),
        "device_key": "" if sensor is None else str(sensor.device_key or ""),
        "initialization_run_uuid": "",
        "runtime_setup_run_uuid": "",
        "checks": [dict(item) for item in checks],
    }


def _pass(check: str, reason: str) -> dict[str, str]:
    return {"check": check, "status": "pass", "reason": reason}


def _fail(check: str, reason: str) -> dict[str, str]:
    return {"check": check, "status": "fail", "reason": reason}


def _warn(check: str, reason: str) -> dict[str, str]:
    return {"check": check, "status": "warn", "reason": reason}


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "ok", "pass"}


def _boundary() -> dict[str, bool]:
    return {
        "opens_com": False,
        "writes_device": False,
        "writes_sn": False,
        "writes_senco": False,
        "controls_pressure": False,
        "controls_temperature": False,
        "controls_gas_route": False,
        "controls_water_route": False,
        "runs_sampling": False,
        "runs_fitting": False,
        "modifies_run_app": False,
        "not_calibration_acceptance_result": True,
    }


def _markdown_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 Initialization DB Preflight",
        "",
        f"- status: `{report.get('status')}`",
        f"- device_count: `{report.get('device_count')}`",
        f"- ready_count: `{report.get('ready_count')}`",
        f"- blocked_count: `{report.get('blocked_count')}`",
        f"- next_gate: `{report.get('next_gate')}`",
        "",
        "## Database",
        "",
    ]
    database = _json_object(report.get("database"))
    lines.extend(
        [
            f"- backend: `{database.get('backend')}`",
            f"- database: `{database.get('database')}`",
            f"- server_version: `{database.get('server_version', '')}`",
            f"- require_postgresql_major: `{database.get('require_postgresql_major')}`",
            f"- ok: `{database.get('ok')}`",
            f"- reason: `{database.get('reason', '')}`",
            "",
            "## Devices",
            "",
            "| SN | Protocol ID | Status | Failed checks |",
            "| --- | --- | --- | --- |",
        ]
    )
    for item in report.get("devices") or []:
        if not isinstance(item, Mapping):
            continue
        failed = [
            str(check.get("reason") or "")
            for check in item.get("checks") or []
            if isinstance(check, Mapping) and check.get("status") != "pass"
        ]
        lines.append(
            "| "
            + " | ".join(
                [
                    str(item.get("sn_code") or ""),
                    str(item.get("protocol_device_id") or ""),
                    str(item.get("status") or ""),
                    "; ".join(failed),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- DB preflight is read-only and does not open COM, write analyzer state, control routes, sample, or fit.",
        ]
    )
    return "\n".join(lines) + "\n"
