"""Offline V1.5 batch initialization closeout evidence index.

This binder consumes already-generated initialization artifacts and turns them
into one pre-gas evidence index. It is deliberately read-only: it does not open
COM ports, write SN/device codes, write SENCO coefficients, control routes, or
connect PostgreSQL.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA = "v1_5_batch_initialization_closeout_index_v1"

READY_STATUS = "ready_for_mature_open_flow_from_initialization_index"
ROUTE_PENDING_STATUS = "ready_for_route_readiness_gate"
REVIEW_STATUS = "review_required"

SN_PATTERN = re.compile(r"^\d{8}$")
READY_WORDS = {
    "pass",
    "passed",
    "ready",
    "ok",
    "verified",
    "written_readback_verified",
    "ready_for_open_flow_main_calibration",
    "ready_for_mature_open_flow_from_initialization_index",
    "readonly_com_minimal_executor_completed_no_write",
}


@dataclass(frozen=True)
class DeviceCloseoutRow:
    ga_label: str
    port: str
    protocol_device_id: str
    sn_code: str
    device_code: str
    algorithm: str
    identity_ready: bool
    getco_ready: bool
    runtime_ready: bool
    auxiliary_neutral_ready: bool
    s9_pressure_ready: bool
    check_scope_ready: bool
    ready_for_pre_gas_index: bool
    reasons: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GateRow:
    gate_id: str
    status: str
    required_before: str
    reason: str
    evidence_role: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"JSON file not found: {source}")
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {source}")
    return payload


def _load_csv(path: str | Path | None) -> list[dict[str, str]]:
    if not path:
        return []
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"CSV file not found: {source}")
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _string(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _string(value).lower() in {"1", "true", "yes", "y", "on", "ok", "pass", "ready", "verified"}


def _status_ready(value: Any) -> bool:
    text = _string(value).lower()
    return text in READY_WORDS or text.startswith("ready_for_") or text.endswith("_pass")


def _field(row: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return _string(value)
    return ""


def _normalize_device_id(value: Any) -> str:
    text = _string(value)
    if text.isdigit():
        return f"{int(text):03d}"
    return text.upper()


def _list_value(value: Any) -> list[float]:
    if isinstance(value, Mapping):
        value = value.get("values")
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            value = decoded
        except Exception:
            value = [part.strip() for part in value.replace(";", ",").split(",") if part.strip()]
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray, str)):
        return []
    out: list[float] = []
    for item in value:
        try:
            out.append(float(item))
        except Exception:
            return []
    return out


def _neutral_pair(values: Sequence[float]) -> bool:
    return len(values) >= 2 and abs(values[0]) <= 1e-6 and abs(values[1] - 1.0) <= 1e-6


def _neutral_quad(values: Sequence[float]) -> bool:
    return (
        len(values) >= 4
        and abs(values[0]) <= 1e-6
        and abs(values[1] - 1.0) <= 1e-6
        and abs(values[2]) <= 1e-6
        and abs(values[3]) <= 1e-6
    )


def _snapshots_from_payload(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in ("identity_getco_snapshots", "snapshots", "devices", "device_rows"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, Mapping)]
    return []


def _getco_values(snapshot: Mapping[str, Any], group: str) -> list[float]:
    getco = snapshot.get("getco")
    if isinstance(getco, Mapping):
        values = _list_value(getco.get(group) or getco.get(group.lower()))
        if values:
            return values
    return _list_value(snapshot.get(group) or snapshot.get(group.lower()))


def _runtime_ready(snapshot: Mapping[str, Any]) -> tuple[bool, list[str]]:
    runtime = snapshot.get("runtime_evidence")
    if not isinstance(runtime, Mapping):
        runtime = snapshot
    reasons: list[str] = []
    mode = _field(runtime, "mode", "mode_value", "target_mode")
    ftd_hz = _field(runtime, "ftd_hz", "analyzer_active_upload_hz", "active_upload_hz")
    average1 = _field(runtime, "average1", "AVERAGE1", "filter_average1")
    average2 = _field(runtime, "average2", "AVERAGE2", "filter_average2")
    if mode and mode not in {"2", "2.0", "MODE2", "mode2"}:
        reasons.append(f"mode_not_2:{mode}")
    if not mode:
        reasons.append("mode2_evidence_missing")
    try:
        if abs(float(ftd_hz) - 1.0) > 1e-6:
            reasons.append(f"ftd_hz_not_1:{ftd_hz}")
    except Exception:
        reasons.append("ftd_hz_evidence_missing")
    if not average1:
        reasons.append("average1_evidence_missing")
    if not average2:
        reasons.append("average2_evidence_missing")
    return not reasons, reasons


def _is_legacy_algorithm(value: str) -> bool:
    return _string(value).lower() in {"", "legacy", "legacy_ratio", "old", "ratio"}


def _check_scope_ready(snapshot: Mapping[str, Any], algorithm: str) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    check_raw = _field(snapshot, "check_monitor_raw", "check_raw")
    check_sent = _truthy(snapshot.get("check_command_sent")) or bool(check_raw)
    if _is_legacy_algorithm(algorithm):
        if check_sent:
            reasons.append("legacy_algorithm_check_must_be_absent")
    else:
        if not check_sent:
            reasons.append("new_algorithm_check_evidence_missing")
    return not reasons, reasons


def _pressure_rows_by_device(
    *,
    pressure_readiness_json: str | Path | None,
    pressure_device_readiness_csv: str | Path | None,
) -> tuple[bool, dict[str, Mapping[str, Any]], str]:
    payload = _load_json(pressure_readiness_json)
    csv_rows = _load_csv(pressure_device_readiness_csv)
    rows: list[Mapping[str, Any]] = []
    rows.extend(csv_rows)
    for key in ("devices", "device_rows", "pressure_device_readiness", "readiness_rows"):
        value = payload.get(key)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, Mapping))
    by_device: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        device = _normalize_device_id(
            row.get("protocol_device_id")
            or row.get("analyzer_device_id")
            or row.get("device_id")
            or row.get("identity_after")
        )
        if device:
            by_device[device] = row
    if not payload and not csv_rows:
        return False, {}, "pressure_s9_evidence_missing"
    if by_device:
        all_rows_ready = all(
            _status_ready(
                row.get("readiness_status")
                or row.get("pressure_status")
                or row.get("status")
                or row.get("post_write_fit_status")
            )
            or _truthy(row.get("can_enter_open_flow_main_calibration"))
            for row in by_device.values()
        )
        return all_rows_ready, by_device, "" if all_rows_ready else "one_or_more_pressure_s9_rows_not_ready"
    return False, {}, "pressure_s9_per_device_rows_missing"


def _route_ready(route_readiness_json: str | Path | None) -> tuple[bool, str]:
    payload = _load_json(route_readiness_json)
    if not payload:
        return False, "route_readiness_evidence_missing"
    status = payload.get("overall_status") or payload.get("status") or payload.get("route_readiness_status")
    if _status_ready(status):
        return True, ""
    return False, f"route_readiness_status={status or 'missing'}"


def _readonly_payload_ready(payload: Mapping[str, Any]) -> tuple[bool, str]:
    if not payload:
        return False, "readonly_com_executor_evidence_missing"
    status = payload.get("overall_status")
    if status != "readonly_com_minimal_executor_completed_no_write":
        return False, f"readonly_com_overall_status={status or 'missing'}"
    for key in (
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "connects_postgresql",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "database_written",
        "formal_release_allowed",
        "database_import_allowed",
    ):
        if payload.get(key) is not False:
            return False, f"{key}_not_false"
    if payload.get("not_real_acceptance_evidence") is not True:
        return False, "not_real_acceptance_evidence_not_true"
    return True, ""


def _build_device_rows(
    snapshots: Iterable[Mapping[str, Any]],
    pressure_by_device: Mapping[str, Mapping[str, Any]],
    pressure_payload_ready: bool,
) -> list[DeviceCloseoutRow]:
    rows: list[DeviceCloseoutRow] = []
    sn_seen: dict[str, int] = {}
    protocol_seen: dict[str, int] = {}
    snapshot_list = list(snapshots)
    for snapshot in snapshot_list:
        sn = _field(snapshot, "sn_code_read", "sn_code", "device_code", "sn_code_expected")
        device_code = _field(snapshot, "device_code", "sn_code_expected", "sn_code_read", "sn_code")
        protocol = _normalize_device_id(
            _field(snapshot, "protocol_device_id_read", "protocol_device_id_expected", "protocol_device_id", "device_id")
        )
        if sn:
            sn_seen[sn] = sn_seen.get(sn, 0) + 1
        if protocol:
            protocol_seen[protocol] = protocol_seen.get(protocol, 0) + 1
    for snapshot in snapshot_list:
        reasons: list[str] = []
        ga_label = _field(snapshot, "ga_label", "label")
        port = _field(snapshot, "port", "com_port")
        algorithm = _field(snapshot, "algorithm") or "legacy_ratio"
        protocol = _normalize_device_id(
            _field(snapshot, "protocol_device_id_read", "protocol_device_id_expected", "protocol_device_id", "device_id")
        )
        sn = _field(snapshot, "sn_code_read", "sn_code", "device_code", "sn_code_expected")
        device_code = _field(snapshot, "device_code", "sn_code_expected", "sn_code_read", "sn_code")
        identity_ready = bool(protocol and SN_PATTERN.match(sn) and sn != "00000000" and device_code == sn)
        if not protocol:
            reasons.append("protocol_device_id_missing")
        if not SN_PATTERN.match(sn) or sn == "00000000":
            reasons.append(f"sn_code_invalid:{sn or 'missing'}")
        if device_code != sn:
            reasons.append("device_code_not_equal_sn_code")
        if sn and sn_seen.get(sn, 0) > 1:
            identity_ready = False
            reasons.append(f"duplicate_sn_code:{sn}")
        if protocol and protocol_seen.get(protocol, 0) > 1:
            identity_ready = False
            reasons.append(f"duplicate_protocol_device_id:{protocol}")

        getco_ready = all(_getco_values(snapshot, f"GETCO{index}") for index in range(1, 10))
        if not getco_ready:
            missing = [f"GETCO{index}" for index in range(1, 10) if not _getco_values(snapshot, f"GETCO{index}")]
            reasons.append("getco_missing:" + ",".join(missing))

        runtime_ready, runtime_reasons = _runtime_ready(snapshot)
        reasons.extend(runtime_reasons)

        s5 = _getco_values(snapshot, "GETCO5")
        s6 = _getco_values(snapshot, "GETCO6")
        s7 = _getco_values(snapshot, "GETCO7")
        s8 = _getco_values(snapshot, "GETCO8")
        auxiliary_neutral_ready = _neutral_pair(s5) and _neutral_pair(s6) and _neutral_quad(s7) and _neutral_quad(s8)
        if not auxiliary_neutral_ready:
            if not _neutral_pair(s5):
                reasons.append("s5_getco5_not_neutral")
            if not _neutral_pair(s6):
                reasons.append("s6_getco6_not_neutral")
            if not _neutral_quad(s7):
                reasons.append("s7_getco7_not_neutral")
            if not _neutral_quad(s8):
                reasons.append("s8_getco8_not_neutral")

        pressure_row = pressure_by_device.get(protocol)
        s9_pressure_ready = pressure_payload_ready and (
            not pressure_by_device
            or (
                pressure_row is not None
                and (
                    _status_ready(
                        pressure_row.get("readiness_status")
                        or pressure_row.get("pressure_status")
                        or pressure_row.get("status")
                        or pressure_row.get("post_write_fit_status")
                    )
                    or _truthy(pressure_row.get("can_enter_open_flow_main_calibration"))
                )
            )
        )
        if not s9_pressure_ready:
            reasons.append("s9_pressure_readiness_missing_or_not_ready")

        check_scope_ready, check_reasons = _check_scope_ready(snapshot, algorithm)
        reasons.extend(check_reasons)

        ready = (
            identity_ready
            and getco_ready
            and runtime_ready
            and auxiliary_neutral_ready
            and s9_pressure_ready
            and check_scope_ready
        )
        rows.append(
            DeviceCloseoutRow(
                ga_label=ga_label,
                port=port,
                protocol_device_id=protocol,
                sn_code=sn,
                device_code=device_code,
                algorithm=algorithm,
                identity_ready=identity_ready,
                getco_ready=getco_ready,
                runtime_ready=runtime_ready,
                auxiliary_neutral_ready=auxiliary_neutral_ready,
                s9_pressure_ready=s9_pressure_ready,
                check_scope_ready=check_scope_ready,
                ready_for_pre_gas_index=ready,
                reasons=";".join(reasons),
            )
        )
    return rows


def _gate(
    gate_id: str,
    ready: bool,
    *,
    required_before: str,
    reason: str,
    evidence_role: str,
) -> GateRow:
    return GateRow(
        gate_id=gate_id,
        status="pass" if ready else "review_required",
        required_before=required_before,
        reason="" if ready else reason,
        evidence_role=evidence_role,
    )


def build_v1_5_batch_initialization_closeout_index(
    *,
    readonly_com_executor_json: str | Path | None = None,
    readonly_identity_getco_snapshot_json: str | Path | None = None,
    pressure_readiness_json: str | Path | None = None,
    pressure_device_readiness_csv: str | Path | None = None,
    route_readiness_json: str | Path | None = None,
    pre_gas_readiness_json: str | Path | None = None,
) -> dict[str, Any]:
    readonly_payload = _load_json(readonly_com_executor_json)
    identity_payload = _load_json(readonly_identity_getco_snapshot_json)
    snapshots = _snapshots_from_payload(readonly_payload) or _snapshots_from_payload(identity_payload)
    readonly_ready, readonly_reason = _readonly_payload_ready(readonly_payload)
    pressure_payload_ready, pressure_by_device, pressure_reason = _pressure_rows_by_device(
        pressure_readiness_json=pressure_readiness_json,
        pressure_device_readiness_csv=pressure_device_readiness_csv,
    )
    route_ready, route_reason = _route_ready(route_readiness_json)
    pre_gas_payload = _load_json(pre_gas_readiness_json)
    pre_gas_ready = _status_ready(pre_gas_payload.get("overall_status")) if pre_gas_payload else False

    device_rows = _build_device_rows(snapshots, pressure_by_device, pressure_payload_ready)
    active_protocols = {row.protocol_device_id for row in device_rows if row.protocol_device_id}
    pressure_covers_active_devices = bool(active_protocols) and active_protocols.issubset(set(pressure_by_device))
    pressure_ready = pressure_payload_ready and pressure_covers_active_devices
    if pressure_payload_ready and not pressure_covers_active_devices:
        pressure_reason = "pressure_s9_per_device_rows_missing_for_active_batch"
    device_count_ready = 1 <= len(device_rows) <= 6
    if not device_count_ready:
        device_count_reason = f"active_device_count={len(device_rows)}_outside_1_to_6"
    else:
        device_count_reason = ""
    all_devices_ready = device_count_ready and all(row.ready_for_pre_gas_index for row in device_rows)
    batch_closeout_ready = readonly_ready and all_devices_ready and pressure_ready
    open_flow_ready = batch_closeout_ready and route_ready

    if open_flow_ready:
        overall_status = READY_STATUS
    elif batch_closeout_ready and not route_ready:
        overall_status = ROUTE_PENDING_STATUS
    else:
        overall_status = REVIEW_STATUS

    gate_rows = [
        _gate(
            "readonly_com_identity_getco_closeout",
            readonly_ready,
            required_before="pre_gas_readiness_index",
            reason=readonly_reason,
            evidence_role="SN/device_code, protocol ID, GETCO1-9, runtime, CHECK-scope no-write readback",
        ),
        _gate(
            "active_device_count_1_to_6",
            device_count_ready,
            required_before="pre_gas_readiness_index",
            reason=device_count_reason,
            evidence_role="batch scope",
        ),
        _gate(
            "device_identity_getco_runtime_auxiliary_s5_s8_closeout",
            all_devices_ready,
            required_before="pre_gas_readiness_index",
            reason="one_or_more_device_closeout_rows_not_ready",
            evidence_role="per-device readiness",
        ),
        _gate(
            "pressure_s9_closeout",
            pressure_ready,
            required_before="mature_open_flow_route",
            reason=pressure_reason,
            evidence_role="SENCO9 write/readback/reverify or pressure readiness",
        ),
        _gate(
            "formal_route_readiness",
            route_ready,
            required_before="mature_open_flow_route",
            reason=route_reason,
            evidence_role="PACE/relay/dewpoint/route readiness",
        ),
        _gate(
            "pre_gas_readiness_sidecar_reference",
            pre_gas_ready or not pre_gas_payload,
            required_before="review_traceability",
            reason="" if not pre_gas_payload else f"pre_gas_readiness_status={pre_gas_payload.get('overall_status') or 'missing'}",
            evidence_role="optional cross-check sidecar",
        ),
    ]

    review_reasons = [row.reason for row in gate_rows if row.reason]
    review_reasons.extend(row.reasons for row in device_rows if row.reasons)
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": overall_status,
        "batch_initialization_closeout_ready": batch_closeout_ready,
        "ready_for_mature_open_flow_from_initialization_index": open_flow_ready,
        "ready_for_route_readiness_gate": batch_closeout_ready and not route_ready,
        "device_count": len(device_rows),
        "device_ready_count": sum(1 for row in device_rows if row.ready_for_pre_gas_index),
        "gate_count": len(gate_rows),
        "gate_pass_count": sum(1 for row in gate_rows if row.status == "pass"),
        "review_reasons": review_reasons,
        "readonly_com_executor_json": str(readonly_com_executor_json or ""),
        "readonly_identity_getco_snapshot_json": str(readonly_identity_getco_snapshot_json or ""),
        "pressure_readiness_json": str(pressure_readiness_json or ""),
        "pressure_device_readiness_csv": str(pressure_device_readiness_csv or ""),
        "route_readiness_json": str(route_readiness_json or ""),
        "pre_gas_readiness_json": str(pre_gas_readiness_json or ""),
        "mature_route_baseline": "0620/0621 clean worktree mature physical route",
        "mature_fitting_baseline": "0613 V1.5 fitting path",
        "legacy_point_counts": {"co2": 45, "h2o": 13},
        "new_algorithm_profile_point_counts": {"co2": 47, "h2o": 14},
        "full_production_auto_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "opens_com_ports": False,
        "read_only_real_com_execution_allowed": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "connects_postgresql": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "not_real_acceptance_evidence": True,
        "device_rows": [row.to_json() for row in device_rows],
        "gate_rows": [row.to_json() for row in gate_rows],
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys or ["empty"])
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def _markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 Batch Initialization Closeout Index",
        "",
        f"- schema: `{model['schema']}`",
        f"- overall_status: `{model['overall_status']}`",
        f"- batch_initialization_closeout_ready: `{str(model['batch_initialization_closeout_ready']).lower()}`",
        f"- ready_for_mature_open_flow_from_initialization_index: `{str(model['ready_for_mature_open_flow_from_initialization_index']).lower()}`",
        f"- device_count: `{model['device_count']}`",
        f"- mature_route_baseline: `{model['mature_route_baseline']}`",
        f"- mature_fitting_baseline: `{model['mature_fitting_baseline']}`",
        "",
        "## Meaning",
        "",
        "This artifact binds batch initialization closeout evidence into one pre-gas index. It is not a live runner and it is not release or database import evidence.",
        "",
        "## Gates",
        "",
        "| gate_id | status | required_before | reason |",
        "|---|---|---|---|",
    ]
    for row in model["gate_rows"]:
        lines.append(
            "| `{gate}` | `{status}` | `{required}` | {reason} |".format(
                gate=row["gate_id"],
                status=row["status"],
                required=row["required_before"],
                reason=row.get("reason") or "",
            )
        )
    lines.extend(["", "## Devices", "", "| GA | port | protocol_id | SN | ready | reasons |", "|---|---|---|---|---|---|"])
    for row in model["device_rows"]:
        lines.append(
            "| {ga} | `{port}` | `{pid}` | `{sn}` | `{ready}` | {reasons} |".format(
                ga=row["ga_label"],
                port=row["port"],
                pid=row["protocol_device_id"],
                sn=row["sn_code"],
                ready=str(row["ready_for_pre_gas_index"]).lower(),
                reasons=row.get("reasons") or "",
            )
        )
    if model.get("review_reasons"):
        lines.extend(["", "## Review Reasons", ""])
        for reason in model["review_reasons"]:
            lines.append(f"- `{reason}`")
    lines.extend(
        [
            "",
            "## Non-Execution Boundary",
            "",
            "- opens_com_ports: `false`",
            "- read_only_real_com_execution_allowed: `false`",
            "- controls_pressure: `false`",
            "- controls_water_or_gas_routes: `false`",
            "- connects_postgresql: `false`",
            "- writes_sn: `false`",
            "- writes_device_id: `false`",
            "- writes_coefficients: `false`",
            "- formal_release_allowed: `false`",
            "- database_import_allowed: `false`",
            "- not_real_acceptance_evidence: `true`",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_batch_initialization_closeout_index(
    *,
    output_dir: str | Path,
    readonly_com_executor_json: str | Path | None = None,
    readonly_identity_getco_snapshot_json: str | Path | None = None,
    pressure_readiness_json: str | Path | None = None,
    pressure_device_readiness_csv: str | Path | None = None,
    route_readiness_json: str | Path | None = None,
    pre_gas_readiness_json: str | Path | None = None,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_batch_initialization_closeout_index(
        readonly_com_executor_json=readonly_com_executor_json,
        readonly_identity_getco_snapshot_json=readonly_identity_getco_snapshot_json,
        pressure_readiness_json=pressure_readiness_json,
        pressure_device_readiness_csv=pressure_device_readiness_csv,
        route_readiness_json=route_readiness_json,
        pre_gas_readiness_json=pre_gas_readiness_json,
    )
    paths = {
        "manifest": out / "v1_5_batch_initialization_closeout_index.json",
        "devices": out / "v1_5_batch_initialization_closeout_index_devices.csv",
        "gates": out / "v1_5_batch_initialization_closeout_index_gates.csv",
        "markdown": out / "V1_5_BATCH_INITIALIZATION_CLOSEOUT_INDEX.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(paths["devices"], model["device_rows"])
    _write_csv(paths["gates"], model["gate_rows"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "READY_STATUS",
    "REVIEW_STATUS",
    "ROUTE_PENDING_STATUS",
    "SCHEMA",
    "build_v1_5_batch_initialization_closeout_index",
    "write_v1_5_batch_initialization_closeout_index",
]
