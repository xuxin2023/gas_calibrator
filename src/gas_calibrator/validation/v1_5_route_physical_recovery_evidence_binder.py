"""Bind reviewed V1.5 route physical recovery traces into a packet.

This module is intentionally offline. It reads already-collected CSV evidence
for dry-gas dewpoint, PACE vent roundtrip, and COM22/INL pressure-gauge
readback, then emits the packet consumed by
``v1_5_route_physical_recovery_evidence_packet``. It never opens COM ports,
controls pressure, controls gas/water routes, connects PostgreSQL, or writes
analyzer state.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .v1_5_route_physical_recovery_evidence_packet import SCHEMA as PACKET_SCHEMA
from .v1_5_route_physical_recovery_readiness import VALID_PRESSURE_SOURCES


SCHEMA = "v1_5_route_physical_recovery_evidence_binder_v1"

SIDE_EFFECT_FALSE_KEYS = (
    "opens_com_ports",
    "controls_pressure",
    "controls_water_or_gas_routes",
    "connects_postgresql",
    "writes_coefficients",
    "writes_sn_or_device_code",
    "formal_release_allowed",
    "database_import_allowed",
)

DEWPOINT_COLUMNS = (
    "dewpoint_c",
    "dew_point_c",
    "dewpoint",
    "dewpoint_deg_c",
    "dewpoint_temperature_c",
    "dp_c",
)

TIME_COLUMNS = (
    "elapsed_s",
    "time_s",
    "timestamp_s",
    "mono_s",
    "seconds",
    "sample_time_s",
)

PRESSURE_COLUMNS = (
    "pressure_hpa",
    "pressure_gauge_hpa",
    "inl_pressure_hpa",
    "absolute_pressure_hpa",
    "pressure_abs_hpa",
    "pressure_kpa",
)


@dataclass(frozen=True)
class RouteRecoveryEvidenceBindingFinding:
    severity: str
    requirement: str
    status: str
    reason: str
    required_action: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "true", "yes", "y", "ok", "pass", "ready", "reviewed"}


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _read_csv_rows(path: str | Path) -> list[dict[str, str]]:
    source = Path(path)
    with source.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _row_blob(row: Mapping[str, Any]) -> str:
    return " ".join(f"{key}={value}" for key, value in row.items()).lower()


def _has_no_response(rows: list[Mapping[str, Any]]) -> bool:
    return any("no_response" in _row_blob(row) or "no response" in _row_blob(row) for row in rows)


def _pick_number(row: Mapping[str, Any], candidates: tuple[str, ...]) -> float | None:
    lowered = {str(key).strip().lower(): value for key, value in row.items()}
    for key in candidates:
        value = _number(lowered.get(key))
        if value is not None:
            return value
    return None


def _pick_text(row: Mapping[str, Any], candidates: tuple[str, ...]) -> str:
    lowered = {str(key).strip().lower(): value for key, value in row.items()}
    for key in candidates:
        value = _text(lowered.get(key))
        if value:
            return value
    return ""


def _add(
    findings: list[RouteRecoveryEvidenceBindingFinding],
    *,
    severity: str,
    requirement: str,
    status: str,
    reason: str,
    required_action: str,
) -> None:
    findings.append(
        RouteRecoveryEvidenceBindingFinding(
            severity=severity,
            requirement=requirement,
            status=status,
            reason=reason,
            required_action=required_action,
        )
    )


def _bind_dry_gas(
    *,
    dewpoint_trace_path: str | Path,
    route_or_dryer_check_path: str | Path | None,
    route_or_dryer_check_note: str,
    tail_count: int,
    findings: list[RouteRecoveryEvidenceBindingFinding],
) -> dict[str, Any]:
    rows = _read_csv_rows(dewpoint_trace_path)
    values: list[float] = []
    times: list[float] = []
    for index, row in enumerate(rows):
        dewpoint = _pick_number(row, DEWPOINT_COLUMNS)
        if dewpoint is None:
            continue
        sample_time = _pick_number(row, TIME_COLUMNS)
        values.append(dewpoint)
        times.append(sample_time if sample_time is not None else float(index))
    tail_size = max(2, int(tail_count or 5))
    tail_values = values[-tail_size:]
    tail_times = times[-tail_size:]
    dewpoint_c = max(tail_values) if tail_values else None
    tail_span_c = (max(tail_values) - min(tail_values)) if len(tail_values) >= 2 else None
    if len(tail_values) >= 2:
        elapsed = tail_times[-1] - tail_times[0]
        tail_slope = abs((tail_values[-1] - tail_values[0]) / elapsed) if elapsed > 0 else 0.0
    else:
        tail_slope = None
    checked_ref = _text(route_or_dryer_check_path) or _text(route_or_dryer_check_note)
    status_pass = (
        dewpoint_c is not None
        and dewpoint_c <= -28.0
        and tail_span_c is not None
        and tail_span_c <= 0.5
        and tail_slope is not None
        and tail_slope <= 0.01
        and bool(checked_ref)
    )
    if status_pass:
        _add(
            findings,
            severity="info",
            requirement="dry_gas_dewpoint_recovery",
            status="pass",
            reason=f"Dry-gas dewpoint tail worst value is {dewpoint_c:g} C with stable span/slope.",
            required_action="Feed this packet to the route physical recovery packet validator.",
        )
    else:
        _add(
            findings,
            severity="blocker",
            requirement="dry_gas_dewpoint_recovery",
            status="missing_or_failed",
            reason="Dewpoint trace does not prove <= -28 C, stable tail, and route/dryer review reference.",
            required_action="Recover dry-gas route/dryer state and collect a stable dewpoint trace before opening a continuous queue.",
        )
    return {
        "status": "pass" if status_pass else "failed",
        "dewpoint_c": dewpoint_c,
        "dry_enough_threshold_c": -28.0,
        "tail_span_c": tail_span_c,
        "max_tail_span_c": 0.5,
        "tail_slope_abs_c_per_s": tail_slope,
        "max_tail_slope_abs_c_per_s": 0.01,
        "route_or_dryer_checked": bool(checked_ref),
        "evidence": {
            "dewpoint_trace": str(dewpoint_trace_path),
            "route_or_dryer_check": str(route_or_dryer_check_path or ""),
            "operator_note": route_or_dryer_check_note,
        },
    }


def _vent_state_from_row(row: Mapping[str, Any]) -> str:
    explicit = _pick_text(row, ("vent_state", "vent", "state", "target_state"))
    if explicit:
        return explicit.lower()
    blob = _row_blob(row)
    on_tokens = ("vent 1", "vent=1", "vent,on", "vent_on", "outp on", "outp 1")
    off_tokens = ("vent 0", "vent=0", "vent,off", "vent_off", "outp off", "outp 0")
    if any(token in blob for token in on_tokens):
        return "on"
    if any(token in blob for token in off_tokens):
        return "off"
    return ""


def _bind_pace_vent(
    *,
    pace_vent_trace_path: str | Path,
    findings: list[RouteRecoveryEvidenceBindingFinding],
) -> dict[str, Any]:
    rows = _read_csv_rows(pace_vent_trace_path)
    no_response_absent = not _has_no_response(rows)
    explicit_roundtrip = any(_truthy(row.get("vent_on_off_roundtrip_pass")) for row in rows)
    states = {_vent_state_from_row(row) for row in rows}
    roundtrip = explicit_roundtrip or ({"on", "off"} <= states) or ({"1", "0"} <= states)
    status_pass = bool(rows) and no_response_absent and roundtrip
    if status_pass:
        _add(
            findings,
            severity="info",
            requirement="pace_vent_recovery",
            status="pass",
            reason="PACE vent trace proves ON/OFF roundtrip with no NO_RESPONSE.",
            required_action="Feed this packet to the route physical recovery packet validator.",
        )
    else:
        _add(
            findings,
            severity="blocker",
            requirement="pace_vent_recovery",
            status="missing_or_failed",
            reason="PACE vent trace lacks ON/OFF roundtrip evidence or contains NO_RESPONSE.",
            required_action="Recover PACE vent communication and collect a clean ON/OFF roundtrip trace.",
        )
    return {
        "status": "pass" if status_pass else "failed",
        "vent_on_off_roundtrip_pass": roundtrip,
        "no_response_absent": no_response_absent,
        "evidence": {
            "pace_vent_roundtrip_trace": str(pace_vent_trace_path),
            "operator_note": "",
        },
    }


def _pressure_source_from_rows(rows: list[Mapping[str, Any]], fallback_source: str) -> str:
    for row in rows:
        source = _pick_text(
            row,
            (
                "absolute_pressure_source",
                "pressure_reference_query",
                "pressure_source",
                "source",
                "query",
                "command",
            ),
        ).lower()
        source = source.replace("\\", "/")
        if "inl" in source:
            return "inl"
        if source:
            return source
    return fallback_source.lower().replace("\\", "/")


def _pressure_values_hpa(rows: list[Mapping[str, Any]]) -> list[float]:
    out: list[float] = []
    for row in rows:
        lowered = {str(key).strip().lower(): value for key, value in row.items()}
        for column in PRESSURE_COLUMNS:
            value = _number(lowered.get(column))
            if value is None:
                continue
            out.append(value * 10.0 if column.endswith("_kpa") else value)
            break
    return out


def _bind_pressure_gauge(
    *,
    pressure_gauge_trace_path: str | Path,
    pressure_source: str,
    findings: list[RouteRecoveryEvidenceBindingFinding],
) -> dict[str, Any]:
    rows = _read_csv_rows(pressure_gauge_trace_path)
    no_response_absent = not _has_no_response(rows)
    source = _pressure_source_from_rows(rows, pressure_source)
    values = _pressure_values_hpa(rows)
    source_valid = source in VALID_PRESSURE_SOURCES
    readback_pass = bool(values) and all(300.0 <= value <= 1300.0 for value in values)
    status_pass = bool(rows) and no_response_absent and source_valid and readback_pass
    if status_pass:
        _add(
            findings,
            severity="info",
            requirement="pressure_gauge_recovery",
            status="pass",
            reason="Pressure gauge trace proves INL absolute-pressure readback with valid numeric samples and no NO_RESPONSE.",
            required_action="Feed this packet to the route physical recovery packet validator.",
        )
    else:
        _add(
            findings,
            severity="blocker",
            requirement="pressure_gauge_recovery",
            status="missing_or_failed",
            reason="Pressure gauge trace lacks valid INL absolute-pressure samples or contains NO_RESPONSE.",
            required_action="Recover COM22 pressure-gauge readback and collect an INL absolute-pressure trace.",
        )
    return {
        "status": "pass" if status_pass else "failed",
        "readback_status": "pass" if readback_pass else "failed",
        "absolute_pressure_source": source,
        "no_response_absent": no_response_absent,
        "sample_count": len(values),
        "min_pressure_hpa": min(values) if values else None,
        "max_pressure_hpa": max(values) if values else None,
        "evidence": {
            "pressure_gauge_inl_trace": str(pressure_gauge_trace_path),
            "operator_note": "",
        },
    }


def _accepted_manifest_review(
    *,
    accepted_manifest_path: str | Path | None,
    supersedence_review_id: str,
) -> dict[str, Any]:
    if accepted_manifest_path and supersedence_review_id:
        return {
            "status": "pass",
            "accepted_manifest_path": str(accepted_manifest_path),
            "supersedence_review_id": supersedence_review_id,
        }
    return {
        "status": "missing",
        "accepted_manifest_path": str(accepted_manifest_path or ""),
        "supersedence_review_id": supersedence_review_id,
        "note": "Segmented/direct/retry evidence remains excluded from fitting until accepted-manifest review is supplied.",
    }


def _packet_side_effect_flags() -> dict[str, Any]:
    return {
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "connects_postgresql": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def _next_run_policy() -> dict[str, Any]:
    return {
        "fresh_canonical_queue": True,
        "mature_physical_baseline": "0613/0620/0621",
        "forbidden_surfaces_absent": True,
        "co2_entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
        "h2o_entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
        "note": "Open the next continuous run from mature queue entrypoints only; forbidden surfaces have been reviewed absent.",
    }


def build_v1_5_route_physical_recovery_evidence_binder(
    *,
    dewpoint_trace_path: str | Path,
    pace_vent_trace_path: str | Path,
    pressure_gauge_trace_path: str | Path,
    route_or_dryer_check_path: str | Path | None = None,
    route_or_dryer_check_note: str = "",
    accepted_manifest_path: str | Path | None = None,
    supersedence_review_id: str = "",
    pressure_source: str = "inl",
    tail_count: int = 5,
) -> dict[str, Any]:
    findings: list[RouteRecoveryEvidenceBindingFinding] = []
    dry_gas = _bind_dry_gas(
        dewpoint_trace_path=dewpoint_trace_path,
        route_or_dryer_check_path=route_or_dryer_check_path,
        route_or_dryer_check_note=route_or_dryer_check_note,
        tail_count=tail_count,
        findings=findings,
    )
    pace_vent = _bind_pace_vent(pace_vent_trace_path=pace_vent_trace_path, findings=findings)
    pressure_gauge = _bind_pressure_gauge(
        pressure_gauge_trace_path=pressure_gauge_trace_path,
        pressure_source=pressure_source,
        findings=findings,
    )
    accepted_review = _accepted_manifest_review(
        accepted_manifest_path=accepted_manifest_path,
        supersedence_review_id=supersedence_review_id,
    )
    packet = {
        "schema": PACKET_SCHEMA,
        "dry_gas_dewpoint_recovery": dry_gas,
        "pace_vent_recovery": pace_vent,
        "pressure_gauge_recovery": pressure_gauge,
        "accepted_manifest_review": accepted_review,
        "next_run_policy": _next_run_policy(),
        **_packet_side_effect_flags(),
    }
    blocker_count = sum(1 for item in findings if item.severity == "blocker")
    status = "blocked" if blocker_count else "packet_ready_for_validator"
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "manifest": {
            "status": status,
            "blocker_count": blocker_count,
            "packet_validator_required": True,
            "ready_for_validator": blocker_count == 0,
            "dewpoint_trace_path": str(dewpoint_trace_path),
            "pace_vent_trace_path": str(pace_vent_trace_path),
            "pressure_gauge_trace_path": str(pressure_gauge_trace_path),
            **_packet_side_effect_flags(),
        },
        "findings": [item.to_json() for item in findings],
        "recovery_evidence_packet": packet,
    }


def _markdown(model: Mapping[str, Any]) -> str:
    manifest = model["manifest"] if isinstance(model.get("manifest"), Mapping) else {}
    lines = [
        "# V1.5 Route Physical Recovery Evidence Binder",
        "",
        f"- schema: `{model['schema']}`",
        f"- status: `{manifest.get('status')}`",
        f"- ready_for_validator: `{manifest.get('ready_for_validator')}`",
        f"- blocker_count: `{manifest.get('blocker_count')}`",
        "- boundary: offline trace binding only; no COM, no pressure/route control, no writes, no PostgreSQL.",
        "",
        "## Trace Inputs",
        "",
        f"- dewpoint_trace_path: `{manifest.get('dewpoint_trace_path')}`",
        f"- pace_vent_trace_path: `{manifest.get('pace_vent_trace_path')}`",
        f"- pressure_gauge_trace_path: `{manifest.get('pressure_gauge_trace_path')}`",
        "",
        "## Findings",
        "",
        "| severity | requirement | status | reason | required action |",
        "|---|---|---|---|---|",
    ]
    for row in model.get("findings") or []:
        lines.append(
            "| `{severity}` | `{requirement}` | `{status}` | {reason} | {action} |".format(
                severity=row["severity"],
                requirement=row["requirement"],
                status=row["status"],
                reason=row["reason"],
                action=row["required_action"],
            )
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This binder does not collect live data. It only reads reviewed trace files.",
            "- The emitted packet must still pass `export_v1_5_route_physical_recovery_evidence_packet.py`.",
            "- The binder output is not formal release, database import, route execution, or real acceptance evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_route_physical_recovery_evidence_binder(
    *,
    output_dir: str | Path,
    dewpoint_trace_path: str | Path,
    pace_vent_trace_path: str | Path,
    pressure_gauge_trace_path: str | Path,
    route_or_dryer_check_path: str | Path | None = None,
    route_or_dryer_check_note: str = "",
    accepted_manifest_path: str | Path | None = None,
    supersedence_review_id: str = "",
    pressure_source: str = "inl",
    tail_count: int = 5,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_route_physical_recovery_evidence_binder(
        dewpoint_trace_path=dewpoint_trace_path,
        pace_vent_trace_path=pace_vent_trace_path,
        pressure_gauge_trace_path=pressure_gauge_trace_path,
        route_or_dryer_check_path=route_or_dryer_check_path,
        route_or_dryer_check_note=route_or_dryer_check_note,
        accepted_manifest_path=accepted_manifest_path,
        supersedence_review_id=supersedence_review_id,
        pressure_source=pressure_source,
        tail_count=tail_count,
    )
    paths = {
        "manifest": out / "v1_5_route_physical_recovery_evidence_binder.json",
        "findings": out / "v1_5_route_physical_recovery_evidence_binder_findings.csv",
        "recovery_evidence_packet": out / "v1_5_route_physical_recovery_evidence_packet_from_traces.json",
        "markdown": out / "V1_5_ROUTE_PHYSICAL_RECOVERY_EVIDENCE_BINDER.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["recovery_evidence_packet"].write_text(
        json.dumps(model["recovery_evidence_packet"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with paths["findings"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=tuple(RouteRecoveryEvidenceBindingFinding.__dataclass_fields__.keys()),
        )
        writer.writeheader()
        writer.writerows(model["findings"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "SCHEMA",
    "build_v1_5_route_physical_recovery_evidence_binder",
    "write_v1_5_route_physical_recovery_evidence_binder",
]
