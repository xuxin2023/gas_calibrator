"""V1.5 pressure-channel completion package.

This module is offline-only. It consolidates an already-controlled SENCO9 write,
post-write pressure verification, and COM22 certificate traceability into a
readiness artifact for the formal open-flow CO2/H2O main calibration.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .common import load_csv_rows
from .pressure_channel import validate_pressure_reference_traceability
from .reporting import ValidationMetadata, write_validation_report


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> Dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"JSON file not found: {source}")
    return json.loads(source.read_text(encoding="utf-8"))


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "ok", "pass", "verified"}


def _device_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.isdigit():
        return f"{int(text):03d}"
    return text.upper()


def _file_sha256(path: str | Path) -> str:
    source = Path(path)
    digest = hashlib.sha256()
    with source.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def _json_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _index_by_device(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        if device:
            out[device] = dict(row)
    return out


def _device_sort_key(device_id: str, write_by_device: Mapping[str, Mapping[str, Any]], fit_by_device: Mapping[str, Mapping[str, Any]]) -> tuple[str, str]:
    row = write_by_device.get(device_id) or fit_by_device.get(device_id) or {}
    return (str(row.get("analyzer_prefix") or ""), str(device_id))


def _normalize_device_ids(values: Optional[Sequence[str]]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values or []:
        device = _device_id(value)
        if device and device not in seen:
            out.append(device)
            seen.add(device)
    return out


def _first_traceability_status(trace_rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    if trace_rows:
        row = dict(trace_rows[0])
        return {
            "status": str(row.get("status") or ""),
            "validation_level": str(row.get("validation_level") or ""),
            "reasons": str(row.get("reasons") or ""),
            "device_id": str(row.get("device_id") or ""),
            "certificate_id": str(row.get("certificate_id") or ""),
            "certificate_hash": str(row.get("certificate_hash") or ""),
            "valid_until": str(row.get("valid_until") or ""),
            "uncertainty_hpa": row.get("uncertainty_hpa", ""),
        }
    return {}


def _build_traceability_rows(
    *,
    pressure_reference_path: str | Path,
    pressure_fit_traceability_rows: Sequence[Mapping[str, Any]],
    today: Any = None,
) -> List[Dict[str, Any]]:
    reference = _load_json(pressure_reference_path)
    check = validate_pressure_reference_traceability(reference, today=today)
    from_fit = _first_traceability_status(pressure_fit_traceability_rows)
    return [
        {
            "reference_role": "primary_pressure_reference",
            "status": check.status,
            "validation_level": check.validation_level,
            "reasons": _json_compact(check.reasons),
            "device_id": check.device_id,
            "certificate_id": check.certificate_id,
            "certificate_hash": check.certificate_hash,
            "valid_until": check.valid_until,
            "uncertainty_hpa": check.uncertainty_hpa if check.uncertainty_hpa is not None else "",
            "pressure_reference_json": str(Path(pressure_reference_path).resolve()),
            "pressure_reference_json_sha256": _file_sha256(pressure_reference_path),
            "fit_traceability_status": from_fit.get("status", ""),
            "fit_traceability_validation_level": from_fit.get("validation_level", ""),
            "fit_traceability_reasons": from_fit.get("reasons", ""),
        }
    ]


def _artifact_rows(paths: Mapping[str, str | Path | None]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for role, raw_path in paths.items():
        if not raw_path:
            rows.append({"artifact_role": role, "path": "", "exists": False, "sha256": ""})
            continue
        path = Path(raw_path).resolve()
        rows.append(
            {
                "artifact_role": role,
                "path": str(path),
                "exists": path.exists(),
                "sha256": _file_sha256(path) if path.exists() and path.is_file() else "",
            }
        )
    return rows


def _status_with_reasons(
    *,
    write_row: Mapping[str, Any],
    fit_row: Mapping[str, Any],
    traceability_ok: bool,
    max_abs_offset_kpa: float,
    max_residual_hpa: float,
) -> tuple[str, List[str]]:
    reasons: List[str] = []
    if str(write_row.get("status") or "").strip() != "written_readback_verified":
        reasons.append("senco9_write_not_verified")
    if not _truthy(write_row.get("write_applied")):
        reasons.append("senco9_write_not_applied")
    if not _truthy(write_row.get("readback_verified")):
        reasons.append("senco9_readback_not_verified")
    if _device_id(write_row.get("identity_before")) != _device_id(write_row.get("analyzer_device_id")):
        reasons.append("identity_before_mismatch")
    if _device_id(write_row.get("identity_after")) != _device_id(write_row.get("analyzer_device_id")):
        reasons.append("identity_after_mismatch")
    if str(fit_row.get("status") or "").strip().lower() != "pass":
        reasons.append("post_write_pressure_fit_not_pass")
    if not traceability_ok:
        reasons.append("pressure_reference_traceability_not_pass")
    offset = _safe_float(fit_row.get("offset_only_offset_kpa"))
    if offset is None or abs(offset) > float(max_abs_offset_kpa):
        reasons.append("post_write_offset_out_of_limit")
    residual = _safe_float(fit_row.get("offset_only_residual_max_abs_hpa"))
    if residual is None or residual > float(max_residual_hpa):
        reasons.append("post_write_residual_out_of_limit")
    return ("pass" if not reasons else "blocked", reasons)


def build_pressure_channel_completion_tables(
    *,
    senco9_write_summary_path: str | Path,
    post_write_fit_summary_path: str | Path,
    pressure_reference_path: str | Path,
    pressure_reference_traceability_path: str | Path | None = None,
    old_getco_snapshot_path: str | Path | None = None,
    selected_device_ids: Optional[Sequence[str]] = None,
    known_limitations: Optional[Sequence[Mapping[str, Any]]] = None,
    max_abs_offset_kpa: float = 0.05,
    max_residual_hpa: float = 0.5,
    acceptance_policy_note: str = "",
    today: Any = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build pressure-channel completion/readiness tables from existing artifacts."""

    write_rows = load_csv_rows(senco9_write_summary_path)
    fit_rows = load_csv_rows(post_write_fit_summary_path)
    trace_rows = (
        load_csv_rows(pressure_reference_traceability_path)
        if pressure_reference_traceability_path and Path(pressure_reference_traceability_path).exists()
        else []
    )
    traceability_rows = _build_traceability_rows(
        pressure_reference_path=pressure_reference_path,
        pressure_fit_traceability_rows=trace_rows,
        today=today,
    )
    traceability_ok = str(traceability_rows[0].get("status") or "") == "pass"

    write_by_device = _index_by_device(write_rows)
    fit_by_device = _index_by_device(fit_rows)
    observed_devices = sorted(
        set(write_by_device) | set(fit_by_device),
        key=lambda item: _device_sort_key(item, write_by_device, fit_by_device),
    )
    selected_devices = _normalize_device_ids(selected_device_ids)
    if selected_devices:
        selected_set = set(selected_devices)
        all_devices = sorted(
            set(selected_devices),
            key=lambda item: _device_sort_key(item, write_by_device, fit_by_device),
        )
    else:
        selected_set = set(observed_devices)
        all_devices = observed_devices
    excluded_rows: List[Dict[str, Any]] = []
    for device in observed_devices:
        if device in selected_set:
            continue
        write_row = write_by_device.get(device, {})
        fit_row = fit_by_device.get(device, {})
        reasons = ["not_in_selected_pressure_completion_scope"]
        if str(fit_row.get("status") or "").strip().lower() != "pass":
            reasons.append("post_write_pressure_fit_not_pass")
        if str(write_row.get("status") or "").strip() != "written_readback_verified":
            reasons.append("senco9_write_not_verified")
        excluded_rows.append(
            {
                "analyzer_prefix": write_row.get("analyzer_prefix") or fit_row.get("analyzer_prefix") or "",
                "analyzer_device_id": device,
                "exclusion_status": "excluded_from_this_completion_scope",
                "exclusion_reasons": ";".join(reasons),
                "post_write_fit_status": fit_row.get("status", ""),
                "valid_pair_count": fit_row.get("valid_pair_count", ""),
                "distinct_pressure_points": fit_row.get("distinct_pressure_points", ""),
                "can_enter_open_flow_main_calibration": False,
                "pressure_channel_only": True,
                "not_co2_h2o_fit_evidence": True,
            }
        )
    device_rows: List[Dict[str, Any]] = []
    for device in all_devices:
        write_row = write_by_device.get(device, {})
        fit_row = fit_by_device.get(device, {})
        status, reasons = _status_with_reasons(
            write_row=write_row,
            fit_row=fit_row,
            traceability_ok=traceability_ok,
            max_abs_offset_kpa=float(max_abs_offset_kpa),
            max_residual_hpa=float(max_residual_hpa),
        )
        device_rows.append(
            {
                "analyzer_prefix": write_row.get("analyzer_prefix") or fit_row.get("analyzer_prefix") or "",
                "analyzer_device_id": device,
                "readiness_status": status,
                "readiness_reasons": ";".join(reasons),
                "old_senco9_c0": write_row.get("old_senco9_c0", ""),
                "senco9_offset_delta_kpa": write_row.get("candidate_offset_kpa", ""),
                "target_senco9_c0": write_row.get("target_senco9_c0", ""),
                "senco9_write_status": write_row.get("status", ""),
                "write_applied": _truthy(write_row.get("write_applied")),
                "readback_verified": _truthy(write_row.get("readback_verified")),
                "identity_before": write_row.get("identity_before", ""),
                "identity_after": write_row.get("identity_after", ""),
                "post_write_fit_status": fit_row.get("status", ""),
                "post_write_offset_kpa": fit_row.get("offset_only_offset_kpa", ""),
                "post_write_residual_max_abs_hpa": fit_row.get("offset_only_residual_max_abs_hpa", ""),
                "post_write_slope_bias": fit_row.get("linear_slope_bias", ""),
                "valid_pair_count": fit_row.get("valid_pair_count", ""),
                "distinct_pressure_points": fit_row.get("distinct_pressure_points", ""),
                "reference_span_hpa": fit_row.get("reference_span_hpa", ""),
                "pressure_reference_certificate_id": traceability_rows[0].get("certificate_id", ""),
                "pressure_reference_certificate_hash": traceability_rows[0].get("certificate_hash", ""),
                "pressure_reference_valid_until": traceability_rows[0].get("valid_until", ""),
                "can_enter_open_flow_main_calibration": status == "pass",
                "can_write_co2_h2o_coefficients": False,
                "pressure_channel_only": True,
                "not_co2_h2o_fit_evidence": True,
            }
        )

    all_ready = bool(device_rows) and all(row["readiness_status"] == "pass" for row in device_rows)
    limitation_rows: List[Dict[str, Any]] = []
    for item in known_limitations or []:
        limitation_rows.append(
            {
                "limitation_id": str(item.get("limitation_id") or item.get("id") or ""),
                "status": str(item.get("status") or "engineering_diagnostic"),
                "reason": str(item.get("reason") or ""),
                "impact": str(item.get("impact") or ""),
                "blocks_selected_device_completion": _truthy(item.get("blocks_selected_device_completion")),
                "pressure_channel_only": True,
                "not_co2_h2o_fit_evidence": True,
            }
        )
    summary_rows = [
        {
            "overall_status": "ready_for_open_flow_main_calibration" if all_ready else "blocked",
            "completion_scope_device_ids": ",".join(all_devices),
            "device_count": len(device_rows),
            "ready_device_count": sum(1 for row in device_rows if row["readiness_status"] == "pass"),
            "blocked_device_count": sum(1 for row in device_rows if row["readiness_status"] != "pass"),
            "excluded_device_count": len(excluded_rows),
            "known_limitation_count": len(limitation_rows),
            "pressure_reference_status": traceability_rows[0].get("status", ""),
            "pressure_reference_validation_level": traceability_rows[0].get("validation_level", ""),
            "pressure_reference_certificate_id": traceability_rows[0].get("certificate_id", ""),
            "pressure_reference_certificate_hash": traceability_rows[0].get("certificate_hash", ""),
            "max_abs_offset_kpa_limit": float(max_abs_offset_kpa),
            "max_residual_hpa_limit": float(max_residual_hpa),
            "controls_water_or_gas_routes": False,
            "opens_com_ports": False,
            "writes_coefficients": False,
            "ready_for_open_flow_sampling": all_ready,
            "ready_for_co2_h2o_candidate_review": False,
            "meaning": (
                "Pressure input P has independent traceable evidence and may be used as a precondition "
                "for open-flow CO2/H2O main calibration."
                if all_ready
                else "Pressure channel evidence is incomplete or blocked; formal CO2/H2O coefficient writes remain blocked."
            ),
        }
    ]
    gate_rows = [
        {
            "gate": "pressure_channel_precondition_for_open_flow",
            "status": "pass" if all_ready else "fail",
            "blocks_formal_co2_h2o_coefficient_write": not all_ready,
            "allows_open_flow_sampling": all_ready,
            "allows_pressure_compensation_validation": all_ready,
            "reason": "" if all_ready else "one_or_more_pressure_channel_completion_checks_failed",
        }
    ]
    policy_rows = [
        {
            "policy_id": "pressure_channel_completion_acceptance_policy",
            "max_abs_offset_kpa_limit": float(max_abs_offset_kpa),
            "max_residual_hpa_limit": float(max_residual_hpa),
            "scope": "independent_pressure_input_readiness_for_open_flow_co2_h2o",
            "not_pressure_compensation_acceptance": True,
            "not_co2_h2o_fit_evidence": True,
            "note": str(acceptance_policy_note or "").strip(),
        }
    ]
    artifacts = _artifact_rows(
        {
            "senco9_write_summary": senco9_write_summary_path,
            "post_write_pressure_fit_summary": post_write_fit_summary_path,
            "pressure_reference_json": pressure_reference_path,
            "pressure_reference_traceability": pressure_reference_traceability_path,
            "old_getco9_snapshot": old_getco_snapshot_path,
        }
    )
    return {
        "pressure_channel_completion_summary": summary_rows,
        "pressure_channel_device_readiness": device_rows,
        "pressure_channel_excluded_devices": excluded_rows,
        "pressure_channel_known_limitations": limitation_rows,
        "pressure_channel_traceability": traceability_rows,
        "pressure_channel_readiness_gate": gate_rows,
        "pressure_channel_acceptance_policy": policy_rows,
        "pressure_channel_completion_artifacts": artifacts,
    }


def _write_markdown_report(destination: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    summary = (tables.get("pressure_channel_completion_summary") or [{}])[0]
    devices = list(tables.get("pressure_channel_device_readiness") or [])
    trace = (tables.get("pressure_channel_traceability") or [{}])[0]
    report_path = destination / "pressure_channel_completion_report.md"
    lines = [
        "# V1.5 Pressure Channel Completion Report",
        "",
        f"- Overall status: {summary.get('overall_status', '')}",
        f"- Completion scope: {summary.get('completion_scope_device_ids', '')}",
        f"- Ready devices: {summary.get('ready_device_count', 0)} / {summary.get('device_count', 0)}",
        f"- Excluded devices: {summary.get('excluded_device_count', 0)}",
        f"- Known limitations: {summary.get('known_limitation_count', 0)}",
        f"- Pressure reference: {trace.get('certificate_id', '')} ({trace.get('status', '')})",
        f"- Certificate hash: {trace.get('certificate_hash', '')}",
        f"- Offset limit: {summary.get('max_abs_offset_kpa_limit', '')} kPa",
        f"- Residual limit: {summary.get('max_residual_hpa_limit', '')} hPa",
        "- Boundary: offline evidence consolidation only; no COM ports opened, no PACE/valve/water/gas route control, no coefficient writes.",
        "",
        "## Device Readiness",
        "",
        "| Analyzer | Device ID | Old C0 | Delta kPa | Target C0 | Post-write offset kPa | Max residual hPa | Status |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in devices:
        lines.append(
            "| {prefix} | {device} | {old} | {delta} | {target} | {offset} | {resid} | {status} |".format(
                prefix=row.get("analyzer_prefix", ""),
                device=row.get("analyzer_device_id", ""),
                old=row.get("old_senco9_c0", ""),
                delta=row.get("senco9_offset_delta_kpa", ""),
                target=row.get("target_senco9_c0", ""),
                offset=row.get("post_write_offset_kpa", ""),
                resid=row.get("post_write_residual_max_abs_hpa", ""),
                status=row.get("readiness_status", ""),
            )
        )
    excluded = list(tables.get("pressure_channel_excluded_devices") or [])
    if excluded:
        lines.extend(
            [
                "",
                "## Excluded Devices",
                "",
                "| Analyzer | Device ID | Reason | Post-write fit status |",
                "| --- | --- | --- | --- |",
            ]
        )
        for row in excluded:
            lines.append(
                "| {prefix} | {device} | {reason} | {fit} |".format(
                    prefix=row.get("analyzer_prefix", ""),
                    device=row.get("analyzer_device_id", ""),
                    reason=row.get("exclusion_reasons", ""),
                    fit=row.get("post_write_fit_status", ""),
                )
            )
    limitations = list(tables.get("pressure_channel_known_limitations") or [])
    if limitations:
        lines.extend(
            [
                "",
                "## Known Limitations",
                "",
                "| Limitation | Status | Reason | Impact |",
                "| --- | --- | --- | --- |",
            ]
        )
        for row in limitations:
            lines.append(
                "| {item} | {status} | {reason} | {impact} |".format(
                    item=row.get("limitation_id", ""),
                    status=row.get("status", ""),
                    reason=row.get("reason", ""),
                    impact=row.get("impact", ""),
                )
            )
    policy = list(tables.get("pressure_channel_acceptance_policy") or [])
    if policy:
        lines.extend(
            [
                "",
                "## Acceptance Policy",
                "",
                "| Policy | Scope | Offset limit kPa | Residual limit hPa | Note |",
                "| --- | --- | ---: | ---: | --- |",
            ]
        )
        for row in policy:
            lines.append(
                "| {policy} | {scope} | {offset} | {residual} | {note} |".format(
                    policy=row.get("policy_id", ""),
                    scope=row.get("scope", ""),
                    offset=row.get("max_abs_offset_kpa_limit", ""),
                    residual=row.get("max_residual_hpa_limit", ""),
                    note=str(row.get("note", "")).replace("|", "/"),
                )
            )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- This package verifies the analyzer internal pressure input P against COM22 after the controlled SENCO9 pressure-channel update.",
            "- It is a precondition for open-flow CO2/H2O main calibration, not CO2/H2O fitting evidence by itself.",
            "- CO2/H2O candidate coefficients remain blocked until open-flow component samples pass QC and reviewer approval.",
            "- Sealed pressure points and dynamic pressure probes remain diagnostic and do not enter formal CO2/H2O fitting.",
            "",
        ]
    )
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def write_pressure_channel_completion_report(
    *,
    output_dir: str | Path,
    senco9_write_summary_path: str | Path,
    post_write_fit_summary_path: str | Path,
    pressure_reference_path: str | Path,
    pressure_reference_traceability_path: str | Path | None = None,
    old_getco_snapshot_path: str | Path | None = None,
    selected_device_ids: Optional[Sequence[str]] = None,
    known_limitations: Optional[Sequence[Mapping[str, Any]]] = None,
    max_abs_offset_kpa: float = 0.05,
    max_residual_hpa: float = 0.5,
    acceptance_policy_note: str = "",
    today: Any = None,
) -> Dict[str, Path]:
    """Write the pressure-channel completion workbook, CSVs, and markdown report."""

    destination = Path(output_dir).resolve()
    tables = build_pressure_channel_completion_tables(
        senco9_write_summary_path=senco9_write_summary_path,
        post_write_fit_summary_path=post_write_fit_summary_path,
        pressure_reference_path=pressure_reference_path,
        pressure_reference_traceability_path=pressure_reference_traceability_path,
        old_getco_snapshot_path=old_getco_snapshot_path,
        selected_device_ids=selected_device_ids,
        known_limitations=known_limitations,
        max_abs_offset_kpa=max_abs_offset_kpa,
        max_residual_hpa=max_residual_hpa,
        acceptance_policy_note=acceptance_policy_note,
        today=today,
    )
    devices = [
        f"{row.get('analyzer_prefix')}:{row.get('analyzer_device_id')}"
        for row in tables.get("pressure_channel_device_readiness", [])
    ]
    metadata = ValidationMetadata(
        tool_name="export_v1_5_pressure_channel_completion",
        created_at=_now(),
        analyzers=devices,
        input_paths=[
            str(Path(senco9_write_summary_path).resolve()),
            str(Path(post_write_fit_summary_path).resolve()),
            str(Path(pressure_reference_path).resolve()),
            str(Path(pressure_reference_traceability_path).resolve()) if pressure_reference_traceability_path else "",
            str(Path(old_getco_snapshot_path).resolve()) if old_getco_snapshot_path else "",
        ],
        output_dir=str(destination),
        config_summary={
            "max_abs_offset_kpa": float(max_abs_offset_kpa),
            "max_residual_hpa": float(max_residual_hpa),
            "acceptance_policy_note": str(acceptance_policy_note or "").strip(),
            "selected_device_ids": _normalize_device_ids(selected_device_ids),
            "known_limitations": list(known_limitations or []),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
        notes=[
            "Offline pressure-channel completion package.",
            "No COM ports are opened and no water/gas route, PACE, valve, SENCO, or device-ID writes are performed.",
            "This is pressure-input readiness evidence for open-flow CO2/H2O calibration, not CO2/H2O fit evidence.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="pressure_channel_completion",
        metadata=metadata,
        tables=tables,
    )
    outputs["markdown"] = _write_markdown_report(destination, tables)
    return outputs
