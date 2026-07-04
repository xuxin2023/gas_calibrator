"""Controlled SENCO9 write-review artifacts for V1.5 pressure calibration.

This module is offline-only. It reads no-write pressure/SENCO9 fit artifacts and
turns them into a single-device write-review package. It never opens COM ports,
controls PACE or valves, switches water/gas routes, writes SENCO9, or changes a
device ID.
"""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..senco_format import format_senco_values
from .common import load_csv_rows
from .reporting import ValidationMetadata, write_validation_report


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "pass", "ok", "verified"}


def _table_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def _load_json(path: str | Path | None) -> Dict[str, Any]:
    if path in (None, ""):
        return {}
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"JSON file not found: {source}")
    return json.loads(source.read_text(encoding="utf-8"))


def _extract_old_getco9(snapshot: Mapping[str, Any], device_id: str = "") -> str:
    if not snapshot:
        return ""
    candidates: List[Any] = []
    if device_id:
        for key in (device_id, f"device_{device_id}", f"YGAS_{device_id}"):
            value = snapshot.get(key)
            if isinstance(value, Mapping):
                candidates.extend([value.get("GETCO9"), value.get("GETCO9_before"), value.get("SENCO9"), value.get("9")])
        devices = snapshot.get("devices")
        if isinstance(devices, Mapping):
            value = devices.get(device_id)
            if isinstance(value, Mapping):
                candidates.extend([value.get("GETCO9"), value.get("GETCO9_before"), value.get("SENCO9"), value.get("9")])
    candidates.extend(
        [
            snapshot.get("GETCO9"),
            snapshot.get("SENCO9"),
            snapshot.get("senco9"),
            snapshot.get("9"),
            snapshot.get("old_getco9"),
            snapshot.get("old_senco9"),
        ]
    )
    coefficients = snapshot.get("coefficients")
    if isinstance(coefficients, Mapping):
        candidates.extend([coefficients.get("GETCO9"), coefficients.get("SENCO9"), coefficients.get("9")])
    for value in candidates:
        if value not in (None, ""):
            if isinstance(value, (list, tuple)):
                return ",".join(str(item) for item in value)
            return str(value)
    return ""


def _candidate_supported(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("status") or "").strip().lower() == "pass"
        and str(row.get("recommendation") or "").strip()
        == "review_senco9_offset_candidate_no_write"
        and not _truthy(row.get("write_allowed"))
    )


def _linear_exception_supported(row: Mapping[str, Any]) -> bool:
    intercept = _safe_float(row.get("linear_intercept_kpa"))
    slope = _safe_float(row.get("linear_slope"))
    offset_mean = _safe_float(row.get("offset_only_residual_mean_abs_hpa"))
    offset_max = _safe_float(row.get("offset_only_residual_max_abs_hpa"))
    residual_mean = _safe_float(row.get("linear_residual_mean_abs_hpa"))
    residual_max = _safe_float(row.get("linear_residual_max_abs_hpa"))
    return (
        str(row.get("status") or "").strip().lower() == "fail"
        and intercept is not None
        and slope is not None
        and (
            (offset_mean is not None and offset_mean > 1.0)
            or (offset_max is not None and offset_max > 2.0)
        )
        and residual_mean is not None
        and residual_mean <= 0.75
        and residual_max is not None
        and residual_max <= 1.5
        and not _truthy(row.get("write_allowed"))
    )


def _linear_exception_command(row: Mapping[str, Any]) -> str:
    intercept = _safe_float(row.get("linear_intercept_kpa"))
    slope = _safe_float(row.get("linear_slope"))
    if intercept is None or slope is None:
        return ""
    return "SENCO9,YGAS,FFF," + ",".join(format_senco_values((intercept, slope, 0.0, 0.0)))


def _selected(row: Mapping[str, Any], *, selected_device_id: str, selected_prefix: str) -> bool:
    device_id = str(row.get("analyzer_device_id") or "").strip()
    prefix = str(row.get("analyzer_prefix") or "").strip().lower()
    if selected_device_id and device_id == selected_device_id:
        return True
    if selected_prefix and prefix == selected_prefix.lower():
        return True
    return False


def build_pressure_senco9_write_review_tables(
    *,
    fit_summary_rows: Sequence[Mapping[str, Any]],
    point_mean_rows: Optional[Sequence[Mapping[str, Any]]] = None,
    selected_analyzer_device_id: str = "",
    selected_analyzer_prefix: str = "",
    old_getco_snapshot: Optional[Mapping[str, Any]] = None,
    reviewer: str = "",
    approver: str = "",
    allow_linear_senco9_exception: bool = False,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Build controlled-write review tables from no-write fit results."""

    selected_device_id = str(selected_analyzer_device_id or "").strip()
    selected_prefix = str(selected_analyzer_prefix or "").strip().lower()
    point_rows = list(point_mean_rows or [])
    old_snapshot = dict(old_getco_snapshot or {})
    reviewer_name = str(reviewer or "").strip()
    approver_name = str(approver or "").strip()

    candidate_rows: List[Dict[str, Any]] = []
    selected_rows: List[Mapping[str, Any]] = []
    supported_count = 0
    for row in fit_summary_rows:
        offset_supported = _candidate_supported(row)
        linear_supported = bool(allow_linear_senco9_exception) and _linear_exception_supported(row)
        supported = offset_supported or linear_supported
        if supported:
            supported_count += 1
        is_selected = _selected(row, selected_device_id=selected_device_id, selected_prefix=selected_prefix)
        if is_selected:
            selected_rows.append(row)
        device_id = str(row.get("analyzer_device_id") or "").strip()
        old_getco9 = _extract_old_getco9(old_snapshot, device_id) if is_selected else ""
        candidate_command = (
            _linear_exception_command(row) if linear_supported else row.get("senco9_candidate_command", "")
        )
        candidate_model = "linear_exception" if linear_supported else "offset_only"
        candidate_rows.append(
            {
                "analyzer_prefix": str(row.get("analyzer_prefix") or ""),
                "analyzer_device_id": device_id,
                "candidate_status": (
                    "supported_linear_exception_for_review"
                    if linear_supported
                    else "supported_for_review"
                    if offset_supported
                    else "blocked"
                ),
                "candidate_model": candidate_model,
                "selected_for_controlled_write_review": bool(is_selected),
                "fit_status": str(row.get("status") or ""),
                "recommendation": str(row.get("recommendation") or ""),
                "reason": str(row.get("reason") or ""),
                "valid_pair_count": row.get("valid_pair_count", ""),
                "distinct_pressure_points": row.get("distinct_pressure_points", ""),
                "reference_span_hpa": row.get("reference_span_hpa", ""),
                "candidate_offset_kpa": row.get("offset_only_offset_kpa", ""),
                "offset_residual_mean_abs_hpa": row.get("offset_only_residual_mean_abs_hpa", ""),
                "offset_residual_max_abs_hpa": row.get("offset_only_residual_max_abs_hpa", ""),
                "linear_intercept_kpa": row.get("linear_intercept_kpa", ""),
                "linear_slope": row.get("linear_slope", ""),
                "linear_slope_bias": row.get("linear_slope_bias", ""),
                "linear_residual_mean_abs_hpa": row.get("linear_residual_mean_abs_hpa", ""),
                "linear_residual_max_abs_hpa": row.get("linear_residual_max_abs_hpa", ""),
                "candidate_command": candidate_command,
                "candidate_command_scope": "review_only_not_execution_do_not_broadcast_fff",
                "old_getco9_snapshot": old_getco9,
                "write_allowed_by_evaluation_artifact": bool(_truthy(row.get("write_allowed"))),
                "write_allowed_by_this_tool": False,
            }
        )

    selected_supported = [
        row
        for row in selected_rows
        if _candidate_supported(row) or (allow_linear_senco9_exception and _linear_exception_supported(row))
    ]
    old_getco9_selected = ""
    selected_device = ""
    selected_channel = ""
    selected_command = ""
    if len(selected_supported) == 1:
        selected = selected_supported[0]
        selected_device = str(selected.get("analyzer_device_id") or "").strip()
        selected_channel = str(selected.get("analyzer_prefix") or "").strip()
        selected_command = (
            _linear_exception_command(selected)
            if allow_linear_senco9_exception and _linear_exception_supported(selected)
            else str(selected.get("senco9_candidate_command") or "").strip()
        )
        old_getco9_selected = _extract_old_getco9(old_snapshot, selected_device)

    checks: List[Dict[str, Any]] = []

    def add_check(check: str, status: str, reasons: Iterable[str], **extra: Any) -> None:
        checks.append(
            {
                "check": check,
                "status": status,
                "reasons": ";".join(str(item) for item in reasons if item),
                **{str(key): _table_value(value) for key, value in extra.items()},
            }
        )

    add_check(
        "candidate_evidence_available",
        "pass" if supported_count else "fail",
        [] if supported_count else ["no_supported_no_write_offset_candidate"],
        supported_candidate_count=supported_count,
    )
    add_check(
        "single_device_selection",
        "pass" if len(selected_supported) == 1 else "fail",
        []
        if len(selected_supported) == 1
        else [
            "select_exactly_one_supported_analyzer_device_id"
            if not selected_supported
            else "multiple_selected_supported_analyzers",
        ],
        selected_analyzer_device_id=selected_device,
        selected_analyzer_prefix=selected_channel,
    )
    add_check(
        "old_getco9_snapshot",
        "pass" if bool(old_getco9_selected) else "fail",
        [] if old_getco9_selected else ["old_getco9_snapshot_missing_for_rollback"],
    )
    add_check(
        "reviewer_approver",
        "pass"
        if reviewer_name and approver_name and reviewer_name != approver_name
        else "fail",
        [
            reason
            for reason in (
                "reviewer_missing" if not reviewer_name else "",
                "approver_missing" if not approver_name else "",
                "reviewer_and_approver_must_differ"
                if reviewer_name and approver_name and reviewer_name == approver_name
                else "",
            )
            if reason
        ],
        reviewer=reviewer_name,
        approver=approver_name,
    )
    add_check(
        "evaluation_artifact_no_write",
        "pass"
        if not any(_truthy(row.get("write_allowed")) for row in fit_summary_rows)
        else "fail",
        []
        if not any(_truthy(row.get("write_allowed")) for row in fit_summary_rows)
        else ["input_fit_artifact_claims_write_allowed"],
    )
    add_check(
        "physical_boundary",
        "pass",
        [],
        controls_water_or_gas_routes=False,
        controls_humidity_generator=False,
        writes_senco9=False,
        writes_device_id=False,
        formal_co2_h2o_fit=False,
        allow_linear_senco9_exception=bool(allow_linear_senco9_exception),
    )
    add_check(
        "broadcast_address_guard",
        "pass",
        [],
        candidate_command_scope="review_only",
        execution_command_generated=False,
        required_writer_scope="single_selected_analyzer_port_and_device_id",
    )

    failed = [row for row in checks if row["status"] == "fail"]
    review_status = "ready_for_controlled_single_device_write_review" if not failed else "blocked"
    summary = [
        {
            "review_status": review_status,
            "failed_checks": ";".join(str(row["check"]) for row in failed),
            "supported_candidate_count": supported_count,
            "selected_analyzer_prefix": selected_channel,
            "selected_analyzer_device_id": selected_device,
            "selected_candidate_command": selected_command,
            "selected_candidate_command_is_review_only": True,
            "execution_command_generated": False,
            "old_getco9_snapshot_present": bool(old_getco9_selected),
            "reviewer": reviewer_name,
            "approver": approver_name,
            "allow_linear_senco9_exception": bool(allow_linear_senco9_exception),
            "write_allowed_by_this_tool": False,
            "controls_water_or_gas_routes": False,
            "writes_senco9": False,
            "writes_device_id": False,
            "not_real_acceptance_evidence": True,
        }
    ]

    execution_steps = [
        {
            "order": 1,
            "step": "freeze_current_run",
            "required": True,
            "meaning": "Ensure no open-flow CO2/H2O or water-route run is active.",
            "device_action": "none",
        },
        {
            "order": 2,
            "step": "backup_old_getco9",
            "required": True,
            "meaning": "Read and hash current GETCO9/SENCO9 before any possible write.",
            "device_action": "read_only",
        },
        {
            "order": 3,
            "step": "approve_single_device",
            "required": True,
            "meaning": "Reviewer and approver authorize exactly one analyzer device ID.",
            "device_action": "none",
        },
        {
            "order": 4,
            "step": "controlled_senco9_write",
            "required": True,
            "meaning": "Use the candidate only in a separate locked writer, never in this review tool.",
            "device_action": "single_device_senco9_write_after_approval_no_broadcast",
        },
        {
            "order": 5,
            "step": "readback_getco9",
            "required": True,
            "meaning": "Read back the written coefficient and compare to candidate.",
            "device_action": "read_only_after_write",
        },
        {
            "order": 6,
            "step": "post_write_pressure_verification",
            "required": True,
            "meaning": "Run no-write pressure channel verification before CO2/H2O coefficient work.",
            "device_action": "pressure_only_no_write",
        },
        {
            "order": 7,
            "step": "rollback_if_verification_fails",
            "required": True,
            "meaning": "Restore old GETCO9/SENCO9 from the pre-write snapshot.",
            "device_action": "single_device_rollback_after_approval",
        },
    ]

    rollback_rows = [
        {
            "selected_analyzer_prefix": selected_channel,
            "selected_analyzer_device_id": selected_device,
            "old_getco9_snapshot": old_getco9_selected,
            "rollback_available": bool(old_getco9_selected),
            "rollback_boundary": "single selected analyzer only; no water/gas route action",
        }
    ]

    selected_points = [
        {
            **dict(row),
            "selected_for_controlled_write_review": str(row.get("analyzer_prefix") or "").strip() == selected_channel,
        }
        for row in point_rows
    ]

    tables = {
        "pressure_senco9_write_review_summary": summary,
        "pressure_senco9_write_review_checks": checks,
        "pressure_senco9_write_candidates": candidate_rows,
        "controlled_write_steps": execution_steps,
        "rollback_plan": rollback_rows,
        "selected_point_means": selected_points,
    }
    context = {
        "review_status": review_status,
        "selected_analyzer_device_id": selected_device,
        "selected_analyzer_prefix": selected_channel,
        "selected_candidate_command": selected_command,
    }
    return tables, context


def _runbook_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]], context: Mapping[str, Any]) -> str:
    summary = (tables.get("pressure_senco9_write_review_summary") or [{}])[0]
    lines = [
        "# V1.5 Pressure/SENCO9 Controlled Write Review",
        "",
        f"- review_status: {summary.get('review_status', '')}",
        f"- selected_analyzer_device_id: {summary.get('selected_analyzer_device_id', '')}",
        f"- selected_analyzer_prefix: {summary.get('selected_analyzer_prefix', '')}",
        "- write_allowed_by_this_tool: false",
        "- execution_command_generated: false",
        "- controls_water_or_gas_routes: false",
        "- writes_device_id: false",
        "- formal_co2_h2o_fit: false",
        "",
        "## Candidate Command (Review Only)",
        "",
        "```text",
        str(context.get("selected_candidate_command") or "blocked_until_single_device_review_passes"),
        "```",
        "",
        "Do not execute the review command directly on a multi-analyzer bench. "
        "The later locked writer must address exactly one selected analyzer port/device ID and must not use a broadcast write.",
        "",
        "## Required Gates",
        "",
    ]
    for row in tables.get("pressure_senco9_write_review_checks", []):
        lines.append(f"- {row.get('check')}: {row.get('status')} {row.get('reasons') or ''}".rstrip())
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "SENCO9 changes the analyzer internal pressure input used by CO2/H2O calculations. "
            "This package only decides whether one selected analyzer is ready for a separate, "
            "locked, approved writer. It is not a CO2/H2O calibration fit and does not write anything.",
            "",
            "## Execution Boundary",
            "",
            "Only one analyzer device ID may be written at a time. The device identity must come from "
            "the analyzer's own MODE2 frame ID, not from the acquisition channel name. Candidate commands in "
            "this report are value evidence only, not executable broadcast instructions.",
            "",
        ]
    )
    return "\n".join(lines)


def write_pressure_senco9_write_review_report(
    *,
    fit_dir: str | Path,
    output_dir: str | Path,
    selected_analyzer_device_id: str = "",
    selected_analyzer_prefix: str = "",
    old_getco_snapshot_path: str | Path | None = None,
    reviewer: str = "",
    approver: str = "",
    allow_linear_senco9_exception: bool = False,
) -> Dict[str, Path]:
    root = Path(fit_dir).resolve()
    summary_path = root / "pressure_fit_summary.csv"
    point_means_path = root / "pressure_fit_point_means.csv"
    if not summary_path.exists():
        raise FileNotFoundError(f"Fit summary not found: {summary_path}")
    fit_summary_rows = load_csv_rows(summary_path)
    point_mean_rows = load_csv_rows(point_means_path) if point_means_path.exists() else []
    old_getco_snapshot = _load_json(old_getco_snapshot_path)
    tables, context = build_pressure_senco9_write_review_tables(
        fit_summary_rows=fit_summary_rows,
        point_mean_rows=point_mean_rows,
        selected_analyzer_device_id=selected_analyzer_device_id,
        selected_analyzer_prefix=selected_analyzer_prefix,
        old_getco_snapshot=old_getco_snapshot,
        reviewer=reviewer,
        approver=approver,
        allow_linear_senco9_exception=allow_linear_senco9_exception,
    )
    destination = Path(output_dir).resolve()
    metadata = ValidationMetadata(
        tool_name="export_v1_5_pressure_senco9_write_review",
        created_at=datetime.now().isoformat(timespec="seconds"),
        analyzers=[
            f"{row.get('analyzer_prefix')}:{row.get('analyzer_device_id')}"
            for row in tables.get("pressure_senco9_write_candidates", [])
            if row.get("analyzer_prefix")
        ],
        input_paths=[
            str(summary_path),
            str(point_means_path) if point_means_path.exists() else "",
            str(Path(old_getco_snapshot_path).resolve()) if old_getco_snapshot_path else "",
        ],
        output_dir=str(destination),
        config_summary={
            "review_status": context.get("review_status", ""),
            "selected_analyzer_device_id": context.get("selected_analyzer_device_id", ""),
            "write_allowed": False,
            "allow_linear_senco9_exception": bool(allow_linear_senco9_exception),
        },
        notes=[
            "Offline controlled SENCO9 write-review package.",
            "No COM ports are opened and no PACE, valve, water/gas route, SENCO9, or device-ID writes are performed.",
            "A separate locked writer is still required for any approved single-device write.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="pressure_senco9_write_review",
        metadata=metadata,
        tables=tables,
    )
    runbook_path = destination / "pressure_senco9_controlled_write_runbook.md"
    runbook_path.write_text(_runbook_markdown(tables, context), encoding="utf-8")
    outputs["runbook"] = runbook_path
    return outputs
