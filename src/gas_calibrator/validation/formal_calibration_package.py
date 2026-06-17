"""Formal V1.5 calibration evidence package builder.

The package combines already-recorded artifacts into reviewer-facing evidence.
It does not run devices, switch routes, control PACE, or write coefficients.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .formal_open_flow_artifacts import (
    build_formal_open_flow_tables,
    load_plan_snapshot,
    load_pressure_check_rows,
    load_pressure_reference_snapshot,
)
from .artifact_rows import load_latest_sample_rows
from .common import load_csv_rows
from .pressure_channel import (
    build_pressure_channel_tables,
)
from .reporting import ValidationMetadata, write_validation_report


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _table_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return _compact_json(value)
    return value


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes", "pass"}


def _split_reasons(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [item for item in str(value).split(";") if item]


def _is_quick_check_artifact_source(value: Any) -> bool:
    return str(value or "").strip() in {
        "pressure_quick_check_artifact",
        "external_pressure_quick_check_artifact",
        "pressure_channel_completion_artifact",
        "external_pressure_completion_artifact",
    }


def _is_pressure_completion_source(value: Any) -> bool:
    return str(value or "").strip() in {
        "pressure_channel_completion_artifact",
        "external_pressure_completion_artifact",
    }


def _load_pressure_completion_tables(pressure_path: Optional[Path]) -> Dict[str, List[Dict[str, Any]]]:
    if pressure_path is None:
        return {
            "pressure_validation_summary": [],
            "pressure_reference_traceability": [],
            "measurement_model": [],
            "paired_samples": [],
            "rejected_samples": [],
        }
    root = pressure_path.parent
    device_path = root / "pressure_channel_device_readiness.csv"
    trace_path = root / "pressure_channel_traceability.csv"
    policy_path = root / "pressure_channel_acceptance_policy.csv"
    device_rows = load_csv_rows(device_path) if device_path.exists() else []
    trace_rows = load_csv_rows(trace_path) if trace_path.exists() else []
    policy_rows = load_csv_rows(policy_path) if policy_path.exists() else []
    validation_summary: List[Dict[str, Any]] = []
    measurement_model: List[Dict[str, Any]] = []
    for row in device_rows:
        allows = _bool_value(row.get("can_enter_open_flow_main_calibration"))
        readiness_status = str(row.get("readiness_status") or "").strip().lower()
        status = "pass" if allows and readiness_status == "pass" else "fail"
        reasons = str(row.get("readiness_reasons") or "").strip()
        validation_summary.append(
            {
                "validation_mode": "pressure_channel_completion",
                "status": status,
                "validation_level": "formal_pressure_completion" if status == "pass" else "blocked_pressure_completion",
                "reason": reasons,
                "sample_count": row.get("valid_pair_count", ""),
                "valid_pair_count": row.get("valid_pair_count", ""),
                "rejected_pair_count": "",
                "analyzer_pressure_mean_hpa": "",
                "com22_pressure_mean_hpa": "",
                "pace_pressure_mean_hpa": "",
                "analyzer_minus_com22_mean_hpa": "",
                "analyzer_minus_com22_max_abs_hpa": row.get("post_write_residual_max_abs_hpa", ""),
                "pace_minus_com22_mean_hpa": "",
                "allowed_for_co2_h2o_formal_work": status == "pass",
                "traceability": _compact_json(trace_rows[0] if trace_rows else {}),
                "measurement_model": _compact_json(
                    {
                        "source": "pressure_channel_completion",
                        "post_write_offset_kpa": row.get("post_write_offset_kpa", ""),
                        "post_write_residual_max_abs_hpa": row.get("post_write_residual_max_abs_hpa", ""),
                        "senco9_write_status": row.get("senco9_write_status", ""),
                        "pressure_reference_certificate_id": row.get("pressure_reference_certificate_id", ""),
                        "pressure_reference_certificate_hash": row.get("pressure_reference_certificate_hash", ""),
                    }
                ),
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": row.get("analyzer_device_id", ""),
            }
        )
        measurement_model.append(
            {
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": row.get("analyzer_device_id", ""),
                "model": "pressure_channel_completion_precondition",
                "pressure_input_quantity": "analyzer_internal_pressure_P",
                "post_write_fit_status": row.get("post_write_fit_status", ""),
                "post_write_residual_max_abs_hpa": row.get("post_write_residual_max_abs_hpa", ""),
                "senco9_write_status": row.get("senco9_write_status", ""),
                "policy": _compact_json(policy_rows[0] if policy_rows else {}),
                "not_co2_h2o_fit_evidence": True,
            }
        )
    return {
        "pressure_validation_summary": validation_summary,
        "pressure_reference_traceability": trace_rows,
        "measurement_model": measurement_model,
        "paired_samples": [],
        "rejected_samples": [],
    }


def _candidate_review_rows(
    *,
    open_flow_summary: List[Mapping[str, Any]],
    pressure_summary: Mapping[str, Any],
    pressure_summary_by_prefix: Optional[Mapping[str, Mapping[str, Any]]] = None,
    pressure_summary_by_device_id: Optional[Mapping[str, Mapping[str, Any]]] = None,
    pressure_check_source: str,
    require_quick_check_artifact: bool,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in open_flow_summary:
        prefix = str(item.get("analyzer_prefix") or "")
        open_flow_device_id = str(item.get("analyzer_device_id") or "").strip()
        item_pressure_summary = dict((pressure_summary_by_device_id or {}).get(open_flow_device_id) or {})
        pressure_binding = "device_id"
        if not item_pressure_summary:
            item_pressure_summary = dict((pressure_summary_by_prefix or {}).get(prefix) or pressure_summary)
            pressure_binding = "prefix"
        pressure_allows = _bool_value(item_pressure_summary.get("allowed_for_co2_h2o_formal_work"))
        blockers = _split_reasons(item.get("candidate_fit_blockers"))
        pressure_device_id = str(item_pressure_summary.get("analyzer_device_id") or "").strip()
        if not open_flow_device_id:
            blockers.append("open_flow_analyzer_identity_missing")
        elif not pressure_device_id:
            blockers.append("pressure_channel_analyzer_identity_missing")
        elif open_flow_device_id != pressure_device_id:
            blockers.append("pressure_channel_identity_mismatch")
        if not _bool_value(item.get("candidate_fit_allowed")):
            blockers.append("open_flow_candidate_not_allowed")
        if not pressure_allows:
            blockers.append("pressure_channel_validation_not_formal_pass")
        if require_quick_check_artifact and not _is_quick_check_artifact_source(pressure_check_source):
            blockers.append("pressure_quick_check_artifact_missing")
        deduped: List[str] = []
        for blocker in blockers:
            if blocker and blocker not in deduped:
                deduped.append(blocker)
        rows.append(
            {
                "component": item.get("component", ""),
                "analyzer_prefix": item.get("analyzer_prefix", ""),
                "analyzer_device_id": item.get("analyzer_device_id", ""),
                "candidate_review_status": "ready_for_reviewer" if not deduped else "blocked",
                "candidate_fit_may_be_reviewed": not deduped,
                "candidate_fit_auto_write_allowed": False,
                "blockers": ";".join(deduped),
                "a_grade_count": item.get("a_grade_count", 0),
                "b_grade_count": item.get("b_grade_count", 0),
                "rejected_count": item.get("rejected_count", 0),
                "pressure_condition_warning_count": item.get("pressure_condition_warning_count", 0),
                "window_report_warnings": item.get("window_report_warnings", ""),
                "pressure_check_source": pressure_check_source,
                "pressure_validation_status": item_pressure_summary.get("status", ""),
                "pressure_validation_level": item_pressure_summary.get("validation_level", ""),
                "pressure_validation_reason": item_pressure_summary.get("reason", ""),
                "pressure_binding": pressure_binding,
                "pressure_analyzer_device_id": pressure_device_id,
            }
        )
    return rows


def build_formal_calibration_package_tables(
    *,
    run_dir: str | Path,
    plan: Mapping[str, Any],
    pressure_reference: Mapping[str, Any],
    component: str = "both",
    analyzer_prefix: str = "ga01",
    require_quick_check_artifact: bool = True,
    pressure_check_path: str | Path | None = None,
    today: Any = None,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    root = Path(run_dir).resolve()
    _, sample_rows = load_latest_sample_rows(root)
    pressure_source, pressure_rows, pressure_path = load_pressure_check_rows(
        root,
        fallback_rows=sample_rows,
        pressure_check_path=pressure_check_path,
    )
    open_tables, open_context = build_formal_open_flow_tables(
        run_dir=root,
        plan=plan,
        pressure_reference=pressure_reference,
        component=component,
        analyzer_prefix=analyzer_prefix,
        pressure_check_path=pressure_check_path,
        today=today,
    )
    analyzer_prefixes = list(open_context.get("analyzer_prefixes") or [analyzer_prefix])
    pressure_tables: Dict[str, List[Dict[str, Any]]] = {
        "pressure_validation_summary": [],
        "pressure_reference_traceability": [],
        "measurement_model": [],
        "paired_samples": [],
        "rejected_samples": [],
    }
    pressure_summary_by_prefix: Dict[str, Mapping[str, Any]] = {}
    pressure_summary_by_device_id: Dict[str, Mapping[str, Any]] = {}
    if _is_pressure_completion_source(pressure_source):
        pressure_tables = _load_pressure_completion_tables(pressure_path)
        for summary in pressure_tables.get("pressure_validation_summary") or []:
            prefix = str(summary.get("analyzer_prefix") or "")
            if prefix:
                pressure_summary_by_prefix[prefix] = summary
            pressure_device_id = str(summary.get("analyzer_device_id") or "").strip()
            if pressure_device_id:
                pressure_summary_by_device_id[pressure_device_id] = summary
    else:
        for prefix in analyzer_prefixes:
            per_pressure_tables = build_pressure_channel_tables(
                pressure_rows,
                pressure_reference=pressure_reference,
                analyzer_prefix=prefix,
                today=today,
            )
            for key in pressure_tables:
                pressure_tables[key].extend(per_pressure_tables.get(key, []))
            summary_rows = per_pressure_tables.get("pressure_validation_summary") or [{}]
            pressure_summary_by_prefix[str(prefix)] = summary_rows[0]
            pressure_device_id = str(summary_rows[0].get("analyzer_device_id") or "").strip()
            if pressure_device_id:
                pressure_summary_by_device_id[pressure_device_id] = summary_rows[0]
    pressure_summary = (
        pressure_tables["pressure_validation_summary"][0]
        if pressure_tables["pressure_validation_summary"]
        else {}
    )
    review_rows = _candidate_review_rows(
        open_flow_summary=open_tables["run_summary"],
        pressure_summary=pressure_summary,
        pressure_summary_by_prefix=pressure_summary_by_prefix,
        pressure_summary_by_device_id=pressure_summary_by_device_id,
        pressure_check_source=pressure_source,
        require_quick_check_artifact=require_quick_check_artifact,
    )
    package_blockers: List[str] = []
    if any(row["candidate_review_status"] != "ready_for_reviewer" for row in review_rows):
        package_blockers.append("candidate_review_blocked")
    if require_quick_check_artifact and not _is_quick_check_artifact_source(pressure_source):
        package_blockers.append("pressure_quick_check_artifact_missing")

    package_summary = [
        {
            "package_status": "ready_for_reviewer" if not package_blockers else "blocked",
            "package_blockers": ";".join(package_blockers),
            "run_dir": str(root),
            "component": component,
            "analyzer_prefix": analyzer_prefix,
            "analyzer_prefixes": ";".join(str(item) for item in analyzer_prefixes),
            "pressure_check_source": pressure_source,
            "pressure_check_path": str(pressure_path) if pressure_path else "",
            "formal_execution_order": (
                "PRECHECK;PRESSURE_CHANNEL_QUICK_CHECK;OPEN_FLOW_PURGE;"
                "STABILITY_GATE;SAMPLE_WINDOW;QC_AND_REPORT;CANDIDATE_REVIEW"
            ),
            "auto_write_allowed": False,
        }
    ]
    artifact_manifest = [
        {"artifact_role": "samples", "path": open_context.get("samples_path", ""), "required": True},
        {
            "artifact_role": "pressure_channel_quick_check",
            "path": str(pressure_path) if pressure_path else "",
            "required": bool(require_quick_check_artifact),
        },
        {"artifact_role": "formal_plan_snapshot", "path": "", "required": True},
        {"artifact_role": "pressure_reference_snapshot", "path": "", "required": True},
    ]
    tables: Dict[str, List[Dict[str, Any]]] = {
        "package_summary": package_summary,
        "candidate_coefficient_review": review_rows,
        "artifact_manifest": artifact_manifest,
        "open_flow_run_summary": open_tables["run_summary"],
        "pressure_validation_summary": pressure_tables["pressure_validation_summary"],
        "pressure_reference_traceability": pressure_tables["pressure_reference_traceability"],
        "measurement_model": pressure_tables["measurement_model"],
        "a_grade_samples": open_tables["a_grade_samples"],
        "b_grade_review_samples": open_tables["b_grade_review_samples"],
        "rejected_samples": open_tables["rejected_samples"],
        "pressure_paired_samples": pressure_tables["paired_samples"],
        "pressure_rejected_samples": pressure_tables["rejected_samples"],
    }
    context = {
        "pressure_check_source": pressure_source,
        "pressure_check_path": str(pressure_path) if pressure_path else "",
        "package_status": package_summary[0]["package_status"],
        "package_blockers": list(package_blockers),
        "analyzer_prefixes": analyzer_prefixes,
    }
    return tables, context


def write_formal_calibration_package(
    *,
    run_dir: str | Path,
    output_dir: str | Path | None = None,
    plan: Optional[Mapping[str, Any]] = None,
    plan_path: str | Path | None = None,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    pressure_reference_path: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    require_quick_check_artifact: bool = True,
    pressure_check_path: str | Path | None = None,
    today: Any = None,
) -> Dict[str, Path]:
    root = Path(run_dir).resolve()
    plan_data = dict(plan) if plan is not None else load_plan_snapshot(plan_path)
    reference_data = (
        dict(pressure_reference)
        if pressure_reference is not None
        else load_pressure_reference_snapshot(pressure_reference_path)
    )
    tables, context = build_formal_calibration_package_tables(
        run_dir=root,
        plan=plan_data,
        pressure_reference=reference_data,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=require_quick_check_artifact,
        pressure_check_path=pressure_check_path,
        today=today,
    )
    destination = Path(output_dir).resolve() if output_dir else root / "formal_calibration_package"
    metadata = ValidationMetadata(
        tool_name="export_v1_5_formal_calibration_package",
        created_at=datetime.now().isoformat(timespec="seconds"),
        analyzers=list(context.get("analyzer_prefixes") or [analyzer_prefix]),
        input_paths=[
            str(Path(plan_path).resolve()) if plan_path else "",
            str(Path(pressure_reference_path).resolve()) if pressure_reference_path else "",
            context.get("pressure_check_path", ""),
        ],
        output_dir=str(destination),
        config_summary={
            "component": component,
            "analyzer_prefix": analyzer_prefix,
            "analyzer_prefixes": context.get("analyzer_prefixes", []),
            "require_quick_check_artifact": require_quick_check_artifact,
            "pressure_check_source": context.get("pressure_check_source", ""),
            "package_status": context.get("package_status", ""),
            "package_blockers": context.get("package_blockers", []),
        },
        notes=[
            "Sidecar-only formal V1.5 calibration evidence package.",
            "No COM ports are opened and no route, valve, PACE, SENCO9, or coefficient writes are performed.",
            "Candidate coefficient review is separated from automatic device writes.",
        ],
    )
    return write_validation_report(
        destination,
        prefix="formal_calibration_package",
        metadata=metadata,
        tables=tables,
    )
