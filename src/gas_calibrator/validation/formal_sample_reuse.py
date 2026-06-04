"""V1.5 per-analyzer sample reuse review.

This module is sidecar-only. It consumes existing formal open-flow evidence and
candidate-policy tables, then classifies each analyzer device ID as reusable for
candidate fitting, needing verification/additional evidence, or rejected. It
does not open COM ports, switch routes, control PACE/valves, or write analyzer
coefficients.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .formal_candidate_coefficients import (
    CandidateCoefficientPolicyConfig,
    build_candidate_coefficient_tables,
)
from .formal_open_flow_artifacts import load_plan_snapshot, load_pressure_reference_snapshot
from .reporting import ValidationMetadata, write_validation_report


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes", "pass"}


def _int_value(value: Any) -> int:
    try:
        return int(float(str(value or "0")))
    except Exception:
        return 0


def _split(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    return [item for item in str(value).split(";") if item]


def _dedupe(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _reuse_decision(row: Mapping[str, Any]) -> tuple[str, str, List[str], str]:
    status = str(row.get("candidate_status") or "").strip()
    reuse_class = str(row.get("evidence_reuse_class") or "").strip()
    blockers = _dedupe([*_split(row.get("blocked_reasons")), *_split(row.get("formal_review_blockers"))])
    warnings = _dedupe(_split(row.get("warning_reasons")))
    fit_count = _int_value(row.get("fit_sample_count"))
    a_grade_count = _int_value(row.get("formal_a_grade_count"))
    preparation_rejected_count = _int_value(row.get("preparation_rejected_count"))
    verification_count = _int_value(row.get("verification_sample_count"))
    distinct_targets = _int_value(row.get("distinct_fit_targets"))

    if status == "verification_passed" and not blockers:
        return (
            "can_fit",
            "可拟合",
            _dedupe(["a_grade_open_flow_verified", *warnings]),
            "enter_candidate_coefficient_review_no_write",
        )

    if status in {"fit_ready_requires_verification", "verification_failed"} or _bool_value(row.get("allowed_to_fit")):
        reasons = ["candidate_fit_has_enough_a_grade_samples"]
        if status != "verification_passed":
            reasons.append(status or "verification_not_passed")
        if verification_count <= 0:
            reasons.append("independent_verification_missing")
        reasons.extend(blockers)
        reasons.extend(warnings)
        return (
            "needs_verification",
            "需复验",
            _dedupe(reasons),
            "run_independent_verification_point_before_write_review",
        )

    if reuse_class in {
        "a_grade_single_target_review_only",
        "a_grade_fit_samples_need_review",
        "fit_ready_requires_independent_verification",
    } or (a_grade_count > 0 and fit_count > 0 and distinct_targets < 2):
        reasons = [
            reuse_class or "a_grade_evidence_not_complete_for_curve",
            f"distinct_fit_targets={distinct_targets}",
        ]
        reasons.extend(blockers)
        reasons.extend(warnings)
        return (
            "needs_verification",
            "需复验",
            _dedupe(reasons),
            "reuse_as_evidence_then_collect_missing_points_or_verification",
        )

    if a_grade_count > 0 and fit_count == 0 and preparation_rejected_count > 0:
        reasons = [
            "a_grade_samples_missing_fit_target_or_preparation_fields",
            f"preparation_rejected_count={preparation_rejected_count}",
        ]
        reasons.extend(blockers)
        reasons.extend(warnings)
        return (
            "needs_verification",
            "需复验",
            _dedupe(reasons),
            "bind_reference_target_and_rerun_reuse_review",
        )

    reasons = [reuse_class or status or "not_reusable_for_formal_fit"]
    reasons.extend(blockers)
    reasons.extend(warnings)
    return (
        "reject",
        "拒绝",
        _dedupe(reasons),
        "do_not_fit_this_device_component;keep_as_diagnostic_evidence",
    )


def build_sample_reuse_review_tables(
    *,
    run_dir: str | Path,
    plan: Mapping[str, Any],
    pressure_reference: Mapping[str, Any],
    component: str = "both",
    analyzer_prefix: str = "all",
    require_quick_check_artifact: bool = True,
    pressure_check_path: str | Path | None = None,
    cfg: Optional[CandidateCoefficientPolicyConfig] = None,
    today: Any = None,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Build per-device reuse review tables from existing evidence."""

    candidate_tables, candidate_context = build_candidate_coefficient_tables(
        run_dir=run_dir,
        plan=plan,
        pressure_reference=pressure_reference,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=require_quick_check_artifact,
        pressure_check_path=pressure_check_path,
        cfg=cfg,
        today=today,
    )
    policy_rows = list(candidate_tables.get("candidate_policy_summary") or [])
    reuse_rows: List[Dict[str, Any]] = []
    for row in policy_rows:
        decision, decision_cn, reasons, next_action = _reuse_decision(row)
        reuse_rows.append(
            {
                "component": row.get("component", ""),
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": row.get("analyzer_device_id", ""),
                "reuse_decision": decision,
                "reuse_decision_cn": decision_cn,
                "reuse_reasons": ";".join(reasons),
                "recommended_next_action": next_action,
                "fit_sample_count": row.get("fit_sample_count", 0),
                "verification_sample_count": row.get("verification_sample_count", 0),
                "distinct_fit_targets": row.get("distinct_fit_targets", 0),
                "formal_a_grade_count": row.get("formal_a_grade_count", 0),
                "formal_rejected_count": row.get("formal_rejected_count", 0),
                "preparation_rejected_count": row.get("preparation_rejected_count", 0),
                "formal_pressure_condition_warning_count": row.get(
                    "formal_pressure_condition_warning_count", 0
                ),
                "candidate_status": row.get("candidate_status", ""),
                "evidence_reuse_class": row.get("evidence_reuse_class", ""),
                "candidate_blockers": row.get("blocked_reasons", ""),
                "formal_review_blockers": row.get("formal_review_blockers", ""),
                "warning_reasons": row.get("warning_reasons", ""),
                "selected_model_terms": row.get("selected_model_terms", ""),
                "frozen_terms": row.get("frozen_terms", ""),
                "scope_boundary": (
                    "this_analyzer_device_id_only;do_not_block_other_analyzers;"
                    "pressure_not_co2_h2o_polynomial_fit_variable"
                ),
                "auto_write_allowed": False,
            }
        )

    counts: Dict[str, int] = {}
    for row in reuse_rows:
        key = str(row.get("reuse_decision") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    decision_counts = [
        {
            "reuse_decision": key,
            "reuse_decision_cn": {
                "can_fit": "可拟合",
                "needs_verification": "需复验",
                "reject": "拒绝",
            }.get(key, key),
            "count": value,
        }
        for key, value in sorted(counts.items())
    ]

    run_summary = [
        {
            "run_dir": str(Path(run_dir).resolve()),
            "component": component,
            "analyzer_prefix": analyzer_prefix,
            "reuse_row_count": len(reuse_rows),
            "can_fit_count": counts.get("can_fit", 0),
            "needs_verification_count": counts.get("needs_verification", 0),
            "reject_count": counts.get("reject", 0),
            "candidate_run_status": candidate_context.get("candidate_run_status", ""),
            "package_status": candidate_context.get("package_status", ""),
            "pressure_check_source": candidate_context.get("pressure_check_source", ""),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "physical_meaning": (
                "Reuse is scoped by analyzer_device_id and component. Stable CO2/H2O "
                "component evidence can be reused even when another analyzer failed; "
                "wet-route pressure warnings remain report evidence, not polynomial fit terms."
            ),
        }
    ]
    tables: Dict[str, List[Dict[str, Any]]] = {
        "sample_reuse_run_summary": run_summary,
        "sample_reuse_by_device": reuse_rows,
        "sample_reuse_decision_counts": decision_counts,
        "candidate_policy_summary": policy_rows,
    }
    context = {
        "reuse_row_count": len(reuse_rows),
        "decision_counts": counts,
        "candidate_run_status": candidate_context.get("candidate_run_status", ""),
        "package_status": candidate_context.get("package_status", ""),
        "pressure_check_source": candidate_context.get("pressure_check_source", ""),
        "analyzer_prefixes": candidate_context.get("analyzer_prefixes", []),
    }
    return tables, context


def _write_markdown_report(destination: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    summary = (tables.get("sample_reuse_run_summary") or [{}])[0]
    rows = list(tables.get("sample_reuse_by_device") or [])
    report_path = destination / "sample_reuse_review_report.md"
    lines = [
        "# V1.5 Sample Reuse Review",
        "",
        f"- Run dir: `{summary.get('run_dir', '')}`",
        f"- Component: `{summary.get('component', '')}`",
        f"- Analyzer scope: `{summary.get('analyzer_prefix', '')}`",
        f"- Can fit: {summary.get('can_fit_count', 0)}",
        f"- Needs verification: {summary.get('needs_verification_count', 0)}",
        f"- Reject: {summary.get('reject_count', 0)}",
        "- Boundary: offline/no-write review only; no COM ports, no route or pressure control, no coefficient writes.",
        "",
        "| Component | Prefix | Device ID | Decision | Fit Samples | Verification | A Grade | Pressure Warnings | Next Action | Reasons |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {component} | {prefix} | {device} | {decision} | {fit} | {verify} | {a_grade} | {pressure_warn} | {action} | {reasons} |".format(
                component=row.get("component", ""),
                prefix=row.get("analyzer_prefix", ""),
                device=row.get("analyzer_device_id", ""),
                decision=row.get("reuse_decision_cn", ""),
                fit=row.get("fit_sample_count", ""),
                verify=row.get("verification_sample_count", ""),
                a_grade=row.get("formal_a_grade_count", ""),
                pressure_warn=row.get("formal_pressure_condition_warning_count", ""),
                action=row.get("recommended_next_action", ""),
                reasons=row.get("reuse_reasons", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- `可拟合` means the existing A-grade open-flow evidence has passed independent verification for this device/component and may enter no-write candidate coefficient review.",
            "- `需复验` means the evidence is useful, but still needs an independent verification point, more target points, or missing traceability evidence before write review.",
            "- `拒绝` means this device/component must not enter formal fitting from the current evidence; keep the rows as diagnostic traceability.",
            "- Decisions are scoped to `analyzer_device_id`, not the serial channel label.",
            "- Pressure warnings do not create CO2/H2O polynomial pressure terms in V1.5.",
            "",
        ]
    )
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def write_sample_reuse_review(
    *,
    run_dir: str | Path,
    output_dir: str | Path,
    plan: Optional[Mapping[str, Any]] = None,
    plan_path: str | Path | None = None,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    pressure_reference_path: str | Path | None = None,
    pressure_check_path: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "all",
    require_quick_check_artifact: bool = True,
    cfg: Optional[CandidateCoefficientPolicyConfig] = None,
    today: Any = None,
) -> Dict[str, Path]:
    """Write per-device reuse review CSV/XLSX/Markdown artifacts."""

    plan_data = dict(plan) if plan is not None else load_plan_snapshot(plan_path)
    reference_data = (
        dict(pressure_reference)
        if pressure_reference is not None
        else load_pressure_reference_snapshot(pressure_reference_path)
    )
    tables, context = build_sample_reuse_review_tables(
        run_dir=run_dir,
        plan=plan_data,
        pressure_reference=reference_data,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=require_quick_check_artifact,
        pressure_check_path=pressure_check_path,
        cfg=cfg,
        today=today,
    )
    destination = Path(output_dir).resolve()
    metadata = ValidationMetadata(
        tool_name="export_v1_5_sample_reuse_review",
        created_at=_now(),
        analyzers=list(context.get("analyzer_prefixes") or [analyzer_prefix]),
        input_paths=[
            str(Path(run_dir).resolve()),
            str(Path(plan_path).resolve()) if plan_path else "",
            str(Path(pressure_reference_path).resolve()) if pressure_reference_path else "",
            str(Path(pressure_check_path).resolve()) if pressure_check_path else "",
        ],
        output_dir=str(destination),
        config_summary={
            "component": component,
            "analyzer_prefix": analyzer_prefix,
            "reuse_row_count": context.get("reuse_row_count", 0),
            "decision_counts": context.get("decision_counts", {}),
            "candidate_run_status": context.get("candidate_run_status", ""),
            "package_status": context.get("package_status", ""),
            "pressure_check_source": context.get("pressure_check_source", ""),
            "auto_write_allowed": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
        notes=[
            "Offline V1.5 per-device sample reuse review.",
            "A failed analyzer device ID does not invalidate other analyzer device IDs.",
            "Pressure warnings are report evidence and uncertainty inputs, not CO2/H2O polynomial pressure terms.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="sample_reuse_review",
        metadata=metadata,
        tables=tables,
    )
    outputs["markdown"] = _write_markdown_report(destination, tables)
    return outputs
