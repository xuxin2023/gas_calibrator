"""V1.5 calibratable-point policy.

This module is pure/offline. It does not open COM ports, control valves, or
write analyzer coefficients. It turns open-flow evidence into a practical
decision: sample now, keep waiting with the route open, normalize/review, or
reject/rebuild the point.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import fmean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


@dataclass(frozen=True)
class CalibratablePointConfig:
    """Thresholds for point-level calibration usability.

    The limits are evidence-routing thresholds, not a replacement for the final
    uncertainty budget. CO2 does not need an identical dewpoint at every gas
    point; it needs a stable, measured water-vapor state whose influence is
    either small or explicitly normalized.
    """

    co2_direct_fit_h2o_mmol_max: float = 10.0
    co2_state_normalization_h2o_mmol_max: float = 50.0
    co2_h2o_span_mmol_max: float = 0.5
    h2o_span_mmol_max: float = 0.5
    dewpoint_span_c_max: float = 0.20
    co2_ratio_span_max: float = 0.001
    h2o_ratio_span_max: float = 0.001
    max_extra_wait_after_minimum_s: float = 900.0


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _first_value(row: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _series(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[float]:
    values: List[float] = []
    for row in rows:
        value = _safe_float(_first_value(row, keys))
        if value is not None:
            values.append(value)
    return values


def _span(values: Sequence[float]) -> Optional[float]:
    return float(max(values) - min(values)) if values else None


def _mean(values: Sequence[float]) -> Optional[float]:
    return float(fmean(values)) if values else None


def _contains_reason(reasons: Sequence[Any], token: str) -> bool:
    needle = str(token)
    return any(needle in str(reason or "") for reason in reasons)


def _dedupe(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _component_ratio_series(
    rows: Sequence[Mapping[str, Any]],
    *,
    component: str,
    analyzer_prefix: str,
) -> List[float]:
    key = "h2o_ratio_f" if component == "h2o" else "co2_ratio_f"
    return _series(rows, (f"{analyzer_prefix}_{key}", key))


def evaluate_calibratable_point(
    rows: Sequence[Mapping[str, Any]],
    *,
    component: str,
    analyzer_prefix: str,
    sample_readiness: Mapping[str, Any],
    qc_summary: Mapping[str, Any],
    cfg: CalibratablePointConfig = CalibratablePointConfig(),
) -> Dict[str, Any]:
    """Classify whether the current point can be used for calibration fitting."""

    component_key = str(component or "").strip().lower()
    if component_key not in {"co2", "h2o"}:
        raise ValueError("component must be 'co2' or 'h2o'")

    blockers = list(sample_readiness.get("blockers") or [])
    readiness_warnings = list(sample_readiness.get("warnings") or [])
    reasons: List[str] = []
    warnings: List[str] = []

    actual_purge_s = _safe_float(sample_readiness.get("actual_purge_s"))
    minimum_purge_s = _safe_float(sample_readiness.get("minimum_purge_s"))
    route_evidence_present = bool(sample_readiness.get("route_evidence_present"))
    readiness_status = str(sample_readiness.get("readiness_status") or "").strip().lower()

    dewpoint_values = _series(rows, ("dewpoint_c", "dewpoint_live_c", "reference_dewpoint_c"))
    h2o_values = _series(
        rows,
        (
            f"{analyzer_prefix}_h2o_mmol",
            f"{analyzer_prefix}_h2o_mmol_mol",
            "h2o_mmol",
            "h2o_mmol_mol",
            "reference_h2o_mmol",
        ),
    )
    ratio_values = _component_ratio_series(
        rows,
        component=component_key,
        analyzer_prefix=analyzer_prefix,
    )
    dewpoint_span = _span(dewpoint_values)
    h2o_mean = _mean(h2o_values)
    h2o_span = _span(h2o_values)
    ratio_span = _span(ratio_values)

    metrics = {
        "dewpoint_span_c": dewpoint_span,
        "h2o_mmol_mean": h2o_mean,
        "h2o_mmol_span": h2o_span,
        "component_ratio_span": ratio_span,
        "actual_purge_s": actual_purge_s,
        "minimum_purge_s": minimum_purge_s,
    }

    if _contains_reason(blockers, "route_not_open") or _contains_reason(
        blockers,
        "plan_traceability",
    ):
        return _decision(
            component=component_key,
            analyzer_prefix=analyzer_prefix,
            sample_readiness=sample_readiness,
            qc_summary=qc_summary,
            grade="C",
            calibratable=False,
            role="reject_rebuild_point",
            action="stop_and_rebuild_point",
            reasons=["route_or_traceability_invalid", *[str(item) for item in blockers]],
            warnings=warnings,
            metrics=metrics,
            route_evidence_present=route_evidence_present,
        )

    if _contains_reason(blockers, "minimum_purge_not_met"):
        return _decision(
            component=component_key,
            analyzer_prefix=analyzer_prefix,
            sample_readiness=sample_readiness,
            qc_summary=qc_summary,
            grade="C",
            calibratable=False,
            role="wait_not_yet_calibratable",
            action="continue_purge_to_minimum_with_route_open",
            reasons=[
                *[str(item) for item in blockers],
                "minimum_purge_is_lower_bound_not_acceptance",
            ],
            warnings=warnings,
            metrics=metrics,
            route_evidence_present=route_evidence_present,
        )

    if readiness_status == "fail":
        extra_wait = None
        if actual_purge_s is not None and minimum_purge_s is not None:
            extra_wait = max(0.0, actual_purge_s - minimum_purge_s)
        action = "continue_physical_stability_wait_with_route_open"
        if extra_wait is not None and extra_wait > float(cfg.max_extra_wait_after_minimum_s):
            action = "stop_and_rebuild_point"
        return _decision(
            component=component_key,
            analyzer_prefix=analyzer_prefix,
            sample_readiness=sample_readiness,
            qc_summary=qc_summary,
            grade="C",
            calibratable=False,
            role="wait_or_rebuild_not_calibratable",
            action=action,
            reasons=[*[str(item) for item in blockers], "physical_state_not_ready"],
            warnings=warnings,
            metrics=metrics,
            route_evidence_present=route_evidence_present,
        )

    dewpoint_unstable = dewpoint_span is not None and dewpoint_span > float(
        cfg.dewpoint_span_c_max
    )
    if component_key == "co2":
        h2o_unstable = h2o_span is not None and h2o_span > float(
            cfg.co2_h2o_span_mmol_max
        )
    else:
        h2o_unstable = h2o_span is not None and h2o_span > float(cfg.h2o_span_mmol_max)
    ratio_limit = cfg.h2o_ratio_span_max if component_key == "h2o" else cfg.co2_ratio_span_max
    ratio_unstable = ratio_span is not None and ratio_span > float(ratio_limit)

    if dewpoint_unstable or h2o_unstable or ratio_unstable:
        if dewpoint_unstable:
            reasons.append("dewpoint_tail_not_stable")
        if h2o_unstable:
            reasons.append("h2o_state_not_stable")
        if ratio_unstable:
            reasons.append("component_ratio_not_stable")
        return _decision(
            component=component_key,
            analyzer_prefix=analyzer_prefix,
            sample_readiness=sample_readiness,
            qc_summary=qc_summary,
            grade="C",
            calibratable=False,
            role="wait_not_yet_calibratable",
            action="continue_stability_wait_with_route_open",
            reasons=reasons,
            warnings=warnings,
            metrics=metrics,
            route_evidence_present=route_evidence_present,
        )

    if component_key == "co2":
        if h2o_mean is None:
            grade = "B"
            role = "state_review_missing_h2o_quantity"
            action = "sample_now_but_require_h2o_state_review"
            warnings.append("h2o_quantity_missing_for_co2_state_normalization")
        elif h2o_mean <= float(cfg.co2_direct_fit_h2o_mmol_max):
            grade = "A"
            role = "direct_fit"
            action = "sample_now_do_not_chase_lower_dewpoint"
        elif h2o_mean <= float(cfg.co2_state_normalization_h2o_mmol_max):
            grade = "B"
            role = "state_normalized_fit_review"
            action = "sample_now_with_h2o_state_normalization"
            warnings.append("co2_h2o_state_requires_normalization_and_uncertainty")
        else:
            grade = "C"
            role = "reject_wet_contamination_or_wrong_route"
            action = "stop_or_switch_to_dry_conditioning_then_rebuild_point"
            reasons.append("co2_sample_too_wet_for_validated_state_normalization")
    else:
        if h2o_mean is None:
            grade = "C"
            role = "reject_missing_h2o_reference"
            action = "stop_and_restore_humidity_reference_evidence"
            reasons.append("h2o_reference_quantity_missing")
        else:
            grade = "A"
            role = "direct_h2o_fit"
            action = "sample_now_when_dewpoint_and_h2o_ratio_are_stable"

    return _decision(
        component=component_key,
        analyzer_prefix=analyzer_prefix,
        sample_readiness=sample_readiness,
        qc_summary=qc_summary,
        grade=grade,
        calibratable=grade in {"A", "B"},
        role=role,
        action=action,
        reasons=reasons,
        warnings=[*warnings, *[str(item) for item in readiness_warnings]],
        metrics=metrics,
        route_evidence_present=route_evidence_present,
    )


def _decision(
    *,
    component: str,
    analyzer_prefix: str,
    sample_readiness: Mapping[str, Any],
    qc_summary: Mapping[str, Any],
    grade: str,
    calibratable: bool,
    role: str,
    action: str,
    reasons: Sequence[str],
    warnings: Sequence[str],
    metrics: Mapping[str, Any],
    route_evidence_present: bool,
) -> Dict[str, Any]:
    return {
        "component": component,
        "analyzer_prefix": analyzer_prefix,
        "analyzer_device_id": qc_summary.get("analyzer_device_id"),
        "calibratability_grade": grade,
        "calibratable": calibratable,
        "candidate_fit_ready": calibratable,
        "fit_input_role": role,
        "time_optimization_action": action,
        "reasons": _dedupe(list(reasons)),
        "warnings": _dedupe(list(warnings)),
        "readiness_status": sample_readiness.get("readiness_status"),
        "sample_readiness_blockers": list(sample_readiness.get("blockers") or []),
        "sample_readiness_warnings": list(sample_readiness.get("warnings") or []),
        "route_evidence_present": route_evidence_present,
        "metrics": {str(key): value for key, value in metrics.items()},
        "physical_meaning": (
            "A means direct fit evidence; B means stable evidence that may be used "
            "with explicit state normalization/review; C means the point should not "
            "be used for formal fitting from the current state."
        ),
    }
