"""V1 CO2 point suitability / calibration evidence budget helpers.

This layer does not add new live gates. It only consumes the existing
decision-waterfall evidence and summarizes whether a point looks suitable for
calibration use, how strongly it should be weighted, and why.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def build_co2_point_suitability(result: Mapping[str, Any]) -> Dict[str, Any]:
    result_map = dict(result or {})
    lineage = dict(result_map.get("co2_decision_lineage") or {})
    stages = list(lineage.get("stages") or [])
    stage_by_name = {
        _as_text(stage.get("stage_name")): dict(stage or {})
        for stage in stages
        if _as_text(stage.get("stage_name"))
    }

    waterfall_status = _as_text(result_map.get("co2_decision_waterfall_status") or lineage.get("waterfall_status"))
    measured_value_source = _as_text(result_map.get("measured_value_source") or lineage.get("measured_value_source"))
    selected_source = _as_text(result_map.get("co2_source_selected") or lineage.get("selected_source"))
    selected_segment = _as_text(
        result_map.get("co2_source_segment_selected")
        or result_map.get("co2_source_segment_id")
        or lineage.get("selected_segment")
    )

    phase_stage = stage_by_name.get("phase", {})
    segment_stage = stage_by_name.get("source_segment", {})
    temporal_stage = stage_by_name.get("temporal", {})
    quarantine_stage = stage_by_name.get("quarantine", {})
    source_stage = stage_by_name.get("source_trust", {})
    steady_stage = stage_by_name.get("steady_state", {})

    phase_excluded_ratio = _as_float(result_map.get("co2_phase_excluded_ratio"))
    segment_excluded_ratio = _as_float(result_map.get("co2_source_segment_settle_excluded_ratio"))
    bad_frame_ratio = _as_float(result_map.get("co2_bad_frame_ratio"))
    soft_warn_ratio = _as_float(result_map.get("co2_soft_warn_ratio"))
    cadence_coverage_ratio = _as_float(result_map.get("co2_cadence_coverage_ratio"))

    temporal_status = _as_text(result_map.get("co2_temporal_contract_status"))
    source_switch_reason = _as_text(result_map.get("co2_source_switch_reason"))
    temporal_reason = _as_text(result_map.get("co2_temporal_contract_reason"))
    steady_reason = _as_text(result_map.get("co2_steady_window_reason"))
    trust_reason = _as_text(result_map.get("co2_source_trust_reason"))

    hard_blocked = any(
        [
            measured_value_source == "co2_no_trusted_source",
            _as_text(source_stage.get("status")) == "fail",
            _as_text(temporal_stage.get("status")) == "fail",
            waterfall_status == "fail" and measured_value_source == "co2_no_trusted_source",
        ]
    )

    steady_budget = 30.0
    if measured_value_source == "co2_no_trusted_source":
        steady_points = 0.0
    elif measured_value_source == "co2_steady_state_window" and _as_text(steady_stage.get("status")) == "pass":
        steady_points = 30.0
    elif measured_value_source == "co2_trailing_window_fallback":
        steady_points = 18.0
    elif _as_text(steady_stage.get("status")) in {"warn", "degraded"}:
        steady_points = 20.0
    else:
        steady_points = 12.0

    source_budget = 25.0
    if not selected_source or _as_text(source_stage.get("status")) == "fail":
        source_points = 0.0
    elif source_switch_reason or _as_text(source_stage.get("status")) in {"warn", "degraded"}:
        source_points = 15.0
    else:
        source_points = 25.0

    temporal_budget = 20.0
    if temporal_status in {"fail"} or _as_text(temporal_stage.get("status")) == "fail":
        temporal_points = 0.0
    elif temporal_status in {"warn", "fallback_row_count", "degraded"}:
        temporal_points = 14.0
    else:
        temporal_points = 20.0

    cleanliness_budget = 15.0
    if _as_int(result_map.get("co2_bad_frame_count")) == 0 and _as_int(result_map.get("co2_soft_warn_count")) == 0:
        cleanliness_points = 15.0
    elif bad_frame_ratio <= 0.10 and soft_warn_ratio <= 0.20:
        cleanliness_points = 10.0
    elif bad_frame_ratio < 0.25:
        cleanliness_points = 5.0
    else:
        cleanliness_points = 0.0

    stability_budget = 10.0
    combined_excluded = max(0.0, phase_excluded_ratio) + max(0.0, segment_excluded_ratio)
    if combined_excluded <= 0.05:
        stability_points = 10.0
    elif combined_excluded <= 0.20:
        stability_points = 7.0
    elif combined_excluded <= 0.40:
        stability_points = 4.0
    else:
        stability_points = 0.0
    if cadence_coverage_ratio and cadence_coverage_ratio < 0.75:
        stability_points = min(stability_points, 4.0)

    evidence_score = steady_points + source_points + temporal_points + cleanliness_points + stability_points
    evidence_score = round(_clamp(evidence_score, 0.0, 100.0), 1)

    if hard_blocked:
        suitability_status = "unfit"
        calibration_candidate_recommended = False
        calibration_candidate_hard_blocked = True
        calibration_weight_recommended = 0.0
    elif (
        evidence_score >= 85.0
        and measured_value_source == "co2_steady_state_window"
        and not source_switch_reason
        and _as_text(source_stage.get("status")) == "pass"
        and temporal_status not in {"warn", "fallback_row_count", "degraded", "fail"}
    ):
        suitability_status = "fit"
        calibration_candidate_recommended = True
        calibration_candidate_hard_blocked = False
        calibration_weight_recommended = 1.0 if evidence_score >= 95.0 else round(evidence_score / 100.0, 2)
    else:
        suitability_status = "advisory"
        calibration_candidate_recommended = True
        calibration_candidate_hard_blocked = False
        calibration_weight_recommended = round(_clamp(evidence_score / 100.0, 0.35, 0.95), 2)

    reason_parts = []
    if measured_value_source == "co2_no_trusted_source":
        reason_parts.append("no_trusted_source")
    elif measured_value_source == "co2_trailing_window_fallback":
        reason_parts.append("steady_state_fallback")
    else:
        reason_parts.append("steady_state_window")
    if source_switch_reason:
        reason_parts.append("source_fallback")
    if temporal_status in {"warn", "fallback_row_count", "degraded"}:
        reason_parts.append("temporal_limited")
    elif temporal_status == "fail":
        reason_parts.append("temporal_failed")
    if _as_int(result_map.get("co2_bad_frame_count")) > 0:
        reason_parts.append("bad_frame_present")
    elif _as_int(result_map.get("co2_soft_warn_count")) > 0:
        reason_parts.append("soft_warn_present")
    if combined_excluded > 0.0:
        reason_parts.append("phase_or_segment_exclusion")
    if hard_blocked:
        reason_parts.append("hard_blocked")
    elif suitability_status == "fit":
        reason_parts.append("fit_for_calibration")
    else:
        reason_parts.append("use_with_advisory_weight")

    evidence_budget_summary = (
        f"steady={steady_points:.0f}/{steady_budget:.0f};"
        f"source={source_points:.0f}/{source_budget:.0f};"
        f"temporal={temporal_points:.0f}/{temporal_budget:.0f};"
        f"cleanliness={cleanliness_points:.0f}/{cleanliness_budget:.0f};"
        f"stability={stability_points:.0f}/{stability_budget:.0f};"
        f"total={evidence_score:.1f}/100"
    )
    reason_chain_parts = [
        f"waterfall={waterfall_status or 'unknown'}",
        f"source={selected_source or 'none'}",
        f"segment={selected_segment or 'none'}",
        f"measured={measured_value_source or 'none'}",
    ]
    for text in (source_switch_reason, trust_reason, temporal_reason, steady_reason):
        text = _as_text(text)
        if text:
            reason_chain_parts.append(text)

    return {
        "co2_point_suitability_status": suitability_status,
        "co2_calibration_candidate_recommended": calibration_candidate_recommended,
        "co2_calibration_candidate_hard_blocked": calibration_candidate_hard_blocked,
        "co2_calibration_weight_recommended": calibration_weight_recommended,
        "co2_evidence_score": evidence_score,
        "co2_point_evidence_budget_reason": ";".join(reason_parts),
        "co2_point_evidence_budget_summary": evidence_budget_summary,
        "co2_point_suitability_reason_chain": ";".join(reason_chain_parts),
    }
