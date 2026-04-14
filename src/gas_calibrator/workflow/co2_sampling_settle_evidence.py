"""V1 CO2 sampling / settle evidence sidecar helpers.

This layer stays advisory-only. It consumes existing V1 hardened point-level
evidence and summarizes whether sampling/settle evidence looks ready,
fallback-but-usable, manual-review-worthy, or unfit for score-oriented use.
It does not change live measured values or introduce new live gates.
"""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .co2_calibration_candidate_pack import (
    _FIELD_ALIASES,
    _as_bool,
    _as_float,
    _as_text,
    _first_available,
    build_co2_calibration_candidate_point,
    write_csv_rows,
)


_EXTRA_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "co2_phase_excluded_count": ("co2_phase_excluded_count", "气路相位排除样本数"),
    "co2_phase_excluded_ratio": ("co2_phase_excluded_ratio", "气路相位排除样本占比"),
    "co2_phase_reason_summary": ("co2_phase_reason_summary", "气路相位排除摘要"),
    "co2_candidate_rows_before_phase_gate": ("co2_candidate_rows_before_phase_gate", "气路相位门禁前候选样本数"),
    "co2_candidate_rows_after_phase_gate": ("co2_candidate_rows_after_phase_gate", "气路相位门禁后候选样本数"),
    "co2_candidate_rows_before_segment_gate": ("co2_candidate_rows_before_segment_gate", "气路分段门禁前候选样本数"),
    "co2_candidate_rows_after_segment_gate": ("co2_candidate_rows_after_segment_gate", "气路分段门禁后候选样本数"),
    "co2_candidate_rows_after_quarantine": ("co2_candidate_rows_after_quarantine", "气路隔离后候选样本数"),
    "co2_source_segment_selected": ("co2_source_segment_selected", "气路选用来源分段"),
    "co2_source_segment_settle_excluded_count": (
        "co2_source_segment_settle_excluded_count",
        "气路来源分段排除数",
    ),
    "co2_source_segment_settle_excluded_ratio": (
        "co2_source_segment_settle_excluded_ratio",
        "气路来源分段排除占比",
    ),
    "co2_source_segment_reason_summary": ("co2_source_segment_reason_summary", "气路来源分段摘要"),
    "co2_temporal_candidate_rows_before_gate": (
        "co2_temporal_candidate_rows_before_gate",
        "气路时间门禁前候选样本数",
    ),
    "co2_temporal_candidate_rows_after_gate": (
        "co2_temporal_candidate_rows_after_gate",
        "气路时间门禁后候选样本数",
    ),
    "co2_timestamp_monotonic": ("co2_timestamp_monotonic", "气路时间戳单调"),
    "co2_duplicate_timestamp_count": ("co2_duplicate_timestamp_count", "气路重复时间戳数"),
    "co2_backward_timestamp_count": ("co2_backward_timestamp_count", "气路回退时间戳数"),
    "co2_large_gap_count": ("co2_large_gap_count", "气路大间隙数"),
    "co2_effective_dwell_seconds": ("co2_effective_dwell_seconds", "气路有效驻留时间s"),
    "co2_nominal_dt_seconds": ("co2_nominal_dt_seconds", "气路标称采样间隔s"),
    "co2_cadence_coverage_ratio": ("co2_cadence_coverage_ratio", "气路节奏覆盖率"),
    "co2_temporal_fallback_reason": ("co2_temporal_fallback_reason", "气路时间契约降级原因"),
    "co2_steady_window_found": ("co2_steady_window_found", "气路稳态窗命中"),
    "co2_steady_window_sample_count": ("co2_steady_window_sample_count", "气路稳态窗样本数"),
    "co2_steady_window_status": ("co2_steady_window_status", "气路稳态窗结果"),
    "co2_steady_window_reason": ("co2_steady_window_reason", "气路稳态窗原因"),
    "co2_bad_frame_count": ("co2_bad_frame_count", "气路坏帧数"),
    "co2_bad_frame_ratio": ("co2_bad_frame_ratio", "气路坏帧占比"),
    "co2_soft_warn_count": ("co2_soft_warn_count", "气路软警告帧数"),
    "co2_soft_warn_ratio": ("co2_soft_warn_ratio", "气路软警告帧占比"),
    "point_quality_status": ("point_quality_status", "点位质量结果"),
    "point_quality_reason": ("point_quality_reason", "点位质量原因"),
    "co2_decision_waterfall_reason": ("co2_decision_waterfall_reason", "气路决策瀑布原因"),
}

_SAMPLING_FIELD_ALIASES = {**_FIELD_ALIASES, **_EXTRA_FIELD_ALIASES}


def _first_sampling_value(row: Mapping[str, Any], field: str) -> Any:
    for alias in _SAMPLING_FIELD_ALIASES.get(field, (field,)):
        if alias in row and row.get(alias) not in (None, ""):
            return row.get(alias)
    return None


def _format_optional(value: Optional[float], *, digits: int = 3) -> str:
    if value is None:
        return "unknown"
    return f"{value:.{digits}f}"


def _group_key_for_row(row: Mapping[str, Any]) -> str:
    candidate_row = build_co2_calibration_candidate_point(row)
    return _as_text(candidate_row.get("co2_calibration_group_key"))


def _confidence_bucket(score: float) -> str:
    if score >= 85.0:
        return "high"
    if score >= 65.0:
        return "medium"
    if score > 0.0:
        return "low"
    return "none"


def build_co2_sampling_settle_point(row: Mapping[str, Any]) -> Dict[str, Any]:
    candidate = build_co2_calibration_candidate_point(row)

    measured_source = _as_text(candidate.get("measured_value_source"))
    suitability_status = _as_text(candidate.get("co2_point_suitability_status"))
    candidate_recommended = _as_bool(candidate.get("co2_calibration_candidate_recommended"))
    hard_blocked = _as_bool(candidate.get("co2_calibration_candidate_hard_blocked"))
    waterfall_status = _as_text(candidate.get("co2_decision_waterfall_status"))
    source_selected = _as_text(candidate.get("co2_source_selected"))
    source_switch_reason = _as_text(candidate.get("co2_source_switch_reason"))
    temporal_status = _as_text(candidate.get("co2_temporal_contract_status")).lower()
    temporal_reason = _as_text(candidate.get("co2_temporal_contract_reason"))
    steady_status = _as_text(_first_sampling_value(row, "co2_steady_window_status")).lower()
    steady_reason = _as_text(_first_sampling_value(row, "co2_steady_window_reason"))

    phase_excluded_ratio = _as_float(_first_sampling_value(row, "co2_phase_excluded_ratio")) or 0.0
    segment_excluded_ratio = _as_float(_first_sampling_value(row, "co2_source_segment_settle_excluded_ratio")) or 0.0
    bad_frame_ratio = _as_float(_first_sampling_value(row, "co2_bad_frame_ratio")) or 0.0
    soft_warn_ratio = _as_float(_first_sampling_value(row, "co2_soft_warn_ratio")) or 0.0
    cadence_coverage_ratio = _as_float(_first_sampling_value(row, "co2_cadence_coverage_ratio"))
    effective_dwell_seconds = _as_float(_first_sampling_value(row, "co2_effective_dwell_seconds"))

    backward_timestamp_count = int(_as_float(_first_sampling_value(row, "co2_backward_timestamp_count")) or 0.0)
    duplicate_timestamp_count = int(_as_float(_first_sampling_value(row, "co2_duplicate_timestamp_count")) or 0.0)
    large_gap_count = int(_as_float(_first_sampling_value(row, "co2_large_gap_count")) or 0.0)
    steady_window_found = _as_bool(_first_sampling_value(row, "co2_steady_window_found"))

    fallback_reasons: List[str] = []
    if _first_sampling_value(row, "co2_phase_excluded_ratio") is None:
        fallback_reasons.append("phase_fields_missing")
    if _first_sampling_value(row, "co2_source_segment_settle_excluded_ratio") is None:
        fallback_reasons.append("segment_fields_missing")
    if _first_sampling_value(row, "co2_temporal_contract_status") is None:
        fallback_reasons.append("temporal_fields_missing")
    if _first_sampling_value(row, "co2_steady_window_found") is None:
        fallback_reasons.append("steady_window_fields_missing")

    confidence = 100.0
    if measured_source == "co2_trailing_window_fallback":
        confidence -= 18.0
    if source_switch_reason:
        confidence -= 12.0
    if suitability_status == "advisory":
        confidence -= 8.0
    if temporal_status in {"warn", "degraded", "fallback_row_count"}:
        confidence -= 15.0
    if cadence_coverage_ratio is not None:
        if cadence_coverage_ratio < 0.50:
            confidence -= 25.0
        elif cadence_coverage_ratio < 0.75:
            confidence -= 15.0
        elif cadence_coverage_ratio < 0.90:
            confidence -= 5.0
    if effective_dwell_seconds is not None:
        if effective_dwell_seconds < 2.0:
            confidence -= 20.0
        elif effective_dwell_seconds < 4.0:
            confidence -= 10.0
    for ratio, small_penalty, large_penalty in (
        (phase_excluded_ratio, 10.0, 20.0),
        (segment_excluded_ratio, 10.0, 20.0),
        (bad_frame_ratio, 10.0, 20.0),
        (soft_warn_ratio, 5.0, 10.0),
    ):
        if ratio > 0.40:
            confidence -= large_penalty
        elif ratio > 0.15:
            confidence -= small_penalty
        elif ratio > 0.0:
            confidence -= small_penalty / 2.0
    if duplicate_timestamp_count > 0:
        confidence -= 8.0
    if large_gap_count > 0:
        confidence -= 12.0
    if not steady_window_found and measured_source != "co2_trailing_window_fallback":
        confidence -= 15.0
    if hard_blocked or measured_source == "co2_no_trusted_source" or waterfall_status == "fail" or temporal_status == "fail":
        confidence = 0.0
    confidence = max(0.0, min(100.0, round(confidence, 1)))

    if hard_blocked or suitability_status == "unfit" or measured_source == "co2_no_trusted_source" or waterfall_status == "fail" or temporal_status == "fail":
        settle_status = "unfit"
        manual_review_required = True
        recommended_for_score_path = False
    elif (
        temporal_status in {"warn", "degraded", "fallback_row_count"}
        or backward_timestamp_count > 0
        or large_gap_count > 0
        or duplicate_timestamp_count > 1
        or (cadence_coverage_ratio is not None and cadence_coverage_ratio < 0.75)
        or confidence < 60.0
    ):
        settle_status = "manual_review"
        manual_review_required = True
        recommended_for_score_path = candidate_recommended
    elif measured_source == "co2_trailing_window_fallback" or source_switch_reason or suitability_status == "advisory":
        settle_status = "fallback_but_usable"
        manual_review_required = False
        recommended_for_score_path = candidate_recommended
    else:
        settle_status = "ready"
        manual_review_required = False
        recommended_for_score_path = candidate_recommended

    summary_parts = [
        f"confidence={confidence:.1f}/100",
        f"phase_excluded_ratio={phase_excluded_ratio:.3f}",
        f"segment_excluded_ratio={segment_excluded_ratio:.3f}",
        f"temporal={temporal_status or 'unknown'}",
        f"steady={steady_status or 'unknown'}",
        f"source={source_selected or 'none'}",
    ]
    if cadence_coverage_ratio is not None:
        summary_parts.append(f"cadence_coverage={cadence_coverage_ratio:.3f}")
    if effective_dwell_seconds is not None:
        summary_parts.append(f"effective_dwell_s={effective_dwell_seconds:.3f}")

    reason_chain_parts = [
        f"waterfall={waterfall_status or 'unknown'}",
        f"measured={measured_source or 'none'}",
        f"source={source_selected or 'none'}",
        f"settle={settle_status}",
    ]
    for text in (
        source_switch_reason,
        _as_text(_first_sampling_value(row, "co2_phase_reason_summary")),
        _as_text(_first_sampling_value(row, "co2_source_segment_reason_summary")),
        temporal_reason,
        _as_text(_first_sampling_value(row, "co2_temporal_fallback_reason")),
        steady_reason,
        _as_text(candidate.get("co2_calibration_reason_chain")),
        _as_text(_first_sampling_value(row, "point_quality_reason")),
    ):
        if _as_text(text):
            reason_chain_parts.append(_as_text(text))

    return {
        "point_title": candidate.get("point_title"),
        "point_no": candidate.get("point_no"),
        "point_tag": candidate.get("point_tag"),
        "point_row": candidate.get("point_row"),
        "route": candidate.get("route"),
        "pressure_target_label": candidate.get("pressure_target_label"),
        "co2_ppm_target": candidate.get("co2_ppm_target"),
        "temp_chamber_c": candidate.get("temp_chamber_c"),
        "measured_value": candidate.get("measured_value"),
        "measured_value_source": measured_source,
        "co2_point_suitability_status": suitability_status,
        "co2_calibration_candidate_status": candidate.get("co2_calibration_candidate_status"),
        "co2_calibration_candidate_recommended": candidate_recommended,
        "co2_calibration_candidate_hard_blocked": hard_blocked,
        "co2_calibration_weight_recommended": candidate.get("co2_calibration_weight_recommended"),
        "co2_calibration_reason_chain": candidate.get("co2_calibration_reason_chain"),
        "co2_evidence_score": candidate.get("co2_evidence_score"),
        "co2_decision_waterfall_status": waterfall_status,
        "co2_decision_selected_stage_path": candidate.get("co2_decision_selected_stage_path"),
        "co2_source_selected": source_selected,
        "co2_source_switch_reason": source_switch_reason,
        "co2_source_segment_selected": _as_text(_first_sampling_value(row, "co2_source_segment_selected")),
        "co2_temporal_contract_status": _as_text(_first_sampling_value(row, "co2_temporal_contract_status")),
        "co2_temporal_contract_reason": temporal_reason,
        "co2_temporal_fallback_reason": _as_text(_first_sampling_value(row, "co2_temporal_fallback_reason")),
        "co2_timestamp_monotonic": _first_sampling_value(row, "co2_timestamp_monotonic"),
        "co2_duplicate_timestamp_count": duplicate_timestamp_count,
        "co2_backward_timestamp_count": backward_timestamp_count,
        "co2_large_gap_count": large_gap_count,
        "co2_effective_dwell_seconds": effective_dwell_seconds,
        "co2_nominal_dt_seconds": _as_float(_first_sampling_value(row, "co2_nominal_dt_seconds")),
        "co2_cadence_coverage_ratio": cadence_coverage_ratio,
        "co2_steady_window_found": steady_window_found,
        "co2_steady_window_status": _as_text(_first_sampling_value(row, "co2_steady_window_status")),
        "co2_steady_window_reason": steady_reason,
        "co2_steady_window_sample_count": _as_float(_first_sampling_value(row, "co2_steady_window_sample_count")),
        "co2_phase_excluded_count": _as_float(_first_sampling_value(row, "co2_phase_excluded_count")) or 0.0,
        "co2_phase_excluded_ratio": phase_excluded_ratio,
        "co2_phase_reason_summary": _as_text(_first_sampling_value(row, "co2_phase_reason_summary")),
        "co2_source_segment_settle_excluded_count": _as_float(_first_sampling_value(row, "co2_source_segment_settle_excluded_count")) or 0.0,
        "co2_source_segment_settle_excluded_ratio": segment_excluded_ratio,
        "co2_source_segment_reason_summary": _as_text(_first_sampling_value(row, "co2_source_segment_reason_summary")),
        "co2_bad_frame_count": _as_float(_first_sampling_value(row, "co2_bad_frame_count")) or 0.0,
        "co2_bad_frame_ratio": bad_frame_ratio,
        "co2_soft_warn_count": _as_float(_first_sampling_value(row, "co2_soft_warn_count")) or 0.0,
        "co2_soft_warn_ratio": soft_warn_ratio,
        "co2_sampling_settle_status": settle_status,
        "co2_sampling_manual_review_required": manual_review_required,
        "co2_sampling_recommended_for_score_path": recommended_for_score_path,
        "co2_sampling_window_confidence": _confidence_bucket(confidence),
        "co2_sampling_confidence_score": confidence,
        "co2_sampling_settle_reason_chain": ";".join(part for part in reason_chain_parts if part),
        "co2_sampling_settle_summary": ";".join(summary_parts),
        "co2_sampling_settle_fallback_reason": ";".join(fallback_reasons),
        "co2_sampling_group_key": _group_key_for_row(row),
    }


def build_co2_sampling_settle_points(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [build_co2_sampling_settle_point(row) for row in rows]


def _top_reason_summary(rows: Sequence[Mapping[str, Any]], key: str, *, top_n: int = 4) -> str:
    counter: Counter[str] = Counter()
    for row in rows:
        for token in _as_text(row.get(key)).split(";"):
            token = token.strip()
            if token:
                counter[token] += 1
    if not counter:
        return ""
    return ";".join(f"{name}:{count}" for name, count in counter.most_common(top_n))


def build_co2_sampling_settle_groups(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        payload = dict(row)
        grouped.setdefault(_as_text(payload.get("co2_sampling_group_key")), []).append(payload)

    output: List[Dict[str, Any]] = []
    for group_key, items in sorted(grouped.items(), key=lambda item: item[0]):
        status_counter = Counter(_as_text(row.get("co2_sampling_settle_status")) for row in items)
        confidence_scores = [float(row.get("co2_sampling_confidence_score") or 0.0) for row in items]
        confidence_counter = Counter(_as_text(row.get("co2_sampling_window_confidence")) for row in items)
        output.append(
            {
                "co2_sampling_group_key": group_key,
                "route": items[0].get("route"),
                "pressure_target_label": items[0].get("pressure_target_label"),
                "co2_ppm_target": items[0].get("co2_ppm_target"),
                "temp_chamber_c": items[0].get("temp_chamber_c"),
                "point_count_total": len(items),
                "ready_count": status_counter.get("ready", 0),
                "fallback_but_usable_count": status_counter.get("fallback_but_usable", 0),
                "manual_review_count": status_counter.get("manual_review", 0),
                "unfit_count": status_counter.get("unfit", 0),
                "manual_review_required_count": sum(
                    1 for row in items if _as_bool(row.get("co2_sampling_manual_review_required"))
                ),
                "recommended_for_score_path_count": sum(
                    1 for row in items if _as_bool(row.get("co2_sampling_recommended_for_score_path"))
                ),
                "high_confidence_count": confidence_counter.get("high", 0),
                "medium_confidence_count": confidence_counter.get("medium", 0),
                "low_confidence_count": confidence_counter.get("low", 0),
                "none_confidence_count": confidence_counter.get("none", 0),
                "mean_confidence_score": round(mean(confidence_scores), 3) if confidence_scores else 0.0,
                "group_attention_recommended": status_counter.get("manual_review", 0) > 0 or status_counter.get("unfit", 0) > 0,
                "group_reason_summary": _top_reason_summary(items, "co2_sampling_settle_reason_chain", top_n=5),
            }
        )
    return output


def build_co2_sampling_settle_evidence(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    points = build_co2_sampling_settle_points(rows)
    groups = build_co2_sampling_settle_groups(points)

    status_counter = Counter(_as_text(row.get("co2_sampling_settle_status")) for row in points)
    confidence_counter = Counter(_as_text(row.get("co2_sampling_window_confidence")) for row in points)
    summary = {
        "point_count_total": len(points),
        "ready_count": status_counter.get("ready", 0),
        "fallback_but_usable_count": status_counter.get("fallback_but_usable", 0),
        "manual_review_count": status_counter.get("manual_review", 0),
        "unfit_count": status_counter.get("unfit", 0),
        "manual_review_required_count": sum(
            1 for row in points if _as_bool(row.get("co2_sampling_manual_review_required"))
        ),
        "recommended_for_score_path_count": sum(
            1 for row in points if _as_bool(row.get("co2_sampling_recommended_for_score_path"))
        ),
        "high_confidence_count": confidence_counter.get("high", 0),
        "medium_confidence_count": confidence_counter.get("medium", 0),
        "low_confidence_count": confidence_counter.get("low", 0),
        "none_confidence_count": confidence_counter.get("none", 0),
        "group_count_total": len(groups),
        "top_reason_summary": _top_reason_summary(points, "co2_sampling_settle_reason_chain", top_n=6),
        "fallback_reason_summary": _top_reason_summary(points, "co2_sampling_settle_fallback_reason", top_n=4),
        "evidence_source": "replay_or_exported_v1_points_sampling_settle_sidecar",
        "not_real_acceptance_evidence": True,
    }
    return {"summary": summary, "points": points, "groups": groups}


def render_co2_sampling_settle_report(payload: Mapping[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    groups = list(payload.get("groups") or [])
    lines = [
        "# V1 CO2 sampling / settle evidence",
        "",
        "> replay evidence only",
        "> not real acceptance evidence",
        "",
        "## 总览",
        f"- 总点数: {summary.get('point_count_total', 0)}",
        f"- ready / fallback-but-usable / manual-review / unfit: "
        f"{summary.get('ready_count', 0)} / {summary.get('fallback_but_usable_count', 0)} / "
        f"{summary.get('manual_review_count', 0)} / {summary.get('unfit_count', 0)}",
        f"- manual_review_required: {summary.get('manual_review_required_count', 0)}",
        f"- recommended_for_score_path: {summary.get('recommended_for_score_path_count', 0)}",
        f"- confidence high / medium / low / none: "
        f"{summary.get('high_confidence_count', 0)} / {summary.get('medium_confidence_count', 0)} / "
        f"{summary.get('low_confidence_count', 0)} / {summary.get('none_confidence_count', 0)}",
        f"- top reasons: {summary.get('top_reason_summary', '')}",
        f"- fallback reasons: {summary.get('fallback_reason_summary', '')}",
        "",
        "## 重点分组",
    ]
    if not groups:
        lines.append("- 无可用分组")
    else:
        ranked = sorted(
            groups,
            key=lambda row: (
                not _as_bool(row.get("group_attention_recommended")),
                -int(row.get("manual_review_count") or 0),
                -int(row.get("unfit_count") or 0),
                float(row.get("mean_confidence_score") or 0.0),
            ),
        )
        for row in ranked[:10]:
            lines.append(
                "- "
                + f"{row.get('co2_sampling_group_key')}: "
                + f"ready={row.get('ready_count')} "
                + f"fallback={row.get('fallback_but_usable_count')} "
                + f"manual_review={row.get('manual_review_count')} "
                + f"unfit={row.get('unfit_count')} "
                + f"mean_confidence={row.get('mean_confidence_score')} "
                + f"attention={row.get('group_attention_recommended')} "
                + f"reasons={row.get('group_reason_summary')}"
            )
    lines.extend(
        [
            "",
            "## 说明",
            "- 这份 sidecar 只消费现有 hardened V1 CO2 的 sampling / settle / temporal / suitability 证据，不改变 live measured_value。",
            "- 它的价值是把 ready、fallback-but-usable、manual-review、unfit 这四类采样语义说清楚，方便工程师回看和后续离线审查。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_co2_sampling_settle_artifacts(output_dir: Path, payload: Mapping[str, Any]) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    points_path = output_dir / "sampling_settle_points.csv"
    groups_path = output_dir / "sampling_settle_groups.csv"
    summary_csv_path = output_dir / "sampling_settle_summary.csv"
    summary_json_path = output_dir / "sampling_settle_summary.json"
    report_path = output_dir / "sampling_settle_report.md"

    write_csv_rows(points_path, payload.get("points") or [])
    write_csv_rows(groups_path, payload.get("groups") or [])
    write_csv_rows(summary_csv_path, [payload.get("summary") or {}])
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    report_path.write_text(render_co2_sampling_settle_report(payload), encoding="utf-8")
    return {
        "points_csv": points_path,
        "groups_csv": groups_path,
        "summary_csv": summary_csv_path,
        "summary_json": summary_json_path,
        "report_md": report_path,
    }
