"""Offline CO2 SENCO1/SENCO3 model-structure review.

This module compares no-pressure S1/S3 model structures on already-recorded
V1.5 open-flow CO2 evidence. It never opens COM ports, controls gas/water
routes, or writes coefficients. S5 is intentionally excluded: output-layer
linear trim must not be used to hide S1/S3 main-model residuals.
"""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np

from .co2_fit_algorithm_matrix import (
    CORE_TERMS,
    TEMP_TERMS,
    FitPoint,
    ModelVariant,
    _load_fit_points,
    _safe_float,
)
from .co2_relative_s13_objective_review import (
    DEFAULT_OBJECTIVES,
    DEFAULT_ZERO_OFFSETS_PPM,
    _apply_zero_offset,
    _fit_objective,
)


DEFAULT_TERMS_BY_STRUCTURE: Mapping[str, tuple[str, ...]] = {
    "core_ratio_only_diagnostic": CORE_TERMS,
    "core_plus_linear_temp": CORE_TERMS + ("T", "RT"),
    "core_plus_full_temp": CORE_TERMS + TEMP_TERMS,
}
DEFAULT_STRUCTURES = tuple(DEFAULT_TERMS_BY_STRUCTURE)
DEFAULT_STRUCTURE_OBJECTIVES = (
    "absolute_lstsq",
    "relative_weighted_lstsq",
    "low_end_priority_lstsq",
)


@dataclass(frozen=True)
class StructureCandidate:
    structure_id: str
    terms: tuple[str, ...]
    objective_id: str
    zero_offset_ppm: float
    low_end_multiplier: float
    diagnostic_only: bool = False

    @property
    def model_id(self) -> str:
        return f"{self.structure_id}__{self.objective_id}__zero{self.zero_offset_ppm:g}"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: str | Path | None) -> List[Dict[str, Any]]:
    if not path:
        return []
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    headers: List[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def _device_groups(points: Sequence[FitPoint]) -> Dict[str, List[FitPoint]]:
    groups: Dict[str, List[FitPoint]] = {}
    for point in points:
        groups.setdefault(point.device_id, []).append(point)
    return groups


def _point_lookup(rows: Sequence[Mapping[str, Any]]) -> Dict[tuple[str, str], Mapping[str, Any]]:
    lookup: Dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in rows:
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        identity = str(row.get("point_identity") or row.get("sample_index") or "").strip()
        if device and identity:
            lookup[(device, identity)] = row
    return lookup


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _target_group(point: FitPoint) -> str:
    return f"{float(point.target_ppm):g}ppm"


def _temperature_group(point: FitPoint) -> str:
    identity = str(point.point_identity or "").strip()
    if identity.startswith("T") and "_" in identity:
        return identity.split("_", 1)[0]
    return f"T{float(point.temperature_c):g}"


def _is_zero_anchor(point: FitPoint) -> bool:
    marker = str(point.zero_anchor_class or "").lower()
    return abs(float(point.target_ppm)) <= 1.0e-9 or "zero" in marker


def _ratio_grade(row: Mapping[str, Any] | None) -> str:
    if not row:
        return "unknown"
    std = _safe_float(
        row.get("co2_ratio_f_std")
        or row.get("ratio_std")
        or row.get("R_CO2_std")
        or row.get("co2_ratio_std")
    )
    if std is None:
        return "unknown"
    if std <= 0.0005:
        return "A"
    if std <= 0.001:
        return "B"
    return "C"


def _dryness_grade(row: Mapping[str, Any] | None) -> str:
    if not row:
        return "unknown"
    dew = _safe_float(
        row.get("dewpoint_c_mean")
        or row.get("dewpoint_c")
        or row.get("dewpoint_mean_c")
        or row.get("dp_c")
    )
    if dew is None:
        return "unknown"
    if dew <= -28.0:
        return "deep_dry"
    if dew <= -20.0:
        return "usable_but_not_deep_dry"
    return "dryness_risk"


def _physical_qc_label(row: Mapping[str, Any] | None) -> str:
    ratio = _ratio_grade(row)
    dryness = _dryness_grade(row)
    if ratio == "A" and dryness == "deep_dry":
        return "physical_qc_good"
    if ratio in {"B", "unknown"} or dryness in {"usable_but_not_deep_dry", "unknown"}:
        return "physical_qc_review"
    return "physical_qc_degrade_candidate"


def _terms_physical_meaning(structure_id: str) -> str:
    if structure_id == "core_ratio_only_diagnostic":
        return "Ratio-only diagnostic; useful to show whether temperature terms are needed, not preferred for full-temperature writing."
    if structure_id == "core_plus_linear_temp":
        return "No-pressure S1/S3 with linear temperature and R*T terms; checks whether T2 is overfitting."
    if structure_id == "core_plus_full_temp":
        return "No-pressure S1/S3 main contract using R polynomial plus T, T2, and R*T."
    return "No-pressure S1/S3 structure review candidate."


def _make_variant(candidate: StructureCandidate) -> ModelVariant:
    return ModelVariant(
        model_id=candidate.model_id,
        terms=candidate.terms,
        pressure_unit="kpa",
        preserve_existing_pressure_slots=False,
        use_celsius_temperature=False,
        apply_h2o_dry_basis_target_bridge=False,
        write_contract=(
            "diagnostic_only_not_direct_write"
            if candidate.diagnostic_only
            else "no_pressure_senco13_structure_review_no_s5"
        ),
    )


def _build_candidates(
    *,
    structures: Sequence[str],
    objectives: Sequence[str],
    zero_offsets_ppm: Sequence[float],
    low_end_multiplier: float,
) -> List[StructureCandidate]:
    candidates: List[StructureCandidate] = []
    for structure_id in structures:
        terms = DEFAULT_TERMS_BY_STRUCTURE.get(structure_id)
        if not terms:
            continue
        diagnostic_only = structure_id == "core_ratio_only_diagnostic"
        for zero_offset in zero_offsets_ppm:
            for objective_id in objectives:
                candidates.append(
                    StructureCandidate(
                        structure_id=structure_id,
                        terms=tuple(terms),
                        objective_id=str(objective_id),
                        zero_offset_ppm=float(zero_offset),
                        low_end_multiplier=float(low_end_multiplier),
                        diagnostic_only=diagnostic_only,
                    )
                )
    return candidates


def _numeric_metric(row: Mapping[str, Any], key: str) -> float:
    value = _safe_float(row.get(key))
    if value is None:
        return float("inf")
    return abs(float(value))


def _selection_score(row: Mapping[str, Any]) -> tuple[float, float, float, float]:
    return (
        _numeric_metric(row, "max_abs_relative_error_percent"),
        _numeric_metric(row, "low_end_max_abs_relative_error_percent"),
        _numeric_metric(row, "rmse_ppm"),
        _numeric_metric(row, "zero_anchor_max_abs_error_ppm"),
    )


def _candidate_action(best: Mapping[str, Any], baseline: Mapping[str, Any] | None) -> str:
    best_max = _safe_float(best.get("max_abs_relative_error_percent"))
    baseline_max = _safe_float(baseline.get("max_abs_relative_error_percent")) if baseline else None
    if best_max is None:
        return "review_manually_no_write"
    if baseline_max is not None and best_max < baseline_max * 0.85:
        return "review_s1s3_structure_candidate_no_write"
    if best.get("zero_offset_ppm") not in (0, 0.0, "0", "0.0"):
        return "review_zero_anchor_traceability_before_write"
    return "keep_main_contract_review_residual_sources"


def _summarize_candidate(
    *,
    device_id: str,
    candidate: StructureCandidate,
    summary: Mapping[str, Any],
) -> Dict[str, Any]:
    row = dict(summary)
    row.update(
        {
            "device_id": device_id,
            "structure_id": candidate.structure_id,
            "structure_family": "diagnostic" if candidate.diagnostic_only else "senco13_main_model_candidate",
            "terms": ";".join(candidate.terms),
            "uses_pressure_terms": False,
            "uses_s5_output_trim": False,
            "diagnostic_only": candidate.diagnostic_only,
            "physical_meaning": _terms_physical_meaning(candidate.structure_id),
            "auto_write_allowed": False,
        }
    )
    return row


def _residual_with_context(
    residual: Mapping[str, Any],
    *,
    candidate: StructureCandidate,
    metadata: Mapping[tuple[str, str], Mapping[str, Any]],
) -> Dict[str, Any]:
    device = _device_id(residual.get("device_id"))
    identity = str(residual.get("point_identity") or "").strip()
    meta = metadata.get((device, identity), {})
    row = dict(residual)
    row.update(
        {
            "structure_id": candidate.structure_id,
            "structure_family": "diagnostic" if candidate.diagnostic_only else "senco13_main_model_candidate",
            "terms": ";".join(candidate.terms),
            "uses_pressure_terms": False,
            "uses_s5_output_trim": False,
            "ratio_grade": _ratio_grade(meta),
            "dryness_grade": _dryness_grade(meta),
            "physical_qc_label": _physical_qc_label(meta),
            "review_note": _point_review_note(residual, meta),
        }
    )
    return row


def _point_review_note(row: Mapping[str, Any], meta: Mapping[str, Any] | None) -> str:
    rel = _safe_float(row.get("relative_error_percent"))
    target = _safe_float(row.get("target_ppm"))
    qc = _physical_qc_label(meta)
    zero = "zero" in str(row.get("zero_anchor_class") or "").lower() or (target is not None and abs(target) <= 1e-9)
    if qc == "physical_qc_degrade_candidate":
        return "physical_qc_degrade_candidate"
    if zero:
        return "zero_anchor_keep_but_traceability_or_assigned_value_must_be_reviewed"
    if rel is not None and abs(float(rel)) >= 1.0 and qc == "physical_qc_good":
        return "do_not_silently_exclude_good_physics_point_review_model_or_target"
    return "keep_or_review_with_context"


def _relative_values(rows: Sequence[Mapping[str, Any]]) -> List[float]:
    values = []
    for row in rows:
        rel = _safe_float(row.get("relative_error_percent"))
        if rel is not None:
            values.append(float(rel))
    return values


def _max_abs(values: Sequence[float]) -> float | str:
    return max(abs(value) for value in values) if values else ""


def _build_segment_diagnostics(
    *,
    device_id: str,
    residuals: Sequence[Mapping[str, Any]],
    low_end_target_ppm: float,
) -> List[Dict[str, Any]]:
    low = []
    high = []
    zero = []
    for row in residuals:
        target = _safe_float(row.get("target_ppm"))
        if target is None:
            continue
        zero_marker = str(row.get("zero_anchor_class") or "").lower()
        identity = str(row.get("point_identity") or "").lower()
        is_zero_anchor = (
            abs(float(target)) <= 1.0e-9
            or "zero" in zero_marker
            or identity.endswith("_0ppm")
        )
        if is_zero_anchor:
            zero.append(row)
        elif float(target) <= float(low_end_target_ppm):
            low.append(row)
        else:
            high.append(row)
    return [
        {
            "device_id": device_id,
            "segment_id": "zero_anchor",
            "point_count": len(zero),
            "max_abs_error_ppm": _max_abs([float(_safe_float(row.get("error_ppm")) or 0.0) for row in zero]),
            "max_abs_relative_error_percent": "",
            "physical_meaning": "Zero gas anchors constrain the intercept but need traceable assigned CO2 content.",
        },
        {
            "device_id": device_id,
            "segment_id": f"low_nonzero_le_{float(low_end_target_ppm):g}ppm",
            "point_count": len(low),
            "max_abs_error_ppm": _max_abs([float(_safe_float(row.get("error_ppm")) or 0.0) for row in low]),
            "max_abs_relative_error_percent": _max_abs(_relative_values(low)),
            "physical_meaning": "Low-end residuals determine whether S1/S3 intercept/low-end shape is adequate.",
        },
        {
            "device_id": device_id,
            "segment_id": f"high_gt_{float(low_end_target_ppm):g}ppm",
            "point_count": len(high),
            "max_abs_error_ppm": _max_abs([float(_safe_float(row.get("error_ppm")) or 0.0) for row in high]),
            "max_abs_relative_error_percent": _max_abs(_relative_values(high)),
            "physical_meaning": "High-end residuals show whether low-end improvement sacrifices span points.",
        },
    ]


def _build_temperature_bias(
    *,
    device_id: str,
    residuals: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in residuals:
        identity = str(row.get("point_identity") or "").strip()
        if identity.startswith("T") and "_" in identity:
            key = identity.split("_", 1)[0]
        else:
            temp = _safe_float(row.get("temperature_c"))
            key = f"T{temp:g}" if temp is not None else "T_unknown"
        groups[key].append(row)
    rows = []
    for key, items in sorted(groups.items()):
        errors = [float(_safe_float(row.get("error_ppm")) or 0.0) for row in items]
        rels = _relative_values(items)
        rows.append(
            {
                "device_id": device_id,
                "temperature_group": key,
                "point_count": len(items),
                "mean_error_ppm": sum(errors) / len(errors) if errors else "",
                "max_abs_error_ppm": _max_abs(errors),
                "max_abs_relative_error_percent": _max_abs(rels),
                "same_sign_residual_fraction": _same_sign_fraction(errors),
                "temperature_model_review": _temperature_bias_note(errors),
            }
        )
    return rows


def _same_sign_fraction(values: Sequence[float]) -> float | str:
    if not values:
        return ""
    positives = sum(1 for value in values if value > 0.0)
    negatives = sum(1 for value in values if value < 0.0)
    return max(positives, negatives) / len(values)


def _temperature_bias_note(errors: Sequence[float]) -> str:
    if not errors:
        return "no_points"
    same_sign = _same_sign_fraction(errors)
    mean_error = sum(errors) / len(errors)
    if isinstance(same_sign, float) and same_sign >= 0.8 and abs(mean_error) >= 3.0:
        return "temperature_group_bias_review_temperature_terms_or_temperature_channel"
    return "no_strong_temperature_group_bias"


def _build_model_capacity_boundary(
    *,
    device_id: str,
    baseline: Mapping[str, Any],
    best: Mapping[str, Any],
    selected_residuals: Sequence[Mapping[str, Any]],
    low_end_target_ppm: float,
) -> Dict[str, Any]:
    baseline_max = _safe_float(baseline.get("max_abs_relative_error_percent"))
    best_max = _safe_float(best.get("max_abs_relative_error_percent"))
    baseline_low = _safe_float(baseline.get("low_end_max_abs_relative_error_percent"))
    best_low = _safe_float(best.get("low_end_max_abs_relative_error_percent"))
    if baseline_max is not None and best_max is not None:
        improvement = float(baseline_max) - float(best_max)
        improvement_fraction = improvement / float(baseline_max) if baseline_max else 0.0
    else:
        improvement = ""
        improvement_fraction = ""

    low_good_large = 0
    low_good_total = 0
    zero_anchor_count = 0
    temperature_bias_groups = 0
    target_bias_groups = 0
    by_temperature: Dict[str, List[float]] = defaultdict(list)
    by_target: Dict[str, List[float]] = defaultdict(list)
    for row in selected_residuals:
        target = _safe_float(row.get("target_ppm"))
        error = _safe_float(row.get("error_ppm"))
        rel = _safe_float(row.get("relative_error_percent"))
        if target is None or error is None:
            continue
        identity = str(row.get("point_identity") or "")
        if abs(float(target)) <= 1.0e-9 or "zero" in str(row.get("zero_anchor_class") or "").lower():
            zero_anchor_count += 1
        if 0.0 < float(target) <= float(low_end_target_ppm):
            if row.get("physical_qc_label") == "physical_qc_good":
                low_good_total += 1
                if rel is not None and abs(float(rel)) >= 1.0:
                    low_good_large += 1
            temp_key = identity.split("_", 1)[0] if identity.startswith("T") and "_" in identity else "T_unknown"
            by_temperature[temp_key].append(float(error))
            by_target[f"{float(target):g}ppm"].append(float(error))

    for errors in by_temperature.values():
        same_sign = _same_sign_fraction(errors)
        if isinstance(same_sign, float) and same_sign >= 0.8 and abs(sum(errors) / len(errors)) >= 3.0:
            temperature_bias_groups += 1
    for errors in by_target.values():
        same_sign = _same_sign_fraction(errors)
        if isinstance(same_sign, float) and same_sign >= 0.8 and abs(sum(errors) / len(errors)) >= 3.0:
            target_bias_groups += 1

    if best_max is not None and float(best_max) <= 1.5:
        status = "s1s3_candidate_close_enough_for_write_review"
        next_action = "先保留 S1/S3 候选，进入受控写入前评审；S5 只作为后续输出层微调。"
    elif isinstance(improvement_fraction, float) and improvement_fraction >= 0.15:
        status = "s1s3_objective_or_anchor_choice_has_meaningful_gain"
        next_action = "S1/S3 仍有可解释改进空间，优先评审该候选的零气假设和低端残差。"
    elif low_good_large > 0 and (temperature_bias_groups > 0 or target_bias_groups > 0):
        status = "s1s3_writable_slots_likely_exhausted_for_this_dataset"
        next_action = "低端点物理状态良好但同温度/同气点仍有系统残差；不要先用 S5 掩盖，先复核零气指定值、气瓶/阀路映射和温度项模型边界。"
    else:
        status = "manual_model_boundary_review"
        next_action = "逐点复核残差来源，确认是否有可剔除的坏物理状态点；无证据时不自动删点。"

    return {
        "device_id": device_id,
        "baseline_structure_id": baseline.get("structure_id", ""),
        "baseline_objective_id": baseline.get("objective_id", ""),
        "baseline_zero_offset_ppm": baseline.get("zero_offset_ppm", ""),
        "baseline_max_abs_relative_error_percent": baseline.get("max_abs_relative_error_percent", ""),
        "baseline_low_end_max_abs_relative_error_percent": baseline.get("low_end_max_abs_relative_error_percent", ""),
        "best_structure_id": best.get("structure_id", ""),
        "best_objective_id": best.get("objective_id", ""),
        "best_zero_offset_ppm": best.get("zero_offset_ppm", ""),
        "best_max_abs_relative_error_percent": best.get("max_abs_relative_error_percent", ""),
        "best_low_end_max_abs_relative_error_percent": best.get("low_end_max_abs_relative_error_percent", ""),
        "best_vs_baseline_improvement_percent_points": improvement,
        "best_vs_baseline_improvement_fraction": improvement_fraction,
        "zero_anchor_count": zero_anchor_count,
        "low_end_good_physics_point_count": low_good_total,
        "low_end_good_physics_large_residual_count": low_good_large,
        "low_end_temperature_bias_group_count": temperature_bias_groups,
        "low_end_target_bias_group_count": target_bias_groups,
        "model_capacity_status": status,
        "recommended_next_action": next_action,
        "uses_pressure_terms": False,
        "uses_s5_output_trim": False,
        "writes_coefficients": False,
    }


def _build_selected_common_mode_patterns(
    selected_residuals: Sequence[Mapping[str, Any]],
    *,
    low_end_target_ppm: float,
) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in selected_residuals:
        target = _safe_float(row.get("target_ppm"))
        error = _safe_float(row.get("error_ppm"))
        if target is None or error is None:
            continue
        if not (0.0 < float(target) <= float(low_end_target_ppm)):
            continue
        identity = str(row.get("point_identity") or "").strip()
        if not identity:
            continue
        groups[identity].append(row)

    out: List[Dict[str, Any]] = []
    for identity, items in sorted(groups.items()):
        errors = [float(_safe_float(row.get("error_ppm")) or 0.0) for row in items]
        rels = [
            abs(float(_safe_float(row.get("relative_error_percent"))))
            for row in items
            if _safe_float(row.get("relative_error_percent")) is not None
        ]
        positives = sum(1 for value in errors if value > 0.0)
        negatives = sum(1 for value in errors if value < 0.0)
        same_sign = _same_sign_fraction(errors)
        common = isinstance(same_sign, float) and len(items) >= 3 and same_sign >= 0.8 and abs(sum(errors) / len(errors)) >= 3.0
        out.append(
            {
                "point_identity": identity,
                "device_count": len(items),
                "target_ppm": items[0].get("target_ppm", ""),
                "temperature_c": items[0].get("temperature_c", ""),
                "mean_error_ppm": sum(errors) / len(errors) if errors else "",
                "positive_error_count": positives,
                "negative_error_count": negatives,
                "same_sign_residual_fraction": same_sign,
                "max_abs_relative_error_percent": max(rels) if rels else "",
                "ratio_grade_counts": _counts(row.get("ratio_grade") for row in items),
                "dryness_grade_counts": _counts(row.get("dryness_grade") for row in items),
                "common_mode_status": "common_mode_suspect" if common else "device_specific_or_balanced",
                "physical_interpretation": (
                    "多台设备在同一低端点同向偏差，优先复核共同目标值、阀路/气瓶状态和 S1/S3 低端温度形状。"
                    if common
                    else "同一点残差方向不一致，更像设备个体差异或局部模型残差。"
                ),
                "writes_coefficients": False,
            }
        )
    out.sort(
        key=lambda row: (
            0 if row.get("common_mode_status") == "common_mode_suspect" else 1,
            -float(row.get("max_abs_relative_error_percent") or 0.0),
        )
    )
    return out


def _counts(values: Iterable[Any]) -> str:
    counter: Dict[str, int] = defaultdict(int)
    for value in values:
        text = str(value or "unknown")
        counter[text] += 1
    return ";".join(f"{key}:{counter[key]}" for key in sorted(counter))


def _leave_one_out_review(
    *,
    device_points: Sequence[FitPoint],
    selected: StructureCandidate,
    selected_summary: Mapping[str, Any],
    metadata: Mapping[tuple[str, str], Mapping[str, Any]],
    min_relative_target_ppm: float,
    low_end_target_ppm: float,
    max_points: int = 80,
) -> List[Dict[str, Any]]:
    fit_points = [point for point in device_points if point.source_role == "fit"]
    if len(fit_points) <= 4:
        return []
    baseline = _safe_float(selected_summary.get("max_abs_relative_error_percent"))
    baseline_low = _safe_float(selected_summary.get("low_end_max_abs_relative_error_percent"))
    if baseline is None:
        return []
    rows: List[Dict[str, Any]] = []
    limited = fit_points[: int(max_points)]
    for point in limited:
        reduced = [item for item in device_points if item is not point]
        adjusted = _apply_zero_offset(reduced, selected.zero_offset_ppm)
        variant = _make_variant(selected)
        summary, residuals = _fit_objective(
            adjusted,
            variant=variant,
            objective_id=selected.objective_id,
            zero_offset_ppm=selected.zero_offset_ppm,
            min_relative_target_ppm=min_relative_target_ppm,
            low_end_target_ppm=low_end_target_ppm,
            low_end_multiplier=selected.low_end_multiplier,
            irls_iterations=5,
        )
        if not summary:
            continue
        new_max = _safe_float(summary.get("max_abs_relative_error_percent"))
        new_low = _safe_float(summary.get("low_end_max_abs_relative_error_percent"))
        meta = metadata.get((point.device_id, point.point_identity), {})
        improvement = float(baseline) - float(new_max) if new_max is not None else ""
        low_improvement = (
            float(baseline_low) - float(new_low)
            if baseline_low is not None and new_low is not None
            else ""
        )
        rows.append(
            {
                "device_id": point.device_id,
                "point_identity": point.point_identity,
                "target_ppm": point.target_ppm,
                "temperature_group": _temperature_group(point),
                "target_group": _target_group(point),
                "selected_structure_id": selected.structure_id,
                "selected_objective_id": selected.objective_id,
                "selected_zero_offset_ppm": selected.zero_offset_ppm,
                "baseline_max_abs_relative_error_percent": baseline,
                "leave_one_out_max_abs_relative_error_percent": new_max if new_max is not None else "",
                "max_relative_error_improvement_percent_points": improvement,
                "low_end_relative_error_improvement_percent_points": low_improvement,
                "ratio_grade": _ratio_grade(meta),
                "dryness_grade": _dryness_grade(meta),
                "physical_qc_label": _physical_qc_label(meta),
                "exclusion_recommendation": _exclusion_recommendation(point, meta, improvement),
                "writes_coefficients": False,
            }
        )
    rows.sort(
        key=lambda row: float(row.get("max_relative_error_improvement_percent_points") or -1.0e9),
        reverse=True,
    )
    return rows


def _exclusion_recommendation(point: FitPoint, meta: Mapping[str, Any] | None, improvement: Any) -> str:
    qc = _physical_qc_label(meta)
    if qc == "physical_qc_degrade_candidate":
        return "degrade_or_exclude_if_raw_evidence_confirms_bad_physical_state"
    value = _safe_float(improvement)
    if value is not None and value >= 0.5:
        if _is_zero_anchor(point):
            return "do_not_drop_zero_anchor_review_assigned_zero_value"
        return "manual_review_only_good_physics_point_may_indicate_model_or_target_issue"
    return "keep"


def _selected_candidate_from_summary(row: Mapping[str, Any]) -> StructureCandidate:
    terms = tuple(str(row.get("terms") or "").split(";"))
    terms = tuple(term for term in terms if term)
    return StructureCandidate(
        structure_id=str(row.get("structure_id") or ""),
        terms=terms,
        objective_id=str(row.get("objective_id") or ""),
        zero_offset_ppm=float(_safe_float(row.get("zero_offset_ppm")) or 0.0),
        low_end_multiplier=float(_safe_float(row.get("low_end_multiplier")) or 3.0),
        diagnostic_only=str(row.get("diagnostic_only")).lower() == "true",
    )


def build_co2_s13_model_structure_review(
    *,
    fit_points_csv: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    structures: Sequence[str] = DEFAULT_STRUCTURES,
    objectives: Sequence[str] = DEFAULT_STRUCTURE_OBJECTIVES,
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    min_relative_target_ppm: float = 50.0,
    low_end_target_ppm: float = 300.0,
    low_end_multiplier: float = 3.0,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build no-write S1/S3 model-structure review tables."""

    points = _load_fit_points(
        fit_points_csv,
        exclude_device_ids=exclude_device_ids,
        treatment_plan_csv=fit_point_treatment_plan_csv,
    )
    raw_rows = _read_csv(fit_points_csv)
    metadata = _point_lookup(raw_rows)
    by_device = _device_groups(points)
    candidates = _build_candidates(
        structures=structures,
        objectives=objectives,
        zero_offsets_ppm=zero_offsets_ppm,
        low_end_multiplier=low_end_multiplier,
    )

    structure_summary: List[Dict[str, Any]] = []
    structure_residuals: List[Dict[str, Any]] = []
    selected_rows: List[Dict[str, Any]] = []
    point_influence_rows: List[Dict[str, Any]] = []
    segment_rows: List[Dict[str, Any]] = []
    temperature_rows: List[Dict[str, Any]] = []
    treatment_rows: List[Dict[str, Any]] = []
    model_capacity_rows: List[Dict[str, Any]] = []
    selected_residual_rows: List[Dict[str, Any]] = []

    for device_id, device_points in sorted(by_device.items()):
        device_summary: List[Dict[str, Any]] = []
        device_residuals_by_model: Dict[str, List[Dict[str, Any]]] = {}
        for candidate in candidates:
            adjusted = _apply_zero_offset(device_points, candidate.zero_offset_ppm)
            variant = _make_variant(candidate)
            summary, residuals = _fit_objective(
                adjusted,
                variant=variant,
                objective_id=candidate.objective_id,
                zero_offset_ppm=candidate.zero_offset_ppm,
                min_relative_target_ppm=float(min_relative_target_ppm),
                low_end_target_ppm=float(low_end_target_ppm),
                low_end_multiplier=float(candidate.low_end_multiplier),
                irls_iterations=5,
            )
            if not summary:
                continue
            summary_row = _summarize_candidate(
                device_id=device_id,
                candidate=candidate,
                summary=summary,
            )
            device_summary.append(summary_row)
            structure_summary.append(summary_row)
            contextual = [
                _residual_with_context(row, candidate=candidate, metadata=metadata)
                for row in residuals
            ]
            device_residuals_by_model[candidate.model_id] = contextual
            structure_residuals.extend(contextual)

        if not device_summary:
            continue
        non_diagnostic = [row for row in device_summary if not row.get("diagnostic_only")]
        baseline = next(
            (
                row
                for row in non_diagnostic
                if row.get("structure_id") == "core_plus_full_temp"
                and row.get("objective_id") == "absolute_lstsq"
                and float(row.get("zero_offset_ppm") or 0.0) == 0.0
            ),
            non_diagnostic[0] if non_diagnostic else device_summary[0],
        )
        best = min(non_diagnostic or device_summary, key=_selection_score)
        selected_candidate = _selected_candidate_from_summary(best)
        selected_residuals = device_residuals_by_model.get(selected_candidate.model_id, [])
        selected_residual_rows.extend(selected_residuals)
        selected_rows.append(
            {
                "device_id": device_id,
                "baseline_structure_id": baseline.get("structure_id"),
                "baseline_objective_id": baseline.get("objective_id"),
                "baseline_zero_offset_ppm": baseline.get("zero_offset_ppm"),
                "baseline_max_abs_relative_error_percent": baseline.get("max_abs_relative_error_percent"),
                "best_structure_id": best.get("structure_id"),
                "best_objective_id": best.get("objective_id"),
                "best_zero_offset_ppm": best.get("zero_offset_ppm"),
                "best_terms": best.get("terms"),
                "best_max_abs_relative_error_percent": best.get("max_abs_relative_error_percent"),
                "best_low_end_max_abs_relative_error_percent": best.get("low_end_max_abs_relative_error_percent"),
                "best_rmse_ppm": best.get("rmse_ppm"),
                "best_s1_payload_scientific": best.get("s1_payload_scientific"),
                "best_s3_payload_scientific": best.get("s3_payload_scientific"),
                "recommended_no_write_action": _candidate_action(best, baseline),
                "uses_pressure_terms": False,
                "uses_s5_output_trim": False,
                "auto_write_allowed": False,
                "physical_meaning": (
                    "Selected only for offline S1/S3 structure review. S5 remains excluded and pressure terms stay zero."
                ),
            }
        )
        model_capacity_rows.append(
            _build_model_capacity_boundary(
                device_id=device_id,
                baseline=baseline,
                best=best,
                selected_residuals=selected_residuals,
                low_end_target_ppm=float(low_end_target_ppm),
            )
        )
        segment_rows.extend(
            _build_segment_diagnostics(
                device_id=device_id,
                residuals=selected_residuals,
                low_end_target_ppm=float(low_end_target_ppm),
            )
        )
        temperature_rows.extend(_build_temperature_bias(device_id=device_id, residuals=selected_residuals))
        point_influence_rows.extend(
            _leave_one_out_review(
                device_points=device_points,
                selected=selected_candidate,
                selected_summary=best,
                metadata=metadata,
                min_relative_target_ppm=float(min_relative_target_ppm),
                low_end_target_ppm=float(low_end_target_ppm),
            )
        )
        for row in selected_residuals:
            rel = _safe_float(row.get("relative_error_percent"))
            target = _safe_float(row.get("target_ppm"))
            if rel is None and (target is None or abs(float(target)) > 1e-9):
                continue
            if (rel is not None and abs(float(rel)) >= 1.0) or str(row.get("review_note") or "").startswith("zero_anchor"):
                treatment_rows.append(
                    {
                        "device_id": device_id,
                        "point_identity": row.get("point_identity"),
                        "target_ppm": row.get("target_ppm"),
                        "temperature_c": row.get("temperature_c"),
                        "relative_error_percent": row.get("relative_error_percent"),
                        "error_ppm": row.get("error_ppm"),
                        "ratio_grade": row.get("ratio_grade"),
                        "dryness_grade": row.get("dryness_grade"),
                        "physical_qc_label": row.get("physical_qc_label"),
                        "recommendation": _point_review_note(row, metadata.get((device_id, str(row.get("point_identity") or "")))),
                        "auto_exclude": False,
                        "physical_meaning": (
                            "Do not delete a point only because residual is large. Exclusion needs bad physical-state evidence."
                        ),
                    }
                )

    run_summary = [
        {
            "created_at": _now(),
            "fit_points_csv": str(Path(fit_points_csv).resolve()),
            "fit_point_treatment_plan_csv": (
                str(Path(fit_point_treatment_plan_csv).resolve()) if fit_point_treatment_plan_csv else ""
            ),
            "device_count": len(by_device),
            "point_count": len(points),
            "structure_candidate_count": len(structure_summary),
            "selected_candidate_count": len(selected_rows),
            "model_capacity_boundary_count": len(model_capacity_rows),
            "structures": ";".join(structures),
            "objectives": ";".join(objectives),
            "zero_offsets_ppm": ";".join(f"{float(value):g}" for value in zero_offsets_ppm),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "uses_pressure_terms": False,
            "uses_s5_output_trim": False,
            "not_real_acceptance_evidence": True,
        }
    ]
    return {
        "run_summary": run_summary,
        "structure_summary": structure_summary,
        "selected_structure_candidates": selected_rows,
        "structure_residuals": structure_residuals,
        "point_influence_review": point_influence_rows,
        "segment_diagnostic": segment_rows,
        "temperature_bias_diagnostic": temperature_rows,
        "point_treatment_recommendations": treatment_rows,
        "model_capacity_boundary": model_capacity_rows,
        "selected_low_end_common_mode_patterns": _build_selected_common_mode_patterns(
            selected_residual_rows,
            low_end_target_ppm=float(low_end_target_ppm),
        ),
    }


def write_co2_s13_model_structure_review(
    *,
    fit_points_csv: str | Path,
    output_dir: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    structures: Sequence[str] = DEFAULT_STRUCTURES,
    objectives: Sequence[str] = DEFAULT_STRUCTURE_OBJECTIVES,
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    min_relative_target_ppm: float = 50.0,
    low_end_target_ppm: float = 300.0,
    low_end_multiplier: float = 3.0,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_model_structure_review(
        fit_points_csv=fit_points_csv,
        fit_point_treatment_plan_csv=fit_point_treatment_plan_csv,
        exclude_device_ids=exclude_device_ids,
        structures=structures,
        objectives=objectives,
        zero_offsets_ppm=zero_offsets_ppm,
        min_relative_target_ppm=min_relative_target_ppm,
        low_end_target_ppm=low_end_target_ppm,
        low_end_multiplier=low_end_multiplier,
    )
    outputs = {
        "run_summary": output / "co2_s13_model_structure_run_summary.csv",
        "structure_summary": output / "co2_s13_model_structure_summary.csv",
        "selected_structure_candidates": output / "co2_s13_selected_structure_candidates.csv",
        "structure_residuals": output / "co2_s13_model_structure_residuals.csv",
        "point_influence_review": output / "co2_s13_point_influence_review.csv",
        "segment_diagnostic": output / "co2_s13_segment_diagnostic.csv",
        "temperature_bias_diagnostic": output / "co2_s13_temperature_bias_diagnostic.csv",
        "point_treatment_recommendations": output / "co2_s13_point_treatment_recommendations.csv",
        "model_capacity_boundary": output / "co2_s13_model_capacity_boundary.csv",
        "selected_low_end_common_mode_patterns": output / "co2_s13_selected_low_end_common_mode_patterns.csv",
        "metadata": output / "co2_s13_model_structure_meta.json",
        "markdown": output / "co2_s13_model_structure_review_zh.md",
    }
    for key in (
        "run_summary",
        "structure_summary",
        "selected_structure_candidates",
        "structure_residuals",
        "point_influence_review",
        "segment_diagnostic",
        "temperature_bias_diagnostic",
        "point_treatment_recommendations",
        "model_capacity_boundary",
        "selected_low_end_common_mode_patterns",
    ):
        _write_csv(outputs[key], tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_model_structure_review",
                "created_at": _now(),
                "inputs": {
                    "fit_points_csv": str(Path(fit_points_csv).resolve()),
                    "fit_point_treatment_plan_csv": (
                        str(Path(fit_point_treatment_plan_csv).resolve())
                        if fit_point_treatment_plan_csv
                        else ""
                    ),
                    "exclude_device_ids": list(exclude_device_ids),
                    "structures": list(structures),
                    "objectives": list(objectives),
                    "zero_offsets_ppm": list(zero_offsets_ppm),
                    "min_relative_target_ppm": min_relative_target_ppm,
                    "low_end_target_ppm": low_end_target_ppm,
                    "low_end_multiplier": low_end_multiplier,
                },
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "uses_s5_output_trim": False,
                    "not_real_acceptance_evidence": True,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_markdown(outputs["markdown"], tables)
    return outputs


def _fmt(value: Any, digits: int = 5) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}g}"


def _write_markdown_zh(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    selected = list(tables.get("selected_structure_candidates") or [])
    treatment = list(tables.get("point_treatment_recommendations") or [])
    segment = list(tables.get("segment_diagnostic") or [])
    lines = [
        "# V1.5 CO2 S1/S3 主模型结构修正评审",
        "",
        "本报告只做离线评审：不打开 COM、不控制气路/水路、不写 SENCO。S5 被排除在本评审之外，不能用输出层线性修正掩盖 S1/S3 主模型残差。",
        "",
        "## 逐台候选结果",
        "",
        "| 设备 ID | 推荐结构 | 目标函数 | 零气假设(ppm) | 最大相对误差(%) | 低端最大相对误差(%) | 建议 |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in selected:
        lines.append(
            "| {device_id} | {structure} | {objective} | {zero} | {max_rel} | {low_rel} | {action} |".format(
                device_id=row.get("device_id", ""),
                structure=row.get("best_structure_id", ""),
                objective=row.get("best_objective_id", ""),
                zero=_fmt(row.get("best_zero_offset_ppm"), 3),
                max_rel=_fmt(row.get("best_max_abs_relative_error_percent"), 4),
                low_rel=_fmt(row.get("best_low_end_max_abs_relative_error_percent"), 4),
                action=row.get("recommended_no_write_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 分段诊断",
            "",
            "| 设备 ID | 分段 | 点数 | 最大绝对误差(ppm) | 最大相对误差(%) |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in segment:
        lines.append(
            "| {device_id} | {segment_id} | {count} | {max_abs} | {max_rel} |".format(
                device_id=row.get("device_id", ""),
                segment_id=row.get("segment_id", ""),
                count=row.get("point_count", ""),
                max_abs=_fmt(row.get("max_abs_error_ppm"), 4),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 4),
            )
        )
    lines.extend(
        [
            "",
            "## 点位处理建议",
            "",
            "只有存在明确物理状态问题的点，才建议降级或剔除；ratio 达到 A 级且露点深干的点，即使残差较大，也优先判为模型、目标值或零气锚定问题。",
            "",
            "| 设备 ID | 点位 | 目标(ppm) | 相对误差(%) | ratio 等级 | 干燥等级 | 建议 |",
            "| --- | --- | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in treatment[:80]:
        lines.append(
            "| {device_id} | {point} | {target} | {rel} | {ratio} | {dry} | {rec} |".format(
                device_id=row.get("device_id", ""),
                point=row.get("point_identity", ""),
                target=_fmt(row.get("target_ppm"), 4),
                rel=_fmt(row.get("relative_error_percent"), 4),
                ratio=row.get("ratio_grade", ""),
                dry=row.get("dryness_grade", ""),
                rec=row.get("recommendation", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 结论边界",
            "",
            "- 本评审不引入压力项；正式开放流通 CO2 主校准仍保持当前大气压条件下的 no-pressure S1/S3 合同。",
            "- 零气点用于 CO2 低端锚定，但其 assigned CO2 ppm 需要按证据或不确定度评审，不能简单等同于 H2O 干气锚点。",
            "- S5 只能在 S1/S3 主模型评审完成后作为输出层修正评审，不能先用于遮蔽主模型残差。",
        ]
    )
    path.write_text("\ufeff" + "\n".join(lines) + "\n", encoding="utf-8")


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    """Write the no-write model-structure review in readable Chinese.

    A clean final definition is kept at EOF so historical mojibake blocks above
    cannot leak into newly exported evidence.
    """

    selected = list(tables.get("selected_structure_candidates") or [])
    capacity = list(tables.get("model_capacity_boundary") or [])
    common = list(tables.get("selected_low_end_common_mode_patterns") or [])
    segment = list(tables.get("segment_diagnostic") or [])
    treatment = list(tables.get("point_treatment_recommendations") or [])

    lines = [
        "# V1.5 CO2 S1/S3 主模型结构修正评审",
        "",
        "本报告只做离线评审：不打开 COM、不控制气路或水路、不写 SENCO。评审边界固定为当前大气压开放流通 CO2 主校准，压力项冻结为 0，S5 输出层线性修正不参与本轮 S1/S3 主模型判断。",
        "",
        "## 物理判断原则",
        "",
        "- CO2 标准气点用于拟合 CO2 主链路；零气只作为 CO2 低端锚点，不能和 H2O 干气锚点混为一类。",
        "- 如果 ratio 已达 A 级、露点处于深干状态，但同一低端点在多台设备上同向偏差，优先怀疑共同目标值、阀路/气瓶状态或 S1/S3 低端模型形状，不应静默删点。",
        "- 如果 S1/S3 可写槽位无法进一步降低低端系统残差，S5 只能作为后续输出层修正评审，不能先用来掩盖主模型问题。",
        "",
        "## 逐台候选结果",
        "",
        "| 设备 ID | 推荐结构 | 目标函数 | 零气假设 ppm | 最大相对误差 % | 低端最大相对误差 % | 建议 |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in selected:
        lines.append(
            "| {device} | {structure} | {objective} | {zero} | {max_rel} | {low_rel} | {action} |".format(
                device=row.get("device_id", ""),
                structure=row.get("best_structure_id", ""),
                objective=row.get("best_objective_id", ""),
                zero=_fmt(row.get("best_zero_offset_ppm"), 4),
                max_rel=_fmt(row.get("best_max_abs_relative_error_percent"), 5),
                low_rel=_fmt(row.get("best_low_end_max_abs_relative_error_percent"), 5),
                action=row.get("recommended_no_write_action", ""),
            )
        )

    lines.extend(
        [
            "",
            "## S1/S3 模型容量边界",
            "",
            "| 设备 ID | 基线最大相对误差 % | 最优最大相对误差 % | 改善百分点 | 低端好物理状态大残差点数 | 温度偏置组数 | 目标偏置组数 | 判断 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in capacity:
        lines.append(
            "| {device} | {base} | {best} | {gain} | {large} | {tbias} | {gbias} | {status} |".format(
                device=row.get("device_id", ""),
                base=_fmt(row.get("baseline_max_abs_relative_error_percent"), 5),
                best=_fmt(row.get("best_max_abs_relative_error_percent"), 5),
                gain=_fmt(row.get("best_vs_baseline_improvement_percent_points"), 5),
                large=row.get("low_end_good_physics_large_residual_count", ""),
                tbias=row.get("low_end_temperature_bias_group_count", ""),
                gbias=row.get("low_end_target_bias_group_count", ""),
                status=row.get("model_capacity_status", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 低端共同模式",
            "",
            "| 点位 | 设备数 | 平均误差 ppm | 正误差数 | 负误差数 | 最大相对误差 % | ratio 等级 | 干燥等级 | 判断 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in common[:40]:
        lines.append(
            "| {point} | {count} | {mean} | {pos} | {neg} | {max_rel} | {ratio} | {dry} | {status} |".format(
                point=row.get("point_identity", ""),
                count=row.get("device_count", ""),
                mean=_fmt(row.get("mean_error_ppm"), 5),
                pos=row.get("positive_error_count", ""),
                neg=row.get("negative_error_count", ""),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 5),
                ratio=row.get("ratio_grade_counts", ""),
                dry=row.get("dryness_grade_counts", ""),
                status=row.get("common_mode_status", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 分段诊断",
            "",
            "| 设备 ID | 分段 | 点数 | 最大绝对误差 ppm | 最大相对误差 % |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in segment:
        lines.append(
            "| {device} | {segment_id} | {count} | {max_abs} | {max_rel} |".format(
                device=row.get("device_id", ""),
                segment_id=row.get("segment_id", ""),
                count=row.get("point_count", ""),
                max_abs=_fmt(row.get("max_abs_error_ppm"), 5),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 5),
            )
        )

    lines.extend(
        [
            "",
            "## 点位处理建议",
            "",
            "下表只列残差较大的点或零气锚点。所有 `auto_exclude` 均保持 false：点位剔除必须有明确坏物理状态证据，不能因为结果不好看就删除。",
            "",
            "| 设备 ID | 点位 | 目标 ppm | 相对误差 % | ratio 等级 | 干燥等级 | 建议 |",
            "| --- | --- | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in treatment[:100]:
        lines.append(
            "| {device} | {point} | {target} | {rel} | {ratio} | {dry} | {rec} |".format(
                device=row.get("device_id", ""),
                point=row.get("point_identity", ""),
                target=_fmt(row.get("target_ppm"), 5),
                rel=_fmt(row.get("relative_error_percent"), 5),
                ratio=row.get("ratio_grade", ""),
                dry=row.get("dryness_grade", ""),
                rec=row.get("recommendation", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 结论边界",
            "",
            "- 本报告不会批准写入，只给出 S1/S3 主模型是否还有可解释改进空间。",
            "- 若最优 S1/S3 仍在 A 级 ratio、深干露点的低端点出现多设备同向残差，优先追共同点状态和模型容量边界。",
            "- S5/S6 属于输出层修正，应在 S1/S3、S2/S4 主链路评审完成后单独计算和复验。",
        ]
    )
    path.write_text("\ufeff" + "\n".join(lines) + "\n", encoding="utf-8")


def _write_markdown_final_zh(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    selected = list(tables.get("selected_structure_candidates") or [])
    capacity = list(tables.get("model_capacity_boundary") or [])
    common = list(tables.get("selected_low_end_common_mode_patterns") or [])
    segment = list(tables.get("segment_diagnostic") or [])
    treatment = list(tables.get("point_treatment_recommendations") or [])

    lines = [
        "# V1.5 CO2 S1/S3 主模型结构修正评审",
        "",
        "本报告只做离线评审：不打开 COM、不控制气路或水路、不写 SENCO。评审边界固定为当前大气压开放流通 CO2 主校准，压力项冻结为 0，S5 输出层线性修正不参与本轮 S1/S3 主模型判断。",
        "",
        "## 物理判断原则",
        "",
        "- CO2 标准气点用于拟合 CO2 主链路；零气只作为 CO2 低端锚点，不能和 H2O 干气锚点混为一类。",
        "- 如果 ratio 已达 A 级、露点处于深干状态，但同一低端点在多台设备上同向偏差，优先怀疑共同目标值、阀路或 S1/S3 低端模型形状，不应静默删点。",
        "- 如果 S1/S3 可写槽位无法进一步降低低端系统残差，S5 只能作为后续输出层修正评审，不能先用来掩盖主模型问题。",
        "",
        "## 逐台候选结果",
        "",
        "| 设备 ID | 推荐结构 | 目标函数 | 零气假设 ppm | 最大相对误差 % | 低端最大相对误差 % | 建议 |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in selected:
        lines.append(
            "| {device} | {structure} | {objective} | {zero} | {max_rel} | {low_rel} | {action} |".format(
                device=row.get("device_id", ""),
                structure=row.get("best_structure_id", ""),
                objective=row.get("best_objective_id", ""),
                zero=_fmt(row.get("best_zero_offset_ppm"), 4),
                max_rel=_fmt(row.get("best_max_abs_relative_error_percent"), 5),
                low_rel=_fmt(row.get("best_low_end_max_abs_relative_error_percent"), 5),
                action=row.get("recommended_no_write_action", ""),
            )
        )

    lines.extend(
        [
            "",
            "## S1/S3 模型容量边界",
            "",
            "| 设备 ID | 基线最大相对误差 % | 最优最大相对误差 % | 改善百分点 | 低端好物理状态大残差点数 | 温度偏置组数 | 目标偏置组数 | 判断 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in capacity:
        lines.append(
            "| {device} | {base} | {best} | {gain} | {large} | {tbias} | {gbias} | {status} |".format(
                device=row.get("device_id", ""),
                base=_fmt(row.get("baseline_max_abs_relative_error_percent"), 5),
                best=_fmt(row.get("best_max_abs_relative_error_percent"), 5),
                gain=_fmt(row.get("best_vs_baseline_improvement_percent_points"), 5),
                large=row.get("low_end_good_physics_large_residual_count", ""),
                tbias=row.get("low_end_temperature_bias_group_count", ""),
                gbias=row.get("low_end_target_bias_group_count", ""),
                status=row.get("model_capacity_status", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 低端共同模式",
            "",
            "| 点位 | 设备数 | 平均误差 ppm | 正误差数 | 负误差数 | 最大相对误差 % | ratio 等级 | 干燥等级 | 判断 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in common[:40]:
        lines.append(
            "| {point} | {count} | {mean} | {pos} | {neg} | {max_rel} | {ratio} | {dry} | {status} |".format(
                point=row.get("point_identity", ""),
                count=row.get("device_count", ""),
                mean=_fmt(row.get("mean_error_ppm"), 5),
                pos=row.get("positive_error_count", ""),
                neg=row.get("negative_error_count", ""),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 5),
                ratio=row.get("ratio_grade_counts", ""),
                dry=row.get("dryness_grade_counts", ""),
                status=row.get("common_mode_status", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 分段诊断",
            "",
            "| 设备 ID | 分段 | 点数 | 最大绝对误差 ppm | 最大相对误差 % |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in segment:
        lines.append(
            "| {device} | {segment_id} | {count} | {max_abs} | {max_rel} |".format(
                device=row.get("device_id", ""),
                segment_id=row.get("segment_id", ""),
                count=row.get("point_count", ""),
                max_abs=_fmt(row.get("max_abs_error_ppm"), 5),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 5),
            )
        )

    lines.extend(
        [
            "",
            "## 点位处理建议",
            "",
            "下表只列残差较大的点或零气锚点。所有 `auto_exclude` 均保持 false：点位剔除必须有明确坏物理状态证据，不能因为结果不好看就删除。",
            "",
            "| 设备 ID | 点位 | 目标 ppm | 相对误差 % | ratio 等级 | 干燥等级 | 建议 |",
            "| --- | --- | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in treatment[:100]:
        lines.append(
            "| {device} | {point} | {target} | {rel} | {ratio} | {dry} | {rec} |".format(
                device=row.get("device_id", ""),
                point=row.get("point_identity", ""),
                target=_fmt(row.get("target_ppm"), 5),
                rel=_fmt(row.get("relative_error_percent"), 5),
                ratio=row.get("ratio_grade", ""),
                dry=row.get("dryness_grade", ""),
                rec=row.get("recommendation", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 结论边界",
            "",
            "- 本报告不会批准写入，只给出 S1/S3 主模型是否还有可解释改进空间。",
            "- 若最优 S1/S3 仍在 A 级 ratio、深干露点的低端点出现多设备同向残差，优先追共同点状态和模型容量边界。",
            "- S5/S6 属于输出层修正，应在 S1/S3、S2/S4 主链路评审完成后单独计算和复验。",
        ]
    )
    path.write_text("\ufeff" + "\n".join(lines) + "\n", encoding="utf-8")


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    """Write the human review in readable Chinese.

    This function intentionally shadows the older mojibake writer above while
    keeping its public output filename stable for traceability.
    """

    selected = list(tables.get("selected_structure_candidates") or [])
    capacity = list(tables.get("model_capacity_boundary") or [])
    common = list(tables.get("selected_low_end_common_mode_patterns") or [])
    segment = list(tables.get("segment_diagnostic") or [])
    treatment = list(tables.get("point_treatment_recommendations") or [])

    lines = [
        "# V1.5 CO2 S1/S3 主模型结构修正评审",
        "",
        "本报告只做离线评审：不打开 COM、不控制气路/水路、不写 SENCO。评审边界固定为当前大气压开放流通主校准，压力项冻结为 0，S5 输出层线性修正不参与本轮 S1/S3 主模型判断。",
        "",
        "## 物理判断原则",
        "",
        "- CO2 标准气点用于拟合 CO2 主链路；零气只作为 CO2 低端锚点，不能和 H2O 干气锚点混为一类。",
        "- 如果 ratio 已达 A 级、露点处于深干状态，但同一低端点在多台设备上同向偏差，优先怀疑共同目标值、阀路/气瓶状态或 S1/S3 低端模型形状，不应静默删点。",
        "- 如果 S1/S3 可写槽位无法进一步降低低端系统残差，S5 只能作为后续输出层修正评审，不能先用来掩盖主模型问题。",
        "",
        "## 逐台候选结果",
        "",
        "| 设备 ID | 推荐结构 | 目标函数 | 零气假设 ppm | 最大相对误差 % | 低端最大相对误差 % | 建议 |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in selected:
        lines.append(
            "| {device} | {structure} | {objective} | {zero} | {max_rel} | {low_rel} | {action} |".format(
                device=row.get("device_id", ""),
                structure=row.get("best_structure_id", ""),
                objective=row.get("best_objective_id", ""),
                zero=_fmt(row.get("best_zero_offset_ppm"), 4),
                max_rel=_fmt(row.get("best_max_abs_relative_error_percent"), 5),
                low_rel=_fmt(row.get("best_low_end_max_abs_relative_error_percent"), 5),
                action=row.get("recommended_no_write_action", ""),
            )
        )

    lines.extend(
        [
            "",
            "## S1/S3 模型容量边界",
            "",
            "| 设备 ID | 基线最大相对误差 % | 最优最大相对误差 % | 改善百分点 | 低端好物理状态大残差点数 | 温度偏置组数 | 目标偏置组数 | 判断 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in capacity:
        lines.append(
            "| {device} | {base} | {best} | {gain} | {large} | {tbias} | {gbias} | {status} |".format(
                device=row.get("device_id", ""),
                base=_fmt(row.get("baseline_max_abs_relative_error_percent"), 5),
                best=_fmt(row.get("best_max_abs_relative_error_percent"), 5),
                gain=_fmt(row.get("best_vs_baseline_improvement_percent_points"), 5),
                large=row.get("low_end_good_physics_large_residual_count", ""),
                tbias=row.get("low_end_temperature_bias_group_count", ""),
                gbias=row.get("low_end_target_bias_group_count", ""),
                status=row.get("model_capacity_status", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 低端共同模式",
            "",
            "| 点位 | 设备数 | 平均误差 ppm | 正误差数 | 负误差数 | 最大相对误差 % | ratio 等级 | 干燥等级 | 判断 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in common[:40]:
        lines.append(
            "| {point} | {count} | {mean} | {pos} | {neg} | {max_rel} | {ratio} | {dry} | {status} |".format(
                point=row.get("point_identity", ""),
                count=row.get("device_count", ""),
                mean=_fmt(row.get("mean_error_ppm"), 5),
                pos=row.get("positive_error_count", ""),
                neg=row.get("negative_error_count", ""),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 5),
                ratio=row.get("ratio_grade_counts", ""),
                dry=row.get("dryness_grade_counts", ""),
                status=row.get("common_mode_status", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 分段诊断",
            "",
            "| 设备 ID | 分段 | 点数 | 最大绝对误差 ppm | 最大相对误差 % |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in segment:
        lines.append(
            "| {device} | {segment_id} | {count} | {max_abs} | {max_rel} |".format(
                device=row.get("device_id", ""),
                segment_id=row.get("segment_id", ""),
                count=row.get("point_count", ""),
                max_abs=_fmt(row.get("max_abs_error_ppm"), 5),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 5),
            )
        )

    lines.extend(
        [
            "",
            "## 点位处理建议",
            "",
            "下表只列残差较大的点或零气锚点。所有 `auto_exclude` 均保持 false：点位剔除必须有明确坏物理状态证据，不能因为结果不好看就删除。",
            "",
            "| 设备 ID | 点位 | 目标 ppm | 相对误差 % | ratio 等级 | 干燥等级 | 建议 |",
            "| --- | --- | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in treatment[:100]:
        lines.append(
            "| {device} | {point} | {target} | {rel} | {ratio} | {dry} | {rec} |".format(
                device=row.get("device_id", ""),
                point=row.get("point_identity", ""),
                target=_fmt(row.get("target_ppm"), 5),
                rel=_fmt(row.get("relative_error_percent"), 5),
                ratio=row.get("ratio_grade", ""),
                dry=row.get("dryness_grade", ""),
                rec=row.get("recommendation", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 结论边界",
            "",
            "- 本报告不会批准写入，只给出 S1/S3 主模型是否还有可解释改进空间。",
            "- 若最优 S1/S3 仍在 A 级 ratio、深干露点的低端点出现多设备同向残差，优先追共同点状态和模型容量边界。",
            "- S5/S6 属于输出层修正，应在 S1/S3、S2/S4 主链路评审完成后单独计算和复验。",
        ]
    )
    path.write_text("\ufeff" + "\n".join(lines) + "\n", encoding="utf-8")


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    _write_markdown_final_zh(path, tables)
    return
    selected = list(tables.get("selected_structure_candidates") or [])
    treatment = list(tables.get("point_treatment_recommendations") or [])
    segment = list(tables.get("segment_diagnostic") or [])
    lines = [
        "# V1.5 CO2 S1/S3 主模型结构修正评审",
        "",
        "本报告只做离线评审：不打开 COM、不控制气路/水路、不写 SENCO。S5 被排除在本评审之外，不能用输出层线性修正掩盖 S1/S3 主模型残差。",
        "",
        "## 逐台候选结构",
        "",
        "| 设备ID | 推荐结构 | 目标函数 | 零气假设(ppm) | 最大相对误差(%) | 低端最大相对误差(%) | 建议 |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in selected:
        lines.append(
            "| {device_id} | {structure} | {objective} | {zero} | {max_rel} | {low_rel} | {action} |".format(
                device_id=row.get("device_id", ""),
                structure=row.get("best_structure_id", ""),
                objective=row.get("best_objective_id", ""),
                zero=_fmt(row.get("best_zero_offset_ppm"), 3),
                max_rel=_fmt(row.get("best_max_abs_relative_error_percent"), 4),
                low_rel=_fmt(row.get("best_low_end_max_abs_relative_error_percent"), 4),
                action=row.get("recommended_no_write_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 分段诊断",
            "",
            "| 设备ID | 分段 | 点数 | 最大绝对误差(ppm) | 最大相对误差(%) |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in segment:
        lines.append(
            "| {device_id} | {segment_id} | {count} | {max_abs} | {max_rel} |".format(
                device_id=row.get("device_id", ""),
                segment_id=row.get("segment_id", ""),
                count=row.get("point_count", ""),
                max_abs=_fmt(row.get("max_abs_error_ppm"), 4),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 4),
            )
        )
    lines.extend(
        [
            "",
            "## 点位处理建议",
            "",
            "只有存在明确物理状态问题的点，才建议降级或剔除；ratio A 且露点深干的点即便残差大，也优先判为模型/目标/零气锚定问题。",
            "",
            "| 设备ID | 点位 | 目标(ppm) | 相对误差(%) | ratio等级 | 干燥等级 | 建议 |",
            "| --- | --- | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in treatment[:80]:
        lines.append(
            "| {device_id} | {point} | {target} | {rel} | {ratio} | {dry} | {rec} |".format(
                device_id=row.get("device_id", ""),
                point=row.get("point_identity", ""),
                target=_fmt(row.get("target_ppm"), 4),
                rel=_fmt(row.get("relative_error_percent"), 4),
                ratio=row.get("ratio_grade", ""),
                dry=row.get("dryness_grade", ""),
                rec=row.get("recommendation", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 结论边界",
            "",
            "- 本评审不引入压力项；正式开放流通 CO2 主校准仍保持当前大气压条件下的 no-pressure S1/S3 合同。",
            "- 零气点用于 CO2 低端锚定，但其 assigned CO2 ppm 需要按证据或不确定度评审，不能简单等同于 H2O 干气锚点。",
            "- S5 只能在 S1/S3 主模型评审完成后作为输出层修正评审，不能先用于遮蔽主模型残差。",
        ]
    )
    path.write_text("\ufeff" + "\n".join(lines) + "\n", encoding="utf-8")
