"""Offline CO2 SENCO1/SENCO3 multi-strategy fit review.

This module compares several S1/S3 fitting strategies on already-recorded
V1.5 open-flow CO2 evidence. It is deliberately offline and no-write: it never
opens COM ports, controls gas/water routes, or writes coefficients.
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

from .co2_s13_model_structure_review import build_co2_s13_model_structure_review
from .co2_senco5_linear_trim_review import _fit_quantized_command_trim


DEFAULT_ZERO_OFFSETS_PPM = (0.0, 2.0, 5.0, 8.0, 10.0)
DEFAULT_LOW_END_TARGET_PPM = 300.0
DEFAULT_MIN_RELATIVE_TARGET_PPM = 50.0
DEFAULT_TOP_N = 5
DEFAULT_S5_ACCEPTANCE_PERCENT = 1.0
DEFAULT_S5_C0_DECIMALS = 3
DEFAULT_S5_C1_DECIMALS = 3
DEFAULT_S5_C1_MIN = 0.90
DEFAULT_S5_C1_MAX = 1.10


@dataclass(frozen=True)
class StrategyPass:
    profile_id: str
    description: str
    structures: tuple[str, ...]
    objectives: tuple[str, ...]
    zero_offsets_ppm: tuple[float, ...]
    low_end_multiplier: float = 3.0


DEFAULT_STRATEGY_PASSES: tuple[StrategyPass, ...] = (
    StrategyPass(
        profile_id="baseline_full_temp_absolute_zero0",
        description="当前主合同基线：R 多项式 + T/T2/RT，零气按 0 ppm，绝对误差最小二乘。",
        structures=("core_plus_full_temp",),
        objectives=("absolute_lstsq",),
        zero_offsets_ppm=(0.0,),
        low_end_multiplier=3.0,
    ),
    StrategyPass(
        profile_id="full_temp_objective_zero_sweep",
        description="主合同结构不变，比较绝对、相对、低端优先、鲁棒相对目标，并扫描零气估算。",
        structures=("core_plus_full_temp",),
        objectives=(
            "absolute_lstsq",
            "relative_weighted_lstsq",
            "low_end_priority_lstsq",
            "relative_irls_lstsq",
        ),
        zero_offsets_ppm=DEFAULT_ZERO_OFFSETS_PPM,
        low_end_multiplier=3.0,
    ),
    StrategyPass(
        profile_id="low_end_priority_x5",
        description="主合同结构不变，增强低端权重，检查 100-300 ppm 相对误差能否降低。",
        structures=("core_plus_full_temp",),
        objectives=("low_end_priority_lstsq",),
        zero_offsets_ppm=DEFAULT_ZERO_OFFSETS_PPM,
        low_end_multiplier=5.0,
    ),
    StrategyPass(
        profile_id="low_end_priority_x8",
        description="主合同结构不变，强低端权重诊断；若高端明显恶化，不宜写入。",
        structures=("core_plus_full_temp",),
        objectives=("low_end_priority_lstsq",),
        zero_offsets_ppm=DEFAULT_ZERO_OFFSETS_PPM,
        low_end_multiplier=8.0,
    ),
    StrategyPass(
        profile_id="linear_temperature_guard",
        description="去掉 T2，仅保留 T 和 R*T，检查全温度模型是否被二次温度项过度牵引。",
        structures=("core_plus_linear_temp",),
        objectives=("absolute_lstsq", "relative_weighted_lstsq", "relative_irls_lstsq"),
        zero_offsets_ppm=DEFAULT_ZERO_OFFSETS_PPM,
        low_end_multiplier=3.0,
    ),
    StrategyPass(
        profile_id="ratio_only_diagnostic",
        description="仅 R 多项式诊断，不作为多温度正式写入候选，用于判断温度项是否必要。",
        structures=("core_ratio_only_diagnostic",),
        objectives=("absolute_lstsq", "relative_weighted_lstsq"),
        zero_offsets_ppm=(0.0, 5.0),
        low_end_multiplier=3.0,
    ),
)


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


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _bool_text(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _fmt(value: Any, digits: int = 3) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def _score(row: Mapping[str, Any]) -> tuple[float, float, float, float, float]:
    max_rel = _safe_float(row.get("max_abs_relative_error_percent"))
    low_rel = _safe_float(row.get("low_end_max_abs_relative_error_percent"))
    rmse = _safe_float(row.get("rmse_ppm"))
    zero_abs = _safe_float(row.get("zero_anchor_max_abs_error_ppm"))
    condition = _safe_float(row.get("condition_number_scaled"))
    return (
        float(max_rel) if max_rel is not None else float("inf"),
        float(low_rel) if low_rel is not None else float("inf"),
        float(rmse) if rmse is not None else float("inf"),
        float(zero_abs) if zero_abs is not None else float("inf"),
        float(condition) if condition is not None else float("inf"),
    )


def _strategy_id(row: Mapping[str, Any]) -> str:
    return (
        f"{row.get('strategy_profile_id')}|{row.get('structure_id')}|{row.get('objective_id')}"
        f"|zero={float(_safe_float(row.get('zero_offset_ppm')) or 0.0):g}"
        f"|lowx={float(_safe_float(row.get('low_end_multiplier')) or 0.0):g}"
    )


def _is_diagnostic(row: Mapping[str, Any]) -> bool:
    return _bool_text(row.get("diagnostic_only")) or str(row.get("structure_family") or "") == "diagnostic"


def _candidate_action(
    row: Mapping[str, Any],
    baseline: Mapping[str, Any] | None,
    *,
    material_improvement_fraction: float = 0.15,
) -> str:
    if _is_diagnostic(row):
        return "diagnostic_only_do_not_write"
    best = _safe_float(row.get("max_abs_relative_error_percent"))
    baseline_value = _safe_float(baseline.get("max_abs_relative_error_percent")) if baseline else None
    zero_offset = _safe_float(row.get("zero_offset_ppm")) or 0.0
    if best is not None and best <= 1.5:
        if abs(zero_offset) > 1.0e-12:
            return "s1s3_candidate_close_but_requires_zero_gas_value_review"
        return "s1s3_candidate_for_write_review_no_s5"
    if baseline_value is not None and best is not None and baseline_value > 0.0:
        improvement = (baseline_value - best) / baseline_value
        if improvement >= float(material_improvement_fraction):
            return "s1s3_strategy_improves_but_not_ready_for_write"
    if abs(zero_offset) > 1.0e-12:
        return "zero_offset_changes_low_end_review_traceability"
    return "keep_as_review_candidate_no_write"


def _target_segment(row: Mapping[str, Any], low_end_target_ppm: float) -> str:
    target = _safe_float(row.get("target_ppm"))
    identity = str(row.get("point_identity") or "").lower()
    marker = str(row.get("zero_anchor_class") or "").lower()
    if target is None:
        return "unknown"
    point_label = identity.rsplit("_", 1)[-1]
    zero_markers = {
        "estimated_zero_anchor",
        "zero_anchor",
        "co2_zero_anchor",
        "co2_zero_gas_anchor",
    }
    if marker in zero_markers or marker.startswith("zero_") or point_label == "0ppm":
        return "zero_anchor"
    if float(target) <= float(low_end_target_ppm):
        return "low_nonzero"
    if float(target) <= 600.0:
        return "mid_400_600"
    return "high_700_plus"


def _temperature_group(row: Mapping[str, Any]) -> str:
    identity = str(row.get("point_identity") or "")
    if identity.startswith("T") and "_" in identity:
        return identity.split("_", 1)[0]
    temp = _safe_float(row.get("temperature_c"))
    return f"T{temp:g}" if temp is not None else "T_unknown"


def _segment_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    low_end_target_ppm: float,
) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        device = _device_id(row.get("device_id"))
        strategy = str(row.get("strategy_id") or "")
        if not device or not strategy:
            continue
        grouped[(device, strategy, _target_segment(row, low_end_target_ppm))].append(row)

    out: List[Dict[str, Any]] = []
    for (device, strategy, segment), items in sorted(grouped.items()):
        errors: List[float] = []
        rels: List[float] = []
        same_sign_values: List[float] = []
        worst_point = ""
        worst_abs_rel = -1.0
        for row in items:
            error = _safe_float(row.get("error_ppm"))
            rel = _safe_float(row.get("relative_error_percent"))
            if error is not None:
                errors.append(float(error))
                same_sign_values.append(float(error))
            if rel is not None:
                abs_rel = abs(float(rel))
                rels.append(abs_rel)
                if abs_rel > worst_abs_rel:
                    worst_abs_rel = abs_rel
                    worst_point = str(row.get("point_identity") or "")
        positive = sum(1 for value in same_sign_values if value > 0)
        negative = sum(1 for value in same_sign_values if value < 0)
        same_sign_fraction = (
            max(positive, negative) / len(same_sign_values) if same_sign_values else ""
        )
        out.append(
            {
                "device_id": device,
                "strategy_id": strategy,
                "segment_id": segment,
                "point_count": len(items),
                "max_abs_error_ppm": max(abs(value) for value in errors) if errors else "",
                "mean_error_ppm": (sum(errors) / len(errors)) if errors else "",
                "max_abs_relative_error_percent": max(rels) if rels else "",
                "worst_point_identity": worst_point,
                "same_sign_fraction": same_sign_fraction,
            }
        )
    return out


def _temperature_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        device = _device_id(row.get("device_id"))
        strategy = str(row.get("strategy_id") or "")
        if not device or not strategy:
            continue
        grouped[(device, strategy, _temperature_group(row))].append(row)

    out: List[Dict[str, Any]] = []
    for (device, strategy, temperature), items in sorted(grouped.items()):
        rels: List[float] = []
        errors: List[float] = []
        for row in items:
            if "zero" in _target_segment(row, DEFAULT_LOW_END_TARGET_PPM):
                continue
            error = _safe_float(row.get("error_ppm"))
            rel = _safe_float(row.get("relative_error_percent"))
            if error is not None:
                errors.append(float(error))
            if rel is not None:
                rels.append(abs(float(rel)))
        out.append(
            {
                "device_id": device,
                "strategy_id": strategy,
                "temperature_group": temperature,
                "nonzero_point_count": len(rels),
                "max_abs_relative_error_percent": max(rels) if rels else "",
                "mean_error_ppm": sum(errors) / len(errors) if errors else "",
            }
        )
    return out


def _annotate_summary_row(row: Mapping[str, Any], profile: StrategyPass) -> Dict[str, Any]:
    out = dict(row)
    out["strategy_profile_id"] = profile.profile_id
    out["strategy_profile_description"] = profile.description
    out["low_end_multiplier"] = float(profile.low_end_multiplier)
    out["strategy_id"] = _strategy_id(out)
    out["uses_pressure_terms"] = False
    out["uses_s5_output_trim"] = False
    out["writes_coefficients"] = False
    out["auto_write_allowed"] = False
    return out


def _annotate_residual_row(row: Mapping[str, Any], profile: StrategyPass, summary: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    out["strategy_profile_id"] = profile.profile_id
    out["strategy_profile_description"] = profile.description
    out["low_end_multiplier"] = float(profile.low_end_multiplier)
    out["strategy_id"] = _strategy_id(summary)
    out["uses_pressure_terms"] = False
    out["uses_s5_output_trim"] = False
    out["writes_coefficients"] = False
    return out


def _baseline_by_device(summary_rows: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    baseline: Dict[str, Mapping[str, Any]] = {}
    for row in summary_rows:
        if row.get("strategy_profile_id") != "baseline_full_temp_absolute_zero0":
            continue
        device = _device_id(row.get("device_id"))
        if device:
            baseline[device] = row
    return baseline


def _top_candidates(
    summary_rows: Sequence[Mapping[str, Any]],
    *,
    top_n: int,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    by_device: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    baseline_lookup = _baseline_by_device(summary_rows)
    for row in summary_rows:
        device = _device_id(row.get("device_id"))
        if not device:
            continue
        by_device[device].append(row)

    top_rows: List[Dict[str, Any]] = []
    best_rows: List[Dict[str, Any]] = []
    for device, rows in sorted(by_device.items()):
        non_diagnostic = [row for row in rows if not _is_diagnostic(row)]
        candidates = sorted(non_diagnostic or rows, key=_score)
        baseline = baseline_lookup.get(device)
        for rank, row in enumerate(candidates[: int(top_n)], start=1):
            best_value = _safe_float(row.get("max_abs_relative_error_percent"))
            baseline_value = _safe_float(baseline.get("max_abs_relative_error_percent")) if baseline else None
            improvement_points = (
                float(baseline_value) - float(best_value)
                if baseline_value is not None and best_value is not None
                else ""
            )
            improvement_fraction = (
                improvement_points / float(baseline_value)
                if isinstance(improvement_points, float) and baseline_value not in (None, 0.0)
                else ""
            )
            annotated = dict(row)
            annotated.update(
                {
                    "rank": rank,
                    "baseline_max_abs_relative_error_percent": (
                        baseline_value if baseline_value is not None else ""
                    ),
                    "improvement_percent_points": improvement_points,
                    "improvement_fraction": improvement_fraction,
                    "requires_zero_gas_assigned_value_review": abs(
                        float(_safe_float(row.get("zero_offset_ppm")) or 0.0)
                    )
                    > 1.0e-12,
                    "recommended_no_write_action": _candidate_action(row, baseline),
                }
            )
            top_rows.append(annotated)
            if rank == 1:
                best_rows.append(annotated)
    return top_rows, best_rows


def _best_residuals(
    residual_rows: Sequence[Mapping[str, Any]],
    best_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    best_keys = {
        (
            _device_id(row.get("device_id")),
            str(row.get("strategy_profile_id") or ""),
            str(row.get("structure_id") or ""),
            str(row.get("objective_id") or ""),
            float(_safe_float(row.get("zero_offset_ppm")) or 0.0),
            float(_safe_float(row.get("low_end_multiplier")) or 0.0),
        ): row
        for row in best_rows
    }
    out: List[Dict[str, Any]] = []
    for row in residual_rows:
        key = (
            _device_id(row.get("device_id")),
            str(row.get("strategy_profile_id") or ""),
            str(row.get("structure_id") or ""),
            str(row.get("objective_id") or ""),
            float(_safe_float(row.get("zero_offset_ppm")) or 0.0),
            float(_safe_float(row.get("low_end_multiplier")) or 0.0),
        )
        if key in best_keys:
            out.append(dict(row))
    return out


def _s5_trim_input_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_relative_target_ppm: float,
) -> List[Dict[str, float]]:
    trim_rows: List[Dict[str, float]] = []
    for row in rows:
        target = _safe_float(row.get("target_ppm"))
        measured = _safe_float(row.get("prediction_ppm"))
        if target is None or measured is None:
            continue
        if abs(float(target)) < float(min_relative_target_ppm):
            continue
        trim_rows.append({"_target": float(target), "_measured": float(measured)})
    return trim_rows


def _s5_corrected_prediction(row: Mapping[str, Any], *, c0: float, c1: float) -> Optional[float]:
    prediction = _safe_float(row.get("prediction_ppm"))
    if prediction is None:
        return None
    return float(prediction) * float(c1) + float(c0)


def _s5_metric_row(
    rows: Sequence[Mapping[str, Any]],
    *,
    c0: float,
    c1: float,
    min_relative_target_ppm: float,
) -> Dict[str, Any]:
    errors: List[float] = []
    rels: List[float] = []
    low_rels: List[float] = []
    zero_abs: List[float] = []
    worst_point = ""
    worst_rel = -1.0
    for row in rows:
        target = _safe_float(row.get("target_ppm"))
        corrected = _s5_corrected_prediction(row, c0=c0, c1=c1)
        if target is None or corrected is None:
            continue
        error = float(corrected) - float(target)
        errors.append(error)
        if abs(float(target)) >= float(min_relative_target_ppm):
            rel = error / float(target) * 100.0
            abs_rel = abs(rel)
            rels.append(abs_rel)
            if abs(float(target)) <= DEFAULT_LOW_END_TARGET_PPM:
                low_rels.append(abs_rel)
            if abs_rel > worst_rel:
                worst_rel = abs_rel
                worst_point = str(row.get("point_identity") or "")
        else:
            zero_abs.append(abs(error))
    return {
        "s5_point_count": len(errors),
        "s5_relative_point_count": len(rels),
        "s5_max_abs_relative_error_percent": max(rels) if rels else "",
        "s5_low_end_max_abs_relative_error_percent": max(low_rels) if low_rels else "",
        "s5_max_abs_error_ppm": max(abs(value) for value in errors) if errors else "",
        "s5_rmse_ppm": math.sqrt(sum(value * value for value in errors) / len(errors)) if errors else "",
        "s5_zero_anchor_max_abs_error_ppm": max(zero_abs) if zero_abs else "",
        "s5_worst_point_identity": worst_point,
    }


def _s5_review_action(row: Mapping[str, Any], *, acceptance_percent: float) -> str:
    if _is_diagnostic(row):
        return "diagnostic_only_do_not_write"
    status = str(row.get("s5_status") or "")
    if status != "reviewable_no_write":
        return status or "s5_blocked"
    max_rel = _safe_float(row.get("s5_max_abs_relative_error_percent"))
    if max_rel is not None and float(max_rel) <= float(acceptance_percent):
        return "s1s3_plus_s5_candidate_for_write_review_no_write"
    return "s1s3_plus_s5_improves_but_not_within_acceptance"


def _s5_trim_summary_rows(
    summary_rows: Sequence[Mapping[str, Any]],
    residual_rows: Sequence[Mapping[str, Any]],
    *,
    min_relative_target_ppm: float,
    acceptance_percent: float,
    c0_decimals: int,
    c1_decimals: int,
    c1_min: float,
    c1_max: float,
) -> List[Dict[str, Any]]:
    residual_by_strategy: Dict[tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in residual_rows:
        residual_by_strategy[(_device_id(row.get("device_id")), str(row.get("strategy_id") or ""))].append(row)

    out: List[Dict[str, Any]] = []
    for row in summary_rows:
        device = _device_id(row.get("device_id"))
        strategy_id = str(row.get("strategy_id") or "")
        residuals = residual_by_strategy.get((device, strategy_id), [])
        trim_rows = _s5_trim_input_rows(residuals, min_relative_target_ppm=float(min_relative_target_ppm))
        annotated = dict(row)
        annotated.update(
            {
                "s5_review_scope": "final_output_affine_layer_no_write",
                "s5_acceptance_percent": float(acceptance_percent),
                "s5_input_point_count": len(trim_rows),
                "s5_c0_decimals": int(c0_decimals),
                "s5_c1_decimals": int(c1_decimals),
                "uses_s5_output_trim": True,
                "writes_coefficients": False,
                "auto_write_allowed": False,
            }
        )
        if len(trim_rows) < 2:
            annotated.update(
                {
                    "s5_status": "blocked_relative_points_lt_2",
                    "s5_C0": "",
                    "s5_C1": "",
                    "s5_command_preview": "",
                    "s5_recommended_no_write_action": "blocked_relative_points_lt_2",
                }
            )
            out.append(annotated)
            continue
        c0, c1, fit_max_pct, fit_max_ppm, fit_rmse = _fit_quantized_command_trim(
            trim_rows,
            c0_decimals=int(c0_decimals),
            c1_decimals=int(c1_decimals),
            c1_min=float(c1_min),
            c1_max=float(c1_max),
        )
        metrics = _s5_metric_row(
            residuals,
            c0=float(c0),
            c1=float(c1),
            min_relative_target_ppm=float(min_relative_target_ppm),
        )
        annotated.update(
            {
                "s5_status": "reviewable_no_write",
                "s5_C0": float(c0),
                "s5_C1": float(c1),
                "s5_command_preview": f"SENCO5,YGAS,FFF,{float(c0):.{int(c0_decimals)}f},{float(c1):.{int(c1_decimals)}f}",
                "s5_relative_fit_max_abs_error_percent": fit_max_pct,
                "s5_relative_fit_max_abs_error_ppm": fit_max_ppm,
                "s5_relative_fit_rmse_ppm": fit_rmse,
                **metrics,
                "s5_recommended_no_write_action": "",
            }
        )
        annotated["s5_recommended_no_write_action"] = _s5_review_action(
            annotated,
            acceptance_percent=float(acceptance_percent),
        )
        out.append(annotated)
    return out


def _s5_score(row: Mapping[str, Any]) -> tuple[float, float, float, float, float]:
    max_rel = _safe_float(row.get("s5_max_abs_relative_error_percent"))
    low_rel = _safe_float(row.get("s5_low_end_max_abs_relative_error_percent"))
    rmse = _safe_float(row.get("s5_rmse_ppm"))
    zero_abs = _safe_float(row.get("s5_zero_anchor_max_abs_error_ppm"))
    base_rel = _safe_float(row.get("max_abs_relative_error_percent"))
    return (
        float(max_rel) if max_rel is not None else float("inf"),
        float(low_rel) if low_rel is not None else float("inf"),
        float(rmse) if rmse is not None else float("inf"),
        float(zero_abs) if zero_abs is not None else float("inf"),
        float(base_rel) if base_rel is not None else float("inf"),
    )


def _top_s5_candidates(
    s5_rows: Sequence[Mapping[str, Any]],
    *,
    top_n: int,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    by_device: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in s5_rows:
        device = _device_id(row.get("device_id"))
        if device:
            by_device[device].append(row)

    top_rows: List[Dict[str, Any]] = []
    best_rows: List[Dict[str, Any]] = []
    for device, rows in sorted(by_device.items()):
        non_diagnostic = [row for row in rows if not _is_diagnostic(row)]
        candidates = sorted(non_diagnostic or rows, key=_s5_score)
        for rank, row in enumerate(candidates[: int(top_n)], start=1):
            annotated = dict(row)
            annotated["s5_rank"] = rank
            top_rows.append(annotated)
            if rank == 1:
                best_rows.append(annotated)
    return top_rows, best_rows


def _s5_best_residuals(
    residual_rows: Sequence[Mapping[str, Any]],
    best_s5_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    best_keys = {
        (_device_id(row.get("device_id")), str(row.get("strategy_id") or "")): row
        for row in best_s5_rows
    }
    out: List[Dict[str, Any]] = []
    for row in residual_rows:
        key = (_device_id(row.get("device_id")), str(row.get("strategy_id") or ""))
        candidate = best_keys.get(key)
        if not candidate:
            continue
        c0 = _safe_float(candidate.get("s5_C0")) or 0.0
        c1 = _safe_float(candidate.get("s5_C1")) or 1.0
        corrected = _s5_corrected_prediction(row, c0=float(c0), c1=float(c1))
        target = _safe_float(row.get("target_ppm"))
        item = dict(row)
        item["s1s3_error_ppm_before_s5"] = item.get("error_ppm", "")
        item["s1s3_relative_error_percent_before_s5"] = item.get("relative_error_percent", "")
        item["s5_C0"] = float(c0)
        item["s5_C1"] = float(c1)
        item["s5_corrected_prediction_ppm"] = corrected if corrected is not None else ""
        if corrected is not None and target is not None:
            error = float(corrected) - float(target)
            item["s5_error_ppm"] = error
            item["s5_relative_error_percent"] = error / float(target) * 100.0 if float(target) else ""
            item["error_ppm"] = item["s5_error_ppm"]
            item["relative_error_percent"] = item["s5_relative_error_percent"]
        item["uses_s5_output_trim"] = True
        item["writes_coefficients"] = False
        out.append(item)
    return out


def build_co2_s13_multistrategy_fit_review(
    *,
    fit_points_csv: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    strategy_passes: Sequence[StrategyPass] = DEFAULT_STRATEGY_PASSES,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    low_end_target_ppm: float = DEFAULT_LOW_END_TARGET_PPM,
    top_n: int = DEFAULT_TOP_N,
    s5_acceptance_percent: float = DEFAULT_S5_ACCEPTANCE_PERCENT,
    s5_c0_decimals: int = DEFAULT_S5_C0_DECIMALS,
    s5_c1_decimals: int = DEFAULT_S5_C1_DECIMALS,
    s5_c1_min: float = DEFAULT_S5_C1_MIN,
    s5_c1_max: float = DEFAULT_S5_C1_MAX,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build multi-strategy no-write S1/S3 review tables."""

    strategy_summary: List[Dict[str, Any]] = []
    strategy_residuals: List[Dict[str, Any]] = []
    profile_rows: List[Dict[str, Any]] = []

    for profile in strategy_passes:
        tables = build_co2_s13_model_structure_review(
            fit_points_csv=fit_points_csv,
            fit_point_treatment_plan_csv=fit_point_treatment_plan_csv,
            exclude_device_ids=exclude_device_ids,
            structures=profile.structures,
            objectives=profile.objectives,
            zero_offsets_ppm=profile.zero_offsets_ppm,
            min_relative_target_ppm=float(min_relative_target_ppm),
            low_end_target_ppm=float(low_end_target_ppm),
            low_end_multiplier=float(profile.low_end_multiplier),
        )
        profile_rows.append(
            {
                "strategy_profile_id": profile.profile_id,
                "description": profile.description,
                "structures": ";".join(profile.structures),
                "objectives": ";".join(profile.objectives),
                "zero_offsets_ppm": ";".join(f"{float(value):g}" for value in profile.zero_offsets_ppm),
                "low_end_multiplier": float(profile.low_end_multiplier),
                "candidate_count": len(tables.get("structure_summary") or []),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            }
        )
        annotated_summary = [
            _annotate_summary_row(row, profile)
            for row in tables.get("structure_summary", [])
        ]
        strategy_summary.extend(annotated_summary)
        summary_lookup = {
            (
                _device_id(row.get("device_id")),
                str(row.get("structure_id") or ""),
                str(row.get("objective_id") or ""),
                float(_safe_float(row.get("zero_offset_ppm")) or 0.0),
            ): row
            for row in annotated_summary
        }
        for residual in tables.get("structure_residuals", []):
            key = (
                _device_id(residual.get("device_id")),
                str(residual.get("structure_id") or ""),
                str(residual.get("objective_id") or ""),
                float(_safe_float(residual.get("zero_offset_ppm")) or 0.0),
            )
            summary = summary_lookup.get(key)
            if summary:
                strategy_residuals.append(_annotate_residual_row(residual, profile, summary))

    top_rows, best_rows = _top_candidates(strategy_summary, top_n=top_n)
    best_residuals = _best_residuals(strategy_residuals, best_rows)
    s5_trim_summary = _s5_trim_summary_rows(
        top_rows,
        strategy_residuals,
        min_relative_target_ppm=float(min_relative_target_ppm),
        acceptance_percent=float(s5_acceptance_percent),
        c0_decimals=int(s5_c0_decimals),
        c1_decimals=int(s5_c1_decimals),
        c1_min=float(s5_c1_min),
        c1_max=float(s5_c1_max),
    )
    s5_top_rows, s5_best_rows = _top_s5_candidates(s5_trim_summary, top_n=top_n)
    s5_best_residuals = _s5_best_residuals(strategy_residuals, s5_best_rows)

    run_summary = [
        {
            "created_at": _now(),
            "fit_points_csv": str(Path(fit_points_csv).resolve()),
            "fit_point_treatment_plan_csv": (
                str(Path(fit_point_treatment_plan_csv).resolve()) if fit_point_treatment_plan_csv else ""
            ),
            "exclude_device_ids": ";".join(_device_id(item) for item in exclude_device_ids),
            "strategy_profile_count": len(strategy_passes),
            "candidate_count": len(strategy_summary),
            "best_device_count": len(best_rows),
            "min_relative_target_ppm": float(min_relative_target_ppm),
            "low_end_target_ppm": float(low_end_target_ppm),
            "s5_acceptance_percent": float(s5_acceptance_percent),
            "s5_c0_decimals": int(s5_c0_decimals),
            "s5_c1_decimals": int(s5_c1_decimals),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "uses_pressure_terms": False,
            "uses_s5_output_trim": "review_only_no_write",
            "not_real_acceptance_evidence": True,
        }
    ]
    return {
        "run_summary": run_summary,
        "strategy_profiles": profile_rows,
        "strategy_summary": strategy_summary,
        "strategy_residuals": strategy_residuals,
        "top_candidates": top_rows,
        "best_by_device": best_rows,
        "best_residuals": best_residuals,
        "s5_trim_summary": s5_trim_summary,
        "s5_top_candidates": s5_top_rows,
        "s5_best_by_device": s5_best_rows,
        "s5_best_residuals": s5_best_residuals,
        "segment_summary": _segment_rows(best_residuals, low_end_target_ppm=float(low_end_target_ppm)),
        "temperature_summary": _temperature_rows(best_residuals),
        "s5_segment_summary": _segment_rows(s5_best_residuals, low_end_target_ppm=float(low_end_target_ppm)),
        "s5_temperature_summary": _temperature_rows(s5_best_residuals),
    }


def _render_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    best = list(tables.get("best_by_device") or [])
    s5_best = list(tables.get("s5_best_by_device") or [])
    top = list(tables.get("top_candidates") or [])
    s5_top = list(tables.get("s5_top_candidates") or [])
    segment = list(tables.get("segment_summary") or [])
    s5_segment = list(tables.get("s5_segment_summary") or [])
    profiles = list(tables.get("strategy_profiles") or [])
    lines = [
        "# V1.5 CO2 S1/S3 多策略拟合比较评审",
        "",
        f"生成时间：{_now()}",
        "",
        "## 边界",
        "",
        "- 本评审只使用既有离线证据，不打开 COM，不控制气路/水路，不写入 SENCO。",
        "- 所有策略均冻结压力项，不把当前大气压开放流通主校准误差塞进压力项。",
        "- S5 输出层线性修正不参与本轮主链路选择；S5 只能在 S1/S3 残差来源解释清楚后作为最终显示层微调。",
        "- 零气 CO2 赋值只是低端锚点敏感性分析；非 0 ppm 假设必须回到零气证书、露点、管路状态和不确定度评审。",
        "",
        "## 策略组",
        "",
    ]
    for row in profiles:
        lines.append(f"- `{row.get('strategy_profile_id')}`：{row.get('description')}")
    lines.extend(
        [
            "",
            "## 每台最佳候选",
            "",
            "| 设备ID | 最佳策略 | 模型结构 | 目标函数 | 零气估算 ppm | 最大相对误差 % | 低端最大相对误差 % | RMSE ppm | 相比基线改善 % | 建议 |",
            "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in best:
        lines.append(
            "| {device} | {profile} | {structure} | {objective} | {zero} | {max_rel} | {low_rel} | {rmse} | {improve} | {action} |".format(
                device=row.get("device_id", ""),
                profile=row.get("strategy_profile_id", ""),
                structure=row.get("structure_id", ""),
                objective=row.get("objective_id", ""),
                zero=_fmt(row.get("zero_offset_ppm")),
                max_rel=_fmt(row.get("max_abs_relative_error_percent")),
                low_rel=_fmt(row.get("low_end_max_abs_relative_error_percent")),
                rmse=_fmt(row.get("rmse_ppm")),
                improve=_fmt(row.get("improvement_fraction")),
                action=row.get("recommended_no_write_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## S1/S3 + S5 鏄剧ず灞傚厛琛屼慨姝ｈ瘎瀹?",
            "",
            "| 璁惧ID | 鏈€浣崇瓥鐣?| 闆舵皵浼扮畻 ppm | S1/S3 鏈€澶х浉瀵硅宸?% | S5 C0 | S5 C1 | S5 鍛戒护 | S5 后鏈€澶х浉瀵硅宸?% | S5 后浣庣鏈€澶х浉瀵硅宸?% | 鏈€宸偣 | 寤鸿 |",
            "| --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- | --- |",
        ]
    )
    for row in s5_best:
        lines.append(
            "| {device} | {profile} | {zero} | {base_rel} | {c0} | {c1} | `{cmd}` | {s5_rel} | {s5_low} | {worst} | {action} |".format(
                device=row.get("device_id", ""),
                profile=row.get("strategy_profile_id", ""),
                zero=_fmt(row.get("zero_offset_ppm")),
                base_rel=_fmt(row.get("max_abs_relative_error_percent")),
                c0=_fmt(row.get("s5_C0")),
                c1=_fmt(row.get("s5_C1")),
                cmd=row.get("s5_command_preview", ""),
                s5_rel=_fmt(row.get("s5_max_abs_relative_error_percent")),
                s5_low=_fmt(row.get("s5_low_end_max_abs_relative_error_percent")),
                worst=row.get("s5_worst_point_identity", ""),
                action=row.get("s5_recommended_no_write_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 候选排行榜（每台前几名）",
            "",
            "| 设备ID | 排名 | 策略 | 模型结构 | 目标函数 | 零气估算 ppm | 低端权重 | 最大相对误差 % | 低端最大相对误差 % | S1 | S3 |",
            "| --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in top:
        lines.append(
            "| {device} | {rank} | {profile} | {structure} | {objective} | {zero} | {lowx} | {max_rel} | {low_rel} | `{s1}` | `{s3}` |".format(
                device=row.get("device_id", ""),
                rank=row.get("rank", ""),
                profile=row.get("strategy_profile_id", ""),
                structure=row.get("structure_id", ""),
                objective=row.get("objective_id", ""),
                zero=_fmt(row.get("zero_offset_ppm")),
                lowx=_fmt(row.get("low_end_multiplier")),
                max_rel=_fmt(row.get("max_abs_relative_error_percent")),
                low_rel=_fmt(row.get("low_end_max_abs_relative_error_percent")),
                s1=row.get("s1_payload_scientific", ""),
                s3=row.get("s3_payload_scientific", ""),
            )
        )
    lines.extend(
        [
            "",
            "## S5 候选排行（no-write）",
            "",
            "| 璁惧ID | 鎺掑悕 | 绛栫暐 | 鐩爣鍑芥暟 | 闆舵皵浼扮畻 ppm | S5 C0 | S5 C1 | S5 后鏈€澶х浉瀵硅宸?% | S5 后浣庣鏈€澶х浉瀵硅宸?% | S1 | S3 |",
            "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in s5_top:
        lines.append(
            "| {device} | {rank} | {profile} | {objective} | {zero} | {c0} | {c1} | {s5_rel} | {s5_low} | `{s1}` | `{s3}` |".format(
                device=row.get("device_id", ""),
                rank=row.get("s5_rank", ""),
                profile=row.get("strategy_profile_id", ""),
                objective=row.get("objective_id", ""),
                zero=_fmt(row.get("zero_offset_ppm")),
                c0=_fmt(row.get("s5_C0")),
                c1=_fmt(row.get("s5_C1")),
                s5_rel=_fmt(row.get("s5_max_abs_relative_error_percent")),
                s5_low=_fmt(row.get("s5_low_end_max_abs_relative_error_percent")),
                s1=row.get("s1_payload_scientific", ""),
                s3=row.get("s3_payload_scientific", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 最佳候选分段残差",
            "",
            "| 设备ID | 分段 | 点数 | 最大绝对误差 ppm | 最大相对误差 % | 最差点 | 同号比例 |",
            "| --- | --- | ---: | ---: | ---: | --- | ---: |",
        ]
    )
    for row in segment:
        lines.append(
            "| {device} | {segment} | {count} | {abs_err} | {rel} | {worst} | {same} |".format(
                device=row.get("device_id", ""),
                segment=row.get("segment_id", ""),
                count=row.get("point_count", ""),
                abs_err=_fmt(row.get("max_abs_error_ppm")),
                rel=_fmt(row.get("max_abs_relative_error_percent")),
                worst=row.get("worst_point_identity", ""),
                same=_fmt(row.get("same_sign_fraction")),
            )
        )
    lines.extend(
        [
            "",
            "## S5 后分段残差",
            "",
            "| 璁惧ID | 鍒嗘 | 鐐规暟 | 鏈€澶х粷瀵硅宸?ppm | 鏈€澶х浉瀵硅宸?% | 鏈€宸偣 | 鍚屽彿姣斾緥 |",
            "| --- | --- | ---: | ---: | ---: | --- | ---: |",
        ]
    )
    for row in s5_segment:
        lines.append(
            "| {device} | {segment} | {count} | {abs_err} | {rel} | {worst} | {same} |".format(
                device=row.get("device_id", ""),
                segment=row.get("segment_id", ""),
                count=row.get("point_count", ""),
                abs_err=_fmt(row.get("max_abs_error_ppm")),
                rel=_fmt(row.get("max_abs_relative_error_percent")),
                worst=row.get("worst_point_identity", ""),
                same=_fmt(row.get("same_sign_fraction")),
            )
        )
    lines.extend(
        [
            "",
            "## 物理结论",
            "",
            "- 如果最佳策略仍然主要在低端超差，而高端相对稳定，优先检查零气 CO2 赋值、低端气瓶目标值、露点/残余气状态和温度项边界。",
            "- 如果强低端权重能降低低端误差但明显抬高高端误差，说明不是简单权重问题，可能是主模型结构或目标状态不一致。",
            "- 如果线性温度结构优于完整温度结构，需要复核 T2 是否被少数温度组牵引；如果完整温度结构更优，则保留当前 S1/S3 主合同。",
            "- ratio-only 只用于诊断，不适合多温度正式写入，因为它无法解释光学腔体温度对 R 到浓度转换的影响。",
            "",
        ]
    )
    return "\n".join(lines)


def write_co2_s13_multistrategy_fit_review(
    *,
    fit_points_csv: str | Path,
    output_dir: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    strategy_passes: Sequence[StrategyPass] = DEFAULT_STRATEGY_PASSES,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    low_end_target_ppm: float = DEFAULT_LOW_END_TARGET_PPM,
    top_n: int = DEFAULT_TOP_N,
    s5_acceptance_percent: float = DEFAULT_S5_ACCEPTANCE_PERCENT,
    s5_c0_decimals: int = DEFAULT_S5_C0_DECIMALS,
    s5_c1_decimals: int = DEFAULT_S5_C1_DECIMALS,
    s5_c1_min: float = DEFAULT_S5_C1_MIN,
    s5_c1_max: float = DEFAULT_S5_C1_MAX,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_multistrategy_fit_review(
        fit_points_csv=fit_points_csv,
        fit_point_treatment_plan_csv=fit_point_treatment_plan_csv,
        exclude_device_ids=exclude_device_ids,
        strategy_passes=strategy_passes,
        min_relative_target_ppm=min_relative_target_ppm,
        low_end_target_ppm=low_end_target_ppm,
        top_n=top_n,
        s5_acceptance_percent=s5_acceptance_percent,
        s5_c0_decimals=s5_c0_decimals,
        s5_c1_decimals=s5_c1_decimals,
        s5_c1_min=s5_c1_min,
        s5_c1_max=s5_c1_max,
    )
    outputs = {
        "run_summary": output / "co2_s13_multistrategy_run_summary.csv",
        "strategy_profiles": output / "co2_s13_multistrategy_profiles.csv",
        "strategy_summary": output / "co2_s13_multistrategy_summary.csv",
        "strategy_residuals": output / "co2_s13_multistrategy_residuals.csv",
        "top_candidates": output / "co2_s13_multistrategy_top_candidates.csv",
        "best_by_device": output / "co2_s13_multistrategy_best_by_device.csv",
        "best_residuals": output / "co2_s13_multistrategy_best_residuals.csv",
        "s5_trim_summary": output / "co2_s13_multistrategy_s5_trim_summary.csv",
        "s5_top_candidates": output / "co2_s13_multistrategy_s5_top_candidates.csv",
        "s5_best_by_device": output / "co2_s13_multistrategy_s5_best_by_device.csv",
        "s5_best_residuals": output / "co2_s13_multistrategy_s5_best_residuals.csv",
        "segment_summary": output / "co2_s13_multistrategy_segment_summary.csv",
        "temperature_summary": output / "co2_s13_multistrategy_temperature_summary.csv",
        "s5_segment_summary": output / "co2_s13_multistrategy_s5_segment_summary.csv",
        "s5_temperature_summary": output / "co2_s13_multistrategy_s5_temperature_summary.csv",
        "metadata": output / "co2_s13_multistrategy_meta.json",
        "markdown": output / "co2_s13_multistrategy_review_zh.md",
    }
    for key in (
        "run_summary",
        "strategy_profiles",
        "strategy_summary",
        "strategy_residuals",
        "top_candidates",
        "best_by_device",
        "best_residuals",
        "s5_trim_summary",
        "s5_top_candidates",
        "s5_best_by_device",
        "s5_best_residuals",
        "segment_summary",
        "temperature_summary",
        "s5_segment_summary",
        "s5_temperature_summary",
    ):
        _write_csv(outputs[key], tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_multistrategy_fit_review",
                "created_at": _now(),
                "inputs": {
                    "fit_points_csv": str(Path(fit_points_csv).resolve()),
                    "fit_point_treatment_plan_csv": (
                        str(Path(fit_point_treatment_plan_csv).resolve())
                        if fit_point_treatment_plan_csv
                        else ""
                    ),
                    "exclude_device_ids": list(exclude_device_ids),
                    "strategy_profiles": [profile.profile_id for profile in strategy_passes],
                    "min_relative_target_ppm": min_relative_target_ppm,
                    "low_end_target_ppm": low_end_target_ppm,
                    "top_n": top_n,
                    "s5_acceptance_percent": s5_acceptance_percent,
                    "s5_c0_decimals": s5_c0_decimals,
                    "s5_c1_decimals": s5_c1_decimals,
                    "s5_c1_min": s5_c1_min,
                    "s5_c1_max": s5_c1_max,
                },
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "uses_s5_output_trim": "review_only_no_write",
                    "not_real_acceptance_evidence": True,
                },
                "outputs": {key: str(path) for key, path in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs["markdown"].write_text("\ufeff" + _render_markdown(tables), encoding="utf-8")
    return outputs
