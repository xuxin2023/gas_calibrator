"""Offline CO2 S1/S3 model-capacity review for V1.5.

This review asks one narrow question: when clean open-flow CO2 points still
leave large residuals, is the current writable S1/S3 model structure too small,
or are the data/targets themselves inconsistent?

The module is deliberately offline and no-write. It never opens COM ports,
controls gas/water routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from .co2_fit_algorithm_matrix import FitPoint, _load_fit_points
from .co2_senco5_linear_trim_review import _fit_quantized_command_trim


DEFAULT_ZERO_OFFSETS_PPM = (0.0, 2.0, 5.0, 8.0, 10.0)
DEFAULT_OBJECTIVES = ("absolute_lstsq", "relative_weighted_lstsq", "low_end_priority_lstsq")
DEFAULT_MIN_RELATIVE_TARGET_PPM = 50.0
DEFAULT_LOW_END_TARGET_PPM = 300.0
DEFAULT_LOW_END_MULTIPLIER = 5.0


@dataclass(frozen=True)
class CapacityStructure:
    structure_id: str
    base_terms: Tuple[str, ...]
    include_temp_group_offset: bool = False
    include_temp_group_ratio_slope: bool = False
    diagnostic_only: bool = True
    physical_meaning: str = ""


CAPACITY_STRUCTURES: Tuple[CapacityStructure, ...] = (
    CapacityStructure(
        structure_id="current_writable_senco13",
        base_terms=("intercept", "R", "R2", "R3", "T", "T2", "RT"),
        diagnostic_only=False,
        physical_meaning=(
            "当前可写主合同：R 三次项 + T/T2/RT；压力项冻结为 0，由 SENCO9 单独处理。"
        ),
    ),
    CapacityStructure(
        structure_id="diagnostic_add_R2T_RT2",
        base_terms=("intercept", "R", "R2", "R3", "T", "T2", "RT", "R2T", "RT2"),
        physical_meaning=(
            "诊断模型：在当前合同上增加二阶比值-温度交叉项，检查光学比值响应是否存在更高阶温度耦合。"
        ),
    ),
    CapacityStructure(
        structure_id="diagnostic_ratio_R4_R5",
        base_terms=("intercept", "R", "R2", "R3", "R4", "R5", "T", "T2", "RT"),
        physical_meaning=(
            "诊断模型：在当前合同上增加 R4/R5，检查 CO2 ratio 到浓度的非线性是否超过 S1 三次项容量。"
        ),
    ),
    CapacityStructure(
        structure_id="diagnostic_ratio_R4_R5_plus_R2T_RT2",
        base_terms=("intercept", "R", "R2", "R3", "R4", "R5", "T", "T2", "RT", "R2T", "RT2"),
        physical_meaning=(
            "诊断模型：同时加入高阶 R 和高阶 R/T 交叉项，检查误差是否来自比值非线性和温度耦合叠加。"
        ),
    ),
    CapacityStructure(
        structure_id="diagnostic_add_R2T_RT2_T3",
        base_terms=("intercept", "R", "R2", "R3", "T", "T2", "T3", "RT", "R2T", "RT2"),
        physical_meaning=(
            "诊断模型：进一步加入 T3，检查温度边界是否不能由 T/T2 表达。"
        ),
    ),
    CapacityStructure(
        structure_id="diagnostic_temp_group_offset",
        base_terms=("intercept", "R", "R2", "R3", "T", "T2", "RT"),
        include_temp_group_offset=True,
        physical_meaning=(
            "诊断模型：在当前合同上加入温度组共模偏置，判断误差是否是某个温度组整体目标状态偏移。"
        ),
    ),
    CapacityStructure(
        structure_id="diagnostic_temp_group_ratio_slope",
        base_terms=("intercept", "R", "R2", "R3", "T", "T2", "RT"),
        include_temp_group_ratio_slope=True,
        physical_meaning=(
            "诊断模型：在当前合同上加入温度组比值斜率修正，判断不同温度组是否需要不同 R 响应斜率。"
        ),
    ),
    CapacityStructure(
        structure_id="diagnostic_temp_group_offset_and_slope",
        base_terms=("intercept", "R", "R2", "R3", "T", "T2", "RT"),
        include_temp_group_offset=True,
        include_temp_group_ratio_slope=True,
        physical_meaning=(
            "诊断模型：同时加入温度组偏置和温度组 R 斜率；若该模型显著改善，根因更接近温度组目标状态/模型容量问题。"
        ),
    ),
)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


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


def _is_zero_anchor(point: FitPoint) -> bool:
    marker = str(point.zero_anchor_class or "").lower()
    return abs(float(point.target_ppm)) < 1.0e-9 or "zero" in marker


def _target_for_fit(point: FitPoint, zero_offset_ppm: float) -> float:
    if _is_zero_anchor(point):
        return float(zero_offset_ppm)
    return float(point.target_ppm)


def _temp_group(point: FitPoint) -> str:
    match = re.search(r"T([mp-]?\d+)", str(point.point_identity or ""), flags=re.IGNORECASE)
    if match:
        raw = match.group(1).lower().replace("m", "-").replace("p", "")
        try:
            return f"T{int(float(raw)):+d}"
        except Exception:
            return f"T{raw}"
    return f"T{round(float(point.temperature_c)):+d}"


def _relative_error_percent(error_ppm: float, target_ppm: float, *, min_relative_target_ppm: float) -> Optional[float]:
    if abs(float(target_ppm)) < float(min_relative_target_ppm):
        return None
    return 100.0 * float(error_ppm) / float(target_ppm)


def _feature_names(structure: CapacityStructure, points: Sequence[FitPoint]) -> Tuple[str, ...]:
    names = list(structure.base_terms)
    groups = sorted({_temp_group(point) for point in points})
    reference = groups[0] if groups else ""
    for group in groups:
        if group == reference:
            continue
        if structure.include_temp_group_offset:
            names.append(f"G:{group}:offset")
        if structure.include_temp_group_ratio_slope:
            names.append(f"G:{group}:R")
    return tuple(names)


def _centers(points: Sequence[FitPoint]) -> Tuple[float, float]:
    if not points:
        return 0.0, 273.15
    ratio_center = float(np.mean([float(point.ratio) for point in points]))
    temp_center = float(np.mean([float(point.temperature_c) + 273.15 for point in points]))
    return ratio_center, temp_center


def _feature_value(name: str, point: FitPoint, *, ratio_center: float, temp_center: float) -> float:
    r = float(point.ratio) - float(ratio_center)
    t = float(point.temperature_c) + 273.15 - float(temp_center)
    if name == "intercept":
        return 1.0
    if name == "R":
        return r
    if name == "R2":
        return r * r
    if name == "R3":
        return r**3
    if name == "R4":
        return r**4
    if name == "R5":
        return r**5
    if name == "T":
        return t
    if name == "T2":
        return t * t
    if name == "T3":
        return t**3
    if name == "RT":
        return r * t
    if name == "R2T":
        return r * r * t
    if name == "RT2":
        return r * t * t
    if name.startswith("G:"):
        _, group, kind = name.split(":", 2)
        if _temp_group(point) != group:
            return 0.0
        if kind == "offset":
            return 1.0
        if kind == "R":
            return r
    raise ValueError(f"Unsupported CO2 enhanced model term: {name}")


def _matrix(
    points: Sequence[FitPoint],
    names: Sequence[str],
    *,
    ratio_center: float,
    temp_center: float,
) -> np.ndarray:
    return np.asarray(
        [[_feature_value(name, point, ratio_center=ratio_center, temp_center=temp_center) for name in names] for point in points],
        dtype=float,
    )


def _weights(
    points: Sequence[FitPoint],
    targets: Sequence[float],
    *,
    objective_id: str,
    min_relative_target_ppm: float,
    low_end_target_ppm: float,
    low_end_multiplier: float,
) -> np.ndarray:
    values: List[float] = []
    for point, target in zip(points, targets):
        target_abs = abs(float(target))
        if objective_id == "absolute_lstsq":
            weight = 1.0
        else:
            weight = 1.0 / max(target_abs, float(min_relative_target_ppm))
            if objective_id == "low_end_priority_lstsq" and 0.0 < target_abs <= float(low_end_target_ppm):
                weight *= float(low_end_multiplier)
        values.append(weight)
    return np.asarray(values, dtype=float)


def _weighted_scaled_lstsq(matrix: np.ndarray, target: np.ndarray, weights: np.ndarray) -> Tuple[np.ndarray, int, float]:
    weighted_matrix = matrix * weights[:, None]
    weighted_target = target * weights
    scales = np.linalg.norm(weighted_matrix, axis=0)
    scales = np.where(np.isfinite(scales) & (scales > 0.0), scales, 1.0)
    scaled = weighted_matrix / scales
    rank = int(np.linalg.matrix_rank(scaled))
    condition = float(np.linalg.cond(scaled))
    fitted, *_ = np.linalg.lstsq(scaled, weighted_target, rcond=None)
    return np.asarray(fitted, dtype=float) / scales, rank, condition


def _fit_rows(
    *,
    device_id: str,
    points: Sequence[FitPoint],
    structure: CapacityStructure,
    objective_id: str,
    zero_offset_ppm: float,
    min_relative_target_ppm: float,
    low_end_target_ppm: float,
    low_end_multiplier: float,
    include_s5_review: bool,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    fit_points = [point for point in points if point.source_role == "fit"]
    if not fit_points:
        return {}, []
    feature_names = _feature_names(structure, fit_points)
    ratio_center, temp_center = _centers(fit_points)
    matrix = _matrix(fit_points, feature_names, ratio_center=ratio_center, temp_center=temp_center)
    targets = np.asarray([_target_for_fit(point, zero_offset_ppm) for point in fit_points], dtype=float)
    weights = _weights(
        fit_points,
        targets,
        objective_id=objective_id,
        min_relative_target_ppm=min_relative_target_ppm,
        low_end_target_ppm=low_end_target_ppm,
        low_end_multiplier=low_end_multiplier,
    )
    coeffs, rank, condition = _weighted_scaled_lstsq(matrix, targets, weights)
    predictions = matrix @ coeffs

    residual_rows: List[Dict[str, Any]] = []
    s5_input_rows: List[Dict[str, Any]] = []
    errors: List[float] = []
    rel_errors: List[float] = []
    low_rel_errors: List[float] = []
    zero_errors: List[float] = []
    for point, target, prediction in zip(fit_points, targets, predictions):
        error = float(prediction) - float(target)
        rel = _relative_error_percent(
            error,
            float(target),
            min_relative_target_ppm=min_relative_target_ppm,
        )
        errors.append(error)
        if rel is not None:
            rel_errors.append(rel)
            if float(target) <= float(low_end_target_ppm):
                low_rel_errors.append(rel)
            s5_input_rows.append({"_measured": float(prediction), "_target": float(target)})
        elif _is_zero_anchor(point):
            zero_errors.append(abs(error))
        residual_rows.append(
            {
                "device_id": device_id,
                "structure_id": structure.structure_id,
                "objective_id": objective_id,
                "zero_offset_ppm": float(zero_offset_ppm),
                "point_identity": point.point_identity,
                "target_ppm_for_fit": float(target),
                "certificate_target_ppm": float(point.target_ppm),
                "prediction_ppm": float(prediction),
                "error_ppm": error,
                "relative_error_percent": rel if rel is not None else "",
                "ratio": float(point.ratio),
                "temperature_c": float(point.temperature_c),
                "pressure_hpa": float(point.pressure_hpa),
                "h2o_mmol": point.h2o_mmol if point.h2o_mmol is not None else "",
                "zero_anchor_class": point.zero_anchor_class,
                "temp_group": _temp_group(point),
            }
        )

    s5_c0 = ""
    s5_c1 = ""
    s5_max_rel = ""
    s5_max_abs = ""
    s5_rmse = ""
    s5_worst = ""
    if include_s5_review and len(s5_input_rows) >= 4:
        c0, c1, max_rel, max_abs, rmse = _fit_quantized_command_trim(
            s5_input_rows,
            c0_decimals=3,
            c1_decimals=3,
            c1_min=0.85,
            c1_max=1.15,
        )
        s5_c0 = c0
        s5_c1 = c1
        s5_max_rel = max_rel
        s5_max_abs = max_abs
        s5_rmse = rmse
        worst_score = -1.0
        for row in residual_rows:
            target = _safe_float(row.get("target_ppm_for_fit"))
            pred = _safe_float(row.get("prediction_ppm"))
            if target is None or pred is None or abs(target) < min_relative_target_ppm:
                continue
            corrected = pred * c1 + c0
            score = abs(100.0 * (corrected - target) / target)
            if score > worst_score:
                worst_score = score
                s5_worst = row.get("point_identity", "")

    rmse = math.sqrt(sum(item * item for item in errors) / len(errors)) if errors else ""
    summary = {
        "device_id": device_id,
        "structure_id": structure.structure_id,
        "diagnostic_only": bool(structure.diagnostic_only),
        "objective_id": objective_id,
        "zero_offset_ppm": float(zero_offset_ppm),
        "fit_point_count": len(fit_points),
        "relative_point_count": len(rel_errors),
        "feature_count": len(feature_names),
        "matrix_rank": rank,
        "condition_number_scaled": condition,
        "terms": ";".join(feature_names),
        "max_abs_relative_error_percent": max(abs(item) for item in rel_errors) if rel_errors else "",
        "mean_abs_relative_error_percent": (
            sum(abs(item) for item in rel_errors) / len(rel_errors) if rel_errors else ""
        ),
        "low_end_max_abs_relative_error_percent": max(abs(item) for item in low_rel_errors) if low_rel_errors else "",
        "zero_anchor_max_abs_error_ppm": max(zero_errors) if zero_errors else "",
        "max_abs_error_ppm": max(abs(item) for item in errors) if errors else "",
        "rmse_ppm": rmse,
        "s5_C0": s5_c0,
        "s5_C1": s5_c1,
        "s5_max_abs_relative_error_percent": s5_max_rel,
        "s5_max_abs_error_ppm": s5_max_abs,
        "s5_rmse_ppm": s5_rmse,
        "s5_worst_point_identity": s5_worst,
        "controls_water_or_gas_routes": False,
        "opens_com_ports": False,
        "writes_coefficients": False,
        "auto_write_allowed": False,
        "physical_meaning": structure.physical_meaning,
    }
    return summary, residual_rows


def _score(row: Mapping[str, Any], *, prefer_s5: bool = False) -> Tuple[float, float, float, int]:
    primary_key = "s5_max_abs_relative_error_percent" if prefer_s5 else "max_abs_relative_error_percent"
    primary = _safe_float(row.get(primary_key))
    fallback = _safe_float(row.get("max_abs_relative_error_percent"))
    rmse = _safe_float(row.get("rmse_ppm"))
    features = _safe_float(row.get("feature_count"))
    return (
        float(primary) if primary is not None else float("inf"),
        float(fallback) if fallback is not None else float("inf"),
        float(rmse) if rmse is not None else float("inf"),
        int(features) if features is not None else 999,
    )


def _best_by_device(rows: Sequence[Mapping[str, Any]], *, prefer_s5: bool) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("device_id") or ""), []).append(row)
    best: Dict[str, Dict[str, Any]] = {}
    for device_id, device_rows in grouped.items():
        selected = min(device_rows, key=lambda row: _score(row, prefer_s5=prefer_s5))
        best[device_id] = dict(selected)
    return best


def _recommendation(
    *,
    current: Mapping[str, Any] | None,
    diagnostic: Mapping[str, Any] | None,
    acceptance_percent: float,
) -> str:
    if not current or not diagnostic:
        return "evidence_incomplete_review_manually"
    current_s5 = _safe_float(current.get("s5_max_abs_relative_error_percent"))
    diagnostic_s5 = _safe_float(diagnostic.get("s5_max_abs_relative_error_percent"))
    current_rel = _safe_float(current.get("max_abs_relative_error_percent"))
    diagnostic_rel = _safe_float(diagnostic.get("max_abs_relative_error_percent"))
    current_score = current_s5 if current_s5 is not None else current_rel
    diagnostic_score = diagnostic_s5 if diagnostic_s5 is not None else diagnostic_rel
    if current_score is not None and current_score <= acceptance_percent:
        return "current_writable_contract_reviewable_with_output_trim"
    if diagnostic_score is not None and diagnostic_score <= acceptance_percent:
        return "root_cause_current_senco13_model_capacity_insufficient"
    if (
        current_score is not None
        and diagnostic_score is not None
        and diagnostic_score < current_score * 0.7
    ):
        return "model_capacity_improves_but_still_needs_target_state_or_hardware_review"
    return "target_state_anchor_or_physical_evidence_needs_deeper_review"


def build_co2_s13_enhanced_model_capacity_review(
    *,
    fit_points_csv: str | Path,
    exclude_device_ids: Sequence[str] = (),
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    objectives: Sequence[str] = DEFAULT_OBJECTIVES,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    low_end_target_ppm: float = DEFAULT_LOW_END_TARGET_PPM,
    low_end_multiplier: float = DEFAULT_LOW_END_MULTIPLIER,
    acceptance_percent: float = 1.0,
    include_s5_review: bool = True,
) -> Dict[str, List[Dict[str, Any]]]:
    points = _load_fit_points(fit_points_csv, exclude_device_ids=exclude_device_ids)
    by_device: Dict[str, List[FitPoint]] = {}
    for point in points:
        by_device.setdefault(_device_id(point.device_id), []).append(point)

    summary_rows: List[Dict[str, Any]] = []
    residual_rows: List[Dict[str, Any]] = []
    for device_id, device_points in sorted(by_device.items()):
        for structure in CAPACITY_STRUCTURES:
            for objective_id in objectives:
                for zero_offset in zero_offsets_ppm:
                    summary, residuals = _fit_rows(
                        device_id=device_id,
                        points=device_points,
                        structure=structure,
                        objective_id=str(objective_id),
                        zero_offset_ppm=float(zero_offset),
                        min_relative_target_ppm=float(min_relative_target_ppm),
                        low_end_target_ppm=float(low_end_target_ppm),
                        low_end_multiplier=float(low_end_multiplier),
                        include_s5_review=bool(include_s5_review),
                    )
                    if summary:
                        summary_rows.append(summary)
                        residual_rows.extend(residuals)

    best_s5 = _best_by_device(summary_rows, prefer_s5=True)
    best_no_s5 = _best_by_device(summary_rows, prefer_s5=False)
    decision_rows: List[Dict[str, Any]] = []
    for device_id in sorted(by_device):
        current_candidates = [
            row
            for row in summary_rows
            if row.get("device_id") == device_id and row.get("structure_id") == "current_writable_senco13"
        ]
        diagnostic_candidates = [
            row
            for row in summary_rows
            if row.get("device_id") == device_id and row.get("structure_id") != "current_writable_senco13"
        ]
        current_best = min(current_candidates, key=lambda row: _score(row, prefer_s5=True)) if current_candidates else None
        diagnostic_best = min(diagnostic_candidates, key=lambda row: _score(row, prefer_s5=True)) if diagnostic_candidates else None
        selected = best_s5.get(device_id, {})
        decision_rows.append(
            {
                "device_id": device_id,
                "current_best_structure_id": current_best.get("structure_id") if current_best else "",
                "current_best_objective_id": current_best.get("objective_id") if current_best else "",
                "current_best_zero_offset_ppm": current_best.get("zero_offset_ppm") if current_best else "",
                "current_best_s5_max_abs_relative_error_percent": (
                    current_best.get("s5_max_abs_relative_error_percent") if current_best else ""
                ),
                "diagnostic_best_structure_id": diagnostic_best.get("structure_id") if diagnostic_best else "",
                "diagnostic_best_objective_id": diagnostic_best.get("objective_id") if diagnostic_best else "",
                "diagnostic_best_zero_offset_ppm": diagnostic_best.get("zero_offset_ppm") if diagnostic_best else "",
                "diagnostic_best_s5_max_abs_relative_error_percent": (
                    diagnostic_best.get("s5_max_abs_relative_error_percent") if diagnostic_best else ""
                ),
                "selected_best_structure_id": selected.get("structure_id", ""),
                "selected_best_is_diagnostic_only": selected.get("diagnostic_only", ""),
                "selected_best_s5_max_abs_relative_error_percent": selected.get("s5_max_abs_relative_error_percent", ""),
                "selected_best_s5_command_preview": (
                    f"SENCO5,YGAS,FFF,{selected.get('s5_C0')},{selected.get('s5_C1')}"
                    if selected.get("s5_C0") != "" and selected.get("s5_C1") != ""
                    else ""
                ),
                "recommendation": _recommendation(
                    current=current_best,
                    diagnostic=diagnostic_best,
                    acceptance_percent=float(acceptance_percent),
                ),
            }
        )

    return {
        "capacity_summary": summary_rows,
        "capacity_best_by_device_s5": list(best_s5.values()),
        "capacity_best_by_device_no_s5": list(best_no_s5.values()),
        "capacity_decision": decision_rows,
        "capacity_residuals": residual_rows,
        "run_summary": [
            {
                "generated_at": _now(),
                "fit_points_csv": str(fit_points_csv),
                "device_count": len(by_device),
                "summary_row_count": len(summary_rows),
                "residual_row_count": len(residual_rows),
                "zero_offsets_ppm": ";".join(str(float(item)) for item in zero_offsets_ppm),
                "objectives": ";".join(objectives),
                "acceptance_percent": float(acceptance_percent),
                "include_s5_review": bool(include_s5_review),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "not_real_acceptance_evidence": True,
            }
        ],
    }


def write_co2_s13_enhanced_model_capacity_review(
    *,
    fit_points_csv: str | Path,
    output_dir: str | Path,
    exclude_device_ids: Sequence[str] = (),
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    objectives: Sequence[str] = DEFAULT_OBJECTIVES,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    low_end_target_ppm: float = DEFAULT_LOW_END_TARGET_PPM,
    low_end_multiplier: float = DEFAULT_LOW_END_MULTIPLIER,
    acceptance_percent: float = 1.0,
    include_s5_review: bool = True,
) -> Dict[str, str]:
    output = Path(output_dir)
    tables = build_co2_s13_enhanced_model_capacity_review(
        fit_points_csv=fit_points_csv,
        exclude_device_ids=exclude_device_ids,
        zero_offsets_ppm=zero_offsets_ppm,
        objectives=objectives,
        min_relative_target_ppm=min_relative_target_ppm,
        low_end_target_ppm=low_end_target_ppm,
        low_end_multiplier=low_end_multiplier,
        acceptance_percent=acceptance_percent,
        include_s5_review=include_s5_review,
    )
    paths = {
        "run_summary": output / "co2_s13_enhanced_capacity_run_summary.csv",
        "capacity_summary": output / "co2_s13_enhanced_capacity_summary.csv",
        "capacity_best_by_device_s5": output / "co2_s13_enhanced_capacity_best_by_device_s5.csv",
        "capacity_best_by_device_no_s5": output / "co2_s13_enhanced_capacity_best_by_device_no_s5.csv",
        "capacity_decision": output / "co2_s13_enhanced_capacity_decision.csv",
        "capacity_residuals": output / "co2_s13_enhanced_capacity_residuals.csv",
        "metadata": output / "co2_s13_enhanced_capacity_meta.json",
        "markdown": output / "co2_s13_enhanced_capacity_review_zh.md",
    }
    for key, path in paths.items():
        if key in {"metadata", "markdown"}:
            continue
        _write_csv(path, tables[key])
    output.mkdir(parents=True, exist_ok=True)
    paths["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_enhanced_model_capacity_review",
                "generated_at": _now(),
                "inputs": {"fit_points_csv": str(fit_points_csv)},
                "include_s5_review": bool(include_s5_review),
                "no_write_boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "not_real_acceptance_evidence": True,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    paths["markdown"].write_text(_markdown(tables), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def _fmt(value: Any, digits: int = 3) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}f}"


def _markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    lines: List[str] = [
        "# CO2 S1/S3 增强模型容量根因评审",
        "",
        "## 结论摘要",
        "",
        "本报告只使用已采集证据离线计算，不打开 COM、不控制气路/水路、不写 SENCO。当前目标是判断误差大的根因是否来自单个坏点，还是来自当前可写 S1/S3 模型容量不足。",
        "",
        "## 逐设备结论",
        "",
        "| 设备ID | 当前可写合同+S5最大相对误差(%) | 最佳诊断结构 | 诊断+S5最大相对误差(%) | 建议 |",
        "|---|---:|---|---:|---|",
    ]
    for row in tables.get("capacity_decision", []):
        lines.append(
            "| {device} | {current} | {structure} | {diagnostic} | {rec} |".format(
                device=row.get("device_id", ""),
                current=_fmt(row.get("current_best_s5_max_abs_relative_error_percent")),
                structure=row.get("diagnostic_best_structure_id", ""),
                diagnostic=_fmt(row.get("diagnostic_best_s5_max_abs_relative_error_percent")),
                rec=row.get("recommendation", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            "- 如果当前可写合同仍大于 1%，但诊断模型能明显降低误差，说明采样点不一定坏，而是 `R/T` 响应中存在当前 `SENCO1/SENCO3` 不能表达的温度耦合或温度组共模偏移。",
            "- 如果删除共模点不能改善，而温度组诊断项能改善，根因更接近模型容量或目标状态桥接问题，不应靠随意剔除点来伪造合格。",
            "- 诊断结构不是写入指令。只有 `current_writable_senco13` 对应当前可写 S1/S3 主合同；其它结构用于定位根因和指导固件/算法升级。",
            "",
            "## 最佳 S5 辅助后候选",
            "",
            "| 设备ID | 结构 | 目标函数 | 零气估计(ppm) | S5 C0 | S5 C1 | 最大相对误差(%) | 最差点 | 是否诊断结构 |",
            "|---|---|---|---:|---:|---:|---:|---|---|",
        ]
    )
    for row in tables.get("capacity_best_by_device_s5", []):
        lines.append(
            "| {device} | {structure} | {objective} | {zero} | {c0} | {c1} | {maxrel} | {worst} | {diag} |".format(
                device=row.get("device_id", ""),
                structure=row.get("structure_id", ""),
                objective=row.get("objective_id", ""),
                zero=_fmt(row.get("zero_offset_ppm")),
                c0=_fmt(row.get("s5_C0")),
                c1=_fmt(row.get("s5_C1")),
                maxrel=_fmt(row.get("s5_max_abs_relative_error_percent")),
                worst=row.get("s5_worst_point_identity", ""),
                diag=row.get("diagnostic_only", ""),
            )
        )
    return "\n".join(lines) + "\n"
