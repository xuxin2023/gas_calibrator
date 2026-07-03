"""Offline ID-scoped CO2 anchored ratio-repair review for V1.5.

The review uses current-state CO2 points as anchors to map old full-temperature
factory ratios into a current-state-equivalent ratio domain, then refits the
V1.5 current-atmosphere SENCO1/SENCO3 main chain:

    CO2 = f(R, T), pressure terms frozen to zero

It does not open COM ports, control gas/water routes, or write coefficients.
Old displayed CO2 values are retained only as auxiliary evidence because the
old run may have been captured while the analyzer already had internal
coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from ..senco_format import format_senco_values, rounded_senco_values
from .co2_three_point_state_bridge import (
    Co2StatePoint,
    load_current_points_from_sample_files,
    load_points_from_run_root,
)
from .co2_firmware_contract import co2_raw_to_firmware_final_ppm


TERMS: Tuple[str, ...] = ("intercept", "R", "R2", "R3", "T", "T2", "RT")


@dataclass(frozen=True)
class RatioBridge:
    model_id: str
    description: str
    physical_meaning: str
    coefficients: Tuple[float, ...]
    state_driver: Optional[str] = None
    target_mode: str = "certificate_final_ppm"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


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
        writer.writerows([dict(row) for row in rows])


def _fmt_float(value: Any, digits: int = 6) -> Any:
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return value


def _nearest_old_anchor(current: Co2StatePoint, old_points: Sequence[Co2StatePoint]) -> Optional[Co2StatePoint]:
    candidates = [
        point
        for point in old_points
        if abs(point.nominal_ppm - current.nominal_ppm) <= 5.0
        and 15.0 <= point.chamber_temp_c <= 25.0
    ]
    if not candidates:
        candidates = [
            point
            for point in old_points
            if abs(point.nominal_ppm - current.nominal_ppm) <= 5.0
        ]
    if not candidates:
        return None
    return min(candidates, key=lambda point: abs(point.chamber_temp_c - current.chamber_temp_c))


def _anchor_pairs(
    *,
    old_points: Sequence[Co2StatePoint],
    current_points: Sequence[Co2StatePoint],
) -> List[Tuple[Co2StatePoint, Co2StatePoint]]:
    pairs: List[Tuple[Co2StatePoint, Co2StatePoint]] = []
    for current in sorted(current_points, key=lambda point: point.nominal_ppm):
        old = _nearest_old_anchor(current, old_points)
        if old is not None:
            pairs.append((old, current))
    return pairs


def _build_bridges(anchor_pairs: Sequence[Tuple[Co2StatePoint, Co2StatePoint]]) -> List[RatioBridge]:
    if not anchor_pairs:
        return []
    old_r = np.asarray([old.co2_ratio_f for old, _ in anchor_pairs], dtype=float)
    current_r = np.asarray([current.co2_ratio_f for _, current in anchor_pairs], dtype=float)
    target = np.asarray([current.nominal_ppm for _, current in anchor_pairs], dtype=float)
    delta = current_r - old_r
    bridges = [
        RatioBridge(
            model_id="identity_no_repair",
            description="No ratio repair; baseline for comparison.",
            physical_meaning="旧比值直接参与拟合，只用于观察未修正状态的误差。",
            coefficients=(),
        ),
        RatioBridge(
            model_id="constant_delta",
            description="Add the mean current-minus-old ratio offset.",
            physical_meaning="表示旧状态和当前状态之间存在近似固定的 ratio 零点偏移。",
            coefficients=(float(np.mean(delta)),),
        ),
        RatioBridge(
            model_id="firmware_h2o_raw_target_no_ratio_repair",
            description=(
                "Keep original factory ratios unchanged and fit the raw SENCO1/SENCO3 "
                "target implied by the firmware H2O dry-basis output layer."
            ),
            physical_meaning=(
                "不修正旧比值；按固件输出合同把证书 CO2 换算为 SENCO1/SENCO3 原始层目标，"
                "即 raw_target = certificate_CO2 * (1 - H2O_mmol_mol / 1000)。"
            ),
            coefficients=(),
            target_mode="firmware_h2o_raw_target_ppm",
        ),
    ]
    if len(anchor_pairs) >= 2:
        slope, intercept = np.polyfit(old_r, current_r, 1)
        delta_slope, delta_intercept = np.polyfit(target, delta, 1)
        bridges.extend(
            [
                RatioBridge(
                    model_id="affine_ratio_bridge",
                    description="Map old R to current R with current_R = intercept + slope * old_R.",
                    physical_meaning="表示旧状态到当前状态的 ratio 零点和尺度同时发生变化，是优先评估的物理桥接模型。",
                    coefficients=(float(intercept), float(slope)),
                ),
                RatioBridge(
                    model_id="target_linear_delta",
                    description="Add a target-indexed linear delta fitted by the current anchors.",
                    physical_meaning="只作为训练证据修复诊断；它依赖标准气目标值，不能作为未知样品运行时变换。",
                    coefficients=(float(delta_intercept), float(delta_slope)),
                ),
            ]
        )
    if len(anchor_pairs) >= 3:
        quad = np.polyfit(old_r, current_r, 2)
        bridges.append(
            RatioBridge(
                model_id="quadratic_ratio_bridge_diagnostic",
                description="Map old R to current R with a quadratic curve through the three anchors.",
                physical_meaning="三点可精确约束二次曲线，但过拟合风险高，只能作为诊断上限。",
                coefficients=tuple(float(value) for value in quad),
            )
        )
    state_drivers = {
        "h2o_mmol_mol": "H2O mmol/mol",
        "dewpoint_c": "露点",
        "chamber_temp_c": "腔体温度",
        "pressure_hpa": "压力",
    }
    for state_key, label in state_drivers.items():
        state_deltas: List[float] = []
        ratio_deltas: List[float] = []
        targets: List[float] = []
        for old, current in anchor_pairs:
            old_value = getattr(old, state_key)
            current_value = getattr(current, state_key)
            if old_value is None or current_value is None:
                continue
            state_deltas.append(float(current_value) - float(old_value))
            ratio_deltas.append(float(current.co2_ratio_f) - float(old.co2_ratio_f))
            targets.append(float(current.nominal_ppm))
        if len(state_deltas) < 2 or len({round(value, 12) for value in state_deltas}) < 2:
            continue
        ratio_slope, ratio_intercept = np.polyfit(
            np.asarray(state_deltas, dtype=float),
            np.asarray(ratio_deltas, dtype=float),
            1,
        )
        state_slope, state_intercept = np.polyfit(
            np.asarray(targets, dtype=float),
            np.asarray(state_deltas, dtype=float),
            1,
        )
        bridges.append(
            RatioBridge(
                model_id=f"state_{state_key}_delta_bridge",
                description=(
                    f"Estimate current-minus-old ratio delta from same-gas-point {label} delta."
                ),
                physical_meaning=(
                    f"使用同一气点的新旧 {label} 差异解释 ratio 偏移；"
                    "这是状态感知的训练证据修复，避免把气体状态差异误认为纯仪器漂移。"
                ),
                coefficients=(
                    float(ratio_intercept),
                    float(ratio_slope),
                    float(state_intercept),
                    float(state_slope),
                ),
                state_driver=state_key,
            )
        )
    return bridges


def _correct_ratio(point: Co2StatePoint, bridge: RatioBridge) -> float:
    r = point.co2_ratio_f
    if bridge.model_id == "identity_no_repair":
        return r
    if bridge.model_id == "constant_delta":
        return r + bridge.coefficients[0]
    if bridge.model_id == "firmware_h2o_raw_target_no_ratio_repair":
        return r
    if bridge.model_id == "affine_ratio_bridge":
        intercept, slope = bridge.coefficients
        return intercept + slope * r
    if bridge.model_id == "target_linear_delta":
        intercept, slope = bridge.coefficients
        return r + intercept + slope * point.nominal_ppm
    if bridge.model_id == "quadratic_ratio_bridge_diagnostic":
        a, b, c = bridge.coefficients
        return a * r * r + b * r + c
    if bridge.state_driver:
        ratio_intercept, ratio_slope, state_intercept, state_slope = bridge.coefficients
        estimated_state_delta = state_intercept + state_slope * point.nominal_ppm
        return r + ratio_intercept + ratio_slope * estimated_state_delta
    raise KeyError(bridge.model_id)


def _correlation(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    if len(xs) < 2 or len(ys) < 2:
        return None
    x = np.asarray(xs, dtype=float)
    y = np.asarray(ys, dtype=float)
    if float(np.std(x)) <= 1.0e-15 or float(np.std(y)) <= 1.0e-15:
        return None
    return float(np.corrcoef(x, y)[0, 1])


def _state_sensitivity_rows(anchor_pairs: Sequence[Tuple[Co2StatePoint, Co2StatePoint]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    state_drivers = {
        "h2o_mmol_mol": ("H2O mmol/mol", "水汽摩尔分数变化"),
        "dewpoint_c": ("露点 °C", "露点状态变化"),
        "chamber_temp_c": ("腔体温度 °C", "分析仪腔体温度变化"),
        "pressure_hpa": ("压力 hPa", "开放流通压力状态变化"),
    }
    for state_key, (label, meaning) in state_drivers.items():
        state_deltas: List[float] = []
        ratio_deltas: List[float] = []
        for old, current in anchor_pairs:
            old_value = getattr(old, state_key)
            current_value = getattr(current, state_key)
            if old_value is None or current_value is None:
                continue
            state_deltas.append(float(current_value) - float(old_value))
            ratio_deltas.append(float(current.co2_ratio_f) - float(old.co2_ratio_f))
        if not state_deltas:
            continue
        slope = ""
        intercept = ""
        if len(state_deltas) >= 2 and len({round(value, 12) for value in state_deltas}) >= 2:
            fitted_slope, fitted_intercept = np.polyfit(
                np.asarray(state_deltas, dtype=float),
                np.asarray(ratio_deltas, dtype=float),
                1,
            )
            slope = float(fitted_slope)
            intercept = float(fitted_intercept)
        rows.append(
            {
                "state_driver": state_key,
                "state_label": label,
                "physical_meaning": meaning,
                "anchor_count": len(state_deltas),
                "state_delta_min": min(state_deltas),
                "state_delta_max": max(state_deltas),
                "state_delta_mean": float(np.mean(np.asarray(state_deltas, dtype=float))),
                "ratio_delta_min": min(ratio_deltas),
                "ratio_delta_max": max(ratio_deltas),
                "ratio_delta_mean": float(np.mean(np.asarray(ratio_deltas, dtype=float))),
                "ratio_delta_per_state_unit": slope,
                "ratio_delta_intercept": intercept,
                "correlation": _correlation(state_deltas, ratio_deltas),
            }
        )
    return rows


def _feature(ratio: float, temperature_c: float) -> np.ndarray:
    temp_k = temperature_c + 273.15
    return np.asarray([1.0, ratio, ratio * ratio, ratio**3, temp_k, temp_k * temp_k, ratio * temp_k], dtype=float)


def _centered_matrix(points: Sequence[Co2StatePoint], ratios: Sequence[float], *, ratio_center: float, temp_center_k: float) -> np.ndarray:
    rows: List[List[float]] = []
    for point, ratio in zip(points, ratios):
        rd = float(ratio) - ratio_center
        td = point.chamber_temp_c + 273.15 - temp_center_k
        rows.append([1.0, rd, rd * rd, rd**3, td, td * td, rd * td])
    return np.asarray(rows, dtype=float)


def _centered_to_absolute(coefficients: Sequence[float], *, ratio_center: float, temp_center_k: float) -> np.ndarray:
    b0, b1, b2, b3, bt, bt2, brt = [float(value) for value in coefficients]
    r0 = float(ratio_center)
    t0 = float(temp_center_k)
    return np.asarray(
        [
            b0 - r0 * b1 + (r0**2) * b2 - (r0**3) * b3 - t0 * bt + (t0**2) * bt2 + r0 * t0 * brt,
            b1 - 2.0 * r0 * b2 + 3.0 * (r0**2) * b3 - t0 * brt,
            b2 - 3.0 * r0 * b3,
            b3,
            bt - 2.0 * t0 * bt2 - r0 * brt,
            bt2,
            brt,
        ],
        dtype=float,
    )


def _scaled_lstsq(matrix: np.ndarray, target: np.ndarray) -> Tuple[np.ndarray, int, float]:
    scales = np.linalg.norm(matrix, axis=0)
    scales = np.where(np.isfinite(scales) & (scales > 0.0), scales, 1.0)
    scaled = matrix / scales
    rank = int(np.linalg.matrix_rank(scaled))
    condition = float(np.linalg.cond(scaled))
    coeffs, *_ = np.linalg.lstsq(scaled, target, rcond=None)
    return np.asarray(coeffs, dtype=float) / scales, rank, condition


def _fit_senco13(points: Sequence[Co2StatePoint], ratios: Sequence[float]) -> Tuple[np.ndarray, int, float]:
    ratio_center = float(np.mean(np.asarray(ratios, dtype=float)))
    temp_center = float(np.mean([point.chamber_temp_c + 273.15 for point in points]))
    matrix = _centered_matrix(points, ratios, ratio_center=ratio_center, temp_center_k=temp_center)
    centered_coeffs, rank, condition = _scaled_lstsq(
        matrix,
        np.asarray([point.target_ppm for point in points], dtype=float),
    )
    return _centered_to_absolute(centered_coeffs, ratio_center=ratio_center, temp_center_k=temp_center), rank, condition


def _raw_target_from_firmware_final(point: Co2StatePoint) -> float:
    """Return the raw SENCO1/SENCO3 target behind the firmware CO2 final layer."""

    denominator = 1.0 - float(point.h2o_mmol_mol) / 1000.0
    if not math.isfinite(denominator) or denominator <= 0.0:
        return float(point.target_ppm)
    return float(point.target_ppm) * denominator


def _fit_target(point: Co2StatePoint, *, target_mode: str) -> float:
    if target_mode == "firmware_h2o_raw_target_ppm":
        return _raw_target_from_firmware_final(point)
    return float(point.target_ppm)


def _fit_senco13_for_bridge(
    points: Sequence[Co2StatePoint],
    ratios: Sequence[float],
    *,
    target_mode: str,
) -> Tuple[np.ndarray, int, float]:
    ratio_center = float(np.mean(np.asarray(ratios, dtype=float)))
    temp_center = float(np.mean([point.chamber_temp_c + 273.15 for point in points]))
    matrix = _centered_matrix(points, ratios, ratio_center=ratio_center, temp_center_k=temp_center)
    centered_coeffs, rank, condition = _scaled_lstsq(
        matrix,
        np.asarray([_fit_target(point, target_mode=target_mode) for point in points], dtype=float),
    )
    return _centered_to_absolute(centered_coeffs, ratio_center=ratio_center, temp_center_k=temp_center), rank, condition


def _relative_error_pct(predicted: float, target: float) -> Optional[float]:
    if abs(target) <= 1.0e-9:
        return None
    return 100.0 * (predicted - target) / target


def _metrics(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    errors = [float(row["error_ppm"]) for row in rows]
    relative = [
        abs(float(row["relative_error_pct"]))
        for row in rows
        if row.get("relative_error_pct") not in (None, "")
    ]
    zero_errors = [
        abs(float(row["error_ppm"]))
        for row in rows
        if row.get("relative_error_pct") in (None, "")
    ]
    return {
        "eval_count": len(rows),
        "rmse_ppm": float(np.sqrt(np.mean(np.asarray(errors, dtype=float) ** 2))) if errors else "",
        "max_abs_error_ppm": max((abs(item) for item in errors), default=""),
        "mean_abs_error_ppm": (sum(abs(item) for item in errors) / len(errors)) if errors else "",
        "max_abs_relative_error_pct": max(relative, default=""),
        "mean_abs_relative_error_pct": (sum(relative) / len(relative)) if relative else "",
        "max_zero_abs_error_ppm": max(zero_errors, default=""),
    }


def _prediction_rows(
    *,
    bridge: RatioBridge,
    coeffs: Sequence[float],
    points: Sequence[Co2StatePoint],
    eval_set: str,
    use_corrected_ratio: bool,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    coeff_array = np.asarray(coeffs, dtype=float)
    for point in points:
        corrected = _correct_ratio(point, bridge)
        ratio = corrected if use_corrected_ratio else point.co2_ratio_f
        raw_predicted = float(_feature(ratio, point.chamber_temp_c) @ coeff_array)
        if bridge.target_mode == "firmware_h2o_raw_target_ppm":
            final_predicted = co2_raw_to_firmware_final_ppm(raw_predicted, point.h2o_mmol_mol)
            predicted = raw_predicted if final_predicted is None else float(final_predicted)
        else:
            predicted = raw_predicted
        error = predicted - point.target_ppm
        relative = _relative_error_pct(predicted, point.target_ppm)
        rows.append(
            {
                "bridge_model": bridge.model_id,
                "target_mode": bridge.target_mode,
                "eval_set": eval_set,
                "uses_corrected_ratio": use_corrected_ratio,
                "point_identity": point.point_identity,
                "role": point.role,
                "target_ppm": point.target_ppm,
                "fit_target_ppm": _fit_target(point, target_mode=bridge.target_mode),
                "raw_model_ppm": raw_predicted,
                "predicted_ppm": predicted,
                "error_ppm": error,
                "relative_error_pct": "" if relative is None else relative,
                "original_ratio_f": point.co2_ratio_f,
                "corrected_ratio_f": corrected,
                "ratio_delta_applied": corrected - point.co2_ratio_f,
                "h2o_mmol_mol": point.h2o_mmol_mol,
                "dewpoint_c": point.dewpoint_c,
                "chamber_temp_c": point.chamber_temp_c,
                "pressure_hpa": point.pressure_hpa,
                "displayed_co2_ppm_aux": point.displayed_co2_ppm,
                "sample_count": point.sample_count,
                "usable_count": point.usable_count,
            }
        )
    return rows


def build_co2_anchor_ratio_repair_tables(
    *,
    old_run_dir: str | Path,
    current_sample_files: Sequence[str | Path],
    target_device_id: str,
) -> Dict[str, Any]:
    target_id = _device_id(target_device_id)
    old_points = load_points_from_run_root(
        old_run_dir,
        source_set="old_fulltemp_coefficients_present",
        target_device_id=target_id,
    )
    current_points = load_current_points_from_sample_files(
        current_sample_files,
        source_set="current_state_anchor",
        target_device_id=target_id,
    )
    anchor_pairs = _anchor_pairs(old_points=old_points, current_points=current_points)
    bridges = _build_bridges(anchor_pairs)

    anchor_rows: List[Dict[str, Any]] = []
    for old, current in anchor_pairs:
        anchor_rows.append(
            {
                "nominal_ppm": current.nominal_ppm,
                "current_target_ppm": current.target_ppm,
                "old_point": old.point_identity,
                "current_point": current.point_identity,
                "old_ratio_f": old.co2_ratio_f,
                "current_ratio_f": current.co2_ratio_f,
                "delta_ratio_f": current.co2_ratio_f - old.co2_ratio_f,
                "old_h2o_mmol_mol": old.h2o_mmol_mol,
                "current_h2o_mmol_mol": current.h2o_mmol_mol,
                "old_dewpoint_c": old.dewpoint_c,
                "current_dewpoint_c": current.dewpoint_c,
                "old_chamber_temp_c": old.chamber_temp_c,
                "current_chamber_temp_c": current.chamber_temp_c,
                "old_pressure_hpa": old.pressure_hpa,
                "current_pressure_hpa": current.pressure_hpa,
            }
        )

    summary_rows: List[Dict[str, Any]] = []
    coefficient_rows: List[Dict[str, Any]] = []
    prediction_rows: List[Dict[str, Any]] = []

    for bridge in bridges:
        corrected_ratios = [_correct_ratio(point, bridge) for point in old_points]
        if len(old_points) < len(TERMS):
            summary_rows.append(
                {
                    "bridge_model": bridge.model_id,
                    "status": "blocked_insufficient_old_points",
                    "old_train_count": len(old_points),
                    "term_count": len(TERMS),
                }
            )
            continue
        coeffs, rank, condition = _fit_senco13_for_bridge(
            old_points,
            corrected_ratios,
            target_mode=bridge.target_mode,
        )
        rounded_coeffs = np.asarray(rounded_senco_values(coeffs), dtype=float)
        primary = [float(coeffs[0]), float(coeffs[1]), float(coeffs[2]), float(coeffs[3]), 0.0, 0.0]
        secondary = [float(coeffs[4]), float(coeffs[5]), float(coeffs[6]), 0.0, 0.0, 0.0]
        for term, coeff, rounded_coeff in zip(TERMS, coeffs, rounded_coeffs):
            coefficient_rows.append(
                {
                    "device_id": target_id,
                    "bridge_model": bridge.model_id,
                    "term": term,
                    "coefficient": float(coeff),
                    "rounded_coefficient": float(rounded_coeff),
                    "senco_group": "SENCO1" if term in {"intercept", "R", "R2", "R3"} else "SENCO3",
                }
            )
        eval_jobs = (
            ("old_all_corrected_ratio", old_points, True),
            ("old_all_original_ratio", old_points, False),
            ("current_anchor_actual_ratio", current_points, False),
        )
        for eval_set, points, use_corrected in eval_jobs:
            rows = _prediction_rows(
                bridge=bridge,
                coeffs=coeffs,
                points=points,
                eval_set=eval_set,
                use_corrected_ratio=use_corrected,
            )
            prediction_rows.extend(rows)
            summary_rows.append(
                {
                    "device_id": target_id,
                    "bridge_model": bridge.model_id,
                    "eval_set": eval_set,
                    "status": "reviewable_no_write",
                    "old_train_count": len(old_points),
                    "anchor_count": len(anchor_pairs),
                    "matrix_rank": rank,
                    "term_count": len(TERMS),
                    "condition_number_scaled": condition,
                    "bridge_coefficients_json": json.dumps(list(bridge.coefficients), separators=(",", ":")),
                    "state_driver": bridge.state_driver or "",
                    "target_mode": bridge.target_mode,
                    "senco1_payload_scientific": ",".join(format_senco_values(primary)),
                    "senco3_payload_scientific": ",".join(format_senco_values(secondary)),
                    "pressure_terms": "frozen_zero_independent_senco9_workflow",
                    "description": bridge.description,
                    "physical_meaning": bridge.physical_meaning,
                    **_metrics(rows),
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                }
            )

    manifest = {
        "schema": "v1_5_co2_anchor_ratio_repair_v1",
        "generated_at": _now(),
        "target_device_id": target_id,
        "old_run_dir": str(old_run_dir),
        "current_sample_files": [str(path) for path in current_sample_files],
        "old_point_count": len(old_points),
        "current_anchor_count": len(current_points),
        "matched_anchor_count": len(anchor_pairs),
        "boundary": "offline_no_com_no_route_control_no_senco_write",
        "physical_contract": (
            "Current-state anchors are used to repair old factory CO2 ratios. "
            "Old displayed CO2/H2O values are auxiliary only because old internal coefficients "
            "may already have been active. Pressure terms remain frozen because V1.5 pressure "
            "is handled by the independent SENCO9 workflow."
        ),
    }
    recommendation_rows = _recommendation_rows(
        manifest=manifest,
        summary_rows=summary_rows,
        coefficient_rows=coefficient_rows,
    )
    write_review_contract_rows = _write_review_contract_rows(
        manifest=manifest,
        recommendation_rows=recommendation_rows,
        summary_rows=summary_rows,
    )
    return {
        "manifest": manifest,
        "anchor_rows": anchor_rows,
        "state_sensitivity_rows": _state_sensitivity_rows(anchor_pairs),
        "recommendation_rows": recommendation_rows,
        "write_review_contract_rows": write_review_contract_rows,
        "summary_rows": summary_rows,
        "coefficient_rows": coefficient_rows,
        "prediction_rows": prediction_rows,
    }


def _best_rows(summary_rows: Sequence[Mapping[str, Any]], eval_set: str) -> List[Mapping[str, Any]]:
    rows = [row for row in summary_rows if row.get("eval_set") == eval_set]
    return sorted(
        rows,
        key=lambda row: (
            float(row.get("max_abs_relative_error_pct") or 1.0e12),
            float(row.get("max_abs_error_ppm") or 1.0e12),
        ),
    )


def _physical_priority(row: Mapping[str, Any]) -> Tuple[int, float, float]:
    model = str(row.get("bridge_model") or "")
    state_driver = str(row.get("state_driver") or "")
    target_mode = str(row.get("target_mode") or "")
    # H2O is a gas-composition state and is therefore the safest physical
    # explanation when it performs well. Pressure is diagnostic because the
    # pressure channel is handled independently by SENCO9, and three anchors can
    # make pressure look better than it really is.
    if model == "firmware_h2o_raw_target_no_ratio_repair" or target_mode == "firmware_h2o_raw_target_ppm":
        priority = -1
    elif state_driver == "h2o_mmol_mol":
        priority = 0
    elif state_driver == "chamber_temp_c":
        priority = 1
    elif model == "affine_ratio_bridge":
        priority = 2
    elif state_driver == "pressure_hpa":
        priority = 3
    elif model == "target_linear_delta":
        priority = 4
    elif model == "quadratic_ratio_bridge_diagnostic":
        priority = 5
    elif state_driver:
        priority = 6
    else:
        priority = 7
    return (
        priority,
        float(row.get("max_abs_relative_error_pct") or 1.0e12),
        float(row.get("max_abs_error_ppm") or 1.0e12),
    )


def _find_coefficients(
    coefficient_rows: Sequence[Mapping[str, Any]],
    *,
    bridge_model: str,
    group: str,
) -> List[float]:
    order = ("intercept", "R", "R2", "R3") if group == "SENCO1" else ("T", "T2", "RT")
    by_term = {
        str(row.get("term") or ""): row
        for row in coefficient_rows
        if str(row.get("bridge_model") or "") == bridge_model and str(row.get("senco_group") or "") == group
    }
    values: List[float] = []
    for term in order:
        row = by_term.get(term)
        if not row:
            return []
        values.append(float(row["coefficient"]))
    if group == "SENCO1":
        values.extend([0.0, 0.0])
    else:
        values.extend([0.0, 0.0, 0.0])
    return values


def _recommendation_rows(
    *,
    manifest: Mapping[str, Any],
    summary_rows: Sequence[Mapping[str, Any]],
    coefficient_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    current_rows = [
        row for row in summary_rows if row.get("eval_set") == "current_anchor_actual_ratio"
    ]
    if not current_rows:
        return [
            {
                "target_device_id": manifest.get("target_device_id", ""),
                "recommendation_status": "blocked_no_current_anchor_evaluation",
                "reason": "No current-anchor evaluation rows were available.",
            }
        ]
    numeric_best = sorted(
        current_rows,
        key=lambda row: (
            float(row.get("max_abs_relative_error_pct") or 1.0e12),
            float(row.get("max_abs_error_ppm") or 1.0e12),
        ),
    )[0]
    candidates = [
        row
        for row in current_rows
        if str(row.get("bridge_model") or "") not in {"identity_no_repair", "constant_delta"}
        and float(row.get("max_abs_relative_error_pct") or 1.0e12) <= 1.5
    ]
    physical = sorted(candidates, key=_physical_priority)[0] if candidates else numeric_best
    bridge = str(physical.get("bridge_model") or "")
    s1 = _find_coefficients(coefficient_rows, bridge_model=bridge, group="SENCO1")
    s3 = _find_coefficients(coefficient_rows, bridge_model=bridge, group="SENCO3")
    warnings: List[str] = []
    if str(numeric_best.get("bridge_model") or "") != bridge:
        warnings.append(
            "numeric_best_differs_from_physical_recommendation"
        )
    if str(physical.get("target_mode") or "") == "firmware_h2o_raw_target_ppm":
        warnings.append(
            "keeps_original_ratio_and_fits_raw_senco13_target_using_firmware_h2o_layer"
        )
    if str(numeric_best.get("state_driver") or "") == "pressure_hpa":
        warnings.append(
            "pressure_driver_is_diagnostic_only_because_pressure_channel_is_independent_senco9"
        )
    if int(float(manifest.get("matched_anchor_count") or 0)) <= 3:
        warnings.append("only_three_current_anchors_overfit_risk")
    if not s1 or not s3:
        status = "blocked_missing_candidate_payload"
    elif candidates:
        status = "reviewable_no_write_candidate"
    else:
        status = "blocked_current_anchor_error_too_large"
    return [
        {
            "target_device_id": manifest.get("target_device_id", ""),
            "recommendation_status": status,
            "recommended_bridge_model": bridge,
            "recommended_state_driver": physical.get("state_driver", ""),
            "recommended_target_mode": physical.get("target_mode", ""),
            "numeric_best_bridge_model": numeric_best.get("bridge_model", ""),
            "numeric_best_state_driver": numeric_best.get("state_driver", ""),
            "numeric_best_target_mode": numeric_best.get("target_mode", ""),
            "recommended_current_max_abs_error_ppm": physical.get("max_abs_error_ppm", ""),
            "recommended_current_max_abs_relative_error_pct": physical.get("max_abs_relative_error_pct", ""),
            "recommended_current_rmse_ppm": physical.get("rmse_ppm", ""),
            "numeric_best_current_max_abs_error_ppm": numeric_best.get("max_abs_error_ppm", ""),
            "numeric_best_current_max_abs_relative_error_pct": numeric_best.get("max_abs_relative_error_pct", ""),
            "senco1_payload_scientific": ",".join(format_senco_values(s1)) if s1 else "",
            "senco3_payload_scientific": ",".join(format_senco_values(s3)) if s3 else "",
            "warnings": ";".join(warnings),
            "physical_reason": (
                "A no-ratio-repair firmware raw-target fit is preferred when it is within review range. "
                "It preserves the original factory ratio evidence and moves the correction into the "
                "candidate coefficient contract. H2O-state bridge is the next preferred explanation because "
                "H2O changes gas composition while pressure remains an independently calibrated input."
            ),
            "boundary": "offline_no_com_no_route_control_no_senco_write",
        }
    ]


def _markdown(tables: Mapping[str, Any]) -> str:
    manifest = tables["manifest"]
    anchor_rows = list(tables["anchor_rows"])
    state_rows = list(tables["state_sensitivity_rows"])
    summary_rows = list(tables["summary_rows"])
    recommendation_rows = list(tables.get("recommendation_rows", []))
    write_review_contract_rows = list(tables.get("write_review_contract_rows", []))
    best_current = _best_rows(summary_rows, "current_anchor_actual_ratio")
    lines = [
        f"# ID{manifest['target_device_id']} CO2 当前锚点比值修复评估（no-write）",
        "",
        "## 结论",
        "",
        "- 本报告只做离线评估：不打开 COM、不控制水路/气路、不写 SENCO。",
        "- 旧全温采样时传感器内部已有系数，因此旧显示浓度仅作辅助证据；修复和拟合只使用标准气证书值、工厂模式 CO2 比值和温度。",
        "- 压力项冻结为 0，压力通道继续按独立 SENCO9 流程处理。",
    ]
    if best_current:
        best = best_current[0]
        lines.append(
            f"- 当前三点反推表现最好的桥接模型是 `{best['bridge_model']}`："
            f"最大相对误差 `{_fmt(best.get('max_abs_relative_error_pct'), 3)}%`，"
            f"最大绝对误差 `{_fmt(best.get('max_abs_error_ppm'), 3)} ppm`。"
        )
    if recommendation_rows:
        rec = recommendation_rows[0]
        lines.append(
            f"- no-write 物理推荐候选：`{rec.get('recommended_bridge_model', '')}`，"
            f"状态驱动 `{rec.get('recommended_state_driver', '')}`，"
            f"推荐状态 `{rec.get('recommendation_status', '')}`。"
        )
    lines.extend(
        [
            "",
            "## 当前锚点定义的旧/新比值偏移",
            "",
            "| 标称 ppm | 当前证书 ppm | 旧点 | 当前点 | 旧 R_f | 当前 R_f | ΔR_f | 旧 H2O | 当前 H2O | ΔH2O | 旧露点 | 当前露点 | ΔT | ΔP |",
            "|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in anchor_rows:
        lines.append(
            "| {nom} | {target} | {old_point} | {cur_point} | {old_r} | {cur_r} | {dr} | {old_h} | {cur_h} | {dh} | {old_dp} | {cur_dp} | {dt} | {dp} |".format(
                nom=_fmt(row.get("nominal_ppm"), 0),
                target=_fmt(row.get("current_target_ppm"), 3),
                old_point=row.get("old_point", ""),
                cur_point=row.get("current_point", ""),
                old_r=_fmt(row.get("old_ratio_f"), 6),
                cur_r=_fmt(row.get("current_ratio_f"), 6),
                dr=_fmt(row.get("delta_ratio_f"), 6),
                old_h=_fmt(row.get("old_h2o_mmol_mol"), 3),
                cur_h=_fmt(row.get("current_h2o_mmol_mol"), 3),
                dh=_fmt(float(row.get("current_h2o_mmol_mol")) - float(row.get("old_h2o_mmol_mol")), 3),
                old_dp=_fmt(row.get("old_dewpoint_c"), 2),
                cur_dp=_fmt(row.get("current_dewpoint_c"), 2),
                dt=_fmt(float(row.get("current_chamber_temp_c")) - float(row.get("old_chamber_temp_c")), 3),
                dp=_fmt(float(row.get("current_pressure_hpa")) - float(row.get("old_pressure_hpa")), 3),
            )
        )
    lines.extend(
        [
            "",
            "## 同气点状态变量对 ΔR_f 的解释",
            "",
            "| 状态变量 | 锚点数 | 状态差最小值 | 状态差最大值 | 状态差均值 | ΔR_f 均值 | ΔR_f/状态单位 | 相关系数 | 物理意义 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in state_rows:
        lines.append(
            "| {label} | {count} | {minv} | {maxv} | {meanv} | {rmean} | {slope} | {corr} | {meaning} |".format(
                label=row.get("state_label", ""),
                count=_fmt(row.get("anchor_count"), 0),
                minv=_fmt(row.get("state_delta_min"), 6),
                maxv=_fmt(row.get("state_delta_max"), 6),
                meanv=_fmt(row.get("state_delta_mean"), 6),
                rmean=_fmt(row.get("ratio_delta_mean"), 6),
                slope=_fmt(row.get("ratio_delta_per_state_unit"), 9),
                corr=_fmt(row.get("correlation"), 3),
                meaning=row.get("physical_meaning", ""),
            )
        )
    lines.extend(
        [
            "",
            "## no-write 推荐候选",
            "",
            "| 推荐状态 | 推荐模型 | 状态驱动 | 当前最大误差 ppm | 当前最大误差 % | 数字最优模型 | 警告 | SENCO1 | SENCO3 |",
            "|---|---|---|---:|---:|---|---|---|---|",
        ]
    )
    for row in recommendation_rows:
        lines.append(
            "| {status} | {model} | {driver} | {maxppm} | {maxpct} | {numeric} | {warnings} | `{s1}` | `{s3}` |".format(
                status=row.get("recommendation_status", ""),
                model=row.get("recommended_bridge_model", ""),
                driver=row.get("recommended_state_driver", ""),
                maxppm=_fmt(row.get("recommended_current_max_abs_error_ppm"), 3),
                maxpct=_fmt(row.get("recommended_current_max_abs_relative_error_pct"), 3),
                numeric=row.get("numeric_best_bridge_model", ""),
                warnings=row.get("warnings", ""),
                s1=row.get("senco1_payload_scientific", ""),
                s3=row.get("senco3_payload_scientific", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 修复模型效果汇总",
            "",
            "| 桥接模型 | 状态驱动 | 评估集 | 点数 | 最大绝对误差 ppm | 最大相对误差 % | RMSE ppm | SENCO1 | SENCO3 |",
            "|---|---|---|---:|---:|---:|---:|---|---|",
        ]
    )
    for row in summary_rows:
        lines.append(
            "| {model} | {driver} | {eval_set} | {count} | {maxppm} | {maxpct} | {rmse} | `{s1}` | `{s3}` |".format(
                model=row.get("bridge_model", ""),
                driver=row.get("state_driver", ""),
                eval_set=row.get("eval_set", ""),
                count=_fmt(row.get("eval_count"), 0),
                maxppm=_fmt(row.get("max_abs_error_ppm"), 3),
                maxpct=_fmt(row.get("max_abs_relative_error_pct"), 3),
                rmse=_fmt(row.get("rmse_ppm"), 3),
                s1=row.get("senco1_payload_scientific", ""),
                s3=row.get("senco3_payload_scientific", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            "- 如果 `old_all_corrected_ratio` 明显优于 `old_all_original_ratio`，说明旧全温比值确实可被当前锚点定义的状态偏移解释。",
            "- 如果 `current_anchor_actual_ratio` 仍然误差较大，说明只靠这三个当前锚点修复旧全温曲面还不够，不能直接进入写入。",
            "- `affine_ratio_bridge` 优先级最高，因为它对应 ratio 零点和比例尺度变化；`quadratic_ratio_bridge_diagnostic` 只能作为三点诊断上限。",
            "- `state_*_delta_bridge` 使用同一气点的新旧 H2O、露点、温度或压力差异解释 ratio 偏移；它们用于判断偏移是否来自物理状态变化。",
            "- `target_linear_delta` 使用标准气目标值修复训练证据，适合解释旧数据，不适合作为未知样品运行时变换。",
        ]
    )
    return "\n".join(lines) + "\n"


def _current_summary(
    summary_rows: Sequence[Mapping[str, Any]],
    *,
    bridge_model: str,
) -> Mapping[str, Any]:
    for row in summary_rows:
        if (
            str(row.get("bridge_model") or "") == bridge_model
            and str(row.get("eval_set") or "") == "current_anchor_actual_ratio"
        ):
            return row
    return {}


def _write_review_contract_rows(
    *,
    manifest: Mapping[str, Any],
    recommendation_rows: Sequence[Mapping[str, Any]],
    summary_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Build a strict no-write gate contract for human pre-write review."""

    if not recommendation_rows:
        return [
            {
                "target_device_id": manifest.get("target_device_id", ""),
                "component": "co2",
                "write_gate_status": "blocked_no_recommendation",
                "candidate_write_allowed": False,
                "writes_coefficients": False,
                "boundary": manifest.get("boundary", ""),
            }
        ]

    rec = dict(recommendation_rows[0])
    recommended_model = str(rec.get("recommended_bridge_model") or "")
    recommended = _current_summary(summary_rows, bridge_model=recommended_model)
    raw_target = _current_summary(
        summary_rows,
        bridge_model="firmware_h2o_raw_target_no_ratio_repair",
    )
    matched_anchor_count = int(float(manifest.get("matched_anchor_count") or 0))
    uses_pressure_driver = str(rec.get("recommended_state_driver") or "") == "pressure_hpa"
    raw_target_error = float(raw_target.get("max_abs_relative_error_pct") or 1.0e12)
    recommended_error = float(recommended.get("max_abs_relative_error_pct") or 1.0e12)

    if str(rec.get("recommendation_status") or "").startswith("blocked"):
        gate_status = str(rec.get("recommendation_status"))
    elif matched_anchor_count <= 3:
        gate_status = "blocked_three_anchor_current_state_review_only"
    elif uses_pressure_driver:
        gate_status = "blocked_pressure_bridge_diagnostic_only"
    elif recommended_error > 1.5:
        gate_status = "blocked_current_anchor_error_too_large"
    else:
        gate_status = "ready_for_human_no_write_review"

    raw_target_status = (
        "diagnostic_failed_h2o_target_alone_not_sufficient"
        if raw_target_error > 1.5
        else "diagnostic_passed_h2o_target_alone_explains_current_anchors"
    )
    return [
        {
            "target_device_id": manifest.get("target_device_id", ""),
            "component": "co2",
            "write_gate_status": gate_status,
            "candidate_write_allowed": False,
            "writes_coefficients": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "recommended_bridge_model": recommended_model,
            "recommended_target_mode": rec.get("recommended_target_mode", ""),
            "recommended_state_driver": rec.get("recommended_state_driver", ""),
            "recommended_current_max_abs_error_ppm": recommended.get("max_abs_error_ppm", ""),
            "recommended_current_max_abs_relative_error_pct": recommended.get("max_abs_relative_error_pct", ""),
            "raw_target_no_ratio_status": raw_target_status,
            "raw_target_no_ratio_current_max_abs_error_ppm": raw_target.get("max_abs_error_ppm", ""),
            "raw_target_no_ratio_current_max_abs_relative_error_pct": raw_target.get("max_abs_relative_error_pct", ""),
            "old_point_count": manifest.get("old_point_count", ""),
            "current_anchor_count": manifest.get("current_anchor_count", ""),
            "matched_anchor_count": matched_anchor_count,
            "firmware_formula_contract": "raw_senco13_ppm_then_h2o_dry_basis_final_ppm_reviewed",
            "pressure_contract": "pressure_terms_frozen_senco9_independent",
            "ratio_evidence_contract": "old_factory_ratio_preserved_bridge_is_offline_review_only",
            "senco5_senco6_contract": "separate_output_layer_review_required_before_write",
            "senco1_payload_scientific": rec.get("senco1_payload_scientific", ""),
            "senco3_payload_scientific": rec.get("senco3_payload_scientific", ""),
            "warnings": rec.get("warnings", ""),
            "physical_reason": (
                "The current anchors show a state-dependent difference between old and current ratio evidence. "
                "Because only three current anchors are available and pressure is an independent SENCO9 input, "
                "this package is evidence for human review, not automatic write approval."
            ),
            "boundary": manifest.get("boundary", ""),
        }
    ]


def _fmt(value: Any, digits: int = 6) -> str:
    if value in (None, ""):
        return ""
    try:
        number = float(value)
    except Exception:
        return str(value)
    return f"{number:.{digits}f}"


def _markdown(tables: Mapping[str, Any]) -> str:
    """Render a clean Chinese no-write review report.

    This definition intentionally overrides the older report renderer above,
    which had historical mojibake strings.  The evidence tables are unchanged.
    """

    manifest = tables["manifest"]
    anchor_rows = list(tables["anchor_rows"])
    state_rows = list(tables["state_sensitivity_rows"])
    summary_rows = list(tables["summary_rows"])
    recommendation_rows = list(tables.get("recommendation_rows", []))
    write_review_contract_rows = list(tables.get("write_review_contract_rows", []))
    best_current = _best_rows(summary_rows, "current_anchor_actual_ratio")
    lines = [
        f"# ID{manifest['target_device_id']} CO2 状态归一化与系数候选评估（no-write）",
        "",
        "## 摘要",
        "",
        f"- 旧全温点数：`{manifest['old_point_count']}`。",
        f"- 当前锚点数：`{manifest['current_anchor_count']}`；成功匹配：`{manifest['matched_anchor_count']}`。",
        "- 本评估不打开 COM、不控制气路/水路、不写 SENCO。",
        "- 原始旧比值不被覆盖；所有 bridge/normalization 只用于离线系数候选评审。",
        "- 压力项冻结为 0，压力通道继续按独立 SENCO9 流程处理。",
    ]
    if best_current:
        best = best_current[0]
        lines.append(
            f"- 当前三点反推表现最好的模型是 `{best['bridge_model']}`："
            f"最大相对误差 `{_fmt(best.get('max_abs_relative_error_pct'), 3)}%`，"
            f"最大绝对误差 `{_fmt(best.get('max_abs_error_ppm'), 3)} ppm`。"
        )
    if recommendation_rows:
        rec = recommendation_rows[0]
        lines.append(
            f"- no-write 物理推荐候选：`{rec.get('recommended_bridge_model', '')}`，"
            f"目标合同 `{rec.get('recommended_target_mode', '')}`，"
            f"状态驱动 `{rec.get('recommended_state_driver', '')}`，"
            f"推荐状态 `{rec.get('recommendation_status', '')}`。"
        )
    lines.extend(
        [
            "",
            "## 当前锚点定义的旧/新状态差异",
            "",
            "| 标称 ppm | 当前证书 ppm | 旧点 | 当前点 | 旧 R_f | 当前 R_f | ΔR_f | 旧 H2O | 当前 H2O | ΔH2O | 旧露点 | 当前露点 | ΔT | ΔP |",
            "|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in anchor_rows:
        lines.append(
            "| {nom} | {target} | {old_point} | {cur_point} | {old_r} | {cur_r} | {dr} | {old_h} | {cur_h} | {dh} | {old_dp} | {cur_dp} | {dt} | {dp} |".format(
                nom=_fmt(row.get("nominal_ppm"), 0),
                target=_fmt(row.get("current_target_ppm"), 3),
                old_point=row.get("old_point", ""),
                cur_point=row.get("current_point", ""),
                old_r=_fmt(row.get("old_ratio_f"), 6),
                cur_r=_fmt(row.get("current_ratio_f"), 6),
                dr=_fmt(row.get("delta_ratio_f"), 6),
                old_h=_fmt(row.get("old_h2o_mmol_mol"), 3),
                cur_h=_fmt(row.get("current_h2o_mmol_mol"), 3),
                dh=_fmt(float(row.get("current_h2o_mmol_mol")) - float(row.get("old_h2o_mmol_mol")), 3),
                old_dp=_fmt(row.get("old_dewpoint_c"), 2),
                cur_dp=_fmt(row.get("current_dewpoint_c"), 2),
                dt=_fmt(float(row.get("current_chamber_temp_c")) - float(row.get("old_chamber_temp_c")), 3),
                dp=_fmt(float(row.get("current_pressure_hpa")) - float(row.get("old_pressure_hpa")), 3),
            )
        )
    lines.extend(
        [
            "",
            "## 同气点状态变量对 ΔR_f 的解释",
            "",
            "| 状态变量 | 锚点数 | 状态差最小值 | 状态差最大值 | 状态差均值 | ΔR_f 均值 | ΔR_f/状态单位 | 相关系数 | 物理意义 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in state_rows:
        lines.append(
            "| {label} | {count} | {minv} | {maxv} | {meanv} | {rmean} | {slope} | {corr} | {meaning} |".format(
                label=row.get("state_label", ""),
                count=_fmt(row.get("anchor_count"), 0),
                minv=_fmt(row.get("state_delta_min"), 6),
                maxv=_fmt(row.get("state_delta_max"), 6),
                meanv=_fmt(row.get("state_delta_mean"), 6),
                rmean=_fmt(row.get("ratio_delta_mean"), 6),
                slope=_fmt(row.get("ratio_delta_per_state_unit"), 9),
                corr=_fmt(row.get("correlation"), 3),
                meaning=row.get("physical_meaning", ""),
            )
        )
    lines.extend(
        [
            "",
            "## no-write 推荐候选",
            "",
            "| 推荐状态 | 推荐模型 | 目标合同 | 状态驱动 | 当前最大误差 ppm | 当前最大误差 % | 数字最优模型 | 警告 | SENCO1 | SENCO3 |",
            "|---|---|---|---|---:|---:|---|---|---|---|",
        ]
    )
    for row in recommendation_rows:
        lines.append(
            "| {status} | {model} | {target_mode} | {driver} | {maxppm} | {maxpct} | {numeric} | {warnings} | `{s1}` | `{s3}` |".format(
                status=row.get("recommendation_status", ""),
                model=row.get("recommended_bridge_model", ""),
                target_mode=row.get("recommended_target_mode", ""),
                driver=row.get("recommended_state_driver", ""),
                maxppm=_fmt(row.get("recommended_current_max_abs_error_ppm"), 3),
                maxpct=_fmt(row.get("recommended_current_max_abs_relative_error_pct"), 3),
                numeric=row.get("numeric_best_bridge_model", ""),
                warnings=row.get("warnings", ""),
                s1=row.get("senco1_payload_scientific", ""),
                s3=row.get("senco3_payload_scientific", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 写入前合同门禁",
            "",
            "| 门禁状态 | 写入允许 | 推荐模型 | 原始比值合同 | 压力合同 | H2O raw-target 对照 | 物理说明 |",
            "|---|---:|---|---|---|---|---|",
        ]
    )
    for row in write_review_contract_rows:
        lines.append(
            "| {status} | {allowed} | {model} | {ratio} | {pressure} | {raw_status} | {reason} |".format(
                status=row.get("write_gate_status", ""),
                allowed=row.get("candidate_write_allowed", False),
                model=row.get("recommended_bridge_model", ""),
                ratio=row.get("ratio_evidence_contract", ""),
                pressure=row.get("pressure_contract", ""),
                raw_status=row.get("raw_target_no_ratio_status", ""),
                reason=row.get("physical_reason", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 模型效果汇总",
            "",
            "| 桥接/归一化模型 | 目标合同 | 状态驱动 | 评估集 | 点数 | 最大绝对误差 ppm | 最大相对误差 % | RMSE ppm | SENCO1 | SENCO3 |",
            "|---|---|---|---|---:|---:|---:|---:|---|---|",
        ]
    )
    for row in summary_rows:
        lines.append(
            "| {model} | {target_mode} | {driver} | {eval_set} | {count} | {maxppm} | {maxpct} | {rmse} | `{s1}` | `{s3}` |".format(
                model=row.get("bridge_model", ""),
                target_mode=row.get("target_mode", ""),
                driver=row.get("state_driver", ""),
                eval_set=row.get("eval_set", ""),
                count=_fmt(row.get("eval_count"), 0),
                maxppm=_fmt(row.get("max_abs_error_ppm"), 3),
                maxpct=_fmt(row.get("max_abs_relative_error_pct"), 3),
                rmse=_fmt(row.get("rmse_ppm"), 3),
                s1=row.get("senco1_payload_scientific", ""),
                s3=row.get("senco3_payload_scientific", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            "- `firmware_h2o_raw_target_no_ratio_repair` 不改变旧比值，只把最终 CO2 证书目标换算为 SENCO1/SENCO3 原始层应拟合的目标；如果该项仍不合格，说明问题不只是固件 H2O 干基修正层。",
            "- bridge/normalization 是离线候选系数评审的状态域归一化，不是覆盖原始旧比值；原始 R/T/P/H2O 仍作为证据保留。",
            "- 如果 `current_anchor_actual_ratio` 仍然误差较大，说明只靠当前三个锚点解释旧全温曲面还不够，不能直接进入写入。",
            "- `state_*_delta_bridge` 使用同一气点的新旧 H2O、露点、温度或压力差异解释 ratio 偏移；它们用于判断偏移是否来自物理状态变化。",
            "- 压力模型即便数值最优也只能作为诊断，因为 V1.5 已把压力通道通过 SENCO9 独立校准和验证。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_co2_anchor_ratio_repair_report(
    *,
    old_run_dir: str | Path,
    current_sample_files: Sequence[str | Path],
    output_dir: str | Path,
    target_device_id: str,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_anchor_ratio_repair_tables(
        old_run_dir=old_run_dir,
        current_sample_files=current_sample_files,
        target_device_id=target_device_id,
    )
    outputs: Dict[str, Path] = {}
    manifest_path = output / "co2_anchor_ratio_repair_manifest.json"
    manifest_path.write_text(json.dumps(tables["manifest"], ensure_ascii=False, indent=2), encoding="utf-8-sig")
    outputs["manifest_json"] = manifest_path
    for name, rows in (
        ("co2_anchor_ratio_repair_anchors", tables["anchor_rows"]),
        ("co2_anchor_ratio_repair_state_sensitivity", tables["state_sensitivity_rows"]),
        ("co2_anchor_ratio_repair_recommendation", tables["recommendation_rows"]),
        ("co2_anchor_ratio_repair_write_review_contract", tables["write_review_contract_rows"]),
        ("co2_anchor_ratio_repair_summary", tables["summary_rows"]),
        ("co2_anchor_ratio_repair_coefficients", tables["coefficient_rows"]),
        ("co2_anchor_ratio_repair_predictions", tables["prediction_rows"]),
    ):
        path = output / f"{name}.csv"
        _write_csv(path, [{key: _fmt_float(value) for key, value in row.items()} for row in rows])
        outputs[f"{name}_csv"] = path
    markdown_path = output / "co2_anchor_ratio_repair_report.md"
    markdown_path.write_text(_markdown(tables), encoding="utf-8-sig")
    outputs["markdown"] = markdown_path
    return outputs
