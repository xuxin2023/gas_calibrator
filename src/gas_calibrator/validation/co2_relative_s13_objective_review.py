"""Offline CO2 SENCO1/SENCO3 relative-error objective review.

This review compares S1/S3 fitting objectives for already-recorded V1.5
open-flow CO2 evidence. It is deliberately no-write: it never opens COM ports,
controls gas/water routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np

from ..senco_format import format_senco_values
from .co2_fit_algorithm_matrix import (
    CENTERABLE_TERMS,
    MODEL_VARIANTS,
    FitPoint,
    ModelVariant,
    _centered_matrix,
    _centered_to_absolute,
    _display_prediction_from_raw,
    _h2o_dry_basis_factor,
    _load_fit_points,
    _matrix,
    _preserved_pressure_offset,
    _raw_senco13_fit_target,
    _relative_error_percent,
    _safe_float,
    _temperature_value,
)
from .co2_zero_s5_sensitivity_review import DEFAULT_ZERO_OFFSETS_PPM


DEFAULT_MODEL_ID = "senco13_temperature_terms_pressure_zero"
DEFAULT_OBJECTIVES = (
    "absolute_lstsq",
    "relative_weighted_lstsq",
    "low_end_priority_lstsq",
    "relative_irls_lstsq",
)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    headers: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _model_variant(model_id: str) -> ModelVariant:
    for variant in MODEL_VARIANTS:
        if variant.model_id == model_id:
            return variant
    raise ValueError(f"Unknown CO2 model variant: {model_id}")


def _is_zero_gas_anchor(point: FitPoint) -> bool:
    marker = str(point.zero_anchor_class or "").lower()
    return abs(float(point.target_ppm)) <= 1.0e-9 or "zero" in marker


def _apply_zero_offset(points: Sequence[FitPoint], zero_offset_ppm: float) -> List[FitPoint]:
    adjusted: List[FitPoint] = []
    for point in points:
        if _is_zero_gas_anchor(point):
            adjusted.append(
                replace(
                    point,
                    target_ppm=float(zero_offset_ppm),
                    target_uncertainty_ppm=(
                        point.target_uncertainty_ppm
                        if point.target_uncertainty_ppm is not None
                        else abs(float(zero_offset_ppm))
                    ),
                    zero_anchor_class=point.zero_anchor_class or "estimated_zero_anchor",
                )
            )
        else:
            adjusted.append(point)
    return adjusted


def _group_by_device(points: Sequence[FitPoint]) -> Dict[str, List[FitPoint]]:
    grouped: Dict[str, List[FitPoint]] = {}
    for point in points:
        grouped.setdefault(point.device_id, []).append(point)
    return grouped


def _weighted_scaled_lstsq(
    matrix: np.ndarray,
    target: np.ndarray,
    weights: np.ndarray,
) -> tuple[np.ndarray, int, float]:
    weights = np.asarray(weights, dtype=float)
    weights = np.where(np.isfinite(weights) & (weights > 0.0), weights, 1.0)
    weighted_matrix = matrix * weights[:, None]
    weighted_target = target * weights
    scales = np.linalg.norm(weighted_matrix, axis=0)
    scales = np.where(np.isfinite(scales) & (scales > 0.0), scales, 1.0)
    scaled = weighted_matrix / scales
    rank = int(np.linalg.matrix_rank(scaled))
    condition = float(np.linalg.cond(scaled))
    fitted, *_ = np.linalg.lstsq(scaled, weighted_target, rcond=None)
    return np.asarray(fitted, dtype=float) / scales, rank, condition


def _base_relative_weights(
    points: Sequence[FitPoint],
    *,
    min_relative_target_ppm: float,
    low_end_target_ppm: float,
    low_end_multiplier: float,
    objective_id: str,
) -> np.ndarray:
    weights: List[float] = []
    for point in points:
        target = abs(float(point.target_ppm))
        if objective_id == "absolute_lstsq":
            weight = 1.0
        else:
            weight = 1.0 / max(target, float(min_relative_target_ppm))
            if objective_id == "low_end_priority_lstsq" and 0.0 < target <= float(low_end_target_ppm):
                weight *= float(low_end_multiplier)
        weights.append(weight)
    return np.asarray(weights, dtype=float)


def _fit_weighted_coefficients(
    fit_points: Sequence[FitPoint],
    *,
    variant: ModelVariant,
    target: np.ndarray,
    weights: np.ndarray,
) -> tuple[np.ndarray, int, float, float, str]:
    absolute = _matrix(fit_points, variant)
    _, absolute_rank, absolute_condition = _weighted_scaled_lstsq(absolute, target, weights)
    if not set(variant.terms).issubset(CENTERABLE_TERMS):
        coeffs, rank, condition = _weighted_scaled_lstsq(absolute, target, weights)
        return coeffs, rank, condition, absolute_condition, "absolute_firmware_terms_weighted"

    ratio_center = float(np.mean([point.ratio for point in fit_points])) if fit_points else 0.0
    temp_center = float(np.mean([_temperature_value(point, variant) for point in fit_points])) if fit_points else 273.15
    centered = _centered_matrix(
        fit_points,
        variant,
        ratio_center=ratio_center,
        temp_center=temp_center,
    )
    centered_coefficients, centered_rank, centered_condition = _weighted_scaled_lstsq(
        centered,
        target,
        weights,
    )
    absolute_coefficients = _centered_to_absolute(
        variant.terms,
        centered_coefficients,
        ratio_center=ratio_center,
        temp_center=temp_center,
    )
    return (
        absolute_coefficients,
        centered_rank,
        centered_condition,
        absolute_condition,
        "centered_R_T_weighted_transformed_to_firmware_absolute_terms",
    )


def _predict_rows(
    points: Sequence[FitPoint],
    *,
    variant: ModelVariant,
    coeff_array: np.ndarray,
    objective_id: str,
    zero_offset_ppm: float,
    old_secondary: Sequence[float] = (),
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for point in points:
        raw_pred = float(_matrix([point], variant)[0] @ coeff_array)
        raw_pred += _preserved_pressure_offset(point, variant, old_secondary)
        factor = _h2o_dry_basis_factor(point, variant)
        display_pred = _display_prediction_from_raw(point, variant, raw_pred)
        raw_target = float(point.target_ppm) * factor
        error = display_pred - float(point.target_ppm)
        relative_error = _relative_error_percent(error, float(point.target_ppm))
        rows.append(
            {
                "device_id": point.device_id,
                "objective_id": objective_id,
                "zero_offset_ppm": float(zero_offset_ppm),
                "model_id": variant.model_id,
                "source_role": point.source_role,
                "point_identity": point.point_identity,
                "target_ppm": point.target_ppm,
                "zero_anchor_class": point.zero_anchor_class,
                "prediction_ppm": display_pred,
                "error_ppm": error,
                "relative_error_percent": relative_error if relative_error is not None else "",
                "raw_senco13_target_ppm": raw_target,
                "raw_senco13_prediction_ppm": raw_pred,
                "ratio": point.ratio,
                "temperature_c": point.temperature_c,
                "pressure_hpa": point.pressure_hpa,
                "h2o_mmol": point.h2o_mmol if point.h2o_mmol is not None else "",
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            }
        )
    return rows


def _metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_relative_target_ppm: float,
    low_end_target_ppm: float,
) -> Dict[str, Any]:
    errors: List[float] = []
    relative_errors: List[float] = []
    low_end_relative_errors: List[float] = []
    zero_abs_errors: List[float] = []
    for row in rows:
        target = _safe_float(row.get("target_ppm"))
        prediction = _safe_float(row.get("prediction_ppm"))
        if target is None or prediction is None:
            continue
        error = float(prediction) - float(target)
        errors.append(error)
        marker = str(row.get("zero_anchor_class") or "").lower()
        if abs(float(target)) >= float(min_relative_target_ppm):
            rel = 100.0 * error / float(target)
            relative_errors.append(rel)
            if float(target) <= float(low_end_target_ppm):
                low_end_relative_errors.append(rel)
        elif "zero" in marker or abs(float(target)) < float(min_relative_target_ppm):
            zero_abs_errors.append(abs(error))
    return {
        "point_count": len(errors),
        "relative_point_count": len(relative_errors),
        "rmse_ppm": math.sqrt(sum(item * item for item in errors) / len(errors)) if errors else "",
        "max_abs_error_ppm": max(abs(item) for item in errors) if errors else "",
        "mean_error_ppm": sum(errors) / len(errors) if errors else "",
        "max_abs_relative_error_percent": max(abs(item) for item in relative_errors) if relative_errors else "",
        "mean_abs_relative_error_percent": (
            sum(abs(item) for item in relative_errors) / len(relative_errors) if relative_errors else ""
        ),
        "low_end_max_abs_relative_error_percent": (
            max(abs(item) for item in low_end_relative_errors) if low_end_relative_errors else ""
        ),
        "zero_anchor_max_abs_error_ppm": max(zero_abs_errors) if zero_abs_errors else "",
    }


def _score_for_selection(row: Mapping[str, Any]) -> tuple[float, float, float]:
    max_rel = _safe_float(row.get("max_abs_relative_error_percent"))
    low_rel = _safe_float(row.get("low_end_max_abs_relative_error_percent"))
    rmse = _safe_float(row.get("rmse_ppm"))
    return (
        float(max_rel) if max_rel is not None else float("inf"),
        float(low_rel) if low_rel is not None else float("inf"),
        float(rmse) if rmse is not None else float("inf"),
    )


def _payloads(coeffs: Mapping[str, float]) -> Dict[str, Any]:
    primary = [
        float(coeffs.get("intercept", 0.0)),
        float(coeffs.get("R", 0.0)),
        float(coeffs.get("R2", 0.0)),
        float(coeffs.get("R3", 0.0)),
    ]
    secondary = [
        float(coeffs.get("T", 0.0)),
        float(coeffs.get("T2", 0.0)),
        float(coeffs.get("RT", 0.0)),
        0.0,
        0.0,
        0.0,
    ]
    return {
        "s1_payload": json.dumps(primary, ensure_ascii=False, separators=(",", ":")),
        "s3_payload": json.dumps(secondary, ensure_ascii=False, separators=(",", ":")),
        "s1_payload_scientific": ",".join(format_senco_values(primary)),
        "s3_payload_scientific": ",".join(format_senco_values(secondary)),
    }


def _fit_objective(
    points: Sequence[FitPoint],
    *,
    variant: ModelVariant,
    objective_id: str,
    zero_offset_ppm: float,
    min_relative_target_ppm: float,
    low_end_target_ppm: float,
    low_end_multiplier: float,
    irls_iterations: int,
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    fit_points = [point for point in points if point.source_role == "fit"]
    if not fit_points:
        return {}, []
    target = np.asarray([_raw_senco13_fit_target(point, variant, ()) for point in fit_points], dtype=float)
    weights = _base_relative_weights(
        fit_points,
        min_relative_target_ppm=float(min_relative_target_ppm),
        low_end_target_ppm=float(low_end_target_ppm),
        low_end_multiplier=float(low_end_multiplier),
        objective_id="relative_weighted_lstsq" if objective_id == "relative_irls_lstsq" else objective_id,
    )
    coeff_array: Optional[np.ndarray] = None
    rank = 0
    condition = float("inf")
    absolute_condition = float("inf")
    fit_basis = ""
    if objective_id == "relative_irls_lstsq":
        base = weights.copy()
        for _ in range(max(1, int(irls_iterations))):
            coeff_array, rank, condition, absolute_condition, fit_basis = _fit_weighted_coefficients(
                fit_points,
                variant=variant,
                target=target,
                weights=weights,
            )
            fit_rows = _predict_rows(
                fit_points,
                variant=variant,
                coeff_array=coeff_array,
                objective_id=objective_id,
                zero_offset_ppm=zero_offset_ppm,
            )
            relative = []
            for row in fit_rows:
                target_ppm = _safe_float(row.get("target_ppm"))
                rel = _safe_float(row.get("relative_error_percent"))
                if target_ppm is not None and target_ppm >= min_relative_target_ppm and rel is not None:
                    relative.append(abs(float(rel)))
            if not relative:
                break
            median = float(np.median(relative)) or 1.0
            reweight = []
            for point, old_weight in zip(fit_points, base):
                target_ppm = abs(float(point.target_ppm))
                pred = float(_matrix([point], variant)[0] @ coeff_array)
                error = pred - float(point.target_ppm)
                rel = abs(100.0 * error / target_ppm) if target_ppm >= min_relative_target_ppm else 0.0
                reweight.append(float(old_weight) * math.sqrt(1.0 + rel / median))
            weights = np.asarray(reweight, dtype=float)
    else:
        coeff_array, rank, condition, absolute_condition, fit_basis = _fit_weighted_coefficients(
            fit_points,
            variant=variant,
            target=target,
            weights=weights,
        )
    if coeff_array is None:
        return {}, []
    coeffs = {term: float(value) for term, value in zip(variant.terms, coeff_array)}
    rows = _predict_rows(
        points,
        variant=variant,
        coeff_array=coeff_array,
        objective_id=objective_id,
        zero_offset_ppm=zero_offset_ppm,
    )
    metrics = _metrics(
        rows,
        min_relative_target_ppm=float(min_relative_target_ppm),
        low_end_target_ppm=float(low_end_target_ppm),
    )
    summary = {
        "objective_id": objective_id,
        "zero_offset_ppm": float(zero_offset_ppm),
        "model_id": variant.model_id,
        "fit_point_count": len(fit_points),
        "matrix_rank": rank,
        "term_count": len(variant.terms),
        "condition_number_scaled": condition,
        "absolute_condition_number_scaled": absolute_condition,
        "fit_basis": fit_basis,
        "min_relative_target_ppm": float(min_relative_target_ppm),
        "low_end_target_ppm": float(low_end_target_ppm),
        "low_end_multiplier": float(low_end_multiplier) if objective_id == "low_end_priority_lstsq" else "",
        "irls_iterations": int(irls_iterations) if objective_id == "relative_irls_lstsq" else "",
        "max_abs_relative_error_percent": metrics["max_abs_relative_error_percent"],
        "mean_abs_relative_error_percent": metrics["mean_abs_relative_error_percent"],
        "low_end_max_abs_relative_error_percent": metrics["low_end_max_abs_relative_error_percent"],
        "zero_anchor_max_abs_error_ppm": metrics["zero_anchor_max_abs_error_ppm"],
        "max_abs_error_ppm": metrics["max_abs_error_ppm"],
        "rmse_ppm": metrics["rmse_ppm"],
        "mean_error_ppm": metrics["mean_error_ppm"],
        "relative_point_count": metrics["relative_point_count"],
        "point_count": metrics["point_count"],
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "auto_write_allowed": False,
    }
    summary.update(_payloads(coeffs))
    return summary, rows


def build_co2_relative_s13_objective_review(
    *,
    fit_residuals_csv: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    objectives: Sequence[str] = DEFAULT_OBJECTIVES,
    model_id: str = DEFAULT_MODEL_ID,
    min_relative_target_ppm: float = 50.0,
    low_end_target_ppm: float = 300.0,
    low_end_multiplier: float = 3.0,
    irls_iterations: int = 5,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build offline no-write S1/S3 objective comparison tables."""

    variant = _model_variant(model_id)
    points = _load_fit_points(
        fit_residuals_csv,
        exclude_device_ids=exclude_device_ids,
        treatment_plan_csv=fit_point_treatment_plan_csv,
    )
    by_device = _group_by_device(points)
    objective_set = tuple(dict.fromkeys(objectives))
    summary_rows: List[Dict[str, Any]] = []
    residual_rows: List[Dict[str, Any]] = []
    selected_rows: List[Dict[str, Any]] = []

    for device_id, device_points in sorted(by_device.items()):
        device_summaries: List[Dict[str, Any]] = []
        for zero_offset in zero_offsets_ppm:
            adjusted = _apply_zero_offset(device_points, float(zero_offset))
            for objective_id in objective_set:
                summary, rows = _fit_objective(
                    adjusted,
                    variant=variant,
                    objective_id=objective_id,
                    zero_offset_ppm=float(zero_offset),
                    min_relative_target_ppm=float(min_relative_target_ppm),
                    low_end_target_ppm=float(low_end_target_ppm),
                    low_end_multiplier=float(low_end_multiplier),
                    irls_iterations=int(irls_iterations),
                )
                if not summary:
                    continue
                summary["device_id"] = device_id
                summary["zero_anchor_count"] = sum(1 for point in adjusted if _is_zero_gas_anchor(point))
                summary["requires_zero_gas_traceability_review"] = any(
                    _is_zero_gas_anchor(point) and "estimated" in str(point.zero_anchor_class or "").lower()
                    for point in adjusted
                )
                summary["physical_meaning"] = _objective_physical_meaning(objective_id)
                device_summaries.append(summary)
                summary_rows.append(summary)
                residual_rows.extend(rows)
        if not device_summaries:
            continue
        baseline = next(
            (
                row
                for row in device_summaries
                if row.get("objective_id") == "absolute_lstsq" and float(row.get("zero_offset_ppm") or 0.0) == 0.0
            ),
            device_summaries[0],
        )
        best = min(device_summaries, key=_score_for_selection)
        selected_rows.append(
            {
                "device_id": device_id,
                "baseline_objective_id": baseline.get("objective_id"),
                "baseline_zero_offset_ppm": baseline.get("zero_offset_ppm"),
                "baseline_max_abs_relative_error_percent": baseline.get("max_abs_relative_error_percent"),
                "baseline_low_end_max_abs_relative_error_percent": baseline.get(
                    "low_end_max_abs_relative_error_percent"
                ),
                "best_objective_id": best.get("objective_id"),
                "best_zero_offset_ppm": best.get("zero_offset_ppm"),
                "best_max_abs_relative_error_percent": best.get("max_abs_relative_error_percent"),
                "best_low_end_max_abs_relative_error_percent": best.get(
                    "low_end_max_abs_relative_error_percent"
                ),
                "best_zero_anchor_max_abs_error_ppm": best.get("zero_anchor_max_abs_error_ppm"),
                "best_s1_payload_scientific": best.get("s1_payload_scientific"),
                "best_s3_payload_scientific": best.get("s3_payload_scientific"),
                "recommended_no_write_action": _recommend_action(baseline, best),
                "requires_zero_gas_traceability_review": best.get("requires_zero_gas_traceability_review"),
                "auto_write_allowed": False,
                "physical_meaning": (
                    "The selected objective is the best offline S1/S3 relative-error candidate. It is not a "
                    "write approval; zero-gas CO2 content must remain traceable before formal release."
                ),
            }
        )
    run_summary = [
        {
            "created_at": _now(),
            "fit_residuals_csv": str(Path(fit_residuals_csv).resolve()),
            "fit_point_treatment_plan_csv": (
                str(Path(fit_point_treatment_plan_csv).resolve()) if fit_point_treatment_plan_csv else ""
            ),
            "device_count": len(by_device),
            "scenario_count": len(summary_rows),
            "model_id": model_id,
            "objectives": ";".join(objective_set),
            "zero_offsets_ppm": ";".join(f"{float(value):g}" for value in zero_offsets_ppm),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        }
    ]
    return {
        "run_summary": run_summary,
        "objective_summary": summary_rows,
        "objective_residuals": residual_rows,
        "selected_candidates": selected_rows,
    }


def _objective_physical_meaning(objective_id: str) -> str:
    if objective_id == "absolute_lstsq":
        return "Minimizes ppm residuals. High-concentration points can dominate the fit."
    if objective_id == "relative_weighted_lstsq":
        return "Approximates relative-error minimization by scaling residuals by target concentration."
    if objective_id == "low_end_priority_lstsq":
        return "Adds extra low-end weight so 100-300 ppm points are not sacrificed by high ppm points."
    if objective_id == "relative_irls_lstsq":
        return "Iteratively increases weight on high relative-error points to reduce worst-case relative error."
    return "Unknown objective; review only."


def _recommend_action(baseline: Mapping[str, Any], best: Mapping[str, Any]) -> str:
    baseline_score = _safe_float(baseline.get("max_abs_relative_error_percent"))
    best_score = _safe_float(best.get("max_abs_relative_error_percent"))
    if baseline_score is None or best_score is None:
        return "review_manually_no_write"
    objective_changed = best.get("objective_id") != baseline.get("objective_id")
    zero_changed = float(best.get("zero_offset_ppm") or 0.0) != float(baseline.get("zero_offset_ppm") or 0.0)
    if best_score < baseline_score * 0.85 and objective_changed:
        return "review_relative_objective_s1s3_candidate"
    if best_score < baseline_score * 0.85 and zero_changed:
        return "review_zero_anchor_s1s3_candidate"
    if objective_changed:
        return "minor_improvement_review_before_write"
    return "keep_absolute_lstsq_and_investigate_residual_sources"


def write_co2_relative_s13_objective_review(
    *,
    fit_residuals_csv: str | Path,
    output_dir: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    objectives: Sequence[str] = DEFAULT_OBJECTIVES,
    model_id: str = DEFAULT_MODEL_ID,
    min_relative_target_ppm: float = 50.0,
    low_end_target_ppm: float = 300.0,
    low_end_multiplier: float = 3.0,
    irls_iterations: int = 5,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_relative_s13_objective_review(
        fit_residuals_csv=fit_residuals_csv,
        fit_point_treatment_plan_csv=fit_point_treatment_plan_csv,
        exclude_device_ids=exclude_device_ids,
        zero_offsets_ppm=zero_offsets_ppm,
        objectives=objectives,
        model_id=model_id,
        min_relative_target_ppm=min_relative_target_ppm,
        low_end_target_ppm=low_end_target_ppm,
        low_end_multiplier=low_end_multiplier,
        irls_iterations=irls_iterations,
    )
    outputs = {
        "run_summary": output / "co2_relative_s13_objective_run_summary.csv",
        "objective_summary": output / "co2_relative_s13_objective_summary.csv",
        "objective_residuals": output / "co2_relative_s13_objective_residuals.csv",
        "selected_candidates": output / "co2_relative_s13_objective_selected_candidates.csv",
        "metadata": output / "co2_relative_s13_objective_meta.json",
        "markdown": output / "co2_relative_s13_objective_review_zh.md",
    }
    for key in ("run_summary", "objective_summary", "objective_residuals", "selected_candidates"):
        _write_csv(outputs[key], tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_relative_s13_objective_review",
                "created_at": _now(),
                "inputs": {
                    "fit_residuals_csv": str(Path(fit_residuals_csv).resolve()),
                    "fit_point_treatment_plan_csv": (
                        str(Path(fit_point_treatment_plan_csv).resolve())
                        if fit_point_treatment_plan_csv
                        else ""
                    ),
                    "exclude_device_ids": list(exclude_device_ids),
                    "zero_offsets_ppm": list(zero_offsets_ppm),
                    "objectives": list(objectives),
                    "model_id": model_id,
                    "min_relative_target_ppm": min_relative_target_ppm,
                    "low_end_target_ppm": low_end_target_ppm,
                    "low_end_multiplier": low_end_multiplier,
                    "irls_iterations": irls_iterations,
                },
                "boundary": {
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
    _write_markdown(outputs["markdown"], tables)
    return outputs


def _fmt(value: Any, digits: int = 6) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}g}"


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    selected = list(tables.get("selected_candidates") or [])
    lines = [
        "# V1.5 CO2 S1/S3 相对误差目标审计",
        "",
        f"- 生成时间：{_now()}",
        "- 边界：离线 no-write；不打开 COM；不控制气路/水路；不写 SENCO。",
        "- 物理合同：CO2 主链路仍为滤波后 ratio + 温度项拟合 S1/S3；压力项不参与主校准拟合。",
        "- 审计目的：比较绝对 ppm 最小二乘与相对误差优先目标，避免高浓度点在数学上淹没低浓度点。",
        "- 零气说明：CO2 零气锚点只做零点灵敏度评审；无证书时不得直接作为正式释放证据。",
        "",
        "## 逐设备推荐",
        "",
        "| 设备ID | 基线最大相对误差% | 基线低端最大相对误差% | 最优目标 | 零点估计ppm | 最优最大相对误差% | 最优低端最大相对误差% | 建议 |",
        "| --- | ---: | ---: | --- | ---: | ---: | ---: | --- |",
    ]
    for row in selected:
        lines.append(
            "| {device} | {base_rel} | {base_low} | {objective} | {zero} | {best_rel} | {best_low} | {action} |".format(
                device=row.get("device_id", ""),
                base_rel=_fmt(row.get("baseline_max_abs_relative_error_percent"), 4),
                base_low=_fmt(row.get("baseline_low_end_max_abs_relative_error_percent"), 4),
                objective=row.get("best_objective_id", ""),
                zero=_fmt(row.get("best_zero_offset_ppm"), 4),
                best_rel=_fmt(row.get("best_max_abs_relative_error_percent"), 4),
                best_low=_fmt(row.get("best_low_end_max_abs_relative_error_percent"), 4),
                action=row.get("recommended_no_write_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 专业判断",
            "",
            "1. 如果相对误差优先目标显著优于绝对最小二乘，说明原拟合不是光学不可校，而是数学目标函数不适合低端精度要求。",
            "2. 如果相对误差目标仍不能明显改善，应优先追查零气 CO2 实际含量、点位物理状态、低端 ratio 稳定性、ref/signal 异常，而不是用 S5 硬拉。",
            "3. S5/S6 属于最终输出层线性修正。S1/S3 主链路没有合理前，不应让 S5 承担主拟合职责。",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8-sig")
