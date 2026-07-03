"""CO2 zero-anchor and SENCO5 output-layer sensitivity review.

This module is offline-only. It consumes already-recorded V1.5 open-flow CO2
fit evidence, refits the no-pressure SENCO1/SENCO3 contract under several
estimated zero-gas CO2 assumptions, and then evaluates SENCO5 as the final
displayed-concentration affine layer.

It never opens COM ports, controls routes, or writes SENCO values.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..senco_format import format_senco_values
from .co2_fit_algorithm_matrix import (
    MODEL_VARIANTS,
    FitPoint,
    ModelVariant,
    _fit_one,
    _load_fit_points,
    _safe_float,
)
from .co2_senco5_linear_trim_review import _fit_quantized_command_trim


DEFAULT_ZERO_OFFSETS_PPM = (0.0, 2.0, 5.0, 8.0, 10.0)
DEFAULT_MODEL_ID = "senco13_temperature_terms_pressure_zero"


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
    out: List[FitPoint] = []
    for point in points:
        if _is_zero_gas_anchor(point):
            out.append(
                replace(
                    point,
                    target_ppm=float(zero_offset_ppm),
                    target_uncertainty_ppm=(
                        point.target_uncertainty_ppm
                        if point.target_uncertainty_ppm is not None
                        else abs(float(zero_offset_ppm))
                    ),
                    zero_anchor_class=(
                        point.zero_anchor_class
                        if point.zero_anchor_class
                        else "estimated_zero_anchor"
                    ),
                )
            )
        else:
            out.append(point)
    return out


def _metric_target(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(row.get("target_ppm"))


def _metric_prediction(row: Mapping[str, Any], *, c0: float = 0.0, c1: float = 1.0) -> Optional[float]:
    value = _safe_float(row.get("prediction_ppm"))
    if value is None:
        return None
    return float(value) * float(c1) + float(c0)


def _is_relative_standard(row: Mapping[str, Any], *, min_target_ppm: float) -> bool:
    target = _metric_target(row)
    return target is not None and abs(float(target)) >= float(min_target_ppm)


def _is_zero_anchor_row(row: Mapping[str, Any], *, min_target_ppm: float) -> bool:
    target = _metric_target(row)
    marker = str(row.get("zero_anchor_class") or "").lower()
    return target is not None and (abs(float(target)) < float(min_target_ppm) or "zero" in marker)


def _metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    c0: float = 0.0,
    c1: float = 1.0,
    min_relative_target_ppm: float = 50.0,
) -> Dict[str, Any]:
    errors: List[float] = []
    rel_errors: List[float] = []
    low_rel_errors: List[float] = []
    zero_abs_errors: List[float] = []
    for row in rows:
        target = _metric_target(row)
        prediction = _metric_prediction(row, c0=c0, c1=c1)
        if target is None or prediction is None:
            continue
        error = float(prediction) - float(target)
        errors.append(error)
        if _is_relative_standard(row, min_target_ppm=min_relative_target_ppm):
            rel = error / float(target) * 100.0
            rel_errors.append(rel)
            if float(target) <= 300.0:
                low_rel_errors.append(rel)
        if _is_zero_anchor_row(row, min_target_ppm=min_relative_target_ppm):
            zero_abs_errors.append(abs(error))
    return {
        "point_count": len(errors),
        "relative_point_count": len(rel_errors),
        "rmse_ppm": math.sqrt(sum(value * value for value in errors) / len(errors)) if errors else "",
        "max_abs_error_ppm": max(abs(value) for value in errors) if errors else "",
        "mean_error_ppm": sum(errors) / len(errors) if errors else "",
        "max_abs_relative_error_percent": max(abs(value) for value in rel_errors) if rel_errors else "",
        "mean_abs_relative_error_percent": (
            sum(abs(value) for value in rel_errors) / len(rel_errors) if rel_errors else ""
        ),
        "low_end_max_abs_relative_error_percent": (
            max(abs(value) for value in low_rel_errors) if low_rel_errors else ""
        ),
        "zero_anchor_max_abs_error_ppm": max(zero_abs_errors) if zero_abs_errors else "",
    }


def _trim_input_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_relative_target_ppm: float,
) -> List[Dict[str, Any]]:
    trim_rows: List[Dict[str, Any]] = []
    for row in rows:
        target = _metric_target(row)
        measured = _safe_float(row.get("prediction_ppm"))
        if target is None or measured is None:
            continue
        if abs(float(target)) < float(min_relative_target_ppm):
            continue
        trim_rows.append({"_target": float(target), "_measured": float(measured)})
    return trim_rows


def _fit_s5_trim(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_relative_target_ppm: float,
    c0_decimals: int,
    c1_decimals: int,
    c1_min: float,
    c1_max: float,
) -> Dict[str, Any]:
    trim_rows = _trim_input_rows(rows, min_relative_target_ppm=min_relative_target_ppm)
    if len(trim_rows) < 2:
        return {
            "s5_status": "blocked_points_lt_2",
            "s5_C0": "",
            "s5_C1": "",
            "s5_command_preview": "",
        }
    c0, c1, max_abs_pct, max_abs_ppm, rmse_ppm = _fit_quantized_command_trim(
        trim_rows,
        c0_decimals=c0_decimals,
        c1_decimals=c1_decimals,
        c1_min=c1_min,
        c1_max=c1_max,
    )
    return {
        "s5_status": "reviewable_no_write",
        "s5_C0": float(c0),
        "s5_C1": float(c1),
        "s5_command_preview": (
            f"SENCO5,YGAS,FFF,{float(c0):.{int(c0_decimals)}f},{float(c1):.{int(c1_decimals)}f}"
        ),
        "s5_relative_fit_max_abs_error_percent": max_abs_pct,
        "s5_relative_fit_max_abs_error_ppm": max_abs_ppm,
        "s5_relative_fit_rmse_ppm": rmse_ppm,
    }


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


def _group_by_device(points: Sequence[FitPoint]) -> Dict[str, List[FitPoint]]:
    out: Dict[str, List[FitPoint]] = {}
    for point in points:
        out.setdefault(point.device_id, []).append(point)
    return out


def _format_float(value: Any, digits: int = 6) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}g}"


def _scenario_status(row: Mapping[str, Any], *, max_abs_c0_ppm: float, max_abs_c1_delta: float) -> str:
    c0 = _safe_float(row.get("s5_C0"))
    c1 = _safe_float(row.get("s5_C1"))
    if c0 is None or c1 is None:
        return "s1s3_only_reviewable_no_write"
    if abs(float(c0)) > float(max_abs_c0_ppm):
        return "blocked_s5_c0_exceeds_output_layer_scope"
    if abs(float(c1) - 1.0) > float(max_abs_c1_delta):
        return "blocked_s5_c1_exceeds_output_layer_scope"
    return "s1s3_plus_s5_reviewable_no_write"


def build_co2_zero_s5_sensitivity_review(
    *,
    fit_residuals_csv: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    model_id: str = DEFAULT_MODEL_ID,
    min_relative_target_ppm: float = 50.0,
    s5_c0_decimals: int = 3,
    s5_c1_decimals: int = 3,
    s5_c1_min: float = 0.90,
    s5_c1_max: float = 1.10,
    max_abs_s5_c0_ppm: float = 100.0,
    max_abs_s5_c1_delta: float = 0.10,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build no-write CO2 zero-anchor/SENCO5 review tables."""

    variant = _model_variant(model_id)
    points = _load_fit_points(
        fit_residuals_csv,
        exclude_device_ids=exclude_device_ids,
        treatment_plan_csv=fit_point_treatment_plan_csv,
    )
    by_device = _group_by_device(points)
    scenario_rows: List[Dict[str, Any]] = []
    residual_rows: List[Dict[str, Any]] = []
    selected_rows: List[Dict[str, Any]] = []

    for device_id, device_points in sorted(by_device.items()):
        device_scenarios: List[Dict[str, Any]] = []
        for zero_offset in zero_offsets_ppm:
            adjusted = _apply_zero_offset(device_points, float(zero_offset))
            coeffs, rank, condition, absolute_condition, fit_basis, predictions, fit_metrics, _ = _fit_one(
                adjusted,
                variant=variant,
                old_secondary=(),
            )
            if not predictions:
                continue
            base_metrics = _metrics(
                predictions,
                min_relative_target_ppm=float(min_relative_target_ppm),
            )
            trim = _fit_s5_trim(
                predictions,
                min_relative_target_ppm=float(min_relative_target_ppm),
                c0_decimals=int(s5_c0_decimals),
                c1_decimals=int(s5_c1_decimals),
                c1_min=float(s5_c1_min),
                c1_max=float(s5_c1_max),
            )
            c0 = float(_safe_float(trim.get("s5_C0")) or 0.0)
            c1 = float(_safe_float(trim.get("s5_C1")) or 1.0)
            s5_metrics = _metrics(
                predictions,
                c0=c0,
                c1=c1,
                min_relative_target_ppm=float(min_relative_target_ppm),
            )
            zero_anchor_count = sum(1 for point in adjusted if _is_zero_gas_anchor(point))
            payloads = _payloads(coeffs)
            scenario = {
                "device_id": device_id,
                "scenario_id": f"zero_offset_{float(zero_offset):g}_ppm",
                "zero_offset_ppm": float(zero_offset),
                "model_id": variant.model_id,
                "fit_point_count": fit_metrics.get("n", ""),
                "zero_anchor_count": zero_anchor_count,
                "relative_point_min_target_ppm": float(min_relative_target_ppm),
                "matrix_rank": rank,
                "term_count": len(variant.terms),
                "condition_number_scaled": condition,
                "absolute_condition_number_scaled": absolute_condition,
                "fit_basis": fit_basis,
                "s1s3_max_abs_relative_error_percent": base_metrics["max_abs_relative_error_percent"],
                "s1s3_mean_abs_relative_error_percent": base_metrics["mean_abs_relative_error_percent"],
                "s1s3_low_end_max_abs_relative_error_percent": base_metrics[
                    "low_end_max_abs_relative_error_percent"
                ],
                "s1s3_max_abs_error_ppm": base_metrics["max_abs_error_ppm"],
                "s1s3_rmse_ppm": base_metrics["rmse_ppm"],
                "s1s3_zero_anchor_max_abs_error_ppm": base_metrics["zero_anchor_max_abs_error_ppm"],
                **trim,
                "s5_max_abs_relative_error_percent": s5_metrics["max_abs_relative_error_percent"],
                "s5_mean_abs_relative_error_percent": s5_metrics["mean_abs_relative_error_percent"],
                "s5_low_end_max_abs_relative_error_percent": s5_metrics[
                    "low_end_max_abs_relative_error_percent"
                ],
                "s5_max_abs_error_ppm": s5_metrics["max_abs_error_ppm"],
                "s5_rmse_ppm": s5_metrics["rmse_ppm"],
                "s5_zero_anchor_max_abs_error_ppm": s5_metrics["zero_anchor_max_abs_error_ppm"],
                **payloads,
                "scenario_status": "",
                "physical_meaning": (
                    "Zero offset is an estimated CO2-zero sensitivity assumption; S1/S3 remains no-pressure "
                    "open-flow R/T fit, and S5 is only the final displayed CO2 affine layer."
                ),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            }
            scenario["scenario_status"] = _scenario_status(
                scenario,
                max_abs_c0_ppm=float(max_abs_s5_c0_ppm),
                max_abs_c1_delta=float(max_abs_s5_c1_delta),
            )
            scenario_rows.append(scenario)
            device_scenarios.append(scenario)

            for row in predictions:
                target = _metric_target(row)
                prediction = _safe_float(row.get("prediction_ppm"))
                corrected = _metric_prediction(row, c0=c0, c1=c1)
                if target is None or prediction is None or corrected is None:
                    continue
                residual_rows.append(
                    {
                        "device_id": device_id,
                        "scenario_id": scenario["scenario_id"],
                        "point_identity": row.get("point_identity", ""),
                        "source_role": row.get("source_role", ""),
                        "target_ppm": float(target),
                        "zero_anchor_class": row.get("zero_anchor_class", ""),
                        "ratio": row.get("ratio", ""),
                        "temperature_c": row.get("temperature_c", ""),
                        "pressure_hpa": row.get("pressure_hpa", ""),
                        "prediction_ppm": float(prediction),
                        "error_ppm": float(prediction) - float(target),
                        "relative_error_percent": (
                            (float(prediction) - float(target)) / float(target) * 100.0
                            if _is_relative_standard(row, min_target_ppm=float(min_relative_target_ppm))
                            else ""
                        ),
                        "s5_C0": c0,
                        "s5_C1": c1,
                        "s5_corrected_ppm": float(corrected),
                        "s5_error_ppm": float(corrected) - float(target),
                        "s5_relative_error_percent": (
                            (float(corrected) - float(target)) / float(target) * 100.0
                            if _is_relative_standard(row, min_target_ppm=float(min_relative_target_ppm))
                            else ""
                        ),
                    }
                )

        if device_scenarios:
            best_s1s3 = min(
                device_scenarios,
                key=lambda row: (
                    float(_safe_float(row.get("s1s3_max_abs_relative_error_percent")) or math.inf),
                    float(_safe_float(row.get("s1s3_max_abs_error_ppm")) or math.inf),
                    abs(float(_safe_float(row.get("zero_offset_ppm")) or 0.0)),
                ),
            )
            best_s5 = min(
                device_scenarios,
                key=lambda row: (
                    float(_safe_float(row.get("s5_max_abs_relative_error_percent")) or math.inf),
                    float(_safe_float(row.get("s5_max_abs_error_ppm")) or math.inf),
                    abs(float(_safe_float(row.get("s5_C1")) or 1.0) - 1.0),
                    abs(float(_safe_float(row.get("s5_C0")) or 0.0)),
                ),
            )
            baseline = next(
                (row for row in device_scenarios if abs(float(row.get("zero_offset_ppm") or 0.0)) <= 1e-9),
                device_scenarios[0],
            )
            baseline_rel = _safe_float(baseline.get("s1s3_max_abs_relative_error_percent"))
            best_s1s3_rel = _safe_float(best_s1s3.get("s1s3_max_abs_relative_error_percent"))
            best_s5_rel = _safe_float(best_s5.get("s5_max_abs_relative_error_percent"))
            selected_rows.append(
                {
                    "device_id": device_id,
                    "baseline_zero0_s1s3_max_abs_relative_error_percent": baseline_rel if baseline_rel is not None else "",
                    "best_s1s3_scenario_id": best_s1s3["scenario_id"],
                    "best_s1s3_zero_offset_ppm": best_s1s3["zero_offset_ppm"],
                    "best_s1s3_max_abs_relative_error_percent": best_s1s3_rel if best_s1s3_rel is not None else "",
                    "best_s5_scenario_id": best_s5["scenario_id"],
                    "best_s5_zero_offset_ppm": best_s5["zero_offset_ppm"],
                    "best_s5_C0": best_s5.get("s5_C0", ""),
                    "best_s5_C1": best_s5.get("s5_C1", ""),
                    "best_s5_command_preview": best_s5.get("s5_command_preview", ""),
                    "best_s5_max_abs_relative_error_percent": best_s5_rel if best_s5_rel is not None else "",
                    "recommended_no_write_action": (
                        "review_s1s3_plus_s5_output_layer"
                        if best_s5_rel is not None
                        and best_s1s3_rel is not None
                        and best_s5_rel < best_s1s3_rel
                        else "review_s1s3_zero_anchor_sensitivity_only"
                    ),
                    "requires_zero_gas_traceability_review": (
                        abs(float(best_s1s3.get("zero_offset_ppm") or 0.0)) > 1e-9
                        or abs(float(best_s5.get("zero_offset_ppm") or 0.0)) > 1e-9
                    ),
                    "auto_write_allowed": False,
                    "physical_meaning": (
                        "Selection minimizes non-zero standard-gas relative error. Zero-gas offset remains an "
                        "estimated sensitivity assumption unless the zero-gas CO2 value is certificate-backed."
                    ),
                }
            )

    run_summary = [
        {
            "created_at": _now(),
            "source_csv": str(Path(fit_residuals_csv).resolve()),
            "fit_point_treatment_plan_csv": (
                str(Path(fit_point_treatment_plan_csv).resolve())
                if fit_point_treatment_plan_csv
                else ""
            ),
            "model_id": model_id,
            "zero_offsets_ppm": ";".join(f"{float(item):g}" for item in zero_offsets_ppm),
            "min_relative_target_ppm": float(min_relative_target_ppm),
            "review_scope": "co2_senco13_zero_anchor_and_senco5_output_layer_sensitivity",
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        }
    ]
    return {
        "run_summary": run_summary,
        "scenario_summary": scenario_rows,
        "scenario_residuals": residual_rows,
        "selected_candidates": selected_rows,
    }


def write_co2_zero_s5_sensitivity_review(
    *,
    fit_residuals_csv: str | Path,
    output_dir: str | Path,
    fit_point_treatment_plan_csv: str | Path | None = None,
    exclude_device_ids: Sequence[str] = (),
    zero_offsets_ppm: Sequence[float] = DEFAULT_ZERO_OFFSETS_PPM,
    model_id: str = DEFAULT_MODEL_ID,
    min_relative_target_ppm: float = 50.0,
    s5_c0_decimals: int = 3,
    s5_c1_decimals: int = 3,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_zero_s5_sensitivity_review(
        fit_residuals_csv=fit_residuals_csv,
        fit_point_treatment_plan_csv=fit_point_treatment_plan_csv,
        exclude_device_ids=exclude_device_ids,
        zero_offsets_ppm=zero_offsets_ppm,
        model_id=model_id,
        min_relative_target_ppm=min_relative_target_ppm,
        s5_c0_decimals=s5_c0_decimals,
        s5_c1_decimals=s5_c1_decimals,
    )
    outputs = {
        "run_summary": output / "co2_zero_s5_sensitivity_run_summary.csv",
        "scenario_summary": output / "co2_zero_s5_sensitivity_scenario_summary.csv",
        "scenario_residuals": output / "co2_zero_s5_sensitivity_residuals.csv",
        "selected_candidates": output / "co2_zero_s5_sensitivity_selected_candidates.csv",
        "metadata": output / "co2_zero_s5_sensitivity_meta.json",
        "markdown": output / "co2_zero_s5_sensitivity_review_zh.md",
    }
    for key in ("run_summary", "scenario_summary", "scenario_residuals", "selected_candidates"):
        _write_csv(outputs[key], tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_zero_s5_sensitivity_review",
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
                    "model_id": model_id,
                    "min_relative_target_ppm": min_relative_target_ppm,
                    "s5_c0_decimals": s5_c0_decimals,
                    "s5_c1_decimals": s5_c1_decimals,
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


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    selected = list(tables.get("selected_candidates") or [])
    scenarios = list(tables.get("scenario_summary") or [])
    lines = [
        "# V1.5 CO2 零点锚定与 SENCO5 输出层灵敏度评审",
        "",
        f"- 生成时间：{_now()}",
        "- 边界：离线 no-write；不打开 COM；不控制气路/水路；不写 SENCO。",
        "- 物理合同：CO2 主链路使用滤波后 CO2 ratio 与温度项拟合 SENCO1/SENCO3；压力项冻结为 0，压力由 SENCO9 独立处理。",
        "- 零气处理：零气 CO2 含量没有证书时，只作为估计零点灵敏度，不作为正式证书值。",
        "- S5 处理：SENCO5 仅是最终显示 CO2 的线性层 `CO2_display*C1 + C0`，不能替代 S1/S3 主拟合，也不能掩盖气路或光学异常。",
        "",
        "## 推荐候选",
        "",
        "| 设备ID | 0ppm 基线 S1/S3 最大相对误差% | 最佳 S1/S3 场景 | 最佳 S1/S3 最大相对误差% | 最佳 S5 场景 | S5 C0 | S5 C1 | S5 最大相对误差% | 建议 |",
        "| --- | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |",
    ]
    for row in selected:
        lines.append(
            "| {device} | {base} | {s13_scene} | {s13_rel} | {s5_scene} | {c0} | {c1} | {s5_rel} | {action} |".format(
                device=row.get("device_id", ""),
                base=_format_float(row.get("baseline_zero0_s1s3_max_abs_relative_error_percent")),
                s13_scene=row.get("best_s1s3_scenario_id", ""),
                s13_rel=_format_float(row.get("best_s1s3_max_abs_relative_error_percent")),
                s5_scene=row.get("best_s5_scenario_id", ""),
                c0=_format_float(row.get("best_s5_C0")),
                c1=_format_float(row.get("best_s5_C1")),
                s5_rel=_format_float(row.get("best_s5_max_abs_relative_error_percent")),
                action=row.get("recommended_no_write_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 场景明细",
            "",
            "| 设备ID | 零气假设 ppm | S1/S3 最大相对误差% | S1/S3 低端最大相对误差% | S5 C0 | S5 C1 | S5 最大相对误差% | 状态 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in scenarios:
        lines.append(
            "| {device} | {zero} | {s13_rel} | {s13_low} | {c0} | {c1} | {s5_rel} | {status} |".format(
                device=row.get("device_id", ""),
                zero=_format_float(row.get("zero_offset_ppm")),
                s13_rel=_format_float(row.get("s1s3_max_abs_relative_error_percent")),
                s13_low=_format_float(row.get("s1s3_low_end_max_abs_relative_error_percent")),
                c0=_format_float(row.get("s5_C0")),
                c1=_format_float(row.get("s5_C1")),
                s5_rel=_format_float(row.get("s5_max_abs_relative_error_percent")),
                status=row.get("scenario_status", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 判断说明",
            "",
            "- 如果最佳场景需要非零零气假设，说明低端截距对结果敏感；正式发布前应补充零气 CO2 含量或把该假设写入不确定度预算。",
            "- 如果 S5 能显著压低非零标准气最大相对误差，且 C0/C1 幅度小，它属于合理的最终显示层修正。",
            "- 如果 S5 需要很大的 C0 或 C1 偏离 1，优先回查气路稳定、标准气证书、ratio、ref_signal、温度通道和固件输出链。",
            "- 本报告不授权写入，只提供受控写入前的离线评审依据。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
