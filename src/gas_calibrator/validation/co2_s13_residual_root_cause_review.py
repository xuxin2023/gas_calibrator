"""CO2 SENCO1/SENCO3 residual root-cause review.

This module is an offline/no-write review for recorded V1.5 CO2 open-flow
evidence. It joins fitted residuals with sampling physics so the next action
can be chosen from evidence: S1/S3 model shape, zero-gas anchoring, point
quality treatment, or a later S5 output-layer trim.
"""

from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


LOW_END_TARGET_PPM = 400.0


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


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


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _point_key(row: Mapping[str, Any]) -> Tuple[str, str]:
    return (
        _device_id(row.get("analyzer_device_id") or row.get("device_id")),
        str(row.get("point_identity") or "").strip(),
    )


def _residual_key(row: Mapping[str, Any]) -> Tuple[str, str]:
    return (
        _device_id(row.get("device_id") or row.get("analyzer_device_id")),
        str(row.get("point_identity") or "").strip(),
    )


def _selected_keys(selected_rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for row in selected_rows:
        device = _device_id(row.get("device_id"))
        objective = str(row.get("best_objective_id") or "").strip()
        zero_offset = _safe_float(row.get("best_zero_offset_ppm"))
        model = str(row.get("best_model_id") or "").strip()
        if device and objective and zero_offset is not None:
            result[device] = {
                "objective_id": objective,
                "zero_offset_ppm": float(zero_offset),
                "model_id": model,
            }
    return result


def _matches_selected(row: Mapping[str, Any], selected: Mapping[str, Mapping[str, Any]]) -> bool:
    device = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
    expected = selected.get(device)
    if not expected:
        return False
    row_objective = str(row.get("objective_id") or "").strip()
    row_zero = _safe_float(row.get("zero_offset_ppm"))
    if row_objective != expected["objective_id"]:
        return False
    if row_zero is None or abs(float(row_zero) - float(expected["zero_offset_ppm"])) > 1.0e-9:
        return False
    expected_model = str(expected.get("model_id") or "")
    row_model = str(row.get("model_id") or "")
    if expected_model and row_model and row_model != expected_model:
        return False
    return True


def _water_vapor_pressure_hpa(dewpoint_c: float) -> float:
    if dewpoint_c < 0.0:
        return 6.112 * math.exp((22.46 * dewpoint_c) / (272.62 + dewpoint_c))
    return 6.112 * math.exp((17.62 * dewpoint_c) / (243.12 + dewpoint_c))


def _dewpoint_h2o_mmol(dewpoint_c: Any, pressure_hpa: Any) -> Optional[float]:
    dew = _safe_float(dewpoint_c)
    pressure = _safe_float(pressure_hpa)
    if dew is None or pressure is None or pressure <= 0.0:
        return None
    e_hpa = _water_vapor_pressure_hpa(float(dew))
    if e_hpa <= 0.0 or e_hpa >= pressure:
        return None
    return 1000.0 * e_hpa / (pressure - e_hpa)


def _ratio_grade(std_value: Any) -> str:
    std = _safe_float(std_value)
    if std is None:
        return "unknown"
    if std <= 0.0005:
        return "A"
    if std <= 0.001:
        return "B"
    return "C"


def _dryness_grade(dewpoint_c: Any) -> str:
    dew = _safe_float(dewpoint_c)
    if dew is None:
        return "unknown"
    if dew <= -28.0:
        return "deep_dry"
    if dew <= -20.0:
        return "usable_but_not_deep_dry"
    return "dryness_risk"


def _temperature_group(row: Mapping[str, Any]) -> str:
    explicit = row.get("temp_set_c")
    number = _safe_float(explicit)
    if number is not None:
        return f"T{number:g}"
    identity = str(row.get("point_identity") or "")
    if "_" in identity:
        return identity.split("_", 1)[0]
    return ""


def _target_group(row: Mapping[str, Any]) -> str:
    target = _safe_float(row.get("target_ppm") or row.get("target_value") or row.get("source_nominal_ppm"))
    if target is None:
        identity = str(row.get("point_identity") or "")
        if "_" in identity:
            return identity.rsplit("_", 1)[-1]
        return ""
    return f"{target:g}ppm"


def _abs_or_blank(value: Any) -> Any:
    number = _safe_float(value)
    return abs(float(number)) if number is not None else ""


def _same_sign_fraction(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    positives = sum(1 for value in values if value > 0.0)
    negatives = sum(1 for value in values if value < 0.0)
    return max(positives, negatives) / len(values)


def _common_point_status(errors: Sequence[float]) -> str:
    if len(errors) < 3:
        return "insufficient_devices"
    spread = pstdev(errors) if len(errors) > 1 else 0.0
    if _same_sign_fraction(errors) >= 0.8 and abs(mean(errors)) >= max(1.0, 0.5 * spread):
        return "common_mode_suspect"
    return "device_specific_or_balanced"


def _physical_flags(row: Mapping[str, Any]) -> List[str]:
    flags: List[str] = []
    ratio_grade = str(row.get("ratio_grade") or "")
    dryness_grade = str(row.get("dryness_grade") or "")
    dew_h2o = _safe_float(row.get("dewpoint_derived_h2o_mmol"))
    analyzer_h2o = _safe_float(row.get("analyzer_h2o_mmol"))
    ref_signal = _safe_float(row.get("ref_signal_mean"))
    status = str(row.get("status_register_qc_values") or "").strip().lower()
    if ratio_grade not in {"A", "unknown"}:
        flags.append("ratio_window_not_A")
    if dryness_grade == "dryness_risk":
        flags.append("dewpoint_not_dry_enough")
    if dew_h2o is not None and analyzer_h2o is not None and dew_h2o < 1.0 and analyzer_h2o > 10.0:
        flags.append("analyzer_h2o_output_inconsistent_with_dewpoint")
    if ref_signal is not None and ref_signal >= 4000.0:
        flags.append("ref_signal_near_configured_full_scale_hint")
    if status in {"", "missing", "none", "null"}:
        flags.append("status_register_missing")
    return flags


def _hypothesis(row: Mapping[str, Any]) -> Tuple[str, str, str]:
    target = _safe_float(row.get("target_ppm")) or 0.0
    rel_abs = _safe_float(row.get("abs_relative_error_percent"))
    ratio_grade = str(row.get("ratio_grade") or "")
    dryness_grade = str(row.get("dryness_grade") or "")
    common_status = str(row.get("common_mode_status") or "")
    zero_class = str(row.get("zero_anchor_class") or "").lower()
    flags = _physical_flags(row)

    if abs(target) <= 1e-9 or "zero" in zero_class:
        return (
            "zero_anchor_not_traceably_zero",
            "0ppm 气不能按数学零点理解；应按估算 CO2 含量和不确定度处理。",
            "review_zero_gas_co2_content_and_low_end_intercept",
        )
    if ratio_grade not in {"A", "unknown"}:
        return (
            "ratio_window_not_A_grade",
            "滤波后 CO2 ratio 未达到 A 级稳定，当前窗口可能仍含切气过渡或管路残留。",
            "prefer_longer_open_flow_or_best_stable_window",
        )
    if dryness_grade == "dryness_risk":
        return (
            "dryness_state_not_sufficient",
            "露点未进入足够干燥状态，CO2 点可能受到水汽稀释或管路残湿状态差异影响。",
            "extend_purge_or_reclassify_point_before_refit",
        )
    if "ref_signal_near_configured_full_scale_hint" in flags:
        return (
            "optical_reference_working_region_risk",
            "参考信号接近配置满值提示区，应检查 SETCO2/SETILLUM、光路和 CO2 signal 是否处于健康工作区。",
            "review_ref_signal_and_factory_signal_before_s13_write",
        )
    if common_status == "common_mode_suspect":
        return (
            "common_mode_target_or_model_shape",
            "多台设备同一气点同向偏差，更像目标锚点、阀路气瓶状态或 S1/S3 模型形状问题。",
            "review_target_mapping_zero_anchor_and_s1s3_model_shape",
        )
    if "analyzer_h2o_output_inconsistent_with_dewpoint" in flags:
        return (
            "analyzer_h2o_output_not_physical_for_co2_bridge",
            "露点换算显示样气很干，但分析仪 H2O 输出很高；当前 H2O 输出层不能作为 CO2 干基修正输入。",
            "keep_h2o_bridge_disabled_for_s1s3_and_review_s6_separately",
        )
    if rel_abs is not None and rel_abs > 3.0:
        return (
            "device_specific_residual",
            "该点主要表现为单台设备残差，优先看该设备光学信号、ratio 局部稳定性、温度输入和既有系数状态。",
            "device_level_signal_and_temperature_review",
        )
    return (
        "ordinary_fit_residual",
        "残差未显示明确物理异常，保留为拟合形状评审证据。",
        "keep_for_model_review",
    )


def _selected_residual_rows(
    *,
    fit_points: Sequence[Mapping[str, Any]],
    residual_rows: Sequence[Mapping[str, Any]],
    selected_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    metadata = {_point_key(row): row for row in fit_points}
    selected = _selected_keys(selected_rows)
    filtered = [dict(row) for row in residual_rows if _matches_selected(row, selected)]

    errors_by_point: Dict[str, List[float]] = defaultdict(list)
    for row in filtered:
        error = _safe_float(row.get("error_ppm"))
        if error is not None:
            errors_by_point[str(row.get("point_identity") or "")].append(float(error))
    point_status = {point: _common_point_status(errors) for point, errors in errors_by_point.items()}

    result: List[Dict[str, Any]] = []
    for row in filtered:
        device, identity = _residual_key(row)
        meta = metadata.get((device, identity), {})
        target = _safe_float(row.get("target_ppm")) or 0.0
        rel = _safe_float(row.get("relative_error_percent"))
        if rel is None and abs(target) > 1.0e-12:
            error = _safe_float(row.get("error_ppm")) or 0.0
            rel = 100.0 * float(error) / float(target)
        dew_h2o = _dewpoint_h2o_mmol(meta.get("dewpoint_mean_c"), meta.get("pressure_gauge_mean_hpa") or row.get("pressure_hpa"))
        merged: Dict[str, Any] = {
            "device_id": device,
            "point_identity": identity,
            "temperature_group": _temperature_group(meta or row),
            "target_group": _target_group(row),
            "target_ppm": target,
            "prediction_ppm": row.get("prediction_ppm"),
            "error_ppm": row.get("error_ppm"),
            "relative_error_percent": "" if rel is None else rel,
            "abs_relative_error_percent": abs(float(rel)) if rel is not None else "",
            "abs_error_ppm": _abs_or_blank(row.get("error_ppm")),
            "objective_id": row.get("objective_id"),
            "zero_offset_ppm": row.get("zero_offset_ppm"),
            "model_id": row.get("model_id"),
            "zero_anchor_class": row.get("zero_anchor_class") or meta.get("zero_anchor_class"),
            "ratio": row.get("ratio") or meta.get("ratio"),
            "co2_ratio_f_std": meta.get("co2_ratio_f_std"),
            "ratio_grade": _ratio_grade(meta.get("co2_ratio_f_std")),
            "temperature_c": row.get("temperature_c") or meta.get("temperature_c"),
            "thermometer_temp_mean_c": meta.get("thermometer_temp_mean_c"),
            "analyzer_chamber_temp_mean_c": meta.get("analyzer_chamber_temp_mean_c"),
            "pressure_hpa": row.get("pressure_hpa") or meta.get("pressure_gauge_mean_hpa"),
            "dewpoint_mean_c": meta.get("dewpoint_mean_c"),
            "dryness_grade": _dryness_grade(meta.get("dewpoint_mean_c")),
            "dewpoint_derived_h2o_mmol": "" if dew_h2o is None else dew_h2o,
            "analyzer_h2o_mmol": row.get("h2o_mmol") or meta.get("h2o_mmol_mean"),
            "ref_signal_mean": meta.get("ref_signal_mean"),
            "co2_signal_mean": meta.get("co2_signal_mean"),
            "sample_count": meta.get("sample_count"),
            "usable_sample_count": meta.get("usable_sample_count"),
            "status_register_qc_values": meta.get("status_register_qc_values"),
            "fit_inclusion_status": meta.get("fit_inclusion_status"),
            "common_mode_status": point_status.get(identity, ""),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        }
        merged["physical_state_flags"] = ";".join(_physical_flags(merged))
        cause, physical_reason, next_action = _hypothesis(merged)
        merged["root_cause_hypothesis"] = cause
        merged["physical_reason"] = physical_reason
        merged["next_action"] = next_action
        result.append(merged)
    return result


def _is_nonzero_standard(row: Mapping[str, Any]) -> bool:
    target = _safe_float(row.get("target_ppm"))
    if target is None or target <= 0.0:
        return False
    marker = str(row.get("zero_anchor_class") or "").lower()
    return "zero" not in marker


def _is_low_end_standard(row: Mapping[str, Any]) -> bool:
    target = _safe_float(row.get("target_ppm"))
    return _is_nonzero_standard(row) and target is not None and target <= LOW_END_TARGET_PPM


def _numeric_values(rows: Sequence[Mapping[str, Any]], key: str, *, absolute: bool = False) -> List[float]:
    values: List[float] = []
    for row in rows:
        value = _safe_float(row.get(key))
        if value is not None:
            values.append(abs(float(value)) if absolute else float(value))
    return values


def _mean_or_blank(values: Sequence[float]) -> float | str:
    return mean(values) if values else ""


def _min_or_blank(values: Sequence[float]) -> float | str:
    return min(values) if values else ""


def _max_or_blank(values: Sequence[float]) -> float | str:
    return max(values) if values else ""


def _root_for_low_end_group(items: Sequence[Mapping[str, Any]]) -> Tuple[str, str]:
    errors = _numeric_values(items, "error_ppm")
    same_sign = _same_sign_fraction(errors)
    ratio_counter = Counter(str(row.get("ratio_grade") or "") for row in items)
    dry_counter = Counter(str(row.get("dryness_grade") or "") for row in items)
    flag_text = ";".join(str(row.get("physical_state_flags") or "") for row in items)
    if ratio_counter.get("B", 0) or ratio_counter.get("C", 0):
        return (
            "ratio_stability_first",
            "先处理滤波后 CO2 ratio 未达 A 级的问题；低端点对截距非常敏感，ratio 窗口不稳会直接放大相对误差。",
        )
    if dry_counter.get("dryness_risk", 0):
        return (
            "dryness_first",
            "先处理露点/干燥状态；低端 CO2 点如果样气含湿状态不一致，模型会把物理状态差异误吸收到 S1/S3。",
        )
    if "ref_signal_near_configured_full_scale_hint" in flag_text:
        return (
            "optical_reference_review",
            "同一低端气点出现参考信号工作区风险，应先查 SETCO2/SETILLUM、光路和 CO2 signal，不要直接让 S1/S3 吸收。",
        )
    if same_sign >= 0.8 and len(errors) >= 3:
        return (
            "common_mode_or_model_shape",
            "多台设备在同一低端点同向偏差，优先查目标气瓶/阀路映射/共同管路状态，其次评审 S1/S3 低端模型形状。",
        )
    return (
        "device_specific_or_balanced",
        "同一点各设备残差方向不完全一致，更像设备个体差异或模型局部形状限制；保留逐台评审。",
    )


def _low_end_pattern_summary(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if _is_low_end_standard(row):
            grouped[str(row.get("point_identity") or "")].append(row)

    result: List[Dict[str, Any]] = []
    for point, items in sorted(grouped.items()):
        errors = _numeric_values(items, "error_ppm")
        rels = _numeric_values(items, "relative_error_percent", absolute=True)
        dew = _numeric_values(items, "dewpoint_mean_c")
        ratio_std = _numeric_values(items, "co2_ratio_f_std")
        ref_signal = _numeric_values(items, "ref_signal_mean")
        ratio_counter = Counter(str(row.get("ratio_grade") or "") for row in items)
        dry_counter = Counter(str(row.get("dryness_grade") or "") for row in items)
        cause, recommendation = _root_for_low_end_group(items)
        result.append(
            {
                "point_identity": point,
                "device_count": len(items),
                "target_group": _target_group(items[0]) if items else "",
                "temperature_group": str(items[0].get("temperature_group") or "") if items else "",
                "mean_error_ppm": _mean_or_blank(errors),
                "same_sign_fraction": _same_sign_fraction(errors),
                "positive_error_count": sum(1 for value in errors if value > 0.0),
                "negative_error_count": sum(1 for value in errors if value < 0.0),
                "max_abs_relative_error_percent": _max_or_blank(rels),
                "ratio_grade_counts": ";".join(f"{key}:{value}" for key, value in sorted(ratio_counter.items()) if key),
                "dryness_grade_counts": ";".join(f"{key}:{value}" for key, value in sorted(dry_counter.items()) if key),
                "dewpoint_mean_c": _mean_or_blank(dew),
                "dewpoint_min_c": _min_or_blank(dew),
                "dewpoint_max_c": _max_or_blank(dew),
                "co2_ratio_f_std_max": _max_or_blank(ratio_std),
                "ref_signal_min": _min_or_blank(ref_signal),
                "ref_signal_max": _max_or_blank(ref_signal),
                "common_mode_status": _common_point_status(errors),
                "likely_root_cause": cause,
                "recommended_next_action": recommendation,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            }
        )
    result.sort(key=lambda row: float(_safe_float(row.get("max_abs_relative_error_percent")) or 0.0), reverse=True)
    return result


def _device_low_end_temperature_bias(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if _is_low_end_standard(row):
            grouped[(str(row.get("device_id") or ""), str(row.get("temperature_group") or ""))].append(row)

    result: List[Dict[str, Any]] = []
    for (device, temp_group), items in sorted(grouped.items()):
        errors = _numeric_values(items, "error_ppm")
        rels = _numeric_values(items, "relative_error_percent", absolute=True)
        targets = ";".join(sorted({str(row.get("target_group") or "") for row in items}))
        sign = "balanced"
        if errors and _same_sign_fraction(errors) >= 0.8:
            sign = "mostly_positive" if sum(1 for value in errors if value > 0.0) >= sum(1 for value in errors if value < 0.0) else "mostly_negative"
        result.append(
            {
                "device_id": device,
                "temperature_group": temp_group,
                "low_end_point_count": len(items),
                "target_groups": targets,
                "mean_error_ppm": _mean_or_blank(errors),
                "max_abs_relative_error_percent": _max_or_blank(rels),
                "error_sign_pattern": sign,
                "interpretation": (
                    "同一设备同一温度组低端点若同向偏差，优先检查该温度组的温度输入、零气/低端锚定和 S1/S3 温度项。"
                    if sign != "balanced"
                    else "低端残差方向较分散，优先看单点质量和局部模型形状。"
                ),
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            }
        )
    result.sort(key=lambda row: float(_safe_float(row.get("max_abs_relative_error_percent")) or 0.0), reverse=True)
    return result


def _group_summary(rows: Sequence[Mapping[str, Any]], group_keys: Sequence[str]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, ...], List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row.get(key) or "") for key in group_keys)].append(row)
    result: List[Dict[str, Any]] = []
    for key, items in sorted(grouped.items()):
        errors = _numeric_values(items, "error_ppm")
        rels = _numeric_values(items, "relative_error_percent", absolute=True)
        row: Dict[str, Any] = {name: value for name, value in zip(group_keys, key)}
        row.update(
            {
                "point_count": len(items),
                "mean_error_ppm": _mean_or_blank(errors),
                "max_abs_error_ppm": _max_or_blank([abs(value) for value in errors]),
                "max_abs_relative_error_percent": _max_or_blank(rels),
                "ratio_grades": ";".join(sorted({str(item.get("ratio_grade") or "") for item in items if item.get("ratio_grade")})),
                "dryness_grades": ";".join(sorted({str(item.get("dryness_grade") or "") for item in items if item.get("dryness_grade")})),
                "root_cause_hypotheses": ";".join(
                    sorted({str(item.get("root_cause_hypothesis") or "") for item in items if item.get("root_cause_hypothesis")})
                ),
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            }
        )
        result.append(row)
    return result


def _zero5_conclusion(base_rel: Optional[float], rel5: Optional[float]) -> str:
    if base_rel is None or rel5 is None:
        return "insufficient_data"
    if rel5 < base_rel * 0.85:
        return "5ppm_zero_assumption_clearly_improves"
    if rel5 < base_rel:
        return "5ppm_zero_assumption_slightly_improves"
    if rel5 > base_rel * 1.05:
        return "5ppm_zero_assumption_worsens"
    return "5ppm_zero_assumption_neutral"


def _zero_offset_rows(summary_rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    abs_rows = [row for row in summary_rows if str(row.get("objective_id") or "") == "absolute_lstsq"]
    grouped: Dict[str, Dict[float, Mapping[str, Any]]] = defaultdict(dict)
    for row in abs_rows:
        device = _device_id(row.get("device_id"))
        zero = _safe_float(row.get("zero_offset_ppm"))
        if device and zero is not None:
            grouped[device][float(zero)] = row
    result: List[Dict[str, Any]] = []
    for device, by_zero in sorted(grouped.items()):
        base = by_zero.get(0.0)
        row5 = by_zero.get(5.0)
        row10 = by_zero.get(10.0)
        base_rel = _safe_float(base.get("max_abs_relative_error_percent") if base else None)
        rel5 = _safe_float(row5.get("max_abs_relative_error_percent") if row5 else None)
        rel10 = _safe_float(row10.get("max_abs_relative_error_percent") if row10 else None)
        best_zero, best_rel = min(
            [(zero, _safe_float(row.get("max_abs_relative_error_percent"))) for zero, row in by_zero.items()],
            key=lambda item: float("inf") if item[1] is None else float(item[1]),
        )
        improvement5 = ""
        if base_rel is not None and rel5 is not None and abs(base_rel) > 1.0e-12:
            improvement5 = 100.0 * (base_rel - rel5) / base_rel
        result.append(
            {
                "device_id": device,
                "abs_lstsq_zero0_max_rel_percent": "" if base_rel is None else base_rel,
                "abs_lstsq_zero5_max_rel_percent": "" if rel5 is None else rel5,
                "abs_lstsq_zero10_max_rel_percent": "" if rel10 is None else rel10,
                "zero5_relative_improvement_percent": improvement5,
                "best_zero_offset_ppm_under_abs_lstsq": best_zero,
                "best_abs_lstsq_max_rel_percent": "" if best_rel is None else best_rel,
                "zero5_conclusion": _zero5_conclusion(base_rel, rel5),
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            }
        )
    return result


def build_co2_s13_residual_root_cause_review(
    *,
    fit_points_csv: str | Path,
    objective_residuals_csv: str | Path,
    objective_summary_csv: str | Path,
    selected_candidates_csv: str | Path,
    worst_point_limit: int = 15,
) -> Dict[str, List[Dict[str, Any]]]:
    fit_points = _read_csv(fit_points_csv)
    residuals = _read_csv(objective_residuals_csv)
    objective_summary = _read_csv(objective_summary_csv)
    selected = _read_csv(selected_candidates_csv)

    selected_residuals = _selected_residual_rows(
        fit_points=fit_points,
        residual_rows=residuals,
        selected_rows=selected,
    )
    standard_residuals = [row for row in selected_residuals if _is_nonzero_standard(row)]
    worst = sorted(
        standard_residuals,
        key=lambda row: float(_safe_float(row.get("abs_relative_error_percent")) or -1.0),
        reverse=True,
    )[: int(worst_point_limit)]
    zero_worst = sorted(
        [row for row in selected_residuals if str(row.get("zero_anchor_class") or "").lower().find("zero") >= 0],
        key=lambda row: float(_safe_float(row.get("abs_error_ppm")) or -1.0),
        reverse=True,
    )[: int(worst_point_limit)]
    return {
        "run_summary": [
            {
                "created_at": _now(),
                "fit_points_csv": str(Path(fit_points_csv).resolve()),
                "objective_residuals_csv": str(Path(objective_residuals_csv).resolve()),
                "objective_summary_csv": str(Path(objective_summary_csv).resolve()),
                "selected_candidates_csv": str(Path(selected_candidates_csv).resolve()),
                "fit_point_count": len(fit_points),
                "selected_residual_count": len(selected_residuals),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "not_real_acceptance_evidence": True,
            }
        ],
        "selected_residuals": selected_residuals,
        "worst_nonzero_points": worst,
        "worst_zero_anchor_points": zero_worst,
        "low_end_pattern_summary": _low_end_pattern_summary(selected_residuals),
        "device_low_end_temperature_bias": _device_low_end_temperature_bias(selected_residuals),
        "by_device_temperature": _group_summary(selected_residuals, ("device_id", "temperature_group")),
        "by_device_target": _group_summary(selected_residuals, ("device_id", "target_group")),
        "by_point_identity": _group_summary(selected_residuals, ("point_identity",)),
        "zero_offset_effect": _zero_offset_rows(objective_summary),
    }


def write_co2_s13_residual_root_cause_review(
    *,
    fit_points_csv: str | Path,
    objective_residuals_csv: str | Path,
    objective_summary_csv: str | Path,
    selected_candidates_csv: str | Path,
    output_dir: str | Path,
    worst_point_limit: int = 15,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_residual_root_cause_review(
        fit_points_csv=fit_points_csv,
        objective_residuals_csv=objective_residuals_csv,
        objective_summary_csv=objective_summary_csv,
        selected_candidates_csv=selected_candidates_csv,
        worst_point_limit=worst_point_limit,
    )
    outputs = {
        "run_summary": output / "co2_s13_residual_root_cause_run_summary.csv",
        "selected_residuals": output / "co2_s13_selected_residuals_with_physics.csv",
        "worst_nonzero_points": output / "co2_s13_worst_nonzero_points.csv",
        "worst_zero_anchor_points": output / "co2_s13_worst_zero_anchor_points.csv",
        "low_end_pattern_summary": output / "co2_s13_low_end_pattern_summary.csv",
        "device_low_end_temperature_bias": output / "co2_s13_device_low_end_temperature_bias.csv",
        "by_device_temperature": output / "co2_s13_by_device_temperature.csv",
        "by_device_target": output / "co2_s13_by_device_target.csv",
        "by_point_identity": output / "co2_s13_by_point_identity.csv",
        "zero_offset_effect": output / "co2_s13_zero_offset_effect.csv",
        "metadata": output / "co2_s13_residual_root_cause_meta.json",
        "markdown": output / "co2_s13_residual_root_cause_review_zh.md",
    }
    for key in (
        "run_summary",
        "selected_residuals",
        "worst_nonzero_points",
        "worst_zero_anchor_points",
        "low_end_pattern_summary",
        "device_low_end_temperature_bias",
        "by_device_temperature",
        "by_device_target",
        "by_point_identity",
        "zero_offset_effect",
    ):
        _write_csv(outputs[key], tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_residual_root_cause_review",
                "created_at": _now(),
                "inputs": {
                    "fit_points_csv": str(Path(fit_points_csv).resolve()),
                    "objective_residuals_csv": str(Path(objective_residuals_csv).resolve()),
                    "objective_summary_csv": str(Path(objective_summary_csv).resolve()),
                    "selected_candidates_csv": str(Path(selected_candidates_csv).resolve()),
                    "worst_point_limit": worst_point_limit,
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


def _fmt(value: Any, digits: int = 5) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}g}"


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    lines = [
        "# V1.5 CO2 S1/S3 低端残差根因拆解报告",
        "",
        f"- 生成时间：{_now()}",
        "- 边界：离线 no-write；不打开 COM；不控制气路/水路；不写 SENCO。",
        "- 目的：把 S1/S3 残差拆到设备、温度、气点和物理状态，决定下一步应修主拟合、零气锚点、点位质量，还是后置评审 S5 输出层。",
        "- 拟合合同：压力项冻结为 0；S5 不参与本报告；CO2 零气锚点与 H2O 干气锚点分开处理。",
        "",
        "## 0ppm 赋值敏感性",
        "",
        "| 设备ID | 0ppm假设最大相对误差% | 5ppm假设最大相对误差% | 10ppm假设最大相对误差% | 5ppm结论 |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for row in tables.get("zero_offset_effect", []):
        lines.append(
            "| {device} | {z0} | {z5} | {z10} | {conclusion} |".format(
                device=row.get("device_id", ""),
                z0=_fmt(row.get("abs_lstsq_zero0_max_rel_percent"), 4),
                z5=_fmt(row.get("abs_lstsq_zero5_max_rel_percent"), 4),
                z10=_fmt(row.get("abs_lstsq_zero10_max_rel_percent"), 4),
                conclusion=row.get("zero5_conclusion", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 低端气点共态模式",
            "",
            "| 点位 | 台数 | 平均误差ppm | 同向比例 | 最大相对误差% | ratio等级 | 露点范围°C | 判断 |",
            "| --- | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in tables.get("low_end_pattern_summary", [])[:18]:
        lines.append(
            "| {point} | {count} | {mean_error} | {same_sign} | {max_rel} | {ratio} | {dew_min}..{dew_max} | {cause} |".format(
                point=row.get("point_identity", ""),
                count=row.get("device_count", ""),
                mean_error=_fmt(row.get("mean_error_ppm"), 5),
                same_sign=_fmt(row.get("same_sign_fraction"), 3),
                max_rel=_fmt(row.get("max_abs_relative_error_percent"), 4),
                ratio=row.get("ratio_grade_counts", ""),
                dew_min=_fmt(row.get("dewpoint_min_c"), 4),
                dew_max=_fmt(row.get("dewpoint_max_c"), 4),
                cause=row.get("likely_root_cause", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 最差非零气点",
            "",
            "| 设备ID | 点位 | 目标ppm | 误差ppm | 相对误差% | ratio等级 | 露点°C | 根因假设 | 下一步 |",
            "| --- | --- | ---: | ---: | ---: | --- | ---: | --- | --- |",
        ]
    )
    for row in tables.get("worst_nonzero_points", [])[:18]:
        lines.append(
            "| {device} | {point} | {target} | {error} | {rel} | {ratio_grade} | {dew} | {cause} | {action} |".format(
                device=row.get("device_id", ""),
                point=row.get("point_identity", ""),
                target=_fmt(row.get("target_ppm"), 5),
                error=_fmt(row.get("error_ppm"), 5),
                rel=_fmt(row.get("abs_relative_error_percent"), 4),
                ratio_grade=row.get("ratio_grade", ""),
                dew=_fmt(row.get("dewpoint_mean_c"), 4),
                cause=row.get("root_cause_hypothesis", ""),
                action=row.get("next_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 专业判断",
            "",
            "1. 若非零低端点在多台设备上同向偏差，而 ratio 与露点均合格，优先怀疑共同点位状态、目标锚点或 S1/S3 低端模型形状。",
            "2. 若偏差只集中在某台设备，应查该设备 ref_signal、CO2 signal、温度输入和既有系数，不应把其它设备一起降级。",
            "3. 0ppm 气只能作为低端 CO2 锚点，不能等同于 H2O 干气锚点；估算零气 CO2 含量可以评审，但必须带不确定度和证据链。",
            "4. S5 是最终显示层线性修正。S1/S3 的低端残差根因没解释清楚前，不应先用 S5 掩盖主模型问题。",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8-sig")
