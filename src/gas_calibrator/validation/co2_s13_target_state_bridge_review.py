"""Offline CO2 S1/S3 target-state bridge review.

This review joins selected S1/S3 residuals with recorded open-flow physical
state evidence. It is intentionally offline/no-write: it never opens COM
ports, controls gas/water routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


LOW_END_LIMIT_PPM = 400.0


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


def _point_identity(row: Mapping[str, Any]) -> str:
    return str(row.get("point_identity") or row.get("sample_index") or "").strip()


def _target_ppm(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(
        row.get("target_ppm")
        or row.get("target_value")
        or row.get("source_nominal_ppm")
        or row.get("nominal_ppm")
    )


def _temperature_group(row: Mapping[str, Any]) -> str:
    identity = _point_identity(row)
    if identity.startswith("T") and "_" in identity:
        return identity.split("_", 1)[0]
    value = _safe_float(row.get("temp_set_c") or row.get("temperature_c"))
    if value is None:
        return ""
    return f"T{value:g}"


def _water_vapor_pressure_hpa(dewpoint_c: float) -> float:
    if dewpoint_c < 0.0:
        return 6.112 * math.exp((22.46 * dewpoint_c) / (272.62 + dewpoint_c))
    return 6.112 * math.exp((17.62 * dewpoint_c) / (243.12 + dewpoint_c))


def _dewpoint_h2o_mmol(dewpoint_c: Any, pressure_hpa: Any) -> Optional[float]:
    dew = _safe_float(dewpoint_c)
    pressure = _safe_float(pressure_hpa)
    if dew is None or pressure is None or pressure <= 0.0:
        return None
    vapor = _water_vapor_pressure_hpa(float(dew))
    if vapor <= 0.0 or vapor >= pressure:
        return None
    return 1000.0 * vapor / (pressure - vapor)


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


def _selected_spec(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    selected: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        device = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
        objective = str(row.get("best_objective_id") or row.get("objective_id") or "").strip()
        zero_offset = _safe_float(row.get("best_zero_offset_ppm") or row.get("zero_offset_ppm"))
        model_id = str(row.get("best_model_id") or row.get("model_id") or "").strip()
        structure_id = str(row.get("best_structure_id") or row.get("structure_id") or "").strip()
        if device and objective and zero_offset is not None:
            selected[device] = {
                "objective_id": objective,
                "zero_offset_ppm": float(zero_offset),
                "model_id": model_id,
                "structure_id": structure_id,
            }
    return selected


def _residual_matches(row: Mapping[str, Any], selected: Mapping[str, Mapping[str, Any]]) -> bool:
    device = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
    spec = selected.get(device)
    if not spec:
        return False
    if str(row.get("objective_id") or "").strip() != spec["objective_id"]:
        return False
    zero = _safe_float(row.get("zero_offset_ppm"))
    if zero is None or abs(float(zero) - float(spec["zero_offset_ppm"])) > 1e-9:
        return False
    expected_model = str(spec.get("model_id") or "")
    row_model = str(row.get("model_id") or "")
    if expected_model and row_model and expected_model != row_model:
        return False
    expected_structure = str(spec.get("structure_id") or "")
    row_structure = str(row.get("structure_id") or "")
    if expected_structure and row_structure and expected_structure != row_structure:
        return False
    return True


def _sign(value: float) -> int:
    if value > 0.0:
        return 1
    if value < 0.0:
        return -1
    return 0


def _same_sign_fraction(values: Sequence[float]) -> float:
    signs = [_sign(value) for value in values if _sign(value) != 0]
    if not signs:
        return 0.0
    counts = Counter(signs)
    return max(counts.values()) / len(signs)


def _span(values: Sequence[Optional[float]]) -> Optional[float]:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if len(clean) < 2:
        return None
    return max(clean) - min(clean)


def _join_residuals_with_state(
    residual_rows: Sequence[Mapping[str, Any]],
    fit_rows: Sequence[Mapping[str, Any]],
    selected_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    selected = _selected_spec(selected_rows)
    fit_lookup: Dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in fit_rows:
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        identity = _point_identity(row)
        if device and identity:
            fit_lookup[(device, identity)] = row

    enriched: List[Dict[str, Any]] = []
    for residual in residual_rows:
        if selected and not _residual_matches(residual, selected):
            continue
        if str(residual.get("source_role") or "fit").strip() != "fit":
            continue
        device = _device_id(residual.get("device_id") or residual.get("analyzer_device_id"))
        identity = _point_identity(residual)
        fit = fit_lookup.get((device, identity), {})
        target = _target_ppm(residual) if _target_ppm(residual) is not None else _target_ppm(fit)
        if target is None:
            continue
        temperature = _safe_float(residual.get("temperature_c") or fit.get("temperature_c"))
        pressure = _safe_float(residual.get("pressure_hpa") or fit.get("pressure_hpa"))
        dewpoint = _safe_float(
            fit.get("dewpoint_mean_c")
            or fit.get("dewpoint_c_mean")
            or residual.get("dewpoint_mean_c")
            or residual.get("dewpoint_c_mean")
        )
        ratio_std = _safe_float(
            fit.get("co2_ratio_f_std")
            or residual.get("co2_ratio_f_std")
            or residual.get("ratio_std")
        )
        ratio = _safe_float(residual.get("ratio") or fit.get("ratio") or fit.get("co2_ratio_f_mean"))
        rel_error = _safe_float(residual.get("relative_error_percent"))
        error_ppm = _safe_float(residual.get("error_ppm"))
        dew_h2o = _dewpoint_h2o_mmol(dewpoint, pressure)
        enriched.append(
            {
                "device_id": device,
                "point_identity": identity,
                "temperature_group": _temperature_group(residual) or _temperature_group(fit),
                "target_ppm": float(target),
                "is_zero_anchor": abs(float(target)) <= 1e-9
                or "zero" in str(residual.get("zero_anchor_class") or fit.get("zero_anchor_class") or "").lower(),
                "ratio": ratio if ratio is not None else "",
                "ratio_std": ratio_std if ratio_std is not None else "",
                "ratio_grade": _ratio_grade(ratio_std),
                "temperature_c": temperature if temperature is not None else "",
                "pressure_hpa": pressure if pressure is not None else "",
                "dewpoint_c": dewpoint if dewpoint is not None else "",
                "dewpoint_derived_h2o_mmol": dew_h2o if dew_h2o is not None else "",
                "analyzer_h2o_mmol": fit.get("h2o_mmol_mean") or residual.get("h2o_mmol") or "",
                "dryness_grade": _dryness_grade(dewpoint),
                "error_ppm": error_ppm if error_ppm is not None else "",
                "relative_error_percent": rel_error if rel_error is not None else "",
                "abs_relative_error_percent": abs(rel_error) if rel_error is not None else "",
                "model_id": residual.get("model_id") or "",
                "objective_id": residual.get("objective_id") or "",
                "zero_offset_ppm": residual.get("zero_offset_ppm") or "",
            }
        )
    return enriched


def _temperature_group_bias(enriched: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in enriched:
        groups[(str(row["device_id"]), str(row["temperature_group"]))].append(row)
    out: List[Dict[str, Any]] = []
    for (device, temp), rows in sorted(groups.items()):
        nonzero = [row for row in rows if not row.get("is_zero_anchor")]
        rel_values = [
            float(row["relative_error_percent"])
            for row in nonzero
            if _safe_float(row.get("relative_error_percent")) is not None
        ]
        ppm_values = [
            float(row["error_ppm"])
            for row in rows
            if _safe_float(row.get("error_ppm")) is not None
        ]
        low_rel = [
            float(row["relative_error_percent"])
            for row in nonzero
            if _safe_float(row.get("relative_error_percent")) is not None
            and (_safe_float(row.get("target_ppm")) or 0.0) <= LOW_END_LIMIT_PPM
        ]
        high_rel = [
            float(row["relative_error_percent"])
            for row in nonzero
            if _safe_float(row.get("relative_error_percent")) is not None
            and (_safe_float(row.get("target_ppm")) or 0.0) > LOW_END_LIMIT_PPM
        ]
        ratio_grades = Counter(str(row.get("ratio_grade") or "") for row in rows)
        dryness_grades = Counter(str(row.get("dryness_grade") or "") for row in rows)
        dew_values = [_safe_float(row.get("dewpoint_c")) for row in rows]
        h2o_values = [_safe_float(row.get("dewpoint_derived_h2o_mmol")) for row in rows]
        bias_status = "insufficient_nonzero_points"
        if len(rel_values) >= 2:
            same_sign = _same_sign_fraction(rel_values)
            mean_rel = mean(rel_values)
            if same_sign >= 0.8 and abs(mean_rel) >= 1.0:
                bias_status = "temperature_group_same_sign_bias"
            elif low_rel and high_rel and _same_sign_fraction(low_rel) < 1.0:
                bias_status = "mixed_low_end_shape"
            else:
                bias_status = "balanced_or_device_specific"
        out.append(
            {
                "device_id": device,
                "temperature_group": temp,
                "point_count": len(rows),
                "nonzero_point_count": len(nonzero),
                "zero_anchor_count": len(rows) - len(nonzero),
                "mean_relative_error_percent": mean(rel_values) if rel_values else "",
                "max_abs_relative_error_percent": max((abs(v) for v in rel_values), default=""),
                "mean_error_ppm_all_points": mean(ppm_values) if ppm_values else "",
                "same_sign_fraction_nonzero": _same_sign_fraction(rel_values),
                "low_end_mean_relative_error_percent": mean(low_rel) if low_rel else "",
                "high_end_mean_relative_error_percent": mean(high_rel) if high_rel else "",
                "ratio_grade_counts": json.dumps(dict(ratio_grades), ensure_ascii=False),
                "dryness_grade_counts": json.dumps(dict(dryness_grades), ensure_ascii=False),
                "dewpoint_span_c": _span(dew_values) if _span(dew_values) is not None else "",
                "dewpoint_h2o_span_mmol": _span(h2o_values) if _span(h2o_values) is not None else "",
                "temperature_bias_status": bias_status,
            }
        )
    return out


def _same_target_bridge(enriched: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[tuple[str, float], List[Mapping[str, Any]]] = defaultdict(list)
    for row in enriched:
        target = _safe_float(row.get("target_ppm"))
        if target is None:
            continue
        groups[(str(row["device_id"]), round(float(target), 6))].append(row)
    out: List[Dict[str, Any]] = []
    for (device, target), rows in sorted(groups.items()):
        if len(rows) < 2:
            continue
        nonzero_rel = [
            float(row["relative_error_percent"])
            for row in rows
            if _safe_float(row.get("relative_error_percent")) is not None
        ]
        ppm_errors = [
            float(row["error_ppm"])
            for row in rows
            if _safe_float(row.get("error_ppm")) is not None
        ]
        ratio_values = [_safe_float(row.get("ratio")) for row in rows]
        dew_values = [_safe_float(row.get("dewpoint_c")) for row in rows]
        h2o_values = [_safe_float(row.get("dewpoint_derived_h2o_mmol")) for row in rows]
        pressure_values = [_safe_float(row.get("pressure_hpa")) for row in rows]
        signs = {_sign(value) for value in nonzero_rel if _sign(value) != 0}
        rel_span = _span([abs(v) for v in nonzero_rel])
        signed_span = _span(nonzero_rel)
        dew_span = _span(dew_values)
        h2o_span = _span(h2o_values)
        pressure_span = _span(pressure_values)
        ratio_span = _span(ratio_values)
        bridge_status = "same_target_balanced_or_small_residual"
        physical_state_span = any(
            [
                dew_span is not None and dew_span > 5.0,
                h2o_span is not None and h2o_span > 0.3,
                pressure_span is not None and pressure_span > 3.0,
            ]
        )
        if len(signs) > 1 and signed_span is not None and abs(float(signed_span)) >= 2.0:
            bridge_status = (
                "sign_flip_with_physical_state_span"
                if physical_state_span
                else "sign_flip_without_obvious_state_span"
            )
        elif physical_state_span:
            bridge_status = "same_target_physical_state_span"
        out.append(
            {
                "device_id": device,
                "target_ppm": target,
                "temperature_groups": ";".join(str(row.get("temperature_group") or "") for row in rows),
                "point_identities": ";".join(str(row.get("point_identity") or "") for row in rows),
                "relative_error_values_percent": ";".join(
                    f"{float(v):.6g}" for v in nonzero_rel
                ),
                "relative_error_signed_span_percent": signed_span if signed_span is not None else "",
                "relative_error_abs_span_percent": rel_span if rel_span is not None else "",
                "max_abs_relative_error_percent": max((abs(v) for v in nonzero_rel), default=""),
                "error_ppm_span": _span(ppm_errors) if _span(ppm_errors) is not None else "",
                "ratio_span": ratio_span if ratio_span is not None else "",
                "dewpoint_span_c": dew_span if dew_span is not None else "",
                "dewpoint_h2o_span_mmol": h2o_span if h2o_span is not None else "",
                "pressure_span_hpa": pressure_span if pressure_span is not None else "",
                "bridge_status": bridge_status,
                "physical_interpretation": _bridge_interpretation(bridge_status, target),
            }
        )
    return out


def _bridge_interpretation(status: str, target: float) -> str:
    if status == "sign_flip_without_obvious_state_span":
        return (
            "同一气点跨温度残差换符号，但露点/压力跨度不明显；更像 S1/S3 低端温度形状或目标锚定边界。"
        )
    if status == "sign_flip_with_physical_state_span":
        return (
            "同一气点跨温度残差换符号且物理状态跨度明显；应先评审管路状态桥接，再讨论模型改形。"
        )
    if status == "same_target_physical_state_span":
        return "同一气点跨温度状态差异明显；拟合前需要确认是否可归一化到同一干燥状态。"
    if target <= 1e-9:
        return "零气锚点用于低端约束，但不能被当成可追溯的数学零点。"
    return "该气点跨温度残差未显示强桥接异常。"


def _root_cause_summary(
    enriched: Sequence[Mapping[str, Any]],
    temp_bias: Sequence[Mapping[str, Any]],
    same_target: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    by_device: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in enriched:
        by_device[str(row["device_id"])].append(row)
    bridge_by_device: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in same_target:
        bridge_by_device[str(row["device_id"])].append(row)
    temp_by_device: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in temp_bias:
        temp_by_device[str(row["device_id"])].append(row)
    out: List[Dict[str, Any]] = []
    for device, rows in sorted(by_device.items()):
        rel_rows = [
            row
            for row in rows
            if _safe_float(row.get("abs_relative_error_percent")) is not None
        ]
        worst = max(
            rel_rows,
            key=lambda row: float(row.get("abs_relative_error_percent") or 0.0),
            default=None,
        )
        sign_flip_count = sum(
            1
            for row in bridge_by_device.get(device, [])
            if str(row.get("bridge_status") or "").startswith("sign_flip")
        )
        physical_span_count = sum(
            1
            for row in bridge_by_device.get(device, [])
            if "physical_state_span" in str(row.get("bridge_status") or "")
        )
        temp_same_sign_count = sum(
            1
            for row in temp_by_device.get(device, [])
            if row.get("temperature_bias_status") == "temperature_group_same_sign_bias"
        )
        t_minus_20_high = [
            row
            for row in rows
            if str(row.get("temperature_group")) == "T-20"
            and (_safe_float(row.get("target_ppm")) or 0.0) > LOW_END_LIMIT_PPM
            and _safe_float(row.get("relative_error_percent")) is not None
        ]
        t_minus_20_negative_fraction = (
            sum(1 for row in t_minus_20_high if float(row["relative_error_percent"]) < 0.0)
            / len(t_minus_20_high)
            if t_minus_20_high
            else 0.0
        )
        zero_errors = [
            abs(float(row["error_ppm"]))
            for row in rows
            if row.get("is_zero_anchor") and _safe_float(row.get("error_ppm")) is not None
        ]
        hypothesis = "review_target_state_and_model_boundary"
        if sign_flip_count and not physical_span_count:
            hypothesis = "low_end_temperature_shape_or_anchor_boundary"
        elif physical_span_count:
            hypothesis = "physical_state_bridge_needed_before_refit"
        if t_minus_20_negative_fraction >= 0.8:
            hypothesis += ";low_temperature_high_end_negative_bias"
        out.append(
            {
                "device_id": device,
                "fit_point_count": len(rows),
                "worst_point_identity": worst.get("point_identity") if worst else "",
                "worst_target_ppm": worst.get("target_ppm") if worst else "",
                "worst_temperature_group": worst.get("temperature_group") if worst else "",
                "max_abs_relative_error_percent": worst.get("abs_relative_error_percent") if worst else "",
                "zero_anchor_max_abs_error_ppm": max(zero_errors) if zero_errors else "",
                "same_target_sign_flip_count": sign_flip_count,
                "same_target_physical_state_span_count": physical_span_count,
                "temperature_same_sign_bias_group_count": temp_same_sign_count,
                "t_minus_20_high_end_negative_fraction": t_minus_20_negative_fraction,
                "primary_hypothesis": hypothesis,
                "recommended_next_action": _next_action(hypothesis),
            }
        )
    return out


def _next_action(hypothesis: str) -> str:
    if "physical_state_bridge_needed" in hypothesis:
        return "先做状态归一化/桥接评审，再更新 S1/S3；不要直接删点或先用 S5 掩盖。"
    if "low_temperature_high_end_negative_bias" in hypothesis:
        return "优先检查 T-20 高端目标状态、温度项边界和高端点一致性。"
    if "low_end_temperature_shape" in hypothesis:
        return "优先审计 T20/T30 低端共同偏差、零气估计和低端温度项；S5 只能后置。"
    return "保留 A 级物理点，继续做模型结构和目标状态审计。"


def build_co2_s13_target_state_bridge_review(
    *,
    fit_points_csv: str | Path,
    residuals_csv: str | Path,
    selected_candidates_csv: str | Path,
) -> Dict[str, List[Dict[str, Any]]]:
    fit_rows = _read_csv(fit_points_csv)
    residual_rows = _read_csv(residuals_csv)
    selected_rows = _read_csv(selected_candidates_csv)
    enriched = _join_residuals_with_state(residual_rows, fit_rows, selected_rows)
    temp_bias = _temperature_group_bias(enriched)
    same_target = _same_target_bridge(enriched)
    summary = _root_cause_summary(enriched, temp_bias, same_target)
    return {
        "selected_residual_state_rows": enriched,
        "temperature_group_bias": temp_bias,
        "same_target_state_bridge": same_target,
        "root_cause_summary": summary,
    }


def _render_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    summary = list(tables.get("root_cause_summary", []))
    same_target = list(tables.get("same_target_state_bridge", []))
    temp_bias = list(tables.get("temperature_group_bias", []))
    lines = [
        "# V1.5 CO2 S1/S3 目标状态桥接审计",
        "",
        f"生成时间：{_now()}",
        "",
        "## 边界",
        "",
        "- 本报告只使用已有 CSV 证据离线计算。",
        "- 不打开 COM、不控制气路/水路、不写 SENCO。",
        "- 压力项不进入 CO2 主拟合判断；S5 只作为后置输出层修正。",
        "",
        "## 结论摘要",
        "",
    ]
    for row in summary:
        lines.append(
            "- 设备 {device}: 最大相对误差 {err}%，最差点 {point}；假设：{hyp}".format(
                device=row.get("device_id"),
                err=_fmt(row.get("max_abs_relative_error_percent")),
                point=row.get("worst_point_identity"),
                hyp=row.get("primary_hypothesis"),
            )
        )
    sign_flip = [
        row for row in same_target if str(row.get("bridge_status") or "").startswith("sign_flip")
    ]
    physical = [
        row for row in same_target if "physical_state_span" in str(row.get("bridge_status") or "")
    ]
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            f"- 同气点跨温度残差换符号的组合数：{len(sign_flip)}。",
            f"- 其中同时带明显露点/压力状态跨度的组合数：{len(physical)}。",
            "- 如果换符号但露点、压力、ratio 稳定性都良好，问题更像 S1/S3 低端温度形状或目标锚定边界，而不是单个点坏。",
            "- 如果同一气点跨温度状态跨度明显，先做状态桥接/归一化评审，再决定是否改模型或降级点位。",
            "",
            "## 温度组提示",
            "",
        ]
    )
    for row in temp_bias:
        if row.get("temperature_bias_status") == "temperature_group_same_sign_bias":
            lines.append(
                "- 设备 {device} {temp}: 非零点同向偏差，均值 {mean_err}%；需要检查该温度组状态或温度项边界。".format(
                    device=row.get("device_id"),
                    temp=row.get("temperature_group"),
                    mean_err=_fmt(row.get("mean_relative_error_percent")),
                )
            )
    lines.extend(
        [
            "",
            "## 建议",
            "",
            "1. 不建议因为 600 ppm 单点看起来偏正就直接剔除；前一轮灵敏度已经显示剔除 600 ppm 不能解决主残差。",
            "2. 0 气估计可以继续作为灵敏度项，但目前不是全局根因；仅个别设备对非零估计明显敏感。",
            "3. 下一步应聚焦 T20/T30 低端同气点符号翻转、T-20 高端负偏和 T40 零点边界，而不是先写 S5。",
            "",
        ]
    )
    return "\n".join(lines)


def _fmt(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{float(number):.3f}"


def write_co2_s13_target_state_bridge_review(
    *,
    fit_points_csv: str | Path,
    residuals_csv: str | Path,
    selected_candidates_csv: str | Path,
    output_dir: str | Path,
) -> Dict[str, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_target_state_bridge_review(
        fit_points_csv=fit_points_csv,
        residuals_csv=residuals_csv,
        selected_candidates_csv=selected_candidates_csv,
    )
    outputs = {
        "selected_residual_state_rows": out_dir / "co2_s13_selected_residual_state_rows.csv",
        "temperature_group_bias": out_dir / "co2_s13_temperature_group_bias.csv",
        "same_target_state_bridge": out_dir / "co2_s13_same_target_state_bridge.csv",
        "root_cause_summary": out_dir / "co2_s13_target_state_bridge_root_cause_summary.csv",
        "metadata": out_dir / "co2_s13_target_state_bridge_meta.json",
        "markdown": out_dir / "co2_s13_target_state_bridge_review_zh.md",
    }
    for key, path in outputs.items():
        if key in {"metadata", "markdown"}:
            continue
        _write_csv(path, tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "generated_at": _now(),
                "fit_points_csv": str(fit_points_csv),
                "residuals_csv": str(residuals_csv),
                "selected_candidates_csv": str(selected_candidates_csv),
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "uses_s5_output_trim": False,
                    "not_real_acceptance_evidence": True,
                },
                "tables": {key: str(path) for key, path in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs["markdown"].write_text("\ufeff" + _render_markdown(tables), encoding="utf-8")
    return outputs
