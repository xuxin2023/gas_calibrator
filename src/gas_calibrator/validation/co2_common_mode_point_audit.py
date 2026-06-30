"""Offline common-mode point audit for V1.5 CO2 SENCO1/SENCO3 fitting.

The audit consumes already-exported fit points and fitting-matrix predictions.
It never opens COM ports, controls routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


@dataclass(frozen=True)
class AuditInputs:
    fit_points_csv: str | Path
    predictions_csv: str | Path
    recommendation_csv: str | Path


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
        writer.writerows([dict(row) for row in rows])


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _safe_int(value: Any) -> Optional[int]:
    number = _safe_float(value)
    if number is None:
        return None
    return int(round(number))


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _numbers(values: Iterable[Any]) -> List[float]:
    result: List[float] = []
    for value in values:
        number = _safe_float(value)
        if number is not None:
            result.append(float(number))
    return result


def _mean(values: Sequence[float]) -> str:
    return f"{mean(values):.6g}" if values else ""


def _median(values: Sequence[float]) -> str:
    return f"{median(values):.6g}" if values else ""


def _std(values: Sequence[float]) -> str:
    return f"{pstdev(values):.6g}" if len(values) > 1 else "0" if values else ""


def _max_abs(values: Sequence[float]) -> str:
    return f"{max(abs(item) for item in values):.6g}" if values else ""


def _range_text(values: Sequence[float]) -> str:
    if not values:
        return ""
    return f"{min(values):.6g}..{max(values):.6g}"


def _relative_error(error_ppm: float, target_ppm: float) -> Optional[float]:
    if abs(target_ppm) <= 1.0e-9:
        return None
    return 100.0 * error_ppm / target_ppm


def _recommended_models(rows: Sequence[Mapping[str, Any]]) -> Dict[str, str]:
    models: Dict[str, str] = {}
    for row in rows:
        item = str(row.get("recommendation_item") or "").strip()
        if not item.startswith("device_") or not item.endswith("_next_candidate"):
            continue
        device = _device_id(item.removeprefix("device_").removesuffix("_next_candidate"))
        model = str(row.get("recommendation") or "").strip()
        if device and model:
            models[device] = model
    return models


def _fit_point_key(row: Mapping[str, Any]) -> Tuple[str, str]:
    return (
        _device_id(row.get("analyzer_device_id") or row.get("device_id")),
        str(row.get("point_identity") or "").strip(),
    )


def _prediction_key(row: Mapping[str, Any]) -> Tuple[str, str]:
    return (
        _device_id(row.get("device_id") or row.get("analyzer_device_id")),
        str(row.get("point_identity") or "").strip(),
    )


def _selected_predictions(
    prediction_rows: Sequence[Mapping[str, Any]],
    recommended_models: Mapping[str, str],
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    fallback_model = "senco13_temperature_terms_pressure_zero"
    for row in prediction_rows:
        device = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
        model = str(row.get("model_id") or "").strip()
        expected = recommended_models.get(device, fallback_model)
        if model == expected:
            selected.append(dict(row))
    return selected


def _status_register_summary(values: Sequence[str]) -> str:
    cleaned = [str(item or "").strip() for item in values if str(item or "").strip()]
    if not cleaned:
        return ""
    unique = sorted(set(cleaned))
    if len(unique) <= 4:
        return ";".join(unique)
    return ";".join(unique[:4]) + f";...(+{len(unique) - 4})"


def _water_vapor_pressure_hpa(dewpoint_c: float) -> float:
    """Saturation vapor pressure at dew/frost point.

    The negative dewpoint points in this project are closer to frost-point
    physics, so use the ice Magnus branch below 0 C. The value is only used as
    a consistency diagnostic against the dewpoint reference, not as a formal
    H2O calibration target.
    """

    if dewpoint_c < 0.0:
        return 6.112 * math.exp((22.46 * dewpoint_c) / (272.62 + dewpoint_c))
    return 6.112 * math.exp((17.62 * dewpoint_c) / (243.12 + dewpoint_c))


def _dewpoint_h2o_mmol(dewpoint_c: Any, pressure_hpa: Any) -> Optional[float]:
    dew = _safe_float(dewpoint_c)
    pressure = _safe_float(pressure_hpa)
    if dew is None or pressure is None or pressure <= 0.0:
        return None
    e_hpa = _water_vapor_pressure_hpa(dew)
    if e_hpa <= 0.0 or e_hpa >= pressure:
        return None
    return 1000.0 * e_hpa / (pressure - e_hpa)


def _review_threshold_ppm(target_ppm: float) -> float:
    # Keep this below the final release target; it is an audit trigger, not an
    # acceptance limit. Zero anchors need an absolute trigger because relative
    # error has no physical meaning at zero.
    return max(3.0, 0.01 * abs(target_ppm))


def _point_hypothesis(row: Mapping[str, Any]) -> Tuple[str, str]:
    common = str(row.get("common_mode_status") or "") == "common_mode_suspect"
    target = _safe_float(row.get("target_ppm_median")) or 0.0
    h2o_mean = _safe_float(row.get("h2o_mmol_mean"))
    h2o_std = _safe_float(row.get("h2o_mmol_std"))
    h2o_reference_delta = _safe_float(row.get("analyzer_minus_dewpoint_h2o_mmol_mean"))
    dew_mean = _safe_float(row.get("dewpoint_c_mean"))
    pressure_std = _safe_float(row.get("pressure_hpa_std"))
    ratio_std_max = _safe_float(row.get("ratio_std_max"))
    zero_classes = str(row.get("zero_anchor_classes") or "").lower()
    status_text = str(row.get("status_register_qc_values") or "").lower()

    flags: List[str] = []
    if "estimated_zero" in zero_classes or abs(target) <= 1.0e-9:
        flags.append("zero_anchor_uncertainty")
    if h2o_mean is not None and h2o_mean > 20.0:
        flags.append("analyzer_h2o_output_high")
    if h2o_std is not None and h2o_std > 5.0:
        flags.append("analyzer_h2o_output_inconsistent_across_devices")
    if h2o_reference_delta is not None and abs(h2o_reference_delta) > 2.0:
        flags.append("analyzer_h2o_not_consistent_with_dewpoint_reference")
    if dew_mean is not None and dew_mean > -28.0:
        flags.append("dewpoint_not_deep_dry")
    if pressure_std is not None and pressure_std > 1.0:
        flags.append("pressure_state_spread")
    if ratio_std_max is not None and ratio_std_max > 0.0005:
        flags.append("ratio_A_gate_not_met_on_at_least_one_device")
    if status_text and status_text not in {"missing", "pass", "ok", "0"}:
        flags.append("status_register_evidence_present")

    if not common:
        return (
            "not_common_mode",
            "未呈现多设备同向大残差；优先按单台设备拟合形状或局部采样质量检查。",
        )
    if "zero_anchor_uncertainty" in flags:
        return (
            "estimated_zero_anchor_common_bias",
            "多设备同向偏差发生在零点/估算零锚附近，可能是零气 CO2 含量、低端截距或零点不确定度造成，不能直接用 S5 吸收。",
        )
    if "dewpoint_not_deep_dry" in flags:
        return (
            "reference_dewpoint_dryness_margin",
            "多设备同向偏差发生在露点参考未达到深干裕量的点位，应先确认管路干燥和开放流通状态，再判断是否进入 S1/S3 主拟合。",
        )
    if "ratio_A_gate_not_met_on_at_least_one_device" in flags:
        return (
            "ratio_window_quality_common_risk",
            "多设备同向偏差且至少一台 ratio 稳定性未达 A 级目标，优先审采样窗口是否真正代表稳定标准气。",
        )
    return (
        "source_route_target_or_model_common_bias",
        "多设备同向大残差且 ratio 稳定、露点参考可解释时，优先审气瓶目标、阀路映射、零点锚定和 S1/S3 模型形状；未校准的 CO2/H2O 输出不能作为拒绝依据。",
    )


def _point_action(row: Mapping[str, Any], hypothesis: str) -> str:
    if str(row.get("common_mode_status") or "") != "common_mode_suspect":
        return "保留为普通拟合证据；若该点仍是单台最差点，再进入单台残差审计。"
    if hypothesis == "estimated_zero_anchor_common_bias":
        return "不要把该零点当成无不确定度真零；按估算零锚做灵敏度，必要时使用有 CO2 证书的零气或给定低端绝对不确定度。"
    if hypothesis == "reference_dewpoint_dryness_margin":
        return "以露点参考和 ratio 稳定窗口为准判断样气状态；若未达到深干目标，则降级或延长吹扫，不用分析仪输出值替代参考。"
    if hypothesis == "ratio_window_quality_common_risk":
        return "审计 ratio 稳定窗口；若窗口不达 A 级或阀路状态不连续，则剔除/降级并记录原因。"
    return "先核对气瓶证书值、阀路映射、点位标签、零点锚定和模型形状；H2O 输出不可信时只禁止 H2O bridge，不直接拒绝该 CO2 ratio 点。"


def _build_point_audit_rows(
    *,
    selected_predictions: Sequence[Mapping[str, Any]],
    fit_points_by_key: Mapping[Tuple[str, str], Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    by_point: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in selected_predictions:
        role = str(row.get("source_role") or "").strip().lower()
        if role and role not in {"fit", "verification"}:
            continue
        by_point[str(row.get("point_identity") or "").strip()].append(row)

    rows: List[Dict[str, Any]] = []
    for point_identity, prediction_group in sorted(by_point.items()):
        if not point_identity:
            continue
        errors = _numbers(row.get("error_ppm") for row in prediction_group)
        targets = _numbers(row.get("target_ppm") for row in prediction_group)
        if not errors or not targets:
            continue
        target_median = median(targets)
        threshold = _review_threshold_ppm(target_median)
        positive = sum(1 for error in errors if error > 1.0e-9)
        negative = sum(1 for error in errors if error < -1.0e-9)
        sign_consensus = max(positive, negative) / len(errors) if errors else 0.0
        mean_error = mean(errors)
        common = (
            len(errors) >= 3
            and sign_consensus >= 0.8
            and abs(mean_error) >= threshold
        )

        fit_rows = [fit_points_by_key.get(_prediction_key(row), {}) for row in prediction_group]
        h2o_values = _numbers(row.get("h2o_mmol_mean") or row.get("h2o_mmol") for row in fit_rows)
        dew_values = _numbers(row.get("dewpoint_mean_c") or row.get("dewpoint_c") for row in fit_rows)
        pressure_values = _numbers(
            row.get("pressure_gauge_mean_hpa")
            or row.get("pressure_hpa")
            or row.get("pressure_gauge_hpa")
            for row in fit_rows
        )
        dewpoint_h2o_values = _numbers(
            _dewpoint_h2o_mmol(
                row.get("dewpoint_mean_c") or row.get("dewpoint_c"),
                row.get("pressure_gauge_mean_hpa")
                or row.get("pressure_hpa")
                or row.get("pressure_gauge_hpa"),
            )
            for row in fit_rows
        )
        h2o_delta_values: List[float] = []
        for row in fit_rows:
            analyzer_h2o = _safe_float(row.get("h2o_mmol_mean") or row.get("h2o_mmol"))
            reference_h2o = _dewpoint_h2o_mmol(
                row.get("dewpoint_mean_c") or row.get("dewpoint_c"),
                row.get("pressure_gauge_mean_hpa")
                or row.get("pressure_hpa")
                or row.get("pressure_gauge_hpa"),
            )
            if analyzer_h2o is not None and reference_h2o is not None:
                h2o_delta_values.append(analyzer_h2o - reference_h2o)
        ratio_std_values = _numbers(row.get("co2_ratio_f_std") for row in fit_rows)
        ratio_values = _numbers(row.get("ratio") or row.get("co2_ratio_f_mean") for row in fit_rows)
        ref_values = _numbers(row.get("ref_signal_mean") for row in fit_rows)
        co2_signal_values = _numbers(row.get("co2_signal_mean") for row in fit_rows)
        sample_counts = _numbers(row.get("usable_sample_count") or row.get("sample_count") for row in fit_rows)
        zero_classes = sorted(set(str(row.get("zero_anchor_class") or "").strip() for row in fit_rows if str(row.get("zero_anchor_class") or "").strip()))
        status_values = [str(row.get("status_register_qc_values") or "").strip() for row in fit_rows]
        rel_errors = [
            _relative_error(error, target)
            for error, target in zip(errors, targets)
            if _relative_error(error, target) is not None
        ]
        model_ids = sorted(set(str(row.get("model_id") or "") for row in prediction_group if str(row.get("model_id") or "")))
        h2o_delta_mean = mean(h2o_delta_values) if h2o_delta_values else None
        h2o_bridge_status = (
            "do_not_use_analyzer_h2o_output_for_co2_bridge"
            if h2o_delta_mean is not None and abs(h2o_delta_mean) > 2.0
            else "dewpoint_and_analyzer_h2o_consistent_for_bridge_review"
            if h2o_delta_mean is not None
            else "no_dewpoint_h2o_bridge_evidence"
        )

        row = {
            "point_identity": point_identity,
            "target_ppm_median": f"{target_median:.6g}",
            "device_count": len(errors),
            "devices": ";".join(sorted(_device_id(row.get("device_id")) for row in prediction_group)),
            "selected_models": ";".join(model_ids),
            "mean_error_ppm": f"{mean_error:.6g}",
            "median_error_ppm": _median(errors),
            "max_abs_error_ppm": _max_abs(errors),
            "max_abs_relative_error_percent": _max_abs(rel_errors),
            "positive_error_count": positive,
            "negative_error_count": negative,
            "sign_consensus_fraction": f"{sign_consensus:.6g}",
            "review_threshold_ppm": f"{threshold:.6g}",
            "common_mode_status": "common_mode_suspect" if common else "not_common_mode",
            "h2o_mmol_mean": _mean(h2o_values),
            "h2o_mmol_std": _std(h2o_values),
            "dewpoint_derived_h2o_mmol_mean": _mean(dewpoint_h2o_values),
            "analyzer_minus_dewpoint_h2o_mmol_mean": _mean(h2o_delta_values),
            "h2o_bridge_input_status": h2o_bridge_status,
            "dewpoint_c_mean": _mean(dew_values),
            "dewpoint_c_range": _range_text(dew_values),
            "pressure_hpa_mean": _mean(pressure_values),
            "pressure_hpa_std": _std(pressure_values),
            "ratio_mean": _mean(ratio_values),
            "ratio_range": _range_text(ratio_values),
            "ratio_std_max": _max_abs(ratio_std_values),
            "ref_signal_mean": _mean(ref_values),
            "ref_signal_std": _std(ref_values),
            "co2_signal_mean": _mean(co2_signal_values),
            "co2_signal_std": _std(co2_signal_values),
            "usable_sample_count_min": min(sample_counts) if sample_counts else "",
            "zero_anchor_classes": ";".join(zero_classes),
            "status_register_qc_values": _status_register_summary(status_values),
        }
        hypothesis, meaning = _point_hypothesis(row)
        row["root_cause_hypothesis"] = hypothesis
        row["physical_meaning"] = meaning
        row["recommended_action"] = _point_action(row, hypothesis)
        rows.append(row)

    rows.sort(
        key=lambda row: (
            0 if row["common_mode_status"] == "common_mode_suspect" else 1,
            -abs(_safe_float(row.get("mean_error_ppm")) or 0.0),
            str(row.get("point_identity") or ""),
        )
    )
    return rows


def _build_device_worst_rows(
    *,
    selected_predictions: Sequence[Mapping[str, Any]],
    fit_points_by_key: Mapping[Tuple[str, str], Mapping[str, Any]],
    top_n: int = 8,
) -> List[Dict[str, Any]]:
    by_device: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in selected_predictions:
        by_device[_device_id(row.get("device_id") or row.get("analyzer_device_id"))].append(row)

    rows: List[Dict[str, Any]] = []
    for device, group in sorted(by_device.items()):
        scored: List[Tuple[float, Mapping[str, Any]]] = []
        for row in group:
            error = _safe_float(row.get("error_ppm"))
            target = _safe_float(row.get("target_ppm"))
            if error is None or target is None:
                continue
            relative = _relative_error(error, target)
            score = abs(relative) if relative is not None else abs(error)
            scored.append((score, row))
        for rank, (_, row) in enumerate(sorted(scored, key=lambda item: item[0], reverse=True)[:top_n], start=1):
            fit_row = fit_points_by_key.get(_prediction_key(row), {})
            error = _safe_float(row.get("error_ppm")) or 0.0
            target = _safe_float(row.get("target_ppm")) or 0.0
            relative = _relative_error(error, target)
            rows.append(
                {
                    "device_id": device,
                    "analyzer_prefix": row.get("analyzer_prefix") or fit_row.get("analyzer_prefix") or "",
                    "rank": rank,
                    "point_identity": row.get("point_identity") or "",
                    "model_id": row.get("model_id") or "",
                    "target_ppm": row.get("target_ppm") or "",
                    "prediction_ppm": row.get("prediction_ppm") or "",
                    "error_ppm": row.get("error_ppm") or "",
                    "relative_error_percent": f"{relative:.6g}" if relative is not None else "",
                    "ratio": fit_row.get("ratio") or fit_row.get("co2_ratio_f_mean") or row.get("ratio") or "",
                    "ratio_std": fit_row.get("co2_ratio_f_std") or "",
                    "temperature_c": fit_row.get("temperature_c") or row.get("temperature_c") or "",
                    "thermometer_temp_mean_c": fit_row.get("thermometer_temp_mean_c") or "",
                    "h2o_mmol": fit_row.get("h2o_mmol_mean") or row.get("h2o_mmol") or "",
                    "dewpoint_mean_c": fit_row.get("dewpoint_mean_c") or "",
                    "pressure_hpa": fit_row.get("pressure_gauge_mean_hpa") or row.get("pressure_hpa") or "",
                    "ref_signal_mean": fit_row.get("ref_signal_mean") or "",
                    "co2_signal_mean": fit_row.get("co2_signal_mean") or "",
                    "source_sample_path": fit_row.get("source_sample_path") or "",
                }
            )
    return rows


def build_co2_common_mode_point_audit_tables(
    *,
    inputs: AuditInputs,
) -> Dict[str, List[Dict[str, Any]]]:
    fit_rows = _read_csv(inputs.fit_points_csv)
    prediction_rows = _read_csv(inputs.predictions_csv)
    recommendation_rows = _read_csv(inputs.recommendation_csv)
    recommended = _recommended_models(recommendation_rows)
    selected = _selected_predictions(prediction_rows, recommended)
    fit_by_key = {_fit_point_key(row): row for row in fit_rows}
    point_rows = _build_point_audit_rows(
        selected_predictions=selected,
        fit_points_by_key=fit_by_key,
    )
    worst_rows = _build_device_worst_rows(
        selected_predictions=selected,
        fit_points_by_key=fit_by_key,
    )
    summary_rows = [
        {
            "metric": "fit_point_rows",
            "value": len(fit_rows),
            "meaning": "输入拟合点记录数。",
        },
        {
            "metric": "selected_prediction_rows",
            "value": len(selected),
            "meaning": "按每台推荐 S1/S3 合同筛选后的预测记录数。",
        },
        {
            "metric": "common_mode_suspect_points",
            "value": sum(1 for row in point_rows if row.get("common_mode_status") == "common_mode_suspect"),
            "meaning": "多设备同向大残差点数；这些点应先审物理状态，不应直接用 S5 掩盖。",
        },
        {
            "metric": "selected_device_models",
            "value": json.dumps(recommended, ensure_ascii=False, sort_keys=True),
            "meaning": "每台设备本轮审计采用的候选 S1/S3 模型。",
        },
    ]
    return {
        "co2_common_mode_point_audit_summary": summary_rows,
        "co2_common_mode_point_audit": point_rows,
        "co2_device_worst_point_audit": worst_rows,
    }


def write_co2_common_mode_point_audit_report(
    *,
    inputs: AuditInputs,
    output_dir: str | Path,
) -> Dict[str, Path]:
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    tables = build_co2_common_mode_point_audit_tables(inputs=inputs)
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        path = destination / f"{name}.csv"
        _write_csv(path, rows)
        outputs[f"{name}_csv"] = path
    meta = {
        "tool_name": "export_v1_5_co2_common_mode_point_audit",
        "created_at": _now(),
        "inputs": {
            "fit_points_csv": str(Path(inputs.fit_points_csv).resolve()),
            "predictions_csv": str(Path(inputs.predictions_csv).resolve()),
            "recommendation_csv": str(Path(inputs.recommendation_csv).resolve()),
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    meta_path = destination / "co2_common_mode_point_audit_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["meta_json"] = meta_path
    outputs["markdown"] = _write_markdown(destination / "co2_common_mode_point_audit_zh.md", tables)
    return outputs


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    point_rows = list(tables.get("co2_common_mode_point_audit") or [])
    suspect_rows = [row for row in point_rows if row.get("common_mode_status") == "common_mode_suspect"]
    worst_rows = list(tables.get("co2_device_worst_point_audit") or [])
    lines = [
        "# V1.5 CO2 S1/S3 共模点位审计报告",
        "",
        "- 边界：离线审计；不打开 COM；不控制气路/水路；不写 SENCO。",
        "- 目的：把“多台设备同时同向偏差”的点位先从物理状态上解释清楚，再决定是否进入 S1/S3 主拟合。",
        "- 物理原则：S1/S3 是 CO2 光学比值与温度主链路；S5 是最终显示层线性修正，不能用来掩盖气瓶、阀路、露点、采样窗口或零气锚点问题。",
        "",
        "## 共模可疑点",
        "",
    ]
    if not suspect_rows:
        lines.append("未发现达到共模阈值的多设备同向大残差点。")
    else:
        for row in suspect_rows[:12]:
            lines.append(
                "- "
                f"{row.get('point_identity')}: 平均误差 {row.get('mean_error_ppm')} ppm，"
                f"最大绝对误差 {row.get('max_abs_error_ppm')} ppm，"
                f"同向比例 {row.get('sign_consensus_fraction')}，"
                f"H2O {row.get('h2o_mmol_mean')} mmol/mol，"
                f"露点 {row.get('dewpoint_c_mean')} °C，"
                f"判断：{row.get('physical_meaning')}"
            )
    lines.extend(
        [
            "",
            "## 处理建议",
            "",
        ]
    )
    for row in suspect_rows[:12]:
        lines.append(f"- {row.get('point_identity')}: {row.get('recommended_action')}")
    if not suspect_rows:
        lines.append("- 当前可以优先转入单台设备最差点审计。")
    lines.extend(
        [
            "",
            "## 逐台最差点",
            "",
        ]
    )
    for row in worst_rows[:24]:
        lines.append(
            "- "
            f"{row.get('device_id')} #{row.get('rank')} {row.get('point_identity')}: "
            f"误差 {row.get('error_ppm')} ppm，"
            f"相对误差 {row.get('relative_error_percent')}%，"
            f"H2O {row.get('h2o_mmol')} mmol/mol，"
            f"ratio std {row.get('ratio_std')}"
        )
    lines.extend(
        [
            "",
            "## 下一步",
            "",
            "1. 对共模点先核查气瓶目标、阀路映射、露点/H2O、ratio 稳定窗口、状态寄存器和样本路径。",
            "2. 若证据显示点位物理状态不合格，则降级或剔除，并在报告中记录拒绝原因。",
            "3. 若点位物理状态合格，再检查目标值和零气估计；仍不能解释时才重新评估 S1/S3 模型。",
            "4. 只有 S1/S3 主链路残差已合理后，才进入 S5 输出层修正评审。",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


# Keep the corrected Chinese/physics contract below the legacy definitions.
# Python resolves these names at call time, so the audit uses the corrected
# ratio-first interpretation without relying on fragile mojibake string edits.
def _point_hypothesis(row: Mapping[str, Any]) -> Tuple[str, str]:
    common = str(row.get("common_mode_status") or "") == "common_mode_suspect"
    target = _safe_float(row.get("target_ppm_median")) or 0.0
    h2o_mean = _safe_float(row.get("h2o_mmol_mean"))
    h2o_std = _safe_float(row.get("h2o_mmol_std"))
    h2o_reference_delta = _safe_float(row.get("analyzer_minus_dewpoint_h2o_mmol_mean"))
    dew_mean = _safe_float(row.get("dewpoint_c_mean"))
    pressure_std = _safe_float(row.get("pressure_hpa_std"))
    ratio_std_max = _safe_float(row.get("ratio_std_max"))
    zero_classes = str(row.get("zero_anchor_classes") or "").lower()
    status_text = str(row.get("status_register_qc_values") or "").lower()

    flags: List[str] = []
    if "estimated_zero" in zero_classes or abs(target) <= 1.0e-9:
        flags.append("zero_anchor_uncertainty")
    if h2o_mean is not None and h2o_mean > 20.0:
        flags.append("analyzer_h2o_output_high")
    if h2o_std is not None and h2o_std > 5.0:
        flags.append("analyzer_h2o_output_inconsistent_across_devices")
    if h2o_reference_delta is not None and abs(h2o_reference_delta) > 2.0:
        flags.append("analyzer_h2o_not_consistent_with_dewpoint_reference")
    if dew_mean is not None and dew_mean > -28.0:
        flags.append("dewpoint_not_deep_dry")
    if pressure_std is not None and pressure_std > 1.0:
        flags.append("pressure_state_spread")
    if ratio_std_max is not None and ratio_std_max > 0.0005:
        flags.append("ratio_A_gate_not_met_on_at_least_one_device")
    if status_text and status_text not in {"missing", "pass", "ok", "0"}:
        flags.append("status_register_evidence_present")

    if not common:
        return (
            "not_common_mode",
            "未呈现多设备同向大残差；优先按单台设备拟合形状或局部采样质量检查。",
        )
    if "zero_anchor_uncertainty" in flags:
        return (
            "estimated_zero_anchor_common_bias",
            "多设备同向偏差发生在零点或估算零锚附近，可能来自零气 CO2 含量、低端截距或零点不确定度；不能直接用 S5 吸收。",
        )
    if "dewpoint_not_deep_dry" in flags:
        return (
            "reference_dewpoint_dryness_margin",
            "多设备同向偏差发生在露点参考未达到深干余量的点位，应先确认管路干燥和开放流通状态，再判断是否进入 S1/S3 主拟合。",
        )
    if "ratio_A_gate_not_met_on_at_least_one_device" in flags:
        return (
            "ratio_window_quality_common_risk",
            "多设备同向偏差且至少一台 ratio 稳定性未达 A 级目标，优先审查采样窗口是否真正代表稳定标准气。",
        )
    return (
        "source_route_target_or_model_common_bias",
        "多设备同向大残差且 ratio 稳定、露点参考可解释时，优先审气瓶目标、阀路映射、零点锚定和 S1/S3 模型形状；未校准的 CO2/H2O 输出不能作为拒绝依据。",
    )


def _point_action(row: Mapping[str, Any], hypothesis: str) -> str:
    if str(row.get("common_mode_status") or "") != "common_mode_suspect":
        return "保留为普通拟合证据；若该点仍是单台最差点，再进入单台残差审计。"
    if hypothesis == "estimated_zero_anchor_common_bias":
        return "不要把该零点当成无不确定度真零；按估算零锚做灵敏度，必要时使用有 CO2 证书的零气或给定低端绝对不确定度。"
    if hypothesis == "reference_dewpoint_dryness_margin":
        return "以露点参考和 ratio 稳定窗口为准判断样气状态；若未达到深干目标，则降级或延长吹扫，不用分析仪输出值替代参考。"
    if hypothesis == "ratio_window_quality_common_risk":
        return "审计 ratio 稳定窗口；若窗口不达 A 级或阀路状态不连续，则剔除或降级并记录原因。"
    return "先核对气瓶证书值、阀路映射、点位标签、零点锚定和模型形状；H2O 输出不可信时只禁用 H2O bridge，不直接拒绝该 CO2 ratio 点。"


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    point_rows = list(tables.get("co2_common_mode_point_audit") or [])
    suspect_rows = [row for row in point_rows if row.get("common_mode_status") == "common_mode_suspect"]
    worst_rows = list(tables.get("co2_device_worst_point_audit") or [])
    lines = [
        "# V1.5 CO2 S1/S3 共模点位审计报告",
        "",
        "- 边界：离线审计；不打开 COM；不控制气路/水路；不写 SENCO。",
        "- 目的：把多台设备同一气点同向偏差从物理状态上解释清楚，再决定是否进入 S1/S3 主拟合。",
        "- 物理原则：S1/S3 主链路以滤波后 CO2 ratio、标准气证书值、温度输入和当前大气压开放流通状态为核心；校准前的浓度输出/H2O 输出可能本来就是错的，不能作为拒绝 CO2 ratio 点的直接依据。",
        "- S5 定位：S5 是最终显示层线性修正，只有在 S1/S3 主链路残差已经合理后才进入评审，不能用来掩盖气瓶、阀路、露点、采样窗口或零气锚点问题。",
        "",
        "## 共模可疑点",
        "",
    ]
    if not suspect_rows:
        lines.append("未发现达到共模阈值的多设备同向大残差点。")
    else:
        for row in suspect_rows[:12]:
            lines.append(
                "- "
                f"{row.get('point_identity')}: 平均误差 {row.get('mean_error_ppm')} ppm，"
                f"最大绝对误差 {row.get('max_abs_error_ppm')} ppm，"
                f"同向比例 {row.get('sign_consensus_fraction')}，"
                f"ratio std max {row.get('ratio_std_max')}，"
                f"露点 {row.get('dewpoint_c_mean')} C，"
                f"露点折算 H2O {row.get('dewpoint_derived_h2o_mmol_mean')} mmol/mol，"
                f"分析仪 H2O 输出状态 {row.get('h2o_bridge_input_status')}。"
            )
            lines.append(f"  - 判断：{row.get('physical_meaning')}")
    lines.extend(["", "## 处理建议", ""])
    for row in suspect_rows[:12]:
        lines.append(f"- {row.get('point_identity')}: {row.get('recommended_action')}")
    if not suspect_rows:
        lines.append("- 当前可以优先转入单台设备最差点审计。")
    lines.extend(["", "## 逐台最差点", ""])
    for row in worst_rows[:24]:
        lines.append(
            "- "
            f"{row.get('device_id')} #{row.get('rank')} {row.get('point_identity')}: "
            f"误差 {row.get('error_ppm')} ppm，"
            f"相对误差 {row.get('relative_error_percent')}%，"
            f"ratio {row.get('ratio')}，"
            f"ratio std {row.get('ratio_std')}，"
            f"露点 {row.get('dewpoint_mean_c')} C。"
        )
    lines.extend(
        [
            "",
            "## 下一步",
            "",
            "1. 对共模点先核查气瓶目标、阀路映射、露点、ratio 稳定窗口、状态寄存器和样本路径。",
            "2. 若证据显示点位物理状态不合格，则降级或剔除，并在报告中记录拒绝原因。",
            "3. 若点位物理状态合格，再检查目标值和零气估计；仍不能解释时，重新评估 S1/S3 模型形状和低端截距。",
            "4. 只有 S1/S3 主链路残差已经合理后，才进入 S5 输出层修正评审。",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path
