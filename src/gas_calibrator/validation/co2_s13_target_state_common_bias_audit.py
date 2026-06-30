"""Offline CO2 target-state and common-bias root-cause audit.

This review consumes already-generated V1.5 CO2 S1/S3 residual evidence and
the original fit point metadata. It is intentionally no-write: it does not open
COM ports, control any gas/water route, or write SENCO. The purpose is to
separate per-analyzer fitting error from shared point/temperature/route-state
error, then estimate whether a final SENCO5 affine output trim could bring the
displayed CO2 error below the requested relative-error target.
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

from .co2_senco5_linear_trim_review import (
    _fit_linear_trim,
    _fit_quantized_command_trim,
)


@dataclass(frozen=True)
class Co2TargetStateCommonBiasAuditConfig:
    acceptance_pct: float = 1.0
    min_relative_target_ppm: float = 50.0
    common_sign_fraction_threshold: float = 0.8
    common_bias_ppm_threshold: float = 5.0
    zero_bias_ppm_threshold: float = 2.0
    dry_dewpoint_gate_c: float = -28.0
    ratio_std_a_gate: float = 0.0005
    pressure_spread_gate_hpa: float = 3.0
    command_c0_decimals: int = 3
    command_c1_decimals: int = 3
    command_c1_min: float = 0.0
    command_c1_max: float = 2.0


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    source = Path(path)
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    keys: List[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in keys})


def _mean(values: Iterable[Any]) -> Optional[float]:
    numeric = [value for value in (_safe_float(item) for item in values) if value is not None]
    if not numeric:
        return None
    return float(sum(numeric) / len(numeric))


def _minimum(values: Iterable[Any]) -> Optional[float]:
    numeric = [value for value in (_safe_float(item) for item in values) if value is not None]
    return min(numeric) if numeric else None


def _maximum(values: Iterable[Any]) -> Optional[float]:
    numeric = [value for value in (_safe_float(item) for item in values) if value is not None]
    return max(numeric) if numeric else None


def _std(values: Iterable[Any]) -> Optional[float]:
    numeric = [value for value in (_safe_float(item) for item in values) if value is not None]
    if len(numeric) < 2:
        return None
    avg = sum(numeric) / len(numeric)
    return float(math.sqrt(sum((value - avg) ** 2 for value in numeric) / (len(numeric) - 1)))


def _parse_temperature_group(point_identity: Any, fallback: Any = None) -> str:
    text = str(point_identity or "")
    match = re.search(r"T(-?\d+(?:\.\d+)?)", text)
    if match:
        value = float(match.group(1))
        return f"T{value:g}"
    value = _safe_float(fallback)
    if value is not None:
        return f"T{value:g}"
    return ""


def _parse_target_group(point_identity: Any, target_ppm: Any = None) -> str:
    text = str(point_identity or "")
    match = re.search(r"(-?\d+(?:\.\d+)?)ppm", text, re.IGNORECASE)
    if match:
        return f"{float(match.group(1)):g}ppm"
    value = _safe_float(target_ppm)
    if value is not None:
        return f"{value:g}ppm"
    return ""


def _point_sort_key(point_identity: Any) -> Tuple[float, float, str]:
    temp_group = _parse_temperature_group(point_identity)
    target_group = _parse_target_group(point_identity)
    temp = _safe_float(temp_group.removeprefix("T")) if temp_group else 999.0
    target = _safe_float(target_group.removesuffix("ppm")) if target_group else 999999.0
    return (temp if temp is not None else 999.0, target if target is not None else 999999.0, str(point_identity))


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _physical_index(fit_rows: Sequence[Mapping[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], List[Mapping[str, Any]]] = {}
    for row in fit_rows:
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        point = str(row.get("point_identity") or "")
        if device and point:
            grouped.setdefault((device, point), []).append(row)

    index: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for key, rows in grouped.items():
        first = rows[0]
        index[key] = {
            "physical_source_rows": len(rows),
            "source_label": first.get("source_label", ""),
            "source_sample_path": first.get("source_sample_path", ""),
            "fit_inclusion_status": first.get("fit_inclusion_status", ""),
            "fit_inclusion_reason": first.get("fit_inclusion_reason", ""),
            "status_register_qc_values": first.get("status_register_qc_values", ""),
            "source_nominal_ppm": first.get("source_nominal_ppm", ""),
            "temp_set_c": _mean(row.get("temp_set_c") for row in rows),
            "dewpoint_mean_c": _mean(
                row.get("dewpoint_mean_c") or row.get("dewpoint_c_mean") for row in rows
            ),
            "co2_ratio_f_std": _mean(row.get("co2_ratio_f_std") for row in rows),
            "co2_ratio_f_std_max": _maximum(row.get("co2_ratio_f_std") for row in rows),
            "ratio": _mean(row.get("ratio") for row in rows),
            "temperature_c": _mean(row.get("temperature_c") for row in rows),
            "thermometer_temp_mean_c": _mean(row.get("thermometer_temp_mean_c") for row in rows),
            "analyzer_chamber_temp_mean_c": _mean(
                row.get("analyzer_chamber_temp_mean_c") for row in rows
            ),
            "pressure_hpa": _mean(row.get("pressure_hpa") for row in rows),
            "pressure_gauge_mean_hpa": _mean(row.get("pressure_gauge_mean_hpa") for row in rows),
            "h2o_mmol_mean": _mean(row.get("h2o_mmol_mean") for row in rows),
            "sample_count": _minimum(row.get("sample_count") for row in rows),
            "usable_sample_count": _minimum(row.get("usable_sample_count") for row in rows),
            "ref_signal_mean": _mean(row.get("ref_signal_mean") for row in rows),
            "co2_signal_mean": _mean(row.get("co2_signal_mean") for row in rows),
        }
    return index


def _selected_residual_rows(
    residual_rows: Sequence[Mapping[str, Any]],
    physical: Mapping[Tuple[str, str], Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    for row in residual_rows:
        if "selected_strategy" in row and not _truthy(row.get("selected_strategy")):
            continue
        if _truthy(row.get("diagnostic_only")):
            continue
        if str(row.get("source_role") or "fit").strip().lower() != "fit":
            continue
        device = _device_id(row.get("device_id"))
        point = str(row.get("point_identity") or "")
        item = dict(row)
        item["device_id"] = device
        item["point_identity"] = point
        item["temperature_group"] = _parse_temperature_group(point, row.get("temperature_c"))
        item["target_group"] = _parse_target_group(point, row.get("target_ppm"))
        phys = physical.get((device, point), {})
        for key, value in phys.items():
            item[f"physical_{key}"] = value
        selected.append(item)
    return selected


def _same_sign_metrics(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    positives = 0
    negatives = 0
    zeros = 0
    for row in rows:
        value = _safe_float(row.get("error_ppm"))
        if value is None:
            continue
        if value > 0:
            positives += 1
        elif value < 0:
            negatives += 1
        else:
            zeros += 1
    total = positives + negatives + zeros
    dominant = max(positives, negatives, zeros)
    direction = "positive" if positives >= negatives and positives >= zeros else "negative"
    if zeros > positives and zeros > negatives:
        direction = "zero"
    return {
        "positive_count": positives,
        "negative_count": negatives,
        "zero_count": zeros,
        "same_sign_fraction": dominant / total if total else 0.0,
        "dominant_sign": direction,
    }


def _relative_error(row: Mapping[str, Any], *, min_target_ppm: float) -> Optional[float]:
    target = _safe_float(row.get("target_ppm"))
    error = _safe_float(row.get("error_ppm"))
    if target is None or error is None or abs(target) < float(min_target_ppm):
        return None
    return error / target * 100.0


def _classify_group(
    rows: Sequence[Mapping[str, Any]],
    *,
    group_kind: str,
    group_id: str,
    cfg: Co2TargetStateCommonBiasAuditConfig,
) -> Tuple[str, str]:
    if not rows:
        return "no_rows", ""
    signs = _same_sign_metrics(rows)
    mean_error = _mean(row.get("error_ppm") for row in rows) or 0.0
    target_values = [_safe_float(row.get("target_ppm")) for row in rows]
    is_zero_group = all((value is not None and abs(value) < cfg.min_relative_target_ppm) for value in target_values)
    same_sign = float(signs["same_sign_fraction"]) >= cfg.common_sign_fraction_threshold
    common_bias = abs(mean_error) >= (
        cfg.zero_bias_ppm_threshold if is_zero_group else cfg.common_bias_ppm_threshold
    )
    dewpoint_max = _maximum(row.get("physical_dewpoint_mean_c") for row in rows)
    ratio_std_max = _maximum(row.get("physical_co2_ratio_f_std") for row in rows)
    pressure_spread = _std(row.get("physical_pressure_hpa") for row in rows)
    flags: List[str] = []

    if is_zero_group and same_sign and common_bias:
        flags.append("零气 CO2 估计或低端目标值需要审计")
    if not is_zero_group and same_sign and common_bias:
        flags.append("同一气点多设备同号偏差，优先审计证书值/点位映射/共同管路状态")
    if group_kind in {"temperature_group", "point_identity"} and (
        group_id.startswith("T20") or group_id.startswith("T40")
    ) and common_bias:
        flags.append("T20/T40 温度项模型边界或该温度组目标状态需要审计")
    if dewpoint_max is not None and dewpoint_max > cfg.dry_dewpoint_gate_c:
        flags.append("露点未达到深干门限，需审计水汽稀释/干燥目标状态")
    if ratio_std_max is not None and ratio_std_max > cfg.ratio_std_a_gate:
        flags.append("ratio 稳定性未达到 A 级门限")
    if pressure_spread is not None and pressure_spread > cfg.pressure_spread_gate_hpa:
        flags.append("开放流通压力状态在设备间差异偏大")

    if flags:
        if same_sign and common_bias:
            classification = "common_target_state_or_model_boundary"
        elif is_zero_group:
            classification = "zero_anchor_state_review"
        else:
            classification = "physical_state_review"
    else:
        classification = "device_specific_or_random_residual"
        flags.append("未见明确共同同号偏差，优先按逐设备残差处理")
    return classification, "；".join(flags)


def _aggregate(
    rows: Sequence[Mapping[str, Any]],
    *,
    key_name: str,
    cfg: Co2TargetStateCommonBiasAuditConfig,
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = {}
    for row in rows:
        key = str(row.get(key_name) or "")
        if key:
            grouped.setdefault(key, []).append(row)

    out: List[Dict[str, Any]] = []
    for key, items in grouped.items():
        signs = _same_sign_metrics(items)
        relative_errors = [
            value for value in (_relative_error(row, min_target_ppm=cfg.min_relative_target_ppm) for row in items)
            if value is not None
        ]
        classification, reason = _classify_group(items, group_kind=key_name, group_id=key, cfg=cfg)
        out.append(
            {
                key_name: key,
                "row_count": len(items),
                "device_count": len({row.get("device_id") for row in items}),
                "target_ppm_mean": _mean(row.get("target_ppm") for row in items),
                "mean_error_ppm": _mean(row.get("error_ppm") for row in items),
                "max_abs_error_ppm": _maximum(abs(_safe_float(row.get("error_ppm")) or 0.0) for row in items),
                "mean_relative_error_percent": _mean(relative_errors),
                "max_abs_relative_error_percent": max((abs(value) for value in relative_errors), default=""),
                "positive_count": signs["positive_count"],
                "negative_count": signs["negative_count"],
                "same_sign_fraction": signs["same_sign_fraction"],
                "dominant_sign": signs["dominant_sign"],
                "dewpoint_mean_c": _mean(row.get("physical_dewpoint_mean_c") for row in items),
                "dewpoint_max_c": _maximum(row.get("physical_dewpoint_mean_c") for row in items),
                "ratio_std_max": _maximum(row.get("physical_co2_ratio_f_std") for row in items),
                "pressure_hpa_std": _std(row.get("physical_pressure_hpa") for row in items),
                "h2o_mmol_mean": _mean(row.get("physical_h2o_mmol_mean") for row in items),
                "sample_count_min": _minimum(row.get("physical_sample_count") for row in items),
                "usable_sample_count_min": _minimum(row.get("physical_usable_sample_count") for row in items),
                "classification": classification,
                "root_cause_candidate": reason,
            }
        )
    if key_name == "point_identity":
        return sorted(out, key=lambda row: _point_sort_key(row.get("point_identity")))
    if key_name == "temperature_group":
        return sorted(out, key=lambda row: _safe_float(str(row.get("temperature_group")).removeprefix("T")) or 999.0)
    return sorted(out, key=lambda row: _safe_float(str(row.get("target_group")).removesuffix("ppm")) or 999999.0)


def _s5_rows_for_device(rows: Sequence[Mapping[str, Any]], *, min_target_ppm: float) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        target = _safe_float(row.get("target_ppm"))
        measured = _safe_float(row.get("prediction_ppm"))
        if target is None or measured is None or target < min_target_ppm:
            continue
        item = dict(row)
        item["_target"] = target
        item["_measured"] = measured
        out.append(item)
    return out


def _s5_evaluation(
    rows: Sequence[Mapping[str, Any]],
    *,
    cfg: Co2TargetStateCommonBiasAuditConfig,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    by_device: Dict[str, List[Mapping[str, Any]]] = {}
    for row in rows:
        by_device.setdefault(str(row.get("device_id")), []).append(row)

    summary: List[Dict[str, Any]] = []
    residuals: List[Dict[str, Any]] = []
    for device in sorted(by_device):
        fit_rows = _s5_rows_for_device(by_device[device], min_target_ppm=cfg.min_relative_target_ppm)
        pre_rel = [
            abs(float(row["_measured"]) - float(row["_target"])) / float(row["_target"]) * 100.0
            for row in fit_rows
            if float(row["_target"]) != 0.0
        ]
        pre_abs = [abs(float(row["_measured"]) - float(row["_target"])) for row in fit_rows]
        blockers: List[str] = []
        if len(fit_rows) < 3:
            blockers.append("points<3")
            summary.append(
                {
                    "device_id": device,
                    "point_count": len(fit_rows),
                    "pre_s5_max_abs_relative_error_percent": max(pre_rel) if pre_rel else "",
                    "post_s5_max_abs_relative_error_percent": "",
                    "theoretical_s5_can_reach_1pct": False,
                    "blocked_reasons": ";".join(blockers),
                    "writes_coefficients": False,
                    "theoretical_only": True,
                }
            )
            continue

        continuous_c0, continuous_c1 = _fit_linear_trim(fit_rows)
        payload_c0, payload_c1, payload_max_pct, payload_max_ppm, payload_rmse_ppm = _fit_quantized_command_trim(
            fit_rows,
            c0_decimals=int(cfg.command_c0_decimals),
            c1_decimals=int(cfg.command_c1_decimals),
            c1_min=float(cfg.command_c1_min),
            c1_max=float(cfg.command_c1_max),
        )
        for row in sorted(fit_rows, key=lambda item: _point_sort_key(item.get("point_identity"))):
            target = float(row["_target"])
            measured = float(row["_measured"])
            corrected = measured * float(payload_c1) + float(payload_c0)
            error_ppm = corrected - target
            error_pct = error_ppm / target * 100.0 if target else 0.0
            residuals.append(
                {
                    "device_id": device,
                    "point_identity": row.get("point_identity", ""),
                    "target_ppm": target,
                    "pre_s5_prediction_ppm": measured,
                    "pre_s5_error_ppm": measured - target,
                    "pre_s5_relative_error_percent": (measured - target) / target * 100.0 if target else "",
                    "payload_C0": payload_c0,
                    "payload_C1": payload_c1,
                    "post_s5_prediction_ppm": corrected,
                    "post_s5_error_ppm": error_ppm,
                    "post_s5_relative_error_percent": error_pct,
                    "temperature_group": row.get("temperature_group", ""),
                    "target_group": row.get("target_group", ""),
                    "dewpoint_mean_c": row.get("physical_dewpoint_mean_c", ""),
                    "ratio_std": row.get("physical_co2_ratio_f_std", ""),
                }
            )

        can_reach = float(payload_max_pct) <= float(cfg.acceptance_pct)
        summary.append(
            {
                "device_id": device,
                "point_count": len(fit_rows),
                "pre_s5_max_abs_relative_error_percent": max(pre_rel) if pre_rel else "",
                "pre_s5_max_abs_error_ppm": max(pre_abs) if pre_abs else "",
                "continuous_C0": continuous_c0,
                "continuous_C1": continuous_c1,
                "payload_C0": payload_c0,
                "payload_C1": payload_c1,
                "post_s5_max_abs_relative_error_percent": payload_max_pct,
                "post_s5_max_abs_error_ppm": payload_max_ppm,
                "post_s5_rmse_ppm": payload_rmse_ppm,
                "theoretical_s5_can_reach_1pct": can_reach,
                "command_preview": f"SENCO5,YGAS,FFF,{payload_c0:.3f},{payload_c1:.3f}",
                "s5_physical_scope": "final displayed CO2 affine output layer only; not a replacement for S1/S3 raw optical model",
                "writes_coefficients": False,
                "theoretical_only": True,
            }
        )
    return summary, residuals


def _root_cause_recommendations(
    *,
    point_bias: Sequence[Mapping[str, Any]],
    temp_bias: Sequence[Mapping[str, Any]],
    target_bias: Sequence[Mapping[str, Any]],
    s5_summary: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    common_points = [
        row
        for row in point_bias
        if str(row.get("classification")) == "common_target_state_or_model_boundary"
    ]
    if common_points:
        rows.append(
            {
                "priority": "P1",
                "topic": "共同同号偏差",
                "finding": f"{len(common_points)} 个点位存在多设备共同同号偏差",
                "recommendation": "先审计证书值、点位映射、阀切换目标状态、露点/干燥窗口，再决定是否用 S5 输出层修正。",
            }
        )
    t20_t40 = [
        row
        for row in temp_bias
        if str(row.get("temperature_group")) in {"T20", "T40"}
        and abs(_safe_float(row.get("mean_error_ppm")) or 0.0) >= 2.0
    ]
    if t20_t40:
        rows.append(
            {
                "priority": "P1",
                "topic": "T20/T40 共同偏差",
                "finding": "T20/T40 温度组仍有可见平均偏差",
                "recommendation": "检查该温度组的目标状态一致性和温度项边界；不要把压力项重新引入当前大气压开放流通主拟合。",
            }
        )
    low_end = [
        row
        for row in target_bias
        if (_safe_float(str(row.get("target_group")).removesuffix("ppm")) or 999999.0) <= 300.0
        and abs(_safe_float(row.get("mean_error_ppm")) or 0.0) >= 2.0
    ]
    if low_end:
        rows.append(
            {
                "priority": "P1",
                "topic": "低端锚定",
                "finding": "100/200/300 ppm 或零气组仍有低端共偏",
                "recommendation": "把零气 CO2 估计作为 CO2 锚点单独审计，不与 H2O 干气锚点混用；必要时用可信低浓度标气加强低端。",
            }
        )
    can_reach = [row for row in s5_summary if _truthy(row.get("theoretical_s5_can_reach_1pct"))]
    rows.append(
        {
            "priority": "P2",
            "topic": "S5 理论输出层",
            "finding": f"{len(can_reach)}/{len(s5_summary)} 台设备理论 S5 后可达到 1% 目标",
            "recommendation": "S5 可以作为最终显示层同包评审；若 S1/S3 残差呈共同点位偏差，S5 只能压显示误差，不能替代目标状态审计。",
        }
    )
    return rows


def build_co2_s13_target_state_common_bias_audit(
    *,
    fit_points_csv: str | Path,
    selected_residuals_csv: str | Path,
    best_by_device_csv: str | Path,
    cfg: Co2TargetStateCommonBiasAuditConfig = Co2TargetStateCommonBiasAuditConfig(),
) -> Dict[str, List[Dict[str, Any]]]:
    fit_rows = _read_csv(fit_points_csv)
    residual_rows = _read_csv(selected_residuals_csv)
    best_rows = _read_csv(best_by_device_csv)
    physical = _physical_index(fit_rows)
    selected = _selected_residual_rows(residual_rows, physical)

    point_bias = _aggregate(selected, key_name="point_identity", cfg=cfg)
    temp_bias = _aggregate(selected, key_name="temperature_group", cfg=cfg)
    target_bias = _aggregate(selected, key_name="target_group", cfg=cfg)
    s5_summary, s5_residuals = _s5_evaluation(selected, cfg=cfg)
    recommendations = _root_cause_recommendations(
        point_bias=point_bias,
        temp_bias=temp_bias,
        target_bias=target_bias,
        s5_summary=s5_summary,
    )
    run_summary = [
        {
            "created_at": _now(),
            "fit_points_csv": str(Path(fit_points_csv).resolve()),
            "selected_residuals_csv": str(Path(selected_residuals_csv).resolve()),
            "best_by_device_csv": str(Path(best_by_device_csv).resolve()),
            "device_count": len({_device_id(row.get("device_id")) for row in selected}),
            "selected_residual_count": len(selected),
            "best_strategy_device_count": len(best_rows),
            "acceptance_pct": cfg.acceptance_pct,
            "common_sign_fraction_threshold": cfg.common_sign_fraction_threshold,
            "dry_dewpoint_gate_c": cfg.dry_dewpoint_gate_c,
            "ratio_std_a_gate": cfg.ratio_std_a_gate,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "uses_pressure_terms": False,
            "s5_evaluation_theoretical_only": True,
            "not_real_acceptance_evidence": True,
        }
    ]
    return {
        "run_summary": run_summary,
        "selected_residuals_with_physical_state": selected,
        "point_common_bias": point_bias,
        "temperature_group_bias": temp_bias,
        "target_group_bias": target_bias,
        "s5_theoretical_trim_by_device": s5_summary,
        "s5_theoretical_trim_residuals": s5_residuals,
        "root_cause_recommendations": recommendations,
    }


def write_co2_s13_target_state_common_bias_audit(
    *,
    fit_points_csv: str | Path,
    selected_residuals_csv: str | Path,
    best_by_device_csv: str | Path,
    output_dir: str | Path,
    cfg: Co2TargetStateCommonBiasAuditConfig = Co2TargetStateCommonBiasAuditConfig(),
) -> Dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_target_state_common_bias_audit(
        fit_points_csv=fit_points_csv,
        selected_residuals_csv=selected_residuals_csv,
        best_by_device_csv=best_by_device_csv,
        cfg=cfg,
    )
    paths = {
        "run_summary": output / "co2_s13_target_state_common_bias_run_summary.csv",
        "selected_residuals_with_physical_state": output
        / "co2_s13_selected_residuals_with_physical_state.csv",
        "point_common_bias": output / "co2_s13_point_common_bias.csv",
        "temperature_group_bias": output / "co2_s13_temperature_group_bias.csv",
        "target_group_bias": output / "co2_s13_target_group_bias.csv",
        "s5_theoretical_trim_by_device": output / "co2_s13_s5_theoretical_trim_by_device.csv",
        "s5_theoretical_trim_residuals": output / "co2_s13_s5_theoretical_trim_residuals.csv",
        "root_cause_recommendations": output / "co2_s13_root_cause_recommendations.csv",
        "metadata": output / "co2_s13_target_state_common_bias_meta.json",
        "markdown": output / "co2_s13_target_state_common_bias_audit_zh.md",
    }
    for key, path in paths.items():
        if key in {"metadata", "markdown"}:
            continue
        _write_csv(path, tables[key])
    paths["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_target_state_common_bias_audit",
                "created_at": _now(),
                "input_files": {
                    "fit_points_csv": str(Path(fit_points_csv).resolve()),
                    "selected_residuals_csv": str(Path(selected_residuals_csv).resolve()),
                    "best_by_device_csv": str(Path(best_by_device_csv).resolve()),
                },
                "boundary": tables["run_summary"][0],
                "config": cfg.__dict__,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_markdown(paths["markdown"], tables)
    return paths


def _fmt(value: Any, digits: int = 4) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}g}"


def _top_rows(rows: Sequence[Mapping[str, Any]], *, limit: int = 12) -> List[Mapping[str, Any]]:
    return sorted(
        rows,
        key=lambda row: abs(_safe_float(row.get("mean_error_ppm")) or 0.0),
        reverse=True,
    )[:limit]


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    lines = [
        "# V1.5 CO2 目标状态与低端共同偏差根因审计",
        "",
        "本报告只基于已采集证据离线计算：不打开串口、不控阀、不写 SENCO，也不是实时 acceptance 证据。",
        "目的不是替代 S1/S3 主链路拟合，而是判断 T20/T40、低端和共同点位偏差更像来自零气估计、证书/点位映射、管路目标状态，还是温度项模型边界。",
        "",
        "## 总体边界",
        "",
    ]
    summary = tables.get("run_summary", [{}])[0]
    lines.extend(
        [
            f"- 设备数：{summary.get('device_count', '')}",
            f"- 残差行数：{summary.get('selected_residual_count', '')}",
            f"- S5 评估：理论输出层修正，不写入设备。",
            f"- 压力项：当前大气压开放流通主拟合冻结压力项，不在本审计中重新启用。",
            "",
            "## 共同偏差最明显的点位",
            "",
            "| 点位 | 均值误差 ppm | 最大相对误差 % | 同号比例 | 露点最大值 | ratio std 最大值 | 根因候选 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in _top_rows(tables.get("point_common_bias", []), limit=12):
        lines.append(
            "| {point} | {mean} | {rel} | {same} | {dew} | {ratio} | {cause} |".format(
                point=row.get("point_identity", ""),
                mean=_fmt(row.get("mean_error_ppm")),
                rel=_fmt(row.get("max_abs_relative_error_percent")),
                same=_fmt(row.get("same_sign_fraction")),
                dew=_fmt(row.get("dewpoint_max_c")),
                ratio=_fmt(row.get("ratio_std_max")),
                cause=str(row.get("root_cause_candidate", "")),
            )
        )
    lines.extend(
        [
            "",
            "## 温度组偏差",
            "",
            "| 温度组 | 行数 | 均值误差 ppm | 最大相对误差 % | 同号比例 | 根因候选 |",
            "| --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in tables.get("temperature_group_bias", []):
        lines.append(
            "| {temp} | {count} | {mean} | {rel} | {same} | {cause} |".format(
                temp=row.get("temperature_group", ""),
                count=row.get("row_count", ""),
                mean=_fmt(row.get("mean_error_ppm")),
                rel=_fmt(row.get("max_abs_relative_error_percent")),
                same=_fmt(row.get("same_sign_fraction")),
                cause=str(row.get("root_cause_candidate", "")),
            )
        )
    lines.extend(
        [
            "",
            "## 目标气点偏差",
            "",
            "| 气点 | 行数 | 均值误差 ppm | 最大相对误差 % | 同号比例 | 根因候选 |",
            "| --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in tables.get("target_group_bias", []):
        lines.append(
            "| {target} | {count} | {mean} | {rel} | {same} | {cause} |".format(
                target=row.get("target_group", ""),
                count=row.get("row_count", ""),
                mean=_fmt(row.get("mean_error_ppm")),
                rel=_fmt(row.get("max_abs_relative_error_percent")),
                same=_fmt(row.get("same_sign_fraction")),
                cause=str(row.get("root_cause_candidate", "")),
            )
        )
    lines.extend(
        [
            "",
            "## S5 理论输出层修正能力",
            "",
            "| 设备ID | S5前最大相对误差 % | C0 | C1 | S5后理论最大相对误差 % | 是否可到 1% | 命令预览 |",
            "| --- | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in tables.get("s5_theoretical_trim_by_device", []):
        lines.append(
            "| {dev} | {pre} | {c0} | {c1} | {post} | {ok} | `{cmd}` |".format(
                dev=row.get("device_id", ""),
                pre=_fmt(row.get("pre_s5_max_abs_relative_error_percent")),
                c0=_fmt(row.get("payload_C0")),
                c1=_fmt(row.get("payload_C1")),
                post=_fmt(row.get("post_s5_max_abs_relative_error_percent")),
                ok="是" if _truthy(row.get("theoretical_s5_can_reach_1pct")) else "否",
                cmd=row.get("command_preview", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 审计结论",
            "",
        ]
    )
    for row in tables.get("root_cause_recommendations", []):
        lines.append(
            f"- {row.get('priority', '')} / {row.get('topic', '')}：{row.get('finding', '')}。{row.get('recommendation', '')}"
        )
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            "- 如果同一温度/同一气点多台设备同号偏差，优先怀疑共同目标状态：证书值、点位映射、阀路、露点/干燥状态或温度项模型边界，而不是某一台设备的随机误差。",
            "- 零气 CO2 锚点只约束 CO2 低端，不等同于 H2O 干气锚点；二者不能混成一个低端点。",
            "- S5 是最终显示浓度的线性输出层 `浓度*C1+C0`，可以压缩稳定残差，但不能替代 S1/S3 对原始 ratio/T 的主模型，也不能掩盖未稳定的管路状态。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
