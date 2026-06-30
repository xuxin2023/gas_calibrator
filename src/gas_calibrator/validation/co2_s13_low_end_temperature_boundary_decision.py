"""Decision layer for CO2 S1/S3 low-end and temperature-boundary review.

This module consumes existing offline V1.5 review artifacts. It does not refit,
open COM ports, control routes, or write coefficients. Its purpose is to turn
several audit CSVs into a per-device next-action table so the operator does not
mistake zero-gas assigned-value review for valve/certificate mapping failure, or
use S5 output trim before the S1/S3 main-model residual source is understood.
"""

from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence


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


def _safe_int(value: Any) -> int:
    number = _safe_float(value)
    return int(number) if number is not None else 0


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _by_device(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    out: Dict[str, Mapping[str, Any]] = {}
    for row in rows:
        device = _device_id(row.get("device_id"))
        if device:
            out[device] = row
    return out


def _segment_lookup(rows: Sequence[Mapping[str, Any]]) -> Dict[tuple[str, str], Mapping[str, Any]]:
    out: Dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in rows:
        device = _device_id(row.get("device_id"))
        segment = str(row.get("segment_id") or "")
        if device and segment:
            out[(device, segment)] = row
    return out


def _segment_metric(
    segments: Mapping[tuple[str, str], Mapping[str, Any]],
    device: str,
    prefix: str,
    key: str,
) -> Any:
    for (item_device, segment), row in segments.items():
        if item_device == device and segment.startswith(prefix):
            return row.get(key, "")
    return ""


def _best_float(*values: Any) -> Optional[float]:
    for value in values:
        number = _safe_float(value)
        if number is not None:
            return number
    return None


def _decision_for_device(
    *,
    device: str,
    mapping: Mapping[str, Any],
    capacity: Mapping[str, Any],
    segments: Mapping[tuple[str, str], Mapping[str, Any]],
) -> Dict[str, Any]:
    ratio_violations = _safe_int(mapping.get("ratio_monotonic_violation_count"))
    mapping_suspects = _safe_int(mapping.get("mapping_suspect_count"))
    zero_reviews = _safe_int(mapping.get("zero_anchor_assigned_value_review_count"))
    low_common = _safe_int(mapping.get("low_end_common_bias_group_count"))
    low_temp_bias = _safe_int(capacity.get("low_end_temperature_bias_group_count"))
    low_target_bias = _safe_int(capacity.get("low_end_target_bias_group_count"))
    improvement_fraction = _safe_float(capacity.get("best_vs_baseline_improvement_fraction"))
    best_max = _safe_float(capacity.get("best_max_abs_relative_error_percent"))
    best_low = _safe_float(capacity.get("best_low_end_max_abs_relative_error_percent"))
    baseline_max = _safe_float(capacity.get("baseline_max_abs_relative_error_percent"))
    low_segment = _safe_float(_segment_metric(segments, device, "low_nonzero", "max_abs_relative_error_percent"))
    high_segment = _safe_float(_segment_metric(segments, device, "high_", "max_abs_relative_error_percent"))
    zero_abs = _safe_float(_segment_metric(segments, device, "zero_anchor", "max_abs_error_ppm"))

    diagnosis = "s1s3_low_end_temperature_boundary_review"
    recommendation = "do_not_write_s5_first_review_s1s3_low_end_temperature_boundary"
    physical_reason = (
        "ratio-目标排序和真实点位映射是自洽的，但 S1/S3 仍留下低端残差；"
        "不能先用 S5 掩盖主模型边界问题。"
    )
    if ratio_violations or mapping_suspects:
        diagnosis = "route_or_point_mapping_blocker"
        recommendation = "block_coefficient_write_review_route_certificate_mapping"
        physical_reason = "ratio 排序或真实点位映射问题会先破坏拟合输入，必须在模型评审前阻断写入。"
    elif best_max is not None and best_max <= 1.5:
        diagnosis = "s1s3_candidate_near_acceptance_review"
        recommendation = "review_s1s3_candidate_before_optional_s5_trim"
        physical_reason = "S1/S3 候选已经接近工程目标；S5 只能作为最后的输出层微调。"
    elif zero_reviews and low_common:
        diagnosis = "zero_anchor_and_low_end_temperature_boundary"
        recommendation = "review_zero_gas_assigned_value_and_s1s3_low_end_temperature_terms"
        physical_reason = (
            "零气 CO2 赋值约束截距，同时低端温度组仍有共同残差；"
            "下一步应重点评审低端锚点和温度形状项。"
        )
    elif low_temp_bias or low_target_bias:
        diagnosis = "low_end_temperature_or_target_boundary"
        recommendation = "review_low_end_temperature_or_target_bias_before_s5"
        physical_reason = "低端残差按温度或目标点聚集，输出层 S5 不是第一物理修复手段。"
    elif improvement_fraction is not None and improvement_fraction >= 0.15:
        diagnosis = "s1s3_objective_has_meaningful_gain_but_still_not_enough"
        recommendation = "continue_s1s3_objective_anchor_review_no_write"
        physical_reason = "调整 S1/S3 目标函数或零气假设有帮助，但当前残差仍然不适合写入。"

    return {
        "device_id": device,
        "ratio_monotonic_violation_count": ratio_violations,
        "mapping_suspect_count": mapping_suspects,
        "zero_anchor_assigned_value_review_count": zero_reviews,
        "low_end_common_bias_group_count": low_common,
        "low_end_temperature_bias_group_count": low_temp_bias,
        "low_end_target_bias_group_count": low_target_bias,
        "baseline_max_abs_relative_error_percent": baseline_max if baseline_max is not None else "",
        "best_s1s3_max_abs_relative_error_percent": best_max if best_max is not None else "",
        "best_s1s3_low_end_max_abs_relative_error_percent": best_low if best_low is not None else "",
        "best_vs_baseline_improvement_fraction": improvement_fraction if improvement_fraction is not None else "",
        "zero_anchor_max_abs_error_ppm": zero_abs if zero_abs is not None else "",
        "low_nonzero_max_abs_relative_error_percent": low_segment if low_segment is not None else "",
        "high_max_abs_relative_error_percent": high_segment if high_segment is not None else "",
        "boundary_diagnosis": diagnosis,
        "recommended_next_action": recommendation,
        "physical_reason": physical_reason,
        "uses_pressure_terms": False,
        "uses_s5_output_trim": False,
        "writes_coefficients": False,
    }


def build_co2_s13_low_end_temperature_boundary_decision(
    *,
    ratio_mapping_device_summary_csv: str | Path,
    model_capacity_boundary_csv: str | Path,
    segment_diagnostic_csv: str | Path,
) -> List[Dict[str, Any]]:
    mapping_by_device = _by_device(_read_csv(ratio_mapping_device_summary_csv))
    capacity_by_device = _by_device(_read_csv(model_capacity_boundary_csv))
    segments = _segment_lookup(_read_csv(segment_diagnostic_csv))
    devices = sorted(set(mapping_by_device) | set(capacity_by_device) | {device for device, _ in segments})
    return [
        _decision_for_device(
            device=device,
            mapping=mapping_by_device.get(device, {}),
            capacity=capacity_by_device.get(device, {}),
            segments=segments,
        )
        for device in devices
    ]


def _fmt(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.3f}"


def _render_markdown(rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# V1.5 CO2 S1/S3 低端温度边界决策审计",
        "",
        f"生成时间：{_now()}",
        "",
        "## 边界",
        "",
        "- 本报告汇总已有离线审计，不重新拟合。",
        "- 不打开 COM、不控制气路/水路、不写 SENCO。",
        "- 压力项保持冻结；S5 输出层修正不用于掩盖 S1/S3 主模型残差。",
        "",
        "## 逐台判断",
        "",
    ]
    for row in rows:
        lines.append(
            "- 设备 {device}: 诊断 {diagnosis}；S1/S3 最优最大相对误差 {best}%；低端最大相对误差 {low}%；"
            "高端最大相对误差 {high}%；建议 {action}。".format(
                device=row.get("device_id"),
                diagnosis=row.get("boundary_diagnosis"),
                best=_fmt(row.get("best_s1s3_max_abs_relative_error_percent")),
                low=_fmt(row.get("low_nonzero_max_abs_relative_error_percent")),
                high=_fmt(row.get("high_max_abs_relative_error_percent")),
                action=row.get("recommended_next_action"),
            )
        )
        lines.append(f"  物理原因：{row.get('physical_reason')}")
    lines.extend(
        [
            "",
            "## 结论",
            "",
            "- 只要 ratio 顺序和真实点位映射是可信的，残差就不应优先归咎于阀路错接。",
            "- 低端多点同向偏差代表截距、零气赋值或温度项边界问题，需要先在 S1/S3 主模型中解释。",
            "- S5/S6 是最终显示层线性修正，只能在主链路残差已被解释后作为微调。",
            "",
        ]
    )
    return "\n".join(lines)


def write_co2_s13_low_end_temperature_boundary_decision(
    *,
    ratio_mapping_device_summary_csv: str | Path,
    model_capacity_boundary_csv: str | Path,
    segment_diagnostic_csv: str | Path,
    output_dir: str | Path,
) -> Dict[str, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = build_co2_s13_low_end_temperature_boundary_decision(
        ratio_mapping_device_summary_csv=ratio_mapping_device_summary_csv,
        model_capacity_boundary_csv=model_capacity_boundary_csv,
        segment_diagnostic_csv=segment_diagnostic_csv,
    )
    outputs = {
        "decision": out_dir / "co2_s13_low_end_temperature_boundary_decision.csv",
        "metadata": out_dir / "co2_s13_low_end_temperature_boundary_decision_meta.json",
        "markdown": out_dir / "co2_s13_low_end_temperature_boundary_decision_zh.md",
    }
    _write_csv(outputs["decision"], rows)
    outputs["metadata"].write_text(
        json.dumps(
            {
                "generated_at": _now(),
                "inputs": {
                    "ratio_mapping_device_summary_csv": str(ratio_mapping_device_summary_csv),
                    "model_capacity_boundary_csv": str(model_capacity_boundary_csv),
                    "segment_diagnostic_csv": str(segment_diagnostic_csv),
                },
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "uses_s5_output_trim": False,
                    "not_real_acceptance_evidence": True,
                },
                "outputs": {key: str(path) for key, path in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs["markdown"].write_text("\ufeff" + _render_markdown(rows), encoding="utf-8")
    return outputs
