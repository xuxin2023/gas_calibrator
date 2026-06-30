"""Offline CO2 SENCO1/SENCO3 low-end model correction review.

This module joins existing V1.5 offline audit artifacts into a single
decision report. It intentionally does not refit, open COM ports, control
routes, or write SENCO coefficients. The purpose is to decide whether the
remaining low-end residuals should be treated as S1/S3 model-shape issues,
zero-gas traceability issues, physical-state problems, or merely high
influence points that must not be silently deleted.
"""

from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


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
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _bool_text(value: Any) -> str:
    text = str(value).strip().lower()
    return "true" if text in {"1", "true", "yes"} else "false"


def _counts(values: Iterable[str]) -> str:
    counter = Counter(str(value or "").strip() for value in values if str(value or "").strip())
    return ";".join(f"{key}:{value}" for key, value in sorted(counter.items()))


def _avg(values: Iterable[Any]) -> str:
    numbers = [value for value in (_safe_float(item) for item in values) if value is not None]
    return "" if not numbers else f"{mean(numbers):.6g}"


def _max_abs(values: Iterable[Any]) -> str:
    numbers = [abs(value) for value in (_safe_float(item) for item in values) if value is not None]
    return "" if not numbers else f"{max(numbers):.6g}"


def _fmt(value: Any, digits: int = 5) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}g}"


def _device_model_decision(row: Mapping[str, Any]) -> str:
    max_rel = _safe_float(row.get("best_max_abs_relative_error_percent"))
    low_rel = _safe_float(row.get("best_low_end_max_abs_relative_error_percent"))
    zero = _safe_float(row.get("best_zero_offset_ppm"))
    if max_rel is None:
        return "缺少模型评审结果"
    if max_rel <= 1.5 and (low_rel is None or low_rel <= 1.5):
        return "S1/S3 主模型候选可进入写入前评审"
    if zero is not None and abs(zero) > 1.0e-9:
        return "S1/S3 仍需先评审零气指定值与截距"
    return "S1/S3 主模型低端残差仍需修正评审"


def _common_mode_decision(row: Mapping[str, Any]) -> str:
    same = _safe_float(row.get("same_sign_residual_fraction"))
    mean_error = _safe_float(row.get("mean_error_ppm"))
    ratio_grade = str(row.get("ratio_grade_counts") or "")
    dry_grade = str(row.get("dryness_grade_counts") or "")
    status = str(row.get("common_mode_status") or "")
    if same is not None and same >= 0.8 and mean_error is not None and status == "common_mode_suspect":
        if "A:" in ratio_grade and "deep_dry:" in dry_grade:
            return "好物理状态下仍有共同偏差，优先查目标值/零气/S1S3形状"
        return "存在共同偏差，但需先复核物理 QC 状态"
    if same is not None and same >= 0.8 and mean_error is not None:
        return "同向但未达到共同偏差门槛，保留观察"
    return "不是强共同偏差主因"


def _exclusion_decision(row: Mapping[str, Any]) -> str:
    improved = _safe_float(row.get("improved_device_count"))
    worsened = _safe_float(row.get("worsened_device_count"))
    delta = _safe_float(row.get("mean_max_relative_error_improvement_percent_points"))
    if improved is None or worsened is None or delta is None:
        return "缺少删点敏感性证据"
    if improved >= 6 and worsened <= 0 and delta > 0.2:
        return "高影响点，但必须有明确物理原因才可剔除"
    if improved > worsened and delta > 0.5:
        return "高影响但不一致，不允许自动剔除"
    if worsened > improved:
        return "删除会恶化拟合，应保留"
    return "影响较小，保留"


def _build_device_rows(selected_rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in selected_rows:
        out.append(
            {
                "device_id": _device_id(row.get("device_id")),
                "best_structure_id": row.get("best_structure_id", ""),
                "best_objective_id": row.get("best_objective_id", ""),
                "best_zero_offset_ppm": row.get("best_zero_offset_ppm", ""),
                "best_max_abs_relative_error_percent": row.get("best_max_abs_relative_error_percent", ""),
                "best_low_end_max_abs_relative_error_percent": row.get(
                    "best_low_end_max_abs_relative_error_percent", ""
                ),
                "uses_pressure_terms": _bool_text(row.get("uses_pressure_terms")),
                "uses_s5_output_trim": _bool_text(row.get("uses_s5_output_trim")),
                "auto_write_allowed": _bool_text(row.get("auto_write_allowed")),
                "main_model_decision": _device_model_decision(row),
                "physical_note": (
                    "仅评审 S1/S3 主链路；压力项和 S5 输出层修正均排除在本轮主模型判断之外。"
                ),
            }
        )
    return out


def _build_zero_rows(zero_rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in zero_rows:
        out.append(
            {
                "zero_offset_ppm": row.get("zero_offset_ppm", ""),
                "mean_max_abs_relative_error_percent": row.get("mean_max_abs_relative_error_percent", ""),
                "best_max_abs_relative_error_percent": row.get("best_max_abs_relative_error_percent", ""),
                "worst_max_abs_relative_error_percent": row.get("worst_max_abs_relative_error_percent", ""),
                "mean_low_end_max_abs_relative_error_percent": row.get(
                    "mean_low_end_max_abs_relative_error_percent", ""
                ),
                "zero_anchor_decision": (
                    "零气偏置只是敏感性假设，不等于证书值；零气仍作为 CO2 低端锚点并带不确定度评审。"
                ),
            }
        )
    return out


def _build_common_rows(
    common_rows: Sequence[Mapping[str, Any]],
    exclusion_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    by_point = {str(row.get("excluded_point_identity") or ""): row for row in exclusion_rows}
    out: List[Dict[str, Any]] = []
    for row in common_rows:
        point = str(row.get("point_identity") or "")
        exclusion = by_point.get(point, {})
        out.append(
            {
                "point_identity": point,
                "target_ppm": row.get("target_ppm", ""),
                "temperature_c": row.get("temperature_c", ""),
                "device_count": row.get("device_count", ""),
                "mean_error_ppm": row.get("mean_error_ppm", ""),
                "same_sign_residual_fraction": row.get("same_sign_residual_fraction", ""),
                "ratio_grade_counts": row.get("ratio_grade_counts", ""),
                "dryness_grade_counts": row.get("dryness_grade_counts", ""),
                "common_mode_status": row.get("common_mode_status", ""),
                "common_mode_decision": _common_mode_decision(row),
                "exclusion_sensitivity_interpretation": exclusion.get("exclusion_interpretation", ""),
                "exclusion_decision": _exclusion_decision(exclusion),
                "auto_exclude_allowed": False,
            }
        )
    return out


def _build_temperature_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("temperature_group") or "")].append(row)
    out: List[Dict[str, Any]] = []
    for temp_group, items in sorted(grouped.items()):
        out.append(
            {
                "temperature_group": temp_group,
                "device_group_count": len(items),
                "mean_of_device_mean_error_ppm": _avg(row.get("mean_error_ppm") for row in items),
                "max_abs_device_mean_error_ppm": _max_abs(row.get("mean_error_ppm") for row in items),
                "max_abs_error_ppm": _max_abs(row.get("max_abs_error_ppm") for row in items),
                "same_sign_residual_fraction_mean": _avg(
                    row.get("same_sign_residual_fraction") for row in items
                ),
                "temperature_review_counts": _counts(row.get("temperature_model_review", "") for row in items),
                "temperature_model_decision": (
                    "需要结合低端锚点密度和温度形状继续评审"
                    if temp_group in {"T20", "T30", "T40"}
                    else "作为温度上下文保留"
                ),
            }
        )
    return out


def _build_action_rows(
    device_rows: Sequence[Mapping[str, Any]],
    common_rows: Sequence[Mapping[str, Any]],
    zero_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    nonzero_zero_count = sum(
        1 for row in device_rows if (_safe_float(row.get("best_zero_offset_ppm")) or 0.0) != 0.0
    )
    common_good = [
        row
        for row in common_rows
        if row.get("common_mode_decision") == "好物理状态下仍有共同偏差，优先查目标值/零气/S1S3形状"
    ]
    zero_sensitivity_count = len(zero_rows)
    return [
        {
            "priority": "P0",
            "topic": "S1/S3 主链路边界",
            "finding": f"{len(common_good)} 个低端点在 A 级 ratio/深干露点证据下仍有多设备同向偏差。",
            "action": "先评审 S1/S3 低端温度形状、目标状态和零气锚点；不使用 S5 掩盖主模型残差。",
            "writes_coefficients": False,
        },
        {
            "priority": "P0",
            "topic": "零气锚点",
            "finding": f"{nonzero_zero_count} 台设备的最优离线候选偏向非零 zero_offset；已比较 {zero_sensitivity_count} 组零气假设。",
            "action": "这只是敏感性，不等于零气证书值；正式拟合需给零气 CO2 指定值和不确定度。",
            "writes_coefficients": False,
        },
        {
            "priority": "P1",
            "topic": "删点策略",
            "finding": "删点敏感性只说明高影响点存在，不能自动剔除。",
            "action": "只有证据证明阀路、气瓶、露点、ratio 或状态寄存器异常时才降级/剔除。",
            "writes_coefficients": False,
        },
        {
            "priority": "P1",
            "topic": "S5 输出层",
            "finding": "S5 未参与本轮评审。",
            "action": "S1/S3 主模型评审通过后，S5 才能作为最终显示层线性微调单独评审。",
            "writes_coefficients": False,
        },
    ]


def build_co2_s13_low_end_model_correction_review(
    *,
    model_structure_dir: str | Path,
    anchor_target_audit_dir: str | Path,
    residual_root_cause_dir: str | Path | None = None,
) -> Dict[str, List[Dict[str, Any]]]:
    model_dir = Path(model_structure_dir).resolve()
    anchor_dir = Path(anchor_target_audit_dir).resolve()
    residual_dir = Path(residual_root_cause_dir).resolve() if residual_root_cause_dir else None

    selected = _read_csv(model_dir / "co2_s13_selected_structure_candidates.csv")
    temperature = _read_csv(model_dir / "co2_s13_temperature_bias_diagnostic.csv")
    common = _read_csv(anchor_dir / "co2_s13_low_end_common_mode_audit.csv")
    exclusion = _read_csv(anchor_dir / "co2_s13_point_exclusion_sensitivity.csv")
    zero = _read_csv(anchor_dir / "co2_s13_zero_offset_selection_summary.csv")
    residual_low_end = (
        _read_csv(residual_dir / "co2_s13_low_end_pattern_summary.csv") if residual_dir else []
    )

    device_rows = _build_device_rows(selected)
    common_rows = _build_common_rows(common, exclusion)
    zero_rows = _build_zero_rows(zero)
    temperature_rows = _build_temperature_rows(temperature)
    action_rows = _build_action_rows(device_rows, common_rows, zero_rows)
    run_summary = [
        {
            "created_at": _now(),
            "model_structure_dir": str(model_dir),
            "anchor_target_audit_dir": str(anchor_dir),
            "residual_root_cause_dir": str(residual_dir or ""),
            "device_count": len(device_rows),
            "common_mode_point_count": len(common_rows),
            "residual_low_end_pattern_count": len(residual_low_end),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "uses_pressure_terms": False,
            "uses_s5_output_trim": False,
            "not_real_acceptance_evidence": True,
        }
    ]
    return {
        "run_summary": run_summary,
        "device_model_decision": device_rows,
        "zero_anchor_decision": zero_rows,
        "low_end_common_mode_decision": common_rows,
        "temperature_group_decision": temperature_rows,
        "recommended_actions": action_rows,
    }


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    lines = [
        "# V1.5 CO2 S1/S3 低端模型修正评审",
        "",
        "边界：本报告只合并既有离线审计表；不打开 COM、不控制气路/水路、不写 SENCO。S5 输出层修正不参与 S1/S3 主链路判断。",
        "",
        "## 1. 逐台 S1/S3 候选状态",
        "",
        "| 设备 ID | 结构 | 目标函数 | 零气假设 ppm | 最大相对误差 % | 低端最大相对误差 % | 判断 |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in tables.get("device_model_decision", []):
        lines.append(
            "| {device} | {structure} | {objective} | {zero} | {max_rel} | {low_rel} | {decision} |".format(
                device=row.get("device_id", ""),
                structure=row.get("best_structure_id", ""),
                objective=row.get("best_objective_id", ""),
                zero=_fmt(row.get("best_zero_offset_ppm")),
                max_rel=_fmt(row.get("best_max_abs_relative_error_percent")),
                low_rel=_fmt(row.get("best_low_end_max_abs_relative_error_percent")),
                decision=row.get("main_model_decision", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 2. 低端共同偏差与删点敏感性",
            "",
            "| 点位 | 目标 ppm | 温度 | 均值误差 ppm | 同向比例 | ratio/露点 | 判断 | 删点结论 |",
            "| --- | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in tables.get("low_end_common_mode_decision", []):
        lines.append(
            "| {point} | {target} | {temp} | {err} | {same} | {qc} | {decision} | {exclude} |".format(
                point=row.get("point_identity", ""),
                target=_fmt(row.get("target_ppm")),
                temp=_fmt(row.get("temperature_c")),
                err=_fmt(row.get("mean_error_ppm")),
                same=_fmt(row.get("same_sign_residual_fraction")),
                qc=f"{row.get('ratio_grade_counts', '')}; {row.get('dryness_grade_counts', '')}",
                decision=row.get("common_mode_decision", ""),
                exclude=row.get("exclusion_decision", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 3. 零气锚点敏感性",
            "",
            "| 零气假设 ppm | 平均最大相对误差 % | 最优 % | 最差 % | 判断 |",
            "| ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in tables.get("zero_anchor_decision", []):
        lines.append(
            "| {zero} | {mean_rel} | {best} | {worst} | {decision} |".format(
                zero=_fmt(row.get("zero_offset_ppm")),
                mean_rel=_fmt(row.get("mean_max_abs_relative_error_percent")),
                best=_fmt(row.get("best_max_abs_relative_error_percent")),
                worst=_fmt(row.get("worst_max_abs_relative_error_percent")),
                decision=row.get("zero_anchor_decision", ""),
            )
        )

    lines.extend(
        [
            "",
            "## 4. 温度组残差线索",
            "",
            "| 温度组 | 设备组数 | 设备均值误差均值 ppm | 设备均值误差最大绝对值 ppm | 同向比例均值 | 判断 |",
            "| --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in tables.get("temperature_group_decision", []):
        lines.append(
            "| {temp} | {count} | {mean_err} | {max_mean_err} | {same} | {decision} |".format(
                temp=row.get("temperature_group", ""),
                count=row.get("device_group_count", ""),
                mean_err=_fmt(row.get("mean_of_device_mean_error_ppm")),
                max_mean_err=_fmt(row.get("max_abs_device_mean_error_ppm")),
                same=_fmt(row.get("same_sign_residual_fraction_mean")),
                decision=row.get("temperature_model_decision", ""),
            )
        )

    lines.extend(["", "## 5. 建议动作", ""])
    for row in tables.get("recommended_actions", []):
        lines.append(
            "- {priority} {topic}: {finding} {action}".format(
                priority=row.get("priority", ""),
                topic=row.get("topic", ""),
                finding=row.get("finding", ""),
                action=row.get("action", ""),
            )
        )
    lines.extend(
        [
            "",
            "物理解释：如果低端点 ratio A 级、露点深干且多台同向偏差，优先解释为共同目标状态、零气锚点或 S1/S3 低端温度形状问题；不能因为删点改善就静默删点。S5 是最终显示层修正，不能先用来掩盖主模型残差。",
        ]
    )
    path.write_text("\ufeff" + "\n".join(lines) + "\n", encoding="utf-8")


def write_co2_s13_low_end_model_correction_review(
    *,
    model_structure_dir: str | Path,
    anchor_target_audit_dir: str | Path,
    residual_root_cause_dir: str | Path | None,
    output_dir: str | Path,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_low_end_model_correction_review(
        model_structure_dir=model_structure_dir,
        anchor_target_audit_dir=anchor_target_audit_dir,
        residual_root_cause_dir=residual_root_cause_dir,
    )
    outputs = {
        "run_summary": output / "co2_s13_low_end_model_correction_run_summary.csv",
        "device_model_decision": output / "co2_s13_device_model_decision.csv",
        "zero_anchor_decision": output / "co2_s13_zero_anchor_decision.csv",
        "low_end_common_mode_decision": output / "co2_s13_low_end_common_mode_decision.csv",
        "temperature_group_decision": output / "co2_s13_temperature_group_decision.csv",
        "recommended_actions": output / "co2_s13_low_end_model_recommended_actions.csv",
        "metadata": output / "co2_s13_low_end_model_correction_meta.json",
        "markdown": output / "co2_s13_low_end_model_correction_review_zh.md",
    }
    for key in (
        "run_summary",
        "device_model_decision",
        "zero_anchor_decision",
        "low_end_common_mode_decision",
        "temperature_group_decision",
        "recommended_actions",
    ):
        _write_csv(outputs[key], tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_low_end_model_correction_review",
                "created_at": _now(),
                "inputs": {
                    "model_structure_dir": str(Path(model_structure_dir).resolve()),
                    "anchor_target_audit_dir": str(Path(anchor_target_audit_dir).resolve()),
                    "residual_root_cause_dir": (
                        str(Path(residual_root_cause_dir).resolve())
                        if residual_root_cause_dir
                        else ""
                    ),
                },
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "uses_s5_output_trim": False,
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
