"""Offline CO2 S1/S3 minimal bridge or resampling plan.

This module does not open COM ports, control routes, or write coefficients. It
turns an existing CO2 residual table into a short, evidence-ranked action list:
which points should be bridged from existing evidence and which points need a
minimal repeat sample before a controlled write review can be trusted.
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence


DEFAULT_ACCEPTANCE_PERCENT = 1.0
DEFAULT_MIN_RELATIVE_TARGET_PPM = 50.0
DEFAULT_COMMON_DEVICE_COUNT = 3
POINT_PATTERN = re.compile(r"^T(?P<temp>-?\d+(?:\.\d+)?)_(?P<ppm>\d+(?:\.\d+)?)ppm$")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


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


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
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


def _parse_point_identity(point_identity: str) -> Dict[str, Any]:
    match = POINT_PATTERN.match(str(point_identity or ""))
    if not match:
        return {"temperature_c": "", "gas_ppm": ""}
    return {
        "temperature_c": float(match.group("temp")),
        "gas_ppm": float(match.group("ppm")),
    }


def _signed_coherence(signs: Sequence[int]) -> str:
    non_zero = [sign for sign in signs if sign != 0]
    if not non_zero:
        return "no_signed_error"
    positive = sum(1 for sign in non_zero if sign > 0)
    negative = sum(1 for sign in non_zero if sign < 0)
    ratio = max(positive, negative) / len(non_zero)
    if ratio >= 0.8:
        return "same_direction_bias_positive" if positive > negative else "same_direction_bias_negative"
    return "mixed_direction_bias"


def _recommend_point_action(
    *,
    devices_over_acceptance: int,
    common_device_count: int,
    max_abs_relative_error_percent: float,
    signed_coherence: str,
    gas_ppm: Any,
    acceptance_percent: float,
) -> str:
    if devices_over_acceptance >= common_device_count:
        if signed_coherence.startswith("same_direction"):
            return "bridge_or_resample_common_source_state"
        return "resample_common_point_before_write"
    if max_abs_relative_error_percent > acceptance_percent:
        if _safe_float(gas_ppm) is not None and float(gas_ppm) <= 200:
            return "review_low_end_anchor_for_device_specific_bias"
        return "device_specific_residual_review"
    return "monitor_only"


def _recommend_zh(action: str) -> str:
    return {
        "bridge_or_resample_common_source_state": "跨设备同点位同向超差，优先做源状态桥接；若桥接后仍超差，只补采该点。",
        "resample_common_point_before_write": "跨设备同点位超差但方向不一致，先最小补采该点，不建议直接写入。",
        "review_low_end_anchor_for_device_specific_bias": "低端点单设备超差，先复核零气/低端锚点和该设备残差，不扩大重跑。",
        "device_specific_residual_review": "单设备残差问题，先设备侧评审，不影响其它设备候选。",
        "monitor_only": "未超过门限，保留证据即可。",
    }.get(action, action)


def build_co2_s13_minimal_bridge_plan(
    *,
    closure_summary_csv: str | Path,
    corrected_residuals_csv: str | Path,
    acceptance_percent: float = DEFAULT_ACCEPTANCE_PERCENT,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    common_device_count: int = DEFAULT_COMMON_DEVICE_COUNT,
) -> Dict[str, Any]:
    summary_rows = _read_csv(closure_summary_csv)
    residual_rows = _read_csv(corrected_residuals_csv)

    device_plan: List[Dict[str, Any]] = []
    for row in summary_rows:
        max_after_s5 = _safe_float(row.get("s5_max_abs_relative_error_percent"))
        action = "blocked_minimal_bridge_required"
        if max_after_s5 is not None and max_after_s5 <= float(acceptance_percent):
            action = "candidate_for_controlled_write_review"
        device_plan.append(
            {
                "device_id": _device_id(row.get("device_id")),
                "s1s3_max_abs_relative_error_percent": row.get("s1s3_max_abs_relative_error_percent", ""),
                "s5_max_abs_relative_error_percent": row.get("s5_max_abs_relative_error_percent", ""),
                "s5_command_preview": row.get("s5_command_preview", ""),
                "worst_relative_point_identity": row.get("s5_worst_relative_point_identity", ""),
                "recommended_action": action,
                "writes_coefficients": False,
                "opens_com_ports": False,
            }
        )

    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in residual_rows:
        target = _safe_float(row.get("target_ppm"))
        relative = _safe_float(row.get("s5_relative_error_percent"))
        if target is None or relative is None:
            continue
        if abs(target) < float(min_relative_target_ppm):
            continue
        grouped[str(row.get("point_identity") or "")].append(row)

    point_plan: List[Dict[str, Any]] = []
    for point_identity, rows in grouped.items():
        rel_values = [_safe_float(row.get("s5_relative_error_percent")) for row in rows]
        rel_values = [value for value in rel_values if value is not None]
        if not rel_values:
            continue
        abs_values = [abs(value) for value in rel_values]
        over_rows = [
            row
            for row in rows
            if (_safe_float(row.get("s5_relative_error_percent")) is not None)
            and abs(float(_safe_float(row.get("s5_relative_error_percent")))) > float(acceptance_percent)
        ]
        signs = [1 if value > 0 else -1 if value < 0 else 0 for value in rel_values]
        meta = _parse_point_identity(point_identity)
        signed_coherence = _signed_coherence(signs)
        max_abs = max(abs_values)
        action = _recommend_point_action(
            devices_over_acceptance=len(over_rows),
            common_device_count=int(common_device_count),
            max_abs_relative_error_percent=max_abs,
            signed_coherence=signed_coherence,
            gas_ppm=meta.get("gas_ppm"),
            acceptance_percent=float(acceptance_percent),
        )
        point_plan.append(
            {
                "priority_score": round(max_abs * max(1, len(over_rows)), 6),
                "point_identity": point_identity,
                "temperature_c": meta.get("temperature_c", ""),
                "gas_ppm": meta.get("gas_ppm", ""),
                "affected_device_count": len(rows),
                "devices_over_acceptance": len(over_rows),
                "devices_over_acceptance_list": ";".join(_device_id(row.get("device_id")) for row in over_rows),
                "max_abs_relative_error_percent_after_s5": max_abs,
                "mean_abs_relative_error_percent_after_s5": sum(abs_values) / len(abs_values),
                "signed_mean_relative_error_percent_after_s5": sum(rel_values) / len(rel_values),
                "signed_coherence": signed_coherence,
                "ratio_grades": ";".join(sorted({str(row.get("ratio_grade") or "") for row in rows if row.get("ratio_grade")})),
                "dryness_grades": ";".join(sorted({str(row.get("dryness_grade") or "") for row in rows if row.get("dryness_grade")})),
                "physical_qc_labels": ";".join(sorted({str(row.get("physical_qc_label") or "") for row in rows if row.get("physical_qc_label")})),
                "recommended_action": action,
                "recommended_action_zh": _recommend_zh(action),
                "minimum_next_step": (
                    "先用现有同点位温度/露点/压力/H2O/ratio 证据做桥接闭合；桥接失败才补采该点。"
                    if action == "bridge_or_resample_common_source_state"
                    else "只补采或复核该点，不重跑全温度全气点。"
                    if action != "monitor_only"
                    else "无需补采。"
                ),
                "writes_coefficients": False,
                "opens_com_ports": False,
            }
        )

    point_plan.sort(key=lambda row: (float(row.get("priority_score") or 0.0), float(row.get("max_abs_relative_error_percent_after_s5") or 0.0)), reverse=True)

    return {
        "run_summary": [
            {
                "created_at": _now(),
                "closure_summary_csv": str(Path(closure_summary_csv).resolve()),
                "corrected_residuals_csv": str(Path(corrected_residuals_csv).resolve()),
                "acceptance_percent": float(acceptance_percent),
                "min_relative_target_ppm": float(min_relative_target_ppm),
                "common_device_count": int(common_device_count),
                "device_count": len(device_plan),
                "point_count": len(point_plan),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "not_real_acceptance_evidence": True,
            }
        ],
        "device_plan": device_plan,
        "point_plan": point_plan,
    }


def _fmt(value: Any, digits: int = 3) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def render_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    device_plan = list(tables.get("device_plan") or [])
    point_plan = list(tables.get("point_plan") or [])
    lines = [
        "# CO2 最小补采 / 状态桥接闭合计划",
        "",
        "本报告只使用已有 V1.5 开放流通采样和离线残差证据；不打开 COM、不控制气路/水路、不写 SENCO。",
        "",
        "## 结论",
        "",
        "当前 `S1/S3 + S5` 仍不能把所有设备压到目标误差内，因此下一步不应盲目写入，而应只处理贡献最大的共同点位。优先级按“超差幅度 × 超差设备数”排序。",
        "",
        "## 逐台闭合状态",
        "",
        "| 设备ID | S1/S3最大相对误差% | S5后最大相对误差% | 最差点 | S5预览 | 结论 |",
        "| --- | ---: | ---: | --- | --- | --- |",
    ]
    for row in device_plan:
        lines.append(
            "| {device} | {s13} | {s5} | {worst} | `{cmd}` | {action} |".format(
                device=row.get("device_id", ""),
                s13=_fmt(row.get("s1s3_max_abs_relative_error_percent")),
                s5=_fmt(row.get("s5_max_abs_relative_error_percent")),
                worst=row.get("worst_relative_point_identity", ""),
                cmd=row.get("s5_command_preview", ""),
                action=row.get("recommended_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 优先处理点位",
            "",
            "| 优先级 | 点位 | 超差设备数 | 最大相对误差% | 平均相对误差% | 方向 | 建议 |",
            "| ---: | --- | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in point_plan[:12]:
        lines.append(
            "| {score} | {point} | {over} | {max_rel} | {mean_rel} | {coherence} | {action} |".format(
                score=_fmt(row.get("priority_score")),
                point=row.get("point_identity", ""),
                over=row.get("devices_over_acceptance", ""),
                max_rel=_fmt(row.get("max_abs_relative_error_percent_after_s5")),
                mean_rel=_fmt(row.get("mean_abs_relative_error_percent_after_s5")),
                coherence=row.get("signed_coherence", ""),
                action=row.get("recommended_action_zh", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            "- 如果同一个温度/气点在多台设备上同时超差，优先判断为该点的源状态、管路状态或目标状态不连续，而不是某一台设备独立坏点。",
            "- 如果 ratio 证据为 A 级但仍跨设备超差，说明问题不能靠延长单点采样简单解决，应优先做状态桥接或最小补采。",
            "- `S5` 是最终显示层线性修正，只能压缩整体偏差，不能替代 `S1/S3` 主模型对低端和温度边界的解释能力。",
            "- CO2 零气锚点只约束 CO2 低端；不要把 H2O 干气锚点混作 CO2 零气证据。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_co2_s13_minimal_bridge_plan(
    *,
    closure_summary_csv: str | Path,
    corrected_residuals_csv: str | Path,
    output_dir: str | Path,
    acceptance_percent: float = DEFAULT_ACCEPTANCE_PERCENT,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    common_device_count: int = DEFAULT_COMMON_DEVICE_COUNT,
) -> Dict[str, str]:
    output = Path(output_dir)
    tables = build_co2_s13_minimal_bridge_plan(
        closure_summary_csv=closure_summary_csv,
        corrected_residuals_csv=corrected_residuals_csv,
        acceptance_percent=acceptance_percent,
        min_relative_target_ppm=min_relative_target_ppm,
        common_device_count=common_device_count,
    )
    paths = {
        "run_summary": output / "co2_s13_minimal_bridge_run_summary.csv",
        "device_plan": output / "co2_s13_minimal_bridge_device_plan.csv",
        "point_plan": output / "co2_s13_minimal_bridge_point_plan.csv",
        "metadata": output / "co2_s13_minimal_bridge_meta.json",
        "markdown": output / "co2_s13_minimal_bridge_plan_zh.md",
    }
    _write_csv(paths["run_summary"], tables["run_summary"])
    _write_csv(paths["device_plan"], tables["device_plan"])
    _write_csv(paths["point_plan"], tables["point_plan"])
    output.mkdir(parents=True, exist_ok=True)
    with paths["metadata"].open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "tool": "co2_s13_minimal_bridge_plan",
                "created_at": _now(),
                "inputs": {
                    "closure_summary_csv": str(Path(closure_summary_csv).resolve()),
                    "corrected_residuals_csv": str(Path(corrected_residuals_csv).resolve()),
                    "acceptance_percent": acceptance_percent,
                    "min_relative_target_ppm": min_relative_target_ppm,
                    "common_device_count": common_device_count,
                },
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "not_real_acceptance_evidence": True,
                },
                "outputs": {key: str(value.resolve()) for key, value in paths.items()},
            },
            handle,
            ensure_ascii=False,
            indent=2,
        )
    with paths["markdown"].open("w", encoding="utf-8") as handle:
        handle.write(render_markdown(tables))
    return {key: str(value) for key, value in paths.items()}
