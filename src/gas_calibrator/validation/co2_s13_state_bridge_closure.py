"""Offline CO2 point-state bridge closure review.

This review answers one narrow question after a fast S1/S3 + S5 closure fails:
if a temperature/gas point has a common cross-device bias, can the remaining
error be explained by that point's shared source/route state?

The calculation is leave-one-out by point identity. For each device at the same
temperature/gas point, it subtracts the mean signed error of the other devices
at that point. This is a diagnostic bridge only. It is not a writeable SENCO
model and it never opens COM ports, controls routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Mapping, Optional, Sequence


DEFAULT_ACCEPTANCE_PERCENT = 1.0
DEFAULT_MIN_RELATIVE_TARGET_PPM = 50.0
DEFAULT_MIN_BRIDGE_SUPPORT = 3
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


def _target(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(row.get("target_ppm"))


def _base_error(row: Mapping[str, Any]) -> Optional[float]:
    explicit = _safe_float(row.get("s5_error_ppm"))
    if explicit is not None:
        return explicit
    target = _target(row)
    prediction = _safe_float(row.get("s5_corrected_prediction_ppm"))
    if target is None or prediction is None:
        return None
    return float(prediction) - float(target)


def _relative(error: float, target: float, *, min_relative_target_ppm: float) -> Optional[float]:
    if abs(float(target)) < float(min_relative_target_ppm):
        return None
    return 100.0 * float(error) / float(target)


def _mean_signed(values: Sequence[float]) -> float:
    return float(mean(values)) if values else 0.0


def _state_fields(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "temperature_c": row.get("temperature_c", ""),
        "pressure_hpa": row.get("pressure_hpa", ""),
        "h2o_mmol": row.get("h2o_mmol", ""),
        "ratio": row.get("ratio", ""),
        "ratio_grade": row.get("ratio_grade", ""),
        "dryness_grade": row.get("dryness_grade", ""),
        "physical_qc_label": row.get("physical_qc_label", ""),
    }


def build_co2_s13_state_bridge_closure(
    *,
    corrected_residuals_csv: str | Path,
    acceptance_percent: float = DEFAULT_ACCEPTANCE_PERCENT,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    min_bridge_support: int = DEFAULT_MIN_BRIDGE_SUPPORT,
) -> Dict[str, Any]:
    rows = _read_csv(corrected_residuals_csv)
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    usable_rows: List[Dict[str, Any]] = []
    for row in rows:
        target = _target(row)
        error = _base_error(row)
        point = str(row.get("point_identity") or "").strip()
        if target is None or error is None or not point:
            continue
        if abs(float(target)) < float(min_relative_target_ppm):
            continue
        item = dict(row)
        item["_target"] = float(target)
        item["_base_error"] = float(error)
        item["_base_relative"] = _relative(
            float(error),
            float(target),
            min_relative_target_ppm=float(min_relative_target_ppm),
        )
        grouped[point].append(item)
        usable_rows.append(item)

    bridged_rows: List[Dict[str, Any]] = []
    for point, point_rows in grouped.items():
        point_errors = [float(row["_base_error"]) for row in point_rows]
        for row in point_rows:
            other_errors = [
                float(other["_base_error"])
                for other in point_rows
                if _device_id(other.get("device_id")) != _device_id(row.get("device_id"))
            ]
            correction = _mean_signed(other_errors) if len(other_errors) >= 1 else 0.0
            bridged_error = float(row["_base_error"]) - correction
            bridged_relative = _relative(
                bridged_error,
                float(row["_target"]),
                min_relative_target_ppm=float(min_relative_target_ppm),
            )
            bridged_rows.append(
                {
                    "device_id": _device_id(row.get("device_id")),
                    "point_identity": point,
                    **_parse_point_identity(point),
                    "target_ppm": row["_target"],
                    "s5_error_ppm": row["_base_error"],
                    "s5_relative_error_percent": row["_base_relative"] if row["_base_relative"] is not None else "",
                    "point_common_error_ppm": _mean_signed(point_errors),
                    "loo_bridge_correction_ppm": correction,
                    "bridge_support_count": len(other_errors),
                    "bridged_error_ppm": bridged_error,
                    "bridged_relative_error_percent": bridged_relative if bridged_relative is not None else "",
                    "base_over_acceptance": abs(float(row["_base_relative"] or 0.0)) > float(acceptance_percent)
                    if row["_base_relative"] is not None
                    else False,
                    "bridge_over_acceptance": abs(float(bridged_relative or 0.0)) > float(acceptance_percent)
                    if bridged_relative is not None
                    else False,
                    "bridge_is_diagnostic_only": True,
                    "writes_coefficients": False,
                    "opens_com_ports": False,
                    **_state_fields(row),
                }
            )

    device_grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    point_grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in bridged_rows:
        device_grouped[str(row.get("device_id"))].append(row)
        point_grouped[str(row.get("point_identity"))].append(row)

    device_summary: List[Dict[str, Any]] = []
    for device, device_rows in sorted(device_grouped.items()):
        base_rel = [
            abs(float(value))
            for value in (row.get("s5_relative_error_percent") for row in device_rows)
            if _safe_float(value) is not None
        ]
        bridge_rel = [
            abs(float(value))
            for value in (row.get("bridged_relative_error_percent") for row in device_rows)
            if _safe_float(value) is not None
        ]
        over_before = [row for row in device_rows if row.get("base_over_acceptance")]
        over_after = [row for row in device_rows if row.get("bridge_over_acceptance")]
        device_summary.append(
            {
                "device_id": device,
                "point_count": len(device_rows),
                "base_max_abs_relative_error_percent": max(base_rel) if base_rel else "",
                "bridged_max_abs_relative_error_percent": max(bridge_rel) if bridge_rel else "",
                "base_over_acceptance_count": len(over_before),
                "bridged_over_acceptance_count": len(over_after),
                "bridged_worst_point": max(
                    device_rows,
                    key=lambda row: abs(float(_safe_float(row.get("bridged_relative_error_percent")) or 0.0)),
                ).get("point_identity")
                if device_rows
                else "",
                "bridge_closes_device_to_acceptance": bool(bridge_rel) and max(bridge_rel) <= float(acceptance_percent),
                "recommended_action": (
                    "bridge_closes_device_diagnostic_only"
                    if bridge_rel and max(bridge_rel) <= float(acceptance_percent)
                    else "bridge_reduces_but_not_closed"
                ),
                "writes_coefficients": False,
                "opens_com_ports": False,
            }
        )

    point_summary: List[Dict[str, Any]] = []
    for point, point_rows in sorted(point_grouped.items()):
        base_rel = [
            abs(float(value))
            for value in (row.get("s5_relative_error_percent") for row in point_rows)
            if _safe_float(value) is not None
        ]
        bridge_rel = [
            abs(float(value))
            for value in (row.get("bridged_relative_error_percent") for row in point_rows)
            if _safe_float(value) is not None
        ]
        over_before = [row for row in point_rows if row.get("base_over_acceptance")]
        over_after = [row for row in point_rows if row.get("bridge_over_acceptance")]
        meta = _parse_point_identity(point)
        supported = len(point_rows) >= int(min_bridge_support)
        point_summary.append(
            {
                "point_identity": point,
                "temperature_c": meta.get("temperature_c", ""),
                "gas_ppm": meta.get("gas_ppm", ""),
                "device_count": len(point_rows),
                "base_over_acceptance_count": len(over_before),
                "bridged_over_acceptance_count": len(over_after),
                "base_max_abs_relative_error_percent": max(base_rel) if base_rel else "",
                "bridged_max_abs_relative_error_percent": max(bridge_rel) if bridge_rel else "",
                "mean_common_error_ppm": _mean_signed([float(row.get("s5_error_ppm") or 0.0) for row in point_rows]),
                "bridge_support_is_sufficient": supported,
                "bridge_closes_point_to_acceptance": bool(bridge_rel) and max(bridge_rel) <= float(acceptance_percent),
                "recommended_action": (
                    "accept_existing_point_with_bridge_evidence"
                    if supported and bridge_rel and max(bridge_rel) <= float(acceptance_percent)
                    else "minimal_resample_this_point"
                    if supported
                    else "insufficient_bridge_support_resample"
                ),
                "writes_coefficients": False,
                "opens_com_ports": False,
            }
        )
    point_summary.sort(
        key=lambda row: (
            int(row.get("bridged_over_acceptance_count") or 0),
            float(_safe_float(row.get("bridged_max_abs_relative_error_percent")) or 0.0),
            float(_safe_float(row.get("base_max_abs_relative_error_percent")) or 0.0),
        ),
        reverse=True,
    )

    return {
        "run_summary": [
            {
                "created_at": _now(),
                "corrected_residuals_csv": str(Path(corrected_residuals_csv).resolve()),
                "acceptance_percent": float(acceptance_percent),
                "min_relative_target_ppm": float(min_relative_target_ppm),
                "min_bridge_support": int(min_bridge_support),
                "device_count": len(device_summary),
                "point_count": len(point_summary),
                "bridge_is_diagnostic_only": True,
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "not_real_acceptance_evidence": True,
            }
        ],
        "device_summary": device_summary,
        "point_summary": point_summary,
        "bridged_residuals": bridged_rows,
    }


def _fmt(value: Any, digits: int = 3) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def render_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    device_summary = list(tables.get("device_summary") or [])
    point_summary = list(tables.get("point_summary") or [])
    closed_devices = [row for row in device_summary if row.get("bridge_closes_device_to_acceptance")]
    closed_points = [row for row in point_summary if row.get("bridge_closes_point_to_acceptance")]
    lines = [
        "# CO2 状态桥接闭合评审",
        "",
        "本报告只做离线诊断桥接：按同一温度/气点的跨设备共同偏差做留一修正，用于判断是否需要最小补采。不打开 COM、不控制气路/水路、不写 SENCO。",
        "",
        "## 总体结论",
        "",
        f"- 桥接后全设备闭合数量：{len(closed_devices)} / {len(device_summary)}。",
        f"- 桥接后全点位闭合数量：{len(closed_points)} / {len(point_summary)}。",
        "- 若桥接能显著闭合，说明主要误差来自共同点位源状态/管路状态；若桥接仍不能闭合，则该点需要最小补采或模型结构继续评审。",
        "",
        "## 逐设备结果",
        "",
        "| 设备ID | S5后最大相对误差% | 桥接后最大相对误差% | 桥接后超差点数 | 建议 |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for row in device_summary:
        lines.append(
            "| {device} | {base} | {bridge} | {over} | {action} |".format(
                device=row.get("device_id", ""),
                base=_fmt(row.get("base_max_abs_relative_error_percent")),
                bridge=_fmt(row.get("bridged_max_abs_relative_error_percent")),
                over=row.get("bridged_over_acceptance_count", ""),
                action=row.get("recommended_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 逐点位结果",
            "",
            "| 点位 | S5后最大相对误差% | 桥接后最大相对误差% | 桥接后超差设备数 | 建议 |",
            "| --- | ---: | ---: | ---: | --- |",
        ]
    )
    for row in point_summary[:16]:
        lines.append(
            "| {point} | {base} | {bridge} | {over} | {action} |".format(
                point=row.get("point_identity", ""),
                base=_fmt(row.get("base_max_abs_relative_error_percent")),
                bridge=_fmt(row.get("bridged_max_abs_relative_error_percent")),
                over=row.get("bridged_over_acceptance_count", ""),
                action=row.get("recommended_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 物理意义",
            "",
            "- 这个桥接不是校准系数，也不是写入方案；它只是回答：同一点位的共同偏差是否足以解释超差。",
            "- 如果同点位留一桥接后误差明显下降，优先处理该点的气源状态、管路残留、露点/水汽状态或目标映射，而不是盲目扩大 S5。",
            "- 如果桥接后仍超过目标，则说明该点不能只靠共同状态解释，需要最小补采或重新评审 S1/S3 模型结构。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_co2_s13_state_bridge_closure(
    *,
    corrected_residuals_csv: str | Path,
    output_dir: str | Path,
    acceptance_percent: float = DEFAULT_ACCEPTANCE_PERCENT,
    min_relative_target_ppm: float = DEFAULT_MIN_RELATIVE_TARGET_PPM,
    min_bridge_support: int = DEFAULT_MIN_BRIDGE_SUPPORT,
) -> Dict[str, str]:
    output = Path(output_dir)
    tables = build_co2_s13_state_bridge_closure(
        corrected_residuals_csv=corrected_residuals_csv,
        acceptance_percent=acceptance_percent,
        min_relative_target_ppm=min_relative_target_ppm,
        min_bridge_support=min_bridge_support,
    )
    paths = {
        "run_summary": output / "co2_s13_state_bridge_run_summary.csv",
        "device_summary": output / "co2_s13_state_bridge_device_summary.csv",
        "point_summary": output / "co2_s13_state_bridge_point_summary.csv",
        "bridged_residuals": output / "co2_s13_state_bridge_residuals.csv",
        "metadata": output / "co2_s13_state_bridge_meta.json",
        "markdown": output / "co2_s13_state_bridge_review_zh.md",
    }
    _write_csv(paths["run_summary"], tables["run_summary"])
    _write_csv(paths["device_summary"], tables["device_summary"])
    _write_csv(paths["point_summary"], tables["point_summary"])
    _write_csv(paths["bridged_residuals"], tables["bridged_residuals"])
    output.mkdir(parents=True, exist_ok=True)
    with paths["metadata"].open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "tool": "co2_s13_state_bridge_closure",
                "created_at": _now(),
                "inputs": {
                    "corrected_residuals_csv": str(Path(corrected_residuals_csv).resolve()),
                    "acceptance_percent": acceptance_percent,
                    "min_relative_target_ppm": min_relative_target_ppm,
                    "min_bridge_support": min_bridge_support,
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
