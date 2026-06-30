"""Offline CO2 ratio-target monotonicity and low-end bias audit.

The audit is for already-selected V1.5 S1/S3 residual/state rows. It checks
whether filtered CO2 ratio is physically ordered against standard gas targets,
whether point identities and certificate targets are mapped consistently, and
whether low-end residuals share a common bias pattern. It is no-write and never
opens COM ports or controls routes.
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Mapping, Optional, Sequence


LOW_END_LIMIT_PPM = 400.0
RATIO_EPS = 1.0e-6


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


def _is_zero_anchor(row: Mapping[str, Any]) -> bool:
    flag = str(row.get("is_zero_anchor") or "").strip().lower()
    if flag in {"true", "1", "yes"}:
        return True
    identity = str(row.get("point_identity") or "").lower()
    return identity.endswith("_0ppm") or "_0ppm" in identity


def _target_ppm(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(row.get("target_ppm") or row.get("target_value"))


def _ratio(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(row.get("ratio") or row.get("co2_ratio_f_mean"))


def _relative_error(row: Mapping[str, Any]) -> Optional[float]:
    return _safe_float(row.get("relative_error_percent"))


def _identity_nominal_ppm(identity: Any) -> Optional[float]:
    text = str(identity or "")
    match = re.search(r"_([0-9]+(?:\.[0-9]+)?)ppm", text, flags=re.IGNORECASE)
    if not match:
        return None
    return _safe_float(match.group(1))


def _mapping_tolerance_ppm(nominal: float) -> float:
    return max(5.0, abs(float(nominal)) * 0.02)


def _group_rows(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> Dict[tuple[str, ...], List[Mapping[str, Any]]]:
    grouped: Dict[tuple[str, ...], List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        key = tuple(str(row.get(field) or "") for field in keys)
        grouped[key].append(row)
    return dict(grouped)


def _sort_target_rows(rows: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    return sorted(
        [row for row in rows if _target_ppm(row) is not None and _ratio(row) is not None],
        key=lambda row: float(_target_ppm(row) or 0.0),
    )


def _monotonic_direction(sorted_rows: Sequence[Mapping[str, Any]]) -> str:
    if len(sorted_rows) < 2:
        return "insufficient"
    first = _ratio(sorted_rows[0])
    last = _ratio(sorted_rows[-1])
    if first is None or last is None or abs(first - last) <= RATIO_EPS:
        return "flat_or_unknown"
    return "decreasing" if last < first else "increasing"


def _build_monotonicity_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    grouped = _group_rows(rows, ("device_id", "temperature_group"))
    for (device, temp), group in sorted(grouped.items()):
        sorted_rows = _sort_target_rows(group)
        direction = _monotonic_direction(sorted_rows)
        violations: List[str] = []
        adjacent_slopes: List[float] = []
        for left, right in zip(sorted_rows, sorted_rows[1:]):
            left_target = float(_target_ppm(left) or 0.0)
            right_target = float(_target_ppm(right) or 0.0)
            left_ratio = float(_ratio(left) or 0.0)
            right_ratio = float(_ratio(right) or 0.0)
            if abs(right_target - left_target) <= 1.0e-12:
                continue
            slope = (right_ratio - left_ratio) / (right_target - left_target)
            adjacent_slopes.append(slope)
            if direction == "decreasing" and slope > RATIO_EPS:
                violations.append(f"{left.get('point_identity')}->{right.get('point_identity')}")
            elif direction == "increasing" and slope < -RATIO_EPS:
                violations.append(f"{left.get('point_identity')}->{right.get('point_identity')}")
        low_rows = [row for row in sorted_rows if not _is_zero_anchor(row) and float(_target_ppm(row) or 0.0) <= LOW_END_LIMIT_PPM]
        out.append(
            {
                "device_id": device,
                "temperature_group": temp,
                "point_count": len(sorted_rows),
                "target_sequence": ";".join(f"{float(_target_ppm(row) or 0.0):g}" for row in sorted_rows),
                "ratio_sequence": ";".join(f"{float(_ratio(row) or 0.0):.6g}" for row in sorted_rows),
                "expected_ratio_direction": direction,
                "adjacent_violation_count": len(violations),
                "violation_pairs": ";".join(violations),
                "mean_adjacent_slope": mean(adjacent_slopes) if adjacent_slopes else "",
                "low_end_point_count": len(low_rows),
                "ratio_target_status": (
                    "ratio_target_mapping_suspect"
                    if violations
                    else "ratio_target_monotonic"
                    if direction in {"decreasing", "increasing"}
                    else "ratio_target_insufficient"
                ),
            }
        )
    return out


def _build_mapping_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    duplicate_counts = Counter(
        (
            _device_id(row.get("device_id")),
            str(row.get("temperature_group") or ""),
            str(row.get("point_identity") or ""),
        )
        for row in rows
    )
    out: List[Dict[str, Any]] = []
    for row in rows:
        device = _device_id(row.get("device_id"))
        identity = str(row.get("point_identity") or "")
        is_zero_anchor = _is_zero_anchor(row)
        nominal = _identity_nominal_ppm(identity)
        target = _target_ppm(row)
        delta = None
        status = "identity_nominal_missing"
        if nominal is not None and target is not None:
            delta = float(target) - float(nominal)
            if is_zero_anchor and abs(delta) > _mapping_tolerance_ppm(float(nominal)):
                status = "zero_anchor_assigned_value_review"
            elif is_zero_anchor:
                status = "zero_anchor_identity_value"
            else:
                status = (
                    "target_matches_certificate_value"
                    if abs(delta) <= _mapping_tolerance_ppm(float(nominal))
                    else "target_identity_mapping_suspect"
                )
        dup_count = duplicate_counts[(device, str(row.get("temperature_group") or ""), identity)]
        if dup_count > 1:
            status = "duplicate_point_identity_suspect"
        out.append(
            {
                "device_id": device,
                "temperature_group": row.get("temperature_group") or "",
                "point_identity": identity,
                "is_zero_anchor": is_zero_anchor,
                "identity_nominal_ppm": nominal if nominal is not None else "",
                "target_ppm": target if target is not None else "",
                "target_minus_identity_nominal_ppm": delta if delta is not None else "",
                "mapping_tolerance_ppm": _mapping_tolerance_ppm(float(nominal)) if nominal is not None else "",
                "duplicate_count": dup_count,
                "mapping_status": status,
                "ratio_grade": row.get("ratio_grade") or "",
                "dryness_grade": row.get("dryness_grade") or "",
            }
        )
    return out


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


def _build_low_end_bias_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    grouped = _group_rows(rows, ("device_id", "temperature_group"))
    for (device, temp), group in sorted(grouped.items()):
        low_rows = [
            row
            for row in group
            if not _is_zero_anchor(row)
            and (_target_ppm(row) or 0.0) <= LOW_END_LIMIT_PPM
            and _relative_error(row) is not None
        ]
        if not low_rows:
            continue
        rels = [float(_relative_error(row) or 0.0) for row in low_rows]
        same_sign = _same_sign_fraction(rels)
        out.append(
            {
                "device_id": device,
                "temperature_group": temp,
                "low_end_point_count": len(low_rows),
                "low_end_targets": ";".join(f"{float(_target_ppm(row) or 0.0):g}" for row in sorted(low_rows, key=lambda row: float(_target_ppm(row) or 0.0))),
                "mean_low_end_relative_error_percent": mean(rels),
                "max_abs_low_end_relative_error_percent": max(abs(value) for value in rels),
                "same_sign_fraction": same_sign,
                "dominant_sign": "positive" if mean(rels) > 0 else "negative" if mean(rels) < 0 else "mixed",
                "low_end_bias_status": (
                    "low_end_common_bias"
                    if len(low_rows) >= 2 and same_sign >= 0.8 and max(abs(value) for value in rels) >= 1.0
                    else "low_end_mixed_or_small"
                ),
            }
        )
    return out


def _build_device_summary(
    monotonicity_rows: Sequence[Mapping[str, Any]],
    mapping_rows: Sequence[Mapping[str, Any]],
    low_end_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    devices = sorted(
        {
            _device_id(row.get("device_id"))
            for table in (monotonicity_rows, mapping_rows, low_end_rows)
            for row in table
            if _device_id(row.get("device_id"))
        }
    )
    summary: List[Dict[str, Any]] = []
    for device in devices:
        mono = [row for row in monotonicity_rows if _device_id(row.get("device_id")) == device]
        mapping = [row for row in mapping_rows if _device_id(row.get("device_id")) == device]
        low = [row for row in low_end_rows if _device_id(row.get("device_id")) == device]
        violation_count = sum(int(row.get("adjacent_violation_count") or 0) for row in mono)
        zero_anchor_reviews = [
            row
            for row in mapping
            if str(row.get("mapping_status") or "") == "zero_anchor_assigned_value_review"
        ]
        mapping_suspects = [
            row
            for row in mapping
            if str(row.get("mapping_status") or "")
            not in {
                "target_matches_certificate_value",
                "zero_anchor_identity_value",
                "zero_anchor_assigned_value_review",
            }
        ]
        low_common = [row for row in low if row.get("low_end_bias_status") == "low_end_common_bias"]
        max_low = max(
            [_safe_float(row.get("max_abs_low_end_relative_error_percent")) or 0.0 for row in low],
            default=0.0,
        )
        action = "review_s13_low_end_model_boundary"
        reason = "ratio 与目标浓度排序在物理上自洽，但低端共同偏差仍然存在。"
        if violation_count:
            action = "block_write_review_route_or_point_mapping"
            reason = "至少一个温度组内 filtered ratio 与目标浓度不单调，需要先排查阀路、点位或数据映射。"
        elif mapping_suspects:
            action = "review_certificate_point_mapping_before_refit"
            reason = "存在真实点位、证书目标值或重复点位映射疑点，写系数前必须先复核。"
        elif zero_anchor_reviews and low_common:
            action = "review_zero_anchor_assigned_value_and_low_end_model_boundary"
            reason = "零气 CO2 赋值是低端截距的溯源假设，同时低端共同偏差仍然存在。"
        elif zero_anchor_reviews:
            action = "review_zero_anchor_assigned_value_not_route_mapping"
            reason = "只有零气赋值与点位名义 0ppm 不同，这不是阀路或气瓶映射错误证据。"
        elif not low_common and max_low < 1.0:
            action = "low_end_bias_not_primary"
            reason = "低端残差较小或方向混合，应优先检查高端和温度边界。"
        summary.append(
            {
                "device_id": device,
                "temperature_group_count": len(mono),
                "ratio_monotonic_violation_count": violation_count,
                "mapping_suspect_count": len(mapping_suspects),
                "zero_anchor_assigned_value_review_count": len(zero_anchor_reviews),
                "low_end_common_bias_group_count": len(low_common),
                "max_low_end_relative_error_percent": max_low,
                "recommended_action": action,
                "physical_reason": reason,
            }
        )
    return summary


def build_co2_s13_ratio_target_mapping_audit(
    *,
    selected_residual_state_csv: str | Path,
) -> Dict[str, List[Dict[str, Any]]]:
    rows = _read_csv(selected_residual_state_csv)
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["device_id"] = _device_id(item.get("device_id") or item.get("analyzer_device_id"))
        normalized.append(item)
    monotonicity = _build_monotonicity_rows(normalized)
    mapping = _build_mapping_rows(normalized)
    low_end = _build_low_end_bias_rows(normalized)
    summary = _build_device_summary(monotonicity, mapping, low_end)
    return {
        "ratio_target_monotonicity": monotonicity,
        "point_mapping_audit": mapping,
        "low_end_common_bias": low_end,
        "device_summary": summary,
    }


def _fmt(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.3f}"


def _render_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    summary = list(tables.get("device_summary", []))
    low = list(tables.get("low_end_common_bias", []))
    mono = list(tables.get("ratio_target_monotonicity", []))
    lines = [
        "# V1.5 CO2 ratio-目标与低端共同偏差审计",
        "",
        f"生成时间：{_now()}",
        "",
        "## 边界",
        "",
        "- 本报告只使用已有离线 CSV 证据。",
        "- 不打开 COM、不控制气路/水路、不写 SENCO。",
        "- 本报告只判断点位物理一致性和残差结构，不给出写入批准。",
        "",
        "## 逐台结论",
        "",
    ]
    for row in summary:
        lines.append(
            "- 设备 {device}: ratio 单调违规 {mono} 组，映射疑点 {mapping} 个，低端共同偏差 {low} 组，低端最大相对误差 {err}%；建议：{action}。".format(
                device=row.get("device_id"),
                mono=row.get("ratio_monotonic_violation_count"),
                mapping=row.get("mapping_suspect_count"),
                low=row.get("low_end_common_bias_group_count"),
                err=_fmt(row.get("max_low_end_relative_error_percent")),
                action=row.get("recommended_action"),
            )
        )
        lines.append(f"  物理原因：{row.get('physical_reason')}")
    lines.extend(["", "## 低端共同偏差温度组", ""])
    for row in low:
        if row.get("low_end_bias_status") != "low_end_common_bias":
            continue
        lines.append(
            "- 设备 {device} {temp}: 低端 {targets} ppm 同向 {sign}，均值 {mean_err}%，最大 {max_err}%。".format(
                device=row.get("device_id"),
                temp=row.get("temperature_group"),
                targets=row.get("low_end_targets"),
                sign=row.get("dominant_sign"),
                mean_err=_fmt(row.get("mean_low_end_relative_error_percent")),
                max_err=_fmt(row.get("max_abs_low_end_relative_error_percent")),
            )
        )
    violations = [row for row in mono if int(row.get("adjacent_violation_count") or 0) > 0]
    lines.extend(["", "## ratio-目标单调性提示", ""])
    if not violations:
        lines.append("- 未发现 CO2 filtered ratio 与目标浓度排序相反的温度组；这说明点位映射和阀路顺序没有出现明显反物理证据。")
    else:
        for row in violations:
            lines.append(
                "- 设备 {device} {temp}: 违规 {count} 对，{pairs}。".format(
                    device=row.get("device_id"),
                    temp=row.get("temperature_group"),
                    count=row.get("adjacent_violation_count"),
                    pairs=row.get("violation_pairs"),
                )
            )
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            "- 如果 ratio 随目标气浓度单调变化，说明气瓶/阀路映射大概率没有反接或错点；残差更可能来自模型形状、目标状态或设备响应非线性。",
            "- 如果低端 100/200/300/400 ppm 在同一温度组同向偏差，说明低端锚定或温度项边界需要优先处理。",
            "- 0 气是 CO2 低端锚点；它不能和 H2O 干气锚点混用。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_markdown_zh(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    summary = list(tables.get("device_summary", []))
    low = list(tables.get("low_end_common_bias", []))
    mono = list(tables.get("ratio_target_monotonicity", []))
    lines = [
        "# V1.5 CO2 ratio-目标映射与低端共同偏差审计",
        "",
        f"生成时间：{_now()}",
        "",
        "## 边界",
        "",
        "- 本报告只使用已有离线 CSV 证据。",
        "- 不打开 COM、不控制气路或水路、不写 SENCO。",
        "- 本报告只判断点位物理一致性和残差结构，不给出写入批准。",
        "- 零气锚点的 CO2 赋值属于低端截距假设评审，不等同于阀路或气瓶映射错误。",
        "",
        "## 逐台结论",
        "",
    ]
    for row in summary:
        lines.append(
            "- 设备 {device}: ratio 单调违规 {mono} 组，真实映射疑点 {mapping} 个，零气赋值评审 {zero} 个，"
            "低端共同偏差 {low} 组，低端最大相对误差 {err}%；建议：{action}。".format(
                device=row.get("device_id"),
                mono=row.get("ratio_monotonic_violation_count"),
                mapping=row.get("mapping_suspect_count"),
                zero=row.get("zero_anchor_assigned_value_review_count", 0),
                low=row.get("low_end_common_bias_group_count"),
                err=_fmt(row.get("max_low_end_relative_error_percent")),
                action=row.get("recommended_action"),
            )
        )
        lines.append(f"  物理原因：{row.get('physical_reason')}")
    lines.extend(["", "## 低端共同偏差温度组", ""])
    for row in low:
        if row.get("low_end_bias_status") != "low_end_common_bias":
            continue
        lines.append(
            "- 设备 {device} {temp}: 低端 {targets} ppm 同向 {sign}，均值 {mean_err}%，最大 {max_err}%。".format(
                device=row.get("device_id"),
                temp=row.get("temperature_group"),
                targets=row.get("low_end_targets"),
                sign=row.get("dominant_sign"),
                mean_err=_fmt(row.get("mean_low_end_relative_error_percent")),
                max_err=_fmt(row.get("max_abs_low_end_relative_error_percent")),
            )
        )
    violations = [row for row in mono if int(row.get("adjacent_violation_count") or 0) > 0]
    lines.extend(["", "## ratio-目标单调性提示", ""])
    if not violations:
        lines.append("- 未发现 CO2 filtered ratio 与目标浓度排序相反的温度组；这说明点位映射和阀路顺序没有明显反物理证据。")
    else:
        for row in violations:
            lines.append(
                "- 设备 {device} {temp}: 违规 {count} 对，{pairs}。".format(
                    device=row.get("device_id"),
                    temp=row.get("temperature_group"),
                    count=row.get("adjacent_violation_count"),
                    pairs=row.get("violation_pairs"),
                )
            )
    lines.extend(
        [
            "",
            "## 物理解释",
            "",
            "- 如果 ratio 随目标气浓度单调变化，说明气瓶/阀路映射大概率没有反接或错点；残差更可能来自模型形状、目标状态或设备响应非线性。",
            "- 如果低端 100/200/300/400 ppm 在同一温度组同向偏差，说明低端锚定或温度项边界需要优先处理。",
            "- 0 气是 CO2 低端锚点；它不能和 H2O 干气锚点混用。",
            "",
        ]
    )
    return "\n".join(lines)


def write_co2_s13_ratio_target_mapping_audit(
    *,
    selected_residual_state_csv: str | Path,
    output_dir: str | Path,
) -> Dict[str, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_ratio_target_mapping_audit(
        selected_residual_state_csv=selected_residual_state_csv,
    )
    outputs = {
        "ratio_target_monotonicity": out_dir / "co2_s13_ratio_target_monotonicity.csv",
        "point_mapping_audit": out_dir / "co2_s13_point_mapping_audit.csv",
        "low_end_common_bias": out_dir / "co2_s13_low_end_common_bias.csv",
        "device_summary": out_dir / "co2_s13_ratio_mapping_device_summary.csv",
        "metadata": out_dir / "co2_s13_ratio_mapping_audit_meta.json",
        "markdown": out_dir / "co2_s13_ratio_mapping_audit_zh.md",
    }
    for key, path in outputs.items():
        if key in {"metadata", "markdown"}:
            continue
        _write_csv(path, tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "generated_at": _now(),
                "selected_residual_state_csv": str(selected_residual_state_csv),
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "not_real_acceptance_evidence": True,
                },
                "tables": {key: str(path) for key, path in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs["markdown"].write_text("\ufeff" + _render_markdown_zh(tables), encoding="utf-8")
    return outputs
