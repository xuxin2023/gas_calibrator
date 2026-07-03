"""Offline CO2 S1/S3 root-cause closure review.

This module consolidates the V1.5 CO2 S1/S3 source-state, target-mapping,
bridge, and fit-repair reviews into one no-write decision package.  It never
opens COM ports, controls routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
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
    numeric = _safe_float(value)
    if numeric is None:
        return 0
    return int(numeric)


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _is_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def _index_by_device(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    out: Dict[str, Mapping[str, Any]] = {}
    for row in rows:
        device = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if device:
            out[device] = row
    return out


def _group_by_point(rows: Sequence[Mapping[str, Any]]) -> Dict[str, List[Mapping[str, Any]]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        point = str(row.get("point_identity") or row.get("temperature_group", "") + "_" + row.get("target_group", "")).strip()
        if point:
            grouped[point].append(row)
    return dict(grouped)


def _max_abs(values: Sequence[Optional[float]]) -> Optional[float]:
    clean = [abs(float(value)) for value in values if value is not None and math.isfinite(float(value))]
    return max(clean) if clean else None


def _device_decision(
    *,
    device: str,
    ratio_row: Mapping[str, Any],
    bridge_row: Mapping[str, Any],
    correction_row: Mapping[str, Any],
    repair_rows: Sequence[Mapping[str, Any]],
    acceptance_percent: float,
) -> Dict[str, Any]:
    mapping_suspect = _safe_int(ratio_row.get("mapping_suspect_count"))
    ratio_violations = _safe_int(ratio_row.get("ratio_monotonic_violation_count"))
    zero_anchor_reviews = _safe_int(ratio_row.get("zero_anchor_assigned_value_review_count"))
    bridge_max = _safe_float(bridge_row.get("max_abs_relative_error_percent"))
    correction_best = _safe_float(correction_row.get("best_max_abs_relative_error_percent"))
    repair_s1s3 = _max_abs([_safe_float(row.get("s1s3_worst_relative_error_percent")) for row in repair_rows])
    repair_s5 = _max_abs([_safe_float(row.get("s5_worst_relative_error_percent")) for row in repair_rows])

    blockers: List[str] = []
    if mapping_suspect:
        blockers.append("target_or_point_mapping_suspect")
    if ratio_violations:
        blockers.append("ratio_target_monotonicity_violation")
    if zero_anchor_reviews:
        blockers.append("zero_anchor_assigned_value_review")
    if bridge_max is not None and bridge_max > acceptance_percent:
        blockers.append("s1s3_main_chain_error_exceeds_target")
    if correction_best is not None and correction_best > acceptance_percent:
        blockers.append("simple_bridge_or_s5_cannot_close_error")

    if blockers:
        status = "blocked_do_not_write_s13"
        action = "resolve_source_state_target_mapping_or_model_boundary_before_senco13_write"
    else:
        status = "review_possible"
        action = "candidate_can_enter_controlled_write_review"

    return {
        "device_id": device,
        "status": status,
        "blockers": ";".join(blockers),
        "ratio_mapping_action": ratio_row.get("recommended_action", ""),
        "ratio_mapping_reason": ratio_row.get("physical_reason", ""),
        "bridge_primary_hypothesis": bridge_row.get("primary_hypothesis", ""),
        "bridge_worst_point": bridge_row.get("worst_point_identity", ""),
        "bridge_max_abs_relative_error_percent": bridge_max if bridge_max is not None else "",
        "simple_bridge_best_candidate": correction_row.get("best_candidate_id", ""),
        "simple_bridge_best_max_abs_relative_error_percent": correction_best if correction_best is not None else "",
        "repair_review_worst_s1s3_relative_error_percent": repair_s1s3 if repair_s1s3 is not None else "",
        "repair_review_worst_s5_relative_error_percent": repair_s5 if repair_s5 is not None else "",
        "next_action": action,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
    }


def _point_decision(
    *,
    point: str,
    common_rows: Sequence[Mapping[str, Any]],
    source_rows: Sequence[Mapping[str, Any]],
    mapping_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    common = common_rows[0] if common_rows else {}
    source = source_rows[0] if source_rows else {}
    mapping_suspect_count = sum(1 for row in mapping_rows if str(row.get("mapping_status") or "") not in {"", "target_matches_certificate_value", "zero_anchor_identity_value"})
    zero_anchor_review_count = sum(1 for row in mapping_rows if str(row.get("mapping_status") or "") == "zero_anchor_assigned_value_review")
    is_zero_anchor_point = point.strip().lower().endswith("_0ppm") or zero_anchor_review_count > 0
    flags = str(source.get("state_flags") or "")
    root = str(common.get("root_cause_class") or "")
    rel = _safe_float(common.get("max_abs_relative_error_percent") or source.get("max_abs_relative_error_percent"))
    source_labels = source.get("source_labels", "")

    blockers: List[str] = []
    treatment = "keep_for_review_not_auto_exclude"
    if "shared_point_bias" in flags or "common_mode" in root:
        blockers.append("common_mode_bias")
    if mapping_suspect_count and not is_zero_anchor_point:
        blockers.append("mapping_suspect")
    if zero_anchor_review_count or (mapping_suspect_count and is_zero_anchor_point):
        blockers.append("zero_anchor_value_review")
    if point in {"T20_400ppm", "T20_600ppm", "T30_300ppm"}:
        blockers.append("pressure_state_outlier_review")
    if point in {"T30_200ppm", "T30_300ppm", "T30_400ppm", "T30_600ppm"}:
        blockers.append("supplement_source_bridge_not_proven")
    hard_hold_blockers = {
        "mapping_suspect",
        "pressure_state_outlier_review",
        "supplement_source_bridge_not_proven",
    }
    if any(blocker in hard_hold_blockers for blocker in blockers):
        treatment = "hold_as_diagnostic_until_bridge_evidence_passes"
    elif "zero_anchor_value_review" in blockers:
        treatment = "review_zero_anchor_value_before_fit"
    elif "common_mode_bias" in blockers:
        treatment = "keep_for_model_boundary_review_not_auto_exclude"

    return {
        "point_identity": point,
        "temperature_group": common.get("temperature_group") or source.get("temperature_group") or "",
        "target_ppm": common.get("target_ppm") or "",
        "device_count": common.get("device_count") or source.get("device_count") or "",
        "max_abs_relative_error_percent": rel if rel is not None else "",
        "source_labels": source_labels,
        "state_flags": flags,
        "root_cause_class": root,
        "mapping_suspect_count": mapping_suspect_count,
        "zero_anchor_review_count": zero_anchor_review_count,
        "blockers": ";".join(blockers),
        "recommended_treatment": treatment,
        "physical_meaning": _point_physical_meaning(point, blockers, flags, root),
    }


def _point_physical_meaning(point: str, blockers: Sequence[str], flags: str, root: str) -> str:
    if "supplement_source_bridge_not_proven" in blockers:
        return "该点来自补点运行或与主温度组来源不一致，必须先证明露点、压力、阀路、端口映射和气源状态可桥接。"
    if "pressure_state_outlier_review" in blockers:
        return "该点在同温度组内压力状态离群；CO2 主拟合不带压力项，但压力状态离群说明开放流通目标状态可能变了。"
    if "zero_anchor_value_review" in blockers:
        return "零气低端锚点不是普通湿度干点，必须单独确认 CO2 估算值和证据链。"
    if "common_mode_bias" in blockers or "common_mode" in root or "shared_point_bias" in flags:
        return "多台设备同一气点同向偏差，且物理状态已稳定，更像模型边界或目标状态共同偏差。"
    return "未发现必须剔除的独立异常，但仍应随最终拟合策略复核。"


def _action_plan(
    *,
    gate_status: str,
    source_decisions: Sequence[Mapping[str, Any]],
    device_decisions: Sequence[Mapping[str, Any]],
    point_decisions: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    p0_topics = [str(row.get("topic") or "") for row in source_decisions if str(row.get("priority") or "") == "P0"]
    blocked_devices = [row["device_id"] for row in device_decisions if str(row.get("status")) != "review_possible"]
    held_points = [row["point_identity"] for row in point_decisions if str(row.get("recommended_treatment")) == "hold_as_diagnostic_until_bridge_evidence_passes"]
    return [
        {
            "priority": "P0",
            "action": "block_senco13_write",
            "reason": gate_status,
            "details": ";".join(p0_topics),
            "physical_meaning": "S1/S3 是 CO2 主链路系数，源状态或目标映射未闭环前写入会把气路状态问题固化进设备。",
        },
        {
            "priority": "P0",
            "action": "formalize_bridge_gate_for_supplement_points",
            "reason": "T30 supplement source differs from main run",
            "details": ";".join(point for point in held_points if point.startswith("T30_")),
            "physical_meaning": "补点只有在露点、压力、阀位、端口映射、气瓶目标值和稳定窗口一致时才能作为 A 级拟合点。",
        },
        {
            "priority": "P1",
            "action": "audit_pressure_state_outliers_as_state_evidence",
            "reason": "pressure is frozen from fitting but not ignored as state evidence",
            "details": ";".join(point for point in held_points if point in {"T20_400ppm", "T20_600ppm", "T30_300ppm"}),
            "physical_meaning": "当前大气压开放流通拟合不加入压力项；但压力状态离群代表流量/排气/阀路状态可能不同。",
        },
        {
            "priority": "P1",
            "action": "review_zero_anchor_and_low_end_boundary",
            "reason": "low-end common bias remains after ratio and dewpoint pass",
            "details": ";".join(blocked_devices),
            "physical_meaning": "低端锚点影响截距，不能把 CO2 零气锚点和 H2O 干气锚点混成一个低端点。",
        },
    ]


def build_co2_s13_root_cause_closure_review(
    *,
    source_state_dir: str | Path,
    ratio_mapping_dir: str | Path,
    target_state_bridge_dir: str | Path,
    bridge_correction_dir: str | Path,
    repair_fit_dir: str | Path,
    error_root_cause_dir: str | Path,
    acceptance_percent: float = 1.0,
) -> Dict[str, List[Dict[str, Any]]]:
    source_dir = Path(source_state_dir)
    ratio_dir = Path(ratio_mapping_dir)
    target_dir = Path(target_state_bridge_dir)
    correction_dir = Path(bridge_correction_dir)
    repair_dir = Path(repair_fit_dir)
    error_dir = Path(error_root_cause_dir)

    source_summary = _read_csv(source_dir / "co2_s13_source_state_run_summary.csv")
    source_decisions = _read_csv(source_dir / "co2_s13_source_state_root_cause_decision.csv")
    source_points = _read_csv(source_dir / "co2_s13_point_common_bias_with_state.csv")
    ratio_summary = _read_csv(ratio_dir / "co2_s13_ratio_mapping_device_summary.csv")
    mapping_rows = _read_csv(ratio_dir / "co2_s13_point_mapping_audit.csv")
    target_summary = _read_csv(target_dir / "co2_s13_target_state_bridge_root_cause_summary.csv")
    correction_summary = _read_csv(correction_dir / "co2_s13_bridge_correction_device_recommendations.csv")
    repair_by_device = _read_csv(repair_dir / "co2_s13_source_state_repair_strategy_by_device.csv")
    repair_gate = _read_csv(repair_dir / "co2_s13_source_state_repair_write_gate.csv")
    common_points = _read_csv(error_dir / "co2_s13_error_common_mode_points.csv")

    ratio_by_device = _index_by_device(ratio_summary)
    target_by_device = _index_by_device(target_summary)
    correction_by_device = _index_by_device(correction_summary)
    repair_by_dev_group: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    candidate_strategy = str((repair_gate[0] if repair_gate else {}).get("candidate_strategy_label") or "")
    for row in repair_by_device:
        if candidate_strategy and str(row.get("strategy_label") or "") != candidate_strategy:
            continue
        device = _device_id(row.get("device_id"))
        if device:
            repair_by_dev_group[device].append(row)

    devices = sorted(set(ratio_by_device) | set(target_by_device) | set(correction_by_device) | set(repair_by_dev_group))
    device_decisions = [
        _device_decision(
            device=device,
            ratio_row=ratio_by_device.get(device, {}),
            bridge_row=target_by_device.get(device, {}),
            correction_row=correction_by_device.get(device, {}),
            repair_rows=repair_by_dev_group.get(device, []),
            acceptance_percent=acceptance_percent,
        )
        for device in devices
    ]

    common_by_point = _group_by_point(common_points)
    source_by_point = _group_by_point(source_points)
    mapping_by_point = _group_by_point(mapping_rows)
    points = sorted(set(common_by_point) | set(source_by_point) | set(mapping_by_point))
    point_decisions = [
        _point_decision(
            point=point,
            common_rows=common_by_point.get(point, []),
            source_rows=source_by_point.get(point, []),
            mapping_rows=mapping_by_point.get(point, []),
        )
        for point in points
    ]

    source_status = str((source_summary[0] if source_summary else {}).get("write_gate_status") or "")
    repair_status = str((repair_gate[0] if repair_gate else {}).get("status") or "")
    blocked = (
        source_status.startswith("blocked")
        or repair_status.startswith("blocked")
        or any(str(row.get("status")) != "review_possible" for row in device_decisions)
    )
    gate_status = "blocked_root_cause_not_closed" if blocked else "review_possible"
    action_plan = _action_plan(
        gate_status=gate_status,
        source_decisions=source_decisions,
        device_decisions=device_decisions,
        point_decisions=point_decisions,
    )
    run_summary = [
        {
            "created_at": _now(),
            "source_state_dir": str(source_dir.resolve()),
            "ratio_mapping_dir": str(ratio_dir.resolve()),
            "target_state_bridge_dir": str(target_dir.resolve()),
            "bridge_correction_dir": str(correction_dir.resolve()),
            "repair_fit_dir": str(repair_dir.resolve()),
            "error_root_cause_dir": str(error_dir.resolve()),
            "device_count": len(device_decisions),
            "point_count": len(point_decisions),
            "held_point_count": sum(
                1 for row in point_decisions if row.get("recommended_treatment") == "hold_as_diagnostic_until_bridge_evidence_passes"
            ),
            "blocked_device_count": sum(1 for row in device_decisions if str(row.get("status")) != "review_possible"),
            "source_state_status": source_status,
            "repair_fit_status": repair_status,
            "write_gate_status": gate_status,
            "acceptance_percent": acceptance_percent,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "uses_pressure_terms": False,
            "not_real_acceptance_evidence": True,
        }
    ]
    return {
        "run_summary": run_summary,
        "device_decisions": device_decisions,
        "point_decisions": point_decisions,
        "action_plan": action_plan,
    }


def _markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    summary = tables["run_summary"][0] if tables.get("run_summary") else {}
    lines: List[str] = [
        "# V1.5 CO2 S1/S3 根因收敛评审",
        "",
        "## 总结论",
        "",
        f"- 写入门禁：`{summary.get('write_gate_status', '')}`",
        f"- 设备数：{summary.get('device_count', '')}",
        f"- 需要 hold 的点位数：{summary.get('held_point_count', '')}",
        f"- 被阻断设备数：{summary.get('blocked_device_count', '')}",
        "",
        "物理结论：当前 CO2 主校准可以冻结压力项，但不能把源状态差异当作浓度系数误差吸收。"
        "同一温度组内的补点来源、压力状态离群和共同偏差未闭环前，S1/S3 不应写入。",
        "",
        "## 逐设备结论",
        "",
        "| 设备 | 状态 | S1/S3 最大相对误差 % | 简单桥接/S5 后 % | 阻断原因 |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for row in tables.get("device_decisions", []):
        lines.append(
            "| {device_id} | {status} | {s13} | {bridge} | {blockers} |".format(
                device_id=row.get("device_id", ""),
                status=row.get("status", ""),
                s13=_fmt(row.get("bridge_max_abs_relative_error_percent")),
                bridge=_fmt(row.get("simple_bridge_best_max_abs_relative_error_percent")),
                blockers=row.get("blockers", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 关键点位处理",
            "",
            "| 点位 | 处理 | 最大相对误差 % | 阻断证据 | 物理意义 |",
            "| --- | --- | ---: | --- | --- |",
        ]
    )
    for row in tables.get("point_decisions", []):
        if not row.get("blockers"):
            continue
        lines.append(
            "| {point} | {treatment} | {rel} | {blockers} | {meaning} |".format(
                point=row.get("point_identity", ""),
                treatment=row.get("recommended_treatment", ""),
                rel=_fmt(row.get("max_abs_relative_error_percent")),
                blockers=row.get("blockers", ""),
                meaning=row.get("physical_meaning", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 下一步动作",
            "",
            "| 优先级 | 动作 | 原因 | 物理意义 |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in tables.get("action_plan", []):
        lines.append(
            "| {priority} | {action} | {reason} | {physical_meaning} |".format(
                priority=row.get("priority", ""),
                action=row.get("action", ""),
                reason=row.get("reason", ""),
                physical_meaning=row.get("physical_meaning", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 边界",
            "",
            "- 本评审不开 COM、不控气路/水路、不写 SENCO。",
            "- S5/S6 是输出层修正，不能替代 S1/S3 主链路源状态闭环。",
            "- CO2 零气锚点和 H2O 干气锚点必须分开处理。",
        ]
    )
    return "\n".join(lines) + "\n"


def _fmt(value: Any, digits: int = 3) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}f}"


def write_co2_s13_root_cause_closure_review(
    *,
    source_state_dir: str | Path,
    ratio_mapping_dir: str | Path,
    target_state_bridge_dir: str | Path,
    bridge_correction_dir: str | Path,
    repair_fit_dir: str | Path,
    error_root_cause_dir: str | Path,
    output_dir: str | Path,
    acceptance_percent: float = 1.0,
) -> Dict[str, Path]:
    tables = build_co2_s13_root_cause_closure_review(
        source_state_dir=source_state_dir,
        ratio_mapping_dir=ratio_mapping_dir,
        target_state_bridge_dir=target_state_bridge_dir,
        bridge_correction_dir=bridge_correction_dir,
        repair_fit_dir=repair_fit_dir,
        error_root_cause_dir=error_root_cause_dir,
        acceptance_percent=acceptance_percent,
    )
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "run_summary": output / "co2_s13_root_cause_closure_run_summary.csv",
        "device_decisions": output / "co2_s13_root_cause_closure_device_decisions.csv",
        "point_decisions": output / "co2_s13_root_cause_closure_point_decisions.csv",
        "action_plan": output / "co2_s13_root_cause_closure_action_plan.csv",
        "metadata": output / "co2_s13_root_cause_closure_meta.json",
        "markdown": output / "co2_s13_root_cause_closure_review_zh.md",
    }
    for key in ("run_summary", "device_decisions", "point_decisions", "action_plan"):
        _write_csv(paths[key], tables[key])
    paths["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_root_cause_closure_review",
                "created_at": _now(),
                "inputs": {
                    "source_state_dir": str(Path(source_state_dir).resolve()),
                    "ratio_mapping_dir": str(Path(ratio_mapping_dir).resolve()),
                    "target_state_bridge_dir": str(Path(target_state_bridge_dir).resolve()),
                    "bridge_correction_dir": str(Path(bridge_correction_dir).resolve()),
                    "repair_fit_dir": str(Path(repair_fit_dir).resolve()),
                    "error_root_cause_dir": str(Path(error_root_cause_dir).resolve()),
                    "acceptance_percent": acceptance_percent,
                },
                "boundaries": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "not_real_acceptance_evidence": True,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8-sig",
    )
    paths["markdown"].write_text(_markdown(tables), encoding="utf-8-sig")
    return paths
