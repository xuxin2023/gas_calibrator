"""Offline CO2 S1/S3 source-state repair fit review for V1.5.

This layer does not refit raw evidence itself. It collects already-generated
multi-strategy fit reviews, S5 trim reviews, enhanced diagnostic reviews, and
source-state audit results into one no-write release decision. It never opens
COM ports, controls gas/water routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


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


def _fmt(value: Any, digits: int = 3) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _strategy_label(path: Path, explicit_label: str | None = None) -> str:
    if explicit_label:
        return explicit_label
    name = path.name
    if name.startswith("co2_s13_multistrategy_"):
        name = name.removeprefix("co2_s13_multistrategy_")
    return name


def _parse_strategy_dir(item: str | Path | tuple[str, str | Path]) -> tuple[str, Path]:
    if isinstance(item, tuple):
        return str(item[0]), Path(item[1])
    text = str(item)
    if "=" in text:
        label, raw = text.split("=", 1)
        return label.strip(), Path(raw.strip())
    path = Path(text)
    return _strategy_label(path), path


def _read_meta(path: Path) -> Dict[str, Any]:
    target = path / "co2_s13_multistrategy_meta.json"
    if not target.exists():
        return {}
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _max_abs_relative(rows: Sequence[Mapping[str, Any]], field: str = "relative_error_percent") -> tuple[Optional[float], str]:
    worst: Optional[float] = None
    worst_point = ""
    for row in rows:
        rel = _safe_float(row.get(field))
        if rel is None:
            continue
        abs_rel = abs(float(rel))
        if worst is None or abs_rel > worst:
            worst = abs_rel
            worst_point = str(row.get("point_identity") or row.get("s5_worst_point_identity") or "")
    return worst, worst_point


def _held_points_from_plan(path: str | Path | None) -> tuple[List[str], List[Dict[str, Any]]]:
    rows = _read_csv(path)
    held: List[str] = []
    for row in rows:
        point = str(row.get("point_identity") or "").strip()
        if point:
            held.append(point)
    return held, rows


def _strategy_decision(
    *,
    max_s1s3: Optional[float],
    max_s5: Optional[float],
    acceptance_percent: float,
    held_count: int,
    diagnostic_only: bool = False,
) -> str:
    if diagnostic_only:
        return "diagnostic_only_not_writeable"
    if max_s1s3 is not None and max_s1s3 <= acceptance_percent and held_count == 0:
        return "s1s3_candidate_ready_for_formal_review"
    if max_s1s3 is not None and max_s1s3 <= acceptance_percent:
        return "s1s3_candidate_requires_held_point_traceability_review"
    if max_s5 is not None and max_s5 <= acceptance_percent:
        return "s5_can_fit_output_but_main_chain_still_not_released"
    return "blocked_no_writeable_s1s3_or_s5_path"


def _summarize_strategy_dir(
    *,
    label: str,
    path: Path,
    acceptance_percent: float,
) -> tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    best_rows = _read_csv(path / "co2_s13_multistrategy_best_by_device.csv")
    s5_rows = _read_csv(path / "co2_s13_multistrategy_s5_best_by_device.csv")
    best_residuals = _read_csv(path / "co2_s13_multistrategy_best_residuals.csv")
    s5_residuals = _read_csv(path / "co2_s13_multistrategy_s5_best_residuals.csv")
    meta = _read_meta(path)
    treatment_path = (
        meta.get("inputs", {}).get("fit_point_treatment_plan_csv")
        if isinstance(meta.get("inputs"), dict)
        else ""
    )
    held_points, treatment_rows = _held_points_from_plan(treatment_path)

    residuals_by_device: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in best_residuals:
        device = _device_id(row.get("device_id"))
        if device:
            residuals_by_device[device].append(row)

    s5_residuals_by_device: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in s5_residuals:
        device = _device_id(row.get("device_id"))
        if device:
            s5_residuals_by_device[device].append(row)

    s5_by_device = {_device_id(row.get("device_id")): row for row in s5_rows}
    by_device: List[Dict[str, Any]] = []
    s1s3_values: List[float] = []
    s5_values: List[float] = []
    for row in best_rows:
        device = _device_id(row.get("device_id"))
        s1s3 = _safe_float(row.get("max_abs_relative_error_percent"))
        if s1s3 is not None:
            s1s3_values.append(float(s1s3))
        residual_worst, residual_worst_point = _max_abs_relative(residuals_by_device.get(device, []))
        s5 = s5_by_device.get(device, {})
        s5_max = _safe_float(s5.get("s5_max_abs_relative_error_percent"))
        if s5_max is not None:
            s5_values.append(float(s5_max))
        s5_worst, s5_worst_point = _max_abs_relative(
            s5_residuals_by_device.get(device, []),
            field="s5_relative_error_percent",
        )
        by_device.append(
            {
                "strategy_label": label,
                "device_id": device,
                "s1s3_strategy_profile_id": row.get("strategy_profile_id", ""),
                "s1s3_objective_id": row.get("objective_id", ""),
                "s1s3_zero_offset_ppm": row.get("zero_offset_ppm", ""),
                "s1s3_fit_point_count": row.get("fit_point_count", ""),
                "s1s3_max_abs_relative_error_percent": s1s3 if s1s3 is not None else "",
                "s1s3_low_end_max_abs_relative_error_percent": row.get(
                    "low_end_max_abs_relative_error_percent",
                    "",
                ),
                "s1s3_worst_point_identity": residual_worst_point,
                "s1s3_worst_relative_error_percent": residual_worst if residual_worst is not None else "",
                "s1_payload_scientific": row.get("s1_payload_scientific", ""),
                "s3_payload_scientific": row.get("s3_payload_scientific", ""),
                "s5_C0": s5.get("s5_C0", ""),
                "s5_C1": s5.get("s5_C1", ""),
                "s5_command_preview": s5.get("s5_command_preview", ""),
                "s5_max_abs_relative_error_percent": s5_max if s5_max is not None else "",
                "s5_low_end_max_abs_relative_error_percent": s5.get(
                    "s5_low_end_max_abs_relative_error_percent",
                    "",
                ),
                "s5_worst_point_identity": s5_worst_point or s5.get("s5_worst_point_identity", ""),
                "s5_worst_relative_error_percent": s5_worst if s5_worst is not None else "",
                "held_point_count": len(held_points),
                "held_points": ";".join(held_points),
                "uses_pressure_terms": row.get("uses_pressure_terms", ""),
                "writes_coefficients": False,
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
            }
        )

    max_s1s3 = max(s1s3_values) if s1s3_values else None
    max_s5 = max(s5_values) if s5_values else None
    summary = {
        "strategy_label": label,
        "source_dir": str(path.resolve()),
        "fit_point_treatment_plan_csv": str(Path(treatment_path).resolve()) if treatment_path else "",
        "held_point_count": len(held_points),
        "held_points": ";".join(held_points),
        "device_count": len(by_device),
        "s1s3_max_abs_relative_error_percent_all_devices": max_s1s3 if max_s1s3 is not None else "",
        "s1s3_mean_max_abs_relative_error_percent": (
            sum(s1s3_values) / len(s1s3_values) if s1s3_values else ""
        ),
        "s1s3_devices_within_acceptance": sum(1 for value in s1s3_values if value <= acceptance_percent),
        "s5_max_abs_relative_error_percent_all_devices": max_s5 if max_s5 is not None else "",
        "s5_mean_max_abs_relative_error_percent": sum(s5_values) / len(s5_values) if s5_values else "",
        "s5_devices_within_acceptance": sum(1 for value in s5_values if value <= acceptance_percent),
        "acceptance_percent": acceptance_percent,
        "decision": _strategy_decision(
            max_s1s3=max_s1s3,
            max_s5=max_s5,
            acceptance_percent=acceptance_percent,
            held_count=len(held_points),
        ),
        "physical_meaning": (
            "该策略仍是离线审计；hold 点必须有源状态、压力状态、阀路或目标映射证据，不能静默删除。"
        ),
        "writes_coefficients": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
    }
    for row in treatment_rows:
        row["strategy_label"] = label
        row["treatment_plan_csv"] = str(Path(treatment_path).resolve()) if treatment_path else ""
    return summary, by_device, treatment_rows


def _summarize_enhanced_dir(
    *,
    path: Path,
    acceptance_percent: float,
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows = _read_csv(path / "co2_s13_enhanced_capacity_best_by_device_no_s5.csv")
    values: List[float] = []
    by_device: List[Dict[str, Any]] = []
    for row in rows:
        value = _safe_float(row.get("max_abs_relative_error_percent"))
        if value is not None:
            values.append(float(value))
        by_device.append(
            {
                "strategy_label": "enhanced_diagnostic_capacity",
                "device_id": _device_id(row.get("device_id")),
                "s1s3_strategy_profile_id": row.get("structure_id", ""),
                "s1s3_objective_id": row.get("objective_id", ""),
                "s1s3_zero_offset_ppm": row.get("zero_offset_ppm", ""),
                "s1s3_fit_point_count": row.get("fit_point_count", ""),
                "s1s3_max_abs_relative_error_percent": value if value is not None else "",
                "s1s3_low_end_max_abs_relative_error_percent": row.get(
                    "low_end_max_abs_relative_error_percent",
                    "",
                ),
                "s1s3_worst_point_identity": "",
                "s1s3_worst_relative_error_percent": "",
                "s1_payload_scientific": "",
                "s3_payload_scientific": "",
                "s5_C0": "",
                "s5_C1": "",
                "s5_command_preview": "",
                "s5_max_abs_relative_error_percent": "",
                "s5_low_end_max_abs_relative_error_percent": "",
                "s5_worst_point_identity": "",
                "s5_worst_relative_error_percent": "",
                "held_point_count": "",
                "held_points": "",
                "uses_pressure_terms": False,
                "diagnostic_only": True,
                "writes_coefficients": False,
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "physical_meaning": row.get("physical_meaning", ""),
            }
        )
    max_value = max(values) if values else None
    summary = {
        "strategy_label": "enhanced_diagnostic_capacity",
        "source_dir": str(path.resolve()),
        "fit_point_treatment_plan_csv": "",
        "held_point_count": "",
        "held_points": "",
        "device_count": len(by_device),
        "s1s3_max_abs_relative_error_percent_all_devices": max_value if max_value is not None else "",
        "s1s3_mean_max_abs_relative_error_percent": sum(values) / len(values) if values else "",
        "s1s3_devices_within_acceptance": sum(1 for value in values if value <= acceptance_percent),
        "s5_max_abs_relative_error_percent_all_devices": "",
        "s5_mean_max_abs_relative_error_percent": "",
        "s5_devices_within_acceptance": "",
        "acceptance_percent": acceptance_percent,
        "decision": "diagnostic_only_not_writeable",
        "physical_meaning": (
            "增强模型只用于判断当前固件合同容量是否不足；其温度组 offset/slope 项不能直接写入 SENCO1/SENCO3。"
        ),
        "writes_coefficients": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
    }
    return summary, by_device


def _source_gate_status(source_state_audit_dir: Path) -> tuple[str, str, List[Dict[str, Any]], Dict[str, Any]]:
    run_summary = _read_csv(source_state_audit_dir / "co2_s13_source_state_run_summary.csv")
    decisions = _read_csv(source_state_audit_dir / "co2_s13_source_state_root_cause_decision.csv")
    first = run_summary[0] if run_summary else {}
    status = str(first.get("write_gate_status") or "missing_source_state_gate")
    topics = str(first.get("write_gate_blocker_topics") or "")
    if not topics and decisions:
        topics = ";".join(str(row.get("topic") or "") for row in decisions if row.get("topic"))
    return status, topics, decisions, first


def _choose_best_strategy(strategy_rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    candidates = [row for row in strategy_rows if row.get("strategy_label") != "enhanced_diagnostic_capacity"]
    if not candidates:
        return None

    def key(row: Mapping[str, Any]) -> tuple[float, float, float]:
        s5 = _safe_float(row.get("s5_max_abs_relative_error_percent_all_devices"))
        s1 = _safe_float(row.get("s1s3_max_abs_relative_error_percent_all_devices"))
        held = _safe_float(row.get("held_point_count"))
        return (
            s5 if s5 is not None else float("inf"),
            s1 if s1 is not None else float("inf"),
            held if held is not None else 999.0,
        )

    return min(candidates, key=key)


def build_co2_s13_source_state_repair_fit_review(
    *,
    source_state_audit_dir: str | Path,
    strategy_dirs: Sequence[str | Path | tuple[str, str | Path]],
    enhanced_dir: str | Path | None = None,
    acceptance_percent: float = 1.0,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build no-write source-state repair fit review tables."""

    source_dir = Path(source_state_audit_dir)
    source_status, source_topics, source_decisions, source_summary = _source_gate_status(source_dir)
    strategy_summary: List[Dict[str, Any]] = []
    by_device: List[Dict[str, Any]] = []
    treatments: List[Dict[str, Any]] = []

    for item in strategy_dirs:
        label, directory = _parse_strategy_dir(item)
        summary, device_rows, treatment_rows = _summarize_strategy_dir(
            label=label,
            path=directory,
            acceptance_percent=float(acceptance_percent),
        )
        strategy_summary.append(summary)
        by_device.extend(device_rows)
        treatments.extend(treatment_rows)

    if enhanced_dir:
        enhanced_summary, enhanced_by_device = _summarize_enhanced_dir(
            path=Path(enhanced_dir),
            acceptance_percent=float(acceptance_percent),
        )
        strategy_summary.append(enhanced_summary)
        by_device.extend(enhanced_by_device)

    best = _choose_best_strategy(strategy_summary)
    best_s1 = _safe_float(best.get("s1s3_max_abs_relative_error_percent_all_devices")) if best else None
    best_s5 = _safe_float(best.get("s5_max_abs_relative_error_percent_all_devices")) if best else None
    best_label = str(best.get("strategy_label") or "") if best else ""
    if str(source_status).startswith("blocked"):
        status = "blocked_source_state_discontinuity"
        reason = source_topics or source_status
        next_action = "repair_source_state_or_collect_bridge_evidence_before_senco13_write"
    elif best_s1 is not None and best_s1 <= acceptance_percent:
        status = "ready_for_s1s3_write_review"
        reason = "s1s3_candidate_within_acceptance"
        next_action = "prepare_controlled_senco13_write_pack_then_reverify_no_s5"
    elif best_s5 is not None and best_s5 <= acceptance_percent:
        status = "blocked_main_chain_not_ready_even_if_s5_can_trim"
        reason = "s5_output_trim_must_not_hide_s1s3_source_state_error"
        next_action = "fix_s1s3_main_chain_or_formalize_bridge_before_s5"
    else:
        status = "blocked_no_writeable_fit_strategy"
        reason = "no_s1s3_or_s1s3_plus_s5_strategy_meets_acceptance"
        next_action = "supplement_or_repair_state_inconsistent_points"

    write_gate = [
        {
            "gate_id": "co2_s13_source_state_repair_fit_gate",
            "status": status,
            "reason": reason,
            "candidate_strategy_label": best_label,
            "candidate_s1s3_max_abs_relative_error_percent": best_s1 if best_s1 is not None else "",
            "candidate_s5_max_abs_relative_error_percent": best_s5 if best_s5 is not None else "",
            "acceptance_percent": float(acceptance_percent),
            "source_state_gate_status": source_status,
            "source_state_blocker_topics": source_topics,
            "physical_meaning": (
                "CO2 主校准可以冻结压力项，但不能忽略源状态；同一温度组不同运行来源、压力状态离群和非仿射锯齿偏差会污染 S1/S3。"
            ),
            "next_action": next_action,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        }
    ]

    run_summary = [
        {
            "created_at": _now(),
            "source_state_audit_dir": str(source_dir.resolve()),
            "source_state_gate_status": source_status,
            "source_state_blocker_topics": source_topics,
            "source_state_fit_row_count": source_summary.get("fit_row_count", ""),
            "strategy_count": len(strategy_summary),
            "device_row_count": len(by_device),
            "treatment_row_count": len(treatments),
            "write_gate_status": status,
            "candidate_write_allowed": status.startswith("ready"),
            "acceptance_percent": float(acceptance_percent),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        }
    ]

    return {
        "run_summary": run_summary,
        "strategy_summary": strategy_summary,
        "strategy_by_device": by_device,
        "point_treatment": treatments,
        "source_state_decisions": source_decisions,
        "write_gate": write_gate,
    }


def _render_markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    summary = tables.get("run_summary", [{}])[0]
    gate = tables.get("write_gate", [{}])[0]
    lines = [
        "# V1.5 CO2 S1/S3 源状态修复版拟合评审",
        "",
        "## 结论",
        "",
        f"- 写入门禁：`{gate.get('status', '')}`",
        f"- 候选策略：`{gate.get('candidate_strategy_label', '')}`",
        f"- 候选 S1/S3 最大相对误差：{_fmt(gate.get('candidate_s1s3_max_abs_relative_error_percent'))} %",
        f"- 候选 S1/S3+S5 最大相对误差：{_fmt(gate.get('candidate_s5_max_abs_relative_error_percent'))} %",
        f"- 阻断原因：{gate.get('reason', '')}",
        "",
        "物理判断：当前问题不是单纯多加一个压力项或输出层 S5 就能解决。正式 CO2 主校准在当前大气压开放流通下应冻结压力项，但仍必须保证同一温度组的气源状态、阀路状态、露点/压力状态和目标映射一致。",
        "",
        "## 各策略对比",
        "",
        "| 策略 | hold 点数 | 设备数 | S1/S3 最大相对误差 % | S5 后最大相对误差 % | 判定 |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in tables.get("strategy_summary", []):
        lines.append(
            "| {label} | {held} | {devices} | {s1} | {s5} | {decision} |".format(
                label=row.get("strategy_label", ""),
                held=row.get("held_point_count", ""),
                devices=row.get("device_count", ""),
                s1=_fmt(row.get("s1s3_max_abs_relative_error_percent_all_devices")),
                s5=_fmt(row.get("s5_max_abs_relative_error_percent_all_devices")),
                decision=row.get("decision", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 逐设备候选误差",
            "",
            "| 策略 | 设备 ID | S1/S3 最大相对误差 % | S1/S3 最差点 | S5 后最大相对误差 % | S5 最差点 |",
            "| --- | --- | ---: | --- | ---: | --- |",
        ]
    )
    for row in tables.get("strategy_by_device", []):
        lines.append(
            "| {strategy} | {device} | {s1} | {p1} | {s5} | {p5} |".format(
                strategy=row.get("strategy_label", ""),
                device=row.get("device_id", ""),
                s1=_fmt(row.get("s1s3_max_abs_relative_error_percent")),
                p1=row.get("s1s3_worst_point_identity", ""),
                s5=_fmt(row.get("s5_max_abs_relative_error_percent")),
                p5=row.get("s5_worst_point_identity", ""),
            )
        )
    if tables.get("point_treatment"):
        lines.extend(
            [
                "",
                "## hold 点处理依据",
                "",
                "| 策略 | 点位 | 处理 | 物理依据 |",
                "| --- | --- | --- | --- |",
            ]
        )
        for row in tables["point_treatment"]:
            lines.append(
                "| {strategy} | {point} | {policy} | {basis} |".format(
                    strategy=row.get("strategy_label", ""),
                    point=row.get("point_identity", ""),
                    policy=row.get("fit_policy", ""),
                    basis=row.get("exclusion_basis", ""),
                )
            )
    lines.extend(
        [
            "",
            "## 源状态审计要点",
            "",
            "| 优先级 | 主题 | 发现 | 下一步 |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in tables.get("source_state_decisions", []):
        lines.append(
            "| {priority} | {topic} | {finding} | {action} |".format(
                priority=row.get("priority", ""),
                topic=row.get("topic", ""),
                finding=row.get("finding", ""),
                action=row.get("action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 边界",
            "",
            f"- 证据来源目录：`{summary.get('source_state_audit_dir', '')}`",
            "- 本评审不开串口、不控气路/水路、不写 SENCO。",
            "- 增强模型只用于诊断固件合同容量，不是当前可写 S1/S3 合同。",
            "- S5 是最终显示层仿射修正，不能用来掩盖 S1/S3 主链路源状态问题。",
            "",
        ]
    )
    return "\n".join(lines)


def write_co2_s13_source_state_repair_fit_review(
    *,
    source_state_audit_dir: str | Path,
    strategy_dirs: Sequence[str | Path | tuple[str, str | Path]],
    output_dir: str | Path,
    enhanced_dir: str | Path | None = None,
    acceptance_percent: float = 1.0,
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_s13_source_state_repair_fit_review(
        source_state_audit_dir=source_state_audit_dir,
        strategy_dirs=strategy_dirs,
        enhanced_dir=enhanced_dir,
        acceptance_percent=float(acceptance_percent),
    )
    outputs = {
        "run_summary": output / "co2_s13_source_state_repair_run_summary.csv",
        "strategy_summary": output / "co2_s13_source_state_repair_strategy_summary.csv",
        "strategy_by_device": output / "co2_s13_source_state_repair_strategy_by_device.csv",
        "point_treatment": output / "co2_s13_source_state_repair_point_treatment.csv",
        "source_state_decisions": output / "co2_s13_source_state_repair_decisions.csv",
        "write_gate": output / "co2_s13_source_state_repair_write_gate.csv",
        "metadata": output / "co2_s13_source_state_repair_meta.json",
        "markdown": output / "co2_s13_source_state_repair_review_zh.md",
    }
    for key in (
        "run_summary",
        "strategy_summary",
        "strategy_by_device",
        "point_treatment",
        "source_state_decisions",
        "write_gate",
    ):
        _write_csv(outputs[key], tables[key])
    outputs["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_s13_source_state_repair_fit_review",
                "created_at": _now(),
                "inputs": {
                    "source_state_audit_dir": str(Path(source_state_audit_dir).resolve()),
                    "strategy_dirs": [str(_parse_strategy_dir(item)[1].resolve()) for item in strategy_dirs],
                    "enhanced_dir": str(Path(enhanced_dir).resolve()) if enhanced_dir else "",
                    "acceptance_percent": float(acceptance_percent),
                },
                "boundary": {
                    "opens_com_ports": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "uses_pressure_terms": False,
                    "uses_s5_output_trim": "review_only_no_write",
                    "not_real_acceptance_evidence": True,
                },
                "outputs": {key: str(path) for key, path in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs["markdown"].write_text("\ufeff" + _render_markdown(tables), encoding="utf-8")
    return outputs
