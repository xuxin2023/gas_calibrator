"""CO2 S1/S3 blocker closure action review.

This offline review consumes the root-cause closure package and turns each
blocked point into a concrete next action. It never opens COM ports, controls
routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence


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


def _tokens(value: Any) -> List[str]:
    return [item.strip() for item in str(value or "").split(";") if item.strip()]


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "None", "null"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _fmt(value: Any, digits: int = 3) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}f}"


def _point_temperature_group(point: str) -> str:
    if point.startswith("T") and "_" in point:
        return point.split("_", 1)[0]
    return ""


def _point_action(row: Mapping[str, Any]) -> Dict[str, Any]:
    point = str(row.get("point_identity") or "")
    treatment = str(row.get("recommended_treatment") or "")
    blockers = _tokens(row.get("blockers"))
    rel = row.get("max_abs_relative_error_percent") or ""

    if "supplement_source_bridge_not_proven" in blockers:
        action = "hold_until_source_bridge_evidence_or_rerun_main_source"
        fit_use = "do_not_use_in_formal_s13_until_bridge_passes"
        required = "核对气瓶证书值、气路标签、阀路径、露点、压力状态、ratio 门禁和运行来源是否可与主运行桥接。"
        meaning = "T30 补点来自不同运行状态；数据本身可能有效，但物理状态桥接前不能作为 A 级拟合证据。"
        priority = "P0"
    elif "pressure_state_outlier_review" in blockers:
        action = "hold_until_pressure_state_explained"
        fit_use = "do_not_add_pressure_term; hold point until open_flow_state_is_explained"
        required = "复核 COM22/PACE 压力轨迹、开放流通通大气证据、管路阻力和采样窗口附近的压力瞬态。"
        meaning = "CO2 拟合冻结压力项，但压力状态离群仍说明开放流通物理状态可能发生差异。"
        priority = "P0"
    elif "zero_anchor_value_review" in blockers:
        action = "review_zero_gas_co2_assigned_value_and_absolute_ppm_error"
        fit_use = "candidate_low_end_anchor_after_value_review"
        required = "给零气分配可追溯或有依据估算的 CO2 ppm；零点按绝对 ppm 评审，不按相对百分比放大判断。"
        meaning = "零气约束 S1/S3 截距，必须和 H2O 干气锚点分开，不能作为普通映射错误丢掉。"
        priority = "P1"
    elif "common_mode_bias" in blockers:
        action = "keep_for_model_boundary_review"
        fit_use = "keep_candidate_fit_point_not_auto_exclude"
        required = "比较模型结构、低端项和温度项；不能只因多台设备同向偏差就删除该点。"
        meaning = "多台设备同向残差更像模型边界或目标状态共性偏差，而不是单台分析仪故障。"
        priority = "P1"
    elif treatment == "keep_for_review_not_auto_exclude":
        action = "keep_with_standard_qc_review"
        fit_use = "candidate_fit_point"
        required = "保留常规 QC 证据。"
        meaning = "未发现必须阻断的物理状态问题。"
        priority = "P2"
    else:
        action = "review_manually"
        fit_use = "review_before_fit"
        required = "inspect root-cause closure row"
        meaning = "No automated closure rule matched this point."
        priority = "P2"

    return {
        "point_identity": point,
        "temperature_group": row.get("temperature_group") or _point_temperature_group(point),
        "current_treatment": treatment,
        "blockers": ";".join(blockers),
        "max_abs_relative_error_percent": rel,
        "closure_priority": priority,
        "closure_action": action,
        "next_fit_use": fit_use,
        "required_evidence": required,
        "physical_meaning": meaning,
    }


def _summary_rows(
    *,
    run_summary: Mapping[str, Any],
    device_rows: Sequence[Mapping[str, Any]],
    point_actions: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    counts = Counter(str(row.get("closure_action") or "") for row in point_actions)
    hard_actions = {
        "hold_until_source_bridge_evidence_or_rerun_main_source",
        "hold_until_pressure_state_explained",
    }
    hard_hold_count = sum(1 for row in point_actions if str(row.get("closure_action")) in hard_actions)
    zero_review_count = sum(1 for row in point_actions if str(row.get("closure_action")) == "review_zero_gas_co2_assigned_value_and_absolute_ppm_error")
    blocked_devices = [
        str(row.get("device_id") or "")
        for row in device_rows
        if str(row.get("status") or "") != "review_possible"
    ]
    can_refit_for_review = hard_hold_count == 0 and zero_review_count == 0
    return [
        {
            "metric": "write_gate_status",
            "value": run_summary.get("write_gate_status") or "unknown",
            "physical_meaning": "S1/S3 write remains blocked until source-state and low-end anchor closure passes.",
        },
        {
            "metric": "blocked_device_count",
            "value": len(blocked_devices),
            "physical_meaning": "Devices still blocked from controlled SENCO1/SENCO3 write.",
        },
        {
            "metric": "hard_hold_point_count",
            "value": hard_hold_count,
            "physical_meaning": "Points requiring bridge evidence or pressure-state explanation before entering formal fit.",
        },
        {
            "metric": "zero_anchor_review_point_count",
            "value": zero_review_count,
            "physical_meaning": "Zero-gas anchors requiring CO2 assigned-value review before final intercept fitting.",
        },
        {
            "metric": "closure_action_counts",
            "value": json.dumps(dict(sorted(counts.items())), ensure_ascii=False),
            "physical_meaning": "Distribution of closure actions for all reviewed points.",
        },
        {
            "metric": "can_refit_for_controlled_write",
            "value": str(can_refit_for_review).lower(),
            "physical_meaning": "True only when no hard-held source-state point and no unresolved zero-anchor review remains.",
        },
    ]


def _markdown(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    summary = {str(row.get("metric")): row.get("value") for row in tables.get("summary", [])}
    lines = [
        "# V1.5 CO2 S1/S3 阻断闭环行动评审",
        "",
        "## 总结论",
        "",
        f"- 当前写入门禁：`{summary.get('write_gate_status', '')}`",
        f"- 硬 hold 点数：{summary.get('hard_hold_point_count', '')}",
        f"- 零气锚点评审点数：{summary.get('zero_anchor_review_point_count', '')}",
        f"- 是否可进入受控写入前重拟合：`{summary.get('can_refit_for_controlled_write', '')}`",
        "",
        "物理结论：现在不是继续盲目改 S5，也不是直接写 S1/S3。应先把源状态差异、压力状态离群和零气低端锚点闭环，再重算主链路。",
        "",
        "## 点位行动清单",
        "",
        "| 点位 | 行动 | 拟合使用建议 | 需要补充的证据 |",
        "| --- | --- | --- | --- |",
    ]
    for row in tables.get("point_actions", []):
        if not row.get("blockers") and row.get("closure_priority") != "P0":
            continue
        lines.append(
            "| {point} | {action} | {fit_use} | {evidence} |".format(
                point=row.get("point_identity", ""),
                action=row.get("closure_action", ""),
                fit_use=row.get("next_fit_use", ""),
                evidence=row.get("required_evidence", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 边界",
            "",
            "- 本评审不打开 COM，不控制气路/水路/PACE，不写 SENCO。",
            "- CO2 零气锚点按 CO2 低端截距处理，H2O 干气锚点按低水汽锚点处理，两者不能混用。",
            "- 当前大气压开放流通 CO2 主拟合仍不引入压力项；压力异常只作为物理状态证据。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_co2_s13_blocker_closure_action_review(
    *,
    root_cause_closure_dir: str | Path,
    output_dir: str | Path,
) -> Dict[str, Path]:
    source = Path(root_cause_closure_dir).resolve()
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)

    run_rows = _read_csv(source / "co2_s13_root_cause_closure_run_summary.csv")
    device_rows = _read_csv(source / "co2_s13_root_cause_closure_device_decisions.csv")
    point_rows = _read_csv(source / "co2_s13_root_cause_closure_point_decisions.csv")
    run_summary = run_rows[0] if run_rows else {}
    point_actions = [_point_action(row) for row in point_rows]
    summary_rows = _summary_rows(
        run_summary=run_summary,
        device_rows=device_rows,
        point_actions=point_actions,
    )
    tables = {
        "summary": summary_rows,
        "point_actions": point_actions,
    }

    outputs = {
        "summary": output / "co2_s13_blocker_closure_action_summary.csv",
        "point_actions": output / "co2_s13_blocker_closure_point_actions.csv",
        "markdown": output / "co2_s13_blocker_closure_action_review_zh.md",
        "metadata": output / "co2_s13_blocker_closure_action_meta.json",
    }
    _write_csv(outputs["summary"], summary_rows)
    _write_csv(outputs["point_actions"], point_actions)
    outputs["markdown"].write_text(_markdown(tables), encoding="utf-8-sig")
    outputs["metadata"].write_text(
        json.dumps(
            {
                "created_at": _now(),
                "tool": "co2_s13_blocker_closure_action_review",
                "root_cause_closure_dir": str(source),
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
        encoding="utf-8",
    )
    return outputs
