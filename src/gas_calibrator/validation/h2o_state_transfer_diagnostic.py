"""Offline H2O state-transfer diagnostic for V1.5 SENCO2/SENCO4 candidates.

This module reads already produced evidence tables and writes diagnostic
artifacts. It never opens COM ports, controls gas/water routes, or writes
SENCO coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence


@dataclass(frozen=True)
class H2OStateTransferDiagnosticInputs:
    candidate_device_policy_csv: Path
    state_transfer_csv: Path
    target_device_ids: tuple[str, ...] = ("084",)
    raw_excess_limit_mmol: float = 0.1
    post_s6_relative_limit_pct: float = 2.0


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("GA"):
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _float(value: Any) -> float | None:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _read_csv(path: str | Path | None) -> List[Dict[str, Any]]:
    if not path:
        return []
    source = Path(path)
    if not source.exists():
        return []
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: List[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])
    return path


def _policy_by_device(path: str | Path) -> Dict[str, Mapping[str, Any]]:
    out: Dict[str, Mapping[str, Any]] = {}
    for row in _read_csv(path):
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        if device:
            out[device] = row
    return out


def _state_rows_by_device(path: str | Path) -> Dict[str, List[Mapping[str, Any]]]:
    out: Dict[str, List[Mapping[str, Any]]] = {}
    for row in _read_csv(path):
        device = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if device:
            out.setdefault(device, []).append(row)
    return out


def _absmax(rows: Sequence[Mapping[str, Any]], field: str) -> float | None:
    values = [_float(row.get(field)) for row in rows]
    finite = [abs(value) for value in values if value is not None]
    return max(finite) if finite else None


def _worst_row(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    if not rows:
        return {}
    return max(
        rows,
        key=lambda row: abs(_float(row.get("raw_replay_delta_minus_reference_delta_mmol")) or 0.0),
    )


def _decision(
    *,
    raw_excess: float | None,
    post_s6_relative: float | None,
    raw_limit: float,
    post_limit: float,
) -> tuple[str, str]:
    failures: List[str] = []
    if raw_excess is None:
        failures.append("missing_senco24_raw_state_transfer")
    elif raw_excess > raw_limit:
        failures.append("senco24_raw_state_transfer_excess_shift")
    if post_s6_relative is None:
        failures.append("missing_post_s6_state_transfer_error")
    elif post_s6_relative > post_limit:
        failures.append("post_s6_state_transfer_relative_error_exceeds_limit")
    if failures:
        return "blocked_h2o_write_requires_special_diagnostic", ";".join(failures)
    return "state_transfer_passed_can_continue_normal_review", ""


def build_h2o_state_transfer_diagnostic_tables(
    inputs: H2OStateTransferDiagnosticInputs,
) -> Dict[str, List[Dict[str, Any]]]:
    policies = _policy_by_device(inputs.candidate_device_policy_csv)
    transfer = _state_rows_by_device(inputs.state_transfer_csv)
    targets = tuple(_device_id(device) for device in inputs.target_device_ids)
    decision_rows: List[Dict[str, Any]] = []
    point_rows: List[Dict[str, Any]] = []
    plan_rows: List[Dict[str, Any]] = []

    for device in targets:
        rows = transfer.get(device, [])
        policy = policies.get(device, {})
        max_raw = _absmax(rows, "raw_replay_delta_minus_reference_delta_mmol")
        max_post = _absmax(rows, "post_existing_s6_abs_rel_pct")
        max_ratio = _absmax(rows, "h2o_ratio_f_mean_delta_post_minus_s24")
        max_temp = _absmax(rows, "chamber_temp_c_mean_delta_post_minus_s24")
        max_ref = _absmax(rows, "live_reference_h2o_mmol_delta_post_minus_s24")
        worst = _worst_row(rows)
        decision, blockers = _decision(
            raw_excess=max_raw,
            post_s6_relative=max_post,
            raw_limit=float(inputs.raw_excess_limit_mmol),
            post_limit=float(inputs.post_s6_relative_limit_pct),
        )

        if decision.startswith("blocked"):
            next_action = (
                "暂停084水路S2/S4/S6写入；做084专项干气、低湿、高湿、重复点诊断；"
                "检查H2O ratio/raw ratio/ref_signal/H2O signal/状态寄存器/腔体温度后再决定维修或重采。"
            )
            physical_interpretation = (
                "S2/S4主水路曲面在复验状态下的原始预测变化明显大于参考露点和压力可解释的真实H2O变化，"
                "说明主响应不可迁移；S6输出层修正不能掩盖这种主曲面失稳。"
            )
        else:
            next_action = "允许继续常规候选系数评审，但仍需写入前GETCO快照和写后H2O复验。"
            physical_interpretation = "S2/S4原始响应在校准和复验状态之间可迁移，S6只作为输出层小修正。"

        decision_rows.append(
            {
                "device_id": device,
                "candidate_status_before_transfer_gate": policy.get("candidate_status", ""),
                "state_transfer_decision": decision,
                "blockers": blockers,
                "point_count": len(rows),
                "max_abs_raw_excess_shift_mmol": max_raw if max_raw is not None else "",
                "raw_excess_limit_mmol": float(inputs.raw_excess_limit_mmol),
                "max_abs_post_s6_relative_error_pct": max_post if max_post is not None else "",
                "post_s6_relative_limit_pct": float(inputs.post_s6_relative_limit_pct),
                "max_abs_ratio_delta": max_ratio if max_ratio is not None else "",
                "max_abs_chamber_temp_delta_c": max_temp if max_temp is not None else "",
                "max_abs_reference_h2o_delta_mmol": max_ref if max_ref is not None else "",
                "worst_point_id": worst.get("point_id", ""),
                "physical_interpretation": physical_interpretation,
                "next_safe_action": next_action,
                "auto_write_allowed": False,
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            }
        )

        for row in rows:
            raw_excess = _float(row.get("raw_replay_delta_minus_reference_delta_mmol"))
            post_rel = _float(row.get("post_existing_s6_abs_rel_pct"))
            point_decision, point_blockers = _decision(
                raw_excess=abs(raw_excess) if raw_excess is not None else None,
                post_s6_relative=abs(post_rel) if post_rel is not None else None,
                raw_limit=float(inputs.raw_excess_limit_mmol),
                post_limit=float(inputs.post_s6_relative_limit_pct),
            )
            point_rows.append(
                {
                    "device_id": device,
                    "point_id": row.get("point_id", ""),
                    "point_label": row.get("point_label_s24", ""),
                    "h2o_ratio_delta_post_minus_s24": row.get("h2o_ratio_f_mean_delta_post_minus_s24", ""),
                    "chamber_temp_delta_c": row.get("chamber_temp_c_mean_delta_post_minus_s24", ""),
                    "dewpoint_delta_c": row.get("dewpoint_live_c_mean_delta_post_minus_s24", ""),
                    "pressure_delta_hpa": row.get("pressure_gauge_hpa_mean_delta_post_minus_s24", ""),
                    "reference_h2o_delta_mmol": row.get("live_reference_h2o_mmol_delta_post_minus_s24", ""),
                    "senco24_replay_delta_mmol": row.get("senco24_replay_h2o_mmol_mean_delta_post_minus_s24", ""),
                    "raw_excess_shift_mmol": row.get("raw_replay_delta_minus_reference_delta_mmol", ""),
                    "post_s6_error_mmol": row.get("post_existing_s6_error_mmol", ""),
                    "post_s6_abs_rel_pct": row.get("post_existing_s6_abs_rel_pct", ""),
                    "point_state_transfer_decision": point_decision,
                    "point_blockers": point_blockers,
                }
            )

        plan_steps = [
            (
                1,
                "identity_snapshot",
                "读取并记录设备ID、GETCO2/4/6/7/8、状态寄存器；确认没有把其它串口设备当作084。",
            ),
            (
                2,
                "dry_gas_repeatability",
                "通干气/N2，持续开放流通，记录H2O ratio/raw ratio/ref_signal/H2O signal/露点/压力/温度，判断低水锚是否稳定。",
            ),
            (
                3,
                "low_humidity_repeatability",
                "跑低湿点，至少两段独立稳定窗口；比较ratio变化与露点换算H2O变化是否同向且比例合理。",
            ),
            (
                4,
                "high_humidity_repeatability",
                "跑高湿点，检查H2O signal、ref_signal、状态寄存器是否出现饱和、光功率或信号异常。",
            ),
            (
                5,
                "same_point_return_check",
                "回到一个已测湿度点重复采样；若参考露点接近但S2/S4原始回放差异仍大，判定设备水路响应异常。",
            ),
        ]
        for sequence, phase, action in plan_steps:
            plan_rows.append(
                {
                    "device_id": device,
                    "sequence": sequence,
                    "phase": phase,
                    "action": action,
                    "write_senco_allowed": False,
                    "route_control_required_later": phase != "identity_snapshot",
                    "required_signals": (
                        "H2O ratio filtered/raw; ref_signal; h2o_signal; chamber_temp; case_temp; "
                        "status_register; dewpoint; COM22 pressure"
                    ),
                }
            )

    return {
        "h2o_state_transfer_device_decision": decision_rows,
        "h2o_state_transfer_point_evidence": point_rows,
        "h2o_state_transfer_minimal_diagnostic_plan": plan_rows,
    }


def write_h2o_state_transfer_diagnostic_report(
    *, inputs: H2OStateTransferDiagnosticInputs, output_dir: str | Path
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_h2o_state_transfer_diagnostic_tables(inputs)
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        outputs[name] = _write_csv(output / f"{name}.csv", rows)
    meta = {
        "tool": "h2o_state_transfer_diagnostic",
        "created_at": _now(),
        "inputs": {
            "candidate_device_policy_csv": str(inputs.candidate_device_policy_csv.resolve()),
            "state_transfer_csv": str(inputs.state_transfer_csv.resolve()),
            "target_device_ids": list(inputs.target_device_ids),
            "raw_excess_limit_mmol": float(inputs.raw_excess_limit_mmol),
            "post_s6_relative_limit_pct": float(inputs.post_s6_relative_limit_pct),
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    meta_path = output / "h2o_state_transfer_diagnostic_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["meta"] = meta_path
    outputs["markdown"] = _write_markdown(output / "h2o_state_transfer_diagnostic_zh.md", tables)
    return outputs


def _fmt(value: Any, digits: int = 6) -> str:
    number = _float(value)
    if number is None:
        return str(value or "")
    return f"{number:.{digits}f}".rstrip("0").rstrip(".")


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    decisions = list(tables.get("h2o_state_transfer_device_decision", []))
    points = list(tables.get("h2o_state_transfer_point_evidence", []))
    lines = [
        "# V1.5 H2O 状态迁移专项诊断报告",
        "",
        "## 边界",
        "",
        "- 本报告只使用既有离线证据，不打开 COM。",
        "- 本报告不控制气路、水路、PACE、湿度发生器或阀门。",
        "- 本报告不写入 SENCO。",
        "",
        "## 结论",
        "",
    ]
    for row in decisions:
        device = row.get("device_id", "")
        decision = row.get("state_transfer_decision", "")
        lines.extend(
            [
                f"- 设备 `{device}`：`{decision}`。",
                f"- 最大 S2/S4 原始额外漂移：`{_fmt(row.get('max_abs_raw_excess_shift_mmol'))} mmol/mol`，限值 `{_fmt(row.get('raw_excess_limit_mmol'))} mmol/mol`。",
                f"- S6 后最大相对误差：`{_fmt(row.get('max_abs_post_s6_relative_error_pct'))}%`，限值 `{_fmt(row.get('post_s6_relative_limit_pct'))}%`。",
                f"- 最差点：`{row.get('worst_point_id', '')}`。",
                f"- 物理解释：{row.get('physical_interpretation', '')}",
                f"- 下一步：{row.get('next_safe_action', '')}",
                "",
            ]
        )
    lines.extend(
        [
            "## 逐点证据",
            "",
            "| 设备 | 点位 | ratio变化 | 腔体温度变化°C | 参考H2O变化mmol/mol | S2/S4原始变化mmol/mol | 额外漂移mmol/mol | S6后相对误差% | 判定 |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in points:
        lines.append(
            "| {device} | {point} | {ratio} | {temp} | {ref} | {raw} | {excess} | {rel} | {decision} |".format(
                device=row.get("device_id", ""),
                point=row.get("point_id", ""),
                ratio=_fmt(row.get("h2o_ratio_delta_post_minus_s24")),
                temp=_fmt(row.get("chamber_temp_delta_c")),
                ref=_fmt(row.get("reference_h2o_delta_mmol")),
                raw=_fmt(row.get("senco24_replay_delta_mmol")),
                excess=_fmt(row.get("raw_excess_shift_mmol")),
                rel=_fmt(row.get("post_s6_abs_rel_pct")),
                decision=row.get("point_state_transfer_decision", ""),
            )
        )
    lines.extend(
        [
            "",
            "## 最小专项诊断计划",
            "",
            "| 顺序 | 阶段 | 动作 |",
            "| ---: | --- | --- |",
        ]
    )
    for row in tables.get("h2o_state_transfer_minimal_diagnostic_plan", []):
        lines.append(f"| {row.get('sequence', '')} | {row.get('phase', '')} | {row.get('action', '')} |")
    lines.extend(
        [
            "",
            "## 判断原则",
            "",
            "如果同一湿度状态下参考露点和压力变化很小，但 H2O ratio 或 S2/S4 原始回放出现明显额外漂移，说明问题不应由 S6 输出层吸收。",
            "这种情况应优先检查 H2O 光学通道、参考信号、H2O 信号、温度响应、状态寄存器和管路记忆效应。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
