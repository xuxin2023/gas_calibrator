"""CO2 SENCO1/SENCO3 fit-point treatment plan.

This module turns the common-mode point audit into a fit decision checklist.
It is offline-only: no COM ports, no route control, and no coefficient writes.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


@dataclass(frozen=True)
class TreatmentPlanInputs:
    common_mode_audit_csv: str | Path


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


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
        writer.writerows([dict(row) for row in rows])


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    return number


def _tokens(text: Any) -> List[str]:
    return [item.strip() for item in str(text or "").split(";") if item.strip()]


def _ratio_grade(ratio_std_max: Optional[float]) -> str:
    if ratio_std_max is None:
        return "unknown"
    if ratio_std_max <= 0.0005:
        return "A"
    if ratio_std_max <= 0.001:
        return "B"
    return "review"


def _dryness_grade(dewpoint_c: Optional[float], dewpoint_h2o_mmol: Optional[float]) -> str:
    if dewpoint_c is None and dewpoint_h2o_mmol is None:
        return "unknown"
    if dewpoint_c is not None and dewpoint_c <= -28.0:
        return "deep_dry"
    if dewpoint_h2o_mmol is not None and dewpoint_h2o_mmol <= 1.0:
        return "deep_dry"
    if dewpoint_c is not None and dewpoint_c <= -20.0:
        return "dry_but_review"
    return "not_deep_dry"


def _target_category(target_ppm: Optional[float], zero_classes: Iterable[str]) -> str:
    zero_text = ";".join(zero_classes).lower()
    if "estimated_zero" in zero_text:
        return "estimated_zero_anchor"
    if target_ppm is not None and abs(target_ppm) <= 1.0e-9:
        return "zero_anchor"
    return "standard_nonzero_point"


def _decide_point(row: Mapping[str, Any]) -> Dict[str, Any]:
    common_status = str(row.get("common_mode_status") or "")
    root_cause = str(row.get("root_cause_hypothesis") or "")
    target = _safe_float(row.get("target_ppm_median"))
    ratio_std = _safe_float(row.get("ratio_std_max"))
    dewpoint = _safe_float(row.get("dewpoint_c_mean"))
    dew_h2o = _safe_float(row.get("dewpoint_derived_h2o_mmol_mean"))
    h2o_bridge_status = str(row.get("h2o_bridge_input_status") or "")
    zero_classes = _tokens(row.get("zero_anchor_classes"))
    ratio_grade = _ratio_grade(ratio_std)
    dry_grade = _dryness_grade(dewpoint, dew_h2o)
    target_category = _target_category(target, zero_classes)

    bridge_policy = (
        "disable_h2o_bridge_for_s1s3"
        if h2o_bridge_status == "do_not_use_analyzer_h2o_output_for_co2_bridge"
        else "h2o_bridge_allowed_only_if_traceable"
    )
    exclusion_basis = "do_not_exclude_by_uncalibrated_output"

    if ratio_grade not in {"A", "B"}:
        fit_policy = "hold_for_ratio_window_review"
        review_priority = "P0"
        physical_reason = "ratio 稳定窗口未达到拟合门槛，不能证明采样窗口代表稳定标准气。"
    elif dry_grade == "not_deep_dry":
        fit_policy = "hold_for_route_dryness_review"
        review_priority = "P0"
        physical_reason = "露点参考未达到气路深干要求，应先确认管路干燥和开放流通。"
    elif target_category in {"estimated_zero_anchor", "zero_anchor"}:
        fit_policy = "include_as_zero_anchor_with_uncertainty"
        review_priority = "P1"
        physical_reason = "低端锚点有物理价值，但不能当作无不确定度真零，应进入零点灵敏度评估。"
    elif common_status == "common_mode_suspect":
        fit_policy = "include_after_target_route_model_review"
        review_priority = "P1"
        physical_reason = (
            "ratio 稳定且露点参考可解释，多台同向偏差更像目标值、阀路标签、"
            "零点锚定或 S1/S3 模型形状问题；不能因输出浓度不一致直接剔除。"
        )
    else:
        fit_policy = "include_as_standard_s1s3_fit_point"
        review_priority = "P2"
        physical_reason = "物理证据未显示共模风险，可作为普通 S1/S3 拟合点。"

    if root_cause == "estimated_zero_anchor_common_bias":
        fit_policy = "include_as_zero_anchor_with_uncertainty"
        review_priority = "P1"
        physical_reason = "共模偏差发生在零点附近，应按估算零锚和低端截距不确定度处理。"

    return {
        "point_identity": row.get("point_identity") or "",
        "target_ppm_median": row.get("target_ppm_median") or "",
        "devices": row.get("devices") or "",
        "device_count": row.get("device_count") or "",
        "common_mode_status": common_status,
        "root_cause_hypothesis": root_cause,
        "fit_policy": fit_policy,
        "review_priority": review_priority,
        "exclusion_basis": exclusion_basis,
        "bridge_policy": bridge_policy,
        "target_category": target_category,
        "ratio_grade": ratio_grade,
        "ratio_std_max": row.get("ratio_std_max") or "",
        "dryness_grade": dry_grade,
        "dewpoint_c_mean": row.get("dewpoint_c_mean") or "",
        "dewpoint_derived_h2o_mmol_mean": row.get("dewpoint_derived_h2o_mmol_mean") or "",
        "analyzer_h2o_mmol_mean": row.get("h2o_mmol_mean") or "",
        "h2o_bridge_input_status": h2o_bridge_status,
        "mean_error_ppm": row.get("mean_error_ppm") or "",
        "max_abs_error_ppm": row.get("max_abs_error_ppm") or "",
        "max_abs_relative_error_percent": row.get("max_abs_relative_error_percent") or "",
        "physical_reason": physical_reason,
        "next_action": _next_action(fit_policy, bridge_policy),
    }


def _next_action(fit_policy: str, bridge_policy: str) -> str:
    if fit_policy == "hold_for_ratio_window_review":
        return "复核该点采样窗口、ratio A/B 门禁和状态寄存器；未通过前不进入正式 S1/S3。"
    if fit_policy == "hold_for_route_dryness_review":
        return "复核露点、阀路和开放流通证据；必要时降级该点或重新采样。"
    if fit_policy == "include_as_zero_anchor_with_uncertainty":
        return "进入 S1/S3 拟合，但同时做零气 CO2 含量/低端截距灵敏度评估。"
    if fit_policy == "include_after_target_route_model_review":
        suffix = "；禁用 H2O bridge" if bridge_policy == "disable_h2o_bridge_for_s1s3" else ""
        return "核对气瓶目标、阀路标签和模型形状后保留为拟合点" + suffix + "。"
    return "保留为标准 S1/S3 拟合点。"


def build_co2_senco13_fit_point_treatment_plan(
    *,
    inputs: TreatmentPlanInputs,
) -> Dict[str, List[Dict[str, Any]]]:
    audit_rows = _read_csv(inputs.common_mode_audit_csv)
    treatment_rows = [_decide_point(row) for row in audit_rows]

    policy_counts: Dict[str, int] = {}
    for row in treatment_rows:
        policy = str(row.get("fit_policy") or "unknown")
        policy_counts[policy] = policy_counts.get(policy, 0) + 1

    bridge_disabled = sum(
        1
        for row in treatment_rows
        if row.get("bridge_policy") == "disable_h2o_bridge_for_s1s3"
    )
    summary_rows = [
        {
            "metric": "input_audit_points",
            "value": len(audit_rows),
            "physical_meaning": "输入共模审计点位数。",
        },
        {
            "metric": "fit_policy_counts",
            "value": json.dumps(policy_counts, ensure_ascii=False, sort_keys=True),
            "physical_meaning": "逐点拟合处理策略分布。",
        },
        {
            "metric": "h2o_bridge_disabled_points",
            "value": bridge_disabled,
            "physical_meaning": "分析仪 H2O 输出与露点参考不一致，不能用于 CO2 bridge 的点数。",
        },
        {
            "metric": "global_s1s3_policy",
            "value": "ratio_first_pressure_frozen_s5_deferred",
            "physical_meaning": "S1/S3 先按 ratio/温度/标准值拟合；当前大气压开放流通数据冻结压力项；S5 后置。",
        },
    ]
    return {
        "co2_senco13_fit_point_treatment_summary": summary_rows,
        "co2_senco13_fit_point_treatment_plan": treatment_rows,
    }


def write_co2_senco13_fit_point_treatment_plan(
    *,
    inputs: TreatmentPlanInputs,
    output_dir: str | Path,
) -> Dict[str, Path]:
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    tables = build_co2_senco13_fit_point_treatment_plan(inputs=inputs)
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        path = destination / f"{name}.csv"
        _write_csv(path, rows)
        outputs[f"{name}_csv"] = path
    meta = {
        "tool_name": "export_v1_5_co2_senco13_fit_point_treatment_plan",
        "created_at": _now(),
        "inputs": {
            "common_mode_audit_csv": str(Path(inputs.common_mode_audit_csv).resolve()),
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
        "physics_contract": {
            "primary_fit_signal": "filtered_CO2_ratio",
            "do_not_reject_by": "uncalibrated_CO2_or_H2O_output",
            "pressure_terms": "frozen_for_current_atmospheric_open_flow",
            "s5_s6": "final_output_layer_after_main_chain_review",
        },
    }
    meta_path = destination / "co2_senco13_fit_point_treatment_plan_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["meta_json"] = meta_path
    outputs["markdown"] = _write_markdown(
        destination / "co2_senco13_fit_point_treatment_plan_zh.md",
        tables,
    )
    return outputs


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    treatment_rows = list(tables.get("co2_senco13_fit_point_treatment_plan") or [])
    priority_rows = [row for row in treatment_rows if row.get("review_priority") in {"P0", "P1"}]
    lines = [
        "# V1.5 CO2 S1/S3 点位处理计划",
        "",
        "- 边界：离线处理计划；不打开 COM；不控制气路/水路；不写 SENCO。",
        "- 核心原则：校准前输出浓度可能不对，所以不能因为 CO2/H2O 输出不一致而剔除点；S1/S3 主链路看滤波后 CO2 ratio、标准气目标、温度输入和稳定窗口。",
        "- 压力项：本轮 CO2 主校准是当前大气压开放流通数据，压力项冻结，不允许用压力项吸收残差。",
        "- S5：只作为 S1/S3 主链路评审后的最终显示层修正，不提前吸收共模点位问题。",
        "",
        "## 需要优先评审的点",
        "",
    ]
    if not priority_rows:
        lines.append("没有 P0/P1 点位。")
    for row in priority_rows:
        lines.append(
            "- "
            f"{row.get('point_identity')}: {row.get('fit_policy')}，"
            f"ratio={row.get('ratio_grade')}，dry={row.get('dryness_grade')}，"
            f"最大绝对误差 {row.get('max_abs_error_ppm')} ppm，"
            f"最大相对误差 {row.get('max_abs_relative_error_percent')}%。"
        )
        lines.append(f"  - 物理解释：{row.get('physical_reason')}")
        lines.append(f"  - 下一步：{row.get('next_action')}")
    lines.extend(
        [
            "",
            "## 全局拟合规则",
            "",
            "1. ratio A/B 且露点参考合格的非零点，不因未校准输出浓度不一致而剔除。",
            "2. 零点进入拟合时必须带零气 CO2 含量/低端截距不确定度评审。",
            "3. 若分析仪 H2O 输出与露点参考不一致，禁用 H2O bridge；不能反过来拒绝 CO2 ratio 点。",
            "4. 正式 S1/S3 候选先不带压力项；带压力项模型只能作为诊断，不作为写入合同。",
            "5. S5 在 S1/S3 主链路残差合理后再评审。",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path
