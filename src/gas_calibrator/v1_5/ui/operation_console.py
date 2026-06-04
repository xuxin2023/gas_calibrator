"""V1.5 formal calibration operation-console skeleton.

The console model describes the stage-eight UI pages without controlling
devices. It is intentionally read-only/sidecar-first: no COM, no route control,
no valve/PACE control, and no SENCO or coefficient writes.
"""

from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence


PAGE_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "key": "dashboard",
        "title": "首页 / 运行总览",
        "purpose": "让操作员知道当前 run、阶段、范围、阻塞原因和是否可进入下一步。",
        "physical_signals": ["run_id", "current_stage", "CO2/H2O scope", "pressure coverage"],
        "calibration_gates": ["traceability_bound", "no_write_boundary_visible"],
    },
    {
        "key": "plan_select",
        "title": "校准计划选择",
        "purpose": "冻结校准计划、标准气证书、配置 hash 和 no-write 边界。",
        "physical_signals": ["standard_gas_certificate", "sampling_window", "stability_thresholds"],
        "calibration_gates": ["plan_snapshot_required", "standard_gas_valid"],
    },
    {
        "key": "precheck",
        "title": "设备预检",
        "purpose": "确认分析仪、COM22、露点仪、PACE 和 GETCO 备份处于可解释状态。",
        "physical_signals": ["status_register", "GETCO1-9", "PACE_OUTP", "zero_valve", "COM22"],
        "calibration_gates": ["p0_precheck_blocks_formal_run", "GETCO_backup_required"],
    },
    {
        "key": "pressure_channel_verify",
        "title": "压力通道验证",
        "purpose": "独立证明分析仪内部 pressure_kpa 与 COM22 压力参考一致。",
        "physical_signals": ["COM22_pressure", "PACE_pressure", "analyzer_pressure_kpa", "delta_hpa"],
        "calibration_gates": ["pressure_channel_pass_required_for_write_review"],
    },
    {
        "key": "open_flow_sampling",
        "title": "开放流通采样",
        "purpose": "显示吹扫、稳定门禁、正式采样窗口和当前阻塞原因。",
        "physical_signals": [
            "CO2",
            "H2O",
            "dewpoint",
            "H2O_dry_ppmv",
            "COM22_pressure",
            "analyzer_pressure",
            "CO2_ratio",
            "H2O_ratio",
            "ref_signal",
            "chamber_temp",
            "case_temp",
        ],
        "calibration_gates": ["stability_gate_pass", "status_register_pass", "mode2_contract_pass"],
    },
    {
        "key": "qc_review",
        "title": "QC 与点位评审",
        "purpose": "解释每个点为什么 A 级、降级或拒绝。",
        "physical_signals": ["FrameQC", "PressureChannelQC", "DewpointHumidityQC", "FactorySignalQC"],
        "calibration_gates": ["a_grade_only_enters_formal_fit", "reject_reason_required"],
    },
    {
        "key": "report_review",
        "title": "候选系数与报告",
        "purpose": "展示候选系数证据、报告发布门禁和不确定度状态。",
        "physical_signals": ["old_GETCO", "candidate_coefficients", "residuals", "uncertainty_budget"],
        "calibration_gates": ["no_auto_write", "released_uncertainty_required_for_formal_issue"],
    },
    {
        "key": "approval",
        "title": "审核与归档",
        "purpose": "记录 reviewer/approver、归档状态、hash 和写入前后证据。",
        "physical_signals": ["report_hash", "coefficient_hash", "audit_events", "archive_status"],
        "calibration_gates": ["operator_cannot_self_approve", "write_requires_separate_approval"],
    },
]


ROLE_PERMISSIONS = {
    "operator": {
        "select_plan": True,
        "run_sampling": True,
        "view_qc": True,
        "edit_qc_thresholds": False,
        "generate_candidate_coefficients": False,
        "approve_writes": False,
        "write_senco": False,
        "edit_high_risk_parameters": False,
    },
    "engineer": {
        "select_plan": True,
        "run_sampling": True,
        "view_qc": True,
        "edit_qc_thresholds": True,
        "generate_candidate_coefficients": True,
        "approve_writes": False,
        "write_senco": False,
        "edit_high_risk_parameters": False,
    },
    "reviewer": {
        "select_plan": False,
        "run_sampling": False,
        "view_qc": True,
        "edit_qc_thresholds": "review_required",
        "generate_candidate_coefficients": True,
        "approve_writes": True,
        "write_senco": False,
        "edit_high_risk_parameters": False,
    },
    "admin": {
        "select_plan": True,
        "run_sampling": True,
        "view_qc": True,
        "edit_qc_thresholds": True,
        "generate_candidate_coefficients": True,
        "approve_writes": True,
        "write_senco": "controlled_after_approval",
        "edit_high_risk_parameters": "advanced_authorization_required",
    },
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _cards_by_key(workbench_model: Mapping[str, Any] | None) -> Dict[str, Mapping[str, Any]]:
    if not workbench_model:
        return {}
    return {
        str(card.get("key") or ""): card
        for card in workbench_model.get("cards", [])
        if isinstance(card, Mapping)
    }


def _card_status(cards: Mapping[str, Mapping[str, Any]], key: str, default: str = "pending") -> str:
    return str((cards.get(key) or {}).get("status") or default)


def _page_status(page_key: str, cards: Mapping[str, Mapping[str, Any]], workbench_model: Mapping[str, Any] | None) -> str:
    if page_key == "dashboard":
        return str((workbench_model or {}).get("preflight_status") or "pending")
    if page_key == "plan_select":
        return _card_status(cards, "formal_plan", "missing")
    if page_key == "precheck":
        return str((workbench_model or {}).get("preflight_status") or "pending")
    if page_key == "pressure_channel_verify":
        return _card_status(cards, "pressure_quick_check", "pending")
    if page_key == "open_flow_sampling":
        return _card_status(cards, "open_flow_samples", "pending")
    if page_key == "qc_review":
        return _card_status(cards, "qc_package", "pending")
    if page_key == "report_review":
        return _card_status(cards, "report_release", "pending")
    if page_key == "approval":
        report_status = _card_status(cards, "report_release", "pending")
        return "pending" if report_status in {"draft_only", "review_ready"} else report_status
    return "pending"


def _page_blockers(page_key: str, cards: Mapping[str, Mapping[str, Any]]) -> List[str]:
    mapped = {
        "plan_select": "formal_plan",
        "pressure_channel_verify": "pressure_quick_check",
        "open_flow_sampling": "open_flow_samples",
        "qc_review": "qc_package",
        "report_review": "report_release",
        "approval": "report_release",
    }.get(page_key)
    if mapped:
        return [str(item) for item in (cards.get(mapped) or {}).get("blockers", []) if str(item)]
    return []


def build_operation_console_model(
    *,
    workbench_model: Mapping[str, Any] | None = None,
    role: str = "operator",
) -> Dict[str, Any]:
    """Build a read-only operation-console model from evidence status."""

    normalized_role = role if role in ROLE_PERMISSIONS else "operator"
    cards = _cards_by_key(workbench_model)
    pages: List[Dict[str, Any]] = []
    for definition in PAGE_DEFINITIONS:
        page_key = str(definition["key"])
        status = _page_status(page_key, cards, workbench_model)
        pages.append(
            {
                **definition,
                "status": status,
                "blockers": _page_blockers(page_key, cards),
                "read_only": True,
                "device_control_enabled": False,
                "senco_write_enabled": False,
            }
        )

    return {
        "schema_version": "v1_5_operation_console_v0",
        "generated_at": _now(),
        "role": normalized_role,
        "sidecar_only": True,
        "read_only_first_release": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "default_route_scope": "open_flow_current_atmosphere_component_calibration",
        "diagnostic_exclusions": [
            "sealed_pressure_points_not_formal_fit_default",
            "pace_output_dynamic_control_diagnostic_only",
            "pace_act_sink_bias_diagnostic_only",
            "vent_hold_diagnostic_only",
        ],
        "pages": pages,
        "role_permissions": ROLE_PERMISSIONS[normalized_role],
        "source_workbench": {
            "run_dir": str((workbench_model or {}).get("run_dir") or ""),
            "preflight_status": str((workbench_model or {}).get("preflight_status") or ""),
            "package_status": str((workbench_model or {}).get("package_status") or ""),
        },
    }


def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def render_operation_console_html(model: Mapping[str, Any]) -> str:
    rows = "\n".join(
        f"<tr><td>{_esc(page.get('title'))}</td><td>{_esc(page.get('status'))}</td>"
        f"<td>{_esc(page.get('purpose'))}</td><td>{_esc(';'.join(page.get('blockers') or []))}</td></tr>"
        for page in model.get("pages", [])
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link rel="icon" href="data:," />
  <title>V1.5 正式校准操作台</title>
  <style>
    body {{ margin: 0; font: 14px/1.5 "Microsoft YaHei", "Segoe UI", Arial, sans-serif; background: #f7f8fb; color: #172033; }}
    main {{ max-width: 1320px; margin: 0 auto; padding: 24px; }}
    header {{ display: flex; justify-content: space-between; gap: 16px; border-bottom: 1px solid #d9e0ea; padding-bottom: 16px; }}
    h1 {{ margin: 0 0 6px; font-size: 28px; letter-spacing: 0; }}
    .flags {{ background: #fff; border: 1px solid #d9e0ea; border-radius: 8px; padding: 12px; min-width: 360px; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; color: #667085; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; margin-top: 20px; border: 1px solid #d9e0ea; }}
    th, td {{ border-bottom: 1px solid #d9e0ea; padding: 9px 10px; text-align: left; vertical-align: top; word-break: break-word; }}
    th {{ background: #eef2f6; }}
    .note {{ color: #667085; }}
    @media (max-width: 900px) {{ header {{ display: block; }} .flags {{ min-width: 0; margin-top: 12px; }} .grid {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
<main>
  <header>
    <div>
      <h1>V1.5 正式校准操作台</h1>
      <p class="note">只读/半自动第一版：围绕开放流通 CO2/H2O 主校准、压力通道独立验证和报告评审。</p>
    </div>
    <aside class="flags">
      <strong>安全边界</strong>
      <div class="grid">
        <span>COM: {_esc(model.get('opens_com_ports'))}</span>
        <span>水路/气路: {_esc(model.get('controls_water_or_gas_routes'))}</span>
        <span>阀/PACE: {_esc(model.get('controls_valves_or_pace'))}</span>
        <span>SENCO: {_esc(model.get('writes_coefficients'))}</span>
      </div>
    </aside>
  </header>
  <table>
    <thead><tr><th>页面</th><th>状态</th><th>校准意义</th><th>阻塞原因</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</main>
</body>
</html>
"""


def write_operation_console(
    *,
    output_dir: str | Path,
    workbench_model: Mapping[str, Any] | None = None,
    role: str = "operator",
) -> Dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    model = build_operation_console_model(workbench_model=workbench_model, role=role)
    json_path = root / "v1_5_operation_console.json"
    html_path = root / "v1_5_operation_console.html"
    json_path.write_text(json.dumps(model, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    html_path.write_text(render_operation_console_html(model), encoding="utf-8")
    return {"model": json_path, "html": html_path}
