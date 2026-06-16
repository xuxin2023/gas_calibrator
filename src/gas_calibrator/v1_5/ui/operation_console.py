"""Read-only V1.5 formal calibration operation console.

This first UI surface is deliberately evidence-first. It reads already-created
workbench, run-evidence, archive, and calibratability JSON files and renders a
static operator/reviewer console. It never opens COM ports, controls water/gas
routes, commands valves/PACE, or writes analyzer coefficients.
"""

from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping


PAGE_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "key": "dashboard",
        "title": "首页 / 运行总览",
        "purpose": "让操作员看到当前 run、阶段、可校准状态、正式签发阻塞项和安全边界。",
        "physical_signals": ["run_id", "current_stage", "method_backbone_ready", "formal_release_ready"],
        "calibration_gates": ["traceability_bound", "no_write_boundary_visible"],
    },
    {
        "key": "plan_select",
        "title": "校准计划选择",
        "purpose": "冻结校准计划、标准气证书、配置 hash 和 no-write/受控写入边界。",
        "physical_signals": ["standard_gas_certificate", "sampling_window", "stability_thresholds"],
        "calibration_gates": ["plan_snapshot_required", "standard_gas_valid"],
    },
    {
        "key": "precheck",
        "title": "设备预检",
        "purpose": "确认分析仪身份、GETCO 旧系数、状态寄存器和参考设备处于可解释状态。",
        "physical_signals": ["device_id", "GETCO1-9", "status_register", "reference_devices"],
        "calibration_gates": ["device_identity_required", "GETCO_backup_required"],
    },
    {
        "key": "pressure_channel_verify",
        "title": "压力通道验证",
        "purpose": "先证明分析仪内部 pressure_kpa 与 COM22 压力参考一致，避免把压力错误混入 CO2/H2O 拟合。",
        "physical_signals": ["COM22_pressure", "PACE_pressure", "analyzer_pressure_kpa", "delta_hpa"],
        "calibration_gates": ["pressure_channel_pass_required_before_component_calibration"],
    },
    {
        "key": "open_flow_sampling",
        "title": "开放流通采样",
        "purpose": "显示吹扫、稳定门禁、采样窗口和当前阻塞原因，确保采样时气路/水路仍保持开放流通。",
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
        "calibration_gates": ["minimum_purge_elapsed", "ratio_stable", "dewpoint_state_interpretable"],
    },
    {
        "key": "qc_review",
        "title": "QC 与点位评审",
        "purpose": "解释每个点为什么可用于拟合、降级或拒绝，防止坏点静默进入系数。",
        "physical_signals": ["FrameQC", "PressureChannelQC", "DewpointHumidityQC", "FactorySignalQC"],
        "calibration_gates": ["a_grade_only_enters_formal_fit", "reject_reason_required"],
    },
    {
        "key": "report_review",
        "title": "候选系数与报告",
        "purpose": "展示旧系数、候选系数、复验误差、报告状态和不确定度状态。",
        "physical_signals": ["old_GETCO", "candidate_coefficients", "reverification_error", "uncertainty_budget"],
        "calibration_gates": ["candidate_review_required", "post_write_reverification_required"],
    },
    {
        "key": "approval",
        "title": "审核与归档",
        "purpose": "记录审核、批准、数据库归档、报告 hash 和写入/回读/复验闭环。",
        "physical_signals": ["report_hash", "coefficient_hash", "audit_events", "archive_status"],
        "calibration_gates": ["operator_cannot_self_approve", "database_import_or_dry_run_visible"],
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


STATUS_LABELS = {
    "pass": "通过",
    "ready_for_reviewer": "可评审",
    "formal_release_ready": "可正式签发",
    "demonstrated_calibratable_for_verified_scope": "已证明可校准范围",
    "conditionally_calibratable_needs_release_closure": "有条件可校准",
    "not_calibratable_until_p0_resolved": "P0 阻断",
    "not_yet_calibratable_evidence_incomplete": "证据不足",
    "review_ready": "待评审",
    "draft_only": "草稿",
    "pending": "待补齐",
    "missing": "缺失",
    "partial": "部分完成",
    "blocked": "阻断",
    "fail": "失败",
    "not_releasable": "不可发布",
    "not_attempted": "未执行",
    "write_attempted": "已发生写入",
    "dry_run": "数据库 dry-run",
    "import": "已导入数据库",
    "skip": "跳过",
    "skipped": "跳过",
}


STATUS_TONE = {
    "pass": "ok",
    "ready_for_reviewer": "ok",
    "formal_release_ready": "ok",
    "demonstrated_calibratable_for_verified_scope": "ok",
    "conditionally_calibratable_needs_release_closure": "warn",
    "review_ready": "warn",
    "draft_only": "warn",
    "pending": "warn",
    "partial": "warn",
    "missing": "bad",
    "blocked": "bad",
    "fail": "bad",
    "not_releasable": "bad",
    "not_calibratable_until_p0_resolved": "bad",
    "not_yet_calibratable_evidence_incomplete": "bad",
    "not_attempted": "muted",
    "skipped": "muted",
    "skip": "muted",
}


STAGE_TO_PAGE = {
    "full_flow_contract_gate": "dashboard",
    "plan_traceability": "plan_select",
    "identity_getco_epoch0": "precheck",
    "pressure_quick_check": "pressure_channel_verify",
    "co2_open_flow": "open_flow_sampling",
    "h2o_open_flow": "open_flow_sampling",
    "candidate_review": "report_review",
    "controlled_write_events": "approval",
    "post_write_reverification": "report_review",
    "evidence_bundle": "approval",
    "database_import": "approval",
    "reports": "report_review",
}


CARD_TO_PAGE = {
    "formal_plan": "plan_select",
    "device_precheck": "precheck",
    "pressure_quick_check": "pressure_channel_verify",
    "open_flow_samples": "open_flow_sampling",
    "qc_package": "qc_review",
    "candidate_coefficients": "report_review",
    "post_write_reverification": "report_review",
    "report_release": "report_review",
    "database_import": "approval",
    "evidence_bundle": "approval",
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _as_list(value: Any) -> List[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _cards_by_key(workbench_model: Mapping[str, Any] | None) -> Dict[str, Mapping[str, Any]]:
    if not workbench_model:
        return {}
    return {
        str(card.get("key") or ""): card
        for card in workbench_model.get("cards", [])
        if isinstance(card, Mapping)
    }


def _stage_map(run_evidence_status: Mapping[str, Any] | None) -> Dict[str, Mapping[str, Any]]:
    if not run_evidence_status:
        return {}
    return {
        str(row.get("stage_id") or ""): row
        for row in run_evidence_status.get("stage_statuses", [])
        if isinstance(row, Mapping) and str(row.get("stage_id") or "")
    }


def _status_label(status: Any) -> str:
    text = str(status or "pending")
    return STATUS_LABELS.get(text, text)


def _tone(status: Any) -> str:
    return STATUS_TONE.get(str(status or "pending"), "muted")


def _worst_status(statuses: Iterable[str]) -> str:
    values = [str(item or "") for item in statuses if str(item or "")]
    for status in ("blocked", "fail", "missing"):
        if status in values:
            return status
    for status in ("partial", "pending", "draft_only", "review_ready", "not_attempted"):
        if status in values:
            return status
    for status in ("pass", "ready_for_reviewer", "write_attempted"):
        if status in values:
            return "pass"
    return "pending"


def _card_status(cards: Mapping[str, Mapping[str, Any]], key: str, default: str = "pending") -> str:
    return str(cards.get(key, {}).get("status") or default)


def _page_stage_rows(page_key: str, stages: Mapping[str, Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    return [row for stage_id, row in stages.items() if STAGE_TO_PAGE.get(stage_id) == page_key]


def _page_card_rows(page_key: str, cards: Mapping[str, Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    return [row for key, row in cards.items() if CARD_TO_PAGE.get(key) == page_key]


def _stage_reason(row: Mapping[str, Any]) -> str:
    reason = str(row.get("reason") or "").strip()
    return reason or str(row.get("status") or "pending")


def _stage_line(row: Mapping[str, Any]) -> str:
    title = str(row.get("title") or row.get("stage_id") or "stage")
    return f"{title}: {_status_label(row.get('status'))} ({_stage_reason(row)})"


def _page_status(page_key: str, cards: Mapping[str, Mapping[str, Any]], stages: Mapping[str, Mapping[str, Any]]) -> str:
    statuses = [str(row.get("status") or "") for row in _page_stage_rows(page_key, stages)]
    statuses.extend(str(row.get("status") or "") for row in _page_card_rows(page_key, cards))
    if page_key == "dashboard":
        statuses.extend(
            [
                _card_status(cards, "formal_plan"),
                _card_status(cards, "report_release", default="pending"),
            ]
        )
    return _worst_status(statuses)


def _page_blockers(page_key: str, cards: Mapping[str, Mapping[str, Any]], stages: Mapping[str, Mapping[str, Any]]) -> List[str]:
    blockers: List[str] = []
    for row in _page_stage_rows(page_key, stages):
        status = str(row.get("status") or "")
        if status not in {"pass", "ready_for_reviewer", "write_attempted"}:
            blockers.append(_stage_line(row))
    for row in _page_card_rows(page_key, cards):
        blockers.extend(str(item) for item in _as_list(row.get("blockers")) if str(item))
    return blockers or ["无阻塞；等待对应证据刷新。"]


def _summary_cards(
    *,
    cards: Mapping[str, Mapping[str, Any]],
    run_evidence_status: Mapping[str, Any] | None,
    calibration_capability: Mapping[str, Any] | None,
    archive_index: Mapping[str, Any] | None,
) -> List[Dict[str, Any]]:
    capability = calibration_capability or {}
    verification = capability.get("verification_rollup") if isinstance(capability.get("verification_rollup"), Mapping) else {}
    archive_database = archive_index.get("database") if isinstance((archive_index or {}).get("database"), Mapping) else {}

    method_status = (
        "pass"
        if capability.get("method_backbone_ready") is True
        else _card_status(cards, "formal_plan", "pending")
    )
    capability_status = str(capability.get("status") or capability.get("capability_status") or "pending")
    release_status = (
        "formal_release_ready"
        if capability.get("formal_release_ready") is True
        else str(capability_status or _card_status(cards, "report_release", "pending"))
    )
    verification_status = str(verification.get("status") or _card_status(cards, "post_write_reverification", "pending"))
    database_status = str(
        archive_database.get("mode")
        or (run_evidence_status or {}).get("database_mode")
        or _card_status(cards, "database_import", "pending")
    )

    max_error = verification.get("max_abs_error_pct")
    verification_detail = "未发现复验证据"
    if max_error is not None:
        verification_detail = f"最大相对误差 {float(max_error):.6g}%"

    return [
        {
            "key": "method_backbone",
            "title": "正式方法骨架",
            "status": method_status,
            "label": _status_label(method_status),
            "detail": "压力先验证，随后开放流通 CO2/H2O，压力补偿后置；封路压力点不进入正式拟合。",
        },
        {
            "key": "verification",
            "title": "复验/能力证据",
            "status": verification_status,
            "label": _status_label(verification_status),
            "detail": verification_detail,
        },
        {
            "key": "formal_release",
            "title": "正式签发状态",
            "status": release_status,
            "label": _status_label(release_status),
            "detail": "只有报告、审核、归档、写入回读和复验闭环完成后才可正式签发。",
        },
        {
            "key": "database",
            "title": "数据库/证据归档",
            "status": database_status,
            "label": _status_label(database_status),
            "detail": "数据库保存索引、审核与状态；原始帧、报告和图表仍以证据包文件为准。",
        },
    ]


def build_operation_console_model(
    *,
    workbench_model: Mapping[str, Any] | None = None,
    run_evidence_status: Mapping[str, Any] | None = None,
    calibration_capability: Mapping[str, Any] | None = None,
    archive_index: Mapping[str, Any] | None = None,
    role: str = "operator",
) -> Dict[str, Any]:
    cards = _cards_by_key(workbench_model)
    stages = _stage_map(run_evidence_status)
    role_key = role if role in ROLE_PERMISSIONS else "operator"
    summary_cards = _summary_cards(
        cards=cards,
        run_evidence_status=run_evidence_status,
        calibration_capability=calibration_capability,
        archive_index=archive_index,
    )

    pages: List[Dict[str, Any]] = []
    for definition in PAGE_DEFINITIONS:
        page_key = str(definition["key"])
        status = _page_status(page_key, cards, stages)
        stage_rows = _page_stage_rows(page_key, stages)
        card_rows = _page_card_rows(page_key, cards)
        pages.append(
            {
                **definition,
                "status": status,
                "status_label": _status_label(status),
                "tone": _tone(status),
                "read_only": True,
                "device_control_enabled": False,
                "blockers": _page_blockers(page_key, cards, stages),
                "evidence_refs": [
                    {
                        "kind": "stage",
                        "key": str(row.get("stage_id") or ""),
                        "status": str(row.get("status") or ""),
                        "title": str(row.get("title") or ""),
                        "artifact_count": int(row.get("artifact_count") or 0),
                    }
                    for row in stage_rows
                ]
                + [
                    {
                        "kind": "workbench_card",
                        "key": str(row.get("key") or ""),
                        "status": str(row.get("status") or ""),
                        "title": str(row.get("title") or row.get("key") or ""),
                        "metric": str(row.get("metric") or ""),
                    }
                    for row in card_rows
                ],
            }
        )

    capability_status = str(
        (calibration_capability or {}).get("status")
        or (calibration_capability or {}).get("capability_status")
        or "not_available"
    )
    return {
        "schema_version": "v1_5_operation_console_v1",
        "generated_at": _now(),
        "role": role_key,
        "role_permissions": ROLE_PERMISSIONS[role_key],
        "sidecar_only": True,
        "read_only_first_release": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "cannot_write_senco": True,
        "cannot_clear_senco": True,
        "cannot_modify_analyzer_id": True,
        "not_real_device_control": True,
        "calibration_capability_status": capability_status,
        "calibration_capability_label": _status_label(capability_status),
        "run_id": str(
            (workbench_model or {}).get("run_id")
            or (run_evidence_status or {}).get("run_id")
            or (archive_index or {}).get("run_id")
            or ""
        ),
        "run_dir": str(
            (workbench_model or {}).get("run_dir")
            or (run_evidence_status or {}).get("run_dir")
            or (archive_index or {}).get("run_dir")
            or ""
        ),
        "summary_cards": summary_cards,
        "pages": pages,
        "source_evidence": {
            "has_workbench_model": bool(workbench_model),
            "has_run_evidence_status": bool(run_evidence_status),
            "has_calibration_capability": bool(calibration_capability),
            "has_archive_index": bool(archive_index),
        },
        "physical_boundary_statement": (
            "本操作台仅展示证据状态；不打开串口、不控制水路/气路、不控制阀或 PACE、"
            "不写 SENCO、不清除系数、不修改分析仪 ID。"
        ),
    }


def _badge(label: str, tone: str) -> str:
    return f'<span class="badge {html.escape(tone)}">{_esc(label)}</span>'


def render_operation_console_html(model: Mapping[str, Any]) -> str:
    cards_html = "\n".join(
        f"""
        <section class="summary-card {html.escape(str(card.get('status') or 'pending'))}">
          <div class="card-title">{_esc(card.get('title'))}</div>
          <div class="card-status">{_badge(str(card.get('label') or card.get('status') or '待补齐'), _tone(card.get('status')))}</div>
          <p>{_esc(card.get('detail'))}</p>
        </section>
        """
        for card in model.get("summary_cards", [])
        if isinstance(card, Mapping)
    )
    pages_html = "\n".join(
        f"""
        <tr>
          <td>{index + 1}</td>
          <td><strong>{_esc(page.get('title'))}</strong><br><span>{_esc(page.get('key'))}</span></td>
          <td>{_badge(str(page.get('status_label') or page.get('status')), str(page.get('tone') or 'muted'))}</td>
          <td>{_esc(page.get('purpose'))}</td>
          <td>{_esc('、'.join(str(item) for item in page.get('physical_signals', [])))}</td>
          <td>{_esc('；'.join(str(item) for item in page.get('blockers', [])))}</td>
        </tr>
        """
        for index, page in enumerate(model.get("pages", []))
        if isinstance(page, Mapping)
    )
    source = model.get("source_evidence") if isinstance(model.get("source_evidence"), Mapping) else {}
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>V1.5 正式校准操作台</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #162033;
      --muted: #627084;
      --line: #d7dee8;
      --panel: #ffffff;
      --bg: #f3f6fa;
      --ok: #167a45;
      --warn: #9a5a00;
      --bad: #a32727;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "Noto Sans CJK SC", Arial, sans-serif;
      background: var(--bg);
      color: var(--ink);
      letter-spacing: 0;
    }}
    header {{
      padding: 22px 28px 14px;
      border-bottom: 1px solid var(--line);
      background: #fff;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 26px;
      line-height: 1.25;
      font-weight: 700;
    }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px 22px;
      color: var(--muted);
      font-size: 13px;
    }}
    main {{ padding: 18px 28px 28px; }}
    .boundary {{
      border: 1px solid var(--line);
      background: #fff;
      padding: 12px 14px;
      margin-bottom: 16px;
      font-size: 14px;
      line-height: 1.6;
    }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(4, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }}
    .summary-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      min-height: 126px;
    }}
    .card-title {{ font-weight: 700; margin-bottom: 8px; }}
    .card-status {{ margin-bottom: 8px; }}
    p {{ margin: 0; line-height: 1.55; color: var(--muted); }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid var(--line);
      table-layout: fixed;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 9px 10px;
      text-align: left;
      vertical-align: top;
      font-size: 13px;
      line-height: 1.45;
      overflow-wrap: anywhere;
    }}
    th {{ background: #eaf0f7; color: #243247; }}
    th:nth-child(1), td:nth-child(1) {{ width: 44px; text-align: center; }}
    th:nth-child(2), td:nth-child(2) {{ width: 180px; }}
    th:nth-child(3), td:nth-child(3) {{ width: 96px; }}
    th:nth-child(4), td:nth-child(4) {{ width: 25%; }}
    th:nth-child(5), td:nth-child(5) {{ width: 24%; }}
    .badge {{
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      border: 1px solid currentColor;
      white-space: nowrap;
    }}
    .badge.ok {{ color: var(--ok); background: #e9f6ef; }}
    .badge.warn {{ color: var(--warn); background: #fff5dc; }}
    .badge.bad {{ color: var(--bad); background: #ffecec; }}
    .badge.muted {{ color: var(--muted); background: #f2f4f7; }}
    span {{ color: var(--muted); }}
    @media (max-width: 1180px) {{
      .summary {{ grid-template-columns: repeat(2, minmax(180px, 1fr)); }}
      table {{ table-layout: auto; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>V1.5 正式校准操作台</h1>
    <div class="meta">
      <div>生成时间：{_esc(model.get('generated_at'))}</div>
      <div>角色：{_esc(model.get('role'))}</div>
      <div>Run ID：{_esc(model.get('run_id') or '未绑定')}</div>
      <div>可校准状态：{_esc(model.get('calibration_capability_label'))}</div>
    </div>
  </header>
  <main>
    <section class="boundary">
      <strong>安全边界：</strong>{_esc(model.get('physical_boundary_statement'))}
      <br>
      <strong>证据来源：</strong>
      workbench={_esc(source.get('has_workbench_model'))}，
      run_evidence={_esc(source.get('has_run_evidence_status'))}，
      capability={_esc(source.get('has_calibration_capability'))}，
      archive={_esc(source.get('has_archive_index'))}
    </section>
    <section class="summary">{cards_html}</section>
    <table aria-label="V1.5 操作页面状态">
      <thead>
        <tr>
          <th>#</th>
          <th>页面</th>
          <th>状态</th>
          <th>物理/校准意义</th>
          <th>关键物理量</th>
          <th>阻塞或待刷新证据</th>
        </tr>
      </thead>
      <tbody>
        {pages_html}
      </tbody>
    </table>
  </main>
</body>
</html>
"""


def write_operation_console(
    *,
    output_dir: str | Path,
    workbench_model: Mapping[str, Any] | None = None,
    run_evidence_status: Mapping[str, Any] | None = None,
    calibration_capability: Mapping[str, Any] | None = None,
    archive_index: Mapping[str, Any] | None = None,
    role: str = "operator",
) -> Dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_operation_console_model(
        workbench_model=workbench_model,
        run_evidence_status=run_evidence_status,
        calibration_capability=calibration_capability,
        archive_index=archive_index,
        role=role,
    )
    model_path = out / "v1_5_operation_console.json"
    html_path = out / "v1_5_operation_console.html"
    model_path.write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    html_path.write_text(render_operation_console_html(model), encoding="utf-8-sig")
    return {"model": model_path, "html": html_path}


__all__ = [
    "PAGE_DEFINITIONS",
    "ROLE_PERMISSIONS",
    "build_operation_console_model",
    "render_operation_console_html",
    "write_operation_console",
]
