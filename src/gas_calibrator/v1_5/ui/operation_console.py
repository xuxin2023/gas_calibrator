"""Read-only V1.5 formal calibration operation console.

This module only renders evidence that was already produced by the V1.5
calibration tools. It must never open serial ports, control valves, control
PACE, or write analyzer coefficients.
"""

from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


PAGE_DEFINITIONS: list[dict[str, Any]] = [
    {
        "key": "dashboard",
        "title": "首页 / 运行总览",
        "purpose": "显示 run_id、阶段状态、正式范围和当前阻塞原因。",
        "physical_signals": ["run_id", "current_stage", "method_backbone_ready", "formal_release_ready"],
        "calibration_gates": ["traceability_bound", "no_write_boundary_visible"],
    },
    {
        "key": "plan_select",
        "title": "校准计划选择",
        "purpose": "展示校准计划、标准气证书、采样窗口和配置版本。",
        "physical_signals": ["standard_gas_certificate", "sampling_window", "stability_thresholds"],
        "calibration_gates": ["plan_snapshot_required", "standard_gas_valid"],
    },
    {
        "key": "precheck",
        "title": "设备预检",
        "purpose": "展示分析仪、参考设备、GETCO 备份和状态寄存器证据。",
        "physical_signals": ["device_id", "GETCO1-9", "status_register", "reference_devices"],
        "calibration_gates": ["device_identity_required", "GETCO_backup_required"],
    },
    {
        "key": "pressure_channel_verify",
        "title": "压力通道验证",
        "purpose": "展示分析仪内部压力 P 与 COM22/PACE 的独立比对证据。",
        "physical_signals": ["COM22_pressure", "PACE_pressure", "analyzer_pressure_kpa", "delta_hpa"],
        "calibration_gates": ["pressure_channel_pass_required_before_component_calibration"],
    },
    {
        "key": "open_flow_sampling",
        "title": "开放流通采样",
        "purpose": "展示 CO2/H2O、露点、压力、ratio/signal 和稳定门禁证据。",
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
        "purpose": "解释每个点为何进入拟合、降级或拒绝。",
        "physical_signals": ["FrameQC", "PressureChannelQC", "DewpointHumidityQC", "FactorySignalQC"],
        "calibration_gates": ["a_grade_only_enters_formal_fit", "reject_reason_required"],
    },
    {
        "key": "report_review",
        "title": "候选系数与报告",
        "purpose": "展示候选系数、误差、复验、报告和未闭环项。",
        "physical_signals": ["old_GETCO", "candidate_coefficients", "reverification_error", "uncertainty_budget"],
        "calibration_gates": ["candidate_review_required", "post_write_reverification_required"],
    },
    {
        "key": "approval",
        "title": "审核与归档",
        "purpose": "展示审计、数据库归档、证书和受控写入门禁。",
        "physical_signals": ["report_hash", "coefficient_hash", "audit_events", "archive_status"],
        "calibration_gates": ["operator_cannot_self_approve", "database_import_or_dry_run_visible"],
    },
]


ROLE_PERMISSIONS: dict[str, dict[str, bool]] = {
    "operator": {
        "view": True,
        "select_plan": False,
        "change_plan": False,
        "run_sampling": False,
        "view_qc": True,
        "edit_qc_thresholds": False,
        "generate_candidate_coefficients": False,
        "approve_writes": False,
        "write_senco": False,
        "edit_high_risk_parameters": False,
        "approve_release": False,
    },
    "engineer": {
        "view": True,
        "select_plan": False,
        "change_plan": False,
        "run_sampling": False,
        "view_qc": True,
        "edit_qc_thresholds": False,
        "generate_candidate_coefficients": False,
        "approve_writes": False,
        "write_senco": False,
        "edit_high_risk_parameters": False,
        "approve_release": False,
    },
    "reviewer": {
        "view": True,
        "select_plan": False,
        "change_plan": False,
        "run_sampling": False,
        "view_qc": True,
        "edit_qc_thresholds": False,
        "generate_candidate_coefficients": False,
        "approve_writes": False,
        "write_senco": False,
        "edit_high_risk_parameters": False,
        "approve_release": False,
    },
    "admin": {
        "view": True,
        "select_plan": False,
        "change_plan": False,
        "run_sampling": False,
        "view_qc": True,
        "edit_qc_thresholds": False,
        "generate_candidate_coefficients": False,
        "approve_writes": False,
        "write_senco": False,
        "edit_high_risk_parameters": False,
        "approve_release": False,
    },
}


STATUS_LABELS: dict[str, str] = {
    "pass": "通过",
    "ready": "就绪",
    "ready_for_reviewer": "可评审",
    "formal_release_ready": "可正式签发",
    "in_progress": "进行中",
    "review_required": "需复核",
    "ready_for_next_action": "可执行下一步",
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
    "error": "错误",
    "not_releasable": "不可发布",
    "not_attempted": "未执行",
    "write_attempted": "已发生写入",
    "dry_run": "数据库 dry-run",
    "import": "已导入数据库",
    "skip": "跳过",
    "skipped": "跳过",
    "present": "已发现",
    "not_found": "未发现",
    "authorization_required": "需授权",
    "blocked_controlled_gate": "受控门禁",
    "waiting_for_artifacts": "等待证据",
    "manual_review": "人工评审",
    "planned_controlled_gates": "受控计划",
}


STATUS_TONE: dict[str, str] = {
    "pass": "good",
    "ready": "good",
    "ready_for_reviewer": "good",
    "formal_release_ready": "good",
    "demonstrated_calibratable_for_verified_scope": "good",
    "present": "good",
    "import": "good",
    "review_ready": "info",
    "planned_controlled_gates": "info",
    "authorization_required": "info",
    "manual_review": "info",
    "dry_run": "warn",
    "draft_only": "warn",
    "partial": "warn",
    "pending": "warn",
    "waiting_for_artifacts": "warn",
    "not_attempted": "warn",
    "missing": "bad",
    "not_found": "bad",
    "blocked": "bad",
    "blocked_controlled_gate": "bad",
    "fail": "bad",
    "error": "bad",
    "not_releasable": "bad",
    "write_attempted": "bad",
}


STATUS_SEVERITY = [
    "error",
    "fail",
    "blocked",
    "blocked_controlled_gate",
    "missing",
    "not_found",
    "not_releasable",
    "write_attempted",
    "partial",
    "waiting_for_artifacts",
    "authorization_required",
    "pending",
    "draft_only",
    "dry_run",
    "not_attempted",
    "manual_review",
    "planned_controlled_gates",
    "review_ready",
    "ready_for_reviewer",
    "demonstrated_calibratable_for_verified_scope",
    "formal_release_ready",
    "present",
    "ready",
    "pass",
]


STAGE_TO_PAGE: dict[str, str] = {
    "full_flow_contract_gate": "dashboard",
    "formal_initialization": "precheck",
    "plan_snapshot": "plan_select",
    "precheck": "precheck",
    "pressure_quick_check": "pressure_channel_verify",
    "pressure_channel_calibration": "pressure_channel_verify",
    "co2_open_flow": "open_flow_sampling",
    "h2o_open_flow": "open_flow_sampling",
    "co2_open_flow_sampling": "open_flow_sampling",
    "h2o_open_flow_sampling": "open_flow_sampling",
    "qc_classification": "qc_review",
    "post_run_coefficient_executor": "report_review",
    "reports": "report_review",
    "formal_report": "report_review",
    "database_import": "approval",
    "controlled_component_write_placeholder": "approval",
    "controlled_write": "approval",
}


CARD_TO_PAGE: dict[str, str] = {
    "formal_plan": "plan_select",
    "preflight": "precheck",
    "preflight_status": "precheck",
    "pressure_quick_check": "pressure_channel_verify",
    "open_flow_samples": "open_flow_sampling",
    "qc_package": "qc_review",
    "post_write_reverification": "report_review",
    "report_release": "report_review",
    "database_import": "approval",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _status_label(status: str | None) -> str:
    if not status:
        return "未知"
    return STATUS_LABELS.get(status, status)


def _tone(status: str | None) -> str:
    if not status:
        return "warn"
    return STATUS_TONE.get(status, "warn")


def _worst_status(statuses: list[str]) -> str:
    normalized = [status for status in statuses if status]
    if not normalized:
        return "pending"
    for status in STATUS_SEVERITY:
        if status in normalized:
            return status
    return normalized[0]


def _cards_by_key(workbench_model: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    cards: dict[str, Mapping[str, Any]] = {}
    for card in _as_list(workbench_model.get("cards")):
        if isinstance(card, Mapping) and card.get("key"):
            cards[str(card["key"])] = card
    return cards


def _stage_rows(run_evidence_status: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows = run_evidence_status.get("stage_statuses")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, Mapping)]
    return []


def _stage_reason(row: Mapping[str, Any]) -> str:
    title = row.get("title") or row.get("stage_id") or row.get("step_id") or "未命名阶段"
    reason = row.get("reason") or "无原因说明"
    status = _status_label(str(row.get("status") or "pending"))
    return f"{title}: {status} - {reason}"


def _card_reason(card: Mapping[str, Any]) -> str:
    key = card.get("key") or "未命名卡片"
    status = _status_label(str(card.get("status") or "pending"))
    blockers = "；".join(str(item) for item in _as_list(card.get("blockers")) if item)
    metric = card.get("metric")
    detail = blockers or metric or "无阻塞说明"
    return f"{key}: {status} - {detail}"


def _page_stage_rows(page_key: str, run_evidence_status: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [
        row
        for row in _stage_rows(run_evidence_status)
        if STAGE_TO_PAGE.get(str(row.get("stage_id") or row.get("step_id") or "")) == page_key
    ]


def _page_card_rows(page_key: str, workbench_model: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [
        card
        for card in _cards_by_key(workbench_model).values()
        if CARD_TO_PAGE.get(str(card.get("key") or "")) == page_key
    ]


def _page_status(page_key: str, workbench_model: Mapping[str, Any], run_evidence_status: Mapping[str, Any]) -> str:
    statuses: list[str] = []
    for row in _page_stage_rows(page_key, run_evidence_status):
        statuses.append(str(row.get("status") or "pending"))
    for card in _page_card_rows(page_key, workbench_model):
        statuses.append(str(card.get("status") or "pending"))
    if page_key == "dashboard":
        statuses.append(str(workbench_model.get("preflight_status") or "pending"))
    return _worst_status(statuses)


def _page_blockers(page_key: str, workbench_model: Mapping[str, Any], run_evidence_status: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    for row in _page_stage_rows(page_key, run_evidence_status):
        status = str(row.get("status") or "pending")
        if status != "pass":
            blockers.append(_stage_reason(row))
    for card in _page_card_rows(page_key, workbench_model):
        status = str(card.get("status") or "pending")
        card_blockers = [str(item) for item in _as_list(card.get("blockers")) if item]
        if status != "pass" or card_blockers:
            blockers.append(_card_reason(card))
            blockers.extend(card_blockers)
    if not blockers:
        blockers.append("无阻塞；等待对应证据刷新。")
    return blockers


def _full_flow_stage_manifest_panel(run_evidence_status: Mapping[str, Any]) -> dict[str, Any]:
    manifest = run_evidence_status.get("full_flow_stage_manifest")
    if not isinstance(manifest, Mapping):
        return {
            "key": "full_flow_stage_manifest",
            "available": False,
            "status": "not_found",
            "label": _status_label("not_found"),
            "status_label": _status_label("not_found"),
            "tone": _tone("not_found"),
            "source_path": None,
            "schema": "",
            "stage_count": 0,
            "current_manifest_stage": None,
            "one_button_live_runner_ready": False,
            "status_counts": {},
            "detail": "stage_manifest_missing",
            "attention_rows": [],
        }

    rows = [row for row in _as_list(manifest.get("stage_statuses")) if isinstance(row, Mapping)]
    attention_rows: list[dict[str, Any]] = []
    for row in rows:
        status = str(row.get("status") or "pending")
        if status in {"pass", "not_attempted"}:
            continue
        attention_rows.append(
            {
                "order": row.get("order"),
                "step_id": row.get("step_id") or row.get("stage_id"),
                "title": row.get("title"),
                "phase": row.get("phase"),
                "status": status,
                "status_label": _status_label(status),
                "reason": row.get("reason"),
            }
        )
    attention_rows = attention_rows[:12]
    panel_status = "planned_controlled_gates" if attention_rows else "pass"
    current = manifest.get("current_manifest_stage") or "none"
    one_button = bool(manifest.get("one_button_live_runner_ready"))
    return {
        "key": "full_flow_stage_manifest",
        "available": True,
        "status": panel_status,
        "label": _status_label(panel_status),
        "status_label": _status_label(panel_status),
        "tone": _tone(panel_status),
        "source_path": manifest.get("source_path"),
        "schema": manifest.get("schema") or "",
        "stage_count": int(manifest.get("stage_count") or len(rows)),
        "current_manifest_stage": manifest.get("current_manifest_stage"),
        "one_button_live_runner_ready": one_button,
        "status_counts": dict(manifest.get("status_counts") or {}),
        "detail": f"current={current}; one_button_live_runner_ready={one_button}",
        "attention_rows": attention_rows,
    }


def _formal_run_status_panel(formal_run_status: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(formal_run_status, Mapping) or not formal_run_status:
        return {
            "key": "formal_run_status",
            "available": False,
            "status": "not_found",
            "label": _status_label("not_found"),
            "status_label": _status_label("not_found"),
            "tone": _tone("not_found"),
            "overall_status": "not_found",
            "current_stage": "",
            "next_action": "formal_run_status_missing",
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "can_continue_physical_flow": False,
            "detail": "formal_run_status_missing",
        }

    overall_status = str(formal_run_status.get("overall_status") or "pending")
    current_stage = str(formal_run_status.get("current_stage") or "")
    next_action = str(formal_run_status.get("next_action") or "")
    formal_release_allowed = bool(formal_run_status.get("formal_release_allowed"))
    database_import_allowed = bool(formal_run_status.get("database_import_allowed"))
    can_continue_physical_flow = bool(formal_run_status.get("can_continue_physical_flow"))
    detail = (
        f"当前阶段={current_stage or 'unknown'}; "
        f"下一步={next_action or 'none'}; "
        f"可继续物理流程={can_continue_physical_flow}; "
        f"正式放行={formal_release_allowed}; "
        f"数据库导入={database_import_allowed}"
    )
    return {
        "key": "formal_run_status",
        "available": True,
        "status": overall_status,
        "label": _status_label(overall_status),
        "status_label": _status_label(overall_status),
        "tone": _tone(overall_status),
        "overall_status": overall_status,
        "current_stage": current_stage,
        "next_action": next_action,
        "formal_release_allowed": formal_release_allowed,
        "database_import_allowed": database_import_allowed,
        "can_continue_physical_flow": can_continue_physical_flow,
        "detail": detail,
        "physical_boundaries": dict(formal_run_status.get("physical_boundaries") or {}),
    }


def _summary_cards(
    workbench_model: Mapping[str, Any],
    run_evidence_status: Mapping[str, Any],
    calibration_capability: Mapping[str, Any],
    archive_index: Mapping[str, Any],
    formal_run_status: Mapping[str, Any],
) -> list[dict[str, Any]]:
    cards = _cards_by_key(workbench_model)
    capability_status = str(
        calibration_capability.get("status")
        or calibration_capability.get("capability_status")
        or "pending"
    )
    method_status = (
        "pass"
        if calibration_capability.get("method_backbone_ready") is True
        else str(cards.get("formal_plan", {}).get("status") or "pending")
    )
    verification_rollup = calibration_capability.get("verification_rollup") or {}
    if not isinstance(verification_rollup, Mapping):
        verification_rollup = {}
    verification_status = str(
        verification_rollup.get("status")
        or cards.get("post_write_reverification", {}).get("status")
        or "pending"
    )
    max_error = verification_rollup.get("max_abs_error_pct")
    if isinstance(max_error, (int, float)):
        verification_detail = f"最大相对误差 {max_error:.6g}%"
    else:
        verification_detail = "未发现复验证据"

    formal_status_panel = _formal_run_status_panel(formal_run_status)
    if formal_status_panel["available"]:
        formal_release_status = (
            "formal_release_ready"
            if formal_status_panel["formal_release_allowed"]
            else str(formal_status_panel["overall_status"] or "pending")
        )
    else:
        formal_release_status = (
            "formal_release_ready"
            if calibration_capability.get("formal_release_ready") is True
            else capability_status
        )
    archive_database = archive_index.get("database") if isinstance(archive_index.get("database"), Mapping) else {}
    database_status = str(
        ("ready" if formal_status_panel["available"] and formal_status_panel["database_import_allowed"] else "")
        or archive_database.get("mode")
        or run_evidence_status.get("database_mode")
        or cards.get("database_import", {}).get("status")
        or "pending"
    )
    manifest_panel = _full_flow_stage_manifest_panel(run_evidence_status)
    result = [
        {
            "key": "formal_run_status",
            "title": "正式运行状态",
            "status": formal_status_panel["status"],
            "label": formal_status_panel["status_label"],
            "status_label": formal_status_panel["status_label"],
            "tone": formal_status_panel["tone"],
            "detail": formal_status_panel["detail"],
        },
        {
            "key": "method_backbone",
            "title": "方法主干",
            "status": method_status,
            "label": _status_label(method_status),
            "status_label": _status_label(method_status),
            "tone": _tone(method_status),
            "detail": "开放流通 CO2/H2O 主校准 + 压力独立验证 + 后置复验。",
        },
        {
            "key": "verification",
            "title": "写后复验",
            "status": verification_status,
            "label": _status_label(verification_status),
            "status_label": _status_label(verification_status),
            "tone": _tone(verification_status),
            "detail": verification_detail,
        },
        {
            "key": "formal_release",
            "title": "正式放行",
            "status": formal_release_status,
            "label": _status_label(formal_release_status),
            "status_label": _status_label(formal_release_status),
            "tone": _tone(formal_release_status),
            "detail": "正式签发仍要求报告、数据库、审计和证据包闭环。",
        },
        {
            "key": "database",
            "title": "数据库归档",
            "status": database_status,
            "label": _status_label(database_status),
            "status_label": _status_label(database_status),
            "tone": _tone(database_status),
            "detail": "数据库用于索引证据链；原始帧仍以文件证据包保留。",
        },
        {
            "key": "full_flow_stage_manifest",
            "title": "全流程阶段清单",
            "status": manifest_panel["status"],
            "label": manifest_panel["status_label"],
            "status_label": manifest_panel["status_label"],
            "tone": manifest_panel["tone"],
            "detail": manifest_panel["detail"],
        },
    ]
    return result


def build_operation_console_model(
    *,
    workbench_model: Mapping[str, Any] | None = None,
    run_evidence_status: Mapping[str, Any] | None = None,
    calibration_capability: Mapping[str, Any] | None = None,
    archive_index: Mapping[str, Any] | None = None,
    formal_run_status: Mapping[str, Any] | None = None,
    role: str = "operator",
) -> dict[str, Any]:
    """Build a serializable read-only operation-console model."""

    workbench_model = workbench_model or {}
    run_evidence_status = run_evidence_status or {}
    calibration_capability = calibration_capability or {}
    archive_index = archive_index or {}
    formal_run_status = formal_run_status or {}
    role_permissions = ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS["operator"]).copy()
    stage_manifest_panel = _full_flow_stage_manifest_panel(run_evidence_status)
    formal_status_panel = _formal_run_status_panel(formal_run_status)
    summary_cards = _summary_cards(
        workbench_model,
        run_evidence_status,
        calibration_capability,
        archive_index,
        formal_run_status,
    )
    pages: list[dict[str, Any]] = []
    for definition in PAGE_DEFINITIONS:
        key = str(definition["key"])
        status = _page_status(key, workbench_model, run_evidence_status)
        stage_rows = _page_stage_rows(key, run_evidence_status)
        card_rows = _page_card_rows(key, workbench_model)
        pages.append(
            {
                **definition,
                "status": status,
                "status_label": _status_label(status),
                "tone": _tone(status),
                "read_only": True,
                "device_control_enabled": False,
                "blockers": _page_blockers(key, workbench_model, run_evidence_status),
                "evidence_refs": [
                    {
                        "kind": "stage",
                        "key": str(row.get("stage_id") or row.get("step_id") or ""),
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

    run_id = (
        workbench_model.get("run_id")
        or run_evidence_status.get("run_id")
        or archive_index.get("run_id")
        or "unknown"
    )
    run_dir = (
        workbench_model.get("run_dir")
        or run_evidence_status.get("run_dir")
        or archive_index.get("run_dir")
        or ""
    )
    capability_status = str(
        calibration_capability.get("status")
        or calibration_capability.get("capability_status")
        or "pending"
    )
    model = {
        "schema": "v1_5_operation_console_v1",
        "schema_version": "v1_5_operation_console_v1",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "run_dir": run_dir,
        "role": role,
        "role_permissions": role_permissions,
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
        "physical_boundary_statement": (
            "本操作台仅展示证据状态；不打开串口、不控制水路/气路、不控制阀或 PACE、"
            "不写 SENCO、不清除系数、不修改分析仪 ID。"
        ),
        "source_evidence": {
            "has_workbench_model": bool(workbench_model),
            "has_run_evidence_status": bool(run_evidence_status),
            "has_formal_run_status": bool(formal_run_status),
            "has_calibration_capability": bool(calibration_capability),
            "has_archive_index": bool(archive_index),
            "has_full_flow_stage_manifest": stage_manifest_panel["available"],
        },
        "summary_cards": summary_cards,
        "stage_manifest_panel": stage_manifest_panel,
        "formal_run_status_panel": formal_status_panel,
        "pages": pages,
    }
    return model


def _render_summary_cards(cards: list[Mapping[str, Any]]) -> str:
    items = []
    for card in cards:
        items.append(
            "<section class='card {tone}'>"
            "<h3>{title}</h3>"
            "<p class='status'>{status}</p>"
            "<p>{detail}</p>"
            "</section>".format(
                tone=_esc(card.get("tone")),
                title=_esc(card.get("title")),
                status=_esc(card.get("status_label")),
                detail=_esc(card.get("detail")),
            )
        )
    return "\n".join(items)


def _render_manifest_panel(panel: Mapping[str, Any]) -> str:
    rows = []
    for row in _as_list(panel.get("attention_rows")):
        if not isinstance(row, Mapping):
            continue
        rows.append(
            "<tr><td>{order}</td><td>{step}</td><td>{phase}</td><td>{status}</td><td>{reason}</td></tr>".format(
                order=_esc(row.get("order")),
                step=_esc(row.get("step_id") or row.get("title")),
                phase=_esc(row.get("phase")),
                status=_esc(row.get("status_label")),
                reason=_esc(row.get("reason")),
            )
        )
    if not rows:
        rows.append("<tr><td colspan='5'>暂无需要人工关注的阶段。</td></tr>")
    counts = ", ".join(f"{key}={value}" for key, value in dict(panel.get("status_counts") or {}).items())
    return """
<section class='panel'>
  <h2>全流程阶段清单</h2>
  <p>状态：<strong>{status}</strong>；当前阶段：{current}；one_button_live_runner_ready={ready}</p>
  <p>状态计数：{counts}</p>
  <table>
    <thead><tr><th>序号</th><th>阶段</th><th>环节</th><th>状态</th><th>原因</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</section>
""".format(
        status=_esc(panel.get("status_label")),
        current=_esc(panel.get("current_manifest_stage") or "none"),
        ready=_esc(panel.get("one_button_live_runner_ready")),
        counts=_esc(counts or "无"),
        rows="\n".join(rows),
    )


def _render_pages(pages: list[Mapping[str, Any]]) -> str:
    rows = []
    for page in pages:
        rows.append(
            "<tr class='{tone}'><td>{title}</td><td>{status}</td><td>{purpose}</td><td>{blockers}</td></tr>".format(
                tone=_esc(page.get("tone")),
                title=_esc(page.get("title")),
                status=_esc(page.get("status_label")),
                purpose=_esc(page.get("purpose")),
                blockers=_esc("；".join(str(item) for item in _as_list(page.get("blockers")))),
            )
        )
    return """
<section class='panel'>
  <h2>页面与门禁</h2>
  <table>
    <thead><tr><th>页面</th><th>状态</th><th>物理意义</th><th>阻塞 / 说明</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</section>
""".format(rows="\n".join(rows))


def render_operation_console_html(model: Mapping[str, Any]) -> str:
    """Render the operation-console model as standalone UTF-8 HTML."""

    source = model.get("source_evidence") if isinstance(model.get("source_evidence"), Mapping) else {}
    source_text = "；".join(f"{key}={value}" for key, value in dict(source).items())
    return """<!doctype html>
<html lang='zh-CN'>
<head>
  <meta charset='utf-8'>
  <title>V1.5 正式校准操作台</title>
  <style>
    body {{ font-family: "Microsoft YaHei", "Segoe UI", sans-serif; margin: 0; background: #f5f7fb; color: #1f2937; }}
    header {{ background: #102033; color: #fff; padding: 22px 32px; }}
    main {{ padding: 24px 32px; }}
    h1, h2, h3 {{ margin-top: 0; }}
    .boundary {{ background: #fff7ed; border-left: 5px solid #f97316; padding: 12px 16px; margin: 16px 0; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 14px; margin: 18px 0; }}
    .card, .panel {{ background: #fff; border: 1px solid #dbe3ee; border-radius: 8px; padding: 14px 16px; box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06); }}
    .status {{ font-weight: 700; }}
    .good .status, tr.good td:nth-child(2) {{ color: #047857; }}
    .warn .status, tr.warn td:nth-child(2) {{ color: #b45309; }}
    .bad .status, tr.bad td:nth-child(2) {{ color: #b91c1c; }}
    .info .status, tr.info td:nth-child(2) {{ color: #1d4ed8; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 8px 10px; text-align: left; vertical-align: top; }}
    th {{ background: #eef2f7; }}
    code {{ background: #eef2f7; padding: 2px 4px; border-radius: 4px; }}
  </style>
</head>
<body>
  <header>
    <h1>V1.5 正式校准操作台</h1>
    <p>run_id={run_id}；角色={role}；生成时间={generated_at}</p>
  </header>
  <main>
    <section class='boundary'><strong>安全边界：</strong>{boundary}</section>
    <p><strong>证据来源：</strong>{source}</p>
    <section class='cards'>{cards}</section>
    {manifest}
    {pages}
  </main>
</body>
</html>
""".format(
        run_id=_esc(model.get("run_id")),
        role=_esc(model.get("role")),
        generated_at=_esc(model.get("generated_at")),
        boundary=_esc(model.get("physical_boundary_statement")),
        source=_esc(source_text),
        cards=_render_summary_cards(_as_list(model.get("summary_cards"))),
        manifest=_render_manifest_panel(model.get("stage_manifest_panel") or {}),
        pages=_render_pages(_as_list(model.get("pages"))),
    )


def write_operation_console(
    *,
    output_dir: str | Path,
    workbench_model: Mapping[str, Any] | None = None,
    run_evidence_status: Mapping[str, Any] | None = None,
    calibration_capability: Mapping[str, Any] | None = None,
    archive_index: Mapping[str, Any] | None = None,
    formal_run_status: Mapping[str, Any] | None = None,
    role: str = "operator",
) -> dict[str, Path]:
    """Write JSON and HTML operation-console artifacts."""

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    model = build_operation_console_model(
        workbench_model=workbench_model,
        run_evidence_status=run_evidence_status,
        calibration_capability=calibration_capability,
        archive_index=archive_index,
        formal_run_status=formal_run_status,
        role=role,
    )
    model_path = output / "v1_5_operation_console.json"
    html_path = output / "v1_5_operation_console.html"
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
