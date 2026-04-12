from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .acceptance_model import build_user_visible_evidence_boundary
from .governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES as _GOV_FILENAMES


METROLOGY_CALIBRATION_CONTRACT_FILENAME = _GOV_FILENAMES["metrology_calibration_contract"]

_REFERENCE_DEVICE_CLASSES = [
    {
        "device_class": "dewpoint_meter",
        "required_evidence_fields": [
            "instrument_id",
            "model",
            "serial_number",
            "certificate_id",
            "calibration_date",
            "due_date",
            "reference_role",
        ],
    },
    {
        "device_class": "digital_pressure_meter",
        "required_evidence_fields": [
            "instrument_id",
            "model",
            "serial_number",
            "certificate_id",
            "calibration_date",
            "due_date",
            "pressure_range",
        ],
    },
    {
        "device_class": "digital_thermometer",
        "required_evidence_fields": [
            "instrument_id",
            "model",
            "serial_number",
            "certificate_id",
            "calibration_date",
            "due_date",
            "measurement_role",
        ],
    },
    {
        "device_class": "temperature_chamber",
        "required_evidence_fields": [
            "instrument_id",
            "model",
            "serial_number",
            "equipment_record_id",
            "temperature_range",
            "setpoint_traceability",
        ],
    },
    {
        "device_class": "humidity_generator",
        "required_evidence_fields": [
            "instrument_id",
            "model",
            "serial_number",
            "equipment_record_id",
            "humidity_range",
            "source_water_quality",
        ],
    },
    {
        "device_class": "pressure_controller",
        "required_evidence_fields": [
            "instrument_id",
            "model",
            "serial_number",
            "equipment_record_id",
            "pressure_range",
            "control_mode",
        ],
    },
    {
        "device_class": "standard_gas_source",
        "required_evidence_fields": [
            "gas_id",
            "gas_name",
            "certificate_id",
            "batch_id",
            "purity",
            "cross_interference_note",
        ],
    },
]

_STEP2_EXECUTE_NOW_ITEMS = [
    "reference_traceability_contract_schema",
    "calibration_execution_contract_schema",
    "data_quality_gate_schema",
    "uncertainty_budget_template_schema",
    "coefficient_verification_contract_schema",
    "evidence_traceability_contract_schema",
    "reviewer_reporting_contract_schema",
]

_STAGE3_EXECUTION_ITEMS = [
    "real_reference_instrument_enforcement",
    "certificate_cycle_hard_blocking",
    "real_run_uncertainty_result",
    "coefficient_writeback_real_acceptance",
    "real_acceptance_pass_fail",
]


def build_metrology_calibration_contract(
    *,
    run_id: str,
    simulation_mode: bool,
    config_governance_handoff: dict[str, Any] | None = None,
) -> dict[str, Any]:
    governance = dict(config_governance_handoff or {})
    boundary = build_user_visible_evidence_boundary(simulation_mode=simulation_mode)

    simulation_only = bool(governance.get("simulation_only", simulation_mode))
    real_port_device_count = int(governance.get("real_port_device_count", 0) or 0)
    engineering_only_flag_count = int(governance.get("engineering_only_flag_count", 0) or 0)
    enabled_engineering_flags = _normalize_string_list(governance.get("enabled_engineering_flags"))
    overall_status = "contract_ready_for_stage3_bridge"
    blocking_items: list[str] = []
    warning_items = [
        "simulation_offline_headless_only",
        "not_real_acceptance_evidence",
        "stage3_real_validation_pending",
    ]
    if not simulation_mode or not simulation_only or real_port_device_count != 0:
        warning_items.append("simulation_only_boundary_not_satisfied")
    if engineering_only_flag_count or enabled_engineering_flags:
        warning_items.append("engineering_only_flags_enabled")

    stage_assignment = {
        "execute_now_in_step2_tail": list(_STEP2_EXECUTE_NOW_ITEMS),
        "defer_to_stage3_real_validation": list(_STAGE3_EXECUTION_ITEMS),
    }
    notes = [
        "metrology_grade_design_contract",
        "step2_tail_step3_bridge",
        "simulation_offline_headless_only",
        "not_real_acceptance_evidence",
        "default_path_unchanged",
    ]

    reference_traceability_contract = {
        "device_classes": list(_REFERENCE_DEVICE_CLASSES),
        "placeholder_only": True,
        "certificate_cycle_hard_blocking_stage": "stage3_real_validation",
        "step2_runtime_hard_blocking": False,
        "required_reference_chain_declaration": True,
    }
    calibration_execution_contract = {
        "temperature_point_contract": {
            "required": True,
            "source": "plan_or_points_matrix",
            "stage": "step2_tail",
        },
        "pressure_point_contract": {
            "required": True,
            "supports_ambient_open": True,
            "supports_sealed_hold": True,
            "supports_pressure_control": True,
        },
        "gas_point_contract": {
            "required": True,
            "supports_standard_gas_source": True,
            "supports_water_path": True,
            "supports_gas_path": True,
        },
        "route_contexts": [
            "water_path",
            "gas_path",
            "ambient_open",
            "sealed_hold",
            "pressure_control",
        ],
        "sampling_window_contract": {
            "sample_window_seconds_required": True,
            "frame_count_required": True,
        },
        "stabilization_prerequisites": [
            "pressure_stability_gate",
            "dewpoint_stability_gate",
            "route_context_complete",
        ],
        "preseal_postseal_action_order": [
            "ambient_open_reference_capture",
            "preseal_stabilization",
            "sealed_or_pressure_control_transition",
            "postseal_quality_guard",
        ],
        "default_workflow_unchanged": True,
    }
    data_quality_contract = {
        "gates": [
            {"gate_id": "pressure_stability_gate", "required": True},
            {"gate_id": "dewpoint_stability_gate", "required": True},
            {"gate_id": "latest_frame_freshness", "required": True},
            {"gate_id": "reference_provenance", "required": True},
            {"gate_id": "route_context_completeness", "required": True},
            {"gate_id": "coefficient_fitting_input_completeness", "required": True},
        ],
        "default_chain_extension": False,
        "schema_only_in_step2": True,
    }
    uncertainty_budget_template = {
        "template_only": True,
        "final_result_stage": "stage3_real_validation",
        "components": [
            "repeatability",
            "reference_standard_uncertainty",
            "resolution",
            "temperature_effect",
            "pressure_effect",
            "humidity_dewpoint_stability",
            "gas_purity_cross_interference",
            "fit_residual_backfit_error",
            "coefficient_write_read_consistency",
        ],
    }
    coefficient_verification_contract = {
        "source_version_required": True,
        "pre_write_validation_required": True,
        "post_write_readback_required": True,
        "model_reconciliation_required": True,
        "real_device_acceptance_stage": "stage3_real_validation",
    }
    evidence_traceability_contract = {
        "required_entities": [
            "raw_data",
            "reference_data",
            "summary",
            "report",
            "coefficients",
            "run_metadata",
            "governance_artifact",
        ],
        "required_link_keys": [
            "run_id",
            "point_id",
            "sample_id",
            "analyzer_id",
            "route_id",
            "artifact_role",
            "config_hash",
            "coefficient_version",
        ],
        "simulation_evidence_only": True,
    }
    reporting_contract = {
        "reviewer_summary_language": "zh-CN",
        "raw_appendix_format": "json",
        "governance_notice_code": "metrology_design_contract_only",
        "not_real_acceptance_statement_required": True,
        "required_sections": [
            "reviewer_summary",
            "raw_machine_readable_appendix",
            "metrology_governance_notice",
        ],
    }

    reviewer_display = _build_reviewer_display(
        overall_status=overall_status,
        blocking_items=blocking_items,
        warning_items=warning_items,
    )

    return {
        "schema_version": "1.0",
        "artifact_type": "metrology_calibration_contract",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "phase": "step2_tail_step3_bridge",
        "stage_assignment": stage_assignment,
        "mode": "simulation_only",
        "overall_status": overall_status,
        "real_acceptance_ready": False,
        "evidence_mode": "simulation_offline_headless",
        "evidence_source": boundary.get("evidence_source"),
        "not_real_acceptance_evidence": True,
        "acceptance_level": boundary.get("acceptance_level"),
        "promotion_state": boundary.get("promotion_state"),
        "reference_traceability_contract": reference_traceability_contract,
        "calibration_execution_contract": calibration_execution_contract,
        "data_quality_contract": data_quality_contract,
        "uncertainty_budget_template": uncertainty_budget_template,
        "coefficient_verification_contract": coefficient_verification_contract,
        "evidence_traceability_contract": evidence_traceability_contract,
        "reporting_contract": reporting_contract,
        "stage3_execution_items": list(_STAGE3_EXECUTION_ITEMS),
        "blocking_items": blocking_items,
        "warning_items": warning_items,
        "notes": notes,
        "reviewer_display": reviewer_display,
    }


def _build_reviewer_display(
    *,
    overall_status: str,
    blocking_items: list[str],
    warning_items: list[str],
) -> dict[str, Any]:
    status_line = (
        "阶段状态：计量级校准体系设计合同已完成第二阶段制度化，可作为第三阶段准入桥。"
        if overall_status == "contract_ready_for_stage3_bridge"
        else "阶段状态：计量级校准体系设计合同仍需补齐 simulation-only 边界后再作为第三阶段准入桥。"
    )
    summary_text = (
        "计量级设计合同：当前已把参考溯源、执行 schema、数据质量 gate、不确定度模板、系数校验、证据追踪与报告约定固化为 V2-only 离线治理工件；"
        "它服务于 Step 2 收尾与 Step 3 准入准备，不是 real acceptance 结论。"
    )
    execute_now_text = (
        "当前执行（第二阶段收尾）：固化参考标准与溯源链 contract、校准执行 contract、数据质量 contract、"
        "不确定度模板、系数验证 contract、证据追踪 contract 和 reviewer 摘要输出。"
    )
    defer_to_stage3_text = (
        "第三阶段再执行：真实参考表强制执行、真实证书/检定周期硬阻塞、基于真实 run 的最终不确定度结果、"
        "真机系数写入 acceptance、real acceptance pass/fail。"
    )
    section_lines = [
        "参考溯源链：已定义露点仪、数字压力计、数字温度计、温箱、湿度发生器、压力控制器、标准气源的必填证据字段与占位约定。",
        "校准执行合同：已定义温度点、压力点、气点、水路/气路、ambient/sealed/pressure-control 场景、采样窗口与判稳前提。",
        "数据质量合同：已定义压力稳定、露点稳定、最新帧新鲜度、参考来源、上下文完整性与拟合输入完整性 gate schema。",
        "不确定度模板：当前只固化模板项，不输出真实最终不确定度结果。",
        "系数验证合同：已定义来源版本、写前校验、写后回读与模型对账，真机 acceptance 延后到第三阶段。",
        "证据追踪与报告：已定义 raw/reference/summary/report/coefficients/governance 之间的关联键与 reviewer 摘要要求。",
    ]
    blocking_text = "阻塞项：无。" if not blocking_items else "阻塞项：" + "、".join(blocking_items) + "。"
    warning_text = (
        "提示：本工件只描述计量级高标准设计合同与离线治理证据，不代表 real acceptance evidence；"
        "当前 warning code 包括 " + "、".join(warning_items) + "。"
    )
    return {
        "summary_text": summary_text,
        "status_line": status_line,
        "execute_now_text": execute_now_text,
        "defer_to_stage3_text": defer_to_stage3_text,
        "section_lines": section_lines,
        "blocking_text": blocking_text,
        "warning_text": warning_text,
    }


def _normalize_string_list(values: Any) -> list[str]:
    rows: list[str] = []
    for value in list(values or []):
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows
