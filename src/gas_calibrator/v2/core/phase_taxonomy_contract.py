from __future__ import annotations

from typing import Any
import re


TAXONOMY_CONTRACT_VERSION = "step2-taxonomy-contract-v1"

METHOD_CONFIRMATION_FAMILY = "method_confirmation"
UNCERTAINTY_INPUT_FAMILY = "uncertainty_input"
TRACEABILITY_NODE_FAMILY = "traceability_node"
GAP_CLASSIFICATION_FAMILY = "gap_classification"
GAP_SEVERITY_FAMILY = "gap_severity"
REVIEWER_NEXT_STEP_TEMPLATE_FAMILY = "reviewer_next_step_template"

_PAYLOAD_COMPLETE_BUCKET = "actual_simulated_run_with_payload_complete"
_PAYLOAD_PARTIAL_BUCKET = "actual_simulated_run_with_payload_partial"
_TOKEN_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+")


def _entry(
    family: str,
    canonical_key: str,
    *,
    i18n_key: str,
    zh_label: str,
    en_label: str,
    aliases: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "family": family,
        "canonical_key": canonical_key,
        "i18n_key": i18n_key,
        "zh_label": zh_label,
        "en_label": en_label,
        "aliases": tuple(str(item).strip() for item in aliases if str(item).strip()),
    }


_TAXONOMY_REGISTRY: dict[str, dict[str, dict[str, Any]]] = {
    METHOD_CONFIRMATION_FAMILY: {},
    UNCERTAINTY_INPUT_FAMILY: {},
    TRACEABILITY_NODE_FAMILY: {},
    GAP_CLASSIFICATION_FAMILY: {},
    GAP_SEVERITY_FAMILY: {},
    REVIEWER_NEXT_STEP_TEMPLATE_FAMILY: {},
}

_PHASE_TAXONOMY_PROFILES: dict[tuple[str, str], dict[str, Any]] = {}

_ALIAS_INDEX: dict[str, dict[str, str]] = {}


_TAXONOMY_REGISTRY[METHOD_CONFIRMATION_FAMILY].update(
    {
        "ambient_baseline_stabilization_rule": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "ambient_baseline_stabilization_rule",
            i18n_key="taxonomy.method_confirmation.ambient_baseline_stabilization_rule",
            zh_label="环境基线稳定规则",
            en_label="Ambient baseline stabilization rule",
        ),
        "ambient_diagnostic_decision_threshold": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "ambient_diagnostic_decision_threshold",
            i18n_key="taxonomy.method_confirmation.ambient_diagnostic_decision_threshold",
            zh_label="环境诊断判定阈值",
            en_label="Ambient diagnostic decision threshold",
        ),
        "ambient_diagnostic_drift_review": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "ambient_diagnostic_drift_review",
            i18n_key="taxonomy.method_confirmation.ambient_diagnostic_drift_review",
            zh_label="环境诊断漂移复核",
            en_label="Ambient diagnostic drift review",
        ),
        "water_preseal_window_definition": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "water_preseal_window_definition",
            i18n_key="taxonomy.method_confirmation.water_preseal_window_definition",
            zh_label="水路 preseal 窗口定义",
            en_label="Water preseal window definition",
        ),
        "water_route_conditioning_repeatability": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "water_route_conditioning_repeatability",
            i18n_key="taxonomy.method_confirmation.water_route_conditioning_repeatability",
            zh_label="水路调理重复性",
            en_label="Water route conditioning repeatability",
        ),
        "water_preseal_release_criteria": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "water_preseal_release_criteria",
            i18n_key="taxonomy.method_confirmation.water_preseal_release_criteria",
            zh_label="水路 preseal 释放准则",
            en_label="Water preseal release criteria",
        ),
        "gas_preseal_window_definition": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "gas_preseal_window_definition",
            i18n_key="taxonomy.method_confirmation.gas_preseal_window_definition",
            zh_label="气路 preseal 窗口定义",
            en_label="Gas preseal window definition",
        ),
        "gas_route_conditioning_repeatability": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "gas_route_conditioning_repeatability",
            i18n_key="taxonomy.method_confirmation.gas_route_conditioning_repeatability",
            zh_label="气路调理重复性",
            en_label="Gas route conditioning repeatability",
        ),
        "gas_preseal_release_criteria": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "gas_preseal_release_criteria",
            i18n_key="taxonomy.method_confirmation.gas_preseal_release_criteria",
            zh_label="气路 preseal 释放准则",
            en_label="Gas preseal release criteria",
        ),
        "water_pressure_stabilization_hold_confirmation": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "water_pressure_stabilization_hold_confirmation",
            i18n_key="taxonomy.method_confirmation.water_pressure_stabilization_hold_confirmation",
            zh_label="水路压力稳定保持确认",
            en_label="Water pressure stabilization hold confirmation",
        ),
        "gas_pressure_stabilization_hold_confirmation": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "gas_pressure_stabilization_hold_confirmation",
            i18n_key="taxonomy.method_confirmation.gas_pressure_stabilization_hold_confirmation",
            zh_label="气路压力稳定保持确认",
            en_label="Gas pressure stabilization hold confirmation",
        ),
        "ambient_sample_ready_dwell_confirmation": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "ambient_sample_ready_dwell_confirmation",
            i18n_key="taxonomy.method_confirmation.ambient_sample_ready_dwell_confirmation",
            zh_label="环境 sample-ready 停留确认",
            en_label="Ambient sample-ready dwell confirmation",
        ),
        "ambient_sample_release_criteria": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "ambient_sample_release_criteria",
            i18n_key="taxonomy.method_confirmation.ambient_sample_release_criteria",
            zh_label="环境样本释放准则",
            en_label="Ambient sample release criteria",
        ),
        "water_sample_ready_dwell_confirmation": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "water_sample_ready_dwell_confirmation",
            i18n_key="taxonomy.method_confirmation.water_sample_ready_dwell_confirmation",
            zh_label="水路 sample-ready 停留确认",
            en_label="Water sample-ready dwell confirmation",
        ),
        "water_sample_release_criteria": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "water_sample_release_criteria",
            i18n_key="taxonomy.method_confirmation.water_sample_release_criteria",
            zh_label="水路样本释放准则",
            en_label="Water sample release criteria",
        ),
        "gas_sample_ready_dwell_confirmation": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "gas_sample_ready_dwell_confirmation",
            i18n_key="taxonomy.method_confirmation.gas_sample_ready_dwell_confirmation",
            zh_label="气路 sample-ready 停留确认",
            en_label="Gas sample-ready dwell confirmation",
        ),
        "gas_sample_release_criteria": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "gas_sample_release_criteria",
            i18n_key="taxonomy.method_confirmation.gas_sample_release_criteria",
            zh_label="气路样本释放准则",
            en_label="Gas sample release criteria",
        ),
        "recovery_retry_scenario_confirmation": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "recovery_retry_scenario_confirmation",
            i18n_key="taxonomy.method_confirmation.recovery_retry_scenario_confirmation",
            zh_label="恢复重试场景确认",
            en_label="Recovery retry scenario confirmation",
        ),
        "safe_recovery_procedure_confirmation": _entry(
            METHOD_CONFIRMATION_FAMILY,
            "safe_recovery_procedure_confirmation",
            i18n_key="taxonomy.method_confirmation.safe_recovery_procedure_confirmation",
            zh_label="安全恢复流程确认",
            en_label="Safe recovery procedure confirmation",
        ),
    }
)

_TAXONOMY_REGISTRY[UNCERTAINTY_INPUT_FAMILY].update(
    {
        "ambient_pressure_baseline": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "ambient_pressure_baseline",
            i18n_key="taxonomy.uncertainty_input.ambient_pressure_baseline",
            zh_label="环境压力基线",
            en_label="Ambient pressure baseline",
        ),
        "ambient_humidity_baseline": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "ambient_humidity_baseline",
            i18n_key="taxonomy.uncertainty_input.ambient_humidity_baseline",
            zh_label="环境湿度基线",
            en_label="Ambient humidity baseline",
        ),
        "ambient_temperature_baseline": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "ambient_temperature_baseline",
            i18n_key="taxonomy.uncertainty_input.ambient_temperature_baseline",
            zh_label="环境温度基线",
            en_label="Ambient temperature baseline",
        ),
        "humidity_reference_window": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "humidity_reference_window",
            i18n_key="taxonomy.uncertainty_input.humidity_reference_window",
            zh_label="湿度参考窗口",
            en_label="Humidity reference window",
        ),
        "preseal_pressure_term": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "preseal_pressure_term",
            i18n_key="taxonomy.uncertainty_input.preseal_pressure_term",
            zh_label="preseal 压力项",
            en_label="Preseal pressure term",
        ),
        "preseal_temperature_term": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "preseal_temperature_term",
            i18n_key="taxonomy.uncertainty_input.preseal_temperature_term",
            zh_label="preseal 温度项",
            en_label="Preseal temperature term",
        ),
        "reference_gas_window": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "reference_gas_window",
            i18n_key="taxonomy.uncertainty_input.reference_gas_window",
            zh_label="标准气窗口",
            en_label="Reference gas window",
        ),
        "humidity_reference": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "humidity_reference",
            i18n_key="taxonomy.uncertainty_input.humidity_reference",
            zh_label="湿度参考",
            en_label="Humidity reference",
        ),
        "pressure_reference": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "pressure_reference",
            i18n_key="taxonomy.uncertainty_input.pressure_reference",
            zh_label="压力参考",
            en_label="Pressure reference",
        ),
        "temperature_reference": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "temperature_reference",
            i18n_key="taxonomy.uncertainty_input.temperature_reference",
            zh_label="温度参考",
            en_label="Temperature reference",
        ),
        "reference_gas_value": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "reference_gas_value",
            i18n_key="taxonomy.uncertainty_input.reference_gas_value",
            zh_label="标准气值",
            en_label="Reference gas value",
        ),
        "ambient_stabilization_window": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "ambient_stabilization_window",
            i18n_key="taxonomy.uncertainty_input.ambient_stabilization_window",
            zh_label="环境稳定窗口",
            en_label="Ambient stabilization window",
        ),
        "ambient_pressure_drift_allowance": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "ambient_pressure_drift_allowance",
            i18n_key="taxonomy.uncertainty_input.ambient_pressure_drift_allowance",
            zh_label="环境压力漂移容差",
            en_label="Ambient pressure drift allowance",
        ),
        "humidity_stabilization_window": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "humidity_stabilization_window",
            i18n_key="taxonomy.uncertainty_input.humidity_stabilization_window",
            zh_label="湿度稳定窗口",
            en_label="Humidity stabilization window",
        ),
        "pressure_settling_allowance": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "pressure_settling_allowance",
            i18n_key="taxonomy.uncertainty_input.pressure_settling_allowance",
            zh_label="压力收敛容差",
            en_label="Pressure settling allowance",
        ),
        "reference_gas_stabilization_window": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "reference_gas_stabilization_window",
            i18n_key="taxonomy.uncertainty_input.reference_gas_stabilization_window",
            zh_label="标准气稳定窗口",
            en_label="Reference gas stabilization window",
        ),
        "retry_timing_tolerance": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "retry_timing_tolerance",
            i18n_key="taxonomy.uncertainty_input.retry_timing_tolerance",
            zh_label="重试时序容差",
            en_label="Retry timing tolerance",
        ),
        "fault_capture_debounce_window": _entry(
            UNCERTAINTY_INPUT_FAMILY,
            "fault_capture_debounce_window",
            i18n_key="taxonomy.uncertainty_input.fault_capture_debounce_window",
            zh_label="故障捕获消抖窗口",
            en_label="Fault capture debounce window",
        ),
    }
)

_TAXONOMY_REGISTRY[TRACEABILITY_NODE_FAMILY].update(
    {
        "ambient_environment_reference_chain": _entry(
            TRACEABILITY_NODE_FAMILY,
            "ambient_environment_reference_chain",
            i18n_key="taxonomy.traceability_node.ambient_environment_reference_chain",
            zh_label="环境参考链",
            en_label="Ambient environment reference chain",
        ),
        "ambient_pressure_reference_link": _entry(
            TRACEABILITY_NODE_FAMILY,
            "ambient_pressure_reference_link",
            i18n_key="taxonomy.traceability_node.ambient_pressure_reference_link",
            zh_label="环境压力参考链路",
            en_label="Ambient pressure reference link",
        ),
        "ambient_climate_baseline_stub": _entry(
            TRACEABILITY_NODE_FAMILY,
            "ambient_climate_baseline_stub",
            i18n_key="taxonomy.traceability_node.ambient_climate_baseline_stub",
            zh_label="环境气候基线骨架",
            en_label="Ambient climate baseline stub",
        ),
        "humidity_reference_chain": _entry(
            TRACEABILITY_NODE_FAMILY,
            "humidity_reference_chain",
            i18n_key="taxonomy.traceability_node.humidity_reference_chain",
            zh_label="湿度参考链",
            en_label="Humidity reference chain",
        ),
        "dew_point_reference_link": _entry(
            TRACEABILITY_NODE_FAMILY,
            "dew_point_reference_link",
            i18n_key="taxonomy.traceability_node.dew_point_reference_link",
            zh_label="露点参考链路",
            en_label="Dew-point reference link",
        ),
        "preseal_conditioning_window_stub": _entry(
            TRACEABILITY_NODE_FAMILY,
            "preseal_conditioning_window_stub",
            i18n_key="taxonomy.traceability_node.preseal_conditioning_window_stub",
            zh_label="preseal 调理窗口骨架",
            en_label="Preseal conditioning window stub",
        ),
        "standard_gas_chain": _entry(
            TRACEABILITY_NODE_FAMILY,
            "standard_gas_chain",
            i18n_key="taxonomy.traceability_node.standard_gas_chain",
            zh_label="标准气参考链",
            en_label="Standard gas chain",
        ),
        "pressure_reference_link": _entry(
            TRACEABILITY_NODE_FAMILY,
            "pressure_reference_link",
            i18n_key="taxonomy.traceability_node.pressure_reference_link",
            zh_label="压力参考链路",
            en_label="Pressure reference link",
        ),
        "temperature_reference_link": _entry(
            TRACEABILITY_NODE_FAMILY,
            "temperature_reference_link",
            i18n_key="taxonomy.traceability_node.temperature_reference_link",
            zh_label="温度参考链路",
            en_label="Temperature reference link",
        ),
        "sample_release_trace_stub": _entry(
            TRACEABILITY_NODE_FAMILY,
            "sample_release_trace_stub",
            i18n_key="taxonomy.traceability_node.sample_release_trace_stub",
            zh_label="样本释放 trace 骨架",
            en_label="Sample release trace stub",
        ),
        "software_event_log_chain": _entry(
            TRACEABILITY_NODE_FAMILY,
            "software_event_log_chain",
            i18n_key="taxonomy.traceability_node.software_event_log_chain",
            zh_label="软件事件日志链",
            en_label="Software event log chain",
        ),
        "recovery_audit_trail_stub": _entry(
            TRACEABILITY_NODE_FAMILY,
            "recovery_audit_trail_stub",
            i18n_key="taxonomy.traceability_node.recovery_audit_trail_stub",
            zh_label="恢复审计轨迹骨架",
            en_label="Recovery audit trail stub",
        ),
    }
)

_TAXONOMY_REGISTRY[GAP_CLASSIFICATION_FAMILY].update(
    {
        "ambient_baseline_payload_complete_anchor": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "ambient_baseline_payload_complete_anchor",
            i18n_key="taxonomy.gap_classification.ambient_baseline_payload_complete_anchor",
            zh_label="环境基线 payload 完整锚点",
            en_label="Ambient baseline payload-complete anchor",
        ),
        "ambient_baseline_trace_only_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "ambient_baseline_trace_only_gap",
            i18n_key="taxonomy.gap_classification.ambient_baseline_trace_only_gap",
            zh_label="环境基线仅 trace 缺口",
            en_label="Ambient baseline trace-only gap",
        ),
        "ambient_baseline_model_only_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "ambient_baseline_model_only_gap",
            i18n_key="taxonomy.gap_classification.ambient_baseline_model_only_gap",
            zh_label="环境基线仅模型缺口",
            en_label="Ambient baseline model-only gap",
        ),
        "ambient_baseline_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "ambient_baseline_gap",
            i18n_key="taxonomy.gap_classification.ambient_baseline_gap",
            zh_label="环境基线缺口",
            en_label="Ambient baseline gap",
        ),
        "ambient_baseline_reviewer_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "ambient_baseline_reviewer_gap",
            i18n_key="taxonomy.gap_classification.ambient_baseline_reviewer_gap",
            zh_label="环境基线审阅缺口",
            en_label="Ambient baseline reviewer gap",
        ),
        "conditioning_window_partial_payload": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "conditioning_window_partial_payload",
            i18n_key="taxonomy.gap_classification.conditioning_window_partial_payload",
            zh_label="调理窗口 payload 部分缺口",
            en_label="Conditioning window partial payload",
        ),
        "payload_complete_synthetic_reviewer_anchor": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "payload_complete_synthetic_reviewer_anchor",
            i18n_key="taxonomy.gap_classification.payload_complete_synthetic_reviewer_anchor",
            zh_label="payload 完整仿真审阅锚点",
            en_label="Payload-complete synthetic reviewer anchor",
        ),
        "ambient_sample_ready_payload_complete_anchor": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "ambient_sample_ready_payload_complete_anchor",
            i18n_key="taxonomy.gap_classification.ambient_sample_ready_payload_complete_anchor",
            zh_label="环境 sample-ready payload 完整锚点",
            en_label="Ambient sample-ready payload-complete anchor",
        ),
        "ambient_sample_ready_trace_only_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "ambient_sample_ready_trace_only_gap",
            i18n_key="taxonomy.gap_classification.ambient_sample_ready_trace_only_gap",
            zh_label="环境 sample-ready 仅 trace 缺口",
            en_label="Ambient sample-ready trace-only gap",
        ),
        "ambient_sample_ready_model_only_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "ambient_sample_ready_model_only_gap",
            i18n_key="taxonomy.gap_classification.ambient_sample_ready_model_only_gap",
            zh_label="环境 sample-ready 仅模型缺口",
            en_label="Ambient sample-ready model-only gap",
        ),
        "ambient_sample_ready_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "ambient_sample_ready_gap",
            i18n_key="taxonomy.gap_classification.ambient_sample_ready_gap",
            zh_label="环境 sample-ready 缺口",
            en_label="Ambient sample-ready gap",
        ),
        "ambient_sample_ready_reviewer_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "ambient_sample_ready_reviewer_gap",
            i18n_key="taxonomy.gap_classification.ambient_sample_ready_reviewer_gap",
            zh_label="环境 sample-ready 审阅缺口",
            en_label="Ambient sample-ready reviewer gap",
        ),
        "water_sample_ready_payload_complete_anchor": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "water_sample_ready_payload_complete_anchor",
            i18n_key="taxonomy.gap_classification.water_sample_ready_payload_complete_anchor",
            zh_label="水路 sample-ready payload 完整锚点",
            en_label="Water sample-ready payload-complete anchor",
        ),
        "water_sample_ready_trace_only_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "water_sample_ready_trace_only_gap",
            i18n_key="taxonomy.gap_classification.water_sample_ready_trace_only_gap",
            zh_label="水路 sample-ready 仅 trace 缺口",
            en_label="Water sample-ready trace-only gap",
        ),
        "water_sample_ready_model_only_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "water_sample_ready_model_only_gap",
            i18n_key="taxonomy.gap_classification.water_sample_ready_model_only_gap",
            zh_label="水路 sample-ready 仅模型缺口",
            en_label="Water sample-ready model-only gap",
        ),
        "water_sample_ready_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "water_sample_ready_gap",
            i18n_key="taxonomy.gap_classification.water_sample_ready_gap",
            zh_label="水路 sample-ready 缺口",
            en_label="Water sample-ready gap",
        ),
        "water_sample_ready_reviewer_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "water_sample_ready_reviewer_gap",
            i18n_key="taxonomy.gap_classification.water_sample_ready_reviewer_gap",
            zh_label="水路 sample-ready 审阅缺口",
            en_label="Water sample-ready reviewer gap",
        ),
        "gas_sample_ready_payload_complete_anchor": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "gas_sample_ready_payload_complete_anchor",
            i18n_key="taxonomy.gap_classification.gas_sample_ready_payload_complete_anchor",
            zh_label="气路 sample-ready payload 完整锚点",
            en_label="Gas sample-ready payload-complete anchor",
        ),
        "gas_sample_ready_trace_only_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "gas_sample_ready_trace_only_gap",
            i18n_key="taxonomy.gap_classification.gas_sample_ready_trace_only_gap",
            zh_label="气路 sample-ready 仅 trace 缺口",
            en_label="Gas sample-ready trace-only gap",
        ),
        "gas_sample_ready_model_only_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "gas_sample_ready_model_only_gap",
            i18n_key="taxonomy.gap_classification.gas_sample_ready_model_only_gap",
            zh_label="气路 sample-ready 仅模型缺口",
            en_label="Gas sample-ready model-only gap",
        ),
        "gas_sample_ready_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "gas_sample_ready_gap",
            i18n_key="taxonomy.gap_classification.gas_sample_ready_gap",
            zh_label="气路 sample-ready 缺口",
            en_label="Gas sample-ready gap",
        ),
        "gas_sample_ready_reviewer_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "gas_sample_ready_reviewer_gap",
            i18n_key="taxonomy.gap_classification.gas_sample_ready_reviewer_gap",
            zh_label="气路 sample-ready 审阅缺口",
            en_label="Gas sample-ready reviewer gap",
        ),
    }
)

_TAXONOMY_REGISTRY[GAP_SEVERITY_FAMILY].update(
    {
        "info": _entry(
            GAP_SEVERITY_FAMILY,
            "info",
            i18n_key="taxonomy.gap_severity.info",
            zh_label="提示",
            en_label="Info",
        ),
        "medium": _entry(
            GAP_SEVERITY_FAMILY,
            "medium",
            i18n_key="taxonomy.gap_severity.medium",
            zh_label="中",
            en_label="Medium",
        ),
        "high": _entry(
            GAP_SEVERITY_FAMILY,
            "high",
            i18n_key="taxonomy.gap_severity.high",
            zh_label="高",
            en_label="High",
        ),
        "low": _entry(
            GAP_SEVERITY_FAMILY,
            "low",
            i18n_key="taxonomy.gap_severity.low",
            zh_label="低",
            en_label="Low",
        ),
    }
)

_TAXONOMY_REGISTRY[GAP_CLASSIFICATION_FAMILY].update(
    {
        "recovery_retry_payload_complete_anchor": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "recovery_retry_payload_complete_anchor",
            i18n_key="taxonomy.gap_classification.recovery_retry_payload_complete_anchor",
            zh_label="恢复重试 payload 完整锚点",
            en_label="Recovery/retry payload-complete anchor",
        ),
        "recovery_retry_trace_only_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "recovery_retry_trace_only_gap",
            i18n_key="taxonomy.gap_classification.recovery_retry_trace_only_gap",
            zh_label="恢复重试仅 trace 缺口",
            en_label="Recovery/retry trace-only gap",
        ),
        "recovery_retry_test_only_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "recovery_retry_test_only_gap",
            i18n_key="taxonomy.gap_classification.recovery_retry_test_only_gap",
            zh_label="恢复重试仅测试缺口",
            en_label="Recovery/retry test-only gap",
        ),
        "recovery_retry_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "recovery_retry_gap",
            i18n_key="taxonomy.gap_classification.recovery_retry_gap",
            zh_label="恢复重试缺口",
            en_label="Recovery/retry gap",
        ),
        "recovery_retry_reviewer_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "recovery_retry_reviewer_gap",
            i18n_key="taxonomy.gap_classification.recovery_retry_reviewer_gap",
            zh_label="恢复重试审阅缺口",
            en_label="Recovery/retry reviewer gap",
        ),
        "trace_only_reviewer_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "trace_only_reviewer_gap",
            i18n_key="taxonomy.gap_classification.trace_only_reviewer_gap",
            zh_label="仅 trace 审阅缺口",
            en_label="Trace-only reviewer gap",
        ),
        "payload_partial_reviewer_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "payload_partial_reviewer_gap",
            i18n_key="taxonomy.gap_classification.payload_partial_reviewer_gap",
            zh_label="payload 部分审阅缺口",
            en_label="Payload-partial reviewer gap",
        ),
        "model_only_reviewer_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "model_only_reviewer_gap",
            i18n_key="taxonomy.gap_classification.model_only_reviewer_gap",
            zh_label="仅模型审阅缺口",
            en_label="Model-only reviewer gap",
        ),
        "test_only_reviewer_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "test_only_reviewer_gap",
            i18n_key="taxonomy.gap_classification.test_only_reviewer_gap",
            zh_label="仅测试审阅缺口",
            en_label="Test-only reviewer gap",
        ),
        "gap_reviewer_gap": _entry(
            GAP_CLASSIFICATION_FAMILY,
            "gap_reviewer_gap",
            i18n_key="taxonomy.gap_classification.gap_reviewer_gap",
            zh_label="缺口审阅缺口",
            en_label="Gap reviewer gap",
        ),
    }
)

_TAXONOMY_REGISTRY[REVIEWER_NEXT_STEP_TEMPLATE_FAMILY].update(
    {
        "ambient_diagnostic_payload_complete_anchor": _entry(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            "ambient_diagnostic_payload_complete_anchor",
            i18n_key="taxonomy.reviewer_next_step.ambient_diagnostic_payload_complete_anchor",
            zh_label="使用环境诊断 payload 作为仿真基线锚点，并将方法、不确定度与溯源闭环继续保留在 readiness-only 工件中。",
            en_label="Use the ambient diagnostic payload as the synthetic baseline anchor, then keep ambient method, uncertainty, and traceability closure in readiness-only artifacts.",
        ),
        "ambient_diagnostic_trace_promotion": _entry(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            "ambient_diagnostic_trace_promotion",
            i18n_key="taxonomy.reviewer_next_step.ambient_diagnostic_trace_promotion",
            zh_label="先把环境诊断 trace 提升为 payload-backed 审阅证据，再关闭环境基线方法、不确定度与溯源缺口。",
            en_label="Promote the ambient diagnostic trace into payload-backed reviewer evidence before closing ambient baseline method, uncertainty, and traceability gaps.",
        ),
        "ambient_diagnostic_gap_closeout": _entry(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            "ambient_diagnostic_gap_closeout",
            i18n_key="taxonomy.reviewer_next_step.ambient_diagnostic_gap_closeout",
            zh_label="先确认环境诊断基线方法条目，再补齐环境压力/湿度/温度不确定度输入，并把环境参考接入溯源骨架，同时保持该阶段仅用于审阅。",
            en_label="Confirm ambient diagnostic baseline method items first, then add ambient pressure/humidity/temperature uncertainty inputs, then tie the ambient references into the traceability stub while keeping this phase reviewer-only.",
        ),
        "water_preseal_partial_gap_closeout": _entry(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            "water_preseal_partial_gap_closeout",
            i18n_key="taxonomy.reviewer_next_step.water_preseal_partial_gap_closeout",
            zh_label="先确认水路 preseal 窗口方法，再补齐 preseal 压力/温度不确定度输入，并把湿度与露点参考接入溯源骨架，同时明确 preseal 仍保持 payload-partial。",
            en_label="Confirm the water preseal window method first, then add the preseal pressure/temperature uncertainty inputs, then tie the humidity and dew-point references into the traceability stub while keeping preseal partial explicit as payload-partial.",
        ),
        "gas_preseal_partial_gap_closeout": _entry(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            "gas_preseal_partial_gap_closeout",
            i18n_key="taxonomy.reviewer_next_step.gas_preseal_partial_gap_closeout",
            zh_label="先确认气路 preseal 窗口方法，再补齐 preseal 压力/温度不确定度输入，并把标准气与压力参考接入溯源骨架，同时明确 preseal 仍保持 payload-partial。",
            en_label="Confirm the gas preseal window method first, then add the preseal pressure/temperature uncertainty inputs, then tie the standard-gas and pressure references into the traceability stub while keeping preseal partial explicit as payload-partial.",
        ),
        "water_pressure_stable_payload_complete_anchor": _entry(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            "water_pressure_stable_payload_complete_anchor",
            i18n_key="taxonomy.reviewer_next_step.water_pressure_stable_payload_complete_anchor",
            zh_label="使用水路 pressure-stable payload 作为仿真审阅锚点，在发布参考证据前，将证书与溯源闭环继续保留在 readiness-only 工件中。",
            en_label="Use the water pressure-stable payload as the synthetic reviewer anchor, then keep certificate and traceability closure in readiness-only artifacts until released reference evidence exists.",
        ),
        "gas_pressure_stable_payload_complete_anchor": _entry(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            "gas_pressure_stable_payload_complete_anchor",
            i18n_key="taxonomy.reviewer_next_step.gas_pressure_stable_payload_complete_anchor",
            zh_label="使用气路 pressure-stable payload 作为仿真审阅锚点，在发布参考证据前，将证书与溯源闭环继续保留在 readiness-only 工件中。",
            en_label="Use the gas pressure-stable payload as the synthetic reviewer anchor, then keep certificate and traceability closure in readiness-only artifacts until released reference evidence exists.",
        ),
        "ambient_sample_ready_payload_complete_anchor": _entry(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            "ambient_sample_ready_payload_complete_anchor",
            i18n_key="taxonomy.reviewer_next_step.ambient_sample_ready_payload_complete_anchor",
            zh_label="使用环境 sample-ready payload 作为仿真释放锚点，并将范围、方法、不确定度与溯源闭环继续保留在 readiness-only 工件中。",
            en_label="Use the ambient sample-ready payload as the synthetic release anchor, then keep scope, method, uncertainty, and traceability closure in readiness-only artifacts.",
        ),
        "ambient_sample_ready_trace_promotion": _entry(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            "ambient_sample_ready_trace_promotion",
            i18n_key="taxonomy.reviewer_next_step.ambient_sample_ready_trace_promotion",
            zh_label="先把环境 sample-ready trace 提升为 payload-backed 审阅证据，再关闭停留、不确定度与溯源缺口。",
            en_label="Promote the ambient sample-ready trace into payload-backed reviewer evidence before closing dwell, uncertainty, and traceability gaps.",
        ),
        "ambient_sample_ready_gap_closeout": _entry(
            REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
            "ambient_sample_ready_gap_closeout",
            i18n_key="taxonomy.reviewer_next_step.ambient_sample_ready_gap_closeout",
            zh_label="先确认环境 sample-ready 停留与释放方法条目，再补齐稳定性不确定度输入，并把环境释放参考接入溯源骨架，同时保持该阶段仅用于审阅。",
            en_label="Confirm ambient sample-ready dwell and release method items first, then add stabilization uncertainty inputs, then tie the ambient release references into the traceability stub while keeping this phase reviewer-only.",
        ),
    }
)

def _normalize_lookup_value(value: Any) -> str:
    if isinstance(value, dict):
        for key in (
            "canonical_key",
            "key",
            "display_key",
            "label",
            "display_label",
            "en_label",
            "zh_label",
            "value",
        ):
            candidate = str(value.get(key) or "").strip()
            if candidate:
                value = candidate
                break
        else:
            value = ""
    return _TOKEN_RE.sub("", str(value or "").strip().lower())


def _rebuild_alias_index() -> None:
    global _ALIAS_INDEX
    alias_index: dict[str, dict[str, str]] = {}
    for family, items in _TAXONOMY_REGISTRY.items():
        family_index: dict[str, str] = {}
        for canonical_key, entry in items.items():
            for alias in (
                canonical_key,
                canonical_key.replace("_", " "),
                entry["en_label"],
                entry["zh_label"],
                *entry["aliases"],
            ):
                normalized = _TOKEN_RE.sub("", str(alias or "").strip().lower())
                if normalized:
                    family_index[normalized] = canonical_key
        alias_index[family] = family_index
    _ALIAS_INDEX = alias_index


def list_taxonomy_entries(family: str) -> list[dict[str, Any]]:
    return [dict(item) for item in _TAXONOMY_REGISTRY.get(str(family or "").strip(), {}).values()]


def normalize_taxonomy_key(family: str, value: Any, *, default: str | None = None) -> str:
    family_name = str(family or "").strip()
    if not family_name:
        return str(default or "")
    normalized = _normalize_lookup_value(value)
    if not normalized:
        return str(default or "")
    return str(_ALIAS_INDEX.get(family_name, {}).get(normalized) or default or "")


def taxonomy_entry(family: str, value: Any) -> dict[str, Any] | None:
    canonical_key = normalize_taxonomy_key(str(family or "").strip(), value)
    if not canonical_key:
        return None
    entry = _TAXONOMY_REGISTRY.get(str(family or "").strip(), {}).get(canonical_key)
    return dict(entry) if entry else None


def taxonomy_i18n_key(family: str, value: Any) -> str:
    entry = taxonomy_entry(family, value)
    return str(entry.get("i18n_key") or "") if entry else ""


def taxonomy_display_label(family: str, value: Any, *, locale: str = "zh_CN", default: str | None = None) -> str:
    entry = taxonomy_entry(family, value)
    if not entry:
        if default is not None:
            return default
        return str(value or "").strip()
    if str(locale or "").lower().startswith("en"):
        return str(entry.get("en_label") or default or "")
    return str(entry.get("zh_label") or default or "")


def normalize_taxonomy_keys(family: str, values: Any) -> list[str]:
    rows: list[str] = []
    for value in list(values or []):
        canonical_key = normalize_taxonomy_key(family, value)
        if canonical_key and canonical_key not in rows:
            rows.append(canonical_key)
    return rows


def taxonomy_display_labels(family: str, values: Any, *, locale: str = "zh_CN") -> list[str]:
    rows: list[str] = []
    for canonical_key in normalize_taxonomy_keys(family, values):
        label = taxonomy_display_label(family, canonical_key, locale=locale)
        if label and label not in rows:
            rows.append(label)
    return rows


def taxonomy_text_replacements(*, locale: str = "zh_CN") -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for family, items in _TAXONOMY_REGISTRY.items():
        for canonical_key, entry in items.items():
            target = taxonomy_display_label(family, canonical_key, locale=locale)
            for source in (
                canonical_key,
                canonical_key.replace("_", " "),
                str(entry.get("en_label") or ""),
                str(entry.get("zh_label") or ""),
            ):
                source_text = str(source or "").strip()
                if not source_text or source_text == target:
                    continue
                pair = (source_text, target)
                if pair not in seen:
                    seen.add(pair)
                    rows.append(pair)
    return sorted(rows, key=lambda item: len(item[0]), reverse=True)


def phase_taxonomy_profile(route_family: str, phase_name: str) -> dict[str, Any]:
    return dict(
        _PHASE_TAXONOMY_PROFILES.get(
            (str(route_family or "").strip(), str(phase_name or "").strip()),
            {},
        )
    )


def _phase_profile_value(
    *,
    profile: dict[str, Any],
    field_name: str,
    coverage_bucket: str,
    payload_completeness: str,
) -> Any:
    value = profile.get(field_name)
    if not isinstance(value, dict):
        return value
    for key in (
        str(coverage_bucket or "").strip(),
        str(payload_completeness or "").strip(),
        "default",
    ):
        if key and key in value:
            return value[key]
    return None
