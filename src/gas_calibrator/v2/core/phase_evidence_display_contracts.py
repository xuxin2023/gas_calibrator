"""Phase evidence display contracts — single source of truth for V1.2 phase/taxonomy
display text across results, reports, review center, historical, and reviewer artifacts.

This module centralizes:
- Artifact/summary keys
- Chinese default title_text / summary_text / section label / type label
- English fallback equivalents
- i18n key mappings
- Reviewer-facing default summary text
- V1.2 phase term mappings

Design principles:
- Chinese default, English fallback
- No formal acceptance / formal claim language
- All evidence is simulation-only
- Single source of truth — modules import from here, not hardcode their own copies
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
PHASE_EVIDENCE_DISPLAY_CONTRACTS_VERSION: str = "2.9.0"

# ---------------------------------------------------------------------------
# Artifact keys for V1.2 phase evidence
# ---------------------------------------------------------------------------
PHASE_EVIDENCE_ARTIFACT_KEYS: tuple[str, ...] = (
    "point_taxonomy_summary",
    "measurement_phase_coverage_report",
    "phase_transition_bridge",
    "v12_alignment_summary",
)

# ---------------------------------------------------------------------------
# Chinese default display texts
# ---------------------------------------------------------------------------
PHASE_EVIDENCE_TITLE_TEXTS: dict[str, str] = {
    "point_taxonomy_summary": "点位语义摘要",
    "measurement_phase_coverage_report": "测量阶段覆盖",
    "phase_transition_bridge": "阶段过渡桥接",
    "v12_alignment_summary": "V1.2 对齐摘要",
}

PHASE_EVIDENCE_SUMMARY_TEXTS: dict[str, str] = {
    "point_taxonomy_summary": "点位语义与门禁分类摘要",
    "measurement_phase_coverage_report": "测量核心阶段覆盖与缺口摘要",
    "phase_transition_bridge": "Step 2 tail / Stage 3 桥接状态摘要",
    "v12_alignment_summary": "V1.2 仿真证据对齐状态摘要",
}

PHASE_EVIDENCE_SECTION_LABELS: dict[str, str] = {
    "point_taxonomy_summary": "点位语义",
    "measurement_phase_coverage_report": "测量阶段覆盖",
    "phase_transition_bridge": "阶段桥接",
    "v12_alignment_summary": "V1.2 对齐",
}

PHASE_EVIDENCE_TYPE_LABELS: dict[str, str] = {
    "point_taxonomy_summary": "点位语义",
    "measurement_phase_coverage_report": "测量阶段覆盖",
    "phase_transition_bridge": "阶段桥接",
    "v12_alignment_summary": "V1.2 对齐摘要",
}

# ---------------------------------------------------------------------------
# English fallback display texts
# ---------------------------------------------------------------------------
PHASE_EVIDENCE_TITLE_TEXTS_EN: dict[str, str] = {
    "point_taxonomy_summary": "Point Taxonomy Summary",
    "measurement_phase_coverage_report": "Measurement Phase Coverage",
    "phase_transition_bridge": "Phase Transition Bridge",
    "v12_alignment_summary": "V1.2 Alignment Summary",
}

PHASE_EVIDENCE_SUMMARY_TEXTS_EN: dict[str, str] = {
    "point_taxonomy_summary": "Point taxonomy and gate classification summary",
    "measurement_phase_coverage_report": "Measurement-core phase coverage and gap summary",
    "phase_transition_bridge": "Step 2 tail / Stage 3 bridge status summary",
    "v12_alignment_summary": "V1.2 simulation evidence alignment status summary",
}

PHASE_EVIDENCE_SECTION_LABELS_EN: dict[str, str] = {
    "point_taxonomy_summary": "Point Taxonomy",
    "measurement_phase_coverage_report": "Phase Coverage",
    "phase_transition_bridge": "Phase Bridge",
    "v12_alignment_summary": "V1.2 Alignment",
}

PHASE_EVIDENCE_TYPE_LABELS_EN: dict[str, str] = {
    "point_taxonomy_summary": "Point Taxonomy",
    "measurement_phase_coverage_report": "Measurement Phase Coverage",
    "phase_transition_bridge": "Phase Bridge",
    "v12_alignment_summary": "V1.2 Alignment Summary",
}

# ---------------------------------------------------------------------------
# i18n key mappings
# ---------------------------------------------------------------------------
PHASE_EVIDENCE_I18N_KEYS: dict[str, str] = {
    "point_taxonomy_summary": "phase_evidence.point_taxonomy_summary",
    "measurement_phase_coverage_report": "phase_evidence.measurement_phase_coverage_report",
    "phase_transition_bridge": "phase_evidence.phase_transition_bridge",
    "v12_alignment_summary": "phase_evidence.v12_alignment_summary",
}

# ---------------------------------------------------------------------------
# V1.2 phase terms — Chinese default / English fallback
# ---------------------------------------------------------------------------
PHASE_TERMS: dict[str, str] = {
    "ambient": "环境条件",
    "ambient_open": "环境开路",
    "flush_gate": "冲洗门禁",
    "preseal": "前封气",
    "postseal": "后封气",
    "stale_gauge": "压力参考陈旧",
    "phase_transition": "阶段过渡",
    "bridge": "桥接",
    "governance_handoff": "治理交接",
    "parity": "一致性比对",
    "resilience": "导出韧性",
    "point_taxonomy": "点位语义",
    "measurement_phase_coverage": "测量阶段覆盖",
}

PHASE_TERMS_EN: dict[str, str] = {
    "ambient": "Ambient",
    "ambient_open": "Ambient Open",
    "flush_gate": "Flush Gate",
    "preseal": "Preseal",
    "postseal": "Postseal",
    "stale_gauge": "Stale Gauge",
    "phase_transition": "Phase Transition",
    "bridge": "Bridge",
    "governance_handoff": "Governance Handoff",
    "parity": "Parity",
    "resilience": "Resilience",
    "point_taxonomy": "Point Taxonomy",
    "measurement_phase_coverage": "Measurement Phase Coverage",
}

# ---------------------------------------------------------------------------
# Phase transition bridge section labels
# ---------------------------------------------------------------------------
BRIDGE_SECTION_LABELS: dict[str, str] = {
    "reference_traceability_contract": "参考溯源链 contract",
    "calibration_execution_contract": "校准执行 contract",
    "data_quality_contract": "数据质量 contract",
    "uncertainty_budget_template": "不确定度模板",
    "coefficient_verification_contract": "系数验证 contract",
    "evidence_traceability_contract": "证据追踪 contract",
    "reporting_contract": "报告 contract",
}

BRIDGE_SECTION_LABELS_EN: dict[str, str] = {
    "reference_traceability_contract": "Reference Traceability Contract",
    "calibration_execution_contract": "Calibration Execution Contract",
    "data_quality_contract": "Data Quality Contract",
    "uncertainty_budget_template": "Uncertainty Budget Template",
    "coefficient_verification_contract": "Coefficient Verification Contract",
    "evidence_traceability_contract": "Evidence Traceability Contract",
    "reporting_contract": "Reporting Contract",
}

# ---------------------------------------------------------------------------
# Phase transition bridge reviewer display texts
# ---------------------------------------------------------------------------
BRIDGE_REVIEWER_TEXTS: dict[str, str] = {
    "status_ready_for_engineering_isolation": "阶段状态：当前仍处于 Step 2 tail / Stage 3 bridge，但已具备 engineering-isolation 准备。",
    "status_step2_tail_in_progress": "阶段状态：当前仍处于 Step 2 tail，制度化设计已到位，但 readiness 阻塞项尚未全部闭环。",
    "status_blocked_before_stage3": "阶段状态：当前仍停留在 Stage 3 前置桥接阶段，尚不能进入真实计量验证。",
    "summary_text": "阶段桥工件：本工件统一汇总 Step 2 readiness 与 metrology 设计合同，用于回答离第三阶段还有多远；它不是 real acceptance 结论，也不能替代真实计量验证。",
    "current_stage_text": "当前阶段：Step 2 tail / Stage 3 bridge，仅允许 simulation/offline/headless 证据。",
    "next_stage_ready": "下一阶段：可进入 engineering-isolation，继续收集非 real-acceptance 的工程隔离证据。",
    "next_stage_not_ready": "下一阶段：先补齐 Step 2 tail 阻塞项，再进入 engineering-isolation 准备。",
    "execute_now_prefix": "当前执行：",
    "defer_to_stage3_prefix": "第三阶段执行：",
    "no_blocking": "阻塞项：无。",
    "blocking_prefix": "阻塞项：",
    "warning_prefix": "提示：",
    "recommended_next_stage_prefix": "推荐下一阶段：",
    "gate_status_defined": "已制度化",
    "gate_status_missing": "缺失",
}

BRIDGE_REVIEWER_TEXTS_EN: dict[str, str] = {
    "status_ready_for_engineering_isolation": "Phase status: still in Step 2 tail / Stage 3 bridge, but engineering-isolation readiness is met.",
    "status_step2_tail_in_progress": "Phase status: still in Step 2 tail; institutional design is in place, but readiness blockers are not yet fully closed.",
    "status_blocked_before_stage3": "Phase status: still in the Stage 3 pre-bridge phase; cannot enter real metrology verification yet.",
    "summary_text": "Phase bridge artifact: this artifact consolidates Step 2 readiness and metrology design contracts to answer how far from Stage 3; it is not real acceptance evidence and cannot replace real metrology verification.",
    "current_stage_text": "Current stage: Step 2 tail / Stage 3 bridge; only simulation/offline/headless evidence is allowed.",
    "next_stage_ready": "Next stage: can enter engineering-isolation, continuing to collect non-real-acceptance engineering isolation evidence.",
    "next_stage_not_ready": "Next stage: close Step 2 tail blockers first, then enter engineering-isolation readiness.",
    "execute_now_prefix": "Execute now: ",
    "defer_to_stage3_prefix": "Stage 3 execution: ",
    "no_blocking": "Blockers: none.",
    "blocking_prefix": "Blockers: ",
    "warning_prefix": "Warning: ",
    "recommended_next_stage_prefix": "Recommended next stage: ",
    "gate_status_defined": "Defined",
    "gate_status_missing": "Missing",
}

# ---------------------------------------------------------------------------
# Results gateway fallback labels — Chinese default / English fallback
# ---------------------------------------------------------------------------
RESULTS_FALLBACK_LABELS: dict[str, str] = {
    "results_file": "结果文件",
    "generated": "已生成",
    "missing": "缺失",
    "sample_count": "样本数",
    "point_summary_count": "点摘要数",
    "artifact_roles": "工件角色",
    "config_safety": "配置安全",
    "evidence_source": "证据来源",
    "offline_diagnostic": "离线诊断",
    "workbench_evidence": "工作台诊断证据",
}

RESULTS_FALLBACK_LABELS_EN: dict[str, str] = {
    "results_file": "Results file",
    "generated": "Generated",
    "missing": "Missing",
    "sample_count": "Sample count",
    "point_summary_count": "Point summary count",
    "artifact_roles": "Artifact roles",
    "config_safety": "Config safety",
    "evidence_source": "Evidence source",
    "offline_diagnostic": "Offline diagnostic",
    "workbench_evidence": "Workbench diagnostic evidence",
}

# ---------------------------------------------------------------------------
# Resolve helpers
# ---------------------------------------------------------------------------


def resolve_phase_evidence_title(key: str, *, lang: str = "zh") -> str:
    """Resolve phase evidence title. Chinese default, English fallback."""
    if lang == "en":
        return PHASE_EVIDENCE_TITLE_TEXTS_EN.get(key, key)
    return PHASE_EVIDENCE_TITLE_TEXTS.get(key, key)


def resolve_phase_term(key: str, *, lang: str = "zh") -> str:
    """Resolve V1.2 phase term. Chinese default, English fallback."""
    if lang == "en":
        return PHASE_TERMS_EN.get(key, key)
    return PHASE_TERMS.get(key, key)


def resolve_bridge_section_label(key: str, *, lang: str = "zh") -> str:
    """Resolve bridge section label. Chinese default, English fallback."""
    if lang == "en":
        return BRIDGE_SECTION_LABELS_EN.get(key, key)
    return BRIDGE_SECTION_LABELS.get(key, key)


def resolve_bridge_reviewer_text(key: str, *, lang: str = "zh") -> str:
    """Resolve bridge reviewer display text. Chinese default, English fallback."""
    if lang == "en":
        return BRIDGE_REVIEWER_TEXTS_EN.get(key, key)
    return BRIDGE_REVIEWER_TEXTS.get(key, key)


def resolve_results_fallback_label(key: str, *, lang: str = "zh") -> str:
    """Resolve results gateway fallback label. Chinese default, English fallback."""
    if lang == "en":
        return RESULTS_FALLBACK_LABELS_EN.get(key, key)
    return RESULTS_FALLBACK_LABELS.get(key, key)


# ---------------------------------------------------------------------------
# Formatter display labels — Chinese default / English fallback
# Used by review_surface_formatter.py module-level dicts
# ---------------------------------------------------------------------------
FORMATTER_DISPLAY_LABELS: dict[str, str] = {
    "artifacts": "工件",
    "plots": "图表",
    "primary": "主工件",
    "supporting": "支撑工件",
    "coverage": "覆盖",
    "complete": "完整",
    "gapped": "缺口",
    "missing_label": "缺少",
    "no_gaps": "无缺口",
    "visible": "可见",
    "present": "存在",
    "external": "外部",
    "catalog": "当前运行基线",
    "filtered": "当前筛选",
    "failed": "失败",
    "degraded": "降级",
    "diagnostic": "仅诊断",
    "high": "高",
    "medium": "中",
    "low": "低",
}

FORMATTER_DISPLAY_LABELS_EN: dict[str, str] = {
    "artifacts": "Artifacts",
    "plots": "Plots",
    "primary": "Primary",
    "supporting": "Supporting",
    "coverage": "Coverage",
    "complete": "Complete",
    "gapped": "Gapped",
    "missing_label": "Missing",
    "no_gaps": "No gaps",
    "visible": "Visible",
    "present": "Present",
    "external": "External",
    "catalog": "Current-run catalog baseline",
    "filtered": "Current filtered",
    "failed": "Failed",
    "degraded": "Degraded",
    "diagnostic": "Diagnostic only",
    "high": "High",
    "medium": "Medium",
    "low": "Low",
}

# ---------------------------------------------------------------------------
# Artifact compatibility row labels — Chinese default / English fallback
# Used by results_gateway._decorate_artifact_compatibility_row
# ---------------------------------------------------------------------------
COMPATIBILITY_ROW_LABELS: dict[str, str] = {
    "version": "版本",
    "status": "状态",
    "reader_mode": "读取",
    "schema_contract": "合同/Schema",
    "regenerate_recommended": "建议再生成 reviewer/index sidecar",
    "recommendation": "建议",
    "no_rewrite_primary": "不改写原始主证据",
}

COMPATIBILITY_ROW_LABELS_EN: dict[str, str] = {
    "version": "Version",
    "status": "Status",
    "reader_mode": "Reader",
    "schema_contract": "Schema/Contract",
    "regenerate_recommended": "Regenerate reviewer/index sidecar recommended",
    "recommendation": "Recommendation",
    "no_rewrite_primary": "Do not rewrite primary evidence",
}

# ---------------------------------------------------------------------------
# Historical rollup labels — Chinese default / English fallback
# Used by historical_artifacts._build_operation_report
# ---------------------------------------------------------------------------
HISTORICAL_ROLLUP_LABELS: dict[str, str] = {
    "scope_run_count": "认可范围包运行数",
    "canonical_direct": "canonical 直读",
    "compatibility_adapter": "兼容适配读取",
    "readiness_status": "就绪状态",
}

HISTORICAL_ROLLUP_LABELS_EN: dict[str, str] = {
    "scope_run_count": "Scope package run count",
    "canonical_direct": "Canonical direct reads",
    "compatibility_adapter": "Compatibility adapter reads",
    "readiness_status": "Readiness status",
}

# ---------------------------------------------------------------------------
# Measurement digest line labels — Chinese default / English fallback
# Used by review_surface_formatter.build_measurement_review_digest_lines
# ---------------------------------------------------------------------------
MEASUREMENT_DIGEST_LABELS: dict[str, str] = {
    "payload_complete_phases": "payload 完整阶段",
    "payload_partial_phases": "payload 部分阶段",
    "trace_only_phases": "仅 trace 阶段",
    "next_artifacts": "下一步补证工件",
    "blockers": "当前阻塞",
    "preseal_partial_guidance": "preseal 部分 payload 提示",
    "linked_method_items": "关联方法确认条目",
    "linked_uncertainty_inputs": "关联不确定度输入",
    "linked_traceability_nodes": "关联溯源节点",
    "reviewer_next_steps": "审阅下一步",
    "phase_contrast": "preseal / pressure_stable 对照",
    "readiness_impact": "就绪度影响",
    "linked_readiness": "关联就绪工件",
    "gap_index": "缺口索引",
    "boundary": "边界",
    "non_claim": "非声明边界",
    "phase_non_claim": "非声明边界",
}

MEASUREMENT_DIGEST_LABELS_EN: dict[str, str] = {
    "payload_complete_phases": "payload complete phases",
    "payload_partial_phases": "payload partial phases",
    "trace_only_phases": "trace-only phases",
    "next_artifacts": "Next artifacts",
    "blockers": "Blockers",
    "preseal_partial_guidance": "Preseal partial payload guidance",
    "linked_method_items": "Linked method confirmation items",
    "linked_uncertainty_inputs": "Linked uncertainty inputs",
    "linked_traceability_nodes": "Linked traceability nodes",
    "reviewer_next_steps": "Reviewer next steps",
    "phase_contrast": "Preseal / pressure_stable contrast",
    "readiness_impact": "Readiness impact",
    "linked_readiness": "Linked readiness anchors",
    "gap_index": "Gap index",
    "boundary": "Boundary",
    "non_claim": "Non-claim boundary",
    "phase_non_claim": "Non-claim boundary",
}

# ---------------------------------------------------------------------------
# Readiness digest line labels — Chinese default / English fallback
# Used by review_surface_formatter.build_readiness_review_digest_lines
# ---------------------------------------------------------------------------
READINESS_DIGEST_LABELS: dict[str, str] = {
    "scope_overview": "认可范围概览",
    "decision_rule": "决策规则概览",
    "conformity_boundary": "符合性边界",
    "protocol_overview": "方法确认概览",
    "validation_matrix_completeness": "验证矩阵完整度",
    "current_evidence_coverage": "当前证据覆盖",
    "top_gaps": "主要缺口",
    "readiness_status": "验证就绪状态",
    "uncertainty_overview": "不确定度概览",
    "budget_component_summary": "预算组件摘要",
    "top_contributors": "主要不确定度贡献",
    "data_completeness": "数据完整度",
    "placeholder_completeness": "占位完整度",
    "standard_family": "标准族",
    "required_evidence_categories": "要求证据类别",
    "boundary": "边界",
    "non_claim": "非声明边界",
}

READINESS_DIGEST_LABELS_EN: dict[str, str] = {
    "scope_overview": "Scope overview",
    "decision_rule": "Decision rule overview",
    "conformity_boundary": "Conformity boundary",
    "protocol_overview": "Method confirmation overview",
    "validation_matrix_completeness": "Validation matrix completeness",
    "current_evidence_coverage": "Current evidence coverage",
    "top_gaps": "Top gaps",
    "readiness_status": "Verification readiness status",
    "uncertainty_overview": "Uncertainty overview",
    "budget_component_summary": "Budget component summary",
    "top_contributors": "Top uncertainty contributors",
    "data_completeness": "Data completeness",
    "placeholder_completeness": "Placeholder completeness",
    "standard_family": "Standard family",
    "required_evidence_categories": "Required evidence categories",
    "boundary": "Boundary",
    "non_claim": "Non-claim boundary",
}


def resolve_formatter_label(key: str, *, lang: str = "zh") -> str:
    """Resolve formatter display label. Chinese default, English fallback."""
    if lang == "en":
        return FORMATTER_DISPLAY_LABELS_EN.get(key, key)
    return FORMATTER_DISPLAY_LABELS.get(key, key)


def resolve_compatibility_row_label(key: str, *, lang: str = "zh") -> str:
    """Resolve artifact compatibility row label. Chinese default, English fallback."""
    if lang == "en":
        return COMPATIBILITY_ROW_LABELS_EN.get(key, key)
    return COMPATIBILITY_ROW_LABELS.get(key, key)


def resolve_historical_rollup_label(key: str, *, lang: str = "zh") -> str:
    """Resolve historical rollup label. Chinese default, English fallback."""
    if lang == "en":
        return HISTORICAL_ROLLUP_LABELS_EN.get(key, key)
    return HISTORICAL_ROLLUP_LABELS.get(key, key)


def resolve_measurement_digest_label(key: str, *, lang: str = "zh") -> str:
    """Resolve measurement digest line label. Chinese default, English fallback."""
    if lang == "en":
        return MEASUREMENT_DIGEST_LABELS_EN.get(key, key)
    return MEASUREMENT_DIGEST_LABELS.get(key, key)


def resolve_readiness_digest_label(key: str, *, lang: str = "zh") -> str:
    """Resolve readiness digest line label. Chinese default, English fallback."""
    if lang == "en":
        return READINESS_DIGEST_LABELS_EN.get(key, key)
    return READINESS_DIGEST_LABELS.get(key, key)


# ---------------------------------------------------------------------------
# Step 2 boundary markers
# ---------------------------------------------------------------------------
PHASE_EVIDENCE_STEP2_BOUNDARY: dict[str, str | bool] = {
    "evidence_source": "simulated",
    "not_real_acceptance_evidence": True,
    "not_ready_for_formal_claim": True,
    "reviewer_only": True,
    "readiness_mapping_only": True,
}
