"""Step 3 admission dossier contracts — single source of truth for the
Step 3 admission dossier title, section ordering, section labels, status buckets,
blocker/next-step labels, boundary markers, and i18n keys.

The admission dossier is the final Step 2 governance package that aggregates
freeze_audit + closeout_package + closeout_readiness + governance/parity/resilience/
phase evidence into a single "Step 3 admission material" payload.

It is a governance material package, NOT a formal approval.
admission_candidate means "Step 3 review candidate material is ready",
NOT "Step 3 is approved" or "real acceptance is granted".

No formal acceptance / formal claim / real acceptance language.

Step 2 boundary:
  - evidence_source = "simulated"
  - not_real_acceptance_evidence = True
  - not_ready_for_formal_claim = True
  - reviewer_only = True
  - readiness_mapping_only = True
  - primary_evidence_rewritten = False
  - real_acceptance_ready = False
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_CONTRACTS_VERSION: str = "2.24.0"

# ---------------------------------------------------------------------------
# Status buckets
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_STATUS_OK: str = "ok"
ADMISSION_DOSSIER_STATUS_ATTENTION: str = "attention"
ADMISSION_DOSSIER_STATUS_BLOCKER: str = "blocker"
ADMISSION_DOSSIER_STATUS_REVIEWER_ONLY: str = "reviewer_only"

# ---------------------------------------------------------------------------
# Title / summary (Chinese default / English fallback)
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_TITLE_ZH: str = "Step 3 准入材料"
ADMISSION_DOSSIER_TITLE_EN: str = "Step 3 Admission Dossier"

ADMISSION_DOSSIER_SUMMARY_ZH: str = (
    "Step 3 准入材料：汇总 Step 2 全部收官证据，判断当前是否具备进入下一阶段审阅的条件。"
    "仅为阶段准入治理材料，不是正式放行结论，不是 real acceptance evidence。"
)
ADMISSION_DOSSIER_SUMMARY_EN: str = (
    "Step 3 admission dossier: aggregates all Step 2 closeout evidence to determine "
    "if conditions for next-stage review are met. This is a governance material package only, "
    "not a formal release conclusion. Not real acceptance evidence."
)

# ---------------------------------------------------------------------------
# Section ordering
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_SECTION_ORDER: tuple[str, ...] = (
    "freeze_audit",
    "closeout_package",
    "closeout_readiness",
    "governance_handoff",
    "parity_resilience",
    "phase_evidence",
    "blockers",
    "next_steps",
    "boundary",
)

# ---------------------------------------------------------------------------
# Section labels (Chinese default / English fallback)
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_SECTION_LABELS_ZH: dict[str, str] = {
    "freeze_audit": "冻结审计",
    "closeout_package": "收官包",
    "closeout_readiness": "收官就绪度",
    "governance_handoff": "治理交接",
    "parity_resilience": "一致性/韧性",
    "phase_evidence": "阶段证据 / V1.2 对齐",
    "blockers": "阻塞项",
    "next_steps": "下一步",
    "boundary": "边界声明",
}

ADMISSION_DOSSIER_SECTION_LABELS_EN: dict[str, str] = {
    "freeze_audit": "Freeze Audit",
    "closeout_package": "Closeout Package",
    "closeout_readiness": "Closeout Readiness",
    "governance_handoff": "Governance Handoff",
    "parity_resilience": "Parity/Resilience",
    "phase_evidence": "Phase Evidence / V1.2 Alignment",
    "blockers": "Blockers",
    "next_steps": "Next Steps",
    "boundary": "Boundary Declaration",
}

# ---------------------------------------------------------------------------
# Status labels
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_STATUS_LABELS_ZH: dict[str, str] = {
    ADMISSION_DOSSIER_STATUS_OK: "准入材料就绪",
    ADMISSION_DOSSIER_STATUS_ATTENTION: "存在需关注项",
    ADMISSION_DOSSIER_STATUS_BLOCKER: "存在阻塞项",
    ADMISSION_DOSSIER_STATUS_REVIEWER_ONLY: "仅限审阅观察",
}

ADMISSION_DOSSIER_STATUS_LABELS_EN: dict[str, str] = {
    ADMISSION_DOSSIER_STATUS_OK: "Admission material ready",
    ADMISSION_DOSSIER_STATUS_ATTENTION: "Attention items present",
    ADMISSION_DOSSIER_STATUS_BLOCKER: "Blockers present",
    ADMISSION_DOSSIER_STATUS_REVIEWER_ONLY: "Reviewer-only observation",
}

# ---------------------------------------------------------------------------
# Blocker labels
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_BLOCKER_KEYS: tuple[str, ...] = (
    "freeze_audit_blocker",
    "closeout_package_blocker",
    "closeout_readiness_blocker",
    "governance_blocker",
    "parity_mismatch",
    "resilience_failure",
    "missing_freeze_audit",
    "missing_closeout_package",
    "real_acceptance_not_ready",
)

ADMISSION_DOSSIER_BLOCKER_LABELS_ZH: dict[str, str] = {
    "freeze_audit_blocker": "冻结审计存在阻塞项",
    "closeout_package_blocker": "收官包存在阻塞项",
    "closeout_readiness_blocker": "收官就绪度存在阻塞项",
    "governance_blocker": "治理交接存在阻塞项",
    "parity_mismatch": "一致性校验不通过",
    "resilience_failure": "韧性测试失败",
    "missing_freeze_audit": "冻结审计数据缺失",
    "missing_closeout_package": "收官包数据缺失",
    "real_acceptance_not_ready": "真实验收尚未就绪（需 Step 3 真机验证）",
}

ADMISSION_DOSSIER_BLOCKER_LABELS_EN: dict[str, str] = {
    "freeze_audit_blocker": "Freeze audit has blockers",
    "closeout_package_blocker": "Closeout package has blockers",
    "closeout_readiness_blocker": "Closeout readiness has blockers",
    "governance_blocker": "Governance handoff has blockers",
    "parity_mismatch": "Parity check failed",
    "resilience_failure": "Resilience test failed",
    "missing_freeze_audit": "Freeze audit data missing",
    "missing_closeout_package": "Closeout package data missing",
    "real_acceptance_not_ready": "Real acceptance not ready (requires Step 3 real device validation)",
}

# ---------------------------------------------------------------------------
# Next-step labels
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_NEXT_STEP_KEYS: tuple[str, ...] = (
    "resolve_blockers",
    "complete_simulation_evidence",
    "obtain_real_device_access",
    "run_step3_real_validation",
    "review_governance_handoff",
)

ADMISSION_DOSSIER_NEXT_STEPS_ZH: dict[str, str] = {
    "resolve_blockers": "解决当前阻塞项",
    "complete_simulation_evidence": "补全仿真证据",
    "obtain_real_device_access": "获取真实设备访问授权",
    "run_step3_real_validation": "运行 Step 3 真实验证（需明确授权）",
    "review_governance_handoff": "审阅治理交接",
}

ADMISSION_DOSSIER_NEXT_STEPS_EN: dict[str, str] = {
    "resolve_blockers": "Resolve current blockers",
    "complete_simulation_evidence": "Complete simulation evidence",
    "obtain_real_device_access": "Obtain real device access authorization",
    "run_step3_real_validation": "Run Step 3 real validation (requires explicit authorization)",
    "review_governance_handoff": "Review governance handoff",
}

# ---------------------------------------------------------------------------
# Admission candidate notice
# ---------------------------------------------------------------------------

ADMISSION_CANDIDATE_NOTICE_ZH: str = (
    "admission_candidate = True 仅表示 Step 3 审阅候选材料已具备，"
    "不是 Step 3 已批准，不是正式放行结论。"
)
ADMISSION_CANDIDATE_NOTICE_EN: str = (
    "admission_candidate = True means Step 3 review candidate material is ready only, "
    "not Step 3 approval, not formal release conclusion."
)

# ---------------------------------------------------------------------------
# Simulation-only / reviewer-only / non-claim boundary markers
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_SIMULATION_ONLY_BOUNDARY_ZH: str = (
    "本准入材料仅基于 simulation / offline / headless 证据，"
    "不代表 real acceptance evidence，不构成正式放行结论。"
    "进入 Step 3 真实验证仍需明确授权和真实设备验证。"
)
ADMISSION_DOSSIER_SIMULATION_ONLY_BOUNDARY_EN: str = (
    "This admission dossier is based on simulation / offline / headless evidence only. "
    "It does not represent real acceptance evidence and does not constitute a formal release conclusion. "
    "Proceeding to Step 3 real validation still requires explicit authorization and real device verification."
)

ADMISSION_DOSSIER_REVIEWER_ONLY_NOTICE_ZH: str = (
    "本准入材料仅供 reviewer 审阅，不作为 operator 操作依据，不形成 formal compliance claim。"
)
ADMISSION_DOSSIER_REVIEWER_ONLY_NOTICE_EN: str = (
    "This admission dossier is for reviewer review only, not as operator action basis, "
    "and does not form formal compliance claim."
)

ADMISSION_DOSSIER_NON_CLAIM_NOTICE_ZH: str = (
    "不形成 formal compliance claim / accreditation claim / real acceptance evidence。"
)
ADMISSION_DOSSIER_NON_CLAIM_NOTICE_EN: str = (
    "Does not form formal compliance claim / accreditation claim / real acceptance evidence."
)

# ---------------------------------------------------------------------------
# Step 2 boundary markers
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_STEP2_BOUNDARY: dict[str, bool | str] = {
    "evidence_source": "simulated",
    "not_real_acceptance_evidence": True,
    "not_ready_for_formal_claim": True,
    "reviewer_only": True,
    "readiness_mapping_only": True,
    "primary_evidence_rewritten": False,
    "real_acceptance_ready": False,
}

# ---------------------------------------------------------------------------
# i18n keys
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_I18N_KEYS: dict[str, str] = {
    "title": "admission_dossier.title",
    "summary": "admission_dossier.summary",
    "section_freeze_audit": "admission_dossier.section.freeze_audit",
    "section_closeout_package": "admission_dossier.section.closeout_package",
    "section_closeout_readiness": "admission_dossier.section.closeout_readiness",
    "section_governance_handoff": "admission_dossier.section.governance_handoff",
    "section_parity_resilience": "admission_dossier.section.parity_resilience",
    "section_phase_evidence": "admission_dossier.section.phase_evidence",
    "section_blockers": "admission_dossier.section.blockers",
    "section_next_steps": "admission_dossier.section.next_steps",
    "section_boundary": "admission_dossier.section.boundary",
    "status_ok": "admission_dossier.status.ok",
    "status_attention": "admission_dossier.status.attention",
    "status_blocker": "admission_dossier.status.blocker",
    "status_reviewer_only": "admission_dossier.status.reviewer_only",
    "simulation_only_boundary": "admission_dossier.simulation_only_boundary",
    "reviewer_only_notice": "admission_dossier.reviewer_only_notice",
    "non_claim_notice": "admission_dossier.non_claim_notice",
    "admission_candidate_notice": "admission_dossier.admission_candidate_notice",
    "no_content": "admission_dossier.no_content",
    "panel": "admission_dossier.panel",
    "dossier_status": "admission_dossier.dossier_status",
    "admission_dossier_source": "admission_dossier.admission_dossier_source",
    "dossier_sections": "admission_dossier.dossier_sections",
    "admission_candidate": "admission_dossier.admission_candidate",
}

# ---------------------------------------------------------------------------
# Resolve helpers
# ---------------------------------------------------------------------------


def resolve_admission_dossier_title(*, lang: str = "zh") -> str:
    return ADMISSION_DOSSIER_TITLE_EN if lang == "en" else ADMISSION_DOSSIER_TITLE_ZH


def resolve_admission_dossier_summary(*, lang: str = "zh") -> str:
    return ADMISSION_DOSSIER_SUMMARY_EN if lang == "en" else ADMISSION_DOSSIER_SUMMARY_ZH


def resolve_admission_dossier_section_label(key: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return ADMISSION_DOSSIER_SECTION_LABELS_EN.get(key, key)
    return ADMISSION_DOSSIER_SECTION_LABELS_ZH.get(key, key)


def resolve_admission_dossier_status_label(status: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return ADMISSION_DOSSIER_STATUS_LABELS_EN.get(status, status)
    return ADMISSION_DOSSIER_STATUS_LABELS_ZH.get(status, status)


def resolve_admission_dossier_blocker_label(key: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return ADMISSION_DOSSIER_BLOCKER_LABELS_EN.get(key, key)
    return ADMISSION_DOSSIER_BLOCKER_LABELS_ZH.get(key, key)


def resolve_admission_dossier_next_step_label(key: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return ADMISSION_DOSSIER_NEXT_STEPS_EN.get(key, key)
    return ADMISSION_DOSSIER_NEXT_STEPS_ZH.get(key, key)


def resolve_admission_dossier_simulation_only_boundary(*, lang: str = "zh") -> str:
    return ADMISSION_DOSSIER_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else ADMISSION_DOSSIER_SIMULATION_ONLY_BOUNDARY_ZH


def resolve_admission_dossier_reviewer_only_notice(*, lang: str = "zh") -> str:
    return ADMISSION_DOSSIER_REVIEWER_ONLY_NOTICE_EN if lang == "en" else ADMISSION_DOSSIER_REVIEWER_ONLY_NOTICE_ZH


def resolve_admission_dossier_non_claim_notice(*, lang: str = "zh") -> str:
    return ADMISSION_DOSSIER_NON_CLAIM_NOTICE_EN if lang == "en" else ADMISSION_DOSSIER_NON_CLAIM_NOTICE_ZH


def resolve_admission_candidate_notice(*, lang: str = "zh") -> str:
    return ADMISSION_CANDIDATE_NOTICE_EN if lang == "en" else ADMISSION_CANDIDATE_NOTICE_ZH
