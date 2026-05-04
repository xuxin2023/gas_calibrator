"""Step 2 freeze audit contracts — single source of truth for the release-candidate
freeze audit title, section ordering, section labels, status buckets,
blocker/next-step labels, boundary markers, and i18n keys.

The freeze audit is an upper-level aggregation view on top of the closeout package.
It does NOT replace the closeout package — it provides a reviewer-first RC/freeze
audit perspective that answers: "Is Step 2 in RC review candidate state? What's
blocking? Why is this not Step 3 / real acceptance?"

No formal acceptance / formal claim / real acceptance language.
freeze_candidate means "Step 2 RC review candidate", NOT "release approval".

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

FREEZE_AUDIT_CONTRACTS_VERSION: str = "2.23.0"

# ---------------------------------------------------------------------------
# Status buckets (aligned with closeout readiness)
# ---------------------------------------------------------------------------

FREEZE_AUDIT_STATUS_OK: str = "ok"
FREEZE_AUDIT_STATUS_ATTENTION: str = "attention"
FREEZE_AUDIT_STATUS_BLOCKER: str = "blocker"
FREEZE_AUDIT_STATUS_REVIEWER_ONLY: str = "reviewer_only"

# ---------------------------------------------------------------------------
# Title / summary (Chinese default / English fallback)
# ---------------------------------------------------------------------------

FREEZE_AUDIT_TITLE_ZH: str = "Step 2 冻结审计"
FREEZE_AUDIT_TITLE_EN: str = "Step 2 Freeze Audit"

FREEZE_AUDIT_SUMMARY_ZH: str = (
    "Step 2 release-candidate 冻结审计：汇总 suite / parity / resilience / governance / closeout 状态，"
    "判断当前是否进入 RC 审阅候选态。不是正式放行结论，不是 real acceptance evidence。"
)
FREEZE_AUDIT_SUMMARY_EN: str = (
    "Step 2 release-candidate freeze audit: aggregates suite / parity / resilience / "
    "governance / closeout status to determine RC review candidate state. "
    "Not a formal release conclusion. Not real acceptance evidence."
)

# ---------------------------------------------------------------------------
# Section ordering — canonical order for the freeze audit
# ---------------------------------------------------------------------------

FREEZE_AUDIT_SECTION_ORDER: tuple[str, ...] = (
    "suite",
    "parity",
    "resilience",
    "governance",
    "closeout",
    "boundary",
)

# ---------------------------------------------------------------------------
# Section labels (Chinese default / English fallback)
# ---------------------------------------------------------------------------

FREEZE_AUDIT_SECTION_LABELS_ZH: dict[str, str] = {
    "suite": "测试套件",
    "parity": "一致性",
    "resilience": "韧性",
    "governance": "治理",
    "closeout": "收官包",
    "boundary": "边界声明",
}

FREEZE_AUDIT_SECTION_LABELS_EN: dict[str, str] = {
    "suite": "Test Suite",
    "parity": "Parity",
    "resilience": "Resilience",
    "governance": "Governance",
    "closeout": "Closeout Package",
    "boundary": "Boundary Declaration",
}

# ---------------------------------------------------------------------------
# Status labels (Chinese default / English fallback)
# ---------------------------------------------------------------------------

FREEZE_AUDIT_STATUS_LABELS_ZH: dict[str, str] = {
    FREEZE_AUDIT_STATUS_OK: "冻结审计就绪",
    FREEZE_AUDIT_STATUS_ATTENTION: "存在需关注项",
    FREEZE_AUDIT_STATUS_BLOCKER: "存在阻塞项",
    FREEZE_AUDIT_STATUS_REVIEWER_ONLY: "仅限审阅观察",
}

FREEZE_AUDIT_STATUS_LABELS_EN: dict[str, str] = {
    FREEZE_AUDIT_STATUS_OK: "Freeze audit ready",
    FREEZE_AUDIT_STATUS_ATTENTION: "Attention items present",
    FREEZE_AUDIT_STATUS_BLOCKER: "Blockers present",
    FREEZE_AUDIT_STATUS_REVIEWER_ONLY: "Reviewer-only observation",
}

# ---------------------------------------------------------------------------
# Blocker labels (Chinese default / English fallback)
# ---------------------------------------------------------------------------

FREEZE_AUDIT_BLOCKER_KEYS: tuple[str, ...] = (
    "closeout_blocker",
    "parity_mismatch",
    "resilience_failure",
    "governance_blocker",
    "suite_failure",
    "missing_closeout_package",
)

FREEZE_AUDIT_BLOCKER_LABELS_ZH: dict[str, str] = {
    "closeout_blocker": "收官包存在阻塞项",
    "parity_mismatch": "一致性校验不通过",
    "resilience_failure": "韧性测试失败",
    "governance_blocker": "治理交接存在阻塞项",
    "suite_failure": "测试套件存在失败",
    "missing_closeout_package": "收官包数据缺失",
}

FREEZE_AUDIT_BLOCKER_LABELS_EN: dict[str, str] = {
    "closeout_blocker": "Closeout package has blockers",
    "parity_mismatch": "Parity check failed",
    "resilience_failure": "Resilience test failed",
    "governance_blocker": "Governance handoff has blockers",
    "suite_failure": "Test suite has failures",
    "missing_closeout_package": "Closeout package data missing",
}

# ---------------------------------------------------------------------------
# Next-step labels (Chinese default / English fallback)
# ---------------------------------------------------------------------------

FREEZE_AUDIT_NEXT_STEP_KEYS: tuple[str, ...] = (
    "resolve_blockers",
    "run_parity_check",
    "run_resilience_test",
    "review_governance_handoff",
    "run_full_suite",
    "proceed_to_step3_real_validation",
)

FREEZE_AUDIT_NEXT_STEPS_ZH: dict[str, str] = {
    "resolve_blockers": "解决当前阻塞项",
    "run_parity_check": "运行一致性校验",
    "run_resilience_test": "运行韧性测试",
    "review_governance_handoff": "审阅治理交接",
    "run_full_suite": "运行完整测试套件",
    "proceed_to_step3_real_validation": "进入 Step 3 真实验证（需明确授权）",
}

FREEZE_AUDIT_NEXT_STEPS_EN: dict[str, str] = {
    "resolve_blockers": "Resolve current blockers",
    "run_parity_check": "Run parity check",
    "run_resilience_test": "Run resilience test",
    "review_governance_handoff": "Review governance handoff",
    "run_full_suite": "Run full test suite",
    "proceed_to_step3_real_validation": "Proceed to Step 3 real validation (requires explicit authorization)",
}

# ---------------------------------------------------------------------------
# Freeze candidate notice (Chinese default / English fallback)
# ---------------------------------------------------------------------------

FREEZE_CANDIDATE_NOTICE_ZH: str = (
    "freeze_candidate = True 仅表示 Step 2 RC 审阅候选，不是正式放行批准。"
)
FREEZE_CANDIDATE_NOTICE_EN: str = (
    "freeze_candidate = True means Step 2 RC review candidate only, not formal release approval."
)

# ---------------------------------------------------------------------------
# Simulation-only / reviewer-only / non-claim boundary markers
# ---------------------------------------------------------------------------

FREEZE_AUDIT_SIMULATION_ONLY_BOUNDARY_ZH: str = (
    "本冻结审计仅基于 simulation / offline / headless 证据，"
    "不代表 real acceptance evidence，不构成正式放行结论。"
)
FREEZE_AUDIT_SIMULATION_ONLY_BOUNDARY_EN: str = (
    "This freeze audit is based on simulation / offline / headless evidence only. "
    "It does not represent real acceptance evidence and does not constitute a formal release conclusion."
)

FREEZE_AUDIT_REVIEWER_ONLY_NOTICE_ZH: str = (
    "本冻结审计仅供 reviewer 审阅，不作为 operator 操作依据，不形成 formal compliance claim。"
)
FREEZE_AUDIT_REVIEWER_ONLY_NOTICE_EN: str = (
    "This freeze audit is for reviewer review only, not as operator action basis, "
    "and does not form formal compliance claim."
)

FREEZE_AUDIT_NON_CLAIM_NOTICE_ZH: str = (
    "不形成 formal compliance claim / accreditation claim / real acceptance evidence。"
)
FREEZE_AUDIT_NON_CLAIM_NOTICE_EN: str = (
    "Does not form formal compliance claim / accreditation claim / real acceptance evidence."
)

# ---------------------------------------------------------------------------
# Step 2 boundary markers (constant for all freeze audit payloads)
# ---------------------------------------------------------------------------

FREEZE_AUDIT_STEP2_BOUNDARY: dict[str, bool | str] = {
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

FREEZE_AUDIT_I18N_KEYS: dict[str, str] = {
    "title": "freeze_audit.title",
    "summary": "freeze_audit.summary",
    "section_suite": "freeze_audit.section.suite",
    "section_parity": "freeze_audit.section.parity",
    "section_resilience": "freeze_audit.section.resilience",
    "section_governance": "freeze_audit.section.governance",
    "section_closeout": "freeze_audit.section.closeout",
    "section_boundary": "freeze_audit.section.boundary",
    "status_ok": "freeze_audit.status.ok",
    "status_attention": "freeze_audit.status.attention",
    "status_blocker": "freeze_audit.status.blocker",
    "status_reviewer_only": "freeze_audit.status.reviewer_only",
    "simulation_only_boundary": "freeze_audit.simulation_only_boundary",
    "reviewer_only_notice": "freeze_audit.reviewer_only_notice",
    "non_claim_notice": "freeze_audit.non_claim_notice",
    "freeze_candidate_notice": "freeze_audit.freeze_candidate_notice",
    "no_content": "freeze_audit.no_content",
    "panel": "freeze_audit.panel",
    "audit_status": "freeze_audit.audit_status",
}

# ---------------------------------------------------------------------------
# Resolve helpers
# ---------------------------------------------------------------------------


def resolve_freeze_audit_title(*, lang: str = "zh") -> str:
    return FREEZE_AUDIT_TITLE_EN if lang == "en" else FREEZE_AUDIT_TITLE_ZH


def resolve_freeze_audit_summary(*, lang: str = "zh") -> str:
    return FREEZE_AUDIT_SUMMARY_EN if lang == "en" else FREEZE_AUDIT_SUMMARY_ZH


def resolve_freeze_audit_section_label(key: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return FREEZE_AUDIT_SECTION_LABELS_EN.get(key, key)
    return FREEZE_AUDIT_SECTION_LABELS_ZH.get(key, key)


def resolve_freeze_audit_status_label(status: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return FREEZE_AUDIT_STATUS_LABELS_EN.get(status, status)
    return FREEZE_AUDIT_STATUS_LABELS_ZH.get(status, status)


def resolve_freeze_audit_blocker_label(key: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return FREEZE_AUDIT_BLOCKER_LABELS_EN.get(key, key)
    return FREEZE_AUDIT_BLOCKER_LABELS_ZH.get(key, key)


def resolve_freeze_audit_next_step_label(key: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return FREEZE_AUDIT_NEXT_STEPS_EN.get(key, key)
    return FREEZE_AUDIT_NEXT_STEPS_ZH.get(key, key)


def resolve_freeze_audit_simulation_only_boundary(*, lang: str = "zh") -> str:
    return FREEZE_AUDIT_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else FREEZE_AUDIT_SIMULATION_ONLY_BOUNDARY_ZH


def resolve_freeze_audit_reviewer_only_notice(*, lang: str = "zh") -> str:
    return FREEZE_AUDIT_REVIEWER_ONLY_NOTICE_EN if lang == "en" else FREEZE_AUDIT_REVIEWER_ONLY_NOTICE_ZH


def resolve_freeze_audit_non_claim_notice(*, lang: str = "zh") -> str:
    return FREEZE_AUDIT_NON_CLAIM_NOTICE_EN if lang == "en" else FREEZE_AUDIT_NON_CLAIM_NOTICE_ZH


def resolve_freeze_candidate_notice(*, lang: str = "zh") -> str:
    return FREEZE_CANDIDATE_NOTICE_EN if lang == "en" else FREEZE_CANDIDATE_NOTICE_ZH
