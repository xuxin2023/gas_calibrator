"""Step 2 closeout package contracts — single source of truth for the canonical
closeout package title, section ordering, section labels, boundary markers,
artifact names, and i18n keys.

The closeout package is the single canonical reviewer bundle that aggregates
all Step 2 closeout-related objects into one stable, exportable, replayable
package. It does NOT replace existing closeout readiness / digest / compact
summary payloads — it is an upper-level aggregation layer.

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

CLOSEOUT_PACKAGE_CONTRACTS_VERSION: str = "2.22.0"

# ---------------------------------------------------------------------------
# Package title / summary (Chinese default / English fallback)
# ---------------------------------------------------------------------------

CLOSEOUT_PACKAGE_TITLE_ZH: str = "Step 2 收官包"
CLOSEOUT_PACKAGE_TITLE_EN: str = "Step 2 Closeout Package"

CLOSEOUT_PACKAGE_SUMMARY_ZH: str = (
    "Step 2 仿真收官包：将当前阶段所有收官相关对象聚合为单一 canonical reviewer bundle。"
    "不是正式放行结论，不是 real acceptance evidence。"
)
CLOSEOUT_PACKAGE_SUMMARY_EN: str = (
    "Step 2 simulation closeout package: aggregates all closeout-related objects "
    "into a single canonical reviewer bundle. Not a formal release conclusion. "
    "Not real acceptance evidence."
)

# ---------------------------------------------------------------------------
# Section ordering — canonical order for the closeout package
# ---------------------------------------------------------------------------

CLOSEOUT_PACKAGE_SECTION_ORDER: tuple[str, ...] = (
    "readiness",
    "digest",
    "governance_handoff",
    "compact_summaries",
    "parity_resilience",
    "phase_evidence",
    "stage_admission",
    "engineering_isolation_checklist",
    "blockers",
    "next_steps",
    "boundary",
)

# ---------------------------------------------------------------------------
# Section labels (Chinese default / English fallback)
# ---------------------------------------------------------------------------

CLOSEOUT_PACKAGE_SECTION_LABELS_ZH: dict[str, str] = {
    "readiness": "收官就绪度",
    "digest": "收官摘要",
    "governance_handoff": "治理交接",
    "compact_summaries": "紧凑摘要",
    "parity_resilience": "一致性/韧性",
    "phase_evidence": "阶段证据 / V1.2 对齐",
    "stage_admission": "阶段准入审阅包",
    "engineering_isolation_checklist": "工程隔离准入清单",
    "blockers": "阻塞项",
    "next_steps": "下一步",
    "boundary": "边界声明",
}

CLOSEOUT_PACKAGE_SECTION_LABELS_EN: dict[str, str] = {
    "readiness": "Closeout Readiness",
    "digest": "Closeout Digest",
    "governance_handoff": "Governance Handoff",
    "compact_summaries": "Compact Summaries",
    "parity_resilience": "Parity/Resilience",
    "phase_evidence": "Phase Evidence / V1.2 Alignment",
    "stage_admission": "Stage Admission Review Pack",
    "engineering_isolation_checklist": "Engineering Isolation Admission Checklist",
    "blockers": "Blockers",
    "next_steps": "Next Steps",
    "boundary": "Boundary Declaration",
}

# ---------------------------------------------------------------------------
# Canonical artifact / field names
# ---------------------------------------------------------------------------

CLOSEOUT_PACKAGE_ARTIFACT_TYPE: str = "step2_closeout_package"
CLOSEOUT_PACKAGE_FILENAME: str = "step2_closeout_package.json"
CLOSEOUT_PACKAGE_REVIEWER_FILENAME: str = "step2_closeout_package_reviewer.json"

# ---------------------------------------------------------------------------
# Simulation-only / reviewer-only / non-claim boundary markers
# ---------------------------------------------------------------------------

CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_ZH: str = (
    "本收官包仅基于 simulation / offline / headless 证据，"
    "不代表 real acceptance evidence，不构成正式放行结论。"
)
CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_EN: str = (
    "This closeout package is based on simulation / offline / headless evidence only. "
    "It does not represent real acceptance evidence and does not constitute a formal release conclusion."
)

CLOSEOUT_PACKAGE_REVIEWER_ONLY_NOTICE_ZH: str = (
    "本收官包仅供 reviewer 审阅，不作为 operator 操作依据，不形成 formal compliance claim。"
)
CLOSEOUT_PACKAGE_REVIEWER_ONLY_NOTICE_EN: str = (
    "This closeout package is for reviewer review only, not as operator action basis, "
    "and does not form formal compliance claim."
)

CLOSEOUT_PACKAGE_NON_CLAIM_NOTICE_ZH: str = (
    "不形成 formal compliance claim / accreditation claim / real acceptance evidence。"
)
CLOSEOUT_PACKAGE_NON_CLAIM_NOTICE_EN: str = (
    "Does not form formal compliance claim / accreditation claim / real acceptance evidence."
)

# ---------------------------------------------------------------------------
# Step 2 boundary markers (constant for all closeout package payloads)
# ---------------------------------------------------------------------------

CLOSEOUT_PACKAGE_STEP2_BOUNDARY: dict[str, bool | str] = {
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

CLOSEOUT_PACKAGE_I18N_KEYS: dict[str, str] = {
    "title": "closeout_package.title",
    "summary": "closeout_package.summary",
    "section_readiness": "closeout_package.section.readiness",
    "section_digest": "closeout_package.section.digest",
    "section_governance_handoff": "closeout_package.section.governance_handoff",
    "section_compact_summaries": "closeout_package.section.compact_summaries",
    "section_parity_resilience": "closeout_package.section.parity_resilience",
    "section_phase_evidence": "closeout_package.section.phase_evidence",
    "section_stage_admission": "closeout_package.section.stage_admission",
    "section_engineering_isolation_checklist": "closeout_package.section.engineering_isolation_checklist",
    "section_blockers": "closeout_package.section.blockers",
    "section_next_steps": "closeout_package.section.next_steps",
    "section_boundary": "closeout_package.section.boundary",
    "simulation_only_boundary": "closeout_package.simulation_only_boundary",
    "reviewer_only_notice": "closeout_package.reviewer_only_notice",
    "non_claim_notice": "closeout_package.non_claim_notice",
    "package_status": "closeout_package.package_status",
    "no_content": "closeout_package.no_content",
}

# ---------------------------------------------------------------------------
# Resolve helpers
# ---------------------------------------------------------------------------


def resolve_closeout_package_title(*, lang: str = "zh") -> str:
    return CLOSEOUT_PACKAGE_TITLE_EN if lang == "en" else CLOSEOUT_PACKAGE_TITLE_ZH


def resolve_closeout_package_summary(*, lang: str = "zh") -> str:
    return CLOSEOUT_PACKAGE_SUMMARY_EN if lang == "en" else CLOSEOUT_PACKAGE_SUMMARY_ZH


def resolve_closeout_package_section_label(key: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return CLOSEOUT_PACKAGE_SECTION_LABELS_EN.get(key, key)
    return CLOSEOUT_PACKAGE_SECTION_LABELS_ZH.get(key, key)


def resolve_closeout_package_simulation_only_boundary(*, lang: str = "zh") -> str:
    return CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_ZH


def resolve_closeout_package_reviewer_only_notice(*, lang: str = "zh") -> str:
    return CLOSEOUT_PACKAGE_REVIEWER_ONLY_NOTICE_EN if lang == "en" else CLOSEOUT_PACKAGE_REVIEWER_ONLY_NOTICE_ZH


def resolve_closeout_package_non_claim_notice(*, lang: str = "zh") -> str:
    return CLOSEOUT_PACKAGE_NON_CLAIM_NOTICE_EN if lang == "en" else CLOSEOUT_PACKAGE_NON_CLAIM_NOTICE_ZH
