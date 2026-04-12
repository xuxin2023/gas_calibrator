"""Shared contract constants for Step 2 tail governance handoff artifacts.

Single source of truth for artifact keys, filenames, roles, display labels,
i18n keys, and canonical ordering of the governance handoff chain.

Step 2 boundary:
  - evidence_source = "simulated"
  - not_real_acceptance_evidence = True
  - not_ready_for_formal_claim = True
  - reviewer_only = True
  - readiness_mapping_only = True
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_CONTRACTS_VERSION: str = "2.6.1"

# ---------------------------------------------------------------------------
# Canonical artifact keys (in handoff chain order)
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_ARTIFACT_KEYS: tuple[str, ...] = (
    "step2_readiness_summary",
    "metrology_calibration_contract",
    "phase_transition_bridge",
    "phase_transition_bridge_reviewer_artifact",
    "stage_admission_review_pack",
    "stage_admission_review_pack_reviewer_artifact",
    "engineering_isolation_admission_checklist",
    "engineering_isolation_admission_checklist_reviewer_artifact",
)

# ---------------------------------------------------------------------------
# Filenames
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_FILENAMES: dict[str, str] = {
    "step2_readiness_summary": "step2_readiness_summary.json",
    "metrology_calibration_contract": "metrology_calibration_contract.json",
    "phase_transition_bridge": "phase_transition_bridge.json",
    "phase_transition_bridge_reviewer_artifact": "phase_transition_bridge_reviewer.md",
    "stage_admission_review_pack": "stage_admission_review_pack.json",
    "stage_admission_review_pack_reviewer_artifact": "stage_admission_review_pack.md",
    "engineering_isolation_admission_checklist": "engineering_isolation_admission_checklist.json",
    "engineering_isolation_admission_checklist_reviewer_artifact": "engineering_isolation_admission_checklist.md",
}

# ---------------------------------------------------------------------------
# Artifact roles
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_ROLES: dict[str, str] = {
    "step2_readiness_summary": "execution_summary",
    "metrology_calibration_contract": "execution_summary",
    "phase_transition_bridge": "execution_summary",
    "phase_transition_bridge_reviewer_artifact": "formal_analysis",
    "stage_admission_review_pack": "execution_summary",
    "stage_admission_review_pack_reviewer_artifact": "formal_analysis",
    "engineering_isolation_admission_checklist": "execution_summary",
    "engineering_isolation_admission_checklist_reviewer_artifact": "formal_analysis",
}

# ---------------------------------------------------------------------------
# Display labels (Chinese default)
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_DISPLAY_LABELS: dict[str, str] = {
    "step2_readiness_summary": "Step 2 就绪度摘要",
    "metrology_calibration_contract": "计量校准合同",
    "phase_transition_bridge": "阶段过渡桥接",
    "phase_transition_bridge_reviewer_artifact": "阶段过渡桥接审阅",
    "stage_admission_review_pack": "阶段准入评审包",
    "stage_admission_review_pack_reviewer_artifact": "阶段准入评审包审阅",
    "engineering_isolation_admission_checklist": "工程隔离准入清单",
    "engineering_isolation_admission_checklist_reviewer_artifact": "工程隔离准入清单审阅",
}

# ---------------------------------------------------------------------------
# Display labels (English fallback)
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN: dict[str, str] = {
    "step2_readiness_summary": "Step 2 Readiness Summary",
    "metrology_calibration_contract": "Metrology Calibration Contract",
    "phase_transition_bridge": "Phase Transition Bridge",
    "phase_transition_bridge_reviewer_artifact": "Phase Transition Bridge Review",
    "stage_admission_review_pack": "Stage Admission Review Pack",
    "stage_admission_review_pack_reviewer_artifact": "Stage Admission Review Pack Review",
    "engineering_isolation_admission_checklist": "Engineering Isolation Admission Checklist",
    "engineering_isolation_admission_checklist_reviewer_artifact": "Engineering Isolation Admission Checklist Review",
}

# ---------------------------------------------------------------------------
# i18n keys
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_I18N_KEYS: dict[str, str] = {
    "step2_readiness_summary": "governance_handoff.step2_readiness_summary",
    "metrology_calibration_contract": "governance_handoff.metrology_calibration_contract",
    "phase_transition_bridge": "governance_handoff.phase_transition_bridge",
    "phase_transition_bridge_reviewer_artifact": "governance_handoff.phase_transition_bridge_reviewer",
    "stage_admission_review_pack": "governance_handoff.stage_admission_review_pack",
    "stage_admission_review_pack_reviewer_artifact": "governance_handoff.stage_admission_review_pack_reviewer",
    "engineering_isolation_admission_checklist": "governance_handoff.engineering_isolation_admission_checklist",
    "engineering_isolation_admission_checklist_reviewer_artifact": "governance_handoff.engineering_isolation_admission_checklist_reviewer",
}

# ---------------------------------------------------------------------------
# Surface visibility / linked surfaces
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_SURFACE_VISIBILITY: dict[str, bool] = {
    "step2_readiness_summary": True,
    "metrology_calibration_contract": True,
    "phase_transition_bridge": True,
    "phase_transition_bridge_reviewer_artifact": True,
    "stage_admission_review_pack": True,
    "stage_admission_review_pack_reviewer_artifact": True,
    "engineering_isolation_admission_checklist": True,
    "engineering_isolation_admission_checklist_reviewer_artifact": True,
}

# ---------------------------------------------------------------------------
# Phase assignment
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_PHASES: dict[str, str] = {
    "step2_readiness_summary": "step2_readiness_bridge",
    "metrology_calibration_contract": "step2_tail_step3_bridge",
    "phase_transition_bridge": "step2_tail_stage3_bridge",
    "phase_transition_bridge_reviewer_artifact": "step2_tail_stage3_bridge",
    "stage_admission_review_pack": "step2_tail_stage3_bridge",
    "stage_admission_review_pack_reviewer_artifact": "step2_tail_stage3_bridge",
    "engineering_isolation_admission_checklist": "step2_tail_stage3_bridge",
    "engineering_isolation_admission_checklist_reviewer_artifact": "step2_tail_stage3_bridge",
}

# ---------------------------------------------------------------------------
# Step 2 boundary markers
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_STEP2_BOUNDARY: dict[str, dict[str, bool | str]] = {
    key: {
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
    }
    for key in GOVERNANCE_HANDOFF_ARTIFACT_KEYS
}

# ---------------------------------------------------------------------------
# Reviewer artifact pairing (primary -> reviewer)
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_REVIEWER_PAIRING: dict[str, str] = {
    "phase_transition_bridge": "phase_transition_bridge_reviewer_artifact",
    "stage_admission_review_pack": "stage_admission_review_pack_reviewer_artifact",
    "engineering_isolation_admission_checklist": "engineering_isolation_admission_checklist_reviewer_artifact",
}

# ---------------------------------------------------------------------------
# Helper: resolve display label
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Summary texts (Chinese default)
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_SUMMARY_TEXTS: dict[str, str] = {
    "step2_readiness_summary": "Step 2 就绪度摘要：汇总当前 Step 2 仿真 / 离线 / headless 验证状态。",
    "metrology_calibration_contract": "计量校准合同：当前已固化的 contract / schema / template / digest / reporting contract。",
    "phase_transition_bridge": "阶段桥工件：当前仍处于 Step 2 tail / Stage 3 bridge，用于说明离第三阶段真实计量验证还有多远。不是 real acceptance。",
    "phase_transition_bridge_reviewer_artifact": "阶段桥审阅工件：reviewer 对 phase transition bridge 的离线审阅留痕。不是 real acceptance。",
    "stage_admission_review_pack": "阶段准入评审包：汇总 readiness / metrology / bridge / reviewer 的治理交接状态。不是 real acceptance。",
    "stage_admission_review_pack_reviewer_artifact": "阶段准入评审包审阅：reviewer 对 stage admission review pack 的离线审阅留痕。不是 real acceptance。",
    "engineering_isolation_admission_checklist": "准入清单：基于现有 readiness / metrology / bridge / review pack 收口进入 engineering-isolation 前的已满足项、待确认项与仅限 Stage 3 的项。",
    "engineering_isolation_admission_checklist_reviewer_artifact": "工程隔离准入清单审阅：reviewer 对 engineering isolation admission checklist 的离线审阅留痕。不是 real acceptance。",
}

# ---------------------------------------------------------------------------
# Summary texts (English fallback)
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_SUMMARY_TEXTS_EN: dict[str, str] = {
    "step2_readiness_summary": "Step 2 readiness summary: current Step 2 simulation / offline / headless validation status.",
    "metrology_calibration_contract": "Metrology calibration contract: current institutionalized contract / schema / template / digest / reporting contract.",
    "phase_transition_bridge": "Phase transition bridge: currently in Step 2 tail / Stage 3 bridge, indicating distance to Stage 3 real metrology validation. Not real acceptance.",
    "phase_transition_bridge_reviewer_artifact": "Phase transition bridge review: reviewer offline review trace for phase transition bridge. Not real acceptance.",
    "stage_admission_review_pack": "Stage admission review pack: summary of readiness / metrology / bridge / reviewer governance handoff status. Not real acceptance.",
    "stage_admission_review_pack_reviewer_artifact": "Stage admission review pack review: reviewer offline review trace for stage admission review pack. Not real acceptance.",
    "engineering_isolation_admission_checklist": "Admission checklist: based on readiness / metrology / bridge / review pack, closing out satisfied items, pending items, and Stage 3-only items before engineering-isolation.",
    "engineering_isolation_admission_checklist_reviewer_artifact": "Engineering isolation admission checklist review: reviewer offline review trace for engineering isolation admission checklist. Not real acceptance.",
}

# ---------------------------------------------------------------------------
# Title texts (Chinese default / English fallback combined)
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_TITLE_TEXTS: dict[str, str] = {
    key: f"{GOVERNANCE_HANDOFF_DISPLAY_LABELS[key]} / {GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN[key]}"
    for key in GOVERNANCE_HANDOFF_ARTIFACT_KEYS
}

# ---------------------------------------------------------------------------
# Anchor IDs
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_ANCHOR_IDS: dict[str, str] = {
    "step2_readiness_summary": "step2-readiness-summary",
    "metrology_calibration_contract": "metrology-calibration-contract",
    "phase_transition_bridge": "phase-transition-bridge",
    "phase_transition_bridge_reviewer_artifact": "phase-transition-bridge-reviewer",
    "stage_admission_review_pack": "stage-admission-review-pack",
    "stage_admission_review_pack_reviewer_artifact": "stage-admission-review-pack-reviewer",
    "engineering_isolation_admission_checklist": "engineering-isolation-admission-checklist",
    "engineering_isolation_admission_checklist_reviewer_artifact": "engineering-isolation-admission-checklist-reviewer",
}

# ---------------------------------------------------------------------------
# Combined role texts (for artifact entries that combine primary + reviewer roles)
# ---------------------------------------------------------------------------

GOVERNANCE_HANDOFF_COMBINED_ROLE_TEXTS: dict[str, str] = {
    key: f"{GOVERNANCE_HANDOFF_ROLES[key]} + {GOVERNANCE_HANDOFF_ROLES[GOVERNANCE_HANDOFF_REVIEWER_PAIRING[key]]}"
    for key in GOVERNANCE_HANDOFF_REVIEWER_PAIRING
}

# ---------------------------------------------------------------------------
# Helper: resolve display label
# ---------------------------------------------------------------------------


def resolve_governance_handoff_display_label(
    key: str,
    *,
    lang: str = "zh",
) -> str:
    """Resolve display label for a governance handoff artifact key.

    Args:
        key: Artifact key from GOVERNANCE_HANDOFF_ARTIFACT_KEYS.
        lang: Language code ("zh" for Chinese default, "en" for English fallback).

    Returns:
        Display label string.
    """
    if lang == "en":
        return GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN.get(key, key)
    return GOVERNANCE_HANDOFF_DISPLAY_LABELS.get(key, key)


# ---------------------------------------------------------------------------
# Helper: resolve summary text
# ---------------------------------------------------------------------------


def resolve_governance_handoff_summary_text(
    key: str,
    *,
    lang: str = "zh",
) -> str:
    """Resolve summary text for a governance handoff artifact key.

    Args:
        key: Artifact key from GOVERNANCE_HANDOFF_ARTIFACT_KEYS.
        lang: Language code ("zh" for Chinese default, "en" for English fallback).

    Returns:
        Summary text string.
    """
    if lang == "en":
        return GOVERNANCE_HANDOFF_SUMMARY_TEXTS_EN.get(key, key)
    return GOVERNANCE_HANDOFF_SUMMARY_TEXTS.get(key, key)
