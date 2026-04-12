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

GOVERNANCE_HANDOFF_CONTRACTS_VERSION: str = "2.6.0"

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
    "engineering_isolation_admission_checklist": "engineering_isolation_admission_checklist_reviewer_arteract",
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
