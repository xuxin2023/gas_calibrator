"""Step 2 freeze audit builder — aggregate from closeout package, closeout readiness,
parity/resilience, governance handoff, acceptance governance, and phase evidence
into a single release-candidate freeze audit payload for reviewer consumption.

This is an upper-level aggregation view on top of the closeout package.
It does NOT replace the closeout package — it provides a reviewer-first RC/freeze
audit perspective.

Does NOT claim formal acceptance / formal approval.
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

from datetime import datetime, timezone
from typing import Any

from .step2_freeze_audit_contracts import (
    FREEZE_AUDIT_STATUS_OK,
    FREEZE_AUDIT_STATUS_ATTENTION,
    FREEZE_AUDIT_STATUS_BLOCKER,
    FREEZE_AUDIT_STATUS_REVIEWER_ONLY,
    FREEZE_AUDIT_SECTION_ORDER,
    FREEZE_AUDIT_TITLE_ZH,
    FREEZE_AUDIT_TITLE_EN,
    FREEZE_AUDIT_SUMMARY_ZH,
    FREEZE_AUDIT_SUMMARY_EN,
    FREEZE_AUDIT_STEP2_BOUNDARY,
    FREEZE_AUDIT_SIMULATION_ONLY_BOUNDARY_ZH,
    FREEZE_AUDIT_SIMULATION_ONLY_BOUNDARY_EN,
    resolve_freeze_audit_title,
    resolve_freeze_audit_summary,
    resolve_freeze_audit_section_label,
    resolve_freeze_audit_status_label,
    resolve_freeze_audit_blocker_label,
    resolve_freeze_audit_next_step_label,
    resolve_freeze_audit_simulation_only_boundary,
    resolve_freeze_audit_reviewer_only_notice,
    resolve_freeze_audit_non_claim_notice,
    resolve_freeze_candidate_notice,
)
from .step2_closeout_readiness_contracts import (
    CLOSEOUT_STATUS_OK,
    CLOSEOUT_STATUS_ATTENTION,
    CLOSEOUT_STATUS_BLOCKER,
    CLOSEOUT_STATUS_REVIEWER_ONLY,
)

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

FREEZE_AUDIT_BUILDER_VERSION: str = "2.23.0"


# ---------------------------------------------------------------------------
# build_step2_freeze_audit — main entry point
# ---------------------------------------------------------------------------

def build_step2_freeze_audit(
    *,
    run_id: str = "",
    # Core inputs
    step2_closeout_package: dict[str, Any] | None = None,
    step2_closeout_readiness: dict[str, Any] | None = None,
    parity_resilience_summary: dict[str, Any] | None = None,
    governance_handoff: dict[str, Any] | None = None,
    acceptance_governance: dict[str, Any] | None = None,
    phase_evidence: dict[str, Any] | None = None,
    # Optional signals
    suite_signals: dict[str, Any] | None = None,
    # Config
    lang: str = "zh",
) -> dict[str, Any]:
    """Build a Step 2 release-candidate freeze audit payload.

    Aggregates from closeout package + closeout readiness + parity/resilience +
    governance + acceptance governance + phase evidence into a single RC/freeze
    audit view. Does not replace the closeout package.

    All boundary markers are enforced: simulation-only, not real acceptance,
    not ready for formal claim, reviewer-only.

    Args:
        run_id: Current run identifier.
        step2_closeout_package: Output of build_step2_closeout_package.
        step2_closeout_readiness: Output of build_step2_closeout_readiness.
        parity_resilience_summary: Parity/resilience summary payload.
        governance_handoff: Governance handoff payload.
        acceptance_governance: Acceptance governance payload.
        phase_evidence: Phase evidence payload.
        suite_signals: Optional suite/smoke/regression signals.
        lang: "zh" (default) or "en".

    Returns:
        Dict with freeze audit fields suitable for reviewer display.
    """
    _pkg = dict(step2_closeout_package or {})
    _readiness = dict(step2_closeout_readiness or {})
    _parity = dict(parity_resilience_summary or {})
    _governance = dict(governance_handoff or {})
    _acceptance = dict(acceptance_governance or {})
    _phase = dict(phase_evidence or {})
    _suite = dict(suite_signals or {})

    # --- Build audit sections ---
    audit_sections = _build_audit_sections(
        pkg=_pkg,
        readiness=_readiness,
        parity=_parity,
        governance=_governance,
        suite=_suite,
        lang=lang,
    )

    # --- Build blockers ---
    blockers = _build_blockers(
        pkg=_pkg,
        readiness=_readiness,
        parity=_parity,
        governance=_governance,
        suite=_suite,
        lang=lang,
    )

    # --- Build next steps ---
    next_steps = _build_next_steps(
        blockers=blockers,
        pkg=_pkg,
        lang=lang,
    )

    # --- Derive audit_status ---
    audit_status = _derive_audit_status(
        blockers=blockers,
        audit_sections=audit_sections,
    )

    # --- Derive freeze_candidate ---
    freeze_candidate = _derive_freeze_candidate(
        audit_status=audit_status,
        pkg=_pkg,
    )

    # --- Build reviewer summary line ---
    reviewer_summary_line = _build_reviewer_summary_line(
        audit_status=audit_status,
        freeze_candidate=freeze_candidate,
        lang=lang,
    )

    # --- Build reviewer summary lines ---
    reviewer_summary_lines = _build_reviewer_summary_lines(
        audit_status=audit_status,
        freeze_candidate=freeze_candidate,
        blockers=blockers,
        next_steps=next_steps,
        lang=lang,
    )

    # --- Build simulation-only boundary ---
    simulation_only_boundary = _build_simulation_only_boundary()

    # --- Freeze candidate notice ---
    freeze_candidate_notice = resolve_freeze_candidate_notice(lang=lang)

    return {
        "schema_version": "1.0",
        "artifact_type": "step2_freeze_audit",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "phase": "step2_freeze_audit",
        "audit_version": FREEZE_AUDIT_BUILDER_VERSION,
        "audit_status": audit_status,
        "audit_status_label": resolve_freeze_audit_status_label(audit_status, lang=lang),
        "reviewer_summary_line": reviewer_summary_line,
        "reviewer_summary_lines": reviewer_summary_lines,
        "blockers": blockers,
        "next_steps": next_steps,
        "audit_sections": audit_sections,
        "section_order": list(FREEZE_AUDIT_SECTION_ORDER),
        "freeze_candidate": freeze_candidate,
        "freeze_candidate_notice_zh": resolve_freeze_candidate_notice(lang="zh"),
        "freeze_candidate_notice_en": resolve_freeze_candidate_notice(lang="en"),
        "simulation_only_boundary": simulation_only_boundary,
        "freeze_audit_source": "rebuilt",
        # Step 2 boundary markers — always enforced
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_audit_sections(
    *,
    pkg: dict[str, Any],
    readiness: dict[str, Any],
    parity: dict[str, Any],
    governance: dict[str, Any],
    suite: dict[str, Any],
    lang: str,
) -> dict[str, dict[str, Any]]:
    """Build audit sections from source payloads."""
    sections: dict[str, dict[str, Any]] = {}

    # suite
    suite_status = str(suite.get("status") or "")
    suite_pass = bool(suite.get("all_passed") or False)
    sections["suite"] = {
        "status": suite_status or ("ok" if suite_pass else ""),
        "summary_zh": "测试套件通过" if suite_pass else "测试套件状态未知",
        "summary_en": "Test suite passed" if suite_pass else "Test suite status unknown",
    }

    # parity
    parity_status = str(parity.get("status") or parity.get("parity_status") or "")
    parity_pass = parity_status in ("ok", "pass", "passed")
    sections["parity"] = {
        "status": parity_status or ("ok" if parity_pass else ""),
        "summary_zh": "一致性校验通过" if parity_pass else "一致性状态未知",
        "summary_en": "Parity check passed" if parity_pass else "Parity status unknown",
    }

    # resilience
    resilience_status = str(parity.get("resilience_status") or "")
    resilience_pass = resilience_status in ("ok", "pass", "passed")
    sections["resilience"] = {
        "status": resilience_status or ("ok" if resilience_pass else ""),
        "summary_zh": "韧性测试通过" if resilience_pass else "韧性状态未知",
        "summary_en": "Resilience test passed" if resilience_pass else "Resilience status unknown",
    }

    # governance
    gov_blockers = list(governance.get("blockers") or [])
    gov_status = "ok" if not gov_blockers else "blocker"
    sections["governance"] = {
        "status": gov_status,
        "summary_zh": "治理交接无阻塞" if not gov_blockers else f"治理交接存在 {len(gov_blockers)} 个阻塞项",
        "summary_en": "Governance handoff clear" if not gov_blockers else f"Governance handoff has {len(gov_blockers)} blocker(s)",
    }

    # closeout
    pkg_status = str(pkg.get("package_status") or "")
    sections["closeout"] = {
        "status": pkg_status,
        "summary_zh": str(pkg.get("reviewer_summary_line") or "收官包状态未知"),
        "summary_en": str(pkg.get("reviewer_summary_line") or "Closeout package status unknown"),
    }

    return sections


def _build_blockers(
    *,
    pkg: dict[str, Any],
    readiness: dict[str, Any],
    parity: dict[str, Any],
    governance: dict[str, Any],
    suite: dict[str, Any],
    lang: str,
) -> list[dict[str, str]]:
    """Build blockers from source payloads."""
    blockers: list[dict[str, str]] = []

    # Closeout package blockers
    pkg_status = str(pkg.get("package_status") or "")
    if pkg_status in (CLOSEOUT_STATUS_BLOCKER, "blocker"):
        blockers.append({
            "key": "closeout_blocker",
            "label_zh": resolve_freeze_audit_blocker_label("closeout_blocker", lang="zh"),
            "label_en": resolve_freeze_audit_blocker_label("closeout_blocker", lang="en"),
        })

    # Missing closeout package
    if not pkg:
        blockers.append({
            "key": "missing_closeout_package",
            "label_zh": resolve_freeze_audit_blocker_label("missing_closeout_package", lang="zh"),
            "label_en": resolve_freeze_audit_blocker_label("missing_closeout_package", lang="en"),
        })

    # Parity mismatch
    parity_status = str(parity.get("status") or parity.get("parity_status") or "")
    if parity_status in ("fail", "failed", "mismatch", "error"):
        blockers.append({
            "key": "parity_mismatch",
            "label_zh": resolve_freeze_audit_blocker_label("parity_mismatch", lang="zh"),
            "label_en": resolve_freeze_audit_blocker_label("parity_mismatch", lang="en"),
        })

    # Resilience failure
    resilience_status = str(parity.get("resilience_status") or "")
    if resilience_status in ("fail", "failed", "error"):
        blockers.append({
            "key": "resilience_failure",
            "label_zh": resolve_freeze_audit_blocker_label("resilience_failure", lang="zh"),
            "label_en": resolve_freeze_audit_blocker_label("resilience_failure", lang="en"),
        })

    # Governance blockers
    gov_blockers = list(governance.get("blockers") or [])
    if gov_blockers:
        blockers.append({
            "key": "governance_blocker",
            "label_zh": resolve_freeze_audit_blocker_label("governance_blocker", lang="zh"),
            "label_en": resolve_freeze_audit_blocker_label("governance_blocker", lang="en"),
        })

    # Suite failure
    suite_pass = bool(suite.get("all_passed") or False)
    suite_status = str(suite.get("status") or "")
    if not suite_pass and suite_status in ("fail", "failed", "error"):
        blockers.append({
            "key": "suite_failure",
            "label_zh": resolve_freeze_audit_blocker_label("suite_failure", lang="zh"),
            "label_en": resolve_freeze_audit_blocker_label("suite_failure", lang="en"),
        })

    return blockers


def _build_next_steps(
    *,
    blockers: list[dict[str, str]],
    pkg: dict[str, Any],
    lang: str,
) -> list[dict[str, str]]:
    """Build next steps based on current state."""
    next_steps: list[dict[str, str]] = []

    blocker_keys = {b.get("key") for b in blockers}

    if "closeout_blocker" in blocker_keys or "missing_closeout_package" in blocker_keys:
        next_steps.append({
            "key": "resolve_blockers",
            "label_zh": resolve_freeze_audit_next_step_label("resolve_blockers", lang="zh"),
            "label_en": resolve_freeze_audit_next_step_label("resolve_blockers", lang="en"),
        })

    if "parity_mismatch" in blocker_keys:
        next_steps.append({
            "key": "run_parity_check",
            "label_zh": resolve_freeze_audit_next_step_label("run_parity_check", lang="zh"),
            "label_en": resolve_freeze_audit_next_step_label("run_parity_check", lang="en"),
        })

    if "resilience_failure" in blocker_keys:
        next_steps.append({
            "key": "run_resilience_test",
            "label_zh": resolve_freeze_audit_next_step_label("run_resilience_test", lang="zh"),
            "label_en": resolve_freeze_audit_next_step_label("run_resilience_test", lang="en"),
        })

    if "governance_blocker" in blocker_keys:
        next_steps.append({
            "key": "review_governance_handoff",
            "label_zh": resolve_freeze_audit_next_step_label("review_governance_handoff", lang="zh"),
            "label_en": resolve_freeze_audit_next_step_label("review_governance_handoff", lang="en"),
        })

    if "suite_failure" in blocker_keys:
        next_steps.append({
            "key": "run_full_suite",
            "label_zh": resolve_freeze_audit_next_step_label("run_full_suite", lang="zh"),
            "label_en": resolve_freeze_audit_next_step_label("run_full_suite", lang="en"),
        })

    # If no blockers, suggest next step toward Step 3
    if not blockers:
        next_steps.append({
            "key": "proceed_to_step3_real_validation",
            "label_zh": resolve_freeze_audit_next_step_label("proceed_to_step3_real_validation", lang="zh"),
            "label_en": resolve_freeze_audit_next_step_label("proceed_to_step3_real_validation", lang="en"),
        })

    return next_steps


def _derive_audit_status(
    *,
    blockers: list[dict[str, str]],
    audit_sections: dict[str, dict[str, Any]],
) -> str:
    """Derive audit_status from blockers and section statuses."""
    if blockers:
        return FREEZE_AUDIT_STATUS_BLOCKER

    # Check for attention items in sections
    for _section_key, section in audit_sections.items():
        status = str(section.get("status") or "")
        if status in (CLOSEOUT_STATUS_ATTENTION, "attention"):
            return FREEZE_AUDIT_STATUS_ATTENTION

    # Check if closeout package is in ok state
    closeout_status = str(audit_sections.get("closeout", {}).get("status") or "")
    if closeout_status in (CLOSEOUT_STATUS_OK, "ok"):
        return FREEZE_AUDIT_STATUS_OK

    # Default to reviewer_only if no clear signal
    return FREEZE_AUDIT_STATUS_REVIEWER_ONLY


def _derive_freeze_candidate(
    *,
    audit_status: str,
    pkg: dict[str, Any],
) -> bool:
    """Derive freeze_candidate from audit_status and closeout package.

    freeze_candidate = True means "Step 2 RC review candidate", NOT release approval.
    Only set to True when audit_status is ok or attention (no blockers).
    """
    if audit_status in (FREEZE_AUDIT_STATUS_OK, FREEZE_AUDIT_STATUS_ATTENTION):
        return True
    return False


def _build_reviewer_summary_line(
    *,
    audit_status: str,
    freeze_candidate: bool,
    lang: str,
) -> str:
    """Build one-line reviewer summary."""
    if lang == "en":
        if freeze_candidate:
            return "Step 2 freeze audit: RC review candidate. Not formal release approval."
        if audit_status == FREEZE_AUDIT_STATUS_BLOCKER:
            return "Step 2 freeze audit: blockers present. Not formal release approval."
        return "Step 2 freeze audit: reviewer-only observation. Not formal release approval."
    if freeze_candidate:
        return "Step 2 冻结审计：RC 审阅候选。不是正式放行批准。"
    if audit_status == FREEZE_AUDIT_STATUS_BLOCKER:
        return "Step 2 冻结审计：存在阻塞项。不是正式放行批准。"
    return "Step 2 冻结审计：仅限审阅观察。不是正式放行批准。"


def _build_reviewer_summary_lines(
    *,
    audit_status: str,
    freeze_candidate: bool,
    blockers: list[dict[str, str]],
    next_steps: list[dict[str, str]],
    lang: str,
) -> list[str]:
    """Build multi-line reviewer summary."""
    lines: list[str] = []

    # Title
    title = FREEZE_AUDIT_TITLE_EN if lang == "en" else FREEZE_AUDIT_TITLE_ZH
    lines.append(title)

    # Status line
    status_label = resolve_freeze_audit_status_label(audit_status, lang=lang)
    if lang == "en":
        lines.append(f"Status: {status_label}")
    else:
        lines.append(f"状态：{status_label}")

    # Freeze candidate line
    if lang == "en":
        lines.append(f"Freeze candidate: {freeze_candidate}")
    else:
        lines.append(f"冻结候选：{'是' if freeze_candidate else '否'}")

    # Freeze candidate notice
    lines.append(resolve_freeze_candidate_notice(lang=lang))

    # Blockers
    if blockers:
        if lang == "en":
            lines.append(f"Blockers ({len(blockers)}):")
        else:
            lines.append(f"阻塞项（{len(blockers)}）：")
        for blocker in blockers:
            label = str(blocker.get(f"label_{lang}") or blocker.get("key") or "")
            lines.append(f"  - {label}")

    # Next steps
    if next_steps:
        if lang == "en":
            lines.append(f"Next steps ({len(next_steps)}):")
        else:
            lines.append(f"下一步（{len(next_steps)}）：")
        for step in next_steps:
            label = str(step.get(f"label_{lang}") or step.get("key") or "")
            lines.append(f"  - {label}")

    # Simulation-only boundary
    lines.append(resolve_freeze_audit_simulation_only_boundary(lang=lang))

    # Reviewer-only notice
    lines.append(resolve_freeze_audit_reviewer_only_notice(lang=lang))

    # Non-claim notice
    lines.append(resolve_freeze_audit_non_claim_notice(lang=lang))

    return lines


def _build_simulation_only_boundary() -> dict[str, bool | str]:
    """Build simulation-only boundary markers dict."""
    return dict(FREEZE_AUDIT_STEP2_BOUNDARY)


# ---------------------------------------------------------------------------
# Fallback helper — for missing freeze audit data
# ---------------------------------------------------------------------------

def build_freeze_audit_fallback(
    *,
    lang: str = "zh",
) -> dict[str, Any]:
    """Build freeze audit fallback default value.

    Guarantees all Step 2 boundary markers.
    Does not modify old run files; generates in-memory only.
    """
    _boundary = FREEZE_AUDIT_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else FREEZE_AUDIT_SIMULATION_ONLY_BOUNDARY_ZH
    _status_label = resolve_freeze_audit_status_label(FREEZE_AUDIT_STATUS_REVIEWER_ONLY, lang=lang)
    _summary_line = (
        "Step 2 freeze audit: fallback — no persisted data. Not formal release approval."
        if lang == "en"
        else "Step 2 冻结审计：fallback — 无持久化数据。不是正式放行批准。"
    )
    _summary_lines = [
        FREEZE_AUDIT_TITLE_EN if lang == "en" else FREEZE_AUDIT_TITLE_ZH,
        f"Status: {_status_label}" if lang == "en" else f"状态：{_status_label}",
        _summary_line,
        resolve_freeze_candidate_notice(lang=lang),
        _boundary,
        resolve_freeze_audit_reviewer_only_notice(lang=lang),
        resolve_freeze_audit_non_claim_notice(lang=lang),
    ]

    return {
        "schema_version": "1.0",
        "artifact_type": "step2_freeze_audit_fallback",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": "",
        "phase": "step2_freeze_audit",
        "audit_version": FREEZE_AUDIT_BUILDER_VERSION,
        "audit_status": FREEZE_AUDIT_STATUS_REVIEWER_ONLY,
        "audit_status_label": _status_label,
        "reviewer_summary_line": _summary_line,
        "reviewer_summary_lines": _summary_lines,
        "blockers": [],
        "next_steps": [],
        "audit_sections": {},
        "section_order": list(FREEZE_AUDIT_SECTION_ORDER),
        "freeze_candidate": False,
        "freeze_candidate_notice_zh": resolve_freeze_candidate_notice(lang="zh"),
        "freeze_candidate_notice_en": resolve_freeze_candidate_notice(lang="en"),
        "simulation_only_boundary": _boundary,
        "freeze_audit_source": "fallback",
        # Step 2 boundary markers — all enforced
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
    }
