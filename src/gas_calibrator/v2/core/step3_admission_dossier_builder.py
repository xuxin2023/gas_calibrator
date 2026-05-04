"""Step 3 admission dossier builder — aggregate from freeze_audit + closeout_package +
closeout_readiness + governance/parity/resilience/phase evidence into a single
Step 3 admission dossier payload.

This is the final Step 2 governance package. It does NOT replace freeze_audit
or closeout_package — it provides the top-level "Step 3 admission material" view.

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

from datetime import datetime, timezone
from typing import Any

from .step3_admission_dossier_contracts import (
    ADMISSION_DOSSIER_STATUS_OK,
    ADMISSION_DOSSIER_STATUS_ATTENTION,
    ADMISSION_DOSSIER_STATUS_BLOCKER,
    ADMISSION_DOSSIER_STATUS_REVIEWER_ONLY,
    ADMISSION_DOSSIER_SECTION_ORDER,
    ADMISSION_DOSSIER_TITLE_ZH,
    ADMISSION_DOSSIER_TITLE_EN,
    ADMISSION_DOSSIER_STEP2_BOUNDARY,
    ADMISSION_DOSSIER_SIMULATION_ONLY_BOUNDARY_ZH,
    ADMISSION_DOSSIER_SIMULATION_ONLY_BOUNDARY_EN,
    resolve_admission_dossier_title,
    resolve_admission_dossier_summary,
    resolve_admission_dossier_section_label,
    resolve_admission_dossier_status_label,
    resolve_admission_dossier_blocker_label,
    resolve_admission_dossier_next_step_label,
    resolve_admission_dossier_simulation_only_boundary,
    resolve_admission_dossier_reviewer_only_notice,
    resolve_admission_dossier_non_claim_notice,
    resolve_admission_candidate_notice,
)
from .step2_closeout_readiness_contracts import (
    CLOSEOUT_STATUS_OK,
    CLOSEOUT_STATUS_ATTENTION,
    CLOSEOUT_STATUS_BLOCKER,
)

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

ADMISSION_DOSSIER_BUILDER_VERSION: str = "2.24.0"


# ---------------------------------------------------------------------------
# build_step3_admission_dossier — main entry point
# ---------------------------------------------------------------------------

def build_step3_admission_dossier(
    *,
    run_id: str = "",
    # Core inputs
    step2_freeze_audit: dict[str, Any] | None = None,
    step2_closeout_package: dict[str, Any] | None = None,
    step2_closeout_readiness: dict[str, Any] | None = None,
    governance_handoff: dict[str, Any] | None = None,
    parity_resilience_summary: dict[str, Any] | None = None,
    phase_evidence: dict[str, Any] | None = None,
    acceptance_governance: dict[str, Any] | None = None,
    # Config
    lang: str = "zh",
) -> dict[str, Any]:
    """Build a Step 3 admission dossier payload."""
    _freeze = dict(step2_freeze_audit or {})
    _pkg = dict(step2_closeout_package or {})
    _readiness = dict(step2_closeout_readiness or {})
    _governance = dict(governance_handoff or {})
    _parity = dict(parity_resilience_summary or {})
    _phase = dict(phase_evidence or {})
    _acceptance = dict(acceptance_governance or {})

    # --- Build dossier sections ---
    dossier_sections = _build_dossier_sections(
        freeze=_freeze,
        pkg=_pkg,
        readiness=_readiness,
        governance=_governance,
        parity=_parity,
        phase=_phase,
        lang=lang,
    )

    # --- Build blockers ---
    blockers = _build_blockers(
        freeze=_freeze,
        pkg=_pkg,
        readiness=_readiness,
        governance=_governance,
        parity=_parity,
        lang=lang,
    )

    # --- Build next steps ---
    next_steps = _build_next_steps(
        blockers=blockers,
        lang=lang,
    )

    # --- Derive dossier_status ---
    dossier_status = _derive_dossier_status(
        blockers=blockers,
        dossier_sections=dossier_sections,
    )

    # --- Derive admission_candidate ---
    admission_candidate = _derive_admission_candidate(
        dossier_status=dossier_status,
    )

    # --- Build reviewer summary line ---
    reviewer_summary_line = _build_reviewer_summary_line(
        dossier_status=dossier_status,
        admission_candidate=admission_candidate,
        lang=lang,
    )

    # --- Build reviewer summary lines ---
    reviewer_summary_lines = _build_reviewer_summary_lines(
        dossier_status=dossier_status,
        admission_candidate=admission_candidate,
        blockers=blockers,
        next_steps=next_steps,
        lang=lang,
    )

    # --- Build source versions ---
    source_versions = _build_source_versions(
        freeze=_freeze,
        pkg=_pkg,
        readiness=_readiness,
        governance=_governance,
        parity=_parity,
        phase=_phase,
    )

    return {
        "schema_version": "1.0",
        "artifact_type": "step3_admission_dossier",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "phase": "step3_admission_dossier",
        "dossier_version": ADMISSION_DOSSIER_BUILDER_VERSION,
        "dossier_status": dossier_status,
        "dossier_status_label": resolve_admission_dossier_status_label(dossier_status, lang=lang),
        "reviewer_summary_line": reviewer_summary_line,
        "reviewer_summary_lines": reviewer_summary_lines,
        "blockers": blockers,
        "next_steps": next_steps,
        "dossier_sections": dossier_sections,
        "section_order": list(ADMISSION_DOSSIER_SECTION_ORDER),
        "admission_candidate": admission_candidate,
        "admission_candidate_notice_zh": resolve_admission_candidate_notice(lang="zh"),
        "admission_candidate_notice_en": resolve_admission_candidate_notice(lang="en"),
        "simulation_only_boundary": resolve_admission_dossier_simulation_only_boundary(lang=lang),
        "source_versions": source_versions,
        "admission_dossier_source": "rebuilt",
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

def _build_dossier_sections(
    *,
    freeze: dict[str, Any],
    pkg: dict[str, Any],
    readiness: dict[str, Any],
    governance: dict[str, Any],
    parity: dict[str, Any],
    phase: dict[str, Any],
    lang: str,
) -> dict[str, dict[str, Any]]:
    sections: dict[str, dict[str, Any]] = {}

    # freeze_audit
    audit_status = str(freeze.get("audit_status") or "")
    sections["freeze_audit"] = {
        "status": audit_status,
        "summary_zh": str(freeze.get("reviewer_summary_line") or "冻结审计状态未知"),
        "summary_en": str(freeze.get("reviewer_summary_line") or "Freeze audit status unknown"),
    }

    # closeout_package
    pkg_status = str(pkg.get("package_status") or "")
    sections["closeout_package"] = {
        "status": pkg_status,
        "summary_zh": str(pkg.get("reviewer_summary_line") or "收官包状态未知"),
        "summary_en": str(pkg.get("reviewer_summary_line") or "Closeout package status unknown"),
    }

    # closeout_readiness
    closeout_status = str(readiness.get("closeout_status") or "")
    sections["closeout_readiness"] = {
        "status": closeout_status,
        "summary_zh": str(readiness.get("reviewer_summary_line") or "收官就绪度状态未知"),
        "summary_en": str(readiness.get("reviewer_summary_line") or "Closeout readiness status unknown"),
    }

    # governance_handoff
    gov_blockers = list(governance.get("blockers") or [])
    sections["governance_handoff"] = {
        "status": "ok" if not gov_blockers else "blocker",
        "summary_zh": "治理交接无阻塞" if not gov_blockers else f"治理交接存在 {len(gov_blockers)} 个阻塞项",
        "summary_en": "Governance handoff clear" if not gov_blockers else f"Governance handoff has {len(gov_blockers)} blocker(s)",
    }

    # parity_resilience
    parity_status = str(parity.get("status") or parity.get("parity_status") or "")
    sections["parity_resilience"] = {
        "status": parity_status,
        "summary_zh": "一致性/韧性状态已知" if parity_status else "一致性/韧性状态未知",
        "summary_en": "Parity/resilience status known" if parity_status else "Parity/resilience status unknown",
    }

    # phase_evidence
    sections["phase_evidence"] = {
        "status": "ok" if phase else "",
        "summary_zh": "阶段证据可用" if phase else "阶段证据缺失",
        "summary_en": "Phase evidence available" if phase else "Phase evidence missing",
    }

    return sections


def _build_blockers(
    *,
    freeze: dict[str, Any],
    pkg: dict[str, Any],
    readiness: dict[str, Any],
    governance: dict[str, Any],
    parity: dict[str, Any],
    lang: str,
) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []

    # Missing freeze_audit
    if not freeze:
        blockers.append({
            "key": "missing_freeze_audit",
            "label_zh": resolve_admission_dossier_blocker_label("missing_freeze_audit", lang="zh"),
            "label_en": resolve_admission_dossier_blocker_label("missing_freeze_audit", lang="en"),
        })

    # Freeze audit blocker
    audit_status = str(freeze.get("audit_status") or "")
    if audit_status in ("blocker", CLOSEOUT_STATUS_BLOCKER):
        blockers.append({
            "key": "freeze_audit_blocker",
            "label_zh": resolve_admission_dossier_blocker_label("freeze_audit_blocker", lang="zh"),
            "label_en": resolve_admission_dossier_blocker_label("freeze_audit_blocker", lang="en"),
        })

    # Missing closeout_package
    if not pkg:
        blockers.append({
            "key": "missing_closeout_package",
            "label_zh": resolve_admission_dossier_blocker_label("missing_closeout_package", lang="zh"),
            "label_en": resolve_admission_dossier_blocker_label("missing_closeout_package", lang="en"),
        })

    # Closeout package blocker
    pkg_status = str(pkg.get("package_status") or "")
    if pkg_status in ("blocker", CLOSEOUT_STATUS_BLOCKER):
        blockers.append({
            "key": "closeout_package_blocker",
            "label_zh": resolve_admission_dossier_blocker_label("closeout_package_blocker", lang="zh"),
            "label_en": resolve_admission_dossier_blocker_label("closeout_package_blocker", lang="en"),
        })

    # Closeout readiness blocker
    closeout_status = str(readiness.get("closeout_status") or "")
    if closeout_status in ("blocker", CLOSEOUT_STATUS_BLOCKER):
        blockers.append({
            "key": "closeout_readiness_blocker",
            "label_zh": resolve_admission_dossier_blocker_label("closeout_readiness_blocker", lang="zh"),
            "label_en": resolve_admission_dossier_blocker_label("closeout_readiness_blocker", lang="en"),
        })

    # Governance blocker
    gov_blockers = list(governance.get("blockers") or [])
    if gov_blockers:
        blockers.append({
            "key": "governance_blocker",
            "label_zh": resolve_admission_dossier_blocker_label("governance_blocker", lang="zh"),
            "label_en": resolve_admission_dossier_blocker_label("governance_blocker", lang="en"),
        })

    # Parity mismatch
    parity_status = str(parity.get("status") or parity.get("parity_status") or "")
    if parity_status in ("fail", "failed", "mismatch", "error"):
        blockers.append({
            "key": "parity_mismatch",
            "label_zh": resolve_admission_dossier_blocker_label("parity_mismatch", lang="zh"),
            "label_en": resolve_admission_dossier_blocker_label("parity_mismatch", lang="en"),
        })

    # Real acceptance not ready (always true in Step 2)
    blockers.append({
        "key": "real_acceptance_not_ready",
        "label_zh": resolve_admission_dossier_blocker_label("real_acceptance_not_ready", lang="zh"),
        "label_en": resolve_admission_dossier_blocker_label("real_acceptance_not_ready", lang="en"),
    })

    return blockers


def _build_next_steps(
    *,
    blockers: list[dict[str, str]],
    lang: str,
) -> list[dict[str, str]]:
    next_steps: list[dict[str, str]] = []
    blocker_keys = {b.get("key") for b in blockers}

    if blocker_keys & {"freeze_audit_blocker", "closeout_package_blocker", "closeout_readiness_blocker", "missing_freeze_audit", "missing_closeout_package"}:
        next_steps.append({
            "key": "resolve_blockers",
            "label_zh": resolve_admission_dossier_next_step_label("resolve_blockers", lang="zh"),
            "label_en": resolve_admission_dossier_next_step_label("resolve_blockers", lang="en"),
        })

    if "parity_mismatch" in blocker_keys:
        next_steps.append({
            "key": "complete_simulation_evidence",
            "label_zh": resolve_admission_dossier_next_step_label("complete_simulation_evidence", lang="zh"),
            "label_en": resolve_admission_dossier_next_step_label("complete_simulation_evidence", lang="en"),
        })

    if "governance_blocker" in blocker_keys:
        next_steps.append({
            "key": "review_governance_handoff",
            "label_zh": resolve_admission_dossier_next_step_label("review_governance_handoff", lang="zh"),
            "label_en": resolve_admission_dossier_next_step_label("review_governance_handoff", lang="en"),
        })

    # Always suggest real device access as a next step (since real_acceptance_not_ready is always a blocker)
    next_steps.append({
        "key": "obtain_real_device_access",
        "label_zh": resolve_admission_dossier_next_step_label("obtain_real_device_access", lang="zh"),
        "label_en": resolve_admission_dossier_next_step_label("obtain_real_device_access", lang="en"),
    })
    next_steps.append({
        "key": "run_step3_real_validation",
        "label_zh": resolve_admission_dossier_next_step_label("run_step3_real_validation", lang="zh"),
        "label_en": resolve_admission_dossier_next_step_label("run_step3_real_validation", lang="en"),
    })

    return next_steps


def _derive_dossier_status(
    *,
    blockers: list[dict[str, str]],
    dossier_sections: dict[str, dict[str, Any]],
) -> str:
    if blockers:
        return ADMISSION_DOSSIER_STATUS_BLOCKER
    for section in dossier_sections.values():
        status = str(section.get("status") or "")
        if status in (CLOSEOUT_STATUS_ATTENTION, "attention"):
            return ADMISSION_DOSSIER_STATUS_ATTENTION
    return ADMISSION_DOSSIER_STATUS_REVIEWER_ONLY


def _derive_admission_candidate(
    *,
    dossier_status: str,
) -> bool:
    """admission_candidate = True only when no blockers (attention is ok).
    Note: real_acceptance_not_ready is always a blocker in Step 2,
    so admission_candidate will always be False in Step 2."""
    if dossier_status in (ADMISSION_DOSSIER_STATUS_OK, ADMISSION_DOSSIER_STATUS_ATTENTION):
        return True
    return False


def _build_reviewer_summary_line(
    *,
    dossier_status: str,
    admission_candidate: bool,
    lang: str,
) -> str:
    if lang == "en":
        if admission_candidate:
            return "Step 3 admission dossier: candidate material ready. Not Step 3 approval."
        if dossier_status == ADMISSION_DOSSIER_STATUS_BLOCKER:
            return "Step 3 admission dossier: blockers present. Not Step 3 approval."
        return "Step 3 admission dossier: reviewer-only observation. Not Step 3 approval."
    if admission_candidate:
        return "Step 3 准入材料：候选材料已具备。不是 Step 3 批准。"
    if dossier_status == ADMISSION_DOSSIER_STATUS_BLOCKER:
        return "Step 3 准入材料：存在阻塞项。不是 Step 3 批准。"
    return "Step 3 准入材料：仅限审阅观察。不是 Step 3 批准。"


def _build_reviewer_summary_lines(
    *,
    dossier_status: str,
    admission_candidate: bool,
    blockers: list[dict[str, str]],
    next_steps: list[dict[str, str]],
    lang: str,
) -> list[str]:
    lines: list[str] = []
    title = ADMISSION_DOSSIER_TITLE_EN if lang == "en" else ADMISSION_DOSSIER_TITLE_ZH
    lines.append(title)
    status_label = resolve_admission_dossier_status_label(dossier_status, lang=lang)
    if lang == "en":
        lines.append(f"Status: {status_label}")
        lines.append(f"Admission candidate: {admission_candidate}")
    else:
        lines.append(f"状态：{status_label}")
        lines.append(f"准入候选：{'是' if admission_candidate else '否'}")
    lines.append(resolve_admission_candidate_notice(lang=lang))
    if blockers:
        if lang == "en":
            lines.append(f"Blockers ({len(blockers)}):")
        else:
            lines.append(f"阻塞项（{len(blockers)}）：")
        for blocker in blockers:
            label = str(blocker.get(f"label_{lang}") or blocker.get("key") or "")
            lines.append(f"  - {label}")
    if next_steps:
        if lang == "en":
            lines.append(f"Next steps ({len(next_steps)}):")
        else:
            lines.append(f"下一步（{len(next_steps)}）：")
        for step in next_steps:
            label = str(step.get(f"label_{lang}") or step.get("key") or "")
            lines.append(f"  - {label}")
    lines.append(resolve_admission_dossier_simulation_only_boundary(lang=lang))
    lines.append(resolve_admission_dossier_reviewer_only_notice(lang=lang))
    lines.append(resolve_admission_dossier_non_claim_notice(lang=lang))
    return lines


def _build_source_versions(
    *,
    freeze: dict[str, Any],
    pkg: dict[str, Any],
    readiness: dict[str, Any],
    governance: dict[str, Any],
    parity: dict[str, Any],
    phase: dict[str, Any],
) -> dict[str, str]:
    versions: dict[str, str] = {}
    for name, src in [("freeze_audit", freeze), ("closeout_package", pkg),
                      ("closeout_readiness", readiness), ("governance_handoff", governance),
                      ("parity_resilience", parity), ("phase_evidence", phase)]:
        v = str(src.get("schema_version") or src.get("audit_version") or src.get("package_version") or "")
        if v:
            versions[name] = v
    return versions


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------

def build_admission_dossier_fallback(
    *,
    lang: str = "zh",
) -> dict[str, Any]:
    """Build admission dossier fallback."""
    _boundary = ADMISSION_DOSSIER_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else ADMISSION_DOSSIER_SIMULATION_ONLY_BOUNDARY_ZH
    _status_label = resolve_admission_dossier_status_label(ADMISSION_DOSSIER_STATUS_REVIEWER_ONLY, lang=lang)
    _summary_line = (
        "Step 3 admission dossier: fallback — no persisted data. Not Step 3 approval."
        if lang == "en"
        else "Step 3 准入材料：fallback — 无持久化数据。不是 Step 3 批准。"
    )
    _summary_lines = [
        ADMISSION_DOSSIER_TITLE_EN if lang == "en" else ADMISSION_DOSSIER_TITLE_ZH,
        f"Status: {_status_label}" if lang == "en" else f"状态：{_status_label}",
        _summary_line,
        resolve_admission_candidate_notice(lang=lang),
        _boundary,
        resolve_admission_dossier_reviewer_only_notice(lang=lang),
        resolve_admission_dossier_non_claim_notice(lang=lang),
    ]
    return {
        "schema_version": "1.0",
        "artifact_type": "step3_admission_dossier_fallback",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": "",
        "phase": "step3_admission_dossier",
        "dossier_version": ADMISSION_DOSSIER_BUILDER_VERSION,
        "dossier_status": ADMISSION_DOSSIER_STATUS_REVIEWER_ONLY,
        "dossier_status_label": _status_label,
        "reviewer_summary_line": _summary_line,
        "reviewer_summary_lines": _summary_lines,
        "blockers": [],
        "next_steps": [],
        "dossier_sections": {},
        "section_order": list(ADMISSION_DOSSIER_SECTION_ORDER),
        "admission_candidate": False,
        "admission_candidate_notice_zh": resolve_admission_candidate_notice(lang="zh"),
        "admission_candidate_notice_en": resolve_admission_candidate_notice(lang="en"),
        "simulation_only_boundary": _boundary,
        "source_versions": {},
        "admission_dossier_source": "fallback",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
    }
