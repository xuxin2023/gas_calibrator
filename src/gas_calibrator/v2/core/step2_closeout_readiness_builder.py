"""Step 2 closeout readiness builder — aggregate from existing compact summary,
governance handoff, parity/resilience, acceptance governance, and phase evidence
into a unified closeout readiness payload for reviewer consumption.

This is an aggregation layer, not a parallel summary. It consumes existing
payloads and produces a single reviewer-first view.

Step 2 boundary:
  - evidence_source = "simulated"
  - not_real_acceptance_evidence = True
  - not_ready_for_formal_claim = True
  - reviewer_only = True
  - readiness_mapping_only = True
  - primary_evidence_rewritten = False
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .step2_closeout_readiness_contracts import (
    CLOSEOUT_CONTRIBUTING_SECTIONS,
    CLOSEOUT_CONTRIBUTING_SECTION_LABELS_ZH,
    CLOSEOUT_CONTRIBUTING_SECTION_LABELS_EN,
    CLOSEOUT_STEP2_BOUNDARY,
    CLOSEOUT_STATUS_ATTENTION,
    CLOSEOUT_STATUS_BLOCKER,
    CLOSEOUT_STATUS_OK,
    CLOSEOUT_STATUS_REVIEWER_ONLY,
    CLOSEOUT_SIMULATION_ONLY_BOUNDARY_ZH,
    CLOSEOUT_SIMULATION_ONLY_BOUNDARY_EN,
    CLOSEOUT_TITLE_ZH,
    CLOSEOUT_TITLE_EN,
    CLOSEOUT_SUMMARY_LINE_ZH,
    CLOSEOUT_SUMMARY_LINE_EN,
    CLOSEOUT_BLOCKER_LABELS_ZH,
    CLOSEOUT_BLOCKER_LABELS_EN,
    CLOSEOUT_NEXT_STEPS_ZH,
    CLOSEOUT_NEXT_STEPS_EN,
    resolve_closeout_status_label,
    resolve_closeout_blocker_label,
    resolve_closeout_next_step_label,
    resolve_closeout_contributing_section_label,
    resolve_closeout_simulation_only_boundary,
    resolve_closeout_reviewer_only_notice,
    resolve_closeout_non_claim_notice,
)

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

CLOSEOUT_READINESS_BUILDER_VERSION: str = "2.20.0"


# ---------------------------------------------------------------------------
# build_step2_closeout_readiness — main entry point
# ---------------------------------------------------------------------------

def build_step2_closeout_readiness(
    *,
    run_id: str = "",
    # Existing payload inputs
    step2_readiness_summary: dict[str, Any] | None = None,
    compact_summary_packs: list[dict[str, Any]] | None = None,
    governance_handoff: dict[str, Any] | None = None,
    parity_resilience: dict[str, Any] | None = None,
    acceptance_governance: dict[str, Any] | None = None,
    phase_evidence: dict[str, Any] | None = None,
    # Config
    lang: str = "zh",
) -> dict[str, Any]:
    """Build a unified Step 2 closeout readiness payload.

    Aggregates from existing payloads; does not duplicate or replace them.
    All boundary markers are enforced: simulation-only, not real acceptance,
    not ready for formal claim, reviewer-only.

    Args:
        run_id: Current run identifier.
        step2_readiness_summary: Output of build_step2_readiness_summary.
        compact_summary_packs: Compact summary packs list.
        governance_handoff: Governance handoff payload.
        parity_resilience: Parity/resilience summary payload.
        acceptance_governance: Acceptance governance payload.
        phase_evidence: Phase evidence payload.
        lang: "zh" (default) or "en".

    Returns:
        Dict with closeout readiness fields suitable for reviewer display.
    """
    _readiness = dict(step2_readiness_summary or {})
    _packs = list(compact_summary_packs or [])
    _governance = dict(governance_handoff or {})
    _parity = dict(parity_resilience or {})
    _acceptance = dict(acceptance_governance or {})
    _phase = dict(phase_evidence or {})

    # --- Derive closeout status from step2_readiness gates ---
    overall_status = str(_readiness.get("overall_status") or "not_ready")
    blocking_items = list(_readiness.get("blocking_items") or [])
    warning_items = list(_readiness.get("warning_items") or [])
    gates = list(_readiness.get("gates") or [])

    # Determine closeout status bucket
    if overall_status == "ready_for_engineering_isolation" and not blocking_items:
        closeout_status = CLOSEOUT_STATUS_OK
    elif blocking_items:
        closeout_status = CLOSEOUT_STATUS_BLOCKER
    elif warning_items:
        closeout_status = CLOSEOUT_STATUS_ATTENTION
    else:
        closeout_status = CLOSEOUT_STATUS_REVIEWER_ONLY

    # --- Build reviewer summary line ---
    reviewer_summary_line = _build_reviewer_summary_line(
        closeout_status=closeout_status,
        blocking_items=blocking_items,
        lang=lang,
    )

    # --- Build reviewer summary lines (multi-line) ---
    reviewer_summary_lines = _build_reviewer_summary_lines(
        closeout_status=closeout_status,
        blocking_items=blocking_items,
        warning_items=warning_items,
        gates=gates,
        lang=lang,
    )

    # --- Build blockers with labels ---
    blockers = _build_blockers(blocking_items, lang=lang)

    # --- Build next steps ---
    next_steps = _build_next_steps(
        closeout_status=closeout_status,
        blocking_items=blocking_items,
        warning_items=warning_items,
        lang=lang,
    )

    # --- Build contributing sections ---
    contributing_sections = _build_contributing_sections(
        packs=_packs,
        governance=_governance,
        parity=_parity,
        acceptance=_acceptance,
        phase=_phase,
        lang=lang,
    )

    # --- Simulation-only boundary text ---
    simulation_only_boundary = (
        CLOSEOUT_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else CLOSEOUT_SIMULATION_ONLY_BOUNDARY_ZH
    )

    # --- Rendered compact sections (from packs, if available) ---
    rendered_compact_sections = _extract_compact_section_summaries(_packs, lang=lang)

    # --- Evidence source from step2_readiness ---
    evidence_source = str(_readiness.get("evidence_source") or "simulated")

    # --- Gate status / summary / alignment (Step 2.19) ---
    gate_status = str(_readiness.get("overall_status") or "not_ready")
    gate_summary = _build_gate_summary(gates)
    closeout_gate_alignment = _build_closeout_gate_alignment(
        closeout_status=closeout_status,
        gate_status=gate_status,
    )

    return {
        "schema_version": "1.0",
        "artifact_type": "step2_closeout_readiness",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "phase": "step2_closeout",
        "closeout_status": closeout_status,
        "closeout_readiness_source": "rebuilt",
        "closeout_status_label": resolve_closeout_status_label(closeout_status, lang=lang),
        "reviewer_summary_line": reviewer_summary_line,
        "reviewer_summary_lines": reviewer_summary_lines,
        "blockers": blockers,
        "next_steps": next_steps,
        "contributing_sections": contributing_sections,
        "simulation_only_boundary": simulation_only_boundary,
        "rendered_compact_sections": rendered_compact_sections,
        # Gate fields (Step 2.19) — aligned with step2_readiness gates
        "gate_status": gate_status,
        "gate_summary": gate_summary,
        "closeout_gate_alignment": closeout_gate_alignment,
        # Step 2 boundary markers — always enforced
        "evidence_source": evidence_source,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
        # Raw inputs preserved for traceability
        "source_readiness_status": overall_status,
        "source_blocking_items": blocking_items,
        "source_warning_items": warning_items,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_reviewer_summary_line(
    *,
    closeout_status: str,
    blocking_items: list[str],
    lang: str,
) -> str:
    if closeout_status == CLOSEOUT_STATUS_OK:
        if lang == "en":
            return "Step 2 closeout: phase ready, no blockers. Not real acceptance."
        return "Step 2 收官：阶段就绪，无阻塞项。不是 real acceptance。"
    if closeout_status == CLOSEOUT_STATUS_BLOCKER:
        count = len(blocking_items)
        if lang == "en":
            return f"Step 2 closeout: {count} blocker(s) present. Not real acceptance."
        return f"Step 2 收官：存在 {count} 项阻塞。不是 real acceptance。"
    if closeout_status == CLOSEOUT_STATUS_ATTENTION:
        if lang == "en":
            return "Step 2 closeout: attention items present, no blockers. Not real acceptance."
        return "Step 2 收官：存在需关注项，无阻塞。不是 real acceptance。"
    # reviewer_only
    if lang == "en":
        return "Step 2 closeout: reviewer-only observation. Not real acceptance."
    return "Step 2 收官：仅限审阅观察。不是 real acceptance。"


def _build_reviewer_summary_lines(
    *,
    closeout_status: str,
    blocking_items: list[str],
    warning_items: list[str],
    gates: list[dict[str, Any]],
    lang: str,
) -> list[str]:
    lines: list[str] = []

    # Title line
    title = CLOSEOUT_TITLE_EN if lang == "en" else CLOSEOUT_TITLE_ZH
    lines.append(title)

    # Status line
    status_label = resolve_closeout_status_label(closeout_status, lang=lang)
    if lang == "en":
        lines.append(f"Status: {status_label}")
    else:
        lines.append(f"状态：{status_label}")

    # Summary line
    lines.append(CLOSEOUT_SUMMARY_LINE_EN if lang == "en" else CLOSEOUT_SUMMARY_LINE_ZH)

    # Blockers
    if blocking_items:
        if lang == "en":
            lines.append(f"Blockers ({len(blocking_items)}):")
        else:
            lines.append(f"阻塞项（{len(blocking_items)}）：")
        for item in blocking_items:
            label = resolve_closeout_blocker_label(item, lang=lang)
            lines.append(f"  - {label}")

    # Warnings (as attention items)
    if warning_items:
        if lang == "en":
            lines.append(f"Attention items ({len(warning_items)}):")
        else:
            lines.append(f"需关注项（{len(warning_items)}）：")
        for item in warning_items:
            lines.append(f"  - {item}")

    # Gate summary
    pass_count = sum(1 for g in gates if str(g.get("status") or "") == "pass")
    total_count = len(gates)
    if total_count > 0:
        if lang == "en":
            lines.append(f"Gates: {pass_count}/{total_count} passed")
        else:
            lines.append(f"门禁：{pass_count}/{total_count} 通过")

    # Simulation-only boundary
    lines.append(resolve_closeout_simulation_only_boundary(lang=lang))

    # Reviewer-only notice
    lines.append(resolve_closeout_reviewer_only_notice(lang=lang))

    # Non-claim notice
    lines.append(resolve_closeout_non_claim_notice(lang=lang))

    return lines


def _build_blockers(blocking_items: list[str], *, lang: str) -> list[dict[str, Any]]:
    return [
        {
            "key": item,
            "label": resolve_closeout_blocker_label(item, lang=lang),
        }
        for item in blocking_items
    ]


def _build_next_steps(
    *,
    closeout_status: str,
    blocking_items: list[str],
    warning_items: list[str],
    lang: str,
) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []

    if closeout_status == CLOSEOUT_STATUS_OK:
        steps.append({
            "key": "proceed_to_engineering_isolation",
            "label": resolve_closeout_next_step_label("proceed_to_engineering_isolation", lang=lang),
        })
    else:
        # Map blocking items to remediation steps
        for item in blocking_items:
            step_key = _blocker_to_next_step_key(item)
            steps.append({
                "key": step_key,
                "label": resolve_closeout_next_step_label(step_key, lang=lang),
            })

    # Always add the "await real acceptance" step
    steps.append({
        "key": "await_real_acceptance",
        "label": resolve_closeout_next_step_label("await_real_acceptance", lang=lang),
    })

    return steps


def _blocker_to_next_step_key(blocker: str) -> str:
    mapping = {
        "simulation_only_boundary": "fix_simulation_boundary",
        "readiness_evidence_complete": "complete_governance_evidence",
        "headless_smoke_path_available": "verify_headless_smoke",
        "shared_experiment_flags_default_off": "resolve_experiment_flags",
        "step2_gate_status": "resolve_experiment_flags",
    }
    return mapping.get(blocker, blocker)


def _build_contributing_sections(
    *,
    packs: list[dict[str, Any]],
    governance: dict[str, Any],
    parity: dict[str, Any],
    acceptance: dict[str, Any],
    phase: dict[str, Any],
    lang: str,
) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    # Compact summary
    sections.append({
        "key": "compact_summary",
        "label": resolve_closeout_contributing_section_label("compact_summary", lang=lang),
        "available": bool(packs),
        "pack_count": len(packs),
    })

    # Governance handoff
    sections.append({
        "key": "governance_handoff",
        "label": resolve_closeout_contributing_section_label("governance_handoff", lang=lang),
        "available": bool(governance),
    })

    # Parity/resilience
    parity_status = str(parity.get("status") or parity.get("parity_status") or "")
    sections.append({
        "key": "parity_resilience",
        "label": resolve_closeout_contributing_section_label("parity_resilience", lang=lang),
        "available": bool(parity),
        "status": parity_status or None,
    })

    # Acceptance governance
    acceptance_level = str(acceptance.get("acceptance_level") or "")
    sections.append({
        "key": "acceptance_governance",
        "label": resolve_closeout_contributing_section_label("acceptance_governance", lang=lang),
        "available": bool(acceptance),
        "acceptance_level": acceptance_level or None,
        "readiness_mapping_only": bool(acceptance.get("readiness_mapping_only", True)),
    })

    # Phase evidence
    sections.append({
        "key": "phase_evidence",
        "label": resolve_closeout_contributing_section_label("phase_evidence", lang=lang),
        "available": bool(phase),
    })

    return sections


def _extract_compact_section_summaries(
    packs: list[dict[str, Any]],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    """Extract lightweight section summaries from compact summary packs."""
    result: list[dict[str, Any]] = []
    for pack in packs:
        summary_key = str(pack.get("summary_key") or "")
        display_label = str(pack.get("display_label") or summary_key)
        summary_line = str(pack.get("summary_line") or "")
        severity = str(pack.get("severity", "info") or "info")
        result.append({
            "summary_key": summary_key,
            "display_label": display_label,
            "summary_line": summary_line,
            "severity": severity,
        })
    return result


def _build_gate_summary(
    gates: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build gate pass/blocked summary from step2_readiness gates list."""
    pass_count = sum(1 for g in gates if str(g.get("status") or "") == "pass")
    total_count = len(gates)
    blocked_gate_ids = [
        str(g.get("gate_id") or "")
        for g in gates
        if str(g.get("status") or "") in {"blocked", "not_ready"}
    ]
    blocked_count = len(blocked_gate_ids)
    return {
        "pass_count": pass_count,
        "total_count": total_count,
        "blocked_count": blocked_count,
        "blocked_gate_ids": blocked_gate_ids,
    }


def _build_closeout_gate_alignment(
    *,
    closeout_status: str,
    gate_status: str,
) -> dict[str, Any]:
    """Build alignment record between closeout_status and gate_status."""
    _ALIGNMENT_MAP: dict[tuple[str, str], bool] = {
        ("ok", "ready_for_engineering_isolation"): True,
        ("blocker", "not_ready"): True,
        ("attention", "not_ready"): True,
        ("reviewer_only", "not_ready"): True,
    }
    aligned = _ALIGNMENT_MAP.get((closeout_status, gate_status), False)
    return {
        "closeout_status": closeout_status,
        "gate_status": gate_status,
        "aligned": aligned,
    }
