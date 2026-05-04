"""Step 2 closeout package builder — aggregate from existing closeout readiness,
digest, governance handoff, compact summary packs, parity/resilience, phase evidence,
stage admission review pack, and engineering isolation admission checklist into a
single canonical closeout package for reviewer consumption.

This is an aggregation layer, not a parallel summary. It consumes existing
payloads and produces a single reviewer-first bundle.

Does NOT replace existing closeout readiness / digest / compact summary payloads.
Does NOT claim formal acceptance / formal approval.

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

from .step2_closeout_package_contracts import (
    CLOSEOUT_PACKAGE_ARTIFACT_TYPE,
    CLOSEOUT_PACKAGE_SECTION_LABELS_ZH,
    CLOSEOUT_PACKAGE_SECTION_LABELS_EN,
    CLOSEOUT_PACKAGE_SECTION_ORDER,
    CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_ZH,
    CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_EN,
    CLOSEOUT_PACKAGE_STEP2_BOUNDARY,
    CLOSEOUT_PACKAGE_TITLE_ZH,
    CLOSEOUT_PACKAGE_TITLE_EN,
    CLOSEOUT_PACKAGE_SUMMARY_ZH,
    CLOSEOUT_PACKAGE_SUMMARY_EN,
    resolve_closeout_package_section_label,
    resolve_closeout_package_simulation_only_boundary,
    resolve_closeout_package_reviewer_only_notice,
    resolve_closeout_package_non_claim_notice,
)
from .step2_closeout_readiness_contracts import (
    CLOSEOUT_STATUS_OK,
    CLOSEOUT_STATUS_ATTENTION,
    CLOSEOUT_STATUS_BLOCKER,
    CLOSEOUT_STATUS_REVIEWER_ONLY,
    resolve_closeout_status_label,
)
from .reviewer_summary_packs import extract_control_flow_compare_summary

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

CLOSEOUT_PACKAGE_BUILDER_VERSION: str = "2.22.0"


def _build_compare_field_payload(compare: dict[str, Any]) -> dict[str, Any]:
    return {
        "compare_available": bool(compare.get("available")),
        "compare_status": str(compare.get("compare_status") or ""),
        "compare_status_display": str(compare.get("compare_status_display") or ""),
        "compare_summary_line": str(compare.get("compare_summary_line") or ""),
        "compare_summary_lines": list(compare.get("compare_summary_lines") or []),
        "compare_validation_profile": str(compare.get("validation_profile") or ""),
        "compare_target_route": str(compare.get("target_route") or ""),
        "compare_target_route_display": str(compare.get("target_route_display") or ""),
        "compare_first_failure_phase": str(compare.get("first_failure_phase") or ""),
        "compare_first_failure_phase_display": str(compare.get("first_failure_phase_display") or ""),
        "compare_next_check": str(compare.get("next_check") or ""),
        "compare_next_check_display": str(compare.get("next_check_display") or ""),
        "compare_point_presence_diff": str(compare.get("point_presence_diff") or ""),
        "compare_sample_count_diff": str(compare.get("sample_count_diff") or ""),
        "compare_route_trace_diff": str(compare.get("route_trace_diff") or ""),
        "compare_key_action_mismatches": list(compare.get("key_action_mismatches") or []),
        "compare_physical_route_mismatch": str(compare.get("physical_route_mismatch") or ""),
    }


def _build_reviewer_summary_line_with_compare(
    *,
    package_status: str,
    compare: dict[str, Any],
    lang: str,
) -> str:
    base = _build_reviewer_summary_line(package_status=package_status, lang=lang)
    if not bool(compare.get("available")):
        return base
    status = str(compare.get("compare_status_display") or compare.get("compare_status") or "--")
    next_check = str(compare.get("next_check_display") or compare.get("next_check") or "--")
    suffix = (
        f" | Compare: {status} | Next check: {next_check}"
        if lang == "en"
        else f" | 对齐状态：{status} | 下一步检查：{next_check}"
    )
    return base + suffix


def _build_reviewer_summary_lines_with_compare(
    *,
    package_status: str,
    readiness: dict[str, Any],
    compare: dict[str, Any],
    lang: str,
) -> list[str]:
    lines = list(
        _build_reviewer_summary_lines(
            package_status=package_status,
            readiness=readiness,
            lang=lang,
        )
    )
    if not bool(compare.get("available")):
        return lines
    compare_summary_line = str(compare.get("compare_summary_line") or "").strip()
    if not compare_summary_line:
        status = str(compare.get("compare_status_display") or compare.get("compare_status") or "--")
        next_check = str(compare.get("next_check_display") or compare.get("next_check") or "--")
        compare_summary_line = (
            f"Compare: {status} | Next check: {next_check}"
            if lang == "en"
            else f"离线对齐：{status} | 下一步检查：{next_check}"
        )
    insert_at = 3 if len(lines) >= 3 else len(lines)
    if compare_summary_line and compare_summary_line not in lines:
        lines.insert(insert_at, compare_summary_line)
    return lines


def _build_sections_with_compare(
    *,
    readiness: dict[str, Any],
    digest: dict[str, Any],
    governance: dict[str, Any],
    packs: list[dict[str, Any]],
    parity: dict[str, Any],
    phase: dict[str, Any],
    stage_admission: dict[str, Any],
    eng_isolation: dict[str, Any],
    compare: dict[str, Any],
    lang: str,
) -> list[dict[str, Any]]:
    sections = _build_sections(
        readiness=readiness,
        digest=digest,
        governance=governance,
        packs=packs,
        parity=parity,
        phase=phase,
        stage_admission=stage_admission,
        eng_isolation=eng_isolation,
        lang=lang,
    )
    if not bool(compare.get("available")):
        return sections
    compare_fields = {
        "compare_available": True,
        "compare_status": str(compare.get("compare_status") or ""),
        "compare_status_display": str(compare.get("compare_status_display") or ""),
        "compare_summary_line": str(compare.get("compare_summary_line") or ""),
        "compare_first_failure_phase": str(compare.get("first_failure_phase") or ""),
        "compare_first_failure_phase_display": str(compare.get("first_failure_phase_display") or ""),
        "compare_next_check": str(compare.get("next_check") or ""),
        "compare_next_check_display": str(compare.get("next_check_display") or ""),
    }
    for index, section in enumerate(sections):
        if str(section.get("key") or "") != "compact_summaries":
            continue
        sections[index] = {**section, **compare_fields}
        break
    return sections


# ---------------------------------------------------------------------------
# build_step2_closeout_package — main entry point
# ---------------------------------------------------------------------------

def build_step2_closeout_package(
    *,
    run_id: str = "",
    # Existing payload inputs
    step2_closeout_readiness: dict[str, Any] | None = None,
    step2_closeout_digest: dict[str, Any] | None = None,
    stage_admission_review_pack: dict[str, Any] | None = None,
    engineering_isolation_admission_checklist: dict[str, Any] | None = None,
    compact_summary_packs: list[dict[str, Any]] | None = None,
    governance_handoff: dict[str, Any] | None = None,
    parity_resilience: dict[str, Any] | None = None,
    phase_evidence: dict[str, Any] | None = None,
    # Config
    lang: str = "zh",
) -> dict[str, Any]:
    """Build a unified Step 2 closeout package.

    Aggregates from existing payloads; does not duplicate or replace them.
    All boundary markers are enforced: simulation-only, not real acceptance,
    not ready for formal claim, reviewer-only.

    Args:
        run_id: Current run identifier.
        step2_closeout_readiness: Output of build_step2_closeout_readiness.
        step2_closeout_digest: Closeout digest payload.
        stage_admission_review_pack: Stage admission review pack payload.
        engineering_isolation_admission_checklist: Engineering isolation checklist.
        compact_summary_packs: Compact summary packs list.
        governance_handoff: Governance handoff payload.
        parity_resilience: Parity/resilience summary payload.
        phase_evidence: Phase evidence payload.
        lang: "zh" (default) or "en".

    Returns:
        Dict with closeout package fields suitable for reviewer display.
    """
    _readiness = dict(step2_closeout_readiness or {})
    _digest = dict(step2_closeout_digest or {})
    _stage_admission = dict(stage_admission_review_pack or {})
    _eng_isolation = dict(engineering_isolation_admission_checklist or {})
    _packs = list(compact_summary_packs or [])
    _governance = dict(governance_handoff or {})
    _parity = dict(parity_resilience or {})
    _phase = dict(phase_evidence or {})
    _compare = extract_control_flow_compare_summary(_packs)

    # --- Derive package_status from closeout readiness ---
    closeout_status = str(_readiness.get("closeout_status") or CLOSEOUT_STATUS_REVIEWER_ONLY)
    package_status = closeout_status  # Same bucket system

    # --- Build reviewer summary line ---
    reviewer_summary_line = _build_reviewer_summary_line_with_compare(
        package_status=package_status,
        compare=_compare,
        lang=lang,
    )

    # --- Build reviewer summary lines (multi-line) ---
    reviewer_summary_lines = _build_reviewer_summary_lines_with_compare(
        package_status=package_status,
        readiness=_readiness,
        compare=_compare,
        lang=lang,
    )

    # --- Build sections ---
    sections = _build_sections_with_compare(
        readiness=_readiness,
        digest=_digest,
        governance=_governance,
        packs=_packs,
        parity=_parity,
        phase=_phase,
        stage_admission=_stage_admission,
        eng_isolation=_eng_isolation,
        compare=_compare,
        lang=lang,
    )

    # --- Build blockers from closeout readiness ---
    blockers = list(_readiness.get("blockers") or [])

    # --- Build next_steps from closeout readiness ---
    next_steps = list(_readiness.get("next_steps") or [])

    # --- Simulation-only boundary text ---
    simulation_only_boundary = (
        CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_ZH
    )

    # --- Source versions ---
    source_versions = _build_source_versions(
        readiness=_readiness,
        digest=_digest,
        governance=_governance,
        parity=_parity,
        phase=_phase,
    )

    return {
        "schema_version": "1.0",
        "artifact_type": CLOSEOUT_PACKAGE_ARTIFACT_TYPE,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "phase": "step2_closeout",
        "package_version": CLOSEOUT_PACKAGE_BUILDER_VERSION,
        "package_status": package_status,
        "package_status_label": resolve_closeout_status_label(package_status, lang=lang),
        "reviewer_summary_line": reviewer_summary_line,
        "reviewer_summary_lines": reviewer_summary_lines,
        "sections": sections,
        "section_order": list(CLOSEOUT_PACKAGE_SECTION_ORDER),
        "blockers": blockers,
        "next_steps": next_steps,
        "simulation_only_boundary": simulation_only_boundary,
        "source_versions": source_versions,
        "closeout_package_source": "rebuilt",
        **_build_compare_field_payload(_compare),
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

def _build_reviewer_summary_line(
    *,
    package_status: str,
    lang: str,
) -> str:
    if package_status == CLOSEOUT_STATUS_OK:
        if lang == "en":
            return "Step 2 closeout package: phase ready, no blockers. Not real acceptance."
        return "Step 2 收官包：阶段就绪，无阻塞项。不是 real acceptance。"
    if package_status == CLOSEOUT_STATUS_BLOCKER:
        if lang == "en":
            return "Step 2 closeout package: blockers present. Not real acceptance."
        return "Step 2 收官包：存在阻塞项。不是 real acceptance。"
    if package_status == CLOSEOUT_STATUS_ATTENTION:
        if lang == "en":
            return "Step 2 closeout package: attention items present. Not real acceptance."
        return "Step 2 收官包：存在需关注项。不是 real acceptance。"
    # reviewer_only
    if lang == "en":
        return "Step 2 closeout package: reviewer-only observation. Not real acceptance."
    return "Step 2 收官包：仅限审阅观察。不是 real acceptance。"


def _build_reviewer_summary_lines(
    *,
    package_status: str,
    readiness: dict[str, Any],
    lang: str,
) -> list[str]:
    lines: list[str] = []

    # Title line
    title = CLOSEOUT_PACKAGE_TITLE_EN if lang == "en" else CLOSEOUT_PACKAGE_TITLE_ZH
    lines.append(title)

    # Status line
    status_label = resolve_closeout_status_label(package_status, lang=lang)
    if lang == "en":
        lines.append(f"Status: {status_label}")
    else:
        lines.append(f"状态：{status_label}")

    # Summary line
    lines.append(CLOSEOUT_PACKAGE_SUMMARY_EN if lang == "en" else CLOSEOUT_PACKAGE_SUMMARY_ZH)

    # Blockers from readiness
    blockers = list(readiness.get("blockers") or [])
    if blockers:
        if lang == "en":
            lines.append(f"Blockers ({len(blockers)}):")
        else:
            lines.append(f"阻塞项（{len(blockers)}）：")
        for blocker in blockers:
            label = str(blocker.get("label") or blocker.get("key") or "")
            lines.append(f"  - {label}")

    # Next steps from readiness
    next_steps = list(readiness.get("next_steps") or [])
    if next_steps:
        if lang == "en":
            lines.append(f"Next steps ({len(next_steps)}):")
        else:
            lines.append(f"下一步（{len(next_steps)}）：")
        for step in next_steps:
            label = str(step.get("label") or step.get("key") or "")
            lines.append(f"  - {label}")

    # Simulation-only boundary
    lines.append(resolve_closeout_package_simulation_only_boundary(lang=lang))

    # Reviewer-only notice
    lines.append(resolve_closeout_package_reviewer_only_notice(lang=lang))

    # Non-claim notice
    lines.append(resolve_closeout_package_non_claim_notice(lang=lang))

    return lines


def _build_sections(
    *,
    readiness: dict[str, Any],
    digest: dict[str, Any],
    governance: dict[str, Any],
    packs: list[dict[str, Any]],
    parity: dict[str, Any],
    phase: dict[str, Any],
    stage_admission: dict[str, Any],
    eng_isolation: dict[str, Any],
    lang: str,
) -> list[dict[str, Any]]:
    """Build sections in canonical order."""
    sections: list[dict[str, Any]] = []

    # readiness
    sections.append({
        "key": "readiness",
        "label": resolve_closeout_package_section_label("readiness", lang=lang),
        "available": bool(readiness),
        "status": str(readiness.get("closeout_status") or ""),
        "summary_line": str(readiness.get("reviewer_summary_line") or ""),
    })

    # digest
    sections.append({
        "key": "digest",
        "label": resolve_closeout_package_section_label("digest", lang=lang),
        "available": bool(digest),
    })

    # governance_handoff
    sections.append({
        "key": "governance_handoff",
        "label": resolve_closeout_package_section_label("governance_handoff", lang=lang),
        "available": bool(governance),
    })

    # compact_summaries
    sections.append({
        "key": "compact_summaries",
        "label": resolve_closeout_package_section_label("compact_summaries", lang=lang),
        "available": bool(packs),
        "pack_count": len(packs),
    })

    # parity_resilience
    parity_status = str(parity.get("status") or parity.get("parity_status") or "")
    sections.append({
        "key": "parity_resilience",
        "label": resolve_closeout_package_section_label("parity_resilience", lang=lang),
        "available": bool(parity),
        "status": parity_status or None,
    })

    # phase_evidence
    sections.append({
        "key": "phase_evidence",
        "label": resolve_closeout_package_section_label("phase_evidence", lang=lang),
        "available": bool(phase),
    })

    # stage_admission
    sections.append({
        "key": "stage_admission",
        "label": resolve_closeout_package_section_label("stage_admission", lang=lang),
        "available": bool(stage_admission),
    })

    # engineering_isolation_checklist
    checklist_status = str(eng_isolation.get("overall_status") or "")
    sections.append({
        "key": "engineering_isolation_checklist",
        "label": resolve_closeout_package_section_label("engineering_isolation_checklist", lang=lang),
        "available": bool(eng_isolation),
        "status": checklist_status or None,
    })

    # blockers
    blockers = list(readiness.get("blockers") or [])
    sections.append({
        "key": "blockers",
        "label": resolve_closeout_package_section_label("blockers", lang=lang),
        "available": bool(blockers),
        "count": len(blockers),
    })

    # next_steps
    next_steps = list(readiness.get("next_steps") or [])
    sections.append({
        "key": "next_steps",
        "label": resolve_closeout_package_section_label("next_steps", lang=lang),
        "available": bool(next_steps),
        "count": len(next_steps),
    })

    # boundary
    sections.append({
        "key": "boundary",
        "label": resolve_closeout_package_section_label("boundary", lang=lang),
        "available": True,
    })

    return sections


def _build_source_versions(
    *,
    readiness: dict[str, Any],
    digest: dict[str, Any],
    governance: dict[str, Any],
    parity: dict[str, Any],
    phase: dict[str, Any],
) -> dict[str, str]:
    """Collect version strings from source payloads."""
    versions: dict[str, str] = {}
    if readiness:
        v = str(readiness.get("schema_version") or "")
        if v:
            versions["closeout_readiness"] = v
    if digest:
        v = str(digest.get("schema_version") or "")
        if v:
            versions["closeout_digest"] = v
    if governance:
        v = str(governance.get("schema_version") or "")
        if v:
            versions["governance_handoff"] = v
    if parity:
        v = str(parity.get("schema_version") or "")
        if v:
            versions["parity_resilience"] = v
    if phase:
        v = str(phase.get("schema_version") or "")
        if v:
            versions["phase_evidence"] = v
    return versions


# ---------------------------------------------------------------------------
# Fallback helper — for missing closeout package data
# ---------------------------------------------------------------------------

def build_closeout_package_fallback(
    *,
    lang: str = "zh",
) -> dict[str, Any]:
    """Build closeout package fallback default value.

    Guarantees all Step 2 boundary markers.
    Does not modify old run files; generates in-memory only.
    """
    _boundary = CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_ZH
    _status_label = resolve_closeout_status_label(CLOSEOUT_STATUS_REVIEWER_ONLY, lang=lang)
    _summary_line = (
        "Step 2 closeout package: fallback — no persisted data. Not real acceptance."
        if lang == "en"
        else "Step 2 收官包：fallback — 无持久化数据。不是 real acceptance。"
    )
    _summary_lines = [
        CLOSEOUT_PACKAGE_TITLE_EN if lang == "en" else CLOSEOUT_PACKAGE_TITLE_ZH,
        f"Status: {_status_label}" if lang == "en" else f"状态：{_status_label}",
        _summary_line,
        _boundary,
        resolve_closeout_package_reviewer_only_notice(lang=lang),
        resolve_closeout_package_non_claim_notice(lang=lang),
    ]

    return {
        "schema_version": "1.0",
        "artifact_type": "step2_closeout_package_fallback",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": "",
        "phase": "step2_closeout",
        "package_version": CLOSEOUT_PACKAGE_BUILDER_VERSION,
        "package_status": CLOSEOUT_STATUS_REVIEWER_ONLY,
        "package_status_label": _status_label,
        "reviewer_summary_line": _summary_line,
        "reviewer_summary_lines": _summary_lines,
        "sections": [],
        "section_order": list(CLOSEOUT_PACKAGE_SECTION_ORDER),
        "blockers": [],
        "next_steps": [],
        "simulation_only_boundary": _boundary,
        "source_versions": {},
        "closeout_package_source": "fallback",
        **_build_compare_field_payload({}),
        # Step 2 boundary markers — all enforced
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
    }
