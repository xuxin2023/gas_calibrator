"""Reviewer summary packs — stable contract wrappers around compact summary builders.

Each pack wraps a builder result into a structured dict with:
- Stable key / label / priority / severity
- Backward-compatible summary_line / summary_lines / compact_summary_lines
- Surface budget hints for deterministic display governance
- Step 2 boundary markers (simulation-only, not real acceptance)

Design principles:
- Chinese default, English fallback
- No formal acceptance / formal claim language
- All evidence is simulation-only
- Backward compatible with existing summary_line / summary_lines consumers
- Does not change business data structures, only adds packaging contract
"""

from __future__ import annotations

from typing import Any

from .reviewer_summary_builders import (
    REVIEWER_SUMMARY_BUILDERS_VERSION as _BUILDERS_VERSION,
    build_measurement_digest_compact_summary,
    build_readiness_digest_compact_summary,
    build_phase_evidence_compact_summary,
    build_v12_alignment_compact_summary,
    build_governance_handoff_compact_summary,
    build_parity_resilience_compact_summary,
)
from .compact_summary_budget import apply_surface_budget
from .phase_evidence_display_contracts import (
    PHASE_EVIDENCE_STEP2_BOUNDARY,
)

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
REVIEWER_SUMMARY_PACKS_VERSION: str = "2.14.0"

# ---------------------------------------------------------------------------
# Pack summary keys — the 6 compact summary domains
# ---------------------------------------------------------------------------
PACK_SUMMARY_KEYS: tuple[str, ...] = (
    "measurement_digest",
    "readiness_digest",
    "phase_evidence",
    "v12_alignment",
    "governance_handoff",
    "parity_resilience",
)

# ---------------------------------------------------------------------------
# Default priorities — lower number = higher priority (displayed first)
# ---------------------------------------------------------------------------
PACK_DEFAULT_PRIORITIES: dict[str, int] = {
    "phase_evidence": 10,
    "measurement_digest": 20,
    "v12_alignment": 30,
    "governance_handoff": 40,
    "readiness_digest": 50,
    "parity_resilience": 60,
}

# ---------------------------------------------------------------------------
# Default max_lines_hint — suggested maximum lines per pack for display
# ---------------------------------------------------------------------------
PACK_DEFAULT_MAX_LINES_HINT: dict[str, int] = {
    "measurement_digest": 7,
    "readiness_digest": 7,
    "phase_evidence": 3,
    "v12_alignment": 8,
    "governance_handoff": 4,
    "parity_resilience": 4,
}

# ---------------------------------------------------------------------------
# Display labels — Chinese default / English fallback
# ---------------------------------------------------------------------------
PACK_DISPLAY_LABELS: dict[str, str] = {
    "measurement_digest": "测量审阅摘要",
    "readiness_digest": "就绪审阅摘要",
    "phase_evidence": "阶段证据摘要",
    "v12_alignment": "V1.2 对齐摘要",
    "governance_handoff": "治理交接摘要",
    "parity_resilience": "一致性/韧性摘要",
    "header": "紧凑摘要包",
}

PACK_DISPLAY_LABELS_EN: dict[str, str] = {
    "measurement_digest": "Measurement Review Digest",
    "readiness_digest": "Readiness Review Digest",
    "phase_evidence": "Phase Evidence Summary",
    "v12_alignment": "V1.2 Alignment Summary",
    "governance_handoff": "Governance Handoff Summary",
    "parity_resilience": "Parity/Resilience Summary",
    "header": "Compact Summary Pack",
}

# ---------------------------------------------------------------------------
# Surface budget hints — per-surface line budget for each pack
# ---------------------------------------------------------------------------
PACK_SURFACE_BUDGET_HINT: dict[str, dict[str, int]] = {
    "measurement_digest": {"results_gateway": 7, "review_center": 7, "historical": 5},
    "readiness_digest": {"results_gateway": 7, "review_center": 7, "historical": 5},
    "phase_evidence": {"results_gateway": 3, "review_center": 3, "historical": 3},
    "v12_alignment": {"results_gateway": 8, "review_center": 8, "historical": 6},
    "governance_handoff": {"results_gateway": 4, "review_center": 4, "historical": 3},
    "parity_resilience": {"results_gateway": 4, "review_center": 4, "historical": 3},
}


# ---------------------------------------------------------------------------
# Pack sorting — deterministic ordering by (priority ASC, summary_key ASC)
# ---------------------------------------------------------------------------

def sort_packs_by_priority(packs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort compact summary packs by (priority ASC, summary_key ASC).

    Returns a new list; does not modify the input list.
    Deterministic: same input always produces same output.
    """
    return sorted(packs, key=lambda p: (p.get("priority", 99), p.get("summary_key", "")))


# ---------------------------------------------------------------------------
# Render context — unified context for downstream surface consumption
# ---------------------------------------------------------------------------

def build_compact_summary_render_context(
    packs: list[dict[str, Any]],
    *,
    surface: str,
    budget: int | None = None,
    lang: str = "zh",
) -> dict[str, Any]:
    """Build a unified render context for compact summary packs on a given surface.

    This is the primary entry point for downstream consumers (app_facade,
    historical_artifacts, review_center_artifact_scope) to consume
    compact_summary_packs with deterministic ordering and budget governance.

    Args:
        packs: List of pack dicts from compact_summary_packs.
        surface: Surface name (e.g. "review_center", "historical").
        budget: Override budget. If None, uses SURFACE_DEFAULT_BUDGETS[surface].
        lang: "zh" (default) or "en".

    Returns:
        Dict with:
        - compact_summary_packs: sorted packs
        - compact_summary_sections: alias for compact_summary_packs (compat)
        - compact_summary_order: list of summary_key in display order
        - compact_summary_budget: budget usage summary dict
    """
    sorted_packs = sort_packs_by_priority(packs)
    budget_result = apply_surface_budget(sorted_packs, surface=surface, budget=budget)
    compact_summary_order = [p["summary_key"] for p in sorted_packs]
    compact_summary_budget = {
        "total_lines": budget_result["used"] + budget_result["truncated_count"],
        "pack_count": len(sorted_packs),
        "used": budget_result["used"],
        "budget": budget_result["budget"],
        "truncated_count": budget_result["truncated_count"],
    }
    return {
        "compact_summary_packs": sorted_packs,
        "compact_summary_sections": sorted_packs,
        "compact_summary_order": compact_summary_order,
        "compact_summary_budget": compact_summary_budget,
    }


# ---------------------------------------------------------------------------
# Pack wrapper — the core packaging function
# ---------------------------------------------------------------------------

def _wrap_as_pack(
    *,
    summary_key: str,
    builder_result: dict[str, Any],
    display_label: str,
    priority: int,
    severity: str,
    max_lines_hint: int,
    surface_budget_hint: dict[str, int],
) -> dict[str, Any]:
    """Wrap a builder result into a stable pack dict.

    Returns a dict with all required pack fields:
    - summary_key, display_label, priority, severity
    - summary_line (backward compat), summary_lines, compact_summary_lines
    - max_lines_hint, surface_budget_hint
    - boundary_markers
    - evidence_source, reviewer_only, not_real_acceptance_evidence, not_ready_for_formal_claim
    - builders_version, pack_version
    """
    summary_lines = list(builder_result.get("summary_lines") or [])
    compact_summary_lines = list(builder_result.get("summary_lines") or [])
    # summary_line: backward-compatible single-line join
    summary_line = " | ".join(summary_lines) if summary_lines else ""

    return {
        "summary_key": summary_key,
        "display_label": display_label,
        "priority": priority,
        "severity": severity,
        "summary_line": summary_line,
        "summary_lines": summary_lines,
        "compact_summary_lines": compact_summary_lines,
        "max_lines_hint": max_lines_hint,
        "surface_budget_hint": dict(surface_budget_hint),
        "boundary_markers": dict(PHASE_EVIDENCE_STEP2_BOUNDARY),
        "evidence_source": "simulated",
        "reviewer_only": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "builders_version": _BUILDERS_VERSION,
        "pack_version": REVIEWER_SUMMARY_PACKS_VERSION,
    }


# ---------------------------------------------------------------------------
# Pack builders — one per compact summary domain
# ---------------------------------------------------------------------------

def build_measurement_digest_pack(
    payload: dict[str, Any],
    *,
    priority: int | None = None,
    max_lines_hint: int | None = None,
) -> dict[str, Any]:
    """Build a measurement digest compact summary pack."""
    builder_result = build_measurement_digest_compact_summary(payload)
    return _wrap_as_pack(
        summary_key="measurement_digest",
        builder_result=builder_result,
        display_label=PACK_DISPLAY_LABELS["measurement_digest"],
        priority=priority if priority is not None else PACK_DEFAULT_PRIORITIES["measurement_digest"],
        severity="info",
        max_lines_hint=max_lines_hint if max_lines_hint is not None else PACK_DEFAULT_MAX_LINES_HINT["measurement_digest"],
        surface_budget_hint=PACK_SURFACE_BUDGET_HINT["measurement_digest"],
    )


def build_readiness_digest_pack(
    payload: dict[str, Any],
    *,
    priority: int | None = None,
    max_lines_hint: int | None = None,
) -> dict[str, Any]:
    """Build a readiness digest compact summary pack."""
    builder_result = build_readiness_digest_compact_summary(payload)
    return _wrap_as_pack(
        summary_key="readiness_digest",
        builder_result=builder_result,
        display_label=PACK_DISPLAY_LABELS["readiness_digest"],
        priority=priority if priority is not None else PACK_DEFAULT_PRIORITIES["readiness_digest"],
        severity="info",
        max_lines_hint=max_lines_hint if max_lines_hint is not None else PACK_DEFAULT_MAX_LINES_HINT["readiness_digest"],
        surface_budget_hint=PACK_SURFACE_BUDGET_HINT["readiness_digest"],
    )


def build_phase_evidence_pack(
    payload: dict[str, Any],
    *,
    priority: int | None = None,
    max_lines_hint: int | None = None,
) -> dict[str, Any]:
    """Build a phase evidence compact summary pack."""
    builder_result = build_phase_evidence_compact_summary(payload)
    return _wrap_as_pack(
        summary_key="phase_evidence",
        builder_result=builder_result,
        display_label=PACK_DISPLAY_LABELS["phase_evidence"],
        priority=priority if priority is not None else PACK_DEFAULT_PRIORITIES["phase_evidence"],
        severity="info",
        max_lines_hint=max_lines_hint if max_lines_hint is not None else PACK_DEFAULT_MAX_LINES_HINT["phase_evidence"],
        surface_budget_hint=PACK_SURFACE_BUDGET_HINT["phase_evidence"],
    )


def build_v12_alignment_pack(
    payload: dict[str, Any],
    *,
    priority: int | None = None,
    max_lines_hint: int | None = None,
) -> dict[str, Any]:
    """Build a V1.2 alignment compact summary pack.

    Severity is derived from payload:
    - If alignment_status == "attention" -> severity = "attention"
    - Otherwise -> severity = "info"
    """
    builder_result = build_v12_alignment_compact_summary(payload)
    # Derive severity from v12_compact or payload
    v12_compact = dict(builder_result.get("v12_compact") or {})
    alignment_status = str(
        payload.get("alignment_status")
        or v12_compact.get("alignment_status")
        or "info"
    )
    severity = "attention" if alignment_status == "attention" else "info"
    return _wrap_as_pack(
        summary_key="v12_alignment",
        builder_result=builder_result,
        display_label=PACK_DISPLAY_LABELS["v12_alignment"],
        priority=priority if priority is not None else PACK_DEFAULT_PRIORITIES["v12_alignment"],
        severity=severity,
        max_lines_hint=max_lines_hint if max_lines_hint is not None else PACK_DEFAULT_MAX_LINES_HINT["v12_alignment"],
        surface_budget_hint=PACK_SURFACE_BUDGET_HINT["v12_alignment"],
    )


def build_governance_handoff_pack(
    payload: dict[str, Any],
    *,
    priority: int | None = None,
    max_lines_hint: int | None = None,
) -> dict[str, Any]:
    """Build a governance handoff compact summary pack.

    Severity is derived from payload:
    - If blockers is non-empty -> severity = "blocker"
    - Otherwise -> severity = "info"
    """
    builder_result = build_governance_handoff_compact_summary(payload)
    # Derive severity from blockers
    blockers = payload.get("blockers") or payload.get("blocking_items") or []
    if isinstance(blockers, str):
        blockers = [blockers]
    severity = "blocker" if blockers and len(blockers) > 0 else "info"
    return _wrap_as_pack(
        summary_key="governance_handoff",
        builder_result=builder_result,
        display_label=PACK_DISPLAY_LABELS["governance_handoff"],
        priority=priority if priority is not None else PACK_DEFAULT_PRIORITIES["governance_handoff"],
        severity=severity,
        max_lines_hint=max_lines_hint if max_lines_hint is not None else PACK_DEFAULT_MAX_LINES_HINT["governance_handoff"],
        surface_budget_hint=PACK_SURFACE_BUDGET_HINT["governance_handoff"],
    )


def build_parity_resilience_pack(
    payload: dict[str, Any],
    *,
    priority: int | None = None,
    max_lines_hint: int | None = None,
) -> dict[str, Any]:
    """Build a parity/resilience compact summary pack."""
    builder_result = build_parity_resilience_compact_summary(payload)
    return _wrap_as_pack(
        summary_key="parity_resilience",
        builder_result=builder_result,
        display_label=PACK_DISPLAY_LABELS["parity_resilience"],
        priority=priority if priority is not None else PACK_DEFAULT_PRIORITIES["parity_resilience"],
        severity="info",
        max_lines_hint=max_lines_hint if max_lines_hint is not None else PACK_DEFAULT_MAX_LINES_HINT["parity_resilience"],
        surface_budget_hint=PACK_SURFACE_BUDGET_HINT["parity_resilience"],
    )
