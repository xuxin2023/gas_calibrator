"""Review center evidence scan contracts — family-aware budget, priority, and ordering.

This module is the single source of truth for:
- artifact family ordering (scan sequence)
- per-family scan budget allocation
- family priority levels
- deterministic fallback behavior when a family's budget is exhausted

Design principles:
- Each family gets its own budget slice; families do not starve each other.
- Scan order is deterministic and priority-based.
- Budget exhaustion produces explicit omitted/budget_limited signals, not silent gaps.
- All evidence remains simulation-only; no real acceptance semantics.
"""

from __future__ import annotations

from ..core.phase_evidence_display_contracts import (
    PHASE_TERMS as _PHASE_TERMS,
    PHASE_TERMS_EN as _PHASE_TERMS_EN,
)
from ..core.reviewer_summary_builders import (
    build_v12_alignment_compact_summary as _build_v12_compact,
    build_parity_resilience_compact_summary as _build_parity_resilience_compact,
    REVIEWER_SUMMARY_BUILDERS_VERSION as _BUILDERS_VERSION,
)

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
REVIEW_CENTER_SCAN_CONTRACTS_VERSION: str = "2.12.0"

# ---------------------------------------------------------------------------
# Artifact family definitions
# ---------------------------------------------------------------------------
# Each family tuple: (family_key, filename, priority, default_budget, roots_kind)
#   family_key: stable identifier used in diagnostics and index_summary
#   filename: the JSON file to search for
#   priority: lower number = scanned first (higher priority)
#   default_budget: per-family scan budget (directory visits allowed for this family)
#   roots_kind: "run_roots" | "suite_roots" | "compare_roots"

REVIEW_CENTER_ARTIFACT_FAMILIES: tuple[tuple[str, str, int, int, str], ...] = (
    # (family_key, filename, priority, default_budget, roots_kind)
    # Budget per family is independent; families do not starve each other.
    # Budget must be large enough for os.walk across all roots for that family.
    ("suite", "suite_summary.json", 10, 64, "suite_roots"),
    ("parity", "summary_parity_report.json", 20, 64, "compare_roots"),
    ("resilience", "export_resilience_report.json", 21, 64, "compare_roots"),
    ("workbench", "workbench_action_report.json", 30, 48, "run_roots"),
    ("stability", "multi_source_stability_evidence.json", 40, 48, "run_roots"),
    ("state_transition", "state_transition_evidence.json", 41, 48, "run_roots"),
    ("measurement_phase_coverage", "measurement_phase_coverage_report.json", 42, 48, "run_roots"),
    ("artifact_compatibility", "compatibility_scan_summary.json", 43, 48, "run_roots"),
    ("readiness_governance", "readiness_governance", 50, 64, "run_roots"),
    ("analytics", "analytics_summary.json", 60, 48, "run_roots"),
    ("offline_diagnostic", "diagnostic_summary.json", 70, 48, "run_roots"),
)

# ---------------------------------------------------------------------------
# Derived lookup tables
# ---------------------------------------------------------------------------

FAMILY_KEY_TO_FILENAME: dict[str, str] = {
    fam[0]: fam[1] for fam in REVIEW_CENTER_ARTIFACT_FAMILIES
}

FAMILY_KEY_TO_PRIORITY: dict[str, int] = {
    fam[0]: fam[2] for fam in REVIEW_CENTER_ARTIFACT_FAMILIES
}

FAMILY_KEY_TO_BUDGET: dict[str, int] = {
    fam[0]: fam[3] for fam in REVIEW_CENTER_ARTIFACT_FAMILIES
}

FAMILY_KEY_TO_ROOTS_KIND: dict[str, str] = {
    fam[0]: fam[4] for fam in REVIEW_CENTER_ARTIFACT_FAMILIES
}

# Families sorted by priority (deterministic scan order)
FAMILY_SCAN_ORDER: tuple[str, ...] = tuple(
    fam[0] for fam in sorted(REVIEW_CENTER_ARTIFACT_FAMILIES, key=lambda f: f[2])
)

# ---------------------------------------------------------------------------
# Global budget fallback (used when family-aware budget is not active)
# ---------------------------------------------------------------------------
REVIEW_CENTER_SCAN_BUDGET_GLOBAL_FALLBACK: int = 192

# ---------------------------------------------------------------------------
# V1.2 phase evidence families — the artifact families that V1.2 cares about
# ---------------------------------------------------------------------------
V12_PHASE_EVIDENCE_FAMILIES: tuple[str, ...] = (
    "parity",
    "resilience",
    "measurement_phase_coverage",
    "stability",
    "state_transition",
    "artifact_compatibility",
)

# ---------------------------------------------------------------------------
# V1.2 taxonomy/phase display terms (Chinese default, English fallback)
# ---------------------------------------------------------------------------
V12_PHASE_DISPLAY_TERMS: dict[str, str] = _PHASE_TERMS

V12_PHASE_DISPLAY_TERMS_EN: dict[str, str] = _PHASE_TERMS_EN


def resolve_v12_phase_display(key: str, *, lang: str = "zh") -> str:
    """Resolve V1.2 phase display term. Chinese default, English fallback."""
    if lang == "en":
        return V12_PHASE_DISPLAY_TERMS_EN.get(key, key)
    return V12_PHASE_DISPLAY_TERMS.get(key, key)


# ---------------------------------------------------------------------------
# Budget allocation helpers
# ---------------------------------------------------------------------------


def allocate_family_budgets(
    *,
    global_budget: int | None = None,
) -> dict[str, int]:
    """Allocate per-family scan budgets.

    If global_budget is provided, families share it proportionally
    based on their default_budget weights. Otherwise each family
    gets its default_budget.

    Returns a dict mapping family_key -> allocated_budget.
    """
    defaults = FAMILY_KEY_TO_BUDGET
    if global_budget is None:
        return dict(defaults)
    total_weight = sum(defaults.values())
    if total_weight <= 0:
        return {k: 0 for k in defaults}
    allocated: dict[str, int] = {}
    remaining = global_budget
    for key in FAMILY_SCAN_ORDER:
        share = max(1, int(global_budget * defaults[key] / total_weight))
        allocated[key] = min(share, remaining)
        remaining -= allocated[key]
        if remaining <= 0:
            break
    # Any remaining budget goes to the last family
    if remaining > 0:
        last_key = FAMILY_SCAN_ORDER[-1]
        allocated[last_key] = allocated.get(last_key, 0) + remaining
    return allocated


def build_family_budget_summary(
    family_budgets: dict[str, int],
    family_used: dict[str, int],
) -> dict[str, dict[str, int | bool | str]]:
    """Build a diagnostic summary of per-family budget usage.

    Returns a dict mapping family_key -> {
        "budget": int, "used": int, "remaining": int,
        "exhausted": bool, "status": "ok"|"budget_limited"|"omitted"
    }
    """
    summary: dict[str, dict[str, int | bool | str]] = {}
    for key in FAMILY_SCAN_ORDER:
        budget = family_budgets.get(key, 0)
        used = family_used.get(key, 0)
        remaining = max(0, budget - used)
        exhausted = used >= budget and budget > 0
        if budget <= 0:
            status: str = "omitted"
        elif exhausted:
            status = "budget_limited"
        else:
            status = "ok"
        summary[key] = {
            "budget": budget,
            "used": used,
            "remaining": remaining,
            "exhausted": exhausted,
            "status": status,
        }
    return summary


# ---------------------------------------------------------------------------
# Step 2 boundary markers
# ---------------------------------------------------------------------------
REVIEW_CENTER_SCAN_STEP2_BOUNDARY: dict[str, str | bool] = {
    "evidence_source": "simulated",
    "not_real_acceptance_evidence": True,
    "not_ready_for_formal_claim": True,
    "reviewer_only": True,
    "readiness_mapping_only": True,
}


# ---------------------------------------------------------------------------
# V1.2 alignment summary — simulation-only reviewer-first aggregation
# ---------------------------------------------------------------------------


def build_v12_alignment_summary(
    *,
    point_taxonomy_summary: dict | None = None,
    measurement_phase_coverage_report: dict | None = None,
    parity_status: str | None = None,
    resilience_status: str | None = None,
    governance_handoff_blockers: list[str] | None = None,
    family_budget_summary: dict | None = None,
) -> dict[str, Any]:
    """Build a lightweight V1.2 alignment summary for reviewer-first consumption.

    This aggregates simulation evidence across:
    - point taxonomy
    - phase coverage
    - parity / resilience
    - governance handoff blockers

    Output is explicitly:
    - simulated only
    - reviewer only
    - not real acceptance evidence
    - not ready for formal claim
    """
    _taxonomy = dict(point_taxonomy_summary or {})
    _phase_coverage = dict(measurement_phase_coverage_report or {})
    _blockers = list(governance_handoff_blockers or [])
    _family_budget = dict(family_budget_summary or {})

    # Extract key dimensions from taxonomy
    _taxonomy_dims: dict[str, str] = {}
    for dim_key in ("ambient", "ambient_open", "flush_gate", "preseal", "postseal", "stale_gauge"):
        _taxonomy_dims[dim_key] = resolve_v12_phase_display(dim_key)

    # Extract phase coverage digest
    _phase_digest = dict(_phase_coverage.get("digest", {}) or {})
    _phase_health = str(_phase_digest.get("health", "--") or "--")
    _phase_summary = str(_phase_digest.get("summary", "") or "")

    # Build alignment status
    _parity_ok = parity_status == "MATCH"
    _resilience_ok = resilience_status == "MATCH"
    _no_blockers = len(_blockers) == 0
    _phase_ok = _phase_health in ("ok", "attention", "pass", "passed", "--")

    _alignment_status = "aligned" if (_parity_ok and _resilience_ok and _no_blockers and _phase_ok) else "attention"

    # Build budget-limited families list
    _budget_limited = [k for k, v in _family_budget.items() if isinstance(v, dict) and v.get("status") == "budget_limited"]

    # Consume shared compact builders for summary_line
    _v12_compact_payload = {
        "point_taxonomy_summary": _taxonomy,
        "measurement_phase_coverage_report": _phase_coverage,
        "parity_resilience_summary": {
            "parity_status": parity_status or "--",
            "resilience_status": resilience_status or "--",
        },
        "governance_handoff_summary": {
            "blockers": _blockers,
        },
    }
    _v12_compact = _build_v12_compact(_v12_compact_payload)
    _summary_line = " | ".join(_v12_compact.get("summary_lines", []))

    return {
        "v12_alignment_summary": {
            "alignment_status": _alignment_status,
            "parity_status": parity_status or "--",
            "resilience_status": resilience_status or "--",
            "governance_blockers": _blockers,
            "phase_health": _phase_health,
            "phase_summary": _phase_summary,
            "taxonomy_dimensions": _taxonomy_dims,
            "budget_limited_families": _budget_limited,
            "summary_line": _summary_line,
            "compact_summary_lines": list(_v12_compact.get("summary_lines", [])),
            "builders_version": _BUILDERS_VERSION,
        },
        # Step 2 boundary markers — this is simulation-only evidence
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
    }

