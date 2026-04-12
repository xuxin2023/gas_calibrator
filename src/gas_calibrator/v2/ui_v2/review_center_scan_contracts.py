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

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
REVIEW_CENTER_SCAN_CONTRACTS_VERSION: str = "2.7.0"

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
    ("suite", "suite_summary.json", 10, 48, "suite_roots"),
    ("parity", "summary_parity_report.json", 20, 32, "compare_roots"),
    ("resilience", "export_resilience_report.json", 21, 32, "compare_roots"),
    ("workbench", "workbench_action_report.json", 30, 24, "run_roots"),
    ("stability", "multi_source_stability_evidence.json", 40, 24, "run_roots"),
    ("state_transition", "state_transition_evidence.json", 41, 24, "run_roots"),
    ("measurement_phase_coverage", "measurement_phase_coverage_report.json", 42, 24, "run_roots"),
    ("artifact_compatibility", "compatibility_scan_summary.json", 43, 24, "run_roots"),
    ("readiness_governance", "readiness_governance", 50, 32, "run_roots"),
    ("analytics", "analytics_summary.json", 60, 24, "run_roots"),
    ("offline_diagnostic", "diagnostic_summary.json", 70, 24, "run_roots"),
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
V12_PHASE_DISPLAY_TERMS: dict[str, str] = {
    "ambient": "环境条件",
    "ambient_open": "环境开路",
    "flush_gate": "冲洗门禁",
    "preseal": "前封气",
    "postseal": "后封气",
    "stale_gauge": "压力参考陈旧",
    "phase_transition": "阶段过渡",
    "bridge": "桥接",
    "governance_handoff": "治理交接",
    "parity": "一致性比对",
    "resilience": "导出韧性",
    "point_taxonomy": "测点分类",
    "measurement_phase_coverage": "测量阶段覆盖",
}

V12_PHASE_DISPLAY_TERMS_EN: dict[str, str] = {
    "ambient": "Ambient",
    "ambient_open": "Ambient Open",
    "flush_gate": "Flush Gate",
    "preseal": "Preseal",
    "postseal": "Postseal",
    "stale_gauge": "Stale Gauge",
    "phase_transition": "Phase Transition",
    "bridge": "Bridge",
    "governance_handoff": "Governance Handoff",
    "parity": "Parity",
    "resilience": "Resilience",
    "point_taxonomy": "Point Taxonomy",
    "measurement_phase_coverage": "Measurement Phase Coverage",
}


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
