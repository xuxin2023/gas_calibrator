"""Compact summary budget — surface-aware line budget, truncation, and ordering.

This module provides deterministic governance for compact summary line budgets
across different display surfaces (results_gateway, review_center, historical).

Design principles:
- Each surface gets a fixed line budget
- Packs are ordered by (priority, summary_key) — deterministic
- First pack (highest priority) lines are always must_retain
- Lower priority lines are truncated first when budget is exceeded
- Truncation produces explicit diagnostic signals, not silent gaps
- Chinese default, English fallback
- No formal acceptance / formal claim language
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
COMPACT_SUMMARY_BUDGET_VERSION: str = "2.13.0"

# ---------------------------------------------------------------------------
# Surface default budgets — max compact summary lines per surface
# ---------------------------------------------------------------------------
SURFACE_DEFAULT_BUDGETS: dict[str, int] = {
    "results_gateway": 24,
    "review_center": 40,
    "historical": 32,
}

# ---------------------------------------------------------------------------
# Truncation labels — Chinese default / English fallback
# ---------------------------------------------------------------------------
TRUNCATION_LABELS: dict[str, str] = {
    "truncated_hint": "（{count} 行摘要因预算截断未显示）",
    "truncated_note": "摘要行预算已用尽",
}

TRUNCATION_LABELS_EN: dict[str, str] = {
    "truncated_hint": "({count} summary lines truncated due to budget)",
    "truncated_note": "Summary line budget exhausted",
}


# ---------------------------------------------------------------------------
# Surface budget lookup
# ---------------------------------------------------------------------------

def get_surface_budget(surface: str) -> int:
    """Get the default line budget for a surface.

    Returns 0 if the surface is not recognized.
    """
    return SURFACE_DEFAULT_BUDGETS.get(surface, 0)


# ---------------------------------------------------------------------------
# Truncation hint line
# ---------------------------------------------------------------------------

def build_truncation_hint_line(
    truncated_count: int,
    *,
    lang: str = "zh",
) -> str:
    """Build a human-readable truncation hint line.

    Args:
        truncated_count: Number of truncated lines.
        lang: "zh" (default) or "en".

    Returns:
        A string like "（3 行摘要因预算截断未显示）" or
        "(3 summary lines truncated due to budget)".
    """
    if lang == "en":
        return TRUNCATION_LABELS_EN["truncated_hint"].format(count=truncated_count)
    return TRUNCATION_LABELS["truncated_hint"].format(count=truncated_count)


# ---------------------------------------------------------------------------
# Surface budget application — the core governance function
# ---------------------------------------------------------------------------

def apply_surface_budget(
    packs: list[dict[str, Any]],
    *,
    surface: str,
    budget: int | None = None,
) -> dict[str, Any]:
    """Apply surface-aware line budget to a list of compact summary packs.

    Packs are sorted by (priority ASC, summary_key ASC) — deterministic.
    The first pack's lines are always must_retain (as long as budget > 0).
    Subsequent packs' lines are allocated from remaining budget:
    - Lines that fit within budget -> optional_expand
    - Lines that exceed budget -> truncated

    Args:
        packs: List of pack dicts, each with summary_key, priority, summary_lines.
        surface: Surface name (e.g. "results_gateway").
        budget: Override budget. If None, uses SURFACE_DEFAULT_BUDGETS[surface].

    Returns:
        Dict with:
        - must_retain: list[str] — lines from highest-priority pack
        - optional_expand: list[str] — lines from lower-priority packs within budget
        - truncated: list[str] — lines truncated due to budget
        - truncated_count: int
        - truncated_pack_keys: list[str] — pack keys that had lines truncated
        - surface: str
        - budget: int
        - used: int — total lines used (must_retain + optional_expand)
    """
    if budget is None:
        budget = get_surface_budget(surface)

    # Sort packs by (priority ASC, summary_key ASC) — deterministic
    sorted_packs = sorted(packs, key=lambda p: (p.get("priority", 99), p.get("summary_key", "")))

    must_retain: list[str] = []
    optional_expand: list[str] = []
    truncated: list[str] = []
    truncated_pack_keys: list[str] = []
    remaining = budget

    for i, pack in enumerate(sorted_packs):
        lines = list(pack.get("summary_lines") or [])
        pack_key = str(pack.get("summary_key", ""))

        if i == 0:
            # First pack (highest priority) -> must_retain
            # Allocate as many lines as budget allows
            take = min(len(lines), remaining)
            must_retain.extend(lines[:take])
            truncated.extend(lines[take:])
            remaining -= take
            if take < len(lines):
                truncated_pack_keys.append(pack_key)
        else:
            # Subsequent packs -> optional_expand or truncated
            take = min(len(lines), remaining)
            optional_expand.extend(lines[:take])
            truncated.extend(lines[take:])
            remaining -= take
            if take < len(lines):
                truncated_pack_keys.append(pack_key)

    return {
        "must_retain": must_retain,
        "optional_expand": optional_expand,
        "truncated": truncated,
        "truncated_count": len(truncated),
        "truncated_pack_keys": truncated_pack_keys,
        "surface": surface,
        "budget": budget,
        "used": len(must_retain) + len(optional_expand),
    }
