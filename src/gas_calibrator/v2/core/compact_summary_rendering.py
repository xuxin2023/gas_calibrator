"""Compact summary rendering — single source of truth for visible section rendering.

Provides shared helpers for building rendered_summary_sections,
omitted_summary_sections, and compact_summary_budget_display from
compact summary packs and budget data.

All three surfaces (app_facade / review_center, historical, results_gateway)
consume these helpers instead of maintaining parallel logic.

Design principles:
- Deterministic ordering by (priority ASC, summary_key ASC)
- Chinese default, English fallback
- Graceful degradation when compact_summary_packs is missing / None / empty
- No formal acceptance / formal claim language
- No real device / real acceptance / real compare paths
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
COMPACT_SUMMARY_RENDERING_VERSION: str = "2.17.0"

# ---------------------------------------------------------------------------
# Fallback defaults for old runs missing compact_summary_packs
# ---------------------------------------------------------------------------
FALLBACK_COMPACT_SUMMARY_PACKS: list[dict[str, Any]] = []
FALLBACK_COMPACT_SUMMARY_ORDER: list[str] = []
FALLBACK_COMPACT_SUMMARY_BUDGET: dict[str, Any] = {}
FALLBACK_RENDERED_SUMMARY_SECTIONS: list[dict[str, Any]] = []
FALLBACK_OMITTED_SUMMARY_SECTIONS: list[dict[str, Any]] = []
FALLBACK_COMPACT_SUMMARY_BUDGET_DISPLAY: dict[str, Any] = {
    "used": 0,
    "budget": 0,
    "total_lines": 0,
    "truncated_count": 0,
    "pack_count": 0,
}


# ---------------------------------------------------------------------------
# build_visible_sections — unified entry point
# ---------------------------------------------------------------------------

def build_visible_sections(
    packs: list[dict[str, Any]],
    *,
    budget: dict[str, Any] | None = None,
    locale: str = "zh",
) -> dict[str, Any]:
    """Build rendered, omitted, and budget display from packs and budget.

    This is the single entry point that replaces the parallel logic
    previously in app_facade._build_rendered_sections / _build_omitted_sections /
    _build_budget_display and review_center_artifact_scope.
    _build_compact_summary_pack_visible_sections.

    Args:
        packs: Sorted compact summary packs (already ordered by priority).
        budget: Budget dict with keys: used, budget, total_lines,
                truncated_count, pack_count.
        locale: "zh" (default) or "en". Reserved for future i18n expansion.

    Returns:
        Dict with:
        - rendered_summary_sections: list of dicts
        - omitted_summary_sections: list of dicts
        - compact_summary_budget_display: dict
    """
    if not packs:
        return {
            "rendered_summary_sections": [],
            "omitted_summary_sections": [],
            "compact_summary_budget_display": dict(FALLBACK_COMPACT_SUMMARY_BUDGET_DISPLAY),
        }

    _budget = dict(budget or {})
    used = int(_budget.get("used", 0) or 0)
    total_lines = int(_budget.get("total_lines", 0) or 0)
    truncated_count = int(_budget.get("truncated_count", 0) or 0)

    rendered: list[dict[str, Any]] = []
    omitted: list[dict[str, Any]] = []
    lines_remaining = used

    for pack in packs:
        summary_key = str(pack.get("summary_key", ""))
        display_label = str(pack.get("display_label") or summary_key)
        summary_line = str(pack.get("summary_line") or "")
        all_lines = list(pack.get("compact_summary_lines") or pack.get("summary_lines") or [])
        priority = int(pack.get("priority", 99) or 99)
        severity = str(pack.get("severity", "info") or "info")

        # Pack with no lines and budget exhausted -> omit with budget_exhausted
        if lines_remaining <= 0 and not all_lines:
            omitted.append({
                "summary_key": summary_key,
                "display_label": display_label,
                "priority": priority,
                "severity": severity,
                "reason": "budget_exhausted",
            })
            continue

        # Determine how many lines to show for this pack
        show_count = min(len(all_lines), lines_remaining) if lines_remaining > 0 else 0
        shown_lines = all_lines[:show_count]
        pack_truncated = len(all_lines) - show_count

        if show_count > 0:
            rendered.append({
                "summary_key": summary_key,
                "display_label": display_label,
                "summary_line": summary_line,
                "summary_lines": shown_lines,
                "priority": priority,
                "severity": severity,
                "truncated": pack_truncated > 0,
                "truncated_count": pack_truncated,
            })
            lines_remaining -= show_count
        elif all_lines:
            # All lines truncated for this pack
            omitted.append({
                "summary_key": summary_key,
                "display_label": display_label,
                "priority": priority,
                "severity": severity,
                "reason": "budget_truncated",
                "total_lines": len(all_lines),
            })

    # Build budget display
    budget_display = {
        "used": used,
        "budget": int(_budget.get("budget", 0) or 0),
        "total_lines": total_lines,
        "truncated_count": truncated_count,
        "pack_count": int(_budget.get("pack_count", 0) or 0),
    }

    return {
        "rendered_summary_sections": rendered,
        "omitted_summary_sections": omitted,
        "compact_summary_budget_display": budget_display,
    }


# ---------------------------------------------------------------------------
# build_rendered_sections — convenience wrapper (app_facade compat)
# ---------------------------------------------------------------------------

def build_rendered_sections(
    packs: list[dict[str, Any]],
    budget: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build rendered summary sections from compact summary packs.

    Convenience wrapper matching the former app_facade._build_rendered_sections
    signature for backward compatibility.
    """
    result = build_visible_sections(packs, budget=budget)
    return list(result.get("rendered_summary_sections") or [])


# ---------------------------------------------------------------------------
# build_omitted_sections — convenience wrapper (app_facade compat)
# ---------------------------------------------------------------------------

def build_omitted_sections(
    packs: list[dict[str, Any]],
    budget: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build omitted summary sections from compact summary packs.

    Convenience wrapper matching the former app_facade._build_omitted_sections
    signature for backward compatibility.
    """
    result = build_visible_sections(packs, budget=budget)
    return list(result.get("omitted_summary_sections") or [])


# ---------------------------------------------------------------------------
# build_budget_display — convenience wrapper (app_facade compat)
# ---------------------------------------------------------------------------

def build_budget_display(budget: dict[str, Any]) -> dict[str, Any]:
    """Build budget display dict from compact summary budget.

    Convenience wrapper matching the former app_facade._build_budget_display
    signature for backward compatibility.
    """
    return {
        "used": int(budget.get("used", 0) or 0),
        "budget": int(budget.get("budget", 0) or 0),
        "total_lines": int(budget.get("total_lines", 0) or 0),
        "truncated_count": int(budget.get("truncated_count", 0) or 0),
        "pack_count": int(budget.get("pack_count", 0) or 0),
    }


# ---------------------------------------------------------------------------
# build_compact_summary_pack_fields — unified pack fields builder
# ---------------------------------------------------------------------------

def build_compact_summary_pack_fields(
    packs: list[dict[str, Any]] | None = None,
    *,
    surface: str = "review_center",
) -> dict[str, Any]:
    """Build compact summary pack fields for a given surface.

    Unified replacement for the former _build_compact_summary_pack_fields
    in review_center_artifact_scope and the inline logic in app_facade.

    Args:
        packs: Raw compact summary packs (may be unsorted).
        surface: Surface name for budget governance.

    Returns:
        Dict with compact_summary_packs, compact_summary_sections,
        compact_summary_order, compact_summary_budget.
    """
    from .reviewer_summary_packs import build_compact_summary_render_context

    _raw_packs = list(packs or [])
    if _raw_packs:
        try:
            _ctx = build_compact_summary_render_context(_raw_packs, surface=surface)
            return {
                "compact_summary_packs": list(_ctx.get("compact_summary_packs") or []),
                "compact_summary_sections": list(_ctx.get("compact_summary_sections") or []),
                "compact_summary_order": list(_ctx.get("compact_summary_order") or []),
                "compact_summary_budget": dict(_ctx.get("compact_summary_budget") or {}),
            }
        except Exception:
            pass
    return {
        "compact_summary_packs": [],
        "compact_summary_sections": [],
        "compact_summary_order": [],
        "compact_summary_budget": {},
    }


# ---------------------------------------------------------------------------
# build_full_compact_summary_view — combined fields + visible sections
# ---------------------------------------------------------------------------

def build_full_compact_summary_view(
    packs: list[dict[str, Any]] | None = None,
    *,
    surface: str = "review_center",
    locale: str = "zh",
) -> dict[str, Any]:
    """Build the complete compact summary view: pack fields + visible sections.

    This is the highest-level convenience function that combines
    build_compact_summary_pack_fields and build_visible_sections,
    eliminating the need for callers to do two-step construction.

    Args:
        packs: Raw compact summary packs (may be unsorted).
        surface: Surface name for budget governance.
        locale: "zh" (default) or "en".

    Returns:
        Dict with all compact_summary_* fields plus
        rendered_summary_sections, omitted_summary_sections,
        compact_summary_budget_display.
    """
    pack_fields = build_compact_summary_pack_fields(packs, surface=surface)
    visible = build_visible_sections(
        list(pack_fields.get("compact_summary_packs") or []),
        budget=dict(pack_fields.get("compact_summary_budget") or {}),
        locale=locale,
    )
    return {
        **pack_fields,
        **visible,
    }


# ---------------------------------------------------------------------------
# Old-run fallback — stable degradation for missing compact_summary_packs
# ---------------------------------------------------------------------------

def build_old_run_fallback() -> dict[str, Any]:
    """Build stable fallback values for old runs missing compact_summary_packs.

    Returns a dict with all compact summary fields set to safe empty/zero
    defaults. Used when a summary.json from before Step 2.13 is loaded
    and has no compact_summary_packs key.

    Does not modify old run files. Does not produce UI noise.
    """
    return {
        "compact_summary_packs": list(FALLBACK_COMPACT_SUMMARY_PACKS),
        "compact_summary_sections": [],
        "compact_summary_order": list(FALLBACK_COMPACT_SUMMARY_ORDER),
        "compact_summary_budget": dict(FALLBACK_COMPACT_SUMMARY_BUDGET),
        "rendered_summary_sections": list(FALLBACK_RENDERED_SUMMARY_SECTIONS),
        "omitted_summary_sections": list(FALLBACK_OMITTED_SUMMARY_SECTIONS),
        "compact_summary_budget_display": dict(FALLBACK_COMPACT_SUMMARY_BUDGET_DISPLAY),
        "compact_summary_legacy_mode": True,
    }


# ---------------------------------------------------------------------------
# Legacy hint — reviewer-facing compatibility notice for old runs
# ---------------------------------------------------------------------------

def build_legacy_hint(
    *,
    has_packs: bool = False,
    locale: str = "zh",
) -> dict[str, Any]:
    """Build a lightweight reviewer-facing legacy/compatibility hint.

    When old runs lack compact_summary_packs, this produces a non-noisy
    hint that the reviewer is seeing a compatibility view.

    Args:
        has_packs: Whether the run has compact_summary_packs.
        locale: "zh" (default) or "en".

    Returns:
        Dict with:
        - compact_summary_legacy_mode: bool
        - compact_summary_legacy_label: str (Chinese default, English fallback)
        - compact_summary_legacy_hint: str
    """
    if has_packs:
        return {
            "compact_summary_legacy_mode": False,
            "compact_summary_legacy_label": "",
            "compact_summary_legacy_hint": "",
        }
    if locale == "en":
        return {
            "compact_summary_legacy_mode": True,
            "compact_summary_legacy_label": "Compatibility View",
            "compact_summary_legacy_hint": "No compact summary pack provided; using compatibility rendering",
        }
    return {
        "compact_summary_legacy_mode": True,
        "compact_summary_legacy_label": "兼容视图",
        "compact_summary_legacy_hint": "未提供紧凑摘要包，已使用兼容渲染",
    }


# ---------------------------------------------------------------------------
# build_compact_summary_display_text — reviewer-facing display text
# ---------------------------------------------------------------------------

def build_compact_summary_display_text(
    packs: list[dict[str, Any]] | None = None,
    *,
    budget: dict[str, Any] | None = None,
    locale: str = "zh",
) -> dict[str, Any]:
    """Build reviewer-facing display text from compact summary packs.

    Produces a structured dict suitable for rendering in reports page
    and review center panel, including:
    - rendered lines per section (display_label + summary_lines)
    - omitted section labels
    - budget summary line
    - legacy hint (if packs empty)

    Args:
        packs: Sorted compact summary packs.
        budget: Budget dict.
        locale: "zh" (default) or "en".

    Returns:
        Dict with display_text, section_entries, omitted_labels,
        budget_line, legacy_hint.
    """
    _packs = list(packs or [])
    visible = build_visible_sections(_packs, budget=budget, locale=locale)
    rendered = list(visible.get("rendered_summary_sections") or [])
    omitted = list(visible.get("omitted_summary_sections") or [])
    budget_display = dict(visible.get("compact_summary_budget_display") or {})
    legacy = build_legacy_hint(has_packs=bool(_packs), locale=locale)

    section_entries: list[dict[str, Any]] = []
    display_lines: list[str] = []

    for section in rendered:
        label = str(section.get("display_label") or section.get("summary_key", ""))
        summary_line = str(section.get("summary_line") or "")
        section_lines = list(section.get("summary_lines") or [])
        severity = str(section.get("severity", "info") or "info")
        truncated = bool(section.get("truncated", False))
        truncated_count = int(section.get("truncated_count", 0) or 0)
        entry = {
            "display_label": label,
            "summary_line": summary_line,
            "summary_lines": section_lines,
            "severity": severity,
            "truncated": truncated,
            "truncated_count": truncated_count,
        }
        section_entries.append(entry)
        # Build display text lines
        if label:
            display_lines.append(f"[{label}]" if locale == "en" else f"【{label}】")
        if summary_line:
            display_lines.append(f"  {summary_line}")
        for sl in section_lines:
            display_lines.append(f"  - {sl}")
        if truncated and truncated_count > 0:
            if locale == "en":
                display_lines.append(f"  ({truncated_count} lines truncated)")
            else:
                display_lines.append(f"  （{truncated_count} 行截断）")

    omitted_labels = [
        str(s.get("display_label") or s.get("summary_key", "")) for s in omitted
    ]
    if omitted_labels:
        if locale == "en":
            display_lines.append(f"Omitted: {', '.join(omitted_labels)}")
        else:
            display_lines.append(f"已省略：{', '.join(omitted_labels)}")

    # Budget line
    used = int(budget_display.get("used", 0) or 0)
    budget_val = int(budget_display.get("budget", 0) or 0)
    pack_count = int(budget_display.get("pack_count", 0) or 0)
    if budget_val > 0 or pack_count > 0:
        if locale == "en":
            budget_line = f"Budget: {used}/{budget_val} lines, {pack_count} packs"
        else:
            budget_line = f"预算：{used}/{budget_val} 行，{pack_count} 包"
    else:
        budget_line = ""
    if budget_line:
        display_lines.append(budget_line)

    # Legacy hint
    if legacy.get("compact_summary_legacy_mode"):
        display_lines.append(str(legacy.get("compact_summary_legacy_hint") or ""))

    return {
        "display_text": "\n".join(display_lines),
        "section_entries": section_entries,
        "omitted_labels": omitted_labels,
        "budget_line": budget_line,
        "budget_display": budget_display,
        "legacy_hint": legacy,
        "rendered_summary_sections": rendered,
        "omitted_summary_sections": omitted,
        "compact_summary_budget_display": budget_display,
    }
