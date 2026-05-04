"""Step 2.16 compact summary rendering shared helper tests.

Covers:
- build_visible_sections is the single source of truth
- build_rendered_sections / build_omitted_sections / build_budget_display wrappers
- build_compact_summary_pack_fields delegates correctly
- build_full_compact_summary_view combines fields + visible
- build_old_run_fallback provides stable degradation
- Deterministic ordering
- Budget logic consistency across surfaces
- Old-run missing compact_summary_packs degrades gracefully
- No real device / formal approval / real acceptance language
"""

from __future__ import annotations

import pytest


def _make_mock_packs():
    """Create 6 mock packs matching the 6 compact summary domains."""
    from gas_calibrator.v2.core.reviewer_summary_packs import PACK_DEFAULT_PRIORITIES, PACK_DISPLAY_LABELS
    keys = ["measurement_digest", "readiness_digest", "phase_evidence",
            "v12_alignment", "governance_handoff", "parity_resilience"]
    packs = []
    for key in keys:
        packs.append({
            "summary_key": key,
            "display_label": PACK_DISPLAY_LABELS.get(key, key),
            "priority": PACK_DEFAULT_PRIORITIES.get(key, 99),
            "severity": "info",
            "summary_line": f"mock line for {key}",
            "summary_lines": [f"mock line 1 for {key}", f"mock line 2 for {key}"],
            "compact_summary_lines": [f"mock line 1 for {key}", f"mock line 2 for {key}"],
            "evidence_source": "simulated",
            "reviewer_only": True,
            "not_real_acceptance_evidence": True,
        })
    return packs


# ===========================================================================
# 1. build_visible_sections — single source of truth
# ===========================================================================

class TestBuildVisibleSections:
    def test_empty_packs_returns_empty(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        result = build_visible_sections([])
        assert result["rendered_summary_sections"] == []
        assert result["omitted_summary_sections"] == []
        assert result["compact_summary_budget_display"]["pack_count"] == 0

    def test_none_packs_returns_empty(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        result = build_visible_sections(None)
        assert result["rendered_summary_sections"] == []

    def test_with_packs_and_budget(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        packs = _make_mock_packs()
        budget = {"used": 12, "budget": 40, "total_lines": 12, "truncated_count": 0, "pack_count": 6}
        result = build_visible_sections(packs, budget=budget)
        assert len(result["rendered_summary_sections"]) > 0
        assert "compact_summary_budget_display" in result

    def test_deterministic_ordering(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        packs = _make_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 12, "truncated_count": 0, "pack_count": 6}
        result1 = build_visible_sections(packs, budget=budget)
        result2 = build_visible_sections(packs, budget=budget)
        keys1 = [s["summary_key"] for s in result1["rendered_summary_sections"]]
        keys2 = [s["summary_key"] for s in result2["rendered_summary_sections"]]
        assert keys1 == keys2

    def test_tight_budget_produces_omitted(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        packs = _make_mock_packs()
        budget = {"used": 1, "budget": 1, "total_lines": 12, "truncated_count": 11, "pack_count": 6}
        result = build_visible_sections(packs, budget=budget)
        assert len(result["omitted_summary_sections"]) > 0

    def test_rendered_section_fields(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        packs = _make_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 12, "truncated_count": 0, "pack_count": 6}
        result = build_visible_sections(packs, budget=budget)
        for section in result["rendered_summary_sections"]:
            assert "summary_key" in section
            assert "display_label" in section
            assert "summary_line" in section
            assert "summary_lines" in section
            assert "priority" in section
            assert "severity" in section
            assert "truncated" in section
            assert "truncated_count" in section

    def test_omitted_section_fields(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        packs = _make_mock_packs()
        budget = {"used": 1, "budget": 1, "total_lines": 12, "truncated_count": 11, "pack_count": 6}
        result = build_visible_sections(packs, budget=budget)
        for section in result["omitted_summary_sections"]:
            assert "summary_key" in section
            assert "display_label" in section
            assert "priority" in section
            assert "severity" in section
            assert "reason" in section

    def test_budget_display_fields(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        packs = _make_mock_packs()
        budget = {"used": 12, "budget": 40, "total_lines": 12, "truncated_count": 0, "pack_count": 6}
        result = build_visible_sections(packs, budget=budget)
        bd = result["compact_summary_budget_display"]
        assert "used" in bd
        assert "budget" in bd
        assert "total_lines" in bd
        assert "truncated_count" in bd
        assert "pack_count" in bd

    def test_budget_exhausted_reason(self):
        """When budget is 0 and pack has no lines, reason should be budget_exhausted."""
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        packs = [{"summary_key": "empty_pack", "display_label": "Empty", "priority": 10,
                  "severity": "info", "summary_line": "", "summary_lines": [],
                  "compact_summary_lines": []}]
        budget = {"used": 0, "budget": 0, "total_lines": 0, "truncated_count": 0, "pack_count": 1}
        result = build_visible_sections(packs, budget=budget)
        # Pack with no lines and budget exhausted -> budget_exhausted
        if result["omitted_summary_sections"]:
            assert result["omitted_summary_sections"][0]["reason"] == "budget_exhausted"


# ===========================================================================
# 2. Convenience wrappers (app_facade compat)
# ===========================================================================

class TestConvenienceWrappers:
    def test_build_rendered_sections(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_rendered_sections
        packs = _make_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 12, "truncated_count": 0, "pack_count": 6}
        rendered = build_rendered_sections(packs, budget)
        assert len(rendered) > 0

    def test_build_omitted_sections(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_omitted_sections
        packs = _make_mock_packs()
        budget = {"used": 1, "budget": 1, "total_lines": 12, "truncated_count": 11, "pack_count": 6}
        omitted = build_omitted_sections(packs, budget)
        assert len(omitted) > 0

    def test_build_budget_display(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_budget_display
        budget = {"used": 30, "budget": 40, "total_lines": 35, "truncated_count": 5, "pack_count": 6}
        display = build_budget_display(budget)
        assert display["used"] == 30
        assert display["budget"] == 40
        assert display["pack_count"] == 6

    def test_build_rendered_sections_empty(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_rendered_sections
        assert build_rendered_sections([], {}) == []

    def test_build_omitted_sections_empty(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_omitted_sections
        assert build_omitted_sections([], {}) == []


# ===========================================================================
# 3. build_compact_summary_pack_fields
# ===========================================================================

class TestBuildPackFields:
    def test_with_packs(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_pack_fields
        packs = _make_mock_packs()
        result = build_compact_summary_pack_fields(packs, surface="review_center")
        assert len(result["compact_summary_packs"]) == 6
        assert len(result["compact_summary_order"]) == 6
        assert "compact_summary_budget" in result

    def test_empty_packs(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_pack_fields
        result = build_compact_summary_pack_fields([], surface="review_center")
        assert result["compact_summary_packs"] == []
        assert result["compact_summary_order"] == []

    def test_none_packs(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_pack_fields
        result = build_compact_summary_pack_fields(None, surface="review_center")
        assert result["compact_summary_packs"] == []

    def test_different_surfaces(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_pack_fields
        packs = _make_mock_packs()
        rc = build_compact_summary_pack_fields(packs, surface="review_center")
        hist = build_compact_summary_pack_fields(packs, surface="historical")
        rg = build_compact_summary_pack_fields(packs, surface="results_gateway")
        # Same packs, different budgets
        assert rc["compact_summary_budget"]["budget"] == 40
        assert hist["compact_summary_budget"]["budget"] == 32
        assert rg["compact_summary_budget"]["budget"] == 24


# ===========================================================================
# 4. build_full_compact_summary_view
# ===========================================================================

class TestBuildFullView:
    def test_combines_fields_and_visible(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_full_compact_summary_view
        packs = _make_mock_packs()
        result = build_full_compact_summary_view(packs, surface="review_center")
        assert "compact_summary_packs" in result
        assert "compact_summary_order" in result
        assert "compact_summary_budget" in result
        assert "rendered_summary_sections" in result
        assert "omitted_summary_sections" in result
        assert "compact_summary_budget_display" in result

    def test_empty_packs(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_full_compact_summary_view
        result = build_full_compact_summary_view([], surface="review_center")
        assert result["compact_summary_packs"] == []
        assert result["rendered_summary_sections"] == []


# ===========================================================================
# 5. build_old_run_fallback
# ===========================================================================

class TestOldRunFallback:
    def test_fallback_has_all_fields(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_old_run_fallback
        fallback = build_old_run_fallback()
        assert "compact_summary_packs" in fallback
        assert "compact_summary_sections" in fallback
        assert "compact_summary_order" in fallback
        assert "compact_summary_budget" in fallback
        assert "rendered_summary_sections" in fallback
        assert "omitted_summary_sections" in fallback
        assert "compact_summary_budget_display" in fallback

    def test_fallback_values_are_empty_or_zero(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_old_run_fallback
        fallback = build_old_run_fallback()
        assert fallback["compact_summary_packs"] == []
        assert fallback["compact_summary_order"] == []
        assert fallback["rendered_summary_sections"] == []
        assert fallback["omitted_summary_sections"] == []
        assert fallback["compact_summary_budget_display"]["used"] == 0
        assert fallback["compact_summary_budget_display"]["budget"] == 0
        assert fallback["compact_summary_budget_display"]["pack_count"] == 0

    def test_fallback_is_deterministic(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_old_run_fallback
        f1 = build_old_run_fallback()
        f2 = build_old_run_fallback()
        assert f1 == f2


# ===========================================================================
# 6. Cross-surface consistency
# ===========================================================================

class TestCrossSurfaceConsistency:
    def test_rendered_sections_consistent_across_surfaces(self):
        """Same packs produce consistent rendered section structure across surfaces."""
        from gas_calibrator.v2.core.compact_summary_rendering import (
            build_visible_sections, build_compact_summary_pack_fields,
        )
        packs = _make_mock_packs()
        for surface in ["review_center", "historical", "results_gateway"]:
            fields = build_compact_summary_pack_fields(packs, surface=surface)
            result = build_visible_sections(
                fields["compact_summary_packs"],
                budget=fields["compact_summary_budget"],
            )
            # All surfaces produce the same field structure
            assert "rendered_summary_sections" in result
            assert "omitted_summary_sections" in result
            assert "compact_summary_budget_display" in result
            for section in result["rendered_summary_sections"]:
                assert "summary_key" in section
                assert "display_label" in section
                assert "summary_lines" in section

    def test_budget_display_consistent_structure(self):
        from gas_calibrator.v2.core.compact_summary_rendering import (
            build_visible_sections, build_compact_summary_pack_fields,
        )
        packs = _make_mock_packs()
        for surface in ["review_center", "historical", "results_gateway"]:
            fields = build_compact_summary_pack_fields(packs, surface=surface)
            result = build_visible_sections(
                fields["compact_summary_packs"],
                budget=fields["compact_summary_budget"],
            )
            bd = result["compact_summary_budget_display"]
            assert set(bd.keys()) == {"used", "budget", "total_lines", "truncated_count", "pack_count"}


# ===========================================================================
# 7. App facade and review_center_artifact_scope share same helper
# ===========================================================================

class TestSharedHelperConsumption:
    def test_app_facade_uses_shared_helper(self):
        """app_facade._build_rendered_sections delegates to shared helper."""
        from gas_calibrator.v2.ui_v2.controllers.app_facade import (
            _build_rendered_sections, _build_omitted_sections, _build_budget_display,
        )
        from gas_calibrator.v2.core.compact_summary_rendering import (
            build_rendered_sections, build_omitted_sections, build_budget_display,
        )
        packs = _make_mock_packs()
        budget = {"used": 12, "budget": 40, "total_lines": 12, "truncated_count": 0, "pack_count": 6}
        # app_facade wrappers should produce same result as shared helpers
        assert _build_rendered_sections(packs, budget) == build_rendered_sections(packs, budget)
        assert _build_omitted_sections(packs, budget) == build_omitted_sections(packs, budget)
        assert _build_budget_display(budget) == build_budget_display(budget)

    def test_review_center_uses_shared_helper(self):
        """review_center_artifact_scope._build_compact_summary_pack_visible_sections
        delegates to shared helper."""
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import (
            _build_compact_summary_pack_visible_sections,
            _build_compact_summary_pack_fields,
        )
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        packs = _make_mock_packs()
        fields = _build_compact_summary_pack_fields(packs)
        local_result = _build_compact_summary_pack_visible_sections(fields)
        shared_result = build_visible_sections(
            fields["compact_summary_packs"],
            budget=fields["compact_summary_budget"],
        )
        assert local_result["rendered_summary_sections"] == shared_result["rendered_summary_sections"]
        assert local_result["omitted_summary_sections"] == shared_result["omitted_summary_sections"]
        assert local_result["compact_summary_budget_display"] == shared_result["compact_summary_budget_display"]


# ===========================================================================
# 8. Step 2 boundary assertions
# ===========================================================================

class TestStep2Boundary:
    def test_no_real_acceptance_language_in_rendering(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        packs = _make_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 12, "truncated_count": 0, "pack_count": 6}
        result = build_visible_sections(packs, budget=budget)
        # Check no formal acceptance / real acceptance language in output
        for section in result["rendered_summary_sections"]:
            for line in section.get("summary_lines", []):
                assert "real_acceptance" not in str(line).lower()
                assert "formal_claim" not in str(line).lower()

    def test_fallback_no_real_acceptance(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_old_run_fallback
        fallback = build_old_run_fallback()
        # Fallback should not contain any real acceptance evidence
        assert fallback["compact_summary_packs"] == []
        assert fallback["rendered_summary_sections"] == []

    def test_version_is_216(self):
        from gas_calibrator.v2.core.compact_summary_rendering import COMPACT_SUMMARY_RENDERING_VERSION
        assert COMPACT_SUMMARY_RENDERING_VERSION.startswith("2.16")
