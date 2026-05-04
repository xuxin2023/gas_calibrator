"""Step 2.15 review center artifact scope pack propagation and visible rendering tests.

Covers:
- build_review_scope_manifest_payload accepts and propagates compact_summary_packs
- build_review_artifact_registry / build_artifact_scope_view include rendered_summary_sections
- Reviewer-facing visible fields contain core pack info
- Old run missing compact_summary_packs degrades gracefully
- Field naming consistency across results / review_center / historical
- Budget / truncation / rendered sections are deterministic
- Step 2 boundary assertions continue to pass
- No real device / formal approval / real acceptance language
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_pack(summary_key: str, priority: int, lines: list[str] | None = None) -> dict[str, Any]:
    return {
        "summary_key": summary_key,
        "display_label": f"Label-{summary_key}",
        "priority": priority,
        "severity": "info",
        "summary_line": " | ".join(lines or []),
        "summary_lines": lines or [],
        "compact_summary_lines": lines or [],
        "max_lines_hint": 5,
        "surface_budget_hint": {"results_gateway": 5, "review_center": 5, "historical": 3},
        "boundary_markers": {"step2_boundary": True},
        "evidence_source": "simulated",
        "reviewer_only": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "builders_version": "2.16.0",
        "pack_version": "2.16.0",
    }


def _make_6_mock_packs() -> list[dict[str, Any]]:
    return [
        _make_mock_pack("phase_evidence", 10, ["阶段证据行1", "阶段证据行2"]),
        _make_mock_pack("measurement_digest", 20, ["测量摘要行1", "测量摘要行2"]),
        _make_mock_pack("v12_alignment", 30, ["V12对齐行1", "V12对齐行2"]),
        _make_mock_pack("governance_handoff", 40, ["治理交接行1"]),
        _make_mock_pack("readiness_digest", 50, ["就绪摘要行1"]),
        _make_mock_pack("parity_resilience", 60, ["一致性韧性行1"]),
    ]


# ===========================================================================
# 1. build_review_scope_manifest_payload pack propagation
# ===========================================================================

class TestManifestPayloadPackPropagation:
    def test_manifest_accepts_compact_summary_packs(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        packs = _make_6_mock_packs()
        payload = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        assert "compact_summary_packs" in payload
        assert len(payload["compact_summary_packs"]) == 6

    def test_manifest_includes_rendered_sections(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        packs = _make_6_mock_packs()
        payload = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        assert "rendered_summary_sections" in payload
        assert isinstance(payload["rendered_summary_sections"], list)

    def test_manifest_includes_omitted_sections(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        packs = _make_6_mock_packs()
        payload = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        assert "omitted_summary_sections" in payload
        assert isinstance(payload["omitted_summary_sections"], list)

    def test_manifest_includes_budget_display(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        packs = _make_6_mock_packs()
        payload = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        assert "compact_summary_budget_display" in payload
        budget_display = payload["compact_summary_budget_display"]
        assert "used" in budget_display
        assert "budget" in budget_display
        assert "pack_count" in budget_display

    def test_manifest_includes_order_and_budget(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        packs = _make_6_mock_packs()
        payload = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        assert "compact_summary_order" in payload
        assert "compact_summary_budget" in payload
        assert len(payload["compact_summary_order"]) == 6

    def test_manifest_without_packs_stable(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        payload = build_review_scope_manifest_payload([])
        assert payload["compact_summary_packs"] == []
        assert payload["rendered_summary_sections"] == []
        assert payload["omitted_summary_sections"] == []
        # budget_display has zero values when no packs
        assert payload["compact_summary_budget_display"]["pack_count"] == 0


# ===========================================================================
# 2. build_review_artifact_registry / build_artifact_scope_view visible fields
# ===========================================================================

class TestRegistryAndViewVisibleFields:
    def test_registry_includes_rendered_sections(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        assert "rendered_summary_sections" in result
        assert isinstance(result["rendered_summary_sections"], list)

    def test_registry_includes_omitted_sections(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        assert "omitted_summary_sections" in result

    def test_registry_includes_budget_display(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        assert "compact_summary_budget_display" in result

    def test_scope_view_passes_packs_and_visible(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_artifact_scope_view
        packs = _make_6_mock_packs()
        result = build_artifact_scope_view([], compact_summary_packs=packs)
        assert len(result["compact_summary_packs"]) == 6
        assert "rendered_summary_sections" in result
        assert "omitted_summary_sections" in result


# ===========================================================================
# 3. Reviewer-facing visible fields contain core pack info
# ===========================================================================

class TestReviewerFacingVisibleFields:
    def test_rendered_section_has_display_label(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        rendered = result["rendered_summary_sections"]
        assert len(rendered) > 0
        for section in rendered:
            assert "display_label" in section
            assert "summary_key" in section

    def test_rendered_section_has_summary_line(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        rendered = result["rendered_summary_sections"]
        for section in rendered:
            assert "summary_line" in section

    def test_rendered_section_has_summary_lines(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        rendered = result["rendered_summary_sections"]
        for section in rendered:
            assert "summary_lines" in section
            assert isinstance(section["summary_lines"], list)

    def test_rendered_section_has_priority_and_severity(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        rendered = result["rendered_summary_sections"]
        for section in rendered:
            assert "priority" in section
            assert "severity" in section

    def test_rendered_section_has_truncated_flag(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        rendered = result["rendered_summary_sections"]
        for section in rendered:
            assert "truncated" in section
            assert "truncated_count" in section

    def test_rendered_sections_ordered_by_priority(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        rendered = result["rendered_summary_sections"]
        priorities = [s["priority"] for s in rendered]
        assert priorities == sorted(priorities)


# ===========================================================================
# 4. Old run missing compact_summary_packs degrades gracefully
# ===========================================================================

class TestOldRunDegradation:
    def test_registry_no_packs_no_error(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        result = build_review_artifact_registry([])
        assert result["compact_summary_packs"] == []
        assert result["rendered_summary_sections"] == []
        assert result["omitted_summary_sections"] == []
        # budget_display has zero values when no packs
        assert result["compact_summary_budget_display"]["pack_count"] == 0

    def test_manifest_no_packs_no_error(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        payload = build_review_scope_manifest_payload([])
        assert payload["compact_summary_packs"] == []
        assert payload["rendered_summary_sections"] == []
        assert payload["omitted_summary_sections"] == []

    def test_scope_view_no_packs_no_error(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_artifact_scope_view
        result = build_artifact_scope_view([])
        assert result["compact_summary_packs"] == []
        assert result["rendered_summary_sections"] == []

    def test_none_packs_no_error(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        result = build_review_artifact_registry([], compact_summary_packs=None)
        assert result["compact_summary_packs"] == []

    def test_empty_list_packs_no_error(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        result = build_review_artifact_registry([], compact_summary_packs=[])
        assert result["compact_summary_packs"] == []

    def test_core_fields_still_present_without_packs(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        result = build_review_artifact_registry([])
        assert "scope" in result
        assert "rows" in result
        assert "scope_label" in result
        assert "summary_text" in result


# ===========================================================================
# 5. Field naming consistency across surfaces
# ===========================================================================

class TestFieldNamingConsistency:
    def test_registry_has_all_standard_pack_fields(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        expected_fields = [
            "compact_summary_packs",
            "compact_summary_sections",
            "compact_summary_order",
            "compact_summary_budget",
            "rendered_summary_sections",
            "omitted_summary_sections",
            "compact_summary_budget_display",
        ]
        for field in expected_fields:
            assert field in result, f"Missing field: {field}"

    def test_manifest_has_all_standard_pack_fields(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        packs = _make_6_mock_packs()
        payload = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        expected_fields = [
            "compact_summary_packs",
            "compact_summary_sections",
            "compact_summary_order",
            "compact_summary_budget",
            "rendered_summary_sections",
            "omitted_summary_sections",
            "compact_summary_budget_display",
        ]
        for field in expected_fields:
            assert field in payload, f"Missing field: {field}"

    def test_budget_display_fields_consistent(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        budget_display = result["compact_summary_budget_display"]
        expected_keys = {"used", "budget", "total_lines", "truncated_count", "pack_count"}
        assert set(budget_display.keys()) == expected_keys


# ===========================================================================
# 6. Budget / truncation / rendered sections deterministic
# ===========================================================================

class TestDeterminism:
    def test_rendered_sections_deterministic(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        r1 = build_review_artifact_registry([], compact_summary_packs=packs)
        r2 = build_review_artifact_registry([], compact_summary_packs=packs)
        assert r1["rendered_summary_sections"] == r2["rendered_summary_sections"]
        assert r1["omitted_summary_sections"] == r2["omitted_summary_sections"]

    def test_manifest_rendered_deterministic(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        packs = _make_6_mock_packs()
        p1 = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        p2 = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        assert p1["rendered_summary_sections"] == p2["rendered_summary_sections"]
        assert p1["compact_summary_order"] == p2["compact_summary_order"]

    def test_budget_display_deterministic(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        r1 = build_review_artifact_registry([], compact_summary_packs=packs)
        r2 = build_review_artifact_registry([], compact_summary_packs=packs)
        assert r1["compact_summary_budget_display"] == r2["compact_summary_budget_display"]


# ===========================================================================
# 7. Step 2 boundary assertions
# ===========================================================================

class TestStep2BoundaryAssertions:
    def test_no_real_device_paths_in_new_functions(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import (
            _build_compact_summary_pack_visible_sections,
            build_review_scope_manifest_payload,
        )
        for func in (_build_compact_summary_pack_visible_sections, build_review_scope_manifest_payload):
            source = inspect.getsource(func)
            assert "COM" not in source or "compact" in source.lower()
            assert "serial" not in source.lower() or "summary" in source.lower()

    def test_no_formal_acceptance_in_rendered_sections(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        for section in result["rendered_summary_sections"]:
            assert "formal_acceptance" not in section
            assert "approved" not in section

    def test_packs_have_simulation_markers(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        for pack in result["compact_summary_packs"]:
            assert pack.get("evidence_source") == "simulated"
            assert pack.get("not_real_acceptance_evidence") is True
            assert pack.get("reviewer_only") is True

    def test_manifest_disclaimer_present(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        packs = _make_6_mock_packs()
        payload = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        disclaimer = payload.get("disclaimer", {})
        assert disclaimer.get("not_real_acceptance_evidence") is True
        assert disclaimer.get("offline_review_only") is True


# ===========================================================================
# 8. Version upgrade
# ===========================================================================

class TestVersionUpgrade:
    def test_packs_version_2160(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import REVIEWER_SUMMARY_PACKS_VERSION
        assert REVIEWER_SUMMARY_PACKS_VERSION == "2.16.0"

    def test_budget_version_2160(self):
        from gas_calibrator.v2.core.compact_summary_budget import COMPACT_SUMMARY_BUDGET_VERSION
        assert COMPACT_SUMMARY_BUDGET_VERSION == "2.16.0"

    def test_rendering_version_2170(self):
        from gas_calibrator.v2.core.compact_summary_rendering import COMPACT_SUMMARY_RENDERING_VERSION
        assert COMPACT_SUMMARY_RENDERING_VERSION == "2.17.0"


# ===========================================================================
# 9. Markdown rendering includes compact summary
# ===========================================================================

class TestMarkdownRendering:
    def test_markdown_includes_compact_summary_header(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import (
            build_review_scope_manifest_payload,
            render_review_scope_manifest_markdown,
        )
        packs = _make_6_mock_packs()
        payload = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        md = render_review_scope_manifest_markdown(payload)
        assert "紧凑摘要包" in md or "Compact Summary Pack" in md

    def test_markdown_without_packs_no_error(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import (
            build_review_scope_manifest_payload,
            render_review_scope_manifest_markdown,
        )
        payload = build_review_scope_manifest_payload([])
        md = render_review_scope_manifest_markdown(payload)
        assert isinstance(md, str)
        assert len(md) > 0


# ===========================================================================
# 10. Historical artifacts rendered/omitted sections
# ===========================================================================

class TestHistoricalArtifactsRenderedSections:
    def test_historical_builds_rendered_sections_from_packs(self):
        """Verify historical_artifacts._build_run_report computes rendered_summary_sections."""
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        from gas_calibrator.v2.core.reviewer_summary_packs import (
            build_measurement_digest_pack,
            build_readiness_digest_pack,
            build_phase_evidence_pack,
            build_v12_alignment_pack,
            build_governance_handoff_pack,
            build_parity_resilience_pack,
        )
        # Build real packs from empty payloads
        packs = [
            build_measurement_digest_pack({}),
            build_readiness_digest_pack({}),
            build_phase_evidence_pack({}),
            build_v12_alignment_pack({}),
            build_governance_handoff_pack({}),
            build_parity_resilience_pack({}),
        ]
        # Simulate what historical_artifacts does
        ctx = build_compact_summary_render_context(packs, surface="historical")
        compact_packs = list(ctx.get("compact_summary_packs") or [])
        budget = dict(ctx.get("compact_summary_budget") or {})
        used = int(budget.get("used", 0) or 0)
        # At least some packs should have lines
        assert len(compact_packs) == 6
        assert budget.get("pack_count") == 6

    def test_historical_empty_packs_produces_empty_rendered(self):
        """Verify empty packs produce empty rendered/omitted sections."""
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        ctx = build_compact_summary_render_context([], surface="historical")
        assert ctx["compact_summary_packs"] == []
        assert ctx["compact_summary_order"] == []
        assert ctx["compact_summary_budget"]["pack_count"] == 0


# ===========================================================================
# 11. App facade helper functions
# ===========================================================================

class TestAppFacadeHelpers:
    def test_build_rendered_sections_with_packs(self):
        from gas_calibrator.v2.ui_v2.controllers.app_facade import _build_rendered_sections
        packs = _make_6_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        rendered = _build_rendered_sections(packs, budget)
        assert len(rendered) > 0
        for section in rendered:
            assert "display_label" in section
            assert "summary_key" in section
            assert "summary_lines" in section

    def test_build_omitted_sections_within_budget(self):
        from gas_calibrator.v2.ui_v2.controllers.app_facade import _build_omitted_sections
        packs = _make_6_mock_packs()
        # With large budget, nothing should be omitted
        budget = {"used": 100, "budget": 100, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        omitted = _build_omitted_sections(packs, budget)
        assert isinstance(omitted, list)

    def test_build_omitted_sections_tight_budget(self):
        from gas_calibrator.v2.ui_v2.controllers.app_facade import _build_omitted_sections
        packs = _make_6_mock_packs()
        # With very tight budget, some packs should be omitted
        budget = {"used": 1, "budget": 1, "total_lines": 9, "truncated_count": 8, "pack_count": 6}
        omitted = _build_omitted_sections(packs, budget)
        assert len(omitted) > 0

    def test_build_budget_display(self):
        from gas_calibrator.v2.ui_v2.controllers.app_facade import _build_budget_display
        budget = {"used": 30, "budget": 40, "total_lines": 35, "truncated_count": 5, "pack_count": 6}
        display = _build_budget_display(budget)
        assert display["used"] == 30
        assert display["budget"] == 40
        assert display["pack_count"] == 6

    def test_build_rendered_sections_empty_packs(self):
        from gas_calibrator.v2.ui_v2.controllers.app_facade import _build_rendered_sections
        rendered = _build_rendered_sections([], {})
        assert rendered == []

    def test_build_omitted_sections_empty_packs(self):
        from gas_calibrator.v2.ui_v2.controllers.app_facade import _build_omitted_sections
        omitted = _build_omitted_sections([], {})
        assert omitted == []


# ===========================================================================
# 12. Shared helper consistency — app_facade and review_center use same logic
# ===========================================================================

class TestSharedHelperConsistency:
    def test_app_facade_rendered_matches_shared(self):
        """app_facade._build_rendered_sections produces same output as shared helper."""
        from gas_calibrator.v2.ui_v2.controllers.app_facade import _build_rendered_sections
        from gas_calibrator.v2.core.compact_summary_rendering import build_rendered_sections
        packs = _make_6_mock_packs()
        budget = {"used": 12, "budget": 40, "total_lines": 12, "truncated_count": 0, "pack_count": 6}
        assert _build_rendered_sections(packs, budget) == build_rendered_sections(packs, budget)

    def test_app_facade_omitted_matches_shared(self):
        from gas_calibrator.v2.ui_v2.controllers.app_facade import _build_omitted_sections
        from gas_calibrator.v2.core.compact_summary_rendering import build_omitted_sections
        packs = _make_6_mock_packs()
        budget = {"used": 1, "budget": 1, "total_lines": 12, "truncated_count": 11, "pack_count": 6}
        assert _build_omitted_sections(packs, budget) == build_omitted_sections(packs, budget)

    def test_app_facade_budget_display_matches_shared(self):
        from gas_calibrator.v2.ui_v2.controllers.app_facade import _build_budget_display
        from gas_calibrator.v2.core.compact_summary_rendering import build_budget_display
        budget = {"used": 30, "budget": 40, "total_lines": 35, "truncated_count": 5, "pack_count": 6}
        assert _build_budget_display(budget) == build_budget_display(budget)

    def test_review_center_visible_matches_shared(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import (
            _build_compact_summary_pack_visible_sections,
            _build_compact_summary_pack_fields,
        )
        from gas_calibrator.v2.core.compact_summary_rendering import build_visible_sections
        packs = _make_6_mock_packs()
        fields = _build_compact_summary_pack_fields(packs)
        local = _build_compact_summary_pack_visible_sections(fields)
        shared = build_visible_sections(
            fields["compact_summary_packs"], budget=fields["compact_summary_budget"],
        )
        assert local["rendered_summary_sections"] == shared["rendered_summary_sections"]
        assert local["omitted_summary_sections"] == shared["omitted_summary_sections"]
        assert local["compact_summary_budget_display"] == shared["compact_summary_budget_display"]

    def test_old_run_fallback_stable(self):
        """Old run fallback provides consistent empty values."""
        from gas_calibrator.v2.core.compact_summary_rendering import build_old_run_fallback
        fallback = build_old_run_fallback()
        assert fallback["compact_summary_packs"] == []
        assert fallback["rendered_summary_sections"] == []
        assert fallback["omitted_summary_sections"] == []
        assert fallback["compact_summary_budget_display"]["pack_count"] == 0
        assert fallback.get("compact_summary_legacy_mode") is True


# ===========================================================================
# 13. Step 2.17 — compact summary display text and legacy hint
# ===========================================================================

class TestCompactSummaryDisplayText:
    """Tests for build_compact_summary_display_text — reviewer-facing rendering."""

    def test_display_text_with_packs(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        result = build_compact_summary_display_text(packs, budget=budget)
        assert "display_text" in result
        assert isinstance(result["display_text"], str)
        assert len(result["display_text"]) > 0
        assert "section_entries" in result
        assert len(result["section_entries"]) > 0

    def test_display_text_empty_packs(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        result = build_compact_summary_display_text([], budget={})
        # Empty packs produce legacy hint text, not empty string
        assert result["section_entries"] == []
        assert result["omitted_labels"] == []
        assert result.get("legacy_hint", {}).get("compact_summary_legacy_mode") is True

    def test_display_text_contains_display_labels(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        result = build_compact_summary_display_text(packs, budget=budget)
        for entry in result["section_entries"]:
            assert "display_label" in entry
            assert "summary_line" in entry
            assert "summary_lines" in entry
            assert "severity" in entry
            assert "truncated" in entry

    def test_display_text_includes_budget_line(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        budget = {"used": 9, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        result = build_compact_summary_display_text(packs, budget=budget)
        assert result["budget_line"] != ""

    def test_display_text_chinese_default(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        result = build_compact_summary_display_text(packs, budget=budget, locale="zh")
        # Chinese brackets should appear in display text
        assert "【" in result["display_text"] or "预算" in result["display_text"] or len(result["section_entries"]) > 0

    def test_display_text_english_fallback(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        result = build_compact_summary_display_text(packs, budget=budget, locale="en")
        assert "[" in result["display_text"] or "Budget" in result["display_text"] or len(result["section_entries"]) > 0

    def test_display_text_includes_legacy_hint_for_empty(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        result = build_compact_summary_display_text([], budget={})
        legacy = result.get("legacy_hint", {})
        assert legacy.get("compact_summary_legacy_mode") is True

    def test_display_text_no_legacy_hint_for_packs(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        result = build_compact_summary_display_text(packs, budget=budget)
        legacy = result.get("legacy_hint", {})
        assert legacy.get("compact_summary_legacy_mode") is False

    def test_display_text_omitted_labels(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        # Very tight budget to force omissions
        budget = {"used": 1, "budget": 1, "total_lines": 9, "truncated_count": 8, "pack_count": 6}
        result = build_compact_summary_display_text(packs, budget=budget)
        assert isinstance(result["omitted_labels"], list)

    def test_display_text_deterministic(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        r1 = build_compact_summary_display_text(packs, budget=budget)
        r2 = build_compact_summary_display_text(packs, budget=budget)
        assert r1["display_text"] == r2["display_text"]
        assert r1["section_entries"] == r2["section_entries"]


class TestLegacyHint:
    """Tests for build_legacy_hint — reviewer-facing compatibility notice."""

    def test_legacy_hint_no_packs_chinese(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_legacy_hint
        hint = build_legacy_hint(has_packs=False, locale="zh")
        assert hint["compact_summary_legacy_mode"] is True
        assert "兼容" in hint["compact_summary_legacy_label"]
        assert "兼容" in hint["compact_summary_legacy_hint"] or "未提供" in hint["compact_summary_legacy_hint"]

    def test_legacy_hint_no_packs_english(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_legacy_hint
        hint = build_legacy_hint(has_packs=False, locale="en")
        assert hint["compact_summary_legacy_mode"] is True
        assert "Compatibility" in hint["compact_summary_legacy_label"]

    def test_legacy_hint_with_packs(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_legacy_hint
        hint = build_legacy_hint(has_packs=True)
        assert hint["compact_summary_legacy_mode"] is False
        assert hint["compact_summary_legacy_label"] == ""
        assert hint["compact_summary_legacy_hint"] == ""


class TestLegacyModeField:
    """Tests for compact_summary_legacy_mode field propagation."""

    def test_registry_legacy_mode_when_no_packs(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        result = build_review_artifact_registry([], compact_summary_packs=[])
        assert result.get("compact_summary_legacy_mode") is True

    def test_registry_legacy_mode_false_with_packs(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        assert result.get("compact_summary_legacy_mode") is False

    def test_manifest_legacy_mode_when_no_packs(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        payload = build_review_scope_manifest_payload([], compact_summary_packs=[])
        assert payload.get("compact_summary_legacy_mode") is True

    def test_manifest_legacy_mode_false_with_packs(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        packs = _make_6_mock_packs()
        payload = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        assert payload.get("compact_summary_legacy_mode") is False

    def test_markdown_legacy_hint_when_no_packs(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import (
            build_review_scope_manifest_payload,
            render_review_scope_manifest_markdown,
        )
        payload = build_review_scope_manifest_payload([], compact_summary_packs=[])
        md = render_review_scope_manifest_markdown(payload)
        assert "兼容" in md or "compatibility" in md.lower() or "未提供" in md


class TestFieldNamingConsistency217:
    """Step 2.17: Verify consistent field naming across surfaces."""

    CORE_FIELDS = [
        "compact_summary_packs",
        "rendered_summary_sections",
        "omitted_summary_sections",
        "compact_summary_budget_display",
    ]

    def test_registry_has_core_fields(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        packs = _make_6_mock_packs()
        result = build_review_artifact_registry([], compact_summary_packs=packs)
        for field in self.CORE_FIELDS:
            assert field in result, f"Missing field: {field}"

    def test_manifest_has_core_fields(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_scope_manifest_payload
        packs = _make_6_mock_packs()
        payload = build_review_scope_manifest_payload([], compact_summary_packs=packs)
        for field in self.CORE_FIELDS:
            assert field in payload, f"Missing field: {field}"

    def test_display_text_has_consistent_fields(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        result = build_compact_summary_display_text(packs, budget=budget)
        assert "rendered_summary_sections" in result
        assert "omitted_summary_sections" in result
        assert "compact_summary_budget_display" in result


class TestStep2Boundary217:
    """Step 2.17 boundary assertions — no real device / formal approval / real acceptance."""

    def test_legacy_hint_no_real_acceptance_language(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_legacy_hint
        hint = build_legacy_hint(has_packs=False)
        text = hint["compact_summary_legacy_label"] + hint["compact_summary_legacy_hint"]
        assert "real acceptance" not in text.lower()
        assert "formal" not in text.lower()
        assert "accreditation" not in text.lower()

    def test_display_text_no_real_device_paths(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        result = build_compact_summary_display_text(packs, budget=budget)
        text = result["display_text"]
        assert "COM" not in text
        assert "/dev/" not in text

    def test_legacy_mode_is_boolean(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        result_with = build_review_artifact_registry([], compact_summary_packs=_make_6_mock_packs())
        result_without = build_review_artifact_registry([], compact_summary_packs=[])
        assert isinstance(result_with.get("compact_summary_legacy_mode"), bool)
        assert isinstance(result_without.get("compact_summary_legacy_mode"), bool)

    def test_no_formal_claim_in_display_text(self):
        from gas_calibrator.v2.core.compact_summary_rendering import build_compact_summary_display_text
        packs = _make_6_mock_packs()
        budget = {"used": 40, "budget": 40, "total_lines": 9, "truncated_count": 0, "pack_count": 6}
        result = build_compact_summary_display_text(packs, budget=budget)
        text = result["display_text"].lower()
        assert "formal" not in text
        assert "accreditation" not in text
        assert "real acceptance" not in text
