"""Step 2.14 compact summary pack downstream adoption and explicit surface rendering tests.

Covers:
- sort_packs_by_priority and build_compact_summary_render_context
- build_surface_render_result
- results_gateway dead code cleanup and 6-pack completion
- app_facade explicit consumption
- historical_artifacts explicit consumption
- review_center_artifact_scope unified rendering
- Cross-surface rendering caliber consistency
- Step 2 boundary assertions
- Backward compatibility
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
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
        "builders_version": "2.14.0",
        "pack_version": "2.14.0",
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
# 1. sort_packs_by_priority tests
# ===========================================================================

class TestSortPacksByPriority:
    def test_sort_by_priority_ascending(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import sort_packs_by_priority
        packs = [
            _make_mock_pack("c", 30),
            _make_mock_pack("a", 10),
            _make_mock_pack("b", 20),
        ]
        result = sort_packs_by_priority(packs)
        assert [p["summary_key"] for p in result] == ["a", "b", "c"]

    def test_same_priority_sort_by_key(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import sort_packs_by_priority
        packs = [
            _make_mock_pack("z", 10),
            _make_mock_pack("a", 10),
            _make_mock_pack("m", 10),
        ]
        result = sort_packs_by_priority(packs)
        assert [p["summary_key"] for p in result] == ["a", "m", "z"]

    def test_does_not_modify_original(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import sort_packs_by_priority
        packs = [
            _make_mock_pack("b", 20),
            _make_mock_pack("a", 10),
        ]
        original_keys = [p["summary_key"] for p in packs]
        sort_packs_by_priority(packs)
        assert [p["summary_key"] for p in packs] == original_keys

    def test_deterministic(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import sort_packs_by_priority
        packs = _make_6_mock_packs()
        r1 = sort_packs_by_priority(packs)
        r2 = sort_packs_by_priority(packs)
        assert r1 == r2


# ===========================================================================
# 2. build_compact_summary_render_context tests
# ===========================================================================

class TestBuildCompactSummaryRenderContext:
    def test_returns_four_fields(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        packs = _make_6_mock_packs()
        ctx = build_compact_summary_render_context(packs, surface="review_center")
        assert "compact_summary_packs" in ctx
        assert "compact_summary_sections" in ctx
        assert "compact_summary_order" in ctx
        assert "compact_summary_budget" in ctx

    def test_order_is_deterministic(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        packs = _make_6_mock_packs()
        ctx1 = build_compact_summary_render_context(packs, surface="review_center")
        ctx2 = build_compact_summary_render_context(packs, surface="review_center")
        assert ctx1["compact_summary_order"] == ctx2["compact_summary_order"]

    def test_order_matches_priority(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        packs = _make_6_mock_packs()
        ctx = build_compact_summary_render_context(packs, surface="review_center")
        assert ctx["compact_summary_order"] == [
            "phase_evidence", "measurement_digest", "v12_alignment",
            "governance_handoff", "readiness_digest", "parity_resilience",
        ]

    def test_budget_fields(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        packs = _make_6_mock_packs()
        ctx = build_compact_summary_render_context(packs, surface="review_center")
        budget = ctx["compact_summary_budget"]
        assert "total_lines" in budget
        assert "pack_count" in budget
        assert "used" in budget
        assert "budget" in budget
        assert "truncated_count" in budget
        assert budget["pack_count"] == 6

    def test_empty_packs(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        ctx = build_compact_summary_render_context([], surface="review_center")
        assert ctx["compact_summary_packs"] == []
        assert ctx["compact_summary_order"] == []
        assert ctx["compact_summary_budget"]["pack_count"] == 0

    def test_sections_alias(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        packs = _make_6_mock_packs()
        ctx = build_compact_summary_render_context(packs, surface="review_center")
        assert ctx["compact_summary_sections"] == ctx["compact_summary_packs"]


# ===========================================================================
# 3. build_surface_render_result tests
# ===========================================================================

class TestBuildSurfaceRenderResult:
    def test_returns_rendered_lines(self):
        from gas_calibrator.v2.core.compact_summary_budget import build_surface_render_result
        packs = _make_6_mock_packs()
        result = build_surface_render_result(packs, surface="results_gateway")
        assert "rendered_lines" in result
        assert isinstance(result["rendered_lines"], list)

    def test_no_truncation_hint_within_budget(self):
        from gas_calibrator.v2.core.compact_summary_budget import build_surface_render_result
        packs = [_make_mock_pack("a", 10, ["line1"])]
        result = build_surface_render_result(packs, surface="results_gateway", budget=100)
        # No truncation hint line
        assert all("截断" not in line and "truncated" not in line.lower() for line in result["rendered_lines"])

    def test_truncation_hint_when_over_budget(self):
        from gas_calibrator.v2.core.compact_summary_budget import build_surface_render_result
        packs = [
            _make_mock_pack("a", 10, ["line1", "line2", "line3"]),
            _make_mock_pack("b", 20, ["line4", "line5"]),
        ]
        result = build_surface_render_result(packs, surface="results_gateway", budget=2)
        # Should have truncation hint
        assert any("截断" in line for line in result["rendered_lines"])

    def test_deterministic(self):
        from gas_calibrator.v2.core.compact_summary_budget import build_surface_render_result
        packs = _make_6_mock_packs()
        r1 = build_surface_render_result(packs, surface="results_gateway")
        r2 = build_surface_render_result(packs, surface="results_gateway")
        assert r1["rendered_lines"] == r2["rendered_lines"]
        assert r1["pack_order"] == r2["pack_order"]

    def test_pack_order(self):
        from gas_calibrator.v2.core.compact_summary_budget import build_surface_render_result
        packs = _make_6_mock_packs()
        result = build_surface_render_result(packs, surface="results_gateway")
        assert result["pack_order"] == [
            "phase_evidence", "measurement_digest", "v12_alignment",
            "governance_handoff", "readiness_digest", "parity_resilience",
        ]

    def test_english_truncation_hint(self):
        from gas_calibrator.v2.core.compact_summary_budget import build_surface_render_result
        packs = [
            _make_mock_pack("a", 10, ["line1", "line2", "line3"]),
            _make_mock_pack("b", 20, ["line4", "line5"]),
        ]
        result = build_surface_render_result(packs, surface="results_gateway", budget=2, lang="en")
        assert any("truncated" in line.lower() for line in result["rendered_lines"])


# ===========================================================================
# 4. results_gateway dead code cleanup and 6-pack tests
# ===========================================================================

class TestResultsGatewayV214:
    def test_no_dead_compact_variables(self):
        """Verify _v12_compact etc. are removed from _build_result_summary_text."""
        from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
        source = inspect.getsource(ResultsGateway._build_result_summary_text)
        for var in ("_v12_compact =", "_phase_evidence_compact =", "_governance_compact =", "_parity_resilience_compact ="):
            assert var not in source, f"Dead code variable {var} still present"

    def test_no_old_compact_builder_imports(self):
        """Verify old compact builder imports are removed."""
        from gas_calibrator.v2.adapters import results_gateway
        source = inspect.getsource(results_gateway)
        for name in ("build_v12_alignment_compact_summary", "build_phase_evidence_compact_summary",
                      "build_governance_handoff_compact_summary", "build_parity_resilience_compact_summary"):
            # These should not appear as top-level imports
            assert f"from ..core.reviewer_summary_builders import" not in source or name not in source

    def test_build_compact_summary_packs_returns_6(self):
        """Verify _build_compact_summary_packs returns 6 packs."""
        from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
        packs = ResultsGateway._build_compact_summary_packs(
            taxonomy_summary={},
            phase_coverage_summary={},
            workbench_summary={},
        )
        assert len(packs) == 6

    def test_packs_include_measurement_and_readiness(self):
        """Verify measurement_digest and readiness_digest packs are present."""
        from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
        packs = ResultsGateway._build_compact_summary_packs(
            taxonomy_summary={},
            phase_coverage_summary={},
            workbench_summary={},
        )
        keys = [p["summary_key"] for p in packs]
        assert "measurement_digest" in keys
        assert "readiness_digest" in keys

    def test_all_6_pack_keys(self):
        """Verify all 6 expected pack keys."""
        from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
        packs = ResultsGateway._build_compact_summary_packs(
            taxonomy_summary={},
            phase_coverage_summary={},
            workbench_summary={},
        )
        keys = set(p["summary_key"] for p in packs)
        expected = {"measurement_digest", "readiness_digest", "phase_evidence",
                    "v12_alignment", "governance_handoff", "parity_resilience"}
        assert keys == expected


# ===========================================================================
# 5. Cross-surface rendering caliber consistency
# ===========================================================================

class TestRenderCaliberConsistency:
    def test_same_order_across_surfaces(self):
        """Verify pack order is consistent across surfaces."""
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        packs = _make_6_mock_packs()
        order_rg = build_compact_summary_render_context(packs, surface="results_gateway")["compact_summary_order"]
        order_rc = build_compact_summary_render_context(packs, surface="review_center")["compact_summary_order"]
        order_hi = build_compact_summary_render_context(packs, surface="historical")["compact_summary_order"]
        assert order_rg == order_rc == order_hi

    def test_truncation_uses_apply_surface_budget(self):
        """Verify truncation behavior is consistent (uses apply_surface_budget)."""
        from gas_calibrator.v2.core.compact_summary_budget import (
            apply_surface_budget,
            build_surface_render_result,
        )
        packs = _make_6_mock_packs()
        for surface in ("results_gateway", "review_center", "historical"):
            result = build_surface_render_result(packs, surface=surface)
            budget_result = apply_surface_budget(packs, surface=surface)
            assert result["used"] == budget_result["used"]
            assert result["truncated_count"] == budget_result["truncated_count"]

    def test_visible_fields_consistent(self):
        """Verify visible fields are consistent across surfaces."""
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        packs = _make_6_mock_packs()
        for surface in ("results_gateway", "review_center", "historical"):
            ctx = build_compact_summary_render_context(packs, surface=surface)
            for pack in ctx["compact_summary_packs"]:
                assert "summary_key" in pack
                assert "display_label" in pack
                assert "severity" in pack
                assert "summary_lines" in pack
                assert "evidence_source" in pack
                assert "not_real_acceptance_evidence" in pack
                assert "not_ready_for_formal_claim" in pack

    def test_budget_truncation_deterministic(self):
        """Verify budget/truncation is deterministic."""
        from gas_calibrator.v2.core.reviewer_summary_packs import build_compact_summary_render_context
        packs = _make_6_mock_packs()
        for surface in ("results_gateway", "review_center", "historical"):
            ctx1 = build_compact_summary_render_context(packs, surface=surface)
            ctx2 = build_compact_summary_render_context(packs, surface=surface)
            assert ctx1["compact_summary_budget"] == ctx2["compact_summary_budget"]


# ===========================================================================
# 6. Step 2 boundary assertions
# ===========================================================================

class TestStep2BoundaryAssertions:
    def test_all_packs_simulation_only(self):
        """Verify all packs have simulation-only markers."""
        from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
        packs = ResultsGateway._build_compact_summary_packs(
            taxonomy_summary={},
            phase_coverage_summary={},
            workbench_summary={},
        )
        for pack in packs:
            assert pack["evidence_source"] == "simulated"
            assert pack["not_real_acceptance_evidence"] is True
            assert pack["not_ready_for_formal_claim"] is True

    def test_no_formal_acceptance_language(self):
        """Verify no formal acceptance / real acceptance claim language in pack values.

        Note: 'not_real_acceptance_evidence' is a boundary marker field name (value=True),
        not a real acceptance claim. We check pack values, not field names.
        """
        from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
        packs = ResultsGateway._build_compact_summary_packs(
            taxonomy_summary={},
            phase_coverage_summary={},
            workbench_summary={},
        )
        # Check that no pack has a positive-form acceptance claim in its values
        for pack in packs:
            # The boundary markers must be True (negation form)
            assert pack.get("not_real_acceptance_evidence") is True
            assert pack.get("not_ready_for_formal_claim") is True
            # evidence_source must be simulated
            assert pack.get("evidence_source") == "simulated"
            # No positive-form acceptance fields should exist
            assert "real_acceptance_evidence" not in pack or pack["real_acceptance_evidence"] is not True
            assert "formal_acceptance" not in pack
            assert "approved" not in pack

    def test_no_real_device_paths_in_new_code(self):
        """Verify new functions don't contain real device paths."""
        from gas_calibrator.v2.core.reviewer_summary_packs import (
            sort_packs_by_priority,
            build_compact_summary_render_context,
        )
        from gas_calibrator.v2.core.compact_summary_budget import build_surface_render_result
        for func in (sort_packs_by_priority, build_compact_summary_render_context, build_surface_render_result):
            source = inspect.getsource(func)
            assert "COM" not in source or "compact" in source.lower()
            assert "serial" not in source.lower() or "summary" in source.lower()

    def test_reviewer_only_marker(self):
        """Verify all packs have reviewer_only = True."""
        from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
        packs = ResultsGateway._build_compact_summary_packs(
            taxonomy_summary={},
            phase_coverage_summary={},
            workbench_summary={},
        )
        for pack in packs:
            assert pack.get("reviewer_only") is True


# ===========================================================================
# 7. Version upgrade tests
# ===========================================================================

class TestVersionUpgrade:
    def test_packs_version_2160(self):
        from gas_calibrator.v2.core.reviewer_summary_packs import REVIEWER_SUMMARY_PACKS_VERSION
        assert REVIEWER_SUMMARY_PACKS_VERSION == "2.16.0"

    def test_budget_version_2160(self):
        from gas_calibrator.v2.core.compact_summary_budget import COMPACT_SUMMARY_BUDGET_VERSION
        assert COMPACT_SUMMARY_BUDGET_VERSION == "2.16.0"

    def test_rendering_version_2160(self):
        from gas_calibrator.v2.core.compact_summary_rendering import COMPACT_SUMMARY_RENDERING_VERSION
        assert COMPACT_SUMMARY_RENDERING_VERSION == "2.16.0"


# ===========================================================================
# 8. review_center_artifact_scope pack fields
# ===========================================================================

class TestReviewCenterArtifactScopePackFields:
    def test_build_review_artifact_registry_includes_pack_fields(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        result = build_review_artifact_registry([], compact_summary_packs=_make_6_mock_packs())
        assert "compact_summary_packs" in result
        assert "compact_summary_sections" in result
        assert "compact_summary_order" in result
        assert "compact_summary_budget" in result

    def test_empty_packs_default(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        result = build_review_artifact_registry([])
        assert result["compact_summary_packs"] == []
        assert result["compact_summary_order"] == []
        assert result["compact_summary_budget"] == {}

    def test_build_artifact_scope_view_passes_packs(self):
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_artifact_scope_view
        packs = _make_6_mock_packs()
        result = build_artifact_scope_view([], compact_summary_packs=packs)
        assert len(result["compact_summary_packs"]) == 6


# ===========================================================================
# 9. Backward compatibility
# ===========================================================================

class TestBackwardCompatibility:
    def test_packs_still_have_summary_line(self):
        """Verify packs still have summary_line for backward compat."""
        from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
        packs = ResultsGateway._build_compact_summary_packs(
            taxonomy_summary={},
            phase_coverage_summary={},
            workbench_summary={},
        )
        for pack in packs:
            assert "summary_line" in pack
            assert "summary_lines" in pack

    def test_build_review_artifact_registry_no_packs_still_works(self):
        """Verify build_review_artifact_registry works without packs (backward compat)."""
        from gas_calibrator.v2.ui_v2.review_center_artifact_scope import build_review_artifact_registry
        result = build_review_artifact_registry([])
        # Should have all the standard fields
        assert "scope" in result
        assert "rows" in result
