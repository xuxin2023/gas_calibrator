"""Tests for compact_summary_budget — surface-aware line budget, truncation, and ordering.

Covers:
- Budget version
- Surface default budgets
- get_surface_budget
- apply_surface_budget: no truncation when within budget
- apply_surface_budget: truncation when exceeding budget
- apply_surface_budget: must_retain / optional_expand / truncated classification
- apply_surface_budget: first pack lines are must_retain
- apply_surface_budget: deterministic (same input -> same output)
- apply_surface_budget: same priority packs sorted by summary_key
- build_truncation_hint_line: Chinese default
- build_truncation_hint_line: English fallback
- Step 2 boundary: no formal acceptance language
"""

from __future__ import annotations

import pytest

from gas_calibrator.v2.core.compact_summary_budget import (
    COMPACT_SUMMARY_BUDGET_VERSION,
    SURFACE_DEFAULT_BUDGETS,
    TRUNCATION_LABELS,
    TRUNCATION_LABELS_EN,
    get_surface_budget,
    apply_surface_budget,
    build_truncation_hint_line,
)


# ---------------------------------------------------------------------------
# TestBudgetVersion
# ---------------------------------------------------------------------------

class TestBudgetVersion:
    def test_version_is_2_13_0(self):
        assert COMPACT_SUMMARY_BUDGET_VERSION == "2.13.0"


# ---------------------------------------------------------------------------
# TestSurfaceDefaultBudgets
# ---------------------------------------------------------------------------

class TestSurfaceDefaultBudgets:
    def test_contains_three_surfaces(self):
        assert "results_gateway" in SURFACE_DEFAULT_BUDGETS
        assert "review_center" in SURFACE_DEFAULT_BUDGETS
        assert "historical" in SURFACE_DEFAULT_BUDGETS

    def test_values_are_positive(self):
        for surface, budget in SURFACE_DEFAULT_BUDGETS.items():
            assert budget > 0, f"Budget for {surface} must be positive"

    def test_results_gateway_budget(self):
        assert SURFACE_DEFAULT_BUDGETS["results_gateway"] == 24

    def test_review_center_budget(self):
        assert SURFACE_DEFAULT_BUDGETS["review_center"] == 40

    def test_historical_budget(self):
        assert SURFACE_DEFAULT_BUDGETS["historical"] == 32


# ---------------------------------------------------------------------------
# TestGetSurfaceBudget
# ---------------------------------------------------------------------------

class TestGetSurfaceBudget:
    def test_known_surface(self):
        assert get_surface_budget("results_gateway") == 24
        assert get_surface_budget("review_center") == 40
        assert get_surface_budget("historical") == 32

    def test_unknown_surface_returns_zero(self):
        assert get_surface_budget("unknown_surface") == 0


# ---------------------------------------------------------------------------
# TestApplySurfaceBudgetNoTruncation
# ---------------------------------------------------------------------------

class TestApplySurfaceBudgetNoTruncation:
    def test_within_budget_no_truncation(self):
        packs = [
            {"summary_key": "a", "priority": 10, "summary_lines": ["line1", "line2"]},
            {"summary_key": "b", "priority": 20, "summary_lines": ["line3"]},
        ]
        result = apply_surface_budget(packs, surface="results_gateway", budget=10)
        assert result["truncated_count"] == 0
        assert len(result["must_retain"]) + len(result["optional_expand"]) == 3
        assert result["truncated"] == []

    def test_exact_budget_no_truncation(self):
        packs = [
            {"summary_key": "a", "priority": 10, "summary_lines": ["line1", "line2"]},
            {"summary_key": "b", "priority": 20, "summary_lines": ["line3"]},
        ]
        result = apply_surface_budget(packs, surface="results_gateway", budget=3)
        assert result["truncated_count"] == 0
        assert result["used"] == 3


# ---------------------------------------------------------------------------
# TestApplySurfaceBudgetWithTruncation
# ---------------------------------------------------------------------------

class TestApplySurfaceBudgetWithTruncation:
    def test_exceeding_budget_truncates(self):
        packs = [
            {"summary_key": "a", "priority": 10, "summary_lines": ["line1", "line2"]},
            {"summary_key": "b", "priority": 20, "summary_lines": ["line3", "line4", "line5"]},
        ]
        result = apply_surface_budget(packs, surface="results_gateway", budget=3)
        assert result["truncated_count"] > 0
        assert len(result["truncated"]) > 0

    def test_low_priority_truncated_first(self):
        packs = [
            {"summary_key": "high", "priority": 10, "summary_lines": ["h1", "h2"]},
            {"summary_key": "low", "priority": 20, "summary_lines": ["l1", "l2", "l3"]},
        ]
        result = apply_surface_budget(packs, surface="results_gateway", budget=3)
        # high priority lines should be in must_retain
        assert "h1" in result["must_retain"]
        assert "h2" in result["must_retain"]
        # low priority lines should be partially truncated
        assert result["truncated_count"] == 2  # 5 total - 3 budget = 2 truncated


# ---------------------------------------------------------------------------
# TestApplySurfaceBudgetClassification
# ---------------------------------------------------------------------------

class TestApplySurfaceBudgetClassification:
    def test_result_has_required_keys(self):
        packs = [
            {"summary_key": "a", "priority": 10, "summary_lines": ["line1"]},
        ]
        result = apply_surface_budget(packs, surface="results_gateway")
        assert "must_retain" in result
        assert "optional_expand" in result
        assert "truncated" in result
        assert "truncated_count" in result
        assert "truncated_pack_keys" in result
        assert "surface" in result
        assert "budget" in result
        assert "used" in result

    def test_first_pack_lines_are_must_retain(self):
        packs = [
            {"summary_key": "first", "priority": 10, "summary_lines": ["f1", "f2"]},
            {"summary_key": "second", "priority": 20, "summary_lines": ["s1"]},
        ]
        result = apply_surface_budget(packs, surface="results_gateway", budget=10)
        assert "f1" in result["must_retain"]
        assert "f2" in result["must_retain"]

    def test_second_pack_lines_are_optional_expand(self):
        packs = [
            {"summary_key": "first", "priority": 10, "summary_lines": ["f1"]},
            {"summary_key": "second", "priority": 20, "summary_lines": ["s1", "s2"]},
        ]
        result = apply_surface_budget(packs, surface="results_gateway", budget=10)
        assert "s1" in result["optional_expand"]
        assert "s2" in result["optional_expand"]


# ---------------------------------------------------------------------------
# TestApplySurfaceBudgetDeterministic
# ---------------------------------------------------------------------------

class TestApplySurfaceBudgetDeterministic:
    def test_same_input_same_output(self):
        packs = [
            {"summary_key": "a", "priority": 10, "summary_lines": ["l1", "l2"]},
            {"summary_key": "b", "priority": 20, "summary_lines": ["l3", "l4"]},
        ]
        result1 = apply_surface_budget(packs, surface="results_gateway", budget=3)
        result2 = apply_surface_budget(packs, surface="results_gateway", budget=3)
        assert result1 == result2


# ---------------------------------------------------------------------------
# TestApplySurfaceBudgetSamePriority
# ---------------------------------------------------------------------------

class TestApplySurfaceBudgetSamePriority:
    def test_same_priority_sorted_by_key(self):
        packs = [
            {"summary_key": "z_pack", "priority": 10, "summary_lines": ["z1"]},
            {"summary_key": "a_pack", "priority": 10, "summary_lines": ["a1"]},
        ]
        result = apply_surface_budget(packs, surface="results_gateway", budget=1)
        # a_pack comes before z_pack alphabetically, so a1 should be must_retain
        assert "a1" in result["must_retain"]


# ---------------------------------------------------------------------------
# TestBuildTruncationHintLine
# ---------------------------------------------------------------------------

class TestBuildTruncationHintLine:
    def test_chinese_default(self):
        hint = build_truncation_hint_line(3)
        assert "3" in hint
        assert "截断" in hint

    def test_english_fallback(self):
        hint = build_truncation_hint_line(3, lang="en")
        assert "3" in hint
        assert "truncated" in hint.lower()

    def test_zero_count(self):
        hint = build_truncation_hint_line(0)
        assert "0" in hint


# ---------------------------------------------------------------------------
# TestNoFormalAcceptanceLanguage
# ---------------------------------------------------------------------------

class TestNoFormalAcceptanceLanguage:
    def test_no_formal_acceptance_in_labels(self):
        all_labels = (
            list(TRUNCATION_LABELS.values())
            + list(TRUNCATION_LABELS_EN.values())
        )
        for text in all_labels:
            lower = text.lower()
            assert "formal acceptance" not in lower
            assert "formal claim" not in lower
            assert "正式验收" not in text
            assert "正式放行" not in text or "不构成" in text
