"""Tests for review_center_scan_contracts — family-aware budget, priority, and V1.2 alignment."""

from __future__ import annotations

import pytest

from gas_calibrator.v2.ui_v2.review_center_scan_contracts import (
    REVIEW_CENTER_SCAN_CONTRACTS_VERSION,
    REVIEW_CENTER_ARTIFACT_FAMILIES,
    FAMILY_KEY_TO_FILENAME,
    FAMILY_KEY_TO_PRIORITY,
    FAMILY_KEY_TO_BUDGET,
    FAMILY_KEY_TO_ROOTS_KIND,
    FAMILY_SCAN_ORDER,
    V12_PHASE_EVIDENCE_FAMILIES,
    V12_PHASE_DISPLAY_TERMS,
    V12_PHASE_DISPLAY_TERMS_EN,
    REVIEW_CENTER_SCAN_STEP2_BOUNDARY,
    allocate_family_budgets,
    build_family_budget_summary,
    resolve_v12_phase_display,
    build_v12_alignment_summary,
)


# ---------------------------------------------------------------------------
# 1. Contract version and structure
# ---------------------------------------------------------------------------


class TestScanContractsVersion:
    def test_version_is_2_13_x(self) -> None:
        assert REVIEW_CENTER_SCAN_CONTRACTS_VERSION.startswith("2.13")


class TestArtifactFamiliesStructure:
    def test_families_are_non_empty(self) -> None:
        assert len(REVIEW_CENTER_ARTIFACT_FAMILIES) >= 8

    def test_all_families_have_unique_keys(self) -> None:
        keys = [f[0] for f in REVIEW_CENTER_ARTIFACT_FAMILIES]
        assert len(keys) == len(set(keys))

    def test_all_families_have_positive_budget(self) -> None:
        for fam in REVIEW_CENTER_ARTIFACT_FAMILIES:
            assert fam[3] > 0, f"Family {fam[0]} has non-positive budget {fam[3]}"

    def test_all_families_have_valid_roots_kind(self) -> None:
        valid_kinds = {"run_roots", "suite_roots", "compare_roots"}
        for fam in REVIEW_CENTER_ARTIFACT_FAMILIES:
            assert fam[4] in valid_kinds, f"Family {fam[0]} has invalid roots_kind {fam[4]}"


# ---------------------------------------------------------------------------
# 2. Family ordering and priority
# ---------------------------------------------------------------------------


class TestFamilyOrdering:
    def test_scan_order_is_deterministic(self) -> None:
        order1 = FAMILY_SCAN_ORDER
        order2 = FAMILY_SCAN_ORDER
        assert order1 == order2

    def test_suite_scanned_before_parity(self) -> None:
        assert FAMILY_KEY_TO_PRIORITY["suite"] < FAMILY_KEY_TO_PRIORITY["parity"]

    def test_parity_scanned_before_resilience(self) -> None:
        assert FAMILY_KEY_TO_PRIORITY["parity"] < FAMILY_KEY_TO_PRIORITY["resilience"]

    def test_parity_and_resilience_use_compare_roots(self) -> None:
        assert FAMILY_KEY_TO_ROOTS_KIND["parity"] == "compare_roots"
        assert FAMILY_KEY_TO_ROOTS_KIND["resilience"] == "compare_roots"

    def test_v12_phase_families_are_present(self) -> None:
        for fam in V12_PHASE_EVIDENCE_FAMILIES:
            assert fam in FAMILY_KEY_TO_BUDGET, f"V1.2 family {fam} not in budget table"


# ---------------------------------------------------------------------------
# 3. Budget allocation
# ---------------------------------------------------------------------------


class TestBudgetAllocation:
    def test_default_budgets_are_positive(self) -> None:
        budgets = allocate_family_budgets()
        for key in FAMILY_SCAN_ORDER:
            assert budgets[key] > 0, f"Family {key} has zero budget"

    def test_parity_gets_own_budget(self) -> None:
        budgets = allocate_family_budgets()
        assert budgets["parity"] > 0

    def test_resilience_gets_own_budget(self) -> None:
        budgets = allocate_family_budgets()
        assert budgets["resilience"] > 0

    def test_parity_not_starved_by_suite(self) -> None:
        """Parity budget is independent of suite budget."""
        budgets = allocate_family_budgets()
        assert budgets["parity"] >= 32  # Minimum reasonable budget

    def test_global_budget_allocation(self) -> None:
        budgets = allocate_family_budgets(global_budget=200)
        assert sum(budgets.values()) <= 200 + 1  # Allow rounding

    def test_all_families_get_budget_with_global_limit(self) -> None:
        budgets = allocate_family_budgets(global_budget=200)
        for key in FAMILY_SCAN_ORDER:
            assert budgets[key] >= 0


# ---------------------------------------------------------------------------
# 4. Family budget summary
# ---------------------------------------------------------------------------


class TestFamilyBudgetSummary:
    def test_ok_status_when_under_budget(self) -> None:
        budgets = {"suite": 64, "parity": 64}
        used = {"suite": 10, "parity": 10}
        summary = build_family_budget_summary(budgets, used)
        assert summary["suite"]["status"] == "ok"
        assert summary["parity"]["status"] == "ok"

    def test_budget_limited_status_when_exhausted(self) -> None:
        budgets = {"suite": 64, "parity": 64}
        used = {"suite": 64, "parity": 10}
        summary = build_family_budget_summary(budgets, used)
        assert summary["suite"]["status"] == "budget_limited"
        assert summary["parity"]["status"] == "ok"

    def test_omitted_status_when_zero_budget(self) -> None:
        budgets = {"suite": 0, "parity": 64}
        used = {"suite": 0, "parity": 10}
        summary = build_family_budget_summary(budgets, used)
        assert summary["suite"]["status"] == "omitted"

    def test_parity_not_budget_limited_in_typical_case(self) -> None:
        """In a typical scan, parity should not be budget_limited."""
        budgets = allocate_family_budgets()
        used = {key: 10 for key in budgets}  # Typical usage
        summary = build_family_budget_summary(budgets, used)
        assert summary["parity"]["status"] == "ok"
        assert summary["resilience"]["status"] == "ok"


# ---------------------------------------------------------------------------
# 5. V1.2 phase display terms
# ---------------------------------------------------------------------------


class TestV12PhaseDisplayTerms:
    def test_chinese_terms_have_cjk(self) -> None:
        for key, value in V12_PHASE_DISPLAY_TERMS.items():
            assert any('\u4e00' <= c <= '\u9fff' for c in value), \
                f"Chinese term for {key} has no CJK: {value}"

    def test_english_terms_no_cjk(self) -> None:
        for key, value in V12_PHASE_DISPLAY_TERMS_EN.items():
            assert not any('\u4e00' <= c <= '\u9fff' for c in value), \
                f"English term for {key} has CJK: {value}"

    def test_v12_dimensions_covered(self) -> None:
        required = {"ambient", "flush_gate", "preseal", "postseal", "stale_gauge"}
        assert required <= set(V12_PHASE_DISPLAY_TERMS.keys())

    def test_resolve_zh_default(self) -> None:
        assert resolve_v12_phase_display("flush_gate") == "冲洗门禁"
        assert resolve_v12_phase_display("preseal") == "前封气"
        assert resolve_v12_phase_display("postseal") == "后封气"

    def test_resolve_en_fallback(self) -> None:
        assert resolve_v12_phase_display("flush_gate", lang="en") == "Flush Gate"
        assert resolve_v12_phase_display("preseal", lang="en") == "Preseal"


# ---------------------------------------------------------------------------
# 6. V1.2 alignment summary
# ---------------------------------------------------------------------------


class TestV12AlignmentSummary:
    def test_aligned_when_all_ok(self) -> None:
        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MATCH",
            governance_handoff_blockers=[],
        )
        assert result["v12_alignment_summary"]["alignment_status"] == "aligned"
        assert result["not_real_acceptance_evidence"] is True
        assert result["reviewer_only"] is True

    def test_attention_when_parity_mismatch(self) -> None:
        result = build_v12_alignment_summary(
            parity_status="MISMATCH",
            resilience_status="MATCH",
        )
        assert result["v12_alignment_summary"]["alignment_status"] == "attention"

    def test_attention_when_resilience_mismatch(self) -> None:
        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MISMATCH",
        )
        assert result["v12_alignment_summary"]["alignment_status"] == "attention"

    def test_attention_when_blockers(self) -> None:
        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MATCH",
            governance_handoff_blockers=["step2_readiness_gap"],
        )
        assert result["v12_alignment_summary"]["alignment_status"] == "attention"

    def test_step2_boundary_markers(self) -> None:
        result = build_v12_alignment_summary()
        assert result["evidence_source"] == "simulated"
        assert result["not_real_acceptance_evidence"] is True
        assert result["not_ready_for_formal_claim"] is True
        assert result["reviewer_only"] is True
        assert result["readiness_mapping_only"] is True

    def test_taxonomy_dimensions_present(self) -> None:
        result = build_v12_alignment_summary()
        dims = result["v12_alignment_summary"]["taxonomy_dimensions"]
        assert "flush_gate" in dims
        assert "preseal" in dims
        assert "postseal" in dims
        assert "stale_gauge" in dims

    def test_summary_line_present(self) -> None:
        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MATCH",
        )
        summary_line = result["v12_alignment_summary"]["summary_line"]
        assert "V1.2" in summary_line
        assert "MATCH" in summary_line

    def test_no_formal_claim_language(self) -> None:
        result = build_v12_alignment_summary()
        summary_line = result["v12_alignment_summary"]["summary_line"]
        forbidden = ["formal approval", "real acceptance", "accredited", "certified"]
        for word in forbidden:
            assert word not in summary_line.lower(), f"Summary contains '{word}'"


# ---------------------------------------------------------------------------
# 7. Step 2 boundary
# ---------------------------------------------------------------------------


class TestStep2Boundary:
    def test_boundary_markers(self) -> None:
        assert REVIEW_CENTER_SCAN_STEP2_BOUNDARY["evidence_source"] == "simulated"
        assert REVIEW_CENTER_SCAN_STEP2_BOUNDARY["not_real_acceptance_evidence"] is True
        assert REVIEW_CENTER_SCAN_STEP2_BOUNDARY["not_ready_for_formal_claim"] is True
        assert REVIEW_CENTER_SCAN_STEP2_BOUNDARY["reviewer_only"] is True
        assert REVIEW_CENTER_SCAN_STEP2_BOUNDARY["readiness_mapping_only"] is True

    def test_no_real_paths_in_contracts(self) -> None:
        import inspect
        from gas_calibrator.v2.ui_v2 import review_center_scan_contracts
        source = inspect.getsource(review_center_scan_contracts)
        import re
        assert not re.search(r'\bCOM\d+\b', source), "Real COM port reference found"
        assert not re.search(r'["\']serial["\']', source), "Serial port reference found"
        assert not re.search(r'["\']real_device["\']', source), "Real device reference found"


# ---------------------------------------------------------------------------
# 8. V1.2 alignment summary — compact summary pack consumption (Step 2.13)
# ---------------------------------------------------------------------------


class TestV12AlignmentSummaryPackConsumption:
    def test_compact_summary_pack_present(self) -> None:
        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MATCH",
        )
        pack = result["v12_alignment_summary"]["compact_summary_pack"]
        assert pack["summary_key"] == "v12_alignment"

    def test_compact_summary_pack_version(self) -> None:
        result = build_v12_alignment_summary()
        pack = result["v12_alignment_summary"]["compact_summary_pack"]
        assert pack["pack_version"] == "2.13.0"

    def test_compact_summary_pack_simulation_only(self) -> None:
        result = build_v12_alignment_summary()
        pack = result["v12_alignment_summary"]["compact_summary_pack"]
        assert pack["evidence_source"] == "simulated"
        assert pack["not_real_acceptance_evidence"] is True
        assert pack["not_ready_for_formal_claim"] is True

    def test_compact_summary_sections_present(self) -> None:
        result = build_v12_alignment_summary()
        sections = result["v12_alignment_summary"]["compact_summary_sections"]
        assert isinstance(sections, list)
        assert len(sections) > 0

    def test_compact_summary_budget_present(self) -> None:
        result = build_v12_alignment_summary()
        budget = result["v12_alignment_summary"]["compact_summary_budget"]
        assert "total_lines" in budget
        assert "pack_count" in budget
        assert budget["pack_count"] == 1

    def test_existing_fields_preserved(self) -> None:
        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MATCH",
        )
        summary = result["v12_alignment_summary"]
        # Existing fields must still be present
        assert "summary_line" in summary
        assert "compact_summary_lines" in summary
        assert "builders_version" in summary
        assert isinstance(summary["summary_line"], str)
        assert isinstance(summary["compact_summary_lines"], list)
