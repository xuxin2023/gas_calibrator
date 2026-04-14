"""Tests for reviewer_summary_packs — stable contract wrappers around compact summary builders.

Covers:
- Pack version
- All 6 pack builders return required fields
- Pack summary_lines matches builder summary_lines
- Simulation-only markers
- Priority ordering
- Severity derivation (governance_handoff, v12_alignment)
- Override parameters (priority, max_lines_hint)
- No formal acceptance / formal claim language
- No Chinese leakage in English labels
- Step 2 boundary markers
"""

from __future__ import annotations

import pytest

from gas_calibrator.v2.core.reviewer_summary_packs import (
    REVIEWER_SUMMARY_PACKS_VERSION,
    PACK_SUMMARY_KEYS,
    PACK_DEFAULT_PRIORITIES,
    PACK_DEFAULT_MAX_LINES_HINT,
    PACK_DISPLAY_LABELS,
    PACK_DISPLAY_LABELS_EN,
    PACK_SURFACE_BUDGET_HINT,
    build_measurement_digest_pack,
    build_readiness_digest_pack,
    build_phase_evidence_pack,
    build_v12_alignment_pack,
    build_governance_handoff_pack,
    build_parity_resilience_pack,
)
from gas_calibrator.v2.core.reviewer_summary_builders import (
    build_measurement_digest_compact_summary,
    build_readiness_digest_compact_summary,
    build_phase_evidence_compact_summary,
    build_v12_alignment_compact_summary,
    build_governance_handoff_compact_summary,
    build_parity_resilience_compact_summary,
)


# ---------------------------------------------------------------------------
# Required pack fields
# ---------------------------------------------------------------------------
PACK_REQUIRED_FIELDS = (
    "summary_key",
    "display_label",
    "priority",
    "severity",
    "summary_line",
    "summary_lines",
    "compact_summary_lines",
    "max_lines_hint",
    "surface_budget_hint",
    "boundary_markers",
    "evidence_source",
    "reviewer_only",
    "not_real_acceptance_evidence",
    "not_ready_for_formal_claim",
    "builders_version",
    "pack_version",
)


def _has_chinese(s: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in s)


# ---------------------------------------------------------------------------
# TestPackVersion
# ---------------------------------------------------------------------------

class TestPackVersion:
    def test_version_is_2_13_0(self):
        assert REVIEWER_SUMMARY_PACKS_VERSION == "2.13.0"


# ---------------------------------------------------------------------------
# TestPackRequiredFields
# ---------------------------------------------------------------------------

class TestPackRequiredFields:
    @pytest.mark.parametrize("builder_fn,summary_key", [
        (build_measurement_digest_pack, "measurement_digest"),
        (build_readiness_digest_pack, "readiness_digest"),
        (build_phase_evidence_pack, "phase_evidence"),
        (build_v12_alignment_pack, "v12_alignment"),
        (build_governance_handoff_pack, "governance_handoff"),
        (build_parity_resilience_pack, "parity_resilience"),
    ])
    def test_pack_contains_all_required_fields(self, builder_fn, summary_key):
        result = builder_fn({})
        for field in PACK_REQUIRED_FIELDS:
            assert field in result, f"Missing field '{field}' in {summary_key} pack"

    @pytest.mark.parametrize("builder_fn,summary_key", [
        (build_measurement_digest_pack, "measurement_digest"),
        (build_readiness_digest_pack, "readiness_digest"),
        (build_phase_evidence_pack, "phase_evidence"),
        (build_v12_alignment_pack, "v12_alignment"),
        (build_governance_handoff_pack, "governance_handoff"),
        (build_parity_resilience_pack, "parity_resilience"),
    ])
    def test_pack_summary_key(self, builder_fn, summary_key):
        result = builder_fn({})
        assert result["summary_key"] == summary_key


# ---------------------------------------------------------------------------
# TestPackMatchesBuilder
# ---------------------------------------------------------------------------

class TestPackMatchesBuilder:
    def test_measurement_digest_pack_matches_builder(self):
        payload = {"digest": {"payload_complete_phase_summary": "preseal"}}
        pack = build_measurement_digest_pack(payload)
        builder_result = build_measurement_digest_compact_summary(payload)
        assert pack["summary_lines"] == builder_result["summary_lines"]

    def test_readiness_digest_pack_matches_builder(self):
        payload = {"digest": {"scope_overview_summary": "ISO 17025"}}
        pack = build_readiness_digest_pack(payload)
        builder_result = build_readiness_digest_compact_summary(payload)
        assert pack["summary_lines"] == builder_result["summary_lines"]

    def test_phase_evidence_pack_matches_builder(self):
        payload = {"point_taxonomy_summary": {"summary_text": "4 points"}}
        pack = build_phase_evidence_pack(payload)
        builder_result = build_phase_evidence_compact_summary(payload)
        assert pack["summary_lines"] == builder_result["summary_lines"]

    def test_v12_alignment_pack_matches_builder(self):
        payload = {"point_taxonomy_summary": {"status": "complete"}}
        pack = build_v12_alignment_pack(payload)
        builder_result = build_v12_alignment_compact_summary(payload)
        assert pack["summary_lines"] == builder_result["summary_lines"]

    def test_governance_handoff_pack_matches_builder(self):
        payload = {"current_stage": "Step 2", "blockers": []}
        pack = build_governance_handoff_pack(payload)
        builder_result = build_governance_handoff_compact_summary(payload)
        assert pack["summary_lines"] == builder_result["summary_lines"]

    def test_parity_resilience_pack_matches_builder(self):
        payload = {"parity_status": "pass", "resilience_status": "pass"}
        pack = build_parity_resilience_pack(payload)
        builder_result = build_parity_resilience_compact_summary(payload)
        assert pack["summary_lines"] == builder_result["summary_lines"]


# ---------------------------------------------------------------------------
# TestSimulationOnlyMarkers
# ---------------------------------------------------------------------------

class TestSimulationOnlyMarkers:
    @pytest.mark.parametrize("builder_fn", [
        build_measurement_digest_pack,
        build_readiness_digest_pack,
        build_phase_evidence_pack,
        build_v12_alignment_pack,
        build_governance_handoff_pack,
        build_parity_resilience_pack,
    ])
    def test_evidence_source_simulated(self, builder_fn):
        result = builder_fn({})
        assert result["evidence_source"] == "simulated"

    @pytest.mark.parametrize("builder_fn", [
        build_measurement_digest_pack,
        build_readiness_digest_pack,
        build_phase_evidence_pack,
        build_v12_alignment_pack,
        build_governance_handoff_pack,
        build_parity_resilience_pack,
    ])
    def test_not_real_acceptance_evidence(self, builder_fn):
        result = builder_fn({})
        assert result["not_real_acceptance_evidence"] is True

    @pytest.mark.parametrize("builder_fn", [
        build_measurement_digest_pack,
        build_readiness_digest_pack,
        build_phase_evidence_pack,
        build_v12_alignment_pack,
        build_governance_handoff_pack,
        build_parity_resilience_pack,
    ])
    def test_not_ready_for_formal_claim(self, builder_fn):
        result = builder_fn({})
        assert result["not_ready_for_formal_claim"] is True

    @pytest.mark.parametrize("builder_fn", [
        build_measurement_digest_pack,
        build_readiness_digest_pack,
        build_phase_evidence_pack,
        build_v12_alignment_pack,
        build_governance_handoff_pack,
        build_parity_resilience_pack,
    ])
    def test_pack_version(self, builder_fn):
        result = builder_fn({})
        assert result["pack_version"] == "2.13.0"


# ---------------------------------------------------------------------------
# TestPriorityOrdering
# ---------------------------------------------------------------------------

class TestPriorityOrdering:
    def test_priority_ordering_constraint(self):
        """phase_evidence < measurement_digest < v12_alignment < governance_handoff < readiness_digest < parity_resilience"""
        assert PACK_DEFAULT_PRIORITIES["phase_evidence"] < PACK_DEFAULT_PRIORITIES["measurement_digest"]
        assert PACK_DEFAULT_PRIORITIES["measurement_digest"] < PACK_DEFAULT_PRIORITIES["v12_alignment"]
        assert PACK_DEFAULT_PRIORITIES["v12_alignment"] < PACK_DEFAULT_PRIORITIES["governance_handoff"]
        assert PACK_DEFAULT_PRIORITIES["governance_handoff"] < PACK_DEFAULT_PRIORITIES["readiness_digest"]
        assert PACK_DEFAULT_PRIORITIES["readiness_digest"] < PACK_DEFAULT_PRIORITIES["parity_resilience"]


# ---------------------------------------------------------------------------
# TestSeverityDerivation
# ---------------------------------------------------------------------------

class TestSeverityDerivation:
    def test_governance_handoff_with_blockers(self):
        result = build_governance_handoff_pack({"blockers": ["preseal gap"]})
        assert result["severity"] == "blocker"

    def test_governance_handoff_without_blockers(self):
        result = build_governance_handoff_pack({"blockers": []})
        assert result["severity"] == "info"

    def test_governance_handoff_no_blockers_key(self):
        result = build_governance_handoff_pack({})
        assert result["severity"] == "info"

    def test_v12_alignment_attention(self):
        result = build_v12_alignment_pack({"alignment_status": "attention"})
        assert result["severity"] == "attention"

    def test_v12_alignment_aligned(self):
        result = build_v12_alignment_pack({"alignment_status": "aligned"})
        assert result["severity"] == "info"

    def test_v12_alignment_no_status(self):
        result = build_v12_alignment_pack({})
        assert result["severity"] == "info"

    def test_measurement_digest_severity_info(self):
        result = build_measurement_digest_pack({})
        assert result["severity"] == "info"

    def test_readiness_digest_severity_info(self):
        result = build_readiness_digest_pack({})
        assert result["severity"] == "info"

    def test_phase_evidence_severity_info(self):
        result = build_phase_evidence_pack({})
        assert result["severity"] == "info"

    def test_parity_resilience_severity_info(self):
        result = build_parity_resilience_pack({})
        assert result["severity"] == "info"


# ---------------------------------------------------------------------------
# TestOverrideParameters
# ---------------------------------------------------------------------------

class TestOverrideParameters:
    def test_priority_override(self):
        result = build_v12_alignment_pack({}, priority=5)
        assert result["priority"] == 5

    def test_max_lines_hint_override(self):
        result = build_v12_alignment_pack({}, max_lines_hint=20)
        assert result["max_lines_hint"] == 20

    def test_default_priority_used_when_no_override(self):
        result = build_v12_alignment_pack({})
        assert result["priority"] == PACK_DEFAULT_PRIORITIES["v12_alignment"]

    def test_default_max_lines_hint_used_when_no_override(self):
        result = build_v12_alignment_pack({})
        assert result["max_lines_hint"] == PACK_DEFAULT_MAX_LINES_HINT["v12_alignment"]


# ---------------------------------------------------------------------------
# TestBackwardCompatibility
# ---------------------------------------------------------------------------

class TestBackwardCompatibility:
    def test_summary_line_is_pipe_joined(self):
        result = build_v12_alignment_pack({})
        # summary_line should be " | ".join(summary_lines)
        expected = " | ".join(result["summary_lines"])
        assert result["summary_line"] == expected

    def test_compact_summary_lines_equals_summary_lines(self):
        result = build_v12_alignment_pack({})
        assert result["compact_summary_lines"] == result["summary_lines"]


# ---------------------------------------------------------------------------
# TestNoFormalAcceptanceLanguage
# ---------------------------------------------------------------------------

class TestNoFormalAcceptanceLanguage:
    def test_no_formal_acceptance_in_labels(self):
        all_labels = (
            list(PACK_DISPLAY_LABELS.values())
            + list(PACK_DISPLAY_LABELS_EN.values())
        )
        for text in all_labels:
            lower = text.lower()
            assert "formal acceptance" not in lower, f"Formal acceptance language found: {text}"
            assert "formal claim" not in lower, f"Formal claim language found: {text}"
            if "正式放行" in text:
                assert "不构成" in text or "不是" in text, f"正式放行 without negation in: {text}"
            assert "正式验收" not in text, f"正式验收 language found: {text}"


# ---------------------------------------------------------------------------
# TestNoChineseLeakageInEnglish
# ---------------------------------------------------------------------------

class TestNoChineseLeakageInEnglish:
    def test_display_labels_en_no_chinese(self):
        for key, text in PACK_DISPLAY_LABELS_EN.items():
            assert not _has_chinese(text), f"Chinese leakage in EN label '{key}': {text}"


# ---------------------------------------------------------------------------
# TestStep2Boundary
# ---------------------------------------------------------------------------

class TestStep2Boundary:
    @pytest.mark.parametrize("builder_fn", [
        build_measurement_digest_pack,
        build_readiness_digest_pack,
        build_phase_evidence_pack,
        build_v12_alignment_pack,
        build_governance_handoff_pack,
        build_parity_resilience_pack,
    ])
    def test_boundary_markers_step2(self, builder_fn):
        result = builder_fn({})
        markers = result["boundary_markers"]
        assert markers["evidence_source"] == "simulated"
        assert markers["not_real_acceptance_evidence"] is True
        assert markers["reviewer_only"] is True
        assert markers["not_ready_for_formal_claim"] is True
