"""Tests for reviewer_summary_builders — single source of truth for
reviewer-facing compact summary generation.

Covers:
- Builder version
- V1.2 compact summary keys and labels
- Measurement digest compact summary
- Readiness digest compact summary
- Phase evidence compact summary
- V1.2 alignment compact summary
- Governance handoff compact summary
- Parity / resilience compact summary
- Step 2 boundary markers
- No formal acceptance / formal claim language
- No Chinese leakage in English fallbacks
- en_US environment does not leak Chinese default
"""

from __future__ import annotations

import pytest

from gas_calibrator.v2.core.reviewer_summary_builders import (
    REVIEWER_SUMMARY_BUILDERS_VERSION,
    V12_COMPACT_SUMMARY_KEYS,
    V12_COMPACT_SUMMARY_LABELS,
    V12_COMPACT_SUMMARY_LABELS_EN,
    GOVERNANCE_HANDOFF_LABELS,
    GOVERNANCE_HANDOFF_LABELS_EN,
    PARITY_RESILIENCE_LABELS,
    PARITY_RESILIENCE_LABELS_EN,
    resolve_v12_compact_label,
    resolve_governance_handoff_label,
    resolve_parity_resilience_label,
    build_measurement_digest_compact_summary,
    build_readiness_digest_compact_summary,
    build_phase_evidence_compact_summary,
    build_v12_alignment_compact_summary,
    build_governance_handoff_compact_summary,
    build_parity_resilience_compact_summary,
)
from gas_calibrator.v2.core.phase_evidence_display_contracts import (
    PHASE_EVIDENCE_STEP2_BOUNDARY,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _has_chinese(s: str) -> bool:
    """Check if string contains any CJK character."""
    return any("\u4e00" <= ch <= "\u9fff" for ch in s)


# ---------------------------------------------------------------------------
# TestBuilderVersion
# ---------------------------------------------------------------------------

class TestBuilderVersion:
    def test_version_is_2_11_0(self):
        assert REVIEWER_SUMMARY_BUILDERS_VERSION == "2.11.0"


# ---------------------------------------------------------------------------
# TestV12CompactSummaryKeys
# ---------------------------------------------------------------------------

class TestV12CompactSummaryKeys:
    def test_keys_cover_required_domains(self):
        required = (
            "point_taxonomy",
            "measurement_phase_coverage",
            "phase_transition_bridge",
            "parity_resilience",
            "governance_blockers",
            "v12_alignment",
        )
        for key in required:
            assert key in V12_COMPACT_SUMMARY_KEYS, f"Missing V1.2 compact key: {key}"

    def test_labels_cover_all_keys(self):
        for key in V12_COMPACT_SUMMARY_KEYS:
            assert key in V12_COMPACT_SUMMARY_LABELS, f"Missing zh label for key: {key}"
            assert key in V12_COMPACT_SUMMARY_LABELS_EN, f"Missing en label for key: {key}"

    def test_header_and_notes_present(self):
        assert "header" in V12_COMPACT_SUMMARY_LABELS
        assert "simulated_only_note" in V12_COMPACT_SUMMARY_LABELS
        assert "no_formal_claim" in V12_COMPACT_SUMMARY_LABELS


# ---------------------------------------------------------------------------
# TestResolveHelpers
# ---------------------------------------------------------------------------

class TestResolveHelpers:
    def test_v12_compact_label_zh(self):
        assert resolve_v12_compact_label("point_taxonomy") == "点位语义"

    def test_v12_compact_label_en(self):
        assert resolve_v12_compact_label("point_taxonomy", lang="en") == "Point Taxonomy"

    def test_governance_handoff_label_zh(self):
        assert resolve_governance_handoff_label("current_stage") == "当前阶段"

    def test_governance_handoff_label_en(self):
        assert resolve_governance_handoff_label("current_stage", lang="en") == "Current stage"

    def test_parity_resilience_label_zh(self):
        assert resolve_parity_resilience_label("parity_status") == "一致性状态"

    def test_parity_resilience_label_en(self):
        assert resolve_parity_resilience_label("parity_status", lang="en") == "Parity status"


# ---------------------------------------------------------------------------
# TestMeasurementDigestCompactSummary
# ---------------------------------------------------------------------------

class TestMeasurementDigestCompactSummary:
    def test_empty_payload_returns_lines(self):
        result = build_measurement_digest_compact_summary({})
        assert "summary_lines" in result
        assert "detail_lines" in result
        assert "boundary_markers" in result
        assert isinstance(result["summary_lines"], list)
        assert isinstance(result["detail_lines"], list)

    def test_summary_lines_from_payload(self):
        payload = {
            "digest": {
                "payload_complete_phase_summary": "preseal, postseal",
                "payload_partial_phase_summary": "ambient",
                "trace_only_phase_summary": "flush_gate",
                "blocker_summary": "missing preseal payload",
                "next_required_artifacts_summary": "preseal payload",
            }
        }
        result = build_measurement_digest_compact_summary(payload)
        lines = result["summary_lines"]
        assert len(lines) == 5
        # Check that payload complete info appears in lines
        joined = " | ".join(lines)
        assert "preseal" in joined or "payload" in joined

    def test_boundary_markers_step2(self):
        result = build_measurement_digest_compact_summary({})
        markers = result["boundary_markers"]
        assert markers["evidence_source"] == "simulated"
        assert markers["not_real_acceptance_evidence"] is True
        assert markers["reviewer_only"] is True

    def test_include_boundary(self):
        result = build_measurement_digest_compact_summary({}, include_boundary=True)
        assert any("边界" in line or "Boundary" in line for line in result["detail_lines"])

    def test_include_non_claim(self):
        result = build_measurement_digest_compact_summary({}, include_non_claim=True)
        assert any("非声明" in line or "Non-claim" in line for line in result["detail_lines"])


# ---------------------------------------------------------------------------
# TestReadinessDigestCompactSummary
# ---------------------------------------------------------------------------

class TestReadinessDigestCompactSummary:
    def test_empty_payload_returns_lines(self):
        result = build_readiness_digest_compact_summary({})
        assert "summary_lines" in result
        assert "detail_lines" in result
        assert "boundary_markers" in result

    def test_summary_lines_from_payload(self):
        payload = {
            "digest": {
                "scope_overview_summary": "ISO 17025",
                "decision_rule_summary": "binary pass/fail",
                "readiness_status_summary": "not ready",
                "top_gaps_summary": "preseal payload missing",
                "current_evidence_coverage_summary": "60%",
            }
        }
        result = build_readiness_digest_compact_summary(payload)
        lines = result["summary_lines"]
        assert len(lines) == 5

    def test_boundary_markers_step2(self):
        result = build_readiness_digest_compact_summary({})
        markers = result["boundary_markers"]
        assert markers["evidence_source"] == "simulated"
        assert markers["not_real_acceptance_evidence"] is True


# ---------------------------------------------------------------------------
# TestPhaseEvidenceCompactSummary
# ---------------------------------------------------------------------------

class TestPhaseEvidenceCompactSummary:
    def test_empty_payload_returns_lines(self):
        result = build_phase_evidence_compact_summary({})
        assert "summary_lines" in result
        assert "boundary_markers" in result
        assert len(result["summary_lines"]) == 3

    def test_summary_from_sub_sections(self):
        payload = {
            "point_taxonomy_summary": {"summary_text": "4 points classified"},
            "measurement_phase_coverage_report": {"summary_text": "3/4 phases covered"},
            "phase_transition_bridge": {"summary_text": "bridge in progress"},
        }
        result = build_phase_evidence_compact_summary(payload)
        joined = " | ".join(result["summary_lines"])
        assert "4 points" in joined or "点位" in joined


# ---------------------------------------------------------------------------
# TestV12AlignmentCompactSummary
# ---------------------------------------------------------------------------

class TestV12AlignmentCompactSummary:
    def test_empty_payload_returns_lines(self):
        result = build_v12_alignment_compact_summary({})
        assert "summary_lines" in result
        assert "boundary_markers" in result
        assert "v12_compact" in result

    def test_v12_compact_structure(self):
        result = build_v12_alignment_compact_summary({})
        compact = result["v12_compact"]
        assert "point_taxonomy" in compact
        assert "measurement_phase_coverage" in compact
        assert "phase_transition_bridge" in compact
        assert "parity" in compact
        assert "resilience" in compact
        assert "governance_blockers" in compact
        assert "governance_next_steps" in compact

    def test_simulated_only_note_present(self):
        result = build_v12_alignment_compact_summary({})
        joined = " | ".join(result["summary_lines"])
        assert "仿真" in joined or "Simulated" in joined

    def test_no_formal_claim_present(self):
        result = build_v12_alignment_compact_summary({})
        joined = " | ".join(result["summary_lines"])
        assert "正式放行" in joined or "formal" in joined.lower()

    def test_from_sub_sections(self):
        payload = {
            "point_taxonomy_summary": {"status": "complete"},
            "measurement_phase_coverage_report": {"status": "partial"},
            "phase_transition_bridge": {"status": "in_progress"},
            "parity_resilience_summary": {"parity_status": "pass", "resilience_status": "pass"},
            "governance_handoff_summary": {"blockers": ["preseal gap"], "next_steps": "close preseal"},
        }
        result = build_v12_alignment_compact_summary(payload)
        compact = result["v12_compact"]
        assert compact["point_taxonomy"] == "complete"
        assert compact["measurement_phase_coverage"] == "partial"
        assert compact["parity"] == "pass"
        assert compact["resilience"] == "pass"


# ---------------------------------------------------------------------------
# TestGovernanceHandoffCompactSummary
# ---------------------------------------------------------------------------

class TestGovernanceHandoffCompactSummary:
    def test_empty_payload_returns_lines(self):
        result = build_governance_handoff_compact_summary({})
        assert "summary_lines" in result
        assert "boundary_markers" in result
        assert len(result["summary_lines"]) == 4

    def test_from_payload(self):
        payload = {
            "current_stage": "Step 2 tail",
            "blockers": ["preseal gap", "uncertainty budget"],
            "next_steps": "close preseal gap",
            "evidence_source": "simulated",
        }
        result = build_governance_handoff_compact_summary(payload)
        joined = " | ".join(result["summary_lines"])
        assert "Step 2" in joined or "当前阶段" in joined


# ---------------------------------------------------------------------------
# TestParityResilienceCompactSummary
# ---------------------------------------------------------------------------

class TestParityResilienceCompactSummary:
    def test_empty_payload_returns_lines(self):
        result = build_parity_resilience_compact_summary({})
        assert "summary_lines" in result
        assert "boundary_markers" in result
        assert len(result["summary_lines"]) == 4

    def test_from_payload(self):
        payload = {
            "parity_status": "pass",
            "resilience_status": "pass",
            "parity_last_run": "2024-01-01",
            "resilience_last_run": "2024-01-01",
        }
        result = build_parity_resilience_compact_summary(payload)
        joined = " | ".join(result["summary_lines"])
        assert "pass" in joined


# ---------------------------------------------------------------------------
# TestStep2Boundary
# ---------------------------------------------------------------------------

class TestStep2Boundary:
    def test_all_builders_return_step2_boundary(self):
        """All compact summary builders must return Step 2 boundary markers."""
        for builder, args in [
            (build_measurement_digest_compact_summary, ({},)),
            (build_readiness_digest_compact_summary, ({},)),
            (build_phase_evidence_compact_summary, ({},)),
            (build_v12_alignment_compact_summary, ({},)),
            (build_governance_handoff_compact_summary, ({},)),
            (build_parity_resilience_compact_summary, ({},)),
        ]:
            result = builder(*args)
            markers = result["boundary_markers"]
            assert markers["evidence_source"] == "simulated", f"{builder.__name__} evidence_source"
            assert markers["not_real_acceptance_evidence"] is True, f"{builder.__name__} not_real_acceptance_evidence"
            assert markers["reviewer_only"] is True, f"{builder.__name__} reviewer_only"
            assert markers["not_ready_for_formal_claim"] is True, f"{builder.__name__} not_ready_for_formal_claim"


# ---------------------------------------------------------------------------
# TestNoFormalAcceptanceLanguage
# ---------------------------------------------------------------------------

class TestNoFormalAcceptanceLanguage:
    def test_no_formal_acceptance_in_labels(self):
        """No label may contain formal acceptance / formal claim language (except negated forms)."""
        all_labels = (
            list(V12_COMPACT_SUMMARY_LABELS.values())
            + list(V12_COMPACT_SUMMARY_LABELS_EN.values())
            + list(GOVERNANCE_HANDOFF_LABELS.values())
            + list(GOVERNANCE_HANDOFF_LABELS_EN.values())
            + list(PARITY_RESILIENCE_LABELS.values())
            + list(PARITY_RESILIENCE_LABELS_EN.values())
        )
        for text in all_labels:
            lower = text.lower()
            assert "formal acceptance" not in lower, f"Formal acceptance language found: {text}"
            assert "formal claim" not in lower, f"Formal claim language found: {text}"
            # "正式放行" is allowed only in negated form (e.g. "不构成正式放行结论")
            if "正式放行" in text:
                assert "不构成" in text or "不是" in text, f"正式放行 without negation in: {text}"
            assert "正式验收" not in text, f"正式验收 language found: {text}"

    def test_no_real_device_language_in_labels(self):
        """No label may reference real device / real serial / COM port."""
        all_labels = (
            list(V12_COMPACT_SUMMARY_LABELS.values())
            + list(V12_COMPACT_SUMMARY_LABELS_EN.values())
            + list(GOVERNANCE_HANDOFF_LABELS.values())
            + list(GOVERNANCE_HANDOFF_LABELS_EN.values())
            + list(PARITY_RESILIENCE_LABELS.values())
            + list(PARITY_RESILIENCE_LABELS_EN.values())
        )
        for text in all_labels:
            lower = text.lower()
            assert "real device" not in lower, f"Real device language found: {text}"
            assert "serial port" not in lower, f"Serial port language found: {text}"
            assert "com port" not in lower, f"COM port language found: {text}"


# ---------------------------------------------------------------------------
# TestNoChineseLeakageInEnglish
# ---------------------------------------------------------------------------

class TestNoChineseLeakageInEnglish:
    def test_v12_compact_labels_en_no_chinese(self):
        for key, text in V12_COMPACT_SUMMARY_LABELS_EN.items():
            assert not _has_chinese(text), f"Chinese leakage in EN label '{key}': {text}"

    def test_governance_handoff_labels_en_no_chinese(self):
        for key, text in GOVERNANCE_HANDOFF_LABELS_EN.items():
            assert not _has_chinese(text), f"Chinese leakage in EN label '{key}': {text}"

    def test_parity_resilience_labels_en_no_chinese(self):
        for key, text in PARITY_RESILIENCE_LABELS_EN.items():
            assert not _has_chinese(text), f"Chinese leakage in EN label '{key}': {text}"


# ---------------------------------------------------------------------------
# TestLabelConsistency
# ---------------------------------------------------------------------------

class TestLabelConsistency:
    def test_zh_en_keys_match(self):
        """Chinese and English label dicts must have identical key sets."""
        assert set(V12_COMPACT_SUMMARY_LABELS.keys()) == set(V12_COMPACT_SUMMARY_LABELS_EN.keys())
        assert set(GOVERNANCE_HANDOFF_LABELS.keys()) == set(GOVERNANCE_HANDOFF_LABELS_EN.keys())
        assert set(PARITY_RESILIENCE_LABELS.keys()) == set(PARITY_RESILIENCE_LABELS_EN.keys())
