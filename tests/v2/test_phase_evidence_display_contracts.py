"""Tests for phase_evidence_display_contracts — Step 2.8 + 2.9.

Covers:
- Contracts version
- Artifact keys completeness
- Chinese default / English fallback consistency
- No Chinese leakage in English fallbacks
- Bridge section labels completeness
- Bridge reviewer texts completeness
- Results fallback labels completeness
- Formatter display labels completeness (2.9)
- Compatibility row labels completeness (2.9)
- Historical rollup labels completeness (2.9)
- Measurement digest labels completeness (2.9)
- Readiness digest labels completeness (2.9)
- Resolve helpers
- Terminology convergence with review_center_scan_contracts
- Step 2 boundary markers
"""

from __future__ import annotations

import pytest

from gas_calibrator.v2.core.phase_evidence_display_contracts import (
    PHASE_EVIDENCE_DISPLAY_CONTRACTS_VERSION,
    PHASE_EVIDENCE_ARTIFACT_KEYS,
    PHASE_EVIDENCE_TITLE_TEXTS,
    PHASE_EVIDENCE_TITLE_TEXTS_EN,
    PHASE_EVIDENCE_SUMMARY_TEXTS,
    PHASE_EVIDENCE_SUMMARY_TEXTS_EN,
    PHASE_EVIDENCE_SECTION_LABELS,
    PHASE_EVIDENCE_SECTION_LABELS_EN,
    PHASE_EVIDENCE_TYPE_LABELS,
    PHASE_EVIDENCE_TYPE_LABELS_EN,
    PHASE_EVIDENCE_I18N_KEYS,
    PHASE_TERMS,
    PHASE_TERMS_EN,
    BRIDGE_SECTION_LABELS,
    BRIDGE_SECTION_LABELS_EN,
    BRIDGE_REVIEWER_TEXTS,
    BRIDGE_REVIEWER_TEXTS_EN,
    RESULTS_FALLBACK_LABELS,
    RESULTS_FALLBACK_LABELS_EN,
    FORMATTER_DISPLAY_LABELS,
    FORMATTER_DISPLAY_LABELS_EN,
    COMPATIBILITY_ROW_LABELS,
    COMPATIBILITY_ROW_LABELS_EN,
    HISTORICAL_ROLLUP_LABELS,
    HISTORICAL_ROLLUP_LABELS_EN,
    MEASUREMENT_DIGEST_LABELS,
    MEASUREMENT_DIGEST_LABELS_EN,
    READINESS_DIGEST_LABELS,
    READINESS_DIGEST_LABELS_EN,
    PHASE_EVIDENCE_STEP2_BOUNDARY,
    resolve_phase_evidence_title,
    resolve_phase_term,
    resolve_bridge_section_label,
    resolve_bridge_reviewer_text,
    resolve_results_fallback_label,
    resolve_formatter_label,
    resolve_compatibility_row_label,
    resolve_historical_rollup_label,
    resolve_measurement_digest_label,
    resolve_readiness_digest_label,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _has_chinese(s: str) -> bool:
    """Check if string contains any CJK character."""
    return any("\u4e00" <= ch <= "\u9fff" for ch in s)


# ---------------------------------------------------------------------------
# TestContractsVersion
# ---------------------------------------------------------------------------

class TestContractsVersion:
    def test_version_is_2_9_0(self):
        assert PHASE_EVIDENCE_DISPLAY_CONTRACTS_VERSION == "2.9.0"


# ---------------------------------------------------------------------------
# TestArtifactKeys
# ---------------------------------------------------------------------------

class TestArtifactKeys:
    def test_artifact_keys_count(self):
        assert len(PHASE_EVIDENCE_ARTIFACT_KEYS) == 4

    def test_artifact_keys_content(self):
        expected = (
            "point_taxonomy_summary",
            "measurement_phase_coverage_report",
            "phase_transition_bridge",
            "v12_alignment_summary",
        )
        assert PHASE_EVIDENCE_ARTIFACT_KEYS == expected


# ---------------------------------------------------------------------------
# TestChineseDefaultEnglishFallback
# ---------------------------------------------------------------------------

class TestChineseDefaultEnglishFallback:
    """All Chinese default dicts must have matching English fallback dicts with same keys."""

    def _assert_same_keys(self, zh: dict, en: dict, name: str):
        assert set(zh.keys()) == set(en.keys()), f"{name}: key mismatch"

    def test_title_texts_keys_match(self):
        self._assert_same_keys(PHASE_EVIDENCE_TITLE_TEXTS, PHASE_EVIDENCE_TITLE_TEXTS_EN, "title")

    def test_summary_texts_keys_match(self):
        self._assert_same_keys(PHASE_EVIDENCE_SUMMARY_TEXTS, PHASE_EVIDENCE_SUMMARY_TEXTS_EN, "summary")

    def test_section_labels_keys_match(self):
        self._assert_same_keys(PHASE_EVIDENCE_SECTION_LABELS, PHASE_EVIDENCE_SECTION_LABELS_EN, "section")

    def test_type_labels_keys_match(self):
        self._assert_same_keys(PHASE_EVIDENCE_TYPE_LABELS, PHASE_EVIDENCE_TYPE_LABELS_EN, "type")

    def test_phase_terms_keys_match(self):
        self._assert_same_keys(PHASE_TERMS, PHASE_TERMS_EN, "phase_terms")

    def test_bridge_section_labels_keys_match(self):
        self._assert_same_keys(BRIDGE_SECTION_LABELS, BRIDGE_SECTION_LABELS_EN, "bridge_section")

    def test_bridge_reviewer_texts_keys_match(self):
        self._assert_same_keys(BRIDGE_REVIEWER_TEXTS, BRIDGE_REVIEWER_TEXTS_EN, "bridge_reviewer")

    def test_results_fallback_labels_keys_match(self):
        self._assert_same_keys(RESULTS_FALLBACK_LABELS, RESULTS_FALLBACK_LABELS_EN, "results_fallback")


# ---------------------------------------------------------------------------
# TestNoChineseLeakageInEnglish
# ---------------------------------------------------------------------------

class TestNoChineseLeakageInEnglish:
    """English fallback dicts must not contain Chinese characters."""

    def _assert_no_chinese(self, d: dict, name: str):
        for key, value in d.items():
            assert not _has_chinese(value), f"{name}[{key!r}] contains Chinese: {value!r}"

    def test_title_texts_en_no_chinese(self):
        self._assert_no_chinese(PHASE_EVIDENCE_TITLE_TEXTS_EN, "title_en")

    def test_summary_texts_en_no_chinese(self):
        self._assert_no_chinese(PHASE_EVIDENCE_SUMMARY_TEXTS_EN, "summary_en")

    def test_section_labels_en_no_chinese(self):
        self._assert_no_chinese(PHASE_EVIDENCE_SECTION_LABELS_EN, "section_en")

    def test_type_labels_en_no_chinese(self):
        self._assert_no_chinese(PHASE_EVIDENCE_TYPE_LABELS_EN, "type_en")

    def test_phase_terms_en_no_chinese(self):
        self._assert_no_chinese(PHASE_TERMS_EN, "phase_terms_en")

    def test_bridge_section_labels_en_no_chinese(self):
        self._assert_no_chinese(BRIDGE_SECTION_LABELS_EN, "bridge_section_en")

    def test_bridge_reviewer_texts_en_no_chinese(self):
        self._assert_no_chinese(BRIDGE_REVIEWER_TEXTS_EN, "bridge_reviewer_en")

    def test_results_fallback_labels_en_no_chinese(self):
        self._assert_no_chinese(RESULTS_FALLBACK_LABELS_EN, "results_fallback_en")


# ---------------------------------------------------------------------------
# TestChineseDefaultsContainChinese
# ---------------------------------------------------------------------------

class TestChineseDefaultsContainChinese:
    """Chinese default dicts must contain Chinese characters."""

    def _assert_has_chinese(self, d: dict, name: str):
        for key, value in d.items():
            assert _has_chinese(value), f"{name}[{key!r}] has no Chinese: {value!r}"

    def test_title_texts_zh_has_chinese(self):
        self._assert_has_chinese(PHASE_EVIDENCE_TITLE_TEXTS, "title_zh")

    def test_summary_texts_zh_has_chinese(self):
        self._assert_has_chinese(PHASE_EVIDENCE_SUMMARY_TEXTS, "summary_zh")

    def test_phase_terms_zh_has_chinese(self):
        self._assert_has_chinese(PHASE_TERMS, "phase_terms_zh")

    def test_bridge_section_labels_zh_has_chinese(self):
        self._assert_has_chinese(BRIDGE_SECTION_LABELS, "bridge_section_zh")

    def test_bridge_reviewer_texts_zh_has_chinese(self):
        self._assert_has_chinese(BRIDGE_REVIEWER_TEXTS, "bridge_reviewer_zh")

    def test_results_fallback_labels_zh_has_chinese(self):
        self._assert_has_chinese(RESULTS_FALLBACK_LABELS, "results_fallback_zh")


# ---------------------------------------------------------------------------
# TestBridgeSectionLabels
# ---------------------------------------------------------------------------

class TestBridgeSectionLabels:
    EXPECTED_KEYS = {
        "reference_traceability_contract",
        "calibration_execution_contract",
        "data_quality_contract",
        "uncertainty_budget_template",
        "coefficient_verification_contract",
        "evidence_traceability_contract",
        "reporting_contract",
    }

    def test_bridge_section_labels_has_all_keys(self):
        assert set(BRIDGE_SECTION_LABELS.keys()) == self.EXPECTED_KEYS

    def test_bridge_section_labels_en_has_all_keys(self):
        assert set(BRIDGE_SECTION_LABELS_EN.keys()) == self.EXPECTED_KEYS


# ---------------------------------------------------------------------------
# TestBridgeReviewerTexts
# ---------------------------------------------------------------------------

class TestBridgeReviewerTexts:
    EXPECTED_KEYS = {
        "status_ready_for_engineering_isolation",
        "status_step2_tail_in_progress",
        "status_blocked_before_stage3",
        "summary_text",
        "current_stage_text",
        "next_stage_ready",
        "next_stage_not_ready",
        "execute_now_prefix",
        "defer_to_stage3_prefix",
        "no_blocking",
        "blocking_prefix",
        "warning_prefix",
        "recommended_next_stage_prefix",
        "gate_status_defined",
        "gate_status_missing",
    }

    def test_bridge_reviewer_texts_has_all_keys(self):
        assert set(BRIDGE_REVIEWER_TEXTS.keys()) == self.EXPECTED_KEYS

    def test_bridge_reviewer_texts_en_has_all_keys(self):
        assert set(BRIDGE_REVIEWER_TEXTS_EN.keys()) == self.EXPECTED_KEYS

    def test_summary_text_not_real_acceptance(self):
        """Bridge summary must explicitly state it is not real acceptance evidence."""
        assert "real acceptance" in BRIDGE_REVIEWER_TEXTS["summary_text"].lower() or "不是 real acceptance" in BRIDGE_REVIEWER_TEXTS["summary_text"]


# ---------------------------------------------------------------------------
# TestResultsFallbackLabels
# ---------------------------------------------------------------------------

class TestResultsFallbackLabels:
    EXPECTED_KEYS = {
        "results_file", "generated", "missing", "sample_count",
        "point_summary_count", "artifact_roles", "config_safety",
        "evidence_source", "offline_diagnostic", "workbench_evidence",
    }

    def test_results_fallback_labels_has_all_keys(self):
        assert set(RESULTS_FALLBACK_LABELS.keys()) == self.EXPECTED_KEYS

    def test_results_fallback_labels_en_has_all_keys(self):
        assert set(RESULTS_FALLBACK_LABELS_EN.keys()) == self.EXPECTED_KEYS


# ---------------------------------------------------------------------------
# TestResolveHelpers
# ---------------------------------------------------------------------------

class TestResolveHelpers:
    def test_resolve_title_zh(self):
        assert resolve_phase_evidence_title("point_taxonomy_summary") == "点位语义摘要"

    def test_resolve_title_en(self):
        assert resolve_phase_evidence_title("point_taxonomy_summary", lang="en") == "Point Taxonomy Summary"

    def test_resolve_title_unknown_key(self):
        assert resolve_phase_evidence_title("nonexistent") == "nonexistent"

    def test_resolve_phase_term_zh(self):
        assert resolve_phase_term("flush_gate") == "冲洗门禁"

    def test_resolve_phase_term_en(self):
        assert resolve_phase_term("flush_gate", lang="en") == "Flush Gate"

    def test_resolve_bridge_section_label_zh(self):
        assert resolve_bridge_section_label("data_quality_contract") == "数据质量 contract"

    def test_resolve_bridge_section_label_en(self):
        assert resolve_bridge_section_label("data_quality_contract", lang="en") == "Data Quality Contract"

    def test_resolve_bridge_reviewer_text_zh(self):
        result = resolve_bridge_reviewer_text("no_blocking")
        assert result == "阻塞项：无。"

    def test_resolve_bridge_reviewer_text_en(self):
        result = resolve_bridge_reviewer_text("no_blocking", lang="en")
        assert result == "Blockers: none."

    def test_resolve_results_fallback_label_zh(self):
        assert resolve_results_fallback_label("sample_count") == "样本数"

    def test_resolve_results_fallback_label_en(self):
        assert resolve_results_fallback_label("sample_count", lang="en") == "Sample count"


# ---------------------------------------------------------------------------
# TestTerminologyConvergence
# ---------------------------------------------------------------------------

class TestTerminologyConvergence:
    """Verify review_center_scan_contracts uses the same PHASE_TERMS."""

    def test_v12_phase_display_terms_same_as_phase_terms(self):
        from gas_calibrator.v2.ui_v2.review_center_scan_contracts import (
            V12_PHASE_DISPLAY_TERMS,
            V12_PHASE_DISPLAY_TERMS_EN,
        )
        # Same object (identity check)
        assert V12_PHASE_DISPLAY_TERMS is PHASE_TERMS
        assert V12_PHASE_DISPLAY_TERMS_EN is PHASE_TERMS_EN

    def test_point_taxonomy_term_consistent(self):
        """point_taxonomy should be '点位语义' not '测点分类'."""
        assert PHASE_TERMS["point_taxonomy"] == "点位语义"


# ---------------------------------------------------------------------------
# TestI18NKeys
# ---------------------------------------------------------------------------

class TestI18NKeys:
    def test_i18n_keys_cover_all_artifact_keys(self):
        for key in PHASE_EVIDENCE_ARTIFACT_KEYS:
            assert key in PHASE_EVIDENCE_I18N_KEYS, f"Missing i18n key for {key}"

    def test_i18n_keys_format(self):
        for key, i18n_key in PHASE_EVIDENCE_I18N_KEYS.items():
            assert i18n_key.startswith("phase_evidence."), f"Bad i18n key format: {i18n_key}"


# ---------------------------------------------------------------------------
# TestStep2Boundary
# ---------------------------------------------------------------------------

class TestStep2Boundary:
    def test_evidence_source_simulated(self):
        assert PHASE_EVIDENCE_STEP2_BOUNDARY["evidence_source"] == "simulated"

    def test_not_real_acceptance_evidence(self):
        assert PHASE_EVIDENCE_STEP2_BOUNDARY["not_real_acceptance_evidence"] is True

    def test_not_ready_for_formal_claim(self):
        assert PHASE_EVIDENCE_STEP2_BOUNDARY["not_ready_for_formal_claim"] is True

    def test_reviewer_only(self):
        assert PHASE_EVIDENCE_STEP2_BOUNDARY["reviewer_only"] is True

    def test_readiness_mapping_only(self):
        assert PHASE_EVIDENCE_STEP2_BOUNDARY["readiness_mapping_only"] is True


# ---------------------------------------------------------------------------
# TestFormatterDisplayLabels (2.9)
# ---------------------------------------------------------------------------
class TestFormatterDisplayLabels:
    def test_formatter_labels_count(self):
        assert len(FORMATTER_DISPLAY_LABELS) == 20

    def test_formatter_labels_keys_match_en(self):
        assert set(FORMATTER_DISPLAY_LABELS.keys()) == set(FORMATTER_DISPLAY_LABELS_EN.keys())

    def test_formatter_labels_en_no_chinese(self):
        for key, value in FORMATTER_DISPLAY_LABELS_EN.items():
            assert not _has_chinese(value), f"formatter_en[{key!r}] contains Chinese: {value!r}"

    def test_formatter_labels_zh_has_chinese(self):
        for key, value in FORMATTER_DISPLAY_LABELS.items():
            assert _has_chinese(value), f"formatter_zh[{key!r}] has no Chinese: {value!r}"


# ---------------------------------------------------------------------------
# TestCompatibilityRowLabels (2.9)
# ---------------------------------------------------------------------------
class TestCompatibilityRowLabels:
    def test_compat_labels_count(self):
        assert len(COMPATIBILITY_ROW_LABELS) == 7

    def test_compat_labels_keys_match_en(self):
        assert set(COMPATIBILITY_ROW_LABELS.keys()) == set(COMPATIBILITY_ROW_LABELS_EN.keys())

    def test_compat_labels_en_no_chinese(self):
        for key, value in COMPATIBILITY_ROW_LABELS_EN.items():
            assert not _has_chinese(value), f"compat_en[{key!r}] contains Chinese: {value!r}"


# ---------------------------------------------------------------------------
# TestHistoricalRollupLabels (2.9)
# ---------------------------------------------------------------------------
class TestHistoricalRollupLabels:
    def test_historical_labels_count(self):
        assert len(HISTORICAL_ROLLUP_LABELS) == 4

    def test_historical_labels_keys_match_en(self):
        assert set(HISTORICAL_ROLLUP_LABELS.keys()) == set(HISTORICAL_ROLLUP_LABELS_EN.keys())

    def test_historical_labels_en_no_chinese(self):
        for key, value in HISTORICAL_ROLLUP_LABELS_EN.items():
            assert not _has_chinese(value), f"historical_en[{key!r}] contains Chinese: {value!r}"


# ---------------------------------------------------------------------------
# TestMeasurementDigestLabels (2.9)
# ---------------------------------------------------------------------------
class TestMeasurementDigestLabels:
    def test_measurement_labels_count(self):
        assert len(MEASUREMENT_DIGEST_LABELS) == 17

    def test_measurement_labels_keys_match_en(self):
        assert set(MEASUREMENT_DIGEST_LABELS.keys()) == set(MEASUREMENT_DIGEST_LABELS_EN.keys())

    def test_measurement_labels_en_no_chinese(self):
        for key, value in MEASUREMENT_DIGEST_LABELS_EN.items():
            assert not _has_chinese(value), f"measurement_en[{key!r}] contains Chinese: {value!r}"


# ---------------------------------------------------------------------------
# TestReadinessDigestLabels (2.9)
# ---------------------------------------------------------------------------
class TestReadinessDigestLabels:
    def test_readiness_labels_count(self):
        assert len(READINESS_DIGEST_LABELS) == 17

    def test_readiness_labels_keys_match_en(self):
        assert set(READINESS_DIGEST_LABELS.keys()) == set(READINESS_DIGEST_LABELS_EN.keys())

    def test_readiness_labels_en_no_chinese(self):
        for key, value in READINESS_DIGEST_LABELS_EN.items():
            assert not _has_chinese(value), f"readiness_en[{key!r}] contains Chinese: {value!r}"


# ---------------------------------------------------------------------------
# TestNewResolveHelpers (2.9)
# ---------------------------------------------------------------------------
class TestNewResolveHelpers:
    def test_resolve_formatter_label_zh(self):
        assert resolve_formatter_label("artifacts") == "工件"

    def test_resolve_formatter_label_en(self):
        assert resolve_formatter_label("artifacts", lang="en") == "Artifacts"

    def test_resolve_compat_row_label_zh(self):
        assert resolve_compatibility_row_label("version") == "版本"

    def test_resolve_compat_row_label_en(self):
        assert resolve_compatibility_row_label("version", lang="en") == "Version"

    def test_resolve_historical_rollup_label_zh(self):
        assert resolve_historical_rollup_label("scope_run_count") == "认可范围包运行数"

    def test_resolve_historical_rollup_label_en(self):
        assert resolve_historical_rollup_label("scope_run_count", lang="en") == "Scope package run count"

    def test_resolve_measurement_digest_label_zh(self):
        assert resolve_measurement_digest_label("blockers") == "当前阻塞"

    def test_resolve_measurement_digest_label_en(self):
        assert resolve_measurement_digest_label("blockers", lang="en") == "Blockers"

    def test_resolve_readiness_digest_label_zh(self):
        assert resolve_readiness_digest_label("uncertainty_overview") == "不确定度概览"

    def test_resolve_readiness_digest_label_en(self):
        assert resolve_readiness_digest_label("uncertainty_overview", lang="en") == "Uncertainty overview"


# ---------------------------------------------------------------------------
# TestFormatterUsesContracts (2.9)
# ---------------------------------------------------------------------------
class TestFormatterUsesContracts:
    """Verify review_surface_formatter uses contracts for key labels."""

    def test_offline_diagnostic_display_labels_from_contracts(self):
        from gas_calibrator.v2.review_surface_formatter import _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS
        assert _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS["artifacts"] == FORMATTER_DISPLAY_LABELS["artifacts"]
        assert _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS["primary"] == FORMATTER_DISPLAY_LABELS["primary"]

    def test_review_center_coverage_labels_from_contracts(self):
        from gas_calibrator.v2.review_surface_formatter import _REVIEW_CENTER_COVERAGE_LABELS
        assert _REVIEW_CENTER_COVERAGE_LABELS["coverage"] == FORMATTER_DISPLAY_LABELS["coverage"]
        assert _REVIEW_CENTER_COVERAGE_LABELS["missing"] == FORMATTER_DISPLAY_LABELS["missing_label"]

    def test_review_surface_fragment_labels_from_contracts(self):
        from gas_calibrator.v2.review_surface_formatter import _REVIEW_SURFACE_FRAGMENT_LABELS
        assert _REVIEW_SURFACE_FRAGMENT_LABELS["catalog"] == FORMATTER_DISPLAY_LABELS["catalog"]
        assert _REVIEW_SURFACE_FRAGMENT_LABELS["degraded"] == FORMATTER_DISPLAY_LABELS["degraded"]

    def test_no_gaps_from_contracts(self):
        from gas_calibrator.v2.review_surface_formatter import humanize_review_center_coverage_text
        result = humanize_review_center_coverage_text("no gaps")
        assert result == FORMATTER_DISPLAY_LABELS["no_gaps"]
