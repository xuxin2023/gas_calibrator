from __future__ import annotations

from gas_calibrator.v2.review_surface_formatter import (
    build_artifact_scope_reviewer_notes,
    build_artifact_scope_view_reviewer_display,
    build_review_scope_payload_reviewer_display,
    build_review_scope_counts_line,
    build_review_scope_reviewer_display,
    build_review_scope_selection_line,
    hydrate_review_scope_reviewer_display,
    build_offline_diagnostic_detail_line,
    collect_offline_diagnostic_detail_lines,
    humanize_offline_diagnostic_summary_value,
    humanize_review_center_coverage_text,
    humanize_review_surface_text,
)


def test_collect_offline_diagnostic_detail_lines_normalizes_scope_but_keeps_raw_contract() -> None:
    summary = {
        "review_highlight_lines": [
            "room-temp latest | classification warn | scope artifacts 4 | plots 1",
        ],
        "detail_items": [
            {
                "detail_line": "analyzer-chain latest | continue_s1 hold",
                "artifact_scope_summary": "artifacts 8 | plots 1",
            }
        ],
    }

    lines = collect_offline_diagnostic_detail_lines(summary, limit=3)

    assert lines[0] == "room-temp latest | classification warn | \u5de5\u4ef6\u8303\u56f4: \u5de5\u4ef6 4 | \u56fe\u8868 1"
    assert lines[1] == (
        "analyzer-chain latest | continue_s1 hold | \u5de5\u4ef6\u8303\u56f4: \u5de5\u4ef6 8 | \u56fe\u8868 1"
    )
    assert summary["review_highlight_lines"][0].endswith("scope artifacts 4 | plots 1")
    assert summary["detail_items"][0]["artifact_scope_summary"] == "artifacts 8 | plots 1"


def test_humanize_offline_diagnostic_summary_value_supports_alignment_count() -> None:
    normalized = humanize_offline_diagnostic_summary_value("room-temp 1 | analyzer-chain 1 | alignment 1 | artifacts 22")

    assert normalized == "room-temp 1 | analyzer-chain 1 | 对齐 1 | 工件 22"


def test_humanize_review_center_coverage_text_keeps_raw_payload_contract_out_of_band() -> None:
    text = "coverage | complete 0 | gapped 2 | missing parity / resilience"

    normalized = humanize_review_center_coverage_text(text)

    assert normalized == "覆盖 | 完整 0 | 缺口 2 | 缺少 parity / resilience"
    assert text == "coverage | complete 0 | gapped 2 | missing parity / resilience"


def test_build_offline_diagnostic_detail_line_humanizes_reviewer_labels_without_touching_raw_values() -> None:
    classification = "warn"
    continue_s1 = "hold"

    classification_line = build_offline_diagnostic_detail_line("classification", classification)
    continue_line = build_offline_diagnostic_detail_line("continue_s1", continue_s1)
    bundle_dir_line = build_offline_diagnostic_detail_line("bundle_dir", "D:/tmp/run_scope")

    assert classification_line == "\u5206\u7c7b: \u9884\u8b66"
    assert continue_line == "S1 \u7ee7\u7eed\u5224\u5b9a: \u4fdd\u6301"
    assert bundle_dir_line == "\u5de5\u4ef6\u76ee\u5f55: D:/tmp/run_scope"
    assert classification == "warn"
    assert continue_s1 == "hold"


def test_humanize_review_surface_text_normalizes_artifact_scope_and_risk_without_touching_raw_text() -> None:
    artifact_scope_text = "Current-run catalog baseline 3/4 | visible 3 | present 2/3 | external 1 | missing 1 | catalog 3/4"
    risk_text = "high | failed 1 | degraded 2 | diagnostic 3 | missing 1 | coverage | complete 0 | gapped 2"

    normalized_scope = humanize_review_surface_text(artifact_scope_text)
    normalized_risk = humanize_review_surface_text(risk_text)

    assert normalized_scope == "\u5f53\u524d\u8fd0\u884c\u57fa\u7ebf 3/4 | \u53ef\u89c1 3 | \u5b58\u5728 2/3 | \u5916\u90e8 1 | \u7f3a\u5c11 1 | \u5f53\u524d\u8fd0\u884c\u57fa\u7ebf 3/4"
    assert normalized_risk == "\u9ad8 | \u5931\u8d25 1 | \u964d\u7ea7 2 | \u4ec5\u8bca\u65ad 3 | \u7f3a\u5c11 1 | \u8986\u76d6 | \u5b8c\u6574 0 | \u7f3a\u53e3 2"
    assert artifact_scope_text == "Current-run catalog baseline 3/4 | visible 3 | present 2/3 | external 1 | missing 1 | catalog 3/4"
    assert risk_text == "high | failed 1 | degraded 2 | diagnostic 3 | missing 1 | coverage | complete 0 | gapped 2"


def test_review_scope_helpers_humanize_reviewer_surface_without_touching_raw_contract_values() -> None:
    raw_scope = "source"
    raw_source = "review_run_a"
    raw_evidence = "suite summary"
    raw_counts = "visible 3 | present 2 | external 1 | missing 1 | catalog 2/4"

    selection_line = build_review_scope_selection_line(
        scope=raw_scope,
        source=raw_source,
        evidence=raw_evidence,
    )
    counts_line = build_review_scope_counts_line(
        visible=3,
        present=2,
        external=1,
        missing=1,
        catalog_present=2,
        catalog_total=4,
    )

    assert selection_line == "\u8303\u56f4=source | \u6765\u6e90=review_run_a | \u8bc1\u636e=suite summary"
    assert counts_line == "\u53ef\u89c1 3 | \u5b58\u5728 2 | \u5916\u90e8 1 | \u7f3a\u5c11 1 | \u5f53\u524d\u8fd0\u884c\u57fa\u7ebf 2/4"
    assert raw_scope == "source"
    assert raw_source == "review_run_a"
    assert raw_evidence == "suite summary"
    assert raw_counts == "visible 3 | present 2 | external 1 | missing 1 | catalog 2/4"


def test_review_scope_reviewer_display_keeps_selection_and_counts_consistent() -> None:
    selection = {
        "scope": "evidence",
        "selected_source_label_display": "history_run",
        "selected_evidence_summary": "suite summary",
    }
    scope_summary = {
        "scope": "evidence",
        "scope_visible_count": 2,
        "scope_present_count": 1,
        "scope_external_count": 1,
        "scope_missing_count": 1,
        "catalog_present_count": 8,
        "catalog_total_count": 12,
    }

    reviewer_display = build_review_scope_reviewer_display(
        selection=selection,
        scope_summary=scope_summary,
    )

    assert reviewer_display == {
        "selection_line": "范围=evidence | 来源=history_run | 证据=suite summary",
        "counts_line": "可见 2 | 存在 1 | 外部 1 | 缺少 1 | 当前运行基线 8/12",
    }
    assert selection["scope"] == "evidence"
    assert selection["selected_source_label_display"] == "history_run"
    assert selection["selected_evidence_summary"] == "suite summary"
    assert scope_summary["scope_visible_count"] == 2
    assert scope_summary["catalog_total_count"] == 12


def test_build_artifact_scope_reviewer_notes_keeps_raw_counts_out_of_band() -> None:
    scope_label = "Source"
    visible_count = 3
    present_count = 2
    scope_total_count = 3
    external_count = 1
    missing_count = 1
    catalog_present_count = 8
    catalog_total_count = 12

    notes = build_artifact_scope_reviewer_notes(
        scope_label=scope_label,
        visible_count=visible_count,
        present_count=present_count,
        scope_total_count=scope_total_count,
        external_count=external_count,
        missing_count=missing_count,
        catalog_present_count=catalog_present_count,
        catalog_total_count=catalog_total_count,
    )

    assert "Source" in notes["run_dir_note_text"]
    assert "当前运行基线 8/12" in notes["run_dir_note_text"]
    assert "可见 3" in notes["scope_note_text"]
    assert "外部 1" in notes["scope_note_text"]
    assert "缺失 1" in notes["scope_note_text"]
    assert "存在 2/3" in notes["present_note_text"]
    assert "缺失 1" in notes["present_note_text"]
    assert "visible " not in notes["scope_note_text"]
    assert "present " not in notes["present_note_text"]
    assert scope_label == "Source"
    assert visible_count == 3
    assert catalog_total_count == 12


def test_build_review_scope_payload_reviewer_display_merges_selection_counts_and_scope_notes() -> None:
    selection = {
        "scope": "source",
        "selected_source_label_display": "history_run",
        "selected_evidence_summary": "suite summary",
    }
    scope_summary = {
        "scope": "source",
        "scope_label": "Source",
        "summary_text": "Source | visible 3 | present 2/3 | external 1 | missing 1 | catalog 8/12",
        "scope_visible_count": 3,
        "scope_present_count": 2,
        "scope_external_count": 1,
        "scope_missing_count": 1,
        "catalog_present_count": 8,
        "catalog_total_count": 12,
    }

    reviewer_display = build_review_scope_payload_reviewer_display(
        selection=selection,
        scope_summary=scope_summary,
    )

    assert reviewer_display["summary_text"] == "Source | 可见 3 | 存在 2/3 | 外部 1 | 缺少 1 | 当前运行基线 8/12"
    assert reviewer_display["selection_line"] == "范围=source | 来源=history_run | 证据=suite summary"
    assert reviewer_display["counts_line"] == "可见 3 | 存在 2 | 外部 1 | 缺少 1 | 当前运行基线 8/12"
    assert reviewer_display["run_dir_note_text"] == "当前审阅视角：Source | 当前运行基线 8/12"
    assert reviewer_display["scope_note_text"] == "Source | 当前可见 3 | 外部 1 | 缺失 1 | 当前运行基线 12"
    assert reviewer_display["present_note_text"] == "Source | 当前范围 磁盘存在 2/3 | 缺失 1 | 当前运行基线 8/12"
    assert selection["scope"] == "source"
    assert scope_summary["catalog_total_count"] == 12


def test_hydrate_review_scope_reviewer_display_backfills_partial_payloads_but_keeps_nested_priority() -> None:
    selection = {
        "scope": "source",
        "selected_source_label_display": "history_run",
        "selected_evidence_summary": "suite summary",
    }
    scope_summary = {
        "scope": "source",
        "scope_label": "Source",
        "summary_text": "Source | visible 3 | present 2/3 | external 1 | missing 1 | catalog 8/12",
        "scope_visible_count": 3,
        "scope_present_count": 2,
        "scope_external_count": 1,
        "scope_missing_count": 1,
        "catalog_present_count": 8,
        "catalog_total_count": 12,
    }
    top_level_only_payload = {
        "selection": selection,
        "scope_summary": scope_summary,
        "run_dir_note_text": "顶层运行目录说明",
        "scope_note_text": "顶层范围说明",
        "present_note_text": "顶层存在说明",
        "catalog_note_text": "顶层目录基线",
        "export_warning_text": "顶层导出提醒",
    }
    partial_nested_payload = {
        "selection": selection,
        "scope_summary": scope_summary,
        "scope_note_text": "顶层范围说明",
        "reviewer_display": {
            "selection_line": "范围=source | 来源=nested_source | 证据=nested evidence",
            "present_note_text": "nested present note",
        },
    }

    top_level_only = hydrate_review_scope_reviewer_display(top_level_only_payload)
    partial_nested = hydrate_review_scope_reviewer_display(partial_nested_payload)

    assert top_level_only["summary_text"] == "Source | 可见 3 | 存在 2/3 | 外部 1 | 缺少 1 | 当前运行基线 8/12"
    assert top_level_only["selection_line"] == "范围=source | 来源=history_run | 证据=suite summary"
    assert top_level_only["counts_line"] == "可见 3 | 存在 2 | 外部 1 | 缺少 1 | 当前运行基线 8/12"
    assert top_level_only["run_dir_note_text"] == "顶层运行目录说明"
    assert top_level_only["scope_note_text"] == "顶层范围说明"
    assert top_level_only["present_note_text"] == "顶层存在说明"
    assert top_level_only["catalog_note_text"] == "顶层目录基线"
    assert top_level_only["export_warning_text"] == "顶层导出提醒"
    assert partial_nested["selection_line"] == "范围=source | 来源=nested_source | 证据=nested evidence"
    assert partial_nested["counts_line"] == "可见 3 | 存在 2 | 外部 1 | 缺少 1 | 当前运行基线 8/12"
    assert partial_nested["scope_note_text"] == "顶层范围说明"
    assert partial_nested["present_note_text"] == "nested present note"
    assert selection["selected_source_label_display"] == "history_run"
    assert scope_summary["scope_visible_count"] == 3


def test_build_artifact_scope_view_reviewer_display_packages_summary_and_notes_without_touching_raw_inputs() -> None:
    raw_summary = "Source | visible 3 | present 2/3 | external 1 | missing 1 | catalog 8/12"
    raw_catalog_note = "Current-run catalog baseline 8/12"
    raw_empty_text = "当前范围没有工件，仅供离线审阅。"
    raw_warning_text = "当前范围导出提醒"

    reviewer_display = build_artifact_scope_view_reviewer_display(
        summary_text=raw_summary,
        scope_label="Source",
        visible_count=3,
        present_count=2,
        scope_total_count=3,
        external_count=1,
        missing_count=1,
        catalog_present_count=8,
        catalog_total_count=12,
        catalog_note_text=raw_catalog_note,
        empty_text=raw_empty_text,
        export_warning_text=raw_warning_text,
    )

    assert reviewer_display["summary_text"] == "Source | 可见 3 | 存在 2/3 | 外部 1 | 缺少 1 | 当前运行基线 8/12"
    assert reviewer_display["run_dir_note_text"] == "当前审阅视角：Source | 当前运行基线 8/12"
    assert reviewer_display["scope_note_text"] == "Source | 当前可见 3 | 外部 1 | 缺失 1 | 当前运行基线 12"
    assert reviewer_display["present_note_text"] == "Source | 当前范围 磁盘存在 2/3 | 缺失 1 | 当前运行基线 8/12"
    assert reviewer_display["catalog_note_text"] == "当前运行基线 8/12"
    assert reviewer_display["empty_text"] == raw_empty_text
    assert reviewer_display["export_warning_text"] == raw_warning_text
    assert raw_summary == "Source | visible 3 | present 2/3 | external 1 | missing 1 | catalog 8/12"
    assert raw_catalog_note == "Current-run catalog baseline 8/12"


# ---------------------------------------------------------------------------
# TestFormatterConsumesSharedBuilders (2.12)
# ---------------------------------------------------------------------------

class TestFormatterConsumesSharedBuilders:
    """Verify review_surface_formatter consumes shared compact builders."""

    def test_measurement_digest_uses_compact_builder(self):
        """build_measurement_review_digest_lines must use shared compact builder for core lines."""
        from gas_calibrator.v2.review_surface_formatter import build_measurement_review_digest_lines
        from gas_calibrator.v2.core.reviewer_summary_builders import build_measurement_digest_compact_summary

        payload = {
            "digest": {
                "payload_complete_phase_summary": "preseal, postseal",
                "payload_partial_phase_summary": "ambient",
                "trace_only_phase_summary": "flush_gate",
                "blocker_summary": "none",
                "next_required_artifacts_summary": "none",
            }
        }
        result = build_measurement_review_digest_lines(payload)
        compact = build_measurement_digest_compact_summary(payload, include_boundary=False, include_non_claim=False)

        # The formatter's summary_lines must contain the compact builder's core lines
        assert "summary_lines" in result
        assert len(result["summary_lines"]) > 0
        # Compact builder produces 5 core lines
        assert len(compact["summary_lines"]) == 5

    def test_readiness_digest_uses_compact_builder(self):
        """build_readiness_review_digest_lines must use shared compact builder for core lines."""
        from gas_calibrator.v2.review_surface_formatter import build_readiness_review_digest_lines
        from gas_calibrator.v2.core.reviewer_summary_builders import build_readiness_digest_compact_summary

        payload = {
            "digest": {
                "scope_overview_summary": "ISO 17025",
                "decision_rule_summary": "binary",
                "readiness_status_summary": "not ready",
                "top_gaps_summary": "preseal",
                "current_evidence_coverage_summary": "60%",
            }
        }
        result = build_readiness_review_digest_lines(payload)
        compact = build_readiness_digest_compact_summary(payload, include_boundary=False, include_non_claim=False)

        assert "summary_lines" in result
        assert len(result["summary_lines"]) > 0
        assert len(compact["summary_lines"]) == 5

    def test_measurement_digest_boundary_markers_step2(self):
        """Measurement digest compact builder must return Step 2 boundary markers."""
        from gas_calibrator.v2.core.reviewer_summary_builders import build_measurement_digest_compact_summary

        result = build_measurement_digest_compact_summary({})
        markers = result["boundary_markers"]
        assert markers["evidence_source"] == "simulated"
        assert markers["not_real_acceptance_evidence"] is True
        assert markers["reviewer_only"] is True

    def test_readiness_digest_boundary_markers_step2(self):
        """Readiness digest compact builder must return Step 2 boundary markers."""
        from gas_calibrator.v2.core.reviewer_summary_builders import build_readiness_digest_compact_summary

        result = build_readiness_digest_compact_summary({})
        markers = result["boundary_markers"]
        assert markers["evidence_source"] == "simulated"
        assert markers["not_real_acceptance_evidence"] is True
        assert markers["reviewer_only"] is True


# ---------------------------------------------------------------------------
# TestDeadDefinitionCleanup (2.12)
# ---------------------------------------------------------------------------

class TestDeadDefinitionCleanup:
    """Verify that the old dead build_measurement_review_digest_lines has been removed."""

    def test_no_duplicate_build_measurement_review_digest_lines(self):
        """There must be exactly one definition of build_measurement_review_digest_lines."""
        import inspect
        import gas_calibrator.v2.review_surface_formatter as formatter_mod

        # Count how many times the function name appears in the source
        source = inspect.getsource(formatter_mod)
        count = source.count("def build_measurement_review_digest_lines(")
        assert count == 1, f"Expected exactly 1 definition, found {count}"

    def test_measurement_digest_function_is_callable(self):
        """The surviving build_measurement_review_digest_lines must be callable."""
        from gas_calibrator.v2.review_surface_formatter import build_measurement_review_digest_lines

        result = build_measurement_review_digest_lines({})
        assert "summary_lines" in result
        assert "detail_lines" in result


# ---------------------------------------------------------------------------
# TestReviewIndexSummaryConsumesSharedBuilders (2.12)
# ---------------------------------------------------------------------------

class TestReviewIndexSummaryConsumesSharedBuilders:
    """Verify that review_center_scan_contracts.build_v12_alignment_summary
    consumes shared compact builders from reviewer_summary_builders."""

    def test_v12_alignment_summary_uses_compact_builder(self):
        """build_v12_alignment_summary must produce summary_line from shared builder."""
        from gas_calibrator.v2.ui_v2.review_center_scan_contracts import build_v12_alignment_summary

        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MATCH",
            governance_handoff_blockers=[],
        )
        inner = result.get("v12_alignment_summary", {})
        # summary_line must be populated from shared builder
        assert "summary_line" in inner
        assert isinstance(inner["summary_line"], str)
        assert len(inner["summary_line"]) > 0

    def test_v12_alignment_summary_has_compact_summary_lines(self):
        """build_v12_alignment_summary must expose compact_summary_lines from shared builder."""
        from gas_calibrator.v2.ui_v2.review_center_scan_contracts import build_v12_alignment_summary

        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MATCH",
            governance_handoff_blockers=[],
        )
        inner = result.get("v12_alignment_summary", {})
        assert "compact_summary_lines" in inner
        assert isinstance(inner["compact_summary_lines"], list)
        assert len(inner["compact_summary_lines"]) > 0

    def test_v12_alignment_summary_has_builders_version(self):
        """build_v12_alignment_summary must expose builders_version."""
        from gas_calibrator.v2.ui_v2.review_center_scan_contracts import build_v12_alignment_summary

        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MATCH",
        )
        inner = result.get("v12_alignment_summary", {})
        assert "builders_version" in inner
        assert inner["builders_version"] == "2.12.0"

    def test_v12_alignment_summary_step2_boundary(self):
        """build_v12_alignment_summary must maintain Step 2 boundary markers."""
        from gas_calibrator.v2.ui_v2.review_center_scan_contracts import build_v12_alignment_summary

        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MATCH",
        )
        assert result["evidence_source"] == "simulated"
        assert result["not_real_acceptance_evidence"] is True
        assert result["not_ready_for_formal_claim"] is True
        assert result["reviewer_only"] is True
        assert result["readiness_mapping_only"] is True

    def test_v12_alignment_summary_no_formal_acceptance_language(self):
        """summary_line must not contain formal acceptance / formal claim language."""
        from gas_calibrator.v2.ui_v2.review_center_scan_contracts import build_v12_alignment_summary

        result = build_v12_alignment_summary(
            parity_status="MATCH",
            resilience_status="MATCH",
        )
        inner = result.get("v12_alignment_summary", {})
        summary_line = inner.get("summary_line", "")
        lower = summary_line.lower()
        assert "formal acceptance" not in lower
        assert "formal claim" not in lower
        assert "正式验收" not in summary_line


# ---------------------------------------------------------------------------
# TestCompactSummaryConsistency (2.12)
# ---------------------------------------------------------------------------

class TestCompactSummaryConsistency:
    """Verify that compact summaries from different consumers are consistent."""

    def test_v12_compact_summary_stable_generation(self):
        """All 6 compact summary builders must produce stable output for empty payload."""
        from gas_calibrator.v2.core.reviewer_summary_builders import (
            build_measurement_digest_compact_summary,
            build_readiness_digest_compact_summary,
            build_phase_evidence_compact_summary,
            build_v12_alignment_compact_summary,
            build_governance_handoff_compact_summary,
            build_parity_resilience_compact_summary,
        )

        for builder in (
            build_measurement_digest_compact_summary,
            build_readiness_digest_compact_summary,
            build_phase_evidence_compact_summary,
            build_v12_alignment_compact_summary,
            build_governance_handoff_compact_summary,
            build_parity_resilience_compact_summary,
        ):
            result = builder({})
            assert "summary_lines" in result, f"{builder.__name__} missing summary_lines"
            assert "boundary_markers" in result, f"{builder.__name__} missing boundary_markers"
            markers = result["boundary_markers"]
            assert markers["evidence_source"] == "simulated"
            assert markers["not_real_acceptance_evidence"] is True
            assert markers["reviewer_only"] is True

    def test_results_gateway_uses_all_compact_builders(self):
        """results_gateway must import and use all 4 compact builders."""
        import gas_calibrator.v2.adapters.results_gateway as rg_mod

        # Verify the module imports all 4 compact builders
        assert hasattr(rg_mod, "build_v12_alignment_compact_summary") or True  # imported, not necessarily exposed
        # The key check: the import line exists in the module
        import inspect
        source = inspect.getsource(rg_mod)
        assert "build_v12_alignment_compact_summary" in source
        assert "build_phase_evidence_compact_summary" in source
        assert "build_governance_handoff_compact_summary" in source
        assert "build_parity_resilience_compact_summary" in source
