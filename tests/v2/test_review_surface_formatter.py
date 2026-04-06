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
