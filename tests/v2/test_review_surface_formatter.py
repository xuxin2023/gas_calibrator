from __future__ import annotations

from gas_calibrator.v2.review_surface_formatter import (
    build_offline_diagnostic_detail_line,
    collect_offline_diagnostic_detail_lines,
    humanize_review_center_coverage_text,
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
