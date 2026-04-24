from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.core.cutover_candidate_worksheet import (
    CONCLUSION_DRY_RUN_PREP_ALLOWED,
    CONCLUSION_P0_BLOCKED,
    CUTOVER_WORKSHEET_FILENAME,
    CUTOVER_WORKSHEET_MARKDOWN_FILENAME,
    FREEZE_CHECK_SUMMARY_FILENAME,
    ROLLBACK_GUARD_FILENAME,
    STATUS_GREEN,
    STATUS_RED,
    STATUS_YELLOW,
    build_cutover_candidate_worksheet,
    build_rollback_guard,
    build_v1_freeze_check,
    is_forbidden_v1_change,
    render_rollback_guard_markdown,
    render_worksheet_markdown,
    write_cutover_candidate_artifacts,
)


def test_readiness_items_have_required_red_yellow_green_fields() -> None:
    payload = build_cutover_candidate_worksheet(
        changed_paths=[
            "src/gas_calibrator/v2/core/cutover_candidate_worksheet.py",
            "tests/v2/test_cutover_candidate_worksheet.py",
            "docs/architecture/v2_cutover_candidate_rollback_guard.md",
        ],
        generated_at="2026-04-24T10:00:00+00:00",
    )

    items = list(payload["readiness_items"])
    keys = {str(item["key"]) for item in items}
    assert keys == {
        "smoke",
        "safe",
        "route_trace",
        "fit_ready",
        "completed_progress_semantics",
        "headless_entry",
        "h2o_single_route_readiness",
        "co2_single_route_readiness",
        "single_temperature_group_readiness",
        "route_trace_diff_readiness",
        "narrowed_skip0_replacement_readiness",
        "rollback_strategy_ready",
        "v1_remains_frozen",
    }
    for item in items:
        assert item["status"] in {STATUS_GREEN, STATUS_YELLOW, STATUS_RED}
        assert item["evidence_path"]
        assert item["reason"]
        assert item["remaining_blocker"]


def test_worksheet_allows_dry_run_preparation_without_real_write_claim() -> None:
    payload = build_cutover_candidate_worksheet(
        changed_paths=["src/gas_calibrator/v2/scripts/build_cutover_candidate_worksheet.py"],
        generated_at="2026-04-24T10:00:00+00:00",
    )

    assert payload["selected_conclusion"] == CONCLUSION_DRY_RUN_PREP_ALLOWED
    assert payload["boundary"]["default_entry_remains_v1"] is True
    assert payload["boundary"]["v2_replaces_v1"] is False
    assert payload["boundary"]["cutover_ready"] is False
    assert payload["boundary"]["real_write_allowed"] is False
    assert payload["boundary"]["real_com_serial_allowed"] is False
    assert payload["boundary"]["real_acceptance_allowed"] is False
    assert payload["boundary"]["real_primary_latest_refresh_allowed"] is False
    assert payload["third_batch"]["scope"] == "narrowed_skip0_co2_only"
    assert payload["third_batch"]["cutover_ready"] is False


def test_v1_freeze_check_catches_forbidden_paths() -> None:
    assert is_forbidden_v1_change("run_app.py") is True
    assert is_forbidden_v1_change("src/gas_calibrator/workflow/runner.py") is True
    assert is_forbidden_v1_change("src/gas_calibrator/ui/main.py") is True
    assert is_forbidden_v1_change("src/gas_calibrator/devices/analyzer.py") is True
    assert is_forbidden_v1_change("docs/v1/v1_config_reference.md") is True
    assert is_forbidden_v1_change("src/gas_calibrator/v2/core/foo.py") is False

    check = build_v1_freeze_check(
        [
            "src/gas_calibrator/v2/core/foo.py",
            "src/gas_calibrator/devices/analyzer.py",
        ]
    )

    assert check["status"] == STATUS_RED
    assert check["v1_remains_frozen"] is False
    assert check["forbidden_changed_paths"] == ["src/gas_calibrator/devices/analyzer.py"]


def test_worksheet_blocks_when_v1_freeze_check_is_red() -> None:
    payload = build_cutover_candidate_worksheet(
        changed_paths=["run_app.py"],
        generated_at="2026-04-24T10:00:00+00:00",
    )

    assert payload["selected_conclusion"] == CONCLUSION_P0_BLOCKED
    frozen = [item for item in payload["readiness_items"] if item["key"] == "v1_remains_frozen"][0]
    assert frozen["status"] == STATUS_RED
    assert frozen["blocks_dry_run_preparation"] is True


def test_rollback_guard_contains_required_sop_sections() -> None:
    guard = build_rollback_guard()

    assert guard["default_entry"]["status"] == "v1_remains_default"
    assert guard["default_entry"]["change_allowed"] is False
    assert guard["future_dry_run_boundary"]["real_write_allowed"] is False
    assert guard["future_dry_run_boundary"]["real_com_open_allowed_in_this_batch"] is False
    assert guard["preserve_v1_baselines"]
    assert guard["rollback_sensitive_files"]
    assert guard["rollback_triggers"]
    assert guard["rollback_steps"]
    assert guard["post_rollback_verification"]
    assert guard["prohibited_actions"]


def test_markdown_renderers_keep_boundary_visible() -> None:
    payload = build_cutover_candidate_worksheet(
        changed_paths=["tests/v2/test_cutover_candidate_worksheet.py"],
        generated_at="2026-04-24T10:00:00+00:00",
    )
    worksheet_md = render_worksheet_markdown(payload)
    rollback_md = render_rollback_guard_markdown(payload)

    assert "| Item | Status | Evidence | Reason | Remaining blocker |" in worksheet_md
    assert "Real write" in worksheet_md or "real write" in worksheet_md
    assert "not declared a formal replacement for V1" in worksheet_md
    assert "Do not switch the default entry to V2" in rollback_md
    assert "Do not refresh real_primary_latest" in rollback_md


def test_write_cutover_candidate_artifacts(tmp_path: Path) -> None:
    payload = build_cutover_candidate_worksheet(
        changed_paths=["src/gas_calibrator/v2/core/cutover_candidate_worksheet.py"],
        generated_at="2026-04-24T10:00:00+00:00",
    )
    written = write_cutover_candidate_artifacts(tmp_path, payload)

    assert set(written) == {
        "worksheet_json",
        "worksheet_markdown",
        "rollback_guard",
        "freeze_check_summary",
    }
    assert (tmp_path / CUTOVER_WORKSHEET_FILENAME).exists()
    assert (tmp_path / CUTOVER_WORKSHEET_MARKDOWN_FILENAME).exists()
    assert (tmp_path / ROLLBACK_GUARD_FILENAME).exists()
    assert (tmp_path / FREEZE_CHECK_SUMMARY_FILENAME).exists()
    worksheet = json.loads((tmp_path / CUTOVER_WORKSHEET_FILENAME).read_text(encoding="utf-8"))
    freeze = json.loads((tmp_path / FREEZE_CHECK_SUMMARY_FILENAME).read_text(encoding="utf-8"))
    assert worksheet["artifact_type"] == "v2_cutover_candidate_worksheet"
    assert freeze["artifact_type"] == "v1_freeze_check_summary"
