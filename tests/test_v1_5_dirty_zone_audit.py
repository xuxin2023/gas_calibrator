import json

from gas_calibrator.tools.export_v1_5_dirty_zone_audit import main as export_main
from gas_calibrator.validation.v1_5_dirty_zone_audit import (
    build_dirty_zone_audit,
    parse_git_status_short,
    render_dirty_zone_audit_markdown,
)


def test_clean_handoff_entries_are_retained_but_not_package_inputs():
    entries = parse_git_status_short(
        "?? _handoff/V1_5_INITIALIZATION_DUAL_ALGORITHM_SOP_20260627.md\n",
        workspace="clean_worktree",
    )

    assert len(entries) == 1
    entry = entries[0]
    assert entry.category == "clean_handoff_evidence_retained"
    assert entry.severity == "info"
    assert entry.allowed_in_v1_5_package is False
    assert entry.action == "keep_untracked_do_not_stage_into_code_package"


def test_clean_staged_handoff_entries_are_blockers():
    entries = parse_git_status_short(
        "A  _handoff/V1_5_INITIALIZATION_DUAL_ALGORITHM_SOP_20260627.md\n",
        workspace="clean_worktree",
    )

    assert len(entries) == 1
    entry = entries[0]
    assert entry.category == "clean_staged_handoff_blocker"
    assert entry.severity == "blocker"
    assert entry.allowed_in_v1_5_package is False
    assert entry.action == "unstage_keep_as_traceability_evidence"


def test_clean_staged_forbidden_entrypoints_are_blockers(tmp_path):
    audit = build_dirty_zone_audit(
        clean_worktree=tmp_path / "clean",
        root_workspace=tmp_path / "root",
        clean_status_text=(
            "M  src/gas_calibrator/tools/run_v1_corrected_autodelivery.py\n"
            "M  src/gas_calibrator/v2/runner.py\n"
            "M  src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py\n"
            "M  src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py\n"
            "M  src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py\n"
        ),
        root_status_text="",
    )
    by_path = {entry.path: entry for entry in audit.entries}

    assert audit.status == "blocked"
    assert audit.summary["blocker_count"] == 5
    assert by_path["src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"].category == (
        "clean_staged_legacy_v1_entrypoint_blocker"
    )
    assert by_path["src/gas_calibrator/v2/runner.py"].category == "clean_staged_v2_surface_blocker"
    for path in (
        "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py",
        "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
        "src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py",
    ):
        assert by_path[path].category == "clean_staged_noncanonical_entrypoint_blocker"
        assert by_path[path].allowed_in_v1_5_package is False


def test_clean_staged_canonical_queue_entrypoint_remains_review_candidate(tmp_path):
    audit = build_dirty_zone_audit(
        clean_worktree=tmp_path / "clean",
        root_workspace=tmp_path / "root",
        clean_status_text="M  src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py\n",
        root_status_text="",
    )
    entry = audit.entries[0]

    assert audit.status == "review_required"
    assert audit.summary["blocker_count"] == 0
    assert entry.category == "clean_staged_candidate_package"
    assert entry.severity == "review"
    assert entry.allowed_in_v1_5_package is True


def test_root_staged_entries_block_and_root_dirty_entries_stay_isolated(tmp_path):
    audit = build_dirty_zone_audit(
        clean_worktree=tmp_path / "clean",
        root_workspace=tmp_path / "root",
        clean_status_text="",
        root_status_text=(
            "M  src/gas_calibrator/workflow/runner.py\n"
            " M configs/default_config.json\n"
            "?? _handoff/V1_5_DRAFT.md\n"
            "?? src/gas_calibrator/tools/run_v1_5_experimental.py\n"
        ),
    )
    by_path = {entry.path: entry for entry in audit.entries}

    assert audit.status == "blocked"
    assert audit.summary["blocker_count"] == 1
    assert by_path["src/gas_calibrator/workflow/runner.py"].category == "root_staged_pollution_blocker"
    assert by_path["configs/default_config.json"].category == "root_tracked_dirty_isolated"
    assert by_path["_handoff/V1_5_DRAFT.md"].category == "root_handoff_evidence_retained"
    assert by_path["src/gas_calibrator/tools/run_v1_5_experimental.py"].category == "root_untracked_draft_isolated"
    assert all(entry.allowed_in_v1_5_package is False for entry in audit.entries)


def test_clean_worktree_tracked_changes_are_reviewed_small_package_candidates(tmp_path):
    audit = build_dirty_zone_audit(
        clean_worktree=tmp_path / "clean",
        root_workspace=tmp_path / "root",
        clean_status_text=(
            "M  src/gas_calibrator/validation/v1_5_dirty_zone_audit.py\n"
            " M tests/test_v1_5_dirty_zone_audit.py\n"
        ),
        root_status_text="",
    )
    by_path = {entry.path: entry for entry in audit.entries}

    assert audit.status == "review_required"
    assert by_path["src/gas_calibrator/validation/v1_5_dirty_zone_audit.py"].category == (
        "clean_staged_candidate_package"
    )
    assert by_path["tests/test_v1_5_dirty_zone_audit.py"].category == "clean_tracked_change_review_required"
    assert by_path["src/gas_calibrator/validation/v1_5_dirty_zone_audit.py"].allowed_in_v1_5_package is True


def test_dirty_zone_markdown_names_root_isolation_and_no_delete_policy(tmp_path):
    audit = build_dirty_zone_audit(
        clean_worktree=tmp_path / "clean",
        root_workspace=tmp_path / "root",
        clean_status_text="?? _handoff/old_evidence/\n",
        root_status_text=" M src/gas_calibrator/workflow/runner.py\n",
    )
    text = render_dirty_zone_audit_markdown(audit)

    assert "root_workspace_policy" in text
    assert "isolated_draft_pollution_zone_not_formal_source" in text
    assert "destructive_actions_allowed" in text
    assert "`False`" in text
    assert "root_tracked_dirty_isolated" in text


def test_dirty_zone_exporter_writes_json_markdown_and_csv_from_status_files(tmp_path):
    clean_status = tmp_path / "clean.status"
    root_status = tmp_path / "root.status"
    out = tmp_path / "out"
    clean_status.write_text("?? _handoff/old_evidence/\n", encoding="utf-8")
    root_status.write_text(" M configs/default_config.json\n", encoding="utf-8")

    rc = export_main(
        [
            "--clean-worktree",
            str(tmp_path / "clean"),
            "--root-workspace",
            str(tmp_path / "root"),
            "--clean-status-file",
            str(clean_status),
            "--root-status-file",
            str(root_status),
            "--output-dir",
            str(out),
        ]
    )

    assert rc == 0
    payload = json.loads((out / "v1_5_dirty_zone_audit.json").read_text(encoding="utf-8"))
    assert payload["schema"] == "v1_5_dirty_zone_audit_v1"
    assert payload["status"] == "review_required"
    assert payload["summary"]["warning_count"] == 1
    assert (out / "v1_5_dirty_zone_audit.md").exists()
    assert (out / "v1_5_dirty_zone_entries.csv").exists()


def test_dirty_zone_exporter_can_fail_on_root_staged_blocker(tmp_path):
    clean_status = tmp_path / "clean.status"
    root_status = tmp_path / "root.status"
    clean_status.write_text("", encoding="utf-8")
    root_status.write_text("A  src/gas_calibrator/workflow/runner.py\n", encoding="utf-8")

    rc = export_main(
        [
            "--clean-worktree",
            str(tmp_path / "clean"),
            "--root-workspace",
            str(tmp_path / "root"),
            "--clean-status-file",
            str(clean_status),
            "--root-status-file",
            str(root_status),
            "--output-dir",
            str(tmp_path / "out"),
            "--fail-on-blocker",
        ]
    )

    assert rc == 2
