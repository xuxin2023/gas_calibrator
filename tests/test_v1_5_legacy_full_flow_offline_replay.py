import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_legacy_full_flow_offline_replay import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_legacy_full_flow_offline_replay import (
    EVIDENCE_SOURCE,
    READY_STATUS,
    SCHEMA,
    build_v1_5_legacy_full_flow_offline_replay,
    write_v1_5_legacy_full_flow_offline_replay,
)


ROOT = Path(__file__).resolve().parents[1]
SOURCE_MAIN = "661b2b280b43d85df06c81df09e9d3f02165278b"


def _model() -> dict:
    return build_v1_5_legacy_full_flow_offline_replay(
        repository_root=ROOT, source_origin_main_commit=SOURCE_MAIN
    )


def test_replay_walks_one_legacy_state_machine_without_claiming_production_completion() -> None:
    model = _model()

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == READY_STATUS
    assert model["orchestrator_replay_complete"] is True
    assert model["production_flow_complete"] is False
    assert model["frozen_gap_assessment"] == "program_level_replay_complete_not_production_complete"
    assert model["algorithm_profile_id"] == "legacy_ratio_production"
    assert model["mature_fitting_baseline"] == "0613 V1.5 fitting path"
    assert model["mature_route_baseline"] == "0620/0621 clean-worktree mature physical route path"
    assert model["expected_point_counts"] == {"co2": 45, "h2o": 13}
    assert model["state_machine_stage_count"] == 9
    assert model["source_review_reasons"] == []
    assert model["evidence_source"] == EVIDENCE_SOURCE == "historical_replay"
    assert model["not_real_acceptance_evidence"] is True


def test_replay_preserves_real_holds_and_stage_order() -> None:
    model = _model()
    stages = model["stages"]
    by_id = {row["stage_id"]: row for row in stages}

    assert [row["order"] for row in stages] == list(range(1, 10))
    assert model["current_stage_id"] == "initialization_identity_runtime"
    assert stages[0]["effective_status"] == "hold"
    assert all(row["effective_status"] == "blocked_by_previous_stage" for row in stages[1:])
    assert "active_device_count_not_1_to_6" in by_id["initialization_identity_runtime"]["blocker_codes"]
    assert "no_complete_continuous_mature_route_root" in by_id["mature_route_readiness"]["blocker_codes"]
    assert "co2_composite_not_continuous_route_attestation" in by_id["legacy_co2_45"]["blocker_codes"]
    assert "legacy_h2o_continuous_13_point_root_missing" in by_id["legacy_h2o_13"]["blocker_codes"]
    assert "production_component_qc_evaluator_missing" in by_id["component_qc_and_0613_fit_review"]["blocker_codes"]
    assert "post_run_write_package=not_attempted" in by_id["controlled_write_readback"]["blocker_codes"]
    assert "controlled_write_and_reverification=not_attempted" in by_id["post_write_short_reverify"]["blocker_codes"]
    assert any(code.startswith("formal_archive_database_release=") for code in by_id["archive_release_postgresql18"]["blocker_codes"])


def test_replay_keeps_co2_zero_and_h2o_dry_anchor_roles_separate() -> None:
    model = _model()
    h2o = next(row for row in model["stages"] if row["stage_id"] == "legacy_h2o_13")

    assert "CO2 zero gas is not an interchangeable H2O dry-gas anchor" in h2o["physical_meaning"]
    assert "separately traceable dry-gas anchor" in h2o["next_action"]


def test_replay_keeps_all_execution_and_release_surfaces_locked() -> None:
    model = _model()
    for key in (
        "full_production_auto_allowed",
        "live_queue_execution_allowed",
        "formal_release_allowed",
        "database_import_allowed",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "writes_sn_or_device_code",
        "connects_postgresql",
    ):
        assert model[key] is False


def test_replay_reviews_missing_sources_or_invalid_commit(tmp_path: Path) -> None:
    model = build_v1_5_legacy_full_flow_offline_replay(
        repository_root=tmp_path, source_origin_main_commit="invalid"
    )

    assert model["overall_status"] != READY_STATUS
    assert model["orchestrator_replay_complete"] is False
    assert "source_origin_main_commit_invalid" in model["source_review_reasons"]
    assert any(reason.startswith("source_evidence_missing:") for reason in model["source_review_reasons"])
    assert model["opens_com_ports"] is False


def test_replay_writer_cli_and_entrypoint_classification(tmp_path: Path) -> None:
    output_dir = tmp_path / "replay"
    paths = write_v1_5_legacy_full_flow_offline_replay(
        output_dir=output_dir,
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
    )
    assert all(path.is_file() for path in paths.values())
    payload = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    markdown = paths["markdown"].read_text(encoding="utf-8")
    assert payload["overall_status"] == READY_STATUS
    assert "45 点 CO2 composite 只可诊断" in markdown
    assert "CO2 zero gas 与 H2O dry-gas anchor 不可互换" in markdown

    cli_dir = tmp_path / "cli"
    assert cli_main(
        [
            "--repository-root",
            str(ROOT),
            "--source-origin-main-commit",
            SOURCE_MAIN,
            "--output-dir",
            str(cli_dir),
        ]
    ) == 0
    assert (cli_dir / "v1_5_legacy_full_flow_offline_replay.json").is_file()

    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_legacy_full_flow_offline_replay.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("legacy full-flow offline replay" in note for note in entry.notes)
