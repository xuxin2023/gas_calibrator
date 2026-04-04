from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.scripts import run_simulated_compare, run_validation_replay


def test_run_validation_replay_cli_writes_compare_artifacts(tmp_path: Path) -> None:
    exit_code = run_validation_replay.main(
        [
            "--scenario",
            "co2_route_entered_but_sample_count_mismatch",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "cli_case",
        ]
    )

    payload = json.loads((tmp_path / "cli_case" / "control_flow_compare_report.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["compare_status"] == "MISMATCH"
    assert payload["evidence_source"] == "simulated"
    assert payload["evidence_state"] == "replay"
    assert (tmp_path / "cli_case" / "skip0_co2_only_replacement_latest.json").exists()
    assert not (tmp_path / "skip0_co2_only_replacement_latest.json").exists()


def test_run_simulated_compare_defaults_to_success_scenario(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--report-root",
            str(tmp_path),
            "--run-name",
            "sim_case",
        ]
    )

    payload = json.loads((tmp_path / "sim_case" / "control_flow_compare_report.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["compare_status"] == "MATCH"
    assert payload["evidence_source"] == "simulated"
    assert payload["evidence_state"] == "simulated_protocol"
    assert (tmp_path / "replacement_skip0_co2_only_simulated_latest.json").exists()


def test_run_simulated_compare_supports_full_route_profile_and_scenario(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated",
            "--scenario",
            "full_route_success_all_temps_all_sources",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "sim_full_route",
        ]
    )

    payload = json.loads((tmp_path / "sim_full_route" / "control_flow_compare_report.json").read_text(encoding="utf-8"))
    latest = json.loads((tmp_path / "replacement_full_route_simulated_latest.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["compare_status"] == "MATCH"
    assert payload["evidence_state"] == "simulated_protocol"
    assert payload["bench_context"]["target_route"] == "h2o_then_co2"
    assert payload["route_execution_summary"]["compare_status"] == "MATCH"
    assert latest["evidence_source"] == "simulated"
    assert latest["evidence_state"] == "simulated_protocol"
    assert latest["not_real_acceptance_evidence"] is True
