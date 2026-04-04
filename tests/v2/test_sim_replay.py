from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.sim import list_replay_scenarios, load_replay_fixture, materialize_replay_fixture


REQUIRED_SCENARIOS = {
    "full_route_success_all_temps_all_sources",
    "h2o_route_success_single_temp",
    "co2_only_skip0_success_single_temp",
    "sensor_precheck_mode2_partial_frame_fail",
    "sensor_precheck_relaxed_allows_route_entry",
    "pace_no_response_on_cleanup",
    "gauge_no_response",
    "humidity_generator_timeout",
    "v1_route_trace_missing_but_io_log_derivable",
    "resource_locked_serial_port",
    "primary_latest_missing",
    "stale_h2o_latest_present_but_must_not_be_primary",
    "profile_skips_h2o_devices",
    "compare_generates_partial_artifacts_on_failure",
    "co2_route_entered_but_sample_count_mismatch",
    "co2_route_entered_sample_mismatch",
}


def test_replay_fixture_catalog_contains_required_scenarios() -> None:
    assert REQUIRED_SCENARIOS.issubset(set(list_replay_scenarios()))


def test_materialize_success_replay_fixture_writes_report_bundle_and_latest(tmp_path: Path) -> None:
    payload = load_replay_fixture(scenario="co2_only_skip0_success_single_temp")

    result = materialize_replay_fixture(payload, report_root=tmp_path, run_name="success_case")

    report = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))
    latest = json.loads((tmp_path / "success_case" / "skip0_co2_only_replacement_latest.json").read_text(encoding="utf-8"))

    assert report["compare_status"] == "MATCH"
    assert report["evidence_source"] == "simulated"
    assert report["not_real_acceptance_evidence"] is True
    assert report["route_execution_summary"]["target_route"] == "co2"
    assert Path(result["report_markdown"]).exists()
    assert Path(result["artifact_inventory"]).exists()
    assert latest["validation_profile"] == "skip0_co2_only_replacement"
    assert latest["evidence_source"] == "simulated"
    assert latest["compare_status"] == "MATCH"
    assert not (tmp_path / "skip0_co2_only_replacement_latest.json").exists()


def test_materialize_failure_replay_fixture_still_writes_primary_artifacts(tmp_path: Path) -> None:
    payload = load_replay_fixture(scenario="compare_generates_partial_artifacts_on_failure")

    result = materialize_replay_fixture(payload, report_root=tmp_path, run_name="partial_failure")

    report = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))
    latest = json.loads((tmp_path / "partial_failure" / "skip0_co2_only_replacement_latest.json").read_text(encoding="utf-8"))

    assert report["compare_status"] == "NOT_EXECUTED"
    assert report["evidence_source"] == "simulated"
    assert report["replacement_validation"]["presence_evaluable"] is False
    assert Path(result["report_markdown"]).exists()
    assert Path(result["artifact_inventory"]).exists()
    assert latest["compare_status"] == "NOT_EXECUTED"


def test_materialize_full_route_replay_fixture_writes_simulated_full_route_latest(tmp_path: Path) -> None:
    payload = load_replay_fixture(scenario="full_route_success_all_temps_all_sources")

    result = materialize_replay_fixture(
        payload,
        report_root=tmp_path,
        run_name="full_route_case",
        publish_latest=True,
        validation_profile_override="replacement_full_route_simulated",
    )

    report = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))
    latest = json.loads((tmp_path / "replacement_full_route_simulated_latest.json").read_text(encoding="utf-8"))

    assert report["compare_status"] == "MATCH"
    assert report["evidence_source"] == "simulated"
    assert report["bench_context"]["target_route"] == "h2o_then_co2"
    assert latest["validation_profile"] == "replacement_full_route_simulated"
    assert latest["evidence_state"] == "simulated_acceptance_like_coverage"


def test_materialize_snapshot_fixture_writes_stale_latest_indexes(tmp_path: Path) -> None:
    payload = load_replay_fixture(scenario="stale_h2o_latest_present_but_must_not_be_primary")

    result = materialize_replay_fixture(payload, report_root=tmp_path)

    written = [Path(item) for item in result["latest_indexes"]]
    assert written
    stale_payload = json.loads(written[0].read_text(encoding="utf-8"))
    assert stale_payload["validation_profile"] == "h2o_only_replacement"
    assert stale_payload["stale_for_current_bench"] is True


def test_replay_loader_supports_stable_alias_for_stale_h2o_fixture() -> None:
    payload = load_replay_fixture(scenario="stale_h2o_latest_present_but_not_primary")

    assert payload["scenario"] == "stale_h2o_latest_present_but_must_not_be_primary"
