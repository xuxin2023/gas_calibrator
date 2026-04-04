from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.scripts import run_simulated_compare


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_full_route_protocol_compare_writes_report_latest_and_bundle(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated",
            "--scenario",
            "full_route_success_all_temps_all_sources",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "full_route_protocol",
        ]
    )

    report_dir = tmp_path / "full_route_protocol"
    report = _load_json(report_dir / "control_flow_compare_report.json")
    latest = _load_json(tmp_path / "replacement_full_route_simulated_latest.json")
    artifact_inventory = _load_json(report_dir / "artifact_inventory.json")

    assert exit_code == 0
    assert report["compare_status"] == "MATCH"
    assert report["evidence_source"] == "simulated"
    assert report["evidence_state"] == "simulated_protocol"
    assert report["simulation_context"]["simulation_backend"] == "protocol"
    assert report["reference_quality"]["reference_quality"] == "healthy"
    assert report["reference_quality"]["reference_quality_degraded"] is False
    assert Path(report["artifacts"]["replacement_full_route_simulated_bundle"]).exists()
    assert Path(report["artifacts"]["replacement_full_route_simulated_latest"]).exists()
    assert artifact_inventory["complete"] is True
    assert latest["validation_profile"] == "replacement_full_route_simulated"
    assert latest["evidence_state"] == "simulated_protocol"
    assert latest["not_real_acceptance_evidence"] is True


def test_h2o_only_protocol_compare_enters_h2o_route(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_h2o_only_simulated",
            "--scenario",
            "h2o_route_success_single_temp",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "h2o_protocol",
        ]
    )

    report = _load_json(tmp_path / "h2o_protocol" / "control_flow_compare_report.json")

    assert exit_code == 0
    assert report["compare_status"] == "MATCH"
    assert report["evidence_state"] == "simulated_protocol"
    assert report["route_execution_summary"]["target_route"] == "h2o"
    assert report["entered_target_route"]["v2"] is True
    assert int(report["target_route_event_count"]["v2"]) > 0


def test_h2o_only_protocol_compare_uses_relay_route_switching(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_h2o_only_simulated",
            "--scenario",
            "relay_route_switch_h2o_success",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "h2o_relay_protocol",
        ]
    )

    report = _load_json(tmp_path / "h2o_relay_protocol" / "control_flow_compare_report.json")
    route_trace = _load_jsonl(Path(report["artifacts"]["v2_route_trace"]))

    assert exit_code == 0
    assert report["compare_status"] == "MATCH"
    assert report["route_execution_summary"]["target_route"] == "h2o"
    assert any(event.get("action") == "set_h2o_path" for event in route_trace)
    assert any("relay_state" in event for event in route_trace)


def test_skip0_co2_only_protocol_compare_exercises_eight_analyzers(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_skip0_co2_only_simulated",
            "--scenario",
            "co2_only_skip0_success_eight_analyzers",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "co2_eight_analyzers_protocol",
        ]
    )

    report = _load_json(tmp_path / "co2_eight_analyzers_protocol" / "control_flow_compare_report.json")
    run_log = Path(report["metadata"]["v2"]["run_dir"]) / "run.log"

    assert exit_code == 0
    assert report["compare_status"] == "MATCH"
    assert report["simulation_context"]["device_matrix"]["analyzers"]["count"] == 8
    assert report["route_execution_summary"]["target_route"] == "co2"
    assert run_log.exists()
    assert "analyzers=8" in run_log.read_text(encoding="utf-8")


def test_skip0_co2_only_protocol_compare_exercises_eight_analyzers_with_relay(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_skip0_co2_only_simulated",
            "--scenario",
            "co2_only_skip0_success_eight_analyzers_with_relay",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "co2_eight_analyzers_relay_protocol",
        ]
    )

    report = _load_json(tmp_path / "co2_eight_analyzers_relay_protocol" / "control_flow_compare_report.json")
    route_trace = _load_jsonl(Path(report["artifacts"]["v2_route_trace"]))

    assert exit_code == 0
    assert report["compare_status"] == "MATCH"
    assert report["simulation_context"]["device_matrix"]["analyzers"]["count"] == 8
    assert any(event.get("action") == "set_co2_valves" for event in route_trace)
    assert any("relay_state" in event for event in route_trace)


def test_full_route_protocol_compare_uses_relay_and_thermometer_devices(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated",
            "--scenario",
            "full_route_success_with_relay_and_thermometer",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "full_route_relay_thermo_protocol",
        ]
    )

    report = _load_json(tmp_path / "full_route_relay_thermo_protocol" / "control_flow_compare_report.json")

    assert exit_code == 0
    assert report["compare_status"] == "MATCH"
    assert report["simulation_context"]["device_matrix"]["relay"]["channel_count"] == 16
    assert report["simulation_context"]["device_matrix"]["relay_8"]["channel_count"] == 8
    assert report["simulation_context"]["device_matrix"]["thermometer"]["mode"] == "stable"
    assert report["reference_quality"]["thermometer_reference_status"] == "healthy"
    assert report["route_execution_summary"]["route_physical_state_match"]["v2"] is True


def test_relay_route_switch_co2_success_stays_physically_matched(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_skip0_co2_only_simulated",
            "--scenario",
            "relay_route_switch_co2_success",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "relay_co2_protocol",
        ]
    )

    report = _load_json(tmp_path / "relay_co2_protocol" / "control_flow_compare_report.json")

    assert exit_code == 0
    assert report["compare_status"] == "MATCH"
    assert report["route_execution_summary"]["route_physical_state_match"]["v2"] is True
    assert report["route_execution_summary"]["relay_physical_mismatch"]["v2"] is False


def test_chamber_fault_protocol_compare_writes_diagnostic_artifacts(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated_diagnostic",
            "--scenario",
            "temperature_chamber_stalled",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "chamber_stalled_protocol",
        ]
    )

    report_dir = tmp_path / "chamber_stalled_protocol"
    report = _load_json(report_dir / "control_flow_compare_report.json")
    latest = _load_json(tmp_path / "replacement_full_route_simulated_diagnostic_latest.json")

    assert exit_code == 1
    assert report["compare_status"] in {"NOT_EXECUTED", "MISMATCH"}
    assert report["evidence_source"] == "simulated"
    assert report["evidence_state"] == "simulated_protocol"
    assert report["diagnostic_only"] is True
    assert report["not_real_acceptance_evidence"] is True
    assert Path(report["artifacts"]["replacement_full_route_simulated_diagnostic_bundle"]).exists()
    assert latest["validation_profile"] == "replacement_full_route_simulated_diagnostic"
    assert latest["diagnostic_only"] is True


def test_relay_stuck_channel_protocol_compare_becomes_explicit_mismatch(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_h2o_only_simulated",
            "--scenario",
            "relay_stuck_channel_causes_route_mismatch",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "relay_stuck_protocol",
        ]
    )

    report = _load_json(tmp_path / "relay_stuck_protocol" / "control_flow_compare_report.json")
    route_trace = _load_jsonl(Path(report["artifacts"]["v2_route_trace"]))
    h2o_switch = next(event for event in route_trace if event.get("action") == "set_h2o_path")
    relay_8 = h2o_switch.get("relay_state", {}).get("relay_b", {})

    assert exit_code == 1
    assert report["compare_status"] == "MISMATCH"
    assert report["route_execution_summary"]["target_route"] == "h2o"
    assert report["entered_target_route"]["v2"] is True
    assert report["route_execution_summary"]["route_physical_state_match"]["v2"] is False
    assert report["route_execution_summary"]["relay_physical_mismatch"]["v2"] is True
    assert report["valid_for_route_diff"] is False
    assert report["replacement_validation"]["presence_evaluable"] is False
    assert relay_8.get("1") is False
    assert relay_8.get("2") is False
    assert relay_8.get("8") is False


def test_cleanup_restores_all_relays_off_after_successful_protocol_run(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_skip0_co2_only_simulated",
            "--scenario",
            "cleanup_restores_all_relays_off",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "relay_cleanup_protocol",
        ]
    )

    report = _load_json(tmp_path / "relay_cleanup_protocol" / "control_flow_compare_report.json")
    route_trace = _load_jsonl(Path(report["artifacts"]["v2_route_trace"]))
    restore_events = [event for event in route_trace if event.get("action") in {"restore_baseline", "safe_stop"}]
    last_relay_state = restore_events[-1].get("relay_state", {}) if restore_events else {}
    flattened = [state for channels in last_relay_state.values() for state in channels.values()]

    assert exit_code == 0
    assert report["compare_status"] == "MATCH"
    assert flattened
    assert all(state is False for state in flattened)
    assert report["route_execution_summary"]["sides"]["v2"]["cleanup_all_relays_off"] is True


def test_thermometer_reference_protocol_scenarios_capture_simulation_modes(tmp_path: Path) -> None:
    stale_exit = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated_diagnostic",
            "--scenario",
            "thermometer_stale_reference",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "thermometer_stale_protocol",
        ]
    )
    no_response_exit = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated_diagnostic",
            "--scenario",
            "thermometer_no_response",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "thermometer_no_response_protocol",
        ]
    )

    stale_report = _load_json(tmp_path / "thermometer_stale_protocol" / "control_flow_compare_report.json")
    no_response_report = _load_json(tmp_path / "thermometer_no_response_protocol" / "control_flow_compare_report.json")

    assert stale_exit in {0, 1}
    assert no_response_exit in {0, 1}
    assert stale_report["simulation_context"]["device_matrix"]["thermometer"]["mode"] == "stale"
    assert no_response_report["simulation_context"]["device_matrix"]["thermometer"]["mode"] == "no_response"
    assert stale_report["evidence_state"] == "simulated_protocol"
    assert no_response_report["evidence_state"] == "simulated_protocol"
    assert stale_report["reference_quality"]["reference_quality"] == "degraded"
    assert stale_report["reference_quality"]["thermometer_reference_status"] == "stale"
    assert stale_report["reference_quality"]["reference_quality_degraded"] is True
    assert no_response_report["reference_quality"]["reference_quality"] == "failed"
    assert no_response_report["reference_quality"]["thermometer_reference_status"] == "no_response"
    assert no_response_report["reference_quality"]["reference_quality_degraded"] is True


def test_pressure_reference_degraded_is_explicit_in_report(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated_diagnostic",
            "--scenario",
            "pressure_reference_degraded",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "pressure_reference_protocol",
        ]
    )

    report = _load_json(tmp_path / "pressure_reference_protocol" / "control_flow_compare_report.json")

    assert exit_code in {0, 1}
    assert report["evidence_state"] == "simulated_protocol"
    assert report["reference_quality"]["pressure_reference_status"] == "no_response"
    assert report["reference_quality"]["reference_quality"] == "failed"
    assert report["reference_quality"]["reference_quality_degraded"] is True


def test_pressure_reference_wrong_unit_configuration_is_explicit_in_report(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated_diagnostic",
            "--scenario",
            "pressure_gauge_wrong_unit_configuration",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "pressure_wrong_unit_protocol",
        ]
    )

    report = _load_json(tmp_path / "pressure_wrong_unit_protocol" / "control_flow_compare_report.json")

    assert exit_code in {0, 1}
    assert report["evidence_state"] == "simulated_protocol"
    assert report["reference_quality"]["pressure_reference_status"] == "wrong_unit_configuration"
    assert report["reference_quality"]["reference_quality"] == "degraded"
    assert report["reference_quality"]["reference_quality_degraded"] is True


def test_pace_fault_protocol_scenarios_write_diagnostic_artifacts(tmp_path: Path) -> None:
    no_response_exit = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated_diagnostic",
            "--scenario",
            "pace_no_response_cleanup",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "pace_cleanup_protocol",
        ]
    )
    unsupported_exit = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated_diagnostic",
            "--scenario",
            "pace_unsupported_header",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "pace_unsupported_protocol",
        ]
    )

    cleanup_report = _load_json(tmp_path / "pace_cleanup_protocol" / "control_flow_compare_report.json")
    unsupported_report = _load_json(tmp_path / "pace_unsupported_protocol" / "control_flow_compare_report.json")

    assert no_response_exit == 1
    assert unsupported_exit == 1
    assert cleanup_report["evidence_state"] == "simulated_protocol"
    assert unsupported_report["evidence_state"] == "simulated_protocol"
    assert cleanup_report["diagnostic_only"] is True
    assert unsupported_report["diagnostic_only"] is True
    assert Path(cleanup_report["artifacts"]["replacement_full_route_simulated_diagnostic_bundle"]).exists()
    assert Path(unsupported_report["artifacts"]["replacement_full_route_simulated_diagnostic_bundle"]).exists()


def test_humidity_generator_timeout_protocol_compare_writes_diagnostic_artifacts(tmp_path: Path) -> None:
    exit_code = run_simulated_compare.main(
        [
            "--profile",
            "replacement_full_route_simulated_diagnostic",
            "--scenario",
            "humidity_generator_timeout",
            "--report-root",
            str(tmp_path),
            "--run-name",
            "grz_timeout_protocol",
        ]
    )

    report_dir = tmp_path / "grz_timeout_protocol"
    report = _load_json(report_dir / "control_flow_compare_report.json")
    latest = _load_json(tmp_path / "replacement_full_route_simulated_diagnostic_latest.json")

    assert exit_code == 1
    assert report["compare_status"] in {"NOT_EXECUTED", "MISMATCH"}
    assert report["evidence_source"] == "simulated"
    assert report["evidence_state"] == "simulated_protocol"
    assert report["diagnostic_only"] is True
    assert report["not_real_acceptance_evidence"] is True
    assert Path(report["artifacts"]["replacement_full_route_simulated_diagnostic_bundle"]).exists()
    assert latest["validation_profile"] == "replacement_full_route_simulated_diagnostic"
    assert latest["diagnostic_only"] is True
