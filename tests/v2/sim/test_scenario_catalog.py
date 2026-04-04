from __future__ import annotations

from gas_calibrator.v2.sim import get_simulated_scenario, list_simulated_profiles, list_simulated_scenarios


def test_simulated_scenario_catalog_contains_required_profiles_and_scenarios() -> None:
    profiles = set(list_simulated_profiles())
    scenarios = set(list_simulated_scenarios())

    assert {
        "replacement_full_route_simulated",
        "replacement_full_route_simulated_diagnostic",
        "replacement_skip0_co2_only_simulated",
        "replacement_h2o_only_simulated",
    }.issubset(profiles)
    assert {
        "full_route_success_all_temps_all_sources",
        "full_route_success_with_relay_and_thermometer",
        "co2_only_skip0_success_single_temp",
        "co2_only_skip0_success_eight_analyzers",
        "co2_only_skip0_success_eight_analyzers_with_relay",
        "relay_route_switch_co2_success",
        "h2o_route_success_single_temp",
        "relay_route_switch_h2o_success",
        "sensor_precheck_mode2_partial_frame_fail",
        "analyzer_mode2_partial_frame_protocol",
        "relay_stuck_channel_causes_route_mismatch",
        "thermometer_stable_reference",
        "thermometer_stale_reference",
        "thermometer_no_response",
        "sensor_precheck_relaxed_allows_route_entry",
        "cleanup_restores_all_relays_off",
        "pace_no_response_cleanup",
        "pace_no_response_on_cleanup",
        "pace_unsupported_header",
        "gauge_no_response",
        "pressure_reference_degraded",
        "pressure_gauge_wrong_unit_configuration",
        "humidity_generator_timeout",
        "resource_locked_serial_port",
        "profile_skips_h2o_devices",
        "primary_latest_missing",
        "stale_h2o_latest_present_but_not_primary",
        "compare_generates_partial_artifacts_on_failure",
        "co2_route_entered_sample_mismatch",
        "temperature_chamber_stalled",
    }.issubset(scenarios)


def test_simulated_scenario_exposes_device_matrix_and_context() -> None:
    scenario = get_simulated_scenario("profile_skips_h2o_devices")
    context = scenario.simulation_context()

    assert scenario.validation_profile == "replacement_skip0_co2_only_simulated"
    assert context["diagnostic_only"] is False
    assert context["target_route"] == "co2"
    assert context["device_matrix"]["humidity_generator"]["skipped_by_profile"] is True
    assert context["device_matrix"]["dewpoint_meter"]["skipped_by_profile"] is True
    assert context["device_matrix"]["relay"]["channel_count"] == 16
    assert context["device_matrix"]["relay_8"]["channel_count"] == 8
    assert context["device_matrix"]["thermometer"]["mode"] == "stable"
