import json
from pathlib import Path

from gas_calibrator.data.points import load_points_from_excel


def test_v1_800ppm_ingress_points_file_has_expected_sequence() -> None:
    path = Path("configs/points_v1_800ppm_ingress_smoke_20c.xlsx")

    points = load_points_from_excel(path, missing_pressure_policy="carry_forward", carry_forward_h2o=True)

    co2_points = [point for point in points if point.co2_ppm is not None]
    assert [int(point.temp_chamber_c) for point in co2_points] == [20, 20, 20, 20]
    assert [int(point.co2_ppm) for point in co2_points] == [800, 800, 800, 800]
    assert [int(point.target_pressure_hpa) for point in co2_points] == [1000, 800, 600, 500]


def test_v1_800ppm_ingress_override_sets_room_temp_same_gas_smoke_profile() -> None:
    path = Path("configs/overrides/v1_800ppm_ingress_smoke.json")
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["base_config"].endswith("configs/default_config.json")
    assert payload["paths"]["points_excel"].endswith("configs/points_v1_800ppm_ingress_smoke_20c.xlsx")
    assert payload["devices"]["temperature_chamber"]["enabled"] is False
    assert payload["devices"]["humidity_generator"]["enabled"] is False
    assert payload["workflow"]["route_mode"] == "co2_only"
    assert payload["workflow"]["skip_h2o"] is True
    assert payload["workflow"]["selected_temps_c"] == [20.0]
    assert payload["workflow"]["selected_pressure_points"] == [1000, 800, 600, 500]
    assert payload["workflow"]["skip_co2_ppm"] == [0, 100, 200, 300, 400, 500, 600, 700, 900, 1000]
    assert payload["workflow"]["pressure"]["capture_then_hold_enabled"] is True
    assert payload["workflow"]["pressure"]["adaptive_pressure_sampling_enabled"] is True
    assert payload["workflow"]["pressure"]["use_pressure_gauge_for_sampling_gate"] is True
    assert payload["workflow"]["pressure"]["co2_sampling_gate_window_s"] == 10.0
    assert payload["workflow"]["pressure"]["co2_sampling_gate_pressure_fill_s"] == 10.0
    assert payload["workflow"]["pressure"]["continuous_atmosphere_hold"] is False
    assert payload["workflow"]["stability"]["gas_route_dewpoint_gate_enabled"] is True
    assert payload["validation_package"]["rounds"] == 2


def test_low_pressure_post_isolation_diagnostic_override_sets_20s_window() -> None:
    path = Path("configs/overrides/v1_low_pressure_post_isolation_diagnostic.json")
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["base_config"].endswith("configs/default_config.json")
    assert payload["devices"]["temperature_chamber"]["enabled"] is False
    assert payload["devices"]["humidity_generator"]["enabled"] is False
    assert payload["workflow"]["route_mode"] == "co2_only"
    assert payload["workflow"]["skip_h2o"] is True
    assert payload["workflow"]["selected_temps_c"] == [20.0]
    assert payload["workflow"]["selected_pressure_points"] == [800, 600, 500]
    assert payload["workflow"]["pressure"]["capture_then_hold_enabled"] is True
    assert payload["workflow"]["pressure"]["adaptive_pressure_sampling_enabled"] is True
    assert payload["workflow"]["pressure"]["use_pressure_gauge_for_sampling_gate"] is True
    assert payload["workflow"]["pressure"]["continuous_atmosphere_hold"] is False
    assert payload["workflow"]["pressure"]["co2_post_isolation_diagnostic_enabled"] is True
    assert payload["workflow"]["pressure"]["co2_post_isolation_window_s"] == 20.0


def test_pace_post_isolation_diagnostic_override_sets_20s_window() -> None:
    path = Path("configs/overrides/v1_pace_post_isolation_diagnostic.json")
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["base_config"].endswith("configs/default_config.json")
    assert payload["devices"]["temperature_chamber"]["enabled"] is False
    assert payload["devices"]["humidity_generator"]["enabled"] is False
    assert payload["workflow"]["route_mode"] == "co2_only"
    assert payload["workflow"]["skip_h2o"] is True
    assert payload["workflow"]["selected_temps_c"] == [20.0]
    assert payload["workflow"]["selected_pressure_points"] == [800, 600, 500]
    assert payload["workflow"]["pressure"]["capture_then_hold_enabled"] is True
    assert payload["workflow"]["pressure"]["adaptive_pressure_sampling_enabled"] is True
    assert payload["workflow"]["pressure"]["use_pressure_gauge_for_sampling_gate"] is True
    assert payload["workflow"]["pressure"]["continuous_atmosphere_hold"] is False
    assert payload["workflow"]["pressure"]["co2_post_isolation_diagnostic_enabled"] is True
    assert payload["workflow"]["pressure"]["co2_post_isolation_window_s"] == 20.0
