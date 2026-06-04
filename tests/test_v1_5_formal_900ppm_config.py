from pathlib import Path

from gas_calibrator.config import load_config
from gas_calibrator.data.points import load_points_from_excel


ROOT = Path(__file__).resolve().parents[1]


def test_formal_open_flow_900ppm_config_is_no_write_and_passive() -> None:
    cfg_path = ROOT / "configs" / "site_v1_5_formal_open_flow_4ch_no_write_900ppm.json"
    cfg = load_config(cfg_path)

    analyzers = cfg["devices"]["gas_analyzers"]
    assert len(analyzers) == 4
    assert all(item["active_send"] is False for item in analyzers)
    assert all(item["ftd_hz"] == 1 for item in analyzers)
    assert cfg["workflow"]["sensor_precheck"]["active_send"] is False
    assert cfg["workflow"]["postrun_corrected_delivery"]["write_devices"] is False
    assert cfg["workflow"]["postrun_corrected_delivery"]["write_pressure_coefficients"] is False
    assert cfg["workflow"]["startup_pressure_sensor_calibration"]["apply_write"] is False
    assert cfg["workflow"]["analyzer_live_snapshot"]["passive_per_device_workers_enabled"] is True
    assert cfg["coefficients"]["enabled"] is False
    assert (
        cfg["coefficients"]["ratio_poly_fit"]["include_sealed_pressure_points_in_formal_fit"]
        is False
    )


def test_formal_open_flow_900ppm_points_load_as_single_dry_co2_point() -> None:
    points_path = ROOT / "configs" / "points_v1_5_co2_20c_900ppm_open_flow_no_write.xlsx"
    points = load_points_from_excel(points_path, missing_pressure_policy="carry_forward")

    assert len(points) == 1
    point = points[0]
    assert point.temp_chamber_c == 20.0
    assert point.co2_ppm == 900.0
    assert point.target_pressure_hpa == 1000.0
    assert point.is_h2o_point is False
