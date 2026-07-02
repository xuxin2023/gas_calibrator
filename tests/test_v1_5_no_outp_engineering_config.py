import json
from pathlib import Path


CONFIG_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "site_v1_5_no_write_current_hardware_co2_20c_1000ppm_full_pressure_no_tempwait_no_outp.json"
)


def test_v1_5_no_outp_engineering_config_guard() -> None:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    workflow = cfg["workflow"]
    pressure = workflow["pressure"]
    postrun = workflow["postrun_corrected_delivery"]
    startup_pressure = workflow["startup_pressure_sensor_calibration"]
    coefficients = cfg["coefficients"]
    paths = cfg["paths"]

    assert pressure["no_outp_transition_mode"] is True
    assert workflow["collect_only"] is True

    assert coefficients["enabled"] is False
    assert coefficients["sencos"] == {}

    assert startup_pressure["enabled"] is False
    assert startup_pressure["apply_write"] is False

    assert postrun["enabled"] is False
    assert postrun["write_devices"] is False
    assert postrun["write_pressure_coefficients"] is False

    assert workflow["route_mode"] == "co2_only"
    assert workflow["selected_temps"] == [20]
    assert workflow["preserve_explicit_point_matrix"] is True
    assert paths["points_excel"] == "configs/points_v1_5_co2_20c_1000ppm_full_pressure_nowait.xlsx"
