import json
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = (
    REPO_ROOT
    / "configs"
    / "site_v1_5_no_write_current_hardware_co2_20c_1000ppm_full_pressure_no_outp_skip_tempwait.json"
)
BASE_NO_OUTP_CONFIG_PATH = (
    REPO_ROOT
    / "configs"
    / "site_v1_5_no_write_current_hardware_co2_20c_1000ppm_full_pressure_no_tempwait_no_outp.json"
)
DEFAULT_CONFIG_PATH = REPO_ROOT / "configs" / "default_config.json"
pytestmark = pytest.mark.skipif(
    not CONFIG_PATH.is_file() or not BASE_NO_OUTP_CONFIG_PATH.is_file(),
    reason="real-COM no-OUTP engineering configs are not restored",
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_v1_5_no_outp_skip_tempwait_engineering_config_guard() -> None:
    assert CONFIG_PATH.exists()

    cfg = _load(CONFIG_PATH)
    workflow = cfg["workflow"]
    pressure = workflow["pressure"]
    temperature = workflow["stability"]["temperature"]
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

    assert temperature["wait_for_target_before_continue"] is False
    assert temperature["soak_after_reach_s"] == 0
    assert temperature["timeout_s"] == 0
    assert temperature["hard_max_wait_s"] == 0
    assert temperature["window_s"] == 0
    assert temperature["continue_wait_while_progress"] is False
    assert temperature["analyzer_chamber_temp_enabled"] is False
    assert temperature["analyzer_chamber_temp_window_s"] == 0
    assert temperature["analyzer_chamber_temp_timeout_s"] == 0
    assert temperature["analyzer_chamber_temp_first_valid_timeout_s"] == 0


def test_v1_5_skip_tempwait_config_does_not_change_default_or_base_prearm_config() -> None:
    default_cfg = _load(DEFAULT_CONFIG_PATH)
    base_cfg = _load(BASE_NO_OUTP_CONFIG_PATH)

    default_temp = default_cfg["workflow"]["stability"]["temperature"]
    base_temp = base_cfg["workflow"]["stability"]["temperature"]

    assert default_temp["timeout_s"] > 0
    assert default_temp["hard_max_wait_s"] == 0.0
    assert default_temp["analyzer_chamber_temp_enabled"] is True

    assert "wait_for_target_before_continue" not in base_temp
    assert base_temp["timeout_s"] == 120
    assert base_temp["hard_max_wait_s"] == 180
    assert base_temp["analyzer_chamber_temp_enabled"] is False
