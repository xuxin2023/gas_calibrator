from __future__ import annotations

import json
from pathlib import Path

PROFILE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gas_calibrator"
    / "v2"
    / "configs"
    / "validation"
    / "simulated"
    / "a4_single_temp_h2o_co2_no_write_20c_simulated.json"
)


def _profile():
    return json.loads(PROFILE_PATH.read_text(encoding="utf-8"))


def test_simulation_mode_is_true():
    assert _profile()["features"]["simulation_mode"] is True


def test_collect_only_is_true():
    assert _profile()["workflow"]["collect_only"] is True


def test_no_write_guard_active_is_true():
    assert _profile()["workflow"]["no_write_guard_active"] is True


def test_selected_temps_c_is_single_20c():
    assert _profile()["workflow"]["selected_temps_c"] == [20.0]


def test_route_mode_is_h2o_then_co2():
    assert _profile()["workflow"]["route_mode"] == "h2o_then_co2"


def test_no_real_machine_dry_run():
    top = _profile()
    for section_key in top:
        section = top[section_key]
        if isinstance(section, dict):
            assert section.get("mode") != "real_machine_dry_run", f"found real_machine_dry_run in {section_key}"
            assert "execute_probe" not in section, f"found execute_probe in {section_key}"


def test_no_allow_write_fields_set_to_true():
    disallowed = [
        "allow_write_coefficients", "allow_write_zero", "allow_write_span",
        "allow_write_calibration_parameters", "write_coefficients", "write_zero",
        "write_span", "write_calibration",
    ]
    payload = _profile()

    def _scan(obj, path=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in disallowed and v is True:
                    raise AssertionError(f"{path}.{k} is True, must not be")
                _scan(v, f"{path}.{k}")

    _scan(payload, "root")


def test_filename_indicates_a4_single_temp_h2o_co2_draft():
    fname = PROFILE_PATH.name.lower()
    assert "a4" in fname
    assert "single" in fname and "temp" in fname
    assert "h2o" in fname and "co2" in fname
    assert "simulated" in fname
    assert "no_write" in fname
    assert "seven_pressure" not in fname


def test_gas_ambient_gap_risk_recorded():
    notes = _profile()["workflow"]["a4_notes"]
    assert "gas_ambient_point_gap" in notes
    assert "recorded" in str(notes["gas_ambient_point_gap"]).lower()
    assert "not addressed" in str(notes["gas_ambient_point_gap"]).lower()


def test_not_production_ready_declared():
    notes = _profile()["workflow"]["a4_notes"]
    assert notes["not_production_ready"] is True
    assert notes["not_real_machine"] is True
    prod = _profile()["workflow"]["production"]
    assert prod["enabled"] is False
    assert prod["controlled_write"] is False
    assert prod["formal_switch"] is False


def test_points_excel_field_exists():
    assert "points_excel" in _profile()["paths"]


def test_output_dir_matches_a4_single_temp():
    od = _profile()["paths"]["output_dir"]
    assert "a4" in od.lower() or "single_temp" in od.lower() or "20c" in od.lower()
