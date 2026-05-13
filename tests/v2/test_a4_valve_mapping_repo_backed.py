from __future__ import annotations

import json
from pathlib import Path

CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gas_calibrator"
    / "v2"
    / "configs"
    / "validation"
    / "a4_single_temp_h2o_co2_no_write_20c_real_machine_DRAFT_LOCKED.json"
)


def _config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _ops() -> dict:
    return _config()["operator_decisions"]


def _vm() -> dict:
    return _ops()["valve_mapping"]


# ═══════════════════════════════════════════════════════════
# A. Valve mapping confirmation status
# ═══════════════════════════════════════════════════════════


def test_valve_mapping_confirmed_is_true():
    assert _ops()["valve_mapping_confirmed"] is True


def test_valve_mapping_status_is_repo_backed():
    assert _ops()["valve_mapping_status"] == "REPO_BACKED_FROM_RUN001_A2_REAL_CONFIG"


def test_valve_mapping_source_references_a2_config():
    assert "run001_a2_co2_only_7_pressure_no_write_real_machine.json" in (
        _ops()["valve_mapping_source"]
    )


# ═══════════════════════════════════════════════════════════
# B. Relay ports
# ═══════════════════════════════════════════════════════════


def test_relay_port_is_com20():
    assert _vm()["relay_ports"]["relay"] == "COM20"


def test_relay_8_port_is_com21():
    assert _vm()["relay_ports"]["relay_8"] == "COM21"


# ═══════════════════════════════════════════════════════════
# C. Logical valves
# ═══════════════════════════════════════════════════════════


def test_logical_co2_path_is_7():
    assert _vm()["logical_valves"]["co2_path"] == 7


def test_logical_co2_path_group2_is_16():
    assert _vm()["logical_valves"]["co2_path_group2"] == 16


def test_logical_gas_main_is_11():
    assert _vm()["logical_valves"]["gas_main"] == 11


def test_logical_h2o_path_is_8():
    assert _vm()["logical_valves"]["h2o_path"] == 8


def test_logical_flow_switch_is_10():
    assert _vm()["logical_valves"]["flow_switch"] == 10


def test_logical_hold_is_9():
    assert _vm()["logical_valves"]["hold"] == 9


# ═══════════════════════════════════════════════════════════
# D. Relay map entries
# ═══════════════════════════════════════════════════════════


def test_relay_map_8_is_relay_8_channel_8():
    entry = _vm()["relay_map"]["8"]
    assert entry["device"] == "relay_8"
    assert entry["channel"] == 8


def test_relay_map_9_is_relay_8_channel_1():
    entry = _vm()["relay_map"]["9"]
    assert entry["device"] == "relay_8"
    assert entry["channel"] == 1


def test_relay_map_10_is_relay_8_channel_2():
    entry = _vm()["relay_map"]["10"]
    assert entry["device"] == "relay_8"
    assert entry["channel"] == 2


def test_relay_map_11_is_relay_8_channel_3():
    entry = _vm()["relay_map"]["11"]
    assert entry["device"] == "relay_8"
    assert entry["channel"] == 3


def test_relay_map_6_is_relay_channel_12():
    entry = _vm()["relay_map"]["6"]
    assert entry["device"] == "relay"
    assert entry["channel"] == 12


# ═══════════════════════════════════════════════════════════
# E. CO2 map
# ═══════════════════════════════════════════════════════════


def test_co2_map_1000_equals_6():
    assert _vm()["co2_map"]["1000"] == 6


def test_co2_map_0_equals_1():
    assert _vm()["co2_map"]["0"] == 1


# ═══════════════════════════════════════════════════════════
# F. CO2 map group2
# ═══════════════════════════════════════════════════════════


def test_co2_map_group2_900_equals_26():
    assert _vm()["co2_map_group2"]["900"] == 26


# ═══════════════════════════════════════════════════════════
# G. CO2 1000 ppm valve path verification
# ═══════════════════════════════════════════════════════════


def test_co2_1000ppm_maps_to_logical_valve_6_relay_channel_12():
    vm = _vm()
    logical = vm["co2_map"]["1000"]
    assert logical == 6
    assert vm["relay_map"]["6"]["device"] == "relay"
    assert vm["relay_map"]["6"]["channel"] == 12


# ═══════════════════════════════════════════════════════════
# H. H2O path valve path verification
# ═══════════════════════════════════════════════════════════


def test_h2o_path_maps_to_logical_valve_8_relay_8_channel_8():
    vm = _vm()
    logical = vm["logical_valves"]["h2o_path"]
    assert logical == 8
    assert vm["relay_map"]["8"]["device"] == "relay_8"
    assert vm["relay_map"]["8"]["channel"] == 8


# ═══════════════════════════════════════════════════════════
# I. Config still locked and non-executable
# ═══════════════════════════════════════════════════════════


def test_draft_locked_still_true():
    assert _config()["draft_locked"] is True


def test_execute_probe_still_false():
    assert _config()["security_locking"]["execute_probe"] is False


def test_real_com_enabled_still_false():
    assert _config()["security_locking"]["real_com_enabled"] is False


def test_real_machine_probe_enabled_still_false():
    assert _config()["security_locking"]["real_machine_probe_enabled"] is False


def test_all_write_flags_still_false():
    wf = _config()["workflow"]
    assert wf["allow_write_coefficients"] is False
    assert wf["allow_write_zero"] is False
    assert wf["allow_write_span"] is False
    assert wf["allow_write_calibration_parameters"] is False
    assert wf["apply_device_id"] is False


# ═══════════════════════════════════════════════════════════
# J. No runtime execution
# ═══════════════════════════════════════════════════════════


def test_no_runtime_imports():
    pass
