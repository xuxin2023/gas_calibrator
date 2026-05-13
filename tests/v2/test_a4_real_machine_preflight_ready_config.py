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
    / "a4_single_temp_h2o_co2_no_write_20c_real_machine_PREFLIGHT_READY.json"
)


def _config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _ops() -> dict:
    return _config()["operator_decisions"]


def _payload_str() -> str:
    return CONFIG_PATH.read_text(encoding="utf-8").lower()


# ═══════════════════════════════════════════════════════════
# A. File existence and profile stage
# ═══════════════════════════════════════════════════════════


def test_config_file_exists():
    assert CONFIG_PATH.exists(), f"missing: {CONFIG_PATH}"


def test_profile_stage_is_p16_preflight_ready():
    assert _config()["profile_stage"] == "A4_P16_PREFLIGHT_READY"


def test_real_machine_config_is_true():
    assert _config()["real_machine_config"] is True


def test_preflight_ready_is_true():
    assert _config()["preflight_ready"] is True


def test_draft_locked_is_false():
    assert _config()["draft_locked"] is False


# ═══════════════════════════════════════════════════════════
# B. All execution gates still OFF
# ═══════════════════════════════════════════════════════════


def test_execute_probe_is_false():
    assert _config()["security_locking"]["execute_probe"] is False


def test_real_com_enabled_is_false():
    assert _config()["security_locking"]["real_com_enabled"] is False


def test_real_machine_probe_enabled_is_false():
    assert _config()["security_locking"]["real_machine_probe_enabled"] is False


def test_production_enabled_is_false():
    assert _config()["security_locking"]["production_enabled"] is False


def test_controlled_write_is_false():
    assert _config()["security_locking"]["controlled_write"] is False


def test_formal_switch_is_false():
    assert _config()["security_locking"]["formal_switch"] is False


def test_v2_replaces_v1_is_false():
    assert _config()["security_locking"]["v2_replaces_v1"] is False


def test_disable_v1_is_false():
    assert _config()["security_locking"]["disable_v1"] is False


# ═══════════════════════════════════════════════════════════
# C. No-write guards
# ═══════════════════════════════════════════════════════════


def test_collect_only_is_true():
    assert _config()["workflow"]["collect_only"] is True


def test_no_write_guard_active_is_true():
    assert _config()["workflow"]["no_write_guard_active"] is True


def test_all_write_flags_are_false():
    wf = _config()["workflow"]
    assert wf["allow_write_coefficients"] is False
    assert wf["allow_write_zero"] is False
    assert wf["allow_write_span"] is False
    assert wf["allow_write_calibration_parameters"] is False


def test_apply_device_id_is_false():
    assert _config()["workflow"]["apply_device_id"] is False


# ═══════════════════════════════════════════════════════════
# D. Confirmed values
# ═══════════════════════════════════════════════════════════


def test_co2_ppm_value_is_1000():
    assert _ops()["co2_ppm_value"] == 1000.0
    assert _ops()["co2_ppm_confirmed"] is True


def test_selected_temps_c_is_20c():
    assert _config()["workflow"]["selected_temps_c"] == [20.0]


def test_route_mode_is_h2o_then_co2():
    assert _config()["workflow"]["route_mode"] == "h2o_then_co2"


def test_total_points_is_15():
    assert _config()["workflow"]["total_points"] == 15


def test_operator_approval_confirmed():
    assert _ops()["operator_approval_phrase_confirmed"] is True
    assert _ops()["operator_approval_phrase_entered"] is True


# ═══════════════════════════════════════════════════════════
# E. Device ports
# ═══════════════════════════════════════════════════════════


def test_device_ports_contain_all_analyzers():
    devs = _config()["devices"]
    ports = {(a["port"], a["device_id"]) for a in devs["gas_analyzers"]}
    assert ("COM35", "001") in ports
    assert ("COM37", "029") in ports
    assert ("COM41", "003") in ports
    assert ("COM42", "004") in ports


def test_device_ports_contain_all_aux_devices():
    devs = _config()["devices"]
    assert devs["humidity_generator"]["port"] == "COM16"
    assert devs["dewpoint_meter"]["port"] == "COM17"
    assert devs["thermometer"]["port"] == "COM18"
    assert devs["temperature_chamber"]["port"] == "COM19"
    assert devs["relay"]["port"] == "COM20"
    assert devs["relay_8"]["port"] == "COM21"
    assert devs["pressure_gauge"]["port"] == "COM22"
    assert devs["pressure_controller"]["port"] == "COM23"


# ═══════════════════════════════════════════════════════════
# F. Valve mapping
# ═══════════════════════════════════════════════════════════


def test_valve_mapping_confirmed():
    assert _ops()["valve_mapping_confirmed"] is True


def test_co2_1000ppm_maps_to_logical_6_channel_12():
    vm = _ops()["valve_mapping"]
    assert vm["co2_map"]["1000"] == 6
    assert vm["relay_map"]["6"]["device"] == "relay"
    assert vm["relay_map"]["6"]["channel"] == 12


def test_h2o_path_maps_to_logical_8_relay_8_channel_8():
    vm = _ops()["valve_mapping"]
    assert vm["logical_valves"]["h2o_path"] == 8
    assert vm["relay_map"]["8"]["device"] == "relay_8"
    assert vm["relay_map"]["8"]["channel"] == 8


# ═══════════════════════════════════════════════════════════
# G. Preflight command preview
# ═══════════════════════════════════════════════════════════


def test_command_preview_is_preview_only():
    cmd = _config()["preflight_command_preview"]
    assert cmd["command_type"] == "preview_only"


def test_execute_allowed_in_this_commit_is_false():
    cmd = _config()["preflight_command_preview"]
    assert cmd["execute_allowed_in_this_commit"] is False


def test_execution_allowed_only_in_p17():
    cmd = _config()["preflight_command_preview"]
    assert "A4_P17" in cmd["execution_allowed_only_in"]


# ═══════════════════════════════════════════════════════════
# H. Real run unlock requirements
# ═══════════════════════════════════════════════════════════


def test_unlock_requirements_mention_p17_only():
    req = _config()["real_run_unlock_requirements"]
    assert req["allowed_in_phase"] == "A4-P17 only"
    assert req["p16_must_not_execute"] is True
    assert req["execute_probe_requires_separate_p17_command"] is True


def test_unlock_requirements_operator_safety():
    req = _config()["real_run_unlock_requirements"]
    assert req["operator_physically_present"] is True
    assert req["emergency_stop_available"] is True
    assert req["v1_fallback_verified"] is True


# ═══════════════════════════════════════════════════════════
# I. Payload must not contain executable/allow_write=true
# ═══════════════════════════════════════════════════════════


def test_payload_has_no_execute_probe_true():
    payload = _payload_str()
    assert '"execute_probe": true' not in payload


def test_payload_has_no_allow_write_true():
    payload = _payload_str()
    forbidden = [
        '"allow_write_coefficients": true',
        '"allow_write_zero": true',
        '"allow_write_span": true',
        '"allow_write_calibration_parameters": true',
        '"apply_device_id": true',
        '"production_enabled": true',
        '"controlled_write": true',
        '"formal_switch": true',
    ]
    for pattern in forbidden:
        assert pattern not in payload, f"forbidden: {pattern}"
