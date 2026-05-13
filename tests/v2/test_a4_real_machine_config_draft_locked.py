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


def _payload_str() -> str:
    return CONFIG_PATH.read_text(encoding="utf-8")


# ═══════════════════════════════════════════════════════════
# A. File existence and basic structure
# ═══════════════════════════════════════════════════════════


def test_config_file_exists():
    assert CONFIG_PATH.exists(), f"missing: {CONFIG_PATH}"


def test_config_is_valid_json():
    cfg = _config()
    assert isinstance(cfg, dict)
    assert len(cfg) > 5


# ═══════════════════════════════════════════════════════════
# B. Locking and approval gates
# ═══════════════════════════════════════════════════════════


def test_real_machine_config_is_true():
    assert _config()["real_machine_config"] is True


def test_draft_locked_is_true():
    assert _config()["draft_locked"] is True


def test_operator_approval_required_is_true():
    assert _config()["operator_approval_required"] is True


def test_operator_approval_phrase_entered_is_false():
    assert _config()["operator_approval_phrase_entered"] is False


def test_approval_phrase_matches_required():
    cfg = _config()
    assert cfg["operator_approval_phrase_required"] == (
        "APPROVE_A4_SINGLE_TEMP_H2O_CO2_NO_WRITE_PREFLIGHT"
    )


# ═══════════════════════════════════════════════════════════
# C. Security locking — all execution/write gates OFF
# ═══════════════════════════════════════════════════════════


def test_execute_probe_is_false():
    assert _config()["security_locking"]["execute_probe"] is False


def test_real_com_enabled_is_false():
    assert _config()["security_locking"]["real_com_enabled"] is False


def test_real_machine_probe_enabled_is_false():
    assert _config()["security_locking"]["real_machine_probe_enabled"] is False


def test_production_and_write_locks_are_false():
    sec = _config()["security_locking"]
    assert sec["production_enabled"] is False
    assert sec["controlled_write"] is False
    assert sec["formal_switch"] is False


# ═══════════════════════════════════════════════════════════
# D. No-write guards
# ═══════════════════════════════════════════════════════════


def test_collect_only_and_no_write_guard_active():
    wf = _config()["workflow"]
    assert wf["collect_only"] is True
    assert wf["no_write_guard_active"] is True
    assert wf["no_write"] is True


def test_all_write_flags_are_false():
    wf = _config()["workflow"]
    assert wf["allow_write_coefficients"] is False
    assert wf["allow_write_zero"] is False
    assert wf["allow_write_span"] is False
    assert wf["allow_write_calibration_parameters"] is False
    assert wf["apply_device_id"] is False


def test_payload_has_no_write_true():
    payload = _payload_str().lower()
    forbidden = [
        '"allow_write_coefficients": true',
        '"allow_write_zero": true',
        '"allow_write_span": true',
        '"allow_write_calibration_parameters": true',
        '"apply_device_id": true',
        '"production_enabled": true',
        '"controlled_write": true',
        '"formal_switch": true',
        '"execute_probe": true',
    ]
    for pattern in forbidden:
        assert pattern not in payload, f"forbidden pattern found: {pattern}"


# ═══════════════════════════════════════════════════════════
# E. Route and temperature
# ═══════════════════════════════════════════════════════════


def test_route_mode_is_h2o_then_co2():
    assert _config()["workflow"]["route_mode"] == "h2o_then_co2"


def test_selected_temps_is_single_20c():
    assert _config()["workflow"]["selected_temps_c"] == [20.0]


def test_co2_ambient_open_is_deferred():
    assert _config()["workflow"]["co2_ambient_open"] == "deferred"


# ═══════════════════════════════════════════════════════════
# F. CO2 ppm / COM mapping need user decision
# ═══════════════════════════════════════════════════════════


def test_co2_ppm_is_confirmed():
    ops = _config()["operator_decisions"]
    assert ops["co2_ppm_confirmed"] is True
    assert ops["co2_ppm_value"] == 1000.0


def test_device_ports_are_populated_from_repo_evidence():
    ops = _config()["operator_decisions"]
    ports = ops["device_ports"]
    assert isinstance(ports, dict)
    assert ports["analyzers_confirmed"] is True
    assert len(ports["analyzer_ports"]) == 4
    port_ids = {(a["port"], a["device_id"]) for a in ports["analyzer_ports"]}
    assert ("COM35", "ID001") in port_ids
    assert ("COM37", "ID029") in port_ids
    assert ("COM41", "ID003") in port_ids
    assert ("COM42", "ID004") in port_ids
    assert ports["duplicate_device_id"] is False
    assert ports["analyzer_mode"] == "MODE2"
    assert ports["active_send"] is True
    assert ports["pressure_controller"]["port"] == "COM23"
    assert ports["humidity_generator"]["port"] == "COM16"
    assert ports["dewpoint_meter"]["port"] == "COM17"
    assert ports["temperature_chamber"]["port"] == "COM19"
    assert ports["pressure_gauge"]["port"] == "COM22"
    assert ports["relay"]["port"] == "COM20"
    assert ports["relay_8"]["port"] == "COM21"
    assert ports["thermometer"]["port"] == "COM18"


def test_pressure_controller_ready_is_candidate_from_repo():
    ops = _config()["operator_decisions"]
    assert ops["pressure_controller_ready"] == "CANDIDATE_CONFIRMED_BY_REPO_EVIDENCE"


def test_valve_mapping_needs_user_decision():
    ops = _config()["operator_decisions"]
    assert ops["valve_mapping_confirmed"] is False


# ═══════════════════════════════════════════════════════════
# G. Safe-stop and V1 fallback
# ═══════════════════════════════════════════════════════════


def test_safe_stop_flags_are_true():
    ss = _config()["safe_stop"]
    assert ss["valves_baseline_on_abort"] is True
    assert ss["vent_on_abort"] is True
    assert ss["pressure_control_stop_on_abort"] is True
    assert ss["v1_fallback_retained"] is True


def test_simulation_config_must_not_be_used():
    wf = _config()["workflow"]
    assert wf["simulation_config_must_not_be_used_as_real_config"] is True


def test_config_references_simulation_profile_source():
    wf = _config()["workflow"]
    assert "a4_single_temp_h2o_co2_no_write_20c_simulated.json" in (
        wf["simulation_profile_source"]
    )
