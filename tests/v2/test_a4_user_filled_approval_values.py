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


def _payload_str() -> str:
    return CONFIG_PATH.read_text(encoding="utf-8")


# ═══════════════════════════════════════════════════════════
# A. CO2 ppm confirmed by user
# ═══════════════════════════════════════════════════════════


def test_co2_ppm_confirmed_is_true():
    assert _ops()["co2_ppm_confirmed"] is True


def test_co2_ppm_value_is_1000():
    assert _ops()["co2_ppm_value"] == 1000.0


# ═══════════════════════════════════════════════════════════
# B. Operator approval phrase confirmed
# ═══════════════════════════════════════════════════════════


def test_operator_approval_phrase_confirmed_true():
    assert _ops()["operator_approval_phrase_confirmed"] is True


def test_operator_approval_phrase_entered_true():
    assert _ops()["operator_approval_phrase_entered"] is True


def test_operator_approval_phrase_entered_value_matches():
    assert _ops()["operator_approval_phrase_entered_value"] == (
        "APPROVE_A4_SINGLE_TEMP_H2O_CO2_NO_WRITE_PREFLIGHT"
    )


# ═══════════════════════════════════════════════════════════
# C. Route / temperature / points confirmed
# ═══════════════════════════════════════════════════════════


def test_route_confirmed_h2o_then_co2():
    assert _ops()["route_confirmed"] is True
    assert _ops()["route_mode_confirmed"] == "h2o_then_co2"


def test_temperature_confirmed_20c():
    assert _ops()["temperature_confirmed"] is True
    assert _ops()["selected_temps_c_confirmed"] == [20.0]


def test_points_confirmed():
    assert _ops()["points_confirmed"] is True
    assert _ops()["h2o_points_confirmed"] == "ambient_open + 7 sealed pressure"
    assert _ops()["co2_points_confirmed"] == "7 sealed pressure only"


# ═══════════════════════════════════════════════════════════
# D. No-write / V1 fallback / safe-stop confirmed
# ═══════════════════════════════════════════════════════════


def test_no_write_confirmed():
    assert _ops()["no_write_confirmed"] is True
    assert _ops()["no_parameter_write_confirmed"] is True


def test_v1_fallback_confirmed():
    assert _ops()["v1_fallback_confirmed"] is True


def test_safe_stop_confirmed():
    assert _ops()["safe_stop_confirmed"] is True


# ═══════════════════════════════════════════════════════════
# E. Analyzer COM ports confirmed
# ═══════════════════════════════════════════════════════════


def test_analyzer_ports_contain_all_four():
    ports = _ops()["device_ports"]["analyzer_ports"]
    port_ids = {(a["port"], a["device_id"]) for a in ports}
    assert ("COM35", "ID001") in port_ids
    assert ("COM37", "ID029") in port_ids
    assert ("COM41", "ID003") in port_ids
    assert ("COM42", "ID004") in port_ids


def test_duplicate_device_id_is_false():
    assert _ops()["device_ports"]["duplicate_device_id"] is False


def test_analyzer_mode_is_mode2():
    assert _ops()["device_ports"]["analyzer_mode"] == "MODE2"


def test_active_send_is_true():
    assert _ops()["device_ports"]["active_send"] is True


# ═══════════════════════════════════════════════════════════
# F. Config still locked and non-executable
# ═══════════════════════════════════════════════════════════


def test_draft_locked_is_still_true():
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


def test_production_locks_still_false():
    sec = _config()["security_locking"]
    assert sec["production_enabled"] is False
    assert sec["controlled_write"] is False
    assert sec["formal_switch"] is False


def test_simulation_config_must_not_be_used_as_real():
    assert _config()["workflow"]["simulation_config_must_not_be_used_as_real_config"] is True


# ═══════════════════════════════════════════════════════════
# G. Payload does not contain forbidden executable/allow_write=true
# ═══════════════════════════════════════════════════════════


def test_payload_has_no_execute_probe_true():
    payload = _payload_str().lower()
    assert '"execute_probe": true' not in payload


def test_payload_has_no_allow_write_true():
    payload = _payload_str().lower()
    forbidden = [
        '"allow_write_coefficients": true',
        '"allow_write_zero": true',
        '"allow_write_span": true',
        '"allow_write_calibration_parameters": true',
        '"apply_device_id": true',
    ]
    for pattern in forbidden:
        assert pattern not in payload, f"forbidden write found: {pattern}"
