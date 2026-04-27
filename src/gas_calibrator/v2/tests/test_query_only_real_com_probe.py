from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.v2.core.run001_query_only_real_com_probe import (
    QUERY_ONLY_EVIDENCE_MARKERS,
    QUERY_ONLY_REAL_COM_ENV_VAR,
    evaluate_query_only_real_com_gate,
    write_query_only_real_com_probe_artifacts,
)


def _base_config() -> dict:
    return {
        "scope": "query_only",
        "query_only": True,
        "no_write": True,
        "h2o_enabled": False,
        "full_group_enabled": False,
        "route_open_enabled": False,
        "sample_enabled": False,
        "relay_output_enabled": False,
        "valve_command_enabled": False,
        "pressure_setpoint_enabled": False,
        "vent_off_enabled": False,
        "seal_enabled": False,
        "high_pressure_enabled": False,
        "a1r_enabled": False,
        "a2_enabled": False,
        "a3_enabled": False,
        "analyzer_id_write_enabled": False,
        "mode_switch_enabled": False,
        "senco_write_enabled": False,
        "calibration_write_enabled": False,
        "real_primary_latest_refresh": False,
        "devices": {
            "pressure_controller": {"enabled": True, "port": "COM31", "baud": 9600},
            "pressure_gauge": {"enabled": True, "port": "COM30", "baud": 9600},
            "temperature_chamber": {"enabled": True, "port": "COM27", "baud": 9600},
            "thermometer": {"enabled": True, "port": "COM26", "baud": 2400},
            "relay": {"enabled": True, "port": "COM28", "baud": 38400},
            "relay_8": {"enabled": True, "port": "COM29", "baud": 38400},
            "dewpoint_meter": {"enabled": False, "port": "COM25", "baud": 9600},
            "humidity_generator": {"enabled": False, "port": "COM24", "baud": 9600},
            "gas_analyzers": [
                {"name": "ga01", "enabled": True, "port": "COM35", "baud": 115200, "device_id": "001"},
                {"name": "ga02", "enabled": True, "port": "COM37", "baud": 115200, "device_id": "029"},
                {"name": "ga03", "enabled": True, "port": "COM41", "baud": 115200, "device_id": "003"},
                {"name": "ga04", "enabled": True, "port": "COM42", "baud": 115200, "device_id": "004"},
            ],
        },
    }


def _operator_confirmation(tmp_path: Path) -> Path:
    payload = {
        "operator_name": "test-operator",
        "timestamp": "2026-04-27T19:00:00+08:00",
        "branch": "codex/run001-a1-no-write-dry-run",
        "HEAD": "4c5facec951ce168bb4564f19361aa82644049a0",
        "config_path": str(tmp_path / "r0_config.json"),
        "port_manifest": {
            "pressure_controller": "COM31",
            "pressure_gauge": "COM30",
            "temperature_chamber": "COM27",
            "thermometer": "COM26",
            "relay": "COM28",
            "relay_8": "COM29",
            "gas_analyzers": ["COM35", "COM37", "COM41", "COM42"],
            "h2o_disabled": ["COM25", "COM24"],
        },
        "explicit_acknowledgement": {
            "query_only": True,
            "no_write": True,
            "no_route_open": True,
            "no_relay_output": True,
            "no_valve_command": True,
            "no_pressure_setpoint": True,
            "no_seal": True,
            "no_vent_off": True,
            "no_high_pressure": True,
            "no_sample": True,
            "not_real_acceptance": True,
            "v1_fallback_required": True,
        },
    }
    path = tmp_path / "operator_confirmation.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _evaluate(config: dict, tmp_path: Path, *, cli: bool = True, env: bool = True):
    return evaluate_query_only_real_com_gate(
        config,
        cli_allow=cli,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"} if env else {},
        operator_confirmation_path=_operator_confirmation(tmp_path),
    )


def test_query_only_gate_rejects_without_cli_flag(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path, cli=False, env=True)
    assert admission.approved is False
    assert "missing_cli_flag_allow_v2_query_only_real_com" in admission.reasons


def test_query_only_gate_rejects_cli_without_env(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path, cli=True, env=False)
    assert admission.approved is False
    assert "missing_env_gas_cal_v2_query_only_real_com" in admission.reasons


def test_query_only_gate_rejects_env_without_cli(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path, cli=False, env=True)
    assert admission.approved is False
    assert "missing_cli_flag_allow_v2_query_only_real_com" in admission.reasons


def test_query_only_gate_rejects_missing_operator_confirmation_json() -> None:
    admission = evaluate_query_only_real_com_gate(
        _base_config(),
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=None,
    )
    assert admission.approved is False
    assert "missing_operator_confirmation_json" in admission.reasons


def test_query_only_gate_rejects_h2o_enabled(tmp_path: Path) -> None:
    config = _base_config()
    config["h2o_enabled"] = True
    admission = _evaluate(config, tmp_path)
    assert admission.approved is False
    assert "config_h2o_not_disabled" in admission.reasons


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("sample_enabled", "config_sample_enabled_not_disabled"),
        ("route_open_enabled", "config_route_open_enabled_not_disabled"),
        ("relay_output_enabled", "config_relay_output_enabled_not_disabled"),
        ("valve_command_enabled", "config_valve_command_enabled_not_disabled"),
        ("pressure_setpoint_enabled", "config_pressure_setpoint_enabled_not_disabled"),
    ],
)
def test_query_only_gate_rejects_control_capabilities(tmp_path: Path, field: str, reason: str) -> None:
    config = _base_config()
    config[field] = True
    admission = _evaluate(config, tmp_path)
    assert admission.approved is False
    assert reason in admission.reasons


def test_query_only_approved_dry_admission_does_not_open_com(tmp_path: Path) -> None:
    opened: list[str] = []

    def forbidden_open(device):
        opened.append(str(device.get("port")))
        raise AssertionError("dry admission must not open COM")

    summary = write_query_only_real_com_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        execute_query_only=False,
        serial_factory=forbidden_open,
    )

    assert opened == []
    assert summary["admission_approved"] is True
    assert summary["execute_query_only"] is False
    assert summary["real_com_opened"] is False
    assert summary["real_probe_executed"] is False
    assert summary["final_decision"] == "ADMISSION_APPROVED"


def test_query_only_evidence_markers_and_no_write_counts(tmp_path: Path) -> None:
    summary = write_query_only_real_com_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        execute_query_only=False,
    )

    assert summary["evidence_source"] == "real_probe_query_only"
    assert summary["not_real_acceptance_evidence"] is True
    assert summary["acceptance_level"] == "engineering_probe_only"
    assert summary["promotion_state"] == "blocked"
    assert summary["real_primary_latest_refresh"] is False
    for key, expected in QUERY_ONLY_EVIDENCE_MARKERS.items():
        assert summary[key] == expected
    assert summary["attempted_write_count"] == 0
    assert summary["identity_write_command_sent"] is False
    assert summary["calibration_write_command_sent"] is False
    assert summary["senco_write_command_sent"] is False
    assert summary["route_open_command_sent"] is False
    assert summary["relay_output_command_sent"] is False
    assert summary["valve_command_sent"] is False
    assert summary["pressure_setpoint_command_sent"] is False
    assert summary["vent_off_sent"] is False
    assert summary["seal_command_sent"] is False
    assert summary["sample_count"] == 0
    assert summary["points_completed"] == 0

    artifact_paths = summary["artifact_paths"]
    assert Path(artifact_paths["summary"]).exists()
    assert Path(artifact_paths["device_inventory"]).exists()
    assert Path(artifact_paths["query_results"]).exists()
    assert Path(artifact_paths["port_open_close_trace"]).exists()
    assert Path(artifact_paths["operator_confirmation_record"]).exists()
