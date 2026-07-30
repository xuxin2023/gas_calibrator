from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from gas_calibrator.v2.core import (
    run001_query_only_real_com_probe as query_probe,
)
from gas_calibrator.v2.core.run001_query_only_real_com_probe import (
    QUERY_ONLY_EVIDENCE_MARKERS,
    QUERY_ONLY_REAL_COM_ENV_VAR,
    evaluate_query_only_real_com_gate,
    write_query_only_real_com_probe_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTROL_FLAGS = (
    "route_open_enabled",
    "sample_enabled",
    "relay_output_enabled",
    "valve_command_enabled",
    "pressure_setpoint_enabled",
    "vent_off_enabled",
    "seal_enabled",
    "high_pressure_enabled",
    "a1r_enabled",
    "a2_enabled",
    "a3_enabled",
    "analyzer_id_write_enabled",
    "mode_switch_enabled",
    "senco_write_enabled",
    "calibration_write_enabled",
)


def _query_only_config() -> dict:
    config = {
        "scope": "query_only",
        "query_only": True,
        "no_write": True,
        "h2o_enabled": False,
        "full_group_enabled": False,
        "real_primary_latest_refresh": False,
        "devices": {
            "pressure_controller": {
                "enabled": True,
                "port": "COM23",
                "baud": 9600,
            },
            "dewpoint_meter": {"enabled": False, "port": "COM17"},
            "humidity_generator": {"enabled": False, "port": "COM16"},
        },
    }
    config.update({name: False for name in CONTROL_FLAGS})
    return config


def _operator_confirmation(tmp_path: Path) -> Path:
    acknowledgement = {
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
        "no_mode_switch": True,
        "query_only_not_state_neutral": True,
        "pressure_p3_may_cancel_continuous_output": True,
        "no_id_write": True,
        "no_senco_write": True,
        "no_calibration_write": True,
        "no_chamber_write_register": True,
        "no_chamber_set_temperature": True,
        "no_chamber_start": True,
        "no_chamber_stop": True,
        "not_real_acceptance": True,
        "engineering_probe_only": True,
        "v1_fallback_required": True,
        "real_primary_latest_refresh": False,
    }
    payload = {
        "operator_name": "offline-test",
        "timestamp": "2026-07-28T00:00:00+08:00",
        "branch": "offline-test",
        "HEAD": "offline-test",
        "config_path": str(tmp_path / "r0_config.json"),
        "port_manifest": {"pressure_controller": "COM23"},
        "explicit_acknowledgement": acknowledgement,
    }
    path = tmp_path / "operator_confirmation.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("cli_allow", "env", "confirmation", "reason"),
    [
        (
            False,
            {QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
            True,
            "missing_cli_flag_allow_v2_query_only_real_com",
        ),
        (True, {}, True, "missing_env_gas_cal_v2_query_only_real_com"),
        (
            True,
            {QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
            False,
            "missing_operator_confirmation_json",
        ),
    ],
)
def test_r0_gate_requires_all_three_unlock_records(
    tmp_path: Path,
    cli_allow: bool,
    env: dict[str, str],
    confirmation: bool,
    reason: str,
) -> None:
    admission = evaluate_query_only_real_com_gate(
        _query_only_config(),
        cli_allow=cli_allow,
        env=env,
        operator_confirmation_path=(
            _operator_confirmation(tmp_path) if confirmation else None
        ),
    )

    assert admission.approved is False
    assert reason in admission.reasons
    assert admission.evidence["real_com_opened"] is False
    assert admission.evidence["real_probe_executed"] is False


@pytest.mark.parametrize(
    "field", ["route_open_enabled", "sample_enabled", "senco_write_enabled"]
)
def test_r0_gate_rejects_any_control_or_write_capability(
    tmp_path: Path,
    field: str,
) -> None:
    config = _query_only_config()
    config[field] = True

    admission = evaluate_query_only_real_com_gate(
        config,
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
    )

    assert admission.approved is False
    assert f"config_{field}_not_disabled" in admission.reasons


def test_r0_gate_requires_explicit_port_without_historical_fallback(
    tmp_path: Path,
) -> None:
    config = _query_only_config()
    config["devices"]["pressure_controller"]["port"] = ""

    admission = evaluate_query_only_real_com_gate(
        config,
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
    )

    assert admission.approved is False
    assert "config_pressure_controller_port_missing" in admission.reasons
    assert admission.evidence["real_com_opened"] is False


@pytest.mark.parametrize(
    "acknowledgement",
    [
        "query_only_not_state_neutral",
        "pressure_p3_may_cancel_continuous_output",
    ],
)
def test_r0_gate_rejects_missing_query_state_effect_acknowledgement(
    tmp_path: Path,
    acknowledgement: str,
) -> None:
    confirmation_path = _operator_confirmation(tmp_path)
    confirmation = json.loads(confirmation_path.read_text(encoding="utf-8"))
    del confirmation["explicit_acknowledgement"][acknowledgement]
    confirmation_path.write_text(json.dumps(confirmation), encoding="utf-8")

    admission = evaluate_query_only_real_com_gate(
        _query_only_config(),
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=confirmation_path,
    )

    assert admission.approved is False
    assert f"operator_ack_missing_{acknowledgement}" in admission.reasons
    assert admission.evidence["query_only_state_neutral"] is False
    assert admission.evidence["pressure_p3_may_cancel_continuous_output"] is True


def test_r0_approved_dry_admission_never_opens_com_or_promotes(
    tmp_path: Path,
) -> None:
    opened: list[str] = []

    def forbidden_open(device: dict) -> None:
        opened.append(str(device.get("port")))
        raise AssertionError("dry admission must not open COM")

    summary = write_query_only_real_com_probe_artifacts(
        _query_only_config(),
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        branch="offline-test",
        head="offline-test",
        execute_query_only=False,
        serial_factory=forbidden_open,
    )

    assert opened == []
    assert summary["schema_version"] == "v2.run001.query_only_real_com_probe.2"
    assert summary["final_decision"] == "ADMISSION_APPROVED"
    assert summary["execute_query_only"] is False
    assert summary["real_com_opened"] is False
    assert summary["real_probe_executed"] is False
    assert summary["serial_query_command_bytes_sent"] is False
    assert summary["pressure_p3_query_command_sent"] is False
    assert summary["pressure_continuous_output_cancel_possible"] is False
    assert summary["pressure_continuous_output_cancel_attempted"] is False
    assert summary["query_state_effect_class"] == "no_pressure_p3_query_executed"
    for key, expected in QUERY_ONLY_EVIDENCE_MARKERS.items():
        assert summary[key] == expected


def test_r0_simulated_p3_evidence_separates_query_bytes_from_persistent_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _query_only_config()
    config["devices"] = {
        "pressure_gauge": {
            "enabled": True,
            "port": "COM22",
            "baud": 9600,
            "dest_id": "01",
        },
        "dewpoint_meter": {"enabled": False, "port": "COM17"},
        "humidity_generator": {"enabled": False, "port": "COM16"},
    }

    def simulated_pressure_reader(
        _raw_cfg: dict,
        *,
        serial_factory: object,
    ) -> tuple[dict, list[dict]]:
        del serial_factory
        return (
            {
                "device_name": "pressure_gauge",
                "device_type": "pressure_gauge",
                "port": "COM22",
                "dest_id": "01",
                "pressure_gauge_unavailable": False,
                "pressure_gauge_blocks_r1": False,
                "pressure_gauge_probe_status": "readonly_available",
                "paroscientific_p3_read_attempted": True,
                "paroscientific_p3_read_succeeded": True,
                "continuous_cancel_sent": True,
                "pre_cancel_continuous_attempted": True,
                "parsed_pressure_hpa": 1000.0,
                "parse_status": "parse_ok",
            },
            [
                {
                    "device_name": "pressure_gauge",
                    "device_type": "pressure_gauge",
                    "port": "COM22",
                    "action": "read_pressure_open",
                    "result": "ok",
                    "details": {},
                }
            ],
        )

    monkeypatch.setattr(
        query_probe,
        "read_pressure_gauge_raw_capture",
        simulated_pressure_reader,
    )
    summary = write_query_only_real_com_probe_artifacts(
        config,
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        branch="offline-test",
        head="offline-test",
        execute_query_only=True,
        serial_factory=lambda _device: (_ for _ in ()).throw(
            AssertionError("mocked pressure reader must not open a real COM port")
        ),
    )

    assert summary["pressure_p3_query_command_sent"] is True
    assert summary["serial_query_command_bytes_sent"] is True
    assert summary["pressure_continuous_output_cancel_possible"] is True
    assert summary["pressure_continuous_output_cancel_attempted"] is True
    assert summary["query_state_effect_class"] == (
        "volatile_pressure_output_cancel_possible"
    )
    assert summary["query_only_state_neutral"] is False
    assert summary["pressure_p3_is_persistent_write"] is False
    assert summary["persistent_device_state_write_sent"] is False
    assert summary["any_write_command_sent"] is False
    assert summary["mode_switch_command_sent"] is False
    assert summary["promotion_state"] == "blocked"


def test_r0_entrypoint_import_only_self_check_is_offline(tmp_path: Path) -> None:
    script = (
        REPO_ROOT
        / "src"
        / "gas_calibrator"
        / "v2"
        / "scripts"
        / "query_only_com_sanity_probe.py"
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)

    completed = subprocess.run(
        [sys.executable, str(script), "--self-check-import-only"],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload == {
        "temperature_chamber_probe_import_path_fixed": True,
        "temperature_chamber_import_ok": True,
        "temperature_chamber_port_identity_confirmed": False,
        "real_com_opened": False,
        "query_only_probe_executed": False,
        "query_only_state_neutral": False,
        "pressure_p3_may_cancel_continuous_output": True,
        "pressure_p3_is_persistent_write": False,
    }
