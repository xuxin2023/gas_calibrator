from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.v2.core.real_com_probe_gate import (
    REAL_COM_PROBE_ENV_VAR,
    evaluate_conditioning_only_real_com_gate,
)


def _base_config() -> dict:
    return {
        "scope": "conditioning_only",
        "co2_only": True,
        "skip0": True,
        "single_route": True,
        "single_temperature": True,
        "no_write": True,
        "h2o_enabled": False,
        "full_group_enabled": False,
        "a2_enabled": False,
        "a3_enabled": False,
        "vent_off_enabled": False,
        "seal_enabled": False,
        "high_pressure_enabled": False,
        "sample_enabled": False,
        "real_primary_latest_refresh": False,
    }


def _operator_confirmation(tmp_path: Path) -> Path:
    payload = {
        "operator_name": "test-operator",
        "timestamp": "2026-04-27T18:30:00+08:00",
        "branch": "codex/run001-a1-no-write-dry-run",
        "HEAD": "a9688ef5f4461f623e6bd4ef60a23831d437fde1",
        "config_path": str(tmp_path / "config.json"),
        "port_manifest": {
            "pressure_controller": {"port": "COM31", "opened": False},
            "pressure_gauge": {"port": "COM30", "opened": False},
        },
        "explicit_acknowledgement": {
            "conditioning_only": True,
            "no_write": True,
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
    return evaluate_conditioning_only_real_com_gate(
        config,
        cli_allow=cli,
        env={REAL_COM_PROBE_ENV_VAR: "1"} if env else {},
        operator_confirmation_path=_operator_confirmation(tmp_path),
    )


def test_real_com_gate_rejects_without_cli_flag(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path, cli=False, env=True)
    assert admission.approved is False
    assert "missing_cli_flag_allow_v2_conditioning_only_real_com" in admission.reasons


def test_real_com_gate_rejects_cli_without_env(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path, cli=True, env=False)
    assert admission.approved is False
    assert "missing_env_gas_cal_v2_conditioning_only_real_com" in admission.reasons


def test_real_com_gate_rejects_env_without_cli(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path, cli=False, env=True)
    assert admission.approved is False
    assert "missing_cli_flag_allow_v2_conditioning_only_real_com" in admission.reasons


def test_real_com_gate_rejects_missing_operator_confirmation_json() -> None:
    admission = evaluate_conditioning_only_real_com_gate(
        _base_config(),
        cli_allow=True,
        env={REAL_COM_PROBE_ENV_VAR: "1"},
        operator_confirmation_path=None,
    )
    assert admission.approved is False
    assert "missing_operator_confirmation_json" in admission.reasons


def test_real_com_gate_rejects_non_conditioning_scope(tmp_path: Path) -> None:
    config = _base_config()
    config["scope"] = "a2"
    admission = _evaluate(config, tmp_path)
    assert admission.approved is False
    assert "config_scope_not_conditioning_only" in admission.reasons


def test_real_com_gate_rejects_no_write_false(tmp_path: Path) -> None:
    config = _base_config()
    config["no_write"] = False
    admission = _evaluate(config, tmp_path)
    assert admission.approved is False
    assert "config_no_write_not_true" in admission.reasons


def test_real_com_gate_rejects_h2o_enabled(tmp_path: Path) -> None:
    config = _base_config()
    config["h2o_enabled"] = True
    admission = _evaluate(config, tmp_path)
    assert admission.approved is False
    assert "config_h2o_not_disabled" in admission.reasons


def test_real_com_gate_rejects_full_group_enabled(tmp_path: Path) -> None:
    config = _base_config()
    config["full_group_enabled"] = True
    admission = _evaluate(config, tmp_path)
    assert admission.approved is False
    assert "config_full_group_not_disabled" in admission.reasons


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("vent_off_enabled", "config_vent_off_enabled_not_disabled"),
        ("seal_enabled", "config_seal_enabled_not_disabled"),
        ("high_pressure_enabled", "config_high_pressure_enabled_not_disabled"),
        ("sample_enabled", "config_sample_enabled_not_disabled"),
    ],
)
def test_real_com_gate_rejects_forbidden_runtime_capability(
    tmp_path: Path,
    field: str,
    reason: str,
) -> None:
    config = _base_config()
    config[field] = True
    admission = _evaluate(config, tmp_path)
    assert admission.approved is False
    assert reason in admission.reasons


def test_real_com_gate_rejects_real_primary_latest_refresh(tmp_path: Path) -> None:
    config = _base_config()
    config["real_primary_latest_refresh"] = True
    admission = _evaluate(config, tmp_path)
    assert admission.approved is False
    assert "config_real_primary_latest_refresh_not_disabled" in admission.reasons


def test_real_com_gate_approves_admission_only_without_opening_com(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path)
    assert admission.approved is True
    assert admission.reasons == ()
    assert admission.evidence["admission_approved"] is True
    assert admission.evidence["gate_only"] is True
    assert admission.evidence["real_com_opened"] is False
    assert admission.evidence["real_probe_executed"] is False
    assert admission.evidence["attempted_write_count"] == 0
    assert admission.evidence["identity_write_command_sent"] is False
    assert admission.evidence["calibration_write_command_sent"] is False
    assert admission.evidence["senco_write_command_sent"] is False


def test_real_com_gate_evidence_marks_engineering_probe_only_blocked(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path)
    assert admission.evidence["evidence_source"] == "real_probe_conditioning_only"
    assert admission.evidence["not_real_acceptance_evidence"] is True
    assert admission.evidence["acceptance_level"] == "engineering_probe_only"
    assert admission.evidence["promotion_state"] == "blocked"
    assert admission.evidence["real_primary_latest_refresh"] is False
