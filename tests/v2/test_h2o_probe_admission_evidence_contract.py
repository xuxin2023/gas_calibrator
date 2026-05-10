from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from gas_calibrator.v2.core import run001_h2o_only_1_point_no_write_probe as h2o_gate
from gas_calibrator.v2.core.run001_h2o_only_1_point_no_write_probe import (
    H2O_ENV_VAR,
    evaluate_h2o_1_point_no_write_gate,
    load_json_mapping,
)

BRANCH = "codex/v2-golden-recovery-cdb82111"
HEAD = "59ae7125df21e1eec8a561746e378828d922e8c4"


def _config() -> dict:
    return {
        "scope": "run001_h2o_1_point",
        "h2o_only": True,
        "single_route": True,
        "single_temperature_group": True,
        "no_write": True,
        "allowed_branches": [BRANCH],
        "pressure_points_hpa": [1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0],
        "default_cutover_to_v2": False,
        "disable_v1": False,
        "v1_fallback_required": True,
        "a3_enabled": False,
        "co2_enabled": False,
        "full_group_enabled": False,
        "multi_temperature_enabled": False,
        "mode_switch_enabled": False,
        "analyzer_id_write_enabled": False,
        "senco_write_enabled": False,
        "calibration_write_enabled": False,
        "chamber_set_temperature_enabled": False,
        "chamber_start_enabled": False,
        "chamber_stop_enabled": False,
        "real_primary_latest_refresh": False,
    }


def _operator_payload(config_path: Path) -> dict:
    return {
        "operator_name": "contract_test_operator",
        "timestamp": "2026-05-10T00:00:00+00:00",
        "branch": BRANCH,
        "HEAD": HEAD,
        "config_path": str(config_path),
        "promotion_state": "blocked",
        "engineering_probe_only": True,
        "not_real_acceptance_evidence": True,
        "do_not_refresh_real_primary_latest": True,
        "port_manifest": {
            "humidity_generator": {"port": "COM16", "ready": True},
            "dewpoint_meter": {"port": "COM17", "ready": True},
            "pressure_gauge": {"port": "COM22", "ready": True},
            "pressure_controller": {"port": "COM23", "ready": True},
            "analyzers": {
                "ga01": {"port": "COM35", "ready": True},
                "ga02": {"port": "COM37", "ready": True},
                "ga03": {"port": "COM41", "ready": True},
                "ga04": {"port": "COM42", "ready": True},
            },
            "h2o_route_valves": {"ready": True},
            "v1_fallback": {"available": True, "required": True},
        },
        "explicit_acknowledgement": {
            "h2o_only": True,
            "only_h2o_1_point_no_write": True,
            "single_route": True,
            "single_temperature": True,
            "single_pressure_point": True,
            "skip0": True,
            "no_write": True,
            "no_id_write": True,
            "no_senco_write": True,
            "no_calibration_write": True,
            "no_chamber_sv_write": True,
            "no_chamber_set_temperature": True,
            "no_chamber_start": True,
            "no_chamber_stop": True,
            "no_mode_switch": True,
            "engineering_probe_only": True,
            "not_real_acceptance": True,
            "v1_fallback_required": True,
            "do_not_refresh_real_primary_latest": True,
            "a3_enabled": False,
            "co2_enabled": False,
            "full_group_enabled": False,
            "multi_temperature_enabled": False,
            "real_primary_latest_refresh": False,
        },
    }


def _write_operator(tmp_path: Path, config_path: Path, payload: dict | None = None) -> Path:
    path = tmp_path / "operator_confirmation.json"
    path.write_text(json.dumps(payload or _operator_payload(config_path), ensure_ascii=False), encoding="utf-8")
    return path


def _admission(cfg: dict, operator_path: Path, config_path: Path):
    return evaluate_h2o_1_point_no_write_gate(
        cfg,
        cli_allow=True,
        env={H2O_ENV_VAR: "1"},
        operator_confirmation_path=operator_path,
        branch=BRANCH,
        head=HEAD,
        config_path=str(config_path),
        run_app_py_untouched=True,
    )


def test_h2o_admission_evidence_includes_no_write_true(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    operator_path = _write_operator(tmp_path, config_path)

    admission = _admission(_config(), operator_path, config_path)

    assert admission.approved is True
    assert admission.evidence["no_write"] is True


def test_h2o_admission_evidence_includes_no_write_sources(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    operator_path = _write_operator(tmp_path, config_path)

    admission = _admission(_config(), operator_path, config_path)

    assert admission.evidence["config_no_write"] is True
    assert admission.evidence["operator_no_write_ack"] is True
    assert admission.evidence["no_write_contract_source"]


def test_h2o_admission_rejects_when_config_no_write_false(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    operator_path = _write_operator(tmp_path, config_path)
    cfg = _config()
    cfg["no_write"] = False

    admission = _admission(cfg, operator_path, config_path)

    assert admission.approved is False
    assert admission.evidence["no_write"] is False
    assert "config_no_write_not_true" in admission.reasons


def test_h2o_admission_rejects_when_operator_no_write_ack_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    payload = _operator_payload(config_path)
    payload["explicit_acknowledgement"].pop("no_write")
    operator_path = _write_operator(tmp_path, config_path, payload)

    admission = _admission(_config(), operator_path, config_path)

    assert admission.approved is False
    assert admission.evidence["operator_no_write_ack"] is False
    assert admission.evidence["no_write"] is False
    assert "operator_ack_missing_no_write" in admission.reasons


def test_h2o_admission_no_write_does_not_mask_write_flags(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    operator_path = _write_operator(tmp_path, config_path)
    defaults = dict(h2o_gate.H2O_SAFETY_ASSERTION_DEFAULTS)
    defaults["attempted_write_count"] = 1
    defaults["any_write_command_sent"] = True
    monkeypatch.setattr(h2o_gate, "H2O_SAFETY_ASSERTION_DEFAULTS", defaults)

    admission = _admission(_config(), operator_path, config_path)

    assert admission.approved is False
    assert admission.evidence["no_write"] is False
    assert "no_write_write_flags_not_safe" in admission.reasons


def test_h2o_admission_preserves_existing_write_flags(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    operator_path = _write_operator(tmp_path, config_path)

    admission = _admission(_config(), operator_path, config_path)
    evidence = admission.evidence

    assert evidence["attempted_write_count"] == 0
    assert evidence["any_write_command_sent"] is False
    assert evidence["identity_write_command_sent"] is False
    assert evidence["senco_write_command_sent"] is False
    assert evidence["calibration_write_command_sent"] is False
    assert evidence["mode_switch_command_sent"] is False
    assert evidence["chamber_write_register_command_sent"] is False
    assert evidence["chamber_set_temperature_command_sent"] is False
    assert evidence["chamber_start_command_sent"] is False
    assert evidence["chamber_stop_command_sent"] is False
    assert evidence["real_primary_latest_refresh"] is False


def test_h2o_admission_dry_gate_expression_passes() -> None:
    repo = Path(__file__).resolve().parents[2]
    config_path = repo / "src" / "gas_calibrator" / "v2" / "configs" / "validation" / "run001_h2o_only_1_point_no_write_real_machine.json"
    operator_path = repo / "_handoff" / "h2o_1r_1pt_operator_confirmation_59ae7125_local.json"
    cfg = load_json_mapping(config_path)

    admission = evaluate_h2o_1_point_no_write_gate(
        cfg,
        cli_allow=True,
        env={H2O_ENV_VAR: "1"},
        operator_confirmation_path=operator_path,
        branch=BRANCH,
        head=HEAD,
        config_path=str(config_path),
        run_app_py_untouched=True,
    )

    assert admission.approved is True
    assert admission.reasons == ()
    assert admission.evidence.get("no_write") is True
    assert admission.evidence["attempted_write_count"] == 0
    assert admission.evidence["any_write_command_sent"] is False
    assert admission.evidence["identity_write_command_sent"] is False
    assert admission.evidence["senco_write_command_sent"] is False
    assert admission.evidence["calibration_write_command_sent"] is False
    assert admission.evidence["mode_switch_command_sent"] is False
    assert admission.evidence["chamber_set_temperature_command_sent"] is False
    assert admission.evidence["chamber_start_command_sent"] is False
    assert admission.evidence["chamber_stop_command_sent"] is False
    assert admission.evidence["real_primary_latest_refresh"] is False
