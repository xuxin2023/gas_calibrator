from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.validation.engineering_probe_admission import (
    ENGINEERING_PROBE_ENV_VAR,
    evaluate_engineering_probe_admission,
)


EXPECTED_SCOPE = "r1_conditioning_only"


def _config() -> dict[str, object]:
    return {
        "scope": EXPECTED_SCOPE,
        "no_write": True,
        "v1_fallback_required": True,
        "real_primary_latest_refresh": False,
        "h2o_enabled": False,
        "full_group_enabled": False,
        "multi_temperature_enabled": False,
        "analyzer_id_write_enabled": False,
        "senco_write_enabled": False,
        "calibration_write_enabled": False,
        "real_acceptance_enabled": False,
        "default_entry_switch_enabled": False,
        "disable_v1": False,
    }


def _operator_confirmation(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.json"
    payload = {
        "operator_name": "pytest",
        "timestamp": "2026-07-28T00:00:00+08:00",
        "branch": "codex/probe-review",
        "HEAD": "0123456789abcdef",
        "config_path": str(config_path),
        "port_manifest": {"gas_analyzers": ["COM35"]},
        "explicit_acknowledgement": {
            "no_write": True,
            "no_id_write": True,
            "no_senco_write": True,
            "no_calibration_write": True,
            "not_real_acceptance": True,
            "engineering_probe_only": True,
            "v1_fallback_required": True,
            "do_not_refresh_real_primary_latest": True,
            "real_primary_latest_refresh": False,
        },
    }
    path = tmp_path / "operator_confirmation.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _evaluate(
    tmp_path: Path,
    *,
    config: dict[str, object] | None = None,
    cli_allow: bool = True,
    env_allow: bool = True,
):
    return evaluate_engineering_probe_admission(
        config or _config(),
        expected_scope=EXPECTED_SCOPE,
        cli_allow=cli_allow,
        env={ENGINEERING_PROBE_ENV_VAR: "1"} if env_allow else {},
        operator_confirmation_path=_operator_confirmation(tmp_path),
    )


def test_admission_approves_only_the_gate_and_keeps_promotion_blocked(tmp_path: Path) -> None:
    admission = _evaluate(tmp_path)

    assert admission.approved is True
    assert admission.reasons == ()
    assert admission.evidence["gate_only"] is True
    assert admission.evidence["real_com_opened"] is False
    assert admission.evidence["real_probe_executed"] is False
    assert admission.evidence["attempted_write_count"] == 0
    assert admission.evidence["acceptance_level"] == "engineering_probe_only"
    assert admission.evidence["not_real_acceptance_evidence"] is True
    assert admission.evidence["promotion_state"] == "blocked"
    assert admission.evidence["real_primary_latest_refresh"] is False


@pytest.mark.parametrize(
    ("cli_allow", "env_allow", "reason"),
    [
        (False, True, "missing_cli_engineering_probe_unlock"),
        (True, False, "missing_env_engineering_probe_unlock"),
        (False, False, "missing_cli_engineering_probe_unlock"),
    ],
)
def test_admission_requires_two_independent_unlocks(
    tmp_path: Path,
    cli_allow: bool,
    env_allow: bool,
    reason: str,
) -> None:
    admission = _evaluate(tmp_path, cli_allow=cli_allow, env_allow=env_allow)

    assert admission.approved is False
    assert reason in admission.reasons


def test_admission_requires_operator_confirmation(tmp_path: Path) -> None:
    admission = evaluate_engineering_probe_admission(
        _config(),
        expected_scope=EXPECTED_SCOPE,
        cli_allow=True,
        env={ENGINEERING_PROBE_ENV_VAR: "1"},
        operator_confirmation_path=tmp_path / "missing.json",
    )

    assert admission.approved is False
    assert "missing_operator_confirmation_json" in admission.reasons


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("h2o_enabled", "config_forbidden_capability_enabled_h2o_enabled"),
        ("full_group_enabled", "config_forbidden_capability_enabled_full_group_enabled"),
        (
            "multi_temperature_enabled",
            "config_forbidden_capability_enabled_multi_temperature_enabled",
        ),
        (
            "calibration_write_enabled",
            "config_forbidden_capability_enabled_calibration_write_enabled",
        ),
        ("disable_v1", "config_forbidden_capability_enabled_disable_v1"),
    ],
)
def test_admission_rejects_forbidden_capabilities(
    tmp_path: Path,
    field: str,
    reason: str,
) -> None:
    config = _config()
    config[field] = True

    admission = _evaluate(tmp_path, config=config)

    assert admission.approved is False
    assert reason in admission.reasons


def test_admission_rejects_scope_write_and_latest_refresh_drift(tmp_path: Path) -> None:
    config = _config()
    config.update(
        {
            "scope": "full_group",
            "no_write": False,
            "real_primary_latest_refresh": True,
        }
    )

    admission = _evaluate(tmp_path, config=config)

    assert admission.approved is False
    assert "config_scope_mismatch" in admission.reasons
    assert "config_no_write_not_true" in admission.reasons
    assert "config_real_primary_latest_refresh_not_false" in admission.reasons


def test_admission_validates_operator_branch_head_and_config_binding(tmp_path: Path) -> None:
    admission = evaluate_engineering_probe_admission(
        _config(),
        expected_scope=EXPECTED_SCOPE,
        cli_allow=True,
        env={ENGINEERING_PROBE_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        branch="other-branch",
        head="other-head",
        config_path=str(tmp_path / "other-config.json"),
    )

    assert admission.approved is False
    assert "operator_confirmation_branch_mismatch" in admission.reasons
    assert "operator_confirmation_head_mismatch" in admission.reasons
    assert "operator_confirmation_config_path_mismatch" in admission.reasons


def test_admission_has_no_v2_import_dependency() -> None:
    source = Path(
        evaluate_engineering_probe_admission.__code__.co_filename
    ).read_text(encoding="utf-8")

    assert "gas_calibrator.v2" not in source
