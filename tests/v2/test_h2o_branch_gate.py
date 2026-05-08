from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.core.run001_h2o_only_1_point_no_write_probe import (
    H2O_ENV_VALUE,
    H2O_ENV_VAR,
    evaluate_h2o_1_point_no_write_gate,
)


DEFAULT_BRANCH = "codex/run001-a1-no-write-dry-run"
PRISTINE_BRANCH = "codex/v2-h2o-v210-pristine-verify"
HEAD = "70e9c2243a90403b592a45b25bfff9d957dfd8f5"


def _minimal_config() -> dict:
    return {
        "run001_h2o_1_point": {
            "no_write": True,
            "h2o_only": True,
            "single_route": True,
            "single_temperature_group": True,
            "pressure_points_hpa": [1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0],
            "allow_write_coefficients": False,
            "allow_write_zero": False,
            "allow_write_span": False,
            "allow_write_calibration_parameters": False,
            "default_cutover_to_v2": False,
            "disable_v1": False,
            "full_h2o_co2_group": False,
        },
        "workflow": {
            "route_mode": "h2o_only",
            "selected_temps_c": [20.0],
            "skip_co2_ppm": [],
        },
    }


def _operator_confirmation(path: Path, *, branch: str, head: str = HEAD, config_path: str = "") -> Path:
    resolved_config_path = config_path or str(path.parent / "config.json")
    payload = {
        "operator_name": "dry_check_only",
        "timestamp": "2026-05-08T00:00:00Z",
        "branch": branch,
        "HEAD": head,
        "config_path": resolved_config_path,
        "port_manifest": {"dry_check_only": True, "real_com_opened": False},
        "explicit_acknowledgement": {
            "h2o_only": True,
            "only_h2o_1_point_no_write": True,
            "skip0": True,
            "single_route": True,
            "single_temperature": True,
            "single_pressure_point": True,
            "no_write": True,
            "no_id_write": True,
            "no_senco_write": True,
            "no_calibration_write": True,
            "no_chamber_sv_write": True,
            "no_chamber_set_temperature": True,
            "no_chamber_start": True,
            "no_chamber_stop": True,
            "no_mode_switch": True,
            "not_real_acceptance": True,
            "engineering_probe_only": True,
            "v1_fallback_required": True,
            "do_not_refresh_real_primary_latest": True,
            "a3_enabled": False,
            "co2_enabled": False,
            "full_group_enabled": False,
            "multi_temperature_enabled": False,
            "real_primary_latest_refresh": False,
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _perform_gate(cfg: dict, tmp_path: Path, *, branch: str = DEFAULT_BRANCH, head: str = HEAD, config_path: str = ""):
    resolved_config_path = config_path or str(tmp_path / "config.json")
    operator_path = _operator_confirmation(tmp_path / "operator.json", branch=branch, head=head, config_path=resolved_config_path)
    return evaluate_h2o_1_point_no_write_gate(
        cfg,
        cli_allow=True,
        env={H2O_ENV_VAR: H2O_ENV_VALUE},
        operator_confirmation_path=operator_path,
        branch=branch,
        head=HEAD,
        config_path=resolved_config_path,
        run_app_py_untouched=True,
    )


def test_default_allowed_branch_passes_without_configured_allowed_branches(tmp_path: Path) -> None:
    admission = _perform_gate(_minimal_config(), tmp_path, branch=DEFAULT_BRANCH)

    assert admission.approved is True
    assert admission.reasons == ()
    assert admission.evidence["allowed_branches"] == [DEFAULT_BRANCH]
    assert admission.evidence["branch_allowed"] is True


def test_configured_pristine_verification_branch_passes(tmp_path: Path) -> None:
    cfg = _minimal_config()
    cfg["run001_h2o_1_point"]["allowed_branches"] = [DEFAULT_BRANCH, PRISTINE_BRANCH]

    admission = _perform_gate(cfg, tmp_path, branch=PRISTINE_BRANCH)

    assert admission.approved is True
    assert admission.reasons == ()
    assert admission.evidence["current_branch"] == PRISTINE_BRANCH
    assert admission.evidence["allowed_branches"] == [DEFAULT_BRANCH, PRISTINE_BRANCH]
    assert admission.evidence["branch_allowed"] is True


def test_unconfigured_branch_is_rejected(tmp_path: Path) -> None:
    admission = _perform_gate(_minimal_config(), tmp_path, branch="some/other-branch")

    assert admission.approved is False
    assert "current_branch_not_allowed_for_h2o_1_point_no_write" in admission.reasons
    assert admission.evidence["branch_allowed"] is False


def test_no_write_is_not_relaxed_by_allowed_branch(tmp_path: Path) -> None:
    cfg = _minimal_config()
    cfg["run001_h2o_1_point"]["allowed_branches"] = [PRISTINE_BRANCH]
    cfg["run001_h2o_1_point"]["no_write"] = False

    admission = _perform_gate(cfg, tmp_path, branch=PRISTINE_BRANCH)

    assert admission.approved is False
    assert "config_no_write_not_true" in admission.reasons


def test_write_coefficients_are_not_relaxed_by_allowed_branch(tmp_path: Path) -> None:
    cfg = _minimal_config()
    cfg["run001_h2o_1_point"]["allowed_branches"] = [PRISTINE_BRANCH]
    cfg["run001_h2o_1_point"]["allow_write_coefficients"] = True

    admission = _perform_gate(cfg, tmp_path, branch=PRISTINE_BRANCH)

    assert admission.approved is False
    assert "config_calibration_write_enabled_not_disabled" in admission.reasons


def test_operator_head_mismatch_still_rejects(tmp_path: Path) -> None:
    cfg = _minimal_config()
    cfg["run001_h2o_1_point"]["allowed_branches"] = [PRISTINE_BRANCH]

    admission = _perform_gate(cfg, tmp_path, branch=PRISTINE_BRANCH, head="different-head")

    assert admission.approved is False
    assert "operator_confirmation_head_mismatch" in admission.reasons


def test_operator_config_path_mismatch_still_rejects(tmp_path: Path) -> None:
    cfg = _minimal_config()
    cfg["run001_h2o_1_point"]["allowed_branches"] = [PRISTINE_BRANCH]
    operator_path = _operator_confirmation(
        tmp_path / "operator.json",
        branch=PRISTINE_BRANCH,
        config_path=str(tmp_path / "operator-config.json"),
    )

    admission = evaluate_h2o_1_point_no_write_gate(
        cfg,
        cli_allow=True,
        env={H2O_ENV_VAR: H2O_ENV_VALUE},
        operator_confirmation_path=operator_path,
        branch=PRISTINE_BRANCH,
        head=HEAD,
        config_path=str(tmp_path / "expected-config.json"),
        run_app_py_untouched=True,
    )

    assert admission.approved is False
    assert "operator_confirmation_config_path_mismatch" in admission.reasons
