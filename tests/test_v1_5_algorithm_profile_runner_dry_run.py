import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_algorithm_profile_runner_dry_run import main as cli_main
from gas_calibrator.validation import v1_5_algorithm_profile_runner_dry_run as bundle_module
from gas_calibrator.validation.v1_5_algorithm_profile_runner_dry_run import (
    build_v1_5_algorithm_profile_runner_dry_run,
    write_v1_5_algorithm_profile_runner_dry_run_outputs,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _check(model: dict, name: str) -> dict:
    return {row["check"]: row for row in model["checks"]}[name]


def test_profile_runner_dry_run_builds_full_offline_bundle(tmp_path: Path) -> None:
    model = build_v1_5_algorithm_profile_runner_dry_run(
        profile_path=PROFILE_PATH,
        output_dir=tmp_path / "bundle",
    )

    assert model["schema"] == "v1_5_algorithm_profile_runner_dry_run_v1"
    assert model["overall_status"] == "ready_for_profile_driven_runner_dry_run_review"
    assert model["blocker_count"] == 0
    assert model["not_real_acceptance_evidence"] is True
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["writes_device_id"] is False
    assert model["does_not_execute_commands"] is True
    assert model["does_not_modify_runners"] is True
    assert model["co2_runlist_count"] == 47
    assert model["h2o_runlist_count"] == 14
    assert model["runner_integration_status"] == "profile_driven_dry_run_bundle_only_not_runner_wired"

    outputs = model["outputs"]
    assert Path(outputs["runlist_preview"]["manifest"]).exists()
    assert Path(outputs["runlist_preview"]["co2_runlist"]).exists()
    assert Path(outputs["runlist_preview"]["h2o_runlist"]).exists()
    assert Path(outputs["runlist_readiness"]["json"]).exists()
    assert Path(outputs["runner_integration_dry_run"]["json"]).exists()

    assert _check(model, "formal_runlist_preview_generation")["status"] == "ready"
    assert _check(model, "runlist_readiness_gate")["status"] == "ready"
    assert _check(model, "runner_integration_dry_run_plan")["status"] == "ready"
    assert _check(model, "profile_runner_dry_run_offline_boundary")["status"] == "ready"


def test_profile_runner_dry_run_writer_and_cli(tmp_path: Path) -> None:
    model = build_v1_5_algorithm_profile_runner_dry_run(
        profile_path=PROFILE_PATH,
        output_dir=tmp_path / "bundle",
    )
    outputs = write_v1_5_algorithm_profile_runner_dry_run_outputs(model, tmp_path / "bundle")

    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["markdown"].exists()
    assert "profile-driven bundle" in outputs["markdown"].read_text(encoding="utf-8")
    payload = json.loads(outputs["json"].read_text(encoding="utf-8-sig"))
    assert payload["overall_status"] == "ready_for_profile_driven_runner_dry_run_review"

    cli_out = tmp_path / "cli_bundle"
    rc = cli_main(
        [
            "--profile-path",
            str(PROFILE_PATH),
            "--output-dir",
            str(cli_out),
            "--fail-on-blocker",
        ]
    )
    assert rc == 0
    assert (cli_out / "v1_5_algorithm_profile_runner_dry_run.json").exists()
    assert (cli_out / "algorithm_formal_runlist_preview").exists()
    assert (cli_out / "algorithm_runlist_readiness").exists()
    assert (cli_out / "algorithm_runner_integration_dry_run").exists()


def test_profile_runner_dry_run_blocks_when_generated_readiness_is_blocked(
    tmp_path: Path,
    monkeypatch,
) -> None:
    original_builder = bundle_module.build_v1_5_algorithm_runlist_readiness

    def fake_blocked_readiness(*args, **kwargs):
        model = original_builder(*args, **kwargs)
        model["overall_status"] = "blocked"
        model["blocker_count"] = 1
        return model

    monkeypatch.setattr(bundle_module, "build_v1_5_algorithm_runlist_readiness", fake_blocked_readiness)

    model = build_v1_5_algorithm_profile_runner_dry_run(
        profile_path=PROFILE_PATH,
        output_dir=tmp_path / "bundle",
    )

    assert model["overall_status"] == "blocked"
    assert _check(model, "runlist_readiness_gate")["status"] == "blocker"
    assert "readiness_status=blocked" in _check(model, "runlist_readiness_gate")["reasons"]
    assert "readiness_blocker_count=1" in _check(model, "runlist_readiness_gate")["reasons"]
