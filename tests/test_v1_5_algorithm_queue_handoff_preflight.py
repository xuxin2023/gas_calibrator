import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_algorithm_queue_handoff_preflight import main as cli_main
from gas_calibrator.validation.v1_5_algorithm_profile_runner_dry_run import (
    build_v1_5_algorithm_profile_runner_dry_run,
    write_v1_5_algorithm_profile_runner_dry_run_outputs,
)
from gas_calibrator.validation.v1_5_algorithm_queue_handoff_preflight import (
    build_v1_5_algorithm_queue_handoff_preflight,
    write_v1_5_algorithm_queue_handoff_preflight_outputs,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _ready_profile_bundle(tmp_path: Path) -> Path:
    out = tmp_path / "profile_bundle"
    model = build_v1_5_algorithm_profile_runner_dry_run(
        profile_path=PROFILE_PATH,
        output_dir=out,
    )
    paths = write_v1_5_algorithm_profile_runner_dry_run_outputs(model, out)
    return paths["json"]


def _check(model: dict, name: str) -> dict:
    return {row["check"]: row for row in model["checks"]}[name]


def test_queue_handoff_preflight_allows_only_dry_run_review(tmp_path: Path) -> None:
    profile_json = _ready_profile_bundle(tmp_path)

    model = build_v1_5_algorithm_queue_handoff_preflight(
        profile_runner_dry_run_json=profile_json,
    )

    assert model["schema"] == "v1_5_algorithm_queue_handoff_preflight_v1"
    assert model["overall_status"] == "ready_for_dry_run_queue_handoff_review"
    assert model["blocker_count"] == 0
    assert model["dry_run_handoff_review_allowed"] is True
    assert model["live_queue_execution_allowed"] is False
    assert model["database_import_allowed"] is False
    assert model["formal_release_allowed"] is False
    assert model["not_real_acceptance_evidence"] is True
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["does_not_execute_commands"] is True
    assert model["does_not_modify_runners"] is True
    assert model["co2_runlist_count"] == 47
    assert model["h2o_runlist_count"] == 14
    assert model["required_cli_flags"] == ("--dry-run", "--no-prompt")

    assert _check(model, "profile_runner_dry_run_bundle_gate")["status"] == "ready"
    assert _check(model, "runner_integration_dry_run_gate")["status"] == "ready"
    assert _check(model, "co2_dry_run_no_prompt_handoff_gate")["status"] == "ready"
    assert _check(model, "h2o_dry_run_no_prompt_handoff_gate")["status"] == "ready"
    assert _check(model, "live_queue_execution_lock")["status"] == "ready"


def test_queue_handoff_preflight_blocks_missing_dry_run_flag(tmp_path: Path) -> None:
    profile_json = _ready_profile_bundle(tmp_path)
    profile_model = json.loads(profile_json.read_text(encoding="utf-8-sig"))
    runner_json = Path(profile_model["outputs"]["runner_integration_dry_run"]["json"])
    runner_model = json.loads(runner_json.read_text(encoding="utf-8-sig"))
    runner_model["runner_integration_plan"][0]["command_preview"] = runner_model["runner_integration_plan"][0][
        "command_preview"
    ].replace(" --dry-run", "")
    runner_json.write_text(json.dumps(runner_model, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    model = build_v1_5_algorithm_queue_handoff_preflight(
        profile_runner_dry_run_json=profile_json,
    )

    assert model["overall_status"] == "blocked"
    assert _check(model, "co2_dry_run_no_prompt_handoff_gate")["status"] == "blocker"
    assert "co2_dry_run_flag_missing" in _check(model, "co2_dry_run_no_prompt_handoff_gate")["reasons"]
    assert model["live_queue_execution_allowed"] is False


def test_queue_handoff_preflight_blocks_wrong_profile_count(tmp_path: Path) -> None:
    profile_json = _ready_profile_bundle(tmp_path)
    profile_model = json.loads(profile_json.read_text(encoding="utf-8-sig"))
    profile_model["co2_runlist_count"] = 45
    profile_json.write_text(json.dumps(profile_model, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    model = build_v1_5_algorithm_queue_handoff_preflight(
        profile_runner_dry_run_json=profile_json,
    )

    assert model["overall_status"] == "blocked"
    assert _check(model, "profile_runner_dry_run_bundle_gate")["status"] == "blocker"
    assert "profile_co2_count_not_47" in _check(model, "profile_runner_dry_run_bundle_gate")["reasons"]


def test_queue_handoff_preflight_writer_and_cli(tmp_path: Path) -> None:
    profile_json = _ready_profile_bundle(tmp_path)
    model = build_v1_5_algorithm_queue_handoff_preflight(
        profile_runner_dry_run_json=profile_json,
    )

    outputs = write_v1_5_algorithm_queue_handoff_preflight_outputs(model, tmp_path / "preflight")

    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["markdown"].exists()
    assert "live_queue_execution_allowed" in outputs["markdown"].read_text(encoding="utf-8")
    payload = json.loads(outputs["json"].read_text(encoding="utf-8-sig"))
    assert payload["overall_status"] == "ready_for_dry_run_queue_handoff_review"

    cli_out = tmp_path / "cli_preflight"
    rc = cli_main(
        [
            "--profile-runner-dry-run-json",
            str(profile_json),
            "--output-dir",
            str(cli_out),
            "--fail-on-blocker",
        ]
    )
    assert rc == 0
    assert (cli_out / "v1_5_algorithm_queue_handoff_preflight.json").exists()
