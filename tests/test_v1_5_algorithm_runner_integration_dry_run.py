import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_algorithm_runner_integration_dry_run import main as cli_main
from gas_calibrator.validation.v1_5_algorithm_route_profiles import (
    write_v1_5_algorithm_formal_runlist_preview,
)
from gas_calibrator.validation.v1_5_algorithm_runlist_readiness import (
    build_v1_5_algorithm_runlist_readiness,
    write_v1_5_algorithm_runlist_readiness_outputs,
)
from gas_calibrator.validation.v1_5_algorithm_runner_integration_dry_run import (
    build_v1_5_algorithm_runner_integration_dry_run,
    write_v1_5_algorithm_runner_integration_dry_run_outputs,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _ready_artifacts(tmp_path: Path) -> tuple[Path, Path]:
    runlist_dir = tmp_path / "runlist_preview"
    readiness_dir = tmp_path / "runlist_readiness"
    write_v1_5_algorithm_formal_runlist_preview(PROFILE_PATH, runlist_dir)
    readiness = build_v1_5_algorithm_runlist_readiness(runlist_dir=runlist_dir)
    write_v1_5_algorithm_runlist_readiness_outputs(readiness, readiness_dir)
    return readiness_dir, runlist_dir


def _blocked_readiness(readiness_dir: Path) -> None:
    path = readiness_dir / "v1_5_algorithm_runlist_readiness.json"
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    payload["overall_status"] = "blocked"
    payload["blocker_count"] = 1
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _check(model: dict, name: str) -> dict:
    return {row["check"]: row for row in model["checks"]}[name]


def test_algorithm_runner_integration_dry_run_builds_safe_queue_plan(tmp_path: Path) -> None:
    readiness_dir, runlist_dir = _ready_artifacts(tmp_path)

    model = build_v1_5_algorithm_runner_integration_dry_run(
        readiness_dir=readiness_dir,
        runlist_dir=runlist_dir,
    )

    assert model["schema"] == "v1_5_algorithm_runner_integration_dry_run_v1"
    assert model["overall_status"] == "ready_for_runner_integration_dry_run_review"
    assert model["blocker_count"] == 0
    assert model["not_real_acceptance_evidence"] is True
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["does_not_execute_commands"] is True
    assert model["does_not_modify_runners"] is True
    assert model["planned_route_order"] == ["co2", "h2o"]
    assert model["co2_runlist_count"] == 47
    assert model["h2o_runlist_count"] == 14

    plan = {row["route_kind"]: row for row in model["runner_integration_plan"]}
    assert plan["co2"]["runner_module"] == "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
    assert plan["h2o"]["runner_module"] == "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue"
    for row in plan.values():
        command = row["command_preview"]
        assert "--queue-csv" in command
        assert "--dry-run" in command
        assert "--no-prompt" in command
        assert "--execute-controlled-writes" not in command
        assert "--write-coefficients" not in command
        assert row["runner_integration_status"] == "dry_run_preview_only_not_runner_wired"

    assert _check(model, "algorithm_runlist_readiness_gate")["status"] == "ready"
    assert _check(model, "co2_queue_runner_dry_run_plan")["status"] == "ready"
    assert _check(model, "h2o_queue_runner_dry_run_plan")["status"] == "ready"
    assert _check(model, "integration_dry_run_boundary")["status"] == "ready"


def test_algorithm_runner_integration_dry_run_blocks_when_readiness_blocked(tmp_path: Path) -> None:
    readiness_dir, runlist_dir = _ready_artifacts(tmp_path)
    _blocked_readiness(readiness_dir)

    model = build_v1_5_algorithm_runner_integration_dry_run(
        readiness_dir=readiness_dir,
        runlist_dir=runlist_dir,
    )

    assert model["overall_status"] == "blocked"
    assert _check(model, "algorithm_runlist_readiness_gate")["status"] == "blocker"
    assert "readiness_status=blocked" in _check(model, "algorithm_runlist_readiness_gate")["reasons"]
    assert "readiness_blocker_count=1" in _check(model, "algorithm_runlist_readiness_gate")["reasons"]


def test_algorithm_runner_integration_dry_run_writer_and_cli(tmp_path: Path) -> None:
    readiness_dir, runlist_dir = _ready_artifacts(tmp_path)
    model = build_v1_5_algorithm_runner_integration_dry_run(
        readiness_dir=readiness_dir,
        runlist_dir=runlist_dir,
    )

    outputs = write_v1_5_algorithm_runner_integration_dry_run_outputs(model, tmp_path / "dry_run")

    assert outputs["json"].exists()
    assert outputs["plan_csv"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["markdown"].exists()
    assert "co2 -> h2o" in outputs["markdown"].read_text(encoding="utf-8")
    payload = json.loads(outputs["json"].read_text(encoding="utf-8-sig"))
    assert payload["overall_status"] == "ready_for_runner_integration_dry_run_review"

    cli_out = tmp_path / "cli_dry_run"
    rc = cli_main(
        [
            "--readiness-dir",
            str(readiness_dir),
            "--runlist-dir",
            str(runlist_dir),
            "--output-dir",
            str(cli_out),
            "--fail-on-blocker",
        ]
    )
    assert rc == 0
    assert (cli_out / "v1_5_algorithm_runner_integration_dry_run.json").exists()
