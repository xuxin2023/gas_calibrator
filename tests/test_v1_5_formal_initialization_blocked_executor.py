import json
from pathlib import Path

from gas_calibrator.tools.run_v1_5_formal_initialization_blocked_executor import main as blocked_main
from gas_calibrator.tools.run_v1_5_formal_initialization_runner import (
    build_formal_initialization_plan,
    write_formal_initialization_plan,
)
from gas_calibrator.validation.v1_5_formal_initialization_blocked_executor import (
    build_v1_5_formal_initialization_blocked_executor,
    write_v1_5_formal_initialization_blocked_executor_outputs,
)
from gas_calibrator.validation.v1_5_formal_initialization_executor_dry_run import (
    build_v1_5_formal_initialization_executor_dry_run,
    write_v1_5_formal_initialization_executor_dry_run_outputs,
)


def _write_config(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "devices": {
                    "gas_analyzers": [
                        {
                            "name": "GA01",
                            "port": "COM35",
                            "device_id": "001",
                            "sn_code": "01260701",
                            "device_code": "01260701",
                            "enabled": True,
                        }
                    ]
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _write_initialization_plan(tmp_path: Path) -> Path:
    config = _write_config(tmp_path / "runtime.json")
    plan = build_formal_initialization_plan(config_path=config, output_dir=tmp_path / "formal_initialization")
    outputs = write_formal_initialization_plan(plan)
    return outputs["json"]


def _write_dry_run(tmp_path: Path) -> tuple[Path, Path]:
    plan_json = _write_initialization_plan(tmp_path)
    model = build_v1_5_formal_initialization_executor_dry_run(
        formal_initialization_plan_json=plan_json,
    )
    outputs = write_v1_5_formal_initialization_executor_dry_run_outputs(model, tmp_path / "dry_run")
    return plan_json, Path(outputs["json"])


def test_initialization_blocked_executor_consumes_ready_dry_run_but_keeps_live_locked(tmp_path: Path) -> None:
    plan_json, dry_run_json = _write_dry_run(tmp_path)

    model = build_v1_5_formal_initialization_blocked_executor(
        formal_initialization_executor_dry_run_json=dry_run_json,
        formal_initialization_plan_json=plan_json,
    )

    assert model["schema"] == "v1_5_formal_initialization_blocked_executor_v1"
    assert model["overall_status"] == "blocked_pending_controlled_initialization_executor_implementation"
    assert model["blocked_executor_ready"] is True
    assert model["execution_supported"] is False
    assert model["execution_requested"] is False
    assert model["live_execution_allowed"] is False
    assert model["read_only_real_com_execution_allowed"] is False
    assert model["controlled_write_execution_allowed"] is False
    assert model["real_com_execution_allowed"] is False
    assert model["execute_flag_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False
    assert model["controls_pressure"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_sn"] is False
    assert model["writes_device_id"] is False
    assert model["writes_coefficients"] is False
    assert model["database_written"] is False
    assert model["formal_release_allowed"] is False
    assert model["database_import_allowed"] is False
    assert model["not_real_acceptance_evidence"] is True

    checks = {row["check"]: row for row in model["checks"]}
    assert checks["formal_initialization_executor_dry_run_consumed"]["status"] == "ready"
    assert checks["formal_initialization_plan_bound"]["status"] == "ready"
    assert checks["execution_lock_enforced"]["details"]["execution_supported"] is False
    assert checks["real_com_side_effect_lock"]["details"]["opens_com_ports"] is False
    assert checks["controlled_write_side_effect_lock"]["details"]["controlled_write_execution_allowed"] is False


def test_initialization_blocked_executor_reviews_missing_dry_run(tmp_path: Path) -> None:
    plan_json = _write_initialization_plan(tmp_path)

    model = build_v1_5_formal_initialization_blocked_executor(
        formal_initialization_executor_dry_run_json=tmp_path / "missing.json",
        formal_initialization_plan_json=plan_json,
    )

    assert model["overall_status"] == "review_required"
    assert model["blocked_executor_ready"] is False
    assert model["live_execution_allowed"] is False
    checks = {row["check"]: row for row in model["checks"]}
    assert checks["formal_initialization_executor_dry_run_consumed"]["status"] == "review_required"
    assert "formal_initialization_executor_dry_run_missing" in checks[
        "formal_initialization_executor_dry_run_consumed"
    ]["reasons"]


def test_initialization_blocked_executor_writer_and_cli_are_no_com_no_write(tmp_path: Path, capsys) -> None:
    plan_json, dry_run_json = _write_dry_run(tmp_path)
    model = build_v1_5_formal_initialization_blocked_executor(
        formal_initialization_executor_dry_run_json=dry_run_json,
        formal_initialization_plan_json=plan_json,
    )
    outputs = write_v1_5_formal_initialization_blocked_executor_outputs(model, tmp_path / "blocked")

    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["summary_csv"].exists()
    assert outputs["markdown"].exists()
    assert "no-COM, no-write executor stub" in outputs["markdown"].read_text(encoding="utf-8")

    cli_out = tmp_path / "cli"
    rc = blocked_main(
        [
            "--formal-initialization-executor-dry-run-json",
            str(dry_run_json),
            "--formal-initialization-plan-json",
            str(plan_json),
            "--output-dir",
            str(cli_out),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["overall_status"] == "blocked_pending_controlled_initialization_executor_implementation"
    assert payload["blocked_executor_ready"] is True
    assert payload["execution_supported"] is False
    assert payload["live_execution_allowed"] is False
    assert payload["read_only_real_com_execution_allowed"] is False
    assert payload["controlled_write_execution_allowed"] is False
    assert payload["opens_com_ports"] is False
    assert payload["connects_postgresql"] is False
    assert payload["writes_coefficients"] is False
    assert (cli_out / "v1_5_formal_initialization_blocked_executor.json").exists()


def test_initialization_blocked_executor_rejects_live_unlock_flags(tmp_path: Path, capsys) -> None:
    plan_json, dry_run_json = _write_dry_run(tmp_path)

    rc = blocked_main(
        [
            "--formal-initialization-executor-dry-run-json",
            str(dry_run_json),
            "--formal-initialization-plan-json",
            str(plan_json),
            "--output-dir",
            str(tmp_path / "cli"),
            "--execute-read-only-real-com",
        ]
    )

    assert rc == 2
    captured = capsys.readouterr()
    assert "live initialization execution is locked" in captured.err
    assert not (tmp_path / "cli" / "v1_5_formal_initialization_blocked_executor.json").exists()
