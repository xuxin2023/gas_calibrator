import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_initialization_executor_dry_run import main as export_main
from gas_calibrator.tools.run_v1_5_formal_initialization_runner import (
    build_formal_initialization_plan,
    write_formal_initialization_plan,
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


def test_initialization_executor_dry_run_classifies_plan_without_unlocking_live_actions(tmp_path: Path) -> None:
    plan_json = _write_initialization_plan(tmp_path)

    model = build_v1_5_formal_initialization_executor_dry_run(
        formal_initialization_plan_json=plan_json,
    )

    assert model["schema"] == "v1_5_formal_initialization_executor_dry_run_v1"
    assert model["overall_status"] == "ready_for_initialization_executor_dry_run_review"
    assert model["dry_run_review_allowed"] is True
    assert model["live_execution_allowed"] is False
    assert model["read_only_real_com_execution_allowed"] is False
    assert model["controlled_write_execution_allowed"] is False
    assert model["execute_flag_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False
    assert model["writes_coefficients"] is False
    assert model["database_written"] is False
    assert model["not_real_acceptance_evidence"] is True

    by_step = {row["step_id"]: row for row in model["step_reviews"]}
    assert by_step["sn_identity_initialization_plan"]["review_status"] == "dry_run_command_allowed"
    assert by_step["identity_and_getco_epoch0_snapshot"]["review_status"] == "locked_read_only_real_com"
    assert by_step["formal_route_readiness_probe"]["review_status"] == "locked_read_only_real_com"
    assert by_step["senco5_neutralization_gate"]["review_status"] == "locked_controlled_write"
    assert by_step["senco6_neutralization_gate"]["review_status"] == "locked_controlled_write"
    assert by_step["senco78_neutralization_gate"]["review_status"] == "locked_controlled_write"
    assert by_step["initialization_db_preflight_postgresql18_gate"]["review_status"] == "contract_only"
    assert by_step["analyzer_check_monitor_after_chamber_temp_stable_contract"]["review_status"] == (
        "locked_read_only_real_com"
    )
    assert model["step_review_counts"]["locked_controlled_write"] >= 3
    assert model["step_review_counts"]["locked_read_only_real_com"] >= 3


def test_initialization_executor_dry_run_reviews_missing_plan(tmp_path: Path) -> None:
    model = build_v1_5_formal_initialization_executor_dry_run(
        formal_initialization_plan_json=tmp_path / "missing.json",
    )

    assert model["overall_status"] == "review_required"
    assert model["dry_run_review_allowed"] is False
    assert model["live_execution_allowed"] is False
    assert model["connects_postgresql"] is False
    assert model["checks"][0]["status"] == "review_required"
    assert "formal_initialization_plan_missing" in model["checks"][0]["reasons"]


def test_initialization_executor_dry_run_writer_and_cli_are_no_com_no_write(tmp_path: Path, capsys) -> None:
    plan_json = _write_initialization_plan(tmp_path)
    model = build_v1_5_formal_initialization_executor_dry_run(
        formal_initialization_plan_json=plan_json,
    )
    outputs = write_v1_5_formal_initialization_executor_dry_run_outputs(
        model,
        tmp_path / "dry_run",
    )

    assert Path(outputs["json"]).exists()
    assert Path(outputs["steps_csv"]).exists()
    assert Path(outputs["checks_csv"]).exists()
    assert Path(outputs["markdown"]).exists()
    assert "does not execute" in Path(outputs["markdown"]).read_text(encoding="utf-8")

    cli_out = tmp_path / "cli"
    rc = export_main(
        [
            "--formal-initialization-plan-json",
            str(plan_json),
            "--output-dir",
            str(cli_out),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["overall_status"] == "ready_for_initialization_executor_dry_run_review"
    assert payload["live_execution_allowed"] is False
    assert payload["read_only_real_com_execution_allowed"] is False
    assert payload["controlled_write_execution_allowed"] is False
    assert payload["opens_com_ports"] is False
    assert payload["connects_postgresql"] is False
    assert payload["writes_coefficients"] is False
    assert (cli_out / "v1_5_formal_initialization_executor_dry_run.json").exists()
