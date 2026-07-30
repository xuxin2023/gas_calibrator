import csv
import json
from pathlib import Path

import gas_calibrator.v1_5.orchestration.operator_workstation as workstation_module
import pytest
from gas_calibrator.tools.run_v1_5_operator_workstation_dry_run import main as cli_main
from gas_calibrator.v1_5.orchestration.operator_workstation import (
    build_v1_5_operator_workstation_plan,
    execute_v1_5_operator_workstation_dry_run,
    run_v1_5_operator_workstation_application,
)
from gas_calibrator.validation.v1_5_algorithm_route_profiles import (
    build_v1_5_profile_queue_rows,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"
CONFIG_PATH = ROOT / "configs" / "default_config.json"


def _write_csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0])
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _legacy_queues(tmp_path: Path) -> tuple[Path, Path]:
    queues = build_v1_5_profile_queue_rows(
        PROFILE_PATH,
        profile_id="legacy_ratio_production",
    )
    return (
        _write_csv(tmp_path / "co2_runner_queue.csv", queues["co2_rows"]),
        _write_csv(tmp_path / "h2o_runner_queue.csv", queues["h2o_rows"]),
    )


def test_operator_workstation_locks_mature_v1_5_and_keeps_certificate_non_blocking(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    corrupt_certificate = tmp_path / "certificate_metrics_registry.json"
    corrupt_certificate.write_text("{not-json", encoding="utf-8")

    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="six_device_parity",
        certificate_registry_json=corrupt_certificate,
    )

    assert plan["overall_status"] == "ready_for_v1_5_dry_run"
    assert plan["product_name"] == "V1.5 气体分析仪校准工作站"
    assert plan["calibration_kernel"] == "v1_5_legacy_ratio_0613_0620_0621"
    assert plan["profile_id"] == "legacy_ratio_production"
    assert plan["point_counts"] == {"co2": 45, "h2o": 13}
    assert plan["certificate_start_gate"] == "non_blocking"
    assert plan["warnings"] == ["certificate_registry_unreadable_non_blocking"]
    assert plan["v1_fallback_preserved"] is True
    assert plan["modifies_run_app"] is False
    assert plan["v2_role"] == "temporary_migration_and_deletion_pool_not_product_runtime"
    assert all("--dry-run" in row["argv"] for row in plan["routes"])
    assert all("--no-prompt" in row["argv"] for row in plan["routes"])
    assert all("--no-ftd-write" in row["argv"] for row in plan["routes"])


def test_operator_workstation_blocks_point_count_drift_before_runner_execution(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    rows = list(csv.DictReader(co2_queue.open(encoding="utf-8-sig", newline="")))
    _write_csv(co2_queue, rows[:-1])

    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="count_drift",
    )
    result = execute_v1_5_operator_workstation_dry_run(plan)

    assert plan["overall_status"] == "blocked"
    assert "co2_legacy_point_count_mismatch:expected=45,observed=44" in plan["blockers"]
    assert result["execution_started"] is False
    assert result["route_results"] == []


def test_application_service_executes_once_and_writes_once(
    tmp_path: Path,
    monkeypatch,
) -> None:
    executor_calls: list[dict] = []
    writer_calls: list[tuple[dict, Path]] = []
    plan = {
        "overall_status": "ready_for_v1_5_dry_run",
        "point_counts": {"co2": 45, "h2o": 13},
    }

    def fake_executor(payload):
        executor_calls.append(dict(payload))
        return {**dict(payload), "overall_status": "pass"}

    def fake_writer(result, output_dir):
        root = Path(output_dir)
        writer_calls.append((dict(result), root))
        return {
            "json": root / "v1_5_operator_workstation_dry_run.json",
            "markdown": root / "V1_5_OPERATOR_WORKSTATION_DRY_RUN.md",
        }

    monkeypatch.setattr(
        workstation_module,
        "write_v1_5_operator_workstation_outputs",
        fake_writer,
    )

    result, outputs = run_v1_5_operator_workstation_application(
        plan,
        output_dir=tmp_path,
        executor=fake_executor,
    )

    assert len(executor_calls) == 1
    assert len(writer_calls) == 1
    assert writer_calls[0][0] == result
    assert writer_calls[0][1] == tmp_path
    assert outputs["json"].parent == tmp_path
    assert result["overall_status"] == "pass"


def test_application_service_does_not_write_after_executor_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    writer_calls: list[dict] = []

    def failed_executor(_payload):
        raise RuntimeError("synthetic executor failure")

    def fake_writer(result, _output_dir):
        writer_calls.append(dict(result))
        return {}

    monkeypatch.setattr(
        workstation_module,
        "write_v1_5_operator_workstation_outputs",
        fake_writer,
    )

    with pytest.raises(RuntimeError, match="synthetic executor failure"):
        run_v1_5_operator_workstation_application(
            {"overall_status": "ready_for_v1_5_dry_run"},
            output_dir=tmp_path,
            executor=failed_executor,
        )

    assert writer_calls == []


def test_operator_workstation_cli_executes_both_mature_runner_dry_run_branches(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    output_dir = tmp_path / "workstation"

    rc = cli_main(
        [
            "--config",
            str(CONFIG_PATH),
            "--co2-queue-csv",
            str(co2_queue),
            "--h2o-queue-csv",
            str(h2o_queue),
            "--output-dir",
            str(output_dir),
            "--run-id",
            "end_to_end",
        ]
    )

    assert rc == 0
    result = json.loads(
        (output_dir / "v1_5_operator_workstation_dry_run.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert result["overall_status"] == "pass"
    assert result["point_counts"] == {"co2": 45, "h2o": 13}
    assert [(row["route_kind"], row["dry_run_points"]) for row in result["route_results"]] == [
        ("co2", 45),
        ("h2o", 13),
    ]
    assert all(row["status"] == "pass" for row in result["route_results"])
    assert result["opens_com_ports"] is False
    assert result["writes_coefficients"] is False
    assert (output_dir / "V1_5_OPERATOR_WORKSTATION_DRY_RUN.md").exists()
