import csv
import json
from pathlib import Path

import gas_calibrator.v1_5.orchestration.operator_workstation as workstation_module
import pytest
from gas_calibrator.tools.run_v1_5_operator_workstation_dry_run import main as cli_main
from gas_calibrator.v1_5.orchestration.operator_workstation import (
    STARTUP_RECEIPT_SCHEMA,
    build_v1_5_operator_workstation_plan,
    build_v1_5_operator_workstation_startup_receipt,
    execute_v1_5_operator_workstation_dry_run,
    inspect_v1_5_runtime_config,
    run_v1_5_operator_workstation_application,
    write_v1_5_operator_workstation_startup_receipt,
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
    handoff = plan["controlled_execution_handoff"]
    assert handoff["status"] == "blocked_pending_explicit_double_unlock"
    assert handoff["execution_allowed"] is False
    assert handoff["operator_confirmation_embedded"] is False
    assert handoff["uses_existing_mature_runners"] is True
    assert all("--dry-run" not in row["argv_template"] for row in handoff["commands"])
    assert all("--no-ftd-write" in row["argv_template"] for row in handoff["commands"])
    assert all("--engineering-probe-only" in row["argv_template"] for row in handoff["commands"])
    assert handoff["operator_confirmation_required_sha256"]
    assert all(row["queue_csv_sha256"] for row in handoff["commands"])
    assert all(
        row["runner_confirmation_record_expectation"][
            "written_by_mature_runner_before_device_construction"
        ]
        is True
        for row in handoff["commands"]
    )


def test_startup_receipt_binds_inputs_but_keeps_operator_record_blank(
    tmp_path: Path,
) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="receipt_review",
    )

    receipt = build_v1_5_operator_workstation_startup_receipt(plan)

    assert receipt["schema"] == STARTUP_RECEIPT_SCHEMA
    assert receipt["status"] == "startup_preflight_recorded_execution_locked"
    assert receipt["startup_gate_passed"] is True
    assert receipt["runtime_config"]["sha256"]
    assert receipt["queues"]["co2"]["sha256"]
    assert receipt["queues"]["h2o"]["sha256"]
    assert receipt["probe_scope_selected"] is False
    assert receipt["probe_execution_allowed"] is False
    assert receipt["operator_acknowledgement_template"]["completed"] is False
    assert receipt["operator_acknowledgement_template"]["operator_name"] == ""
    assert (
        receipt["operator_acknowledgement_template"]["execution_authorization"]
        is False
    )
    assert receipt["opens_com_ports"] is False
    assert receipt["not_real_acceptance_evidence"] is True


def test_startup_receipt_writer_is_immutable(tmp_path: Path) -> None:
    co2_queue, h2o_queue = _legacy_queues(tmp_path)
    plan = build_v1_5_operator_workstation_plan(
        config_path=CONFIG_PATH,
        co2_queue_csv=co2_queue,
        h2o_queue_csv=h2o_queue,
        output_dir=tmp_path / "workstation",
        run_id="immutable_receipt",
    )
    path = tmp_path / "receipt.json"

    written = write_v1_5_operator_workstation_startup_receipt(plan, path)

    assert written["path"] == str(path.resolve())
    assert written["sha256"]
    assert written["probe_execution_allowed"] is False
    with pytest.raises(FileExistsError):
        write_v1_5_operator_workstation_startup_receipt(plan, path)


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


def test_runtime_config_gate_accepts_unique_protocol_bound_pressure_ports(
    tmp_path: Path,
) -> None:
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    payload["devices"]["pressure_controller"].update(
        {
            "configured_port": "COM23",
            "port": "COM31",
            "runtime_port": "COM31",
            "runtime_port_binding_source": "v1_5_reference_bank_shift_protocol_identity",
            "runtime_port_binding_frozen": True,
        }
    )
    payload["devices"]["pressure_gauge"].update(
        {
            "configured_port": "COM22",
            "port": "COM30",
            "runtime_port": "COM30",
            "runtime_port_binding_source": "v1_5_reference_bank_shift_protocol_identity",
            "runtime_port_binding_frozen": True,
        }
    )
    payload["devices"]["dewpoint_meter"].update(
        {
            "configured_port": "COM17",
            "port": "COM25",
            "runtime_port": "COM25",
            "runtime_port_binding_source": "v1_5_reference_bank_shift",
            "runtime_port_binding_frozen": True,
        }
    )
    payload["v1_5_serial_port_binding"] = {
        "enabled": True,
        "available_ports": ["COM22", "COM23", "COM30", "COM31"],
        "changed_count": 3,
        "blocked_count": 0,
        "gas_analyzer_ports_protected": True,
        "require_protocol_match": True,
    }
    config_path = tmp_path / "runtime_bound_config.json"
    config_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    inspection = inspect_v1_5_runtime_config(config_path)

    assert inspection["status"] == "ready_bound_runtime_config"
    assert inspection["blockers"] == []
    assert len(inspection["sha256"]) == 64
    assert inspection["pressure_devices"]["pressure_controller"]["runtime_port"] == "COM31"
    assert inspection["pressure_devices"]["pressure_gauge"]["runtime_port"] == "COM30"
    assert inspection["reference_devices"]["dewpoint_meter"]["runtime_port"] == "COM25"
    assert inspection["opens_com_ports"] is False
    assert inspection["writes_config"] is False


def test_runtime_config_gate_blocks_dual_bank_mapping_without_unique_identity(
    tmp_path: Path,
) -> None:
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    payload["devices"]["pressure_controller"].update(
        {
            "configured_port": "COM23",
            "port": "COM31",
            "runtime_port_binding_source": "v1_5_reference_bank_shift",
            "runtime_port_binding_frozen": True,
        }
    )
    payload["v1_5_serial_port_binding"] = {
        "enabled": True,
        "available_ports": ["COM23", "COM31"],
        "changed_count": 1,
        "blocked_count": 0,
        "gas_analyzer_ports_protected": True,
        "require_protocol_match": False,
    }
    config_path = tmp_path / "unsafe_bound_config.json"
    config_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    inspection = inspect_v1_5_runtime_config(config_path)

    assert inspection["status"] == "blocked"
    assert (
        "pressure_controller_dual_bank_unique_protocol_identity_missing"
        in inspection["blockers"]
    )


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
