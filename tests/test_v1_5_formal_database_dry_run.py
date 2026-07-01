import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_database_dry_run import main as cli_main
from gas_calibrator.validation.v1_5_formal_database_dry_run import (
    build_v1_5_formal_database_dry_run_contract,
    write_v1_5_formal_database_dry_run_outputs,
)


def _check(model: dict, name: str) -> dict:
    return {row["check"]: row for row in model["checks"]}[name]


def test_formal_database_dry_run_contract_locks_postgresql18_identity_and_tables() -> None:
    model = build_v1_5_formal_database_dry_run_contract(
        planned_devices=[
            {"slot": "GA01", "port": "COM36", "sn_code": "01260601", "protocol_device_id": "047"},
            {"slot": "GA02", "port": "COM37", "sn_code": "01260602", "protocol_device_id": "054"},
        ]
    )

    assert model["schema"] == "v1_5_formal_database_dry_run_contract_v1"
    assert model["overall_status"] == "ready_for_postgresql18_schema_dry_run_review"
    assert model["blocker_count"] == 0
    assert model["production_backend"] == "postgresql"
    assert model["production_postgresql_major"] == 18
    assert model["primary_identity"] == "sn_code/device_code"
    assert model["protocol_device_id_role"] == "compatibility_alias_and_command_identity"
    assert model["transport_role"] == "COM/GA labels are run-local transport mapping only"
    assert model["connects_postgresql"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_sn"] is False
    assert model["writes_device_id"] is False
    assert model["writes_coefficients"] is False
    assert model["database_written"] is False
    assert model["database_import_allowed"] is False
    assert model["formal_release_allowed"] is False
    assert model["not_real_acceptance_evidence"] is True

    core = {row["table_name"]: row for row in model["core_storage_tables"]}
    assert core["sensors"]["present"] is True
    assert "uq_sensors_sn_code" in core["sensors"]["unique_constraints"]
    assert "uq_sensors_device_code" in core["sensors"]["unique_constraints"]
    assert "ix_sensor_identity_alias_lookup" in core["sensor_identity_aliases"]["indexes"]
    assert "uq_measurement_frames_natural_key" in core["measurement_frames"]["unique_constraints"]

    registry = {row["table_name"]: row for row in model["evidence_registry_tables"]}
    assert registry["runs"]["present"] is True
    assert registry["runs"]["unique_constraints"] == "run_id"
    assert registry["sample_files"]["unique_constraints"] == "run_db_id,path"
    assert registry["coefficient_write_events"]["present"] is True

    identity = {row["field"]: row for row in model["identity_contract"]}
    assert identity["sn_code"]["role"] == "production primary identity"
    assert "compatibility" in identity["protocol_device_id"]["role"]
    assert "transport" in identity["com_port"]["role"]

    stages = {row["stage"] for row in model["insert_preview"]}
    assert {
        "initialization_identity",
        "runtime_setup",
        "open_flow_sampling",
        "fit_and_candidate_review",
        "controlled_write_and_readback",
        "archive_report_release",
    }.issubset(stages)
    assert all(row["status"] == "ready" for row in model["planned_device_preview"])

    assert _check(model, "postgresql18_backend_contract")["status"] == "ready"
    assert _check(model, "core_storage_schema_contract")["status"] == "ready"
    assert _check(model, "evidence_registry_schema_contract")["status"] == "ready"
    assert _check(model, "sn_device_code_identity_contract")["status"] == "ready"
    assert _check(model, "planned_device_identity_preview")["status"] == "ready"
    assert _check(model, "insert_preview_contract")["status"] == "ready"
    assert _check(model, "dry_run_does_not_authorize_import_or_release")["status"] == "ready"


def test_formal_database_dry_run_blocks_non_postgresql18_requirement() -> None:
    model = build_v1_5_formal_database_dry_run_contract(required_postgresql_major=17)

    assert model["overall_status"] == "blocked"
    assert _check(model, "postgresql18_backend_contract")["status"] == "blocker"
    assert "required_postgresql_major=17" in _check(model, "postgresql18_backend_contract")["reasons"]
    assert model["database_import_allowed"] is False


def test_formal_database_dry_run_blocks_duplicate_planned_sn() -> None:
    model = build_v1_5_formal_database_dry_run_contract(
        planned_devices=["01260601=047", "01260601=054"]
    )

    assert model["overall_status"] == "blocked"
    planned_check = _check(model, "planned_device_identity_preview")
    assert planned_check["status"] == "blocker"
    assert any("duplicate_sn_code" in reason for reason in planned_check["reasons"])
    assert any(row["status"] == "blocked" for row in model["planned_device_preview"])


def test_formal_database_dry_run_writer_and_cli(tmp_path: Path) -> None:
    model = build_v1_5_formal_database_dry_run_contract(
        planned_devices=["01260601=047", "01260602=054"]
    )
    outputs = write_v1_5_formal_database_dry_run_outputs(model, tmp_path / "db_dry_run")

    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["core_tables_csv"].exists()
    assert outputs["registry_tables_csv"].exists()
    assert outputs["identity_contract_csv"].exists()
    assert outputs["insert_preview_csv"].exists()
    assert outputs["planned_device_preview_csv"].exists()
    assert outputs["markdown"].exists()
    assert "PostgreSQL 18" in outputs["markdown"].read_text(encoding="utf-8")
    payload = json.loads(outputs["json"].read_text(encoding="utf-8-sig"))
    assert payload["overall_status"] == "ready_for_postgresql18_schema_dry_run_review"

    cli_out = tmp_path / "cli_db_dry_run"
    rc = cli_main(
        [
            "--planned-device",
            "01260601=047",
            "--planned-device",
            "01260602=054",
            "--output-dir",
            str(cli_out),
            "--fail-on-blocker",
        ]
    )
    assert rc == 0
    assert (cli_out / "v1_5_formal_database_dry_run.json").exists()


def test_formal_database_dry_run_empty_planned_device_csv_keeps_headers(tmp_path: Path) -> None:
    model = build_v1_5_formal_database_dry_run_contract()
    outputs = write_v1_5_formal_database_dry_run_outputs(model, tmp_path / "db_dry_run")

    planned_text = outputs["planned_device_preview_csv"].read_text(encoding="utf-8-sig")

    assert model["planned_device_preview"] == []
    assert planned_text.splitlines() == [
        "slot,sn_code,device_code,protocol_device_id,port,status,reasons,identity_query_paths,transport_identity_role"
    ]
