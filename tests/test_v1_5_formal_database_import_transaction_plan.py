from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_database_import_transaction_plan import main as cli_main
from gas_calibrator.validation.v1_5_formal_database_dry_run import (
    build_v1_5_formal_database_dry_run_contract,
    write_v1_5_formal_database_dry_run_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_transaction_plan import (
    READY_STATUS,
    build_v1_5_formal_database_import_transaction_plan,
    write_v1_5_formal_database_import_transaction_plan_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _design(path: Path) -> Path:
    return _write_json(
        path,
        {
            "schema": "v1_5_formal_database_import_controlled_executor_design_v1",
            "overall_status": "ready_for_controlled_import_executor_design_review",
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "dsn_env": "V1_5_POSTGRES_DSN",
            "dsn_value_read": False,
            "connects_postgresql": False,
            "database_written": False,
            "database_import_attempted": False,
            "real_import_execution_allowed": False,
            "execution_supported": False,
            "database_import_authorization_binding_ready": True,
            "database_import_preflight_binding_ready": True,
            "evidence_bundle_schema_ready": True,
            "evidence_bundle_binding_ready": True,
            "archive_closure_index_binding_ready": True,
            "senco_authorization_archive_binding_ready": True,
        },
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _optional_inputs(tmp_path: Path) -> dict[str, Path]:
    authorization = _write_json(
        tmp_path / "authorization.json",
        {
            "schema": "v1_5_formal_database_import_authorization_v1",
            "manual_authorization_ready": True,
            "database_import_preflight_binding_ready": True,
            "archive_release_ready": True,
            "archive_closure_index_binding_ready": True,
            "senco_authorization_archive_binding_ready": True,
            "database_import_allowed": True,
        },
    )
    preflight = _write_json(
        tmp_path / "preflight.json",
        {
            "schema": "v1_5_formal_database_import_preflight_v1",
            "overall_status": "ready_for_authorized_postgresql18_import_review",
            "dsn_configured": True,
            "dry_run_contract_ready": True,
        },
    )
    archive = _write_json(
        tmp_path / "archive.json",
        {
            "schema": "v1_5_formal_archive_closure_v1",
            "overall_status": "ready",
            "package_status": "ready",
            "identity_getco_traceability": {"ready_for_archive_release": True},
        },
    )
    evidence = _write_json(
        tmp_path / "bundle.json",
        {
            "schema": "v1_5_evidence_registry",
            "tables": {"runs": [{"run_id": "run-001"}]},
        },
    )
    command = _write_json(
        tmp_path / "command.json",
        {
            "schema": "v1_5_formal_database_import_command_contract_v1",
            "command_contract_ready": True,
            "database_import_authorization_binding_ready": True,
            "database_import_preflight_binding_ready": True,
            "archive_release_ready": True,
            "archive_closure_index_binding_ready": True,
            "senco_authorization_archive_binding_ready": True,
            "evidence_bundle_ready": True,
            "evidence_bundle_schema_ready": True,
            "evidence_bundle_binding_ready": True,
            "connects_postgresql": False,
            "applies_migrations": False,
            "database_import_attempted": False,
            "database_written": False,
            "database_import_allowed": False,
            "real_import_execution_allowed": False,
            "formal_database_import_authorization_json": str(authorization.resolve()),
            "formal_database_import_preflight_json": str(preflight.resolve()),
            "archive_closure_json": str(archive.resolve()),
            "evidence_bundle_json": str(evidence.resolve()),
            "formal_database_import_authorization_sha256": _sha256(authorization),
            "formal_database_import_preflight_sha256": _sha256(preflight),
            "archive_closure_sha256": _sha256(archive),
            "evidence_bundle_sha256": _sha256(evidence),
        },
    )
    return {
        "formal_database_import_command_contract_json": command,
        "formal_database_import_authorization_json": authorization,
        "formal_database_import_preflight_json": preflight,
        "archive_closure_json": archive,
        "evidence_bundle_json": evidence,
    }


def _dry_run(tmp_path: Path, planned_devices: list[dict] | None = None) -> Path:
    model = build_v1_5_formal_database_dry_run_contract(planned_devices=planned_devices)
    return write_v1_5_formal_database_dry_run_outputs(model, tmp_path / "dry_run")["json"]


def test_transaction_plan_is_deterministic_offline_contract(tmp_path: Path) -> None:
    model = build_v1_5_formal_database_import_transaction_plan(
        formal_database_dry_run_json=_dry_run(tmp_path),
        formal_database_import_controlled_executor_design_json=_design(tmp_path / "design.json"),
    )

    assert model["overall_status"] == READY_STATUS
    assert model["transaction_plan_contract_ready"] is True
    assert model["production_transaction_package_ready"] is False
    assert model["production_blocking_reasons"][0] == "planned_device_preview_empty"
    assert model["planned_device_count"] == 0
    assert model["production_postgresql_major"] == 18
    assert model["dsn_value_read"] is False
    assert model["emits_executable_sql"] is False
    assert model["connects_postgresql"] is False
    assert model["database_written"] is False
    assert model["database_import_allowed"] is False
    operations = model["transaction_operations"]
    assert [row["order"] for row in operations] == list(range(1, len(operations) + 1))
    assert all(row["would_execute"] is False for row in operations)
    assert [row["stage"] for row in operations[2:-2]] == [
        "initialization_identity",
        "runtime_setup",
        "pressure_temperature_pre_open_flow",
        "open_flow_sampling",
        "fit_and_candidate_review",
        "controlled_write_and_readback",
        "archive_report_release",
    ]


def test_transaction_plan_accepts_one_to_six_unique_devices_only(tmp_path: Path) -> None:
    devices = [
        {
            "slot": f"GA{index:02d}",
            "sn_code": f"012607{index:02d}",
            "device_code": f"012607{index:02d}",
            "protocol_device_id": f"{index:03d}",
            "port": f"COM{34 + index}",
        }
        for index in range(1, 7)
    ]
    model = build_v1_5_formal_database_import_transaction_plan(
        formal_database_dry_run_json=_dry_run(tmp_path, devices),
        formal_database_import_controlled_executor_design_json=_design(tmp_path / "design.json"),
        **_optional_inputs(tmp_path),
    )

    assert model["transaction_plan_contract_ready"] is True
    assert model["production_transaction_package_ready"] is True
    assert model["planned_device_count"] == 6
    assert model["production_blocking_reasons"] == []
    assert model["database_import_allowed"] is False
    assert model["real_import_execution_allowed"] is False


def test_transaction_plan_blocks_bad_or_duplicate_identity(tmp_path: Path) -> None:
    devices = [
        {
            "slot": "GA01",
            "sn_code": "00000000",
            "device_code": "00000000",
            "protocol_device_id": "001",
        },
        {
            "slot": "GA02",
            "sn_code": "01260702",
            "device_code": "01260702",
            "protocol_device_id": "001",
        },
    ]
    model = build_v1_5_formal_database_import_transaction_plan(
        formal_database_dry_run_json=_dry_run(tmp_path, devices),
        formal_database_import_controlled_executor_design_json=_design(tmp_path / "design.json"),
        **_optional_inputs(tmp_path),
    )

    assert model["transaction_plan_contract_ready"] is True
    assert model["production_transaction_package_ready"] is False
    assert "GA01:sn_code_invalid" in model["production_blocking_reasons"]
    assert "GA02:duplicate_protocol_device_id_in_run" in model["production_blocking_reasons"]


def test_transaction_plan_blocks_duplicate_transport_mapping(tmp_path: Path) -> None:
    devices = [
        {
            "slot": "GA01",
            "sn_code": "01260701",
            "device_code": "01260701",
            "protocol_device_id": "001",
            "port": "COM35",
        },
        {
            "slot": "GA01",
            "sn_code": "01260702",
            "device_code": "01260702",
            "protocol_device_id": "002",
            "port": "COM35",
        },
    ]
    model = build_v1_5_formal_database_import_transaction_plan(
        formal_database_dry_run_json=_dry_run(tmp_path, devices),
        formal_database_import_controlled_executor_design_json=_design(tmp_path / "design.json"),
        **_optional_inputs(tmp_path),
    )
    assert model["production_transaction_package_ready"] is False
    assert "GA01:duplicate_slot" in model["production_blocking_reasons"]
    assert "GA01:duplicate_com_port" in model["production_blocking_reasons"]


def test_transaction_plan_holds_on_backend_or_stage_contract_tamper(tmp_path: Path) -> None:
    dry_run_json = _dry_run(tmp_path)
    dry_run = json.loads(dry_run_json.read_text(encoding="utf-8-sig"))
    dry_run["production_postgresql_major"] = 17
    dry_run["insert_preview"][0]["natural_key"] = ""
    dry_run_json.write_text(json.dumps(dry_run, ensure_ascii=False, indent=2), encoding="utf-8")

    model = build_v1_5_formal_database_import_transaction_plan(
        formal_database_dry_run_json=dry_run_json,
        formal_database_import_controlled_executor_design_json=_design(tmp_path / "design.json"),
    )

    assert model["overall_status"] == "review_required"
    assert model["transaction_plan_contract_ready"] is False
    assert "production_postgresql_major_not_18" in model["contract_reasons"]
    assert "initialization_identity:natural_key_missing" in model["contract_reasons"]


def test_transaction_plan_rehashes_frozen_import_inputs(tmp_path: Path) -> None:
    inputs = _optional_inputs(tmp_path)
    authorization = inputs["formal_database_import_authorization_json"]
    authorization.write_text(
        json.dumps(
            {
                "schema": "v1_5_formal_database_import_authorization_v1",
                "manual_authorization_ready": True,
                "changed_after_command_contract": True,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    model = build_v1_5_formal_database_import_transaction_plan(
        formal_database_dry_run_json=_dry_run(
            tmp_path,
            [
                {
                    "slot": "GA01",
                    "sn_code": "01260701",
                    "device_code": "01260701",
                    "protocol_device_id": "001",
                }
            ],
        ),
        formal_database_import_controlled_executor_design_json=_design(tmp_path / "design.json"),
        **inputs,
    )

    assert model["transaction_plan_contract_ready"] is True
    assert model["production_transaction_package_ready"] is False
    assert (
        "formal_database_import_authorization_sha256_mismatch_with_command_contract"
        in model["production_blocking_reasons"]
    )


def test_transaction_plan_requires_exact_frozen_input_paths(tmp_path: Path) -> None:
    inputs = _optional_inputs(tmp_path)
    command_path = inputs["formal_database_import_command_contract_json"]
    command = json.loads(command_path.read_text(encoding="utf-8"))
    command["formal_database_import_authorization_json"] = str(
        (tmp_path / "different" / "authorization.json").resolve()
    )
    command_path.write_text(json.dumps(command, indent=2), encoding="utf-8")
    model = build_v1_5_formal_database_import_transaction_plan(
        formal_database_dry_run_json=_dry_run(
            tmp_path,
            [
                {
                    "slot": "GA01",
                    "sn_code": "01260701",
                    "device_code": "01260701",
                    "protocol_device_id": "001",
                }
            ],
        ),
        formal_database_import_controlled_executor_design_json=_design(tmp_path / "design.json"),
        **inputs,
    )
    assert model["production_transaction_package_ready"] is False
    assert (
        "formal_database_import_authorization_path_mismatch_with_command_contract"
        in model["production_blocking_reasons"]
    )


def test_transaction_plan_writer_and_cli_never_read_dsn_value(tmp_path: Path, capsys) -> None:
    dry_run_json = _dry_run(tmp_path)
    design_json = _design(tmp_path / "design.json")
    model = build_v1_5_formal_database_import_transaction_plan(
        formal_database_dry_run_json=dry_run_json,
        formal_database_import_controlled_executor_design_json=design_json,
    )
    outputs = write_v1_5_formal_database_import_transaction_plan_outputs(model, tmp_path / "out")
    assert outputs["json"].exists()
    assert outputs["operations_csv"].exists()
    assert "does not emit SQL" in outputs["markdown"].read_text(encoding="utf-8")

    rc = cli_main(
        [
            "--formal-database-dry-run-json",
            str(dry_run_json),
            "--formal-database-import-controlled-executor-design-json",
            str(design_json),
            "--output-dir",
            str(tmp_path / "cli"),
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["connects_postgresql"] is False
    assert payload["database_written"] is False


def test_transaction_plan_module_has_no_db_network_or_process_imports() -> None:
    source_path = Path(
        "src/gas_calibrator/validation/v1_5_formal_database_import_transaction_plan.py"
    )
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".", 1)[0])
    assert roots.isdisjoint({"psycopg", "psycopg2", "sqlalchemy", "socket", "subprocess"})
