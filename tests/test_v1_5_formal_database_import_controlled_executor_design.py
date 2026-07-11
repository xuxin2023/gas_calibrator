import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_database_import_controlled_executor_design import main as cli_main
from gas_calibrator.validation.v1_5_formal_database_import_controlled_executor_design import (
    build_v1_5_formal_database_import_controlled_executor_design,
    write_v1_5_formal_database_import_controlled_executor_design,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _blocked_executor_json(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "blocked_executor" / "v1_5_formal_database_import_blocked_executor.json",
        {
            "schema": "v1_5_formal_database_import_blocked_executor_v1",
            "overall_status": "blocked_pending_controlled_executor_implementation",
            "blocked_executor_ready": True,
            "execution_supported": False,
            "real_import_execution_allowed": False,
            "connects_postgresql": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "applies_migrations": False,
            "database_import_attempted": False,
            "database_written": False,
            "database_import_allowed": False,
            "senco_authorization_archive_binding_ready": True,
            "senco_authorization_archive_binding_json": str(
                (tmp_path / "archive" / "binding.json").resolve()
            ),
            "senco_authorization_archive_binding_sha256": "a" * 64,
        },
    )


def test_controlled_executor_design_is_offline_and_execution_blocked(tmp_path: Path) -> None:
    blocked_executor = _blocked_executor_json(tmp_path)

    tables = build_v1_5_formal_database_import_controlled_executor_design(
        formal_database_import_blocked_executor_json=blocked_executor,
    )
    manifest = tables["manifest"]
    gates = {row["gate"]: row for row in tables["boundary_gates"]}

    assert manifest["overall_status"] == "ready_for_controlled_import_executor_design_review"
    assert manifest["production_state"] == "blocked_design_only"
    assert manifest["execution_supported"] is False
    assert manifest["real_import_execution_allowed"] is False
    assert manifest["connects_postgresql"] is False
    assert manifest["applies_migrations"] is False
    assert manifest["database_written"] is False
    assert manifest["required_future_execute_flag"] == "--execute-controlled-import"
    assert manifest["senco_authorization_archive_binding_ready"] is True
    assert gates["design_only_no_connect"]["status"] == "pass"
    assert gates["future_execute_still_blocked"]["status"] == "pass"


def test_controlled_executor_design_defines_authorization_transaction_readback_and_rollback(tmp_path: Path) -> None:
    tables = build_v1_5_formal_database_import_controlled_executor_design(
        formal_database_import_blocked_executor_json=_blocked_executor_json(tmp_path),
    )
    auth = {row["gate"]: row for row in tables["authorization_contract"]}
    tx = {row["step"]: row for row in tables["transaction_contract"]}
    readback = {row["readback"]: row for row in tables["readback_contract"]}
    rollback = {row["trigger"]: row for row in tables["rollback_contract"]}

    assert auth["explicit_execute_flag"]["future_flag"] == "--execute-controlled-import"
    assert auth["reviewer_approver_dual_authorization"]["future_fields"] == (
        "reviewer;approver;authorization_id"
    )
    assert auth["dsn_env_only"]["contract"].startswith("DSN value must come from environment")
    assert tx["begin_transaction"]["action"].startswith("open one PostgreSQL 18 transaction")
    assert tx["post_insert_readback_before_commit"]["failure_policy"] == (
        "rollback_transaction_no_partial_acceptance"
    )
    assert readback["device_identity"]["expected"].startswith("SN/device_code unique primary identity")
    assert rollback["validation_failure_before_commit"]["rollback_action"] == "rollback transaction"
    assert rollback["post_commit_external_discrepancy"]["rollback_action"].startswith("do not auto-delete")


def test_controlled_executor_design_writer_and_cli_create_artifacts(tmp_path: Path, capsys) -> None:
    blocked_executor = _blocked_executor_json(tmp_path)
    output = tmp_path / "design"

    outputs = write_v1_5_formal_database_import_controlled_executor_design(
        output,
        formal_database_import_blocked_executor_json=blocked_executor,
    )

    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8-sig"))
    assert manifest["not_real_acceptance_evidence"] is True
    assert Path(outputs["authorization_contract"]).exists()
    assert Path(outputs["transaction_contract"]).exists()
    assert Path(outputs["readback_contract"]).exists()
    assert Path(outputs["rollback_contract"]).exists()
    assert "does not implement the real executor" in Path(outputs["summary"]).read_text(encoding="utf-8")
    assert _read_csv(Path(outputs["boundary_gates"]))[0]["gate"] == "design_only_no_connect"

    cli_out = tmp_path / "cli_design"
    rc = cli_main(
        [
            "--formal-database-import-blocked-executor-json",
            str(blocked_executor),
            "--output-dir",
            str(cli_out),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["overall_status"] == "ready_for_controlled_import_executor_design_review"
    assert payload["connects_postgresql"] is False
    assert payload["database_written"] is False
    assert (cli_out / "v1_5_formal_database_import_controlled_executor_design.json").exists()


def test_controlled_executor_design_reviews_missing_blocked_executor(tmp_path: Path) -> None:
    tables = build_v1_5_formal_database_import_controlled_executor_design(
        formal_database_import_blocked_executor_json=tmp_path / "missing.json",
    )

    assert tables["manifest"]["overall_status"] == "review_required"
    assert tables["manifest"]["review_required_count"] == 1
    gates = {row["gate"]: row for row in tables["boundary_gates"]}
    assert gates["blocked_executor_consumed"]["status"] == "review_required"
    assert gates["blocked_executor_consumed"]["evidence"] == "blocked_executor_evidence_missing"


def test_controlled_executor_design_reviews_unbound_senco_archive_evidence(tmp_path: Path) -> None:
    blocked_executor = _blocked_executor_json(tmp_path)
    payload = json.loads(blocked_executor.read_text(encoding="utf-8"))
    payload["senco_authorization_archive_binding_ready"] = False
    blocked_executor.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    tables = build_v1_5_formal_database_import_controlled_executor_design(
        formal_database_import_blocked_executor_json=blocked_executor,
    )

    assert tables["manifest"]["overall_status"] == "review_required"
    gates = {row["gate"]: row for row in tables["boundary_gates"]}
    assert "blocked_executor_senco_authorization_archive_binding_not_ready" in gates[
        "blocked_executor_consumed"
    ]["evidence"]
