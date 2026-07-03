import json
from pathlib import Path

from gas_calibrator.tools.import_v1_5_evidence_package import main as cli_main
from gas_calibrator.validation.v1_5_formal_database_dry_run import (
    build_v1_5_formal_database_dry_run_contract,
    write_v1_5_formal_database_dry_run_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_authorization import (
    build_v1_5_formal_database_import_authorization,
    write_v1_5_formal_database_import_authorization_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_blocked_executor import (
    build_v1_5_formal_database_import_blocked_executor,
    write_v1_5_formal_database_import_blocked_executor_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_command_contract import (
    build_v1_5_formal_database_import_command_contract,
    write_v1_5_formal_database_import_command_contract_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_preflight import (
    build_v1_5_formal_database_import_preflight,
    write_v1_5_formal_database_import_preflight_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _archive_json(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "archive" / "v1_5_formal_archive_closure_index.json",
        {
            "schema": "v1_5_formal_archive_closure_v1",
            "overall_status": "ready",
            "package_status": "ready",
            "identity_getco_traceability": {
                "status": "ready",
                "ready_for_archive_release": True,
                "traceability_review_required": False,
            },
        },
    )


def _evidence_bundle_json(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "archive" / "evidence_bundle.json",
        {"schema": "v1_5_formal_evidence_bundle_v1", "tables": {"devices": [], "samples": []}},
    )


def _ready_command_contract(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    dry_run_model = build_v1_5_formal_database_dry_run_contract()
    dry_run_paths = write_v1_5_formal_database_dry_run_outputs(dry_run_model, tmp_path / "dry_run")
    preflight_model = build_v1_5_formal_database_import_preflight(
        formal_database_dry_run_json=dry_run_paths["json"],
        dsn="postgresql://user:secret@localhost:5432/v15",
    )
    preflight_paths = write_v1_5_formal_database_import_preflight_outputs(
        preflight_model,
        tmp_path / "preflight",
    )
    archive_path = _archive_json(tmp_path)
    bundle_path = _evidence_bundle_json(tmp_path)
    authorization_model = build_v1_5_formal_database_import_authorization(
        formal_database_import_preflight_json=preflight_paths["json"],
        archive_closure_json=archive_path,
        operator="operator-a",
        reviewer="reviewer-a",
        approver="approver-a",
        authorization_id="db-import-001",
    )
    authorization_paths = write_v1_5_formal_database_import_authorization_outputs(
        authorization_model,
        tmp_path / "authorization",
    )
    contract_model = build_v1_5_formal_database_import_command_contract(
        formal_database_import_authorization_json=authorization_paths["json"],
        formal_database_import_preflight_json=preflight_paths["json"],
        archive_closure_json=archive_path,
        evidence_bundle_json=bundle_path,
    )
    contract_paths = write_v1_5_formal_database_import_command_contract_outputs(
        contract_model,
        tmp_path / "command_contract",
    )
    return contract_paths["json"], authorization_paths["json"], preflight_paths["json"], archive_path, bundle_path


def _checks(model: dict) -> dict[str, dict]:
    return {row["check"]: row for row in model["checks"]}


def test_blocked_executor_consumes_ready_contract_without_connecting(tmp_path: Path) -> None:
    contract_json, authorization_json, preflight_json, archive_json, bundle_json = _ready_command_contract(tmp_path)

    model = build_v1_5_formal_database_import_blocked_executor(
        formal_database_import_command_contract_json=contract_json,
        formal_database_import_authorization_json=authorization_json,
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        evidence_bundle_json=bundle_json,
    )

    assert model["schema"] == "v1_5_formal_database_import_blocked_executor_v1"
    assert model["overall_status"] == "blocked_pending_controlled_executor_implementation"
    assert model["blocked_executor_ready"] is True
    assert model["execution_supported"] is False
    assert model["real_import_execution_allowed"] is False
    assert model["connects_postgresql"] is False
    assert model["applies_migrations"] is False
    assert model["database_import_attempted"] is False
    assert model["database_written"] is False
    assert model["database_import_allowed"] is False
    assert _checks(model)["formal_database_import_command_contract_consumed"]["status"] == "ready"
    assert _checks(model)["postgresql_side_effect_lock"]["status"] == "ready"


def test_blocked_executor_reviews_missing_contract_or_input_paths(tmp_path: Path) -> None:
    model = build_v1_5_formal_database_import_blocked_executor(
        formal_database_import_command_contract_json=tmp_path / "missing_contract.json",
        formal_database_import_authorization_json=tmp_path / "missing_auth.json",
        formal_database_import_preflight_json=tmp_path / "missing_preflight.json",
        archive_closure_json=tmp_path / "missing_archive.json",
        evidence_bundle_json=tmp_path / "missing_bundle.json",
    )

    assert model["overall_status"] == "review_required"
    assert model["blocked_executor_ready"] is False
    assert model["connects_postgresql"] is False
    assert model["database_written"] is False
    checks = _checks(model)
    assert "formal_database_import_command_contract_missing" in checks[
        "formal_database_import_command_contract_consumed"
    ]["reasons"]
    assert "formal_database_import_authorization_json_path_missing" in checks[
        "formal_database_import_authorization_bound"
    ]["reasons"]


def test_blocked_executor_writer_and_cli_refuse_execution(tmp_path: Path, capsys) -> None:
    contract_json, authorization_json, preflight_json, archive_json, bundle_json = _ready_command_contract(tmp_path)
    model = build_v1_5_formal_database_import_blocked_executor(
        formal_database_import_command_contract_json=contract_json,
        formal_database_import_authorization_json=authorization_json,
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        evidence_bundle_json=bundle_json,
    )
    outputs = write_v1_5_formal_database_import_blocked_executor_outputs(model, tmp_path / "executor")

    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["summary_csv"].exists()
    assert outputs["markdown"].exists()
    assert "does not connect PostgreSQL" in outputs["markdown"].read_text(encoding="utf-8")

    cli_out = tmp_path / "cli_executor"
    rc = cli_main(
        [
            "--formal-database-import-command-contract-json",
            str(contract_json),
            "--formal-database-import-authorization-json",
            str(authorization_json),
            "--formal-database-import-preflight-json",
            str(preflight_json),
            "--archive-closure-json",
            str(archive_json),
            "--evidence-bundle-json",
            str(bundle_json),
            "--output-dir",
            str(cli_out),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["overall_status"] == "blocked_pending_controlled_executor_implementation"
    assert payload["connects_postgresql"] is False
    assert payload["database_written"] is False
    assert (cli_out / "v1_5_formal_database_import_blocked_executor.json").exists()

    rc = cli_main(
        [
            "--formal-database-import-command-contract-json",
            str(contract_json),
            "--formal-database-import-authorization-json",
            str(authorization_json),
            "--formal-database-import-preflight-json",
            str(preflight_json),
            "--archive-closure-json",
            str(archive_json),
            "--evidence-bundle-json",
            str(bundle_json),
            "--output-dir",
            str(tmp_path / "cli_executor_fail"),
            "--fail-on-blocked",
        ]
    )
    assert rc == 2


def test_real_import_options_are_locked(tmp_path: Path, capsys) -> None:
    rc = cli_main(["--dsn", "postgresql://user:secret@localhost/db"])
    assert rc == 2
    assert "locked" in capsys.readouterr().err

    rc = cli_main(["--apply-migrations"])
    assert rc == 2
    assert "locked" in capsys.readouterr().err

    rc = cli_main(["--execute-controlled-import"])
    assert rc == 2
    assert "--execute-controlled-import" in capsys.readouterr().err

    rc = cli_main(
        [
            "--operator-confirmation-text",
            "I confirm this formal import",
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-a",
            "--authorization-id",
            "auth-001",
        ]
    )
    assert rc == 2
    assert "authorization metadata" in capsys.readouterr().err
