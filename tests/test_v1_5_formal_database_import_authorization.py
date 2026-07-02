import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_database_import_authorization import main as cli_main
from gas_calibrator.validation.v1_5_formal_database_dry_run import (
    build_v1_5_formal_database_dry_run_contract,
    write_v1_5_formal_database_dry_run_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_authorization import (
    build_v1_5_formal_database_import_authorization,
    write_v1_5_formal_database_import_authorization_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_preflight import (
    build_v1_5_formal_database_import_preflight,
    write_v1_5_formal_database_import_preflight_outputs,
)


def _preflight_json(tmp_path: Path, *, dsn: str = "postgresql://user:secret@localhost:5432/v15") -> Path:
    dry_run_model = build_v1_5_formal_database_dry_run_contract()
    dry_run_outputs = write_v1_5_formal_database_dry_run_outputs(dry_run_model, tmp_path / "dry_run")
    preflight_model = build_v1_5_formal_database_import_preflight(
        formal_database_dry_run_json=dry_run_outputs["json"],
        dsn=dsn,
    )
    preflight_outputs = write_v1_5_formal_database_import_preflight_outputs(
        preflight_model,
        tmp_path / "preflight",
    )
    return preflight_outputs["json"]


def _archive_json(tmp_path: Path, *, ready: bool = True) -> Path:
    payload = {
        "schema": "v1_5_formal_archive_closure_v1",
        "overall_status": "ready" if ready else "review_required",
        "package_status": "ready" if ready else "review_required",
        "identity_getco_traceability": {
            "status": "ready" if ready else "review_required",
            "ready_for_archive_release": ready,
            "traceability_review_required": not ready,
        },
    }
    path = tmp_path / "archive" / "v1_5_formal_archive_closure_index.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _check(model: dict, name: str) -> dict:
    return {row["check"]: row for row in model["checks"]}[name]


def test_formal_database_import_authorization_ready_without_connecting(tmp_path: Path) -> None:
    preflight_json = _preflight_json(tmp_path)
    archive_json = _archive_json(tmp_path)

    model = build_v1_5_formal_database_import_authorization(
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        operator="operator-a",
        reviewer="reviewer-a",
        approver="approver-a",
        authorization_id="db-import-001",
    )

    assert model["schema"] == "v1_5_formal_database_import_authorization_v1"
    assert model["overall_status"] == "ready_for_manual_postgresql18_import_authorization"
    assert model["blocker_count"] == 0
    assert model["review_required_count"] == 0
    assert model["preflight_ready"] is True
    assert model["archive_release_ready"] is True
    assert model["manual_authorization_ready"] is True
    assert model["database_import_allowed"] is True
    assert model["connects_postgresql"] is False
    assert model["applies_migrations"] is False
    assert model["database_import_attempted"] is False
    assert model["database_written"] is False
    assert model["not_real_acceptance_evidence"] is True
    assert _check(model, "formal_database_import_preflight_ready")["status"] == "ready"
    assert _check(model, "formal_archive_release_ready")["status"] == "ready"
    assert _check(model, "manual_database_import_authorization_record")["status"] == "ready"


def test_formal_database_import_authorization_review_when_archive_or_labels_missing(tmp_path: Path) -> None:
    preflight_json = _preflight_json(tmp_path)

    model = build_v1_5_formal_database_import_authorization(
        formal_database_import_preflight_json=preflight_json,
        operator="",
        reviewer="reviewer-a",
        approver="",
        authorization_id="",
    )

    assert model["overall_status"] == "review_required"
    assert model["blocker_count"] == 0
    assert model["review_required_count"] == 2
    assert model["preflight_ready"] is True
    assert model["archive_release_ready"] is False
    assert model["manual_authorization_ready"] is False
    assert model["database_import_allowed"] is False
    assert _check(model, "formal_archive_release_ready")["status"] == "review_required"
    assert "archive_closure_missing" in _check(model, "formal_archive_release_ready")["reasons"]
    auth_reasons = _check(model, "manual_database_import_authorization_record")["reasons"]
    assert "operator_missing" in auth_reasons
    assert "approver_missing" in auth_reasons
    assert "authorization_id_missing" in auth_reasons


def test_formal_database_import_authorization_blocks_bad_preflight(tmp_path: Path) -> None:
    missing = tmp_path / "missing_preflight.json"
    archive_json = _archive_json(tmp_path)

    model = build_v1_5_formal_database_import_authorization(
        formal_database_import_preflight_json=missing,
        archive_closure_json=archive_json,
        operator="operator-a",
        reviewer="reviewer-a",
        approver="approver-a",
        authorization_id="db-import-001",
    )

    assert model["overall_status"] == "blocked"
    assert model["blocker_count"] == 1
    assert model["database_import_allowed"] is False
    assert _check(model, "formal_database_import_preflight_ready")["status"] == "blocker"
    assert "formal_database_import_preflight_missing" in _check(
        model,
        "formal_database_import_preflight_ready",
    )["reasons"]


def test_formal_database_import_authorization_writer_and_cli(tmp_path: Path) -> None:
    preflight_json = _preflight_json(tmp_path)
    archive_json = _archive_json(tmp_path)
    model = build_v1_5_formal_database_import_authorization(
        formal_database_import_preflight_json=preflight_json,
        archive_closure_json=archive_json,
        operator="operator-a",
        reviewer="reviewer-a",
        approver="approver-a",
        authorization_id="db-import-001",
    )
    outputs = write_v1_5_formal_database_import_authorization_outputs(model, tmp_path / "authorization")

    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["summary_csv"].exists()
    assert outputs["markdown"].exists()
    assert "does not connect PostgreSQL" in outputs["markdown"].read_text(encoding="utf-8")
    payload = json.loads(outputs["json"].read_text(encoding="utf-8-sig"))
    assert payload["database_import_allowed"] is True

    cli_out = tmp_path / "cli_authorization"
    rc = cli_main(
        [
            "--formal-database-import-preflight-json",
            str(preflight_json),
            "--archive-closure-json",
            str(archive_json),
            "--operator",
            "operator-a",
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-a",
            "--authorization-id",
            "db-import-001",
            "--output-dir",
            str(cli_out),
            "--fail-on-blocker",
            "--fail-on-review-required",
        ]
    )

    assert rc == 0
    assert (cli_out / "v1_5_formal_database_import_authorization.json").exists()
