import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_database_import_preflight import main as cli_main
from gas_calibrator.validation.v1_5_formal_database_dry_run import (
    build_v1_5_formal_database_dry_run_contract,
    write_v1_5_formal_database_dry_run_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_import_preflight import (
    build_v1_5_formal_database_import_preflight,
    write_v1_5_formal_database_import_preflight_outputs,
)


def _dry_run_json(tmp_path: Path, *, postgresql_major: int = 18) -> Path:
    model = build_v1_5_formal_database_dry_run_contract(required_postgresql_major=postgresql_major)
    outputs = write_v1_5_formal_database_dry_run_outputs(model, tmp_path / "dry_run")
    return outputs["json"]


def _check(model: dict, name: str) -> dict:
    return {row["check"]: row for row in model["checks"]}[name]


def test_formal_database_import_preflight_ready_with_dsn_without_connecting(tmp_path: Path) -> None:
    dry_run_json = _dry_run_json(tmp_path)

    model = build_v1_5_formal_database_import_preflight(
        formal_database_dry_run_json=dry_run_json,
        dsn="postgresql://user:secret@localhost:5432/v15",
    )

    assert model["schema"] == "v1_5_formal_database_import_preflight_v1"
    assert model["overall_status"] == "ready_for_authorized_postgresql18_import_review"
    assert model["blocker_count"] == 0
    assert model["review_required_count"] == 0
    assert model["production_backend"] == "postgresql"
    assert model["production_postgresql_major"] == 18
    assert model["dsn_configured"] is True
    assert model["dsn_source"] == "explicit_argument"
    assert model["dsn_fingerprint"]
    assert model["connects_postgresql"] is False
    assert model["applies_migrations"] is False
    assert model["database_import_attempted"] is False
    assert model["database_written"] is False
    assert model["database_import_allowed"] is False
    assert model["formal_release_allowed"] is False
    assert model["not_real_acceptance_evidence"] is True
    assert model["required_authorizations"] == [
        "formal_archive_release",
        "database_import_authorization",
        "reviewer_approval",
    ]
    assert _check(model, "formal_database_dry_run_contract_ready")["status"] == "ready"
    assert _check(model, "postgresql_dsn_configuration_preview")["status"] == "ready"
    assert _check(model, "migration_execution_lock")["status"] == "ready"
    assert _check(model, "database_import_execution_lock")["status"] == "ready"


def test_formal_database_import_preflight_marks_missing_dsn_as_review_required(tmp_path: Path, monkeypatch) -> None:
    dry_run_json = _dry_run_json(tmp_path)
    monkeypatch.delenv("V1_5_POSTGRES_DSN", raising=False)

    model = build_v1_5_formal_database_import_preflight(
        formal_database_dry_run_json=dry_run_json,
        dsn_env="V1_5_POSTGRES_DSN",
    )

    assert model["overall_status"] == "review_required"
    assert model["blocker_count"] == 0
    assert model["review_required_count"] == 1
    assert model["dsn_configured"] is False
    assert model["database_import_allowed"] is False
    assert _check(model, "postgresql_dsn_configuration_preview")["status"] == "review_required"
    assert "dsn_missing" in _check(model, "postgresql_dsn_configuration_preview")["reasons"]


def test_formal_database_import_preflight_blocks_missing_or_bad_dry_run(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"

    model = build_v1_5_formal_database_import_preflight(formal_database_dry_run_json=missing, dsn="postgresql://x")

    assert model["overall_status"] == "blocked"
    assert _check(model, "formal_database_dry_run_contract_ready")["status"] == "blocker"
    assert "formal_database_dry_run_missing" in _check(model, "formal_database_dry_run_contract_ready")["reasons"]

    bad_dry_run_json = _dry_run_json(tmp_path / "bad", postgresql_major=17)
    bad_model = build_v1_5_formal_database_import_preflight(
        formal_database_dry_run_json=bad_dry_run_json,
        dsn="postgresql://x",
    )

    assert bad_model["overall_status"] == "blocked"
    assert _check(bad_model, "formal_database_dry_run_contract_ready")["status"] == "blocker"


def test_formal_database_import_preflight_writer_and_cli(tmp_path: Path) -> None:
    dry_run_json = _dry_run_json(tmp_path)
    model = build_v1_5_formal_database_import_preflight(
        formal_database_dry_run_json=dry_run_json,
        dsn="postgresql://user:secret@localhost:5432/v15",
    )
    outputs = write_v1_5_formal_database_import_preflight_outputs(model, tmp_path / "preflight")

    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["summary_csv"].exists()
    assert outputs["markdown"].exists()
    assert "does not connect PostgreSQL" in outputs["markdown"].read_text(encoding="utf-8")
    payload = json.loads(outputs["json"].read_text(encoding="utf-8-sig"))
    assert payload["database_import_allowed"] is False

    cli_out = tmp_path / "cli_preflight"
    rc = cli_main(
        [
            "--formal-database-dry-run-json",
            str(dry_run_json),
            "--dsn",
            "postgresql://user:secret@localhost:5432/v15",
            "--output-dir",
            str(cli_out),
            "--fail-on-blocker",
            "--fail-on-review-required",
        ]
    )

    assert rc == 0
    assert (cli_out / "v1_5_formal_database_import_preflight.json").exists()
