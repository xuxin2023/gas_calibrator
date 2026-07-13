from __future__ import annotations

import hashlib
import json
from pathlib import Path

from gas_calibrator.storage.v1_5_evidence.schema import Migration, load_migrations
from gas_calibrator.tools import export_v1_5_formal_database_migration_dba_readiness as cli
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_database_migration_dba_readiness import (
    EXPECTED_VERSIONS,
    READY_STATUS,
    build_v1_5_formal_database_migration_dba_readiness,
    write_v1_5_formal_database_migration_dba_readiness_outputs,
)


def _migration(version: str, sql: str) -> Migration:
    return Migration(
        version=version,
        path=Path(f"{version}.sql"),
        sql=sql,
        checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
    )


def test_dba_packet_is_ready_but_never_connects_or_executes() -> None:
    model = build_v1_5_formal_database_migration_dba_readiness()

    assert model["overall_status"] == READY_STATUS
    assert model["blocker_count"] == 0
    assert model["review_required_count"] == 1
    assert model["dba_packet_ready"] is True
    assert [row["version"] for row in model["migrations"]] == list(EXPECTED_VERSIONS)
    assert all(
        row["source_path"].startswith("src/gas_calibrator/")
        for row in model["migrations"]
    )
    assert all("_worktrees" not in row["source_path"] for row in model["migrations"])
    assert model["production_target"] == {
        "backend": "postgresql",
        "postgresql_major": 18,
        "database_name": "gas_calibrator",
        "core_schema": "public",
        "evidence_schema": "v1_5_evidence",
        "dsn_env": "V1_5_POSTGRES_DSN",
    }
    for field in (
        "connects_postgresql",
        "dsn_value_read",
        "applies_migrations",
        "migration_execution_allowed",
        "production_import_execution_allowed",
        "database_written",
        "database_import_allowed",
        "formal_release_allowed",
        "opens_com_ports",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "controls_pressure",
        "controls_water_or_gas_routes",
    ):
        assert model[field] is False


def test_apply_script_is_fixed_transactional_and_checksum_guarded() -> None:
    model = build_v1_5_formal_database_migration_dba_readiness()
    migrations = {row.version: row for row in load_migrations()}
    sql = model["scripts"]["apply_sql"]

    assert sql.startswith("\\set ON_ERROR_STOP on\n")
    assert "BEGIN;" in sql
    assert "COMMIT;" in sql
    assert "SET LOCAL lock_timeout" in sql
    assert "current_database() <> 'gas_calibrator'" in sql
    assert "server_version < 180000 OR server_version >= 190000" in sql
    assert migrations[EXPECTED_VERSIONS[0]].checksum in sql
    assert migrations[EXPECTED_VERSIONS[1]].checksum in sql
    assert migrations[EXPECTED_VERSIONS[1]].sql.rstrip() in sql
    assert "ON CONFLICT (version) DO NOTHING" in sql
    assert "migration 002 ledger/table state mismatch" in sql
    assert "production_import_ledger columns mismatch" in sql
    assert "run_db_id primary key missing" in sql
    assert "run_id unique constraint missing" in sql
    assert "authorization_id unique constraint missing" in sql
    assert "run_db_id foreign key missing" in sql
    assert "run_id index missing" in sql
    assert model["script_sha256"]["apply_sql"] == hashlib.sha256(
        sql.encode("utf-8")
    ).hexdigest()
    assert "DROP " not in sql.upper()
    assert "TRUNCATE " not in sql.upper()


def test_missing_reordered_or_destructive_migration_blocks_packet() -> None:
    migrations = load_migrations()

    missing = build_v1_5_formal_database_migration_dba_readiness(
        migrations=migrations[:1]
    )
    assert missing["overall_status"] == "blocked"
    assert missing["dba_packet_ready"] is False
    assert missing["scripts"]["apply_sql"] == ""

    reordered = build_v1_5_formal_database_migration_dba_readiness(
        migrations=tuple(reversed(migrations))
    )
    assert reordered["overall_status"] == "blocked"
    assert any("migration_sequence_mismatch" in reason for reason in reordered["reasons"])

    destructive_sql = migrations[1].sql + "\nDROP TABLE v1_5_evidence.runs;\n"
    destructive = build_v1_5_formal_database_migration_dba_readiness(
        migrations=(migrations[0], _migration(EXPECTED_VERSIONS[1], destructive_sql))
    )
    assert destructive["overall_status"] == "blocked"
    assert any("destructive_sql_forbidden:DROP" in reason for reason in destructive["reasons"])


def test_writer_emits_reviewable_sql_and_evidence(tmp_path: Path) -> None:
    model = build_v1_5_formal_database_migration_dba_readiness()
    outputs = write_v1_5_formal_database_migration_dba_readiness_outputs(
        model, tmp_path
    )

    assert set(outputs) == {
        "json",
        "checks",
        "migrations",
        "precheck_sql",
        "apply_sql",
        "postcheck_sql",
        "execution_record_template",
        "summary",
    }
    assert all(path.is_file() for path in outputs.values())
    assert "SELECT current_database()" in outputs["precheck_sql"].read_text(
        encoding="utf-8"
    )
    assert "BEGIN;" in outputs["apply_sql"].read_text(encoding="utf-8")
    for role in ("precheck_sql", "apply_sql", "postcheck_sql"):
        assert hashlib.sha256(outputs[role].read_bytes()).hexdigest() == model[
            "script_sha256"
        ][role]
    execution_record = json.loads(
        outputs["execution_record_template"].read_text(encoding="utf-8")
    )
    assert execution_record["template_only"] is True
    assert execution_record["not_execution_evidence"] is True
    assert execution_record["migration_execution_confirmed"] is False
    assert execution_record["production_import_authorized"] is False
    assert "pg_get_constraintdef" in outputs["postcheck_sql"].read_text(
        encoding="utf-8"
    )


def test_cli_rejects_execution_inputs_before_creating_outputs(tmp_path: Path) -> None:
    output = tmp_path / "output"
    assert (
        cli.main(
            [
                "--output-dir",
                str(output),
                "--dsn",
                "postgresql://forbidden/gas_calibrator",
                "--apply-migrations",
            ]
        )
        == 2
    )
    assert not output.exists()


def test_cli_and_entrypoint_are_offline_review_only(tmp_path: Path) -> None:
    output = tmp_path / "output"
    assert cli.main(["--output-dir", str(output), "--fail-on-blocker"]) == 0

    root = tmp_path / "inventory"
    entrypoint = (
        root
        / "src/gas_calibrator/tools/export_v1_5_formal_database_migration_dba_readiness.py"
    )
    entrypoint.parent.mkdir(parents=True)
    entrypoint.write_text("", encoding="utf-8")
    entry = classify_v1_5_entrypoint(entrypoint, root=root)
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False


def test_exporter_source_has_no_database_driver_or_environment_read() -> None:
    paths = [
        Path(
            "src/gas_calibrator/validation/"
            "v1_5_formal_database_migration_dba_readiness.py"
        ),
        Path(
            "src/gas_calibrator/tools/"
            "export_v1_5_formal_database_migration_dba_readiness.py"
        ),
    ]
    source = "\n".join(path.read_text(encoding="utf-8") for path in paths)
    for forbidden in (
        "create_engine",
        "psycopg.connect",
        "os.environ",
        "apply_migrations(",
        "serial.Serial",
    ):
        assert forbidden not in source


def test_committed_sql_artifacts_keep_lf_for_stable_sha256() -> None:
    attributes = Path(
        "docs/v1_5_flow_contract/formal_database_migration_dba_readiness/.gitattributes"
    ).read_text(encoding="ascii")

    assert attributes.strip() == "*.sql text eol=lf"
