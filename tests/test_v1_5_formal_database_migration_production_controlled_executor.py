from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gas_calibrator.storage.v1_5_evidence import production_migration as storage
from gas_calibrator.storage.v1_5_evidence.production_migration import (
    EXPECTED_CONSTRAINTS,
    LEDGER_COLUMNS,
    LEDGER_INDEX,
    ProductionMigrationError,
    execute_production_migration_002,
    migration_state_reasons,
    validate_production_migration_dsn,
)
from gas_calibrator.tools import (
    run_v1_5_formal_database_migration_production_controlled_executor as cli,
)
from gas_calibrator.validation import (
    v1_5_formal_database_migration_production_controlled_executor as executor,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import (
    classify_v1_5_entrypoint,
)
from gas_calibrator.validation.v1_5_formal_database_migration_dba_readiness import (
    build_v1_5_formal_database_migration_dba_readiness,
    write_v1_5_formal_database_migration_dba_readiness_outputs,
)
from gas_calibrator.validation.v1_5_formal_database_migration_production_controlled_executor import (
    AUTHORIZATION_SCHEMA,
    CONFIRMATION_TEMPLATE,
    EXECUTE_FLAG,
    build_migration_execution_preview,
    execute_reviewed_production_migration,
    validate_migration_execution_authorization,
)


POSTGRESQL_SYSTEM_IDENTIFIER = "7345678901234567890"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _make_packet(tmp_path: Path) -> dict[str, Path]:
    readiness = build_v1_5_formal_database_migration_dba_readiness()
    outputs = write_v1_5_formal_database_migration_dba_readiness_outputs(
        readiness, tmp_path / "dba_packet"
    )
    return {
        "dba_readiness": outputs["json"],
        "precheck_sql": outputs["precheck_sql"],
        "apply_sql": outputs["apply_sql"],
        "postcheck_sql": outputs["postcheck_sql"],
    }


def _make_authorization(
    tmp_path: Path, paths: dict[str, Path], **overrides
) -> Path:
    now = datetime.now(timezone.utc)
    payload = {
        "schema": AUTHORIZATION_SCHEMA,
        "requested_flag": EXECUTE_FLAG,
        "confirmation_template": CONFIRMATION_TEMPLATE,
        "operator_confirmed": True,
        "authorization_id": "migration-auth-001",
        "operator": "migration-operator",
        "reviewer": "migration-reviewer",
        "approver": "migration-approver",
        "issued_at": (now - timedelta(minutes=1)).isoformat(),
        "expires_at": (now + timedelta(hours=1)).isoformat(),
        "production_target": {
            "backend": "postgresql",
            "postgresql_major": 18,
            "database_name": "gas_calibrator",
            "core_schema": "public",
            "evidence_schema": "v1_5_evidence",
            "dsn_env": "V1_5_POSTGRES_DSN",
        },
        "boundaries": {
            "applies_migration_002": True,
            "production_evidence_import": False,
            "opens_com_ports": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "grants_database_import": False,
            "grants_formal_release": False,
        },
        "source_bindings": [
            {"role": role, "path": str(path), "sha256": _sha256(path)}
            for role, path in paths.items()
        ],
    }
    payload.update(overrides)
    path = tmp_path / "migration_execution_authorization.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path.resolve()


def _preview(paths: dict[str, Path]) -> dict:
    return build_migration_execution_preview(
        dba_readiness_json=paths["dba_readiness"],
        precheck_sql=paths["precheck_sql"],
        apply_sql=paths["apply_sql"],
        postcheck_sql=paths["postcheck_sql"],
    )


def _good_state(*, applied: bool) -> dict:
    checksums = storage._migration_checksums()
    return {
        "database_name": "gas_calibrator",
        "postgresql_server_version_num": 180003,
        "postgresql_system_identifier": POSTGRESQL_SYSTEM_IDENTIFIER,
        "migration_001_checksum": checksums[storage.MIGRATION_001],
        "migration_002_checksum": checksums[storage.MIGRATION_002] if applied else "",
        "expected_migration_001_checksum": checksums[storage.MIGRATION_001],
        "expected_migration_002_checksum": checksums[storage.MIGRATION_002],
        "ledger_table_present": applied,
        "ledger_columns": list(LEDGER_COLUMNS) if applied else [],
        "ledger_constraints": sorted(EXPECTED_CONSTRAINTS) if applied else [],
        "ledger_indexes": [LEDGER_INDEX] if applied else [],
    }


def _runner_scripts() -> dict[str, str]:
    return {
        "precheck_sql": "SELECT 1;",
        "apply_sql": "BEGIN;\nSELECT 1;\nCOMMIT;\n",
        "postcheck_sql": "SELECT 1;",
    }


def test_preview_is_locked_and_binds_exact_dba_packet(tmp_path: Path) -> None:
    paths = _make_packet(tmp_path)
    model = _preview(paths)

    assert model["migration_execution_package_ready"] is True
    assert model["overall_status"] == (
        "ready_for_postgresql18_migration_execution_authorization_review"
    )
    assert model["dsn_value_read"] is False
    assert model["connects_postgresql"] is False
    assert model["applies_migrations"] is False
    assert model["database_import_allowed"] is False
    assert model["formal_release_allowed"] is False
    assert {row["role"] for row in model["source_bindings"]} == set(paths)


@pytest.mark.parametrize("role", ["precheck_sql", "apply_sql", "postcheck_sql"])
def test_preview_rejects_script_drift(tmp_path: Path, role: str) -> None:
    paths = _make_packet(tmp_path)
    paths[role].write_text(paths[role].read_text(encoding="utf-8") + "\n-- drift\n")

    model = _preview(paths)

    assert model["migration_execution_package_ready"] is False
    assert any(role in reason for reason in model["reasons"])


def test_authorization_requires_three_distinct_actors_and_fixed_boundaries(
    tmp_path: Path,
) -> None:
    paths = _make_packet(tmp_path)
    preview = _preview(paths)
    authorization = _make_authorization(
        tmp_path,
        paths,
        reviewer="same-person",
        approver="same-person",
    )

    with pytest.raises(
        ProductionMigrationError,
        match="three_distinct_actors_required",
    ):
        validate_migration_execution_authorization(
            execution_authorization_json=authorization,
            preview=preview,
            source_paths=paths,
        )


def test_authorization_binds_every_source_path_and_hash(tmp_path: Path) -> None:
    paths = _make_packet(tmp_path)
    preview = _preview(paths)
    authorization = _make_authorization(tmp_path, paths)

    record = validate_migration_execution_authorization(
        execution_authorization_json=authorization,
        preview=preview,
        source_paths=paths,
    )

    assert record["three_distinct_actors"] is True
    assert record["confirmation_matched"] is True
    assert set(record["source_sha256"]) == set(paths)


def test_execute_uses_immutable_script_snapshots_and_keeps_import_locked(
    tmp_path: Path,
) -> None:
    paths = _make_packet(tmp_path)
    authorization = _make_authorization(tmp_path, paths)
    calls: list[dict] = []

    def fake_runner(**kwargs):
        calls.append(kwargs)
        return {
            "status": "production_migration_002_committed",
            "connection_attempted": True,
            "transaction_started": True,
            "transaction_committed": True,
            "commit_uncertain": False,
            "migration_execution_confirmed": True,
            "database_written": True,
            "database_write_state": "committed",
            "precheck_output": [{"status": "pass"}],
            "apply_output": [{"status": "pass"}],
            "postcheck_output": [{"status": "pass"}],
        }

    model = execute_reviewed_production_migration(
        dba_readiness_json=paths["dba_readiness"],
        precheck_sql=paths["precheck_sql"],
        apply_sql=paths["apply_sql"],
        postcheck_sql=paths["postcheck_sql"],
        execution_authorization_json=authorization,
        dsn="postgresql+psycopg://user:secret@localhost/gas_calibrator",
        migration_runner=fake_runner,
    )

    assert len(calls) == 1
    assert "secret" not in json.dumps(model)
    assert calls[0]["scripts"]["apply_sql"].startswith("\\set ON_ERROR_STOP")
    assert model["migration_execution_confirmed"] is True
    assert model["database_import_allowed"] is False
    assert model["formal_release_allowed"] is False
    assert model["opens_com_ports"] is False


def test_source_change_after_authorization_stops_before_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _make_packet(tmp_path)
    authorization = _make_authorization(tmp_path, paths)
    original = executor.validate_migration_execution_authorization
    calls: list[str] = []

    def mutate_after_validation(**kwargs):
        result = original(**kwargs)
        paths["apply_sql"].write_text(
            paths["apply_sql"].read_text(encoding="utf-8") + "\n-- late drift\n",
            encoding="utf-8",
        )
        return result

    monkeypatch.setattr(
        executor, "validate_migration_execution_authorization", mutate_after_validation
    )

    with pytest.raises(ProductionMigrationError, match="changed_after_authorization"):
        execute_reviewed_production_migration(
            dba_readiness_json=paths["dba_readiness"],
            precheck_sql=paths["precheck_sql"],
            apply_sql=paths["apply_sql"],
            postcheck_sql=paths["postcheck_sql"],
            execution_authorization_json=authorization,
            dsn="postgresql+psycopg://user:secret@localhost/gas_calibrator",
            migration_runner=lambda **kwargs: calls.append("called"),
        )
    assert calls == []


def test_cli_default_and_invalid_authorization_never_read_dsn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _make_packet(tmp_path)
    invalid_authorization = _make_authorization(
        tmp_path,
        paths,
        reviewer="same-person",
        approver="same-person",
    )

    def forbidden_dsn_read() -> str:
        raise AssertionError("DSN must remain unread")

    monkeypatch.setattr(cli, "_read_production_dsn", forbidden_dsn_read)
    base_args = [
        "--dba-readiness-json",
        str(paths["dba_readiness"]),
        "--precheck-sql",
        str(paths["precheck_sql"]),
        "--apply-sql",
        str(paths["apply_sql"]),
        "--postcheck-sql",
        str(paths["postcheck_sql"]),
        "--output-dir",
        str(tmp_path / "preview"),
    ]
    assert cli.main(base_args) == 0
    assert cli.main(base_args + ["--execute-postgresql18-migration"]) == 2
    assert (
        cli.main(
            base_args
            + [
                "--execute-postgresql18-migration",
                "--execution-authorization-json",
                str(invalid_authorization),
            ]
        )
        == 2
    )
    template = json.loads(
        (tmp_path / "preview" / "v1_5_postgresql18_migration_execution_authorization_template.json").read_text(
            encoding="utf-8"
        )
    )
    assert template["template_only"] is True
    assert template["boundaries"]["production_evidence_import"] is False
    assert template["source_bindings"]


def test_cli_records_preconnect_hold_after_valid_authorization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _make_packet(tmp_path)
    authorization = _make_authorization(tmp_path, paths)
    output_dir = tmp_path / "preconnect_hold"
    monkeypatch.setattr(
        cli,
        "_read_production_dsn",
        lambda: "postgresql://user:secret@localhost/gas_calibrator",
    )
    monkeypatch.setattr(
        cli,
        "execute_reviewed_production_migration",
        lambda **kwargs: (_ for _ in ()).throw(
            ProductionMigrationError("source_changed_before_connection")
        ),
    )

    result = cli.main(
        [
            "--dba-readiness-json",
            str(paths["dba_readiness"]),
            "--precheck-sql",
            str(paths["precheck_sql"]),
            "--apply-sql",
            str(paths["apply_sql"]),
            "--postcheck-sql",
            str(paths["postcheck_sql"]),
            "--execution-authorization-json",
            str(authorization),
            "--output-dir",
            str(output_dir),
            "--execute-postgresql18-migration",
        ]
    )
    model = json.loads(
        (
            output_dir
            / "v1_5_formal_database_migration_production_controlled_executor.json"
        ).read_text(encoding="utf-8")
    )
    assert result == 2
    assert model["overall_status"] == "migration_execution_preconnect_hold"
    assert model["dsn_value_read"] is True
    assert model["execution_attempted"] is False
    assert model["connects_postgresql"] is False


def test_migration_state_checks_target_version_checksum_and_shape() -> None:
    pre_state = _good_state(applied=False)
    assert migration_state_reasons(pre_state, require_migration_002=False) == []

    wrong_database = {**pre_state, "database_name": "postgres"}
    assert "migration_database_name_must_be_gas_calibrator" in migration_state_reasons(
        wrong_database, require_migration_002=False
    )
    wrong_version = {**pre_state, "postgresql_server_version_num": 170000}
    assert "migration_postgresql_major_must_be_18" in migration_state_reasons(
        wrong_version, require_migration_002=False
    )
    missing_system_identifier = {**pre_state, "postgresql_system_identifier": ""}
    assert "migration_postgresql_system_identifier_invalid" in migration_state_reasons(
        missing_system_identifier, require_migration_002=False
    )
    split_state = {
        **pre_state,
        "migration_002_checksum": pre_state["expected_migration_002_checksum"],
    }
    assert "migration_002_ledger_table_state_mismatch" in migration_state_reasons(
        split_state, require_migration_002=False
    )

    post_state = _good_state(applied=True)
    assert migration_state_reasons(post_state, require_migration_002=True) == []
    bad_columns = {**post_state, "ledger_columns": list(LEDGER_COLUMNS[:-1])}
    assert "production_import_ledger_columns_mismatch" in migration_state_reasons(
        bad_columns, require_migration_002=True
    )
    bad_constraints = {**post_state, "ledger_constraints": []}
    assert "production_import_ledger_constraints_mismatch" in migration_state_reasons(
        bad_constraints, require_migration_002=True
    )


def test_migration_dsn_is_fixed_target_and_psycopg_compatible() -> None:
    safe = validate_production_migration_dsn(
        "postgresql://user:secret@localhost/gas_calibrator"
    )
    assert safe.startswith("postgresql://")
    assert "+psycopg" not in safe
    assert safe.endswith("/gas_calibrator")

    with pytest.raises(ProductionMigrationError, match="database_name"):
        validate_production_migration_dsn(
            "postgresql://user:secret@localhost/postgres"
        )


def test_generated_apply_sql_has_one_strict_transaction_body() -> None:
    readiness = build_v1_5_formal_database_migration_dba_readiness()
    apply_sql = readiness["scripts"]["apply_sql"]

    body = storage._transaction_body(apply_sql)

    assert "BEGIN;" not in body
    assert "COMMIT;" not in body
    assert "LOCK TABLE v1_5_evidence.schema_migrations" in body
    assert "CREATE TABLE IF NOT EXISTS v1_5_evidence.production_import_ledger" in body

    with pytest.raises(
        ProductionMigrationError, match="exact_begin_commit_wrapper_required"
    ):
        storage._transaction_body("SELECT 1;")


class _Connection:
    def __init__(self, *, rollback_fails: bool = False):
        self.rollback_fails = rollback_fails
        self.closed = False

    def rollback(self) -> None:
        if self.rollback_fails:
            raise RuntimeError("connection lost")

    def transaction(self):
        class _Transaction:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                return False

        return _Transaction()

    def close(self) -> None:
        self.closed = True


def test_storage_runner_rolls_back_apply_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection()
    states = [_good_state(applied=False)]
    calls = 0

    monkeypatch.setattr(storage, "_fetch_runtime_state", lambda connection: states[0])

    def fake_script(connection, script):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("apply failed")
        return []

    monkeypatch.setattr(storage, "_execute_script", fake_script)
    scripts = _runner_scripts()
    model = execute_production_migration_002(
        dsn="postgresql+psycopg://user:secret@localhost/gas_calibrator",
        scripts=scripts,
        expected_script_sha256={
            role: hashlib.sha256(value.encode()).hexdigest()
            for role, value in scripts.items()
        },
        readiness_sha256="a" * 64,
        execution_authorization_sha256="b" * 64,
        authorization_id="auth",
        operator="operator",
        reviewer="reviewer",
        approver="approver",
        connect=lambda dsn: connection,
    )
    assert model["status"] == "production_migration_002_rolled_back"
    assert model["rollback_confirmed"] is True
    assert model["commit_uncertain"] is False
    assert model["database_written"] is False


def test_storage_runner_holds_postcheck_failure_after_confirmed_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection()
    states = [_good_state(applied=False)]
    monkeypatch.setattr(storage, "_fetch_runtime_state", lambda connection: states[0])
    calls = 0

    def fake_script(connection, script):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("postcheck unavailable")
        return []

    monkeypatch.setattr(storage, "_execute_script", fake_script)
    scripts = _runner_scripts()
    model = execute_production_migration_002(
        dsn="postgresql+psycopg://user:secret@localhost/gas_calibrator",
        scripts=scripts,
        expected_script_sha256={
            role: hashlib.sha256(value.encode()).hexdigest()
            for role, value in scripts.items()
        },
        readiness_sha256="a" * 64,
        execution_authorization_sha256="b" * 64,
        authorization_id="auth",
        operator="operator",
        reviewer="reviewer",
        approver="approver",
        connect=lambda dsn: connection,
    )
    assert model["status"] == "production_migration_002_postcheck_hold"
    assert model["transaction_committed"] is True
    assert model["commit_uncertain"] is False
    assert model["migration_execution_confirmed"] is False
    assert model["database_write_state"] == "committed_postcheck_failed"


def test_storage_runner_rejects_script_hash_before_connection() -> None:
    calls: list[str] = []
    scripts = _runner_scripts()
    bad_hashes = {
        role: hashlib.sha256(value.encode()).hexdigest()
        for role, value in scripts.items()
    }
    bad_hashes["apply_sql"] = "0" * 64

    with pytest.raises(ProductionMigrationError, match="apply_sql_sha256_mismatch"):
        execute_production_migration_002(
            dsn="postgresql+psycopg://user:secret@localhost/gas_calibrator",
            scripts=scripts,
            expected_script_sha256=bad_hashes,
            readiness_sha256="a" * 64,
            execution_authorization_sha256="b" * 64,
            authorization_id="auth",
            operator="operator",
            reviewer="reviewer",
            approver="approver",
            connect=lambda dsn: calls.append(dsn),
        )
    assert calls == []


def test_storage_runner_reports_commit_uncertain_when_rollback_cannot_confirm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(rollback_fails=True)
    monkeypatch.setattr(
        storage, "_fetch_runtime_state", lambda connection: _good_state(applied=False)
    )
    calls = 0

    def fake_script(connection, script):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("connection lost")
        return []

    monkeypatch.setattr(storage, "_execute_script", fake_script)
    scripts = _runner_scripts()
    model = execute_production_migration_002(
        dsn="postgresql+psycopg://user:secret@localhost/gas_calibrator",
        scripts=scripts,
        expected_script_sha256={
            role: hashlib.sha256(value.encode()).hexdigest()
            for role, value in scripts.items()
        },
        readiness_sha256="a" * 64,
        execution_authorization_sha256="b" * 64,
        authorization_id="auth",
        operator="operator",
        reviewer="reviewer",
        approver="approver",
        connect=lambda dsn: connection,
    )
    assert model["status"] == "production_migration_002_commit_uncertain_hold"
    assert model["commit_uncertain"] is True
    assert model["database_written"] is None


def test_storage_runner_accepts_exact_idempotent_already_applied_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection()
    monkeypatch.setattr(
        storage, "_fetch_runtime_state", lambda connection: _good_state(applied=True)
    )
    monkeypatch.setattr(storage, "_execute_script", lambda connection, script: [])
    scripts = _runner_scripts()
    model = execute_production_migration_002(
        dsn="postgresql+psycopg://user:secret@localhost/gas_calibrator",
        scripts=scripts,
        expected_script_sha256={
            role: hashlib.sha256(value.encode()).hexdigest()
            for role, value in scripts.items()
        },
        readiness_sha256="a" * 64,
        execution_authorization_sha256="b" * 64,
        authorization_id="auth",
        operator="operator",
        reviewer="reviewer",
        approver="approver",
        connect=lambda dsn: connection,
    )
    assert model["status"] == "production_migration_002_idempotent_noop"
    assert model["idempotent"] is True
    assert model["migration_execution_confirmed"] is True
    assert model["database_written"] is False


def test_storage_runner_holds_if_postcheck_is_from_another_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection()
    pre_state = _good_state(applied=False)
    post_state = {
        **_good_state(applied=True),
        "postgresql_system_identifier": "7000000000000000001",
    }
    states = iter((pre_state, post_state))
    monkeypatch.setattr(storage, "_fetch_runtime_state", lambda connection: next(states))
    monkeypatch.setattr(storage, "_execute_script", lambda connection, script: [])
    scripts = _runner_scripts()

    model = execute_production_migration_002(
        dsn="postgresql+psycopg://user:secret@localhost/gas_calibrator",
        scripts=scripts,
        expected_script_sha256={
            role: hashlib.sha256(value.encode()).hexdigest()
            for role, value in scripts.items()
        },
        readiness_sha256="a" * 64,
        execution_authorization_sha256="b" * 64,
        authorization_id="auth",
        operator="operator",
        reviewer="reviewer",
        approver="approver",
        connect=lambda dsn: connection,
    )

    assert model["status"] == "production_migration_002_postcheck_hold"
    assert model["migration_execution_confirmed"] is False
    assert "migration_postgresql_system_identifier_changed" in model[
        "postcheck_reasons"
    ]


def test_entrypoint_is_manual_authorized_database_migration_only(tmp_path: Path) -> None:
    path = (
        tmp_path
        / "src/gas_calibrator/tools/"
        "run_v1_5_formal_database_migration_production_controlled_executor.py"
    )
    path.parent.mkdir(parents=True)
    path.write_text("# tool\n", encoding="utf-8")

    entry = classify_v1_5_entrypoint(path, root=tmp_path)

    assert entry.category == "evidence_database"
    assert entry.formal_status == (
        "manual_authorized_production_database_migration_only"
    )
    assert entry.risk_level == "production_database_migration_risk"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
