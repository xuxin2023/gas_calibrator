"""Controlled PostgreSQL 18 execution for V1.5 migration 002 only.

The public CLI keeps this transaction behind an immutable DBA readiness packet
and fresh three-party authorization.  This module never imports calibration
evidence and never interacts with analyzers or physical routes.
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable, Mapping

from sqlalchemy.engine import make_url

from .production_import import ProductionImportError, validate_production_dsn
from .schema import load_migrations


MIGRATION_001 = "001_v1_5_evidence_registry"
MIGRATION_002 = "002_v1_5_production_import_ledger"
EVIDENCE_SCHEMA = "v1_5_evidence"
LEDGER_TABLE = "production_import_ledger"
LEDGER_COLUMNS = (
    "run_db_id",
    "run_id",
    "evidence_bundle_sha256",
    "transaction_plan_sha256",
    "promotion_preflight_sha256",
    "execution_authorization_sha256",
    "authorization_id",
    "operator_name",
    "reviewer_name",
    "approver_name",
    "table_counts",
    "committed_at",
)
EXPECTED_CONSTRAINTS = {
    "PRIMARY KEY (run_db_id)",
    "UNIQUE (run_id)",
    "UNIQUE (authorization_id)",
    "FOREIGN KEY (run_db_id) REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE",
}
LEDGER_INDEX = "ix_v1_5_evidence_production_import_ledger_run_id"
SCRIPT_ROLES = ("precheck_sql", "apply_sql", "postcheck_sql")


class ProductionMigrationError(ProductionImportError):
    """Raised when migration 002 cannot be executed or verified safely."""


def validate_production_migration_dsn(dsn: str) -> str:
    """Validate the fixed target and return a Psycopg/libpq-compatible URI."""

    try:
        sqlalchemy_dsn = validate_production_dsn(dsn)
    except ProductionImportError as exc:
        raise ProductionMigrationError(str(exc)) from exc
    url = make_url(sqlalchemy_dsn)
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def _migration_checksums() -> dict[str, str]:
    return {row.version: row.checksum for row in load_migrations()}


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _server_sql(value: str) -> str:
    """Remove psql-only meta commands before sending SQL through psycopg."""

    return "\n".join(
        line for line in value.splitlines() if not line.lstrip().startswith("\\")
    ).strip()


def _transaction_body(value: str) -> str:
    lines = _server_sql(value).splitlines()
    begin_rows = [index for index, line in enumerate(lines) if line.strip().upper() == "BEGIN;"]
    commit_rows = [
        index for index, line in enumerate(lines) if line.strip().upper() == "COMMIT;"
    ]
    if len(begin_rows) != 1 or len(commit_rows) != 1:
        raise ProductionMigrationError(
            "apply_sql_exact_begin_commit_wrapper_required"
        )
    begin_index = begin_rows[0]
    commit_index = commit_rows[0]
    if begin_index >= commit_index:
        raise ProductionMigrationError("apply_sql_transaction_wrapper_order_invalid")
    trailing_statements = [
        line
        for line in lines[commit_index + 1 :]
        if line.strip() and not line.lstrip().startswith("--")
    ]
    if trailing_statements:
        raise ProductionMigrationError("apply_sql_statements_after_commit_forbidden")
    body = "\n".join(lines[:begin_index] + lines[begin_index + 1 : commit_index])
    if not body.strip():
        raise ProductionMigrationError("apply_sql_transaction_body_empty")
    return body


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _execute_script(connection: Any, script: str) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    sql = _server_sql(script)
    if not sql:
        raise ProductionMigrationError("migration_script_empty")
    with connection.cursor() as cursor:
        cursor.execute(sql, prepare=False)
        result_index = 0
        while True:
            if cursor.description:
                columns = [str(column.name) for column in cursor.description]
                rows = [
                    {column: _json_value(value) for column, value in zip(columns, row)}
                    for row in cursor.fetchall()
                ]
                outputs.append(
                    {"result_index": result_index, "columns": columns, "rows": rows}
                )
                result_index += 1
            if not cursor.nextset():
                break
    return outputs


def _execute_apply_transaction(
    connection: Any, script: str
) -> list[dict[str, Any]]:
    body = _transaction_body(script)
    with connection.transaction():
        return _execute_script(connection, body)


def _fetch_runtime_state(connection: Any) -> dict[str, Any]:
    checksums = _migration_checksums()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT current_database(),
                   current_setting('server_version_num')::integer,
                   (SELECT system_identifier::text FROM pg_control_system()),
                   (SELECT checksum FROM v1_5_evidence.schema_migrations
                    WHERE version = %s),
                   (SELECT checksum FROM v1_5_evidence.schema_migrations
                    WHERE version = %s),
                   to_regclass('v1_5_evidence.production_import_ledger')::text
            """,
            (MIGRATION_001, MIGRATION_002),
        )
        (
            database_name,
            server_version,
            system_identifier,
            checksum_001,
            checksum_002,
            ledger_regclass,
        ) = cursor.fetchone()

        columns: list[str] = []
        constraints: list[str] = []
        index_names: list[str] = []
        if ledger_regclass:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (EVIDENCE_SCHEMA, LEDGER_TABLE),
            )
            columns = [str(row[0]) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass
                ORDER BY contype, conname
                """
            )
            constraints = [str(row[0]) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s
                ORDER BY indexname
                """,
                (EVIDENCE_SCHEMA, LEDGER_TABLE),
            )
            index_names = [str(row[0]) for row in cursor.fetchall()]

    return {
        "database_name": str(database_name or ""),
        "postgresql_server_version_num": int(server_version),
        "postgresql_system_identifier": str(system_identifier or ""),
        "migration_001_checksum": str(checksum_001 or ""),
        "migration_002_checksum": str(checksum_002 or ""),
        "expected_migration_001_checksum": checksums.get(MIGRATION_001, ""),
        "expected_migration_002_checksum": checksums.get(MIGRATION_002, ""),
        "ledger_table_present": bool(ledger_regclass),
        "ledger_columns": columns,
        "ledger_constraints": constraints,
        "ledger_indexes": index_names,
    }


def migration_state_reasons(
    state: Mapping[str, Any], *, require_migration_002: bool
) -> list[str]:
    """Return fixed-target and schema reasons for a pre/post migration state."""

    reasons: list[str] = []
    if state.get("database_name") != "gas_calibrator":
        reasons.append("migration_database_name_must_be_gas_calibrator")
    server_version = int(state.get("postgresql_server_version_num") or 0)
    if not 180000 <= server_version < 190000:
        reasons.append("migration_postgresql_major_must_be_18")
    system_identifier = str(state.get("postgresql_system_identifier") or "")
    if not system_identifier.isdigit():
        reasons.append("migration_postgresql_system_identifier_invalid")
    if state.get("migration_001_checksum") != state.get(
        "expected_migration_001_checksum"
    ):
        reasons.append("migration_001_missing_or_checksum_mismatch")

    checksum_002 = str(state.get("migration_002_checksum") or "")
    expected_002 = str(state.get("expected_migration_002_checksum") or "")
    table_present = state.get("ledger_table_present") is True
    if checksum_002 and checksum_002 != expected_002:
        reasons.append("migration_002_checksum_mismatch")
    if bool(checksum_002 == expected_002) != table_present:
        reasons.append("migration_002_ledger_table_state_mismatch")
    if require_migration_002 and checksum_002 != expected_002:
        reasons.append("migration_002_not_applied")

    if table_present:
        if tuple(state.get("ledger_columns") or ()) != LEDGER_COLUMNS:
            reasons.append("production_import_ledger_columns_mismatch")
        if not EXPECTED_CONSTRAINTS.issubset(
            set(str(value) for value in state.get("ledger_constraints") or ())
        ):
            reasons.append("production_import_ledger_constraints_mismatch")
        if LEDGER_INDEX not in set(str(value) for value in state.get("ledger_indexes") or ()):
            reasons.append("production_import_ledger_index_missing")
    return reasons


def _default_connect(dsn: str) -> Any:
    import psycopg

    return psycopg.connect(dsn, autocommit=True)


def execute_production_migration_002(
    *,
    dsn: str,
    scripts: Mapping[str, str],
    expected_script_sha256: Mapping[str, str],
    readiness_sha256: str,
    execution_authorization_sha256: str,
    authorization_id: str,
    operator: str,
    reviewer: str,
    approver: str,
    connect: Callable[[str], Any] = _default_connect,
) -> dict[str, Any]:
    """Run precheck, migration 002 and postcheck with conservative hold states."""

    safe_dsn = validate_production_migration_dsn(dsn)
    actors = [
        str(value or "").strip()
        for value in (authorization_id, operator, reviewer, approver)
    ]
    if not all(actors):
        raise ProductionMigrationError(
            "authorization_id_operator_reviewer_approver_required"
        )
    if len({value.casefold() for value in actors[1:]}) != 3:
        raise ProductionMigrationError("operator_reviewer_approver_must_be_distinct")
    for role in SCRIPT_ROLES:
        script = str(scripts.get(role) or "")
        if not script:
            raise ProductionMigrationError(f"{role}_missing")
        if _sha256_text(script) != str(expected_script_sha256.get(role) or ""):
            raise ProductionMigrationError(f"{role}_sha256_mismatch")

    connection = None
    connection_attempted = False
    transaction_started = False
    commit_attempted = False
    apply_committed = False
    precheck_output: list[dict[str, Any]] = []
    apply_output: list[dict[str, Any]] = []
    postcheck_output: list[dict[str, Any]] = []
    pre_state: dict[str, Any] = {}
    post_state: dict[str, Any] = {}
    try:
        connection_attempted = True
        connection = connect(safe_dsn)
        pre_state = _fetch_runtime_state(connection)
        pre_reasons = migration_state_reasons(pre_state, require_migration_002=False)
        if pre_reasons:
            return {
                "status": "production_migration_002_precheck_hold",
                "connection_attempted": True,
                "transaction_started": False,
                "transaction_committed": False,
                "commit_uncertain": False,
                "rollback_attempted": False,
                "rollback_confirmed": False,
                "migration_execution_confirmed": False,
                "database_written": False,
                "database_write_state": "not_started",
                "precheck_state": pre_state,
                "precheck_reasons": pre_reasons,
                "failure_reason": ";".join(pre_reasons),
            }

        precheck_output = _execute_script(connection, scripts["precheck_sql"])
        previously_applied = (
            pre_state.get("migration_002_checksum")
            == pre_state.get("expected_migration_002_checksum")
        )
        transaction_started = True
        commit_attempted = True
        apply_output = _execute_apply_transaction(connection, scripts["apply_sql"])
        apply_committed = True
        try:
            postcheck_output = _execute_script(connection, scripts["postcheck_sql"])
            post_state = _fetch_runtime_state(connection)
        except Exception as exc:
            return {
                "status": "production_migration_002_postcheck_hold",
                "connection_attempted": True,
                "transaction_started": True,
                "transaction_committed": True,
                "commit_attempted": True,
                "commit_uncertain": False,
                "rollback_attempted": False,
                "rollback_confirmed": False,
                "idempotent": previously_applied,
                "migration_execution_confirmed": False,
                "database_written": False if previously_applied else True,
                "database_write_state": (
                    "idempotent_postcheck_failed"
                    if previously_applied
                    else "committed_postcheck_failed"
                ),
                "precheck_state": pre_state,
                "postcheck_state": post_state,
                "precheck_output": precheck_output,
                "apply_output": apply_output,
                "postcheck_output": postcheck_output,
                "failure_type": type(exc).__name__,
                "failure_reason": str(exc),
            }
        post_reasons = migration_state_reasons(post_state, require_migration_002=True)
        if post_state.get("postgresql_system_identifier") != pre_state.get(
            "postgresql_system_identifier"
        ):
            post_reasons.append("migration_postgresql_system_identifier_changed")
        confirmed = not post_reasons
        return {
            "status": (
                "production_migration_002_idempotent_noop"
                if confirmed and previously_applied
                else "production_migration_002_committed"
                if confirmed
                else "production_migration_002_postcheck_hold"
            ),
            "connection_attempted": True,
            "transaction_started": True,
            "transaction_committed": True,
            "commit_attempted": True,
            "commit_uncertain": False,
            "rollback_attempted": False,
            "rollback_confirmed": False,
            "idempotent": previously_applied,
            "migration_execution_confirmed": confirmed,
            "database_written": False if previously_applied else True,
            "database_write_state": (
                "idempotent_noop"
                if previously_applied
                else "committed"
            ),
            "precheck_state": pre_state,
            "postcheck_state": post_state,
            "precheck_output": precheck_output,
            "apply_output": apply_output,
            "postcheck_output": postcheck_output,
            "postcheck_reasons": post_reasons,
            "failure_reason": ";".join(post_reasons),
            "readiness_sha256": readiness_sha256,
            "execution_authorization_sha256": execution_authorization_sha256,
        }
    except Exception as exc:
        rollback_attempted = False
        rollback_confirmed = False
        rollback_error = ""
        if connection is not None and transaction_started and not apply_committed:
            rollback_attempted = True
            try:
                connection.rollback()
                rollback_confirmed = True
            except Exception as rollback_exc:  # pragma: no cover - connection loss
                rollback_error = f"{type(rollback_exc).__name__}:{rollback_exc}"
        commit_uncertain = transaction_started and not apply_committed and not rollback_confirmed
        return {
            "status": (
                "production_migration_002_commit_uncertain_hold"
                if commit_uncertain
                else "production_migration_002_rolled_back"
                if transaction_started
                else "production_migration_002_not_started"
            ),
            "connection_attempted": connection_attempted,
            "transaction_started": transaction_started,
            "transaction_committed": False,
            "commit_attempted": commit_attempted,
            "commit_uncertain": commit_uncertain,
            "rollback_attempted": rollback_attempted,
            "rollback_confirmed": rollback_confirmed,
            "rollback_error": rollback_error,
            "migration_execution_confirmed": False,
            "database_written": None if commit_uncertain else False,
            "database_write_state": (
                "unknown_commit_uncertain"
                if commit_uncertain
                else "rolled_back"
                if rollback_confirmed
                else "not_started"
            ),
            "precheck_state": pre_state,
            "postcheck_state": post_state,
            "precheck_output": precheck_output,
            "apply_output": apply_output,
            "postcheck_output": postcheck_output,
            "failure_type": type(exc).__name__,
            "failure_reason": str(exc),
        }
    finally:
        if connection is not None:
            connection.close()
