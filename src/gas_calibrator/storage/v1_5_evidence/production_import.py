"""Atomic PostgreSQL 18 production import for an authorized V1.5 evidence package.

This module contains the database transaction only.  The public CLI keeps it
behind an immutable promotion preflight and a fresh three-party execution
authorization.  It never creates schemas, applies migrations, or touches
analyzers and physical routes.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Mapping

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

from .bundle import TABLE_NAMES
from .schema import load_migrations
from .staging_import import (
    StagingImportError,
    StagingSchemas,
    _assert_postgresql18,
    _evidence_readback_counts,
    _expected_table_counts,
    _identity_readback,
    _upsert_core_identity,
    _upsert_evidence_rows,
    _sqlalchemy_dsn,
    validate_staging_package,
)


PRODUCTION_DATABASE_NAME = "gas_calibrator"
PRODUCTION_CORE_SCHEMA = "public"
PRODUCTION_EVIDENCE_SCHEMA = "v1_5_evidence"
PRODUCTION_LEDGER_TABLE = "production_import_ledger"
PRODUCTION_LEDGER_MIGRATION = "002_v1_5_production_import_ledger"
SHA256_RE = re.compile(r"[0-9a-f]{64}")


class ProductionImportError(StagingImportError):
    """Raised when the fixed production target or transaction is invalid."""


def validate_production_dsn(dsn: str) -> str:
    """Validate the fixed production database name without exposing secrets."""

    try:
        url = make_url(_sqlalchemy_dsn(dsn))
    except Exception as exc:
        raise ProductionImportError("production_dsn_invalid") from exc
    if not str(url.drivername).startswith("postgresql"):
        raise ProductionImportError("production_dsn_must_use_postgresql")
    if str(url.database or "").strip().lower() != PRODUCTION_DATABASE_NAME:
        raise ProductionImportError(
            f"production_database_name_must_be_{PRODUCTION_DATABASE_NAME}"
        )
    return url.render_as_string(hide_password=False)


def _require_sha256(value: str, role: str) -> str:
    normalized = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(normalized):
        raise ProductionImportError(f"{role}_sha256_invalid")
    return normalized


def _assert_production_schema_ready(connection: Any) -> None:
    inspector = inspect(connection)
    schemas = set(inspector.get_schema_names())
    if PRODUCTION_CORE_SCHEMA not in schemas or PRODUCTION_EVIDENCE_SCHEMA not in schemas:
        raise ProductionImportError("production_schema_missing_migration_required")

    required_core = {"runs", "sensors", "sensor_identity_aliases", "device_events"}
    required_evidence = set(TABLE_NAMES) | {
        "schema_migrations",
        PRODUCTION_LEDGER_TABLE,
    }
    core_tables = set(inspector.get_table_names(schema=PRODUCTION_CORE_SCHEMA))
    evidence_tables = set(inspector.get_table_names(schema=PRODUCTION_EVIDENCE_SCHEMA))
    missing_core = sorted(required_core - core_tables)
    missing_evidence = sorted(required_evidence - evidence_tables)
    if missing_core:
        raise ProductionImportError(
            "production_core_tables_missing:" + ",".join(missing_core)
        )
    if missing_evidence:
        raise ProductionImportError(
            "production_evidence_tables_missing:" + ",".join(missing_evidence)
        )

    migration = next(
        (row for row in load_migrations() if row.version == PRODUCTION_LEDGER_MIGRATION),
        None,
    )
    if migration is None:
        raise ProductionImportError("production_ledger_migration_definition_missing")
    stored_checksum = connection.execute(
        text(
            f'SELECT checksum FROM "{PRODUCTION_EVIDENCE_SCHEMA}".schema_migrations '
            "WHERE version=:version"
        ),
        {"version": PRODUCTION_LEDGER_MIGRATION},
    ).scalar_one_or_none()
    if str(stored_checksum or "") != migration.checksum:
        raise ProductionImportError("production_ledger_migration_not_applied_or_changed")


def execute_production_import(
    *,
    dsn: str,
    transaction_plan: Mapping[str, Any],
    evidence_bundle: Mapping[str, Any],
    transaction_plan_sha256: str,
    evidence_bundle_sha256: str,
    promotion_preflight_sha256: str,
    execution_authorization_sha256: str,
    authorization_id: str,
    operator: str,
    reviewer: str,
    approver: str,
    failure_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Execute one fixed-target atomic import with readback and rollback evidence."""

    devices = validate_staging_package(transaction_plan, evidence_bundle)
    identities = [
        str(value or "").strip()
        for value in (authorization_id, operator, reviewer, approver)
    ]
    if not all(identities):
        raise ProductionImportError(
            "authorization_id_operator_reviewer_approver_required"
        )
    if len({value.casefold() for value in identities[1:]}) != 3:
        raise ProductionImportError("operator_reviewer_approver_must_be_distinct")

    plan_hash = _require_sha256(transaction_plan_sha256, "transaction_plan")
    bundle_hash = _require_sha256(evidence_bundle_sha256, "evidence_bundle")
    promotion_hash = _require_sha256(
        promotion_preflight_sha256, "promotion_preflight"
    )
    execution_auth_hash = _require_sha256(
        execution_authorization_sha256, "execution_authorization"
    )
    safe_dsn = validate_production_dsn(dsn)
    schemas = StagingSchemas(
        core=PRODUCTION_CORE_SCHEMA,
        evidence=PRODUCTION_EVIDENCE_SCHEMA,
    )
    run_db_id = str(evidence_bundle["run_db_id"])
    run_id = str(evidence_bundle["run_id"])
    expected_counts = _expected_table_counts(evidence_bundle["tables"])

    engine = create_engine(safe_dsn, future=True)
    connection = None
    transaction = None
    transaction_started = False
    commit_attempted = False
    try:
        connection = engine.connect()
        transaction = connection.begin()
        transaction_started = True
        server_version_num = _assert_postgresql18(connection)
        _assert_production_schema_ready(connection)

        ledger = connection.execute(
            text(
                f'SELECT * FROM "{PRODUCTION_EVIDENCE_SCHEMA}".'
                f'"{PRODUCTION_LEDGER_TABLE}" WHERE run_db_id=:run_db_id FOR UPDATE'
            ),
            {"run_db_id": run_db_id},
        ).mappings().one_or_none()
        if ledger:
            expected_hashes = {
                "evidence_bundle_sha256": bundle_hash,
                "transaction_plan_sha256": plan_hash,
                "promotion_preflight_sha256": promotion_hash,
                "execution_authorization_sha256": execution_auth_hash,
            }
            if any(str(ledger[key]) != value for key, value in expected_hashes.items()):
                raise ProductionImportError(
                    "production_idempotency_conflict_payload_changed"
                )
            if str(ledger["authorization_id"]) != authorization_id:
                raise ProductionImportError(
                    "production_idempotency_conflict_authorization_changed"
                )
            readback_counts = _evidence_readback_counts(
                connection, PRODUCTION_EVIDENCE_SCHEMA, evidence_bundle
            )
            if readback_counts != expected_counts:
                raise ProductionImportError(
                    "production_idempotent_readback_count_mismatch"
                )
            identity_readback = _identity_readback(
                connection, schemas, devices, run_id
            )
            if any(
                not row["sensor_found"]
                or row["stored_sn_code"] != row["sn_code"]
                or row["stored_device_code"] != row["device_code"]
                or row["protocol_alias_count"] != 1
                for row in identity_readback
            ):
                raise ProductionImportError(
                    "production_idempotent_identity_readback_mismatch"
                )
            result = {
                "status": "production_import_idempotent_noop",
                "idempotent": True,
                "postgresql_server_version_num": server_version_num,
                "run_id": run_id,
                "run_db_id": run_db_id,
                "table_counts": readback_counts,
                "identity_readback": identity_readback,
                "production_database_written": False,
            }
        else:
            existing_run = int(
                connection.execute(
                    text(
                        f'SELECT COUNT(*) FROM "{PRODUCTION_EVIDENCE_SCHEMA}".runs '
                        "WHERE id=:run_db_id"
                    ),
                    {"run_db_id": run_db_id},
                ).scalar_one()
            )
            if existing_run:
                raise ProductionImportError(
                    "production_run_exists_without_idempotency_ledger"
                )

            _upsert_core_identity(
                connection,
                schemas=schemas,
                run_id=run_id,
                devices=devices,
                operator=operator,
                import_scope="production",
            )
            if failure_injector:
                failure_injector("after_core_identity")
            inserted: dict[str, int] = {}
            for table in TABLE_NAMES:
                inserted[table] = _upsert_evidence_rows(
                    connection,
                    PRODUCTION_EVIDENCE_SCHEMA,
                    table,
                    evidence_bundle["tables"].get(table) or [],
                )
                if failure_injector:
                    failure_injector(f"after_{table}")

            readback_counts = _evidence_readback_counts(
                connection, PRODUCTION_EVIDENCE_SCHEMA, evidence_bundle
            )
            if readback_counts != expected_counts:
                raise ProductionImportError(
                    "production_precommit_table_count_mismatch"
                )
            identity_readback = _identity_readback(
                connection, schemas, devices, run_id
            )
            if any(
                not row["sensor_found"]
                or row["stored_sn_code"] != row["sn_code"]
                or row["stored_device_code"] != row["device_code"]
                or row["protocol_alias_count"] != 1
                for row in identity_readback
            ):
                raise ProductionImportError(
                    "production_precommit_identity_readback_mismatch"
                )

            connection.execute(
                text(
                    f'INSERT INTO "{PRODUCTION_EVIDENCE_SCHEMA}".'
                    f'"{PRODUCTION_LEDGER_TABLE}" '
                    "(run_db_id,run_id,evidence_bundle_sha256,transaction_plan_sha256,"
                    "promotion_preflight_sha256,execution_authorization_sha256,"
                    "authorization_id,operator_name,reviewer_name,approver_name,table_counts) "
                    "VALUES (:run_db_id,:run_id,:bundle_sha256,:plan_sha256,"
                    ":promotion_sha256,:execution_auth_sha256,:authorization_id,"
                    ":operator,:reviewer,:approver,CAST(:table_counts AS JSONB))"
                ),
                {
                    "run_db_id": run_db_id,
                    "run_id": run_id,
                    "bundle_sha256": bundle_hash,
                    "plan_sha256": plan_hash,
                    "promotion_sha256": promotion_hash,
                    "execution_auth_sha256": execution_auth_hash,
                    "authorization_id": authorization_id,
                    "operator": operator,
                    "reviewer": reviewer,
                    "approver": approver,
                    "table_counts": json.dumps(readback_counts, sort_keys=True),
                },
            )
            if failure_injector:
                failure_injector("before_commit")
            result = {
                "status": "production_import_committed",
                "idempotent": False,
                "postgresql_server_version_num": server_version_num,
                "run_id": run_id,
                "run_db_id": run_db_id,
                "table_counts": readback_counts,
                "identity_readback": identity_readback,
                "inserted": inserted,
                "production_database_written": True,
            }

        commit_attempted = True
        transaction.commit()
        result["transaction_committed"] = True
        return result
    except Exception as exc:
        rollback_attempted = False
        rollback_confirmed = False
        rollback_error = ""
        if transaction is not None and transaction.is_active:
            rollback_attempted = True
            try:
                transaction.rollback()
                rollback_confirmed = True
            except Exception as rollback_exc:  # pragma: no cover - connection loss
                rollback_error = f"{type(rollback_exc).__name__}:{rollback_exc}"
        commit_uncertain = commit_attempted and not rollback_confirmed
        return {
            "status": (
                "production_import_commit_uncertain_hold"
                if commit_uncertain
                else "production_import_rolled_back"
                if transaction_started
                else "production_import_not_started"
            ),
            "idempotent": False,
            "run_id": run_id,
            "run_db_id": run_db_id,
            "failure_type": type(exc).__name__,
            "failure_reason": str(exc),
            "production_database_written": False,
            "transaction_committed": False,
            "commit_attempted": commit_attempted,
            "commit_uncertain": commit_uncertain,
            "rollback_attempted": rollback_attempted,
            "rollback_confirmed": rollback_confirmed,
            "rollback_error": rollback_error,
        }
    finally:
        if connection is not None:
            connection.close()
        engine.dispose()
