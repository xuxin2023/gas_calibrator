"""Build the no-connect DBA packet for the V1.5 PostgreSQL 18 migrations."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..storage.v1_5_evidence.schema import Migration, load_migrations


SCHEMA = "v1_5_formal_database_migration_dba_readiness_v1"
READY_STATUS = "ready_for_postgresql18_dba_migration_review"
EXPECTED_VERSIONS = (
    "001_v1_5_evidence_registry",
    "002_v1_5_production_import_ledger",
)
PRODUCTION_DATABASE = "gas_calibrator"
PRODUCTION_EVIDENCE_SCHEMA = "v1_5_evidence"
PRODUCTION_DSN_ENV = "V1_5_POSTGRES_DSN"
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
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class MigrationReadinessCheck:
    check: str
    status: str
    reasons: tuple[str, ...]
    physical_meaning: str
    next_action: str
    details: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _check(
    check: str,
    reasons: Sequence[str],
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> MigrationReadinessCheck:
    return MigrationReadinessCheck(
        check=check,
        status="pass" if not reasons else "blocker",
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _migration_map(migrations: Sequence[Migration]) -> dict[str, Migration]:
    return {migration.version: migration for migration in migrations}


def _repository_source_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(REPOSITORY_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def _sequence_reasons(migrations: Sequence[Migration]) -> list[str]:
    versions = [migration.version for migration in migrations]
    reasons: list[str] = []
    if tuple(versions) != EXPECTED_VERSIONS:
        reasons.append(
            "migration_sequence_mismatch:expected="
            + ",".join(EXPECTED_VERSIONS)
            + ":actual="
            + ",".join(versions)
        )
    if len(set(versions)) != len(versions):
        reasons.append("migration_versions_not_unique")
    return reasons


def _content_reasons(migrations: Sequence[Migration]) -> list[str]:
    rows = _migration_map(migrations)
    reasons: list[str] = []
    registry = rows.get(EXPECTED_VERSIONS[0])
    ledger = rows.get(EXPECTED_VERSIONS[1])
    if registry is None:
        reasons.append("registry_migration_missing")
    elif "CREATE TABLE IF NOT EXISTS v1_5_evidence.schema_migrations" not in registry.sql:
        reasons.append("registry_schema_migrations_table_missing")
    if ledger is None:
        reasons.append("production_ledger_migration_missing")
        return reasons

    required_tokens = (
        "CREATE TABLE IF NOT EXISTS v1_5_evidence.production_import_ledger",
        "run_db_id TEXT PRIMARY KEY REFERENCES v1_5_evidence.runs(id)",
        "run_id TEXT NOT NULL UNIQUE",
        "evidence_bundle_sha256 TEXT NOT NULL",
        "transaction_plan_sha256 TEXT NOT NULL",
        "promotion_preflight_sha256 TEXT NOT NULL",
        "execution_authorization_sha256 TEXT NOT NULL",
        "authorization_id TEXT NOT NULL UNIQUE",
        "table_counts JSONB NOT NULL",
        "CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_production_import_ledger_run_id",
    )
    for token in required_tokens:
        if token not in ledger.sql:
            reasons.append(f"production_ledger_required_sql_missing:{token}")
    sql_without_line_comments = "\n".join(
        line.split("--", 1)[0] for line in ledger.sql.splitlines()
    )
    destructive_statement = re.compile(
        r"(?:^|;)\s*(DROP|TRUNCATE|DELETE|UPDATE|ALTER)\b",
        flags=re.IGNORECASE,
    )
    for match in destructive_statement.finditer(sql_without_line_comments):
        reasons.append(
            f"production_ledger_destructive_sql_forbidden:{match.group(1).upper()}"
        )
    return reasons


def _precheck_sql(migrations: Mapping[str, Migration]) -> str:
    registry = migrations[EXPECTED_VERSIONS[0]]
    ledger = migrations[EXPECTED_VERSIONS[1]]
    return "\n".join(
        [
            r"\set ON_ERROR_STOP on",
            "-- V1.5 PostgreSQL 18 DBA precheck. Read-only; run before migration 002.",
            "SELECT current_database() AS database_name,",
            "       current_setting('server_version_num')::integer AS server_version_num;",
            "SELECT version, checksum, applied_at",
            "FROM v1_5_evidence.schema_migrations",
            "WHERE version IN (",
            f"    '{registry.version}',",
            f"    '{ledger.version}'",
            ")",
            "ORDER BY version;",
            "SELECT table_schema, table_name",
            "FROM information_schema.tables",
            "WHERE table_schema = 'v1_5_evidence'",
            "  AND table_name IN ('runs', 'schema_migrations', 'production_import_ledger')",
            "ORDER BY table_name;",
            "",
            f"-- Expected {registry.version} checksum: {registry.checksum}",
            f"-- Expected {ledger.version} checksum: {ledger.checksum}",
            "",
        ]
    )


def _apply_sql(migrations: Mapping[str, Migration]) -> str:
    registry = migrations[EXPECTED_VERSIONS[0]]
    ledger = migrations[EXPECTED_VERSIONS[1]]
    expected_columns = ", ".join(f"'{column}'" for column in LEDGER_COLUMNS)
    return "\n".join(
        [
            r"\set ON_ERROR_STOP on",
            "-- V1.5 PostgreSQL 18 migration 002 DBA packet.",
            "-- Review and execute manually with ON_ERROR_STOP enabled.",
            "BEGIN;",
            "SET LOCAL lock_timeout = '5s';",
            "SET LOCAL statement_timeout = '60s';",
            "LOCK TABLE v1_5_evidence.schema_migrations IN SHARE ROW EXCLUSIVE MODE;",
            "DO $v1_5_migration_guard$",
            "DECLARE",
            "    server_version integer := current_setting('server_version_num')::integer;",
            "BEGIN",
            f"    IF current_database() <> '{PRODUCTION_DATABASE}' THEN",
            "        RAISE EXCEPTION 'wrong production database: %', current_database();",
            "    END IF;",
            "    IF server_version < 180000 OR server_version >= 190000 THEN",
            "        RAISE EXCEPTION 'PostgreSQL major must be 18: %', server_version;",
            "    END IF;",
            "    IF NOT EXISTS (",
            "        SELECT 1 FROM v1_5_evidence.schema_migrations",
            f"        WHERE version = '{registry.version}'",
            f"          AND checksum = '{registry.checksum}'",
            "    ) THEN",
            "        RAISE EXCEPTION 'migration 001 missing or checksum mismatch';",
            "    END IF;",
            "    IF EXISTS (",
            "        SELECT 1 FROM v1_5_evidence.schema_migrations",
            f"        WHERE version = '{ledger.version}'",
            f"          AND checksum <> '{ledger.checksum}'",
            "    ) THEN",
            "        RAISE EXCEPTION 'migration 002 checksum mismatch';",
            "    END IF;",
            "    IF (EXISTS (",
            "        SELECT 1 FROM v1_5_evidence.schema_migrations",
            f"        WHERE version = '{ledger.version}'",
            f"          AND checksum = '{ledger.checksum}'",
            "    )) <> (to_regclass('v1_5_evidence.production_import_ledger') IS NOT NULL) THEN",
            "        RAISE EXCEPTION 'migration 002 ledger/table state mismatch';",
            "    END IF;",
            "END",
            "$v1_5_migration_guard$;",
            "",
            ledger.sql.rstrip(),
            "",
            "INSERT INTO v1_5_evidence.schema_migrations (version, checksum)",
            f"VALUES ('{ledger.version}', '{ledger.checksum}')",
            "ON CONFLICT (version) DO NOTHING;",
            "",
            "DO $v1_5_migration_verify$",
            "DECLARE",
            "    actual_columns text[];",
            "BEGIN",
            "    SELECT array_agg(column_name::text ORDER BY ordinal_position)",
            "    INTO actual_columns",
            "    FROM information_schema.columns",
            "    WHERE table_schema = 'v1_5_evidence'",
            "      AND table_name = 'production_import_ledger';",
            f"    IF actual_columns <> ARRAY[{expected_columns}]::text[] THEN",
            "        RAISE EXCEPTION 'production_import_ledger columns mismatch: %', actual_columns;",
            "    END IF;",
            "    IF NOT EXISTS (",
            "        SELECT 1 FROM v1_5_evidence.schema_migrations",
            f"        WHERE version = '{ledger.version}'",
            f"          AND checksum = '{ledger.checksum}'",
            "    ) THEN",
            "        RAISE EXCEPTION 'migration 002 ledger row missing or checksum mismatch';",
            "    END IF;",
            "    IF NOT EXISTS (",
            "        SELECT 1 FROM pg_constraint",
            "        WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass",
            "          AND contype = 'p'",
            "          AND pg_get_constraintdef(oid) = 'PRIMARY KEY (run_db_id)'",
            "    ) THEN",
            "        RAISE EXCEPTION 'production_import_ledger run_db_id primary key missing';",
            "    END IF;",
            "    IF NOT EXISTS (",
            "        SELECT 1 FROM pg_constraint",
            "        WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass",
            "          AND contype = 'u'",
            "          AND pg_get_constraintdef(oid) = 'UNIQUE (run_id)'",
            "    ) THEN",
            "        RAISE EXCEPTION 'production_import_ledger run_id unique constraint missing';",
            "    END IF;",
            "    IF NOT EXISTS (",
            "        SELECT 1 FROM pg_constraint",
            "        WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass",
            "          AND contype = 'u'",
            "          AND pg_get_constraintdef(oid) = 'UNIQUE (authorization_id)'",
            "    ) THEN",
            "        RAISE EXCEPTION 'production_import_ledger authorization_id unique constraint missing';",
            "    END IF;",
            "    IF NOT EXISTS (",
            "        SELECT 1 FROM pg_constraint",
            "        WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass",
            "          AND contype = 'f'",
            "          AND pg_get_constraintdef(oid) = 'FOREIGN KEY (run_db_id) REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE'",
            "    ) THEN",
            "        RAISE EXCEPTION 'production_import_ledger run_db_id foreign key missing';",
            "    END IF;",
            "    IF to_regclass('v1_5_evidence.ix_v1_5_evidence_production_import_ledger_run_id') IS NULL THEN",
            "        RAISE EXCEPTION 'production_import_ledger run_id index missing';",
            "    END IF;",
            "END",
            "$v1_5_migration_verify$;",
            "COMMIT;",
            "",
        ]
    )


def _postcheck_sql(migrations: Mapping[str, Migration]) -> str:
    ledger = migrations[EXPECTED_VERSIONS[1]]
    return "\n".join(
        [
            r"\set ON_ERROR_STOP on",
            "-- V1.5 PostgreSQL 18 migration 002 postcheck. Read-only.",
            "SELECT current_database() AS database_name,",
            "       current_setting('server_version_num')::integer AS server_version_num;",
            "SELECT version, checksum, applied_at",
            "FROM v1_5_evidence.schema_migrations",
            f"WHERE version = '{ledger.version}';",
            "SELECT column_name, data_type, is_nullable, ordinal_position",
            "FROM information_schema.columns",
            "WHERE table_schema = 'v1_5_evidence'",
            "  AND table_name = 'production_import_ledger'",
            "ORDER BY ordinal_position;",
            "SELECT conname, contype, pg_get_constraintdef(oid) AS definition",
            "FROM pg_constraint",
            "WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass",
            "ORDER BY contype, conname;",
            "SELECT indexname, indexdef",
            "FROM pg_indexes",
            "WHERE schemaname = 'v1_5_evidence'",
            "  AND tablename = 'production_import_ledger'",
            "ORDER BY indexname;",
            f"-- Expected migration checksum: {ledger.checksum}",
            "",
        ]
    )


def build_v1_5_formal_database_migration_dba_readiness(
    *, migrations: Sequence[Migration] | None = None
) -> dict[str, Any]:
    rows = tuple(migrations if migrations is not None else load_migrations())
    sequence_reasons = _sequence_reasons(rows)
    content_reasons = _content_reasons(rows)
    checks = [
        _check(
            "migration_sequence",
            sequence_reasons,
            "Production migration 002 is only meaningful after the exact reviewed migration 001.",
            "Restore the exact ordered 001/002 migration set before DBA review.",
            {"expected_versions": list(EXPECTED_VERSIONS), "actual_versions": [row.version for row in rows]},
        ),
        _check(
            "migration_content",
            content_reasons,
            "The production ledger must bind immutable package and authorization hashes without destructive SQL.",
            "Restore the reviewed ledger DDL and rerun this no-connect exporter.",
            {"forbidden_sql": ["DROP", "TRUNCATE", "DELETE", "UPDATE", "ALTER"]},
        ),
    ]
    reasons = [reason for check in checks for reason in check.reasons]
    migration_map = _migration_map(rows)
    scripts_ready = not reasons and all(version in migration_map for version in EXPECTED_VERSIONS)
    scripts = (
        {
            "precheck_sql": _precheck_sql(migration_map),
            "apply_sql": _apply_sql(migration_map),
            "postcheck_sql": _postcheck_sql(migration_map),
        }
        if scripts_ready
        else {"precheck_sql": "", "apply_sql": "", "postcheck_sql": ""}
    )
    script_sha256 = {
        role: hashlib.sha256(sql.encode("utf-8")).hexdigest()
        for role, sql in scripts.items()
        if sql
    }
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if scripts_ready else "blocked",
        "blocker_count": len(reasons),
        "review_required_count": 1 if scripts_ready else 0,
        "dba_packet_ready": scripts_ready,
        "production_target": {
            "backend": "postgresql",
            "postgresql_major": 18,
            "database_name": PRODUCTION_DATABASE,
            "core_schema": "public",
            "evidence_schema": PRODUCTION_EVIDENCE_SCHEMA,
            "dsn_env": PRODUCTION_DSN_ENV,
        },
        "migrations": [
            {
                "sequence": index,
                "version": migration.version,
                "source_path": _repository_source_path(migration.path),
                "sha256": migration.checksum,
                "sql_bytes": len(migration.sql.encode("utf-8")),
                "role": "prerequisite" if index == 1 else "production_import_ledger",
            }
            for index, migration in enumerate(rows, start=1)
        ],
        "checks": [check.to_json() for check in checks],
        "reasons": reasons,
        "scripts": scripts,
        "script_sha256": script_sha256,
        "dba_execution_contract": {
            "manual_dba_execution_required": True,
            "psql_on_error_stop_required": True,
            "transactional_apply_required": True,
            "precheck_review_required": True,
            "postcheck_capture_required": True,
            "operator_reviewer_approver_record_required": True,
            "rollback_before_commit": "transaction_rollback",
            "post_commit_problem": "DBA_and_reviewer_hold_no_automatic_drop",
        },
        "connects_postgresql": False,
        "dsn_value_read": False,
        "applies_migrations": False,
        "migration_execution_allowed": False,
        "production_import_execution_allowed": False,
        "database_written": False,
        "database_import_allowed": False,
        "formal_release_allowed": False,
        "opens_com_ports": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "not_real_acceptance_evidence": True,
        "evidence_source": "repository_migrations_offline_review",
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["status"])
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def write_v1_5_formal_database_migration_dba_readiness_outputs(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_formal_database_migration_dba_readiness.json",
        "checks": out / "v1_5_formal_database_migration_dba_readiness_checks.csv",
        "migrations": out / "v1_5_formal_database_migration_manifest.csv",
        "precheck_sql": out / "01_v1_5_postgresql18_migration_precheck.sql",
        "apply_sql": out / "02_v1_5_postgresql18_apply_migration_002.sql",
        "postcheck_sql": out / "03_v1_5_postgresql18_migration_postcheck.sql",
        "execution_record_template": out
        / "v1_5_postgresql18_dba_execution_record_template.json",
        "summary": out / "V1_5_FORMAL_DATABASE_MIGRATION_DBA_READINESS.md",
    }
    outputs["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    _write_csv(outputs["checks"], list(model.get("checks") or []))
    _write_csv(outputs["migrations"], list(model.get("migrations") or []))
    scripts = model.get("scripts") if isinstance(model.get("scripts"), Mapping) else {}
    for role in ("precheck_sql", "apply_sql", "postcheck_sql"):
        outputs[role].write_text(
            str(scripts.get(role) or ""), encoding="utf-8", newline="\n"
        )
    outputs["execution_record_template"].write_text(
        json.dumps(
            {
                "schema": "v1_5_postgresql18_dba_execution_record_v1",
                "template_only": True,
                "not_execution_evidence": True,
                "production_target": model.get("production_target"),
                "migration_versions": [
                    row.get("version") for row in model.get("migrations") or []
                ],
                "expected_script_sha256": model.get("script_sha256"),
                "operator": {"name": "", "decision": "", "recorded_at_utc": ""},
                "reviewer": {"name": "", "decision": "", "recorded_at_utc": ""},
                "approver": {"name": "", "decision": "", "recorded_at_utc": ""},
                "precheck": {"output_path": "", "sha256": "", "reviewed": False},
                "apply": {
                    "output_path": "",
                    "sha256": "",
                    "psql_exit_code": None,
                    "transaction_committed": False,
                },
                "postcheck": {"output_path": "", "sha256": "", "reviewed": False},
                "migration_execution_confirmed": False,
                "production_import_authorized": False,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs["summary"].write_text(
        "\n".join(
            [
                "# V1.5 PostgreSQL 18 DBA migration readiness",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- blocker_count: `{model.get('blocker_count')}`",
                f"- dba_packet_ready: `{model.get('dba_packet_ready')}`",
                f"- connects_postgresql: `{model.get('connects_postgresql')}`",
                f"- applies_migrations: `{model.get('applies_migrations')}`",
                f"- migration_execution_allowed: `{model.get('migration_execution_allowed')}`",
                f"- database_import_allowed: `{model.get('database_import_allowed')}`",
                f"- formal_release_allowed: `{model.get('formal_release_allowed')}`",
                "",
                "## Script SHA256",
                "",
                *[
                    f"- {role}: `{sha256}`"
                    for role, sha256 in (model.get("script_sha256") or {}).items()
                ],
                "",
                "This packet is a no-connect DBA handoff. A DBA must separately review and",
                "execute the SQL with ON_ERROR_STOP, capture pre/post checks, and record",
                "operator/reviewer/approver approval in the template-only execution record.",
                "It does not authorize production import.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return outputs
