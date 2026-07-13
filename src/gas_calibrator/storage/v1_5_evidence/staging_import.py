"""Controlled PostgreSQL 18 staging import for V1.5 evidence packages.

The staging importer is deliberately isolated from the production
``v1_5_evidence`` schema.  It writes one 1-6 analyzer batch atomically into
dedicated staging schemas, records an idempotency ledger, and performs
pre-commit identity/count readback.  It never opens COM ports or changes
analyzer state.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.schema import CreateSchema

from ...v2.storage.database import stable_uuid
from ...v2.storage.models import (
    Base,
    DeviceEventRecord,
    RunRecord,
    SensorIdentityAliasRecord,
    SensorRecord,
)
from .bundle import TABLE_NAMES
from .repository import JSONB_COLUMNS, TABLE_COLUMNS
from .schema import load_migrations


STAGING_CORE_SCHEMA_PREFIX = "v1_5_core_staging"
STAGING_EVIDENCE_SCHEMA_PREFIX = "v1_5_evidence_staging"
STAGING_SCHEMA_RE = re.compile(r"[a-z][a-z0-9_]{0,62}")
SN_RE = re.compile(r"\d{8}")
PROTOCOL_ID_RE = re.compile(r"\d{3}")
POSTGRESQL_18_MIN = 180000
POSTGRESQL_19_MIN = 190000


class StagingImportError(RuntimeError):
    """Raised when a staging package or database boundary is invalid."""


@dataclass(frozen=True)
class StagingSchemas:
    core: str
    evidence: str


def validate_staging_schemas(core_schema: str, evidence_schema: str) -> StagingSchemas:
    core = str(core_schema or "").strip().lower()
    evidence = str(evidence_schema or "").strip().lower()
    for value, prefix, role in (
        (core, STAGING_CORE_SCHEMA_PREFIX, "core"),
        (evidence, STAGING_EVIDENCE_SCHEMA_PREFIX, "evidence"),
    ):
        if not STAGING_SCHEMA_RE.fullmatch(value):
            raise StagingImportError(f"{role}_staging_schema_invalid")
        if value != prefix and not value.startswith(prefix + "_"):
            raise StagingImportError(f"{role}_staging_schema_prefix_required")
    if core == evidence:
        raise StagingImportError("core_and_evidence_staging_schemas_must_differ")
    if core in {"public", "v1_5_evidence"} or evidence in {"public", "v1_5_evidence"}:
        raise StagingImportError("production_schema_forbidden")
    return StagingSchemas(core=core, evidence=evidence)


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json_object(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    if not source.exists() or not source.is_file():
        raise StagingImportError(f"input_json_missing:{source}")
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise StagingImportError(f"input_json_not_object:{source}")
    return payload


def _planned_devices(transaction_plan: Mapping[str, Any]) -> list[dict[str, str]]:
    raw_rows = transaction_plan.get("planned_devices")
    if not isinstance(raw_rows, list) or not 1 <= len(raw_rows) <= 6:
        raise StagingImportError("planned_device_count_must_be_1_to_6")
    rows: list[dict[str, str]] = []
    seen_sn: set[str] = set()
    seen_device_code: set[str] = set()
    seen_protocol: set[str] = set()
    for index, raw in enumerate(raw_rows, start=1):
        if not isinstance(raw, Mapping):
            raise StagingImportError(f"GA{index:02d}:planned_device_not_object")
        slot = str(raw.get("slot") or f"GA{index:02d}").strip()
        sn_code = str(raw.get("sn_code") or "").strip()
        device_code = str(raw.get("device_code") or "").strip()
        protocol_id = str(raw.get("protocol_device_id") or "").strip()
        port = str(raw.get("port") or "").strip().upper()
        if not re.fullmatch(r"GA0[1-6]", slot):
            raise StagingImportError(f"{slot}:slot_invalid")
        if not SN_RE.fullmatch(sn_code) or sn_code == "00000000":
            raise StagingImportError(f"{slot}:sn_code_invalid")
        if not SN_RE.fullmatch(device_code) or device_code != sn_code:
            raise StagingImportError(f"{slot}:device_code_must_match_sn_code")
        if not PROTOCOL_ID_RE.fullmatch(protocol_id):
            raise StagingImportError(f"{slot}:protocol_device_id_invalid")
        if sn_code in seen_sn:
            raise StagingImportError(f"{slot}:duplicate_sn_code")
        if device_code in seen_device_code:
            raise StagingImportError(f"{slot}:duplicate_device_code")
        if protocol_id in seen_protocol:
            raise StagingImportError(f"{slot}:duplicate_protocol_device_id")
        seen_sn.add(sn_code)
        seen_device_code.add(device_code)
        seen_protocol.add(protocol_id)
        rows.append(
            {
                "slot": slot,
                "sn_code": sn_code,
                "device_code": device_code,
                "protocol_device_id": protocol_id,
                "port": port,
            }
        )
    return rows


def _bundle_tables(bundle: Mapping[str, Any]) -> Mapping[str, Any]:
    if bundle.get("schema") != "v1_5_evidence_registry":
        raise StagingImportError("evidence_bundle_schema_invalid")
    tables = bundle.get("tables")
    if not isinstance(tables, Mapping):
        raise StagingImportError("evidence_bundle_tables_missing")
    for table in TABLE_NAMES:
        rows = tables.get(table)
        if not isinstance(rows, list):
            raise StagingImportError(f"evidence_bundle_table_invalid:{table}")
    run_db_id = str(bundle.get("run_db_id") or "").strip()
    run_id = str(bundle.get("run_id") or "").strip()
    if not run_db_id or not run_id:
        raise StagingImportError("evidence_bundle_run_identity_missing")
    run_rows = tables.get("runs") or []
    if len(run_rows) != 1:
        raise StagingImportError("evidence_bundle_must_have_one_run_row")
    run_row = run_rows[0]
    if not isinstance(run_row, Mapping):
        raise StagingImportError("evidence_bundle_run_row_invalid")
    if str(run_row.get("id") or "") != run_db_id or str(run_row.get("run_id") or "") != run_id:
        raise StagingImportError("evidence_bundle_run_row_identity_mismatch")
    for table in TABLE_NAMES:
        if table in {"runs", "devices"}:
            continue
        for row in tables.get(table) or []:
            if not isinstance(row, Mapping):
                raise StagingImportError(f"evidence_bundle_row_invalid:{table}")
            if str(row.get("run_db_id") or "") != run_db_id:
                raise StagingImportError(f"evidence_bundle_run_db_id_mismatch:{table}")
    return tables


def _bundle_protocol_ids(bundle: Mapping[str, Any]) -> set[str]:
    tables = _bundle_tables(bundle)
    dut_ids = {
        str(row.get("device_id") or "")
        for row in tables.get("run_devices") or []
        if isinstance(row, Mapping) and row.get("role") == "device_under_test"
    }
    protocol_ids: set[str] = set()
    for row in tables.get("devices") or []:
        if not isinstance(row, Mapping) or str(row.get("id") or "") not in dut_ids:
            continue
        metadata = row.get("metadata") if isinstance(row.get("metadata"), Mapping) else {}
        candidates = (
            metadata.get("protocol_device_id"),
            metadata.get("protocol_device_id_at_run"),
            row.get("serial_number"),
            row.get("display_name"),
        )
        protocol = next(
            (str(value).strip() for value in candidates if PROTOCOL_ID_RE.fullmatch(str(value or "").strip())),
            "",
        )
        if not protocol:
            raise StagingImportError("device_under_test_protocol_device_id_missing")
        protocol_ids.add(protocol)
    if not protocol_ids:
        raise StagingImportError("evidence_bundle_device_under_test_missing")
    return protocol_ids


def validate_staging_package(
    transaction_plan: Mapping[str, Any], bundle: Mapping[str, Any]
) -> list[dict[str, str]]:
    if transaction_plan.get("schema") != "v1_5_formal_database_import_transaction_plan_v1":
        raise StagingImportError("transaction_plan_schema_invalid")
    if transaction_plan.get("transaction_plan_contract_ready") is not True:
        raise StagingImportError("transaction_plan_contract_not_ready")
    if transaction_plan.get("production_backend") != "postgresql":
        raise StagingImportError("transaction_plan_backend_not_postgresql")
    if int(transaction_plan.get("production_postgresql_major") or 0) != 18:
        raise StagingImportError("transaction_plan_postgresql_major_not_18")
    for field in (
        "connects_postgresql",
        "database_import_attempted",
        "database_written",
        "database_import_allowed",
        "real_import_execution_allowed",
        "execution_supported",
        "formal_release_allowed",
    ):
        if transaction_plan.get(field) is not False:
            raise StagingImportError(f"transaction_plan_{field}_must_be_false")
    planned = _planned_devices(transaction_plan)
    planned_protocols = {row["protocol_device_id"] for row in planned}
    bundle_protocols = _bundle_protocol_ids(bundle)
    if planned_protocols != bundle_protocols:
        raise StagingImportError("planned_protocol_ids_do_not_match_evidence_bundle")
    return planned


def _canonical_json_sha256(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _sqlalchemy_dsn(dsn: str) -> str:
    value = str(dsn or "").strip()
    if value.startswith("postgresql://"):
        return "postgresql+psycopg://" + value[len("postgresql://") :]
    if value.startswith("postgres://"):
        return "postgresql+psycopg://" + value[len("postgres://") :]
    return value


def _validate_staging_dsn(dsn: str) -> str:
    from sqlalchemy.engine import make_url

    url = make_url(_sqlalchemy_dsn(dsn))
    if not str(url.drivername).startswith("postgresql"):
        raise StagingImportError("staging_dsn_must_use_postgresql")
    database = str(url.database or "").lower()
    if "staging" not in database and "test" not in database:
        raise StagingImportError("staging_database_name_must_contain_staging_or_test")
    return url.render_as_string(hide_password=False)


def _split_sql(sql_text: str) -> list[str]:
    return [statement.strip() for statement in sql_text.split(";") if statement.strip()]


def _create_staging_schemas(connection: Any, schemas: StagingSchemas) -> None:
    connection.execute(CreateSchema(schemas.core, if_not_exists=True))
    connection.execute(CreateSchema(schemas.evidence, if_not_exists=True))
    translated = connection.execution_options(schema_translate_map={None: schemas.core})
    Base.metadata.create_all(translated)
    for migration in load_migrations():
        rendered = migration.sql.replace("v1_5_evidence", schemas.evidence)
        for statement in _split_sql(rendered):
            connection.exec_driver_sql(statement)
        connection.execute(
            text(
                f'INSERT INTO "{schemas.evidence}".schema_migrations (version, checksum) '
                "VALUES (:version, :checksum) ON CONFLICT (version) DO UPDATE "
                "SET checksum=EXCLUDED.checksum, applied_at=now()"
            ),
            {"version": migration.version, "checksum": migration.checksum},
        )
    connection.exec_driver_sql(
        f'''CREATE TABLE IF NOT EXISTS "{schemas.evidence}".staging_import_ledger (
            run_db_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            bundle_sha256 TEXT NOT NULL,
            transaction_plan_sha256 TEXT NOT NULL,
            authorization_id TEXT NOT NULL,
            operator_name TEXT NOT NULL,
            reviewer_name TEXT NOT NULL,
            approver_name TEXT NOT NULL,
            table_counts JSONB NOT NULL,
            committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )'''
    )


def _assert_postgresql18(connection: Any) -> int:
    version = int(connection.execute(text("SHOW server_version_num")).scalar_one())
    if not POSTGRESQL_18_MIN <= version < POSTGRESQL_19_MIN:
        raise StagingImportError(f"postgresql_server_major_not_18:{version}")
    return version


def _upsert_core_identity(
    connection: Any,
    *,
    schemas: StagingSchemas,
    run_id: str,
    devices: Sequence[Mapping[str, str]],
    operator: str,
) -> None:
    translated = connection.execution_options(schema_translate_map={None: schemas.core})
    run_uuid = stable_uuid("run", run_id)
    run_stmt = postgresql_insert(RunRecord.__table__).values(
        id=run_uuid,
        status="completed",
        run_mode="v1_5_staging_import",
        route_mode="none",
        profile_name="v1_5_postgresql18_staging",
        operator=operator,
        total_points=0,
        successful_points=0,
        failed_points=0,
        warnings=0,
        errors=0,
        notes=json.dumps(
            {
                "source_run_id": run_id,
                "staging_only": True,
                "not_real_acceptance_evidence": True,
            },
            ensure_ascii=False,
        ),
    )
    translated.execute(
        run_stmt.on_conflict_do_update(
            index_elements=[RunRecord.__table__.c.id],
            set_={"operator": operator, "notes": run_stmt.excluded.notes},
        )
    )
    observed_at = datetime.now(timezone.utc)
    for row in devices:
        sn_code = row["sn_code"]
        sensor_uuid = stable_uuid("sensor", f"gas_analyzer_co2_h2o:{sn_code.lower()}")
        metadata = {
            "sn_code": sn_code,
            "device_code": row["device_code"],
            "protocol_device_id_at_run": row["protocol_device_id"],
            "slot_id": row["slot"],
            "port_at_run": row["port"],
            "staging_only": True,
        }
        sensor_stmt = postgresql_insert(SensorRecord.__table__).values(
            sensor_id=sensor_uuid,
            device_key=f"gas_analyzer_co2_h2o:{sn_code.lower()}",
            sn_code=sn_code,
            device_code=row["device_code"],
            analyzer_id=sn_code,
            analyzer_serial=sn_code,
            software_version="v1.5",
            model="gas_analyzer",
            channel_type="gas_analyzer_co2_h2o",
            metadata=metadata,
        )
        translated.execute(
            sensor_stmt.on_conflict_do_update(
                index_elements=[SensorRecord.__table__.c.sensor_id],
                set_={
                    "device_key": sensor_stmt.excluded.device_key,
                    "sn_code": sensor_stmt.excluded.sn_code,
                    "device_code": sensor_stmt.excluded.device_code,
                    "metadata": sensor_stmt.excluded.metadata,
                },
            )
        )
        for alias_type, alias_value in (
            ("sn_code", sn_code),
            ("device_code", row["device_code"]),
            ("protocol_device_id_at_run", row["protocol_device_id"]),
        ):
            alias_uuid = stable_uuid(
                "sensor_identity_alias", sensor_uuid, alias_type, alias_value, run_uuid
            )
            alias_stmt = postgresql_insert(SensorIdentityAliasRecord.__table__).values(
                id=alias_uuid,
                sensor_id=sensor_uuid,
                alias_type=alias_type,
                alias_value=alias_value,
                source_run_id=run_uuid,
                observed_at=observed_at,
                valid_from=observed_at,
                valid_to=None,
                metadata={"source": "v1_5_postgresql18_staging_import"},
            )
            translated.execute(
                alias_stmt.on_conflict_do_update(
                    index_elements=[SensorIdentityAliasRecord.__table__.c.id],
                    set_={"observed_at": observed_at, "valid_from": observed_at},
                )
            )
        event_id = stable_uuid("device_event", run_uuid, sn_code, "v1_5_staging_identity_import")
        event_stmt = postgresql_insert(DeviceEventRecord.__table__).values(
            id=event_id,
            run_id=run_uuid,
            device_name=sn_code,
            event_type="v1_5_staging_identity_import",
            event_data={**metadata, "not_real_acceptance_evidence": True},
            timestamp=observed_at,
        )
        translated.execute(
            event_stmt.on_conflict_do_update(
                index_elements=[DeviceEventRecord.__table__.c.id],
                set_={"event_data": event_stmt.excluded.event_data, "timestamp": observed_at},
            )
        )


def _json_value(value: Any, column: str) -> Any:
    if column not in JSONB_COLUMNS:
        return value
    if value is None:
        value = [] if column in {"package_blockers", "reasons", "blockers"} else {}
    return json.dumps(value, ensure_ascii=False, default=str)


def _upsert_evidence_rows(connection: Any, schema: str, table: str, rows: Sequence[Mapping[str, Any]]) -> int:
    columns = TABLE_COLUMNS[table]
    column_sql = ", ".join(f'"{column}"' for column in columns)
    values_sql = ", ".join(
        f"CAST(:{column} AS JSONB)" if column in JSONB_COLUMNS else f":{column}"
        for column in columns
    )
    updates = ", ".join(
        f'"{column}"=EXCLUDED."{column}"' for column in columns if column != "id"
    )
    if table in {"runs", "devices"}:
        updates += ", updated_at=now()"
    statement = text(
        f'INSERT INTO "{schema}"."{table}" ({column_sql}) VALUES ({values_sql}) '
        f'ON CONFLICT (id) DO UPDATE SET {updates}'
    )
    for row in rows:
        connection.execute(
            statement,
            {column: _json_value(row.get(column), column) for column in columns},
        )
    return len(rows)


def _expected_table_counts(tables: Mapping[str, Any]) -> dict[str, int]:
    return {table: len(tables.get(table) or []) for table in TABLE_NAMES}


def _evidence_readback_counts(connection: Any, schema: str, bundle: Mapping[str, Any]) -> dict[str, int]:
    run_db_id = str(bundle["run_db_id"])
    tables = bundle["tables"]
    device_ids = [str(row.get("id")) for row in tables.get("devices") or []]
    counts: dict[str, int] = {}
    counts["runs"] = int(
        connection.execute(
            text(f'SELECT COUNT(*) FROM "{schema}".runs WHERE id=:run_db_id'),
            {"run_db_id": run_db_id},
        ).scalar_one()
    )
    if device_ids:
        counts["devices"] = int(
            connection.execute(
                text(f'SELECT COUNT(*) FROM "{schema}".devices WHERE id = ANY(:device_ids)'),
                {"device_ids": device_ids},
            ).scalar_one()
        )
    else:
        counts["devices"] = 0
    for table in TABLE_NAMES:
        if table in {"runs", "devices"}:
            continue
        counts[table] = int(
            connection.execute(
                text(f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE run_db_id=:run_db_id'),
                {"run_db_id": run_db_id},
            ).scalar_one()
        )
    return counts


def _identity_readback(
    connection: Any, schemas: StagingSchemas, devices: Sequence[Mapping[str, str]], run_id: str
) -> list[dict[str, Any]]:
    translated = connection.execution_options(schema_translate_map={None: schemas.core})
    rows: list[dict[str, Any]] = []
    run_uuid = stable_uuid("run", run_id)
    for device in devices:
        sensor = translated.execute(
            text(
                f'SELECT sensor_id, sn_code, device_code FROM "{schemas.core}".sensors '
                "WHERE sn_code=:sn_code"
            ),
            {"sn_code": device["sn_code"]},
        ).mappings().one_or_none()
        protocol_count = int(
            translated.execute(
                text(
                    f'SELECT COUNT(*) FROM "{schemas.core}".sensor_identity_aliases '
                    "WHERE source_run_id=:run_uuid AND alias_type='protocol_device_id_at_run' "
                    "AND alias_value=:protocol"
                ),
                {"run_uuid": str(run_uuid), "protocol": device["protocol_device_id"]},
            ).scalar_one()
        )
        rows.append(
            {
                **dict(device),
                "sensor_found": sensor is not None,
                "stored_sn_code": str(sensor["sn_code"]) if sensor else "",
                "stored_device_code": str(sensor["device_code"]) if sensor else "",
                "protocol_alias_count": protocol_count,
            }
        )
    return rows


def execute_staging_import(
    *,
    dsn: str,
    transaction_plan: Mapping[str, Any],
    evidence_bundle: Mapping[str, Any],
    transaction_plan_sha256: str,
    evidence_bundle_sha256: str,
    core_schema: str,
    evidence_schema: str,
    authorization_id: str,
    operator: str,
    reviewer: str,
    approver: str,
    initialize_schemas: bool,
    failure_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Execute one atomic staging-only import and return readback evidence."""

    schemas = validate_staging_schemas(core_schema, evidence_schema)
    devices = validate_staging_package(transaction_plan, evidence_bundle)
    auth_values = [str(value or "").strip() for value in (authorization_id, operator, reviewer, approver)]
    if not all(auth_values):
        raise StagingImportError("authorization_operator_reviewer_approver_required")
    if reviewer == approver:
        raise StagingImportError("reviewer_and_approver_must_differ")
    safe_dsn = _validate_staging_dsn(dsn)
    bundle_hash = evidence_bundle_sha256 or _canonical_json_sha256(evidence_bundle)
    plan_hash = transaction_plan_sha256 or _canonical_json_sha256(transaction_plan)
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
        if initialize_schemas:
            _create_staging_schemas(connection, schemas)
        else:
            available = set(inspect(connection).get_schema_names())
            if schemas.core not in available or schemas.evidence not in available:
                raise StagingImportError("staging_schema_missing_init_required")
        ledger = connection.execute(
            text(
                f'SELECT * FROM "{schemas.evidence}".staging_import_ledger '
                "WHERE run_db_id=:run_db_id FOR UPDATE"
            ),
            {"run_db_id": run_db_id},
        ).mappings().one_or_none()
        if ledger:
            if (
                str(ledger["bundle_sha256"]) != bundle_hash
                or str(ledger["transaction_plan_sha256"]) != plan_hash
            ):
                raise StagingImportError("staging_idempotency_conflict_payload_changed")
            readback_counts = _evidence_readback_counts(
                connection, schemas.evidence, evidence_bundle
            )
            if readback_counts != expected_counts:
                raise StagingImportError("staging_idempotent_readback_count_mismatch")
            identities = _identity_readback(connection, schemas, devices, run_id)
            if any(
                not row["sensor_found"]
                or row["stored_sn_code"] != row["sn_code"]
                or row["stored_device_code"] != row["device_code"]
                or row["protocol_alias_count"] != 1
                for row in identities
            ):
                raise StagingImportError("staging_idempotent_identity_readback_mismatch")
            result = {
                "status": "staging_import_idempotent_noop",
                "idempotent": True,
                "postgresql_server_version_num": server_version_num,
                "run_id": run_id,
                "run_db_id": run_db_id,
                "table_counts": readback_counts,
                "identity_readback": identities,
                "staging_database_written": False,
            }
        else:
            existing_run = int(
                connection.execute(
                    text(
                        f'SELECT COUNT(*) FROM "{schemas.evidence}".runs WHERE id=:run_db_id'
                    ),
                    {"run_db_id": run_db_id},
                ).scalar_one()
            )
            if existing_run:
                raise StagingImportError("staging_run_exists_without_idempotency_ledger")
            _upsert_core_identity(
                connection,
                schemas=schemas,
                run_id=run_id,
                devices=devices,
                operator=operator,
            )
            if failure_injector:
                failure_injector("after_core_identity")
            inserted: dict[str, int] = {}
            for table in TABLE_NAMES:
                inserted[table] = _upsert_evidence_rows(
                    connection,
                    schemas.evidence,
                    table,
                    evidence_bundle["tables"].get(table) or [],
                )
                if failure_injector:
                    failure_injector(f"after_{table}")
            readback_counts = _evidence_readback_counts(
                connection, schemas.evidence, evidence_bundle
            )
            if readback_counts != expected_counts:
                raise StagingImportError("staging_precommit_table_count_mismatch")
            identities = _identity_readback(connection, schemas, devices, run_id)
            if any(
                not row["sensor_found"]
                or row["stored_sn_code"] != row["sn_code"]
                or row["stored_device_code"] != row["device_code"]
                or row["protocol_alias_count"] != 1
                for row in identities
            ):
                raise StagingImportError("staging_precommit_identity_readback_mismatch")
            connection.execute(
                text(
                    f'INSERT INTO "{schemas.evidence}".staging_import_ledger '
                    "(run_db_id,run_id,bundle_sha256,transaction_plan_sha256,authorization_id,"
                    "operator_name,reviewer_name,approver_name,table_counts) "
                    "VALUES (:run_db_id,:run_id,:bundle_sha256,:plan_sha256,:authorization_id,"
                    ":operator,:reviewer,:approver,CAST(:table_counts AS JSONB))"
                ),
                {
                    "run_db_id": run_db_id,
                    "run_id": run_id,
                    "bundle_sha256": bundle_hash,
                    "plan_sha256": plan_hash,
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
                "status": "staging_import_committed",
                "idempotent": False,
                "postgresql_server_version_num": server_version_num,
                "run_id": run_id,
                "run_db_id": run_db_id,
                "table_counts": readback_counts,
                "identity_readback": identities,
                "inserted": inserted,
                "staging_database_written": True,
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
            except Exception as rollback_exc:  # pragma: no cover - requires connection loss
                rollback_error = f"{type(rollback_exc).__name__}:{rollback_exc}"
        commit_uncertain = commit_attempted and not rollback_confirmed
        return {
            "status": (
                "staging_import_commit_uncertain_hold"
                if commit_uncertain
                else "staging_import_rolled_back"
                if transaction_started
                else "staging_import_not_started"
            ),
            "idempotent": False,
            "run_id": run_id,
            "run_db_id": run_db_id,
            "failure_type": type(exc).__name__,
            "failure_reason": str(exc),
            "staging_database_written": False,
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


def query_staging_identity(
    *,
    dsn: str,
    core_schema: str,
    evidence_schema: str,
    query_kind: str,
    query_value: str,
) -> dict[str, Any]:
    """Read staging rows by SN, device_code, protocol ID, or run ID."""

    schemas = validate_staging_schemas(core_schema, evidence_schema)
    safe_dsn = _validate_staging_dsn(dsn)
    kind = str(query_kind or "").strip()
    value = str(query_value or "").strip()
    if kind not in {"sn_code", "device_code", "protocol_device_id", "run_id"}:
        raise StagingImportError("staging_query_kind_invalid")
    if not value:
        raise StagingImportError("staging_query_value_required")
    engine = create_engine(safe_dsn, future=True)
    try:
        with engine.connect() as connection:
            _assert_postgresql18(connection)
            if kind == "run_id":
                run_rows = connection.execute(
                    text(
                        f'SELECT run_db_id,run_id,bundle_sha256,transaction_plan_sha256,'
                        f'authorization_id,committed_at FROM "{schemas.evidence}".staging_import_ledger '
                        "WHERE run_id=:value OR run_db_id=:value ORDER BY committed_at DESC"
                    ),
                    {"value": value},
                ).mappings().all()
                return {"query_kind": kind, "query_value": value, "rows": [dict(row) for row in run_rows]}
            if kind == "protocol_device_id":
                rows = connection.execute(
                    text(
                        f'SELECT s.sensor_id,s.sn_code,s.device_code,a.alias_value AS protocol_device_id,'
                        f'a.source_run_id FROM "{schemas.core}".sensors s '
                        f'JOIN "{schemas.core}".sensor_identity_aliases a ON a.sensor_id=s.sensor_id '
                        "WHERE a.alias_type='protocol_device_id_at_run' AND a.alias_value=:value "
                        "ORDER BY a.observed_at DESC"
                    ),
                    {"value": value},
                ).mappings().all()
            else:
                rows = connection.execute(
                    text(
                        f'SELECT sensor_id,sn_code,device_code FROM "{schemas.core}".sensors '
                        f'WHERE "{kind}"=:value ORDER BY sn_code'
                    ),
                    {"value": value},
                ).mappings().all()
            return {"query_kind": kind, "query_value": value, "rows": [dict(row) for row in rows]}
    finally:
        engine.dispose()
