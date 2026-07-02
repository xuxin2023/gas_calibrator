"""PostgreSQL repository for the V1.5 evidence registry."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from .bundle import TABLE_NAMES, build_traceability_summary_from_tables
from .schema import load_migrations


TABLE_COLUMNS: Dict[str, Sequence[str]] = {
    "runs": (
        "id",
        "run_id",
        "run_dir",
        "plan_id",
        "plan_version",
        "analyzer_id",
        "operator_name",
        "config_hash",
        "package_status",
        "package_blockers",
        "evidence_status",
        "metadata",
    ),
    "devices": (
        "id",
        "device_type",
        "device_role",
        "display_name",
        "serial_number",
        "metadata",
    ),
    "run_devices": ("id", "run_db_id", "device_id", "role", "metadata"),
    "standard_gases": (
        "id",
        "run_db_id",
        "component",
        "cylinder_id",
        "certificate_value",
        "certificate_uncertainty",
        "valid_until",
        "supplier",
        "certificate_hash",
        "metadata",
    ),
    "reference_certificates": (
        "id",
        "run_db_id",
        "device_id",
        "reference_role",
        "certificate_id",
        "certificate_hash",
        "valid_until",
        "uncertainty",
        "unit",
        "metadata",
    ),
    "calibration_points": (
        "id",
        "run_db_id",
        "component",
        "point_key",
        "point_tag",
        "pressure_mode",
        "target_value",
        "sample_count",
        "a_grade_count",
        "b_grade_count",
        "rejected_count",
        "metadata",
    ),
    "sample_files": (
        "id",
        "run_db_id",
        "artifact_role",
        "path",
        "sha256",
        "size_bytes",
        "modified_at",
        "required",
        "metadata",
    ),
    "qc_results": (
        "id",
        "run_db_id",
        "scope",
        "subject_id",
        "rule_name",
        "status",
        "severity",
        "reasons",
        "metrics",
        "source_artifact_id",
        "metadata",
    ),
    "coefficient_snapshots": (
        "id",
        "run_db_id",
        "analyzer_id",
        "snapshot_type",
        "coefficients",
        "coefficients_hash",
        "source_artifact_id",
        "metadata",
    ),
    "coefficient_candidates": (
        "id",
        "run_db_id",
        "component",
        "candidate_status",
        "allowed_for_review",
        "auto_write_allowed",
        "blockers",
        "coefficients",
        "source_artifact_id",
        "metadata",
    ),
    "coefficient_write_events": (
        "id",
        "run_db_id",
        "analyzer_id",
        "event_type",
        "status",
        "approved_by",
        "command_summary",
        "old_coefficients_hash",
        "candidate_id",
        "readback",
        "metadata",
    ),
    "reports": (
        "id",
        "run_db_id",
        "report_type",
        "path",
        "sha256",
        "status",
        "generated_at",
        "metadata",
    ),
    "audit_events": ("id", "run_db_id", "event_type", "actor", "event_at", "payload"),
    "evidence_integrity_checks": (
        "id",
        "run_db_id",
        "check_name",
        "status",
        "severity",
        "details",
    ),
}

JSONB_COLUMNS = {
    "package_blockers",
    "metadata",
    "reasons",
    "metrics",
    "coefficients",
    "blockers",
    "readback",
    "payload",
    "details",
}

CHILD_TABLES = (
    "run_devices",
    "standard_gases",
    "reference_certificates",
    "calibration_points",
    "sample_files",
    "qc_results",
    "coefficient_snapshots",
    "coefficient_candidates",
    "coefficient_write_events",
    "reports",
    "audit_events",
    "evidence_integrity_checks",
)


def _import_psycopg():
    try:
        import psycopg
        from psycopg.rows import dict_row
    except Exception as exc:  # pragma: no cover - depends on optional DB client installation
        raise RuntimeError("psycopg is required for PostgreSQL evidence registry operations") from exc
    return psycopg, dict_row


def _json_value(value: Any) -> str:
    if value is None:
        value = {} if isinstance(value, dict) else value
    return json.dumps(value, ensure_ascii=False, default=str)


def _row_values(row: Mapping[str, Any], columns: Sequence[str]) -> List[Any]:
    values: List[Any] = []
    for column in columns:
        value = row.get(column)
        if column in JSONB_COLUMNS:
            if value is None:
                value = [] if column in {"package_blockers", "reasons", "blockers"} else {}
            values.append(_json_value(value))
        else:
            values.append(value)
    return values


def _placeholder(column: str) -> str:
    return "%s::jsonb" if column in JSONB_COLUMNS else "%s"


def _upsert_sql(table: str, columns: Sequence[str]) -> str:
    column_sql = ", ".join(columns)
    placeholder_sql = ", ".join(_placeholder(column) for column in columns)
    updates = ", ".join(f"{column}=EXCLUDED.{column}" for column in columns if column != "id")
    if table in {"runs", "devices"}:
        updates = f"{updates}, updated_at=now()"
    return (
        f"INSERT INTO v1_5_evidence.{table} ({column_sql}) "
        f"VALUES ({placeholder_sql}) "
        f"ON CONFLICT (id) DO UPDATE SET {updates}"
    )


def apply_migrations(dsn: str) -> List[str]:
    """Create or update the V1.5 evidence registry schema."""

    psycopg, _ = _import_psycopg()
    applied: List[str] = []
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for migration in load_migrations():
                cur.execute(migration.sql)
                cur.execute(
                    """
                    INSERT INTO v1_5_evidence.schema_migrations (version, checksum)
                    VALUES (%s, %s)
                    ON CONFLICT (version) DO UPDATE
                    SET checksum = EXCLUDED.checksum, applied_at = now()
                    """,
                    (migration.version, migration.checksum),
                )
                applied.append(migration.version)
        conn.commit()
    return applied


def _delete_existing_child_rows(cur: Any, run_db_id: str) -> None:
    for table in CHILD_TABLES:
        cur.execute(f"DELETE FROM v1_5_evidence.{table} WHERE run_db_id = %s", (run_db_id,))


def _insert_rows(cur: Any, table: str, rows: Iterable[Mapping[str, Any]]) -> int:
    columns = TABLE_COLUMNS[table]
    sql = _upsert_sql(table, columns)
    count = 0
    for row in rows:
        cur.execute(sql, _row_values(row, columns))
        count += 1
    return count


def import_bundle(dsn: str, bundle: Mapping[str, Any], *, replace_existing: bool = True) -> Dict[str, Any]:
    """Import a V1.5 evidence bundle into PostgreSQL."""

    psycopg, _ = _import_psycopg()
    tables = bundle.get("tables")
    if not isinstance(tables, Mapping):
        raise ValueError("Evidence bundle is missing tables")
    run_db_id = str(bundle.get("run_db_id") or "")
    if not run_db_id:
        raise ValueError("Evidence bundle is missing run_db_id")

    inserted: Dict[str, int] = {}
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            if replace_existing:
                _delete_existing_child_rows(cur, run_db_id)
            for table in TABLE_NAMES:
                rows = tables.get(table) or []
                if table not in TABLE_COLUMNS:
                    continue
                inserted[table] = _insert_rows(cur, table, rows)
        conn.commit()
    return {"run_db_id": run_db_id, "inserted": inserted}


def query_run_summary(dsn: str, run_id: str) -> List[Dict[str, Any]]:
    """Return summary rows for a V1.5 run id or database id."""

    psycopg, dict_row = _import_psycopg()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM v1_5_evidence.run_evidence_summary
                WHERE run_id = %s OR id = %s
                ORDER BY updated_at DESC
                """,
                (run_id, run_id),
            )
            return [dict(row) for row in cur.fetchall()]


def _rows_by_run_db_id(cur: Any, table: str, run_db_id: str, *, order_by: str = "id") -> List[Dict[str, Any]]:
    cur.execute(
        f"SELECT * FROM v1_5_evidence.{table} WHERE run_db_id = %s ORDER BY {order_by}",
        (run_db_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def query_run_traceability(dsn: str, run_id: str) -> Dict[str, Any]:
    """Return the complete traceability view for a V1.5 run id or database id."""

    psycopg, dict_row = _import_psycopg()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM v1_5_evidence.runs
                WHERE run_id = %s OR id = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (run_id, run_id),
            )
            run = cur.fetchone()
            if not run:
                return {
                    "schema": "v1_5_evidence_registry",
                    "schema_version": "001",
                    "run_id": run_id,
                    "run_db_id": "",
                    "found": False,
                    "physical_boundaries": {
                        "sidecar_only": True,
                        "opens_com_ports": False,
                        "controls_water_or_gas_routes": False,
                        "controls_valves_or_pace": False,
                        "writes_coefficients": False,
                        "not_real_acceptance_evidence": True,
                    },
                }
            run_row = dict(run)
            run_db_id = str(run_row["id"])
            tables: Dict[str, List[Dict[str, Any]]] = {"runs": [run_row]}
            cur.execute(
                """
                SELECT d.*
                FROM v1_5_evidence.devices d
                JOIN v1_5_evidence.run_devices rd ON rd.device_id = d.id
                WHERE rd.run_db_id = %s
                ORDER BY d.device_role, d.display_name
                """,
                (run_db_id,),
            )
            tables["devices"] = [dict(row) for row in cur.fetchall()]
            tables["run_devices"] = _rows_by_run_db_id(cur, "run_devices", run_db_id, order_by="role, device_id")
            tables["standard_gases"] = _rows_by_run_db_id(cur, "standard_gases", run_db_id, order_by="component, cylinder_id")
            tables["reference_certificates"] = _rows_by_run_db_id(
                cur,
                "reference_certificates",
                run_db_id,
                order_by="reference_role, certificate_id",
            )
            tables["calibration_points"] = _rows_by_run_db_id(
                cur,
                "calibration_points",
                run_db_id,
                order_by="component, point_key",
            )
            tables["sample_files"] = _rows_by_run_db_id(
                cur,
                "sample_files",
                run_db_id,
                order_by="artifact_role, path",
            )
            tables["qc_results"] = _rows_by_run_db_id(
                cur,
                "qc_results",
                run_db_id,
                order_by="scope, rule_name, subject_id",
            )
            tables["coefficient_snapshots"] = _rows_by_run_db_id(cur, "coefficient_snapshots", run_db_id)
            tables["coefficient_candidates"] = _rows_by_run_db_id(
                cur,
                "coefficient_candidates",
                run_db_id,
                order_by="component, candidate_status",
            )
            tables["coefficient_write_events"] = _rows_by_run_db_id(
                cur,
                "coefficient_write_events",
                run_db_id,
                order_by="event_at, event_type",
            )
            tables["reports"] = _rows_by_run_db_id(cur, "reports", run_db_id, order_by="report_type, path")
            tables["audit_events"] = _rows_by_run_db_id(cur, "audit_events", run_db_id, order_by="event_at, event_type")
            tables["evidence_integrity_checks"] = _rows_by_run_db_id(
                cur,
                "evidence_integrity_checks",
                run_db_id,
                order_by="check_name, status",
            )
            summary = build_traceability_summary_from_tables(
                tables,
                run_id=str(run_row.get("run_id") or run_id),
                run_db_id=run_db_id,
            )
            summary["found"] = True
            return summary


def query_artifacts_by_sha256(dsn: str, sha256: str) -> List[Dict[str, Any]]:
    """Return artifact and report index rows with the given SHA256 hash."""

    psycopg, dict_row = _import_psycopg()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    r.run_id,
                    r.id AS run_db_id,
                    'sample_files' AS source_table,
                    f.artifact_role,
                    NULL::text AS report_type,
                    f.path,
                    f.sha256,
                    f.required,
                    f.size_bytes,
                    f.metadata
                FROM v1_5_evidence.sample_files f
                JOIN v1_5_evidence.runs r ON r.id = f.run_db_id
                WHERE f.sha256 = %s
                UNION ALL
                SELECT
                    r.run_id,
                    r.id AS run_db_id,
                    'reports' AS source_table,
                    rep.report_type AS artifact_role,
                    rep.report_type,
                    rep.path,
                    rep.sha256,
                    FALSE AS required,
                    NULL::bigint AS size_bytes,
                    rep.metadata
                FROM v1_5_evidence.reports rep
                JOIN v1_5_evidence.runs r ON r.id = rep.run_db_id
                WHERE rep.sha256 = %s
                ORDER BY run_id DESC, source_table, artifact_role, path
                """,
                (sha256, sha256),
            )
            return [dict(row) for row in cur.fetchall()]


def mask_dsn(dsn: str) -> str:
    """Return a DSN with password text masked for logs and CLI output."""

    text = str(dsn or "")
    if "://" not in text or "@" not in text:
        return "<configured>" if text else ""
    prefix, rest = text.split("://", 1)
    auth, host = rest.split("@", 1)
    if ":" in auth:
        user = auth.split(":", 1)[0]
        return f"{prefix}://{user}:***@{host}"
    return f"{prefix}://{auth}@{host}"
