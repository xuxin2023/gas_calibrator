"""Authorize and execute the fixed V1.5 PostgreSQL 18 migration 002."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ..storage.v1_5_evidence.production_migration import (
    SCRIPT_ROLES,
    ProductionMigrationError,
    execute_production_migration_002,
)
from ..storage.v1_5_evidence.schema import load_migrations
from .v1_5_formal_database_migration_dba_readiness import (
    EXPECTED_VERSIONS,
    PRODUCTION_DATABASE,
    PRODUCTION_DSN_ENV,
    PRODUCTION_EVIDENCE_SCHEMA,
    READY_STATUS,
    SCHEMA as READINESS_SCHEMA,
    build_v1_5_formal_database_migration_dba_readiness,
)


SCHEMA = "v1_5_formal_database_migration_production_controlled_executor_v1"
AUTHORIZATION_SCHEMA = (
    "v1_5_formal_database_migration_production_execution_authorization_v1"
)
CONFIRMATION_TEMPLATE = "v1_5_postgresql18_migration_002_reviewed_v1"
EXECUTE_FLAG = "--execute-postgresql18-migration"
AUTHORIZATION_PREFLIGHT_STATUS = (
    "ready_for_postgresql18_migration_execution_operator_handoff"
)
MAX_AUTHORIZATION_LIFETIME_SECONDS = 24 * 60 * 60


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return str(path)


def _snapshot(path: Path, role: str) -> tuple[bytes, str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ProductionMigrationError(f"{role}_read_failed") from exc
    return raw, hashlib.sha256(raw).hexdigest()


def _json_snapshot(path: Path, role: str) -> tuple[dict[str, Any], str]:
    raw, digest = _snapshot(path, role)
    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProductionMigrationError(f"{role}_json_invalid") from exc
    if not isinstance(payload, dict):
        raise ProductionMigrationError(f"{role}_json_object_required")
    return payload, digest


def _parse_timestamp(value: Any, role: str) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ProductionMigrationError(
            f"migration_execution_authorization_{role}_invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise ProductionMigrationError(
            f"migration_execution_authorization_{role}_timezone_required"
        )
    return parsed.astimezone(timezone.utc)


def _binding_map(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    rows = payload.get("source_bindings")
    if not isinstance(rows, list):
        return {}
    return {
        str(row.get("role") or ""): row
        for row in rows
        if isinstance(row, Mapping) and str(row.get("role") or "")
    }


def _expected_target() -> dict[str, Any]:
    return {
        "backend": "postgresql",
        "postgresql_major": 18,
        "database_name": PRODUCTION_DATABASE,
        "core_schema": "public",
        "evidence_schema": PRODUCTION_EVIDENCE_SCHEMA,
        "dsn_env": PRODUCTION_DSN_ENV,
    }


def build_migration_execution_preview(
    *,
    dba_readiness_json: str | Path,
    precheck_sql: str | Path,
    apply_sql: str | Path,
    postcheck_sql: str | Path,
) -> dict[str, Any]:
    paths = {
        "dba_readiness": Path(dba_readiness_json).resolve(),
        "precheck_sql": Path(precheck_sql).resolve(),
        "apply_sql": Path(apply_sql).resolve(),
        "postcheck_sql": Path(postcheck_sql).resolve(),
    }
    reasons: list[str] = []
    readiness: dict[str, Any] = {}
    source_bindings: list[dict[str, str]] = []
    try:
        readiness, readiness_sha256 = _json_snapshot(
            paths["dba_readiness"], "dba_readiness"
        )
        source_bindings.append(
            {
                "role": "dba_readiness",
                "path": _display_path(paths["dba_readiness"]),
                "sha256": readiness_sha256,
            }
        )
        if readiness.get("schema") != READINESS_SCHEMA:
            reasons.append("dba_readiness_schema_invalid")
        if readiness.get("overall_status") != READY_STATUS:
            reasons.append("dba_readiness_not_ready")
        if readiness.get("dba_packet_ready") is not True:
            reasons.append("dba_packet_not_ready")
        if readiness.get("production_target") != _expected_target():
            reasons.append("dba_readiness_production_target_invalid")
        for field in (
            "connects_postgresql",
            "dsn_value_read",
            "applies_migrations",
            "migration_execution_allowed",
            "production_import_execution_allowed",
            "database_written",
            "database_import_allowed",
            "formal_release_allowed",
        ):
            if readiness.get(field) is not False:
                reasons.append(f"dba_readiness_boundary_{field}_invalid")

        migrations = readiness.get("migrations")
        expected_migrations = load_migrations()
        if not isinstance(migrations, list) or [
            row.get("version") for row in migrations if isinstance(row, Mapping)
        ] != list(EXPECTED_VERSIONS):
            reasons.append("dba_readiness_migration_sequence_invalid")
        else:
            expected_checksums = {row.version: row.checksum for row in expected_migrations}
            for row in migrations:
                version = str(row.get("version") or "")
                if str(row.get("sha256") or "") != expected_checksums.get(version):
                    reasons.append(f"dba_readiness_migration_checksum_invalid:{version}")

        scripts = readiness.get("scripts")
        recorded_hashes = readiness.get("script_sha256")
        if not isinstance(scripts, Mapping) or not isinstance(recorded_hashes, Mapping):
            reasons.append("dba_readiness_script_bindings_missing")
        else:
            for role in SCRIPT_ROLES:
                raw, digest = _snapshot(paths[role], role)
                source_bindings.append(
                    {
                        "role": role,
                        "path": _display_path(paths[role]),
                        "sha256": digest,
                    }
                )
                try:
                    current_sql = raw.decode("utf-8-sig")
                except UnicodeDecodeError:
                    reasons.append(f"{role}_utf8_invalid")
                    continue
                if current_sql != str(scripts.get(role) or ""):
                    reasons.append(f"{role}_content_mismatch_with_readiness")
                if digest != str(recorded_hashes.get(role) or ""):
                    reasons.append(f"{role}_sha256_mismatch_with_readiness")

        rebuilt = build_v1_5_formal_database_migration_dba_readiness()
        if rebuilt.get("overall_status") != READY_STATUS:
            reasons.append("dba_readiness_rebuild_not_ready")
        if rebuilt.get("script_sha256") != readiness.get("script_sha256"):
            reasons.append("dba_readiness_rebuild_script_sha256_mismatch")
        if rebuilt.get("migrations") != readiness.get("migrations"):
            reasons.append("dba_readiness_rebuild_migrations_mismatch")
    except Exception as exc:
        reasons.append(f"migration_execution_input_load_failed:{type(exc).__name__}")

    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": (
            "ready_for_postgresql18_migration_execution_authorization_review"
            if ready
            else "review_required"
        ),
        "blocker_count": len(reasons),
        "review_required_count": 0,
        "migration_execution_package_ready": ready,
        "export_status": "ok" if ready else "error",
        "production_state": "migration_execution_locked_pending_fresh_authorization",
        "production_target": _expected_target(),
        "migration_versions": list(EXPECTED_VERSIONS),
        "source_bindings": source_bindings,
        "reasons": reasons,
        "execute_flag_required": EXECUTE_FLAG,
        "execution_authorization_schema": AUTHORIZATION_SCHEMA,
        "operator_confirmation_template": CONFIRMATION_TEMPLATE,
        "three_distinct_actors_required": True,
        "authorization_max_lifetime_seconds": MAX_AUTHORIZATION_LIFETIME_SECONDS,
        "authorization_validation_requested": False,
        "authorization_validated": False,
        "dsn_value_read": False,
        "execution_attempted": False,
        "connects_postgresql": False,
        "applies_migrations": False,
        "migration_execution_confirmed": False,
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
        "evidence_source": "postgresql18_migration_locked_preview",
    }


def validate_migration_execution_authorization(
    *,
    execution_authorization_json: str | Path,
    preview: Mapping[str, Any],
    source_paths: Mapping[str, str | Path],
    now: datetime | None = None,
) -> dict[str, Any]:
    if preview.get("migration_execution_package_ready") is not True:
        raise ProductionMigrationError("migration_execution_package_not_ready")
    path = Path(execution_authorization_json).resolve()
    payload, authorization_sha256 = _json_snapshot(
        path, "migration_execution_authorization"
    )
    if payload.get("schema") != AUTHORIZATION_SCHEMA:
        raise ProductionMigrationError("migration_execution_authorization_schema_invalid")
    if payload.get("requested_flag") != EXECUTE_FLAG:
        raise ProductionMigrationError(
            "migration_execution_authorization_requested_flag_invalid"
        )
    if payload.get("confirmation_template") != CONFIRMATION_TEMPLATE:
        raise ProductionMigrationError(
            "migration_execution_authorization_confirmation_template_invalid"
        )
    if payload.get("operator_confirmed") is not True:
        raise ProductionMigrationError(
            "migration_execution_authorization_operator_confirmation_missing"
        )
    identity = {
        field: str(payload.get(field) or "").strip()
        for field in ("authorization_id", "operator", "reviewer", "approver")
    }
    if not all(identity.values()):
        raise ProductionMigrationError(
            "migration_execution_authorization_identity_fields_missing"
        )
    if len({identity[field].casefold() for field in ("operator", "reviewer", "approver")}) != 3:
        raise ProductionMigrationError(
            "migration_execution_authorization_three_distinct_actors_required"
        )

    issued_at = _parse_timestamp(payload.get("issued_at"), "issued_at")
    expires_at = _parse_timestamp(payload.get("expires_at"), "expires_at")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    lifetime = (expires_at - issued_at).total_seconds()
    if lifetime <= 0 or lifetime > MAX_AUTHORIZATION_LIFETIME_SECONDS:
        raise ProductionMigrationError(
            "migration_execution_authorization_lifetime_invalid"
        )
    if current < issued_at or current > expires_at:
        raise ProductionMigrationError(
            "migration_execution_authorization_not_current"
        )
    if payload.get("production_target") != _expected_target():
        raise ProductionMigrationError(
            "migration_execution_authorization_production_target_invalid"
        )
    required_boundaries = {
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
    }
    boundaries = payload.get("boundaries")
    if not isinstance(boundaries, Mapping) or any(
        boundaries.get(key) is not value for key, value in required_boundaries.items()
    ):
        raise ProductionMigrationError(
            "migration_execution_authorization_boundaries_invalid"
        )

    bindings = _binding_map(payload)
    source_sha256: dict[str, str] = {}
    for role, raw_path in source_paths.items():
        current_path = Path(raw_path).resolve()
        binding = bindings.get(role)
        if not binding:
            raise ProductionMigrationError(
                f"migration_execution_authorization_source_binding_missing:{role}"
            )
        if Path(str(binding.get("path") or "")).resolve() != current_path:
            raise ProductionMigrationError(
                f"migration_execution_authorization_source_path_mismatch:{role}"
            )
        _, digest = _snapshot(current_path, role)
        if str(binding.get("sha256") or "").lower() != digest:
            raise ProductionMigrationError(
                f"migration_execution_authorization_source_sha256_mismatch:{role}"
            )
        source_sha256[role] = digest
    return {
        **identity,
        "issued_at": issued_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "authorization_path": str(path),
        "authorization_sha256": authorization_sha256,
        "source_sha256": source_sha256,
        "confirmation_matched": True,
        "three_distinct_actors": True,
    }


def authorization_blocked_model(
    preview: Mapping[str, Any], reason: str
) -> dict[str, Any]:
    return {
        **dict(preview),
        "generated_at": _now(),
        "overall_status": "migration_execution_authorization_blocked",
        "export_status": "error",
        "blocker_count": 1,
        "reasons": list(preview.get("reasons") or []) + [reason],
        "authorization_validation_requested": True,
        "authorization_validated": False,
        "dsn_value_read": False,
        "execution_attempted": False,
        "connects_postgresql": False,
        "applies_migrations": False,
        "migration_execution_confirmed": False,
        "database_written": False,
        "database_import_allowed": False,
    }


def authorization_validated_model(
    preview: Mapping[str, Any], authorization: Mapping[str, Any]
) -> dict[str, Any]:
    """Record a fresh authorization review without reading a DSN or connecting."""

    return {
        **dict(preview),
        "generated_at": _now(),
        "overall_status": AUTHORIZATION_PREFLIGHT_STATUS,
        "production_state": "migration_execution_authorization_validated_no_connect",
        "export_status": "ok",
        "blocker_count": 0,
        "reasons": [],
        "authorization_validation_requested": True,
        "authorization_validated": True,
        "authorization_record": dict(authorization),
        "dsn_value_read": False,
        "execution_attempted": False,
        "connects_postgresql": False,
        "applies_migrations": False,
        "migration_execution_confirmed": False,
        "database_written": False,
        "production_import_execution_allowed": False,
        "database_import_allowed": False,
        "formal_release_allowed": False,
        "not_real_acceptance_evidence": True,
        "evidence_source": "postgresql18_migration_authorization_no_connect_preflight",
    }


def execution_preconnect_hold_model(
    preview: Mapping[str, Any], reason: str
) -> dict[str, Any]:
    return {
        **dict(preview),
        "generated_at": _now(),
        "overall_status": "migration_execution_preconnect_hold",
        "export_status": "error",
        "blocker_count": 1,
        "reasons": list(preview.get("reasons") or []) + [reason],
        "dsn_value_read": True,
        "execution_attempted": False,
        "connects_postgresql": False,
        "applies_migrations": False,
        "migration_execution_confirmed": False,
        "database_written": False,
        "database_import_allowed": False,
        "formal_release_allowed": False,
    }


def execute_reviewed_production_migration(
    *,
    dba_readiness_json: str | Path,
    precheck_sql: str | Path,
    apply_sql: str | Path,
    postcheck_sql: str | Path,
    execution_authorization_json: str | Path,
    dsn: str,
    migration_runner: Callable[..., dict[str, Any]] = execute_production_migration_002,
) -> dict[str, Any]:
    source_paths = {
        "dba_readiness": Path(dba_readiness_json).resolve(),
        "precheck_sql": Path(precheck_sql).resolve(),
        "apply_sql": Path(apply_sql).resolve(),
        "postcheck_sql": Path(postcheck_sql).resolve(),
    }
    preview = build_migration_execution_preview(
        dba_readiness_json=source_paths["dba_readiness"],
        precheck_sql=source_paths["precheck_sql"],
        apply_sql=source_paths["apply_sql"],
        postcheck_sql=source_paths["postcheck_sql"],
    )
    authorization = validate_migration_execution_authorization(
        execution_authorization_json=execution_authorization_json,
        preview=preview,
        source_paths=source_paths,
    )

    snapshots: dict[str, bytes] = {}
    snapshot_sha256: dict[str, str] = {}
    for role, path in source_paths.items():
        raw, digest = _snapshot(path, role)
        if digest != authorization["source_sha256"][role]:
            raise ProductionMigrationError(
                f"migration_execution_source_changed_after_authorization:{role}"
            )
        snapshots[role] = raw
        snapshot_sha256[role] = digest

    revalidated = build_migration_execution_preview(
        dba_readiness_json=source_paths["dba_readiness"],
        precheck_sql=source_paths["precheck_sql"],
        apply_sql=source_paths["apply_sql"],
        postcheck_sql=source_paths["postcheck_sql"],
    )
    if revalidated.get("migration_execution_package_ready") is not True:
        raise ProductionMigrationError(
            "migration_execution_package_changed_after_authorization"
        )
    current_hashes = {
        str(row.get("role") or ""): str(row.get("sha256") or "")
        for row in revalidated.get("source_bindings") or []
        if isinstance(row, Mapping)
    }
    if any(current_hashes.get(role) != digest for role, digest in snapshot_sha256.items()):
        raise ProductionMigrationError(
            "migration_execution_source_changed_during_revalidation"
        )

    readiness = json.loads(snapshots["dba_readiness"].decode("utf-8-sig"))
    scripts = {
        role: snapshots[role].decode("utf-8-sig") for role in SCRIPT_ROLES
    }
    result = migration_runner(
        dsn=dsn,
        scripts=scripts,
        expected_script_sha256=readiness["script_sha256"],
        readiness_sha256=snapshot_sha256["dba_readiness"],
        execution_authorization_sha256=authorization["authorization_sha256"],
        authorization_id=authorization["authorization_id"],
        operator=authorization["operator"],
        reviewer=authorization["reviewer"],
        approver=authorization["approver"],
    )
    confirmed = result.get("migration_execution_confirmed") is True
    return {
        **revalidated,
        **result,
        "generated_at": _now(),
        "authorization_validation_requested": True,
        "authorization_validated": True,
        "overall_status": str(result.get("status") or "migration_execution_failed"),
        "production_state": (
            "migration_002_execution_confirmed"
            if confirmed
            else "migration_002_execution_hold"
        ),
        "export_status": "ok" if confirmed else "error",
        "blocker_count": 0 if confirmed else 1,
        "reasons": (
            []
            if confirmed
            else [str(result.get("failure_reason") or "migration_execution_failed")]
        ),
        "dsn_value_read": True,
        "execution_attempted": True,
        "connects_postgresql": result.get("connection_attempted") is True,
        "applies_migrations": result.get("transaction_started") is True,
        "migration_execution_confirmed": confirmed,
        "production_import_execution_allowed": False,
        "database_import_allowed": False,
        "formal_release_allowed": False,
        "not_real_acceptance_evidence": True,
        "evidence_source": "postgresql18_migration_002_controlled_execution",
        "authorization_record": authorization,
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


def write_migration_execution_outputs(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out
        / "v1_5_formal_database_migration_production_controlled_executor.json",
        "summary_csv": out
        / "v1_5_formal_database_migration_production_summary.csv",
        "source_bindings_csv": out
        / "v1_5_formal_database_migration_production_source_bindings.csv",
        "precheck_output_json": out / "v1_5_postgresql18_migration_precheck_output.json",
        "apply_output_json": out / "v1_5_postgresql18_migration_apply_output.json",
        "postcheck_output_json": out / "v1_5_postgresql18_migration_postcheck_output.json",
        "authorization_template": out
        / "v1_5_postgresql18_migration_execution_authorization_template.json",
        "markdown": out
        / "V1_5_FORMAL_DATABASE_MIGRATION_PRODUCTION_CONTROLLED_EXECUTOR.md",
    }
    outputs["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    _write_csv(
        outputs["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "authorization_validated": model.get("authorization_validated"),
                "execution_attempted": model.get("execution_attempted"),
                "connects_postgresql": model.get("connects_postgresql"),
                "transaction_started": model.get("transaction_started"),
                "transaction_committed": model.get("transaction_committed"),
                "commit_uncertain": model.get("commit_uncertain"),
                "idempotent": model.get("idempotent"),
                "migration_execution_confirmed": model.get(
                    "migration_execution_confirmed"
                ),
                "database_written": model.get("database_written"),
                "database_import_allowed": model.get("database_import_allowed"),
            }
        ],
    )
    _write_csv(outputs["source_bindings_csv"], model.get("source_bindings") or [])
    for key, field in (
        ("precheck_output_json", "precheck_output"),
        ("apply_output_json", "apply_output"),
        ("postcheck_output_json", "postcheck_output"),
    ):
        outputs[key].write_text(
            json.dumps(model.get(field) or [], ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    outputs["authorization_template"].write_text(
        json.dumps(
            {
                "schema": AUTHORIZATION_SCHEMA,
                "template_only": True,
                "requested_flag": EXECUTE_FLAG,
                "confirmation_template": CONFIRMATION_TEMPLATE,
                "operator_confirmed": False,
                "authorization_id": "",
                "operator": "",
                "reviewer": "",
                "approver": "",
                "issued_at": "",
                "expires_at": "",
                "production_target": _expected_target(),
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
                "source_bindings": model.get("source_bindings") or [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 PostgreSQL 18 migration 002 controlled executor",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- authorization_validated: `{model.get('authorization_validated')}`",
                f"- execution_attempted: `{model.get('execution_attempted')}`",
                f"- connects_postgresql: `{model.get('connects_postgresql')}`",
                f"- transaction_committed: `{model.get('transaction_committed')}`",
                f"- commit_uncertain: `{model.get('commit_uncertain')}`",
                f"- migration_execution_confirmed: `{model.get('migration_execution_confirmed')}`",
                f"- database_import_allowed: `{model.get('database_import_allowed')}`",
                f"- formal_release_allowed: `{model.get('formal_release_allowed')}`",
                "",
                "The executor is fixed to PostgreSQL 18 database gas_calibrator and migration 002.",
                "Authorization-only validation never reads the DSN and never opens a database connection.",
                "It never imports calibration evidence and never controls analyzers or routes.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return outputs
