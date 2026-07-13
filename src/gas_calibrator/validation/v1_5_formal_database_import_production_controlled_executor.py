"""Guard and execute the fixed-target V1.5 PostgreSQL 18 production import."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ..storage.v1_5_evidence.production_import import (
    PRODUCTION_CORE_SCHEMA,
    PRODUCTION_DATABASE_NAME,
    PRODUCTION_EVIDENCE_SCHEMA,
    ProductionImportError,
    execute_production_import,
)
from ..storage.v1_5_evidence.staging_import import (
    load_json_object,
    sha256_file,
    validate_staging_package,
)
from .v1_5_formal_database_import_production_promotion_preflight import (
    DEFAULT_PRODUCTION_DSN_ENV,
    READY_STATUS as PROMOTION_READY_STATUS,
    build_v1_5_formal_database_import_production_promotion_preflight,
)


SCHEMA = "v1_5_formal_database_import_production_controlled_executor_v1"
AUTHORIZATION_SCHEMA = (
    "v1_5_formal_database_import_production_execution_authorization_v1"
)
CONFIRMATION_TEMPLATE = "v1_5_postgresql18_production_import_reviewed_v1"
EXECUTE_FLAG = "--execute-production-import"
MAX_AUTHORIZATION_LIFETIME_SECONDS = 24 * 60 * 60


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return str(path)


def _load(path: Path) -> dict[str, Any]:
    return load_json_object(path)


def _binding_map(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    rows = payload.get("source_bindings")
    if not isinstance(rows, list):
        return {}
    return {
        str(row.get("role") or ""): row
        for row in rows
        if isinstance(row, Mapping) and str(row.get("role") or "")
    }


def _parse_timestamp(value: Any, role: str) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ProductionImportError(f"execution_authorization_{role}_invalid") from exc
    if parsed.tzinfo is None:
        raise ProductionImportError(f"execution_authorization_{role}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _promotion_reasons(
    promotion_path: Path,
    plan_path: Path,
    bundle_path: Path,
) -> tuple[list[str], dict[str, Any], list[dict[str, str]]]:
    reasons: list[str] = []
    promotion = _load(promotion_path)
    plan = _load(plan_path)
    bundle = _load(bundle_path)
    devices: list[dict[str, str]] = []

    if promotion.get("schema") != (
        "v1_5_formal_database_import_production_promotion_preflight_v1"
    ):
        reasons.append("promotion_preflight_schema_invalid")
    if promotion.get("overall_status") != PROMOTION_READY_STATUS:
        reasons.append("promotion_preflight_not_ready")
    if promotion.get("production_import_executor_review_allowed") is not True:
        reasons.append("promotion_executor_review_not_allowed")
    for field in (
        "production_import_execution_allowed",
        "connects_postgresql",
        "applies_migrations",
        "database_written",
        "production_database_written",
        "database_import_allowed",
        "formal_release_allowed",
    ):
        if promotion.get(field) is not False:
            reasons.append(f"promotion_boundary_{field}={promotion.get(field)!r}")
    if promotion.get("production_dsn_env") != DEFAULT_PRODUCTION_DSN_ENV:
        reasons.append("promotion_production_dsn_env_invalid")

    recorded_paths = promotion.get("input_paths")
    recorded_hashes = promotion.get("input_sha256")
    if not isinstance(recorded_paths, Mapping) or not isinstance(recorded_hashes, Mapping):
        reasons.append("promotion_input_bindings_missing")
    else:
        for role, current_path in (
            ("transaction_plan", plan_path),
            ("evidence_bundle", bundle_path),
        ):
            recorded_path = Path(str(recorded_paths.get(role) or "")).resolve()
            if recorded_path != current_path:
                reasons.append(f"promotion_{role}_path_mismatch")
            if str(recorded_hashes.get(role) or "").lower() != sha256_file(
                current_path
            ).lower():
                reasons.append(f"promotion_{role}_sha256_mismatch")

    try:
        devices = validate_staging_package(plan, bundle)
    except Exception as exc:
        reasons.append(str(exc))

    staging_path = None
    if isinstance(recorded_paths, Mapping):
        raw_staging_path = str(recorded_paths.get("staging_import") or "").strip()
        staging_path = Path(raw_staging_path).resolve() if raw_staging_path else None
    if not staging_path or not staging_path.is_file():
        reasons.append("promotion_staging_source_missing")
    else:
        try:
            rebuilt = build_v1_5_formal_database_import_production_promotion_preflight(
                staging_import_json=staging_path,
                transaction_plan_json=plan_path,
                evidence_bundle_json=bundle_path,
                production_dsn_env=DEFAULT_PRODUCTION_DSN_ENV,
            )
            if rebuilt.get("overall_status") != PROMOTION_READY_STATUS:
                reasons.append("promotion_preflight_rebuild_not_ready")
            for field in (
                "run_id",
                "run_db_id",
                "planned_device_count",
                "table_counts",
            ):
                if rebuilt.get(field) != promotion.get(field):
                    reasons.append(f"promotion_rebuild_{field}_mismatch")
        except Exception as exc:
            reasons.append(f"promotion_preflight_rebuild_failed:{type(exc).__name__}")
    return reasons, promotion, devices


def build_production_import_preview(
    *,
    promotion_preflight_json: str | Path,
    transaction_plan_json: str | Path,
    evidence_bundle_json: str | Path,
) -> dict[str, Any]:
    paths = {
        "promotion_preflight": Path(promotion_preflight_json).resolve(),
        "transaction_plan": Path(transaction_plan_json).resolve(),
        "evidence_bundle": Path(evidence_bundle_json).resolve(),
    }
    reasons: list[str] = []
    promotion: dict[str, Any] = {}
    devices: list[dict[str, str]] = []
    try:
        reasons, promotion, devices = _promotion_reasons(
            paths["promotion_preflight"],
            paths["transaction_plan"],
            paths["evidence_bundle"],
        )
    except Exception as exc:
        reasons.append(f"production_import_input_load_failed:{type(exc).__name__}")
    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": (
            "ready_for_postgresql18_production_import_execution_authorization_review"
            if ready
            else "review_required"
        ),
        "blocker_count": len(reasons),
        "review_required_count": 0,
        "production_import_package_ready": ready,
        "export_status": "ok" if ready else "error",
        "production_state": "production_execution_locked_pending_fresh_authorization",
        "production_backend": "postgresql",
        "production_postgresql_major": 18,
        "production_database_name": PRODUCTION_DATABASE_NAME,
        "production_core_schema": PRODUCTION_CORE_SCHEMA,
        "production_evidence_schema": PRODUCTION_EVIDENCE_SCHEMA,
        "production_dsn_env": DEFAULT_PRODUCTION_DSN_ENV,
        "dsn_value_read": False,
        "planned_device_count": len(devices),
        "planned_devices": devices,
        "run_id": str(promotion.get("run_id") or ""),
        "run_db_id": str(promotion.get("run_db_id") or ""),
        "table_counts": dict(promotion.get("table_counts") or {}),
        "source_bindings": [
            {
                "role": role,
                "path": _display_path(path),
                "sha256": sha256_file(path) if path.is_file() else "",
            }
            for role, path in paths.items()
        ],
        "reasons": reasons,
        "execute_flag_required": EXECUTE_FLAG,
        "execution_authorization_schema": AUTHORIZATION_SCHEMA,
        "operator_confirmation_template": CONFIRMATION_TEMPLATE,
        "three_distinct_actors_required": True,
        "authorization_max_lifetime_seconds": MAX_AUTHORIZATION_LIFETIME_SECONDS,
        "applies_migrations": False,
        "production_import_execution_allowed": False,
        "execution_attempted": False,
        "connects_postgresql": False,
        "production_database_written": False,
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
        "evidence_source": "production_import_locked_preview",
        "artifact_roles": {
            "execution_summary": [
                "v1_5_formal_database_import_production_controlled_executor.json",
                "v1_5_formal_database_import_production_summary.csv",
            ],
            "execution_rows": [
                "v1_5_formal_database_import_production_identity_readback.csv",
                "v1_5_formal_database_import_production_table_counts.csv",
            ],
            "diagnostic_analysis": [
                "v1_5_formal_database_import_production_source_bindings.csv"
            ],
            "formal_analysis": [
                "V1_5_FORMAL_DATABASE_IMPORT_PRODUCTION_CONTROLLED_EXECUTOR.md"
            ],
        },
    }


def validate_execution_authorization(
    *,
    execution_authorization_json: str | Path,
    preview: Mapping[str, Any],
    promotion_preflight_json: str | Path,
    transaction_plan_json: str | Path,
    evidence_bundle_json: str | Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    if preview.get("production_import_package_ready") is not True:
        raise ProductionImportError("production_import_package_not_ready")
    path = Path(execution_authorization_json).resolve()
    payload = _load(path)
    if payload.get("schema") != AUTHORIZATION_SCHEMA:
        raise ProductionImportError("execution_authorization_schema_invalid")
    if payload.get("requested_flag") != EXECUTE_FLAG:
        raise ProductionImportError("execution_authorization_requested_flag_invalid")
    if payload.get("confirmation_template") != CONFIRMATION_TEMPLATE:
        raise ProductionImportError("execution_authorization_confirmation_template_invalid")
    if payload.get("operator_confirmed") is not True:
        raise ProductionImportError("execution_authorization_operator_confirmation_missing")

    identity = {
        field: str(payload.get(field) or "").strip()
        for field in ("authorization_id", "operator", "reviewer", "approver")
    }
    if not all(identity.values()):
        raise ProductionImportError("execution_authorization_identity_fields_missing")
    if len({identity[field].casefold() for field in ("operator", "reviewer", "approver")}) != 3:
        raise ProductionImportError("execution_authorization_three_distinct_actors_required")

    issued_at = _parse_timestamp(payload.get("issued_at"), "issued_at")
    expires_at = _parse_timestamp(payload.get("expires_at"), "expires_at")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    lifetime = (expires_at - issued_at).total_seconds()
    if lifetime <= 0 or lifetime > MAX_AUTHORIZATION_LIFETIME_SECONDS:
        raise ProductionImportError("execution_authorization_lifetime_invalid")
    if current < issued_at or current > expires_at:
        raise ProductionImportError("execution_authorization_not_current")

    target = payload.get("production_target")
    expected_target = {
        "backend": "postgresql",
        "postgresql_major": 18,
        "dsn_env": DEFAULT_PRODUCTION_DSN_ENV,
        "database_name": PRODUCTION_DATABASE_NAME,
        "core_schema": PRODUCTION_CORE_SCHEMA,
        "evidence_schema": PRODUCTION_EVIDENCE_SCHEMA,
    }
    if not isinstance(target, Mapping) or any(
        target.get(key) != value for key, value in expected_target.items()
    ):
        raise ProductionImportError("execution_authorization_production_target_invalid")

    boundaries = payload.get("boundaries")
    required_boundaries = {
        "production_database_import": True,
        "applies_migrations": False,
        "opens_com_ports": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "grants_formal_release": False,
    }
    if not isinstance(boundaries, Mapping) or any(
        boundaries.get(key) is not value for key, value in required_boundaries.items()
    ):
        raise ProductionImportError("execution_authorization_boundaries_invalid")

    bindings = _binding_map(payload)
    for role, raw_path in (
        ("promotion_preflight", promotion_preflight_json),
        ("transaction_plan", transaction_plan_json),
        ("evidence_bundle", evidence_bundle_json),
    ):
        current_path = Path(raw_path).resolve()
        binding = bindings.get(role)
        if not binding:
            raise ProductionImportError(
                f"execution_authorization_source_binding_missing:{role}"
            )
        if Path(str(binding.get("path") or "")).resolve() != current_path:
            raise ProductionImportError(
                f"execution_authorization_source_path_mismatch:{role}"
            )
        if str(binding.get("sha256") or "").lower() != sha256_file(current_path).lower():
            raise ProductionImportError(
                f"execution_authorization_source_sha256_mismatch:{role}"
            )
    return {
        **identity,
        "issued_at": issued_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "authorization_path": str(path),
        "authorization_sha256": sha256_file(path),
        "confirmation_matched": True,
        "three_distinct_actors": True,
    }


def authorization_blocked_model(
    preview: Mapping[str, Any], reason: str
) -> dict[str, Any]:
    return {
        **dict(preview),
        "generated_at": _now(),
        "overall_status": "production_import_authorization_blocked",
        "export_status": "error",
        "blocker_count": 1,
        "reasons": list(preview.get("reasons") or []) + [reason],
        "dsn_value_read": False,
        "production_import_execution_allowed": False,
        "execution_attempted": False,
        "connects_postgresql": False,
        "production_database_written": False,
        "database_written": False,
        "database_import_allowed": False,
    }


def execute_reviewed_production_import(
    *,
    promotion_preflight_json: str | Path,
    transaction_plan_json: str | Path,
    evidence_bundle_json: str | Path,
    execution_authorization_json: str | Path,
    dsn: str,
    transaction_runner: Callable[..., dict[str, Any]] = execute_production_import,
    failure_injector=None,
) -> dict[str, Any]:
    preview = build_production_import_preview(
        promotion_preflight_json=promotion_preflight_json,
        transaction_plan_json=transaction_plan_json,
        evidence_bundle_json=evidence_bundle_json,
    )
    authorization = validate_execution_authorization(
        execution_authorization_json=execution_authorization_json,
        preview=preview,
        promotion_preflight_json=promotion_preflight_json,
        transaction_plan_json=transaction_plan_json,
        evidence_bundle_json=evidence_bundle_json,
    )
    plan_path = Path(transaction_plan_json).resolve()
    bundle_path = Path(evidence_bundle_json).resolve()
    promotion_path = Path(promotion_preflight_json).resolve()
    result = transaction_runner(
        dsn=dsn,
        transaction_plan=_load(plan_path),
        evidence_bundle=_load(bundle_path),
        transaction_plan_sha256=sha256_file(plan_path),
        evidence_bundle_sha256=sha256_file(bundle_path),
        promotion_preflight_sha256=sha256_file(promotion_path),
        execution_authorization_sha256=authorization["authorization_sha256"],
        authorization_id=authorization["authorization_id"],
        operator=authorization["operator"],
        reviewer=authorization["reviewer"],
        approver=authorization["approver"],
        failure_injector=failure_injector,
    )
    committed = result.get("transaction_committed") is True
    return {
        **preview,
        **result,
        "generated_at": _now(),
        "overall_status": str(result.get("status") or "production_import_failed"),
        "production_state": (
            "production_import_transaction_committed"
            if committed
            else "production_import_transaction_hold"
        ),
        "export_status": "ok" if committed else "error",
        "blocker_count": 0 if committed else 1,
        "reasons": [] if committed else [str(result.get("failure_reason") or "production_import_failed")],
        "dsn_value_read": True,
        "production_import_execution_authorized": True,
        "production_import_execution_allowed": committed,
        "execution_attempted": True,
        "connects_postgresql": True,
        "database_written": result.get("production_database_written") is True,
        "database_import_allowed": committed,
        "formal_release_allowed": False,
        "not_real_acceptance_evidence": True,
        "evidence_source": "postgresql18_production_transaction",
        "authorization_record": authorization,
    }


def _write_csv(
    path: Path, rows: Sequence[Mapping[str, Any]], fields: Sequence[str] = ()
) -> None:
    fieldnames = list(fields)
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames or ["status"])
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def write_production_import_outputs(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out / "v1_5_formal_database_import_production_controlled_executor.json",
        "summary_csv": out / "v1_5_formal_database_import_production_summary.csv",
        "identity_readback_csv": out
        / "v1_5_formal_database_import_production_identity_readback.csv",
        "table_counts_csv": out
        / "v1_5_formal_database_import_production_table_counts.csv",
        "bindings_csv": out
        / "v1_5_formal_database_import_production_source_bindings.csv",
        "markdown": out
        / "V1_5_FORMAL_DATABASE_IMPORT_PRODUCTION_CONTROLLED_EXECUTOR.md",
    }
    paths["json"].write_text(
        json.dumps(model, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8-sig",
    )
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "planned_device_count": model.get("planned_device_count"),
                "execution_attempted": model.get("execution_attempted"),
                "transaction_committed": model.get("transaction_committed"),
                "idempotent": model.get("idempotent"),
                "connects_postgresql": model.get("connects_postgresql"),
                "production_database_written": model.get(
                    "production_database_written"
                ),
                "formal_release_allowed": model.get("formal_release_allowed"),
            }
        ],
    )
    _write_csv(paths["identity_readback_csv"], model.get("identity_readback") or [])
    _write_csv(
        paths["table_counts_csv"],
        [
            {"table": table, "count": count}
            for table, count in sorted((model.get("table_counts") or {}).items())
        ],
    )
    _write_csv(paths["bindings_csv"], model.get("source_bindings") or [])
    paths["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 PostgreSQL 18 production controlled executor",
                "",
                "This artifact records a locked preview or an explicitly authorized production import transaction.",
                "It never grants formal calibration release and never controls analyzers or physical routes.",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- execution_attempted: `{model.get('execution_attempted')}`",
                f"- transaction_committed: `{model.get('transaction_committed')}`",
                f"- idempotent: `{model.get('idempotent')}`",
                f"- production_database_written: `{model.get('production_database_written')}`",
                f"- formal_release_allowed: `{model.get('formal_release_allowed')}`",
                "",
                "The target is fixed to PostgreSQL 18 database gas_calibrator, core schema public, and evidence schema v1_5_evidence.",
                "The executor never creates schemas or applies migrations.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return paths
