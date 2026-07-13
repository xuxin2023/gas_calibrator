"""Review and execute the isolated V1.5 PostgreSQL 18 staging import."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..storage.v1_5_evidence.staging_import import (
    STAGING_CORE_SCHEMA_PREFIX,
    STAGING_EVIDENCE_SCHEMA_PREFIX,
    StagingImportError,
    execute_staging_import,
    load_json_object,
    sha256_file,
    validate_staging_package,
    validate_staging_schemas,
)


SCHEMA = "v1_5_formal_database_import_staging_executor_v1"
CONFIRMATION_TEXT = "I AUTHORIZE V1.5 POSTGRESQL 18 STAGING IMPORT ONLY"
DEFAULT_DSN_ENV = "V1_5_POSTGRES_STAGING_DSN"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return str(path)


def _source_binding(role: str, path: Path, expected_schema: str) -> dict[str, Any]:
    payload = load_json_object(path)
    return {
        "role": role,
        "path": _display_path(path),
        "sha256": sha256_file(path),
        "expected_schema": expected_schema,
        "actual_schema": str(payload.get("schema") or ""),
        "status": "ready" if payload.get("schema") == expected_schema else "invalid",
    }


def build_staging_import_preview(
    *,
    transaction_plan_json: str | Path,
    evidence_bundle_json: str | Path,
    core_schema: str = STAGING_CORE_SCHEMA_PREFIX,
    evidence_schema: str = STAGING_EVIDENCE_SCHEMA_PREFIX,
    dsn_env: str = DEFAULT_DSN_ENV,
) -> dict[str, Any]:
    plan_path = Path(transaction_plan_json).resolve()
    bundle_path = Path(evidence_bundle_json).resolve()
    plan = load_json_object(plan_path)
    bundle = load_json_object(bundle_path)
    reasons: list[str] = []
    planned_devices: list[dict[str, str]] = []
    try:
        schemas = validate_staging_schemas(core_schema, evidence_schema)
    except StagingImportError as exc:
        reasons.append(str(exc))
        schemas = None
    try:
        planned_devices = validate_staging_package(plan, bundle)
    except StagingImportError as exc:
        reasons.append(str(exc))
    dsn_env_name = str(dsn_env or "").strip()
    if not dsn_env_name or ("STAGING" not in dsn_env_name.upper() and "TEST" not in dsn_env_name.upper()):
        reasons.append("dsn_env_name_must_be_staging_or_test_scoped")
    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": (
            "ready_for_postgresql18_staging_import_review" if ready else "review_required"
        ),
        "blocker_count": len(reasons),
        "review_required_count": 0,
        "staging_import_package_ready": ready,
        "export_status": "ok" if ready else "error",
        "production_state": "staging_only_production_import_locked",
        "production_backend": "postgresql",
        "production_postgresql_major": 18,
        "staging_core_schema": schemas.core if schemas else str(core_schema),
        "staging_evidence_schema": schemas.evidence if schemas else str(evidence_schema),
        "dsn_env": dsn_env_name,
        "dsn_value_read": False,
        "planned_device_count": len(planned_devices),
        "planned_devices": planned_devices,
        "source_bindings": [
            _source_binding(
                "formal_database_import_transaction_plan",
                plan_path,
                "v1_5_formal_database_import_transaction_plan_v1",
            ),
            _source_binding("evidence_bundle", bundle_path, "v1_5_evidence_registry"),
        ],
        "reasons": reasons,
        "execute_flag_required": "--execute-staging-import",
        "operator_confirmation_required": CONFIRMATION_TEXT,
        "distinct_reviewer_approver_required": True,
        "initialize_staging_schemas_requested": False,
        "staging_import_execution_allowed": False,
        "execution_attempted": False,
        "connects_postgresql": False,
        "staging_database_written": False,
        "production_database_written": False,
        "database_written": False,
        "database_import_allowed": False,
        "real_import_execution_allowed": False,
        "formal_release_allowed": False,
        "opens_com_ports": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "not_real_acceptance_evidence": True,
        "evidence_source": "staging_database_preview",
        "artifact_roles": {
            "execution_summary": [
                "v1_5_formal_database_import_staging_executor.json",
                "v1_5_formal_database_import_staging_summary.csv",
            ],
            "execution_rows": [
                "v1_5_formal_database_import_staging_identity_readback.csv",
                "v1_5_formal_database_import_staging_table_counts.csv",
            ],
            "diagnostic_analysis": [
                "v1_5_formal_database_import_staging_source_bindings.csv"
            ],
            "formal_analysis": ["V1_5_FORMAL_DATABASE_IMPORT_STAGING_EXECUTOR.md"],
        },
    }


def execute_reviewed_staging_import(
    *,
    preview: Mapping[str, Any],
    transaction_plan_json: str | Path,
    evidence_bundle_json: str | Path,
    dsn: str,
    authorization_id: str,
    operator: str,
    reviewer: str,
    approver: str,
    operator_confirmation_text: str,
    initialize_staging_schemas: bool,
    failure_injector=None,
) -> dict[str, Any]:
    model = dict(preview)
    auth_reasons: list[str] = []
    if preview.get("staging_import_package_ready") is not True:
        auth_reasons.append("staging_import_package_not_ready")
    if operator_confirmation_text != CONFIRMATION_TEXT:
        auth_reasons.append("operator_confirmation_text_mismatch")
    for field, value in (
        ("authorization_id", authorization_id),
        ("operator", operator),
        ("reviewer", reviewer),
        ("approver", approver),
    ):
        if not str(value or "").strip():
            auth_reasons.append(f"{field}_required")
    if str(reviewer or "").strip() == str(approver or "").strip():
        auth_reasons.append("reviewer_and_approver_must_differ")
    if auth_reasons:
        return {
            **model,
            "overall_status": "staging_import_authorization_blocked",
            "export_status": "error",
            "blocker_count": len(auth_reasons),
            "reasons": list(model.get("reasons") or []) + auth_reasons,
            "staging_import_execution_allowed": False,
        }
    plan_path = Path(transaction_plan_json).resolve()
    bundle_path = Path(evidence_bundle_json).resolve()
    result = execute_staging_import(
        dsn=dsn,
        transaction_plan=load_json_object(plan_path),
        evidence_bundle=load_json_object(bundle_path),
        transaction_plan_sha256=sha256_file(plan_path),
        evidence_bundle_sha256=sha256_file(bundle_path),
        core_schema=str(preview["staging_core_schema"]),
        evidence_schema=str(preview["staging_evidence_schema"]),
        authorization_id=authorization_id,
        operator=operator,
        reviewer=reviewer,
        approver=approver,
        initialize_schemas=initialize_staging_schemas,
        failure_injector=failure_injector,
    )
    committed = result.get("transaction_committed") is True
    return {
        **model,
        **result,
        "generated_at": _now(),
        "overall_status": str(result.get("status") or "staging_import_failed"),
        "export_status": "ok" if committed else "error",
        "blocker_count": 0 if committed else 1,
        "reasons": [] if committed else [str(result.get("failure_reason") or "staging_import_failed")],
        "initialize_staging_schemas_requested": bool(initialize_staging_schemas),
        "staging_import_execution_allowed": True,
        "execution_attempted": True,
        "connects_postgresql": True,
        "production_database_written": False,
        "database_written": False,
        "database_import_allowed": False,
        "real_import_execution_allowed": False,
        "formal_release_allowed": False,
        "not_real_acceptance_evidence": True,
        "evidence_source": "postgresql18_staging_transaction",
        "authorization_record": {
            "authorization_id": authorization_id,
            "operator": operator,
            "reviewer": reviewer,
            "approver": approver,
            "reviewer_approver_distinct": reviewer != approver,
            "confirmation_matched": True,
        },
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fields: Sequence[str] = ()) -> None:
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


def write_staging_import_outputs(model: Mapping[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out / "v1_5_formal_database_import_staging_executor.json",
        "summary_csv": out / "v1_5_formal_database_import_staging_summary.csv",
        "identity_readback_csv": out / "v1_5_formal_database_import_staging_identity_readback.csv",
        "table_counts_csv": out / "v1_5_formal_database_import_staging_table_counts.csv",
        "bindings_csv": out / "v1_5_formal_database_import_staging_source_bindings.csv",
        "markdown": out / "V1_5_FORMAL_DATABASE_IMPORT_STAGING_EXECUTOR.md",
    }
    paths["json"].write_text(
        json.dumps(model, ensure_ascii=False, indent=2, default=str), encoding="utf-8-sig"
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
                "staging_database_written": model.get("staging_database_written"),
                "production_database_written": model.get("production_database_written"),
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
                "# V1.5 PostgreSQL 18 staging import executor",
                "",
                "This artifact is staging-only. It is not production database import or real acceptance evidence.",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- planned_device_count: `{model.get('planned_device_count')}`",
                f"- execution_attempted: `{model.get('execution_attempted')}`",
                f"- transaction_committed: `{model.get('transaction_committed')}`",
                f"- idempotent: `{model.get('idempotent')}`",
                f"- staging_database_written: `{model.get('staging_database_written')}`",
                f"- production_database_written: `{model.get('production_database_written')}`",
                f"- formal_release_allowed: `{model.get('formal_release_allowed')}`",
                "",
                "The executor accepts only PostgreSQL 18 databases named for staging/test and schemas with the V1.5 staging prefixes.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return paths
