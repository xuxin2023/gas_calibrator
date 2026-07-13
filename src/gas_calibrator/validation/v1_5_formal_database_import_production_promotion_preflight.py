"""Offline gate between a successful V1.5 staging import and production review.

The gate revalidates the immutable transaction package and the staging
readback evidence.  It deliberately never reads a DSN, connects PostgreSQL,
applies migrations, or writes production data.
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..storage.v1_5_evidence.staging_import import (
    POSTGRESQL_18_MIN,
    POSTGRESQL_19_MIN,
    StagingImportError,
    validate_staging_package,
    validate_staging_schemas,
)


SCHEMA = "v1_5_formal_database_import_production_promotion_preflight_v1"
READY_STATUS = "ready_for_postgresql18_production_import_executor_review"
BLOCKED_STATUS = "blocked"
DEFAULT_PRODUCTION_DSN_ENV = "V1_5_POSTGRES_DSN"
REQUIRED_PLAN_BINDING_ROLES = (
    "controlled_executor_design",
    "command_contract",
    "formal_database_import_authorization",
    "formal_database_import_preflight",
    "archive_closure",
    "evidence_bundle",
)


@dataclass(frozen=True)
class PromotionCheck:
    check: str
    status: str
    reasons: tuple[str, ...]
    evidence_role: str
    physical_meaning: str
    next_action: str
    details: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"json_not_object:{path}")
    return payload


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _binding_map(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    rows = payload.get("source_bindings")
    if not isinstance(rows, list):
        return {}
    return {
        str(row.get("role") or ""): row
        for row in rows
        if isinstance(row, Mapping) and str(row.get("role") or "")
    }


def _resolved_path(value: Any) -> Path | None:
    text = str(value or "").strip()
    return Path(text).resolve() if text else None


def _check(
    *,
    check: str,
    reasons: Sequence[str],
    evidence_role: str,
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> PromotionCheck:
    return PromotionCheck(
        check=check,
        status="ready" if not reasons else "blocker",
        reasons=tuple(reasons),
        evidence_role=evidence_role,
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _staging_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if payload.get("schema") != "v1_5_formal_database_import_staging_executor_v1":
        reasons.append("staging_executor_schema_invalid")
    if payload.get("overall_status") not in {
        "staging_import_committed",
        "staging_import_idempotent_noop",
    }:
        reasons.append(f"staging_status={payload.get('overall_status') or 'missing'}")
    if payload.get("transaction_committed") is not True:
        reasons.append("staging_transaction_not_committed")
    if _as_int(payload.get("blocker_count")):
        reasons.append(f"staging_blocker_count={payload.get('blocker_count')}")
    if payload.get("idempotent") is not True and payload.get("staging_database_written") is not True:
        reasons.append("staging_commit_or_idempotent_readback_missing")
    version = _as_int(payload.get("postgresql_server_version_num"))
    if not POSTGRESQL_18_MIN <= version < POSTGRESQL_19_MIN:
        reasons.append(f"staging_postgresql_server_version_num={version or 'missing'}")
    try:
        validate_staging_schemas(
            str(payload.get("staging_core_schema") or ""),
            str(payload.get("staging_evidence_schema") or ""),
        )
    except StagingImportError as exc:
        reasons.append(str(exc))
    for field in (
        "production_database_written",
        "database_written",
        "database_import_allowed",
        "real_import_execution_allowed",
        "formal_release_allowed",
    ):
        if payload.get(field) is not False:
            reasons.append(f"staging_boundary_{field}={payload.get(field)!r}")
    if payload.get("not_real_acceptance_evidence") is not True:
        reasons.append("staging_not_real_acceptance_marker_missing")
    if payload.get("execution_attempted") is not True:
        reasons.append("staging_execution_attempt_missing")
    if payload.get("connects_postgresql") is not True:
        reasons.append("staging_postgresql_connection_evidence_missing")
    if payload.get("evidence_source") != "postgresql18_staging_transaction":
        reasons.append("staging_evidence_source_invalid")
    authorization = payload.get("authorization_record")
    if not isinstance(authorization, Mapping):
        reasons.append("staging_authorization_record_missing")
    else:
        for field in ("authorization_id", "operator", "reviewer", "approver"):
            if not str(authorization.get(field) or "").strip():
                reasons.append(f"staging_authorization_{field}_missing")
        if authorization.get("reviewer_approver_distinct") is not True:
            reasons.append("staging_reviewer_approver_not_distinct")
        if authorization.get("confirmation_matched") is not True:
            reasons.append("staging_operator_confirmation_not_matched")
    return reasons


def _staging_source_reasons(
    staging: Mapping[str, Any], plan_path: Path, bundle_path: Path
) -> list[str]:
    reasons: list[str] = []
    bindings = _binding_map(staging)
    for role, current_path in (
        ("formal_database_import_transaction_plan", plan_path),
        ("evidence_bundle", bundle_path),
    ):
        binding = bindings.get(role)
        if not binding:
            reasons.append(f"staging_source_binding_missing:{role}")
            continue
        bound_path = _resolved_path(binding.get("path"))
        if bound_path != current_path:
            reasons.append(f"staging_source_path_mismatch:{role}")
        expected_hash = str(binding.get("sha256") or "").lower()
        if not expected_hash or expected_hash != _sha256(current_path).lower():
            reasons.append(f"staging_source_sha256_mismatch:{role}")
    return reasons


def _plan_binding_reasons(plan: Mapping[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    reasons: list[str] = []
    readback: list[dict[str, Any]] = []
    bindings = _binding_map(plan)
    for role in REQUIRED_PLAN_BINDING_ROLES:
        binding = bindings.get(role)
        if not binding:
            reasons.append(f"transaction_plan_source_binding_missing:{role}")
            continue
        path = _resolved_path(binding.get("path"))
        expected_hash = str(binding.get("sha256") or "").lower()
        exists = bool(path and path.is_file())
        current_hash = _sha256(path).lower() if exists and path else ""
        matched = bool(expected_hash and current_hash == expected_hash)
        readback.append(
            {
                "role": role,
                "path": str(path) if path else "",
                "expected_sha256": expected_hash,
                "current_sha256": current_hash,
                "exists": exists,
                "matched": matched,
            }
        )
        if not exists:
            reasons.append(f"transaction_plan_source_missing:{role}")
        elif not matched:
            reasons.append(f"transaction_plan_source_sha256_mismatch:{role}")
    return reasons, readback


def _bound_semantic_reasons(plan: Mapping[str, Any]) -> tuple[list[str], dict[str, Any]]:
    reasons: list[str] = []
    payloads: dict[str, dict[str, Any]] = {}
    bindings = _binding_map(plan)
    for role in REQUIRED_PLAN_BINDING_ROLES:
        binding = bindings.get(role)
        path = _resolved_path(binding.get("path")) if binding else None
        if not path or not path.is_file():
            reasons.append(f"bound_semantic_source_missing:{role}")
            continue
        try:
            payloads[role] = _load_json(path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            reasons.append(f"bound_semantic_source_invalid:{role}:{type(exc).__name__}")

    design = payloads.get("controlled_executor_design", {})
    if design.get("schema") != "v1_5_formal_database_import_controlled_executor_design_v1":
        reasons.append("controlled_executor_design_schema_invalid")
    if design.get("overall_status") != "ready_for_controlled_import_executor_design_review":
        reasons.append("controlled_executor_design_not_ready")
    for field in ("execution_supported", "real_import_execution_allowed", "database_import_allowed", "connects_postgresql", "database_written"):
        if design.get(field) is not False:
            reasons.append(f"controlled_executor_design_boundary_{field}={design.get(field)!r}")

    command = payloads.get("command_contract", {})
    if command.get("schema") != "v1_5_formal_database_import_command_contract_v1":
        reasons.append("command_contract_schema_invalid")
    if command.get("overall_status") != "ready_for_controlled_postgresql18_import_command_review":
        reasons.append("command_contract_not_ready")
    for field in (
        "command_contract_ready",
        "database_import_authorization_binding_ready",
        "database_import_preflight_binding_ready",
        "archive_release_ready",
        "evidence_bundle_ready",
    ):
        if command.get(field) is not True:
            reasons.append(f"command_contract_{field}_not_ready")
    for field in ("connects_postgresql", "database_written", "database_import_allowed"):
        if command.get(field) is not False:
            reasons.append(f"command_contract_boundary_{field}={command.get(field)!r}")

    authorization = payloads.get("formal_database_import_authorization", {})
    if authorization.get("schema") != "v1_5_formal_database_import_authorization_v1":
        reasons.append("production_authorization_schema_invalid")
    if authorization.get("overall_status") != "ready_for_manual_postgresql18_import_authorization":
        reasons.append("production_authorization_not_ready")
    for field in (
        "manual_authorization_ready",
        "archive_release_ready",
        "database_import_allowed",
        "formal_release_allowed",
    ):
        if authorization.get(field) is not True:
            reasons.append(f"production_authorization_{field}_not_ready")
    auth_names = [
        str(authorization.get(field) or "").strip()
        for field in ("authorization_id", "operator", "reviewer", "approver")
    ]
    if not all(auth_names):
        reasons.append("production_authorization_identity_fields_missing")
    if auth_names[2] and auth_names[3] and auth_names[2].casefold() == auth_names[3].casefold():
        reasons.append("production_authorization_reviewer_approver_not_distinct")

    preflight = payloads.get("formal_database_import_preflight", {})
    if preflight.get("schema") != "v1_5_formal_database_import_preflight_v1":
        reasons.append("production_preflight_schema_invalid")
    if preflight.get("overall_status") != "ready_for_authorized_postgresql18_import_review":
        reasons.append("production_preflight_not_ready")
    if preflight.get("production_backend") != "postgresql" or _as_int(
        preflight.get("production_postgresql_major")
    ) != 18:
        reasons.append("production_preflight_postgresql18_contract_invalid")
    for field in ("dsn_configured", "dry_run_contract_ready"):
        if preflight.get(field) is not True:
            reasons.append(f"production_preflight_{field}_not_ready")
    for field in ("connects_postgresql", "database_written", "database_import_allowed", "formal_release_allowed"):
        if preflight.get(field) is not False:
            reasons.append(f"production_preflight_boundary_{field}={preflight.get(field)!r}")

    archive = payloads.get("archive_closure", {})
    if archive.get("schema") != "v1_5_formal_archive_closure_v1":
        reasons.append("archive_closure_schema_invalid")
    if archive.get("overall_status") not in {"ready", "ready_for_formal_release"}:
        reasons.append("archive_closure_not_ready")
    if archive.get("package_status") not in {"ready", "ready_for_formal_release"}:
        reasons.append("archive_package_not_ready")
    traceability = archive.get("identity_getco_traceability")
    if not isinstance(traceability, Mapping) or traceability.get("ready_for_archive_release") is not True:
        reasons.append("archive_identity_traceability_not_ready")
    if isinstance(traceability, Mapping) and traceability.get("traceability_review_required") is True:
        reasons.append("archive_identity_traceability_review_required")

    summary = {
        role: {
            "schema": payload.get("schema", ""),
            "overall_status": payload.get("overall_status", ""),
        }
        for role, payload in payloads.items()
    }
    return reasons, summary


def _package_reasons(
    plan: Mapping[str, Any], bundle: Mapping[str, Any]
) -> tuple[list[str], list[dict[str, str]]]:
    reasons: list[str] = []
    devices: list[dict[str, str]] = []
    try:
        devices = validate_staging_package(plan, bundle)
    except StagingImportError as exc:
        reasons.append(str(exc))
    if plan.get("production_transaction_package_ready") is not True:
        reasons.append("production_transaction_package_not_ready")
    if list(plan.get("production_blocking_reasons") or []):
        reasons.append("production_transaction_package_has_blocking_reasons")
    if plan.get("production_backend") != "postgresql":
        reasons.append("production_backend_not_postgresql")
    if _as_int(plan.get("production_postgresql_major")) != 18:
        reasons.append("production_postgresql_major_not_18")
    return reasons, devices


def _readback_reasons(
    staging: Mapping[str, Any], plan: Mapping[str, Any], bundle: Mapping[str, Any]
) -> list[str]:
    reasons: list[str] = []
    planned = plan.get("planned_devices") if isinstance(plan.get("planned_devices"), list) else []
    identities = (
        staging.get("identity_readback")
        if isinstance(staging.get("identity_readback"), list)
        else []
    )
    if not 1 <= len(identities) <= 6 or len(identities) != len(planned):
        reasons.append("staging_identity_readback_count_mismatch")
    planned_map = {
        str(row.get("slot") or ""): row for row in planned if isinstance(row, Mapping)
    }
    for row in identities:
        if not isinstance(row, Mapping):
            reasons.append("staging_identity_readback_row_invalid")
            continue
        expected = planned_map.get(str(row.get("slot") or ""))
        if not expected:
            reasons.append(f"staging_identity_slot_unplanned:{row.get('slot') or 'missing'}")
            continue
        for field in ("sn_code", "device_code", "protocol_device_id"):
            if str(row.get(field) or "") != str(expected.get(field) or ""):
                reasons.append(f"staging_identity_{field}_mismatch:{row.get('slot')}")
        if row.get("sensor_found") is not True:
            reasons.append(f"staging_sensor_not_found:{row.get('slot')}")
        if str(row.get("stored_sn_code") or "") != str(expected.get("sn_code") or ""):
            reasons.append(f"staging_stored_sn_mismatch:{row.get('slot')}")
        if str(row.get("stored_device_code") or "") != str(expected.get("device_code") or ""):
            reasons.append(f"staging_stored_device_code_mismatch:{row.get('slot')}")
        if _as_int(row.get("protocol_alias_count")) != 1:
            reasons.append(f"staging_protocol_alias_count_invalid:{row.get('slot')}")
    tables = bundle.get("tables") if isinstance(bundle.get("tables"), Mapping) else {}
    expected_counts = {str(name): len(rows) for name, rows in tables.items() if isinstance(rows, list)}
    actual_counts: dict[str, int] = {}
    if isinstance(staging.get("table_counts"), Mapping):
        for name, value in staging["table_counts"].items():
            parsed = _as_int(value, default=-1)
            if parsed < 0:
                reasons.append(f"staging_table_count_invalid:{name}")
            else:
                actual_counts[str(name)] = parsed
    if actual_counts != expected_counts:
        reasons.append("staging_table_count_readback_mismatch")
    if str(staging.get("run_id") or "") != str(bundle.get("run_id") or ""):
        reasons.append("staging_run_id_mismatch")
    if str(staging.get("run_db_id") or "") != str(bundle.get("run_db_id") or ""):
        reasons.append("staging_run_db_id_mismatch")
    return reasons


def build_v1_5_formal_database_import_production_promotion_preflight(
    *,
    staging_import_json: str | Path,
    transaction_plan_json: str | Path,
    evidence_bundle_json: str | Path,
    production_dsn_env: str = DEFAULT_PRODUCTION_DSN_ENV,
) -> dict[str, Any]:
    paths = {
        "staging_import": Path(staging_import_json).resolve(),
        "transaction_plan": Path(transaction_plan_json).resolve(),
        "evidence_bundle": Path(evidence_bundle_json).resolve(),
    }
    payloads: dict[str, dict[str, Any]] = {}
    load_reasons: list[str] = []
    for role, path in paths.items():
        try:
            payloads[role] = _load_json(path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            payloads[role] = {}
            load_reasons.append(f"{role}_load_failed:{type(exc).__name__}")

    staging = payloads["staging_import"]
    plan = payloads["transaction_plan"]
    bundle = payloads["evidence_bundle"]
    plan_binding_reasons, binding_readback = _plan_binding_reasons(plan)
    bound_semantic_reasons, bound_semantic_summary = _bound_semantic_reasons(plan)
    package_reasons, planned_devices = _package_reasons(plan, bundle)

    checks = [
        _check(
            check="required_json_inputs_load",
            reasons=load_reasons,
            evidence_role="immutable_promotion_inputs",
            physical_meaning="Promotion review must consume three explicit immutable JSON inputs.",
            next_action="Regenerate the missing or invalid input before production review.",
            details={role: str(path) for role, path in paths.items()},
        ),
        _check(
            check="staging_transaction_committed_and_isolated",
            reasons=_staging_reasons(staging),
            evidence_role="successful_staging_transaction",
            physical_meaning="Production review starts only after PostgreSQL 18 staging committed and read back safely.",
            next_action="Repair or rerun the staging-only import; do not promote failed staging evidence.",
            details={
                "status": staging.get("overall_status", ""),
                "transaction_committed": staging.get("transaction_committed", False),
                "idempotent": staging.get("idempotent", False),
                "server_version_num": staging.get("postgresql_server_version_num", ""),
            },
        ),
        _check(
            check="staging_sources_match_current_plan_and_bundle",
            reasons=(
                _staging_source_reasons(staging, paths["transaction_plan"], paths["evidence_bundle"])
                if not load_reasons
                else ["staging_source_check_blocked_by_input_load"]
            ),
            evidence_role="staging_input_hash_binding",
            physical_meaning="The exact plan and evidence bundle proven in staging must be the promotion inputs.",
            next_action="Use the exact files recorded by staging or repeat staging with the new files.",
            details={"staging_source_bindings": list(staging.get("source_bindings") or [])},
        ),
        _check(
            check="production_transaction_package_revalidated",
            reasons=package_reasons,
            evidence_role="authorized_archive_bound_transaction_package",
            physical_meaning="The transaction plan must still bind an authorized, released, PostgreSQL 18 package.",
            next_action="Regenerate transaction plan after resolving authorization/archive/evidence blockers.",
            details={
                "transaction_plan_status": plan.get("overall_status", ""),
                "production_transaction_package_ready": plan.get(
                    "production_transaction_package_ready", False
                ),
                "planned_device_count": len(planned_devices),
            },
        ),
        _check(
            check="transaction_plan_bound_sources_unchanged",
            reasons=plan_binding_reasons,
            evidence_role="authorization_archive_evidence_hash_lineage",
            physical_meaning="Authorization, archive, preflight, command, design, and evidence files must remain byte-identical.",
            next_action="Regenerate downstream authorization and plan after any source changes.",
            details={"binding_count": len(binding_readback)},
        ),
        _check(
            check="bound_authorization_archive_and_command_semantics_ready",
            reasons=bound_semantic_reasons,
            evidence_role="independent_bound_source_semantic_review",
            physical_meaning="Promotion must independently re-read the bound authorization, archive, preflight, command, and design artifacts instead of trusting a ready flag in the plan.",
            next_action="Regenerate the invalid upstream artifact and all downstream hashes before promotion review.",
            details=bound_semantic_summary,
        ),
        _check(
            check="staging_identity_and_table_readback_matches_package",
            reasons=_readback_reasons(staging, plan, bundle),
            evidence_role="staging_precommit_and_postcommit_readback",
            physical_meaning="SN/device_code, protocol aliases, run identity, and every evidence table count must match.",
            next_action="Hold promotion and reconcile staging readback with the immutable package.",
            details={
                "identity_readback_count": len(staging.get("identity_readback") or []),
                "table_counts": dict(staging.get("table_counts") or {}),
            },
        ),
        _check(
            check="production_target_and_execution_remain_locked",
            reasons=(
                []
                if production_dsn_env == DEFAULT_PRODUCTION_DSN_ENV
                else [f"production_dsn_env={production_dsn_env or 'missing'}"]
            ),
            evidence_role="production_connection_and_write_lock",
            physical_meaning="This gate names the future DSN environment but never reads it or connects.",
            next_action="Keep production execution in a separate explicitly authorized executor.",
            details={
                "production_dsn_env": production_dsn_env,
                "dsn_value_read": False,
                "connects_postgresql": False,
                "production_database_written": False,
            },
        ),
    ]
    blocker_count = sum(row.status == "blocker" for row in checks)
    ready = blocker_count == 0
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "blocker_count": blocker_count,
        "review_required_count": 0,
        "promotion_preflight_ready": ready,
        "production_import_executor_review_allowed": ready,
        "production_import_execution_allowed": False,
        "production_backend": "postgresql",
        "production_postgresql_major": 18,
        "production_dsn_env": production_dsn_env,
        "dsn_value_read": False,
        "connects_postgresql": False,
        "applies_migrations": False,
        "database_import_attempted": False,
        "database_written": False,
        "production_database_written": False,
        "database_import_allowed": False,
        "formal_release_allowed": False,
        "opens_com_ports": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "not_real_acceptance_evidence": True,
        "input_paths": {role: str(path) for role, path in paths.items()},
        "input_sha256": {
            role: _sha256(path) if path.is_file() else "" for role, path in paths.items()
        },
        "run_id": str(bundle.get("run_id") or ""),
        "run_db_id": str(bundle.get("run_db_id") or ""),
        "planned_device_count": len(planned_devices),
        "planned_devices": planned_devices,
        "identity_readback": list(staging.get("identity_readback") or []),
        "table_counts": dict(staging.get("table_counts") or {}),
        "binding_readback": binding_readback,
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Review and implement a separate production controlled executor that consumes this exact preflight. "
            "This artifact never authorizes or performs production import."
        ),
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
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_v1_5_formal_database_import_production_promotion_preflight_outputs(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_database_import_production_promotion_preflight.json",
        "summary_csv": out / "v1_5_formal_database_import_production_promotion_summary.csv",
        "checks_csv": out / "v1_5_formal_database_import_production_promotion_checks.csv",
        "bindings_csv": out / "v1_5_formal_database_import_production_promotion_bindings.csv",
        "identity_csv": out / "v1_5_formal_database_import_production_promotion_identity.csv",
        "markdown": out / "V1_5_FORMAL_DATABASE_IMPORT_PRODUCTION_PROMOTION_PREFLIGHT.md",
    }
    out.mkdir(parents=True, exist_ok=True)
    paths["json"].write_text(
        json.dumps(model, ensure_ascii=False, indent=2, default=str), encoding="utf-8-sig"
    )
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "blocker_count": model.get("blocker_count"),
                "promotion_preflight_ready": model.get("promotion_preflight_ready"),
                "planned_device_count": model.get("planned_device_count"),
                "production_import_execution_allowed": model.get(
                    "production_import_execution_allowed"
                ),
                "connects_postgresql": model.get("connects_postgresql"),
                "production_database_written": model.get("production_database_written"),
            }
        ],
    )
    _write_csv(paths["checks_csv"], list(model.get("checks") or []))
    _write_csv(paths["bindings_csv"], list(model.get("binding_readback") or []))
    _write_csv(paths["identity_csv"], list(model.get("identity_readback") or []))
    lines = [
        "# V1.5 PostgreSQL 18 production promotion preflight",
        "",
        "This is an offline gate between staging proof and a future production executor review.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        f"- promotion_preflight_ready: `{model.get('promotion_preflight_ready')}`",
        f"- planned_device_count: `{model.get('planned_device_count')}`",
        f"- production_import_execution_allowed: `{model.get('production_import_execution_allowed')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        f"- production_database_written: `{model.get('production_database_written')}`",
        f"- not_real_acceptance_evidence: `{model.get('not_real_acceptance_evidence')}`",
        "",
        "A ready result permits code review of a separate controlled production executor only. It does not authorize import.",
        "",
        "## Checks",
        "",
    ]
    for row in model.get("checks") or []:
        reasons = ";".join(row.get("reasons") or [])
        lines.append(f"- `{row.get('check')}`: `{row.get('status')}` {reasons}".rstrip())
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
