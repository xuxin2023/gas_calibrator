"""Offline PostgreSQL 18 transaction plan for V1.5 evidence imports.

This module turns the reviewed database schema and controlled-executor design
into deterministic, non-executable operation rows.  It never reads a DSN,
builds executable SQL, connects to PostgreSQL, or writes database rows.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_database_import_transaction_plan_v1"
READY_STATUS = "ready_for_postgresql18_transaction_plan_review"
REVIEW_STATUS = "review_required"
PRODUCTION_POSTGRESQL_MAJOR = 18
DEFAULT_DSN_ENV = "V1_5_POSTGRES_DSN"
EXPECTED_STAGES = (
    "initialization_identity",
    "runtime_setup",
    "pressure_temperature_pre_open_flow",
    "open_flow_sampling",
    "fit_and_candidate_review",
    "controlled_write_and_readback",
    "archive_report_release",
)
OPTIONAL_INPUTS = (
    ("formal_database_import_command_contract", "v1_5_formal_database_import_command_contract_v1"),
    ("formal_database_import_authorization", "v1_5_formal_database_import_authorization_v1"),
    ("formal_database_import_preflight", "v1_5_formal_database_import_preflight_v1"),
    ("archive_closure", "v1_5_formal_archive_closure_v1"),
    ("evidence_bundle", "v1_5_evidence_registry"),
)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists() or not source.is_file():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _sha256(path: Path | None) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolved(path: str | Path | None) -> Path | None:
    return Path(path).resolve() if path else None


def _display_path(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return path.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return str(path)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


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


def _binding_row(role: str, path: Path | None, payload: Mapping[str, Any], expected_schema: str) -> dict[str, Any]:
    exists = bool(path and path.exists() and path.is_file())
    actual_schema = str(payload.get("schema") or "") if payload else ""
    return {
        "role": role,
        "path": _display_path(path),
        "exists": exists,
        "sha256": _sha256(path),
        "expected_schema": expected_schema,
        "actual_schema": actual_schema,
        "status": "ready" if exists and actual_schema == expected_schema else "missing_or_invalid",
    }


def _device_reasons(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    if not rows:
        return ["planned_device_preview_empty"]
    if len(rows) > 6:
        reasons.append("planned_device_count_gt_6")
    seen_sn: set[str] = set()
    seen_device: set[str] = set()
    seen_protocol: set[str] = set()
    seen_slots: set[str] = set()
    seen_ports: set[str] = set()
    for index, row in enumerate(rows, start=1):
        label = str(row.get("slot") or f"GA{index:02d}")
        sn_code = str(row.get("sn_code") or "").strip()
        device_code = str(row.get("device_code") or "").strip()
        protocol_id = str(row.get("protocol_device_id") or "").strip()
        port = str(row.get("port") or "").strip().upper()
        if not re.fullmatch(r"GA0[1-6]", label):
            reasons.append(f"{label}:slot_invalid")
        if label in seen_slots:
            reasons.append(f"{label}:duplicate_slot")
        if port and not re.fullmatch(r"COM\d+", port):
            reasons.append(f"{label}:com_port_invalid")
        if port and port in seen_ports:
            reasons.append(f"{label}:duplicate_com_port")
        if not re.fullmatch(r"\d{8}", sn_code) or sn_code == "00000000":
            reasons.append(f"{label}:sn_code_invalid")
        if not re.fullmatch(r"\d{8}", device_code) or device_code == "00000000":
            reasons.append(f"{label}:device_code_invalid")
        if sn_code and device_code and sn_code != device_code:
            reasons.append(f"{label}:device_code_must_match_sn_code")
        if not re.fullmatch(r"\d{3}", protocol_id):
            reasons.append(f"{label}:protocol_device_id_invalid")
        if sn_code in seen_sn:
            reasons.append(f"{label}:duplicate_sn_code")
        if device_code in seen_device:
            reasons.append(f"{label}:duplicate_device_code")
        if protocol_id in seen_protocol:
            reasons.append(f"{label}:duplicate_protocol_device_id_in_run")
        if sn_code:
            seen_sn.add(sn_code)
        if device_code:
            seen_device.add(device_code)
        if protocol_id:
            seen_protocol.add(protocol_id)
        seen_slots.add(label)
        if port:
            seen_ports.add(port)
    return reasons


def _operation_rows(insert_preview: Sequence[Mapping[str, Any]], device_count: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = [
        {
            "order": 1,
            "operation": "offline_validate_frozen_inputs",
            "stage": "pre_transaction",
            "target_tables": "",
            "natural_key": "artifact role + sha256",
            "expected_record_scope": "all bound input artifacts",
            "would_execute": False,
            "failure_policy": "hold_before_database_connection",
        },
        {
            "order": 2,
            "operation": "future_begin_postgresql18_transaction",
            "stage": "transaction",
            "target_tables": "",
            "natural_key": "single transaction per authorized import attempt",
            "expected_record_scope": "one transaction",
            "would_execute": False,
            "failure_policy": "rollback_no_partial_acceptance",
        },
    ]
    for offset, preview in enumerate(insert_preview, start=3):
        stage = str(preview.get("stage") or "")
        rows.append(
            {
                "order": offset,
                "operation": "future_import_stage_rows",
                "stage": stage,
                "target_tables": str(preview.get("target_tables") or ""),
                "natural_key": str(preview.get("natural_key") or ""),
                "source_artifact": str(preview.get("source_artifact") or ""),
                "expected_record_scope": (
                    f"{device_count} planned analyzers plus run/point/artifact rows from frozen evidence"
                ),
                "would_execute": False,
                "failure_policy": "rollback_no_partial_acceptance",
            }
        )
    next_order = len(rows) + 1
    rows.extend(
        [
            {
                "order": next_order,
                "operation": "future_precommit_readback",
                "stage": "precommit_readback",
                "target_tables": "all planned target tables",
                "natural_key": "run_id + sn_code/device_code + artifact sha256",
                "expected_record_scope": "exact counts, identities, roles, and hashes from the frozen plan",
                "would_execute": False,
                "failure_policy": "rollback_no_partial_acceptance",
            },
            {
                "order": next_order + 1,
                "operation": "future_commit",
                "stage": "commit",
                "target_tables": "all planned target tables",
                "natural_key": "authorization_id + run_id + evidence_bundle_sha256",
                "expected_record_scope": "one atomic committed import",
                "would_execute": False,
                "failure_policy": "hold_for_dba_and_reviewer_on_uncertain_commit",
            },
        ]
    )
    return rows


def build_v1_5_formal_database_import_transaction_plan(
    *,
    formal_database_dry_run_json: str | Path,
    formal_database_import_controlled_executor_design_json: str | Path,
    formal_database_import_command_contract_json: str | Path | None = None,
    formal_database_import_authorization_json: str | Path | None = None,
    formal_database_import_preflight_json: str | Path | None = None,
    archive_closure_json: str | Path | None = None,
    evidence_bundle_json: str | Path | None = None,
) -> dict[str, Any]:
    """Build a deterministic no-SQL, no-connect transaction plan."""

    dry_path = _resolved(formal_database_dry_run_json)
    design_path = _resolved(formal_database_import_controlled_executor_design_json)
    dry_run = _load_json(dry_path)
    design = _load_json(design_path)
    optional_paths = {
        "formal_database_import_command_contract": _resolved(formal_database_import_command_contract_json),
        "formal_database_import_authorization": _resolved(formal_database_import_authorization_json),
        "formal_database_import_preflight": _resolved(formal_database_import_preflight_json),
        "archive_closure": _resolved(archive_closure_json),
        "evidence_bundle": _resolved(evidence_bundle_json),
    }
    optional_payloads = {role: _load_json(path) for role, path in optional_paths.items()}

    contract_reasons: list[str] = []
    if dry_run.get("schema") != "v1_5_formal_database_dry_run_contract_v1":
        contract_reasons.append("formal_database_dry_run_schema_invalid")
    if dry_run.get("production_backend") != "postgresql":
        contract_reasons.append("production_backend_not_postgresql")
    if int(dry_run.get("production_postgresql_major") or 0) != PRODUCTION_POSTGRESQL_MAJOR:
        contract_reasons.append("production_postgresql_major_not_18")
    if dry_run.get("primary_identity") != "sn_code/device_code":
        contract_reasons.append("primary_identity_not_sn_code_device_code")
    if design.get("schema") != "v1_5_formal_database_import_controlled_executor_design_v1":
        contract_reasons.append("controlled_executor_design_schema_invalid")
    for key in (
        "connects_postgresql",
        "database_written",
        "database_import_attempted",
        "real_import_execution_allowed",
        "execution_supported",
    ):
        if design.get(key) is not False:
            contract_reasons.append(f"controlled_executor_design_{key}_not_false")
    if design.get("production_backend") != "postgresql":
        contract_reasons.append("controlled_executor_design_backend_not_postgresql")
    if int(design.get("production_postgresql_major") or 0) != PRODUCTION_POSTGRESQL_MAJOR:
        contract_reasons.append("controlled_executor_design_postgresql_major_not_18")
    dsn_env = str(design.get("dsn_env") or "")
    if not dsn_env or design.get("dsn_value_read") is not False:
        contract_reasons.append("dsn_secret_boundary_invalid")

    insert_preview = dry_run.get("insert_preview") or []
    if not isinstance(insert_preview, list):
        insert_preview = []
    stages = tuple(str(row.get("stage") or "") for row in insert_preview if isinstance(row, Mapping))
    if stages != EXPECTED_STAGES:
        contract_reasons.append("insert_preview_stage_order_invalid")
    for row in insert_preview:
        if not isinstance(row, Mapping):
            contract_reasons.append("insert_preview_row_invalid")
            continue
        if not str(row.get("target_tables") or ""):
            contract_reasons.append(f"{row.get('stage') or 'unknown'}:target_tables_missing")
        if not str(row.get("natural_key") or ""):
            contract_reasons.append(f"{row.get('stage') or 'unknown'}:natural_key_missing")

    planned_devices = dry_run.get("planned_device_preview") or []
    if not isinstance(planned_devices, list):
        planned_devices = []
    device_reasons = _device_reasons(planned_devices)
    production_reasons = list(device_reasons)

    bindings = [
        _binding_row(
            "formal_database_dry_run",
            dry_path,
            dry_run,
            "v1_5_formal_database_dry_run_contract_v1",
        ),
        _binding_row(
            "formal_database_import_controlled_executor_design",
            design_path,
            design,
            "v1_5_formal_database_import_controlled_executor_design_v1",
        ),
    ]
    for role, expected_schema in OPTIONAL_INPUTS:
        row = _binding_row(role, optional_paths[role], optional_payloads[role], expected_schema)
        bindings.append(row)
        if row["status"] != "ready":
            production_reasons.append(f"{role}_missing_or_invalid")

    command_contract = optional_payloads["formal_database_import_command_contract"]
    authorization = optional_payloads["formal_database_import_authorization"]
    preflight = optional_payloads["formal_database_import_preflight"]
    archive = optional_payloads["archive_closure"]
    evidence_bundle = optional_payloads["evidence_bundle"]
    if command_contract:
        if command_contract.get("command_contract_ready") is not True:
            production_reasons.append("formal_database_import_command_contract_not_ready")
        for key in (
            "database_import_authorization_binding_ready",
            "database_import_preflight_binding_ready",
            "archive_release_ready",
            "archive_closure_index_binding_ready",
            "senco_authorization_archive_binding_ready",
            "evidence_bundle_ready",
            "evidence_bundle_schema_ready",
            "evidence_bundle_binding_ready",
        ):
            if command_contract.get(key) is not True:
                production_reasons.append(f"command_contract_{key}_not_ready")
        for key in (
            "connects_postgresql",
            "applies_migrations",
            "database_import_attempted",
            "database_written",
            "database_import_allowed",
            "real_import_execution_allowed",
        ):
            if command_contract.get(key) is not False:
                production_reasons.append(f"command_contract_{key}_not_false")
        hash_contracts = (
            (
                "formal_database_import_authorization_sha256",
                "formal_database_import_authorization",
            ),
            ("formal_database_import_preflight_sha256", "formal_database_import_preflight"),
            ("archive_closure_sha256", "archive_closure"),
            ("evidence_bundle_sha256", "evidence_bundle"),
        )
        for hash_field, role in hash_contracts:
            expected_hash = str(command_contract.get(hash_field) or "").lower()
            current_hash = _sha256(optional_paths[role]).lower() if optional_paths[role] else ""
            if not expected_hash or expected_hash != current_hash:
                production_reasons.append(f"{role}_sha256_mismatch_with_command_contract")
        path_contracts = (
            ("formal_database_import_authorization_json", "formal_database_import_authorization"),
            ("formal_database_import_preflight_json", "formal_database_import_preflight"),
            ("archive_closure_json", "archive_closure"),
            ("evidence_bundle_json", "evidence_bundle"),
        )
        for path_field, role in path_contracts:
            expected_path_value = str(command_contract.get(path_field) or "")
            expected_path = _resolved(expected_path_value) if expected_path_value else None
            if expected_path is None or expected_path != optional_paths[role]:
                production_reasons.append(f"{role}_path_mismatch_with_command_contract")
    if authorization:
        if authorization.get("manual_authorization_ready") is not True:
            production_reasons.append("formal_database_import_authorization_not_ready")
        for key in (
            "database_import_preflight_binding_ready",
            "archive_release_ready",
            "archive_closure_index_binding_ready",
            "senco_authorization_archive_binding_ready",
            "database_import_allowed",
        ):
            if authorization.get(key) is not True:
                production_reasons.append(f"authorization_{key}_not_ready")
    if preflight:
        if preflight.get("overall_status") != "ready_for_authorized_postgresql18_import_review":
            production_reasons.append("formal_database_import_preflight_not_ready")
        if preflight.get("dsn_configured") is not True:
            production_reasons.append("formal_database_import_preflight_dsn_not_configured")
        if preflight.get("dry_run_contract_ready") is not True:
            production_reasons.append("formal_database_import_preflight_dry_run_contract_not_ready")
    if archive:
        archive_ready = (
            archive.get("overall_status") == "ready" and archive.get("package_status") == "ready"
        )
        if not archive_ready:
            production_reasons.append("archive_closure_not_ready")
        traceability = archive.get("identity_getco_traceability")
        if not isinstance(traceability, Mapping) or traceability.get("ready_for_archive_release") is not True:
            production_reasons.append("archive_identity_getco_traceability_not_ready")
    if evidence_bundle:
        tables = evidence_bundle.get("tables")
        if not isinstance(tables, Mapping) or not tables.get("runs"):
            production_reasons.append("evidence_bundle_run_rows_missing")
    if design.get("overall_status") != "ready_for_controlled_import_executor_design_review":
        production_reasons.append("controlled_executor_design_not_ready")
    for key in (
        "database_import_authorization_binding_ready",
        "database_import_preflight_binding_ready",
        "evidence_bundle_schema_ready",
        "evidence_bundle_binding_ready",
        "archive_closure_index_binding_ready",
        "senco_authorization_archive_binding_ready",
    ):
        if design.get(key) is not True:
            production_reasons.append(f"controlled_executor_design_{key}_not_ready")

    operations = _operation_rows(insert_preview, len(planned_devices))
    contract_ready = not contract_reasons
    production_ready = contract_ready and not production_reasons
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if contract_ready else REVIEW_STATUS,
        "blocker_count": len(contract_reasons),
        "review_required_count": len(production_reasons),
        "transaction_plan_contract_ready": contract_ready,
        "production_transaction_package_ready": production_ready,
        "production_state": "blocked_offline_transaction_plan_only",
        "production_backend": "postgresql",
        "production_postgresql_major": PRODUCTION_POSTGRESQL_MAJOR,
        "primary_identity": "sn_code/device_code",
        "protocol_device_id_role": "compatibility_alias_and_command_identity",
        "transport_identity_role": "COM_and_GA_are_run_local_transport_mapping_only",
        "planned_device_count": len(planned_devices),
        "dsn_env": dsn_env or DEFAULT_DSN_ENV,
        "dsn_value_read": False,
        "emits_executable_sql": False,
        "connects_postgresql": False,
        "applies_migrations": False,
        "database_import_attempted": False,
        "database_written": False,
        "database_import_allowed": False,
        "real_import_execution_allowed": False,
        "execution_supported": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "formal_release_allowed": False,
        "not_real_acceptance_evidence": True,
        "contract_reasons": contract_reasons,
        "production_blocking_reasons": production_reasons,
        "source_bindings": bindings,
        "planned_devices": [dict(row) for row in planned_devices if isinstance(row, Mapping)],
        "transaction_operations": operations,
        "precommit_readback_contract": {
            "required": True,
            "checks": [
                "run_id_and_archive_hash_exact_match",
                "sn_code_device_code_unique_and_exact_match",
                "protocol_device_id_alias_preserved",
                "target_table_counts_match_plan",
                "artifact_roles_and_sha256_match_evidence_bundle",
            ],
            "failure_policy": "rollback_no_partial_acceptance",
        },
        "rollback_contract": {
            "before_commit": "rollback_transaction_and_record_zero_committed_rows",
            "uncertain_commit": "hold_for_DBA_and_reviewer_no_automatic_delete",
            "automatic_retry": False,
        },
        "next_action": (
            "Keep PostgreSQL disconnected. Supply one current 1-6 device identity preview plus fresh command, "
            "authorization, preflight, archive, and evidence bundle artifacts before a separate real executor is reviewed."
        ),
    }


def write_v1_5_formal_database_import_transaction_plan_outputs(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_database_import_transaction_plan.json",
        "operations_csv": out / "v1_5_formal_database_import_transaction_operations.csv",
        "bindings_csv": out / "v1_5_formal_database_import_transaction_bindings.csv",
        "devices_csv": out / "v1_5_formal_database_import_transaction_devices.csv",
        "markdown": out / "V1_5_FORMAL_DATABASE_IMPORT_TRANSACTION_PLAN.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["operations_csv"], model.get("transaction_operations") or [])
    _write_csv(paths["bindings_csv"], model.get("source_bindings") or [])
    _write_csv(paths["devices_csv"], model.get("planned_devices") or [])
    lines = [
        "# V1.5 PostgreSQL 18 import transaction plan",
        "",
        "This is a deterministic offline plan. It does not emit SQL, read a DSN value, connect PostgreSQL, or import rows.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- transaction_plan_contract_ready: `{model.get('transaction_plan_contract_ready')}`",
        f"- production_transaction_package_ready: `{model.get('production_transaction_package_ready')}`",
        f"- planned_device_count: `{model.get('planned_device_count')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        f"- database_written: `{model.get('database_written')}`",
        f"- database_import_allowed: `{model.get('database_import_allowed')}`",
        "",
        "Production blockers:",
        "",
    ]
    reasons = list(model.get("production_blocking_reasons") or [])
    lines.extend([f"- {reason}" for reason in reasons] or ["- none"])
    lines.extend(
        [
            "",
            "The future executor must use one PostgreSQL 18 transaction, pre-commit identity/count/hash readback, and rollback on mismatch.",
        ]
    )
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
