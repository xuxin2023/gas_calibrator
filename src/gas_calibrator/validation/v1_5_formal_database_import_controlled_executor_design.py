"""Offline design review for a future V1.5 PostgreSQL import executor.

The design is intentionally non-executable. It defines the authorization,
transaction, readback, and rollback contracts that a later controlled executor
must satisfy. It never connects to PostgreSQL or imports rows.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_database_import_controlled_executor_design_v1"
READY_STATUS = "ready_for_controlled_import_executor_design_review"
REVIEW_STATUS = "review_required"
DEFAULT_DSN_ENV = "V1_5_POSTGRES_DSN"


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
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def build_v1_5_formal_database_import_controlled_executor_design(
    *,
    formal_database_import_blocked_executor_json: str | Path | None = None,
    dsn_env: str = DEFAULT_DSN_ENV,
) -> dict[str, Any]:
    """Build a no-connect design package for a future controlled import executor."""

    blocked_path = (
        Path(formal_database_import_blocked_executor_json).resolve()
        if formal_database_import_blocked_executor_json
        else None
    )
    blocked_payload = _load_json(blocked_path)
    dsn_env_name = str(dsn_env or DEFAULT_DSN_ENV).strip() or DEFAULT_DSN_ENV

    review_reasons: list[str] = []
    if not blocked_payload:
        review_reasons.append("blocked_executor_evidence_missing")
    elif blocked_payload.get("schema") != "v1_5_formal_database_import_blocked_executor_v1":
        review_reasons.append(f"blocked_executor_schema={blocked_payload.get('schema') or 'missing'}")
    if blocked_payload and blocked_payload.get("connects_postgresql") is not False:
        review_reasons.append("blocked_executor_boundary_connects_postgresql_not_false")
    if blocked_payload and blocked_payload.get("database_written") is not False:
        review_reasons.append("blocked_executor_boundary_database_written_not_false")
    if blocked_payload and blocked_payload.get("senco_authorization_archive_binding_ready") is not True:
        review_reasons.append("blocked_executor_senco_authorization_archive_binding_not_ready")
    if blocked_payload and blocked_payload.get("archive_closure_index_binding_ready") is not True:
        review_reasons.append("blocked_executor_archive_closure_index_binding_not_ready")
    if blocked_payload and blocked_payload.get("database_import_authorization_binding_ready") is not True:
        review_reasons.append("blocked_executor_database_import_authorization_binding_not_ready")

    authorization_contract = [
        {
            "gate": "explicit_execute_flag",
            "required": True,
            "future_flag": "--execute-controlled-import",
            "contract": "Real import must be impossible unless a future controlled executor explicitly exposes and receives this flag.",
        },
        {
            "gate": "operator_confirmation_text",
            "required": True,
            "future_field": "operator_confirmation_text",
            "contract": "Operator must type an exact confirmation phrase tied to run_id/archive hash.",
        },
        {
            "gate": "reviewer_approver_dual_authorization",
            "required": True,
            "future_fields": "reviewer;approver;authorization_id",
            "contract": "Reviewer and approver must be present, distinct, and recorded in the import summary.",
        },
        {
            "gate": "dsn_env_only",
            "required": True,
            "future_field": dsn_env_name,
            "contract": "DSN value must come from environment/secret store and must not be serialized into repository artifacts.",
        },
        {
            "gate": "consume_frozen_contract_inputs",
            "required": True,
            "future_inputs": (
                "command_contract_json;blocked_executor_json;authorization_json;preflight_json;"
                "archive_closure_json;senco_authorization_archive_binding_json;evidence_bundle_json"
            ),
            "contract": "The real executor must consume reviewed artifacts by explicit path and reject mutable run-folder discovery.",
        },
    ]

    transaction_contract = [
        {
            "step": "pre_transaction_input_validation",
            "order": 1,
            "required": True,
            "action": "validate schemas, hashes, SN/device_code uniqueness, archive release, DSN env presence, and authorization",
            "failure_policy": "abort_before_connect_or_before_transaction",
        },
        {
            "step": "begin_transaction",
            "order": 2,
            "required": True,
            "action": "open one PostgreSQL 18 transaction after all offline gates pass",
            "failure_policy": "rollback_and_write_failed_import_attempt_summary_outside_repo_only",
        },
        {
            "step": "insert_registry_rows",
            "order": 3,
            "required": True,
            "action": "insert run, device, artifact, sample, coefficient, report, and traceability rows with idempotency keys",
            "failure_policy": "rollback_transaction_no_partial_acceptance",
        },
        {
            "step": "post_insert_readback_before_commit",
            "order": 4,
            "required": True,
            "action": "query expected row counts and artifact hashes before commit",
            "failure_policy": "rollback_transaction_no_partial_acceptance",
        },
        {
            "step": "commit",
            "order": 5,
            "required": True,
            "action": "commit only after readback and hash checks match the frozen evidence bundle",
            "failure_policy": "database_import_status_failed_review_required",
        },
    ]

    readback_contract = [
        {
            "readback": "run_identity",
            "required": True,
            "query_scope": "run_id;archive_hash;evidence_bundle_hash",
            "expected": "exact match to frozen archive/evidence bundle",
        },
        {
            "readback": "device_identity",
            "required": True,
            "query_scope": "sn_code;device_code;protocol_device_id;transport_label",
            "expected": "SN/device_code unique primary identity and protocol ID alias preserved",
        },
        {
            "readback": "artifact_hashes",
            "required": True,
            "query_scope": "artifact_role;path;sha256",
            "expected": "all formal archive artifacts indexed with matching sha256",
        },
        {
            "readback": "stage_counts",
            "required": True,
            "query_scope": "pressure;CO2;H2O;fit;write;reverify;reports",
            "expected": "row counts equal import preview; no missing formal stage",
        },
    ]

    rollback_contract = [
        {
            "trigger": "validation_failure_before_commit",
            "rollback_action": "rollback transaction",
            "evidence_required": "failed_import_attempt_summary.json with reason and zero committed rows",
            "acceptance_policy": "not_imported_review_required",
        },
        {
            "trigger": "readback_mismatch_before_commit",
            "rollback_action": "rollback transaction",
            "evidence_required": "readback_mismatch_rows.csv and rollback status",
            "acceptance_policy": "not_imported_review_required",
        },
        {
            "trigger": "post_commit_external_discrepancy",
            "rollback_action": "do not auto-delete production rows; mark import under DBA/reviewer hold",
            "evidence_required": "post_commit_discrepancy_report.json and reviewer decision",
            "acceptance_policy": "database_release_suspended_until_review",
        },
    ]

    boundary_gates = [
        {
            "gate": "design_only_no_connect",
            "status": "pass",
            "evidence": "connects_postgresql=false; database_written=false; execution_supported=false",
        },
        {
            "gate": "blocked_executor_consumed",
            "status": "review_required" if review_reasons else "pass",
            "evidence": ";".join(review_reasons) if review_reasons else str(blocked_path),
        },
        {
            "gate": "future_execute_still_blocked",
            "status": "pass",
            "evidence": "current package does not add a real --execute-controlled-import path",
        },
        {
            "gate": "postgresql18_only",
            "status": "pass",
            "evidence": "future executor target remains PostgreSQL major 18",
        },
    ]

    review_required_count = sum(1 for row in boundary_gates if row["status"] == "review_required")
    manifest = {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if review_required_count == 0 else REVIEW_STATUS,
        "blocker_count": 0,
        "review_required_count": review_required_count,
        "production_state": "blocked_design_only",
        "execution_supported": False,
        "real_import_execution_allowed": False,
        "database_import_allowed": False,
        "connects_postgresql": False,
        "applies_migrations": False,
        "database_import_attempted": False,
        "database_written": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "production_backend": "postgresql",
        "production_postgresql_major": 18,
        "dsn_env": dsn_env_name,
        "dsn_value_read": False,
        "formal_database_import_blocked_executor_json": str(blocked_path) if blocked_path else "",
        "senco_authorization_archive_binding_ready": bool(
            blocked_payload.get("senco_authorization_archive_binding_ready")
        ),
        "senco_authorization_archive_binding_json": str(
            blocked_payload.get("senco_authorization_archive_binding_json") or ""
        ),
        "senco_authorization_archive_binding_sha256": str(
            blocked_payload.get("senco_authorization_archive_binding_sha256") or ""
        ),
        "archive_closure_index_binding_ready": bool(
            blocked_payload.get("archive_closure_index_binding_ready")
        ),
        "archive_closure_json": str(blocked_payload.get("archive_closure_json") or ""),
        "archive_closure_sha256": str(blocked_payload.get("archive_closure_sha256") or ""),
        "database_import_authorization_binding_ready": bool(
            blocked_payload.get("database_import_authorization_binding_ready")
        ),
        "formal_database_import_authorization_json": str(
            blocked_payload.get("formal_database_import_authorization_json") or ""
        ),
        "formal_database_import_authorization_sha256": str(
            blocked_payload.get("formal_database_import_authorization_sha256") or ""
        ),
        "required_future_execute_flag": "--execute-controlled-import",
        "not_real_acceptance_evidence": True,
        "next_action": (
            "Keep import locked. Implement a separate controlled executor only after this design, "
            "authorization, transaction, readback, and rollback contract are reviewed."
        ),
    }
    return {
        "manifest": manifest,
        "authorization_contract": authorization_contract,
        "transaction_contract": transaction_contract,
        "readback_contract": readback_contract,
        "rollback_contract": rollback_contract,
        "boundary_gates": boundary_gates,
    }


def write_v1_5_formal_database_import_controlled_executor_design(
    output_dir: str | Path,
    *,
    formal_database_import_blocked_executor_json: str | Path | None = None,
    dsn_env: str = DEFAULT_DSN_ENV,
) -> dict[str, str]:
    tables = build_v1_5_formal_database_import_controlled_executor_design(
        formal_database_import_blocked_executor_json=formal_database_import_blocked_executor_json,
        dsn_env=dsn_env,
    )
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_formal_database_import_controlled_executor_design.json",
        "authorization_contract": out / "v1_5_formal_database_import_controlled_executor_authorization_contract.csv",
        "transaction_contract": out / "v1_5_formal_database_import_controlled_executor_transaction_contract.csv",
        "readback_contract": out / "v1_5_formal_database_import_controlled_executor_readback_contract.csv",
        "rollback_contract": out / "v1_5_formal_database_import_controlled_executor_rollback_contract.csv",
        "boundary_gates": out / "v1_5_formal_database_import_controlled_executor_boundary_gates.csv",
        "summary": out / "V1_5_FORMAL_DATABASE_IMPORT_CONTROLLED_EXECUTOR_DESIGN.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["authorization_contract"], tables["authorization_contract"])
    _write_csv(outputs["transaction_contract"], tables["transaction_contract"])
    _write_csv(outputs["readback_contract"], tables["readback_contract"])
    _write_csv(outputs["rollback_contract"], tables["rollback_contract"])
    _write_csv(outputs["boundary_gates"], tables["boundary_gates"])
    summary = [
        "# V1.5 formal database import controlled executor design",
        "",
        "This is an offline design review for a future PostgreSQL 18 import executor.",
        "",
        f"- overall_status: `{tables['manifest'].get('overall_status')}`",
        f"- production_state: `{tables['manifest'].get('production_state')}`",
        f"- execution_supported: `{tables['manifest'].get('execution_supported')}`",
        f"- real_import_execution_allowed: `{tables['manifest'].get('real_import_execution_allowed')}`",
        f"- archive_closure_index_binding_ready: `{tables['manifest'].get('archive_closure_index_binding_ready')}`",
        f"- archive_closure_sha256: `{tables['manifest'].get('archive_closure_sha256')}`",
        f"- database_import_authorization_binding_ready: `{tables['manifest'].get('database_import_authorization_binding_ready')}`",
        f"- formal_database_import_authorization_sha256: `{tables['manifest'].get('formal_database_import_authorization_sha256')}`",
        f"- database_import_allowed: `{tables['manifest'].get('database_import_allowed')}`",
        f"- connects_postgresql: `{tables['manifest'].get('connects_postgresql')}`",
        f"- database_written: `{tables['manifest'].get('database_written')}`",
        "",
        "Future executor requirements:",
        "",
        "- Explicit `--execute-controlled-import` flag and exact operator confirmation text.",
        "- Distinct reviewer and approver plus authorization id.",
        "- DSN via environment/secret store only; DSN value must not be serialized.",
        "- One PostgreSQL 18 transaction with pre-commit row-count/hash readback.",
        "- Rollback on validation/readback failure before commit; post-commit discrepancies require DBA/reviewer hold.",
        "",
        "Current package remains blocked and does not implement the real executor.",
    ]
    outputs["summary"].parent.mkdir(parents=True, exist_ok=True)
    outputs["summary"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
