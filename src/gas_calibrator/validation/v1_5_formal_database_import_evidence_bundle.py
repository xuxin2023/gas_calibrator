"""Validate the frozen V1.5 evidence bundle before database-import review.

This module is intentionally offline. It validates the registry shape and the
artifact roles already indexed in ``evidence_bundle.json``; it does not scan a
run directory, open COM ports, or connect to PostgreSQL.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..storage.v1_5_evidence.bundle import TABLE_NAMES


EXPECTED_SCHEMA = "v1_5_evidence_registry"
EXPECTED_SCHEMA_VERSION = "001"

REQUIRED_NONEMPTY_TABLES = (
    "runs",
    "devices",
    "run_devices",
    "standard_gases",
    "reference_certificates",
    "calibration_points",
    "sample_files",
    "qc_results",
    "reports",
    "audit_events",
    "evidence_integrity_checks",
)

REQUIRED_ARTIFACT_ROLES = (
    "raw_samples",
    "formal_plan_snapshot",
    "pressure_reference_snapshot",
    "run_evidence_status",
    "formal_run_status",
    "formal_calibration_report",
)

REQUIRED_ARTIFACT_ROLE_GROUPS = {
    "pressure_channel_evidence": (
        "pressure_channel_quick_check",
        "pressure_channel_completion",
    ),
}

READY_EVIDENCE_STATUSES = {"ready", "ready_for_reviewer", "ready_for_formal_release"}
READY_PACKAGE_STATUSES = {"ready", "ready_for_reviewer", "ready_for_formal_release"}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _rows(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [row for row in value if isinstance(row, Mapping)]


def _artifact_row_reasons(row: Mapping[str, Any], *, label: str) -> list[str]:
    reasons: list[str] = []
    artifact_id = str(row.get("id") or "").strip()
    path_text = str(row.get("path") or "").strip()
    sha256 = str(row.get("sha256") or "").strip().lower()
    if not artifact_id:
        reasons.append(f"{label}_id_missing")
    if not path_text:
        reasons.append(f"{label}_path_missing")
    elif not Path(path_text).is_absolute():
        reasons.append(f"{label}_path_not_absolute")
    if not _SHA256_RE.fullmatch(sha256):
        reasons.append(f"{label}_sha256_invalid")
    return reasons


def validate_v1_5_formal_database_import_evidence_bundle(
    payload: Mapping[str, Any] | None,
) -> tuple[bool, list[str], dict[str, Any]]:
    """Return whether a bundle is structurally eligible for import review."""

    reasons: list[str] = []
    if not isinstance(payload, Mapping) or not payload:
        return False, ["evidence_bundle_missing_or_not_json_object"], {
            "expected_schema": EXPECTED_SCHEMA,
            "expected_schema_version": EXPECTED_SCHEMA_VERSION,
            "required_artifact_roles": list(REQUIRED_ARTIFACT_ROLES),
        }

    schema = str(payload.get("schema") or "").strip()
    schema_version = str(payload.get("schema_version") or "").strip()
    run_id = str(payload.get("run_id") or "").strip()
    run_db_id = str(payload.get("run_db_id") or "").strip()
    if schema != EXPECTED_SCHEMA:
        reasons.append(f"evidence_bundle_schema={schema or 'missing'}")
    if schema_version != EXPECTED_SCHEMA_VERSION:
        reasons.append(f"evidence_bundle_schema_version={schema_version or 'missing'}")
    if not str(payload.get("created_at") or "").strip():
        reasons.append("evidence_bundle_created_at_missing")
    if not run_id:
        reasons.append("evidence_bundle_run_id_missing")
    if not run_db_id:
        reasons.append("evidence_bundle_run_db_id_missing")

    tables = payload.get("tables")
    if not isinstance(tables, Mapping):
        reasons.append("evidence_bundle_tables_not_object")
        tables = {}

    missing_tables: list[str] = []
    invalid_table_types: list[str] = []
    empty_required_tables: list[str] = []
    for table_name in TABLE_NAMES:
        if table_name not in tables:
            missing_tables.append(table_name)
            continue
        value = tables.get(table_name)
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            invalid_table_types.append(table_name)
    for table_name in REQUIRED_NONEMPTY_TABLES:
        if table_name in tables and not _rows(tables.get(table_name)):
            empty_required_tables.append(table_name)
    reasons.extend(f"evidence_bundle_table_missing:{name}" for name in missing_tables)
    reasons.extend(f"evidence_bundle_table_not_array:{name}" for name in invalid_table_types)
    reasons.extend(f"evidence_bundle_required_table_empty:{name}" for name in empty_required_tables)

    run_rows = _rows(tables.get("runs"))
    if len(run_rows) != 1:
        reasons.append(f"evidence_bundle_run_row_count={len(run_rows)}")
    elif run_rows:
        run_row = run_rows[0]
        if str(run_row.get("id") or "").strip() != run_db_id:
            reasons.append("evidence_bundle_run_row_id_mismatch")
        if str(run_row.get("run_id") or "").strip() != run_id:
            reasons.append("evidence_bundle_run_row_run_id_mismatch")
        evidence_status = str(run_row.get("evidence_status") or "").strip()
        package_status = str(run_row.get("package_status") or "").strip()
        if evidence_status not in READY_EVIDENCE_STATUSES:
            reasons.append(f"evidence_bundle_run_evidence_status={evidence_status or 'missing'}")
        if package_status not in READY_PACKAGE_STATUSES:
            reasons.append(f"evidence_bundle_run_package_status={package_status or 'missing'}")

    sample_files = _rows(tables.get("sample_files"))
    rows_by_role: dict[str, list[Mapping[str, Any]]] = {}
    for row in sample_files:
        role = str(row.get("artifact_role") or "").strip()
        if role:
            rows_by_role.setdefault(role, []).append(row)
    present_roles = sorted(rows_by_role)

    missing_roles: list[str] = []
    invalid_roles: list[str] = []
    validated_role_rows = 0
    for role in REQUIRED_ARTIFACT_ROLES:
        role_rows = rows_by_role.get(role, [])
        if not role_rows:
            missing_roles.append(role)
            continue
        valid_row = False
        for index, row in enumerate(role_rows):
            row_reasons = _artifact_row_reasons(row, label=f"artifact_role:{role}:{index}")
            if not row_reasons:
                valid_row = True
                validated_role_rows += 1
            else:
                reasons.extend(row_reasons)
        if not valid_row:
            invalid_roles.append(role)

    role_group_matches: dict[str, list[str]] = {}
    missing_role_groups: list[str] = []
    for group_name, accepted_roles in REQUIRED_ARTIFACT_ROLE_GROUPS.items():
        matching_roles = [role for role in accepted_roles if rows_by_role.get(role)]
        role_group_matches[group_name] = matching_roles
        if not matching_roles:
            missing_role_groups.append(group_name)
            continue
        valid_group_row = False
        for role in matching_roles:
            for index, row in enumerate(rows_by_role[role]):
                row_reasons = _artifact_row_reasons(row, label=f"artifact_role:{role}:{index}")
                if not row_reasons:
                    valid_group_row = True
                    validated_role_rows += 1
                else:
                    reasons.extend(row_reasons)
        if not valid_group_row:
            invalid_roles.extend(matching_roles)

    reasons.extend(f"evidence_bundle_required_artifact_role_missing:{role}" for role in missing_roles)
    reasons.extend(f"evidence_bundle_required_artifact_role_invalid:{role}" for role in sorted(set(invalid_roles)))
    reasons.extend(
        f"evidence_bundle_required_artifact_role_group_missing:{group}"
        for group in missing_role_groups
    )

    required_rows = [row for row in sample_files if row.get("required") is True]
    if not required_rows:
        reasons.append("evidence_bundle_required_artifact_rows_missing")
    for index, row in enumerate(required_rows):
        reasons.extend(_artifact_row_reasons(row, label=f"required_artifact:{index}"))

    artifact_ids = [str(row.get("id") or "").strip() for row in sample_files if row.get("id")]
    if len(artifact_ids) != len(set(artifact_ids)):
        reasons.append("evidence_bundle_duplicate_artifact_ids")

    failing_integrity_checks: list[str] = []
    for row in _rows(tables.get("evidence_integrity_checks")):
        if str(row.get("severity") or "").strip().lower() == "error" and str(
            row.get("status") or ""
        ).strip().lower() != "pass":
            failing_integrity_checks.append(str(row.get("check_name") or "unnamed"))
    reasons.extend(
        f"evidence_bundle_integrity_check_not_pass:{name}" for name in failing_integrity_checks
    )

    details = {
        "schema": schema,
        "schema_version": schema_version,
        "expected_schema": EXPECTED_SCHEMA,
        "expected_schema_version": EXPECTED_SCHEMA_VERSION,
        "run_id": run_id,
        "run_db_id": run_db_id,
        "required_table_names": list(TABLE_NAMES),
        "missing_table_names": missing_tables,
        "invalid_table_types": invalid_table_types,
        "empty_required_tables": empty_required_tables,
        "required_artifact_roles": list(REQUIRED_ARTIFACT_ROLES),
        "required_artifact_role_groups": {
            key: list(value) for key, value in REQUIRED_ARTIFACT_ROLE_GROUPS.items()
        },
        "present_artifact_roles": present_roles,
        "missing_artifact_roles": missing_roles,
        "missing_artifact_role_groups": missing_role_groups,
        "role_group_matches": role_group_matches,
        "sample_file_count": len(sample_files),
        "required_artifact_count": len(required_rows),
        "validated_required_role_row_count": validated_role_rows,
        "failing_integrity_checks": failing_integrity_checks,
    }
    return not reasons, reasons, details
