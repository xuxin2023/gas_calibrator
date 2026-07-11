"""Offline command contract for a future V1.5 PostgreSQL 18 import.

This module reviews the exact inputs and execution locks that a separate real
database import command must consume. It never opens PostgreSQL, applies
migrations, imports rows, opens COM ports, or changes analyzer state.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_artifact_hash_binding import sha256_file
from .v1_5_formal_database_import_archive_index_binding import (
    validate_v1_5_formal_archive_index_binding,
)
from .v1_5_formal_database_import_archive_binding import (
    validate_v1_5_database_import_archive_binding,
)


SCHEMA = "v1_5_formal_database_import_command_contract_v1"
READY_STATUS = "ready_for_controlled_postgresql18_import_command_review"
REVIEW_STATUS = "review_required"
BLOCKED_STATUS = "blocked"
DEFAULT_DSN_ENV = "V1_5_POSTGRES_DSN"
DEFAULT_REQUESTED_COMMAND_MODULE = "gas_calibrator.tools.import_v1_5_evidence_package"


@dataclass(frozen=True)
class FormalDatabaseImportCommandContractCheck:
    check: str
    status: str
    evidence_role: str
    reasons: tuple[str, ...]
    physical_meaning: str
    next_action: str
    details: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


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


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], *, fallback_fields: Sequence[str] = ()) -> None:
    fields: list[str] = [str(field) for field in fallback_fields]
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _check(
    *,
    check: str,
    status: str,
    evidence_role: str,
    reasons: Sequence[str] = (),
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> FormalDatabaseImportCommandContractCheck:
    return FormalDatabaseImportCommandContractCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _authorization_ready(payload: Mapping[str, Any]) -> tuple[bool, list[str], str]:
    reasons: list[str] = []
    if not payload:
        reasons.append("formal_database_import_authorization_missing")
        return False, reasons, "blocker"
    if payload.get("overall_status") != "ready_for_manual_postgresql18_import_authorization":
        reasons.append(f"authorization_status={payload.get('overall_status') or 'missing'}")
    if int(payload.get("blocker_count") or 0):
        reasons.append(f"authorization_blocker_count={payload.get('blocker_count')}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"authorization_review_required_count={payload.get('review_required_count')}")
    if payload.get("production_backend") != "postgresql":
        reasons.append(f"production_backend={payload.get('production_backend') or 'missing'}")
    if payload.get("production_postgresql_major") != 18:
        reasons.append(f"production_postgresql_major={payload.get('production_postgresql_major') or 'missing'}")
    operator = str(payload.get("operator") or "").strip()
    reviewer = str(payload.get("reviewer") or "").strip()
    approver = str(payload.get("approver") or "").strip()
    authorization_id = str(payload.get("authorization_id") or "").strip()
    if not operator:
        reasons.append("authorization_operator_missing")
    if not reviewer:
        reasons.append("authorization_reviewer_missing")
    if not approver:
        reasons.append("authorization_approver_missing")
    if reviewer and approver and reviewer.casefold() == approver.casefold():
        reasons.append("authorization_reviewer_approver_must_be_distinct")
    if not authorization_id:
        reasons.append("authorization_id_missing")
    for field in (
        "preflight_ready",
        "archive_release_ready",
        "archive_closure_index_binding_ready",
        "senco_authorization_archive_binding_ready",
        "manual_authorization_ready",
        "database_import_allowed",
    ):
        if payload.get(field) is not True:
            reasons.append(f"{field}={payload.get(field)!r}")
    for field in (
        "connects_postgresql",
        "opens_com_ports",
        "controls_water_or_gas_routes",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "applies_migrations",
        "database_import_attempted",
        "database_written",
    ):
        if payload.get(field) is not False:
            reasons.append(f"authorization_boundary_{field}={payload.get(field)!r}")
    if not reasons:
        return True, reasons, "ready"
    if "authorization_reviewer_approver_must_be_distinct" in reasons:
        return False, reasons, "blocker"
    if payload.get("overall_status") == "blocked" or int(payload.get("blocker_count") or 0):
        return False, reasons, "blocker"
    return False, reasons, "review_required"


def _preflight_ready(payload: Mapping[str, Any]) -> tuple[bool, list[str], str]:
    reasons: list[str] = []
    if not payload:
        reasons.append("formal_database_import_preflight_missing")
        return False, reasons, "blocker"
    if payload.get("overall_status") != "ready_for_authorized_postgresql18_import_review":
        reasons.append(f"preflight_status={payload.get('overall_status') or 'missing'}")
    if int(payload.get("blocker_count") or 0):
        reasons.append(f"preflight_blocker_count={payload.get('blocker_count')}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"preflight_review_required_count={payload.get('review_required_count')}")
    if payload.get("production_backend") != "postgresql":
        reasons.append(f"production_backend={payload.get('production_backend') or 'missing'}")
    if payload.get("production_postgresql_major") != 18:
        reasons.append(f"production_postgresql_major={payload.get('production_postgresql_major') or 'missing'}")
    if payload.get("dry_run_contract_ready") is not True:
        reasons.append("dry_run_contract_not_ready")
    if payload.get("dsn_configured") is not True:
        reasons.append("dsn_configured_not_true")
    for field in (
        "connects_postgresql",
        "opens_com_ports",
        "controls_water_or_gas_routes",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "applies_migrations",
        "database_import_attempted",
        "database_written",
        "database_import_allowed",
        "formal_release_allowed",
    ):
        if payload.get(field) is not False:
            reasons.append(f"preflight_boundary_{field}={payload.get(field)!r}")
    if not reasons:
        return True, reasons, "ready"
    if payload.get("overall_status") == "blocked" or int(payload.get("blocker_count") or 0):
        return False, reasons, "blocker"
    return False, reasons, "review_required"


def _archive_ready(payload: Mapping[str, Any]) -> tuple[bool, list[str], str]:
    reasons: list[str] = []
    if not payload:
        reasons.append("archive_closure_missing")
        return False, reasons, "review_required"
    if payload.get("overall_status") not in {"ready", "ready_for_formal_release"}:
        reasons.append(f"archive_status={payload.get('overall_status') or 'missing'}")
    if payload.get("package_status") not in {"ready", "ready_for_formal_release"}:
        reasons.append(f"package_status={payload.get('package_status') or 'missing'}")
    traceability = payload.get("identity_getco_traceability") or {}
    if traceability.get("ready_for_archive_release") is not True:
        reasons.append("identity_traceability_not_ready")
    if traceability.get("traceability_review_required") is True:
        reasons.append("identity_traceability_review_required")
    return not reasons, reasons, "ready" if not reasons else "review_required"


def _evidence_bundle_ready(payload: Mapping[str, Any]) -> tuple[bool, list[str], str]:
    reasons: list[str] = []
    if not payload:
        reasons.append("evidence_bundle_missing")
        return False, reasons, "review_required"
    if not isinstance(payload, Mapping):
        reasons.append("evidence_bundle_not_json_object")
    return not reasons, reasons, "ready" if not reasons else "review_required"


def build_v1_5_formal_database_import_command_contract(
    *,
    formal_database_import_authorization_json: str | Path | None = None,
    formal_database_import_preflight_json: str | Path | None = None,
    archive_closure_json: str | Path | None = None,
    evidence_bundle_json: str | Path | None = None,
    dsn_env: str = DEFAULT_DSN_ENV,
    requested_command_module: str = DEFAULT_REQUESTED_COMMAND_MODULE,
) -> dict[str, Any]:
    authorization_path = (
        Path(formal_database_import_authorization_json).resolve()
        if formal_database_import_authorization_json
        else None
    )
    preflight_path = (
        Path(formal_database_import_preflight_json).resolve()
        if formal_database_import_preflight_json
        else None
    )
    archive_path = Path(archive_closure_json).resolve() if archive_closure_json else None
    evidence_bundle_path = Path(evidence_bundle_json).resolve() if evidence_bundle_json else None
    authorization_payload = _load_json(authorization_path)
    preflight_payload = _load_json(preflight_path)
    archive_payload = _load_json(archive_path)
    evidence_bundle_payload = _load_json(evidence_bundle_path)
    dsn_env_name = str(dsn_env or DEFAULT_DSN_ENV).strip() or DEFAULT_DSN_ENV
    command_module = str(requested_command_module or DEFAULT_REQUESTED_COMMAND_MODULE).strip()

    checks: list[FormalDatabaseImportCommandContractCheck] = []

    authorization_ok, authorization_reasons, authorization_status = _authorization_ready(authorization_payload)
    authorization_current_sha = (
        sha256_file(authorization_path) if authorization_path and authorization_path.is_file() else ""
    )
    authorization_hash_ready = bool(authorization_current_sha)
    checks.append(
        _check(
            check="formal_database_import_authorization_ready",
            status=authorization_status,
            evidence_role="required_manual_authorization",
            reasons=authorization_reasons,
            physical_meaning=(
                "A real PostgreSQL import command can only be reviewed after manual authorization, "
                "archive release, and import preflight are all represented by no-connect evidence."
            ),
            next_action="Resolve authorization blockers/review items before preparing a real import command.",
            details={
                "source_path": str(authorization_path) if authorization_path else "",
                "authorization_status": authorization_payload.get("overall_status", ""),
                "database_import_allowed": authorization_payload.get("database_import_allowed", False),
            },
        )
    )

    preflight_ok, preflight_reasons, preflight_status = _preflight_ready(preflight_payload)
    checks.append(
        _check(
            check="formal_database_import_preflight_ready",
            status=preflight_status,
            evidence_role="required_import_preflight",
            reasons=preflight_reasons,
            physical_meaning=(
                "The command contract must point to a PostgreSQL 18 import preflight with DSN presence, "
                "migration lock, and no execution side effects."
            ),
            next_action="Regenerate or review the database import preflight before command review.",
            details={
                "source_path": str(preflight_path) if preflight_path else "",
                "preflight_status": preflight_payload.get("overall_status", ""),
                "dsn_configured": preflight_payload.get("dsn_configured", False),
            },
        )
    )

    archive_ok, archive_reasons, archive_status = _archive_ready(archive_payload)
    checks.append(
        _check(
            check="formal_archive_closure_ready",
            status=archive_status,
            evidence_role="required_release_evidence",
            reasons=archive_reasons,
            physical_meaning="Database import is a formal archive/release action, not a calibration-stage action.",
            next_action="Complete formal archive closure and SN/device_code traceability before real import.",
            details={
                "source_path": str(archive_path) if archive_path else "",
                "archive_status": archive_payload.get("overall_status", ""),
                "package_status": archive_payload.get("package_status", ""),
            },
        )
    )
    checks.append(
        _check(
            check="formal_database_import_authorization_hash_bound",
            status="ready" if authorization_hash_ready else "blocker",
            evidence_role="required_frozen_manual_authorization",
            reasons=() if authorization_hash_ready else ("authorization_json_hash_unavailable",),
            physical_meaning=(
                "The command contract freezes the complete manual authorization JSON, including independent "
                "reviewer and approver identities, before any later executor review."
            ),
            next_action="Generate a complete database-import authorization JSON before command-contract review.",
            details={
                "authorization_json": str(authorization_path) if authorization_path else "",
                "authorization_sha256": authorization_current_sha,
            },
        )
    )

    archive_index_ok, archive_index_reasons, archive_index_detail = (
        validate_v1_5_formal_archive_index_binding(
            archive_path,
            expected_path=authorization_payload.get("archive_closure_json"),
            expected_sha256=str(authorization_payload.get("archive_closure_sha256") or ""),
            source_label="authorization",
        )
    )
    checks.append(
        _check(
            check="formal_archive_index_bound_to_authorization",
            status="ready" if archive_index_ok else "blocker",
            evidence_role="required_frozen_archive_index",
            reasons=archive_index_reasons,
            physical_meaning=(
                "The command contract must consume the exact archive closure index path and SHA-256 that manual "
                "database-import authorization reviewed."
            ),
            next_action="Regenerate database-import authorization after freezing the final archive closure index.",
            details=archive_index_detail,
        )
    )

    binding_ok, binding_reasons, binding_detail = validate_v1_5_database_import_archive_binding(
        archive_payload
    )
    authorization_binding_path = str(
        authorization_payload.get("senco_authorization_archive_binding_json") or ""
    ).strip()
    authorization_binding_sha = str(
        authorization_payload.get("senco_authorization_archive_binding_sha256") or ""
    ).strip().lower()
    if binding_ok and authorization_payload:
        if not authorization_binding_path or Path(authorization_binding_path).resolve() != Path(
            str(binding_detail.get("binding_path") or "")
        ).resolve():
            binding_reasons.append("authorization_archive_binding_path_mismatch")
        if authorization_binding_sha != str(binding_detail.get("binding_sha256") or "").lower():
            binding_reasons.append("authorization_archive_binding_sha256_mismatch")
    binding_ok = binding_ok and not binding_reasons
    checks.append(
        _check(
            check="senco_authorization_archive_binding_ready",
            status="ready" if binding_ok else "blocker",
            evidence_role="required_senco_write_traceability",
            reasons=binding_reasons,
            physical_meaning=(
                "The import command must re-hash the exact SENCO authorization/write/readback binding that was "
                "present when manual database-import authorization was created."
            ),
            next_action="Regenerate archive closure and database-import authorization from the same frozen binding.",
            details={
                **binding_detail,
                "authorization_binding_path": authorization_binding_path,
                "authorization_binding_sha256": authorization_binding_sha,
            },
        )
    )

    evidence_bundle_ok, evidence_reasons, evidence_status = _evidence_bundle_ready(evidence_bundle_payload)
    checks.append(
        _check(
            check="formal_evidence_bundle_ready",
            status=evidence_status,
            evidence_role="required_import_payload",
            reasons=evidence_reasons,
            physical_meaning="The import command must consume the frozen evidence bundle, not scan a mutable run folder.",
            next_action="Build or review the formal evidence bundle before a controlled import command.",
            details={
                "source_path": str(evidence_bundle_path) if evidence_bundle_path else "",
                "bundle_schema": evidence_bundle_payload.get("schema", ""),
            },
        )
    )

    checks.append(
        _check(
            check="controlled_import_command_contract",
            status="ready",
            evidence_role="execution_contract",
            physical_meaning=(
                "This artifact defines the future import command inputs and keeps execution locked off."
            ),
            next_action="A separate real import command must consume this contract and re-check every required input.",
            details={
                "requested_command_module": command_module,
                "dsn_env": dsn_env_name,
                "execution_requested": False,
                "connects_postgresql": False,
                "database_import_attempted": False,
                "database_written": False,
                "required_command_inputs": [
                    "formal_database_import_authorization_json",
                    "formal_database_import_preflight_json",
                    "formal_archive_closure_index_json",
                    "senco_authorization_archive_binding_json",
                    "evidence_bundle_json",
                    "dsn_env",
                ],
            },
        )
    )

    checks.append(
        _check(
            check="migration_execution_lock",
            status="ready",
            evidence_role="migration_boundary",
            physical_meaning="Schema migrations remain outside this V1.5 formal import command contract.",
            next_action="Use a separately reviewed migration procedure if schema changes are ever required.",
            details={"applies_migrations": False, "migrations_applied": False},
        )
    )

    blocker_count = sum(1 for row in checks if row.status == "blocker")
    review_required_count = sum(1 for row in checks if row.status == "review_required")
    command_contract_ready = blocker_count == 0 and review_required_count == 0
    if blocker_count:
        overall_status = BLOCKED_STATUS
    elif review_required_count:
        overall_status = REVIEW_STATUS
    else:
        overall_status = READY_STATUS

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": overall_status,
        "blocker_count": blocker_count,
        "review_required_count": review_required_count,
        "production_backend": "postgresql",
        "production_postgresql_major": 18,
        "formal_database_import_authorization_json": str(authorization_path) if authorization_path else "",
        "formal_database_import_authorization_sha256": authorization_current_sha,
        "database_import_authorization_binding_ready": authorization_hash_ready and authorization_ok,
        "formal_database_import_preflight_json": str(preflight_path) if preflight_path else "",
        "archive_closure_json": str(archive_path) if archive_path else "",
        "evidence_bundle_json": str(evidence_bundle_path) if evidence_bundle_path else "",
        "dsn_env": dsn_env_name,
        "requested_command_module": command_module,
        "authorization_ready": authorization_ok,
        "preflight_ready": preflight_ok,
        "archive_release_ready": archive_ok and binding_ok and archive_index_ok,
        "archive_closure_index_binding_ready": archive_index_ok,
        "archive_closure_sha256": archive_index_detail.get("archive_closure_sha256", ""),
        "senco_authorization_archive_binding_ready": binding_ok,
        "senco_authorization_archive_binding_json": binding_detail.get("binding_path", ""),
        "senco_authorization_archive_binding_sha256": binding_detail.get("binding_sha256", ""),
        "evidence_bundle_ready": evidence_bundle_ok,
        "command_contract_ready": command_contract_ready,
        "connects_postgresql": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "applies_migrations": False,
        "database_import_attempted": False,
        "database_written": False,
        "database_import_allowed": False,
        "real_import_execution_allowed": False,
        "formal_release_allowed": archive_ok and binding_ok and archive_index_ok,
        "not_real_acceptance_evidence": True,
        "required_command_inputs": [
            "formal_database_import_authorization_json",
            "formal_database_import_preflight_json",
            "formal_archive_closure_index_json",
            "senco_authorization_archive_binding_json",
            "evidence_bundle_json",
            "dsn_env",
        ],
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Resolve blockers/review items. A separate controlled PostgreSQL 18 import command must consume this "
            "contract plus authorization, preflight, archive, evidence-bundle, and DSN evidence; this artifact never imports data."
        ),
    }


def write_v1_5_formal_database_import_command_contract_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_database_import_command_contract.json",
        "checks_csv": out / "v1_5_formal_database_import_command_contract_checks.csv",
        "summary_csv": out / "v1_5_formal_database_import_command_contract_summary.csv",
        "markdown": out / "V1_5_FORMAL_DATABASE_IMPORT_COMMAND_CONTRACT.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "blocker_count": model.get("blocker_count"),
                "review_required_count": model.get("review_required_count"),
                "authorization_ready": model.get("authorization_ready"),
                "database_import_authorization_binding_ready": model.get(
                    "database_import_authorization_binding_ready"
                ),
                "formal_database_import_authorization_sha256": model.get(
                    "formal_database_import_authorization_sha256"
                ),
                "preflight_ready": model.get("preflight_ready"),
                "archive_release_ready": model.get("archive_release_ready"),
                "archive_closure_index_binding_ready": model.get(
                    "archive_closure_index_binding_ready"
                ),
                "archive_closure_sha256": model.get("archive_closure_sha256"),
                "senco_authorization_archive_binding_ready": model.get(
                    "senco_authorization_archive_binding_ready"
                ),
                "evidence_bundle_ready": model.get("evidence_bundle_ready"),
                "command_contract_ready": model.get("command_contract_ready"),
                "real_import_execution_allowed": model.get("real_import_execution_allowed"),
                "connects_postgresql": model.get("connects_postgresql"),
                "applies_migrations": model.get("applies_migrations"),
                "database_import_attempted": model.get("database_import_attempted"),
                "database_written": model.get("database_written"),
                "database_import_allowed": model.get("database_import_allowed"),
            }
        ],
    )
    lines = [
        "# V1.5 formal database import command contract",
        "",
        "This is an offline command contract for a future separately controlled PostgreSQL 18 import.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        f"- review_required_count: `{model.get('review_required_count')}`",
        f"- command_contract_ready: `{model.get('command_contract_ready')}`",
        f"- database_import_authorization_binding_ready: `{model.get('database_import_authorization_binding_ready')}`",
        f"- formal_database_import_authorization_sha256: `{model.get('formal_database_import_authorization_sha256')}`",
        f"- archive_closure_index_binding_ready: `{model.get('archive_closure_index_binding_ready')}`",
        f"- archive_closure_sha256: `{model.get('archive_closure_sha256')}`",
        f"- senco_authorization_archive_binding_ready: `{model.get('senco_authorization_archive_binding_ready')}`",
        f"- real_import_execution_allowed: `{model.get('real_import_execution_allowed')}`",
        f"- database_import_allowed: `{model.get('database_import_allowed')}`",
        f"- requested_command_module: `{model.get('requested_command_module')}`",
        "- This artifact does not connect PostgreSQL, apply migrations, import data, open COM, control routes, or write analyzer state.",
        "",
        "## Checks",
        "",
    ]
    for row in model.get("checks", []):
        reasons = ";".join(row.get("reasons") or ())
        lines.append(f"- `{row.get('check')}`: `{row.get('status')}` {reasons}".rstrip())
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
