"""Blocked executor stub for a future V1.5 PostgreSQL 18 import command.

This module is intentionally read-only. It consumes the import command
contract and records that the production import command is still locked. It
never opens PostgreSQL, applies migrations, imports rows, opens COM ports, or
changes analyzer state.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_formal_database_import_archive_binding import (
    validate_v1_5_database_import_archive_binding,
)


SCHEMA = "v1_5_formal_database_import_blocked_executor_v1"
CONTRACT_SCHEMA = "v1_5_formal_database_import_command_contract_v1"
READY_CONTRACT_STATUS = "ready_for_controlled_postgresql18_import_command_review"
BLOCKED_STATUS = "blocked_pending_controlled_executor_implementation"
REVIEW_STATUS = "review_required"
DEFAULT_DSN_ENV = "V1_5_POSTGRES_DSN"
EXPECTED_COMMAND_MODULE = "gas_calibrator.tools.import_v1_5_evidence_package"


@dataclass(frozen=True)
class FormalDatabaseImportBlockedExecutorCheck:
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
) -> FormalDatabaseImportBlockedExecutorCheck:
    return FormalDatabaseImportBlockedExecutorCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _contract_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["formal_database_import_command_contract_missing"]
    if payload.get("schema") != CONTRACT_SCHEMA:
        reasons.append(f"contract_schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != READY_CONTRACT_STATUS:
        reasons.append(f"contract_status={payload.get('overall_status') or 'missing'}")
    if int(payload.get("blocker_count") or 0):
        reasons.append(f"contract_blocker_count={payload.get('blocker_count')}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"contract_review_required_count={payload.get('review_required_count')}")
    if payload.get("command_contract_ready") is not True:
        reasons.append(f"command_contract_ready={payload.get('command_contract_ready')!r}")
    if payload.get("requested_command_module") != EXPECTED_COMMAND_MODULE:
        reasons.append(f"requested_command_module={payload.get('requested_command_module') or 'missing'}")
    if payload.get("production_backend") != "postgresql":
        reasons.append(f"production_backend={payload.get('production_backend') or 'missing'}")
    if payload.get("production_postgresql_major") != 18:
        reasons.append(f"production_postgresql_major={payload.get('production_postgresql_major') or 'missing'}")
    for field in (
        "authorization_ready",
        "preflight_ready",
        "archive_release_ready",
        "senco_authorization_archive_binding_ready",
        "evidence_bundle_ready",
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
        "database_import_allowed",
        "real_import_execution_allowed",
    ):
        if payload.get(field) is not False:
            reasons.append(f"contract_boundary_{field}={payload.get(field)!r}")
    return reasons


def _path_reasons(payload: Mapping[str, Any], expected: str | Path | None, field: str) -> list[str]:
    expected_path = str(Path(expected).resolve()) if expected else ""
    contract_path = str(payload.get(field) or "")
    if not expected_path:
        return [f"{field}_argument_missing"]
    if not Path(expected_path).exists():
        return [f"{field}_path_missing"]
    if contract_path and str(Path(contract_path).resolve()) != expected_path:
        return [f"{field}_differs_from_contract"]
    return []


def build_v1_5_formal_database_import_blocked_executor(
    *,
    formal_database_import_command_contract_json: str | Path | None,
    formal_database_import_authorization_json: str | Path | None = None,
    formal_database_import_preflight_json: str | Path | None = None,
    archive_closure_json: str | Path | None = None,
    evidence_bundle_json: str | Path | None = None,
    dsn_env: str = DEFAULT_DSN_ENV,
) -> dict[str, Any]:
    contract_path = (
        Path(formal_database_import_command_contract_json).resolve()
        if formal_database_import_command_contract_json
        else None
    )
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
    contract = _load_json(contract_path)
    archive_payload = _load_json(archive_path)
    dsn_env_name = str(dsn_env or DEFAULT_DSN_ENV).strip() or DEFAULT_DSN_ENV

    checks: list[FormalDatabaseImportBlockedExecutorCheck] = []

    contract_reasons = _contract_reasons(contract)
    checks.append(
        _check(
            check="formal_database_import_command_contract_consumed",
            status="ready" if not contract_reasons else "review_required",
            evidence_role="required_command_contract",
            reasons=contract_reasons,
            physical_meaning=(
                "The import command stub must consume the reviewed PostgreSQL 18 command contract "
                "before any future real import executor is considered."
            ),
            next_action="Regenerate the command contract until it is ready, then re-run this blocked executor stub.",
            details={
                "source_path": str(contract_path) if contract_path else "",
                "source_status": contract.get("overall_status", ""),
                "requested_command_module": contract.get("requested_command_module", ""),
            },
        )
    )

    path_checks = (
        ("formal_database_import_authorization_bound", authorization_path, "formal_database_import_authorization_json"),
        ("formal_database_import_preflight_bound", preflight_path, "formal_database_import_preflight_json"),
        ("formal_archive_closure_bound", archive_path, "archive_closure_json"),
        ("formal_evidence_bundle_bound", evidence_bundle_path, "evidence_bundle_json"),
    )
    for check_name, source_path, field in path_checks:
        reasons = _path_reasons(contract, source_path, field)
        checks.append(
            _check(
                check=check_name,
                status="ready" if not reasons else "review_required",
                evidence_role="required_import_input",
                reasons=reasons,
                physical_meaning=(
                    "A future controlled import must use the same frozen inputs that were reviewed "
                    "by the command contract, not discover mutable files at execution time."
                ),
                next_action="Pass the exact reviewed artifact path from the command contract.",
                details={
                    "field": field,
                    "argument_path": str(source_path) if source_path else "",
                    "contract_path": str(contract.get(field) or ""),
                },
            )
        )

    binding_ok, binding_reasons, binding_detail = validate_v1_5_database_import_archive_binding(
        archive_payload
    )
    contract_binding_path = str(contract.get("senco_authorization_archive_binding_json") or "").strip()
    contract_binding_sha = str(
        contract.get("senco_authorization_archive_binding_sha256") or ""
    ).strip().lower()
    if binding_ok:
        if not contract_binding_path or Path(contract_binding_path).resolve() != Path(
            str(binding_detail.get("binding_path") or "")
        ).resolve():
            binding_reasons.append("command_contract_archive_binding_path_mismatch")
        if contract_binding_sha != str(binding_detail.get("binding_sha256") or "").lower():
            binding_reasons.append("command_contract_archive_binding_sha256_mismatch")
    binding_ok = binding_ok and not binding_reasons
    checks.append(
        _check(
            check="senco_authorization_archive_binding_bound",
            status="ready" if binding_ok else "review_required",
            evidence_role="required_senco_write_traceability",
            reasons=binding_reasons,
            physical_meaning=(
                "Even the blocked executor must re-hash the archive-bound SENCO authorization/write/readback "
                "evidence before a future real PostgreSQL connection can be reviewed."
            ),
            next_action="Regenerate the command contract from the current frozen archive binding.",
            details={
                **binding_detail,
                "command_contract_binding_path": contract_binding_path,
                "command_contract_binding_sha256": contract_binding_sha,
            },
        )
    )

    if dsn_env_name != str(contract.get("dsn_env") or dsn_env_name):
        dsn_reasons = [f"dsn_env_differs_from_contract={contract.get('dsn_env') or 'missing'}"]
    else:
        dsn_reasons = []
    checks.append(
        _check(
            check="dsn_env_reference_recorded",
            status="ready" if not dsn_reasons else "review_required",
            evidence_role="dsn_reference_only",
            reasons=dsn_reasons,
            physical_meaning=(
                "This stub records the DSN environment variable name only. It does not read the DSN value "
                "and does not open PostgreSQL."
            ),
            next_action="Keep DSN secret handling outside repository artifacts and review it in the future executor.",
            details={"dsn_env": dsn_env_name, "dsn_value_read": False},
        )
    )

    checks.append(
        _check(
            check="execution_lock_enforced",
            status="ready",
            evidence_role="hard_execution_lock",
            physical_meaning=(
                "The V1.5 import command is still a blocked stub: execution is unsupported, "
                "so no --execute path can import production data."
            ),
            next_action="Design a separate controlled executor with double authorization before any real import.",
            details={
                "execution_supported": False,
                "execution_requested": False,
                "real_import_execution_allowed": False,
            },
        )
    )
    checks.append(
        _check(
            check="postgresql_side_effect_lock",
            status="ready",
            evidence_role="no_connect_no_write_boundary",
            physical_meaning=(
                "This command must prove that no PostgreSQL connection, migration, or row import occurred."
            ),
            next_action="Keep this evidence as a pre-executor safety record, not a database-import result.",
            details={
                "connects_postgresql": False,
                "applies_migrations": False,
                "database_import_attempted": False,
                "database_written": False,
            },
        )
    )

    review_required_count = sum(1 for row in checks if row.status == "review_required")
    blocker_count = 0
    contract_ready_for_future_review = review_required_count == 0

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": BLOCKED_STATUS if contract_ready_for_future_review else REVIEW_STATUS,
        "blocker_count": blocker_count,
        "review_required_count": review_required_count,
        "blocked_executor_ready": contract_ready_for_future_review,
        "contract_ready_for_future_execution_review": contract_ready_for_future_review,
        "execution_supported": False,
        "execution_requested": False,
        "real_import_execution_allowed": False,
        "dry_run_only": True,
        "production_backend": "postgresql",
        "production_postgresql_major": 18,
        "formal_database_import_command_contract_json": str(contract_path) if contract_path else "",
        "formal_database_import_authorization_json": str(authorization_path) if authorization_path else "",
        "formal_database_import_preflight_json": str(preflight_path) if preflight_path else "",
        "archive_closure_json": str(archive_path) if archive_path else "",
        "evidence_bundle_json": str(evidence_bundle_path) if evidence_bundle_path else "",
        "senco_authorization_archive_binding_ready": binding_ok,
        "senco_authorization_archive_binding_json": binding_detail.get("binding_path", ""),
        "senco_authorization_archive_binding_sha256": binding_detail.get("binding_sha256", ""),
        "dsn_env": dsn_env_name,
        "dsn_value_read": False,
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
        "formal_release_allowed": False,
        "not_real_acceptance_evidence": True,
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Keep production database import locked. A later controlled executor must add explicit "
            "double authorization, read this stub/contract, connect to PostgreSQL 18 only inside that "
            "future reviewed path, and write separate readback/import evidence."
        ),
    }


def write_v1_5_formal_database_import_blocked_executor_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_database_import_blocked_executor.json",
        "checks_csv": out / "v1_5_formal_database_import_blocked_executor_checks.csv",
        "summary_csv": out / "v1_5_formal_database_import_blocked_executor_summary.csv",
        "markdown": out / "V1_5_FORMAL_DATABASE_IMPORT_BLOCKED_EXECUTOR.md",
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
                "blocked_executor_ready": model.get("blocked_executor_ready"),
                "senco_authorization_archive_binding_ready": model.get(
                    "senco_authorization_archive_binding_ready"
                ),
                "execution_supported": model.get("execution_supported"),
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
        "# V1.5 formal database import blocked executor",
        "",
        "This is a no-connect, no-write executor stub for the future PostgreSQL 18 import command.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocked_executor_ready: `{model.get('blocked_executor_ready')}`",
        f"- senco_authorization_archive_binding_ready: `{model.get('senco_authorization_archive_binding_ready')}`",
        f"- execution_supported: `{model.get('execution_supported')}`",
        f"- real_import_execution_allowed: `{model.get('real_import_execution_allowed')}`",
        f"- database_import_allowed: `{model.get('database_import_allowed')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        f"- applies_migrations: `{model.get('applies_migrations')}`",
        f"- database_written: `{model.get('database_written')}`",
        "- This stub does not connect PostgreSQL, apply migrations, import rows, open COM, control routes, or write analyzer state.",
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
