"""Offline import preflight for the V1.5 PostgreSQL 18 production database.

This module deliberately does not create an engine, connect to PostgreSQL,
apply migrations, or import rows. It reviews whether the dry-run database
contract and DSN/authorization boundaries are ready for a future separately
authorized import step.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_database_import_preflight_v1"
READY_STATUS = "ready_for_authorized_postgresql18_import_review"
REVIEW_STATUS = "review_required"
BLOCKED_STATUS = "blocked"
DEFAULT_DSN_ENV = "V1_5_POSTGRES_DSN"


@dataclass(frozen=True)
class FormalDatabaseImportPreflightCheck:
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
) -> FormalDatabaseImportPreflightCheck:
    return FormalDatabaseImportPreflightCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _dsn_from_inputs(*, dsn: str | None, dsn_env: str) -> tuple[str, str]:
    text = str(dsn or "").strip()
    if text:
        return text, "explicit_argument"
    env_name = str(dsn_env or "").strip()
    if env_name:
        env_value = str(os.environ.get(env_name) or "").strip()
        if env_value:
            return env_value, f"env:{env_name}"
    return "", ""


def _dsn_fingerprint(dsn: str) -> str:
    if not dsn:
        return ""
    return hashlib.sha256(dsn.encode("utf-8")).hexdigest()[:16]


def _dry_run_contract_ready(payload: Mapping[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not payload:
        reasons.append("formal_database_dry_run_missing")
        return False, reasons
    if payload.get("overall_status") != "ready_for_postgresql18_schema_dry_run_review":
        reasons.append(f"dry_run_status={payload.get('overall_status') or 'missing'}")
    if int(payload.get("blocker_count") or 0):
        reasons.append(f"dry_run_blocker_count={payload.get('blocker_count')}")
    if payload.get("production_backend") != "postgresql":
        reasons.append(f"production_backend={payload.get('production_backend') or 'missing'}")
    if payload.get("production_postgresql_major") != 18:
        reasons.append(f"production_postgresql_major={payload.get('production_postgresql_major') or 'missing'}")
    if payload.get("primary_identity") != "sn_code/device_code":
        reasons.append(f"primary_identity={payload.get('primary_identity') or 'missing'}")
    for field in (
        "connects_postgresql",
        "opens_com_ports",
        "controls_water_or_gas_routes",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "database_written",
        "database_import_allowed",
        "formal_release_allowed",
    ):
        if payload.get(field) is not False:
            reasons.append(f"dry_run_boundary_{field}={payload.get(field)!r}")
    return not reasons, reasons


def build_v1_5_formal_database_import_preflight(
    *,
    formal_database_dry_run_json: str | Path | None = None,
    dsn: str | None = None,
    dsn_env: str = DEFAULT_DSN_ENV,
) -> dict[str, Any]:
    dry_run_path = Path(formal_database_dry_run_json).resolve() if formal_database_dry_run_json else None
    dry_run_payload = _load_json(dry_run_path)
    effective_dsn, dsn_source = _dsn_from_inputs(dsn=dsn, dsn_env=dsn_env)
    dsn_configured = bool(effective_dsn)
    dsn_env_name = str(dsn_env or "").strip()

    checks: list[FormalDatabaseImportPreflightCheck] = []

    dry_ready, dry_reasons = _dry_run_contract_ready(dry_run_payload)
    checks.append(
        _check(
            check="formal_database_dry_run_contract_ready",
            status="ready" if dry_ready else "blocker",
            evidence_role="required_schema_and_insert_contract",
            reasons=dry_reasons,
            physical_meaning=(
                "The import preflight is only meaningful after the PostgreSQL 18 schema/insert-preview "
                "contract is ready and still no-write/no-connect."
            ),
            next_action="Regenerate or repair the formal database dry-run contract before reviewing import readiness.",
            details={
                "source_path": str(dry_run_path) if dry_run_path else "",
                "dry_run_status": dry_run_payload.get("overall_status", ""),
                "dry_run_blocker_count": int(dry_run_payload.get("blocker_count") or 0),
            },
        )
    )

    checks.append(
        _check(
            check="postgresql_dsn_configuration_preview",
            status="ready" if dsn_configured else "review_required",
            evidence_role="dsn_configuration_without_connection",
            reasons=() if dsn_configured else ("dsn_missing",),
            physical_meaning="A production import needs a DSN, but this preflight records only presence/fingerprint.",
            next_action="Provide the PostgreSQL 18 DSN only to the separately authorized import step.",
            details={
                "dsn_configured": dsn_configured,
                "dsn_source": dsn_source,
                "dsn_env": dsn_env_name,
                "dsn_fingerprint": _dsn_fingerprint(effective_dsn),
                "connects_postgresql": False,
            },
        )
    )

    checks.append(
        _check(
            check="migration_execution_lock",
            status="ready",
            evidence_role="migration_boundary",
            physical_meaning="Schema migration application must remain a separate authorized operation.",
            next_action="Do not pass migration/apply flags until reviewer authorization and backup policy exist.",
            details={"apply_migrations": False, "migrations_applied": False},
        )
    )
    checks.append(
        _check(
            check="database_import_execution_lock",
            status="ready",
            evidence_role="import_boundary",
            physical_meaning="Preflight can prepare an import review, but it must not insert or update production rows.",
            next_action="Use the dedicated database import command only after archive release and import authorization.",
            details={
                "database_import_attempted": False,
                "database_written": False,
                "database_import_allowed": False,
            },
        )
    )
    checks.append(
        _check(
            check="identity_key_and_alias_contract",
            status="ready" if dry_run_payload.get("primary_identity") == "sn_code/device_code" else "blocker",
            evidence_role="identity_import_contract",
            reasons=()
            if dry_run_payload.get("primary_identity") == "sn_code/device_code"
            else (f"primary_identity={dry_run_payload.get('primary_identity') or 'missing'}",),
            physical_meaning=(
                "Production inserts must use SN/device_code as durable identity while protocol ID remains a lookup alias."
            ),
            next_action="Keep protocol device ID as compatibility alias; do not promote it to production primary identity.",
            details={
                "primary_identity": dry_run_payload.get("primary_identity", ""),
                "protocol_device_id_role": dry_run_payload.get("protocol_device_id_role", ""),
            },
        )
    )
    checks.append(
        _check(
            check="release_and_authorization_gate_required",
            status="ready",
            evidence_role="release_boundary",
            physical_meaning=(
                "Database import is a release/archive action and must be downstream of formal archive closure, "
                "reviewer approval, and explicit database-import authorization."
            ),
            next_action="Keep database_import_allowed=false until formal release and import authorization are both present.",
            details={
                "requires_formal_archive_release": True,
                "requires_database_import_authorization": True,
                "formal_release_allowed": False,
                "database_import_allowed": False,
            },
        )
    )

    blocker_count = sum(1 for row in checks if row.status == "blocker")
    review_required_count = sum(1 for row in checks if row.status == "review_required")
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
        "dsn_env": dsn_env_name,
        "dsn_configured": dsn_configured,
        "dsn_source": dsn_source,
        "dsn_fingerprint": _dsn_fingerprint(effective_dsn),
        "formal_database_dry_run_json": str(dry_run_path) if dry_run_path else "",
        "dry_run_contract_ready": dry_ready,
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
        "primary_identity": dry_run_payload.get("primary_identity", "sn_code/device_code"),
        "protocol_device_id_role": dry_run_payload.get(
            "protocol_device_id_role",
            "compatibility_alias_and_command_identity",
        ),
        "required_authorizations": [
            "formal_archive_release",
            "database_import_authorization",
            "reviewer_approval",
        ],
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Resolve review items, then run a separate explicitly authorized PostgreSQL 18 import command. "
            "This preflight never connects to PostgreSQL and never imports data."
        ),
    }


def write_v1_5_formal_database_import_preflight_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_database_import_preflight.json",
        "checks_csv": out / "v1_5_formal_database_import_preflight_checks.csv",
        "summary_csv": out / "v1_5_formal_database_import_preflight_summary.csv",
        "markdown": out / "V1_5_FORMAL_DATABASE_IMPORT_PREFLIGHT.md",
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
                "production_backend": model.get("production_backend"),
                "production_postgresql_major": model.get("production_postgresql_major"),
                "dsn_configured": model.get("dsn_configured"),
                "connects_postgresql": model.get("connects_postgresql"),
                "applies_migrations": model.get("applies_migrations"),
                "database_import_attempted": model.get("database_import_attempted"),
                "database_import_allowed": model.get("database_import_allowed"),
            }
        ],
    )
    lines = [
        "# V1.5 formal database import preflight",
        "",
        "This is an offline preflight for a future separately authorized PostgreSQL 18 import.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        f"- review_required_count: `{model.get('review_required_count')}`",
        f"- production backend: `{model.get('production_backend')}` `{model.get('production_postgresql_major')}`",
        f"- dsn_configured: `{model.get('dsn_configured')}`",
        f"- dry_run_contract_ready: `{model.get('dry_run_contract_ready')}`",
        f"- database_import_allowed: `{model.get('database_import_allowed')}`",
        "- This preflight does not connect PostgreSQL, apply migrations, import data, open COM, control routes, or write analyzer state.",
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
