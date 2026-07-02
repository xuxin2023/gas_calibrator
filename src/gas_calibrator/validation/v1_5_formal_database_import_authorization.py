"""Offline authorization guard for a future V1.5 PostgreSQL 18 import.

This module reviews whether a separate real database import command would have
the required archive-release and operator-authorization evidence. It never
connects to PostgreSQL, applies migrations, or imports rows.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_database_import_authorization_v1"
READY_STATUS = "ready_for_manual_postgresql18_import_authorization"
REVIEW_STATUS = "review_required"
BLOCKED_STATUS = "blocked"


@dataclass(frozen=True)
class FormalDatabaseImportAuthorizationCheck:
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
) -> FormalDatabaseImportAuthorizationCheck:
    return FormalDatabaseImportAuthorizationCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _preflight_ready(payload: Mapping[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not payload:
        reasons.append("formal_database_import_preflight_missing")
        return False, reasons
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
        "formal_release_allowed",
    ):
        if payload.get(field) is not False:
            reasons.append(f"preflight_boundary_{field}={payload.get(field)!r}")
    return not reasons, reasons


def _archive_release_ready(payload: Mapping[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not payload:
        reasons.append("archive_closure_missing")
        return False, reasons
    if payload.get("overall_status") not in {"ready", "ready_for_formal_release"}:
        reasons.append(f"archive_status={payload.get('overall_status') or 'missing'}")
    if payload.get("package_status") not in {"ready", "ready_for_formal_release"}:
        reasons.append(f"package_status={payload.get('package_status') or 'missing'}")
    traceability = payload.get("identity_getco_traceability") or {}
    if traceability.get("ready_for_archive_release") is not True:
        reasons.append("identity_traceability_not_ready")
    if traceability.get("traceability_review_required") is True:
        reasons.append("identity_traceability_review_required")
    return not reasons, reasons


def _authorization_ready(*, operator: str, reviewer: str, approver: str, authorization_id: str) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not operator.strip():
        reasons.append("operator_missing")
    if not reviewer.strip():
        reasons.append("reviewer_missing")
    if not approver.strip():
        reasons.append("approver_missing")
    if not authorization_id.strip():
        reasons.append("authorization_id_missing")
    return not reasons, reasons


def build_v1_5_formal_database_import_authorization(
    *,
    formal_database_import_preflight_json: str | Path | None = None,
    archive_closure_json: str | Path | None = None,
    operator: str = "",
    reviewer: str = "",
    approver: str = "",
    authorization_id: str = "",
) -> dict[str, Any]:
    preflight_path = (
        Path(formal_database_import_preflight_json).resolve()
        if formal_database_import_preflight_json
        else None
    )
    archive_path = Path(archive_closure_json).resolve() if archive_closure_json else None
    preflight_payload = _load_json(preflight_path)
    archive_payload = _load_json(archive_path)

    checks: list[FormalDatabaseImportAuthorizationCheck] = []

    preflight_ok, preflight_reasons = _preflight_ready(preflight_payload)
    if preflight_ok:
        preflight_status = "ready"
    elif not preflight_payload or preflight_payload.get("overall_status") == "blocked" or int(
        preflight_payload.get("blocker_count") or 0
    ):
        preflight_status = "blocker"
    else:
        preflight_status = "review_required"
    checks.append(
        _check(
            check="formal_database_import_preflight_ready",
            status=preflight_status,
            evidence_role="required_database_preflight",
            reasons=preflight_reasons,
            physical_meaning=(
                "A real import authorization can only be reviewed after the PostgreSQL 18 import preflight "
                "has a DSN fingerprint, a ready dry-run contract, and no execution side effects."
            ),
            next_action="Regenerate or review the formal database import preflight before any import authorization.",
            details={
                "source_path": str(preflight_path) if preflight_path else "",
                "preflight_status": preflight_payload.get("overall_status", ""),
                "dsn_configured": preflight_payload.get("dsn_configured", False),
                "dry_run_contract_ready": preflight_payload.get("dry_run_contract_ready", False),
            },
        )
    )

    archive_ok, archive_reasons = _archive_release_ready(archive_payload)
    checks.append(
        _check(
            check="formal_archive_release_ready",
            status="ready" if archive_ok else "review_required",
            evidence_role="required_release_evidence",
            reasons=archive_reasons,
            physical_meaning=(
                "Production database import is an archive/release action, not an early calibration-stage action."
            ),
            next_action="Complete formal archive closure and SN/device_code traceability before import authorization.",
            details={
                "source_path": str(archive_path) if archive_path else "",
                "archive_status": archive_payload.get("overall_status", ""),
                "package_status": archive_payload.get("package_status", ""),
            },
        )
    )

    auth_ok, auth_reasons = _authorization_ready(
        operator=operator,
        reviewer=reviewer,
        approver=approver,
        authorization_id=authorization_id,
    )
    checks.append(
        _check(
            check="manual_database_import_authorization_record",
            status="ready" if auth_ok else "review_required",
            evidence_role="manual_authorization_record",
            reasons=auth_reasons,
            physical_meaning=(
                "A database import needs an explicit operator/reviewer/approver authorization record; "
                "a passing preflight alone must not trigger production writes."
            ),
            next_action="Record operator, reviewer, approver, and authorization ID before a separate import command.",
            details={
                "operator": operator,
                "reviewer": reviewer,
                "approver": approver,
                "authorization_id": authorization_id,
            },
        )
    )

    checks.append(
        _check(
            check="migration_execution_lock",
            status="ready",
            evidence_role="migration_boundary",
            physical_meaning="This authorization review does not allow schema migrations.",
            next_action="Use a separately reviewed migration procedure if schema changes are ever required.",
            details={"applies_migrations": False, "migrations_applied": False},
        )
    )
    checks.append(
        _check(
            check="real_import_command_must_consume_authorization",
            status="ready",
            evidence_role="execution_boundary",
            physical_meaning=(
                "The future real import command must consume this authorization artifact plus archive and preflight evidence."
            ),
            next_action="Keep real import disabled unless the dedicated import command verifies this artifact.",
            details={
                "database_import_attempted": False,
                "database_written": False,
                "real_import_command_required_inputs": [
                    "formal_database_import_authorization_json",
                    "formal_database_import_preflight_json",
                    "formal_archive_closure_index_json",
                    "evidence_bundle_json",
                ],
            },
        )
    )

    blocker_count = sum(1 for row in checks if row.status == "blocker")
    review_required_count = sum(1 for row in checks if row.status == "review_required")
    manual_authorization_ready = blocker_count == 0 and review_required_count == 0
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
        "formal_database_import_preflight_json": str(preflight_path) if preflight_path else "",
        "archive_closure_json": str(archive_path) if archive_path else "",
        "preflight_ready": preflight_ok,
        "archive_release_ready": archive_ok,
        "manual_authorization_ready": manual_authorization_ready,
        "operator": operator,
        "reviewer": reviewer,
        "approver": approver,
        "authorization_id": authorization_id,
        "connects_postgresql": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "applies_migrations": False,
        "database_import_attempted": False,
        "database_written": False,
        "database_import_allowed": manual_authorization_ready,
        "formal_release_allowed": archive_ok,
        "not_real_acceptance_evidence": True,
        "required_authorizations": [
            "formal_archive_release",
            "database_import_authorization",
            "operator_confirmation",
            "reviewer_approval",
            "approver_approval",
        ],
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Resolve blockers/review items, then run a separate controlled PostgreSQL 18 import command that "
            "consumes this authorization artifact. This review artifact never imports data."
        ),
    }


def write_v1_5_formal_database_import_authorization_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_database_import_authorization.json",
        "checks_csv": out / "v1_5_formal_database_import_authorization_checks.csv",
        "summary_csv": out / "v1_5_formal_database_import_authorization_summary.csv",
        "markdown": out / "V1_5_FORMAL_DATABASE_IMPORT_AUTHORIZATION.md",
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
                "preflight_ready": model.get("preflight_ready"),
                "archive_release_ready": model.get("archive_release_ready"),
                "manual_authorization_ready": model.get("manual_authorization_ready"),
                "connects_postgresql": model.get("connects_postgresql"),
                "applies_migrations": model.get("applies_migrations"),
                "database_import_attempted": model.get("database_import_attempted"),
                "database_written": model.get("database_written"),
                "database_import_allowed": model.get("database_import_allowed"),
            }
        ],
    )
    lines = [
        "# V1.5 formal database import authorization",
        "",
        "This is an offline authorization guard for a future separately controlled PostgreSQL 18 import.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        f"- review_required_count: `{model.get('review_required_count')}`",
        f"- preflight_ready: `{model.get('preflight_ready')}`",
        f"- archive_release_ready: `{model.get('archive_release_ready')}`",
        f"- manual_authorization_ready: `{model.get('manual_authorization_ready')}`",
        f"- database_import_allowed: `{model.get('database_import_allowed')}`",
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
