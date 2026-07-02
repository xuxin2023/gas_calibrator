"""Build an offline V1.5 formal run status rollup.

This module reads existing readiness, evidence, closure, and archive sidecars
and turns them into a small reviewer-facing status dashboard. It is deliberately
read-only: it does not open COM ports, connect to PostgreSQL, control routes or
pressure, or write analyzer coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA = "v1_5_formal_run_status_v1"

READY = "ready"
READY_WITH_PENDING_LIVE_GATE = "ready_with_pending_live_gate"
REVIEW_REQUIRED = "review_required"
MISSING = "missing"
NOT_ATTEMPTED = "not_attempted"
BLOCKED = "blocked"

NON_READY_STATUSES = {REVIEW_REQUIRED, MISSING, NOT_ATTEMPTED, BLOCKED}


@dataclass(frozen=True)
class FormalRunGate:
    gate_id: str
    title: str
    status: str
    source_path: str
    source_status: str
    reason: str
    next_action: str
    physical_meaning: str
    release_gate: bool
    blocks_release: bool
    blocks_physical_flow: bool

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


def _safe_rglob(root: Path, pattern: str) -> list[Path]:
    if not root.exists():
        return []
    return [path for path in root.rglob(pattern) if path.is_file()]


def _latest(root: Path, *patterns: str) -> Path | None:
    matches: list[Path] = []
    for pattern in patterns:
        matches.extend(_safe_rglob(root, pattern))
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


def _explicit_or_latest(root: Path, explicit: str | Path | None, *patterns: str) -> Path | None:
    if explicit:
        return Path(explicit).resolve()
    latest = _latest(root, *patterns)
    return latest.resolve() if latest else None


def _source_status(payload: Mapping[str, Any]) -> str:
    for key in ("overall_status", "readiness_status", "release_status", "package_status", "status"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def _stage_status(payload: Mapping[str, Any], stage_id: str) -> str:
    for row in payload.get("stage_statuses") or []:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("stage_id") or "") == stage_id:
            return str(row.get("status") or "")
    return ""


def _first_gap(payload: Mapping[str, Any]) -> str:
    gaps = payload.get("gaps")
    if not isinstance(gaps, list) or not gaps:
        return ""
    first = gaps[0]
    if isinstance(first, Mapping):
        return str(first.get("reason") or first.get("item") or first.get("gate_id") or "")
    return str(first)


def _gate(
    *,
    gate_id: str,
    title: str,
    status: str,
    source_path: Path | None,
    source_status: str,
    reason: str,
    next_action: str,
    physical_meaning: str,
    release_gate: bool = True,
    blocks_release: bool | None = None,
    blocks_physical_flow: bool = False,
) -> FormalRunGate:
    if blocks_release is None:
        blocks_release = release_gate and status in NON_READY_STATUSES
    return FormalRunGate(
        gate_id=gate_id,
        title=title,
        status=status,
        source_path=str(source_path) if source_path else "",
        source_status=source_status,
        reason=reason,
        next_action=next_action,
        physical_meaning=physical_meaning,
        release_gate=release_gate,
        blocks_release=blocks_release,
        blocks_physical_flow=blocks_physical_flow,
    )


def _initialization_gate(path: Path | None, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    if not payload:
        status = MISSING
        reason = "initialization readiness sidecar missing"
    elif "blocked" in source_status:
        status = BLOCKED
        reason = f"source_status={source_status}"
    elif source_status.startswith("ready") or "ready_for" in source_status:
        status = READY
        reason = f"source_status={source_status}"
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="initialization_readiness",
        title="Initialization readiness",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Generate or refresh initialization readiness before any open-flow step.",
        physical_meaning=(
            "Confirms SN/device_code identity contract, MODE2 runtime, 1Hz upload, "
            "neutral temperature coefficients, PostgreSQL 18 preflight, and initialization evidence."
        ),
        blocks_physical_flow=status in {MISSING, REVIEW_REQUIRED, BLOCKED},
    )


def _getco_gate(path: Path | None, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    traceability_review = bool(payload.get("traceability_review_required"))
    if not payload:
        status = MISSING
        reason = "identity/GETCO readiness sidecar missing"
        blocks_physical = True
    elif source_status == "identity_getco_ready_for_auxiliary_neutralization" and not traceability_review:
        status = READY
        reason = "GETCO epoch-0 and SN/device_code traceability are ready"
        blocks_physical = False
    elif source_status == "identity_getco_ready_for_auxiliary_neutralization" and traceability_review:
        status = REVIEW_REQUIRED
        reason = "GETCO epoch-0 is usable, but SN/device_code traceability needs release review"
        blocks_physical = False
    elif "blocked" in source_status:
        status = BLOCKED
        reason = f"source_status={source_status}"
        blocks_physical = True
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
        blocks_physical = True
    return _gate(
        gate_id="identity_getco_sn_traceability",
        title="Identity, GETCO epoch-0, and SN traceability",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Refresh read-only GETCO/SN identity evidence or resolve traceability review before release.",
        physical_meaning=(
            "Binds transport COM/GA labels to protocol ID, SN/device_code, and GETCO1-9 "
            "epoch-0 coefficients so later writes and reports remain traceable."
        ),
        blocks_physical_flow=blocks_physical,
    )


def _pre_gas_gate(path: Path | None, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    if not payload:
        status = MISSING
        reason = "pre-gas readiness sidecar missing"
    elif "blocked" in source_status:
        status = BLOCKED
        reason = f"source_status={source_status}"
    elif source_status in {
        "ready_for_open_flow_from_sidecar_evidence",
        "ready_for_identity_gate_with_later_live_gates",
    }:
        status = READY_WITH_PENDING_LIVE_GATE if "later_live_gates" in source_status else READY
        reason = f"source_status={source_status}"
    elif source_status.startswith("review_required"):
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status}"
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="pre_gas_readiness",
        title="Pre-gas readiness",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Close pre-gas gaps before starting mature CO2/H2O open-flow queues.",
        physical_meaning=(
            "Collects the gap list from initialization to gas-flow entry: pressure S9, route readiness, "
            "GETCO baseline, S7/S8 neutral state, CHECK timing, and database preflight."
        ),
        blocks_physical_flow=status in {MISSING, REVIEW_REQUIRED, BLOCKED},
    )


def _run_stage_gate(
    *,
    gate_id: str,
    title: str,
    run_path: Path | None,
    run_status: Mapping[str, Any],
    stage_id: str,
    missing_reason: str,
    next_action: str,
    physical_meaning: str,
    physical_flow_gate: bool = False,
) -> FormalRunGate:
    stage_status = _stage_status(run_status, stage_id)
    if not run_status:
        status = MISSING
        reason = "run evidence status sidecar missing"
    elif stage_status == "pass":
        status = READY
        reason = f"{stage_id}=pass"
    elif stage_status in {"not_attempted", ""}:
        status = NOT_ATTEMPTED
        reason = missing_reason
    elif stage_status == "blocked":
        status = BLOCKED
        reason = f"{stage_id}=blocked"
    elif stage_status in {"missing", "partial"}:
        status = REVIEW_REQUIRED
        reason = f"{stage_id}={stage_status}"
    else:
        status = REVIEW_REQUIRED
        reason = f"{stage_id}={stage_status}"
    return _gate(
        gate_id=gate_id,
        title=title,
        status=status,
        source_path=run_path,
        source_status=stage_status,
        reason=reason,
        next_action=next_action,
        physical_meaning=physical_meaning,
        blocks_physical_flow=physical_flow_gate and status in {MISSING, REVIEW_REQUIRED, BLOCKED},
    )


def _archive_gate(
    *,
    closure_path: Path | None,
    closure: Mapping[str, Any],
    archive_path: Path | None,
    archive: Mapping[str, Any],
) -> FormalRunGate:
    closure_status = str(closure.get("release_status") or closure.get("overall_status") or "")
    archive_status = str(archive.get("package_status") or archive.get("overall_status") or "")
    traceability = archive.get("identity_getco_traceability")
    traceability_ready = False
    traceability_review = False
    if isinstance(traceability, Mapping):
        traceability_ready = bool(traceability.get("ready_for_archive_release"))
        traceability_review = bool(traceability.get("traceability_review_required"))

    if not closure and not archive:
        status = MISSING
        reason = "closure readiness and formal archive closure sidecars missing"
    elif "blocked" in closure_status or "blocked" in archive_status:
        status = BLOCKED
        reason = f"closure_status={closure_status or 'missing'} archive_status={archive_status or 'missing'}"
    elif closure_status == "ready_for_formal_release" and not archive:
        status = MISSING
        reason = "closure is ready, but formal archive closure index is missing"
    elif closure_status == "ready_for_formal_release" and traceability_ready and not traceability_review:
        status = READY
        reason = "closure release and archive traceability gates are ready"
    elif closure_status == "ready_for_formal_release" and traceability_review:
        status = REVIEW_REQUIRED
        reason = "closure is ready, but archive SN/GETCO traceability still requires review"
    else:
        status = REVIEW_REQUIRED
        gap = _first_gap(closure) or _first_gap(archive)
        reason = gap or f"closure_status={closure_status or 'missing'} archive_status={archive_status or 'missing'}"

    source_path = archive_path or closure_path
    source_status = f"closure={closure_status or 'missing'}; archive={archive_status or 'missing'}"
    return _gate(
        gate_id="formal_archive_database_release",
        title="Formal archive, database, and release gate",
        status=status,
        source_path=source_path,
        source_status=source_status,
        reason=reason,
        next_action="Close archive/database/report traceability gaps before formal release or database import.",
        physical_meaning=(
            "Final release binds raw evidence, coefficient epochs, reverification, reports, database "
            "indexing, and SN/device_code traceability without changing analyzer state."
        ),
        blocks_physical_flow=False,
    )


def _algorithm_profile_runner_dry_run_gate(path: Path, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    blocker_count = int(payload.get("blocker_count") or 0)
    co2_count = payload.get("co2_runlist_count")
    h2o_count = payload.get("h2o_runlist_count")
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_coefficients") is False
        and payload.get("writes_device_id") is False
        and payload.get("does_not_execute_commands") is True
        and payload.get("does_not_modify_runners") is True
    )
    counts_ok = co2_count == 47 and h2o_count == 14
    if (
        source_status == "ready_for_profile_driven_runner_dry_run_review"
        and blocker_count == 0
        and counts_ok
        and boundary_ok
    ):
        status = READY
        reason = "profile-driven new-algorithm dry-run bundle is ready: CO2/H2O=47/14 and offline boundaries hold"
    else:
        status = REVIEW_REQUIRED
        reasons: list[str] = []
        if source_status != "ready_for_profile_driven_runner_dry_run_review":
            reasons.append(f"source_status={source_status or 'missing'}")
        if blocker_count:
            reasons.append(f"blocker_count={blocker_count}")
        if not counts_ok:
            reasons.append(f"co2_h2o_counts={co2_count}/{h2o_count}")
        if not boundary_ok:
            reasons.append("offline_boundary_not_clean")
        reason = "; ".join(reasons) or "profile-driven new-algorithm dry-run bundle requires review"
    return _gate(
        gate_id="algorithm_profile_runner_dry_run",
        title="New-algorithm profile runner dry-run bundle",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Review the profile-generated 47/14 runlist, readiness gate, and dry-run queue handoff before any future runner wiring.",
        physical_meaning=(
            "Records that the new-algorithm profile can generate CO2 47 / H2O 14 runlist evidence "
            "and dry-run mature-queue handoff plans without executing queues or modifying mature runners."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_database_dry_run_gate(path: Path, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    blocker_count = int(payload.get("blocker_count") or 0)
    backend_ok = (
        payload.get("production_backend") == "postgresql"
        and payload.get("production_postgresql_major") == 18
    )
    identity_ok = payload.get("primary_identity") == "sn_code/device_code"
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("database_import_allowed") is False
        and payload.get("formal_release_allowed") is False
    )
    if (
        source_status == "ready_for_postgresql18_schema_dry_run_review"
        and blocker_count == 0
        and backend_ok
        and identity_ok
        and boundary_ok
    ):
        status = READY
        reason = "PostgreSQL 18 schema/insert dry-run is ready while real import remains unauthorized"
    else:
        status = REVIEW_REQUIRED
        reasons: list[str] = []
        if source_status != "ready_for_postgresql18_schema_dry_run_review":
            reasons.append(f"source_status={source_status or 'missing'}")
        if blocker_count:
            reasons.append(f"blocker_count={blocker_count}")
        if not backend_ok:
            reasons.append(
                f"backend={payload.get('production_backend')}/{payload.get('production_postgresql_major')}"
            )
        if not identity_ok:
            reasons.append(f"primary_identity={payload.get('primary_identity') or 'missing'}")
        if not boundary_ok:
            reasons.append("dry_run_boundary_not_clean")
        reason = "; ".join(reasons) or "PostgreSQL 18 database dry-run contract requires review"
    return _gate(
        gate_id="formal_database_dry_run",
        title="PostgreSQL 18 formal database dry-run contract",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Review the PostgreSQL 18 schema, SN/device_code identity, insert-preview, and dry-run boundaries "
            "before enabling any separate database import step."
        ),
        physical_meaning=(
            "Checks database schema and insert-preview semantics without connecting to PostgreSQL or importing data; "
            "this keeps database readiness separate from formal archive release."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_database_import_preflight_gate(path: Path, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    blocker_count = int(payload.get("blocker_count") or 0)
    review_required_count = int(payload.get("review_required_count") or 0)
    backend_ok = (
        payload.get("production_backend") == "postgresql"
        and payload.get("production_postgresql_major") == 18
    )
    dry_run_ready = payload.get("dry_run_contract_ready") is True
    dsn_configured = payload.get("dsn_configured") is True
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("applies_migrations") is False
        and payload.get("database_import_attempted") is False
        and payload.get("database_written") is False
        and payload.get("database_import_allowed") is False
        and payload.get("formal_release_allowed") is False
    )
    if (
        source_status == "ready_for_authorized_postgresql18_import_review"
        and blocker_count == 0
        and review_required_count == 0
        and backend_ok
        and dry_run_ready
        and dsn_configured
        and boundary_ok
    ):
        status = READY
        reason = "PostgreSQL 18 import preflight is ready while real import remains separately unauthorized"
    elif source_status == "blocked" or blocker_count:
        status = BLOCKED
        reasons: list[str] = []
        if source_status != "blocked":
            reasons.append(f"source_status={source_status or 'missing'}")
        if blocker_count:
            reasons.append(f"blocker_count={blocker_count}")
        if not dry_run_ready:
            reasons.append("dry_run_contract_not_ready")
        if not backend_ok:
            reasons.append(
                f"backend={payload.get('production_backend')}/{payload.get('production_postgresql_major')}"
            )
        if not boundary_ok:
            reasons.append("import_preflight_boundary_not_clean")
        reason = "; ".join(reasons) or "PostgreSQL 18 import preflight is blocked"
    else:
        status = REVIEW_REQUIRED
        reasons = []
        if source_status != "ready_for_authorized_postgresql18_import_review":
            reasons.append(f"source_status={source_status or 'missing'}")
        if review_required_count:
            reasons.append(f"review_required_count={review_required_count}")
        if not backend_ok:
            reasons.append(
                f"backend={payload.get('production_backend')}/{payload.get('production_postgresql_major')}"
            )
        if not dry_run_ready:
            reasons.append("dry_run_contract_not_ready")
        if not dsn_configured:
            reasons.append("dsn_configured=False")
        if not boundary_ok:
            reasons.append("import_preflight_boundary_not_clean")
        reason = "; ".join(reasons) or "PostgreSQL 18 import preflight requires review"
    return _gate(
        gate_id="formal_database_import_preflight",
        title="PostgreSQL 18 formal database import preflight",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Review DSN configuration, migration lock, archive-release dependency, and explicit import authorization "
            "before running any separate production database import."
        ),
        physical_meaning=(
            "Checks that a production database import could be reviewed without opening PostgreSQL, applying migrations, "
            "or importing rows; this separates preflight evidence from real import execution."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_database_import_authorization_gate(path: Path, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    blocker_count = int(payload.get("blocker_count") or 0)
    review_required_count = int(payload.get("review_required_count") or 0)
    backend_ok = (
        payload.get("production_backend") == "postgresql"
        and payload.get("production_postgresql_major") == 18
    )
    prereqs_ok = (
        payload.get("preflight_ready") is True
        and payload.get("archive_release_ready") is True
        and payload.get("manual_authorization_ready") is True
        and payload.get("database_import_allowed") is True
    )
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("applies_migrations") is False
        and payload.get("database_import_attempted") is False
        and payload.get("database_written") is False
    )
    if (
        source_status == "ready_for_manual_postgresql18_import_authorization"
        and blocker_count == 0
        and review_required_count == 0
        and backend_ok
        and prereqs_ok
        and boundary_ok
    ):
        status = READY
        reason = "manual PostgreSQL 18 import authorization evidence is ready; real import remains a separate command"
    elif source_status == "blocked" or blocker_count:
        status = BLOCKED
        reasons: list[str] = []
        if source_status != "blocked":
            reasons.append(f"source_status={source_status or 'missing'}")
        if blocker_count:
            reasons.append(f"blocker_count={blocker_count}")
        if not backend_ok:
            reasons.append(
                f"backend={payload.get('production_backend')}/{payload.get('production_postgresql_major')}"
            )
        if not boundary_ok:
            reasons.append("authorization_boundary_not_clean")
        reason = "; ".join(reasons) or "PostgreSQL 18 import authorization is blocked"
    else:
        status = REVIEW_REQUIRED
        reasons = []
        if source_status != "ready_for_manual_postgresql18_import_authorization":
            reasons.append(f"source_status={source_status or 'missing'}")
        if review_required_count:
            reasons.append(f"review_required_count={review_required_count}")
        if not backend_ok:
            reasons.append(
                f"backend={payload.get('production_backend')}/{payload.get('production_postgresql_major')}"
            )
        if payload.get("preflight_ready") is not True:
            reasons.append("preflight_ready=False")
        if payload.get("archive_release_ready") is not True:
            reasons.append("archive_release_ready=False")
        if payload.get("manual_authorization_ready") is not True:
            reasons.append("manual_authorization_ready=False")
        if payload.get("database_import_allowed") is not True:
            reasons.append("database_import_allowed=False")
        if not boundary_ok:
            reasons.append("authorization_boundary_not_clean")
        reason = "; ".join(reasons) or "PostgreSQL 18 import authorization requires review"
    return _gate(
        gate_id="formal_database_import_authorization",
        title="PostgreSQL 18 formal database import authorization",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Complete archive release and manual import authorization, then run a separate controlled database import command "
            "that consumes this authorization artifact."
        ),
        physical_meaning=(
            "Separates manual database-import authorization from both preflight review and actual PostgreSQL writes; "
            "the status artifact itself remains no-connect/no-import."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_database_import_command_contract_gate(path: Path, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    blocker_count = int(payload.get("blocker_count") or 0)
    review_required_count = int(payload.get("review_required_count") or 0)
    backend_ok = (
        payload.get("production_backend") == "postgresql"
        and payload.get("production_postgresql_major") == 18
    )
    prereqs_ok = (
        payload.get("authorization_ready") is True
        and payload.get("preflight_ready") is True
        and payload.get("archive_release_ready") is True
        and payload.get("evidence_bundle_ready") is True
        and payload.get("command_contract_ready") is True
        and payload.get("database_import_allowed") is False
        and payload.get("real_import_execution_allowed") is False
    )
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("applies_migrations") is False
        and payload.get("database_import_attempted") is False
        and payload.get("database_written") is False
    )
    if (
        source_status == "ready_for_controlled_postgresql18_import_command_review"
        and blocker_count == 0
        and review_required_count == 0
        and backend_ok
        and prereqs_ok
        and boundary_ok
    ):
        status = READY
        reason = "controlled PostgreSQL 18 import command contract is ready; this artifact still does not execute import"
    elif source_status == "blocked" or blocker_count:
        status = BLOCKED
        reasons: list[str] = []
        if source_status != "blocked":
            reasons.append(f"source_status={source_status or 'missing'}")
        if blocker_count:
            reasons.append(f"blocker_count={blocker_count}")
        if not backend_ok:
            reasons.append(
                f"backend={payload.get('production_backend')}/{payload.get('production_postgresql_major')}"
            )
        if not boundary_ok:
            reasons.append("command_contract_boundary_not_clean")
        reason = "; ".join(reasons) or "PostgreSQL 18 import command contract is blocked"
    else:
        status = REVIEW_REQUIRED
        reasons = []
        if source_status != "ready_for_controlled_postgresql18_import_command_review":
            reasons.append(f"source_status={source_status or 'missing'}")
        if review_required_count:
            reasons.append(f"review_required_count={review_required_count}")
        if not backend_ok:
            reasons.append(
                f"backend={payload.get('production_backend')}/{payload.get('production_postgresql_major')}"
            )
        for field in (
            "authorization_ready",
            "preflight_ready",
            "archive_release_ready",
            "evidence_bundle_ready",
            "command_contract_ready",
        ):
            if payload.get(field) is not True:
                reasons.append(f"{field}={payload.get(field)!r}")
        if payload.get("database_import_allowed") is not False:
            reasons.append(f"database_import_allowed={payload.get('database_import_allowed')!r}")
        if payload.get("real_import_execution_allowed") is not False:
            reasons.append(f"real_import_execution_allowed={payload.get('real_import_execution_allowed')!r}")
        if not boundary_ok:
            reasons.append("command_contract_boundary_not_clean")
        reason = "; ".join(reasons) or "PostgreSQL 18 import command contract requires review"
    return _gate(
        gate_id="formal_database_import_command_contract",
        title="PostgreSQL 18 formal database import command contract",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Review the no-connect import command contract. A separate controlled command must consume the contract, "
            "authorization, preflight, archive, evidence bundle, and DSN env before any production import."
        ),
        physical_meaning=(
            "Separates manual import authorization from executable command inputs and keeps migration/import execution "
            "locked off until a future controlled command re-checks the full evidence chain."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_database_import_blocked_executor_gate(path: Path, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    side_effect_lock_clean = (
        payload.get("connects_postgresql") is False
        and payload.get("opens_com_ports") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("applies_migrations") is False
        and payload.get("database_import_attempted") is False
        and payload.get("database_written") is False
        and payload.get("database_import_allowed") is False
        and payload.get("real_import_execution_allowed") is False
    )
    expected_block = (
        source_status == "blocked_pending_controlled_executor_implementation"
        and payload.get("blocked_executor_ready") is True
        and payload.get("execution_supported") is False
        and side_effect_lock_clean
    )
    if expected_block:
        status = REVIEW_REQUIRED
        reason = (
            "blocked PostgreSQL 18 import executor stub consumed the command contract and correctly refused real import"
        )
    elif not side_effect_lock_clean:
        status = BLOCKED
        reason = "blocked executor boundary is not clean; PostgreSQL/COM/write side effects are not locked off"
    elif source_status == "review_required" or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = (
            f"blocked executor input review_required_count={payload.get('review_required_count')}; "
            "regenerate command contract/input references before executor review"
        )
    else:
        status = BLOCKED
        reason = f"blocked executor source_status={source_status or 'missing'} is not the expected locked stub status"
    return _gate(
        gate_id="formal_database_import_blocked_executor",
        title="PostgreSQL 18 formal database import blocked executor",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Keep database import locked. Build a separate controlled executor with double authorization "
            "before any PostgreSQL connection, migration, or row import is allowed."
        ),
        physical_meaning=(
            "Proves the future import command currently consumes reviewed inputs but remains a no-connect, "
            "no-migration, no-write stub rather than a production import."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_database_import_controlled_executor_design_gate(path: Path, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("connects_postgresql") is False
        and payload.get("opens_com_ports") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("applies_migrations") is False
        and payload.get("database_import_attempted") is False
        and payload.get("database_written") is False
        and payload.get("database_import_allowed") is False
        and payload.get("real_import_execution_allowed") is False
        and payload.get("execution_supported") is False
    )
    if source_status == "ready_for_controlled_import_executor_design_review" and boundary_ok:
        status = READY
        reason = "controlled PostgreSQL 18 import executor design is ready; execution remains blocked"
    elif not boundary_ok:
        status = BLOCKED
        reason = "controlled executor design boundary is not clean; no-connect/no-write locks are not preserved"
    elif source_status == "review_required" or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = (
            f"controlled executor design review_required_count={payload.get('review_required_count')}; "
            "review blocked-executor linkage before future executor design is accepted"
        )
    else:
        status = REVIEW_REQUIRED
        reason = f"controlled executor design source_status={source_status or 'missing'} requires review"
    return _gate(
        gate_id="formal_database_import_controlled_executor_design",
        title="PostgreSQL 18 controlled import executor design",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Use this design only as future implementation guidance. Do not connect PostgreSQL until a separate "
            "controlled executor adds explicit execute authorization, transaction, readback, rollback, and import evidence."
        ),
        physical_meaning=(
            "Defines the future real-import safety contract while preserving the current no-connect, "
            "no-migration, no-write V1.5 boundary."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _gap_rows(gates: Iterable[FormalRunGate]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for gate in gates:
        if gate.status in {READY, READY_WITH_PENDING_LIVE_GATE}:
            continue
        rows.append(
            {
                "gate_id": gate.gate_id,
                "status": gate.status,
                "reason": gate.reason,
                "next_action": gate.next_action,
                "blocks_release": gate.blocks_release,
                "blocks_physical_flow": gate.blocks_physical_flow,
            }
        )
    return rows


def build_v1_5_formal_run_status(
    *,
    run_dir: str | Path,
    initialization_readiness_json: str | Path | None = None,
    pre_gas_readiness_json: str | Path | None = None,
    getco_readiness_json: str | Path | None = None,
    run_evidence_status_json: str | Path | None = None,
    full_flow_closure_readiness_json: str | Path | None = None,
    archive_closure_json: str | Path | None = None,
    algorithm_profile_runner_dry_run_json: str | Path | None = None,
    formal_database_dry_run_json: str | Path | None = None,
    formal_database_import_preflight_json: str | Path | None = None,
    formal_database_import_authorization_json: str | Path | None = None,
    formal_database_import_command_contract_json: str | Path | None = None,
    formal_database_import_blocked_executor_json: str | Path | None = None,
    formal_database_import_controlled_executor_design_json: str | Path | None = None,
) -> dict[str, Any]:
    """Return a top-level formal V1.5 status rollup from existing sidecars."""

    root = Path(run_dir).resolve()
    init_path = _explicit_or_latest(root, initialization_readiness_json, "v1_5_initialization_readiness.json")
    pre_gas_path = _explicit_or_latest(root, pre_gas_readiness_json, "v1_5_pre_gas_readiness.json")
    getco_path = _explicit_or_latest(root, getco_readiness_json, "v1_5_getco_identity_readiness.json")
    run_status_path = _explicit_or_latest(root, run_evidence_status_json, "v1_5_run_evidence_status.json")
    closure_path = _explicit_or_latest(root, full_flow_closure_readiness_json, "v1_5_full_flow_closure_readiness.json")
    archive_path = _explicit_or_latest(root, archive_closure_json, "v1_5_formal_archive_closure_index.json")
    algorithm_profile_runner_path = _explicit_or_latest(
        root,
        algorithm_profile_runner_dry_run_json,
        "v1_5_algorithm_profile_runner_dry_run.json",
    )
    formal_database_dry_run_path = _explicit_or_latest(
        root,
        formal_database_dry_run_json,
        "v1_5_formal_database_dry_run.json",
    )
    formal_database_import_preflight_path = _explicit_or_latest(
        root,
        formal_database_import_preflight_json,
        "v1_5_formal_database_import_preflight.json",
    )
    formal_database_import_authorization_path = _explicit_or_latest(
        root,
        formal_database_import_authorization_json,
        "v1_5_formal_database_import_authorization.json",
    )
    formal_database_import_command_contract_path = _explicit_or_latest(
        root,
        formal_database_import_command_contract_json,
        "v1_5_formal_database_import_command_contract.json",
    )
    formal_database_import_blocked_executor_path = _explicit_or_latest(
        root,
        formal_database_import_blocked_executor_json,
        "v1_5_formal_database_import_blocked_executor.json",
    )
    formal_database_import_controlled_executor_design_path = _explicit_or_latest(
        root,
        formal_database_import_controlled_executor_design_json,
        "v1_5_formal_database_import_controlled_executor_design.json",
    )

    init_payload = _load_json(init_path)
    pre_gas_payload = _load_json(pre_gas_path)
    getco_payload = _load_json(getco_path)
    run_payload = _load_json(run_status_path)
    closure_payload = _load_json(closure_path)
    archive_payload = _load_json(archive_path)
    algorithm_profile_runner_payload = _load_json(algorithm_profile_runner_path)
    formal_database_dry_run_payload = _load_json(formal_database_dry_run_path)
    formal_database_import_preflight_payload = _load_json(formal_database_import_preflight_path)
    formal_database_import_authorization_payload = _load_json(formal_database_import_authorization_path)
    formal_database_import_command_contract_payload = _load_json(formal_database_import_command_contract_path)
    formal_database_import_blocked_executor_payload = _load_json(formal_database_import_blocked_executor_path)
    formal_database_import_controlled_executor_design_payload = _load_json(
        formal_database_import_controlled_executor_design_path
    )

    gates = [
        _initialization_gate(init_path, init_payload),
        _getco_gate(getco_path, getco_payload),
        _pre_gas_gate(pre_gas_path, pre_gas_payload),
        _run_stage_gate(
            gate_id="pressure_senco9_pre_open_flow",
            title="Pressure/SENCO9 pre-open-flow check",
            run_path=run_status_path,
            run_status=run_payload,
            stage_id="pressure_quick_check",
            missing_reason="pressure/S9 evidence has not reached pass state",
            next_action="Complete pressure/SENCO9 no-write review or controlled pressure write package before gas flow.",
            physical_meaning="Pressure P must be traceable before CO2/H2O fitting so gas coefficients do not absorb pressure bias.",
            physical_flow_gate=True,
        ),
    ]
    if algorithm_profile_runner_path and algorithm_profile_runner_payload:
        gates.append(
            _algorithm_profile_runner_dry_run_gate(
                algorithm_profile_runner_path,
                algorithm_profile_runner_payload,
            )
        )
    if formal_database_dry_run_path and formal_database_dry_run_payload:
        gates.append(
            _formal_database_dry_run_gate(
                formal_database_dry_run_path,
                formal_database_dry_run_payload,
            )
        )
    if formal_database_import_preflight_path and formal_database_import_preflight_payload:
        gates.append(
            _formal_database_import_preflight_gate(
                formal_database_import_preflight_path,
                formal_database_import_preflight_payload,
            )
        )
    if formal_database_import_authorization_path and formal_database_import_authorization_payload:
        gates.append(
            _formal_database_import_authorization_gate(
                formal_database_import_authorization_path,
                formal_database_import_authorization_payload,
            )
        )
    if formal_database_import_command_contract_path and formal_database_import_command_contract_payload:
        gates.append(
            _formal_database_import_command_contract_gate(
                formal_database_import_command_contract_path,
                formal_database_import_command_contract_payload,
            )
        )
    if formal_database_import_blocked_executor_path and formal_database_import_blocked_executor_payload:
        gates.append(
            _formal_database_import_blocked_executor_gate(
                formal_database_import_blocked_executor_path,
                formal_database_import_blocked_executor_payload,
            )
        )
    if (
        formal_database_import_controlled_executor_design_path
        and formal_database_import_controlled_executor_design_payload
    ):
        gates.append(
            _formal_database_import_controlled_executor_design_gate(
                formal_database_import_controlled_executor_design_path,
                formal_database_import_controlled_executor_design_payload,
            )
        )
    gates.extend(
        [
            _run_stage_gate(
                gate_id="co2_open_flow_mature_queue",
                title="CO2 mature open-flow queue",
                run_path=run_status_path,
                run_status=run_payload,
                stage_id="co2_open_flow",
                missing_reason="CO2 mature open-flow queue evidence has not passed",
                next_action="Run or register the mature V1.5 CO2 open-flow queue evidence.",
                physical_meaning="CO2 calibration points must come from mature open-flow samples, not diagnostic sealed/pressure rows.",
            ),
            _run_stage_gate(
                gate_id="h2o_open_flow_mature_queue",
                title="H2O mature open-flow queue",
                run_path=run_status_path,
                run_status=run_payload,
                stage_id="h2o_open_flow",
                missing_reason="H2O mature open-flow queue evidence has not passed",
                next_action="Run or register the mature V1.5 H2O open-flow queue evidence.",
                physical_meaning="H2O fitting must preserve dewpoint-backed wet points and dry-gas low-water anchors separately.",
            ),
            _run_stage_gate(
                gate_id="candidate_fit_review",
                title="Candidate fit/QC review",
                run_path=run_status_path,
                run_status=run_payload,
                stage_id="candidate_review",
                missing_reason="candidate fit review has not passed",
                next_action="Run no-write candidate fitting/QC review before any controlled write package.",
                physical_meaning="Only eligible A-grade and explicitly reviewed samples should enter SENCO candidate fitting.",
            ),
            _run_stage_gate(
                gate_id="post_run_write_package",
                title="Post-run controlled-write package",
                run_path=run_status_path,
                run_status=run_payload,
                stage_id="post_run_coefficient_executor",
                missing_reason="post-run coefficient executor package has not passed",
                next_action="Generate the post-run executor package with eligibility, write plan, and reverify plan.",
                physical_meaning="The write package separates no-write review from manual authorized controlled SENCO writes.",
            ),
            _run_stage_gate(
                gate_id="controlled_write_and_reverification",
                title="Controlled write and post-write reverification",
                run_path=run_status_path,
                run_status=run_payload,
                stage_id="post_write_reverification",
                missing_reason="post-write reverification has not passed or has not been attempted",
                next_action="After authorized writes, run independent post-write reverification evidence.",
                physical_meaning="A coefficient write is not a formal release until independent open-flow reverification is present.",
            ),
            _archive_gate(
                closure_path=closure_path,
                closure=closure_payload,
                archive_path=archive_path,
                archive=archive_payload,
            ),
        ]
    )

    release_blockers = [gate for gate in gates if gate.blocks_release]
    physical_blockers = [gate for gate in gates if gate.blocks_physical_flow]
    current_gate = next((gate for gate in gates if gate.status in NON_READY_STATUSES), None)
    archive_gate = gates[-1]
    formal_release_allowed = not release_blockers and archive_gate.status == READY
    database_gate = next((gate for gate in gates if gate.gate_id == "formal_database_dry_run"), None)
    database_dry_run_ready = database_gate is None or database_gate.status == READY
    database_import_preflight_gate = next(
        (gate for gate in gates if gate.gate_id == "formal_database_import_preflight"),
        None,
    )
    database_import_preflight_ready = (
        database_import_preflight_gate is None or database_import_preflight_gate.status == READY
    )
    database_import_authorization_gate = next(
        (gate for gate in gates if gate.gate_id == "formal_database_import_authorization"),
        None,
    )
    database_import_authorization_ready = (
        database_import_authorization_gate is None or database_import_authorization_gate.status == READY
    )
    database_import_command_contract_gate = next(
        (gate for gate in gates if gate.gate_id == "formal_database_import_command_contract"),
        None,
    )
    database_import_command_contract_ready = (
        database_import_command_contract_gate is None or database_import_command_contract_gate.status == READY
    )
    database_import_blocked_executor_gate = next(
        (gate for gate in gates if gate.gate_id == "formal_database_import_blocked_executor"),
        None,
    )
    database_import_blocked_executor_ready = (
        database_import_blocked_executor_gate is None or database_import_blocked_executor_gate.status == READY
    )
    database_import_allowed = (
        formal_release_allowed
        and database_dry_run_ready
        and database_import_preflight_ready
        and database_import_authorization_ready
        and database_import_command_contract_ready
        and database_import_blocked_executor_ready
    )
    if any(gate.status == BLOCKED for gate in gates):
        overall_status = "blocked"
    elif any(gate.status == REVIEW_REQUIRED for gate in gates):
        overall_status = "review_required"
    elif formal_release_allowed:
        overall_status = "formal_release_ready"
    elif current_gate:
        overall_status = "in_progress"
    else:
        overall_status = "ready_for_next_action"

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "run_dir": str(root),
        "overall_status": overall_status,
        "current_stage": current_gate.gate_id if current_gate else "complete",
        "next_action": current_gate.next_action if current_gate else "Formal release is ready for reviewer sign-off.",
        "formal_release_allowed": formal_release_allowed,
        "database_import_allowed": database_import_allowed,
        "can_continue_physical_flow": not physical_blockers,
        "physical_boundaries": {
            "offline_status_only": True,
            "opens_com_ports": False,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "writes_device_id": False,
            "not_real_acceptance_evidence": True,
        },
        "linked_inputs": {
            "initialization_readiness_json": str(init_path) if init_path else "",
            "pre_gas_readiness_json": str(pre_gas_path) if pre_gas_path else "",
            "getco_readiness_json": str(getco_path) if getco_path else "",
            "run_evidence_status_json": str(run_status_path) if run_status_path else "",
            "full_flow_closure_readiness_json": str(closure_path) if closure_path else "",
            "archive_closure_json": str(archive_path) if archive_path else "",
            "algorithm_profile_runner_dry_run_json": str(algorithm_profile_runner_path)
            if algorithm_profile_runner_path
            else "",
            "formal_database_dry_run_json": str(formal_database_dry_run_path)
            if formal_database_dry_run_path
            else "",
            "formal_database_import_preflight_json": str(formal_database_import_preflight_path)
            if formal_database_import_preflight_path
            else "",
            "formal_database_import_authorization_json": str(formal_database_import_authorization_path)
            if formal_database_import_authorization_path
            else "",
            "formal_database_import_command_contract_json": str(formal_database_import_command_contract_path)
            if formal_database_import_command_contract_path
            else "",
            "formal_database_import_blocked_executor_json": str(formal_database_import_blocked_executor_path)
            if formal_database_import_blocked_executor_path
            else "",
            "formal_database_import_controlled_executor_design_json": str(
                formal_database_import_controlled_executor_design_path
            )
            if formal_database_import_controlled_executor_design_path
            else "",
        },
        "gates": [gate.to_json() for gate in gates],
        "gaps": _gap_rows(gates),
    }


def render_v1_5_formal_run_status_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 Formal Run Status",
        "",
        f"- schema: `{model.get('schema')}`",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- current_stage: `{model.get('current_stage')}`",
        f"- formal_release_allowed: `{model.get('formal_release_allowed')}`",
        f"- database_import_allowed: `{model.get('database_import_allowed')}`",
        f"- can_continue_physical_flow: `{model.get('can_continue_physical_flow')}`",
        f"- next_action: {model.get('next_action')}",
        "",
        "## Physical Boundaries",
        "",
    ]
    for key, value in (model.get("physical_boundaries") or {}).items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Gates", ""])
    for gate in model.get("gates") or []:
        lines.extend(
            [
                f"### {gate.get('gate_id')}",
                "",
                f"- title: {gate.get('title')}",
                f"- status: `{gate.get('status')}`",
                f"- source_status: `{gate.get('source_status')}`",
                f"- source_path: `{gate.get('source_path')}`",
                f"- reason: {gate.get('reason')}",
                f"- next_action: {gate.get('next_action')}",
                f"- blocks_release: `{gate.get('blocks_release')}`",
                f"- blocks_physical_flow: `{gate.get('blocks_physical_flow')}`",
                f"- physical_meaning: {gate.get('physical_meaning')}",
                "",
            ]
        )
    gaps = model.get("gaps") or []
    lines.extend(["## Gaps", ""])
    if not gaps:
        lines.append("- none")
    else:
        for gap in gaps:
            lines.append(
                f"- `{gap.get('gate_id')}`: {gap.get('status')} - {gap.get('reason')} "
                f"(next: {gap.get('next_action')})"
            )
    lines.append("")
    return "\n".join(lines)


def write_v1_5_formal_run_status_outputs(model: Mapping[str, Any], output_dir: str | Path) -> dict[str, str]:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    json_path = target / "v1_5_formal_run_status.json"
    md_path = target / "v1_5_formal_run_status.md"
    gates_path = target / "v1_5_formal_run_status_gates.csv"
    gaps_path = target / "v1_5_formal_run_status_gaps.csv"

    json_path.write_text(json.dumps(model, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_v1_5_formal_run_status_markdown(model), encoding="utf-8")

    gate_fields = [
        "gate_id",
        "title",
        "status",
        "source_path",
        "source_status",
        "reason",
        "next_action",
        "physical_meaning",
        "release_gate",
        "blocks_release",
        "blocks_physical_flow",
    ]
    with gates_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=gate_fields)
        writer.writeheader()
        for row in model.get("gates") or []:
            writer.writerow({key: row.get(key, "") for key in gate_fields})

    gap_fields = ["gate_id", "status", "reason", "next_action", "blocks_release", "blocks_physical_flow"]
    with gaps_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=gap_fields)
        writer.writeheader()
        for row in model.get("gaps") or []:
            writer.writerow({key: row.get(key, "") for key in gap_fields})

    return {
        "json_path": str(json_path.resolve()),
        "markdown_path": str(md_path.resolve()),
        "gates_csv_path": str(gates_path.resolve()),
        "gaps_csv_path": str(gaps_path.resolve()),
    }
