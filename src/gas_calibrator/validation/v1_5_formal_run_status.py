"""Build an offline V1.5 formal run status rollup.

This module reads existing readiness, evidence, closure, and archive sidecars
and turns them into a small reviewer-facing status dashboard. It is deliberately
read-only: it does not open COM ports, connect to PostgreSQL, control routes or
pressure, or write analyzer coefficients.
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .v1_5_authoritative_resume_state_writer_design import (
    build_v1_5_authoritative_resume_state_writer_design,
)
from .v1_5_authoritative_resume_state_writer_blocked_executor import (
    build_v1_5_authoritative_resume_state_writer_blocked_executor,
)
from .v1_5_authoritative_resume_state_controlled_write_preflight import (
    build_v1_5_authoritative_resume_state_controlled_write_preflight,
)
from .v1_5_authoritative_resume_state_post_write_verification import (
    READY_STATUS as RESUME_STATE_POST_WRITE_READY_STATUS,
    SCHEMA as RESUME_STATE_POST_WRITE_SCHEMA,
    build_v1_5_authoritative_resume_state_post_write_verification,
)
from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    READY_STATUS as OFFLINE_STATE_ADVANCE_POST_WRITE_READY_STATUS,
    SCHEMA as OFFLINE_STATE_ADVANCE_POST_WRITE_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_post_write_verification,
)
from .v1_5_authoritative_resume_offline_state_advance_consumer_readiness import (
    READY_STATUS as OFFLINE_STATE_ADVANCE_CONSUMER_READY_STATUS,
    SCHEMA as OFFLINE_STATE_ADVANCE_CONSUMER_SCHEMA,
    VERIFICATION_COMPARE_KEYS as OFFLINE_STATE_ADVANCE_VERIFICATION_COMPARE_KEYS,
    build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness,
)
from .v1_5_authoritative_resume_offline_state_advance_next_step_plan import (
    READY_STATUS as OFFLINE_STATE_ADVANCE_NEXT_STEP_PLAN_READY_STATUS,
    SCHEMA as OFFLINE_STATE_ADVANCE_NEXT_STEP_PLAN_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_plan,
)
from .v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight import (
    READY_STATUS as OFFLINE_STATE_ADVANCE_NEXT_STEP_AUTHORIZATION_READY_STATUS,
    SCHEMA as OFFLINE_STATE_ADVANCE_NEXT_STEP_AUTHORIZATION_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight,
)
from .v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor import (
    BLOCKED_READY_STATUS as OFFLINE_STATE_ADVANCE_NEXT_STEP_BLOCKED_EXECUTOR_READY_STATUS,
    SCHEMA as OFFLINE_STATE_ADVANCE_NEXT_STEP_BLOCKED_EXECUTOR_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor,
)
from .v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design import (
    READY_STATUS as OFFLINE_STATE_ADVANCE_NEXT_STEP_CONTROLLED_DESIGN_READY_STATUS,
    SCHEMA as OFFLINE_STATE_ADVANCE_NEXT_STEP_CONTROLLED_DESIGN_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design,
)
from .v1_5_senco_artifact_authorization import validate_senco_artifact_authorization


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


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _flatten_completed_steps(step_ids: Iterable[str]) -> list[str]:
    return [item for step_id in step_ids for item in ("--completed-step", str(step_id))]


def _plan_step_ids(payload: Mapping[str, Any]) -> list[str]:
    return [
        str(row.get("step_id") or "")
        for row in payload.get("steps") or []
        if isinstance(row, Mapping)
    ]


def _artifact_sha256(path_value: Any) -> str:
    path = Path(str(path_value or ""))
    if not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    manifest = payload.get("manifest")
    return manifest if isinstance(manifest, Mapping) else payload


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


def _batch_initialization_closeout_gate(
    path: Path | None,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("connects_postgresql") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("formal_release_allowed") is False
        and payload.get("database_import_allowed") is False
        and payload.get("not_real_acceptance_evidence") is True
    )
    device_count = _safe_int(payload.get("device_count"))
    device_ready_count = _safe_int(payload.get("device_ready_count"))
    ready = (
        source_status == "ready_for_mature_open_flow_from_initialization_index"
        and payload.get("batch_initialization_closeout_ready") is True
        and payload.get("ready_for_mature_open_flow_from_initialization_index") is True
        and 1 <= device_count <= 6
        and device_ready_count == device_count
        and payload.get("mature_route_baseline")
        == "0620/0621 clean worktree mature physical route"
        and payload.get("mature_fitting_baseline") == "0613 V1.5 fitting path"
    )
    if not payload:
        status = MISSING
        reason = "batch initialization closeout index missing"
    elif not boundary_ok:
        status = BLOCKED
        reason = "batch initialization closeout index boundary is not clean"
    elif ready:
        status = READY
        reason = f"batch initialization closeout ready for {device_count} active device(s)"
    elif "blocked" in source_status:
        status = BLOCKED
        reason = f"source_status={source_status}"
    else:
        status = REVIEW_REQUIRED
        review_reasons = payload.get("review_reasons")
        if isinstance(review_reasons, list) and review_reasons:
            reason = "; ".join(str(item) for item in review_reasons[:3])
        else:
            reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="batch_initialization_closeout",
        title="Batch initialization closeout before mature open flow",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Bind the authorized read-only COM identity/GETCO/runtime evidence, per-device S5-S8 neutral state, "
            "pressure/S9 readiness, and formal route readiness into one batch closeout index."
        ),
        physical_meaning=(
            "This is the final offline pre-route gate for the active 1-6 device batch. It prevents an early "
            "contract-only pre-gas sidecar from being mistaken for completed live initialization evidence."
        ),
        blocks_physical_flow=status in {MISSING, REVIEW_REQUIRED, BLOCKED},
    )


def _post_closeout_resume_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    expected_batch_closeout_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("does_not_execute_commands") is True
        and payload.get("applies_completed_steps") is False
        and payload.get("live_resume_execution_allowed") is False
        and payload.get("route_authorization_still_required") is True
        and payload.get("opens_com_ports") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("connects_postgresql") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("formal_release_allowed") is False
        and payload.get("database_import_allowed") is False
        and payload.get("not_real_acceptance_evidence") is True
    )
    prefix = [str(item) for item in payload.get("resume_completed_step_ids") or []]
    plan_hash_ok = bool(payload.get("full_flow_plan_sha256")) and _artifact_sha256(
        payload.get("full_flow_plan_json")
    ) == str(payload.get("full_flow_plan_sha256") or "")
    closeout_hash_ok = bool(payload.get("batch_initialization_closeout_sha256")) and _artifact_sha256(
        payload.get("batch_initialization_closeout_json")
    ) == str(payload.get("batch_initialization_closeout_sha256") or "")
    try:
        closeout_path_bound = expected_batch_closeout_path is not None and Path(
            str(payload.get("batch_initialization_closeout_json") or "")
        ).resolve() == expected_batch_closeout_path.resolve()
    except (OSError, RuntimeError):
        closeout_path_bound = False
    ready = (
        source_status == "ready_for_post_closeout_resume_review"
        and payload.get("resume_gate_ready") is True
        and payload.get("ready_for_resume_state_application_review") is True
        and payload.get("next_step_id") == "temperature_channel_fast_review"
        and "batch_initialization_closeout_index" in prefix
        and "post_closeout_resume_gate_snapshot" in prefix
        and plan_hash_ok
        and closeout_hash_ok
        and closeout_path_bound
    )
    if not payload:
        status = MISSING
        reason = "post-closeout resume gate missing"
    elif not boundary_ok:
        status = BLOCKED
        reason = "post-closeout resume gate boundary is not clean"
    elif ready:
        status = READY
        reason = "evidence-bound resume prefix is ready for state-application review"
    elif not plan_hash_ok or not closeout_hash_ok or not closeout_path_bound:
        status = BLOCKED
        reason = "post-closeout resume source hash or batch-closeout path missing or mismatched"
    elif "blocked" in source_status:
        status = BLOCKED
        reason = f"source_status={source_status}"
    else:
        status = REVIEW_REQUIRED
        review_reasons = payload.get("review_reasons")
        reason = (
            "; ".join(str(item) for item in review_reasons[:3])
            if isinstance(review_reasons, list) and review_reasons
            else f"source_status={source_status or 'unknown'}"
        )
    return _gate(
        gate_id="post_closeout_resume_gate",
        title="Post-closeout evidence-bound resume gate",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Regenerate the resume gate from the exact current full-flow plan and ready batch closeout index. "
            "Route authorization remains separate."
        ),
        physical_meaning=(
            "Prevents arbitrary --completed-step lists from being mistaken for evidence-backed initialization "
            "completion. This artifact still does not apply state or execute a route."
        ),
        blocks_physical_flow=status in {MISSING, REVIEW_REQUIRED, BLOCKED},
    )


def _resume_prefix_application_review_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    expected_resume_gate_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("does_not_execute_commands") is True
        and payload.get("applies_completed_steps") is False
        and payload.get("writes_authoritative_state") is False
        and payload.get("would_execute") is False
        and payload.get("live_resume_execution_allowed") is False
        and payload.get("route_authorization_still_required") is True
        and payload.get("opens_com_ports") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("connects_postgresql") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("formal_release_allowed") is False
        and payload.get("database_import_allowed") is False
        and payload.get("not_real_acceptance_evidence") is True
    )
    plan_hash_ok = bool(payload.get("full_flow_plan_sha256")) and _artifact_sha256(
        payload.get("full_flow_plan_json")
    ) == str(payload.get("full_flow_plan_sha256") or "")
    resume_gate_hash_ok = bool(payload.get("post_closeout_resume_gate_sha256")) and _artifact_sha256(
        payload.get("post_closeout_resume_gate_json")
    ) == str(payload.get("post_closeout_resume_gate_sha256") or "")
    batch_closeout_hash_ok = bool(payload.get("batch_initialization_closeout_sha256")) and _artifact_sha256(
        payload.get("batch_initialization_closeout_json")
    ) == str(payload.get("batch_initialization_closeout_sha256") or "")
    try:
        resume_gate_path_bound = expected_resume_gate_path is not None and Path(
            str(payload.get("post_closeout_resume_gate_json") or "")
        ).resolve() == expected_resume_gate_path.resolve()
    except (OSError, RuntimeError):
        resume_gate_path_bound = False
    plan_payload = _load_json(payload.get("full_flow_plan_json"))
    resume_gate_payload = _load_json(payload.get("post_closeout_resume_gate_json"))
    step_ids = _plan_step_ids(plan_payload)
    expected_prefix: list[str] = []
    if "post_closeout_resume_gate_snapshot" in step_ids:
        expected_prefix = step_ids[: step_ids.index("post_closeout_resume_gate_snapshot") + 1]
    application_index = (
        step_ids.index("post_closeout_resume_prefix_application_review")
        if "post_closeout_resume_prefix_application_review" in step_ids
        else -1
    )
    gate_index = (
        step_ids.index("post_closeout_resume_gate_snapshot")
        if "post_closeout_resume_gate_snapshot" in step_ids
        else -1
    )
    next_index = (
        step_ids.index("authoritative_resume_state_writer_design")
        if "authoritative_resume_state_writer_design" in step_ids
        else -1
    )
    adjacent_application_steps = (
        application_index == gate_index + 1
        and next_index == application_index + 1
    )
    expected_after_application = (
        step_ids[: application_index + 1] if adjacent_application_steps else []
    )
    reviewed_prefix = [str(item) for item in payload.get("reviewed_resume_completed_step_ids") or []]
    reviewed_after = [
        str(item) for item in payload.get("reviewed_completed_step_ids_after_application") or []
    ]
    exact_prefix_ok = (
        bool(expected_prefix)
        and len(step_ids) == len(set(step_ids))
        and adjacent_application_steps
        and reviewed_prefix == expected_prefix
        and reviewed_after == expected_after_application
        and [str(item) for item in payload.get("reviewed_resume_cli_arguments") or []]
        == _flatten_completed_steps(expected_prefix)
        and [str(item) for item in payload.get("reviewed_state_application_cli_arguments") or []]
        == _flatten_completed_steps(expected_after_application)
        and resume_gate_payload.get("resume_completed_step_ids") == expected_prefix
        and resume_gate_payload.get("resume_cli_arguments") == _flatten_completed_steps(expected_prefix)
    )
    try:
        plan_path_bound_to_resume_gate = Path(
            str(payload.get("full_flow_plan_json") or "")
        ).resolve() == Path(str(resume_gate_payload.get("full_flow_plan_json") or "")).resolve()
        batch_path_bound_to_resume_gate = Path(
            str(payload.get("batch_initialization_closeout_json") or "")
        ).resolve() == Path(
            str(resume_gate_payload.get("batch_initialization_closeout_json") or "")
        ).resolve()
    except (OSError, RuntimeError):
        plan_path_bound_to_resume_gate = False
        batch_path_bound_to_resume_gate = False
    source_binding_ok = (
        str(payload.get("run_id") or "") == str(plan_payload.get("run_id") or "")
        and str(resume_gate_payload.get("run_id") or "") == str(plan_payload.get("run_id") or "")
        and plan_path_bound_to_resume_gate
        and batch_path_bound_to_resume_gate
        and str(resume_gate_payload.get("full_flow_plan_sha256") or "")
        == str(payload.get("full_flow_plan_sha256") or "")
        and str(resume_gate_payload.get("batch_initialization_closeout_sha256") or "")
        == str(payload.get("batch_initialization_closeout_sha256") or "")
    )
    ready = (
        source_status == "ready_for_resume_prefix_state_application_review"
        and payload.get("resume_prefix_application_review_ready") is True
        and payload.get("resume_prefix_consumed_for_review") is True
        and payload.get("state_preview_current_step_id") == "authoritative_resume_state_writer_design"
        and plan_hash_ok
        and resume_gate_hash_ok
        and batch_closeout_hash_ok
        and resume_gate_path_bound
        and exact_prefix_ok
        and source_binding_ok
    )
    if not payload:
        status = MISSING
        reason = "resume-prefix application review missing"
    elif not boundary_ok:
        status = BLOCKED
        reason = "resume-prefix application review boundary is not clean"
    elif ready:
        status = READY
        reason = "resume prefix is hash-bound and ready for a later authoritative state-application step"
    elif not plan_hash_ok or not resume_gate_hash_ok or not batch_closeout_hash_ok or not resume_gate_path_bound:
        status = BLOCKED
        reason = "resume-prefix application source hash or bound path missing or mismatched"
    elif not exact_prefix_ok or not source_binding_ok:
        status = BLOCKED
        reason = "resume-prefix application exact-prefix or source binding is invalid"
    elif "blocked" in source_status:
        status = BLOCKED
        reason = f"source_status={source_status}"
    else:
        status = REVIEW_REQUIRED
        review_reasons = payload.get("review_reasons")
        reason = (
            "; ".join(str(item) for item in review_reasons[:3])
            if isinstance(review_reasons, list) and review_reasons
            else f"source_status={source_status or 'unknown'}"
        )
    return _gate(
        gate_id="resume_prefix_application_review",
        title="Resume-prefix state-application review",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Regenerate the application review from the exact current plan and resume gate. "
            "A separate package is still required before authoritative state is written."
        ),
        physical_meaning=(
            "Consumes the evidence-bound completed-step prefix for validation and previews the writer-design "
            "review as the next stage without changing state or executing pressure, gas, or water actions."
        ),
        blocks_physical_flow=status in {MISSING, REVIEW_REQUIRED, BLOCKED},
    )


def _authoritative_resume_state_writer_design_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    expected_application_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("design_review_only") is True
        and payload.get("execution_supported") is False
        and payload.get("authoritative_state_write_allowed") is False
        and payload.get("does_not_execute_commands") is True
        and payload.get("applies_completed_steps") is False
        and payload.get("writes_authoritative_state") is False
        and payload.get("would_execute") is False
        and payload.get("live_resume_execution_allowed") is False
        and payload.get("route_authorization_still_required") is True
        and payload.get("opens_com_ports") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("connects_postgresql") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("formal_release_allowed") is False
        and payload.get("database_import_allowed") is False
        and payload.get("not_real_acceptance_evidence") is True
    )
    try:
        application_path_bound = expected_application_path is not None and Path(
            str(payload.get("resume_prefix_application_review_json") or "")
        ).resolve() == expected_application_path.resolve()
    except (OSError, RuntimeError):
        application_path_bound = False
    plan_hash_ok = bool(payload.get("full_flow_plan_sha256")) and _artifact_sha256(
        payload.get("full_flow_plan_json")
    ) == str(payload.get("full_flow_plan_sha256") or "")
    application_hash_ok = bool(
        payload.get("resume_prefix_application_review_sha256")
    ) and _artifact_sha256(payload.get("resume_prefix_application_review_json")) == str(
        payload.get("resume_prefix_application_review_sha256") or ""
    )
    recomputed: dict[str, Any] = {}
    if plan_hash_ok and application_hash_ok and application_path_bound:
        try:
            recomputed = build_v1_5_authoritative_resume_state_writer_design(
                full_flow_plan_json=payload.get("full_flow_plan_json"),
                resume_prefix_application_review_json=expected_application_path,
            )
        except (OSError, ValueError, json.JSONDecodeError):
            recomputed = {}
    exact_design_ok = bool(recomputed) and all(
        payload.get(key) == recomputed.get(key)
        for key in (
            "run_id",
            "full_flow_plan_json",
            "full_flow_plan_sha256",
            "resume_prefix_application_review_json",
            "resume_prefix_application_review_sha256",
            "post_closeout_resume_gate_json",
            "post_closeout_resume_gate_sha256",
            "batch_initialization_closeout_json",
            "batch_initialization_closeout_sha256",
            "proposed_completed_step_ids",
            "proposed_completed_step_cli_arguments",
            "proposed_failed_step_ids",
            "proposed_current_step_id",
            "proposed_authoritative_state_json",
            "proposed_authoritative_state_markdown",
            "proposed_authorization_state",
            "transaction_contract",
        )
    )
    ready = (
        source_status == "ready_for_authoritative_resume_state_writer_design_review"
        and payload.get("authoritative_resume_state_writer_design_ready") is True
        and recomputed.get("authoritative_resume_state_writer_design_ready") is True
        and boundary_ok
        and application_path_bound
        and plan_hash_ok
        and application_hash_ok
        and exact_design_ok
    )
    if not payload:
        status = MISSING
        reason = "authoritative resume-state writer design missing"
    elif not boundary_ok:
        status = BLOCKED
        reason = "authoritative resume-state writer design boundary is not clean"
    elif ready:
        status = READY
        reason = "atomic authoritative resume-state writer contract is ready for implementation review"
    elif not application_path_bound or not plan_hash_ok or not application_hash_ok:
        status = BLOCKED
        reason = "authoritative writer design source path or hash missing or mismatched"
    elif not exact_design_ok:
        status = BLOCKED
        reason = "authoritative writer design differs from independently recomputed plan and prefix"
    elif "blocked" in source_status:
        status = BLOCKED
        reason = f"source_status={source_status}"
    else:
        status = REVIEW_REQUIRED
        review_reasons = payload.get("review_reasons")
        reason = (
            "; ".join(str(item) for item in review_reasons[:3])
            if isinstance(review_reasons, list) and review_reasons
            else f"source_status={source_status or 'unknown'}"
        )
    return _gate(
        gate_id="authoritative_resume_state_writer_design",
        title="Authoritative resume-state writer design",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Regenerate this design from the exact current plan and resume-prefix application review. "
            "A later small package must still implement the blocked-by-default atomic writer."
        ),
        physical_meaning=(
            "Defines compare-and-swap, snapshot, atomic replacement, readback, and rollback requirements "
            "and hands them to a blocked executor without writing state or executing temperature, pressure, gas, or water stages."
        ),
        blocks_physical_flow=status in {MISSING, REVIEW_REQUIRED, BLOCKED},
    )


def _authoritative_resume_state_writer_blocked_executor_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    expected_design_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("production_state") == "blocked_executor_only"
        and payload.get("execution_supported") is False
        and payload.get("execution_requested") is False
        and payload.get("authoritative_state_write_allowed") is False
        and payload.get("write_state_flag_allowed") is False
        and payload.get("state_target_argument_allowed") is False
        and payload.get("expected_state_sha_argument_allowed") is False
        and payload.get("authorization_inputs_allowed") is False
        and payload.get("does_not_execute_commands") is True
        and payload.get("applies_completed_steps") is False
        and payload.get("writes_authoritative_state") is False
        and payload.get("state_file_created") is False
        and payload.get("state_file_replaced") is False
        and payload.get("state_snapshot_created") is False
        and payload.get("rollback_executed") is False
        and payload.get("would_execute") is False
        and payload.get("live_resume_execution_allowed") is False
        and payload.get("route_authorization_still_required") is True
        and payload.get("opens_com_ports") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("connects_postgresql") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("formal_release_allowed") is False
        and payload.get("database_import_allowed") is False
        and payload.get("not_real_acceptance_evidence") is True
    )
    try:
        design_path_bound = expected_design_path is not None and Path(
            str(payload.get("authoritative_resume_state_writer_design_json") or "")
        ).resolve() == expected_design_path.resolve()
    except (OSError, RuntimeError):
        design_path_bound = False
    plan_hash_ok = bool(payload.get("full_flow_plan_sha256")) and _artifact_sha256(
        payload.get("full_flow_plan_json")
    ) == str(payload.get("full_flow_plan_sha256") or "")
    application_hash_ok = bool(
        payload.get("resume_prefix_application_review_sha256")
    ) and _artifact_sha256(payload.get("resume_prefix_application_review_json")) == str(
        payload.get("resume_prefix_application_review_sha256") or ""
    )
    design_hash_ok = bool(
        payload.get("authoritative_resume_state_writer_design_sha256")
    ) and _artifact_sha256(payload.get("authoritative_resume_state_writer_design_json")) == str(
        payload.get("authoritative_resume_state_writer_design_sha256") or ""
    )
    recomputed: dict[str, Any] = {}
    if plan_hash_ok and application_hash_ok and design_hash_ok and design_path_bound:
        try:
            recomputed = build_v1_5_authoritative_resume_state_writer_blocked_executor(
                full_flow_plan_json=payload.get("full_flow_plan_json"),
                resume_prefix_application_review_json=payload.get(
                    "resume_prefix_application_review_json"
                ),
                authoritative_resume_state_writer_design_json=expected_design_path,
            )
        except (OSError, ValueError, json.JSONDecodeError):
            recomputed = {}
    exact_stub_ok = bool(recomputed) and all(
        payload.get(key) == recomputed.get(key)
        for key in (
            "run_id",
            "full_flow_plan_json",
            "full_flow_plan_sha256",
            "resume_prefix_application_review_json",
            "resume_prefix_application_review_sha256",
            "authoritative_resume_state_writer_design_json",
            "authoritative_resume_state_writer_design_sha256",
            "proposed_authoritative_state_json_recorded_only",
            "next_step_id_after_blocked_executor_review",
        )
    ) and payload.get("checks") == json.loads(json.dumps(recomputed.get("checks") or []))
    ready = (
        source_status
        == "blocked_pending_authoritative_resume_state_writer_implementation"
        and payload.get("blocked_executor_ready") is True
        and int(payload.get("review_required_count") or 0) == 0
        and recomputed.get("blocked_executor_ready") is True
        and boundary_ok
        and design_path_bound
        and plan_hash_ok
        and application_hash_ok
        and design_hash_ok
        and exact_stub_ok
    )
    if not payload:
        status = MISSING
        reason = "authoritative resume-state writer blocked executor evidence missing"
    elif not boundary_ok:
        status = BLOCKED
        reason = "authoritative resume-state writer blocked executor boundary is not clean"
    elif ready:
        status = READY
        reason = "authoritative resume-state writer remains blocked and no state target was mutated"
    elif not design_path_bound or not plan_hash_ok or not application_hash_ok or not design_hash_ok:
        status = BLOCKED
        reason = "blocked executor source path or hash missing or mismatched"
    elif not exact_stub_ok:
        status = BLOCKED
        reason = "blocked executor differs from independently recomputed lock evidence"
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="authoritative_resume_state_writer_blocked_executor",
        title="Authoritative resume-state writer blocked executor",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Keep state writing locked. A later separately reviewed controlled writer must implement #91 atomic transaction and authorization requirements."
        ),
        physical_meaning=(
            "Proves that resume-state review cannot create or replace state, open devices, control routes, write coefficients, or import a database."
        ),
        blocks_physical_flow=status in {MISSING, REVIEW_REQUIRED, BLOCKED},
    )


def _authoritative_resume_state_controlled_write_preflight_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    expected_blocked_executor_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("production_state") == "offline_controlled_write_preflight_only"
        and payload.get("execution_supported") is False
        and payload.get("execution_requested") is False
        and payload.get("authoritative_state_write_allowed") is False
        and payload.get("does_not_execute_commands") is True
        and payload.get("writes_authoritative_state") is False
        and payload.get("state_file_created") is False
        and payload.get("state_file_replaced") is False
        and payload.get("state_snapshot_created") is False
        and payload.get("rollback_executed") is False
        and payload.get("would_execute") is False
        and payload.get("live_resume_execution_allowed") is False
        and payload.get("route_authorization_still_required") is True
        and payload.get("opens_com_ports") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("connects_postgresql") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("formal_release_allowed") is False
        and payload.get("database_import_allowed") is False
        and payload.get("not_real_acceptance_evidence") is True
    )
    try:
        blocked_path_bound = expected_blocked_executor_path is not None and Path(
            str(
                payload.get(
                    "authoritative_resume_state_writer_blocked_executor_json"
                )
                or ""
            )
        ).resolve() == expected_blocked_executor_path.resolve()
    except (OSError, RuntimeError):
        blocked_path_bound = False
    source_fields = (
        ("full_flow_plan_json", "full_flow_plan_sha256"),
        (
            "resume_prefix_application_review_json",
            "resume_prefix_application_review_sha256",
        ),
        (
            "authoritative_resume_state_writer_design_json",
            "authoritative_resume_state_writer_design_sha256",
        ),
        (
            "authoritative_resume_state_writer_blocked_executor_json",
            "authoritative_resume_state_writer_blocked_executor_sha256",
        ),
        ("authorization_packet_json", "authorization_packet_sha256"),
    )
    source_hashes_ok = all(
        bool(payload.get(hash_field))
        and _artifact_sha256(payload.get(path_field))
        == str(payload.get(hash_field) or "")
        for path_field, hash_field in source_fields
    )
    preview_path = Path(str(payload.get("candidate_state_preview_json") or ""))
    preview_hash_ok = (
        bool(payload.get("candidate_state_sha256"))
        and bool(payload.get("candidate_state_preview_sha256"))
        and _artifact_sha256(preview_path)
        == str(payload.get("candidate_state_preview_sha256") or "")
        == str(payload.get("candidate_state_sha256") or "")
    )
    recomputed: dict[str, Any] = {}
    if source_hashes_ok and blocked_path_bound:
        try:
            recomputed = build_v1_5_authoritative_resume_state_controlled_write_preflight(
                full_flow_plan_json=payload.get("full_flow_plan_json"),
                resume_prefix_application_review_json=payload.get(
                    "resume_prefix_application_review_json"
                ),
                authoritative_resume_state_writer_design_json=payload.get(
                    "authoritative_resume_state_writer_design_json"
                ),
                authoritative_resume_state_writer_blocked_executor_json=(
                    expected_blocked_executor_path
                ),
                authorization_packet_json=payload.get("authorization_packet_json"),
            )
        except (OSError, ValueError, json.JSONDecodeError):
            recomputed = {}
    exact_preflight_ok = bool(recomputed) and all(
        payload.get(key) == recomputed.get(key)
        for key in (
            "overall_status",
            "blocker_count",
            "review_required_count",
            "controlled_write_preflight_ready",
            "run_id",
            "full_flow_plan_json",
            "full_flow_plan_sha256",
            "resume_prefix_application_review_json",
            "resume_prefix_application_review_sha256",
            "authoritative_resume_state_writer_design_json",
            "authoritative_resume_state_writer_design_sha256",
            "authoritative_resume_state_writer_blocked_executor_json",
            "authoritative_resume_state_writer_blocked_executor_sha256",
            "authorization_packet_json",
            "authorization_packet_sha256",
            "authorization_id",
            "authoritative_state_json_read_only",
            "state_target_exists",
            "observed_existing_state_sha256",
            "expected_existing_state_sha256",
            "candidate_state",
            "candidate_state_sha256",
        )
    ) and payload.get("checks") == json.loads(json.dumps(recomputed.get("checks") or []))
    ready = (
        source_status == "ready_for_authoritative_resume_state_controlled_write_review"
        and payload.get("controlled_write_preflight_ready") is True
        and int(payload.get("blocker_count") or 0) == 0
        and int(payload.get("review_required_count") or 0) == 0
        and recomputed.get("controlled_write_preflight_ready") is True
        and boundary_ok
        and blocked_path_bound
        and source_hashes_ok
        and preview_hash_ok
        and exact_preflight_ok
    )
    if not payload:
        status = MISSING
        reason = "authoritative resume-state controlled-write preflight missing"
    elif not boundary_ok:
        status = BLOCKED
        reason = "authoritative resume-state controlled-write preflight boundary is not clean"
    elif ready:
        status = READY
        reason = "candidate, current-state SHA256, and distinct authorization are bound while state writing remains disabled"
    elif not blocked_path_bound or not source_hashes_ok:
        status = BLOCKED
        reason = "controlled-write preflight source path or hash missing or mismatched"
    elif not preview_hash_ok:
        status = BLOCKED
        reason = "candidate state preview is missing or differs from the authorized candidate hash"
    elif not exact_preflight_ok:
        status = BLOCKED
        reason = "controlled-write preflight differs from independently recomputed evidence"
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="authoritative_resume_state_controlled_write_preflight",
        title="Authoritative resume-state controlled-write preflight",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Keep state writing locked. A separately reviewed atomic writer must consume this exact preflight and recheck the current-state SHA immediately before replacement."
        ),
        physical_meaning=(
            "Binds exact candidate bytes, current target SHA256, and distinct authorization without creating, replacing, or snapshotting the authoritative state."
        ),
        blocks_physical_flow=status in {MISSING, REVIEW_REQUIRED, BLOCKED},
    )


def _authoritative_resume_state_post_write_verification_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    atomic_write_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = all(
        payload.get(field) is False
        for field in (
            "opens_com_ports",
            "controls_pressure",
            "controls_water_or_gas_routes",
            "writes_authoritative_state",
            "writes_sn",
            "writes_device_id",
            "writes_coefficients",
            "connects_postgresql",
            "database_written",
            "formal_release_allowed",
            "database_import_allowed",
        )
    ) and payload.get("not_real_acceptance_evidence") is True
    declared_atomic = Path(str(payload.get("atomic_write_json") or "")).resolve()
    atomic_bound = atomic_write_path is not None and declared_atomic == atomic_write_path.resolve()
    recomputed: dict[str, Any] = {}
    if atomic_bound:
        try:
            recomputed = build_v1_5_authoritative_resume_state_post_write_verification(
                atomic_write_json=atomic_write_path
            )
        except (OSError, ValueError, json.JSONDecodeError):
            recomputed = {}
    exact = bool(recomputed) and all(
        payload.get(key) == recomputed.get(key)
        for key in (
            "schema",
            "overall_status",
            "post_write_verification_ready",
            "blocker_count",
            "blocker_reasons",
            "atomic_write_json",
            "atomic_write_sha256",
            "preflight_json",
            "preflight_sha256",
            "writer_authorization_json",
            "writer_authorization_sha256",
            "authoritative_state_json",
            "authoritative_state_sha256",
            "candidate_state_preview_json",
            "candidate_state_sha256",
        )
    )
    ready = (
        payload.get("schema") == RESUME_STATE_POST_WRITE_SCHEMA
        and source_status == RESUME_STATE_POST_WRITE_READY_STATUS
        and payload.get("post_write_verification_ready") is True
        and int(payload.get("blocker_count") or 0) == 0
        and boundary_ok
        and atomic_bound
        and exact
    )
    if not payload:
        status, reason = MISSING, "authoritative resume-state post-write verification missing"
    elif not boundary_ok:
        status, reason = BLOCKED, "resume-state post-write verification boundary is not clean"
    elif not atomic_bound:
        status, reason = BLOCKED, "post-write verification is not bound to the detected atomic write evidence"
    elif not exact:
        status, reason = BLOCKED, "post-write verification differs from independently recomputed evidence"
    elif ready:
        status, reason = READY, "authoritative resume state exactly matches the authorized candidate and readback evidence"
    else:
        status, reason = BLOCKED, f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="authoritative_resume_state_post_write_verification",
        title="Authoritative resume-state post-write verification",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Regenerate offline post-write verification from the exact atomic-write evidence before resuming physical flow.",
        physical_meaning="A resumed run must consume the exact state bytes that were authorized, atomically written, and read back.",
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=status != READY,
    )


def _authoritative_resume_offline_state_advance_post_write_verification_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    atomic_write_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("state_consumption_allowed") is False
        and payload.get("execution_supported") is False
        and payload.get("resume_execution_allowed") is False
        and all(
            payload.get(field) is False
            for field in (
                "opens_com_ports",
                "controls_pressure",
                "controls_water_or_gas_routes",
                "writes_authoritative_state",
                "writes_sn",
                "writes_device_id",
                "writes_coefficients",
                "connects_postgresql",
                "database_written",
                "formal_release_allowed",
                "database_import_allowed",
            )
        )
        and payload.get("not_real_acceptance_evidence") is True
    )
    declared_atomic = Path(str(payload.get("atomic_write_json") or "")).resolve()
    atomic_bound = atomic_write_path is not None and declared_atomic == atomic_write_path.resolve()
    recomputed: dict[str, Any] = {}
    if atomic_bound:
        try:
            recomputed = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
                atomic_write_json=atomic_write_path
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
            recomputed = {}
    exact = (
        bool(recomputed)
        and payload.get("schema") == recomputed.get("schema")
        and all(
            payload.get(key) == recomputed.get(key)
            for key in OFFLINE_STATE_ADVANCE_VERIFICATION_COMPARE_KEYS
        )
    )
    ready = (
        payload.get("schema") == OFFLINE_STATE_ADVANCE_POST_WRITE_SCHEMA
        and source_status == OFFLINE_STATE_ADVANCE_POST_WRITE_READY_STATUS
        and payload.get("post_write_verification_ready") is True
        and int(payload.get("blocker_count") or 0) == 0
        and not payload.get("blocker_reasons")
        and boundary_ok
        and atomic_bound
        and exact
    )
    if not payload:
        status, reason = MISSING, "offline state-advance post-write verification missing"
    elif not boundary_ok:
        status, reason = BLOCKED, "offline state-advance post-write verification boundary is not clean"
    elif not atomic_bound:
        status, reason = BLOCKED, "offline state-advance verification is not bound to the detected atomic writer"
    elif not exact:
        status, reason = BLOCKED, "offline state-advance verification differs from independently recomputed evidence"
    elif ready:
        status, reason = READY, "one-step offline state advance, rollback snapshot, readback, and lock release are verified"
    else:
        status, reason = BLOCKED, f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="authoritative_resume_offline_state_advance_post_write_verification",
        title="Offline resume-state advance post-write verification",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Regenerate the offline post-write verification from the exact manual writer evidence before CO2 resume planning.",
        physical_meaning="Proves that the completed offline step advanced the canonical state by exactly one position without changing any physical or analyzer authority.",
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=status != READY,
    )


def _authoritative_resume_offline_state_advance_consumer_readiness_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    verification_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("state_consumption_allowed") is True
        and payload.get("execution_supported") is False
        and payload.get("resume_execution_allowed") is False
        and payload.get("would_execute") is False
        and all(
            payload.get(field) is False
            for field in (
                "opens_com_ports",
                "controls_pressure",
                "controls_water_or_gas_routes",
                "writes_authoritative_state",
                "writes_sn",
                "writes_device_id",
                "writes_coefficients",
                "connects_postgresql",
                "database_written",
                "formal_release_allowed",
                "database_import_allowed",
            )
        )
        and payload.get("not_real_acceptance_evidence") is True
    )
    declared_verification = Path(
        str(payload.get("post_write_verification_json") or "")
    ).resolve()
    verification_bound = (
        verification_path is not None
        and declared_verification == verification_path.resolve()
        and str(payload.get("post_write_verification_sha256") or "")
        == _artifact_sha256(verification_path)
    )
    recomputed: dict[str, Any] = {}
    if verification_bound:
        try:
            recomputed = build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
                post_write_verification_json=verification_path
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
            recomputed = {}
    payload_without_time = {key: value for key, value in payload.items() if key != "generated_at"}
    recomputed_without_time = {
        key: value for key, value in recomputed.items() if key != "generated_at"
    }
    exact = bool(recomputed) and payload_without_time == recomputed_without_time
    ready = (
        payload.get("schema") == OFFLINE_STATE_ADVANCE_CONSUMER_SCHEMA
        and source_status == OFFLINE_STATE_ADVANCE_CONSUMER_READY_STATUS
        and payload.get("resume_state_consumer_readiness_ready") is True
        and int(payload.get("blocker_count") or 0) == 0
        and not payload.get("blocker_reasons")
        and boundary_ok
        and verification_bound
        and exact
    )
    if not payload:
        status, reason = MISSING, "offline state-advance consumer readiness missing"
    elif not boundary_ok:
        status, reason = BLOCKED, "offline state-advance consumer boundary is not read-only"
    elif not verification_bound:
        status, reason = BLOCKED, "consumer readiness is not hash-bound to the detected post-write verification"
    elif not exact:
        status, reason = BLOCKED, "consumer readiness differs from independently recomputed evidence"
    elif ready:
        status, reason = READY, "verified state may be consumed for offline planning while resume execution remains locked"
    else:
        status, reason = BLOCKED, f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="authoritative_resume_offline_state_advance_consumer_readiness",
        title="Offline-advanced resume-state consumer readiness",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Allow only offline planning to consume this state; keep CO2 route execution behind its existing explicit authorization gate.",
        physical_meaning="Separates permission to read the verified next state from permission to execute the next physical calibration step.",
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=status != READY,
    )


def _authoritative_resume_offline_state_advance_next_step_plan_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    consumer_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("plan_consumption_allowed") is True
        and payload.get("execution_supported") is False
        and payload.get("next_step_execution_allowed") is False
        and payload.get("resume_execution_allowed") is False
        and payload.get("would_execute") is False
        and all(
            payload.get(field) is False
            for field in (
                "opens_com_ports",
                "controls_pressure",
                "controls_water_or_gas_routes",
                "writes_authoritative_state",
                "writes_sn",
                "writes_device_id",
                "writes_coefficients",
                "connects_postgresql",
                "database_written",
                "formal_release_allowed",
                "database_import_allowed",
            )
        )
        and payload.get("not_real_acceptance_evidence") is True
    )
    declared_consumer = Path(
        str(payload.get("consumer_readiness_json") or "")
    ).resolve()
    consumer_bound = (
        consumer_path is not None
        and declared_consumer == consumer_path.resolve()
        and str(payload.get("consumer_readiness_sha256") or "")
        == _artifact_sha256(consumer_path)
    )
    recomputed: dict[str, Any] = {}
    if consumer_bound:
        try:
            recomputed = build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
                consumer_readiness_json=consumer_path
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
            recomputed = {}
    payload_without_time = {
        key: value for key, value in payload.items() if key != "generated_at"
    }
    recomputed_without_time = {
        key: value for key, value in recomputed.items() if key != "generated_at"
    }
    exact = bool(recomputed) and payload_without_time == recomputed_without_time
    ready = (
        payload.get("schema") == OFFLINE_STATE_ADVANCE_NEXT_STEP_PLAN_SCHEMA
        and source_status == OFFLINE_STATE_ADVANCE_NEXT_STEP_PLAN_READY_STATUS
        and payload.get("next_step_plan_review_ready") is True
        and int(payload.get("blocker_count") or 0) == 0
        and not payload.get("blocker_reasons")
        and boundary_ok
        and consumer_bound
        and exact
    )
    if not payload:
        status, reason = MISSING, "offline state-advance next-step plan missing"
    elif not boundary_ok:
        status, reason = BLOCKED, "offline next-step plan boundary is not review-only"
    elif not consumer_bound:
        status, reason = BLOCKED, "next-step plan is not hash-bound to the detected consumer readiness"
    elif not exact:
        status, reason = BLOCKED, "next-step plan differs from independently recomputed evidence"
    elif ready:
        status, reason = READY, "the exact next canonical step is reviewable while all execution remains locked"
    else:
        status, reason = BLOCKED, f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="authoritative_resume_offline_state_advance_next_step_plan",
        title="Offline-advanced resume-state next-step plan",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Review the exact next-step plan only; retain the mature route's explicit COM and route authorization boundary.",
        physical_meaning="Makes the next canonical action visible without turning verified state consumption into physical execution authority.",
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=status != READY,
    )


def _authoritative_resume_offline_state_advance_next_step_authorization_preflight_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    next_step_plan_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("plan_review_allowed") is True
        and payload.get("execution_supported") is False
        and payload.get("next_step_execution_allowed") is False
        and payload.get("resume_execution_allowed") is False
        and payload.get("would_execute") is False
        and all(
            payload.get(field) is False
            for field in (
                "opens_com_ports",
                "controls_pressure",
                "controls_water_or_gas_routes",
                "writes_authoritative_state",
                "writes_sn",
                "writes_device_id",
                "writes_coefficients",
                "connects_postgresql",
                "database_written",
                "formal_release_allowed",
                "database_import_allowed",
            )
        )
        and payload.get("not_real_acceptance_evidence") is True
    )
    declared_plan = Path(str(payload.get("next_step_plan_json") or "")).resolve()
    plan_bound = (
        next_step_plan_path is not None
        and declared_plan == next_step_plan_path.resolve()
        and str(payload.get("next_step_plan_sha256") or "")
        == _artifact_sha256(next_step_plan_path)
    )
    recomputed: dict[str, Any] = {}
    if plan_bound:
        try:
            recomputed = build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight(
                next_step_plan_json=next_step_plan_path,
                authorization_packet_json=payload.get("authorization_packet_json"),
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
            recomputed = {}
    payload_without_time = {
        key: value for key, value in payload.items() if key != "generated_at"
    }
    recomputed_without_time = {
        key: value for key, value in recomputed.items() if key != "generated_at"
    }
    exact = bool(recomputed) and payload_without_time == recomputed_without_time
    ready = (
        payload.get("schema") == OFFLINE_STATE_ADVANCE_NEXT_STEP_AUTHORIZATION_SCHEMA
        and source_status == OFFLINE_STATE_ADVANCE_NEXT_STEP_AUTHORIZATION_READY_STATUS
        and payload.get("next_step_authorization_preflight_ready") is True
        and payload.get("authorization_packet_validated_offline") is True
        and int(payload.get("review_required_count") or 0) == 0
        and not payload.get("review_reasons")
        and boundary_ok
        and plan_bound
        and exact
    )
    if not payload:
        status, reason = MISSING, "offline next-step authorization preflight missing"
    elif not boundary_ok:
        status, reason = BLOCKED, "next-step authorization preflight boundary is not review-only"
    elif not plan_bound:
        status, reason = BLOCKED, "authorization preflight is not hash-bound to the detected next-step plan"
    elif not exact:
        status, reason = BLOCKED, "authorization preflight differs from independently recomputed evidence"
    elif ready:
        status, reason = READY, "three-party review authorization is bound to the exact plan while execution stays locked"
    else:
        status, reason = BLOCKED, f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="authoritative_resume_offline_state_advance_next_step_authorization_preflight",
        title="Offline next-step review authorization preflight",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Review the exact plan only; a future executor must obtain and revalidate separate physical execution authority.",
        physical_meaning="Records accountable human review of one exact next-step plan without granting COM, pressure, route, write, release, or import authority.",
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=status != READY,
    )


def _authoritative_resume_offline_state_advance_next_step_blocked_executor_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    authorization_preflight_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("blocked_executor_ready") is True
        and payload.get("plan_review_allowed") is True
        and payload.get("execution_supported") is False
        and payload.get("next_step_execution_allowed") is False
        and payload.get("resume_execution_allowed") is False
        and payload.get("execute_flag_allowed") is False
        and payload.get("would_execute") is False
        and all(
            payload.get(field) is False
            for field in (
                "opens_com_ports",
                "controls_pressure",
                "controls_water_or_gas_routes",
                "writes_authoritative_state",
                "writes_sn",
                "writes_device_id",
                "writes_coefficients",
                "connects_postgresql",
                "database_written",
                "formal_release_allowed",
                "database_import_allowed",
            )
        )
        and payload.get("not_real_acceptance_evidence") is True
    )
    declared_preflight = Path(
        str(payload.get("next_step_authorization_preflight_json") or "")
    ).resolve()
    preflight_bound = (
        authorization_preflight_path is not None
        and declared_preflight == authorization_preflight_path.resolve()
        and str(payload.get("next_step_authorization_preflight_sha256") or "")
        == _artifact_sha256(authorization_preflight_path)
    )
    recomputed: dict[str, Any] = {}
    if preflight_bound:
        try:
            recomputed = build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor(
                next_step_authorization_preflight_json=authorization_preflight_path
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
            recomputed = {}
    exact = (
        bool(recomputed)
        and {key: value for key, value in payload.items() if key != "generated_at"}
        == {key: value for key, value in recomputed.items() if key != "generated_at"}
    )
    ready = (
        payload.get("schema") == OFFLINE_STATE_ADVANCE_NEXT_STEP_BLOCKED_EXECUTOR_SCHEMA
        and source_status == OFFLINE_STATE_ADVANCE_NEXT_STEP_BLOCKED_EXECUTOR_READY_STATUS
        and int(payload.get("review_required_count") or 0) == 0
        and not payload.get("review_reasons")
        and boundary_ok
        and preflight_bound
        and exact
    )
    if not payload:
        status, reason = MISSING, "offline next-step blocked executor evidence missing"
    elif not boundary_ok:
        status, reason = BLOCKED, "next-step blocked executor boundary is not locked"
    elif not preflight_bound:
        status, reason = BLOCKED, "blocked executor is not hash-bound to the detected authorization preflight"
    elif not exact:
        status, reason = BLOCKED, "blocked executor differs from independently recomputed lock evidence"
    elif ready:
        status, reason = READY, "next-step execution remains unavailable after fresh authorization revalidation"
    else:
        status, reason = BLOCKED, f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="authoritative_resume_offline_state_advance_next_step_blocked_executor",
        title="Offline next-step blocked executor",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Keep execution unavailable; a future separately reviewed executor must revalidate authorization immediately before any physical action.",
        physical_meaning="Proves that reviewing the next mature V1.5 step cannot open COM, control pressure or routes, write devices, or import data.",
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=status != READY,
    )


def _authoritative_resume_offline_state_advance_next_step_controlled_design_gate(
    path: Path | None,
    payload: Mapping[str, Any],
    blocked_executor_path: Path | None,
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("production_state") == "blocked_design_only"
        and payload.get("controlled_next_step_executor_design_ready") is True
        and payload.get("single_exact_command_only") is True
        and payload.get("shell_execution_allowed") is False
        and payload.get("automatic_retry_allowed") is False
        and payload.get("fallback_entry_allowed") is False
        and payload.get("automatic_state_advance_allowed") is False
        and payload.get("execution_supported") is False
        and payload.get("next_step_execution_allowed") is False
        and payload.get("resume_execution_allowed") is False
        and payload.get("execute_flag_allowed") is False
        and payload.get("would_execute") is False
        and all(
            payload.get(field) is False
            for field in (
                "opens_com_ports",
                "controls_pressure",
                "controls_water_or_gas_routes",
                "writes_authoritative_state",
                "writes_sn",
                "writes_device_id",
                "writes_coefficients",
                "connects_postgresql",
                "database_written",
                "formal_release_allowed",
                "database_import_allowed",
            )
        )
        and payload.get("not_real_acceptance_evidence") is True
    )
    declared_blocked = Path(
        str(payload.get("next_step_blocked_executor_json") or "")
    ).resolve()
    blocked_bound = (
        blocked_executor_path is not None
        and declared_blocked == blocked_executor_path.resolve()
        and str(payload.get("next_step_blocked_executor_sha256") or "")
        == _artifact_sha256(blocked_executor_path)
    )
    recomputed: dict[str, Any] = {}
    if blocked_bound:
        try:
            recomputed_model = build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design(
                next_step_blocked_executor_json=blocked_executor_path
            )
            recomputed = dict(recomputed_model.get("manifest") or {})
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
            recomputed = {}
    exact = (
        bool(recomputed)
        and {key: value for key, value in payload.items() if key != "generated_at"}
        == {key: value for key, value in recomputed.items() if key != "generated_at"}
    )
    ready = (
        payload.get("schema") == OFFLINE_STATE_ADVANCE_NEXT_STEP_CONTROLLED_DESIGN_SCHEMA
        and source_status == OFFLINE_STATE_ADVANCE_NEXT_STEP_CONTROLLED_DESIGN_READY_STATUS
        and int(payload.get("review_required_count") or 0) == 0
        and not payload.get("review_reasons")
        and boundary_ok
        and blocked_bound
        and exact
    )
    if not payload:
        status, reason = MISSING, "offline next-step controlled executor design missing"
    elif not boundary_ok:
        status, reason = BLOCKED, "controlled executor design boundary is not locked"
    elif not blocked_bound:
        status, reason = BLOCKED, "controlled design is not hash-bound to the detected blocked executor"
    elif not exact:
        status, reason = BLOCKED, "controlled design differs from independently recomputed evidence"
    elif ready:
        status, reason = READY, "controlled executor design is reviewable while all execution paths remain unavailable"
    else:
        status, reason = BLOCKED, f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="authoritative_resume_offline_state_advance_next_step_controlled_executor_design",
        title="Offline next-step controlled executor design",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action="Keep execution locked; implement a separate last-moment authorization/preflight validator before any physical executor.",
        physical_meaning="Freezes exact mature-command, least-privilege, failure-hold, and output-evidence contracts without changing route physics.",
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=status != READY,
    )


def _pressure_s9_readiness_index_gate(path: Path | None, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("connects_postgresql") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("writes_senco9") is False
        and payload.get("formal_release_allowed") is False
        and payload.get("database_import_allowed") is False
        and payload.get("database_written") is False
        and payload.get("not_real_acceptance_evidence") is True
    )
    ready = (
        payload.get("ready_for_mature_open_flow_pressure_s9_index") is True
        and source_status == "ready_for_mature_open_flow_pressure_s9_index"
    )
    if not payload:
        status = MISSING
        reason = "pressure/S9 readiness index sidecar missing"
    elif not boundary_ok:
        status = BLOCKED
        reason = "pressure/S9 readiness index boundary is not clean"
    elif ready:
        status = READY
        reason = "pressure/S9 readiness index is ready for mature open-flow"
    elif "blocked" in source_status:
        status = BLOCKED
        reason = f"source_status={source_status}"
    else:
        status = REVIEW_REQUIRED
        review_reasons = payload.get("review_reasons")
        if isinstance(review_reasons, list) and review_reasons:
            reason = "; ".join(str(item) for item in review_reasons[:3])
        else:
            reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="pressure_senco9_pre_open_flow",
        title="Pressure/SENCO9 pre-open-flow check",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Complete the pressure/S9 readiness index with per-device no-write fit basis, "
            "SENCO9 readback, and post-write pressure-only reverify before gas flow."
        ),
        physical_meaning=(
            "Pressure P must be traceable before CO2/H2O fitting so gas coefficients do not "
            "absorb pressure bias. The index separates default offset-only S9 from explicit "
            "linear-S9 controlled exceptions."
        ),
        blocks_physical_flow=status in {MISSING, REVIEW_REQUIRED, BLOCKED},
    )


def _route_physical_recovery_gate(path: Path, payload: Mapping[str, Any]) -> FormalRunGate:
    manifest = _manifest_payload(payload)
    source_status = _source_status(manifest)
    blocker_count = int(manifest.get("blocker_count") or 0)
    review_required_count = int(manifest.get("review_required_count") or 0)
    next_run_allowed = manifest.get("next_continuous_run_allowed") is True
    no_write_boundary = (
        manifest.get("opens_com_ports") is False
        and manifest.get("connects_postgresql") is False
        and manifest.get("controls_pressure") is False
        and manifest.get("controls_water_or_gas_routes") is False
        and manifest.get("writes_coefficients") is False
        and manifest.get("writes_sn_or_device_code") is False
        and manifest.get("formal_release_allowed") is False
        and manifest.get("database_import_allowed") is False
        and manifest.get("not_real_acceptance_evidence") is True
    )
    if not no_write_boundary:
        status = BLOCKED
        reason = "route physical recovery readiness sidecar boundary is not clean"
        blocks_physical = True
    elif source_status == "pass" and blocker_count == 0 and next_run_allowed:
        status = READY
        reason = "route physical recovery evidence and fresh canonical next-run policy are ready"
        blocks_physical = False
    elif blocker_count:
        status = BLOCKED
        reason = (
            f"route physical blockers remain: blocker_count={blocker_count}; "
            "PACE vent, pressure gauge, dry-gas dewpoint, or fresh queue policy is not recovered"
        )
        blocks_physical = True
    elif review_required_count:
        status = REVIEW_REQUIRED
        reason = (
            f"route physical recovery has review_required_count={review_required_count}; "
            "segmented/direct/retry evidence still needs accepted-manifest review"
        )
        blocks_physical = not next_run_allowed
    else:
        status = REVIEW_REQUIRED
        reason = f"route physical recovery source_status={source_status or 'missing'} requires review"
        blocks_physical = not next_run_allowed
    return _gate(
        gate_id="route_physical_recovery_readiness",
        title="Route physical recovery readiness",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Recover PACE vent, pressure-gauge readback, and dry-gas dewpoint stability; then bind "
            "the next run to a fresh 0613/0620/0621 canonical queue before starting continuous CO2/H2O."
        ),
        physical_meaning=(
            "Prevents PACE vent NO_RESPONSE, pressure-gauge NO_RESPONSE, dry-gas dewpoint rebound, "
            "stale running manifests, and direct/retry/manual segments from being treated as a valid next continuous run."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=blocks_physical,
    )


def _run_stage_is_pass(run_status: Mapping[str, Any], stage_id: str) -> bool:
    return _stage_status(run_status, stage_id).strip().lower() == "pass"


def _needs_mature_route_continuity_gate(run_status: Mapping[str, Any]) -> bool:
    return all(
        _run_stage_is_pass(run_status, stage_id)
        for stage_id in ("co2_open_flow", "h2o_open_flow", "candidate_review")
    )


def _mature_route_continuity_gate(
    path: Path | None,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    manifest = _manifest_payload(payload)
    source_status = _source_status(manifest)
    blocker_count = int(manifest.get("blocker_count") or 0)
    review_required_count = int(manifest.get("review_required_count") or 0)
    fit_eligible = manifest.get("continuous_route_run_fit_eligible") is True
    no_write_boundary = (
        manifest.get("opens_com_ports") is False
        and manifest.get("connects_postgresql") is False
        and manifest.get("controls_pressure") is False
        and manifest.get("controls_water_or_gas_routes") is False
        and manifest.get("writes_coefficients") is False
        and manifest.get("writes_sn_or_device_code") is False
        and manifest.get("formal_release_allowed") is False
        and manifest.get("database_import_allowed") is False
        and manifest.get("not_real_acceptance_evidence") is True
    )
    route_kind = str(manifest.get("route_kind") or "co2/h2o").upper()
    if not payload:
        status = REVIEW_REQUIRED
        reason = "mature route continuity gate missing after CO2/H2O/candidate evidence reached pass"
    elif not no_write_boundary:
        status = BLOCKED
        reason = "mature route continuity gate sidecar boundary is not clean"
    elif source_status == "pass" and blocker_count == 0 and review_required_count == 0 and fit_eligible:
        status = READY
        reason = f"{route_kind} mature route continuity gate passed and fit eligibility is explicit"
    elif blocker_count or source_status == "blocked":
        status = BLOCKED
        reason = (
            f"{route_kind} mature route continuity blockers remain: "
            f"source_status={source_status or 'missing'}, blocker_count={blocker_count}"
        )
    else:
        status = REVIEW_REQUIRED
        reason = (
            f"{route_kind} mature route continuity requires review: "
            f"source_status={source_status or 'missing'}, review_required_count={review_required_count}, "
            f"fit_eligible={fit_eligible}"
        )
    return _gate(
        gate_id="mature_route_continuity_gate",
        title="Mature route continuity gate",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Attach a passing mature route continuity gate before using CO2/H2O route evidence for "
            "formal fit-input review, coefficient writes, archive release, or database import."
        ),
        physical_meaning=(
            "Only a fresh, complete, continuous 0613/0620/0621 mature route manifest may feed formal "
            "fitting. Segmented, retry, direct-recovery, 0624/migration, diagnostic, worker, empty, "
            "running, or failed evidence remains diagnostic/recovery evidence."
        ),
        release_gate=True,
        blocks_physical_flow=False,
    )


def _formal_initialization_controlled_executor_design_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("live_execution_allowed") is False
        and payload.get("execution_supported") is False
    )
    if source_status == "ready_for_controlled_initialization_executor_design_review" and boundary_ok:
        status = READY
        reason = "controlled initialization executor design is ready; live initialization remains blocked"
    elif not boundary_ok:
        status = BLOCKED
        reason = "controlled initialization design boundary is not clean; no-COM/no-write locks are not preserved"
    elif source_status == "review_required" or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = (
            f"controlled initialization design review_required_count={payload.get('review_required_count')}; "
            "review blocked-executor linkage before future live initialization design is accepted"
        )
    else:
        status = REVIEW_REQUIRED
        reason = f"controlled initialization design source_status={source_status or 'missing'} requires review"
    return _gate(
        gate_id="formal_initialization_controlled_executor_design",
        title="Controlled initialization executor design",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Use this design only as future implementation guidance. Do not open COM or write analyzer state "
            "until a separate controlled executor adds explicit authorization, readback, and hold evidence."
        ),
        physical_meaning=(
            "Defines the future live-initialization safety contract while preserving the current no-COM, "
            "no-SN-write, no-SENCO-write V1.5 boundary."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_initialization_readonly_com_preflight_design_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("live_execution_allowed") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("execution_supported") is False
    )
    if source_status == "ready_for_readonly_real_com_preflight_design_review" and boundary_ok:
        status = READY
        reason = "read-only real-COM preflight design is ready; real COM remains locked"
    elif not boundary_ok:
        status = BLOCKED
        reason = "read-only real-COM preflight design boundary is not clean; no-COM/no-write locks are not preserved"
    elif source_status == "review_required" or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = (
            f"read-only real-COM preflight design review_required_count={payload.get('review_required_count')}; "
            "review controlled-design linkage before future read-only COM preflight is accepted"
        )
    else:
        status = REVIEW_REQUIRED
        reason = f"read-only real-COM preflight design source_status={source_status or 'missing'} requires review"
    return _gate(
        gate_id="formal_initialization_readonly_com_preflight_design",
        title="Read-only initialization COM preflight design",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Use this design only as future preflight implementation guidance. Do not open COM until a "
            "separate controlled read-only tool adds explicit authorization, port inventory, pacing, identity, "
            "GETCO, CHECK, and hold evidence."
        ),
        physical_meaning=(
            "Defines the future read-only analyzer-contact safety contract while preserving the current no-COM, "
            "no-SN-write, no-SENCO-write V1.5 boundary."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_initialization_readonly_com_preflight_blocked_executor_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    side_effect_lock_clean = (
        payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("live_execution_allowed") is False
        and payload.get("execution_supported") is False
    )
    expected_block = (
        source_status == "blocked_pending_readonly_real_com_preflight_implementation"
        and payload.get("blocked_executor_ready") is True
        and payload.get("execution_supported") is False
        and side_effect_lock_clean
    )
    if expected_block:
        status = REVIEW_REQUIRED
        reason = (
            "blocked read-only COM preflight stub consumed the design contract and correctly refused analyzer contact"
        )
    elif not side_effect_lock_clean:
        status = BLOCKED
        reason = "read-only COM preflight blocked executor boundary is not clean; COM/write side effects are not locked off"
    elif source_status == "review_required" or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = (
            f"read-only COM preflight blocked executor input review_required_count={payload.get('review_required_count')}; "
            "regenerate design/input references before executor review"
        )
    else:
        status = BLOCKED
        reason = (
            f"read-only COM preflight blocked executor source_status={source_status or 'missing'} "
            "is not the expected locked stub status"
        )
    return _gate(
        gate_id="formal_initialization_readonly_com_preflight_blocked_executor",
        title="Read-only initialization COM preflight blocked executor",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Keep analyzer COM contact locked. Build a separate controlled read-only preflight with explicit "
            "authorization, reviewed ports, >=1s pacing, identity/GETCO/CHECK reads, and hold evidence before any COM opens."
        ),
        physical_meaning=(
            "Proves the future read-only COM preflight command currently consumes reviewed inputs but remains a "
            "no-COM, no-write stub rather than analyzer contact."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_initialization_readonly_com_preflight_controlled_executor_design_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("controlled_write_execution_allowed") is False
        and payload.get("live_execution_allowed") is False
        and payload.get("execution_supported") is False
    )
    if source_status == "ready_for_readonly_com_preflight_controlled_executor_design_review" and boundary_ok:
        status = READY
        reason = "controlled read-only COM preflight executor design is ready; real COM remains locked"
    elif not boundary_ok:
        status = BLOCKED
        reason = (
            "controlled read-only COM preflight executor design boundary is not clean; "
            "COM/write/database/route locks are not preserved"
        )
    elif source_status == "review_required" or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = (
            f"controlled read-only COM preflight executor design review_required_count="
            f"{payload.get('review_required_count')}; review blocked-executor linkage before accepting"
        )
    else:
        status = REVIEW_REQUIRED
        reason = (
            "controlled read-only COM preflight executor design "
            f"source_status={source_status or 'missing'} requires review"
        )
    return _gate(
        gate_id="formal_initialization_readonly_com_preflight_controlled_executor_design",
        title="Controlled read-only initialization COM preflight executor design",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Use this design only as future implementation guidance. Do not open analyzer COM until a separate "
            "controlled read-only executor adds explicit authorization, reviewed ports, >=1s pacing, "
            "identity/SN/GETCO/CHECK evidence, and hold records."
        ),
        physical_meaning=(
            "Defines the future controlled read-only analyzer-contact contract while preserving the current "
            "no-COM, no-SN-write, no-SENCO-write, no-database, no-route V1.5 boundary."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_initialization_readonly_com_preflight_controlled_blocked_executor_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("controlled_write_execution_allowed") is False
        and payload.get("live_execution_allowed") is False
        and payload.get("execution_supported") is False
    )
    if source_status == "blocked_pending_controlled_readonly_com_preflight_executor_implementation" and boundary_ok:
        status = READY
        reason = "controlled read-only COM preflight blocked executor is locked; real COM remains disabled"
    elif not boundary_ok:
        status = BLOCKED
        reason = (
            "controlled read-only COM preflight blocked executor boundary is not clean; "
            "COM/write/database/route locks are not preserved"
        )
    elif source_status == "review_required" or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = (
            "controlled read-only COM preflight blocked executor review_required_count="
            f"{payload.get('review_required_count')}; review controlled design linkage before accepting"
        )
    else:
        status = REVIEW_REQUIRED
        reason = (
            "controlled read-only COM preflight blocked executor "
            f"source_status={source_status or 'missing'} requires review"
        )
    return _gate(
        gate_id="formal_initialization_readonly_com_preflight_controlled_blocked_executor",
        title="Controlled read-only initialization COM preflight blocked executor",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Keep analyzer COM locked. A later controlled read-only executor must explicitly implement "
            "authorization, reviewed ports, >=1s pacing, identity/SN/GETCO/CHECK reads, and hold records."
        ),
        physical_meaning=(
            "Proves the future controlled read-only analyzer-contact executor still has no current "
            "COM, no-SN-write, no-SENCO-write, no-database, no-route execution path."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_readonly_com_execution_contract_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("controlled_write_execution_allowed") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
    )
    if (
        source_status == "ready_for_readonly_com_execution_contract_review"
        and payload.get("contract_ready") is True
        and boundary_ok
    ):
        status = READY
        reason = "future read-only COM execution packet contract is reviewed while live COM remains disabled"
    elif not boundary_ok:
        status = BLOCKED
        reason = "read-only COM execution contract boundary locks are not preserved"
    elif "review" in source_status or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="formal_readonly_com_execution_contract",
        title="Read-only COM execution packet contract",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Review the execution packet contract before implementing any real read-only COM executor. "
            "Do not treat this sidecar as live execution authorization."
        ),
        physical_meaning=(
            "Defines the future operator/reviewer authorization, active analyzer list, reviewed COM port inventory, "
            "1s serial pacing, old-algorithm CHECK skip, and no-write/no-route/no-database boundaries."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_readonly_com_execution_blocked_executor_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("controlled_write_execution_allowed") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
    )
    if (
        source_status == "blocked_pending_readonly_com_real_executor_implementation"
        and payload.get("blocked_executor_ready") is True
        and boundary_ok
    ):
        status = READY
        reason = "future read-only COM executor is proven blocked while live COM remains disabled"
    elif not boundary_ok:
        status = BLOCKED
        reason = "read-only COM execution blocked executor boundary locks are not preserved"
    elif "review" in source_status or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="formal_readonly_com_execution_blocked_executor",
        title="Read-only COM execution blocked executor",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Keep analyzer COM locked. A later real read-only executor must explicitly implement authorization, "
            "reviewed ports, >=1s pacing, identity/SN/GETCO/runtime/CHECK reads, and hold records."
        ),
        physical_meaning=(
            "Proves the future real read-only analyzer-contact executor still has no current "
            "COM, no-SN-write, no-SENCO-write, no-database, no-route execution path."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_readonly_com_execution_packet_validator_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("controlled_write_execution_allowed") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
    )
    if (
        source_status
        in {
            "blocked_pending_readonly_com_execution_authorization_packet",
            "ready_for_readonly_com_execution_packet_review",
        }
        and payload.get("packet_validator_ready") is True
        and boundary_ok
    ):
        status = READY
        reason = "read-only COM execution packet validator is ready while live COM remains disabled"
    elif not boundary_ok:
        status = BLOCKED
        reason = "read-only COM execution packet validator boundary locks are not preserved"
    elif "review" in source_status or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="formal_readonly_com_execution_packet_validator",
        title="Read-only COM execution packet validator",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Review packet-validator evidence before any future real read-only COM executor. "
            "A valid packet is still not live execution authorization in this package."
        ),
        physical_meaning=(
            "Checks the future authorization packet, reviewed port inventory, active analyzer list, "
            "1s pacing, SN/device_code, new-algorithm CHECK requirement, old-algorithm CHECK skip, "
            "and no-write/no-route/no-database boundaries without opening COM."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_readonly_com_execution_plan_preview_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("controlled_write_execution_allowed") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("does_not_execute_commands") is True
    )
    if (
        source_status
        in {
            "blocked_pending_validated_readonly_com_execution_packet",
            "ready_for_readonly_com_execution_plan_preview_review",
        }
        and boundary_ok
    ):
        status = READY
        reason = "read-only COM execution plan preview is offline while live COM remains disabled"
    elif not boundary_ok:
        status = BLOCKED
        reason = "read-only COM execution plan preview boundary locks are not preserved"
    elif "review" in source_status or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="formal_readonly_com_execution_plan_preview",
        title="Read-only COM execution plan preview",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Review the future read order before any separate real read-only COM executor implementation. "
            "The preview itself is not live execution authorization."
        ),
        physical_meaning=(
            "Renders the future identity, SN/device_code, GETCO1-9, runtime, and CHECK read order while "
            "keeping old-algorithm CHECK skipped and analyzer COM closed."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_readonly_com_minimal_executor_review_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("controlled_write_execution_allowed") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("execution_supported") is False
        and payload.get("minimal_executor_review_ready") is True
    )
    if source_status == "blocked_pending_minimal_readonly_com_executor_implementation" and boundary_ok:
        status = READY
        reason = "minimal read-only COM executor review is blocked-by-default while output and hold contracts are defined"
    elif not boundary_ok:
        status = BLOCKED
        reason = "minimal read-only COM executor review boundary locks are not preserved"
    elif "review" in source_status or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="formal_readonly_com_minimal_executor_review",
        title="Read-only COM minimal executor review",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Use this review as the implementation checklist for a later real read-only COM executor. "
            "It does not authorize analyzer contact."
        ),
        physical_meaning=(
            "Freezes the minimum future output evidence and failure-hold matrix for read-only analyzer contact "
            "while keeping COM closed and all write/database/route side effects locked."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_readonly_com_minimal_executor_stub_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    boundary_ok = (
        payload.get("opens_com_ports") is False
        and payload.get("read_only_real_com_execution_allowed") is False
        and payload.get("controlled_write_execution_allowed") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("database_written") is False
        and payload.get("execution_supported") is False
        and payload.get("live_execution_allowed") is False
        and payload.get("authorization_context_consumed_as_unlock") is False
    )
    if (
        source_status == "blocked_plan_only_minimal_readonly_com_executor_stub"
        and payload.get("minimal_executor_stub_ready") is True
        and payload.get("would_execute_artifact_ready") is True
        and boundary_ok
    ):
        status = READY
        reason = "minimal read-only COM executor stub recorded would-execute evidence while COM remains locked"
    elif not boundary_ok:
        status = BLOCKED
        reason = "minimal read-only COM executor stub boundary locks are not preserved"
    elif "review" in source_status or int(payload.get("review_required_count") or 0):
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    else:
        status = REVIEW_REQUIRED
        reason = f"source_status={source_status or 'unknown'}"
    return _gate(
        gate_id="formal_readonly_com_minimal_executor_stub",
        title="Read-only COM minimal executor stub",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Review the would-execute trace before any later PR implements real analyzer contact. "
            "This stub is not live COM authorization."
        ),
        physical_meaning=(
            "Records the future minimal read-only COM executor shape as plan-only evidence while keeping "
            "serial ports closed and all SN, SENCO, database, pressure, and route side effects locked."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=False,
    )


def _formal_readonly_com_minimal_executor_gate(
    path: Path,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    no_write_boundary = (
        payload.get("connects_postgresql") is False
        and payload.get("controls_pressure") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_sn") is False
        and payload.get("writes_device_id") is False
        and payload.get("writes_coefficients") is False
        and payload.get("database_written") is False
        and payload.get("formal_release_allowed") is False
        and payload.get("database_import_allowed") is False
        and payload.get("not_real_acceptance_evidence") is True
    )
    if not no_write_boundary:
        status = BLOCKED
        reason = "minimal read-only COM executor boundary is not clean; write/import/route locks are not preserved"
        blocks_physical = True
    elif source_status == "readonly_com_minimal_executor_completed_no_write" and int(payload.get("hold_count") or 0) == 0:
        status = READY
        reason = "minimal read-only COM executor completed identity/SN/GETCO/runtime/CHECK reads without writes"
        blocks_physical = False
    elif source_status == "readonly_com_minimal_executor_hold" or int(payload.get("hold_count") or 0):
        status = REVIEW_REQUIRED
        reason = f"minimal read-only COM executor hold_count={payload.get('hold_count')}"
        blocks_physical = True
    elif source_status == "blocked_missing_execute_readonly_real_com":
        status = REVIEW_REQUIRED
        reason = "minimal read-only COM executor artifact exists but real read-only execution was not requested"
        blocks_physical = False
    else:
        status = REVIEW_REQUIRED
        reason = f"minimal read-only COM executor source_status={source_status or 'missing'} requires review"
        blocks_physical = False
    return _gate(
        gate_id="formal_readonly_com_minimal_executor",
        title="Minimal read-only COM executor",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Review read-only command attempts, raw responses, hold events, and identity/GETCO snapshot. "
            "Do not treat this as SENCO write, release, database import, pressure, or route-control evidence."
        ),
        physical_meaning=(
            "Reads reviewed analyzer COM ports for protocol/SN/GETCO/runtime/CHECK initialization evidence while "
            "preserving no-write, no-database, no-pressure, and no-route boundaries."
        ),
        release_gate=False,
        blocks_release=False,
        blocks_physical_flow=blocks_physical,
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


def _senco_artifact_authorization_gate(
    path: Path | None,
    payload: Mapping[str, Any],
) -> FormalRunGate:
    source_status = _source_status(payload)
    reasons: list[str] = []
    if not payload or path is None:
        status = MISSING
        reason = "main SENCO artifact authorization is missing"
    else:
        manifest_path = str(payload.get("manifest_path") or "").strip()
        reviewer = str(payload.get("reviewer") or "").strip()
        approver = str(payload.get("approver") or "").strip()
        scopes = [
            str(item).strip()
            for item in payload.get("authorized_writer_scopes") or []
            if str(item).strip()
        ]
        device_ids = [
            str(item).strip()
            for item in payload.get("authorized_device_ids") or []
            if str(item).strip()
        ]
        if not manifest_path:
            reasons.append("senco_artifact_authorization_manifest_path_missing")
        else:
            for scope in scopes or [""]:
                valid, validation_reasons, _ = validate_senco_artifact_authorization(
                    path,
                    manifest_path=manifest_path,
                    reviewer=reviewer,
                    approver=approver,
                    writer_scope=scope,
                    device_ids=device_ids,
                )
                if not valid:
                    reasons.extend(validation_reasons)
        reasons = list(dict.fromkeys(reasons))
        if reasons:
            status = BLOCKED
            reason = "; ".join(reasons[:4])
        else:
            status = READY
            reason = (
                "final SENCO manifest hash, writer scopes, device IDs, reviewer, and approver are bound"
            )
    return _gate(
        gate_id="senco_artifact_authorization",
        title="Main SENCO artifact authorization",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Generate or refresh the main SENCO precheck pack with explicit reviewer, distinct approver, "
            "authorization ID, writer scopes, and exact authorized device IDs before controlled writes."
        ),
        physical_meaning=(
            "Binds the exact final coefficient artifact hash and analyzer set to reviewed write intent. "
            "Missing authorization blocks controlled writes and formal release, but does not block mature sampling."
        ),
        blocks_physical_flow=False,
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
    senco_binding = archive.get("senco_authorization_write_traceability")
    senco_binding_ready = False
    senco_binding_status = "missing"
    if isinstance(senco_binding, Mapping):
        senco_binding_ready = bool(senco_binding.get("ready_for_archive_release"))
        senco_binding_status = str(senco_binding.get("overall_status") or "missing")

    if not closure and not archive:
        status = MISSING
        reason = "closure readiness and formal archive closure sidecars missing"
    elif "blocked" in closure_status or "blocked" in archive_status:
        status = BLOCKED
        reason = f"closure_status={closure_status or 'missing'} archive_status={archive_status or 'missing'}"
    elif senco_binding_status == "blocked":
        status = BLOCKED
        reason = "controlled SENCO write authorization/readback archive binding is blocked"
    elif closure_status == "ready_for_formal_release" and not archive:
        status = MISSING
        reason = "closure is ready, but formal archive closure index is missing"
    elif (
        closure_status == "ready_for_formal_release"
        and traceability_ready
        and not traceability_review
        and senco_binding_ready
    ):
        status = READY
        reason = "closure release, identity traceability, and SENCO authorization/write binding gates are ready"
    elif closure_status == "ready_for_formal_release" and traceability_review:
        status = REVIEW_REQUIRED
        reason = "closure is ready, but archive SN/GETCO traceability still requires review"
    elif closure_status == "ready_for_formal_release" and not senco_binding_ready:
        status = REVIEW_REQUIRED
        reason = (
            "closure is ready, but controlled SENCO write authorization/readback archive binding "
            f"is {senco_binding_status}"
        )
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


def _full_flow_automation_closure_gate(path: Path, payload: Mapping[str, Any]) -> FormalRunGate:
    source_status = _source_status(payload)
    automation_status = str(payload.get("automation_closure_status") or "")
    blocker_count = int(payload.get("blocker_count") or 0)
    remaining_gap_count = int(payload.get("remaining_full_auto_gap_count") or 0)
    legacy_counts_ok = payload.get("legacy_point_counts") == {"co2": 45, "h2o": 13}
    new_algorithm_counts_ok = payload.get("new_algorithm_profile_point_counts") == {"co2": 47, "h2o": 14}
    baseline_ok = (
        "0613" in str(payload.get("mature_fitting_baseline") or "")
        and "0620/0621" in str(payload.get("mature_route_baseline") or "")
    )
    boundary_ok = (
        payload.get("full_production_auto_allowed") is False
        and payload.get("formal_release_allowed") is False
        and payload.get("database_import_allowed") is False
        and payload.get("opens_com_ports") is False
        and payload.get("connects_postgresql") is False
        and payload.get("controls_water_or_gas_routes") is False
        and payload.get("writes_coefficients") is False
        and payload.get("not_real_acceptance_evidence") is True
    )
    if (
        source_status == "review_ready"
        and automation_status == "structure_closed_live_full_auto_still_gated"
        and blocker_count == 0
        and remaining_gap_count > 0
        and baseline_ok
        and legacy_counts_ok
        and new_algorithm_counts_ok
        and boundary_ok
    ):
        status = READY
        reason = (
            "V1.5 structure is organized and mature baselines are locked; "
            f"full production automation still has {remaining_gap_count} gated handoff(s)"
        )
    else:
        status = REVIEW_REQUIRED
        reasons: list[str] = []
        if source_status != "review_ready":
            reasons.append(f"source_status={source_status or 'missing'}")
        if automation_status != "structure_closed_live_full_auto_still_gated":
            reasons.append(f"automation_closure_status={automation_status or 'missing'}")
        if blocker_count:
            reasons.append(f"blocker_count={blocker_count}")
        if remaining_gap_count <= 0:
            reasons.append(f"remaining_full_auto_gap_count={remaining_gap_count}")
        if not baseline_ok:
            reasons.append("mature_baseline_not_0613_0620_0621")
        if not legacy_counts_ok:
            reasons.append(f"legacy_point_counts={payload.get('legacy_point_counts')!r}")
        if not new_algorithm_counts_ok:
            reasons.append(f"new_algorithm_profile_point_counts={payload.get('new_algorithm_profile_point_counts')!r}")
        if not boundary_ok:
            reasons.append("offline_boundary_not_clean")
        reason = "; ".join(reasons) or "V1.5 full-flow automation closure map requires review"
    return _gate(
        gate_id="full_flow_automation_closure",
        title="Full-flow automation closure map",
        status=status,
        source_path=path,
        source_status=source_status,
        reason=reason,
        next_action=(
            "Use this closure map to decide the next automation PR. Do not interpret it as live-route, "
            "coefficient-write, PostgreSQL import, or formal release evidence."
        ),
        physical_meaning=(
            "Summarizes the current V1.5 production automation boundary: the formal structure is organized "
            "around 0613 fitting and 0620/0621 mature physical routes, while live execution, writes, reverify, "
            "archive, and database import remain explicit gates."
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
        and payload.get("database_import_preflight_binding_ready") is True
        and payload.get("archive_release_ready") is True
        and payload.get("archive_closure_index_binding_ready") is True
        and payload.get("senco_authorization_archive_binding_ready") is True
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
        if payload.get("database_import_preflight_binding_ready") is not True:
            reasons.append("database_import_preflight_binding_ready=False")
        if payload.get("archive_release_ready") is not True:
            reasons.append("archive_release_ready=False")
        if payload.get("archive_closure_index_binding_ready") is not True:
            reasons.append("archive_closure_index_binding_ready=False")
        if payload.get("senco_authorization_archive_binding_ready") is not True:
            reasons.append("senco_authorization_archive_binding_ready=False")
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
        and payload.get("database_import_authorization_binding_ready") is True
        and payload.get("preflight_ready") is True
        and payload.get("database_import_preflight_binding_ready") is True
        and payload.get("archive_release_ready") is True
        and payload.get("archive_closure_index_binding_ready") is True
        and payload.get("senco_authorization_archive_binding_ready") is True
        and payload.get("evidence_bundle_ready") is True
        and payload.get("evidence_bundle_schema_ready") is True
        and payload.get("evidence_bundle_binding_ready") is True
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
            "database_import_authorization_binding_ready",
            "preflight_ready",
            "database_import_preflight_binding_ready",
            "archive_release_ready",
            "archive_closure_index_binding_ready",
            "senco_authorization_archive_binding_ready",
            "evidence_bundle_ready",
            "evidence_bundle_binding_ready",
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
        and payload.get("database_import_authorization_binding_ready") is True
        and payload.get("database_import_preflight_binding_ready") is True
        and payload.get("evidence_bundle_schema_ready") is True
        and payload.get("evidence_bundle_binding_ready") is True
        and payload.get("archive_closure_index_binding_ready") is True
        and payload.get("senco_authorization_archive_binding_ready") is True
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
        and payload.get("database_import_authorization_binding_ready") is True
        and payload.get("database_import_preflight_binding_ready") is True
        and payload.get("evidence_bundle_schema_ready") is True
        and payload.get("evidence_bundle_binding_ready") is True
        and payload.get("archive_closure_index_binding_ready") is True
        and payload.get("senco_authorization_archive_binding_ready") is True
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
    formal_initialization_controlled_executor_design_json: str | Path | None = None,
    formal_initialization_readonly_com_preflight_design_json: str | Path | None = None,
    formal_initialization_readonly_com_preflight_blocked_executor_json: str | Path | None = None,
    formal_initialization_readonly_com_preflight_controlled_executor_design_json: str | Path | None = None,
    formal_initialization_readonly_com_preflight_controlled_blocked_executor_json: str | Path | None = None,
    formal_readonly_com_execution_contract_json: str | Path | None = None,
    formal_readonly_com_execution_blocked_executor_json: str | Path | None = None,
    formal_readonly_com_execution_packet_validator_json: str | Path | None = None,
    formal_readonly_com_execution_plan_preview_json: str | Path | None = None,
    formal_readonly_com_minimal_executor_review_json: str | Path | None = None,
    formal_readonly_com_minimal_executor_stub_json: str | Path | None = None,
    formal_readonly_com_minimal_executor_json: str | Path | None = None,
    route_physical_recovery_readiness_json: str | Path | None = None,
    mature_route_continuity_gate_json: str | Path | None = None,
    pressure_s9_readiness_index_json: str | Path | None = None,
    pre_gas_readiness_json: str | Path | None = None,
    batch_initialization_closeout_json: str | Path | None = None,
    post_closeout_resume_gate_json: str | Path | None = None,
    resume_prefix_application_review_json: str | Path | None = None,
    authoritative_resume_state_writer_design_json: str | Path | None = None,
    authoritative_resume_state_writer_blocked_executor_json: str | Path | None = None,
    authoritative_resume_state_controlled_write_preflight_json: str | Path | None = None,
    authoritative_resume_state_atomic_write_json: str | Path | None = None,
    authoritative_resume_state_post_write_verification_json: str | Path | None = None,
    authoritative_resume_offline_state_advance_atomic_write_json: str | Path | None = None,
    authoritative_resume_offline_state_advance_post_write_verification_json: str | Path | None = None,
    authoritative_resume_offline_state_advance_consumer_readiness_json: str | Path | None = None,
    authoritative_resume_offline_state_advance_next_step_plan_json: str | Path | None = None,
    authoritative_resume_offline_state_advance_next_step_authorization_preflight_json: str | Path | None = None,
    authoritative_resume_offline_state_advance_next_step_blocked_executor_json: str | Path | None = None,
    authoritative_resume_offline_state_advance_next_step_controlled_executor_design_json: str | Path | None = None,
    getco_readiness_json: str | Path | None = None,
    run_evidence_status_json: str | Path | None = None,
    full_flow_closure_readiness_json: str | Path | None = None,
    archive_closure_json: str | Path | None = None,
    algorithm_profile_runner_dry_run_json: str | Path | None = None,
    full_flow_automation_closure_json: str | Path | None = None,
    senco_artifact_authorization_json: str | Path | None = None,
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
    formal_initialization_controlled_executor_design_path = _explicit_or_latest(
        root,
        formal_initialization_controlled_executor_design_json,
        "v1_5_formal_initialization_controlled_executor_design.json",
    )
    formal_initialization_readonly_com_preflight_design_path = _explicit_or_latest(
        root,
        formal_initialization_readonly_com_preflight_design_json,
        "v1_5_formal_initialization_readonly_com_preflight_design.json",
    )
    formal_initialization_readonly_com_preflight_blocked_executor_path = _explicit_or_latest(
        root,
        formal_initialization_readonly_com_preflight_blocked_executor_json,
        "v1_5_formal_initialization_readonly_com_preflight_blocked_executor.json",
    )
    formal_initialization_readonly_com_preflight_controlled_executor_design_path = _explicit_or_latest(
        root,
        formal_initialization_readonly_com_preflight_controlled_executor_design_json,
        "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.json",
    )
    formal_initialization_readonly_com_preflight_controlled_blocked_executor_path = _explicit_or_latest(
        root,
        formal_initialization_readonly_com_preflight_controlled_blocked_executor_json,
        "v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor.json",
    )
    formal_readonly_com_execution_contract_path = _explicit_or_latest(
        root,
        formal_readonly_com_execution_contract_json,
        "v1_5_formal_readonly_com_execution_contract.json",
    )
    formal_readonly_com_execution_blocked_executor_path = _explicit_or_latest(
        root,
        formal_readonly_com_execution_blocked_executor_json,
        "v1_5_formal_readonly_com_execution_blocked_executor.json",
    )
    formal_readonly_com_execution_packet_validator_path = _explicit_or_latest(
        root,
        formal_readonly_com_execution_packet_validator_json,
        "v1_5_formal_readonly_com_execution_packet_validator.json",
    )
    formal_readonly_com_execution_plan_preview_path = _explicit_or_latest(
        root,
        formal_readonly_com_execution_plan_preview_json,
        "v1_5_formal_readonly_com_execution_plan_preview.json",
    )
    formal_readonly_com_minimal_executor_review_path = _explicit_or_latest(
        root,
        formal_readonly_com_minimal_executor_review_json,
        "v1_5_formal_readonly_com_minimal_executor_review.json",
    )
    formal_readonly_com_minimal_executor_stub_path = _explicit_or_latest(
        root,
        formal_readonly_com_minimal_executor_stub_json,
        "v1_5_formal_readonly_com_minimal_executor_stub.json",
    )
    formal_readonly_com_minimal_executor_path = _explicit_or_latest(
        root,
        formal_readonly_com_minimal_executor_json,
        "v1_5_formal_readonly_com_minimal_executor.json",
    )
    route_physical_recovery_path = _explicit_or_latest(
        root,
        route_physical_recovery_readiness_json,
        "v1_5_route_physical_recovery_readiness.json",
    )
    mature_route_continuity_gate_path = _explicit_or_latest(
        root,
        mature_route_continuity_gate_json,
        "v1_5_mature_route_continuity_gate.json",
    )
    pressure_s9_readiness_index_path = _explicit_or_latest(
        root,
        pressure_s9_readiness_index_json,
        "v1_5_pressure_s9_readiness_index.json",
    )
    pre_gas_path = _explicit_or_latest(root, pre_gas_readiness_json, "v1_5_pre_gas_readiness.json")
    batch_initialization_closeout_path = _explicit_or_latest(
        root,
        batch_initialization_closeout_json,
        "v1_5_batch_initialization_closeout_index.json",
    )
    post_closeout_resume_gate_path = _explicit_or_latest(
        root,
        post_closeout_resume_gate_json,
        "v1_5_post_closeout_resume_gate.json",
    )
    resume_prefix_application_review_path = _explicit_or_latest(
        root,
        resume_prefix_application_review_json,
        "v1_5_resume_prefix_application_review.json",
    )
    authoritative_resume_state_writer_design_path = _explicit_or_latest(
        root,
        authoritative_resume_state_writer_design_json,
        "v1_5_authoritative_resume_state_writer_design.json",
    )
    authoritative_resume_state_writer_blocked_executor_path = _explicit_or_latest(
        root,
        authoritative_resume_state_writer_blocked_executor_json,
        "v1_5_authoritative_resume_state_writer_blocked_executor.json",
    )
    authoritative_resume_state_controlled_write_preflight_path = _explicit_or_latest(
        root,
        authoritative_resume_state_controlled_write_preflight_json,
        "v1_5_resume_state_write_preflight.json",
    )
    authoritative_resume_state_atomic_write_path = _explicit_or_latest(
        root,
        authoritative_resume_state_atomic_write_json,
        "v1_5_resume_state_atomic_write.json",
    )
    authoritative_resume_state_post_write_verification_path = _explicit_or_latest(
        root,
        authoritative_resume_state_post_write_verification_json,
        "v1_5_resume_state_post_write_verification.json",
    )
    authoritative_resume_offline_state_advance_atomic_write_path = _explicit_or_latest(
        root,
        authoritative_resume_offline_state_advance_atomic_write_json,
        "v1_5_authoritative_resume_offline_state_advance_atomic_writer.json",
    )
    authoritative_resume_offline_state_advance_post_write_verification_path = _explicit_or_latest(
        root,
        authoritative_resume_offline_state_advance_post_write_verification_json,
        "v1_5_authoritative_resume_offline_state_advance_post_write_verification.json",
    )
    authoritative_resume_offline_state_advance_consumer_readiness_path = _explicit_or_latest(
        root,
        authoritative_resume_offline_state_advance_consumer_readiness_json,
        "v1_5_authoritative_resume_offline_state_advance_consumer_readiness.json",
    )
    authoritative_resume_offline_state_advance_next_step_plan_path = _explicit_or_latest(
        root,
        authoritative_resume_offline_state_advance_next_step_plan_json,
        "v1_5_authoritative_resume_offline_state_advance_next_step_plan.json",
    )
    authoritative_resume_offline_state_advance_next_step_authorization_preflight_path = _explicit_or_latest(
        root,
        authoritative_resume_offline_state_advance_next_step_authorization_preflight_json,
        "v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight.json",
    )
    authoritative_resume_offline_state_advance_next_step_blocked_executor_path = _explicit_or_latest(
        root,
        authoritative_resume_offline_state_advance_next_step_blocked_executor_json,
        "v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor.json",
    )
    authoritative_resume_offline_state_advance_next_step_controlled_executor_design_path = _explicit_or_latest(
        root,
        authoritative_resume_offline_state_advance_next_step_controlled_executor_design_json,
        "v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design.json",
    )
    getco_path = _explicit_or_latest(root, getco_readiness_json, "v1_5_getco_identity_readiness.json")
    run_status_path = _explicit_or_latest(root, run_evidence_status_json, "v1_5_run_evidence_status.json")
    closure_path = _explicit_or_latest(root, full_flow_closure_readiness_json, "v1_5_full_flow_closure_readiness.json")
    archive_path = _explicit_or_latest(root, archive_closure_json, "v1_5_formal_archive_closure_index.json")
    algorithm_profile_runner_path = _explicit_or_latest(
        root,
        algorithm_profile_runner_dry_run_json,
        "v1_5_algorithm_profile_runner_dry_run.json",
    )
    full_flow_automation_closure_path = _explicit_or_latest(
        root,
        full_flow_automation_closure_json,
        "v1_5_full_flow_automation_closure.json",
    )
    senco_artifact_authorization_path = _explicit_or_latest(
        root,
        senco_artifact_authorization_json,
        "main_senco_artifact_authorization.json",
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
    formal_initialization_controlled_executor_design_payload = _load_json(
        formal_initialization_controlled_executor_design_path
    )
    formal_initialization_readonly_com_preflight_design_payload = _load_json(
        formal_initialization_readonly_com_preflight_design_path
    )
    formal_initialization_readonly_com_preflight_blocked_executor_payload = _load_json(
        formal_initialization_readonly_com_preflight_blocked_executor_path
    )
    formal_initialization_readonly_com_preflight_controlled_executor_design_payload = _load_json(
        formal_initialization_readonly_com_preflight_controlled_executor_design_path
    )
    formal_initialization_readonly_com_preflight_controlled_blocked_executor_payload = _load_json(
        formal_initialization_readonly_com_preflight_controlled_blocked_executor_path
    )
    formal_readonly_com_execution_contract_payload = _load_json(formal_readonly_com_execution_contract_path)
    formal_readonly_com_execution_blocked_executor_payload = _load_json(
        formal_readonly_com_execution_blocked_executor_path
    )
    formal_readonly_com_execution_packet_validator_payload = _load_json(
        formal_readonly_com_execution_packet_validator_path
    )
    formal_readonly_com_execution_plan_preview_payload = _load_json(
        formal_readonly_com_execution_plan_preview_path
    )
    formal_readonly_com_minimal_executor_review_payload = _load_json(
        formal_readonly_com_minimal_executor_review_path
    )
    formal_readonly_com_minimal_executor_stub_payload = _load_json(
        formal_readonly_com_minimal_executor_stub_path
    )
    formal_readonly_com_minimal_executor_payload = _load_json(
        formal_readonly_com_minimal_executor_path
    )
    route_physical_recovery_payload = _load_json(route_physical_recovery_path)
    mature_route_continuity_gate_payload = _load_json(mature_route_continuity_gate_path)
    pressure_s9_readiness_index_payload = _load_json(pressure_s9_readiness_index_path)
    pre_gas_payload = _load_json(pre_gas_path)
    batch_initialization_closeout_payload = _load_json(batch_initialization_closeout_path)
    post_closeout_resume_gate_payload = _load_json(post_closeout_resume_gate_path)
    resume_prefix_application_review_payload = _load_json(resume_prefix_application_review_path)
    authoritative_resume_state_writer_design_payload = _load_json(
        authoritative_resume_state_writer_design_path
    )
    authoritative_resume_state_writer_blocked_executor_payload = _load_json(
        authoritative_resume_state_writer_blocked_executor_path
    )
    authoritative_resume_state_controlled_write_preflight_payload = _load_json(
        authoritative_resume_state_controlled_write_preflight_path
    )
    authoritative_resume_state_post_write_verification_payload = _load_json(
        authoritative_resume_state_post_write_verification_path
    )
    authoritative_resume_offline_state_advance_post_write_verification_payload = _load_json(
        authoritative_resume_offline_state_advance_post_write_verification_path
    )
    authoritative_resume_offline_state_advance_consumer_readiness_payload = _load_json(
        authoritative_resume_offline_state_advance_consumer_readiness_path
    )
    authoritative_resume_offline_state_advance_next_step_plan_payload = _load_json(
        authoritative_resume_offline_state_advance_next_step_plan_path
    )
    authoritative_resume_offline_state_advance_next_step_authorization_preflight_payload = _load_json(
        authoritative_resume_offline_state_advance_next_step_authorization_preflight_path
    )
    authoritative_resume_offline_state_advance_next_step_blocked_executor_payload = _load_json(
        authoritative_resume_offline_state_advance_next_step_blocked_executor_path
    )
    authoritative_resume_offline_state_advance_next_step_controlled_executor_design_payload = _load_json(
        authoritative_resume_offline_state_advance_next_step_controlled_executor_design_path
    )
    getco_payload = _load_json(getco_path)
    run_payload = _load_json(run_status_path)
    closure_payload = _load_json(closure_path)
    archive_payload = _load_json(archive_path)
    algorithm_profile_runner_payload = _load_json(algorithm_profile_runner_path)
    full_flow_automation_closure_payload = _load_json(full_flow_automation_closure_path)
    senco_artifact_authorization_payload = _load_json(senco_artifact_authorization_path)
    formal_database_dry_run_payload = _load_json(formal_database_dry_run_path)
    formal_database_import_preflight_payload = _load_json(formal_database_import_preflight_path)
    formal_database_import_authorization_payload = _load_json(formal_database_import_authorization_path)
    formal_database_import_command_contract_payload = _load_json(formal_database_import_command_contract_path)
    formal_database_import_blocked_executor_payload = _load_json(formal_database_import_blocked_executor_path)
    formal_database_import_controlled_executor_design_payload = _load_json(
        formal_database_import_controlled_executor_design_path
    )

    gates = [_initialization_gate(init_path, init_payload)]
    if (
        formal_initialization_controlled_executor_design_path
        and formal_initialization_controlled_executor_design_payload
    ):
        gates.append(
            _formal_initialization_controlled_executor_design_gate(
                formal_initialization_controlled_executor_design_path,
                formal_initialization_controlled_executor_design_payload,
            )
        )
    if (
        formal_initialization_readonly_com_preflight_design_path
        and formal_initialization_readonly_com_preflight_design_payload
    ):
        gates.append(
            _formal_initialization_readonly_com_preflight_design_gate(
                formal_initialization_readonly_com_preflight_design_path,
                formal_initialization_readonly_com_preflight_design_payload,
            )
        )
    if (
        formal_initialization_readonly_com_preflight_blocked_executor_path
        and formal_initialization_readonly_com_preflight_blocked_executor_payload
    ):
        gates.append(
            _formal_initialization_readonly_com_preflight_blocked_executor_gate(
                formal_initialization_readonly_com_preflight_blocked_executor_path,
                formal_initialization_readonly_com_preflight_blocked_executor_payload,
            )
        )
    if (
        formal_initialization_readonly_com_preflight_controlled_executor_design_path
        and formal_initialization_readonly_com_preflight_controlled_executor_design_payload
    ):
        gates.append(
            _formal_initialization_readonly_com_preflight_controlled_executor_design_gate(
                formal_initialization_readonly_com_preflight_controlled_executor_design_path,
                formal_initialization_readonly_com_preflight_controlled_executor_design_payload,
            )
        )
    if (
        formal_initialization_readonly_com_preflight_controlled_blocked_executor_path
        and formal_initialization_readonly_com_preflight_controlled_blocked_executor_payload
    ):
        gates.append(
            _formal_initialization_readonly_com_preflight_controlled_blocked_executor_gate(
                formal_initialization_readonly_com_preflight_controlled_blocked_executor_path,
                formal_initialization_readonly_com_preflight_controlled_blocked_executor_payload,
            )
        )
    if formal_readonly_com_execution_contract_path and formal_readonly_com_execution_contract_payload:
        gates.append(
            _formal_readonly_com_execution_contract_gate(
                formal_readonly_com_execution_contract_path,
                formal_readonly_com_execution_contract_payload,
            )
        )
    if (
        formal_readonly_com_execution_blocked_executor_path
        and formal_readonly_com_execution_blocked_executor_payload
    ):
        gates.append(
            _formal_readonly_com_execution_blocked_executor_gate(
                formal_readonly_com_execution_blocked_executor_path,
                formal_readonly_com_execution_blocked_executor_payload,
            )
        )
    if (
        formal_readonly_com_execution_packet_validator_path
        and formal_readonly_com_execution_packet_validator_payload
    ):
        gates.append(
            _formal_readonly_com_execution_packet_validator_gate(
                formal_readonly_com_execution_packet_validator_path,
                formal_readonly_com_execution_packet_validator_payload,
            )
        )
    if (
        formal_readonly_com_execution_plan_preview_path
        and formal_readonly_com_execution_plan_preview_payload
    ):
        gates.append(
            _formal_readonly_com_execution_plan_preview_gate(
                formal_readonly_com_execution_plan_preview_path,
                formal_readonly_com_execution_plan_preview_payload,
            )
        )
    if (
        formal_readonly_com_minimal_executor_review_path
        and formal_readonly_com_minimal_executor_review_payload
    ):
        gates.append(
            _formal_readonly_com_minimal_executor_review_gate(
                formal_readonly_com_minimal_executor_review_path,
                formal_readonly_com_minimal_executor_review_payload,
            )
        )
    if (
        formal_readonly_com_minimal_executor_stub_path
        and formal_readonly_com_minimal_executor_stub_payload
    ):
        gates.append(
            _formal_readonly_com_minimal_executor_stub_gate(
                formal_readonly_com_minimal_executor_stub_path,
                formal_readonly_com_minimal_executor_stub_payload,
            )
        )
    if (
        formal_readonly_com_minimal_executor_path
        and formal_readonly_com_minimal_executor_payload
    ):
        gates.append(
            _formal_readonly_com_minimal_executor_gate(
                formal_readonly_com_minimal_executor_path,
                formal_readonly_com_minimal_executor_payload,
            )
        )
    if route_physical_recovery_path and route_physical_recovery_payload:
        gates.append(
            _route_physical_recovery_gate(
                route_physical_recovery_path,
                route_physical_recovery_payload,
            )
        )
    if mature_route_continuity_gate_path or _needs_mature_route_continuity_gate(run_payload):
        gates.append(
            _mature_route_continuity_gate(
                mature_route_continuity_gate_path,
                mature_route_continuity_gate_payload,
            )
        )
    senco_artifact_authorization_gate = _senco_artifact_authorization_gate(
        senco_artifact_authorization_path,
        senco_artifact_authorization_payload,
    )
    gates.extend(
        [
            _getco_gate(getco_path, getco_payload),
            _pre_gas_gate(pre_gas_path, pre_gas_payload),
        ]
    )
    if batch_initialization_closeout_path or formal_readonly_com_minimal_executor_path:
        gates.append(
            _batch_initialization_closeout_gate(
                batch_initialization_closeout_path,
                batch_initialization_closeout_payload,
            )
        )
    if post_closeout_resume_gate_path or batch_initialization_closeout_path:
        gates.append(
            _post_closeout_resume_gate(
                post_closeout_resume_gate_path,
                post_closeout_resume_gate_payload,
                batch_initialization_closeout_path,
            )
        )
    if resume_prefix_application_review_path or post_closeout_resume_gate_path:
        gates.append(
            _resume_prefix_application_review_gate(
                resume_prefix_application_review_path,
                resume_prefix_application_review_payload,
                post_closeout_resume_gate_path,
            )
        )
    if authoritative_resume_state_writer_design_path or resume_prefix_application_review_path:
        gates.append(
            _authoritative_resume_state_writer_design_gate(
                authoritative_resume_state_writer_design_path,
                authoritative_resume_state_writer_design_payload,
                resume_prefix_application_review_path,
            )
        )
    if (
        authoritative_resume_state_writer_blocked_executor_path
        or authoritative_resume_state_writer_design_path
    ):
        gates.append(
            _authoritative_resume_state_writer_blocked_executor_gate(
                authoritative_resume_state_writer_blocked_executor_path,
                authoritative_resume_state_writer_blocked_executor_payload,
                authoritative_resume_state_writer_design_path,
            )
        )
    if (
        authoritative_resume_state_controlled_write_preflight_path
        or authoritative_resume_state_writer_blocked_executor_path
    ):
        gates.append(
            _authoritative_resume_state_controlled_write_preflight_gate(
                authoritative_resume_state_controlled_write_preflight_path,
                authoritative_resume_state_controlled_write_preflight_payload,
                authoritative_resume_state_writer_blocked_executor_path,
            )
        )
    if (
        authoritative_resume_state_atomic_write_path
        or authoritative_resume_state_post_write_verification_path
    ):
        gates.append(
            _authoritative_resume_state_post_write_verification_gate(
                authoritative_resume_state_post_write_verification_path,
                authoritative_resume_state_post_write_verification_payload,
                authoritative_resume_state_atomic_write_path,
            )
        )
    if (
        authoritative_resume_offline_state_advance_atomic_write_path
        or authoritative_resume_offline_state_advance_post_write_verification_path
        or authoritative_resume_offline_state_advance_consumer_readiness_path
        or authoritative_resume_offline_state_advance_next_step_plan_path
        or authoritative_resume_offline_state_advance_next_step_authorization_preflight_path
        or authoritative_resume_offline_state_advance_next_step_blocked_executor_path
        or authoritative_resume_offline_state_advance_next_step_controlled_executor_design_path
    ):
        gates.append(
            _authoritative_resume_offline_state_advance_post_write_verification_gate(
                authoritative_resume_offline_state_advance_post_write_verification_path,
                authoritative_resume_offline_state_advance_post_write_verification_payload,
                authoritative_resume_offline_state_advance_atomic_write_path,
            )
        )
        gates.append(
            _authoritative_resume_offline_state_advance_consumer_readiness_gate(
                authoritative_resume_offline_state_advance_consumer_readiness_path,
                authoritative_resume_offline_state_advance_consumer_readiness_payload,
                authoritative_resume_offline_state_advance_post_write_verification_path,
            )
        )
        gates.append(
            _authoritative_resume_offline_state_advance_next_step_plan_gate(
                authoritative_resume_offline_state_advance_next_step_plan_path,
                authoritative_resume_offline_state_advance_next_step_plan_payload,
                authoritative_resume_offline_state_advance_consumer_readiness_path,
            )
        )
        gates.append(
            _authoritative_resume_offline_state_advance_next_step_authorization_preflight_gate(
                authoritative_resume_offline_state_advance_next_step_authorization_preflight_path,
                authoritative_resume_offline_state_advance_next_step_authorization_preflight_payload,
                authoritative_resume_offline_state_advance_next_step_plan_path,
            )
        )
        gates.append(
            _authoritative_resume_offline_state_advance_next_step_blocked_executor_gate(
                authoritative_resume_offline_state_advance_next_step_blocked_executor_path,
                authoritative_resume_offline_state_advance_next_step_blocked_executor_payload,
                authoritative_resume_offline_state_advance_next_step_authorization_preflight_path,
            )
        )
        gates.append(
            _authoritative_resume_offline_state_advance_next_step_controlled_design_gate(
                authoritative_resume_offline_state_advance_next_step_controlled_executor_design_path,
                authoritative_resume_offline_state_advance_next_step_controlled_executor_design_payload,
                authoritative_resume_offline_state_advance_next_step_blocked_executor_path,
            )
        )
    if pressure_s9_readiness_index_path:
        gates.append(
            _pressure_s9_readiness_index_gate(
                pressure_s9_readiness_index_path,
                pressure_s9_readiness_index_payload,
            )
        )
    else:
        gates.append(
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
            )
        )
    if algorithm_profile_runner_path and algorithm_profile_runner_payload:
        gates.append(
            _algorithm_profile_runner_dry_run_gate(
                algorithm_profile_runner_path,
                algorithm_profile_runner_payload,
            )
        )
    if full_flow_automation_closure_path and full_flow_automation_closure_payload:
        gates.append(
            _full_flow_automation_closure_gate(
                full_flow_automation_closure_path,
                full_flow_automation_closure_payload,
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
            senco_artifact_authorization_gate,
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

    def _database_gate_ready(gate_id: str) -> bool:
        gate = next((candidate for candidate in gates if candidate.gate_id == gate_id), None)
        return gate is not None and gate.status == READY

    database_dry_run_ready = _database_gate_ready("formal_database_dry_run")
    database_import_preflight_ready = _database_gate_ready("formal_database_import_preflight")
    database_import_authorization_ready = _database_gate_ready("formal_database_import_authorization")
    database_import_command_contract_ready = _database_gate_ready("formal_database_import_command_contract")
    database_import_blocked_executor_ready = _database_gate_ready("formal_database_import_blocked_executor")
    database_import_controlled_executor_design_ready = _database_gate_ready(
        "formal_database_import_controlled_executor_design"
    )
    database_import_allowed = (
        formal_release_allowed
        and database_dry_run_ready
        and database_import_preflight_ready
        and database_import_authorization_ready
        and database_import_command_contract_ready
        and database_import_blocked_executor_ready
        and database_import_controlled_executor_design_ready
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
        "full_production_auto_allowed": False,
        "senco_artifact_authorization": {
            "status": senco_artifact_authorization_gate.status,
            "controlled_write_authorization_ready": senco_artifact_authorization_gate.status == READY,
            "source_path": senco_artifact_authorization_gate.source_path,
            "source_status": senco_artifact_authorization_gate.source_status,
            "reason": senco_artifact_authorization_gate.reason,
            "authorization_id": str(senco_artifact_authorization_payload.get("authorization_id") or ""),
            "reviewer": str(senco_artifact_authorization_payload.get("reviewer") or ""),
            "approver": str(senco_artifact_authorization_payload.get("approver") or ""),
            "authorized_writer_scopes": list(
                senco_artifact_authorization_payload.get("authorized_writer_scopes") or []
            ),
            "authorized_device_ids": list(
                senco_artifact_authorization_payload.get("authorized_device_ids") or []
            ),
        },
        "physical_boundaries": {
            "offline_status_only": True,
            "opens_com_ports": False,
            "connects_postgresql": False,
            "real_import_execution_allowed": False,
            "database_written": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "writes_device_id": False,
            "not_real_acceptance_evidence": True,
        },
        "linked_inputs": {
            "initialization_readiness_json": str(init_path) if init_path else "",
            "formal_initialization_controlled_executor_design_json": str(
                formal_initialization_controlled_executor_design_path
            )
            if formal_initialization_controlled_executor_design_path
            else "",
            "formal_initialization_readonly_com_preflight_design_json": str(
                formal_initialization_readonly_com_preflight_design_path
            )
            if formal_initialization_readonly_com_preflight_design_path
            else "",
            "formal_initialization_readonly_com_preflight_blocked_executor_json": str(
                formal_initialization_readonly_com_preflight_blocked_executor_path
            )
            if formal_initialization_readonly_com_preflight_blocked_executor_path
            else "",
            "formal_initialization_readonly_com_preflight_controlled_executor_design_json": str(
                formal_initialization_readonly_com_preflight_controlled_executor_design_path
            )
            if formal_initialization_readonly_com_preflight_controlled_executor_design_path
            else "",
            "formal_initialization_readonly_com_preflight_controlled_blocked_executor_json": str(
                formal_initialization_readonly_com_preflight_controlled_blocked_executor_path
            )
            if formal_initialization_readonly_com_preflight_controlled_blocked_executor_path
            else "",
            "formal_readonly_com_execution_contract_json": str(formal_readonly_com_execution_contract_path)
            if formal_readonly_com_execution_contract_path
            else "",
            "formal_readonly_com_execution_blocked_executor_json": str(
                formal_readonly_com_execution_blocked_executor_path
            )
            if formal_readonly_com_execution_blocked_executor_path
            else "",
            "formal_readonly_com_execution_packet_validator_json": str(
                formal_readonly_com_execution_packet_validator_path
            )
            if formal_readonly_com_execution_packet_validator_path
            else "",
            "formal_readonly_com_execution_plan_preview_json": str(
                formal_readonly_com_execution_plan_preview_path
            )
            if formal_readonly_com_execution_plan_preview_path
            else "",
            "formal_readonly_com_minimal_executor_review_json": str(
                formal_readonly_com_minimal_executor_review_path
            )
            if formal_readonly_com_minimal_executor_review_path
            else "",
            "formal_readonly_com_minimal_executor_stub_json": str(
                formal_readonly_com_minimal_executor_stub_path
            )
            if formal_readonly_com_minimal_executor_stub_path
            else "",
            "formal_readonly_com_minimal_executor_json": str(
                formal_readonly_com_minimal_executor_path
            )
            if formal_readonly_com_minimal_executor_path
            else "",
            "route_physical_recovery_readiness_json": str(route_physical_recovery_path)
            if route_physical_recovery_path
            else "",
            "mature_route_continuity_gate_json": str(mature_route_continuity_gate_path)
            if mature_route_continuity_gate_path
            else "",
            "pressure_s9_readiness_index_json": str(pressure_s9_readiness_index_path)
            if pressure_s9_readiness_index_path
            else "",
            "pre_gas_readiness_json": str(pre_gas_path) if pre_gas_path else "",
            "batch_initialization_closeout_json": str(batch_initialization_closeout_path)
            if batch_initialization_closeout_path
            else "",
            "post_closeout_resume_gate_json": str(post_closeout_resume_gate_path)
            if post_closeout_resume_gate_path
            else "",
            "resume_prefix_application_review_json": str(resume_prefix_application_review_path)
            if resume_prefix_application_review_path
            else "",
            "authoritative_resume_state_writer_design_json": str(
                authoritative_resume_state_writer_design_path
            )
            if authoritative_resume_state_writer_design_path
            else "",
            "authoritative_resume_state_writer_blocked_executor_json": str(
                authoritative_resume_state_writer_blocked_executor_path
            )
            if authoritative_resume_state_writer_blocked_executor_path
            else "",
            "authoritative_resume_state_controlled_write_preflight_json": str(
                authoritative_resume_state_controlled_write_preflight_path
            )
            if authoritative_resume_state_controlled_write_preflight_path
            else "",
            "authoritative_resume_state_atomic_write_json": str(
                authoritative_resume_state_atomic_write_path
            )
            if authoritative_resume_state_atomic_write_path
            else "",
            "authoritative_resume_state_post_write_verification_json": str(
                authoritative_resume_state_post_write_verification_path
            )
            if authoritative_resume_state_post_write_verification_path
            else "",
            "authoritative_resume_offline_state_advance_atomic_write_json": str(
                authoritative_resume_offline_state_advance_atomic_write_path
            )
            if authoritative_resume_offline_state_advance_atomic_write_path
            else "",
            "authoritative_resume_offline_state_advance_post_write_verification_json": str(
                authoritative_resume_offline_state_advance_post_write_verification_path
            )
            if authoritative_resume_offline_state_advance_post_write_verification_path
            else "",
            "authoritative_resume_offline_state_advance_consumer_readiness_json": str(
                authoritative_resume_offline_state_advance_consumer_readiness_path
            )
            if authoritative_resume_offline_state_advance_consumer_readiness_path
            else "",
            "authoritative_resume_offline_state_advance_next_step_plan_json": str(
                authoritative_resume_offline_state_advance_next_step_plan_path
            )
            if authoritative_resume_offline_state_advance_next_step_plan_path
            else "",
            "authoritative_resume_offline_state_advance_next_step_authorization_preflight_json": str(
                authoritative_resume_offline_state_advance_next_step_authorization_preflight_path
            )
            if authoritative_resume_offline_state_advance_next_step_authorization_preflight_path
            else "",
            "authoritative_resume_offline_state_advance_next_step_blocked_executor_json": str(
                authoritative_resume_offline_state_advance_next_step_blocked_executor_path
            )
            if authoritative_resume_offline_state_advance_next_step_blocked_executor_path
            else "",
            "authoritative_resume_offline_state_advance_next_step_controlled_executor_design_json": str(
                authoritative_resume_offline_state_advance_next_step_controlled_executor_design_path
            )
            if authoritative_resume_offline_state_advance_next_step_controlled_executor_design_path
            else "",
            "getco_readiness_json": str(getco_path) if getco_path else "",
            "run_evidence_status_json": str(run_status_path) if run_status_path else "",
            "full_flow_closure_readiness_json": str(closure_path) if closure_path else "",
            "archive_closure_json": str(archive_path) if archive_path else "",
            "algorithm_profile_runner_dry_run_json": str(algorithm_profile_runner_path)
            if algorithm_profile_runner_path
            else "",
            "full_flow_automation_closure_json": str(full_flow_automation_closure_path)
            if full_flow_automation_closure_path
            else "",
            "senco_artifact_authorization_json": str(senco_artifact_authorization_path)
            if senco_artifact_authorization_path
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
