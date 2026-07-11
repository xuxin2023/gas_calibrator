"""Offline preflight for a future controlled V1.5 resume-state writer."""

from __future__ import annotations

import csv
import hashlib
import json
import stat
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from gas_calibrator.v1_5.orchestration.full_flow import (
    FullFlowPlan,
    FullFlowStep,
    build_full_flow_state,
)

from .v1_5_authoritative_resume_state_writer_blocked_executor import (
    build_v1_5_authoritative_resume_state_writer_blocked_executor,
)


SCHEMA = "v1_5_authoritative_resume_state_controlled_write_preflight_v1"
READY_STATUS = "ready_for_authoritative_resume_state_controlled_write_review"
REVIEW_STATUS = "review_required"
BLOCKED_STATUS = "blocked"

PLAN_SCHEMA = "v1_5_full_calibration_flow_plan_v0"
BLOCKED_SCHEMA = "v1_5_authoritative_resume_state_writer_blocked_executor_v1"
BLOCKED_READY_STATUS = "blocked_pending_authoritative_resume_state_writer_implementation"
AUTHORIZATION_SCHEMA = "v1_5_authoritative_resume_state_write_authorization_v1"
AUTHORIZATION_OPERATION = "authoritative_resume_state_controlled_write_preflight"
CONFIRMATION_TEMPLATE = "v1_5_authoritative_resume_state_controlled_write_preflight_v1"

DESIGN_STEP_ID = "authoritative_resume_state_writer_design"
BLOCKED_STEP_ID = "authoritative_resume_state_writer_blocked_executor"
PREFLIGHT_STEP_ID = "authoritative_resume_state_controlled_write_preflight"
NEXT_STEP_ID = "temperature_channel_fast_review"
PREFLIGHT_MODULE = (
    "gas_calibrator.tools.export_v1_5_authoritative_resume_state_controlled_write_preflight"
)


@dataclass(frozen=True)
class AuthoritativeResumeStateControlledWritePreflightCheck:
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
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.is_file():
        return {}
    try:
        payload = json.loads(source.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _sha256(path: str | Path | None) -> str:
    if not path or not Path(path).is_file():
        return ""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _candidate_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_bytes(payload)).hexdigest() if payload else ""


def _same_path(value: Any, expected: Path) -> bool:
    if not value:
        return False
    try:
        return Path(str(value)).resolve() == expected.resolve()
    except (OSError, RuntimeError):
        return False


def _command_value(step: Mapping[str, Any], flag: str) -> str:
    command = step.get("command") or []
    if isinstance(command, str):
        return ""
    values = [str(item) for item in command]
    try:
        return values[values.index(flag) + 1]
    except (ValueError, IndexError):
        return ""


def _check(
    *,
    check: str,
    status: str,
    evidence_role: str,
    reasons: Sequence[str] = (),
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> AuthoritativeResumeStateControlledWritePreflightCheck:
    return AuthoritativeResumeStateControlledWritePreflightCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _has_reparse_point(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    return path.is_symlink() or bool(
        attributes & int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0) or 0)
    )


def _valid_iso_timestamp(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _plan_from_payload(payload: Mapping[str, Any]) -> FullFlowPlan:
    steps: list[FullFlowStep] = []
    for raw in payload.get("steps") or []:
        row = dict(raw)
        steps.append(
            FullFlowStep(
                step_id=str(row.get("step_id") or ""),
                title=str(row.get("title") or ""),
                phase=str(row.get("phase") or ""),
                tool_module=(str(row.get("tool_module")) if row.get("tool_module") else None),
                command=tuple(str(item) for item in row.get("command") or []),
                required_inputs=tuple(str(item) for item in row.get("required_inputs") or []),
                expected_outputs=tuple(str(item) for item in row.get("expected_outputs") or []),
                physical_meaning=str(row.get("physical_meaning") or ""),
                execution_mode=str(row.get("execution_mode") or "offline"),
                gate=str(row.get("gate") or "review"),
                uses_validated_v1_5_entry=bool(
                    row.get("uses_validated_v1_5_entry", True)
                ),
                may_reuse_v1_shared_core=bool(row.get("may_reuse_v1_shared_core", False)),
                opens_com_ports=bool(row.get("opens_com_ports", False)),
                controls_pressure=bool(row.get("controls_pressure", False)),
                controls_gas_route=bool(row.get("controls_gas_route", False)),
                controls_water_route=bool(row.get("controls_water_route", False)),
                writes_coefficients=bool(row.get("writes_coefficients", False)),
                writes_device_id=bool(row.get("writes_device_id", False)),
                coefficient_epoch_event=str(row.get("coefficient_epoch_event") or "none"),
                notes=tuple(str(item) for item in row.get("notes") or []),
            )
        )
    return FullFlowPlan(
        schema=str(payload.get("schema") or ""),
        contract=str(payload.get("contract") or ""),
        run_id=str(payload.get("run_id") or ""),
        created_at=str(payload.get("created_at") or ""),
        config_path=str(payload.get("config_path") or ""),
        output_dir=str(payload.get("output_dir") or ""),
        dry_run_only=bool(payload.get("dry_run_only", False)),
        safety_contract=dict(payload.get("safety_contract") or {}),
        coefficient_epoch_contract=dict(payload.get("coefficient_epoch_contract") or {}),
        physical_order=tuple(str(item) for item in payload.get("physical_order") or []),
        steps=tuple(steps),
        warnings=tuple(str(item) for item in payload.get("warnings") or []),
        metadata=dict(payload.get("metadata") or {}),
    )


def _blocked_evidence_matches(
    payload: Mapping[str, Any],
    recomputed: Mapping[str, Any],
) -> bool:
    keys = (
        "schema",
        "overall_status",
        "blocked_executor_ready",
        "production_state",
        "execution_supported",
        "authoritative_state_write_allowed",
        "full_flow_plan_json",
        "full_flow_plan_sha256",
        "resume_prefix_application_review_json",
        "resume_prefix_application_review_sha256",
        "authoritative_resume_state_writer_design_json",
        "authoritative_resume_state_writer_design_sha256",
        "run_id",
        "proposed_authoritative_state_json_recorded_only",
        "next_step_id_after_blocked_executor_review",
        "writes_authoritative_state",
        "state_file_created",
        "state_file_replaced",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "connects_postgresql",
        "writes_coefficients",
        "formal_release_allowed",
        "database_import_allowed",
        "not_real_acceptance_evidence",
    )
    return all(payload.get(key) == recomputed.get(key) for key in keys) and (
        payload.get("checks") == json.loads(json.dumps(recomputed.get("checks") or []))
    )


def _plan_binding_reasons(
    plan: Mapping[str, Any],
    *,
    plan_path: Path,
    application_path: Path,
    design_path: Path,
    blocked_path: Path,
    authorization_path: Path,
) -> list[str]:
    reasons: list[str] = []
    if plan.get("schema") != PLAN_SCHEMA:
        reasons.append(f"plan_schema={plan.get('schema') or 'missing'}")
    rows = [dict(row) for row in plan.get("steps") or [] if isinstance(row, Mapping)]
    step_ids = [str(row.get("step_id") or "") for row in rows]
    if len(step_ids) != len(set(step_ids)):
        reasons.append("duplicate_full_flow_step_ids")
    required = (DESIGN_STEP_ID, BLOCKED_STEP_ID, PREFLIGHT_STEP_ID, NEXT_STEP_ID)
    for step_id in required:
        if step_id not in step_ids:
            reasons.append(f"required_step_missing:{step_id}")
    if all(step_id in step_ids for step_id in required):
        indexes = [step_ids.index(step_id) for step_id in required]
        if indexes[1:] != [indexes[0] + 1, indexes[0] + 2, indexes[0] + 3]:
            reasons.append("controlled_write_preflight_steps_not_adjacent")
    by_id = {str(row.get("step_id") or ""): row for row in rows}
    step = by_id.get(PREFLIGHT_STEP_ID) or {}
    if str(step.get("tool_module") or "") != PREFLIGHT_MODULE:
        reasons.append("controlled_write_preflight_module_mismatch")
    if not str(step.get("execution_mode") or "").startswith("offline"):
        reasons.append("controlled_write_preflight_not_offline")
    if any(
        bool(step.get(key))
        for key in (
            "opens_com_ports",
            "controls_pressure",
            "controls_gas_route",
            "controls_water_route",
            "writes_device_id",
            "writes_coefficients",
        )
    ):
        reasons.append("controlled_write_preflight_side_effect_boundary_not_clean")
    for flag, expected in (
        ("--full-flow-plan-json", plan_path),
        ("--resume-prefix-application-review-json", application_path),
        ("--authoritative-resume-state-writer-design-json", design_path),
        ("--authoritative-resume-state-writer-blocked-executor-json", blocked_path),
        ("--authorization-packet-json", authorization_path),
    ):
        if not _same_path(_command_value(step, flag), expected):
            reasons.append(f"controlled_write_preflight_path_mismatch:{flag}")
    command = [str(item) for item in step.get("command") or []]
    for required_flag in ("--output-dir", "--fail-on-blocker", "--fail-on-review-required"):
        if required_flag not in command:
            reasons.append(f"controlled_write_preflight_missing_flag:{required_flag}")
    for forbidden in (
        "--execute",
        "--write-state",
        "--replace-state",
        "--allow-real-com",
        "--allow-pressure-control",
        "--allow-route-control",
        "--allow-writes",
        "--allow-database-import",
    ):
        if forbidden in command:
            reasons.append(f"controlled_write_preflight_forbidden_flag:{forbidden}")
    return reasons


def build_v1_5_authoritative_resume_state_controlled_write_preflight(
    *,
    full_flow_plan_json: str | Path,
    resume_prefix_application_review_json: str | Path,
    authoritative_resume_state_writer_design_json: str | Path,
    authoritative_resume_state_writer_blocked_executor_json: str | Path,
    authorization_packet_json: str | Path,
) -> dict[str, Any]:
    plan_path = Path(full_flow_plan_json).resolve()
    application_path = Path(resume_prefix_application_review_json).resolve()
    design_path = Path(authoritative_resume_state_writer_design_json).resolve()
    blocked_path = Path(authoritative_resume_state_writer_blocked_executor_json).resolve()
    authorization_path = Path(authorization_packet_json).resolve()
    plan = _load_json(plan_path)
    design = _load_json(design_path)
    blocked = _load_json(blocked_path)
    authorization = _load_json(authorization_path)
    checks: list[AuthoritativeResumeStateControlledWritePreflightCheck] = []

    plan_reasons = _plan_binding_reasons(
        plan,
        plan_path=plan_path,
        application_path=application_path,
        design_path=design_path,
        blocked_path=blocked_path,
        authorization_path=authorization_path,
    )
    checks.append(
        _check(
            check="controlled_write_preflight_bound_to_canonical_plan",
            status="ready" if not plan_reasons else "blocker",
            evidence_role="canonical_plan_binding",
            reasons=plan_reasons,
            physical_meaning="The preflight must be the exact offline step between the #92 lock proof and temperature review.",
            next_action="Regenerate the canonical full-flow plan and use its exact source paths.",
            details={"full_flow_plan_json": str(plan_path)},
        )
    )

    blocked_reasons: list[str] = []
    recomputed_blocked: dict[str, Any] = {}
    if blocked.get("schema") != BLOCKED_SCHEMA:
        blocked_reasons.append(f"blocked_schema={blocked.get('schema') or 'missing'}")
    if blocked.get("overall_status") != BLOCKED_READY_STATUS:
        blocked_reasons.append(f"blocked_status={blocked.get('overall_status') or 'missing'}")
    if blocked.get("blocked_executor_ready") is not True:
        blocked_reasons.append("blocked_executor_not_ready")
    if not _same_path(blocked.get("full_flow_plan_json"), plan_path):
        blocked_reasons.append("blocked_plan_path_mismatch")
    if not _same_path(blocked.get("resume_prefix_application_review_json"), application_path):
        blocked_reasons.append("blocked_application_path_mismatch")
    if not _same_path(blocked.get("authoritative_resume_state_writer_design_json"), design_path):
        blocked_reasons.append("blocked_design_path_mismatch")
    if str(blocked.get("full_flow_plan_sha256") or "") != _sha256(plan_path):
        blocked_reasons.append("blocked_plan_sha256_mismatch")
    if str(blocked.get("resume_prefix_application_review_sha256") or "") != _sha256(
        application_path
    ):
        blocked_reasons.append("blocked_application_sha256_mismatch")
    if str(blocked.get("authoritative_resume_state_writer_design_sha256") or "") != _sha256(
        design_path
    ):
        blocked_reasons.append("blocked_design_sha256_mismatch")
    try:
        recomputed_blocked = build_v1_5_authoritative_resume_state_writer_blocked_executor(
            full_flow_plan_json=plan_path,
            resume_prefix_application_review_json=application_path,
            authoritative_resume_state_writer_design_json=design_path,
        )
    except (OSError, ValueError, json.JSONDecodeError):
        recomputed_blocked = {}
    if not recomputed_blocked or not _blocked_evidence_matches(blocked, recomputed_blocked):
        blocked_reasons.append("blocked_executor_independent_recompute_mismatch")
    checks.append(
        _check(
            check="blocked_executor_lock_evidence_independently_recomputed",
            status="ready" if not blocked_reasons else "blocker",
            evidence_role="hash_bound_blocked_executor_evidence",
            reasons=blocked_reasons,
            physical_meaning="A controlled-write preflight cannot bypass or replace the #92 state-write lock proof.",
            next_action="Regenerate #91/#92 evidence from the canonical plan before this preflight.",
            details={
                "blocked_executor_json": str(blocked_path),
                "blocked_executor_sha256": _sha256(blocked_path),
            },
        )
    )

    expected_target = plan_path.parent / "v1_5_full_flow_state.json"
    target_value = str(design.get("proposed_authoritative_state_json") or "")
    target_path = Path(target_value).absolute() if target_value else expected_target
    target_exists = target_path.is_file()
    observed_existing_sha256 = _sha256(target_path) if target_exists else "absent"
    target_reasons: list[str] = []
    if not _same_path(target_path, expected_target):
        target_reasons.append("authoritative_state_target_not_canonical")
    if _has_reparse_point(target_path) or _has_reparse_point(target_path.parent):
        target_reasons.append("authoritative_state_target_or_parent_is_reparse_point")
    existing_payload = _load_json(target_path) if target_exists else {}
    if target_exists:
        if existing_payload.get("schema") != "v1_5_full_calibration_flow_state_v0":
            target_reasons.append("existing_state_schema_invalid")
        if str(existing_payload.get("run_id") or "") != str(plan.get("run_id") or ""):
            target_reasons.append("existing_state_run_id_mismatch")

    authorized_at = str(authorization.get("authorized_at") or "").strip()
    candidate_state: dict[str, Any] = {}
    candidate_reasons: list[str] = []
    if not _valid_iso_timestamp(authorized_at):
        candidate_reasons.append("authorization_timestamp_missing_or_invalid")
    if not plan_reasons and not candidate_reasons:
        try:
            typed_plan = _plan_from_payload(plan)
            step_ids = [step.step_id for step in typed_plan.steps]
            prefix = step_ids[: step_ids.index(PREFLIGHT_STEP_ID) + 1]
            state = build_full_flow_state(
                typed_plan,
                completed_steps=prefix,
                failed_steps=(),
                allow_real_com=False,
                allow_pressure_control=False,
                allow_route_control=False,
                allow_writes=False,
            )
            candidate_state = json.loads(json.dumps(state.to_json()))
            candidate_state["created_at"] = authorized_at
            if candidate_state.get("completed_step_ids") != prefix:
                candidate_reasons.append("candidate_completed_prefix_not_exact")
            if candidate_state.get("current_step_id") != NEXT_STEP_ID:
                candidate_reasons.append("candidate_next_step_not_temperature_review")
            if candidate_state.get("failed_step_ids") != []:
                candidate_reasons.append("candidate_failed_steps_not_empty")
            for key in (
                "allow_real_com",
                "allow_pressure_control",
                "allow_route_control",
                "allow_writes",
            ):
                if candidate_state.get(key) is not False:
                    candidate_reasons.append(f"candidate_{key}_not_false")
        except (KeyError, TypeError, ValueError):
            candidate_state = {}
            candidate_reasons.append("candidate_state_generation_failed")
    candidate_hash = _candidate_sha256(candidate_state)
    checks.append(
        _check(
            check="candidate_state_is_exact_safe_contiguous_prefix",
            status="ready" if candidate_state and not candidate_reasons else "blocker",
            evidence_role="deterministic_candidate_state_preview",
            reasons=candidate_reasons,
            physical_meaning="The candidate is generated from the canonical plan and cannot add arbitrary completed or failed stages.",
            next_action="Fix the canonical prefix or authorization timestamp, then regenerate the candidate preview.",
            details={
                "candidate_state_sha256": candidate_hash,
                "candidate_current_step_id": candidate_state.get("current_step_id", ""),
                "candidate_completed_step_count": len(candidate_state.get("completed_step_ids") or []),
            },
        )
    )

    authorization_review_reasons: list[str] = []
    authorization_blocker_reasons: list[str] = []
    if not authorization:
        authorization_review_reasons.append("authorization_packet_missing")
    if authorization.get("schema") != AUTHORIZATION_SCHEMA:
        authorization_blocker_reasons.append(
            f"authorization_schema={authorization.get('schema') or 'missing'}"
        )
    if authorization.get("requested_operation") != AUTHORIZATION_OPERATION:
        authorization_blocker_reasons.append("authorization_requested_operation_mismatch")
    if authorization.get("confirmation_template") != CONFIRMATION_TEMPLATE:
        authorization_blocker_reasons.append("authorization_confirmation_template_mismatch")
    identities = {
        key: str(authorization.get(key) or "").strip()
        for key in ("operator", "reviewer", "approver")
    }
    for key, value in identities.items():
        if not value:
            authorization_review_reasons.append(f"{key}_missing")
    folded = [value.casefold() for value in identities.values() if value]
    if len(folded) != len(set(folded)):
        authorization_blocker_reasons.append("operator_reviewer_approver_must_be_distinct")
    if not str(authorization.get("authorization_id") or "").strip():
        authorization_review_reasons.append("authorization_id_missing")
    if not _valid_iso_timestamp(authorized_at):
        authorization_review_reasons.append("authorized_at_missing_or_invalid")
    if authorization.get("preflight_only") is not True:
        authorization_blocker_reasons.append("authorization_preflight_only_not_true")
    for field in (
        "authoritative_state_write_allowed",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "connects_postgresql",
        "database_import_allowed",
        "formal_release_allowed",
    ):
        if authorization.get(field) is not False:
            authorization_blocker_reasons.append(f"authorization_boundary_{field}_not_false")
    for prefix, source_path in (
        ("full_flow_plan", plan_path),
        ("resume_prefix_application_review", application_path),
        ("authoritative_resume_state_writer_design", design_path),
        ("authoritative_resume_state_writer_blocked_executor", blocked_path),
    ):
        if not _same_path(authorization.get(f"{prefix}_json"), source_path):
            authorization_blocker_reasons.append(f"authorization_{prefix}_path_mismatch")
        if str(authorization.get(f"{prefix}_sha256") or "") != _sha256(source_path):
            authorization_blocker_reasons.append(f"authorization_{prefix}_sha256_mismatch")
    if not _same_path(authorization.get("authoritative_state_json"), expected_target):
        authorization_blocker_reasons.append("authorization_state_target_mismatch")
    expected_existing = str(authorization.get("expected_existing_state_sha256") or "").lower()
    if not expected_existing:
        authorization_review_reasons.append("expected_existing_state_sha256_missing")
    elif expected_existing != observed_existing_sha256.lower():
        authorization_blocker_reasons.append("expected_existing_state_sha256_mismatch")
    expected_candidate = str(authorization.get("expected_candidate_state_sha256") or "").lower()
    if not expected_candidate:
        authorization_review_reasons.append("expected_candidate_state_sha256_missing")
    elif expected_candidate != candidate_hash.lower():
        authorization_blocker_reasons.append("expected_candidate_state_sha256_mismatch")
    authorization_status = (
        "blocker"
        if authorization_blocker_reasons
        else "review_required"
        if authorization_review_reasons
        else "ready"
    )
    checks.append(
        _check(
            check="manual_authorization_packet_binds_sources_target_and_candidate",
            status=authorization_status,
            evidence_role="manual_controlled_write_authorization",
            reasons=(*authorization_blocker_reasons, *authorization_review_reasons),
            physical_meaning="Authorization must bind the exact upstream hashes, current target state, and candidate bytes without enabling a write.",
            next_action="Review the candidate preview, then record distinct operator/reviewer/approver authorization with exact hashes.",
            details={
                "authorization_packet_json": str(authorization_path),
                "authorization_packet_sha256": _sha256(authorization_path),
                "authorization_id": authorization.get("authorization_id", ""),
                **identities,
            },
        )
    )

    expected_existing = str(authorization.get("expected_existing_state_sha256") or "").lower()
    compare_and_swap_reasons = list(target_reasons)
    if expected_existing and expected_existing != observed_existing_sha256.lower():
        compare_and_swap_reasons.append("compare_and_swap_current_state_mismatch")
    checks.append(
        _check(
            check="authoritative_state_target_compare_and_swap_preflight",
            status="ready" if not compare_and_swap_reasons and expected_existing else "blocker",
            evidence_role="read_only_current_state_snapshot",
            reasons=compare_and_swap_reasons
            if expected_existing
            else (*compare_and_swap_reasons, "expected_existing_state_sha256_missing"),
            physical_meaning="The target is read only and must still equal the authorized absent/hash state before any later writer runs.",
            next_action="Refresh authorization from the current canonical state target; do not overwrite a changed state.",
            details={
                "authoritative_state_json": str(expected_target),
                "state_exists": target_exists,
                "observed_existing_state_sha256": observed_existing_sha256,
                "expected_existing_state_sha256": expected_existing,
            },
        )
    )

    checks.append(
        _check(
            check="controlled_write_execution_remains_unimplemented",
            status="ready",
            evidence_role="no_state_write_boundary",
            physical_meaning="This preflight writes review artifacts only and has no authoritative target mutation path.",
            next_action="Implement any future atomic writer in a separate reviewed package.",
            details={
                "execution_supported": False,
                "authoritative_state_write_allowed": False,
                "writes_authoritative_state": False,
                "state_file_created": False,
                "state_file_replaced": False,
                "state_snapshot_created": False,
                "rollback_executed": False,
            },
        )
    )

    blocker_count = sum(1 for row in checks if row.status == "blocker")
    review_required_count = sum(1 for row in checks if row.status == "review_required")
    preflight_ready = blocker_count == 0 and review_required_count == 0
    overall_status = (
        READY_STATUS if preflight_ready else BLOCKED_STATUS if blocker_count else REVIEW_STATUS
    )
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": overall_status,
        "blocker_count": blocker_count,
        "review_required_count": review_required_count,
        "controlled_write_preflight_ready": preflight_ready,
        "production_state": "offline_controlled_write_preflight_only",
        "execution_supported": False,
        "execution_requested": False,
        "authoritative_state_write_allowed": False,
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha256(plan_path),
        "resume_prefix_application_review_json": str(application_path),
        "resume_prefix_application_review_sha256": _sha256(application_path),
        "authoritative_resume_state_writer_design_json": str(design_path),
        "authoritative_resume_state_writer_design_sha256": _sha256(design_path),
        "authoritative_resume_state_writer_blocked_executor_json": str(blocked_path),
        "authoritative_resume_state_writer_blocked_executor_sha256": _sha256(blocked_path),
        "authorization_packet_json": str(authorization_path),
        "authorization_packet_sha256": _sha256(authorization_path),
        "authorization_id": str(authorization.get("authorization_id") or ""),
        "run_id": str(plan.get("run_id") or ""),
        "authoritative_state_json_read_only": str(expected_target),
        "state_target_exists": target_exists,
        "observed_existing_state_sha256": observed_existing_sha256,
        "expected_existing_state_sha256": expected_existing,
        "candidate_state": candidate_state,
        "candidate_state_sha256": candidate_hash,
        "candidate_state_preview_json": "",
        "candidate_state_preview_sha256": "",
        "does_not_execute_commands": True,
        "writes_authoritative_state": False,
        "state_file_created": False,
        "state_file_replaced": False,
        "state_snapshot_created": False,
        "rollback_executed": False,
        "would_execute": False,
        "live_resume_execution_allowed": False,
        "route_authorization_still_required": True,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "connects_postgresql": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Keep state writing locked. The future writer must consume this exact ready preflight and recheck the current-state SHA immediately before atomic replacement."
        ),
    }


def _write_json_bytes(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_json_bytes(payload))


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["empty"])
        writer.writeheader()
        writer.writerows(dict(row) for row in rows)


def write_v1_5_authoritative_resume_state_controlled_write_preflight_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out / "v1_5_resume_state_write_preflight.json",
        "candidate_preview": out / "v1_5_resume_state_candidate_preview.json",
        "checks_csv": out / "v1_5_resume_state_write_preflight_checks.csv",
        "summary_csv": out / "v1_5_resume_state_write_preflight_summary.csv",
        "markdown": out / "V1_5_RESUME_STATE_WRITE_PREFLIGHT.md",
    }
    payload = dict(model)
    candidate = dict(payload.get("candidate_state") or {})
    if candidate:
        _write_json_bytes(paths["candidate_preview"], candidate)
        payload["candidate_state_preview_json"] = str(paths["candidate_preview"].resolve())
        payload["candidate_state_preview_sha256"] = _sha256(paths["candidate_preview"])
    _write_json_bytes(paths["json"], payload)
    _write_csv(paths["checks_csv"], payload.get("checks", []))
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": payload.get("overall_status"),
                "blocker_count": payload.get("blocker_count"),
                "review_required_count": payload.get("review_required_count"),
                "controlled_write_preflight_ready": payload.get(
                    "controlled_write_preflight_ready"
                ),
                "observed_existing_state_sha256": payload.get(
                    "observed_existing_state_sha256"
                ),
                "candidate_state_sha256": payload.get("candidate_state_sha256"),
                "authoritative_state_write_allowed": payload.get(
                    "authoritative_state_write_allowed"
                ),
                "writes_authoritative_state": payload.get("writes_authoritative_state"),
                "opens_com_ports": payload.get("opens_com_ports"),
                "connects_postgresql": payload.get("connects_postgresql"),
            }
        ],
    )
    lines = [
        "# V1.5 Authoritative Resume State Controlled-Write Preflight",
        "",
        "This is an offline, no-state-write preflight. The candidate JSON is a review artifact, not the authoritative state target.",
        "",
        f"- overall_status: `{payload.get('overall_status')}`",
        f"- controlled_write_preflight_ready: `{payload.get('controlled_write_preflight_ready')}`",
        f"- observed_existing_state_sha256: `{payload.get('observed_existing_state_sha256')}`",
        f"- candidate_state_sha256: `{payload.get('candidate_state_sha256')}`",
        f"- authoritative_state_write_allowed: `{payload.get('authoritative_state_write_allowed')}`",
        f"- writes_authoritative_state: `{payload.get('writes_authoritative_state')}`",
        f"- opens_com_ports: `{payload.get('opens_com_ports')}`",
        f"- connects_postgresql: `{payload.get('connects_postgresql')}`",
        "",
        "## Checks",
        "",
    ]
    for row in payload.get("checks", []):
        reasons = ";".join(row.get("reasons") or ())
        lines.append(f"- `{row.get('check')}`: `{row.get('status')}` {reasons}".rstrip())
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths


__all__ = [
    "AUTHORIZATION_OPERATION",
    "AUTHORIZATION_SCHEMA",
    "BLOCKED_STATUS",
    "CONFIRMATION_TEMPLATE",
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_state_controlled_write_preflight",
    "write_v1_5_authoritative_resume_state_controlled_write_preflight_outputs",
]
