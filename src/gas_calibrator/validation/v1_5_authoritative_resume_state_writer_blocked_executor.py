"""Blocked executor for the future V1.5 authoritative resume-state writer."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_state_writer_design import (
    build_v1_5_authoritative_resume_state_writer_design,
)


SCHEMA = "v1_5_authoritative_resume_state_writer_blocked_executor_v1"
DESIGN_SCHEMA = "v1_5_authoritative_resume_state_writer_design_v1"
DESIGN_READY_STATUS = "ready_for_authoritative_resume_state_writer_design_review"
BLOCKED_STATUS = "blocked_pending_authoritative_resume_state_writer_implementation"
REVIEW_STATUS = "review_required"

DESIGN_STEP_ID = "authoritative_resume_state_writer_design"
BLOCKED_EXECUTOR_STEP_ID = "authoritative_resume_state_writer_blocked_executor"
NEXT_STEP_ID = "authoritative_resume_state_controlled_write_preflight"
TEMPERATURE_STEP_ID = "temperature_channel_fast_review"
BLOCKED_EXECUTOR_MODULE = (
    "gas_calibrator.tools.run_v1_5_authoritative_resume_state_writer_blocked_executor"
)


@dataclass(frozen=True)
class AuthoritativeResumeStateBlockedExecutorCheck:
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
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    return dict(payload) if isinstance(payload, Mapping) else {}


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
) -> AuthoritativeResumeStateBlockedExecutorCheck:
    return AuthoritativeResumeStateBlockedExecutorCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _design_boundary_clean(payload: Mapping[str, Any]) -> bool:
    return (
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


def _design_matches_recomputed(
    design: Mapping[str, Any],
    recomputed: Mapping[str, Any],
) -> bool:
    keys = (
        "schema",
        "overall_status",
        "authoritative_resume_state_writer_design_ready",
        "design_review_only",
        "execution_supported",
        "authoritative_state_write_allowed",
        "full_flow_plan_json",
        "full_flow_plan_sha256",
        "resume_prefix_application_review_json",
        "resume_prefix_application_review_sha256",
        "post_closeout_resume_gate_json",
        "post_closeout_resume_gate_sha256",
        "batch_initialization_closeout_json",
        "batch_initialization_closeout_sha256",
        "run_id",
        "proposed_completed_step_ids",
        "proposed_completed_step_cli_arguments",
        "proposed_failed_step_ids",
        "proposed_current_step_id",
        "proposed_authoritative_state_json",
        "proposed_authoritative_state_markdown",
        "proposed_authorization_state",
        "transaction_contract",
        "review_reasons",
        "does_not_execute_commands",
        "applies_completed_steps",
        "writes_authoritative_state",
        "would_execute",
        "live_resume_execution_allowed",
        "route_authorization_still_required",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "connects_postgresql",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "database_written",
        "formal_release_allowed",
        "database_import_allowed",
        "not_real_acceptance_evidence",
    )
    return all(design.get(key) == recomputed.get(key) for key in keys)


def _plan_binding_reasons(
    plan: Mapping[str, Any],
    *,
    plan_path: Path,
    application_path: Path,
    design_path: Path,
) -> list[str]:
    reasons: list[str] = []
    steps = [dict(row) for row in plan.get("steps") or [] if isinstance(row, Mapping)]
    step_ids = [str(row.get("step_id") or "") for row in steps]
    if len(step_ids) != len(set(step_ids)):
        reasons.append("duplicate_full_flow_step_ids")
    required = (
        DESIGN_STEP_ID,
        BLOCKED_EXECUTOR_STEP_ID,
        NEXT_STEP_ID,
        TEMPERATURE_STEP_ID,
    )
    for step_id in required:
        if step_id not in step_ids:
            reasons.append(f"required_step_missing:{step_id}")
    if all(step_id in step_ids for step_id in required):
        design_index = step_ids.index(DESIGN_STEP_ID)
        blocked_index = step_ids.index(BLOCKED_EXECUTOR_STEP_ID)
        next_index = step_ids.index(NEXT_STEP_ID)
        temperature_index = step_ids.index(TEMPERATURE_STEP_ID)
        if not (
            blocked_index == design_index + 1
            and next_index == blocked_index + 1
            and temperature_index == next_index + 1
        ):
            reasons.append("authoritative_state_blocked_executor_steps_not_adjacent")
    by_id = {str(row.get("step_id") or ""): row for row in steps}
    step = by_id.get(BLOCKED_EXECUTOR_STEP_ID) or {}
    if str(step.get("tool_module") or "") != BLOCKED_EXECUTOR_MODULE:
        reasons.append("authoritative_state_blocked_executor_module_mismatch")
    if not str(step.get("execution_mode") or "").startswith("offline"):
        reasons.append("authoritative_state_blocked_executor_not_offline")
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
        reasons.append("authoritative_state_blocked_executor_side_effect_boundary_not_clean")
    for flag, expected in (
        ("--full-flow-plan-json", plan_path),
        ("--resume-prefix-application-review-json", application_path),
        ("--authoritative-resume-state-writer-design-json", design_path),
    ):
        if not _same_path(_command_value(step, flag), expected):
            reasons.append(f"authoritative_state_blocked_executor_path_mismatch:{flag}")
    command = [str(item) for item in step.get("command") or []]
    if "--fail-on-blocked" not in command:
        reasons.append("authoritative_state_blocked_executor_not_fail_closed")
    forbidden = (
        "--execute",
        "--write-state",
        "--replace-state",
        "--authoritative-state-json",
        "--expected-existing-state-sha256",
        "--authorization-id",
        "--operator-confirmation-text",
        "--reviewer",
        "--approver",
        "--allow-real-com",
        "--allow-pressure-control",
        "--allow-route-control",
        "--allow-writes",
        "--allow-database-import",
    )
    for flag in forbidden:
        if flag in command:
            reasons.append(f"authoritative_state_blocked_executor_forbidden_flag:{flag}")
    return reasons


def build_v1_5_authoritative_resume_state_writer_blocked_executor(
    *,
    full_flow_plan_json: str | Path,
    resume_prefix_application_review_json: str | Path,
    authoritative_resume_state_writer_design_json: str | Path,
) -> dict[str, Any]:
    plan_path = Path(full_flow_plan_json).resolve()
    application_path = Path(resume_prefix_application_review_json).resolve()
    design_path = Path(authoritative_resume_state_writer_design_json).resolve()
    plan = _load_json(plan_path)
    design = _load_json(design_path)

    checks: list[AuthoritativeResumeStateBlockedExecutorCheck] = []
    plan_reasons = _plan_binding_reasons(
        plan,
        plan_path=plan_path,
        application_path=application_path,
        design_path=design_path,
    )
    checks.append(
        _check(
            check="blocked_executor_bound_to_canonical_plan",
            status="ready" if not plan_reasons else "review_required",
            evidence_role="canonical_plan_binding",
            reasons=plan_reasons,
            physical_meaning=(
                "The blocked executor must be the exact offline step declared between the writer design and temperature review."
            ),
            next_action="Regenerate the canonical full-flow plan and use its exact design and application paths.",
            details={"full_flow_plan_json": str(plan_path)},
        )
    )

    design_reasons: list[str] = []
    recomputed: dict[str, Any] = {}
    if design.get("schema") != DESIGN_SCHEMA:
        design_reasons.append(f"design_schema={design.get('schema') or 'missing'}")
    if design.get("overall_status") != DESIGN_READY_STATUS:
        design_reasons.append(f"design_status={design.get('overall_status') or 'missing'}")
    if design.get("authoritative_resume_state_writer_design_ready") is not True:
        design_reasons.append("design_not_ready")
    if not _design_boundary_clean(design):
        design_reasons.append("design_boundary_not_clean")
    if not _same_path(design.get("full_flow_plan_json"), plan_path):
        design_reasons.append("design_plan_path_mismatch")
    if not _same_path(design.get("resume_prefix_application_review_json"), application_path):
        design_reasons.append("design_application_path_mismatch")
    if str(design.get("full_flow_plan_sha256") or "") != _sha256(plan_path):
        design_reasons.append("design_plan_sha256_mismatch")
    if str(design.get("resume_prefix_application_review_sha256") or "") != _sha256(
        application_path
    ):
        design_reasons.append("design_application_sha256_mismatch")
    try:
        recomputed = build_v1_5_authoritative_resume_state_writer_design(
            full_flow_plan_json=plan_path,
            resume_prefix_application_review_json=application_path,
        )
    except (OSError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if (
        not recomputed
        or recomputed.get("authoritative_resume_state_writer_design_ready") is not True
        or not _design_matches_recomputed(design, recomputed)
    ):
        design_reasons.append("design_independent_recompute_mismatch")
    checks.append(
        _check(
            check="writer_design_independently_recomputed",
            status="ready" if not design_reasons else "review_required",
            evidence_role="hash_bound_writer_design",
            reasons=design_reasons,
            physical_meaning=(
                "The stub cannot trust a ready flag; it recomputes the exact design from the canonical plan and application review."
            ),
            next_action="Regenerate the writer design from the canonical plan and application review.",
            details={
                "writer_design_json": str(design_path),
                "writer_design_sha256": _sha256(design_path) if design_path.is_file() else "",
            },
        )
    )

    checks.extend(
        [
            _check(
                check="authoritative_state_write_lock_enforced",
                status="ready",
                evidence_role="hard_state_write_lock",
                physical_meaning=(
                    "This command has no supported state-write or replace path and never opens the proposed state target."
                ),
                next_action="Implement a separate controlled writer only after another review package.",
                details={
                    "execution_supported": False,
                    "authoritative_state_write_allowed": False,
                    "writes_authoritative_state": False,
                },
            ),
            _check(
                check="state_target_and_authorization_inputs_inert",
                status="ready",
                evidence_role="no_target_no_authorization_unlock",
                physical_meaning=(
                    "Target path, expected old-state hash, authorization, reviewer, and approver inputs are rejected rather than consumed."
                ),
                next_action="Keep target and authorization inputs unavailable until a controlled writer exists.",
                details={
                    "state_target_argument_allowed": False,
                    "expected_state_sha_argument_allowed": False,
                    "authorization_inputs_allowed": False,
                },
            ),
            _check(
                check="device_route_database_side_effect_lock",
                status="ready",
                evidence_role="no_device_no_route_no_database_boundary",
                physical_meaning=(
                    "State-resume review must not become analyzer COM, pressure, gas/water route, coefficient-write, or PostgreSQL execution."
                ),
                next_action="Keep physical and database actions in their dedicated V1.5 stages.",
                details={
                    "opens_com_ports": False,
                    "controls_pressure": False,
                    "controls_water_or_gas_routes": False,
                    "writes_coefficients": False,
                    "connects_postgresql": False,
                },
            ),
        ]
    )

    review_required_count = sum(1 for row in checks if row.status == "review_required")
    blocked_executor_ready = review_required_count == 0
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": BLOCKED_STATUS if blocked_executor_ready else REVIEW_STATUS,
        "blocker_count": 0,
        "review_required_count": review_required_count,
        "blocked_executor_ready": blocked_executor_ready,
        "production_state": "blocked_executor_only",
        "execution_supported": False,
        "execution_requested": False,
        "authoritative_state_write_allowed": False,
        "write_state_flag_allowed": False,
        "state_target_argument_allowed": False,
        "expected_state_sha_argument_allowed": False,
        "authorization_inputs_allowed": False,
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha256(plan_path) if plan_path.is_file() else "",
        "resume_prefix_application_review_json": str(application_path),
        "resume_prefix_application_review_sha256": (
            _sha256(application_path) if application_path.is_file() else ""
        ),
        "authoritative_resume_state_writer_design_json": str(design_path),
        "authoritative_resume_state_writer_design_sha256": (
            _sha256(design_path) if design_path.is_file() else ""
        ),
        "run_id": str(design.get("run_id") or plan.get("run_id") or ""),
        "proposed_authoritative_state_json_recorded_only": str(
            design.get("proposed_authoritative_state_json") or ""
        ),
        "next_step_id_after_blocked_executor_review": NEXT_STEP_ID,
        "does_not_execute_commands": True,
        "applies_completed_steps": False,
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
            "Keep authoritative state writes locked. A later controlled writer must implement the #91 transaction contract and separate authorization."
        ),
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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


def write_v1_5_authoritative_resume_state_writer_blocked_executor_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_authoritative_resume_state_writer_blocked_executor.json",
        "checks_csv": out / "v1_5_authoritative_resume_state_writer_blocked_executor_checks.csv",
        "summary_csv": out / "v1_5_authoritative_resume_state_writer_blocked_executor_summary.csv",
        "markdown": out / "V1_5_AUTHORITATIVE_RESUME_STATE_WRITER_BLOCKED_EXECUTOR.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "review_required_count": model.get("review_required_count"),
                "blocked_executor_ready": model.get("blocked_executor_ready"),
                "execution_supported": model.get("execution_supported"),
                "authoritative_state_write_allowed": model.get(
                    "authoritative_state_write_allowed"
                ),
                "writes_authoritative_state": model.get("writes_authoritative_state"),
                "state_file_created": model.get("state_file_created"),
                "state_file_replaced": model.get("state_file_replaced"),
                "opens_com_ports": model.get("opens_com_ports"),
                "connects_postgresql": model.get("connects_postgresql"),
            }
        ],
    )
    lines = [
        "# V1.5 Authoritative Resume State Writer Blocked Executor",
        "",
        "This is a no-state-write blocked executor stub. It records that the #91 transaction design is valid while all state mutation remains unavailable.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocked_executor_ready: `{model.get('blocked_executor_ready')}`",
        f"- execution_supported: `{model.get('execution_supported')}`",
        f"- authoritative_state_write_allowed: `{model.get('authoritative_state_write_allowed')}`",
        f"- writes_authoritative_state: `{model.get('writes_authoritative_state')}`",
        f"- state_file_created: `{model.get('state_file_created')}`",
        f"- state_file_replaced: `{model.get('state_file_replaced')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
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


__all__ = [
    "BLOCKED_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_state_writer_blocked_executor",
    "write_v1_5_authoritative_resume_state_writer_blocked_executor_outputs",
]
