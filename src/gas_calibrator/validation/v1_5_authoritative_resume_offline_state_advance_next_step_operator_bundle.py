"""Build and optionally execute one complete V1.5 next-step operator bundle."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from .v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor import (
    run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor,
    write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor,
)
from .v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization import (
    AUTHORIZATION_FILENAME,
    AUTHORIZATION_OPERATION,
    AUTHORIZATION_SCHEMA,
    CONFIRMATION_TEMPLATE,
    MAX_AUTHORIZATION_TTL_S,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization,
    write_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization,
)
from .v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight import (
    build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight,
    write_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight,
)
from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    _contains_reparse,
)

SCHEMA = "v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle_v1"
PREPARED_STATUS = "operator_bundle_prepared_execution_locked"
EXECUTED_STATUS = "operator_bundle_process_completed_pending_post_verification"
HOLD_STATUS = "hold"


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return (
        value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _sha(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""
    except OSError:
        return ""


def _command_sha(command: list[str]) -> str:
    normalized = json.dumps(command, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def build_v1_5_next_step_execution_authorization_packet(
    *,
    controlled_executor_design_json: str | Path,
    authorization_id: str,
    operator: str,
    reviewer: str,
    approver: str,
    ttl_s: float = 900.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    issued = (now or _now()).astimezone(UTC).replace(microsecond=0)
    ttl = float(ttl_s)
    expires = issued + timedelta(seconds=ttl)
    design_path = Path(controlled_executor_design_json).absolute()
    design = _load(design_path)
    blocked_path = Path(
        str(design.get("next_step_blocked_executor_json") or "")
    ).absolute()
    blocked = _load(blocked_path)
    review_path = Path(
        str(blocked.get("next_step_authorization_preflight_json") or "")
    ).absolute()
    plan_path = Path(str(blocked.get("next_step_plan_json") or "")).absolute()
    plan = _load(plan_path)
    consumer_path = Path(str(plan.get("consumer_readiness_json") or "")).absolute()
    full_flow_path = Path(str(plan.get("full_flow_plan_json") or "")).absolute()
    state_path = Path(str(plan.get("authoritative_state_json") or "")).absolute()
    command = [str(value) for value in plan.get("next_step_command") or []]
    return {
        "schema": AUTHORIZATION_SCHEMA,
        "requested_operation": AUTHORIZATION_OPERATION,
        "confirmation_template": CONFIRMATION_TEMPLATE,
        "authorization_id": str(authorization_id),
        "issued_at": _iso(issued),
        "expires_at": _iso(expires),
        "requested_ttl_s": ttl,
        "maximum_ttl_s": MAX_AUTHORIZATION_TTL_S,
        "operator": str(operator).strip(),
        "reviewer": str(reviewer).strip(),
        "approver": str(approver).strip(),
        "controlled_executor_design_json": str(design_path),
        "controlled_executor_design_sha256": _sha(design_path),
        "blocked_executor_json": str(blocked_path),
        "blocked_executor_sha256": _sha(blocked_path),
        "review_authorization_preflight_json": str(review_path),
        "review_authorization_preflight_sha256": _sha(review_path),
        "next_step_plan_json": str(plan_path),
        "next_step_plan_sha256": _sha(plan_path),
        "consumer_readiness_json": str(consumer_path),
        "consumer_readiness_sha256": _sha(consumer_path),
        "full_flow_plan_json": str(full_flow_path),
        "full_flow_plan_sha256": _sha(full_flow_path),
        "authoritative_state_json": str(state_path),
        "authoritative_state_sha256": _sha(state_path),
        "run_id": str(plan.get("run_id") or ""),
        "attempt_id": str(plan.get("attempt_id") or ""),
        "verified_step_id": str(plan.get("verified_step_id") or ""),
        "next_step_id": str(plan.get("next_step_id") or ""),
        "next_step_tool_module": str(plan.get("next_step_tool_module") or ""),
        "next_step_command_sha256": _command_sha(command),
        "structured_confirmation": {
            "exact_one_step_only": True,
            "no_substitute_entry": True,
            "no_shell": True,
            "no_executor_retry": True,
            "no_fallback": True,
            "no_automatic_state_advance": True,
            "mature_runner_owns_physics_and_qc": True,
            "failure_holds": True,
            "no_postgresql_or_release": True,
        },
        "allow_real_com": bool(plan.get("requires_real_com_authorization")),
        "allow_pressure_control": bool(plan.get("requires_pressure_authorization")),
        "allow_route_control": bool(plan.get("requires_route_authorization")),
        "allow_device_or_coefficient_write": bool(
            plan.get("requires_write_authorization")
        ),
        "allow_postgresql_import": False,
        "packet_generated_from_exact_plan": True,
        "capabilities_derived_not_operator_selected": True,
        "not_real_acceptance_evidence": True,
    }


def run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle(
    *,
    controlled_executor_design_json: str | Path,
    authorization_id: str,
    operator: str,
    reviewer: str,
    approver: str,
    output_dir: str | Path,
    ttl_s: float = 900.0,
    execute_next_step: bool = False,
    expected_attempt_id: str = "",
    operator_confirmation_text: str = "",
    timeout_s: float = 86400.0,
    now: datetime | None = None,
    subprocess_runner=subprocess.run,
) -> dict[str, Any]:
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    output = Path(output_dir).absolute()
    if _contains_reparse(output):
        raise ValueError(
            "operator bundle output directory must not contain a reparse point"
        )
    if output.exists() and any(output.iterdir()):
        raise ValueError("operator bundle output directory must be absent or empty")
    authorization_dir = output / "01_execution_authorization"
    validation_dir = output / "02_authorization_validation"
    preflight_dir = output / "03_immediate_execution_preflight"
    executor_dir = output / "04_controlled_executor"
    authorization_path = authorization_dir / AUTHORIZATION_FILENAME

    packet = build_v1_5_next_step_execution_authorization_packet(
        controlled_executor_design_json=controlled_executor_design_json,
        authorization_id=authorization_id,
        operator=operator,
        reviewer=reviewer,
        approver=approver,
        ttl_s=ttl_s,
        now=evaluated_at,
    )
    authorization_dir.mkdir(parents=True, exist_ok=True)
    authorization_path.write_text(
        json.dumps(packet, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    validation = build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization(
        controlled_executor_design_json=controlled_executor_design_json,
        execution_authorization_json=authorization_path,
        now=evaluated_at,
    )
    validation_paths = write_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization(
        validation, validation_dir
    )
    preflight = build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight(
        execution_authorization_validation_json=validation_paths["json"],
        now=evaluated_at,
    )
    preflight_paths = write_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight(
        preflight, preflight_dir
    )
    executor = run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
        next_step_execution_preflight_json=preflight_paths["json"],
        execute_next_step=execute_next_step,
        expected_attempt_id=expected_attempt_id,
        operator_confirmation_text=operator_confirmation_text,
        timeout_s=timeout_s,
        now=evaluated_at,
        subprocess_runner=subprocess_runner,
    )
    executor_paths = write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
        executor, executor_dir
    )
    completed = bool(executor.get("next_step_process_completed"))
    hold_reasons = list(
        dict.fromkeys(
            [
                *[str(value) for value in validation.get("review_reasons") or []],
                *[str(value) for value in preflight.get("hold_reasons") or []],
                *[str(value) for value in executor.get("hold_reasons") or []],
            ]
        )
    )
    if completed:
        status = EXECUTED_STATUS
    elif execute_next_step or hold_reasons:
        status = HOLD_STATUS
    else:
        status = PREPARED_STATUS
    manifest = {
        "schema": SCHEMA,
        "generated_at": _iso(evaluated_at),
        "overall_status": status,
        "operator_bundle_prepared": not hold_reasons,
        "execution_requested": execute_next_step,
        "execution_attempted": bool(executor.get("execution_attempted")),
        "next_step_process_completed": completed,
        "hold_count": len(hold_reasons),
        "hold_reasons": hold_reasons,
        "authorization_id": str(authorization_id),
        "authorization_expires_at": packet.get("expires_at"),
        "run_id": packet.get("run_id"),
        "attempt_id": packet.get("attempt_id"),
        "verified_step_id": packet.get("verified_step_id"),
        "next_step_id": packet.get("next_step_id"),
        "next_step_tool_module": packet.get("next_step_tool_module"),
        "authorized_capabilities": {
            field: packet.get(field)
            for field in (
                "allow_real_com",
                "allow_pressure_control",
                "allow_route_control",
                "allow_device_or_coefficient_write",
                "allow_postgresql_import",
            )
        },
        "execution_authorization_json": str(authorization_path),
        "execution_authorization_sha256": _sha(authorization_path),
        "authorization_validation_json": str(validation_paths["json"]),
        "authorization_validation_sha256": _sha(validation_paths["json"]),
        "execution_preflight_json": str(preflight_paths["json"]),
        "execution_preflight_sha256": _sha(preflight_paths["json"]),
        "executor_evidence_index_json": str(
            executor_paths["post_execution_evidence_index"]
        ),
        "executor_evidence_index_sha256": _sha(
            executor_paths["post_execution_evidence_index"]
        ),
        "shell_used": False,
        "executor_retry_count": 0,
        "fallback_entry_used": False,
        "authoritative_state_advanced": False,
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "next_action": (
            "Run separate post-execution verification before state advance."
            if completed
            else "Keep execution locked or review hold reasons; do not reuse expired authorization."
        ),
    }
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "v1_5_next_step_operator_bundle.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    manifest["manifest_json"] = str(manifest_path)
    manifest["manifest_sha256"] = _sha(manifest_path)
    return manifest


__all__ = [
    "EXECUTED_STATUS",
    "HOLD_STATUS",
    "PREPARED_STATUS",
    "SCHEMA",
    "build_v1_5_next_step_execution_authorization_packet",
    "run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle",
]
