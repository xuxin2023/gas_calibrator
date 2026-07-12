"""Preview the next canonical step from a verified offline-advanced resume state."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.v1_5.orchestration.full_flow import PLAN_SCHEMA

from .v1_5_authoritative_resume_offline_state_advance_consumer_readiness import (
    READY_STATUS as CONSUMER_READY_STATUS,
    SCHEMA as CONSUMER_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness,
)
from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    _contains_reparse,
)
from .v1_5_formal_flow_contract import (
    FORMAL_CO2_FORBIDDEN_FLAGS,
    FORMAL_CO2_TEMPERATURE_ORDER,
    FORMAL_H2O_FORBIDDEN_FLAGS,
    FORMAL_H2O_TEMPERATURE_ORDER,
)

SCHEMA = "v1_5_authoritative_resume_offline_state_advance_next_step_plan_v1"
READY_STATUS = "ready_for_offline_advanced_resume_next_step_plan_review"
BLOCKED_STATUS = "blocked"
CONSUMER_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_consumer_readiness.json"
)

MATURE_ROUTE_MODULES = {
    "co2_open_flow_sampling": (
        "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
    ),
    "h2o_open_flow_sampling": (
        "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue"
    ),
}

CONSUMER_COMPARE_KEYS = (
    "overall_status",
    "resume_state_consumer_readiness_ready",
    "blocker_count",
    "blocker_reasons",
    "post_write_verification_json",
    "post_write_verification_sha256",
    "atomic_write_json",
    "atomic_write_sha256",
    "full_flow_plan_json",
    "full_flow_plan_sha256",
    "authoritative_state_json",
    "authoritative_state_sha256",
    "run_id",
    "attempt_id",
    "verified_step_id",
    "completed_step_ids",
    "next_step_id",
    "state_consumption_allowed",
    "execution_supported",
    "resume_execution_allowed",
    "would_execute",
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
    "not_real_acceptance_evidence",
)


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def _command_value_after(command: list[str], flag: str) -> str:
    try:
        index = command.index(flag)
    except ValueError:
        return ""
    return command[index + 1] if index + 1 < len(command) else ""


def build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
    *, consumer_readiness_json: str | Path
) -> dict[str, Any]:
    consumer_path = Path(consumer_readiness_json).absolute()
    consumer = _load(consumer_path)
    reasons: list[str] = []

    if consumer_path.name != CONSUMER_FILENAME:
        reasons.append("consumer_readiness_filename_not_canonical")
    if _contains_reparse(consumer_path):
        reasons.append("consumer_readiness_path_contains_reparse_point")
    if consumer.get("schema") != CONSUMER_SCHEMA:
        reasons.append("consumer_readiness_schema_invalid")
    if consumer.get("overall_status") != CONSUMER_READY_STATUS:
        reasons.append("consumer_readiness_not_ready")
    if consumer.get("resume_state_consumer_readiness_ready") is not True:
        reasons.append("consumer_readiness_ready_flag_not_true")
    if consumer.get("state_consumption_allowed") is not True:
        reasons.append("state_consumption_not_allowed")
    for field in (
        "execution_supported",
        "resume_execution_allowed",
        "would_execute",
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
    ):
        if consumer.get(field) is not False:
            reasons.append(f"consumer_readiness_{field}_not_false")
    if consumer.get("not_real_acceptance_evidence") is not True:
        reasons.append("consumer_readiness_real_acceptance_boundary_missing")

    verification_path = Path(
        str(consumer.get("post_write_verification_json") or "")
    ).absolute()
    try:
        recomputed = (
            build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
                post_write_verification_json=verification_path
            )
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if not recomputed:
        reasons.append("consumer_readiness_recompute_failed")
    elif any(
        consumer.get(key) != recomputed.get(key) for key in CONSUMER_COMPARE_KEYS
    ):
        reasons.append("consumer_readiness_recompute_mismatch")

    plan_path = Path(str(consumer.get("full_flow_plan_json") or "")).absolute()
    state_path = Path(
        str(consumer.get("authoritative_state_json") or "")
    ).absolute()
    plan = _load(plan_path)
    state = _load(state_path)
    if _contains_reparse(plan_path):
        reasons.append("full_flow_plan_path_contains_reparse_point")
    if _contains_reparse(state_path):
        reasons.append("authoritative_state_path_contains_reparse_point")
    if plan.get("schema") != PLAN_SCHEMA:
        reasons.append("full_flow_plan_schema_invalid")
    if _sha(plan_path) != str(consumer.get("full_flow_plan_sha256") or ""):
        reasons.append("full_flow_plan_sha256_mismatch")
    if _sha(state_path) != str(consumer.get("authoritative_state_sha256") or ""):
        reasons.append("authoritative_state_sha256_mismatch")
    if state.get("schema") != "v1_5_full_calibration_flow_state_v0":
        reasons.append("authoritative_state_schema_invalid")
    if str(plan.get("run_id") or "") != str(consumer.get("run_id") or ""):
        reasons.append("full_flow_plan_run_id_mismatch")
    if str(state.get("run_id") or "") != str(consumer.get("run_id") or ""):
        reasons.append("authoritative_state_run_id_mismatch")

    step_rows = [
        dict(row) for row in plan.get("steps") or [] if isinstance(row, Mapping)
    ]
    step_ids = [str(row.get("step_id") or "") for row in step_rows]
    completed = [str(value) for value in state.get("completed_step_ids") or []]
    if completed != step_ids[: len(completed)]:
        reasons.append("completed_steps_not_exact_contiguous_prefix")
    next_step_id = str(consumer.get("next_step_id") or "")
    expected_next = step_ids[len(completed)] if len(completed) < len(step_ids) else ""
    if not expected_next:
        reasons.append("next_step_missing_after_completed_prefix")
    if next_step_id != expected_next:
        reasons.append("consumer_next_step_mismatch")
    if str(state.get("current_step_id") or "") != expected_next:
        reasons.append("authoritative_state_current_step_mismatch")
    matches = [row for row in step_rows if row.get("step_id") == expected_next]
    if len(matches) != 1:
        reasons.append("next_step_not_unique_in_plan")
    step = matches[0] if len(matches) == 1 else {}

    tool_module = str(step.get("tool_module") or "")
    command = [str(value) for value in step.get("command") or []]
    if step and tool_module not in command:
        reasons.append("next_step_command_tool_module_mismatch")
    if step and step.get("uses_validated_v1_5_entry") is not True:
        reasons.append("next_step_not_validated_v1_5_entry")
    expected_route_module = MATURE_ROUTE_MODULES.get(expected_next)
    if expected_route_module and tool_module != expected_route_module:
        reasons.append(f"mature_route_tool_module_mismatch:{expected_next}")
    if expected_route_module:
        if step.get("execution_mode") != "real_com_route_requires_authorization":
            reasons.append("mature_route_execution_mode_invalid")
        if step.get("opens_com_ports") is not True:
            reasons.append("mature_route_com_boundary_missing")
        if expected_next == "co2_open_flow_sampling":
            if step.get("controls_gas_route") is not True:
                reasons.append("mature_co2_route_control_boundary_missing")
            if step.get("controls_water_route") is not False:
                reasons.append("mature_co2_water_route_boundary_invalid")
        if expected_next == "h2o_open_flow_sampling":
            if step.get("controls_water_route") is not True:
                reasons.append("mature_h2o_route_control_boundary_missing")
            if step.get("controls_gas_route") is not False:
                reasons.append("mature_h2o_gas_route_boundary_invalid")
        config_path = _command_value_after(command, "--config").replace("\\", "/")
        if not config_path.endswith(
            "coefficient_epoch_0_getco_snapshot/runtime_identity_bound_config.json"
        ):
            reasons.append("mature_route_not_runtime_identity_bound")
        expected_order = (
            FORMAL_CO2_TEMPERATURE_ORDER
            if expected_next == "co2_open_flow_sampling"
            else FORMAL_H2O_TEMPERATURE_ORDER
        )
        if _command_value_after(command, "--temperature-order") != expected_order:
            reasons.append("mature_route_temperature_order_invalid")
        forbidden_flags = (
            FORMAL_CO2_FORBIDDEN_FLAGS
            if expected_next == "co2_open_flow_sampling"
            else FORMAL_H2O_FORBIDDEN_FLAGS
        )
        for flag in sorted(forbidden_flags):
            if flag in command:
                reasons.append(f"mature_route_forbidden_flag:{flag}")
        if expected_next == "co2_open_flow_sampling":
            ratio_policy = _command_value_after(
                command, "--co2-ratio-f-preseal-policy"
            )
            if ratio_policy and ratio_policy != "reject":
                reasons.append("mature_co2_ratio_gate_policy_invalid")
        normalized_command = [value.replace("\\", "/").lower() for value in command]
        if any(".v2" in value for value in normalized_command):
            reasons.append("mature_route_v2_reference_forbidden")
        if any("/_handoff/" in f"/{value.strip('/')}" for value in normalized_command):
            reasons.append("mature_route_handoff_reference_forbidden")

    ready = not reasons
    requires_route = bool(
        step.get("controls_gas_route") or step.get("controls_water_route")
    )
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "next_step_plan_review_ready": ready,
        "blocker_count": len(reasons),
        "blocker_reasons": reasons,
        "consumer_readiness_json": str(consumer_path),
        "consumer_readiness_sha256": _sha(consumer_path),
        "post_write_verification_json": str(verification_path),
        "post_write_verification_sha256": _sha(verification_path),
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha(plan_path),
        "authoritative_state_json": str(state_path),
        "authoritative_state_sha256": _sha(state_path),
        "run_id": str(consumer.get("run_id") or ""),
        "attempt_id": str(consumer.get("attempt_id") or ""),
        "verified_step_id": str(consumer.get("verified_step_id") or ""),
        "completed_step_ids": completed,
        "next_step_id": expected_next,
        "next_step_title": str(step.get("title") or ""),
        "next_step_phase": str(step.get("phase") or ""),
        "next_step_tool_module": tool_module,
        "next_step_command": command,
        "next_step_execution_mode": str(step.get("execution_mode") or ""),
        "requires_real_com_authorization": bool(step.get("opens_com_ports")),
        "requires_pressure_authorization": bool(step.get("controls_pressure")),
        "requires_route_authorization": requires_route,
        "requires_write_authorization": bool(
            step.get("writes_coefficients") or step.get("writes_device_id")
        ),
        "mature_route_module_verified": bool(
            expected_route_module and tool_module == expected_route_module
        ),
        "plan_consumption_allowed": ready,
        "execution_supported": False,
        "next_step_execution_allowed": False,
        "resume_execution_allowed": False,
        "would_execute": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_authoritative_state": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def write_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
    model: Mapping[str, Any], output_dir: str | Path
) -> Path:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = (
        out
        / "v1_5_authoritative_resume_offline_state_advance_next_step_plan.json"
    )
    path.write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


__all__ = [
    "BLOCKED_STATUS",
    "CONSUMER_COMPARE_KEYS",
    "READY_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_state_advance_next_step_plan",
    "write_v1_5_authoritative_resume_offline_state_advance_next_step_plan",
]
