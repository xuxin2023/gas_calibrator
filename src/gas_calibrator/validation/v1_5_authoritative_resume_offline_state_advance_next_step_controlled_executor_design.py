"""Design a future controlled executor for one verified V1.5 next step."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor import (
    BLOCKED_READY_STATUS,
    SCHEMA as BLOCKED_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor,
)
from .v1_5_authoritative_resume_offline_state_advance_next_step_plan import (
    READY_STATUS as PLAN_READY_STATUS,
    SCHEMA as PLAN_SCHEMA,
)
from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    _contains_reparse,
)

SCHEMA = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_controlled_executor_design_v1"
)
READY_STATUS = "ready_for_offline_advanced_resume_next_step_controlled_executor_design_review"
REVIEW_STATUS = "review_required"
FUTURE_AUTHORIZATION_SCHEMA = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_execution_authorization_v1"
)
BLOCKED_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_blocked_executor.json"
)


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _command_sha(command: Sequence[Any]) -> str:
    normalized = json.dumps(
        [str(value) for value in command],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design(
    *,
    next_step_blocked_executor_json: str | Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    blocked_path = Path(next_step_blocked_executor_json).absolute()
    blocked = _load(blocked_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    reasons: list[str] = []
    if blocked_path.name != BLOCKED_FILENAME:
        reasons.append("next_step_blocked_executor_filename_not_canonical")
    if _contains_reparse(blocked_path):
        reasons.append("next_step_blocked_executor_path_contains_reparse_point")
    if blocked.get("schema") != BLOCKED_SCHEMA:
        reasons.append("next_step_blocked_executor_schema_invalid")
    if blocked.get("overall_status") != BLOCKED_READY_STATUS:
        reasons.append("next_step_blocked_executor_status_invalid")
    if blocked.get("blocked_executor_ready") is not True:
        reasons.append("next_step_blocked_executor_ready_flag_not_true")
    if int(blocked.get("review_required_count") or 0) or blocked.get(
        "review_reasons"
    ):
        reasons.append("next_step_blocked_executor_contains_review_reasons")
    for field in (
        "execution_supported",
        "next_step_execution_allowed",
        "resume_execution_allowed",
        "execute_flag_allowed",
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
        if blocked.get(field) is not False:
            reasons.append(f"next_step_blocked_executor_boundary_invalid:{field}")
    if blocked.get("not_real_acceptance_evidence") is not True:
        reasons.append(
            "next_step_blocked_executor_boundary_invalid:not_real_acceptance_evidence"
        )
    try:
        recomputed = build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor(
            next_step_authorization_preflight_json=blocked.get(
                "next_step_authorization_preflight_json"
            ),
            now=evaluated_at,
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    exact = (
        bool(recomputed)
        and {key: value for key, value in blocked.items() if key != "generated_at"}
        == {key: value for key, value in recomputed.items() if key != "generated_at"}
    )
    if not recomputed:
        reasons.append("next_step_blocked_executor_recompute_failed")
    elif not exact:
        reasons.append("next_step_blocked_executor_recompute_mismatch")

    plan_path = Path(str(blocked.get("next_step_plan_json") or "")).absolute()
    plan = _load(plan_path)
    if _contains_reparse(plan_path):
        reasons.append("next_step_plan_path_contains_reparse_point")
    if _sha(plan_path) != str(blocked.get("next_step_plan_sha256") or ""):
        reasons.append("next_step_plan_sha256_mismatch")
    if plan.get("schema") != PLAN_SCHEMA or plan.get("overall_status") != PLAN_READY_STATUS:
        reasons.append("next_step_plan_not_ready")
    next_step_id = str(plan.get("next_step_id") or "")
    next_step_module = str(plan.get("next_step_tool_module") or "")
    next_step_command = [str(value) for value in plan.get("next_step_command") or []]
    if next_step_id != str(blocked.get("next_step_id") or ""):
        reasons.append("next_step_id_mismatch_with_blocked_executor")
    if next_step_module != str(blocked.get("next_step_tool_module") or ""):
        reasons.append("next_step_module_mismatch_with_blocked_executor")
    if not next_step_command or next_step_module not in next_step_command:
        reasons.append("next_step_command_missing_exact_tool_module")

    authorization_contract = [
        {
            "field": "authorization_id_and_three_distinct_identities",
            "required": True,
            "rule": "Bind a unique authorization id to distinct operator, reviewer, and approver identities.",
        },
        {
            "field": "issued_at_and_expires_at",
            "required": True,
            "rule": "Use a short UTC lifetime and reject missing, future, or expired authorization.",
        },
        {
            "field": "complete_evidence_chain",
            "required": True,
            "rule": "Bind blocked proof, review preflight, plan, consumer, run, attempt, verified step, and current state SHA256 values.",
        },
        {
            "field": "exact_next_step_command",
            "required": True,
            "rule": "Bind next-step id, mature tool module, normalized command SHA256, and runtime identity config.",
        },
        {
            "field": "least_privilege_capabilities",
            "required": True,
            "rule": "Grant only capabilities explicitly required by the exact canonical step; database import is always separate.",
        },
        {
            "field": "operator_confirmation",
            "required": True,
            "rule": "Confirm one-step execution, no fallback entry, no automatic retry, no state advance, and failure hold.",
        },
    ]
    requirements = {
        "real_com": bool(plan.get("requires_real_com_authorization")),
        "pressure_control": bool(plan.get("requires_pressure_authorization")),
        "route_control": bool(plan.get("requires_route_authorization")),
        "device_or_coefficient_write": bool(plan.get("requires_write_authorization")),
        "postgresql_import": False,
    }
    capability_contract = [
        {
            "capability": capability,
            "required_by_exact_next_step": required,
            "future_authorization_field": f"allow_{capability}",
            "default": False,
            "rule": (
                "May be true only when required by the hash-bound canonical step."
                if capability != "postgresql_import"
                else "Never granted by next-step execution authorization."
            ),
        }
        for capability, required in requirements.items()
    ]
    hold_contract = [
        {
            "trigger": "evidence_path_hash_or_recompute_mismatch",
            "action": "hold_before_process_start",
            "meaning": "Reject copied ready flags, changed state, changed plan, reparse paths, or stale authorization.",
        },
        {
            "trigger": "next_step_id_module_command_or_runtime_config_mismatch",
            "action": "hold_before_process_start",
            "meaning": "Run only the exact mature V1.5 command that #111 reviewed.",
        },
        {
            "trigger": "v1_v2_0624_migration_diagnostic_worker_or_handoff_reference",
            "action": "hold_before_process_start",
            "meaning": "Never substitute legacy, V2, migrated, diagnostic, worker, or evidence-area entrypoints.",
        },
        {
            "trigger": "authorization_missing_expired_identity_conflict_or_excess_capability",
            "action": "hold_before_process_start",
            "meaning": "No implicit, stale, self-approved, or broad physical authority.",
        },
        {
            "trigger": "child_process_launch_or_exit_failure",
            "action": "stop_without_retry_or_fallback",
            "meaning": "Record the failure and require a fresh plan and authorization.",
        },
        {
            "trigger": "pace_vent_pressure_dewpoint_ratio_qc_or_device_gate_failure",
            "action": "preserve_mature_runner_failure_and_hold",
            "meaning": "Do not bypass or reinterpret mature physical and per-device quality gates.",
        },
        {
            "trigger": "missing_expected_output_or_output_hash",
            "action": "hold_post_execution_verification",
            "meaning": "No successful completion or state advance without complete fresh output evidence.",
        },
        {
            "trigger": "partial_side_effect_or_operator_abort",
            "action": "safe_stop_and_require_new_evidence",
            "meaning": "Never silently retry, continue to another point, or advance authoritative state.",
        },
    ]
    evidence_contract = [
        {"artifact": "executor_invocation.json", "required": True},
        {"artifact": "pre_execution_revalidation.json", "required": True},
        {"artifact": "command_attempts.csv", "required": True},
        {"artifact": "child_process_result.json", "required": True},
        {"artifact": "hold_events.csv", "required": True},
        {"artifact": "post_execution_evidence_index.json", "required": True},
    ]
    ready = not reasons
    manifest = {
        "schema": SCHEMA,
        "generated_at": _iso(evaluated_at),
        "overall_status": READY_STATUS if ready else REVIEW_STATUS,
        "controlled_next_step_executor_design_ready": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "production_state": "blocked_design_only",
        "next_step_blocked_executor_json": str(blocked_path),
        "next_step_blocked_executor_sha256": _sha(blocked_path),
        "next_step_plan_json": str(plan_path),
        "next_step_plan_sha256": _sha(plan_path),
        "future_authorization_schema": FUTURE_AUTHORIZATION_SCHEMA,
        "next_step_id_recorded_only": next_step_id,
        "next_step_tool_module_recorded_only": next_step_module,
        "next_step_command_sha256_recorded_only": _command_sha(next_step_command),
        "single_exact_command_only": True,
        "shell_execution_allowed": False,
        "automatic_retry_allowed": False,
        "fallback_entry_allowed": False,
        "automatic_state_advance_allowed": False,
        "execution_supported": False,
        "next_step_execution_allowed": False,
        "resume_execution_allowed": False,
        "execute_flag_allowed": False,
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
        "next_action": (
            "Keep execution locked. Build one last-moment authorization/preflight "
            "validator before any controlled executor implementation."
        ),
    }
    return {
        "manifest": manifest,
        "authorization_contract": authorization_contract,
        "capability_contract": capability_contract,
        "hold_contract": hold_contract,
        "evidence_contract": evidence_contract,
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(dict(row) for row in rows)


def write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = (
        "v1_5_authoritative_resume_offline_state_advance_"
        "next_step_controlled_executor_design"
    )
    outputs = {
        "manifest": out / f"{stem}.json",
        "authorization_contract": out / f"{stem}_authorization_contract.csv",
        "capability_contract": out / f"{stem}_capability_contract.csv",
        "hold_contract": out / f"{stem}_hold_matrix.csv",
        "evidence_contract": out / f"{stem}_evidence_contract.csv",
        "markdown": out
        / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_CONTROLLED_EXECUTOR_DESIGN.md",
    }
    outputs["manifest"].write_text(
        json.dumps(dict(model["manifest"]), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    for key in (
        "authorization_contract",
        "capability_contract",
        "hold_contract",
        "evidence_contract",
    ):
        _write_csv(outputs[key], model[key])
    manifest = model["manifest"]
    outputs["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 Offline Next-Step Controlled Executor Design",
                "",
                "This is an offline design, not an executor.",
                "",
                f"- overall_status: `{manifest.get('overall_status')}`",
                f"- production_state: `{manifest.get('production_state')}`",
                f"- execution_supported: `{manifest.get('execution_supported')}`",
                f"- next_step_execution_allowed: `{manifest.get('next_step_execution_allowed')}`",
                f"- automatic_retry_allowed: `{manifest.get('automatic_retry_allowed')}`",
                f"- fallback_entry_allowed: `{manifest.get('fallback_entry_allowed')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return outputs


__all__ = [
    "FUTURE_AUTHORIZATION_SCHEMA",
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design",
    "write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design",
]
