"""Review the future authoritative V1.5 resume-state writer without writing state."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_authoritative_resume_state_writer_design_v1"
READY_STATUS = "ready_for_authoritative_resume_state_writer_design_review"
BLOCKED_STATUS = "blocked"

APPLICATION_SCHEMA = "v1_5_resume_prefix_application_review_v1"
APPLICATION_READY_STATUS = "ready_for_resume_prefix_state_application_review"
RESUME_GATE_STEP_ID = "post_closeout_resume_gate_snapshot"
APPLICATION_STEP_ID = "post_closeout_resume_prefix_application_review"
WRITER_DESIGN_STEP_ID = "authoritative_resume_state_writer_design"
NEXT_STEP_ID = "temperature_channel_fast_review"
CO2_STEP_ID = "co2_open_flow_sampling"
H2O_STEP_ID = "h2o_open_flow_sampling"
WRITER_DESIGN_MODULE = (
    "gas_calibrator.tools.export_v1_5_authoritative_resume_state_writer_design"
)

FORBIDDEN_FLAGS = (
    "--completed-step",
    "--failed-step",
    "--execute",
    "--execute-offline-commands",
    "--supervised-run-ready-offline",
    "--allow-real-com",
    "--allow-pressure-control",
    "--allow-route-control",
    "--allow-writes",
    "--allow-database-import",
    "--write-state",
    "--replace-state",
)

FORBIDDEN_REFERENCE_TOKENS = (
    "_handoff",
    "formal_queue_migration_20260624",
    "0624",
    "diagnostic",
    "worker",
    "gas_calibrator.v1.",
    "gas_calibrator.v2.",
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_mapping(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return dict(payload)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _step_rows(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in plan.get("steps") or [] if isinstance(row, Mapping)]


def _command_text(step: Mapping[str, Any]) -> str:
    command = step.get("command") or []
    if isinstance(command, str):
        return command
    return " ".join(str(item) for item in command)


def _command_value(step: Mapping[str, Any], flag: str) -> str:
    command = step.get("command") or []
    if isinstance(command, str):
        return ""
    values = [str(item) for item in command]
    try:
        return values[values.index(flag) + 1]
    except (ValueError, IndexError):
        return ""


def _same_resolved_path(value: Any, expected: Path) -> bool:
    if not value:
        return False
    try:
        return Path(str(value)).resolve() == expected.resolve()
    except (OSError, RuntimeError):
        return False


def _flatten_completed_steps(step_ids: Sequence[str]) -> list[str]:
    return [item for step_id in step_ids for item in ("--completed-step", step_id)]


def _application_boundary_clean(payload: Mapping[str, Any]) -> bool:
    return (
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


def build_v1_5_authoritative_resume_state_writer_design(
    *,
    full_flow_plan_json: str | Path,
    resume_prefix_application_review_json: str | Path,
) -> dict[str, Any]:
    plan_path = Path(full_flow_plan_json).resolve()
    application_path = Path(resume_prefix_application_review_json).resolve()
    plan = _load_mapping(plan_path)
    application = _load_mapping(application_path)
    steps = _step_rows(plan)
    step_ids = [str(row.get("step_id") or "") for row in steps]
    reasons: list[str] = []

    if len(step_ids) != len(set(step_ids)):
        reasons.append("duplicate_full_flow_step_ids")
    required_order = (
        RESUME_GATE_STEP_ID,
        APPLICATION_STEP_ID,
        WRITER_DESIGN_STEP_ID,
        NEXT_STEP_ID,
        CO2_STEP_ID,
        H2O_STEP_ID,
    )
    for step_id in required_order:
        if step_id not in step_ids:
            reasons.append(f"required_step_missing:{step_id}")
    if all(step_id in step_ids for step_id in required_order):
        indexes = [step_ids.index(step_id) for step_id in required_order]
        if indexes != sorted(indexes):
            reasons.append("authoritative_writer_design_order_invalid")
        gate_index = step_ids.index(RESUME_GATE_STEP_ID)
        application_index = step_ids.index(APPLICATION_STEP_ID)
        writer_index = step_ids.index(WRITER_DESIGN_STEP_ID)
        next_index = step_ids.index(NEXT_STEP_ID)
        if not (
            application_index == gate_index + 1
            and writer_index == application_index + 1
            and next_index == writer_index + 1
        ):
            reasons.append("authoritative_writer_design_steps_not_adjacent")

    by_id = {str(row.get("step_id") or ""): row for row in steps}
    writer_step = by_id.get(WRITER_DESIGN_STEP_ID) or {}
    if str(writer_step.get("tool_module") or "") != WRITER_DESIGN_MODULE:
        reasons.append("authoritative_writer_design_module_mismatch")
    if not str(writer_step.get("execution_mode") or "").startswith("offline"):
        reasons.append("authoritative_writer_design_not_offline")
    if any(
        bool(writer_step.get(key))
        for key in (
            "opens_com_ports",
            "controls_pressure",
            "controls_gas_route",
            "controls_water_route",
            "writes_device_id",
            "writes_coefficients",
        )
    ):
        reasons.append("authoritative_writer_design_side_effect_boundary_not_clean")
    if not _same_resolved_path(
        _command_value(writer_step, "--full-flow-plan-json"),
        plan_path,
    ):
        reasons.append("authoritative_writer_design_plan_path_mismatch")
    if not _same_resolved_path(
        _command_value(writer_step, "--resume-prefix-application-review-json"),
        application_path,
    ):
        reasons.append("authoritative_writer_design_application_path_mismatch")
    writer_command = [str(item) for item in writer_step.get("command") or []]
    if "--fail-on-blocked" not in writer_command:
        reasons.append("authoritative_writer_design_not_fail_closed")
    for flag in FORBIDDEN_FLAGS:
        if flag in writer_command:
            reasons.append(f"authoritative_writer_design_forbidden_flag:{flag}")

    for step_id in required_order:
        row = by_id.get(step_id) or {}
        surface = " ".join(
            (
                str(row.get("tool_module") or ""),
                _command_text(row),
                str(row.get("gate") or ""),
            )
        ).lower()
        for token in FORBIDDEN_REFERENCE_TOKENS:
            if token in surface:
                reasons.append(f"forbidden_authoritative_writer_surface:{step_id}:{token}")

    expected_after_application: list[str] = []
    expected_after_design: list[str] = []
    if APPLICATION_STEP_ID in step_ids:
        expected_after_application = step_ids[: step_ids.index(APPLICATION_STEP_ID) + 1]
    if WRITER_DESIGN_STEP_ID in step_ids:
        expected_after_design = step_ids[: step_ids.index(WRITER_DESIGN_STEP_ID) + 1]

    if application.get("schema") != APPLICATION_SCHEMA:
        reasons.append("resume_prefix_application_schema_mismatch")
    if application.get("overall_status") != APPLICATION_READY_STATUS:
        reasons.append("resume_prefix_application_status_not_ready")
    if application.get("resume_prefix_application_review_ready") is not True:
        reasons.append("resume_prefix_application_review_not_ready")
    if application.get("resume_prefix_consumed_for_review") is not True:
        reasons.append("resume_prefix_not_consumed_for_review")
    if application.get("state_preview_current_step_id") != WRITER_DESIGN_STEP_ID:
        reasons.append("resume_prefix_application_next_step_mismatch")
    if not _same_resolved_path(application.get("full_flow_plan_json"), plan_path):
        reasons.append("resume_prefix_application_plan_path_mismatch")
    if str(application.get("full_flow_plan_sha256") or "") != _sha256(plan_path):
        reasons.append("resume_prefix_application_plan_sha256_mismatch")
    if application.get("reviewed_completed_step_ids_after_application") != expected_after_application:
        reasons.append("resume_prefix_application_completed_steps_not_exact")
    if application.get("reviewed_state_application_cli_arguments") != _flatten_completed_steps(
        expected_after_application
    ):
        reasons.append("resume_prefix_application_cli_arguments_not_exact")
    if not _application_boundary_clean(application):
        reasons.append("resume_prefix_application_boundary_not_clean")

    resume_gate_value = str(application.get("post_closeout_resume_gate_json") or "")
    batch_value = str(application.get("batch_initialization_closeout_json") or "")
    resume_gate_path = Path(resume_gate_value).resolve() if resume_gate_value else None
    batch_path = Path(batch_value).resolve() if batch_value else None
    if resume_gate_path is None or not resume_gate_path.is_file():
        reasons.append("resume_gate_artifact_missing")
    elif str(application.get("post_closeout_resume_gate_sha256") or "") != _sha256(
        resume_gate_path
    ):
        reasons.append("resume_gate_artifact_sha256_mismatch")
    if batch_path is None or not batch_path.is_file():
        reasons.append("batch_closeout_artifact_missing")
    elif str(application.get("batch_initialization_closeout_sha256") or "") != _sha256(
        batch_path
    ):
        reasons.append("batch_closeout_artifact_sha256_mismatch")
    if str(application.get("run_id") or "") != str(plan.get("run_id") or ""):
        reasons.append("resume_prefix_application_run_id_mismatch")

    state_target = plan_path.parent / "v1_5_full_flow_state.json"
    state_markdown_target = plan_path.parent / "v1_5_full_flow_state.md"
    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "authoritative_resume_state_writer_design_ready": ready,
        "design_review_only": True,
        "execution_supported": False,
        "authoritative_state_write_allowed": False,
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha256(plan_path),
        "resume_prefix_application_review_json": str(application_path),
        "resume_prefix_application_review_sha256": _sha256(application_path),
        "post_closeout_resume_gate_json": str(resume_gate_path) if resume_gate_path else "",
        "post_closeout_resume_gate_sha256": str(
            application.get("post_closeout_resume_gate_sha256") or ""
        ),
        "batch_initialization_closeout_json": str(batch_path) if batch_path else "",
        "batch_initialization_closeout_sha256": str(
            application.get("batch_initialization_closeout_sha256") or ""
        ),
        "run_id": str(plan.get("run_id") or ""),
        "proposed_completed_step_ids": expected_after_design if ready else [],
        "proposed_completed_step_cli_arguments": (
            _flatten_completed_steps(expected_after_design) if ready else []
        ),
        "proposed_failed_step_ids": [],
        "proposed_current_step_id": NEXT_STEP_ID if ready else "",
        "proposed_authoritative_state_json": str(state_target),
        "proposed_authoritative_state_markdown": str(state_markdown_target),
        "proposed_authorization_state": {
            "allow_real_com": False,
            "allow_pressure_control": False,
            "allow_route_control": False,
            "allow_writes": False,
            "allow_database_import": False,
        },
        "transaction_contract": {
            "single_writer_lock_required": True,
            "existing_state_snapshot_required": True,
            "existing_state_sha256_compare_and_swap_required": True,
            "temporary_file_same_directory_required": True,
            "temporary_file_fsync_required": True,
            "atomic_replace_required": True,
            "post_replace_readback_and_sha256_required": True,
            "rollback_snapshot_required": True,
            "rollback_on_readback_mismatch_required": True,
            "run_id_must_match_plan": True,
            "completed_steps_must_equal_reviewed_contiguous_prefix": True,
            "symlink_or_reparse_target_forbidden": True,
        },
        "review_reasons": reasons,
        "does_not_execute_commands": True,
        "applies_completed_steps": False,
        "writes_authoritative_state": False,
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
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys or ["empty"])
        writer.writeheader()
        writer.writerows(dict(row) for row in rows)


def _markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 Authoritative Resume State Writer Design",
        "",
        f"- overall_status: `{model['overall_status']}`",
        f"- design_review_ready: `{str(model['authoritative_resume_state_writer_design_ready']).lower()}`",
        f"- run_id: `{model['run_id']}`",
        f"- proposed_current_step_id: `{model['proposed_current_step_id']}`",
        "",
        "## Transaction Contract",
        "",
    ]
    for key, value in model["transaction_contract"].items():
        lines.append(f"- {key}: `{str(value).lower()}`")
    if model["review_reasons"]:
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{reason}`" for reason in model["review_reasons"])
    lines.extend(
        [
            "",
            "## Locked Boundary",
            "",
            "This artifact defines the future writer contract only. It does not create, replace, or mutate the authoritative full-flow state.",
            "",
            "- execution_supported: `false`",
            "- authoritative_state_write_allowed: `false`",
            "- writes_authoritative_state: `false`",
            "- opens_com_ports: `false`",
            "- controls_pressure: `false`",
            "- controls_water_or_gas_routes: `false`",
            "- connects_postgresql: `false`",
            "- formal_release_allowed: `false`",
            "- database_import_allowed: `false`",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_authoritative_resume_state_writer_design(
    *,
    output_dir: str | Path,
    full_flow_plan_json: str | Path,
    resume_prefix_application_review_json: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_authoritative_resume_state_writer_design(
        full_flow_plan_json=full_flow_plan_json,
        resume_prefix_application_review_json=resume_prefix_application_review_json,
    )
    paths = {
        "manifest": out / "v1_5_authoritative_resume_state_writer_design.json",
        "state_preview": out / "v1_5_authoritative_resume_state_writer_design_preview.csv",
        "transaction_contract": out / "v1_5_authoritative_resume_state_transaction_contract.csv",
        "markdown": out / "V1_5_AUTHORITATIVE_RESUME_STATE_WRITER_DESIGN.md",
    }
    paths["manifest"].write_text(
        json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _write_csv(
        paths["state_preview"],
        [
            {"order": index, "step_id": step_id, "proposed_state": "completed"}
            for index, step_id in enumerate(model["proposed_completed_step_ids"], start=1)
        ],
    )
    _write_csv(
        paths["transaction_contract"],
        [
            {"requirement": key, "required": value}
            for key, value in model["transaction_contract"].items()
        ],
    )
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_state_writer_design",
    "write_v1_5_authoritative_resume_state_writer_design",
]
