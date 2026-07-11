"""Review a V1.5 post-closeout resume prefix without applying state."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_resume_prefix_application_review_v1"
READY_STATUS = "ready_for_resume_prefix_state_application_review"
BLOCKED_STATUS = "blocked"

RESUME_GATE_SCHEMA = "v1_5_post_closeout_resume_gate_v1"
RESUME_GATE_READY_STATUS = "ready_for_post_closeout_resume_review"
RESUME_GATE_STEP_ID = "post_closeout_resume_gate_snapshot"
APPLICATION_REVIEW_STEP_ID = "post_closeout_resume_prefix_application_review"
NEXT_STEP_ID = "temperature_channel_fast_review"
CO2_STEP_ID = "co2_open_flow_sampling"
H2O_STEP_ID = "h2o_open_flow_sampling"
APPLICATION_REVIEW_MODULE = "gas_calibrator.tools.export_v1_5_resume_prefix_application_review"

FORBIDDEN_APPLICATION_FLAGS = (
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


def _boundary_clean(payload: Mapping[str, Any]) -> bool:
    return (
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


def build_v1_5_resume_prefix_application_review(
    *,
    full_flow_plan_json: str | Path,
    post_closeout_resume_gate_json: str | Path,
) -> dict[str, Any]:
    plan_path = Path(full_flow_plan_json).resolve()
    gate_path = Path(post_closeout_resume_gate_json).resolve()
    plan = _load_mapping(plan_path)
    gate = _load_mapping(gate_path)
    steps = _step_rows(plan)
    step_ids = [str(row.get("step_id") or "") for row in steps]
    reasons: list[str] = []

    if len(step_ids) != len(set(step_ids)):
        reasons.append("duplicate_full_flow_step_ids")
    required_order = (
        RESUME_GATE_STEP_ID,
        APPLICATION_REVIEW_STEP_ID,
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
            reasons.append("resume_prefix_application_order_invalid")

    by_id = {str(row.get("step_id") or ""): row for row in steps}
    application_step = by_id.get(APPLICATION_REVIEW_STEP_ID) or {}
    if str(application_step.get("tool_module") or "") != APPLICATION_REVIEW_MODULE:
        reasons.append("resume_prefix_application_review_module_mismatch")
    if not str(application_step.get("execution_mode") or "").startswith("offline"):
        reasons.append("resume_prefix_application_review_not_offline")
    if any(
        bool(application_step.get(key))
        for key in (
            "opens_com_ports",
            "controls_pressure",
            "controls_gas_route",
            "controls_water_route",
            "writes_device_id",
            "writes_coefficients",
        )
    ):
        reasons.append("resume_prefix_application_review_side_effect_boundary_not_clean")
    if not _same_resolved_path(
        _command_value(application_step, "--full-flow-plan-json"),
        plan_path,
    ):
        reasons.append("application_review_plan_path_mismatch_with_full_flow_plan")
    if not _same_resolved_path(
        _command_value(application_step, "--post-closeout-resume-gate-json"),
        gate_path,
    ):
        reasons.append("application_review_gate_path_mismatch_with_full_flow_plan")
    if "--fail-on-blocked" not in (application_step.get("command") or []):
        reasons.append("resume_prefix_application_review_not_fail_closed")
    application_command = [str(item) for item in application_step.get("command") or []]
    for flag in FORBIDDEN_APPLICATION_FLAGS:
        if flag in application_command:
            reasons.append(f"resume_prefix_application_review_forbidden_flag:{flag}")

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
                reasons.append(f"forbidden_resume_application_surface:{step_id}:{token}")

    expected_prefix: list[str] = []
    if RESUME_GATE_STEP_ID in step_ids:
        expected_prefix = step_ids[: step_ids.index(RESUME_GATE_STEP_ID) + 1]
    gate_prefix = [str(item) for item in gate.get("resume_completed_step_ids") or []]
    if gate_prefix != expected_prefix:
        reasons.append("resume_completed_step_prefix_not_exact_or_contiguous")
    if [str(item) for item in gate.get("resume_cli_arguments") or []] != _flatten_completed_steps(
        expected_prefix
    ):
        reasons.append("resume_cli_arguments_do_not_match_exact_prefix")

    if gate.get("schema") != RESUME_GATE_SCHEMA:
        reasons.append("resume_gate_schema_mismatch")
    if gate.get("overall_status") != RESUME_GATE_READY_STATUS:
        reasons.append("resume_gate_status_not_ready")
    if gate.get("resume_gate_ready") is not True:
        reasons.append("resume_gate_not_ready")
    if gate.get("ready_for_resume_state_application_review") is not True:
        reasons.append("resume_gate_not_ready_for_state_application_review")
    if gate.get("next_step_id") != NEXT_STEP_ID:
        reasons.append("resume_gate_next_step_mismatch")
    if str(gate.get("run_id") or "") != str(plan.get("run_id") or ""):
        reasons.append("resume_gate_run_id_mismatch")
    if not _same_resolved_path(gate.get("full_flow_plan_json"), plan_path):
        reasons.append("resume_gate_plan_path_mismatch")
    if str(gate.get("full_flow_plan_sha256") or "") != _sha256(plan_path):
        reasons.append("resume_gate_plan_sha256_mismatch")
    batch_path_value = str(gate.get("batch_initialization_closeout_json") or "")
    batch_path = Path(batch_path_value).resolve() if batch_path_value else None
    if batch_path is None or not batch_path.is_file():
        reasons.append("resume_gate_batch_closeout_missing")
    elif str(gate.get("batch_initialization_closeout_sha256") or "") != _sha256(batch_path):
        reasons.append("resume_gate_batch_closeout_sha256_mismatch")
    if not _boundary_clean(gate):
        reasons.append("resume_gate_boundary_not_clean")

    reviewed_after_application = [*expected_prefix, APPLICATION_REVIEW_STEP_ID]
    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "resume_prefix_application_review_ready": ready,
        "resume_prefix_consumed_for_review": ready,
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha256(plan_path),
        "post_closeout_resume_gate_json": str(gate_path),
        "post_closeout_resume_gate_sha256": _sha256(gate_path),
        "batch_initialization_closeout_json": str(batch_path) if batch_path else "",
        "batch_initialization_closeout_sha256": str(
            gate.get("batch_initialization_closeout_sha256") or ""
        ),
        "run_id": str(plan.get("run_id") or ""),
        "reviewed_resume_completed_step_ids": expected_prefix if ready else [],
        "reviewed_completed_step_ids_after_application": reviewed_after_application if ready else [],
        "reviewed_resume_cli_arguments": _flatten_completed_steps(expected_prefix) if ready else [],
        "state_preview_current_step_id": NEXT_STEP_ID if ready else "",
        "state_preview_current_status": "ready_for_offline_review" if ready else "blocked",
        "downstream_route_step_ids": [CO2_STEP_ID, H2O_STEP_ID],
        "route_authorization_still_required": True,
        "review_reasons": reasons,
        "does_not_execute_commands": True,
        "applies_completed_steps": False,
        "writes_authoritative_state": False,
        "would_execute": False,
        "live_resume_execution_allowed": False,
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
        "# V1.5 Resume Prefix Application Review",
        "",
        f"- overall_status: `{model['overall_status']}`",
        f"- resume_prefix_application_review_ready: `{str(model['resume_prefix_application_review_ready']).lower()}`",
        f"- run_id: `{model['run_id']}`",
        f"- state_preview_current_step_id: `{model['state_preview_current_step_id']}`",
        f"- route_authorization_still_required: `{str(model['route_authorization_still_required']).lower()}`",
        "",
        "## Reviewed Prefix",
        "",
    ]
    lines.extend(f"- `{step_id}`" for step_id in model["reviewed_completed_step_ids_after_application"])
    if model["review_reasons"]:
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{reason}`" for reason in model["review_reasons"])
    lines.extend(
        [
            "",
            "## Non-Execution Boundary",
            "",
            "This artifact consumes the resume gate for validation and state preview only. It does not write the authoritative full-flow state or execute the next step.",
            "",
            "- applies_completed_steps: `false`",
            "- writes_authoritative_state: `false`",
            "- would_execute: `false`",
            "- opens_com_ports: `false`",
            "- controls_pressure: `false`",
            "- controls_water_or_gas_routes: `false`",
            "- writes_coefficients: `false`",
            "- connects_postgresql: `false`",
            "- formal_release_allowed: `false`",
            "- database_import_allowed: `false`",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_resume_prefix_application_review(
    *,
    output_dir: str | Path,
    full_flow_plan_json: str | Path,
    post_closeout_resume_gate_json: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_resume_prefix_application_review(
        full_flow_plan_json=full_flow_plan_json,
        post_closeout_resume_gate_json=post_closeout_resume_gate_json,
    )
    paths = {
        "manifest": out / "v1_5_resume_prefix_application_review.json",
        "state_preview": out / "v1_5_resume_prefix_state_preview.csv",
        "markdown": out / "V1_5_RESUME_PREFIX_APPLICATION_REVIEW.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(
        paths["state_preview"],
        [
            {"order": index, "step_id": step_id, "preview_state": "completed_after_review"}
            for index, step_id in enumerate(
                model["reviewed_completed_step_ids_after_application"],
                start=1,
            )
        ],
    )
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "SCHEMA",
    "build_v1_5_resume_prefix_application_review",
    "write_v1_5_resume_prefix_application_review",
]
