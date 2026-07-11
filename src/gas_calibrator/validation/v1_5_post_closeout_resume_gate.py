"""Offline gate for resuming V1.5 after batch initialization closeout."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_batch_initialization_closeout_index import READY_STATUS as BATCH_READY_STATUS


SCHEMA = "v1_5_post_closeout_resume_gate_v1"
READY_STATUS = "ready_for_post_closeout_resume_review"
BLOCKED_STATUS = "blocked"

BATCH_STEP_ID = "batch_initialization_closeout_index"
RESUME_STEP_ID = "post_closeout_resume_gate_snapshot"
NEXT_STEP_ID = "temperature_channel_fast_review"
CO2_STEP_ID = "co2_open_flow_sampling"
H2O_STEP_ID = "h2o_open_flow_sampling"

CANONICAL_MODULES = {
    BATCH_STEP_ID: "gas_calibrator.tools.export_v1_5_batch_initialization_closeout_index",
    RESUME_STEP_ID: "gas_calibrator.tools.export_v1_5_post_closeout_resume_gate",
    NEXT_STEP_ID: "gas_calibrator.tools.export_v1_5_temperature_channel_review",
    CO2_STEP_ID: "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
    H2O_STEP_ID: "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue",
}

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


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


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


def _same_resolved_path(value: str, expected: Path) -> bool:
    if not value:
        return False
    try:
        return Path(value).resolve() == expected.resolve()
    except (OSError, RuntimeError):
        return False


def _boundary_clean(payload: Mapping[str, Any]) -> bool:
    return (
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


def build_v1_5_post_closeout_resume_gate(
    *,
    full_flow_plan_json: str | Path,
    batch_initialization_closeout_json: str | Path,
) -> dict[str, Any]:
    plan_path = Path(full_flow_plan_json).resolve()
    batch_path = Path(batch_initialization_closeout_json).resolve()
    plan = _load_mapping(plan_path)
    batch = _load_mapping(batch_path)
    steps = _step_rows(plan)
    step_ids = [str(row.get("step_id") or "") for row in steps]
    reasons: list[str] = []

    if len(step_ids) != len(set(step_ids)):
        reasons.append("duplicate_full_flow_step_ids")

    by_id = {str(row.get("step_id") or ""): row for row in steps}
    for step_id, module in CANONICAL_MODULES.items():
        row = by_id.get(step_id)
        if row is None:
            reasons.append(f"required_step_missing:{step_id}")
            continue
        if str(row.get("tool_module") or "") != module:
            reasons.append(f"canonical_module_mismatch:{step_id}")

    required_order = (BATCH_STEP_ID, RESUME_STEP_ID, NEXT_STEP_ID, CO2_STEP_ID, H2O_STEP_ID)
    if all(step_id in step_ids for step_id in required_order):
        indexes = [step_ids.index(step_id) for step_id in required_order]
        if indexes != sorted(indexes):
            reasons.append("post_closeout_resume_order_invalid")

    batch_step = by_id.get(BATCH_STEP_ID) or {}
    if "--fail-on-review-required" not in (batch_step.get("command") or []):
        reasons.append("batch_closeout_not_fail_closed")

    resume_step = by_id.get(RESUME_STEP_ID) or {}
    if not str(resume_step.get("execution_mode") or "").startswith("offline"):
        reasons.append("resume_gate_not_offline")
    if any(
        bool(resume_step.get(key))
        for key in (
            "opens_com_ports",
            "controls_pressure",
            "controls_gas_route",
            "controls_water_route",
            "writes_device_id",
            "writes_coefficients",
        )
    ):
        reasons.append("resume_gate_side_effect_boundary_not_clean")
    if not _same_resolved_path(_command_value(resume_step, "--full-flow-plan-json"), plan_path):
        reasons.append("resume_gate_plan_path_mismatch_with_full_flow_plan")
    if not _same_resolved_path(
        _command_value(resume_step, "--batch-initialization-closeout-json"),
        batch_path,
    ):
        reasons.append("resume_gate_batch_path_mismatch_with_full_flow_plan")

    for step_id in (BATCH_STEP_ID, RESUME_STEP_ID, NEXT_STEP_ID, CO2_STEP_ID, H2O_STEP_ID):
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
                reasons.append(f"forbidden_resume_surface:{step_id}:{token}")

    if batch.get("overall_status") != BATCH_READY_STATUS:
        reasons.append("batch_closeout_status_not_ready")
    if batch.get("batch_initialization_closeout_ready") is not True:
        reasons.append("batch_closeout_not_ready")
    if batch.get("ready_for_mature_open_flow_from_initialization_index") is not True:
        reasons.append("batch_closeout_not_ready_for_mature_open_flow")
    if not _boundary_clean(batch):
        reasons.append("batch_closeout_boundary_not_clean")

    device_count = _safe_int(batch.get("device_count"))
    device_ready_count = _safe_int(batch.get("device_ready_count"))
    if not 1 <= device_count <= 6 or device_ready_count != device_count:
        reasons.append("batch_device_readiness_incomplete")
    if batch.get("mature_route_baseline") != "0620/0621 clean worktree mature physical route":
        reasons.append("mature_route_baseline_mismatch")
    if batch.get("mature_fitting_baseline") != "0613 V1.5 fitting path":
        reasons.append("mature_fitting_baseline_mismatch")

    resume_prefix: list[str] = []
    if RESUME_STEP_ID in step_ids:
        resume_prefix = step_ids[: step_ids.index(RESUME_STEP_ID) + 1]
    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "resume_gate_ready": ready,
        "ready_for_resume_state_application_review": ready,
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha256(plan_path),
        "batch_initialization_closeout_json": str(batch_path),
        "batch_initialization_closeout_sha256": _sha256(batch_path),
        "run_id": str(plan.get("run_id") or ""),
        "device_count": device_count,
        "device_ready_count": device_ready_count,
        "resume_completed_step_ids": resume_prefix if ready else [],
        "resume_cli_arguments": (
            [item for step_id in resume_prefix for item in ("--completed-step", step_id)] if ready else []
        ),
        "next_step_id": NEXT_STEP_ID if ready else "",
        "downstream_route_step_ids": [CO2_STEP_ID, H2O_STEP_ID],
        "route_authorization_still_required": True,
        "review_reasons": reasons,
        "does_not_execute_commands": True,
        "applies_completed_steps": False,
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
        "# V1.5 Post-Closeout Resume Gate",
        "",
        f"- overall_status: `{model['overall_status']}`",
        f"- resume_gate_ready: `{str(model['resume_gate_ready']).lower()}`",
        f"- run_id: `{model['run_id']}`",
        f"- next_step_id: `{model['next_step_id']}`",
        f"- route_authorization_still_required: `{str(model['route_authorization_still_required']).lower()}`",
        "",
        "## Resume Prefix",
        "",
    ]
    lines.extend(f"- `{step_id}`" for step_id in model["resume_completed_step_ids"])
    if model["review_reasons"]:
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{reason}`" for reason in model["review_reasons"])
    lines.extend(
        [
            "",
            "## Non-Execution Boundary",
            "",
            "This artifact does not apply completed steps or execute the next step. It only binds a reviewed resume prefix to exact plan and closeout hashes.",
            "",
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


def write_v1_5_post_closeout_resume_gate(
    *,
    output_dir: str | Path,
    full_flow_plan_json: str | Path,
    batch_initialization_closeout_json: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_post_closeout_resume_gate(
        full_flow_plan_json=full_flow_plan_json,
        batch_initialization_closeout_json=batch_initialization_closeout_json,
    )
    paths = {
        "manifest": out / "v1_5_post_closeout_resume_gate.json",
        "resume_steps": out / "v1_5_post_closeout_resume_steps.csv",
        "markdown": out / "V1_5_POST_CLOSEOUT_RESUME_GATE.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(
        paths["resume_steps"],
        [
            {"order": index, "step_id": step_id, "state": "evidence_bound_completed"}
            for index, step_id in enumerate(model["resume_completed_step_ids"], start=1)
        ],
    )
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "SCHEMA",
    "build_v1_5_post_closeout_resume_gate",
    "write_v1_5_post_closeout_resume_gate",
]
