"""Offline dry-run review for the V1.5 formal initialization executor.

This module does not execute the initialization plan. It consumes the formal
initialization plan JSON and classifies each planned step so reviewers can see
which actions are offline-only, which require read-only real-COM authorization,
and which remain controlled-write locked.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_initialization_executor_dry_run_v1"
PLAN_SCHEMA = "v1_5_formal_initialization_plan_v0"
READY_STATUS = "ready_for_initialization_executor_dry_run_review"
REVIEW_STATUS = "review_required"
MIN_ANALYZER_COMMAND_GAP_S = 1.0


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


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _as_bool(value: Any) -> bool:
    return bool(value)


def _step_review(row: Mapping[str, Any]) -> dict[str, Any]:
    step_id = str(row.get("step_id") or "")
    command = tuple(str(part) for part in row.get("command") or ())
    opens_com = _as_bool(row.get("opens_com_ports"))
    writes_coefficients = _as_bool(row.get("writes_coefficients"))
    writes_device_id = _as_bool(row.get("writes_device_id"))
    controls_pressure = _as_bool(row.get("controls_pressure"))
    controls_gas_route = _as_bool(row.get("controls_gas_route"))
    controls_water_route = _as_bool(row.get("controls_water_route"))

    reasons: list[str] = []
    if writes_device_id:
        status = "blocked_device_id_write_forbidden"
        reasons.append("initialization_executor_must_not_write_device_id_or_sn")
    elif controls_pressure:
        status = "blocked_pressure_control_not_owned_by_initialization_executor"
        reasons.append("pressure_control_must_stay_in_pressure_stage")
    elif controls_gas_route or controls_water_route:
        status = "blocked_route_control_forbidden"
        reasons.append("open_flow_route_control_must_stay_in_mature_co2_h2o_queue")
    elif writes_coefficients:
        status = "locked_controlled_write"
        reasons.append("requires_explicit_controlled_write_authorization_and_readback")
        if opens_com:
            reasons.append("also_requires_read_only_real_com_unlock")
    elif opens_com:
        status = "locked_read_only_real_com"
        reasons.append("requires_explicit_read_only_real_com_authorization")
    elif not command:
        status = "contract_only"
        reasons.append("no_standalone_command_in_formal_initialization_executor")
    else:
        status = "dry_run_command_allowed"
        reasons.append("offline_command_can_be_reviewed_or_run_without_real_com")

    return {
        "step_id": step_id,
        "title": str(row.get("title") or ""),
        "phase": str(row.get("phase") or ""),
        "review_status": status,
        "reasons": ";".join(reasons),
        "execution_mode": str(row.get("execution_mode") or ""),
        "gate": str(row.get("gate") or ""),
        "command_text": " ".join(command),
        "opens_com_ports": opens_com,
        "writes_coefficients": writes_coefficients,
        "writes_device_id": writes_device_id,
        "controls_pressure": controls_pressure,
        "controls_gas_route": controls_gas_route,
        "controls_water_route": controls_water_route,
    }


def _plan_reasons(plan: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not plan:
        return ["formal_initialization_plan_missing"]
    if plan.get("schema") != PLAN_SCHEMA:
        reasons.append(f"plan_schema={plan.get('schema') or 'missing'}")
    if plan.get("dry_run_only") is not True:
        reasons.append(f"dry_run_only={plan.get('dry_run_only')!r}")
    try:
        command_gap = float(plan.get("analyzer_command_gap_s"))
    except Exception:
        reasons.append("analyzer_command_gap_s_missing")
    else:
        if command_gap < MIN_ANALYZER_COMMAND_GAP_S:
            reasons.append(f"analyzer_command_gap_s={command_gap}")
    safety = plan.get("safety_contract") if isinstance(plan.get("safety_contract"), Mapping) else {}
    expected_false = {
        "planner_opens_com_ports": safety.get("planner_opens_com_ports"),
        "planner_writes_coefficients": safety.get("planner_writes_coefficients"),
        "planner_controls_gas_route": safety.get("planner_controls_gas_route"),
        "planner_controls_water_route": safety.get("planner_controls_water_route"),
    }
    for key, value in expected_false.items():
        if value is not False:
            reasons.append(f"{key}={value!r}")
    if safety.get("does_not_write_device_id") is not True:
        reasons.append(f"does_not_write_device_id={safety.get('does_not_write_device_id')!r}")
    return reasons


def build_v1_5_formal_initialization_executor_dry_run(
    *,
    formal_initialization_plan_json: str | Path | None,
) -> dict[str, Any]:
    plan_path = Path(formal_initialization_plan_json).resolve() if formal_initialization_plan_json else None
    plan = _load_json(plan_path)
    step_reviews = [_step_review(row) for row in plan.get("steps") or [] if isinstance(row, Mapping)]
    plan_reasons = _plan_reasons(plan)
    unsafe_step_reasons = [
        f"{row['step_id']}:{row['review_status']}"
        for row in step_reviews
        if str(row["review_status"]).startswith("blocked_")
    ]
    review_required = bool(plan_reasons)
    counts = {
        "dry_run_command_allowed": sum(1 for row in step_reviews if row["review_status"] == "dry_run_command_allowed"),
        "contract_only": sum(1 for row in step_reviews if row["review_status"] == "contract_only"),
        "locked_read_only_real_com": sum(1 for row in step_reviews if row["review_status"] == "locked_read_only_real_com"),
        "locked_controlled_write": sum(1 for row in step_reviews if row["review_status"] == "locked_controlled_write"),
        "blocked": sum(1 for row in step_reviews if str(row["review_status"]).startswith("blocked_")),
    }
    checks = [
        {
            "check": "formal_initialization_plan_consumed",
            "status": "ready" if not plan_reasons else "review_required",
            "reasons": ";".join(plan_reasons),
            "physical_meaning": "The dry-run review must consume the frozen initialization plan before any executor path is considered.",
            "next_action": "Regenerate the formal initialization plan if this check is not ready.",
        },
        {
            "check": "real_com_remains_locked",
            "status": "ready",
            "reasons": "",
            "physical_meaning": "Read-only analyzer contact is a separate operator-authorized step; this package does not open COM ports.",
            "next_action": "Use only a future reviewed controlled executor to unlock real-COM identity/GETCO steps.",
        },
        {
            "check": "controlled_writes_remain_locked",
            "status": "ready",
            "reasons": "",
            "physical_meaning": "S5/S6/S7/S8/S9 initialization writes require old snapshot, authorization, readback, and rollback evidence.",
            "next_action": "Keep coefficient writes out of the dry-run review package.",
        },
        {
            "check": "route_pressure_device_id_not_owned_here",
            "status": "ready" if not unsafe_step_reasons else "review_required",
            "reasons": ";".join(unsafe_step_reasons),
            "physical_meaning": "Initialization automation must not become pressure control, route control, or device-ID rewriting.",
            "next_action": "Move any such action back to its dedicated controlled stage before execution.",
        },
    ]
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": REVIEW_STATUS if review_required else READY_STATUS,
        "blocker_count": 0,
        "review_required_count": sum(1 for row in checks if row["status"] == "review_required"),
        "formal_initialization_plan_json": str(plan_path) if plan_path else "",
        "plan_schema": plan.get("schema", ""),
        "run_id": plan.get("run_id", ""),
        "dry_run_review_allowed": not review_required,
        "live_execution_allowed": False,
        "read_only_real_com_execution_allowed": False,
        "controlled_write_execution_allowed": False,
        "execute_flag_allowed": False,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_water_or_gas_routes": False,
        "controls_pressure": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "not_real_acceptance_evidence": True,
        "minimum_analyzer_command_gap_s": MIN_ANALYZER_COMMAND_GAP_S,
        "observed_analyzer_command_gap_s": plan.get("analyzer_command_gap_s"),
        "step_review_counts": counts,
        "checks": checks,
        "step_reviews": step_reviews,
        "next_action": (
            "Review this dry-run package. A separate PR must implement any future live initialization "
            "executor with explicit real-COM and controlled-write authorization."
        ),
    }


def write_v1_5_formal_initialization_executor_dry_run_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, str]:
    out = Path(output_dir)
    outputs = {
        "json": out / "v1_5_formal_initialization_executor_dry_run.json",
        "steps_csv": out / "v1_5_formal_initialization_executor_dry_run_steps.csv",
        "checks_csv": out / "v1_5_formal_initialization_executor_dry_run_checks.csv",
        "markdown": out / "V1_5_FORMAL_INITIALIZATION_EXECUTOR_DRY_RUN.md",
    }
    _write_json(outputs["json"], model)
    _write_csv(outputs["steps_csv"], [dict(row) for row in model.get("step_reviews") or []])
    _write_csv(outputs["checks_csv"], [dict(row) for row in model.get("checks") or []])
    summary = [
        "# V1.5 formal initialization executor dry-run review",
        "",
        "This package reviews the formal initialization executor boundary without running any plan command.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- dry_run_review_allowed: `{model.get('dry_run_review_allowed')}`",
        f"- live_execution_allowed: `{model.get('live_execution_allowed')}`",
        f"- read_only_real_com_execution_allowed: `{model.get('read_only_real_com_execution_allowed')}`",
        f"- controlled_write_execution_allowed: `{model.get('controlled_write_execution_allowed')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        "",
        "The dry-run review may classify plan steps, but it does not execute them.",
    ]
    outputs["markdown"].parent.mkdir(parents=True, exist_ok=True)
    outputs["markdown"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
