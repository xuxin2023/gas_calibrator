"""Plan-only preview for a future V1.5 authoritative-state resume executor."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from .v1_5_authoritative_resume_state_consumer_contract import (
    READY_STATUS as CONTRACT_READY_STATUS,
    SCHEMA as CONTRACT_SCHEMA,
    build_v1_5_authoritative_resume_state_consumer_contract,
)

SCHEMA = "v1_5_authoritative_resume_executor_plan_preview_v1"
READY_STATUS = "ready_for_resume_executor_plan_preview_review"
BLOCKED_STATUS = "blocked"


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def build_v1_5_authoritative_resume_executor_plan_preview(
    *, consumer_contract_json: str | Path
) -> dict[str, Any]:
    contract_path = Path(consumer_contract_json).resolve()
    contract = _load(contract_path)
    plan_path = Path(str(contract.get("full_flow_plan_json") or "")).resolve()
    verification_path = Path(str(contract.get("post_write_verification_json") or "")).resolve()
    plan = _load(plan_path)
    reasons: list[str] = []
    if contract.get("schema") != CONTRACT_SCHEMA:
        reasons.append("consumer_contract_schema_invalid")
    if contract.get("overall_status") != CONTRACT_READY_STATUS:
        reasons.append("consumer_contract_not_ready")
    if contract.get("resume_state_consumer_contract_ready") is not True:
        reasons.append("consumer_contract_ready_flag_not_true")
    recomputed = build_v1_5_authoritative_resume_state_consumer_contract(
        full_flow_plan_json=plan_path,
        post_write_verification_json=verification_path,
    )
    for key in (
        "overall_status",
        "resume_state_consumer_contract_ready",
        "blocker_count",
        "blocker_reasons",
        "full_flow_plan_json",
        "full_flow_plan_sha256",
        "post_write_verification_json",
        "post_write_verification_sha256",
        "authoritative_state_json",
        "authoritative_state_sha256",
        "completed_step_ids",
        "next_step_id",
    ):
        if contract.get(key) != recomputed.get(key):
            reasons.append(f"consumer_contract_recompute_mismatch:{key}")
    next_step_id = str(contract.get("next_step_id") or "")
    step = next(
        (
            dict(row)
            for row in plan.get("steps") or []
            if isinstance(row, Mapping) and str(row.get("step_id") or "") == next_step_id
        ),
        {},
    )
    if not step:
        reasons.append("next_step_missing_from_plan")
    ready = not reasons
    return {
        "schema": SCHEMA,
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "resume_executor_plan_preview_ready": ready,
        "blocker_count": len(reasons),
        "blocker_reasons": reasons,
        "consumer_contract_json": str(contract_path),
        "consumer_contract_sha256": _sha(contract_path),
        "next_step_id": next_step_id,
        "next_step_title": str(step.get("title") or ""),
        "next_step_phase": str(step.get("phase") or ""),
        "next_step_tool_module": str(step.get("tool_module") or ""),
        "next_step_command": [str(value) for value in step.get("command") or []],
        "requires_real_com_authorization": bool(step.get("opens_com_ports")),
        "requires_pressure_authorization": bool(step.get("controls_pressure")),
        "requires_route_authorization": bool(step.get("controls_gas_route") or step.get("controls_water_route")),
        "requires_write_authorization": bool(step.get("writes_coefficients") or step.get("writes_device_id")),
        "execution_supported": False,
        "resume_execution_allowed": False,
        "would_execute": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def write_v1_5_authoritative_resume_executor_plan_preview(
    model: Mapping[str, Any], output_dir: str | Path
) -> Path:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "v1_5_resume_executor_plan_preview.json"
    path.write_text(json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
