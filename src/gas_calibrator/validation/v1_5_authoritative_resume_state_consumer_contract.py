"""Offline, default-locked contract for consuming V1.5 resume state."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.v1_5.orchestration.full_flow import PLAN_SCHEMA
from .v1_5_authoritative_resume_state_post_write_verification import (
    READY_STATUS as VERIFICATION_READY_STATUS,
    SCHEMA as VERIFICATION_SCHEMA,
)

SCHEMA = "v1_5_authoritative_resume_state_consumer_contract_v1"
READY_STATUS = "ready_for_resume_state_consumer_contract_review"
BLOCKED_STATUS = "blocked"


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def build_v1_5_authoritative_resume_state_consumer_contract(
    *, full_flow_plan_json: str | Path, post_write_verification_json: str | Path
) -> dict[str, Any]:
    plan_path = Path(full_flow_plan_json).resolve()
    verification_path = Path(post_write_verification_json).resolve()
    plan = _load(plan_path)
    verification = _load(verification_path)
    state_path = Path(str(verification.get("authoritative_state_json") or "")).resolve()
    state = _load(state_path)
    reasons: list[str] = []
    if plan.get("schema") != PLAN_SCHEMA:
        reasons.append("full_flow_plan_schema_invalid")
    if verification.get("schema") != VERIFICATION_SCHEMA:
        reasons.append("post_write_verification_schema_invalid")
    if verification.get("overall_status") != VERIFICATION_READY_STATUS:
        reasons.append("post_write_verification_not_ready")
    if verification.get("post_write_verification_ready") is not True:
        reasons.append("post_write_verification_ready_flag_not_true")
    if _sha(state_path) != str(verification.get("authoritative_state_sha256") or ""):
        reasons.append("authoritative_state_sha256_mismatch")
    expected_state_path = plan_path.parent / "v1_5_full_flow_state.json"
    if state_path != expected_state_path.resolve():
        reasons.append("authoritative_state_path_not_canonical_for_plan")
    if state.get("schema") != "v1_5_full_calibration_flow_state_v0":
        reasons.append("authoritative_state_schema_invalid")
    if str(state.get("run_id") or "") != str(plan.get("run_id") or ""):
        reasons.append("authoritative_state_run_id_mismatch")
    step_ids = [
        str(row.get("step_id") or "")
        for row in plan.get("steps") or []
        if isinstance(row, Mapping)
    ]
    completed = [str(value) for value in state.get("completed_step_ids") or []]
    if completed != step_ids[: len(completed)]:
        reasons.append("completed_steps_not_exact_contiguous_prefix")
    expected_next = step_ids[len(completed)] if len(completed) < len(step_ids) else ""
    if str(state.get("current_step_id") or "") != expected_next:
        reasons.append("current_step_not_next_after_completed_prefix")
    if state.get("failed_step_ids") not in ([], ()):
        reasons.append("failed_steps_must_be_empty_before_resume")
    for field in (
        "allow_real_com",
        "allow_pressure_control",
        "allow_route_control",
        "allow_writes",
    ):
        if state.get(field) is not False:
            reasons.append(f"authoritative_state_{field}_not_false")
    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "resume_state_consumer_contract_ready": ready,
        "blocker_count": len(reasons),
        "blocker_reasons": reasons,
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha(plan_path),
        "post_write_verification_json": str(verification_path),
        "post_write_verification_sha256": _sha(verification_path),
        "authoritative_state_json": str(state_path),
        "authoritative_state_sha256": _sha(state_path),
        "completed_step_ids": completed,
        "next_step_id": expected_next,
        "execution_supported": False,
        "resume_execution_allowed": False,
        "would_execute": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_authoritative_state": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def write_v1_5_authoritative_resume_state_consumer_contract(
    model: Mapping[str, Any], output_dir: str | Path
) -> Path:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "v1_5_resume_state_consumer_contract.json"
    path.write_text(json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
