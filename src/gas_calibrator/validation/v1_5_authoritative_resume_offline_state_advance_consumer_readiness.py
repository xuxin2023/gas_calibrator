"""Gate read-only consumption of a verified V1.5 offline-advanced resume state."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.v1_5.orchestration.full_flow import PLAN_SCHEMA

from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    READY_STATUS as VERIFICATION_READY_STATUS,
    SCHEMA as VERIFICATION_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_post_write_verification,
    _contains_reparse,
)

SCHEMA = "v1_5_authoritative_resume_offline_state_advance_consumer_readiness_v1"
READY_STATUS = "ready_for_offline_advanced_resume_state_consumption"
BLOCKED_STATUS = "blocked"
VERIFICATION_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_post_write_verification.json"
)

VERIFICATION_COMPARE_KEYS = (
    "overall_status",
    "post_write_verification_ready",
    "blocker_count",
    "blocker_reasons",
    "atomic_write_json",
    "atomic_write_sha256",
    "state_advance_authorization_json",
    "state_advance_authorization_sha256",
    "offline_state_advance_preflight_json",
    "offline_state_advance_preflight_sha256",
    "authorization_packet_json",
    "authorization_packet_sha256",
    "full_flow_plan_json",
    "full_flow_plan_sha256",
    "authorization_id",
    "run_id",
    "attempt_id",
    "verified_step_id",
    "next_step_id_after_advance",
    "authoritative_state_json",
    "authoritative_state_sha256",
    "candidate_state_preview_json",
    "candidate_state_sha256",
    "rollback_snapshot_path",
    "rollback_snapshot_sha256",
    "writer_lock_path",
    "writer_lock_released",
    "state_consumption_allowed",
    "execution_supported",
    "resume_execution_allowed",
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


def build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
    *, post_write_verification_json: str | Path
) -> dict[str, Any]:
    verification_path = Path(post_write_verification_json).absolute()
    verification = _load(verification_path)
    reasons: list[str] = []
    if verification_path.name != VERIFICATION_FILENAME:
        reasons.append("post_write_verification_filename_not_canonical")
    if _contains_reparse(verification_path):
        reasons.append("post_write_verification_path_contains_reparse_point")
    if verification.get("schema") != VERIFICATION_SCHEMA:
        reasons.append("post_write_verification_schema_invalid")
    if verification.get("overall_status") != VERIFICATION_READY_STATUS:
        reasons.append("post_write_verification_not_ready")
    if verification.get("post_write_verification_ready") is not True:
        reasons.append("post_write_verification_ready_flag_not_true")
    if int(verification.get("blocker_count") or 0) or verification.get(
        "blocker_reasons"
    ):
        reasons.append("post_write_verification_contains_blockers")
    try:
        recomputed = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
            atomic_write_json=verification.get("atomic_write_json")
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if not recomputed:
        reasons.append("post_write_verification_recompute_failed")
    else:
        if recomputed.get("schema") != VERIFICATION_SCHEMA:
            reasons.append("post_write_verification_recomputed_schema_invalid")
        if any(
            verification.get(key) != recomputed.get(key)
            for key in VERIFICATION_COMPARE_KEYS
        ):
            reasons.append("post_write_verification_recompute_mismatch")

    plan_path = Path(str(verification.get("full_flow_plan_json") or "")).absolute()
    state_path = Path(
        str(verification.get("authoritative_state_json") or "")
    ).absolute()
    plan = _load(plan_path)
    state = _load(state_path)
    if _contains_reparse(plan_path):
        reasons.append("full_flow_plan_path_contains_reparse_point")
    if _contains_reparse(state_path):
        reasons.append("authoritative_state_path_contains_reparse_point")
    if plan.get("schema") != PLAN_SCHEMA:
        reasons.append("full_flow_plan_schema_invalid")
    if _sha(plan_path) != str(verification.get("full_flow_plan_sha256") or ""):
        reasons.append("full_flow_plan_sha256_mismatch")
    expected_state_path = plan_path.parent / "v1_5_full_flow_state.json"
    if state_path != expected_state_path.absolute():
        reasons.append("authoritative_state_path_not_canonical_for_plan")
    if _sha(state_path) != str(verification.get("authoritative_state_sha256") or ""):
        reasons.append("authoritative_state_sha256_mismatch")
    if state.get("schema") != "v1_5_full_calibration_flow_state_v0":
        reasons.append("authoritative_state_schema_invalid")
    if str(state.get("run_id") or "") != str(verification.get("run_id") or ""):
        reasons.append("authoritative_state_run_id_mismatch")
    if str(plan.get("run_id") or "") != str(verification.get("run_id") or ""):
        reasons.append("full_flow_plan_run_id_mismatch")
    for field in ("run_id", "attempt_id", "verified_step_id"):
        if not str(verification.get(field) or "").strip():
            reasons.append(f"post_write_verification_{field}_missing")

    step_ids = [
        str(row.get("step_id") or "")
        for row in plan.get("steps") or []
        if isinstance(row, Mapping)
    ]
    completed = [str(value) for value in state.get("completed_step_ids") or []]
    if completed != step_ids[: len(completed)]:
        reasons.append("completed_steps_not_exact_contiguous_prefix")
    verified_step = str(verification.get("verified_step_id") or "")
    if not completed or completed[-1] != verified_step:
        reasons.append("verified_step_not_last_completed_step")
    expected_next = step_ids[len(completed)] if len(completed) < len(step_ids) else ""
    if str(state.get("current_step_id") or "") != expected_next:
        reasons.append("current_step_not_next_after_completed_prefix")
    if str(verification.get("next_step_id_after_advance") or "") != expected_next:
        reasons.append("verification_next_step_mismatch")
    if state.get("failed_step_ids") not in ([], ()):
        reasons.append("failed_steps_must_be_empty_before_consumption")
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
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "resume_state_consumer_readiness_ready": ready,
        "blocker_count": len(reasons),
        "blocker_reasons": reasons,
        "post_write_verification_json": str(verification_path),
        "post_write_verification_sha256": _sha(verification_path),
        "atomic_write_json": str(verification.get("atomic_write_json") or ""),
        "atomic_write_sha256": str(verification.get("atomic_write_sha256") or ""),
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha(plan_path),
        "authoritative_state_json": str(state_path),
        "authoritative_state_sha256": _sha(state_path),
        "run_id": str(verification.get("run_id") or ""),
        "attempt_id": str(verification.get("attempt_id") or ""),
        "verified_step_id": verified_step,
        "completed_step_ids": completed,
        "next_step_id": expected_next,
        "state_consumption_allowed": ready,
        "execution_supported": False,
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


def write_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
    model: Mapping[str, Any], output_dir: str | Path
) -> Path:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "v1_5_authoritative_resume_offline_state_advance_consumer_readiness.json"
    path.write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return path


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness",
    "write_v1_5_authoritative_resume_offline_state_advance_consumer_readiness",
]
