"""Build a no-write CAS preflight after a verified V1.5 offline resume step."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from gas_calibrator.v1_5.orchestration.full_flow import (
    PLAN_SCHEMA,
    build_full_flow_state,
)

from .v1_5_authoritative_resume_offline_post_execution_verifier import (
    READY_STATUS as VERIFIER_READY_STATUS,
    SCHEMA as VERIFIER_SCHEMA,
    build_v1_5_authoritative_resume_offline_post_execution_verifier,
)
from .v1_5_authoritative_resume_state_controlled_write_preflight import (
    _plan_from_payload,
)

SCHEMA = "v1_5_authoritative_resume_offline_state_advance_preflight_v1"
READY_STATUS = "ready_for_authoritative_resume_offline_state_advance_review"
BLOCKED_STATUS = "blocked"

VERIFIER_COMPARE_KEYS = (
    "overall_status",
    "offline_post_execution_verification_ready",
    "review_required_count",
    "review_reasons",
    "offline_executor_json",
    "offline_executor_sha256",
    "attempt_id",
    "run_id",
    "next_step_id",
    "full_flow_plan_json",
    "full_flow_plan_sha256",
    "authoritative_state_json",
    "authoritative_state_sha256_expected",
    "authoritative_state_sha256_current",
    "verified_outputs",
    "authoritative_state_advance_allowed",
    "execution_supported",
    "would_execute",
    "opens_com_ports",
    "controls_pressure",
    "controls_water_or_gas_routes",
    "writes_coefficients",
    "connects_postgresql",
    "formal_release_allowed",
    "database_import_allowed",
    "not_real_acceptance_evidence",
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _candidate_sha(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_bytes(payload)).hexdigest() if payload else ""


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except (OSError, RuntimeError, ValueError):
        return False
    return True


def _valid_iso_timestamp(value: Any) -> bool:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _check(name: str, reasons: Sequence[str], **details: Any) -> dict[str, Any]:
    return {
        "check": name,
        "status": "ready" if not reasons else "blocker",
        "reasons": list(reasons),
        "details": details,
    }


def build_v1_5_authoritative_resume_offline_state_advance_preflight(
    *, offline_post_execution_verifier_json: str | Path
) -> dict[str, Any]:
    verifier_path = Path(offline_post_execution_verifier_json).resolve()
    verifier = _load(verifier_path)
    checks: list[dict[str, Any]] = []

    verifier_reasons: list[str] = []
    if verifier.get("schema") != VERIFIER_SCHEMA:
        verifier_reasons.append("post_execution_verifier_schema_invalid")
    if verifier.get("overall_status") != VERIFIER_READY_STATUS:
        verifier_reasons.append("post_execution_verifier_not_ready")
    if verifier.get("offline_post_execution_verification_ready") is not True:
        verifier_reasons.append("post_execution_verifier_ready_flag_not_true")
    if verifier.get("authoritative_state_advance_allowed") is not False:
        verifier_reasons.append("post_execution_verifier_advanced_state_unexpectedly")
    for field in (
        "execution_supported",
        "would_execute",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "connects_postgresql",
        "formal_release_allowed",
        "database_import_allowed",
    ):
        if verifier.get(field) is not False:
            verifier_reasons.append(f"post_execution_verifier_boundary_invalid:{field}")
    if verifier.get("not_real_acceptance_evidence") is not True:
        verifier_reasons.append(
            "post_execution_verifier_boundary_invalid:not_real_acceptance_evidence"
        )
    try:
        recomputed = build_v1_5_authoritative_resume_offline_post_execution_verifier(
            offline_executor_json=verifier.get("offline_executor_json")
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if not recomputed:
        verifier_reasons.append("post_execution_verifier_recompute_failed")
    else:
        for key in VERIFIER_COMPARE_KEYS:
            if verifier.get(key) != recomputed.get(key):
                verifier_reasons.append(f"post_execution_verifier_recompute_mismatch:{key}")
    checks.append(
        _check(
            "post_execution_verifier_fresh_recompute",
            verifier_reasons,
            verifier_json=str(verifier_path),
            verifier_sha256=_sha(verifier_path),
        )
    )

    plan_path = Path(str(verifier.get("full_flow_plan_json") or "")).resolve()
    state_path = Path(str(verifier.get("authoritative_state_json") or "")).resolve()
    plan = _load(plan_path)
    state = _load(state_path)
    plan_root = plan_path.parent
    plan_reasons: list[str] = []
    if plan.get("schema") != PLAN_SCHEMA:
        plan_reasons.append("full_flow_plan_schema_invalid")
    if str(verifier.get("full_flow_plan_sha256") or "") != _sha(plan_path):
        plan_reasons.append("full_flow_plan_sha256_mismatch")
    if state_path != (plan_root / "v1_5_full_flow_state.json").resolve():
        plan_reasons.append("authoritative_state_path_not_canonical_for_plan")
    expected_state_sha = str(
        verifier.get("authoritative_state_sha256_current") or ""
    )
    current_state_sha = _sha(state_path)
    if not expected_state_sha or current_state_sha != expected_state_sha:
        plan_reasons.append("authoritative_state_compare_and_swap_sha256_mismatch")
    if expected_state_sha != str(
        verifier.get("authoritative_state_sha256_expected") or ""
    ):
        plan_reasons.append("post_execution_state_sha256_contract_inconsistent")
    if state.get("schema") != "v1_5_full_calibration_flow_state_v0":
        plan_reasons.append("authoritative_state_schema_invalid")
    if str(state.get("run_id") or "") != str(plan.get("run_id") or ""):
        plan_reasons.append("authoritative_state_run_id_mismatch")
    if str(verifier.get("run_id") or "") != str(plan.get("run_id") or ""):
        plan_reasons.append("post_execution_verifier_run_id_mismatch")
    checks.append(
        _check(
            "plan_and_current_state_compare_and_swap_binding",
            plan_reasons,
            full_flow_plan_json=str(plan_path),
            authoritative_state_json=str(state_path),
            expected_current_state_sha256=expected_state_sha,
            observed_current_state_sha256=current_state_sha,
        )
    )

    output_reasons: list[str] = []
    verified_outputs: list[dict[str, Any]] = []
    source_rows = verifier.get("verified_outputs") or []
    if not source_rows:
        output_reasons.append("post_execution_verified_outputs_missing")
    for raw in source_rows:
        row = dict(raw) if isinstance(raw, Mapping) else {}
        path = Path(str(row.get("path") or "")).resolve()
        current_sha = _sha(path)
        expected_sha = str(row.get("current_sha256") or "")
        status = "ready"
        if not _is_within(path, plan_root):
            output_reasons.append(f"verified_output_outside_plan_root:{path}")
            status = "blocker"
        if row.get("status") != "ready":
            output_reasons.append(f"verified_output_status_not_ready:{path}")
            status = "blocker"
        if not current_sha or current_sha != expected_sha:
            output_reasons.append(f"verified_output_sha256_changed:{path}")
            status = "blocker"
        verified_outputs.append(
            {
                "path": str(path),
                "expected_sha256": expected_sha,
                "observed_sha256": current_sha,
                "status": status,
            }
        )
    checks.append(
        _check(
            "verified_outputs_unchanged_after_verification",
            output_reasons,
            output_count=len(verified_outputs),
        )
    )

    step_ids = [
        str(row.get("step_id") or "")
        for row in plan.get("steps") or []
        if isinstance(row, Mapping)
    ]
    completed = [str(value) for value in state.get("completed_step_ids") or []]
    executed_step_id = str(verifier.get("next_step_id") or "")
    step_reasons: list[str] = []
    if not step_ids or any(not value for value in step_ids):
        step_reasons.append("full_flow_plan_step_ids_invalid")
    if len(step_ids) != len(set(step_ids)):
        step_reasons.append("full_flow_plan_step_ids_not_unique")
    if completed != step_ids[: len(completed)]:
        step_reasons.append("completed_steps_not_exact_contiguous_prefix")
    expected_executed = step_ids[len(completed)] if len(completed) < len(step_ids) else ""
    if not executed_step_id or executed_step_id != expected_executed:
        step_reasons.append("verified_step_not_next_after_completed_prefix")
    if str(state.get("current_step_id") or "") != executed_step_id:
        step_reasons.append("authoritative_state_current_step_mismatch")
    if executed_step_id in completed:
        step_reasons.append("verified_step_already_completed")
    if state.get("failed_step_ids") not in ([], ()):
        step_reasons.append("authoritative_state_failed_steps_not_empty")
    for field in (
        "allow_real_com",
        "allow_pressure_control",
        "allow_route_control",
        "allow_writes",
    ):
        if state.get(field) is not False:
            step_reasons.append(f"authoritative_state_{field}_not_false")

    candidate_state: dict[str, Any] = {}
    candidate_reasons: list[str] = []
    candidate_completed = [*completed, executed_step_id] if not step_reasons else []
    next_step_id = (
        step_ids[len(candidate_completed)]
        if candidate_completed and len(candidate_completed) < len(step_ids)
        else ""
    )
    executor = _load(Path(str(verifier.get("offline_executor_json") or "")).resolve())
    execution_finished_at = str(executor.get("finished_at") or "")
    if not _valid_iso_timestamp(execution_finished_at):
        candidate_reasons.append("offline_executor_finished_at_invalid")
    candidate_input_reasons = [
        *verifier_reasons,
        *plan_reasons,
        *output_reasons,
        *step_reasons,
    ]
    if not candidate_input_reasons and not candidate_reasons:
        try:
            typed_plan = _plan_from_payload(plan)
            state_model = build_full_flow_state(
                typed_plan,
                completed_steps=candidate_completed,
                failed_steps=(),
                allow_real_com=False,
                allow_pressure_control=False,
                allow_route_control=False,
                allow_writes=False,
            )
            candidate_state = json.loads(json.dumps(state_model.to_json()))
            candidate_state["created_at"] = execution_finished_at
            candidate_state["current_step_id"] = next_step_id
            if not next_step_id:
                candidate_state["current_status"] = "complete"
            if candidate_state.get("completed_step_ids") != candidate_completed:
                candidate_reasons.append("candidate_completed_prefix_not_exact")
            if candidate_state.get("current_step_id") != next_step_id:
                candidate_reasons.append("candidate_next_step_mismatch")
            if candidate_state.get("failed_step_ids") != []:
                candidate_reasons.append("candidate_failed_steps_not_empty")
            for field in (
                "allow_real_com",
                "allow_pressure_control",
                "allow_route_control",
                "allow_writes",
            ):
                if candidate_state.get(field) is not False:
                    candidate_reasons.append(f"candidate_{field}_not_false")
        except (KeyError, TypeError, ValueError):
            candidate_state = {}
            candidate_reasons.append("candidate_state_generation_failed")
    if candidate_reasons:
        candidate_state = {}
    candidate_check_reasons = [*step_reasons, *candidate_reasons]
    if candidate_input_reasons and not candidate_check_reasons:
        candidate_check_reasons.append(
            "candidate_generation_suppressed_by_upstream_blocker"
        )
    checks.append(
        _check(
            "single_contiguous_offline_step_state_advance",
            candidate_check_reasons,
            completed_step_ids_before=completed,
            verified_step_id=executed_step_id,
            completed_step_ids_after=candidate_completed,
            next_step_id_after_advance=next_step_id,
        )
    )

    blocker_reasons = [
        reason
        for row in checks
        for reason in row.get("reasons") or []
    ]
    ready = not blocker_reasons and bool(candidate_state)
    candidate_hash = _candidate_sha(candidate_state)
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "offline_state_advance_preflight_ready": ready,
        "blocker_count": len(blocker_reasons),
        "blocker_reasons": blocker_reasons,
        "production_state": "offline_compare_and_swap_preflight_only",
        "offline_post_execution_verifier_json": str(verifier_path),
        "offline_post_execution_verifier_sha256": _sha(verifier_path),
        "attempt_id": str(verifier.get("attempt_id") or ""),
        "run_id": str(verifier.get("run_id") or ""),
        "verified_step_id": executed_step_id,
        "verified_step_finished_at": execution_finished_at,
        "next_step_id_after_advance": next_step_id,
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha(plan_path),
        "authoritative_state_json": str(state_path),
        "expected_current_state_sha256": expected_state_sha,
        "observed_current_state_sha256": current_state_sha,
        "compare_and_swap_required": True,
        "candidate_state": candidate_state,
        "candidate_state_sha256": candidate_hash,
        "candidate_state_preview_json": "",
        "candidate_state_preview_sha256": "",
        "verified_outputs": verified_outputs,
        "execution_supported": False,
        "would_execute": False,
        "authoritative_state_write_allowed": False,
        "writes_authoritative_state": False,
        "state_file_created": False,
        "state_file_replaced": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "checks": checks,
        "next_action": (
            "Build a separately authorized atomic state writer that consumes this exact preflight and rechecks expected_current_state_sha256 immediately before replacement."
            if ready
            else "Keep authoritative state unchanged and resolve all blockers."
        ),
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["empty"])
        writer.writeheader()
        writer.writerows(dict(row) for row in rows)


def write_v1_5_authoritative_resume_offline_state_advance_preflight(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out / "v1_5_authoritative_resume_offline_state_advance_preflight.json",
        "candidate_preview": out / "v1_5_authoritative_resume_offline_state_candidate.json",
        "checks_csv": out / "v1_5_authoritative_resume_offline_state_advance_checks.csv",
        "summary_csv": out / "v1_5_authoritative_resume_offline_state_advance_summary.csv",
        "markdown": out / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_PREFLIGHT.md",
    }
    payload = dict(model)
    candidate = dict(payload.get("candidate_state") or {})
    if candidate:
        paths["candidate_preview"].write_bytes(_json_bytes(candidate))
        payload["candidate_state_preview_json"] = str(paths["candidate_preview"].resolve())
        payload["candidate_state_preview_sha256"] = _sha(paths["candidate_preview"])
    paths["json"].write_bytes(_json_bytes(payload))
    _write_csv(paths["checks_csv"], payload.get("checks") or [])
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": payload.get("overall_status"),
                "offline_state_advance_preflight_ready": payload.get(
                    "offline_state_advance_preflight_ready"
                ),
                "blocker_count": payload.get("blocker_count"),
                "verified_step_id": payload.get("verified_step_id"),
                "next_step_id_after_advance": payload.get("next_step_id_after_advance"),
                "expected_current_state_sha256": payload.get(
                    "expected_current_state_sha256"
                ),
                "candidate_state_sha256": payload.get("candidate_state_sha256"),
                "authoritative_state_write_allowed": payload.get(
                    "authoritative_state_write_allowed"
                ),
            }
        ],
    )
    paths["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 Authoritative Resume Offline State Advance Preflight",
                "",
                f"- overall_status: `{payload.get('overall_status')}`",
                f"- verified_step_id: `{payload.get('verified_step_id')}`",
                f"- next_step_id_after_advance: `{payload.get('next_step_id_after_advance')}`",
                f"- blocker_count: `{payload.get('blocker_count')}`",
                f"- compare_and_swap_required: `{payload.get('compare_and_swap_required')}`",
                f"- authoritative_state_write_allowed: `{payload.get('authoritative_state_write_allowed')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return paths


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_state_advance_preflight",
    "write_v1_5_authoritative_resume_offline_state_advance_preflight",
]
