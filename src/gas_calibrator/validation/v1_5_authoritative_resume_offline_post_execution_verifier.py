"""Verify one completed V1.5 offline resume step without advancing state."""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_offline_candidate_gate import (
    READY_STATUS as GATE_READY_STATUS,
    SCHEMA as GATE_SCHEMA,
    build_v1_5_authoritative_resume_offline_candidate_gate,
)
from .v1_5_authoritative_resume_offline_executor import (
    EXECUTED_STATUS,
    SCHEMA as EXECUTOR_SCHEMA,
)

SCHEMA = "v1_5_authoritative_resume_offline_post_execution_verifier_v1"
READY_STATUS = "ready_for_offline_resume_post_execution_review"
REVIEW_STATUS = "review_required"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else None


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


def _chain(executor: Mapping[str, Any]) -> dict[str, Any]:
    gate_path = Path(str(executor.get("offline_candidate_gate_json") or "")).resolve()
    gate = _load(gate_path)
    preflight = _load(Path(str(gate.get("execution_preflight_json") or "")).resolve())
    validation_path = Path(
        str(preflight.get("authorization_validation_json") or "")
    ).resolve()
    validation = _load(validation_path)
    authorization_path = Path(
        str(validation.get("authorization_packet_json") or "")
    ).resolve()
    authorization = _load(authorization_path)
    plan_path = Path(str(gate.get("full_flow_plan_json") or "")).resolve()
    plan = _load(plan_path)
    step_id = str(executor.get("next_step_id") or "")
    step = next(
        (
            dict(row)
            for row in plan.get("steps") or []
            if isinstance(row, Mapping) and str(row.get("step_id") or "") == step_id
        ),
        {},
    )
    state_path = Path(str(authorization.get("authoritative_state_json") or "")).resolve()
    return {
        "gate_path": gate_path,
        "gate": gate,
        "validation_path": validation_path,
        "validation": validation,
        "authorization_path": authorization_path,
        "authorization": authorization,
        "plan_path": plan_path,
        "step": step,
        "state_path": state_path,
    }


def build_v1_5_authoritative_resume_offline_post_execution_verifier(
    *, offline_executor_json: str | Path
) -> dict[str, Any]:
    executor_path = Path(offline_executor_json).resolve()
    executor = _load(executor_path)
    reasons: list[str] = []
    if executor.get("schema") != EXECUTOR_SCHEMA:
        reasons.append("offline_executor_schema_invalid")
    if executor.get("overall_status") != EXECUTED_STATUS:
        reasons.append("offline_executor_status_not_executed")
    if executor.get("offline_step_executed") is not True:
        reasons.append("offline_executor_executed_flag_not_true")
    if executor.get("offline_execution_requested") is not True:
        reasons.append("offline_executor_execution_requested_not_true")
    if executor.get("hold_count") != 0 or executor.get("hold_reasons"):
        reasons.append("offline_executor_contains_hold_reasons")
    if executor.get("process_attempted") is not True:
        reasons.append("offline_executor_process_attempted_not_true")
    if executor.get("process_return_code") != 0:
        reasons.append("offline_executor_return_code_not_zero")
    if executor.get("expected_outputs_fresh") is not True:
        reasons.append("offline_executor_outputs_not_fresh")
    if executor.get("authoritative_state_advanced") is not False:
        reasons.append("offline_executor_state_advanced_unexpectedly")
    if executor.get("execution_supported") is not True:
        reasons.append("offline_executor_execution_supported_not_true")
    if executor.get("offline_execution_only") is not True:
        reasons.append("offline_executor_offline_only_not_true")
    for field in (
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "connects_postgresql",
        "database_written",
        "formal_release_allowed",
        "database_import_allowed",
    ):
        if executor.get(field) is not False:
            reasons.append(f"offline_executor_boundary_invalid:{field}")
    if executor.get("not_real_acceptance_evidence") is not True:
        reasons.append("offline_executor_boundary_invalid:not_real_acceptance_evidence")

    chain = _chain(executor)
    gate_path = chain["gate_path"]
    gate = chain["gate"]
    if gate.get("schema") != GATE_SCHEMA:
        reasons.append("offline_candidate_gate_schema_invalid")
    if str(executor.get("offline_candidate_gate_sha256") or "") != _sha(gate_path):
        reasons.append("offline_executor_gate_sha256_mismatch")
    started_at = _parse_time(executor.get("started_at"))
    if started_at is None:
        reasons.append("offline_executor_started_at_invalid")
    else:
        try:
            recomputed_gate = build_v1_5_authoritative_resume_offline_candidate_gate(
                execution_preflight_json=gate.get("execution_preflight_json"),
                now=started_at,
            )
        except (OSError, ValueError, json.JSONDecodeError):
            recomputed_gate = {}
        if recomputed_gate.get("overall_status") != GATE_READY_STATUS:
            reasons.append("offline_executor_gate_not_ready_at_execution")
        if recomputed_gate.get("attempt_id") != executor.get("attempt_id"):
            reasons.append("offline_executor_attempt_id_mismatch")
        if recomputed_gate.get("next_step_id") != executor.get("next_step_id"):
            reasons.append("offline_executor_next_step_mismatch")

    step = chain["step"]
    plan_path = chain["plan_path"]
    plan_root = plan_path.parent
    if not step:
        reasons.append("offline_executor_canonical_step_missing")
    if str(gate.get("full_flow_plan_sha256") or "") != _sha(plan_path):
        reasons.append("offline_executor_plan_sha256_mismatch")
    canonical_command = [str(value) for value in step.get("command") or []]
    expected_runtime_command = [sys.executable, *canonical_command[1:]]
    if [str(value) for value in executor.get("executed_command") or []] != expected_runtime_command:
        reasons.append("offline_executor_runtime_command_mismatch")
    canonical_outputs = {
        str((plan_root / str(value)).resolve()) for value in step.get("expected_outputs") or []
    }
    recorded_paths = {str(Path(value).resolve()) for value in executor.get("expected_output_paths") or []}
    if recorded_paths != canonical_outputs:
        reasons.append("offline_executor_expected_output_path_set_mismatch")
    after = dict(executor.get("expected_output_sha256_after") or {})
    before = dict(executor.get("expected_output_sha256_before") or {})
    if set(after) != canonical_outputs or set(before) != canonical_outputs:
        reasons.append("offline_executor_output_hash_key_set_mismatch")
    verified_outputs: list[dict[str, Any]] = []
    for text in sorted(canonical_outputs):
        path = Path(text)
        current_sha = _sha(path)
        recorded_after = str(after.get(text) or "")
        recorded_before = str(before.get(text) or "")
        status = "ready"
        if not _is_within(path, plan_root):
            reasons.append(f"offline_executor_output_outside_plan_root:{path}")
            status = "review_required"
        if not current_sha:
            reasons.append(f"offline_executor_output_missing:{path}")
            status = "review_required"
        elif current_sha != recorded_after:
            reasons.append(f"offline_executor_output_sha256_mismatch:{path}")
            status = "review_required"
        if recorded_after == recorded_before:
            reasons.append(f"offline_executor_output_not_fresh:{path}")
            status = "review_required"
        verified_outputs.append(
            {
                "path": str(path),
                "recorded_before_sha256": recorded_before,
                "recorded_after_sha256": recorded_after,
                "current_sha256": current_sha,
                "status": status,
            }
        )

    authorization_path = chain["authorization_path"]
    authorization = chain["authorization"]
    validation = chain["validation"]
    if str(validation.get("authorization_packet_sha256") or "") != _sha(
        authorization_path
    ):
        reasons.append("authorization_packet_sha256_mismatch")
    state_path = chain["state_path"]
    expected_state_sha = str(authorization.get("authoritative_state_sha256") or "")
    current_state_sha = _sha(state_path)
    if not expected_state_sha or current_state_sha != expected_state_sha:
        reasons.append("authoritative_state_changed_during_offline_execution")
    ready = not reasons
    checks = [
        {
            "check": "executor_and_gate_binding",
            "status": "ready" if not any("gate" in reason for reason in reasons) else "review_required",
        },
        {
            "check": "canonical_outputs_hash_binding",
            "status": "ready" if all(row["status"] == "ready" for row in verified_outputs) else "review_required",
        },
        {
            "check": "authoritative_state_unchanged",
            "status": "ready" if current_state_sha == expected_state_sha else "review_required",
        },
    ]
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if ready else REVIEW_STATUS,
        "offline_post_execution_verification_ready": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "offline_executor_json": str(executor_path),
        "offline_executor_sha256": _sha(executor_path),
        "attempt_id": str(executor.get("attempt_id") or ""),
        "run_id": str(executor.get("run_id") or ""),
        "next_step_id": str(executor.get("next_step_id") or ""),
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha(plan_path),
        "authoritative_state_json": str(state_path),
        "authoritative_state_sha256_expected": expected_state_sha,
        "authoritative_state_sha256_current": current_state_sha,
        "verified_outputs": verified_outputs,
        "authoritative_state_advance_allowed": False,
        "execution_supported": False,
        "would_execute": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "checks": checks,
        "next_action": (
            "Build a separate compare-and-swap state-advance preflight bound to this verifier evidence."
            if ready
            else "Keep authoritative state unchanged and review verification reasons."
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
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(dict(row) for row in rows)


def write_v1_5_authoritative_resume_offline_post_execution_verifier(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "v1_5_authoritative_resume_offline_post_execution_verifier.json"
    outputs_path = out / "v1_5_authoritative_resume_offline_verified_outputs.csv"
    checks_path = out / "v1_5_authoritative_resume_offline_post_execution_checks.csv"
    markdown_path = out / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_POST_EXECUTION_VERIFIER.md"
    json_path.write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(outputs_path, model.get("verified_outputs") or [])
    _write_csv(checks_path, model.get("checks") or [])
    markdown_path.write_text(
        "\n".join(
            [
                "# V1.5 Authoritative Resume Offline Post-Execution Verifier",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- verification_ready: `{model.get('offline_post_execution_verification_ready')}`",
                f"- attempt_id: `{model.get('attempt_id')}`",
                f"- next_step_id: `{model.get('next_step_id')}`",
                f"- authoritative_state_advance_allowed: `{model.get('authoritative_state_advance_allowed')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "json": json_path,
        "verified_outputs_csv": outputs_path,
        "checks_csv": checks_path,
        "markdown": markdown_path,
    }


__all__ = [
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_post_execution_verifier",
    "write_v1_5_authoritative_resume_offline_post_execution_verifier",
]
