"""Offline design for a future controlled V1.5 authoritative resume executor."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_executor_blocked import (
    BLOCKED_STATUS,
    SCHEMA as BLOCKED_SCHEMA,
    build_v1_5_authoritative_resume_executor_blocked,
)

SCHEMA = "v1_5_authoritative_resume_executor_controlled_design_v1"
READY_STATUS = "ready_for_controlled_resume_executor_design_review"
REVIEW_STATUS = "review_required"
FUTURE_AUTHORIZATION_SCHEMA = "v1_5_authoritative_resume_executor_authorization_v1"

_BLOCKED_COMPARE_KEYS = (
    "overall_status",
    "blocked_executor_ready",
    "review_required_count",
    "review_reasons",
    "production_state",
    "resume_executor_plan_preview_json",
    "resume_executor_plan_preview_sha256",
    "consumer_contract_json",
    "next_step_id_recorded_only",
    "next_step_command_recorded_only",
    "authorization_requirements_recorded_only",
    "rejected_execution_flags",
    "execution_supported",
    "resume_execution_allowed",
    "execution_requested",
    "does_not_execute_commands",
    "would_execute",
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
    "not_real_acceptance_evidence",
)

_LOCKED_BOUNDARY = {
    "execution_supported": False,
    "resume_execution_allowed": False,
    "execution_requested": False,
    "does_not_execute_commands": True,
    "would_execute": False,
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
}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def _command_sha(command: Sequence[Any]) -> str:
    normalized = json.dumps(
        [str(value) for value in command], ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _blocked_reasons(path: Path, payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if payload.get("schema") != BLOCKED_SCHEMA:
        reasons.append("blocked_executor_schema_invalid")
    if payload.get("overall_status") != BLOCKED_STATUS:
        reasons.append("blocked_executor_status_invalid")
    if payload.get("blocked_executor_ready") is not True:
        reasons.append("blocked_executor_ready_flag_not_true")
    for key, expected in _LOCKED_BOUNDARY.items():
        if payload.get(key) is not expected:
            reasons.append(f"blocked_executor_boundary_invalid:{key}")

    preview_path = Path(
        str(payload.get("resume_executor_plan_preview_json") or "")
    ).resolve()
    if str(payload.get("resume_executor_plan_preview_sha256") or "") != _sha(preview_path):
        reasons.append("blocked_executor_plan_preview_sha256_mismatch")
    try:
        recomputed = build_v1_5_authoritative_resume_executor_blocked(
            resume_executor_plan_preview_json=preview_path
        )
    except (OSError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if not recomputed:
        reasons.append("blocked_executor_recompute_failed")
    else:
        for key in _BLOCKED_COMPARE_KEYS:
            if payload.get(key) != recomputed.get(key):
                reasons.append(f"blocked_executor_recompute_mismatch:{key}")
    if not path.is_file():
        reasons.append("blocked_executor_evidence_missing")
    return reasons


def build_v1_5_authoritative_resume_executor_controlled_design(
    *, authoritative_resume_executor_blocked_json: str | Path
) -> dict[str, Any]:
    blocked_path = Path(authoritative_resume_executor_blocked_json).resolve()
    blocked = _load(blocked_path)
    reasons = _blocked_reasons(blocked_path, blocked)
    next_step_id = str(blocked.get("next_step_id_recorded_only") or "")
    next_command = [str(value) for value in blocked.get("next_step_command_recorded_only") or []]
    requirements = dict(blocked.get("authorization_requirements_recorded_only") or {})

    authorization_contract = [
        {
            "field": "authorization_id",
            "required": True,
            "contract": "Unique immutable authorization identifier.",
        },
        {
            "field": "operator_reviewer_approver",
            "required": True,
            "contract": "Operator, reviewer, and approver are present; reviewer and approver are distinct.",
        },
        {
            "field": "issued_at_expires_at",
            "required": True,
            "contract": "Authorization has UTC issue and expiry timestamps and is rejected after expiry.",
        },
        {
            "field": "run_plan_state_binding",
            "required": True,
            "contract": "Bind run_id, full-flow plan SHA256, authoritative-state SHA256, consumer-contract SHA256, and blocked-executor SHA256.",
        },
        {
            "field": "next_step_binding",
            "required": True,
            "contract": "Bind exact next_step_id and normalized command SHA256; no substitute step or command is permitted.",
        },
        {
            "field": "structured_confirmation",
            "required": True,
            "contract": "Confirmation states resume-only, no implicit writes, no implicit database import, and no unrelated physical permissions.",
        },
    ]
    capability_contract = [
        {
            "capability": capability,
            "required_by_canonical_next_step": bool(requirements.get(key)),
            "future_authorization_field": f"allow_{capability}",
            "default": False,
            "rule": "May be true only when the canonical next step requires it; otherwise authorization is rejected.",
        }
        for capability, key in (
            ("real_com", "real_com"),
            ("pressure_control", "pressure"),
            ("route_control", "route"),
            ("device_or_coefficient_write", "write"),
        )
    ]
    capability_contract.append(
        {
            "capability": "postgresql_import",
            "required_by_canonical_next_step": False,
            "future_authorization_field": "allow_postgresql_import",
            "default": False,
            "rule": "Never granted by resume authorization; database import remains a separate controlled stage.",
        }
    )
    hold_contract = [
        {
            "trigger": "evidence_hash_or_recompute_mismatch",
            "action": "hold_before_execution",
            "meaning": "Do not trust copied ready flags or changed plan/state evidence.",
        },
        {
            "trigger": "authorization_missing_expired_or_identity_conflict",
            "action": "hold_before_execution",
            "meaning": "No implicit, stale, self-approved, or ambiguous authorization.",
        },
        {
            "trigger": "next_step_or_command_mismatch",
            "action": "hold_before_execution",
            "meaning": "Resume only the exact canonical next step from the verified state.",
        },
        {
            "trigger": "unneeded_capability_granted",
            "action": "hold_before_execution",
            "meaning": "Resume authorization cannot become a broad COM, pressure, route, write, or database permit.",
        },
        {
            "trigger": "runtime_failure_or_partial_side_effect",
            "action": "stop_and_require_new_evidence",
            "meaning": "A future executor must record attempts and never silently advance authoritative state.",
        },
    ]
    ready = not reasons
    manifest = {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if ready else REVIEW_STATUS,
        "controlled_resume_executor_design_ready": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "production_state": "blocked_design_only",
        "authoritative_resume_executor_blocked_json": str(blocked_path),
        "authoritative_resume_executor_blocked_sha256": _sha(blocked_path),
        "future_authorization_schema": FUTURE_AUTHORIZATION_SCHEMA,
        "next_step_id_recorded_only": next_step_id,
        "next_step_command_sha256_recorded_only": _command_sha(next_command),
        "execution_supported": False,
        "resume_execution_allowed": False,
        "execute_flag_allowed": False,
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
        "next_action": (
            "Keep resume execution locked. Build a separate authorization validator before any executor can consume this design."
        ),
    }
    return {
        "manifest": manifest,
        "authorization_contract": authorization_contract,
        "capability_contract": capability_contract,
        "hold_contract": hold_contract,
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


def write_v1_5_authoritative_resume_executor_controlled_design(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "manifest": out / "v1_5_authoritative_resume_executor_controlled_design.json",
        "authorization_contract": out / "v1_5_resume_executor_authorization_contract.csv",
        "capability_contract": out / "v1_5_resume_executor_capability_contract.csv",
        "hold_contract": out / "v1_5_resume_executor_hold_contract.csv",
        "markdown": out / "V1_5_AUTHORITATIVE_RESUME_EXECUTOR_CONTROLLED_DESIGN.md",
    }
    outputs["manifest"].write_text(
        json.dumps(dict(model["manifest"]), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    for key in ("authorization_contract", "capability_contract", "hold_contract"):
        _write_csv(outputs[key], model[key])
    manifest = model["manifest"]
    outputs["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 Authoritative Resume Executor Controlled Design",
                "",
                "This is an offline design, not a resume executor.",
                "",
                f"- overall_status: `{manifest.get('overall_status')}`",
                f"- production_state: `{manifest.get('production_state')}`",
                f"- execution_supported: `{manifest.get('execution_supported')}`",
                f"- resume_execution_allowed: `{manifest.get('resume_execution_allowed')}`",
                f"- opens_com_ports: `{manifest.get('opens_com_ports')}`",
                f"- connects_postgresql: `{manifest.get('connects_postgresql')}`",
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
    "build_v1_5_authoritative_resume_executor_controlled_design",
    "write_v1_5_authoritative_resume_executor_controlled_design",
]
