"""Keep V1.5 offline-resume state advancement blocked after authorization review."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_offline_state_advance_authorization import (
    READY_STATUS as AUTHORIZATION_READY_STATUS,
    SCHEMA as AUTHORIZATION_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_authorization,
)

SCHEMA = "v1_5_authoritative_resume_offline_state_advance_blocked_executor_v1"
BLOCKED_READY_STATUS = "blocked_pending_authoritative_resume_offline_state_advance_writer"
REVIEW_STATUS = "review_required"

AUTHORIZATION_COMPARE_KEYS = (
    "overall_status",
    "offline_state_advance_authorization_validated",
    "review_required_count",
    "review_reasons",
    "offline_state_advance_preflight_json",
    "offline_state_advance_preflight_sha256",
    "authorization_packet_json",
    "authorization_packet_sha256",
    "authorization_id",
    "authorization_expires_at",
    "run_id",
    "attempt_id",
    "verified_step_id",
    "next_step_id_after_advance",
    "authoritative_state_json",
    "expected_current_state_sha256",
    "candidate_state_preview_json",
    "candidate_state_sha256",
    "compare_and_swap_required",
    "execution_supported",
    "state_write_execution_allowed",
    "would_execute",
    "writes_authoritative_state",
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
    "checks",
)


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def build_v1_5_authoritative_resume_offline_state_advance_blocked_executor(
    *,
    state_advance_authorization_json: str | Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    validation_path = Path(state_advance_authorization_json).absolute()
    validation = _load(validation_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    reasons: list[str] = []
    if validation.get("schema") != AUTHORIZATION_SCHEMA:
        reasons.append("state_advance_authorization_schema_invalid")
    if validation.get("overall_status") != AUTHORIZATION_READY_STATUS:
        reasons.append("state_advance_authorization_not_ready")
    if validation.get("offline_state_advance_authorization_validated") is not True:
        reasons.append("state_advance_authorization_ready_flag_not_true")
    try:
        recomputed = build_v1_5_authoritative_resume_offline_state_advance_authorization(
            offline_state_advance_preflight_json=validation.get(
                "offline_state_advance_preflight_json"
            ),
            authorization_packet_json=validation.get("authorization_packet_json"),
            now=evaluated_at,
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if not recomputed:
        reasons.append("state_advance_authorization_recompute_failed")
    else:
        for key in AUTHORIZATION_COMPARE_KEYS:
            if validation.get(key) != recomputed.get(key):
                reasons.append(f"state_advance_authorization_recompute_mismatch:{key}")
    for field in (
        "execution_supported",
        "state_write_execution_allowed",
        "would_execute",
        "writes_authoritative_state",
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
        if validation.get(field) is not False:
            reasons.append(f"state_advance_authorization_boundary_invalid:{field}")
    if validation.get("not_real_acceptance_evidence") is not True:
        reasons.append(
            "state_advance_authorization_boundary_invalid:not_real_acceptance_evidence"
        )
    ready = not reasons
    checks = [
        {
            "check": "authorization_fresh_recompute_and_lock_boundary",
            "status": "ready" if ready else "review_required",
            "reasons": reasons,
        },
        {
            "check": "state_advance_writer_remains_unimplemented",
            "status": "ready",
            "reasons": [],
        },
    ]
    return {
        "schema": SCHEMA,
        "generated_at": _iso(evaluated_at),
        "overall_status": BLOCKED_READY_STATUS if ready else REVIEW_STATUS,
        "blocked_executor_ready": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "state_advance_authorization_json": str(validation_path),
        "state_advance_authorization_sha256": _sha(validation_path),
        "authorization_id": str(validation.get("authorization_id") or ""),
        "run_id": str(validation.get("run_id") or ""),
        "attempt_id": str(validation.get("attempt_id") or ""),
        "verified_step_id": str(validation.get("verified_step_id") or ""),
        "next_step_id_after_advance": str(
            validation.get("next_step_id_after_advance") or ""
        ),
        "authoritative_state_json": str(validation.get("authoritative_state_json") or ""),
        "expected_current_state_sha256": str(
            validation.get("expected_current_state_sha256") or ""
        ),
        "candidate_state_preview_json": str(
            validation.get("candidate_state_preview_json") or ""
        ),
        "candidate_state_sha256": str(validation.get("candidate_state_sha256") or ""),
        "future_atomic_writer_must_recompute_authorization": True,
        "execution_supported": False,
        "state_write_execution_allowed": False,
        "execute_flag_allowed": False,
        "would_execute": False,
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
            "Keep state advancement blocked. A future atomic writer requires a separate implementation and must consume this exact ready lock proof."
        ),
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields = ("check", "status", "reasons")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "check": row.get("check"),
                    "status": row.get("status"),
                    "reasons": ";".join(str(value) for value in row.get("reasons") or []),
                }
            )


def write_v1_5_authoritative_resume_offline_state_advance_blocked_executor(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out
        / "v1_5_authoritative_resume_offline_state_advance_blocked_executor.json",
        "checks_csv": out
        / "v1_5_authoritative_resume_offline_state_advance_blocked_executor_checks.csv",
        "markdown": out
        / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_BLOCKED_EXECUTOR.md",
    }
    paths["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(paths["checks_csv"], model.get("checks") or [])
    paths["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 Offline Resume State Advance Blocked Executor",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- blocked_executor_ready: `{model.get('blocked_executor_ready')}`",
                f"- execution_supported: `{model.get('execution_supported')}`",
                f"- state_write_execution_allowed: `{model.get('state_write_execution_allowed')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return paths


__all__ = [
    "BLOCKED_READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_state_advance_blocked_executor",
    "write_v1_5_authoritative_resume_offline_state_advance_blocked_executor",
]
