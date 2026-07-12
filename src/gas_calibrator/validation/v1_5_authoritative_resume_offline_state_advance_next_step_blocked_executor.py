"""Keep execution of the reviewed V1.5 next-step plan blocked."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight import (
    READY_STATUS as AUTHORIZATION_READY_STATUS,
    SCHEMA as AUTHORIZATION_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight,
)
from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    _contains_reparse,
)

SCHEMA = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_blocked_executor_v1"
)
BLOCKED_READY_STATUS = (
    "blocked_pending_offline_advanced_resume_next_step_executor_implementation"
)
REVIEW_STATUS = "review_required"
AUTHORIZATION_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_authorization_preflight.json"
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


def build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor(
    *,
    next_step_authorization_preflight_json: str | Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    preflight_path = Path(next_step_authorization_preflight_json).absolute()
    preflight = _load(preflight_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    reasons: list[str] = []
    if preflight_path.name != AUTHORIZATION_FILENAME:
        reasons.append("next_step_authorization_preflight_filename_not_canonical")
    if _contains_reparse(preflight_path):
        reasons.append("next_step_authorization_preflight_path_contains_reparse_point")
    if preflight.get("schema") != AUTHORIZATION_SCHEMA:
        reasons.append("next_step_authorization_preflight_schema_invalid")
    if preflight.get("overall_status") != AUTHORIZATION_READY_STATUS:
        reasons.append("next_step_authorization_preflight_not_ready")
    if preflight.get("next_step_authorization_preflight_ready") is not True:
        reasons.append("next_step_authorization_preflight_ready_flag_not_true")
    if preflight.get("authorization_packet_validated_offline") is not True:
        reasons.append("authorization_packet_validated_offline_not_true")
    if preflight.get("plan_review_allowed") is not True:
        reasons.append("plan_review_allowed_not_true")
    if int(preflight.get("review_required_count") or 0) or preflight.get(
        "review_reasons"
    ):
        reasons.append("next_step_authorization_preflight_contains_review_reasons")
    try:
        recomputed = build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight(
            next_step_plan_json=preflight.get("next_step_plan_json"),
            authorization_packet_json=preflight.get("authorization_packet_json"),
            now=evaluated_at,
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    exact = (
        bool(recomputed)
        and {key: value for key, value in preflight.items() if key != "generated_at"}
        == {key: value for key, value in recomputed.items() if key != "generated_at"}
    )
    if not recomputed:
        reasons.append("next_step_authorization_preflight_recompute_failed")
    elif not exact:
        reasons.append("next_step_authorization_preflight_recompute_mismatch")
    for field in (
        "execution_supported",
        "next_step_execution_allowed",
        "resume_execution_allowed",
        "would_execute",
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
    ):
        if preflight.get(field) is not False:
            reasons.append(f"next_step_authorization_boundary_invalid:{field}")
    if preflight.get("not_real_acceptance_evidence") is not True:
        reasons.append(
            "next_step_authorization_boundary_invalid:not_real_acceptance_evidence"
        )
    ready = not reasons
    checks = [
        {
            "check": "authorization_fresh_recompute_and_lock_boundary",
            "status": "ready" if ready else "review_required",
            "reasons": reasons,
        },
        {
            "check": "next_step_executor_remains_unimplemented",
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
        "next_step_authorization_preflight_json": str(preflight_path),
        "next_step_authorization_preflight_sha256": _sha(preflight_path),
        "authorization_packet_json": str(
            preflight.get("authorization_packet_json") or ""
        ),
        "authorization_packet_sha256": str(
            preflight.get("authorization_packet_sha256") or ""
        ),
        "authorization_id": str(preflight.get("authorization_id") or ""),
        "authorization_expires_at": str(
            preflight.get("authorization_expires_at") or ""
        ),
        "next_step_plan_json": str(preflight.get("next_step_plan_json") or ""),
        "next_step_plan_sha256": str(
            preflight.get("next_step_plan_sha256") or ""
        ),
        "run_id": str(preflight.get("run_id") or ""),
        "attempt_id": str(preflight.get("attempt_id") or ""),
        "verified_step_id": str(preflight.get("verified_step_id") or ""),
        "next_step_id": str(preflight.get("next_step_id") or ""),
        "next_step_tool_module": str(
            preflight.get("next_step_tool_module") or ""
        ),
        "future_executor_must_recompute_authorization": True,
        "plan_review_allowed": ready,
        "execution_supported": False,
        "next_step_execution_allowed": False,
        "resume_execution_allowed": False,
        "execute_flag_allowed": False,
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
        "checks": checks,
        "next_action": (
            "Keep execution blocked. A separately reviewed future executor must "
            "recompute this authorization immediately before any physical action."
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


def write_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = (
        "v1_5_authoritative_resume_offline_state_advance_"
        "next_step_blocked_executor"
    )
    paths = {
        "json": out / f"{stem}.json",
        "checks_csv": out / f"{stem}_checks.csv",
        "markdown": out
        / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_BLOCKED_EXECUTOR.md",
    }
    paths["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_csv(paths["checks_csv"], model.get("checks") or [])
    paths["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 Offline Next-Step Blocked Executor",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- blocked_executor_ready: `{model.get('blocked_executor_ready')}`",
                f"- next_step_execution_allowed: `{model.get('next_step_execution_allowed')}`",
                f"- execute_flag_allowed: `{model.get('execute_flag_allowed')}`",
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
    "build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor",
    "write_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor",
]
