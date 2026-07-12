"""Last-moment offline preflight for a future V1.5 resume executor."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_executor_authorization_validator import (
    READY_STATUS as VALIDATION_READY_STATUS,
    SCHEMA as VALIDATION_SCHEMA,
    build_v1_5_authoritative_resume_executor_authorization_validator,
)

SCHEMA = "v1_5_authoritative_resume_execution_preflight_v1"
READY_STATUS = "ready_for_resume_execution_preflight_review"
REVIEW_STATUS = "review_required"

_VALIDATION_COMPARE_KEYS = (
    "overall_status",
    "resume_executor_authorization_validated_offline",
    "review_required_count",
    "review_reasons",
    "controlled_design_json",
    "controlled_design_sha256",
    "authorization_packet_json",
    "authorization_packet_sha256",
    "authorization_id",
    "run_id",
    "next_step_id",
    "next_step_command_sha256",
    "execution_supported",
    "resume_execution_allowed",
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


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _expected_command(validation: Mapping[str, Any]) -> list[str]:
    design = _load(Path(str(validation.get("controlled_design_json") or "")).resolve())
    blocked = _load(
        Path(str(design.get("authoritative_resume_executor_blocked_json") or "")).resolve()
    )
    return [str(value) for value in blocked.get("next_step_command_recorded_only") or []]


def _attempt_id(
    *, authorization_sha: str, run_id: str, next_step_id: str, command_sha: str, now: datetime
) -> str:
    raw = "|".join((authorization_sha, run_id, next_step_id, command_sha, _iso(now)))
    return f"resume-attempt-{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:20]}"


def build_v1_5_authoritative_resume_execution_preflight(
    *, authorization_validation_json: str | Path, now: datetime | None = None
) -> dict[str, Any]:
    validation_path = Path(authorization_validation_json).resolve()
    validation = _load(validation_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    reasons: list[str] = []
    if validation.get("schema") != VALIDATION_SCHEMA:
        reasons.append("authorization_validation_schema_invalid")
    if validation.get("overall_status") != VALIDATION_READY_STATUS:
        reasons.append("authorization_validation_not_ready")
    if validation.get("resume_executor_authorization_validated_offline") is not True:
        reasons.append("authorization_validation_ready_flag_not_true")
    for key, expected in _LOCKED_BOUNDARY.items():
        if validation.get(key) is not expected:
            reasons.append(f"authorization_validation_boundary_invalid:{key}")

    design_path = Path(str(validation.get("controlled_design_json") or "")).resolve()
    authorization_path = Path(
        str(validation.get("authorization_packet_json") or "")
    ).resolve()
    if str(validation.get("controlled_design_sha256") or "") != _sha(design_path):
        reasons.append("authorization_validation_design_sha256_mismatch")
    if str(validation.get("authorization_packet_sha256") or "") != _sha(
        authorization_path
    ):
        reasons.append("authorization_validation_packet_sha256_mismatch")
    try:
        recomputed = build_v1_5_authoritative_resume_executor_authorization_validator(
            controlled_design_json=design_path,
            authorization_packet_json=authorization_path,
            now=evaluated_at,
        )
    except (OSError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if not recomputed:
        reasons.append("authorization_validation_recompute_failed")
    else:
        for key in _VALIDATION_COMPARE_KEYS:
            if validation.get(key) != recomputed.get(key):
                reasons.append(f"authorization_validation_recompute_mismatch:{key}")
        reasons.extend(
            f"authorization_revalidation:{reason}"
            for reason in recomputed.get("review_reasons") or []
        )

    authorization = _load(authorization_path)
    expires_at = _parse_time(authorization.get("expires_at"))
    seconds_remaining = (
        max(0.0, (expires_at - evaluated_at).total_seconds())
        if expires_at is not None
        else 0.0
    )
    command = _expected_command(validation)
    command_sha = str(validation.get("next_step_command_sha256") or "")
    ready = not reasons
    attempt_id = _attempt_id(
        authorization_sha=_sha(authorization_path),
        run_id=str(validation.get("run_id") or ""),
        next_step_id=str(validation.get("next_step_id") or ""),
        command_sha=command_sha,
        now=evaluated_at,
    )
    capability_envelope = {
        field: bool(authorization.get(field))
        for field in (
            "allow_real_com",
            "allow_pressure_control",
            "allow_route_control",
            "allow_device_or_coefficient_write",
            "allow_postgresql_import",
        )
    }
    checks = [
        {
            "check": "authorization_validation_fresh_recompute",
            "status": "ready" if not reasons else "review_required",
            "reasons": reasons,
        },
        {
            "check": "attempt_envelope_recorded_only",
            "status": "ready",
            "reasons": [],
        },
        {
            "check": "execution_still_locked",
            "status": "ready",
            "reasons": [],
        },
    ]
    return {
        "schema": SCHEMA,
        "generated_at": _iso(evaluated_at),
        "overall_status": READY_STATUS if ready else REVIEW_STATUS,
        "resume_execution_preflight_ready": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "attempt_id": attempt_id,
        "authorization_validation_json": str(validation_path),
        "authorization_validation_sha256": _sha(validation_path),
        "authorization_id": str(validation.get("authorization_id") or ""),
        "authorization_seconds_remaining": seconds_remaining,
        "run_id": str(validation.get("run_id") or ""),
        "next_step_id_recorded_only": str(validation.get("next_step_id") or ""),
        "next_step_command_recorded_only": command,
        "next_step_command_sha256_recorded_only": command_sha,
        "capability_envelope_recorded_only": capability_envelope,
        "execution_supported": False,
        "resume_execution_allowed": False,
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
        "checks": checks,
        "next_action": (
            "Keep execution locked. A later executor must recompute this preflight immediately before acting and consume only this exact attempt envelope."
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


def write_v1_5_authoritative_resume_execution_preflight(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "v1_5_authoritative_resume_execution_preflight.json"
    checks_path = out / "v1_5_authoritative_resume_execution_preflight_checks.csv"
    markdown_path = out / "V1_5_AUTHORITATIVE_RESUME_EXECUTION_PREFLIGHT.md"
    json_path.write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(checks_path, model.get("checks") or [])
    markdown_path.write_text(
        "\n".join(
            [
                "# V1.5 Authoritative Resume Execution Preflight",
                "",
                "This is a last-moment offline preflight, not execution.",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- attempt_id: `{model.get('attempt_id')}`",
                f"- authorization_seconds_remaining: `{model.get('authorization_seconds_remaining')}`",
                f"- execution_supported: `{model.get('execution_supported')}`",
                f"- resume_execution_allowed: `{model.get('resume_execution_allowed')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {"json": json_path, "checks_csv": checks_path, "markdown": markdown_path}


__all__ = [
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_execution_preflight",
    "write_v1_5_authoritative_resume_execution_preflight",
]
