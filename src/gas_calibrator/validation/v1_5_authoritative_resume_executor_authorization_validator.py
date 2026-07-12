"""Offline validator for a future V1.5 authoritative resume authorization packet."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_executor_controlled_design import (
    FUTURE_AUTHORIZATION_SCHEMA,
    READY_STATUS as DESIGN_READY_STATUS,
    SCHEMA as DESIGN_SCHEMA,
    build_v1_5_authoritative_resume_executor_controlled_design,
)

SCHEMA = "v1_5_authoritative_resume_executor_authorization_validator_v1"
READY_STATUS = "ready_for_resume_executor_authorization_review"
REVIEW_STATUS = "review_required"
MAX_AUTHORIZATION_TTL_S = 1800.0

_DESIGN_COMPARE_KEYS = (
    "overall_status",
    "controlled_resume_executor_design_ready",
    "review_required_count",
    "review_reasons",
    "production_state",
    "authoritative_resume_executor_blocked_json",
    "authoritative_resume_executor_blocked_sha256",
    "future_authorization_schema",
    "next_step_id_recorded_only",
    "next_step_command_sha256_recorded_only",
    "execution_supported",
    "resume_execution_allowed",
    "execute_flag_allowed",
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


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
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


def _same_path(value: Any, expected: Path) -> bool:
    try:
        return Path(str(value or "")).resolve() == expected.resolve()
    except (OSError, RuntimeError):
        return False


def _expected_chain(design: Mapping[str, Any]) -> dict[str, Any]:
    blocked_path = Path(
        str(design.get("authoritative_resume_executor_blocked_json") or "")
    ).resolve()
    blocked = _load(blocked_path)
    contract_path = Path(str(blocked.get("consumer_contract_json") or "")).resolve()
    contract = _load(contract_path)
    plan_path = Path(str(contract.get("full_flow_plan_json") or "")).resolve()
    state_path = Path(str(contract.get("authoritative_state_json") or "")).resolve()
    plan = _load(plan_path)
    requirements = dict(blocked.get("authorization_requirements_recorded_only") or {})
    return {
        "blocked_path": blocked_path,
        "contract_path": contract_path,
        "plan_path": plan_path,
        "state_path": state_path,
        "run_id": str(plan.get("run_id") or ""),
        "next_step_id": str(blocked.get("next_step_id_recorded_only") or ""),
        "next_step_command_sha256": str(
            design.get("next_step_command_sha256_recorded_only") or ""
        ),
        "requirements": requirements,
    }


def _design_reasons(design_path: Path, design: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if design.get("schema") != DESIGN_SCHEMA:
        reasons.append("controlled_design_schema_invalid")
    if design.get("overall_status") != DESIGN_READY_STATUS:
        reasons.append("controlled_design_not_ready")
    if design.get("controlled_resume_executor_design_ready") is not True:
        reasons.append("controlled_design_ready_flag_not_true")
    blocked_path = Path(
        str(design.get("authoritative_resume_executor_blocked_json") or "")
    ).resolve()
    if str(design.get("authoritative_resume_executor_blocked_sha256") or "") != _sha(
        blocked_path
    ):
        reasons.append("controlled_design_blocked_executor_sha256_mismatch")
    try:
        recomputed = build_v1_5_authoritative_resume_executor_controlled_design(
            authoritative_resume_executor_blocked_json=blocked_path
        )["manifest"]
    except (OSError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if not recomputed:
        reasons.append("controlled_design_recompute_failed")
    else:
        for key in _DESIGN_COMPARE_KEYS:
            if design.get(key) != recomputed.get(key):
                reasons.append(f"controlled_design_recompute_mismatch:{key}")
    if not design_path.is_file():
        reasons.append("controlled_design_evidence_missing")
    return reasons


def _authorization_reasons(
    authorization: Mapping[str, Any],
    *,
    authorization_path: Path,
    design_path: Path,
    design: Mapping[str, Any],
    expected: Mapping[str, Any],
    now: datetime,
) -> list[str]:
    reasons: list[str] = []
    if authorization.get("schema") != FUTURE_AUTHORIZATION_SCHEMA:
        reasons.append("authorization_schema_invalid")
    for field in ("authorization_id", "operator", "reviewer", "approver"):
        if not str(authorization.get(field) or "").strip():
            reasons.append(f"authorization_field_missing:{field}")
    if str(authorization.get("reviewer") or "").strip() == str(
        authorization.get("approver") or ""
    ).strip():
        reasons.append("authorization_reviewer_must_differ_from_approver")

    issued = _parse_time(authorization.get("issued_at"))
    expires = _parse_time(authorization.get("expires_at"))
    if issued is None:
        reasons.append("authorization_issued_at_invalid")
    if expires is None:
        reasons.append("authorization_expires_at_invalid")
    if issued is not None and expires is not None:
        ttl = (expires - issued).total_seconds()
        if ttl <= 0 or ttl > MAX_AUTHORIZATION_TTL_S:
            reasons.append("authorization_ttl_out_of_range")
        if issued > now:
            reasons.append("authorization_not_yet_valid")
        if expires <= now:
            reasons.append("authorization_expired")

    path_fields = (
        ("controlled_design_json", design_path),
        ("blocked_executor_json", expected["blocked_path"]),
        ("consumer_contract_json", expected["contract_path"]),
        ("full_flow_plan_json", expected["plan_path"]),
        ("authoritative_state_json", expected["state_path"]),
    )
    for field, expected_path in path_fields:
        if not _same_path(authorization.get(field), expected_path):
            reasons.append(f"authorization_path_mismatch:{field}")
        sha_field = field.removesuffix("_json") + "_sha256"
        if str(authorization.get(sha_field) or "") != _sha(expected_path):
            reasons.append(f"authorization_sha256_mismatch:{sha_field}")
    if str(authorization.get("run_id") or "") != expected["run_id"]:
        reasons.append("authorization_run_id_mismatch")
    if str(authorization.get("next_step_id") or "") != expected["next_step_id"]:
        reasons.append("authorization_next_step_id_mismatch")
    if str(authorization.get("next_step_command_sha256") or "") != expected[
        "next_step_command_sha256"
    ]:
        reasons.append("authorization_next_step_command_sha256_mismatch")

    confirmation = dict(authorization.get("structured_confirmation") or {})
    for field in (
        "resume_only",
        "no_implicit_writes",
        "no_database_import",
        "no_unrelated_permissions",
    ):
        if confirmation.get(field) is not True:
            reasons.append(f"authorization_confirmation_missing:{field}")

    requirements = dict(expected["requirements"])
    expected_capabilities = {
        "allow_real_com": bool(requirements.get("real_com")),
        "allow_pressure_control": bool(requirements.get("pressure")),
        "allow_route_control": bool(requirements.get("route")),
        "allow_device_or_coefficient_write": bool(requirements.get("write")),
        "allow_postgresql_import": False,
    }
    for field, expected_value in expected_capabilities.items():
        if authorization.get(field) is not expected_value:
            reasons.append(f"authorization_capability_mismatch:{field}")
    if not authorization_path.is_file():
        reasons.append("authorization_packet_missing")
    return reasons


def build_v1_5_authoritative_resume_executor_authorization_validator(
    *,
    controlled_design_json: str | Path,
    authorization_packet_json: str | Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    design_path = Path(controlled_design_json).resolve()
    authorization_path = Path(authorization_packet_json).resolve()
    design = _load(design_path)
    authorization = _load(authorization_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    design_reasons = _design_reasons(design_path, design)
    expected = _expected_chain(design)
    authorization_reasons = _authorization_reasons(
        authorization,
        authorization_path=authorization_path,
        design_path=design_path,
        design=design,
        expected=expected,
        now=evaluated_at,
    )
    reasons = design_reasons + authorization_reasons
    ready = not reasons
    checks = [
        {
            "check": "controlled_design_recompute_binding",
            "status": "ready" if not design_reasons else "review_required",
            "reasons": design_reasons,
        },
        {
            "check": "authorization_identity_time_evidence_and_capability_binding",
            "status": "ready" if not authorization_reasons else "review_required",
            "reasons": authorization_reasons,
        },
        {
            "check": "execution_remains_unimplemented",
            "status": "ready",
            "reasons": [],
        },
    ]
    return {
        "schema": SCHEMA,
        "generated_at": _iso(evaluated_at),
        "overall_status": READY_STATUS if ready else REVIEW_STATUS,
        "resume_executor_authorization_validated_offline": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "controlled_design_json": str(design_path),
        "controlled_design_sha256": _sha(design_path),
        "authorization_packet_json": str(authorization_path),
        "authorization_packet_sha256": _sha(authorization_path),
        "authorization_id": str(authorization.get("authorization_id") or ""),
        "run_id": expected["run_id"],
        "next_step_id": expected["next_step_id"],
        "next_step_command_sha256": expected["next_step_command_sha256"],
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
            "Keep execution locked. A later executor must independently validate this packet again immediately before any action."
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


def write_v1_5_authoritative_resume_executor_authorization_validator(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "v1_5_resume_executor_authorization_validation.json"
    checks_path = out / "v1_5_resume_executor_authorization_validation_checks.csv"
    markdown_path = out / "V1_5_RESUME_EXECUTOR_AUTHORIZATION_VALIDATION.md"
    json_path.write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(checks_path, model.get("checks") or [])
    markdown_path.write_text(
        "\n".join(
            [
                "# V1.5 Resume Executor Authorization Validation",
                "",
                "This is offline authorization validation, not execution permission.",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- authorization_validated_offline: `{model.get('resume_executor_authorization_validated_offline')}`",
                f"- execution_supported: `{model.get('execution_supported')}`",
                f"- resume_execution_allowed: `{model.get('resume_execution_allowed')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {"json": json_path, "checks_csv": checks_path, "markdown": markdown_path}


__all__ = [
    "MAX_AUTHORIZATION_TTL_S",
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_executor_authorization_validator",
    "write_v1_5_authoritative_resume_executor_authorization_validator",
]
