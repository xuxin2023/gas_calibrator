"""Validate one short-lived authorization for one exact V1.5 next step."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design import (
    FUTURE_AUTHORIZATION_SCHEMA,
    READY_STATUS as DESIGN_READY_STATUS,
    SCHEMA as DESIGN_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design,
)
from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    _contains_reparse,
)

SCHEMA = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_execution_authorization_validation_v1"
)
READY_STATUS = "ready_for_controlled_next_step_execution_preflight"
REVIEW_STATUS = "review_required"
AUTHORIZATION_SCHEMA = FUTURE_AUTHORIZATION_SCHEMA
AUTHORIZATION_OPERATION = "authorize_exact_v1_5_next_step_execution_once"
CONFIRMATION_TEMPLATE = "v1_5_exact_next_step_execution_once_v1"
MAX_AUTHORIZATION_TTL_S = 1800.0
DESIGN_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_controlled_executor_design.json"
)
AUTHORIZATION_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_execution_authorization.json"
)
AUTHORIZATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,95}$")

CONFIRMATION_FIELDS = (
    "exact_one_step_only",
    "no_substitute_entry",
    "no_shell",
    "no_executor_retry",
    "no_fallback",
    "no_automatic_state_advance",
    "mature_runner_owns_physics_and_qc",
    "failure_holds",
    "no_postgresql_or_release",
)


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return (
        value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


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
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""
    except OSError:
        return ""


def _command_sha(command: Sequence[Any]) -> str:
    normalized = json.dumps(
        [str(value) for value in command],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _same_path(value: Any, expected: Path) -> bool:
    try:
        return Path(str(value or "")).absolute() == expected.absolute()
    except (OSError, RuntimeError):
        return False


def _check(name: str, reasons: Sequence[str]) -> dict[str, Any]:
    return {
        "check": name,
        "status": "ready" if not reasons else "review_required",
        "reasons": list(reasons),
    }


def _chain(design: Mapping[str, Any]) -> dict[str, Any]:
    blocked_path = Path(
        str(design.get("next_step_blocked_executor_json") or "")
    ).absolute()
    blocked = _load(blocked_path)
    review_path = Path(
        str(blocked.get("next_step_authorization_preflight_json") or "")
    ).absolute()
    review = _load(review_path)
    plan_path = Path(str(blocked.get("next_step_plan_json") or "")).absolute()
    plan = _load(plan_path)
    consumer_path = Path(str(plan.get("consumer_readiness_json") or "")).absolute()
    full_flow_path = Path(str(plan.get("full_flow_plan_json") or "")).absolute()
    state_path = Path(str(plan.get("authoritative_state_json") or "")).absolute()
    command = [str(value) for value in plan.get("next_step_command") or []]
    return {
        "blocked_path": blocked_path,
        "review_path": review_path,
        "plan_path": plan_path,
        "consumer_path": consumer_path,
        "full_flow_path": full_flow_path,
        "state_path": state_path,
        "blocked": blocked,
        "review": review,
        "plan": plan,
        "command": command,
    }


def build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization(
    *,
    controlled_executor_design_json: str | Path,
    execution_authorization_json: str | Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    design_path = Path(controlled_executor_design_json).absolute()
    packet_path = Path(execution_authorization_json).absolute()
    design = _load(design_path)
    packet = _load(packet_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)

    design_reasons: list[str] = []
    if design_path.name != DESIGN_FILENAME:
        design_reasons.append("controlled_executor_design_filename_not_canonical")
    if _contains_reparse(design_path):
        design_reasons.append("controlled_executor_design_path_contains_reparse_point")
    if design.get("schema") != DESIGN_SCHEMA:
        design_reasons.append("controlled_executor_design_schema_invalid")
    if design.get("overall_status") != DESIGN_READY_STATUS:
        design_reasons.append("controlled_executor_design_not_ready")
    if design.get("controlled_next_step_executor_design_ready") is not True:
        design_reasons.append("controlled_executor_design_ready_flag_not_true")
    if int(design.get("review_required_count") or 0) or design.get("review_reasons"):
        design_reasons.append("controlled_executor_design_contains_review_reasons")
    for field in (
        "execution_supported",
        "next_step_execution_allowed",
        "resume_execution_allowed",
        "execute_flag_allowed",
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
        if design.get(field) is not False:
            design_reasons.append(
                f"controlled_executor_design_boundary_invalid:{field}"
            )
    if design.get("not_real_acceptance_evidence") is not True:
        design_reasons.append(
            "controlled_executor_design_boundary_invalid:not_real_acceptance_evidence"
        )
    blocked_path_for_recompute = Path(
        str(design.get("next_step_blocked_executor_json") or "")
    ).absolute()
    try:
        recomputed = build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design(
            next_step_blocked_executor_json=blocked_path_for_recompute,
            now=evaluated_at,
        )["manifest"]
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    exact = bool(recomputed) and {
        key: value for key, value in design.items() if key != "generated_at"
    } == {key: value for key, value in recomputed.items() if key != "generated_at"}
    if not recomputed:
        design_reasons.append("controlled_executor_design_recompute_failed")
    elif not exact:
        design_reasons.append("controlled_executor_design_recompute_mismatch")

    chain = _chain(design)
    packet_reasons: list[str] = []
    if packet_path.name != AUTHORIZATION_FILENAME:
        packet_reasons.append("execution_authorization_filename_not_canonical")
    if not packet_path.is_file():
        packet_reasons.append("execution_authorization_missing")
    if _contains_reparse(packet_path):
        packet_reasons.append("execution_authorization_path_contains_reparse_point")
    if packet.get("schema") != AUTHORIZATION_SCHEMA:
        packet_reasons.append("execution_authorization_schema_invalid")
    if packet.get("requested_operation") != AUTHORIZATION_OPERATION:
        packet_reasons.append("execution_authorization_operation_invalid")
    if packet.get("confirmation_template") != CONFIRMATION_TEMPLATE:
        packet_reasons.append("execution_authorization_confirmation_template_invalid")
    authorization_id = str(packet.get("authorization_id") or "")
    if not AUTHORIZATION_ID_RE.fullmatch(authorization_id):
        packet_reasons.append("execution_authorization_id_invalid")
    identities = [
        str(packet.get(field) or "").strip()
        for field in ("operator", "reviewer", "approver")
    ]
    if any(not identity for identity in identities):
        packet_reasons.append("execution_authorization_identity_missing")
    elif len(set(identities)) != 3:
        packet_reasons.append("execution_authorization_identities_must_be_distinct")
    issued = _parse_time(packet.get("issued_at"))
    expires = _parse_time(packet.get("expires_at"))
    if issued is None:
        packet_reasons.append("execution_authorization_issued_at_invalid")
    if expires is None:
        packet_reasons.append("execution_authorization_expires_at_invalid")
    if issued is not None and expires is not None:
        ttl_s = (expires - issued).total_seconds()
        if ttl_s <= 0 or ttl_s > MAX_AUTHORIZATION_TTL_S:
            packet_reasons.append("execution_authorization_ttl_out_of_range")
        if issued > evaluated_at:
            packet_reasons.append("execution_authorization_not_yet_valid")
        if expires <= evaluated_at:
            packet_reasons.append("execution_authorization_expired")

    path_bindings = {
        "controlled_executor_design_json": design_path,
        "blocked_executor_json": chain["blocked_path"],
        "review_authorization_preflight_json": chain["review_path"],
        "next_step_plan_json": chain["plan_path"],
        "consumer_readiness_json": chain["consumer_path"],
        "full_flow_plan_json": chain["full_flow_path"],
        "authoritative_state_json": chain["state_path"],
    }
    for field, expected_path in path_bindings.items():
        if not _same_path(packet.get(field), expected_path):
            packet_reasons.append(f"execution_authorization_path_mismatch:{field}")
        sha_field = field.removesuffix("_json") + "_sha256"
        if str(packet.get(sha_field) or "") != _sha(expected_path):
            packet_reasons.append(
                f"execution_authorization_sha256_mismatch:{sha_field}"
            )

    plan = chain["plan"]
    scalar_bindings = {
        "run_id": str(plan.get("run_id") or ""),
        "attempt_id": str(plan.get("attempt_id") or ""),
        "verified_step_id": str(plan.get("verified_step_id") or ""),
        "next_step_id": str(plan.get("next_step_id") or ""),
        "next_step_tool_module": str(plan.get("next_step_tool_module") or ""),
        "next_step_command_sha256": _command_sha(chain["command"]),
    }
    for field, expected in scalar_bindings.items():
        if str(packet.get(field) or "") != expected:
            packet_reasons.append(f"execution_authorization_binding_mismatch:{field}")
    if scalar_bindings["next_step_command_sha256"] != str(
        design.get("next_step_command_sha256_recorded_only") or ""
    ):
        packet_reasons.append("controlled_executor_design_command_sha256_mismatch")

    confirmations = dict(packet.get("structured_confirmation") or {})
    for field in CONFIRMATION_FIELDS:
        if confirmations.get(field) is not True:
            packet_reasons.append(
                f"execution_authorization_confirmation_missing:{field}"
            )
    expected_capabilities = {
        "allow_real_com": bool(plan.get("requires_real_com_authorization")),
        "allow_pressure_control": bool(plan.get("requires_pressure_authorization")),
        "allow_route_control": bool(plan.get("requires_route_authorization")),
        "allow_device_or_coefficient_write": bool(
            plan.get("requires_write_authorization")
        ),
        "allow_postgresql_import": False,
    }
    for field, expected in expected_capabilities.items():
        if packet.get(field) is not expected:
            packet_reasons.append(
                f"execution_authorization_capability_mismatch:{field}"
            )

    checks = [
        _check("controlled_executor_design_exact_recompute", design_reasons),
        _check("short_lived_three_party_exact_execution_authorization", packet_reasons),
    ]
    reasons = [*design_reasons, *packet_reasons]
    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _iso(evaluated_at),
        "overall_status": READY_STATUS if ready else REVIEW_STATUS,
        "execution_authorization_validated": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "controlled_executor_design_json": str(design_path),
        "controlled_executor_design_sha256": _sha(design_path),
        "execution_authorization_json": str(packet_path),
        "execution_authorization_sha256": _sha(packet_path),
        "authorization_id": authorization_id,
        "authorization_expires_at": str(packet.get("expires_at") or ""),
        **{
            field: str(path)
            for field, path in path_bindings.items()
            if field != "controlled_executor_design_json"
        },
        **{
            field.removesuffix("_json") + "_sha256": _sha(path)
            for field, path in path_bindings.items()
            if field != "controlled_executor_design_json"
        },
        **scalar_bindings,
        "authorized_capabilities": expected_capabilities,
        "immediate_preflight_required": True,
        "execution_supported": True,
        "next_step_execution_allowed": False,
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
        "checks": checks,
        "next_action": "Run the immediate preflight at execution time; this validation alone cannot start a process.",
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("check", "status", "reasons"))
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "check": row.get("check"),
                    "status": row.get("status"),
                    "reasons": ";".join(
                        str(value) for value in row.get("reasons") or []
                    ),
                }
            )


def write_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = (
        "v1_5_authoritative_resume_offline_state_advance_"
        "next_step_execution_authorization_validation"
    )
    paths = {
        "json": out / f"{stem}.json",
        "checks_csv": out / f"{stem}_checks.csv",
        "markdown": out / "V1_5_NEXT_STEP_EXECUTION_AUTHORIZATION_VALIDATION.md",
    }
    paths["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(paths["checks_csv"], model.get("checks") or [])
    paths["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 Next-Step Execution Authorization",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- authorization_validated: `{model.get('execution_authorization_validated')}`",
                f"- next_step_execution_allowed: `{model.get('next_step_execution_allowed')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return paths


__all__ = [
    "AUTHORIZATION_FILENAME",
    "AUTHORIZATION_OPERATION",
    "AUTHORIZATION_SCHEMA",
    "CONFIRMATION_FIELDS",
    "CONFIRMATION_TEMPLATE",
    "MAX_AUTHORIZATION_TTL_S",
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization",
    "write_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization",
]
