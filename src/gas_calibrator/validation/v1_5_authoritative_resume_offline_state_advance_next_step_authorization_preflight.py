"""Validate human review authorization for an exact offline next-step plan."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_offline_state_advance_next_step_plan import (
    READY_STATUS as PLAN_READY_STATUS,
    SCHEMA as PLAN_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_plan,
)
from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    _contains_reparse,
)

SCHEMA = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_authorization_preflight_v1"
)
READY_STATUS = "ready_for_offline_advanced_resume_next_step_authorization_preflight_review"
REVIEW_STATUS = "review_required"
AUTHORIZATION_SCHEMA = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_review_authorization_v1"
)
AUTHORIZATION_OPERATION = "authorize_exact_next_step_plan_review_without_execution"
CONFIRMATION_TEMPLATE = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_review_no_execution_v1"
)
PLAN_FILENAME = "v1_5_authoritative_resume_offline_state_advance_next_step_plan.json"
AUTHORIZATION_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_authorization_packet.json"
)
MAX_AUTHORIZATION_TTL_S = 1800.0
AUTHORIZATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,95}$")

CONFIRMATION_FIELDS = (
    "exact_plan_only",
    "review_only",
    "no_execution",
    "no_com",
    "no_pressure_control",
    "no_route_control",
    "no_device_or_coefficient_write",
    "no_postgresql_or_release",
    "mature_route_unchanged",
)


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
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""
    except OSError:
        return ""


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


def build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight(
    *,
    next_step_plan_json: str | Path,
    authorization_packet_json: str | Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    plan_recorded_path = Path(next_step_plan_json).absolute()
    plan_path = plan_recorded_path.resolve()
    authorization_recorded_path = Path(authorization_packet_json).absolute()
    authorization_path = authorization_recorded_path.resolve()
    plan = _load(plan_path)
    authorization = _load(authorization_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)

    plan_reasons: list[str] = []
    if plan_recorded_path.name != PLAN_FILENAME:
        plan_reasons.append("next_step_plan_filename_not_canonical")
    if _contains_reparse(plan_recorded_path):
        plan_reasons.append("next_step_plan_path_contains_reparse_point")
    if plan.get("schema") != PLAN_SCHEMA:
        plan_reasons.append("next_step_plan_schema_invalid")
    if plan.get("overall_status") != PLAN_READY_STATUS:
        plan_reasons.append("next_step_plan_not_ready")
    if plan.get("next_step_plan_review_ready") is not True:
        plan_reasons.append("next_step_plan_ready_flag_not_true")
    if int(plan.get("blocker_count") or 0) or plan.get("blocker_reasons"):
        plan_reasons.append("next_step_plan_contains_blockers")
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
        if plan.get(field) is not False:
            plan_reasons.append(f"next_step_plan_{field}_not_false")
    if plan.get("not_real_acceptance_evidence") is not True:
        plan_reasons.append("next_step_plan_real_acceptance_boundary_missing")
    consumer_path = Path(str(plan.get("consumer_readiness_json") or "")).absolute()
    try:
        recomputed = build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
            consumer_readiness_json=consumer_path
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    exact = (
        bool(recomputed)
        and {key: value for key, value in plan.items() if key != "generated_at"}
        == {key: value for key, value in recomputed.items() if key != "generated_at"}
    )
    if not recomputed:
        plan_reasons.append("next_step_plan_recompute_failed")
    elif not exact:
        plan_reasons.append("next_step_plan_recompute_mismatch")
    checks = [_check("exact_next_step_plan", plan_reasons)]

    authorization_reasons: list[str] = []
    if authorization_recorded_path.name != AUTHORIZATION_FILENAME:
        authorization_reasons.append("authorization_packet_filename_not_canonical")
    if not authorization_recorded_path.is_file():
        authorization_reasons.append("authorization_packet_missing")
    if _contains_reparse(authorization_recorded_path):
        authorization_reasons.append("authorization_packet_path_contains_reparse_point")
    if authorization.get("schema") != AUTHORIZATION_SCHEMA:
        authorization_reasons.append("authorization_schema_invalid")
    if authorization.get("requested_operation") != AUTHORIZATION_OPERATION:
        authorization_reasons.append("authorization_operation_invalid")
    if authorization.get("confirmation_template") != CONFIRMATION_TEMPLATE:
        authorization_reasons.append("authorization_confirmation_template_invalid")
    authorization_id = str(authorization.get("authorization_id") or "")
    if not AUTHORIZATION_ID_RE.fullmatch(authorization_id):
        authorization_reasons.append("authorization_id_invalid")
    people = [
        str(authorization.get(field) or "").strip()
        for field in ("operator", "reviewer", "approver")
    ]
    if any(not value for value in people):
        authorization_reasons.append("authorization_identity_missing")
    elif len(set(people)) != 3:
        authorization_reasons.append("authorization_identities_must_be_distinct")
    issued = _parse_time(authorization.get("issued_at"))
    expires = _parse_time(authorization.get("expires_at"))
    if issued is None:
        authorization_reasons.append("authorization_issued_at_invalid")
    if expires is None:
        authorization_reasons.append("authorization_expires_at_invalid")
    if issued is not None and expires is not None:
        ttl = (expires - issued).total_seconds()
        if ttl <= 0 or ttl > MAX_AUTHORIZATION_TTL_S:
            authorization_reasons.append("authorization_ttl_out_of_range")
        if issued > evaluated_at:
            authorization_reasons.append("authorization_not_yet_valid")
        if expires <= evaluated_at:
            authorization_reasons.append("authorization_expired")

    exact_bindings = {
        "next_step_plan_json": str(plan_recorded_path),
        "next_step_plan_sha256": _sha(plan_recorded_path),
        "consumer_readiness_json": str(consumer_path),
        "consumer_readiness_sha256": str(plan.get("consumer_readiness_sha256") or ""),
        "run_id": str(plan.get("run_id") or ""),
        "attempt_id": str(plan.get("attempt_id") or ""),
        "verified_step_id": str(plan.get("verified_step_id") or ""),
        "next_step_id": str(plan.get("next_step_id") or ""),
        "next_step_tool_module": str(plan.get("next_step_tool_module") or ""),
    }
    for field, expected in exact_bindings.items():
        actual = str(authorization.get(field) or "")
        if field.endswith("_json"):
            if not _same_path(actual, Path(expected)):
                authorization_reasons.append(f"authorization_path_mismatch:{field}")
        elif actual != expected:
            authorization_reasons.append(f"authorization_binding_mismatch:{field}")
    confirmations = dict(authorization.get("structured_confirmation") or {})
    for field in CONFIRMATION_FIELDS:
        if confirmations.get(field) is not True:
            authorization_reasons.append(f"authorization_confirmation_missing:{field}")
    expected_capabilities = {
        "allow_plan_review": True,
        "allow_next_step_execution": False,
        "allow_real_com": False,
        "allow_pressure_control": False,
        "allow_route_control": False,
        "allow_device_or_coefficient_write": False,
        "allow_postgresql_import": False,
    }
    for field, expected in expected_capabilities.items():
        if authorization.get(field) is not expected:
            authorization_reasons.append(f"authorization_capability_mismatch:{field}")
    for field in (
        "next_step_execution_allowed",
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
    ):
        if authorization.get(field) is not False:
            authorization_reasons.append(f"authorization_boundary_{field}_not_false")
    if authorization.get("not_real_acceptance_evidence") is not True:
        authorization_reasons.append(
            "authorization_real_acceptance_boundary_missing"
        )
    checks.append(
        _check(
            "authorization_identity_expiry_confirmation_and_exact_bindings",
            authorization_reasons,
        )
    )

    reasons = [*plan_reasons, *authorization_reasons]
    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _iso(evaluated_at),
        "overall_status": READY_STATUS if ready else REVIEW_STATUS,
        "next_step_authorization_preflight_ready": ready,
        "authorization_packet_validated_offline": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "next_step_plan_json": str(plan_recorded_path),
        "next_step_plan_sha256": _sha(plan_recorded_path),
        "authorization_packet_json": str(authorization_recorded_path),
        "authorization_packet_sha256": _sha(authorization_recorded_path),
        "authorization_id": authorization_id,
        "authorization_expires_at": str(authorization.get("expires_at") or ""),
        "consumer_readiness_json": exact_bindings["consumer_readiness_json"],
        "consumer_readiness_sha256": exact_bindings["consumer_readiness_sha256"],
        "run_id": exact_bindings["run_id"],
        "attempt_id": exact_bindings["attempt_id"],
        "verified_step_id": exact_bindings["verified_step_id"],
        "next_step_id": exact_bindings["next_step_id"],
        "next_step_tool_module": exact_bindings["next_step_tool_module"],
        "plan_review_allowed": ready,
        "execution_supported": False,
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
        "next_action": (
            "Review the exact plan only. A separate future executor authorization must "
            "revalidate all evidence before any physical action."
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


def write_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = (
        "v1_5_authoritative_resume_offline_state_advance_"
        "next_step_authorization_preflight"
    )
    paths = {
        "json": out / f"{stem}.json",
        "checks_csv": out / f"{stem}_checks.csv",
        "markdown": out
        / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_AUTHORIZATION_PREFLIGHT.md",
    }
    paths["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_csv(paths["checks_csv"], model.get("checks") or [])
    paths["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 Offline Next-Step Authorization Preflight",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- plan_review_allowed: `{model.get('plan_review_allowed')}`",
                f"- next_step_execution_allowed: `{model.get('next_step_execution_allowed')}`",
                f"- next_step_id: `{model.get('next_step_id')}`",
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
    "CONFIRMATION_TEMPLATE",
    "MAX_AUTHORIZATION_TTL_S",
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight",
    "write_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight",
]
