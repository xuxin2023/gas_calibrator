"""Validate a future V1.5 offline-resume state-advance authorization packet."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_offline_state_advance_preflight import (
    READY_STATUS as PREFLIGHT_READY_STATUS,
    SCHEMA as PREFLIGHT_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_preflight,
)

SCHEMA = "v1_5_authoritative_resume_offline_state_advance_authorization_v1"
READY_STATUS = "ready_for_authoritative_resume_offline_state_advance_authorization_review"
REVIEW_STATUS = "review_required"
AUTHORIZATION_SCHEMA = "v1_5_authoritative_resume_offline_state_advance_write_authorization_v1"
AUTHORIZATION_OPERATION = "authorize_authoritative_resume_state_advance_after_verified_offline_step"
CONFIRMATION_TEMPLATE = "v1_5_authoritative_resume_offline_state_advance_authorization_v1"
MAX_AUTHORIZATION_TTL_S = 1800.0
PREFLIGHT_FILENAME = "v1_5_authoritative_resume_offline_state_advance_preflight.json"
CANDIDATE_FILENAME = "v1_5_authoritative_resume_offline_state_candidate.json"
AUTHORIZATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,95}$")

PREFLIGHT_COMPARE_KEYS = (
    "overall_status",
    "offline_state_advance_preflight_ready",
    "blocker_count",
    "blocker_reasons",
    "production_state",
    "offline_post_execution_verifier_json",
    "offline_post_execution_verifier_sha256",
    "attempt_id",
    "run_id",
    "verified_step_id",
    "verified_step_finished_at",
    "next_step_id_after_advance",
    "full_flow_plan_json",
    "full_flow_plan_sha256",
    "authoritative_state_json",
    "expected_current_state_sha256",
    "observed_current_state_sha256",
    "compare_and_swap_required",
    "candidate_state",
    "candidate_state_sha256",
    "verified_outputs",
    "execution_supported",
    "would_execute",
    "authoritative_state_write_allowed",
    "writes_authoritative_state",
    "state_file_created",
    "state_file_replaced",
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


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _candidate_sha(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_bytes(payload)).hexdigest() if payload else ""


def _same_path(value: Any, expected: Path) -> bool:
    try:
        return Path(str(value or "")).absolute() == expected.absolute()
    except (OSError, RuntimeError):
        return False


def _has_reparse_point(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    return path.is_symlink() or bool(
        attributes & int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0) or 0)
    )


def _check(name: str, reasons: Sequence[str]) -> dict[str, Any]:
    return {
        "check": name,
        "status": "ready" if not reasons else "review_required",
        "reasons": list(reasons),
    }


def build_v1_5_authoritative_resume_offline_state_advance_authorization(
    *,
    offline_state_advance_preflight_json: str | Path,
    authorization_packet_json: str | Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    preflight_recorded_path = Path(offline_state_advance_preflight_json).absolute()
    preflight_path = preflight_recorded_path.resolve()
    authorization_recorded_path = Path(authorization_packet_json).absolute()
    authorization_path = authorization_recorded_path.resolve()
    preflight = _load(preflight_path)
    authorization = _load(authorization_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)

    preflight_reasons: list[str] = []
    if preflight_recorded_path.name != PREFLIGHT_FILENAME:
        preflight_reasons.append("state_advance_preflight_filename_not_canonical")
    if preflight.get("schema") != PREFLIGHT_SCHEMA:
        preflight_reasons.append("state_advance_preflight_schema_invalid")
    if preflight.get("overall_status") != PREFLIGHT_READY_STATUS:
        preflight_reasons.append("state_advance_preflight_not_ready")
    if preflight.get("offline_state_advance_preflight_ready") is not True:
        preflight_reasons.append("state_advance_preflight_ready_flag_not_true")
    if int(preflight.get("blocker_count") or 0) or preflight.get("blocker_reasons"):
        preflight_reasons.append("state_advance_preflight_contains_blockers")
    if _has_reparse_point(preflight_recorded_path) or _has_reparse_point(
        preflight_recorded_path.parent
    ):
        preflight_reasons.append(
            "state_advance_preflight_or_parent_is_reparse_point"
        )
    try:
        recomputed = build_v1_5_authoritative_resume_offline_state_advance_preflight(
            offline_post_execution_verifier_json=preflight.get(
                "offline_post_execution_verifier_json"
            )
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if not recomputed:
        preflight_reasons.append("state_advance_preflight_recompute_failed")
    else:
        for key in PREFLIGHT_COMPARE_KEYS:
            if preflight.get(key) != recomputed.get(key):
                preflight_reasons.append(f"state_advance_preflight_recompute_mismatch:{key}")

    state_recorded_path = Path(
        str(preflight.get("authoritative_state_json") or "")
    ).absolute()
    expected_state_sha = str(preflight.get("expected_current_state_sha256") or "")
    if not expected_state_sha or _sha(state_recorded_path) != expected_state_sha:
        preflight_reasons.append("authoritative_state_compare_and_swap_sha256_changed")
    if _has_reparse_point(state_recorded_path) or _has_reparse_point(
        state_recorded_path.parent
    ):
        preflight_reasons.append("authoritative_state_target_or_parent_is_reparse_point")

    preview_recorded_path = Path(
        str(preflight.get("candidate_state_preview_json") or "")
    ).absolute()
    if preview_recorded_path.name != CANDIDATE_FILENAME:
        preflight_reasons.append("candidate_state_preview_filename_not_canonical")
    if preview_recorded_path.parent != preflight_recorded_path.parent:
        preflight_reasons.append("candidate_state_preview_not_sibling_of_preflight")
    candidate = dict(preflight.get("candidate_state") or {})
    candidate_sha = str(preflight.get("candidate_state_sha256") or "")
    if not candidate or candidate_sha != _candidate_sha(candidate):
        preflight_reasons.append("candidate_state_sha256_invalid")
    if _sha(preview_recorded_path) != candidate_sha:
        preflight_reasons.append("candidate_state_preview_sha256_mismatch")
    if str(preflight.get("candidate_state_preview_sha256") or "") != candidate_sha:
        preflight_reasons.append("candidate_state_preview_binding_mismatch")
    if _has_reparse_point(preview_recorded_path) or _has_reparse_point(
        preview_recorded_path.parent
    ):
        preflight_reasons.append("candidate_state_preview_or_parent_is_reparse_point")
    checks = [_check("state_advance_preflight_and_candidate_binding", preflight_reasons)]

    authorization_reasons: list[str] = []
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
        "offline_state_advance_preflight_json": str(preflight_recorded_path),
        "offline_state_advance_preflight_sha256": _sha(preflight_recorded_path),
        "authoritative_state_json": str(state_recorded_path),
        "expected_current_state_sha256": expected_state_sha,
        "candidate_state_preview_json": str(preview_recorded_path),
        "candidate_state_sha256": candidate_sha,
        "run_id": str(preflight.get("run_id") or ""),
        "attempt_id": str(preflight.get("attempt_id") or ""),
        "verified_step_id": str(preflight.get("verified_step_id") or ""),
        "next_step_id_after_advance": str(
            preflight.get("next_step_id_after_advance") or ""
        ),
    }
    for field, expected in exact_bindings.items():
        actual = str(authorization.get(field) or "")
        if field.endswith("_json"):
            if not _same_path(actual, Path(expected)):
                authorization_reasons.append(f"authorization_path_mismatch:{field}")
        elif actual != expected:
            authorization_reasons.append(f"authorization_binding_mismatch:{field}")
    if authorization.get("compare_and_swap_required") is not True:
        authorization_reasons.append("authorization_compare_and_swap_not_required")
    confirmations = dict(authorization.get("structured_confirmation") or {})
    for field in (
        "exact_preflight_only",
        "one_verified_offline_step_only",
        "compare_and_swap_before_write",
        "atomic_replace_and_readback_required",
        "rollback_required",
        "no_com",
        "no_pressure_or_route",
        "no_device_or_coefficient_write",
        "no_postgresql_or_release",
    ):
        if confirmations.get(field) is not True:
            authorization_reasons.append(f"authorization_confirmation_missing:{field}")
    expected_capabilities = {
        "allow_authoritative_state_write": True,
        "allow_real_com": False,
        "allow_pressure_control": False,
        "allow_route_control": False,
        "allow_device_or_coefficient_write": False,
        "allow_postgresql_import": False,
    }
    for field, expected in expected_capabilities.items():
        if authorization.get(field) is not expected:
            authorization_reasons.append(f"authorization_capability_mismatch:{field}")
    if not authorization_recorded_path.is_file():
        authorization_reasons.append("authorization_packet_missing")
    if _has_reparse_point(authorization_recorded_path) or _has_reparse_point(
        authorization_recorded_path.parent
    ):
        authorization_reasons.append("authorization_packet_or_parent_is_reparse_point")
    checks.append(
        _check(
            "authorization_identity_expiry_confirmation_and_exact_bindings",
            authorization_reasons,
        )
    )

    reasons = [*preflight_reasons, *authorization_reasons]
    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _iso(evaluated_at),
        "overall_status": READY_STATUS if ready else REVIEW_STATUS,
        "offline_state_advance_authorization_validated": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "offline_state_advance_preflight_json": str(preflight_recorded_path),
        "offline_state_advance_preflight_sha256": _sha(preflight_recorded_path),
        "authorization_packet_json": str(authorization_recorded_path),
        "authorization_packet_sha256": _sha(authorization_recorded_path),
        "authorization_id": authorization_id,
        "authorization_expires_at": str(authorization.get("expires_at") or ""),
        "run_id": exact_bindings["run_id"],
        "attempt_id": exact_bindings["attempt_id"],
        "verified_step_id": exact_bindings["verified_step_id"],
        "next_step_id_after_advance": exact_bindings["next_step_id_after_advance"],
        "authoritative_state_json": str(state_recorded_path),
        "expected_current_state_sha256": expected_state_sha,
        "candidate_state_preview_json": str(preview_recorded_path),
        "candidate_state_sha256": candidate_sha,
        "compare_and_swap_required": True,
        "execution_supported": False,
        "state_write_execution_allowed": False,
        "would_execute": False,
        "writes_authoritative_state": False,
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
            "Keep state writing blocked. A future atomic writer must independently recompute this validation immediately before compare-and-swap."
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


def write_v1_5_authoritative_resume_offline_state_advance_authorization(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out / "v1_5_authoritative_resume_offline_state_advance_authorization.json",
        "checks_csv": out
        / "v1_5_authoritative_resume_offline_state_advance_authorization_checks.csv",
        "markdown": out
        / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_AUTHORIZATION.md",
    }
    paths["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(paths["checks_csv"], model.get("checks") or [])
    paths["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 Offline Resume State Advance Authorization",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- authorization_validated: `{model.get('offline_state_advance_authorization_validated')}`",
                f"- authorization_id: `{model.get('authorization_id')}`",
                f"- state_write_execution_allowed: `{model.get('state_write_execution_allowed')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return paths


__all__ = [
    "AUTHORIZATION_OPERATION",
    "AUTHORIZATION_SCHEMA",
    "CONFIRMATION_TEMPLATE",
    "MAX_AUTHORIZATION_TTL_S",
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_state_advance_authorization",
    "write_v1_5_authoritative_resume_offline_state_advance_authorization",
]
