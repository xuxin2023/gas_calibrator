"""Verify a committed one-step V1.5 offline resume-state advance."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .v1_5_authoritative_resume_offline_state_advance_atomic_writer import (
    COMMITTED_STATUS,
    SCHEMA as WRITER_SCHEMA,
)
from .v1_5_authoritative_resume_offline_state_advance_authorization import (
    AUTHORIZATION_ID_RE,
    AUTHORIZATION_OPERATION,
    AUTHORIZATION_SCHEMA,
    CONFIRMATION_TEMPLATE,
    MAX_AUTHORIZATION_TTL_S,
    READY_STATUS as AUTHORIZATION_READY_STATUS,
    SCHEMA as AUTHORIZATION_VALIDATION_SCHEMA,
)
from .v1_5_authoritative_resume_offline_state_advance_preflight import (
    READY_STATUS as PREFLIGHT_READY_STATUS,
    SCHEMA as PREFLIGHT_SCHEMA,
)
from .v1_5_authoritative_resume_state_atomic_writer import (
    _has_reparse_point,
    _sha256_file,
)

SCHEMA = "v1_5_authoritative_resume_offline_state_advance_post_write_verification_v1"
READY_STATUS = "authoritative_resume_offline_state_advance_post_write_verified"
BLOCKED_STATUS = "blocked"
WRITER_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_atomic_writer.json"
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _same_path(value: Any, expected: Path) -> bool:
    try:
        return Path(str(value or "")).absolute() == expected.absolute()
    except (OSError, RuntimeError):
        return False


def _contains_reparse(path: Path) -> bool:
    absolute = path.absolute()
    return any(_has_reparse_point(candidate) for candidate in (absolute, *absolute.parents))


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else None


def build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
    *, atomic_write_json: str | Path
) -> dict[str, Any]:
    writer_recorded_path = Path(atomic_write_json).absolute()
    writer_path = writer_recorded_path.resolve()
    writer = _load(writer_path)
    reasons: list[str] = []
    if writer_recorded_path.name != WRITER_FILENAME:
        reasons.append("state_advance_writer_filename_not_canonical")
    if _contains_reparse(writer_recorded_path):
        reasons.append("state_advance_writer_path_contains_reparse_point")
    if writer.get("schema") != WRITER_SCHEMA:
        reasons.append("state_advance_writer_schema_invalid")
    if writer.get("overall_status") != COMMITTED_STATUS:
        reasons.append("state_advance_writer_not_committed")
    for field in (
        "authorization_recomputed_ready",
        "authorization_recomputed_under_lock",
        "authorization_revalidated_immediately_before_replace",
        "state_write_execution_allowed",
        "single_writer_lock_acquired",
        "current_state_sha256_rechecked",
        "candidate_sha256_rechecked",
        "write_attempted",
        "authoritative_state_write_committed",
        "writes_authoritative_state",
        "state_file_replaced",
        "state_snapshot_created",
        "final_state_matches_candidate",
    ):
        if writer.get(field) is not True:
            reasons.append(f"state_advance_writer_{field}_not_true")
    if writer.get("failure_reasons") not in ([], ()):
        reasons.append("state_advance_writer_has_failure_reasons")
    if writer.get("rollback_attempted") is not False or writer.get(
        "rollback_confirmed"
    ) is not False:
        reasons.append("successful_state_advance_writer_has_rollback_state")
    if writer.get("final_state_matches_original") is not False:
        reasons.append("successful_state_advance_writer_matches_original")
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
        if writer.get(field) is not False:
            reasons.append(f"state_advance_writer_boundary_invalid:{field}")
    if writer.get("not_real_acceptance_evidence") is not True:
        reasons.append("state_advance_writer_not_real_acceptance_flag_missing")

    validation_path = Path(
        str(writer.get("state_advance_authorization_json") or "")
    ).absolute()
    validation = _load(validation_path)
    if _contains_reparse(validation_path):
        reasons.append("state_advance_authorization_path_contains_reparse_point")
    if _sha256_file(validation_path) != str(
        writer.get("state_advance_authorization_sha256") or ""
    ):
        reasons.append("state_advance_authorization_sha256_mismatch")
    if validation.get("schema") != AUTHORIZATION_VALIDATION_SCHEMA:
        reasons.append("state_advance_authorization_schema_invalid")
    if validation.get("overall_status") != AUTHORIZATION_READY_STATUS:
        reasons.append("state_advance_authorization_not_ready")
    if validation.get("offline_state_advance_authorization_validated") is not True:
        reasons.append("state_advance_authorization_ready_flag_not_true")
    if int(validation.get("review_required_count") or 0) or validation.get(
        "review_reasons"
    ):
        reasons.append("state_advance_authorization_contains_findings")
    if validation.get("compare_and_swap_required") is not True:
        reasons.append("state_advance_authorization_compare_and_swap_not_required")
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
        reasons.append("state_advance_authorization_not_real_acceptance_flag_missing")

    preflight_path = Path(
        str(validation.get("offline_state_advance_preflight_json") or "")
    ).absolute()
    packet_path = Path(
        str(validation.get("authorization_packet_json") or "")
    ).absolute()
    preflight = _load(preflight_path)
    packet = _load(packet_path)
    if _contains_reparse(preflight_path) or _contains_reparse(packet_path):
        reasons.append("authorization_source_path_contains_reparse_point")
    if _sha256_file(preflight_path) != str(
        validation.get("offline_state_advance_preflight_sha256") or ""
    ):
        reasons.append("state_advance_preflight_sha256_mismatch")
    if _sha256_file(packet_path) != str(
        validation.get("authorization_packet_sha256") or ""
    ):
        reasons.append("state_advance_authorization_packet_sha256_mismatch")
    if preflight.get("schema") != PREFLIGHT_SCHEMA:
        reasons.append("state_advance_preflight_schema_invalid")
    if preflight.get("overall_status") != PREFLIGHT_READY_STATUS:
        reasons.append("state_advance_preflight_not_ready")
    if preflight.get("offline_state_advance_preflight_ready") is not True:
        reasons.append("state_advance_preflight_ready_flag_not_true")
    if int(preflight.get("blocker_count") or 0) or preflight.get("blocker_reasons"):
        reasons.append("state_advance_preflight_contains_blockers")
    if preflight.get("compare_and_swap_required") is not True:
        reasons.append("state_advance_preflight_compare_and_swap_not_required")
    for field in (
        "execution_supported",
        "would_execute",
        "authoritative_state_write_allowed",
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
        if preflight.get(field) is not False:
            reasons.append(f"state_advance_preflight_boundary_invalid:{field}")
    if preflight.get("not_real_acceptance_evidence") is not True:
        reasons.append("state_advance_preflight_not_real_acceptance_flag_missing")
    if packet.get("schema") != AUTHORIZATION_SCHEMA:
        reasons.append("authorization_packet_schema_invalid")
    if packet.get("requested_operation") != AUTHORIZATION_OPERATION:
        reasons.append("authorization_packet_operation_invalid")
    if packet.get("confirmation_template") != CONFIRMATION_TEMPLATE:
        reasons.append("authorization_packet_confirmation_invalid")
    if not AUTHORIZATION_ID_RE.fullmatch(str(packet.get("authorization_id") or "")):
        reasons.append("authorization_packet_id_invalid")
    identities = [
        str(packet.get(field) or "").strip()
        for field in ("operator", "reviewer", "approver")
    ]
    if any(not value for value in identities) or len(set(identities)) != 3:
        reasons.append("authorization_packet_identities_invalid")
    if packet.get("allow_authoritative_state_write") is not True:
        reasons.append("authorization_packet_state_write_not_allowed")
    if packet.get("compare_and_swap_required") is not True:
        reasons.append("authorization_packet_compare_and_swap_not_required")
    confirmations = dict(packet.get("structured_confirmation") or {})
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
            reasons.append(f"authorization_packet_confirmation_missing:{field}")
    for field in (
        "allow_real_com",
        "allow_pressure_control",
        "allow_route_control",
        "allow_device_or_coefficient_write",
        "allow_postgresql_import",
    ):
        if packet.get(field) is not False:
            reasons.append(f"authorization_packet_boundary_invalid:{field}")

    static_bindings = (
        "authorization_id",
        "run_id",
        "attempt_id",
        "verified_step_id",
        "next_step_id_after_advance",
        "candidate_state_sha256",
    )
    for field in static_bindings:
        if str(packet.get(field) or "") != str(validation.get(field) or ""):
            reasons.append(f"authorization_packet_binding_mismatch:{field}")
    if not _same_path(
        packet.get("offline_state_advance_preflight_json"), preflight_path
    ):
        reasons.append("authorization_packet_preflight_path_mismatch")
    if str(packet.get("offline_state_advance_preflight_sha256") or "") != _sha256_file(
        preflight_path
    ):
        reasons.append("authorization_packet_preflight_sha256_mismatch")

    writer_time = _parse_time(writer.get("generated_at"))
    revalidated_at = _parse_time(writer.get("authorization_revalidated_at"))
    committed_at = _parse_time(writer.get("committed_at"))
    issued = _parse_time(packet.get("issued_at"))
    expires = _parse_time(packet.get("expires_at"))
    if any(
        value is None
        for value in (writer_time, revalidated_at, committed_at, issued, expires)
    ):
        reasons.append("authorization_or_writer_timestamp_invalid")
    else:
        assert writer_time is not None
        assert revalidated_at is not None
        assert committed_at is not None
        assert issued is not None
        assert expires is not None
        ttl = (expires - issued).total_seconds()
        if ttl <= 0 or ttl > MAX_AUTHORIZATION_TTL_S:
            reasons.append("authorization_packet_ttl_out_of_range")
        if not (issued <= writer_time <= revalidated_at <= committed_at < expires):
            reasons.append("writer_timeline_outside_authorization_window")

    target_path = Path(str(writer.get("authoritative_state_json") or "")).absolute()
    preview_path = Path(
        str(writer.get("candidate_state_preview_json") or "")
    ).absolute()
    snapshot_path = Path(str(writer.get("rollback_snapshot_path") or "")).absolute()
    invocation_path = Path(str(writer.get("invocation_json") or "")).absolute()
    for label, path in (
        ("authoritative_state", target_path),
        ("candidate_preview", preview_path),
        ("rollback_snapshot", snapshot_path),
        ("writer_invocation", invocation_path),
    ):
        if _contains_reparse(path):
            reasons.append(f"{label}_path_contains_reparse_point")
    candidate_sha = str(writer.get("candidate_state_sha256") or "")
    expected_old_sha = str(writer.get("expected_current_state_sha256") or "")
    if not candidate_sha or _sha256_file(preview_path) != candidate_sha:
        reasons.append("candidate_preview_sha256_mismatch")
    if _sha256_file(target_path) != candidate_sha:
        reasons.append("authoritative_state_sha256_mismatch")
    if target_path.is_file() and preview_path.is_file() and (
        target_path.read_bytes() != preview_path.read_bytes()
    ):
        reasons.append("authoritative_state_bytes_differ_from_candidate")
    if str(writer.get("post_write_readback_sha256") or "") != candidate_sha:
        reasons.append("writer_readback_sha256_mismatch")
    if _sha256_file(snapshot_path) != expected_old_sha:
        reasons.append("rollback_snapshot_does_not_match_expected_old_state")
    if str(writer.get("rollback_snapshot_sha256") or "") != expected_old_sha:
        reasons.append("writer_rollback_snapshot_sha256_mismatch")
    expected_snapshot = target_path.with_name(
        f"{target_path.name}.state-advance.rollback."
        f"{hashlib.sha256(str(writer.get('authorization_id') or '').encode('utf-8')).hexdigest()[:12]}.snapshot"
    )
    if not _same_path(snapshot_path, expected_snapshot):
        reasons.append("rollback_snapshot_path_not_canonical")
    expected_lock = target_path.with_name(f"{target_path.name}.lock")
    lock_path = Path(str(writer.get("lock_path") or "")).absolute()
    if not _same_path(lock_path, expected_lock):
        reasons.append("writer_lock_path_not_canonical")
    if lock_path.exists():
        reasons.append("writer_lock_still_present")

    invocation = _load(invocation_path)
    if invocation.get("schema") != (
        "v1_5_authoritative_resume_offline_state_advance_invocation_v1"
    ):
        reasons.append("writer_invocation_schema_invalid")
    if invocation.get("execution_requested") is not True:
        reasons.append("writer_invocation_execution_not_requested")
    if str(invocation.get("authorization_id") or "") != str(
        writer.get("authorization_id") or ""
    ):
        reasons.append("writer_invocation_authorization_id_mismatch")
    if invocation.get("confirmation_template") != CONFIRMATION_TEMPLATE:
        reasons.append("writer_invocation_confirmation_template_invalid")
    for field in ("opens_com_ports", "writes_coefficients", "connects_postgresql"):
        if invocation.get(field) is not False:
            reasons.append(f"writer_invocation_boundary_invalid:{field}")
    if not _same_path(
        invocation.get("state_advance_authorization_json"), validation_path
    ):
        reasons.append("writer_invocation_authorization_path_mismatch")
    if str(invocation.get("state_advance_authorization_sha256") or "") != _sha256_file(
        validation_path
    ):
        reasons.append("writer_invocation_authorization_sha256_mismatch")

    binding_fields = (
        "authorization_id",
        "run_id",
        "attempt_id",
        "verified_step_id",
        "next_step_id_after_advance",
        "candidate_state_sha256",
    )
    for field in binding_fields:
        if str(writer.get(field) or "") != str(validation.get(field) or ""):
            reasons.append(f"writer_authorization_binding_mismatch:{field}")
    for field in (
        "run_id",
        "attempt_id",
        "verified_step_id",
        "next_step_id_after_advance",
        "candidate_state_sha256",
    ):
        if str(preflight.get(field) or "") != str(validation.get(field) or ""):
            reasons.append(f"preflight_authorization_binding_mismatch:{field}")
    if not _same_path(validation.get("authoritative_state_json"), target_path):
        reasons.append("authorization_target_path_mismatch")
    if not _same_path(validation.get("candidate_state_preview_json"), preview_path):
        reasons.append("authorization_candidate_path_mismatch")
    if str(validation.get("expected_current_state_sha256") or "") != expected_old_sha:
        reasons.append("authorization_expected_old_state_sha256_mismatch")
    if str(preflight.get("expected_current_state_sha256") or "") != expected_old_sha:
        reasons.append("preflight_expected_old_state_sha256_mismatch")
    if not _same_path(packet.get("authoritative_state_json"), target_path):
        reasons.append("authorization_packet_target_path_mismatch")
    if not _same_path(packet.get("candidate_state_preview_json"), preview_path):
        reasons.append("authorization_packet_candidate_path_mismatch")
    if str(packet.get("expected_current_state_sha256") or "") != expected_old_sha:
        reasons.append("authorization_packet_expected_old_state_sha256_mismatch")
    if not _same_path(preflight.get("authoritative_state_json"), target_path):
        reasons.append("preflight_target_path_mismatch")
    if not _same_path(preflight.get("candidate_state_preview_json"), preview_path):
        reasons.append("preflight_candidate_path_mismatch")
    if str(preflight.get("candidate_state_sha256") or "") != candidate_sha:
        reasons.append("preflight_candidate_sha256_mismatch")
    if not _same_path(writer.get("offline_state_advance_preflight_json"), preflight_path):
        reasons.append("writer_preflight_path_mismatch")
    if not _same_path(writer.get("authorization_packet_json"), packet_path):
        reasons.append("writer_authorization_packet_path_mismatch")
    if writer.get("confirmation_template") != CONFIRMATION_TEMPLATE:
        reasons.append("writer_confirmation_template_invalid")
    plan_path = Path(str(preflight.get("full_flow_plan_json") or "")).absolute()
    if _contains_reparse(plan_path):
        reasons.append("full_flow_plan_path_contains_reparse_point")
    if _sha256_file(plan_path) != str(preflight.get("full_flow_plan_sha256") or ""):
        reasons.append("full_flow_plan_sha256_mismatch")

    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "post_write_verification_ready": ready,
        "blocker_count": len(reasons),
        "blocker_reasons": reasons,
        "atomic_write_json": str(writer_recorded_path),
        "atomic_write_sha256": _sha256_file(writer_recorded_path),
        "state_advance_authorization_json": str(validation_path),
        "state_advance_authorization_sha256": _sha256_file(validation_path),
        "offline_state_advance_preflight_json": str(preflight_path),
        "offline_state_advance_preflight_sha256": _sha256_file(preflight_path),
        "authorization_packet_json": str(packet_path),
        "authorization_packet_sha256": _sha256_file(packet_path),
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha256_file(plan_path),
        "authorization_id": str(writer.get("authorization_id") or ""),
        "run_id": str(writer.get("run_id") or ""),
        "attempt_id": str(writer.get("attempt_id") or ""),
        "verified_step_id": str(writer.get("verified_step_id") or ""),
        "next_step_id_after_advance": str(
            writer.get("next_step_id_after_advance") or ""
        ),
        "authoritative_state_json": str(target_path),
        "authoritative_state_sha256": _sha256_file(target_path),
        "candidate_state_preview_json": str(preview_path),
        "candidate_state_sha256": candidate_sha,
        "rollback_snapshot_path": str(snapshot_path),
        "rollback_snapshot_sha256": _sha256_file(snapshot_path),
        "writer_lock_path": str(lock_path),
        "writer_lock_released": not lock_path.exists(),
        "state_consumption_allowed": False,
        "execution_supported": False,
        "resume_execution_allowed": False,
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
    }


def write_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out
        / "v1_5_authoritative_resume_offline_state_advance_post_write_verification.json",
        "summary_csv": out
        / "v1_5_authoritative_resume_offline_state_advance_post_write_verification_summary.csv",
        "markdown": out
        / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_POST_WRITE_VERIFICATION.md",
    }
    paths["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    with paths["summary_csv"].open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "overall_status",
                "post_write_verification_ready",
                "blocker_count",
                "blocker_reasons",
                "run_id",
                "verified_step_id",
                "next_step_id_after_advance",
                "authoritative_state_sha256",
                "candidate_state_sha256",
            ),
        )
        writer.writeheader()
        writer.writerow(
            {
                "overall_status": model.get("overall_status"),
                "post_write_verification_ready": model.get(
                    "post_write_verification_ready"
                ),
                "blocker_count": model.get("blocker_count"),
                "blocker_reasons": ";".join(model.get("blocker_reasons") or []),
                "run_id": model.get("run_id"),
                "verified_step_id": model.get("verified_step_id"),
                "next_step_id_after_advance": model.get(
                    "next_step_id_after_advance"
                ),
                "authoritative_state_sha256": model.get(
                    "authoritative_state_sha256"
                ),
                "candidate_state_sha256": model.get("candidate_state_sha256"),
            }
        )
    paths["markdown"].write_text(
        "\n".join(
            (
                "# V1.5 Offline State Advance Post-Write Verification",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- ready: `{model.get('post_write_verification_ready')}`",
                f"- blocker_count: `{model.get('blocker_count')}`",
                f"- verified_step_id: `{model.get('verified_step_id')}`",
                f"- next_step_id_after_advance: `{model.get('next_step_id_after_advance')}`",
                "",
            )
        ),
        encoding="utf-8",
    )
    return paths


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_state_advance_post_write_verification",
    "write_v1_5_authoritative_resume_offline_state_advance_post_write_verification",
]
