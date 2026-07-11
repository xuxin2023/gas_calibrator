"""Offline verification of a completed V1.5 authoritative resume-state write."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .v1_5_authoritative_resume_state_atomic_writer import (
    COMMITTED_STATUS,
    CONFIRMATION_TEMPLATE,
    NOOP_STATUS,
    SCHEMA as WRITER_SCHEMA,
    WRITER_AUTHORIZATION_OPERATION,
    WRITER_AUTHORIZATION_SCHEMA,
)
from .v1_5_authoritative_resume_state_controlled_write_preflight import (
    READY_STATUS as PREFLIGHT_READY_STATUS,
    SCHEMA as PREFLIGHT_SCHEMA,
)


SCHEMA = "v1_5_authoritative_resume_state_post_write_verification_v1"
READY_STATUS = "authoritative_resume_state_post_write_verified"
BLOCKED_STATUS = "blocked"
WRITER_FILENAME = "v1_5_resume_state_atomic_write.json"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path or not Path(path).is_file():
        return {}
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _sha256(path: str | Path | None) -> str:
    if not path or not Path(path).is_file():
        return ""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _same_path(value: Any, expected: Path) -> bool:
    if not value:
        return False
    try:
        return Path(str(value)).resolve() == expected.resolve()
    except (OSError, RuntimeError):
        return False


def build_v1_5_authoritative_resume_state_post_write_verification(
    *, atomic_write_json: str | Path
) -> dict[str, Any]:
    writer_path = Path(atomic_write_json).resolve()
    writer = _load_json(writer_path)
    reasons: list[str] = []
    if writer_path.name != WRITER_FILENAME:
        reasons.append("atomic_write_evidence_filename_not_canonical")
    if writer.get("schema") != WRITER_SCHEMA:
        reasons.append(f"atomic_write_schema={writer.get('schema') or 'missing'}")
    writer_status = str(writer.get("overall_status") or "")
    if writer_status not in {COMMITTED_STATUS, NOOP_STATUS}:
        reasons.append(f"atomic_write_status={writer_status or 'missing'}")
    for field in (
        "preflight_recomputed_ready",
        "authorization_validated",
        "current_state_sha256_rechecked",
        "single_writer_lock_acquired",
        "authoritative_state_write_committed",
    ):
        if writer.get(field) is not True:
            reasons.append(f"atomic_write_{field}_not_true")
    if writer.get("failure_reasons") not in ([], ()):
        reasons.append("atomic_write_has_failure_reasons")
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
            reasons.append(f"atomic_write_boundary_{field}_not_false")
    if writer.get("not_real_acceptance_evidence") is not True:
        reasons.append("atomic_write_not_real_acceptance_flag_missing")

    preflight_path = Path(str(writer.get("preflight_json") or "")).resolve()
    authorization_path = Path(str(writer.get("writer_authorization_json") or "")).resolve()
    target_path = Path(str(writer.get("authoritative_state_json") or "")).resolve()
    preview_path = Path(str(writer.get("candidate_state_preview_json") or "")).resolve()
    preflight = _load_json(preflight_path)
    authorization = _load_json(authorization_path)
    candidate_sha = str(writer.get("candidate_state_sha256") or "")

    if _sha256(preflight_path) != str(writer.get("preflight_sha256") or ""):
        reasons.append("preflight_sha256_mismatch")
    if _sha256(authorization_path) != str(writer.get("writer_authorization_sha256") or ""):
        reasons.append("writer_authorization_sha256_mismatch")
    if preflight.get("schema") != PREFLIGHT_SCHEMA:
        reasons.append("preflight_schema_invalid")
    if preflight.get("overall_status") != PREFLIGHT_READY_STATUS:
        reasons.append("preflight_status_not_ready")
    if preflight.get("controlled_write_preflight_ready") is not True:
        reasons.append("preflight_ready_flag_not_true")
    if str(preflight.get("candidate_state_sha256") or "") != candidate_sha:
        reasons.append("candidate_sha_mismatch_with_preflight")
    if not _same_path(preflight.get("candidate_state_preview_json"), preview_path):
        reasons.append("candidate_preview_path_mismatch_with_preflight")
    if not _same_path(preflight.get("authoritative_state_json_read_only"), target_path):
        reasons.append("target_path_mismatch_with_preflight")

    if authorization.get("schema") != WRITER_AUTHORIZATION_SCHEMA:
        reasons.append("writer_authorization_schema_invalid")
    if authorization.get("requested_operation") != WRITER_AUTHORIZATION_OPERATION:
        reasons.append("writer_authorization_operation_invalid")
    if authorization.get("authoritative_state_write_allowed") is not True:
        reasons.append("writer_authorization_write_not_allowed")
    if authorization.get("confirmation_template") != CONFIRMATION_TEMPLATE:
        reasons.append("writer_authorization_confirmation_template_invalid")
    identities = [
        str(authorization.get(field) or "").strip()
        for field in ("operator", "reviewer", "approver")
    ]
    if any(not value for value in identities):
        reasons.append("writer_authorization_identity_missing")
    if len({value.casefold() for value in identities if value}) != 3:
        reasons.append("writer_authorization_identities_not_distinct")
    if str(authorization.get("authorization_id") or "") != str(
        writer.get("authorization_id") or ""
    ):
        reasons.append("writer_authorization_id_mismatch")
    if not _same_path(authorization.get("preflight_json"), preflight_path):
        reasons.append("writer_authorization_preflight_path_mismatch")
    if str(authorization.get("preflight_sha256") or "") != _sha256(preflight_path):
        reasons.append("writer_authorization_preflight_sha256_mismatch")
    if not _same_path(authorization.get("authoritative_state_json"), target_path):
        reasons.append("writer_authorization_target_path_mismatch")
    if str(authorization.get("candidate_state_sha256") or "") != candidate_sha:
        reasons.append("writer_authorization_candidate_sha256_mismatch")
    for field in (
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "connects_postgresql",
        "database_import_allowed",
        "formal_release_allowed",
    ):
        if authorization.get(field) is not False:
            reasons.append(f"writer_authorization_boundary_{field}_not_false")
    if authorization.get("not_real_acceptance_evidence") is not True:
        reasons.append("writer_authorization_not_real_acceptance_flag_missing")

    preview_sha = _sha256(preview_path)
    target_sha = _sha256(target_path)
    if not candidate_sha or preview_sha != candidate_sha:
        reasons.append("candidate_preview_sha256_mismatch")
    if target_sha != candidate_sha:
        reasons.append("authoritative_state_sha256_mismatch")
    if target_path.is_file() and preview_path.is_file() and target_path.read_bytes() != preview_path.read_bytes():
        reasons.append("authoritative_state_bytes_differ_from_candidate")
    if str(writer.get("post_write_readback_sha256") or "") != candidate_sha:
        reasons.append("writer_readback_sha256_mismatch")
    lock_path = Path(str(writer.get("lock_path") or ""))
    if lock_path.is_file():
        reasons.append("writer_lock_still_present")

    replaced = writer.get("state_file_replaced") is True
    created = writer.get("state_file_created") is True
    if writer_status == COMMITTED_STATUS and created == replaced:
        reasons.append("committed_write_create_replace_flags_invalid")
    snapshot_path = Path(str(writer.get("rollback_snapshot_path") or ""))
    if replaced:
        if writer.get("state_snapshot_created") is not True:
            reasons.append("replacement_snapshot_flag_not_true")
        if _sha256(snapshot_path) != str(writer.get("rollback_snapshot_sha256") or ""):
            reasons.append("replacement_snapshot_sha256_mismatch")
    if writer.get("rollback_attempted") is not False or writer.get("rollback_confirmed") is not False:
        reasons.append("successful_writer_has_rollback_state")

    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "post_write_verification_ready": ready,
        "blocker_count": len(reasons),
        "blocker_reasons": reasons,
        "atomic_write_json": str(writer_path),
        "atomic_write_sha256": _sha256(writer_path),
        "atomic_write_status": writer_status,
        "preflight_json": str(preflight_path),
        "preflight_sha256": _sha256(preflight_path),
        "writer_authorization_json": str(authorization_path),
        "writer_authorization_sha256": _sha256(authorization_path),
        "authoritative_state_json": str(target_path),
        "authoritative_state_sha256": target_sha,
        "candidate_state_preview_json": str(preview_path),
        "candidate_state_sha256": candidate_sha,
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


def write_v1_5_authoritative_resume_state_post_write_verification_outputs(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": out / "v1_5_resume_state_post_write_verification.json",
        "summary_csv": out / "v1_5_resume_state_post_write_verification_summary.csv",
        "markdown": out / "V1_5_RESUME_STATE_POST_WRITE_VERIFICATION.md",
    }
    paths["json"].write_text(json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with paths["summary_csv"].open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("overall_status", "post_write_verification_ready", "blocker_count", "blocker_reasons", "authoritative_state_sha256", "candidate_state_sha256"))
        writer.writeheader()
        writer.writerow({
            "overall_status": model.get("overall_status"),
            "post_write_verification_ready": model.get("post_write_verification_ready"),
            "blocker_count": model.get("blocker_count"),
            "blocker_reasons": ";".join(model.get("blocker_reasons") or []),
            "authoritative_state_sha256": model.get("authoritative_state_sha256"),
            "candidate_state_sha256": model.get("candidate_state_sha256"),
        })
    paths["markdown"].write_text(
        "\n".join((
            "# V1.5 Resume State Post-Write Verification",
            "",
            f"- overall_status: `{model.get('overall_status')}`",
            f"- ready: `{model.get('post_write_verification_ready')}`",
            f"- blocker_count: `{model.get('blocker_count')}`",
            f"- authoritative_state_sha256: `{model.get('authoritative_state_sha256')}`",
            f"- candidate_state_sha256: `{model.get('candidate_state_sha256')}`",
            "",
        )),
        encoding="utf-8",
    )
    return paths


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_state_post_write_verification",
    "write_v1_5_authoritative_resume_state_post_write_verification_outputs",
]
