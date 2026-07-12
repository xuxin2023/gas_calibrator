"""Atomically advance V1.5 authoritative state after one verified offline step."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .v1_5_authoritative_resume_offline_state_advance_authorization import (
    CONFIRMATION_TEMPLATE,
    READY_STATUS as AUTHORIZATION_READY_STATUS,
    SCHEMA as AUTHORIZATION_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_authorization,
)
from .v1_5_authoritative_resume_offline_state_advance_blocked_executor import (
    AUTHORIZATION_COMPARE_KEYS,
)
from .v1_5_authoritative_resume_state_atomic_writer import (
    _acquire_lock,
    _has_reparse_point,
    _sha256_bytes,
    _sha256_file,
    _write_fsynced,
    _write_temp_same_directory,
)

SCHEMA = "v1_5_authoritative_resume_offline_state_advance_atomic_writer_v1"
COMMITTED_STATUS = "authoritative_resume_offline_state_advance_committed"
ROLLED_BACK_STATUS = "authoritative_resume_offline_state_advance_rolled_back"
BLOCKED_STATUS = "blocked"
ROLLBACK_FAILED_STATUS = "rollback_failed"
AUTHORIZATION_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_authorization.json"
)
AUTHORIZATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,95}$")


def _now_datetime() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _has_reparse_in_existing_path(path: Path) -> bool:
    absolute = path.absolute()
    return any(_has_reparse_point(candidate) for candidate in (absolute, *absolute.parents))


def _authorization_matches_recomputed(
    recorded: Mapping[str, Any], recomputed: Mapping[str, Any]
) -> bool:
    return all(recorded.get(key) == recomputed.get(key) for key in AUTHORIZATION_COMPARE_KEYS)


def _validate_authorization(
    *,
    validation_path: Path,
    authorization_id: str,
    evaluated_at: datetime,
) -> tuple[dict[str, Any], Path | None, Path | None, list[str]]:
    validation = _load(validation_path)
    reasons: list[str] = []
    if validation_path.name != AUTHORIZATION_FILENAME:
        reasons.append("state_advance_authorization_filename_not_canonical")
    if _has_reparse_in_existing_path(validation_path):
        reasons.append("state_advance_authorization_path_contains_reparse_point")
    if validation.get("schema") != AUTHORIZATION_SCHEMA:
        reasons.append("state_advance_authorization_schema_invalid")
    if validation.get("overall_status") != AUTHORIZATION_READY_STATUS:
        reasons.append("state_advance_authorization_not_ready")
    if validation.get("offline_state_advance_authorization_validated") is not True:
        reasons.append("state_advance_authorization_ready_flag_not_true")
    if int(validation.get("review_required_count") or 0) or validation.get(
        "review_reasons"
    ):
        reasons.append("state_advance_authorization_contains_findings")
    if str(validation.get("authorization_id") or "") != authorization_id:
        reasons.append("authorization_id_mismatch")
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
        if recomputed.get("schema") != AUTHORIZATION_SCHEMA:
            reasons.append("state_advance_authorization_recomputed_schema_invalid")
        if not _authorization_matches_recomputed(validation, recomputed):
            reasons.append("state_advance_authorization_recompute_mismatch")

    target_value = str(validation.get("authoritative_state_json") or "")
    target = Path(target_value).absolute() if target_value else None
    preview_value = str(validation.get("candidate_state_preview_json") or "")
    preview = Path(preview_value).absolute() if preview_value else None
    if target is None or not target.is_file():
        reasons.append("authoritative_state_missing")
    if preview is None or not preview.is_file():
        reasons.append("candidate_state_preview_missing")
    if target is not None and _has_reparse_in_existing_path(target):
        reasons.append("authoritative_state_path_contains_reparse_point")
    if preview is not None and _has_reparse_in_existing_path(preview):
        reasons.append("candidate_state_preview_path_contains_reparse_point")
    expected_state_sha = str(validation.get("expected_current_state_sha256") or "")
    if target is not None and _sha256_file(target) != expected_state_sha:
        reasons.append("authoritative_state_compare_and_swap_sha256_changed")
    candidate_sha = str(validation.get("candidate_state_sha256") or "")
    if preview is not None and _sha256_file(preview) != candidate_sha:
        reasons.append("candidate_state_preview_sha256_changed")
    if recomputed:
        if target is not None and not _same_path(
            recomputed.get("authoritative_state_json"), target
        ):
            reasons.append("recomputed_authoritative_state_path_mismatch")
        if preview is not None and not _same_path(
            recomputed.get("candidate_state_preview_json"), preview
        ):
            reasons.append("recomputed_candidate_state_preview_path_mismatch")
    return validation, target, preview, reasons


def _base_result(
    *,
    validation_path: Path,
    authorization_id: str,
    confirmation_template: str,
    output_dir: Path,
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "generated_at": _iso(_now_datetime()),
        "overall_status": BLOCKED_STATUS,
        "production_state": "manual_authorized_offline_state_advance_atomic_writer",
        "state_advance_authorization_json": str(validation_path),
        "state_advance_authorization_sha256": _sha256_file(validation_path),
        "authorization_id": authorization_id,
        "confirmation_template": confirmation_template,
        "output_dir": str(output_dir),
        "run_id": "",
        "attempt_id": "",
        "verified_step_id": "",
        "next_step_id_after_advance": "",
        "offline_state_advance_preflight_json": "",
        "authorization_packet_json": "",
        "execution_supported": True,
        "execution_requested": True,
        "authorization_recomputed_ready": False,
        "authorization_recomputed_under_lock": False,
        "state_write_execution_allowed": False,
        "single_writer_lock_acquired": False,
        "current_state_sha256_rechecked": False,
        "candidate_sha256_rechecked": False,
        "write_attempted": False,
        "authoritative_state_write_committed": False,
        "writes_authoritative_state": False,
        "state_file_replaced": False,
        "state_snapshot_created": False,
        "rollback_attempted": False,
        "rollback_confirmed": False,
        "final_state_matches_candidate": False,
        "final_state_matches_original": False,
        "authoritative_state_json": "",
        "expected_current_state_sha256": "",
        "observed_current_state_sha256": "",
        "candidate_state_preview_json": "",
        "candidate_state_sha256": "",
        "post_write_readback_sha256": "",
        "rollback_snapshot_path": "",
        "rollback_snapshot_sha256": "",
        "lock_path": "",
        "invocation_json": "",
        "failure_reasons": [],
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


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    _write_fsynced(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
    )


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["empty"])
        writer.writeheader()
        writer.writerows(dict(row) for row in rows)
        handle.flush()
        os.fsync(handle.fileno())


def write_v1_5_authoritative_resume_offline_state_advance_atomic_writer_outputs(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out
        / "v1_5_authoritative_resume_offline_state_advance_atomic_writer.json",
        "summary_csv": out
        / "v1_5_authoritative_resume_offline_state_advance_atomic_writer_summary.csv",
        "markdown": out
        / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_ATOMIC_WRITER.md",
    }
    _write_json(paths["json"], dict(model))
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "authorization_id": model.get("authorization_id"),
                "run_id": model.get("run_id"),
                "attempt_id": model.get("attempt_id"),
                "verified_step_id": model.get("verified_step_id"),
                "next_step_id_after_advance": model.get(
                    "next_step_id_after_advance"
                ),
                "authorization_recomputed_under_lock": model.get(
                    "authorization_recomputed_under_lock"
                ),
                "write_attempted": model.get("write_attempted"),
                "authoritative_state_write_committed": model.get(
                    "authoritative_state_write_committed"
                ),
                "rollback_attempted": model.get("rollback_attempted"),
                "rollback_confirmed": model.get("rollback_confirmed"),
                "failure_reasons": ";".join(
                    str(value) for value in model.get("failure_reasons") or []
                ),
            }
        ],
    )
    lines = [
        "# V1.5 Offline Resume State Advance Atomic Writer",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- authorization_id: `{model.get('authorization_id')}`",
        f"- authorization_recomputed_under_lock: `{model.get('authorization_recomputed_under_lock')}`",
        f"- authoritative_state_write_committed: `{model.get('authoritative_state_write_committed')}`",
        f"- rollback_confirmed: `{model.get('rollback_confirmed')}`",
        "",
    ]
    if model.get("failure_reasons"):
        lines.extend(["## Failure reasons", ""])
        lines.extend(f"- `{reason}`" for reason in model.get("failure_reasons") or [])
        lines.append("")
    _write_fsynced(paths["markdown"], "\n".join(lines).encode("utf-8"))
    return paths


def execute_v1_5_authoritative_resume_offline_state_advance_atomic_write(
    *,
    state_advance_authorization_json: str | Path,
    authorization_id: str,
    confirmation_template: str,
    output_dir: str | Path,
    after_replace_hook: Callable[[Path], None] | None = None,
) -> dict[str, Any]:
    validation_recorded_path = Path(state_advance_authorization_json).absolute()
    validation_path = validation_recorded_path.resolve()
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    result = _base_result(
        validation_path=validation_recorded_path,
        authorization_id=authorization_id,
        confirmation_template=confirmation_template,
        output_dir=out,
    )
    invocation_path = (
        out / "v1_5_authoritative_resume_offline_state_advance_invocation.json"
    )
    result["invocation_json"] = str(invocation_path)
    _write_json(
        invocation_path,
        {
            "schema": "v1_5_authoritative_resume_offline_state_advance_invocation_v1",
            "generated_at": result["generated_at"],
            "state_advance_authorization_json": str(validation_recorded_path),
            "state_advance_authorization_sha256": result[
                "state_advance_authorization_sha256"
            ],
            "authorization_id": authorization_id,
            "confirmation_template": confirmation_template,
            "execution_requested": True,
            "opens_com_ports": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
        },
    )
    if not AUTHORIZATION_ID_RE.fullmatch(authorization_id):
        result["failure_reasons"].append("authorization_id_format_invalid")
        write_v1_5_authoritative_resume_offline_state_advance_atomic_writer_outputs(
            result, out
        )
        return result
    if confirmation_template != CONFIRMATION_TEMPLATE:
        result["failure_reasons"].append("confirmation_template_mismatch")
        write_v1_5_authoritative_resume_offline_state_advance_atomic_writer_outputs(
            result, out
        )
        return result

    validation, target, preview, reasons = _validate_authorization(
        validation_path=validation_recorded_path,
        authorization_id=authorization_id,
        evaluated_at=_now_datetime(),
    )
    if reasons or target is None or preview is None:
        result["failure_reasons"].extend(reasons or ["state_advance_paths_missing"])
        write_v1_5_authoritative_resume_offline_state_advance_atomic_writer_outputs(
            result, out
        )
        return result
    result["authorization_recomputed_ready"] = True
    result["state_write_execution_allowed"] = True
    for field in (
        "run_id",
        "attempt_id",
        "verified_step_id",
        "next_step_id_after_advance",
        "offline_state_advance_preflight_json",
        "authorization_packet_json",
    ):
        result[field] = str(validation.get(field) or "")
    result["authoritative_state_json"] = str(target)
    result["expected_current_state_sha256"] = str(
        validation.get("expected_current_state_sha256") or ""
    )
    result["candidate_state_preview_json"] = str(preview)
    result["candidate_state_sha256"] = str(
        validation.get("candidate_state_sha256") or ""
    )

    validation_sha_at_start = _sha256_file(validation_path)
    lock_path = target.with_name(f"{target.name}.lock")
    result["lock_path"] = str(lock_path)
    lock_fd: int | None = None
    temp_path: Path | None = None
    rollback_temp_path: Path | None = None
    old_bytes = b""
    old_hash = ""
    replaced = False
    try:
        lock_fd = _acquire_lock(
            lock_path,
            {
                "schema": "v1_5_authoritative_resume_offline_state_advance_lock_v1",
                "authorization_id": authorization_id,
                "run_id": result["run_id"],
                "attempt_id": result["attempt_id"],
                "verified_step_id": result["verified_step_id"],
                "next_step_id_after_advance": result[
                    "next_step_id_after_advance"
                ],
                "created_at": _iso(_now_datetime()),
                "target": str(target),
                "expected_current_state_sha256": result[
                    "expected_current_state_sha256"
                ],
                "candidate_state_sha256": result["candidate_state_sha256"],
            },
        )
        result["single_writer_lock_acquired"] = True

        locked_validation, locked_target, locked_preview, locked_reasons = (
            _validate_authorization(
                validation_path=validation_recorded_path,
                authorization_id=authorization_id,
                evaluated_at=_now_datetime(),
            )
        )
        if locked_reasons:
            raise RuntimeError(
                "authorization_revalidation_under_lock_failed:"
                + ";".join(locked_reasons)
            )
        if _sha256_file(validation_path) != validation_sha_at_start:
            raise RuntimeError("state_advance_authorization_changed_before_write")
        if locked_target is None or not _same_path(locked_target, target):
            raise RuntimeError("authoritative_state_path_changed_before_write")
        if locked_preview is None or not _same_path(locked_preview, preview):
            raise RuntimeError("candidate_preview_path_changed_before_write")
        if locked_validation.get("candidate_state_sha256") != result[
            "candidate_state_sha256"
        ]:
            raise RuntimeError("candidate_sha256_changed_before_write")
        result["authorization_recomputed_under_lock"] = True

        observed_sha = _sha256_file(target)
        result["observed_current_state_sha256"] = observed_sha
        if observed_sha != result["expected_current_state_sha256"]:
            raise RuntimeError("current_state_sha256_changed_after_authorization")
        result["current_state_sha256_rechecked"] = True

        candidate_bytes = preview.read_bytes()
        if _sha256_bytes(candidate_bytes) != result["candidate_state_sha256"]:
            raise RuntimeError("candidate_preview_changed_after_authorization")
        result["candidate_sha256_rechecked"] = True

        old_bytes = target.read_bytes()
        old_hash = _sha256_bytes(old_bytes)
        if old_hash != result["expected_current_state_sha256"]:
            raise RuntimeError("current_state_changed_during_locked_read")
        snapshot_token = hashlib.sha256(authorization_id.encode("utf-8")).hexdigest()[:12]
        snapshot_path = target.with_name(
            f"{target.name}.state-advance.rollback.{snapshot_token}.snapshot"
        )
        _write_fsynced(snapshot_path, old_bytes, exclusive=True)
        result["state_snapshot_created"] = True
        result["rollback_snapshot_path"] = str(snapshot_path)
        result["rollback_snapshot_sha256"] = _sha256_file(snapshot_path)

        temp_path = _write_temp_same_directory(target, candidate_bytes)
        result["write_attempted"] = True
        os.replace(temp_path, target)
        temp_path = None
        replaced = True
        result["state_file_replaced"] = True
        if after_replace_hook is not None:
            after_replace_hook(target)
        readback_sha = _sha256_file(target)
        result["post_write_readback_sha256"] = readback_sha
        if readback_sha != result["candidate_state_sha256"]:
            raise RuntimeError("post_replace_readback_sha256_mismatch")
        result["overall_status"] = COMMITTED_STATUS
        result["authoritative_state_write_committed"] = True
        result["writes_authoritative_state"] = True
        result["final_state_matches_candidate"] = True
    except FileExistsError:
        result["failure_reasons"].append("single_writer_lock_or_snapshot_exists")
    except Exception as exc:
        result["failure_reasons"].append(str(exc) or exc.__class__.__name__)
        if replaced:
            result["rollback_attempted"] = True
            try:
                rollback_temp_path = _write_temp_same_directory(target, old_bytes)
                os.replace(rollback_temp_path, target)
                rollback_temp_path = None
                rollback_ok = _sha256_file(target) == old_hash
                result["rollback_confirmed"] = rollback_ok
                result["final_state_matches_original"] = rollback_ok
                result["overall_status"] = (
                    ROLLED_BACK_STATUS if rollback_ok else ROLLBACK_FAILED_STATUS
                )
                if not rollback_ok:
                    result["failure_reasons"].append("rollback_readback_mismatch")
            except Exception as rollback_exc:  # pragma: no cover - defensive boundary
                result["overall_status"] = ROLLBACK_FAILED_STATUS
                result["failure_reasons"].append(
                    f"rollback_failed:{rollback_exc or rollback_exc.__class__.__name__}"
                )
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        if rollback_temp_path is not None:
            rollback_temp_path.unlink(missing_ok=True)
        if lock_fd is not None:
            os.close(lock_fd)
            lock_path.unlink(missing_ok=True)
    write_v1_5_authoritative_resume_offline_state_advance_atomic_writer_outputs(
        result, out
    )
    return result


__all__ = [
    "BLOCKED_STATUS",
    "COMMITTED_STATUS",
    "CONFIRMATION_TEMPLATE",
    "ROLLED_BACK_STATUS",
    "ROLLBACK_FAILED_STATUS",
    "SCHEMA",
    "execute_v1_5_authoritative_resume_offline_state_advance_atomic_write",
    "write_v1_5_authoritative_resume_offline_state_advance_atomic_writer_outputs",
]
