"""Explicitly authorized atomic writer for V1.5 authoritative resume state."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import stat
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .v1_5_authoritative_resume_state_controlled_write_preflight import (
    READY_STATUS as PREFLIGHT_READY_STATUS,
    SCHEMA as PREFLIGHT_SCHEMA,
    build_v1_5_authoritative_resume_state_controlled_write_preflight,
)


SCHEMA = "v1_5_authoritative_resume_state_atomic_writer_v1"
COMMITTED_STATUS = "authoritative_resume_state_write_committed"
NOOP_STATUS = "authoritative_resume_state_already_current"
ROLLED_BACK_STATUS = "authoritative_resume_state_write_rolled_back"
BLOCKED_STATUS = "blocked"
ROLLBACK_FAILED_STATUS = "rollback_failed"
CONFIRMATION_TEMPLATE = "v1_5_authoritative_resume_state_atomic_write_v1"
WRITER_AUTHORIZATION_SCHEMA = (
    "v1_5_authoritative_resume_state_atomic_write_authorization_v1"
)
WRITER_AUTHORIZATION_OPERATION = "execute_authoritative_resume_state_atomic_write"
PREFLIGHT_FILENAME = "v1_5_resume_state_write_preflight.json"
CANDIDATE_FILENAME = "v1_5_resume_state_candidate_preview.json"
WRITER_AUTHORIZATION_FILENAME = "v1_5_resume_state_atomic_write_authorization.json"
AUTHORIZATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,95}$")


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path or not Path(path).is_file():
        return {}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: str | Path | None) -> str:
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


def _valid_iso_timestamp(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _has_reparse_point(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    return path.is_symlink() or bool(
        attributes & int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0) or 0)
    )


def _preflight_matches_recomputed(
    payload: Mapping[str, Any],
    recomputed: Mapping[str, Any],
) -> bool:
    keys = (
        "schema",
        "overall_status",
        "blocker_count",
        "review_required_count",
        "controlled_write_preflight_ready",
        "production_state",
        "execution_supported",
        "authoritative_state_write_allowed",
        "full_flow_plan_json",
        "full_flow_plan_sha256",
        "resume_prefix_application_review_json",
        "resume_prefix_application_review_sha256",
        "authoritative_resume_state_writer_design_json",
        "authoritative_resume_state_writer_design_sha256",
        "authoritative_resume_state_writer_blocked_executor_json",
        "authoritative_resume_state_writer_blocked_executor_sha256",
        "authorization_packet_json",
        "authorization_packet_sha256",
        "authorization_id",
        "run_id",
        "authoritative_state_json_read_only",
        "state_target_exists",
        "observed_existing_state_sha256",
        "expected_existing_state_sha256",
        "candidate_state",
        "candidate_state_sha256",
        "writes_authoritative_state",
        "state_file_created",
        "state_file_replaced",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "connects_postgresql",
        "writes_coefficients",
        "formal_release_allowed",
        "database_import_allowed",
        "not_real_acceptance_evidence",
    )
    return all(payload.get(key) == recomputed.get(key) for key in keys) and (
        payload.get("checks") == json.loads(json.dumps(recomputed.get("checks") or []))
    )


def _validate_preflight(
    preflight_path: Path,
) -> tuple[dict[str, Any], Path | None, Path | None, list[str]]:
    payload = _load_json(preflight_path)
    reasons: list[str] = []
    if preflight_path.name != PREFLIGHT_FILENAME:
        reasons.append("preflight_filename_not_canonical")
    if payload.get("schema") != PREFLIGHT_SCHEMA:
        reasons.append(f"preflight_schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != PREFLIGHT_READY_STATUS:
        reasons.append(f"preflight_status={payload.get('overall_status') or 'missing'}")
    if payload.get("controlled_write_preflight_ready") is not True:
        reasons.append("controlled_write_preflight_not_ready")
    if int(payload.get("blocker_count") or 0) or int(
        payload.get("review_required_count") or 0
    ):
        reasons.append("controlled_write_preflight_has_open_findings")
    for field in (
        "execution_supported",
        "authoritative_state_write_allowed",
        "writes_authoritative_state",
        "state_file_created",
        "state_file_replaced",
        "state_snapshot_created",
        "rollback_executed",
        "would_execute",
        "live_resume_execution_allowed",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "connects_postgresql",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "database_written",
        "formal_release_allowed",
        "database_import_allowed",
    ):
        if payload.get(field) is not False:
            reasons.append(f"preflight_boundary_{field}_not_false")
    source_fields = (
        ("full_flow_plan_json", "full_flow_plan_sha256"),
        (
            "resume_prefix_application_review_json",
            "resume_prefix_application_review_sha256",
        ),
        (
            "authoritative_resume_state_writer_design_json",
            "authoritative_resume_state_writer_design_sha256",
        ),
        (
            "authoritative_resume_state_writer_blocked_executor_json",
            "authoritative_resume_state_writer_blocked_executor_sha256",
        ),
        ("authorization_packet_json", "authorization_packet_sha256"),
    )
    for path_field, hash_field in source_fields:
        if _sha256_file(payload.get(path_field)) != str(payload.get(hash_field) or ""):
            reasons.append(f"preflight_source_hash_mismatch:{path_field}")
    plan_path_value = str(payload.get("full_flow_plan_json") or "")
    plan_path = Path(plan_path_value).resolve() if plan_path_value else None
    if plan_path is None:
        reasons.append("preflight_plan_path_missing")
    else:
        expected_preflight_path = (
            plan_path.parent
            / "authoritative_resume_state_controlled_write_preflight"
            / PREFLIGHT_FILENAME
        )
        if not _same_path(preflight_path, expected_preflight_path):
            reasons.append("preflight_path_not_declared_canonical_output")
    target_value = str(payload.get("authoritative_state_json_read_only") or "")
    target_path = Path(target_value).absolute() if target_value else None
    if plan_path is not None:
        expected_target = plan_path.parent / "v1_5_full_flow_state.json"
        if target_path is None or not _same_path(target_path, expected_target):
            reasons.append("authoritative_state_target_not_canonical")
    if target_path is not None and (
        _has_reparse_point(target_path) or _has_reparse_point(target_path.parent)
    ):
        reasons.append("authoritative_state_target_or_parent_is_reparse_point")
    if target_path is not None:
        current_hash = _sha256_file(target_path) if target_path.is_file() else "absent"
        if current_hash != str(payload.get("expected_existing_state_sha256") or ""):
            reasons.append("current_state_sha256_changed_after_preflight")
    preview_value = str(payload.get("candidate_state_preview_json") or "")
    preview_path = Path(preview_value).resolve() if preview_value else None
    expected_preview = preflight_path.parent / CANDIDATE_FILENAME
    if preview_path is None or not _same_path(preview_path, expected_preview):
        reasons.append("candidate_preview_path_not_canonical")
    candidate_hash = str(payload.get("candidate_state_sha256") or "")
    if (
        preview_path is None
        or _sha256_file(preview_path) != candidate_hash
        or str(payload.get("candidate_state_preview_sha256") or "") != candidate_hash
    ):
        reasons.append("candidate_preview_sha256_mismatch")
    recomputed: dict[str, Any] = {}
    if plan_path is not None:
        try:
            recomputed = build_v1_5_authoritative_resume_state_controlled_write_preflight(
                full_flow_plan_json=plan_path,
                resume_prefix_application_review_json=payload.get(
                    "resume_prefix_application_review_json"
                ),
                authoritative_resume_state_writer_design_json=payload.get(
                    "authoritative_resume_state_writer_design_json"
                ),
                authoritative_resume_state_writer_blocked_executor_json=payload.get(
                    "authoritative_resume_state_writer_blocked_executor_json"
                ),
                authorization_packet_json=payload.get("authorization_packet_json"),
            )
        except (OSError, ValueError, json.JSONDecodeError):
            recomputed = {}
    if not recomputed or not _preflight_matches_recomputed(payload, recomputed):
        reasons.append("preflight_independent_recompute_mismatch")
    return payload, target_path, preview_path, reasons


def _validate_writer_authorization(
    *,
    authorization_path: Path,
    preflight_path: Path,
    preflight: Mapping[str, Any],
    target: Path,
    authorization_id: str,
    confirmation_template: str,
) -> tuple[dict[str, Any], list[str]]:
    payload = _load_json(authorization_path)
    reasons: list[str] = []
    expected_path = (
        preflight_path.parent.parent
        / "authoritative_resume_state_atomic_write_authorization"
        / WRITER_AUTHORIZATION_FILENAME
    )
    if not _same_path(authorization_path, expected_path):
        reasons.append("writer_authorization_path_not_canonical")
    if payload.get("schema") != WRITER_AUTHORIZATION_SCHEMA:
        reasons.append(f"writer_authorization_schema={payload.get('schema') or 'missing'}")
    if payload.get("requested_operation") != WRITER_AUTHORIZATION_OPERATION:
        reasons.append("writer_authorization_requested_operation_mismatch")
    if str(payload.get("authorization_id") or "") != authorization_id:
        reasons.append("authorization_id_mismatch_with_writer_authorization")
    if payload.get("confirmation_template") != confirmation_template:
        reasons.append("confirmation_template_mismatch_with_writer_authorization")
    if not _valid_iso_timestamp(payload.get("authorized_at")):
        reasons.append("writer_authorization_timestamp_missing_or_invalid")
    identities = [
        str(payload.get(key) or "").strip()
        for key in ("operator", "reviewer", "approver")
    ]
    if any(not value for value in identities):
        reasons.append("writer_authorization_identity_missing")
    if len([value.casefold() for value in identities if value]) != len(
        set(value.casefold() for value in identities if value)
    ):
        reasons.append("writer_operator_reviewer_approver_must_be_distinct")
    if payload.get("authoritative_state_write_allowed") is not True:
        reasons.append("writer_authorization_state_write_allowed_not_true")
    if not _same_path(payload.get("preflight_json"), preflight_path):
        reasons.append("writer_authorization_preflight_path_mismatch")
    if str(payload.get("preflight_sha256") or "") != _sha256_file(preflight_path):
        reasons.append("writer_authorization_preflight_sha256_mismatch")
    if not _same_path(payload.get("authoritative_state_json"), target):
        reasons.append("writer_authorization_target_mismatch")
    if str(payload.get("expected_existing_state_sha256") or "") != str(
        preflight.get("expected_existing_state_sha256") or ""
    ):
        reasons.append("writer_authorization_existing_state_sha256_mismatch")
    if str(payload.get("candidate_state_sha256") or "") != str(
        preflight.get("candidate_state_sha256") or ""
    ):
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
        if payload.get(field) is not False:
            reasons.append(f"writer_authorization_boundary_{field}_not_false")
    if payload.get("not_real_acceptance_evidence") is not True:
        reasons.append("writer_authorization_not_real_acceptance_flag_missing")
    return payload, reasons


def _write_fsynced(path: Path, data: bytes, *, exclusive: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "xb" if exclusive else "wb"
    with path.open(mode) as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _write_temp_same_directory(target: Path, data: bytes) -> Path:
    fd, name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    temp_path = Path(name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    return temp_path


def _current_state_sha256(target: Path) -> str:
    return _sha256_file(target) if target.is_file() else "absent"


def _acquire_lock(lock_path: Path, payload: Mapping[str, Any]) -> int:
    fd: int | None = None
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        data = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        os.write(fd, data)
        os.fsync(fd)
        return fd
    except Exception:
        if fd is not None:
            os.close(fd)
            lock_path.unlink(missing_ok=True)
        raise


def _base_result(
    *,
    preflight_path: Path,
    writer_authorization_path: Path,
    authorization_id: str,
    confirmation_template: str,
    output_dir: Path,
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": BLOCKED_STATUS,
        "production_state": "manual_authorized_atomic_state_writer",
        "preflight_json": str(preflight_path),
        "preflight_sha256": _sha256_file(preflight_path),
        "writer_authorization_json": str(writer_authorization_path),
        "writer_authorization_sha256": _sha256_file(writer_authorization_path),
        "authorization_id": authorization_id,
        "confirmation_template": confirmation_template,
        "output_dir": str(output_dir),
        "execution_supported": True,
        "execution_requested": True,
        "preflight_recomputed_ready": False,
        "authorization_validated": False,
        "current_state_sha256_rechecked": False,
        "single_writer_lock_acquired": False,
        "write_attempted": False,
        "authoritative_state_write_committed": False,
        "already_current_noop": False,
        "state_file_created": False,
        "state_file_replaced": False,
        "state_snapshot_created": False,
        "rollback_attempted": False,
        "rollback_confirmed": False,
        "authoritative_state_json": "",
        "candidate_state_preview_json": "",
        "candidate_state_sha256": "",
        "expected_existing_state_sha256": "",
        "observed_existing_state_sha256": "",
        "post_write_readback_sha256": "",
        "rollback_snapshot_path": "",
        "rollback_snapshot_sha256": "",
        "lock_path": "",
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


def write_v1_5_authoritative_resume_state_atomic_writer_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_resume_state_atomic_write.json",
        "summary_csv": out / "v1_5_resume_state_atomic_write_summary.csv",
        "markdown": out / "V1_5_RESUME_STATE_ATOMIC_WRITE.md",
    }
    _write_json(paths["json"], model)
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "authorization_id": model.get("authorization_id"),
                "preflight_recomputed_ready": model.get("preflight_recomputed_ready"),
                "current_state_sha256_rechecked": model.get(
                    "current_state_sha256_rechecked"
                ),
                "single_writer_lock_acquired": model.get("single_writer_lock_acquired"),
                "write_attempted": model.get("write_attempted"),
                "authoritative_state_write_committed": model.get(
                    "authoritative_state_write_committed"
                ),
                "state_file_created": model.get("state_file_created"),
                "state_file_replaced": model.get("state_file_replaced"),
                "rollback_attempted": model.get("rollback_attempted"),
                "rollback_confirmed": model.get("rollback_confirmed"),
                "failure_reasons": ";".join(model.get("failure_reasons") or []),
            }
        ],
    )
    lines = [
        "# V1.5 Authoritative Resume State Atomic Write",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- authorization_id: `{model.get('authorization_id')}`",
        f"- preflight_recomputed_ready: `{model.get('preflight_recomputed_ready')}`",
        f"- current_state_sha256_rechecked: `{model.get('current_state_sha256_rechecked')}`",
        f"- write_attempted: `{model.get('write_attempted')}`",
        f"- authoritative_state_write_committed: `{model.get('authoritative_state_write_committed')}`",
        f"- state_file_created: `{model.get('state_file_created')}`",
        f"- state_file_replaced: `{model.get('state_file_replaced')}`",
        f"- rollback_attempted: `{model.get('rollback_attempted')}`",
        f"- rollback_confirmed: `{model.get('rollback_confirmed')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        "",
    ]
    if model.get("failure_reasons"):
        lines.extend(["## Failure reasons", ""])
        lines.extend(f"- `{reason}`" for reason in model.get("failure_reasons") or [])
        lines.append("")
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines), encoding="utf-8")
    return paths


def execute_v1_5_authoritative_resume_state_atomic_write(
    *,
    preflight_json: str | Path,
    writer_authorization_json: str | Path,
    authorization_id: str,
    confirmation_template: str,
    output_dir: str | Path,
    after_replace_hook: Callable[[Path], None] | None = None,
) -> dict[str, Any]:
    preflight_path = Path(preflight_json).resolve()
    writer_authorization_path = Path(writer_authorization_json).resolve()
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    result = _base_result(
        preflight_path=preflight_path,
        writer_authorization_path=writer_authorization_path,
        authorization_id=authorization_id,
        confirmation_template=confirmation_template,
        output_dir=out,
    )
    invocation_path = out / "v1_5_resume_state_atomic_write_invocation.json"
    _write_json(
        invocation_path,
        {
            "schema": "v1_5_authoritative_resume_state_atomic_write_invocation_v1",
            "generated_at": result["generated_at"],
            "preflight_json": str(preflight_path),
            "preflight_sha256": result["preflight_sha256"],
            "writer_authorization_json": str(writer_authorization_path),
            "writer_authorization_sha256": result["writer_authorization_sha256"],
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
        write_v1_5_authoritative_resume_state_atomic_writer_outputs(result, out)
        return result
    if confirmation_template != CONFIRMATION_TEMPLATE:
        result["failure_reasons"].append("confirmation_template_mismatch")
        write_v1_5_authoritative_resume_state_atomic_writer_outputs(result, out)
        return result

    preflight, target, preview, preflight_reasons = _validate_preflight(preflight_path)
    if preflight_reasons or target is None or preview is None:
        result["failure_reasons"].extend(preflight_reasons or ["preflight_paths_missing"])
        write_v1_5_authoritative_resume_state_atomic_writer_outputs(result, out)
        return result
    result["preflight_recomputed_ready"] = True
    _writer_authorization, authorization_reasons = _validate_writer_authorization(
        authorization_path=writer_authorization_path,
        preflight_path=preflight_path,
        preflight=preflight,
        target=target,
        authorization_id=authorization_id,
        confirmation_template=confirmation_template,
    )
    if authorization_reasons:
        result["failure_reasons"].extend(authorization_reasons)
        write_v1_5_authoritative_resume_state_atomic_writer_outputs(result, out)
        return result
    result["authorization_validated"] = True
    result["authoritative_state_json"] = str(target)
    result["candidate_state_preview_json"] = str(preview)
    result["candidate_state_sha256"] = str(preflight.get("candidate_state_sha256") or "")
    result["expected_existing_state_sha256"] = str(
        preflight.get("expected_existing_state_sha256") or ""
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path = target.with_name(f"{target.name}.lock")
    result["lock_path"] = str(lock_path)
    lock_fd: int | None = None
    temp_path: Path | None = None
    rollback_temp_path: Path | None = None
    snapshot_path: Path | None = None
    old_exists = False
    old_bytes = b""
    old_hash = "absent"
    replaced = False
    try:
        lock_fd = _acquire_lock(
            lock_path,
            {
                "schema": "v1_5_authoritative_resume_state_writer_lock_v1",
                "authorization_id": authorization_id,
                "created_at": _now(),
                "target": str(target),
                "expected_existing_state_sha256": result[
                    "expected_existing_state_sha256"
                ],
                "candidate_state_sha256": result["candidate_state_sha256"],
            },
        )
        result["single_writer_lock_acquired"] = True

        # The final state and candidate checks must happen while this writer owns
        # the lock; otherwise another writer could modify either input between
        # validation and replacement.
        result["observed_existing_state_sha256"] = _current_state_sha256(target)
        if result["observed_existing_state_sha256"] != result[
            "expected_existing_state_sha256"
        ]:
            raise RuntimeError("current_state_sha256_changed_after_preflight")
        result["current_state_sha256_rechecked"] = True

        candidate_bytes = preview.read_bytes()
        if _sha256_bytes(candidate_bytes) != result["candidate_state_sha256"]:
            raise RuntimeError("candidate_preview_changed_after_preflight")

        old_exists = target.is_file()
        old_bytes = target.read_bytes() if old_exists else b""
        old_hash = _sha256_bytes(old_bytes) if old_exists else "absent"
        if old_hash != result["expected_existing_state_sha256"]:
            raise RuntimeError("current_state_changed_during_locked_read")
        if old_exists and old_hash == result["candidate_state_sha256"]:
            result["overall_status"] = NOOP_STATUS
            result["already_current_noop"] = True
            result["authoritative_state_write_committed"] = True
            result["post_write_readback_sha256"] = result["candidate_state_sha256"]
        else:
            if old_exists:
                snapshot_token = hashlib.sha256(
                    authorization_id.encode("utf-8")
                ).hexdigest()[:12]
                snapshot_path = target.with_name(
                    f"{target.name}.rollback.{snapshot_token}.snapshot"
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
            result["state_file_created"] = not old_exists
            result["state_file_replaced"] = old_exists
            if after_replace_hook is not None:
                after_replace_hook(target)
            readback_hash = _sha256_file(target)
            result["post_write_readback_sha256"] = readback_hash
            if readback_hash != result["candidate_state_sha256"]:
                raise RuntimeError("post_replace_readback_sha256_mismatch")
            result["overall_status"] = COMMITTED_STATUS
            result["authoritative_state_write_committed"] = True
    except FileExistsError:
        result["failure_reasons"].append("single_writer_lock_or_snapshot_exists")
    except Exception as exc:
        result["failure_reasons"].append(str(exc) or exc.__class__.__name__)
        if replaced:
            result["rollback_attempted"] = True
            try:
                if old_exists:
                    rollback_temp_path = _write_temp_same_directory(target, old_bytes)
                    os.replace(rollback_temp_path, target)
                    rollback_temp_path = None
                    rollback_ok = _sha256_file(target) == old_hash
                else:
                    target.unlink(missing_ok=True)
                    rollback_ok = not target.exists()
                result["rollback_confirmed"] = rollback_ok
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
    write_v1_5_authoritative_resume_state_atomic_writer_outputs(result, out)
    return result


__all__ = [
    "BLOCKED_STATUS",
    "COMMITTED_STATUS",
    "CONFIRMATION_TEMPLATE",
    "NOOP_STATUS",
    "ROLLED_BACK_STATUS",
    "ROLLBACK_FAILED_STATUS",
    "SCHEMA",
    "WRITER_AUTHORIZATION_OPERATION",
    "WRITER_AUTHORIZATION_SCHEMA",
    "execute_v1_5_authoritative_resume_state_atomic_write",
    "write_v1_5_authoritative_resume_state_atomic_writer_outputs",
]
