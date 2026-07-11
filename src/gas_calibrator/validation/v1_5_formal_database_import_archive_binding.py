"""Validate the frozen SENCO authorization binding before database import review."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .v1_5_artifact_hash_binding import sha256_file


BINDING_SCHEMA = "v1_5_senco_authorization_archive_binding_v1"
BINDING_ARTIFACT_ROLE = "senco_authorization_write_traceability_json"


def _valid_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _load_json(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _artifact_rows(archive: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [row for row in archive.get("artifacts") or [] if isinstance(row, Mapping)]


def _resolved_path(value: Any) -> str:
    text = str(value or "").strip()
    return str(Path(text).resolve()) if text else ""


def validate_v1_5_database_import_archive_binding(
    archive: Mapping[str, Any],
) -> tuple[bool, list[str], dict[str, Any]]:
    """Re-hash the exact archive binding and all bound source evidence."""

    reasons: list[str] = []
    embedded = archive.get("senco_authorization_write_traceability")
    if not isinstance(embedded, Mapping):
        return False, ["senco_authorization_archive_binding_missing"], {
            "status": "missing",
            "binding_path": "",
            "binding_sha256": "",
        }
    if embedded.get("ready_for_archive_release") is not True:
        reasons.append("senco_authorization_archive_binding_not_ready")
    if embedded.get("schema") != BINDING_SCHEMA:
        reasons.append(f"senco_authorization_archive_binding_embedded_schema={embedded.get('schema') or 'missing'}")
    if int(embedded.get("blocker_count") or 0) or embedded.get("blockers") not in (None, [], ()):
        reasons.append("senco_authorization_archive_binding_has_blockers")
    write_evidence_present = bool(embedded.get("write_evidence_present"))
    if str(embedded.get("overall_status") or "") not in {
        "ready_for_archive_release",
        "not_applicable_no_main_senco_write_evidence",
    }:
        reasons.append(
            f"senco_authorization_archive_binding_status={embedded.get('overall_status') or 'missing'}"
        )
    if write_evidence_present:
        if str(embedded.get("overall_status") or "") != "ready_for_archive_release":
            reasons.append("senco_authorization_archive_write_status_not_ready")
        if not str(embedded.get("authorization_id") or "").strip():
            reasons.append("senco_authorization_archive_authorization_id_missing")
        for key in ("authorization_path", "authorization_sha256", "manifest_path", "manifest_sha256"):
            if not str(embedded.get(key) or "").strip():
                reasons.append(f"senco_authorization_archive_{key}_missing")
        if not list(embedded.get("writer_evidence") or []):
            reasons.append("senco_authorization_archive_writer_evidence_missing")
    elif str(embedded.get("overall_status") or "") != "not_applicable_no_main_senco_write_evidence":
        reasons.append("senco_authorization_archive_no_write_status_invalid")

    artifacts = _artifact_rows(archive)
    binding_records = [row for row in artifacts if row.get("role") == BINDING_ARTIFACT_ROLE]
    if len(binding_records) != 1:
        reasons.append(f"senco_authorization_archive_binding_artifact_count={len(binding_records)}")
        binding_record: Mapping[str, Any] = {}
    else:
        binding_record = binding_records[0]
    binding_path_text = _resolved_path(binding_record.get("path"))
    binding_path = Path(binding_path_text) if binding_path_text else None
    binding_recorded_sha = str(binding_record.get("sha256") or "").strip().lower()
    binding_current_sha = ""
    binding_payload: Mapping[str, Any] = {}
    if binding_path is None or not binding_path.is_file():
        reasons.append("senco_authorization_archive_binding_json_missing")
    else:
        binding_current_sha = sha256_file(binding_path)
        if not _valid_sha256(binding_recorded_sha):
            reasons.append("senco_authorization_archive_binding_sha256_invalid")
        elif binding_current_sha != binding_recorded_sha:
            reasons.append("senco_authorization_archive_binding_sha256_mismatch")
        binding_payload = _load_json(binding_path)
        if not binding_payload:
            reasons.append("senco_authorization_archive_binding_json_invalid")
        elif binding_payload.get("schema") != BINDING_SCHEMA:
            reasons.append(
                f"senco_authorization_archive_binding_schema={binding_payload.get('schema') or 'missing'}"
            )
        elif dict(binding_payload) != dict(embedded):
            reasons.append("senco_authorization_archive_binding_payload_differs_from_archive_index")

    artifacts_by_path: dict[str, Mapping[str, Any]] = {}
    for row in artifacts:
        path = _resolved_path(row.get("path"))
        if path:
            artifacts_by_path[path] = row

    source_pairs: list[tuple[str, str, str, str]] = []
    for role, expected_artifact_role, path_key, hash_key in (
        ("authorization", "senco_artifact_authorization", "authorization_path", "authorization_sha256"),
        ("manifest", "senco_artifact_hash_manifest", "manifest_path", "manifest_sha256"),
    ):
        path = _resolved_path(embedded.get(path_key))
        expected_hash = str(embedded.get(hash_key) or "").strip().lower()
        if path or expected_hash:
            source_pairs.append((role, expected_artifact_role, path, expected_hash))
    for index, row in enumerate(embedded.get("writer_evidence") or [], start=1):
        if not isinstance(row, Mapping):
            reasons.append(f"senco_authorization_writer_evidence_{index}_invalid")
            continue
        if row.get("status") != "pass":
            reasons.append(f"senco_authorization_writer_evidence_{index}_not_pass")
        scope = str(row.get("writer_scope") or "writer").replace("-", "_")
        for suffix, path_key, hash_key in (
            ("metadata", "metadata_path", "metadata_sha256"),
            ("readback_rows", "write_rows_path", "write_rows_sha256"),
        ):
            source_pairs.append(
                (
                    f"writer_{index}_{suffix}",
                    f"senco_write_{index:03d}_{scope}_{suffix}",
                    _resolved_path(row.get(path_key)),
                    str(row.get(hash_key) or "").strip().lower(),
                )
            )

    verified_sources = 0
    for role, expected_artifact_role, path_text, expected_hash in source_pairs:
        if not path_text:
            reasons.append(f"senco_authorization_archive_source_path_missing:{role}")
            continue
        if not _valid_sha256(expected_hash):
            reasons.append(f"senco_authorization_archive_source_sha256_invalid:{role}")
            continue
        source_path = Path(path_text)
        if not source_path.is_file():
            reasons.append(f"senco_authorization_archive_source_missing:{role}")
            continue
        if sha256_file(source_path) != expected_hash:
            reasons.append(f"senco_authorization_archive_source_sha256_mismatch:{role}")
            continue
        artifact = artifacts_by_path.get(path_text)
        if artifact is None:
            reasons.append(f"senco_authorization_archive_source_not_indexed:{role}")
            continue
        if str(artifact.get("sha256") or "").strip().lower() != expected_hash:
            reasons.append(f"senco_authorization_archive_source_index_sha256_mismatch:{role}")
            continue
        if str(artifact.get("role") or "") != expected_artifact_role:
            reasons.append(f"senco_authorization_archive_source_role_mismatch:{role}")
            continue
        verified_sources += 1

    reasons = list(dict.fromkeys(reasons))
    return not reasons, reasons, {
        "status": "ready" if not reasons else "blocked",
        "binding_path": binding_path_text,
        "binding_sha256": binding_recorded_sha,
        "binding_current_sha256": binding_current_sha,
        "binding_overall_status": embedded.get("overall_status"),
        "write_evidence_present": write_evidence_present,
        "source_artifact_count": len(source_pairs),
        "verified_source_artifact_count": verified_sources,
    }
