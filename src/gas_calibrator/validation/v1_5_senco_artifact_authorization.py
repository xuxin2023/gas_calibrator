"""Structured reviewer/approver binding for V1.5 SENCO artifact manifests."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence, Tuple

from .v1_5_artifact_hash_binding import sha256_file


SCHEMA = "v1_5_senco_artifact_authorization_v1"
READY_STATUS = "ready_for_controlled_writer_review"
BLOCKED_STATUS = "blocked"
WRITER_SCOPES = (
    "co2_senco13_pair",
    "h2o_senco24_pair",
    "co2_senco5_linear",
    "h2o_senco6_linear",
)


def _normalized_scopes(values: Sequence[str]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _normalized_device_ids(values: Sequence[str]) -> list[str]:
    normalized = set()
    for value in values:
        text = str(value or "").strip()
        if text:
            normalized.add(text.zfill(3) if text.isdigit() else text)
    return sorted(normalized)


def write_senco_artifact_authorization(
    path: str | Path,
    *,
    manifest_path: str | Path,
    reviewer: str,
    approver: str,
    authorization_id: str,
    authorized_writer_scopes: Sequence[str],
    authorized_device_ids: Sequence[str],
) -> Path:
    destination = Path(path).resolve()
    manifest = Path(manifest_path).resolve()
    reviewer_text = str(reviewer or "").strip()
    approver_text = str(approver or "").strip()
    authorization_text = str(authorization_id or "").strip()
    scopes = _normalized_scopes(authorized_writer_scopes)
    device_ids = _normalized_device_ids(authorized_device_ids)
    reasons: list[str] = []
    if not manifest.is_file():
        reasons.append("artifact_hash_manifest_missing")
    if not reviewer_text:
        reasons.append("artifact_authorization_reviewer_missing")
    if not approver_text:
        reasons.append("artifact_authorization_approver_missing")
    if reviewer_text and reviewer_text == approver_text:
        reasons.append("artifact_authorization_reviewer_equals_approver")
    if not authorization_text:
        reasons.append("artifact_authorization_id_missing")
    if not scopes:
        reasons.append("artifact_authorization_writer_scopes_missing")
    if not device_ids:
        reasons.append("artifact_authorization_device_ids_missing")
    for scope in scopes:
        if scope not in WRITER_SCOPES:
            reasons.append(f"artifact_authorization_unsupported_writer_scope:{scope}")

    payload = {
        "schema": SCHEMA,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": READY_STATUS if not reasons else BLOCKED_STATUS,
        "blockers": list(dict.fromkeys(reasons)),
        "authorization_id": authorization_text,
        "reviewer": reviewer_text,
        "approver": approver_text,
        "authorized_writer_scopes": scopes,
        "authorized_device_ids": device_ids,
        "manifest_path": str(manifest),
        "manifest_sha256": sha256_file(manifest) if manifest.is_file() else "",
        "manual_authorization_record": True,
        "requires_explicit_writer_unlock": True,
        "automatic_execution_allowed": False,
        "opens_com": False,
        "writes_senco": False,
        "controls_routes": False,
        "connects_postgresql": False,
        "not_real_acceptance_evidence": True,
    }
    destination.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return destination


def validate_senco_artifact_authorization(
    path: str | Path,
    *,
    manifest_path: str | Path,
    reviewer: str,
    approver: str,
    writer_scope: str,
    device_ids: Sequence[str],
) -> Tuple[bool, list[str], Mapping[str, Any]]:
    authorization_path = Path(path).resolve()
    manifest = Path(manifest_path).resolve()
    reasons: list[str] = []
    if not authorization_path.is_file():
        return False, ["senco_artifact_authorization_missing"], {
            "authorization_path": str(authorization_path)
        }
    try:
        payload = json.loads(authorization_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, [f"senco_artifact_authorization_invalid:{type(exc).__name__}"], {
            "authorization_path": str(authorization_path)
        }
    if not isinstance(payload, Mapping):
        return False, ["senco_artifact_authorization_not_object"], {
            "authorization_path": str(authorization_path)
        }
    if str(payload.get("schema") or "") != SCHEMA:
        reasons.append("senco_artifact_authorization_schema_mismatch")
    if str(payload.get("overall_status") or "") != READY_STATUS:
        reasons.append("senco_artifact_authorization_not_ready")
    if payload.get("blockers") not in ([], ()):
        reasons.append("senco_artifact_authorization_has_blockers")
    for field, expected in {
        "manual_authorization_record": True,
        "requires_explicit_writer_unlock": True,
        "automatic_execution_allowed": False,
        "opens_com": False,
        "writes_senco": False,
        "controls_routes": False,
        "connects_postgresql": False,
        "not_real_acceptance_evidence": True,
    }.items():
        if field not in payload or bool(payload.get(field)) is not expected:
            reasons.append(f"senco_artifact_authorization_boundary_mismatch:{field}")

    recorded_reviewer = str(payload.get("reviewer") or "").strip()
    recorded_approver = str(payload.get("approver") or "").strip()
    if not recorded_reviewer:
        reasons.append("senco_artifact_authorization_reviewer_missing")
    if not recorded_approver:
        reasons.append("senco_artifact_authorization_approver_missing")
    if recorded_reviewer and recorded_reviewer == recorded_approver:
        reasons.append("senco_artifact_authorization_reviewer_equals_approver")
    if recorded_reviewer != str(reviewer or "").strip():
        reasons.append("senco_artifact_authorization_reviewer_mismatch")
    if recorded_approver != str(approver or "").strip():
        reasons.append("senco_artifact_authorization_approver_mismatch")
    if not str(payload.get("authorization_id") or "").strip():
        reasons.append("senco_artifact_authorization_id_missing")

    scopes = _normalized_scopes(payload.get("authorized_writer_scopes") or ())
    requested_scope = str(writer_scope or "").strip()
    if requested_scope not in WRITER_SCOPES:
        reasons.append(f"senco_artifact_authorization_writer_scope_invalid:{requested_scope or 'missing'}")
    elif requested_scope not in scopes:
        reasons.append(f"senco_artifact_authorization_writer_scope_not_authorized:{requested_scope}")

    authorized_devices = _normalized_device_ids(payload.get("authorized_device_ids") or ())
    requested_devices = _normalized_device_ids(device_ids)
    if not authorized_devices:
        reasons.append("senco_artifact_authorization_device_ids_missing")
    if not requested_devices:
        reasons.append("senco_artifact_authorization_requested_device_ids_missing")
    for device_id in requested_devices:
        if device_id not in authorized_devices:
            reasons.append(f"senco_artifact_authorization_device_not_authorized:{device_id}")

    recorded_manifest = str(payload.get("manifest_path") or "").strip()
    if not recorded_manifest or Path(recorded_manifest).resolve() != manifest:
        reasons.append("senco_artifact_authorization_manifest_path_mismatch")
    recorded_hash = str(payload.get("manifest_sha256") or "").strip().lower()
    if len(recorded_hash) != 64 or any(char not in "0123456789abcdef" for char in recorded_hash):
        reasons.append("senco_artifact_authorization_manifest_sha256_invalid")
    elif not manifest.is_file():
        reasons.append("senco_artifact_authorization_manifest_missing")
    elif sha256_file(manifest) != recorded_hash:
        reasons.append("senco_artifact_authorization_manifest_sha256_mismatch")

    detail = {
        "authorization_path": str(authorization_path),
        "authorization_id": str(payload.get("authorization_id") or ""),
        "reviewer": recorded_reviewer,
        "approver": recorded_approver,
        "writer_scope": requested_scope,
        "authorized_device_ids": authorized_devices,
        "requested_device_ids": requested_devices,
        "manifest_path": str(manifest),
        "manifest_sha256": recorded_hash,
        "status": "pass" if not reasons else "blocked",
    }
    return not reasons, list(dict.fromkeys(reasons)), detail
