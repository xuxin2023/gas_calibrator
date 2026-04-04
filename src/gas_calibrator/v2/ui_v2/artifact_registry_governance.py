from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.artifact_catalog import (
    DEFAULT_ROLE_CATALOG,
    KNOWN_ARTIFACT_KEYS_BY_FILENAME,
    KNOWN_ARTIFACT_ROLES,
    build_role_by_key as _build_role_by_key,
    infer_artifact_identity as _infer_artifact_identity,
    merge_role_catalog as _merge_role_catalog,
    normalize_artifact_role as _normalize_artifact_role,
)

OFFICIAL_EXPORT_STATUSES = frozenset({"ok", "skipped", "missing", "error"})


def normalize_path_token(value: Any) -> str:
    return str(value or "").strip().replace("\\", "/").rstrip("/").lower()


def normalize_artifact_role(value: Any) -> str:
    return _normalize_artifact_role(value)


def normalize_export_status(value: Any) -> str | None:
    status = str(value or "").strip().lower()
    return status if status in OFFICIAL_EXPORT_STATUSES else None


def merge_role_catalog(role_catalog: dict[str, Any] | None = None) -> dict[str, list[str]]:
    return _merge_role_catalog(role_catalog)


def build_role_by_key(role_catalog: dict[str, Any] | None = None) -> dict[str, str]:
    return _build_role_by_key(role_catalog)


def infer_artifact_identity(
    path_or_name: Any,
    *,
    role_catalog: dict[str, Any] | None = None,
) -> dict[str, str]:
    return _infer_artifact_identity(path_or_name, role_catalog=role_catalog)


def build_current_run_governance(
    path: Any,
    *,
    artifact_exports: dict[str, Any] | None = None,
    role_catalog: dict[str, Any] | None = None,
    present_on_disk: bool | None = None,
) -> dict[str, Any]:
    normalized_path = normalize_path_token(path)
    export_payloads = {str(name): dict(payload or {}) for name, payload in dict(artifact_exports or {}).items()}
    export_by_path = {
        normalize_path_token(payload.get("path")): (name, payload)
        for name, payload in export_payloads.items()
        if normalize_path_token(payload.get("path"))
    }

    matched_key = ""
    matched_payload: dict[str, Any] = {}
    if normalized_path and normalized_path in export_by_path:
        matched_key, matched_payload = export_by_path[normalized_path]
    else:
        inferred = infer_artifact_identity(path, role_catalog=role_catalog)
        matched_key = str(inferred.get("artifact_key") or "")
        matched_payload = dict(export_payloads.get(matched_key) or {}) if matched_key else {}

    inferred_identity = infer_artifact_identity(path, role_catalog=role_catalog)
    artifact_key = matched_key or str(inferred_identity.get("artifact_key") or "")
    artifact_role = normalize_artifact_role(
        matched_payload.get("role")
        or inferred_identity.get("artifact_role")
    )
    export_status = normalize_export_status(matched_payload.get("status"))
    export_status_known = export_status is not None
    present_flag = bool(Path(str(path)).exists()) if present_on_disk is None else bool(present_on_disk)
    return {
        "artifact_key": artifact_key,
        "artifact_role": artifact_role,
        "export_status": export_status,
        "export_status_known": export_status_known,
        "exportable_in_current_run": present_flag,
    }
