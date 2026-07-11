"""Path and SHA-256 binding helpers for formal database-import input artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .v1_5_artifact_hash_binding import sha256_file


def snapshot_v1_5_database_import_input(
    path: str | Path | None,
    *,
    input_label: str,
) -> tuple[bool, list[str], dict[str, Any]]:
    """Resolve and hash one database-import input without opening external systems."""

    source = Path(path).resolve() if path else None
    reasons: list[str] = []
    current_sha256 = ""
    if source is None:
        reasons.append(f"{input_label}_path_missing")
    elif not source.is_file():
        reasons.append(f"{input_label}_file_missing")
    else:
        current_sha256 = sha256_file(source)
    return not reasons, reasons, {
        "input_label": input_label,
        "current_path": str(source) if source else "",
        "current_sha256": current_sha256,
    }


def validate_v1_5_database_import_input_binding(
    path: str | Path | None,
    *,
    expected_path: str | Path | None,
    expected_sha256: str,
    source_label: str,
    input_label: str,
) -> tuple[bool, list[str], dict[str, Any]]:
    """Require one input to match the exact path and hash frozen by an earlier gate."""

    ready, reasons, detail = snapshot_v1_5_database_import_input(path, input_label=input_label)
    expected_path_value = Path(expected_path).resolve() if expected_path else None
    expected_sha_value = str(expected_sha256 or "").strip().lower()
    current_path_value = Path(detail["current_path"]) if detail.get("current_path") else None
    current_sha_value = str(detail.get("current_sha256") or "").lower()
    reason_prefix = f"{source_label}_{input_label}"
    if expected_path_value is None:
        reasons.append(f"{reason_prefix}_path_missing")
    elif current_path_value is not None and current_path_value != expected_path_value:
        reasons.append(f"{reason_prefix}_path_mismatch")
    if len(expected_sha_value) != 64 or any(
        char not in "0123456789abcdef" for char in expected_sha_value
    ):
        reasons.append(f"{reason_prefix}_sha256_invalid")
    elif current_sha_value and current_sha_value != expected_sha_value:
        reasons.append(f"{reason_prefix}_sha256_mismatch")
    reasons = list(dict.fromkeys(reasons))
    detail.update(
        {
            "expected_path": str(expected_path_value) if expected_path_value else "",
            "expected_sha256": expected_sha_value,
        }
    )
    return ready and not reasons, reasons, detail
