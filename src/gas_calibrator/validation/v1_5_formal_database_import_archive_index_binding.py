"""Freeze and re-validate the formal archive closure index for database import."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .v1_5_artifact_hash_binding import sha256_file


def snapshot_v1_5_formal_archive_index(
    archive_closure_json: str | Path | None,
) -> tuple[bool, list[str], dict[str, Any]]:
    """Return the resolved archive-index path and its current SHA-256."""

    reasons: list[str] = []
    archive_path = Path(archive_closure_json).resolve() if archive_closure_json else None
    current_sha256 = ""
    if archive_path is None:
        reasons.append("archive_closure_index_path_missing")
    elif not archive_path.is_file():
        reasons.append("archive_closure_index_file_missing")
    else:
        current_sha256 = sha256_file(archive_path)
    return not reasons, reasons, {
        "archive_closure_json": str(archive_path) if archive_path else "",
        "archive_closure_sha256": current_sha256,
    }


def validate_v1_5_formal_archive_index_binding(
    archive_closure_json: str | Path | None,
    *,
    expected_path: str | Path | None,
    expected_sha256: str,
    source_label: str,
) -> tuple[bool, list[str], dict[str, Any]]:
    """Require the live archive index to match a previously frozen path and hash."""

    ready, reasons, detail = snapshot_v1_5_formal_archive_index(archive_closure_json)
    expected_path_text = str(Path(expected_path).resolve()) if expected_path else ""
    expected_sha_text = str(expected_sha256 or "").strip().lower()
    current_path = str(detail.get("archive_closure_json") or "")
    current_sha = str(detail.get("archive_closure_sha256") or "").lower()
    if not expected_path_text:
        reasons.append(f"{source_label}_archive_closure_json_missing")
    elif current_path and current_path != expected_path_text:
        reasons.append(f"{source_label}_archive_closure_json_mismatch")
    if len(expected_sha_text) != 64 or any(char not in "0123456789abcdef" for char in expected_sha_text):
        reasons.append(f"{source_label}_archive_closure_sha256_invalid")
    elif current_sha and current_sha != expected_sha_text:
        reasons.append(f"{source_label}_archive_closure_sha256_mismatch")
    reasons = list(dict.fromkeys(reasons))
    detail.update(
        {
            "expected_archive_closure_json": expected_path_text,
            "expected_archive_closure_sha256": expected_sha_text,
        }
    )
    return ready and not reasons, reasons, detail
