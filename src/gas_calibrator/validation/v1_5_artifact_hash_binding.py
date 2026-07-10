"""SHA256 artifact binding helpers for the V1.5 final SENCO review chain."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence, Tuple


SCHEMA = "v1_5_final_senco_artifact_hash_manifest_v1"
ALGORITHM = "sha256"


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_artifact_hash_manifest(
    path: str | Path,
    *,
    artifacts: Mapping[str, str | Path],
) -> Path:
    destination = Path(path).resolve()
    rows = []
    for role, source in sorted(artifacts.items()):
        source_path = Path(source).resolve()
        if not source_path.is_file():
            continue
        rows.append(
            {
                "role": str(role),
                "path": str(source_path),
                "size_bytes": source_path.stat().st_size,
                "sha256": sha256_file(source_path),
            }
        )
    payload = {
        "schema": SCHEMA,
        "algorithm": ALGORITHM,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "no_write": True,
        "opens_com": False,
        "writes_senco": False,
        "controls_routes": False,
        "artifacts": rows,
    }
    destination.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return destination


def validate_artifact_hash_manifest(
    path: str | Path,
    *,
    required_roles: Sequence[str],
    expected_paths: Mapping[str, str | Path] | None = None,
) -> Tuple[bool, list[str], Dict[str, Any]]:
    manifest_path = Path(path).resolve()
    reasons: list[str] = []
    if not manifest_path.is_file():
        return False, ["artifact_hash_manifest_missing"], {"manifest_path": str(manifest_path)}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, [f"artifact_hash_manifest_invalid:{type(exc).__name__}"], {
            "manifest_path": str(manifest_path)
        }
    if not isinstance(payload, Mapping):
        return False, ["artifact_hash_manifest_not_object"], {"manifest_path": str(manifest_path)}
    if str(payload.get("schema") or "") != SCHEMA:
        reasons.append("artifact_hash_manifest_schema_mismatch")
    if str(payload.get("algorithm") or "").strip().lower() != ALGORITHM:
        reasons.append("artifact_hash_manifest_algorithm_not_sha256")
    for field, expected in {
        "no_write": True,
        "opens_com": False,
        "writes_senco": False,
        "controls_routes": False,
    }.items():
        if field not in payload or bool(payload.get(field)) is not expected:
            reasons.append(f"artifact_hash_manifest_boundary_mismatch:{field}")

    rows = payload.get("artifacts")
    if not isinstance(rows, list):
        return False, reasons + ["artifact_hash_manifest_rows_missing"], {"manifest_path": str(manifest_path)}
    by_role: Dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            reasons.append("artifact_hash_manifest_row_not_object")
            continue
        role = str(row.get("role") or "").strip()
        if not role:
            reasons.append("artifact_hash_manifest_role_missing")
            continue
        if role in by_role:
            reasons.append(f"artifact_hash_manifest_duplicate_role:{role}")
            continue
        by_role[role] = row

    expected_paths = expected_paths or {}
    for role in required_roles:
        if role not in by_role:
            reasons.append(f"artifact_hash_manifest_required_role_missing:{role}")
    roles_to_validate = set(required_roles) | set(expected_paths)
    for role, row in by_role.items():
        if role not in roles_to_validate:
            continue
        source_text = str(row.get("path") or "").strip()
        if not source_text:
            reasons.append(f"artifact_hash_manifest_path_missing:{role}")
            continue
        source = Path(source_text).resolve()
        expected = expected_paths.get(role)
        if expected is not None and source != Path(expected).resolve():
            reasons.append(f"artifact_hash_manifest_path_mismatch:{role}")
        if not source.is_file():
            reasons.append(f"artifact_hash_manifest_source_missing:{role}")
            continue
        expected_size = row.get("size_bytes")
        try:
            size_matches = int(expected_size) == source.stat().st_size
        except Exception:
            size_matches = False
        if not size_matches:
            reasons.append(f"artifact_hash_manifest_size_mismatch:{role}")
        expected_hash = str(row.get("sha256") or "").strip().lower()
        if len(expected_hash) != 64 or any(char not in "0123456789abcdef" for char in expected_hash):
            reasons.append(f"artifact_hash_manifest_sha256_invalid:{role}")
            continue
        if sha256_file(source) != expected_hash:
            reasons.append(f"artifact_hash_manifest_sha256_mismatch:{role}")

    detail = {
        "manifest_path": str(manifest_path),
        "artifact_count": len(by_role),
        "required_roles": list(required_roles),
        "status": "pass" if not reasons else "blocked",
    }
    return not reasons, list(dict.fromkeys(reasons)), detail
