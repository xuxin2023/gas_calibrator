from __future__ import annotations

from pathlib import Path
from typing import Any

from ..runtime.build_info_loader import load_build_info
from ..runtime.release_notes_loader import load_release_notes
from .runtime_manifest import build_runtime_manifest


def run_preflight_checks(runtime_paths: Any, *, ui_root: str | Path | None = None) -> dict[str, Any]:
    root = Path(ui_root) if ui_root is not None else Path(__file__).resolve().parents[1]
    checks: list[dict[str, str]] = []

    for name in ("base_dir", "config_dir", "cache_dir", "logs_dir", "screenshots_dir"):
        path = Path(getattr(runtime_paths, name))
        checks.append(
            {
                "name": name,
                "status": "ok" if path.exists() else "fail",
                "message": str(path),
            }
        )

    build_info = load_build_info(root / "build_info.json")
    checks.append(
        {
            "name": "build_info",
            "status": "ok" if bool(build_info.get("version")) else "warn",
            "message": str(build_info.get("version", "")),
        }
    )

    notes = load_release_notes(root / "release_notes.md")
    checks.append(
        {
            "name": "release_notes",
            "status": "ok" if bool(notes.strip()) else "warn",
            "message": "loaded" if bool(notes.strip()) else "missing",
        }
    )

    manifest = build_runtime_manifest(root)
    checks.append(
        {
            "name": "runtime_manifest",
            "status": "ok" if manifest["missing_count"] == 0 else "warn",
            "message": f"missing={manifest['missing_count']}",
        }
    )

    overall = "ok"
    if any(item["status"] == "fail" for item in checks):
        overall = "fail"
    elif any(item["status"] == "warn" for item in checks):
        overall = "warn"
    return {"overall_status": overall, "checks": checks}
