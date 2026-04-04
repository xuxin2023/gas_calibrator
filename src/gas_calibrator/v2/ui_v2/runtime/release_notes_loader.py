from __future__ import annotations

from pathlib import Path


def load_release_notes(path: str | Path | None) -> str:
    if path is None:
        return ""
    source = Path(path)
    if not source.exists():
        return ""
    try:
        return source.read_text(encoding="utf-8")
    except Exception:
        return ""
