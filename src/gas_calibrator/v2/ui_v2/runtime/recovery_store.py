from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class RecoveryStore:
    """Persist a lightweight crash snapshot for UI-only recovery."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    @classmethod
    def from_runtime_paths(cls, runtime_paths: Any) -> "RecoveryStore":
        return cls(Path(runtime_paths.base_dir) / "runtime" / "crash_snapshot.json")

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return dict(payload) if isinstance(payload, dict) else None

    def save(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        payload = dict(snapshot or {})
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    def clear(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            return
