from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


class RecentRunsStore:
    def __init__(self, path: Path, *, limit: int = 20) -> None:
        self.path = Path(path)
        self.limit = max(1, int(limit))

    def load(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return [dict(item) for item in payload if isinstance(item, dict)]
        except Exception:
            return []
        return []

    def add(self, run_path: str) -> list[dict[str, Any]]:
        run_path = str(run_path or "").strip()
        if not run_path:
            return self.load()
        now = datetime.now(timezone.utc).isoformat()
        rows = [item for item in self.load() if str(item.get("path", "")) != run_path]
        rows.insert(0, {"path": run_path, "opened_at": now})
        rows = rows[: self.limit]
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        return rows
