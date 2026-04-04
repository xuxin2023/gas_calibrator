from __future__ import annotations

import json
from pathlib import Path


class RouteMemory:
    def __init__(self, path: Path, *, default_route: str = "run") -> None:
        self.path = Path(path)
        self.default_route = str(default_route or "run")

    def load(self) -> str:
        if not self.path.exists():
            return self.default_route
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            route = str(payload.get("last_page", self.default_route) or self.default_route)
            return route
        except Exception:
            return self.default_route

    def save(self, page_name: str) -> str:
        route = str(page_name or self.default_route)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"last_page": route}, ensure_ascii=False, indent=2), encoding="utf-8")
        return route
