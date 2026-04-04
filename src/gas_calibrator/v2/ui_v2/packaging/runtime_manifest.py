from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_runtime_manifest(ui_root: str | Path | None = None) -> dict[str, Any]:
    root = Path(ui_root) if ui_root is not None else Path(__file__).resolve().parents[1]
    required = [
        "app.py",
        "shell.py",
        "styles.py",
        "controllers/app_facade.py",
        "controllers/live_state_feed.py",
        "theme/tokens.py",
        "pages/run_control_page.py",
        "widgets/metric_card.py",
    ]
    files = [{"path": item, "present": (root / item).exists()} for item in required]
    return {
        "ui_root": str(root),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "required_files": files,
        "missing_count": sum(1 for item in files if not item["present"]),
    }
