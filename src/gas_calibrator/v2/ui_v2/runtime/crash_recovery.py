from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..i18n import display_page, display_phase, t
from .recovery_store import RecoveryStore


class CrashRecovery:
    """Manage crash snapshot detection and UI-context restoration prompts."""

    def __init__(self, store: RecoveryStore) -> None:
        self.store = store

    def has_pending_recovery(self) -> bool:
        return self.store.exists() and self.load_pending_snapshot() is not None

    def load_pending_snapshot(self) -> dict[str, Any] | None:
        return self.store.load()

    def save_ui_snapshot(
        self,
        *,
        current_page: str,
        ui_snapshot: dict[str, Any],
        logs: list[str] | None = None,
    ) -> dict[str, Any]:
        run = dict(ui_snapshot.get("run", {}) or {})
        payload = {
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "run_id": str(run.get("run_id", "") or ""),
            "phase": str(run.get("phase", "") or ""),
            "message": str(run.get("message", "") or ""),
            "current_page": str(current_page or "run"),
            "current_point": str(run.get("current_point", "") or ""),
            "route": str(run.get("route", "") or ""),
            "progress_pct": float(run.get("progress_pct", 0.0) or 0.0),
            "recent_logs": [str(item) for item in list(logs or [])[-20:]],
        }
        return self.store.save(payload)

    def build_prompt(self, snapshot: dict[str, Any] | None = None) -> str:
        payload = snapshot or self.load_pending_snapshot() or {}
        run_id = str(payload.get("run_id", "--") or "--")
        phase = display_phase(str(payload.get("phase", "--") or "--"), default="--")
        page = str(payload.get("current_page", "run") or "run")
        when = str(payload.get("saved_at", "") or "")
        return t(
            "shell.recovery.prompt",
            when=when or t("shell.recovery.previous_run"),
            run_id=run_id,
            phase=phase,
            page=display_page(page, default=page or "--"),
        )

    def clear(self) -> None:
        self.store.clear()
