from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import display_notification_level, t
from .empty_state import EmptyState


class NotificationCenter(ttk.Frame):
    """Shell-level notification list for warnings, exports, and events."""

    def __init__(self, parent: tk.Misc, *, title: str | None = None) -> None:
        super().__init__(parent, style="Card.TFrame", padding=8)
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        ttk.Label(self, text=title or t("widgets.notification.title"), style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.listbox = tk.Listbox(self, height=5, activestyle="none", borderwidth=0, highlightthickness=0)
        self.listbox.grid(row=1, column=0, sticky="nsew")
        self.empty_state = EmptyState(
            self,
            title=t("widgets.notification.empty_title"),
            message=t("widgets.notification.empty_message"),
        )
        self.empty_state.grid(row=1, column=0, sticky="nsew")

    def render(self, snapshot: dict[str, Any]) -> None:
        items = list(snapshot.get("items", []) or [])
        self.listbox.delete(0, "end")
        if not items:
            self.listbox.grid_remove()
            self.empty_state.grid()
            return
        self.empty_state.grid_remove()
        self.listbox.grid()
        for item in items:
            if isinstance(item, dict):
                raw_level = str(item.get("level", "info") or "info")
                level = display_notification_level(raw_level, default=raw_level.upper())
                message = str(item.get("message", "") or "")
                text = f"[{level}] {message}" if message else f"[{level}]"
            else:
                text = str(item)
            self.listbox.insert("end", text)
