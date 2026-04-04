from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import display_device_status, t


class AnalyzerHealthPanel(ttk.Frame):
    """Readonly analyzer health comparison panel."""

    COLUMNS = ("analyzer", "status", "health", "note")

    def __init__(self, parent: tk.Misc, *, title: str | None = None) -> None:
        super().__init__(parent, style="Card.TFrame", padding=8)
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        ttk.Label(self, text=title or t("widgets.analyzer_health.title"), style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.tree = ttk.Treeview(self, columns=self.COLUMNS, show="headings", height=7)
        self.tree.grid(row=1, column=0, sticky="nsew")
        self.scrollbar_y = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.scrollbar_y.grid(row=1, column=1, sticky="ns", padx=(6, 0))
        self.tree.configure(yscrollcommand=self.scrollbar_y.set)
        widths = {"analyzer": 160, "status": 110, "health": 100, "note": 320}
        headings = (
            t("widgets.analyzer_health.analyzer"),
            t("widgets.analyzer_health.status"),
            t("widgets.analyzer_health.health"),
            t("widgets.analyzer_health.note"),
        )
        for column, title_text in zip(self.COLUMNS, headings, strict=False):
            self.tree.heading(column, text=title_text)
            self.tree.column(column, width=widths[column], anchor="w")

    def render(self, snapshot: dict[str, Any]) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in list(snapshot.get("rows", []) or []):
            self.tree.insert(
                "",
                "end",
                values=(
                    row.get("analyzer", ""),
                    row.get("status_display") or display_device_status(row.get("status", "")),
                    row.get("health", ""),
                    row.get("note", ""),
                ),
            )
