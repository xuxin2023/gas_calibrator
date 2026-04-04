from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import t


class AlgorithmCompareTable(ttk.Frame):
    """Treeview for algorithm comparison/config rows."""

    COLUMNS = ("algorithm", "source", "status", "note")

    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.tree = ttk.Treeview(self, columns=self.COLUMNS, show="headings", height=9)
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.scrollbar_y = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.scrollbar_x = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.scrollbar_y.grid(row=0, column=1, sticky="ns")
        self.scrollbar_x.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        self.tree.configure(yscrollcommand=self.scrollbar_y.set, xscrollcommand=self.scrollbar_x.set)
        widths = {"algorithm": 180, "source": 150, "status": 140, "note": 360}
        headings = (
            t("widgets.algorithm_compare.algorithm", default="算法"),
            t("widgets.algorithm_compare.source", default="来源"),
            t("widgets.algorithm_compare.status", default="状态"),
            t("widgets.algorithm_compare.note", default="备注"),
        )
        for column, title in zip(self.COLUMNS, headings, strict=False):
            self.tree.heading(column, text=title)
            self.tree.column(column, width=widths[column], anchor="w")

    def render(self, rows: list[dict[str, Any]]) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in rows:
            self.tree.insert(
                "",
                "end",
                values=(
                    row.get("algorithm", ""),
                    row.get("source_display", row.get("source", "")),
                    row.get("status_display", row.get("status", "")),
                    row.get("note_display", row.get("note", "")),
                ),
            )
