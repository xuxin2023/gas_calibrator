from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import display_device_status, t


class DeviceStatusTable(ttk.Frame):
    """Treeview for device status rows."""

    COLUMNS = ("name", "status", "port")

    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.tree = ttk.Treeview(self, columns=self.COLUMNS, show="headings", height=8)
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.scrollbar_y = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.scrollbar_y.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=self.scrollbar_y.set)
        headings = (t("widgets.device_status.device"), t("widgets.device_status.status"), t("widgets.device_status.port"))
        for column, title in zip(self.COLUMNS, headings, strict=False):
            self.tree.heading(column, text=title)
            self.tree.column(column, width=140, anchor="w")

    def render(self, rows: list[dict[str, Any]]) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in rows:
            self.tree.insert(
                "",
                "end",
                values=(
                    row.get("name", ""),
                    row.get("status_display") or display_device_status(row.get("status", "")),
                    row.get("port", ""),
                ),
            )
