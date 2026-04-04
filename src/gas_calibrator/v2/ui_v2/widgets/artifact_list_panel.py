from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import t


class ArtifactListPanel(ttk.Frame):
    """Readonly artifact list panel."""

    COLUMNS = ("name", "present", "origin", "role_status", "path")

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
        widths = {"name": 180, "present": 90, "origin": 140, "role_status": 260, "path": 480}
        headings = (
            t("widgets.artifact_list.artifact"),
            t("widgets.artifact_list.present"),
            t("widgets.artifact_list.origin"),
            t("widgets.artifact_list.role_status"),
            t("widgets.artifact_list.path"),
        )
        for column, title in zip(self.COLUMNS, headings, strict=False):
            self.tree.heading(column, text=title)
            self.tree.column(column, width=widths[column], anchor="w")

    def render(self, rows: list[dict[str, Any]]) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in rows:
            role_display = str(row.get("artifact_role_display") or t("widgets.artifact_list.unclassified"))
            status_display = str(
                row.get("export_status_display")
                or t("widgets.artifact_list.export_status_unregistered")
            )
            exportability_display = str(
                row.get("exportability_display")
                or (
                    t("widgets.artifact_list.exportability_current_run")
                    if bool(row.get("listed_in_current_run", False)) and bool(row.get("present_on_disk", row.get("present", False)))
                    else t("widgets.artifact_list.exportability_review_reference")
                )
            )
            self.tree.insert(
                "",
                "end",
                values=(
                    row.get("name", ""),
                    t("common.yes") if bool(row.get("present_on_disk", row.get("present", False))) else t("common.no"),
                    row.get("artifact_origin_display", "")
                    or t(
                        f"widgets.artifact_list.origin_{str(row.get('artifact_origin') or 'current_run')}",
                        default=str(row.get("artifact_origin") or "current_run"),
                    ),
                    row.get("role_status_display", f"{role_display} | {status_display} | {exportability_display}"),
                    row.get("path", ""),
                ),
            )
