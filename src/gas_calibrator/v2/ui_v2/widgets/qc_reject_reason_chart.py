from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..i18n import t


class QCRejectReasonChart(ttk.Frame):
    """Simple bar chart for QC reject reason counts."""

    def __init__(self, parent: tk.Misc, *, title: str | None = None, height: int = 150) -> None:
        super().__init__(parent, style="Card.TFrame", padding=8)
        self._reasons: list[dict[str, object]] = []
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        ttk.Label(self, text=title or t("widgets.qc_reject_reason.title"), style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.canvas = tk.Canvas(self, height=height, highlightthickness=0, background="#ffffff")
        self.canvas.grid(row=1, column=0, sticky="nsew")
        self.canvas.bind("<Configure>", lambda _event: self._redraw())

    def render(self, snapshot: dict[str, object]) -> None:
        self._reasons = [dict(item) for item in list(snapshot.get("rows", []) or [])]
        self._redraw()

    def _redraw(self) -> None:
        self.canvas.delete("all")
        width = max(1, int(self.canvas.winfo_width() or 420))
        height = max(1, int(self.canvas.winfo_height() or 150))
        self.canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        if not self._reasons:
            self.canvas.create_text(width / 2, height / 2, text=t("widgets.qc_reject_reason.no_data"), fill="#627587")
            return

        max_count = max(int(item.get("count", 0) or 0) for item in self._reasons) or 1
        left = 16
        top = 12
        bar_height = 18
        gap = 12
        usable_width = max(1, width - left - 120)
        for index, item in enumerate(self._reasons):
            y = top + index * (bar_height + gap)
            count = int(item.get("count", 0) or 0)
            bar_width = usable_width * count / max_count
            label = str(item.get("reason", "--") or "--")
            self.canvas.create_text(left, y + (bar_height / 2), text=label, anchor="w", fill="#627587")
            self.canvas.create_rectangle(left + 130, y, left + 130 + bar_width, y + bar_height, outline="", fill="#dc2626")
            self.canvas.create_text(left + 135 + bar_width, y + (bar_height / 2), text=str(count), anchor="w", fill="#0f2338")
