from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..i18n import t


class ResidualChart(ttk.Frame):
    """Lightweight residual comparison chart for multiple algorithms."""

    COLORS = ("#0e7c86", "#dc2626", "#15803d", "#f59e0b")

    def __init__(self, parent: tk.Misc, *, title: str | None = None, height: int = 180) -> None:
        super().__init__(parent, style="Card.TFrame", padding=8)
        self._payload: list[dict[str, object]] = []
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        ttk.Label(self, text=title or t("widgets.residual_chart.title"), style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.canvas = tk.Canvas(self, height=height, highlightthickness=0, background="#ffffff")
        self.canvas.grid(row=1, column=0, sticky="nsew")
        self.canvas.bind("<Configure>", lambda _event: self._redraw())

    def render(self, snapshot: dict[str, object]) -> None:
        self._payload = [dict(item) for item in list(snapshot.get("series", []) or [])]
        self._redraw()

    def _redraw(self) -> None:
        self.canvas.delete("all")
        width = max(1, int(self.canvas.winfo_width() or 420))
        height = max(1, int(self.canvas.winfo_height() or 180))
        self.canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        if not self._payload:
            self.canvas.create_text(width / 2, height / 2, text=t("widgets.residual_chart.no_data"), fill="#627587")
            return

        residuals = [float(item) for row in self._payload for item in list(row.get("residuals", []) or [])]
        if not residuals:
            self.canvas.create_text(width / 2, height / 2, text=t("widgets.residual_chart.no_data"), fill="#627587")
            return

        max_abs = max(abs(item) for item in residuals) or 1.0
        margin_left = 18
        margin_right = 12
        margin_top = 18
        margin_bottom = 18
        mid_y = height / 2
        plot_width = max(1, width - margin_left - margin_right)
        plot_height = max(1, height - margin_top - margin_bottom)
        self.canvas.create_line(margin_left, mid_y, width - margin_right, mid_y, fill="#d5dee8", dash=(4, 3))

        for index, row in enumerate(self._payload):
            residual_series = [float(item) for item in list(row.get("residuals", []) or [])]
            if not residual_series:
                continue
            color = self.COLORS[index % len(self.COLORS)]
            count = max(1, len(residual_series) - 1)
            points: list[float] = []
            for offset, value in enumerate(residual_series):
                x = margin_left + plot_width * offset / count
                y = mid_y - (value / max_abs) * (plot_height / 2)
                points.extend((x, y))
                self.canvas.create_oval(x - 2, y - 2, x + 2, y + 2, outline=color, fill=color)
            if len(points) >= 4:
                self.canvas.create_line(*points, fill=color, width=2, smooth=True)
            legend_x = margin_left + 8 + (index * 140)
            self.canvas.create_rectangle(legend_x, 4, legend_x + 10, 14, outline="", fill=color)
            self.canvas.create_text(
                legend_x + 16,
                9,
                text=str(row.get("algorithm", "--") or "--"),
                anchor="w",
                fill="#627587",
            )
