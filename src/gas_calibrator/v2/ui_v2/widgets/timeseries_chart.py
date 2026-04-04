from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..i18n import t


class TimeSeriesChart(ttk.Frame):
    """Lightweight rolling line chart backed by a Tk Canvas."""

    COLORS = ("#0e7c86", "#dc2626", "#15803d", "#f59e0b")

    def __init__(
        self,
        parent: tk.Misc,
        *,
        title: str | None = None,
        max_points: int = 60,
        height: int = 180,
    ) -> None:
        super().__init__(parent, style="Card.TFrame", padding=8)
        self.max_points = max(8, int(max_points))
        self._series: dict[str, list[float]] = {}
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        ttk.Label(self, text=title or t("widgets.timeseries.title"), style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.canvas = tk.Canvas(self, height=height, highlightthickness=0, background="#ffffff")
        self.canvas.grid(row=1, column=0, sticky="nsew")
        self.canvas.bind("<Configure>", lambda _event: self._redraw())

    def set_series(self, series: dict[str, list[float]] | list[float]) -> None:
        if isinstance(series, dict):
            normalized = {
                str(name): [float(item) for item in list(values or [])][-self.max_points :]
                for name, values in series.items()
            }
        else:
            normalized = {"value": [float(item) for item in list(series or [])][-self.max_points :]}
        self._series = normalized
        self._redraw()

    def append(self, value: float | dict[str, float]) -> None:
        if isinstance(value, dict):
            for name, item in value.items():
                bucket = self._series.setdefault(str(name), [])
                bucket.append(float(item))
                if len(bucket) > self.max_points:
                    del bucket[:-self.max_points]
        else:
            bucket = self._series.setdefault("value", [])
            bucket.append(float(value))
            if len(bucket) > self.max_points:
                del bucket[:-self.max_points]
        self._redraw()

    def _redraw(self) -> None:
        self.canvas.delete("all")
        width = max(1, int(self.canvas.winfo_width()))
        height = max(1, int(self.canvas.winfo_height()))
        self.canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        if not self._series:
            self.canvas.create_text(width / 2, height / 2, text=t("widgets.timeseries.no_data"), fill="#627587")
            return

        values = [item for series in self._series.values() for item in series]
        if not values:
            self.canvas.create_text(width / 2, height / 2, text=t("widgets.timeseries.no_data"), fill="#627587")
            return

        min_value = min(values)
        max_value = max(values)
        if min_value == max_value:
            min_value -= 1.0
            max_value += 1.0

        margin_left = 16
        margin_right = 12
        margin_top = 16
        margin_bottom = 18
        plot_width = max(1, width - margin_left - margin_right)
        plot_height = max(1, height - margin_top - margin_bottom)

        self.canvas.create_line(margin_left, height - margin_bottom, width - margin_right, height - margin_bottom, fill="#d5dee8")
        self.canvas.create_line(margin_left, margin_top, margin_left, height - margin_bottom, fill="#d5dee8")

        for index, (name, series) in enumerate(self._series.items()):
            if not series:
                continue
            points: list[float] = []
            count = max(1, len(series) - 1)
            for offset, value in enumerate(series):
                x = margin_left + (plot_width * offset / count)
                y = margin_top + (max_value - value) * plot_height / (max_value - min_value)
                points.extend((x, y))
            color = self.COLORS[index % len(self.COLORS)]
            if len(points) >= 4:
                self.canvas.create_line(*points, fill=color, width=2, smooth=True)
            x = margin_left + 8 + (index * 140)
            self.canvas.create_rectangle(x, 4, x + 10, 14, outline="", fill=color)
            self.canvas.create_text(x + 16, 9, text=name, anchor="w", fill="#627587")
