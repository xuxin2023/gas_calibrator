from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..i18n import display_phase, display_route, t


class RouteProgressTimeline(ttk.Frame):
    """Route progress timeline for H2O/CO2/finalize stages."""

    DEFAULT_STEPS = (
        t("widgets.route_progress.step_h2o"),
        t("widgets.route_progress.step_co2"),
        t("widgets.route_progress.step_finalize"),
    )

    def __init__(self, parent: tk.Misc, *, title: str | None = None) -> None:
        super().__init__(parent, style="Card.TFrame", padding=8)
        self._snapshot: dict[str, object] = {}
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        ttk.Label(self, text=title or t("widgets.route_progress.title"), style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.canvas = tk.Canvas(self, height=90, highlightthickness=0, background="#ffffff")
        self.canvas.grid(row=1, column=0, sticky="nsew")
        self.detail_var = tk.StringVar(value=t("widgets.route_progress.no_active"))
        ttk.Label(self, textvariable=self.detail_var, style="Muted.TLabel").grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.canvas.bind("<Configure>", lambda _event: self._redraw())

    def render(self, snapshot: dict[str, object]) -> None:
        self._snapshot = dict(snapshot or {})
        active = str(
            self._snapshot.get("route_display")
            or display_route(self._snapshot.get("route", "--"))
            or "--"
        )
        phase = str(
            self._snapshot.get("route_phase_display")
            or display_phase(self._snapshot.get("route_phase", "--"))
            or "--"
        )
        completed = int(self._snapshot.get("points_completed", 0) or 0)
        total = int(self._snapshot.get("points_total", 0) or 0)
        self.detail_var.set(
            t(
                "widgets.route_progress.detail",
                route=active,
                phase=phase,
                completed=completed,
                total=total,
            )
        )
        self._redraw()

    def _redraw(self) -> None:
        self.canvas.delete("all")
        width = max(1, int(self.canvas.winfo_width() or 420))
        height = max(1, int(self.canvas.winfo_height() or 90))
        self.canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        steps = [self._display_step(item) for item in list(self._snapshot.get("steps", self.DEFAULT_STEPS) or self.DEFAULT_STEPS)]
        if not steps:
            self.canvas.create_text(width / 2, height / 2, text=t("widgets.route_progress.no_steps"), fill="#627587")
            return

        route = str(self._snapshot.get("route", "") or "").lower()
        active_index = 0
        if route.startswith("co2"):
            active_index = 1
        elif route.startswith("final"):
            active_index = 2

        line_y = height / 2
        start_x = 48
        step_gap = (width - 96) / max(1, len(steps) - 1)
        self.canvas.create_line(start_x, line_y, width - 48, line_y, fill="#d5dee8", width=3)
        for index, step in enumerate(steps):
            x = start_x + index * step_gap
            fill = "#dff5f4" if index < active_index else "#f7fafc"
            outline = "#0e7c86" if index <= active_index else "#d5dee8"
            if index < active_index:
                fill = "#0e7c86"
            self.canvas.create_oval(x - 14, line_y - 14, x + 14, line_y + 14, outline=outline, fill=fill, width=2)
            self.canvas.create_text(x, line_y + 28, text=str(step), fill="#0f2338")

    @staticmethod
    def _display_step(value: object) -> str:
        text = str(value or "").strip().lower()
        if text in {"h2o", "step_h2o", "water"}:
            return t("widgets.route_progress.step_h2o")
        if text in {"co2", "step_co2", "gas"}:
            return t("widgets.route_progress.step_co2")
        if text in {"finalize", "final", "step_finalize"}:
            return t("widgets.route_progress.step_finalize")
        return str(value or "")
