from __future__ import annotations

import math
import tkinter as tk
from typing import Any, Callable

from ..page_i18n import translator_for


class VisitorShowcasePage(tk.Frame):
    """Read-only exhibition surface; it contains no device-control actions."""

    BG = "#06111F"
    PANEL = "#0B1C2E"
    PANEL_ALT = "#0E263A"
    LINE = "#1B4258"
    TEXT = "#F5FBFF"
    MUTED = "#91AABD"
    CYAN = "#35D7E6"
    TEAL = "#30E2A1"
    BLUE = "#5B8CFF"
    AMBER = "#F5C76B"

    def __init__(
        self,
        parent: tk.Misc,
        *,
        on_enter_presentation: Callable[[], None],
        on_exit_presentation: Callable[[], None],
        locale: str = "zh_CN",
        translate: Callable[..., str] | None = None,
    ) -> None:
        super().__init__(parent, bg=self.BG, highlightthickness=0)
        self._t = translate or translator_for(locale)
        self.on_enter_presentation = on_enter_presentation
        self.on_exit_presentation = on_exit_presentation
        self.snapshot: dict[str, Any] = {}
        self.presentation_active = False
        self.canvas = tk.Canvas(self, bg=self.BG, highlightthickness=0, borderwidth=0)
        self.canvas.pack(fill="both", expand=True)
        self.canvas.bind("<Configure>", self._redraw, add="+")
        self.presentation_button = tk.Button(
            self,
            text=self._t("pages.visitor_showcase.actions.enter"),
            command=self._toggle_presentation,
            bg=self.CYAN,
            fg="#03212B",
            activebackground="#78EDF5",
            activeforeground="#03212B",
            borderwidth=0,
            relief="flat",
            font=("Segoe UI", 11, "bold"),
            padx=18,
            pady=8,
            cursor="hand2",
        )
        self.presentation_button.place(relx=0.968, rely=0.052, anchor="ne")

    def render(self, snapshot: dict[str, Any]) -> None:
        self.snapshot = snapshot if snapshot is not None else {}
        self._redraw()

    def set_presentation_active(self, active: bool) -> None:
        self.presentation_active = bool(active)
        self.presentation_button.configure(
            text=self._t(
                "pages.visitor_showcase.actions.exit"
                if self.presentation_active
                else "pages.visitor_showcase.actions.enter"
            )
        )
        self._redraw()

    def _toggle_presentation(self) -> None:
        if self.presentation_active:
            self.on_exit_presentation()
        else:
            self.on_enter_presentation()

    def _redraw(self, _event: tk.Event[tk.Canvas] | None = None) -> None:
        width = max(1180, int(self.canvas.winfo_width() or 0))
        height = max(720, int(self.canvas.winfo_height() or 0))
        c = self.canvas
        c.delete("all")
        c.create_rectangle(0, 0, width, height, fill=self.BG, outline="")
        c.create_oval(
            width * 0.48,
            -height * 0.45,
            width * 1.1,
            height * 0.55,
            fill="#0A2B44",
            outline="",
        )
        c.create_oval(
            -width * 0.24,
            height * 0.5,
            width * 0.36,
            height * 1.3,
            fill="#08263A",
            outline="",
        )

        margin = max(34, int(width * 0.026))
        header_y = max(34, int(height * 0.045))
        c.create_text(
            margin,
            header_y,
            anchor="nw",
            text=self._t("pages.visitor_showcase.eyebrow"),
            fill=self.CYAN,
            font=("Segoe UI", 11, "bold"),
        )
        c.create_text(
            margin,
            header_y + 28,
            anchor="nw",
            text=self._t("pages.visitor_showcase.title"),
            fill=self.TEXT,
            font=("Segoe UI Semibold", max(26, int(height * 0.038)), "bold"),
        )
        c.create_text(
            margin,
            header_y + 88,
            anchor="nw",
            text=self._t("pages.visitor_showcase.subtitle"),
            fill=self.MUTED,
            font=("Segoe UI", max(10, int(height * 0.014))),
        )
        self._pill(
            margin,
            header_y + 112,
            self._t("pages.visitor_showcase.badge.simulated"),
            self.CYAN,
        )
        self._pill(
            margin + 190,
            header_y + 112,
            self._t("pages.visitor_showcase.badge.read_only"),
            self.TEAL,
        )
        self._pill(
            margin + 360,
            header_y + 112,
            self._t("pages.visitor_showcase.badge.not_acceptance"),
            self.AMBER,
        )

        cards_y = header_y + 158
        gap = 14
        card_width = (width - margin * 2 - gap * 3) / 4
        point_counts = dict(self.snapshot.get("point_counts") or {})
        co2_points = int(point_counts.get("co2", 45) or 45)
        h2o_points = int(point_counts.get("h2o", 13) or 13)
        devices = dict(self.snapshot.get("devices") or {})
        initialization = dict(devices.get("initialization_contract") or {})
        powered = int(devices.get("powered_count") or 0)
        configured = int(devices.get("configured_channel_count") or 0)
        upload_rate = initialization.get("upload_rate_hz")
        average1 = initialization.get("average1")
        average2 = initialization.get("average2")
        rate_text = (
            f"{upload_rate} Hz（校准）"
            if upload_rate not in (None, "")
            else "-- Hz（校准）"
        )
        average_text = (
            f"A1={average1 if average1 not in (None, '') else '--'} / "
            f"A2={average2 if average2 not in (None, '') else '--'}"
        )
        metrics = (
            (
                f"{co2_points} / {h2o_points} + 干气锚",
                "pages.visitor_showcase.metric.calibration_points",
                self.CYAN,
            ),
            (
                f"{powered} / {configured}",
                "pages.visitor_showcase.metric.analyzers",
                self.TEAL,
            ),
            (rate_text, "pages.visitor_showcase.metric.sample_rate", self.BLUE),
            (average_text, "pages.visitor_showcase.metric.filter", self.AMBER),
        )
        for index, (value, key, color) in enumerate(metrics):
            x = margin + index * (card_width + gap)
            self._metric_card(
                x,
                cards_y,
                card_width,
                90,
                value,
                self._t(key),
                color,
            )

        process_y = cards_y + 112
        process_h = 128
        self._panel(margin, process_y, width - margin * 2, process_h)
        c.create_text(
            margin + 18,
            process_y + 17,
            anchor="nw",
            text=self._t("pages.visitor_showcase.process.title"),
            fill=self.TEXT,
            font=("Segoe UI Semibold", 13, "bold"),
        )
        process_steps = (
            ("01", "pages.visitor_showcase.process.reference"),
            ("02", "pages.visitor_showcase.process.temperature"),
            ("03", "pages.visitor_showcase.process.pressure"),
            ("04", "pages.visitor_showcase.process.sampling"),
            ("05", "pages.visitor_showcase.process.qc"),
            ("06", "pages.visitor_showcase.process.archive"),
        )
        usable_w = width - margin * 2 - 36
        start_x = margin + 18
        step_gap = usable_w / len(process_steps)
        line_y = process_y + 79
        c.create_line(
            start_x + 16,
            line_y,
            start_x + usable_w - step_gap + 16,
            line_y,
            fill=self.LINE,
            width=3,
        )
        for index, (number, key) in enumerate(process_steps):
            x = start_x + index * step_gap
            c.create_oval(
                x,
                line_y - 15,
                x + 30,
                line_y + 15,
                fill=self.PANEL_ALT,
                outline=self.CYAN,
                width=2,
            )
            c.create_text(
                x + 15,
                line_y,
                text=number,
                fill=self.CYAN,
                font=("Segoe UI", 9, "bold"),
            )
            c.create_text(
                x + 15,
                line_y + 33,
                text=self._t(key),
                fill=self.MUTED,
                font=("Segoe UI", 10),
                anchor="n",
            )

        lower_y = process_y + process_h + 14
        lower_h = max(236, height - lower_y - 58)
        left_w = (width - margin * 2 - gap) * 0.63
        right_w = width - margin * 2 - gap - left_w
        self._panel(margin, lower_y, left_w, lower_h)
        self._panel(margin + left_w + gap, lower_y, right_w, lower_h)
        self._draw_simulated_traces(margin, lower_y, left_w, lower_h)
        self._draw_traceability(margin + left_w + gap, lower_y, right_w, lower_h)

        c.create_text(
            margin,
            height - 26,
            anchor="w",
            text=self._t("pages.visitor_showcase.footer"),
            fill=self.MUTED,
            font=("Segoe UI", 9),
        )
        c.create_text(
            width - margin,
            height - 26,
            anchor="e",
            text=self._t("pages.visitor_showcase.footer_boundary"),
            fill=self.AMBER,
            font=("Segoe UI", 9, "bold"),
        )

    def _metric_card(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        value: str,
        label: str,
        color: str,
    ) -> None:
        self._panel(x, y, width, height)
        self.canvas.create_rectangle(x, y, x + 4, y + height, fill=color, outline="")
        self.canvas.create_text(
            x + 20,
            y + 15,
            anchor="nw",
            text=value,
            fill=self.TEXT,
            font=("Segoe UI Semibold", 22, "bold"),
        )
        self.canvas.create_text(
            x + 20,
            y + 58,
            anchor="nw",
            text=label,
            fill=self.MUTED,
            font=("Segoe UI", 10),
        )

    def _draw_simulated_traces(
        self, x: float, y: float, width: float, height: float
    ) -> None:
        c = self.canvas
        c.create_text(
            x + 18,
            y + 16,
            anchor="nw",
            text=self._t("pages.visitor_showcase.chart.title"),
            fill=self.TEXT,
            font=("Segoe UI Semibold", 13, "bold"),
        )
        c.create_text(
            x + width - 18,
            y + 18,
            anchor="ne",
            text=self._t("pages.visitor_showcase.chart.caption"),
            fill=self.CYAN,
            font=("Segoe UI", 9, "bold"),
        )
        plot_x0, plot_x1 = x + 48, x + width - 22
        plot_y0, plot_y1 = y + 60, y + height - 36
        for i in range(5):
            grid_y = plot_y0 + (plot_y1 - plot_y0) * i / 4
            c.create_line(plot_x0, grid_y, plot_x1, grid_y, fill=self.LINE)
        colors = (self.CYAN, self.TEAL, self.BLUE, "#A78BFA", "#F472B6", self.AMBER)
        for channel, color in enumerate(colors):
            points: list[float] = []
            for index in range(80):
                fraction = index / 79
                response = 0.10 + 0.78 * (
                    1.0 - math.exp(-fraction * (4.3 + channel * 0.13))
                )
                ripple = math.sin(fraction * 18 + channel) * (0.006 + channel * 0.0008)
                px = plot_x0 + fraction * (plot_x1 - plot_x0)
                py = plot_y1 - (response + ripple - channel * 0.012) * (
                    plot_y1 - plot_y0
                )
                points.extend((px, py))
            c.create_line(*points, fill=color, width=2, smooth=True)
        c.create_text(
            plot_x0,
            plot_y1 + 10,
            anchor="nw",
            text=self._t("pages.visitor_showcase.chart.axis_start"),
            fill=self.MUTED,
            font=("Segoe UI", 8),
        )
        c.create_text(
            plot_x1,
            plot_y1 + 10,
            anchor="ne",
            text=self._t("pages.visitor_showcase.chart.axis_end"),
            fill=self.MUTED,
            font=("Segoe UI", 8),
        )

    def _draw_traceability(
        self, x: float, y: float, width: float, height: float
    ) -> None:
        c = self.canvas
        c.create_text(
            x + 18,
            y + 16,
            anchor="nw",
            text=self._t("pages.visitor_showcase.traceability.title"),
            fill=self.TEXT,
            font=("Segoe UI Semibold", 13, "bold"),
        )
        items = (
            ("01", "pages.visitor_showcase.traceability.certificate", self.CYAN),
            ("02", "pages.visitor_showcase.traceability.environment", self.TEAL),
            ("03", "pages.visitor_showcase.traceability.frames", self.BLUE),
            ("04", "pages.visitor_showcase.traceability.qc", "#A78BFA"),
            ("05", "pages.visitor_showcase.traceability.artifacts", self.AMBER),
        )
        top = y + 58
        item_gap = max(34, (height - 88) / len(items))
        for index, (number, key, color) in enumerate(items):
            iy = top + index * item_gap
            c.create_oval(x + 18, iy, x + 42, iy + 24, fill=color, outline="")
            c.create_text(
                x + 30, iy + 12, text=number, fill=self.BG, font=("Segoe UI", 8, "bold")
            )
            c.create_text(
                x + 54,
                iy + 12,
                anchor="w",
                text=self._t(key),
                fill=self.TEXT,
                font=("Segoe UI", 10),
            )
            if index < len(items) - 1:
                c.create_line(
                    x + 30, iy + 25, x + 30, iy + item_gap, fill=self.LINE, width=2
                )

    def _panel(self, x: float, y: float, width: float, height: float) -> None:
        self.canvas.create_rectangle(
            x,
            y,
            x + width,
            y + height,
            fill=self.PANEL,
            outline=self.LINE,
            width=1,
        )

    def _pill(self, x: float, y: float, text: str, color: str) -> None:
        width = max(140, len(text) * 13 + 24)
        self.canvas.create_rectangle(
            x,
            y,
            x + width,
            y + 28,
            fill=self.PANEL_ALT,
            outline=color,
            width=1,
        )
        self.canvas.create_text(
            x + 12,
            y + 14,
            anchor="w",
            text=text,
            fill=color,
            font=("Segoe UI", 9, "bold"),
        )


__all__ = ["VisitorShowcasePage"]
