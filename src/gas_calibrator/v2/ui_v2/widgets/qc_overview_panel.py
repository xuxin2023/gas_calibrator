from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..i18n import t
from .metric_card import MetricCard


class QCOverviewPanel(ttk.Frame):
    """Compact QC summary with metric cards and a simple validity bar."""

    def __init__(self, parent: tk.Misc, *, title: str | None = None) -> None:
        super().__init__(parent, style="Card.TFrame", padding=8)
        self.rowconfigure(2, weight=1)
        self.columnconfigure(0, weight=1)
        ttk.Label(self, text=title or t("widgets.qc_overview.title"), style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))

        cards = ttk.Frame(self, style="Card.TFrame")
        cards.grid(row=1, column=0, sticky="ew")
        for column in range(4):
            cards.columnconfigure(column, weight=1)
        self.score_card = MetricCard(cards, title=t("widgets.qc_overview.score"))
        self.score_card.grid(row=0, column=0, sticky="nsew", padx=4)
        self.valid_card = MetricCard(cards, title=t("widgets.qc_overview.valid"))
        self.valid_card.grid(row=0, column=1, sticky="nsew", padx=4)
        self.invalid_card = MetricCard(cards, title=t("widgets.qc_overview.invalid"))
        self.invalid_card.grid(row=0, column=2, sticky="nsew", padx=4)
        self.total_card = MetricCard(cards, title=t("widgets.qc_overview.total"))
        self.total_card.grid(row=0, column=3, sticky="nsew", padx=4)

        self.canvas = tk.Canvas(self, height=28, highlightthickness=0, background="#ffffff")
        self.canvas.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        self.summary_var = tk.StringVar(value=t("widgets.qc_overview.empty"))
        ttk.Label(self, textvariable=self.summary_var, style="Muted.TLabel").grid(row=3, column=0, sticky="w", pady=(6, 0))

    def render(self, snapshot: dict[str, object]) -> None:
        score = float(snapshot.get("score", 0.0) or 0.0)
        valid = int(snapshot.get("valid_points", 0) or 0)
        invalid = int(snapshot.get("invalid_points", 0) or 0)
        total = int(snapshot.get("total_points", 0) or 0)
        grade = str(snapshot.get("grade", "--") or "--")

        self.score_card.set_value(f"{score:.2f}")
        self.score_card.set_note(t("widgets.qc_overview.grade", grade=grade))
        self.valid_card.set_value(str(valid))
        self.invalid_card.set_value(str(invalid))
        self.total_card.set_value(str(total))
        self.summary_var.set(t("widgets.qc_overview.summary", valid=valid, total=total, invalid=invalid))
        self._draw_bar(valid=valid, invalid=invalid, total=total)

    def _draw_bar(self, *, valid: int, invalid: int, total: int) -> None:
        self.canvas.delete("all")
        width = max(1, int(self.canvas.winfo_width() or 320))
        height = max(1, int(self.canvas.winfo_height() or 28))
        self.canvas.create_rectangle(0, 0, width, height, outline="", fill="#f7fafc")
        if total <= 0:
            self.canvas.create_text(width / 2, height / 2, text=t("widgets.qc_overview.no_totals"), fill="#627587")
            return
        valid_width = width * max(0.0, min(1.0, valid / total))
        self.canvas.create_rectangle(0, 0, valid_width, height, outline="", fill="#15803d")
        self.canvas.create_rectangle(valid_width, 0, width, height, outline="", fill="#dc2626")
        self.canvas.create_text(
            width / 2,
            height / 2,
            text=t("widgets.qc_overview.bar_valid", valid=valid, total=total),
            fill="#ffffff",
        )
