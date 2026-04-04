from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import display_bool, t
from ..widgets.algorithm_compare_table import AlgorithmCompareTable
from ..widgets.metric_card import MetricCard
from ..widgets.scrollable_page_frame import ScrollablePageFrame
from ..widgets.winner_badge import WinnerBadge


class AlgorithmsPage(ttk.Frame):
    """Algorithm comparison/config page."""

    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.page_scaffold = ScrollablePageFrame(self, padding=12)
        self.page_scaffold.grid(row=0, column=0, sticky="nsew")
        body = self.page_scaffold.content
        body.columnconfigure(0, weight=1)
        body.rowconfigure(2, weight=1)

        summary = ttk.Frame(body, style="Card.TFrame")
        summary.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        for column in range(4):
            summary.columnconfigure(column, weight=1)
        self.default_card = MetricCard(summary, title=t("pages.algorithms.default_algorithm"))
        self.default_card.grid(row=0, column=0, sticky="nsew", padx=4)
        self.candidates_card = MetricCard(summary, title=t("pages.algorithms.candidates"))
        self.candidates_card.grid(row=0, column=1, sticky="nsew", padx=4)
        self.model_card = MetricCard(summary, title=t("pages.algorithms.coefficient_model"))
        self.model_card.grid(row=0, column=2, sticky="nsew", padx=4)
        self.autoselect_card = MetricCard(summary, title=t("pages.algorithms.auto_select"))
        self.autoselect_card.grid(row=0, column=3, sticky="nsew", padx=4)
        self.winner_badge = WinnerBadge(body)
        self.winner_badge.grid(row=1, column=0, sticky="ew", pady=(0, 12))
        self.table = AlgorithmCompareTable(body)
        self.table.grid(row=2, column=0, sticky="nsew")

    def render(self, snapshot: dict[str, Any]) -> None:
        self.default_card.set_value(str(snapshot.get("default_algorithm", "--") or "--"))
        self.candidates_card.set_value(str(snapshot.get("candidate_count", 0) or 0))
        self.candidates_card.set_note(", ".join(snapshot.get("candidates", []) or []))
        self.model_card.set_value(str(snapshot.get("coefficient_model", "--") or "--"))
        self.autoselect_card.set_value(display_bool(bool(snapshot.get("auto_select", False))))
        self.winner_badge.render(dict(snapshot.get("winner", {}) or {}))
        self.table.render(list(snapshot.get("rows", []) or []))
        self.page_scaffold._update_scroll_region()
