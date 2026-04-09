from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import t
from ..widgets.ai_summary_panel import AISummaryPanel
from ..widgets.residual_chart import ResidualChart
from ..widgets.review_center_panel import ReviewCenterPanel
from ..widgets.scrollable_page_frame import ScrollablePageFrame


class ResultsPage(ttk.Frame):
    """Result and artifact summary page."""

    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.page_scaffold = ScrollablePageFrame(self, padding=12)
        self.page_scaffold.grid(row=0, column=0, sticky="nsew")
        body = self.page_scaffold.content
        body.columnconfigure(0, weight=3)
        body.columnconfigure(1, weight=3)
        body.columnconfigure(2, weight=4)
        body.rowconfigure(0, weight=1)
        body.rowconfigure(1, weight=1)
        body.rowconfigure(2, weight=1)
        body.rowconfigure(3, weight=1)

        self.overview = self._section(body, 0, 0, t("pages.results.overview"))
        self.algorithm = self._section(body, 0, 1, t("pages.results.algorithm_compare"))
        self.result_summary = self._section(body, 1, 0, t("pages.results.result_summary"))
        self.coefficient_summary = self._section(body, 1, 1, t("pages.results.coefficient_summary"))
        self.review_center = ReviewCenterPanel(body, title=t("pages.results.review_center"), compact=True)
        self.review_center.grid(row=0, column=2, rowspan=2, sticky="nsew", padx=(6, 0), pady=(0, 12))
        self.residual_chart = ResidualChart(body, title=t("pages.results.residual_distribution"), height=160)
        self.residual_chart.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=(0, 6), pady=(0, 0))
        self.ai_summary = AISummaryPanel(body, title=t("pages.results.ai_summary"))
        self.ai_summary.grid(row=2, column=2, sticky="nsew", padx=(6, 0), pady=(0, 0))
        self.qc_summary = self._section(
            body,
            3,
            0,
            t("pages.results.qc_summary", default="质控审阅摘要"),
            columnspan=2,
            height=8,
        )
        self.measurement_core_summary = self._section(
            body,
            3,
            2,
            t("pages.results.measurement_core", default="measurement-core 摘要"),
            height=8,
        )

    def _section(
        self,
        parent: tk.Misc,
        row: int,
        column: int,
        title: str,
        *,
        columnspan: int = 1,
        height: int = 6,
    ) -> tk.Text:
        frame = ttk.Frame(parent, style="Card.TFrame", padding=8)
        frame.grid(
            row=row,
            column=column,
            columnspan=columnspan,
            sticky="nsew",
            padx=(0, 6) if column == 0 else (6, 0),
            pady=(0, 12),
        )
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)
        ttk.Label(frame, text=title, style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        widget = tk.Text(frame, height=height, wrap="word")
        widget.grid(row=1, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=widget.yview)
        scrollbar.grid(row=1, column=1, sticky="ns", padx=(6, 0))
        widget.configure(yscrollcommand=scrollbar.set, state="disabled")
        return widget

    def render(self, snapshot: dict[str, Any]) -> None:
        self._set_text(self.overview, str(snapshot.get("overview_text", "") or t("pages.results.no_overview")))
        self._set_text(self.algorithm, str(snapshot.get("algorithm_compare_text", "") or t("pages.results.no_algorithm_summary")))
        self._set_text(self.result_summary, str(snapshot.get("result_summary_text", "") or t("pages.results.no_result_summary")))
        self._set_text(self.coefficient_summary, str(snapshot.get("coefficient_summary_text", "") or t("pages.results.no_coefficient_summary")))
        self._set_text(self.qc_summary, str(snapshot.get("qc_summary_text", "") or t("pages.results.no_qc_summary", default="暂无质控审阅摘要")))
        self._set_text(
            self.measurement_core_summary,
            str(
                snapshot.get("measurement_core_summary_text", "")
                or t("pages.results.no_measurement_core_summary", default="暂无 measurement-core 摘要")
            ),
        )
        self.review_center.render(dict(snapshot.get("review_center", {}) or {}))
        self.ai_summary.set_text(str(snapshot.get("ai_summary_text", "") or t("pages.results.no_ai_summary")))
        self.residual_chart.render(dict(snapshot.get("residuals", {}) or {}))
        self.page_scaffold._update_scroll_region()

    @staticmethod
    def _set_text(widget: tk.Text, value: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", value.strip() + "\n")
        widget.configure(state="disabled")
