from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import t
from ..widgets.analyzer_health_panel import AnalyzerHealthPanel
from ..widgets.device_workbench import DeviceWorkbench
from ..widgets.device_status_table import DeviceStatusTable
from ..widgets.metric_card import MetricCard
from ..widgets.scrollable_page_frame import ScrollablePageFrame


class DevicesPage(ttk.Frame):
    """Device status overview and simulation-only workbench page."""

    def __init__(self, parent: tk.Misc, *, facade: Any | None = None) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.page_scaffold = ScrollablePageFrame(self, padding=12)
        self.page_scaffold.grid(row=0, column=0, sticky="nsew")
        body = self.page_scaffold.content
        body.columnconfigure(0, weight=1)
        body.rowconfigure(1, weight=1)
        body.rowconfigure(2, weight=1)

        summary = ttk.Frame(body, style="Card.TFrame")
        summary.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        for column in range(4):
            summary.columnconfigure(column, weight=1)
        self.enabled_card = MetricCard(summary, title=t("pages.devices.enabled_devices"))
        self.enabled_card.grid(row=0, column=0, sticky="nsew", padx=4)
        self.listed_card = MetricCard(summary, title=t("pages.devices.listed_rows"))
        self.listed_card.grid(row=0, column=1, sticky="nsew", padx=4)
        self.disabled_card = MetricCard(summary, title=t("pages.devices.disabled_analyzers"))
        self.disabled_card.grid(row=0, column=2, sticky="nsew", padx=4)
        self.warning_card = MetricCard(summary, title=t("pages.devices.warnings_errors"))
        self.warning_card.grid(row=0, column=3, sticky="nsew", padx=4)

        upper = ttk.Frame(body, style="Card.TFrame")
        upper.grid(row=1, column=0, sticky="nsew", pady=(0, 12))
        upper.columnconfigure(0, weight=3)
        upper.columnconfigure(1, weight=2)
        self.table = DeviceStatusTable(upper)
        self.table.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        self.health_panel = AnalyzerHealthPanel(upper)
        self.health_panel.grid(row=0, column=1, sticky="nsew", padx=(6, 0))

        self.workbench = DeviceWorkbench(body, facade=facade)
        self.workbench.grid(row=2, column=0, sticky="nsew")

    def render(self, snapshot: dict[str, Any]) -> None:
        rows = list(snapshot.get("rows", []) or [])
        disabled = list(snapshot.get("disabled_analyzers", []) or [])
        self.enabled_card.set_value(str(snapshot.get("enabled_count", 0) or 0))
        self.listed_card.set_value(str(len(rows)))
        self.disabled_card.set_value(str(len(disabled)))
        self.disabled_card.set_note(", ".join(disabled) if disabled else t("common.none"))
        self.warning_card.set_value(f"{snapshot.get('warning_count', 0) or 0} / {snapshot.get('error_count', 0) or 0}")
        self.table.render(rows)
        self.health_panel.render(dict(snapshot.get("analyzer_health", {}) or {}))
        self.workbench.render(dict(snapshot.get("workbench", {}) or {}))
        self.page_scaffold._update_scroll_region()
