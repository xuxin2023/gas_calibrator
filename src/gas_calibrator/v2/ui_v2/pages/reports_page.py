from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import display_evidence_source, t
from ..review_center_presenter import build_artifact_scope_view
from ..widgets.ai_summary_panel import AISummaryPanel
from ..widgets.artifact_list_panel import ArtifactListPanel
from ..widgets.export_bar import ExportBar
from ..widgets.metric_card import MetricCard
from ..widgets.review_center_panel import ReviewCenterPanel
from ..widgets.scrollable_page_frame import ScrollablePageFrame


class ReportsPage(ttk.Frame):
    """Artifact/report page."""

    def __init__(self, parent: tk.Misc, *, exporter: Any | None = None) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.exporter = exporter
        self._artifact_rows: list[dict[str, Any]] = []
        self._artifact_scope_snapshot: dict[str, Any] = {"scope": "all"}
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.page_scaffold = ScrollablePageFrame(self, padding=12)
        self.page_scaffold.grid(row=0, column=0, sticky="nsew")
        body = self.page_scaffold.content
        body.columnconfigure(0, weight=3)
        body.columnconfigure(1, weight=2)
        body.rowconfigure(1, weight=1)

        summary = ttk.Frame(body, style="Card.TFrame")
        summary.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 12))
        for column in range(3):
            summary.columnconfigure(column, weight=1)
        self.run_dir_card = MetricCard(summary, title=t("pages.reports.run_dir"))
        self.run_dir_card.grid(row=0, column=0, sticky="nsew", padx=4)
        self.artifact_count_card = MetricCard(summary, title=t("pages.reports.artifacts"))
        self.artifact_count_card.grid(row=0, column=1, sticky="nsew", padx=4)
        self.present_count_card = MetricCard(summary, title=t("pages.reports.present"))
        self.present_count_card.grid(row=0, column=2, sticky="nsew", padx=4)

        artifacts_frame = ttk.Frame(body, style="Card.TFrame", padding=8)
        artifacts_frame.grid(row=1, column=0, sticky="nsew", padx=(0, 6), pady=(0, 12))
        artifacts_frame.columnconfigure(0, weight=1)
        artifacts_frame.rowconfigure(2, weight=1)
        ttk.Label(
            artifacts_frame,
            text=t("pages.reports.artifact_list_title"),
            style="Section.TLabel",
        ).grid(row=0, column=0, sticky="w", pady=(0, 6))
        artifact_scope_bar = ttk.Frame(artifacts_frame, style="Card.TFrame")
        artifact_scope_bar.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        artifact_scope_bar.columnconfigure(0, weight=1)
        self.artifact_scope_var = tk.StringVar(
            value=t(
                "pages.reports.artifact_scope.summary_all",
                visible=0,
                present=0,
                total=0,
                external=0,
                missing=0,
                catalog_present=0,
                catalog_total=0,
            )
        )
        self.artifact_scope_notice_var = tk.StringVar(value=t("pages.reports.artifact_scope.disclaimer"))
        ttk.Label(artifact_scope_bar, textvariable=self.artifact_scope_var, style="Muted.TLabel").grid(
            row=0,
            column=0,
            sticky="w",
            padx=(0, 8),
        )
        self.clear_artifact_scope_button = ttk.Button(
            artifact_scope_bar,
            text=t("pages.reports.artifact_scope.clear"),
            command=self._clear_artifact_scope,
            state="disabled",
        )
        self.clear_artifact_scope_button.grid(row=0, column=1, sticky="e")
        self.artifacts = ArtifactListPanel(artifacts_frame)
        self.artifacts.grid(row=2, column=0, sticky="nsew")
        ttk.Label(
            artifacts_frame,
            textvariable=self.artifact_scope_notice_var,
            style="Muted.TLabel",
            wraplength=760,
            justify="left",
        ).grid(row=3, column=0, sticky="ew", pady=(6, 0))

        right = ttk.Frame(body, style="Card.TFrame")
        right.grid(row=1, column=1, sticky="nsew", padx=(6, 0), pady=(0, 12))
        right.columnconfigure(0, weight=1)
        right.rowconfigure(2, weight=1)
        right.rowconfigure(3, weight=1)
        right.rowconfigure(4, weight=1)
        right.rowconfigure(5, weight=1)
        self.export_bar = ExportBar(
            right,
            on_export_json=self._export_json,
            on_export_csv=self._export_csv,
            on_export_all=self._export_all,
            on_export_review_manifest=self._export_review_manifest,
        )
        self.export_bar.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        self.export_scope_notice_var = tk.StringVar(value="")
        ttk.Label(
            right,
            textvariable=self.export_scope_notice_var,
            style="Muted.TLabel",
            wraplength=460,
            justify="left",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 12))
        self.review_center = ReviewCenterPanel(
            right,
            title=t("pages.reports.review_center"),
            on_selection_changed=self._on_review_selection_changed,
        )
        self.review_center.grid(row=2, column=0, sticky="nsew", pady=(0, 12))
        self.result_summary = self._text_panel(
            right,
            row=3,
            title=t("pages.reports.result_summary", default="运行与治理摘要"),
        )
        self.qc_summary = self._text_panel(
            right,
            row=4,
            title=t("pages.reports.qc_summary", default="质控审阅摘要"),
        )
        self.ai_summary = AISummaryPanel(right, title=t("pages.reports.ai_report_summary"))
        self.ai_summary.grid(row=5, column=0, sticky="nsew")

    def render(self, snapshot: dict[str, Any]) -> None:
        rows = list(snapshot.get("files", []) or [])
        self._artifact_rows = [dict(item) for item in rows]
        self.run_dir_card.set_value(str(snapshot.get("run_dir", "--") or "--"))
        self.review_center.render(dict(snapshot.get("review_center", {}) or {}))
        self._apply_artifact_scope(self.review_center.get_selection_snapshot())
        if not str(snapshot.get("result_summary_text", "") or "").strip():
            snapshot = dict(snapshot)
            snapshot["result_summary_text"] = self._build_result_summary_fallback(snapshot)
        result_summary_text = str(
            snapshot.get("result_summary_text", "")
            or snapshot.get("review_digest_text", "")
            or t("pages.reports.no_result_summary", default="暂无运行与治理摘要")
        )
        self._set_text(self.result_summary, result_summary_text)
        self._set_text(self.qc_summary, str(snapshot.get("qc_summary_text", "") or t("pages.reports.no_qc_summary", default="暂无质控审阅摘要")))
        self.ai_summary.set_text(str(snapshot.get("ai_summary_text", "") or t("pages.reports.no_ai_report_summary")))
        self.export_bar.render(dict(snapshot.get("export", {}) or {}))
        self.page_scaffold._update_scroll_region()

    def _text_panel(self, parent: tk.Misc, *, row: int, title: str) -> tk.Text:
        frame = ttk.Frame(parent, style="Card.TFrame", padding=8)
        frame.grid(row=row, column=0, sticky="nsew", pady=(0, 12))
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)
        ttk.Label(frame, text=title, style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        widget = tk.Text(frame, height=7, wrap="word")
        widget.grid(row=1, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=widget.yview)
        scrollbar.grid(row=1, column=1, sticky="ns", padx=(6, 0))
        widget.configure(yscrollcommand=scrollbar.set, state="disabled")
        return widget

    @staticmethod
    def _set_text(widget: tk.Text, value: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", value.strip() + "\n")
        widget.configure(state="disabled")

    def _on_review_selection_changed(self, selection: dict[str, Any]) -> None:
        self._apply_artifact_scope(selection)

    def _apply_artifact_scope(self, selection: dict[str, Any] | None) -> None:
        self._artifact_scope_snapshot = dict(selection or {"scope": "all"})
        scoped_view = build_artifact_scope_view(self._artifact_rows, selection=self._artifact_scope_snapshot)
        self.artifacts.render(list(scoped_view.get("rows", []) or []))
        self.artifact_scope_var.set(str(scoped_view.get("summary_text") or t("common.none")))
        self.artifact_scope_notice_var.set(
            str(scoped_view.get("empty_text") or scoped_view.get("disclaimer_text") or t("common.none"))
        )
        self.clear_artifact_scope_button.configure(
            state="normal" if bool(scoped_view.get("clear_enabled", False)) else "disabled"
        )
        visible_count = int(scoped_view.get("visible_count", 0) or 0)
        present_count = int(scoped_view.get("scope_present_count", scoped_view.get("present_count", 0)) or 0)
        scope_total_count = int(scoped_view.get("scope_visible_count", scoped_view.get("total_count", 0)) or 0)
        external_count = int(scoped_view.get("scope_external_count", 0) or 0)
        missing_count = int(scoped_view.get("scope_missing_count", 0) or 0)
        catalog_total_count = int(scoped_view.get("catalog_total_count", 0) or 0)
        catalog_present_count = int(scoped_view.get("catalog_present_count", 0) or 0)
        scope_label = str(scoped_view.get("scope_label") or t("pages.reports.artifact_scope.label_all"))
        self.run_dir_card.set_note(
            t(
                "pages.reports.artifact_scope.run_dir_note",
                scope=scope_label,
                catalog_present=catalog_present_count,
                catalog_total=catalog_total_count,
                default=f"Current review scope: {scope_label} | catalog {catalog_present_count}/{catalog_total_count}",
            )
        )
        self.artifact_count_card.set_value(str(visible_count))
        self.artifact_count_card.set_note(
            t(
                "pages.reports.artifact_scope.scope_note",
                scope=scope_label,
                visible=visible_count,
                total=scope_total_count,
                external=external_count,
                missing=missing_count,
                catalog_total=catalog_total_count,
                default=f"{scope_label} | visible {visible_count} | external {external_count} | missing {missing_count} | catalog {catalog_total_count}",
            )
        )
        self.present_count_card.set_value(str(present_count))
        self.present_count_card.set_note(
            t(
                "pages.reports.artifact_scope.present_note",
                scope=scope_label,
                present=present_count,
                visible=visible_count,
                total=scope_total_count,
                missing=missing_count,
                catalog_present=catalog_present_count,
                catalog_total=catalog_total_count,
                default=f"{scope_label} | present {present_count}/{scope_total_count} | missing {missing_count} | catalog {catalog_present_count}/{catalog_total_count}",
            )
        )
        self.export_scope_notice_var.set(str(scoped_view.get("export_warning_text") or ""))

    def _clear_artifact_scope(self) -> None:
        self.review_center.clear_selection_scope()

    def _export_json(self) -> tuple[bool, str] | dict[str, object]:
        if self.exporter is None:
            return False, t("widgets.export_bar.unavailable")
        return self.exporter.export_artifacts("json")

    def _export_csv(self) -> tuple[bool, str] | dict[str, object]:
        if self.exporter is None:
            return False, t("widgets.export_bar.unavailable")
        return self.exporter.export_artifacts("csv")

    def _export_all(self) -> tuple[bool, str] | dict[str, object]:
        if self.exporter is None:
            return False, t("widgets.export_bar.unavailable")
        return self.exporter.export_artifacts("all")

    def _export_review_manifest(self) -> tuple[bool, str] | dict[str, object]:
        if self.exporter is None or not hasattr(self.exporter, "export_review_scope_manifest"):
            return False, t("widgets.export_bar.unavailable")
        return self.exporter.export_review_scope_manifest(selection=self._artifact_scope_snapshot)
