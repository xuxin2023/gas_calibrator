from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import t
from ..widgets.qc_reject_reason_chart import QCRejectReasonChart
from ..widgets.qc_overview_panel import QCOverviewPanel
from ..widgets.scrollable_page_frame import ScrollablePageFrame


class QCPage(ttk.Frame):
    """QC overview page backed by facade snapshots."""

    COLUMNS = ("point_index", "route", "temperature_c", "co2_ppm", "quality_score", "valid", "reason")

    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.score_var = tk.StringVar(value="0.00")
        self.grade_var = tk.StringVar(value="--")
        self.valid_var = tk.StringVar(value="0")
        self.invalid_var = tk.StringVar(value="0")
        self.total_var = tk.StringVar(value="0")
        self.overview = QCOverviewPanel(self)
        self.reject_chart = QCRejectReasonChart(self)
        self.tree = ttk.Treeview(self, columns=self.COLUMNS, show="headings", height=8)
        self.details = tk.Text(self, height=10, wrap="word")
        self._build()

    def _build(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.page_scaffold = ScrollablePageFrame(self, padding=12)
        self.page_scaffold.grid(row=0, column=0, sticky="nsew")
        body = self.page_scaffold.content
        body.columnconfigure(0, weight=3)
        body.columnconfigure(1, weight=2)
        body.rowconfigure(1, weight=1)
        body.rowconfigure(2, weight=1)

        summary = ttk.Frame(body, style="Card.TFrame")
        summary.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 12))
        for column in range(5):
            summary.columnconfigure(column, weight=1)
        self._metric(summary, 0, t("pages.qc.score"), self.score_var)
        self._metric(summary, 1, t("pages.qc.grade"), self.grade_var)
        self._metric(summary, 2, t("pages.qc.valid"), self.valid_var)
        self._metric(summary, 3, t("pages.qc.invalid"), self.invalid_var)
        self._metric(summary, 4, t("pages.qc.total"), self.total_var)

        self.overview.grid(in_=body, row=1, column=0, sticky="nsew", padx=(0, 6), pady=(0, 12))
        self.reject_chart.grid(in_=body, row=1, column=1, sticky="nsew", padx=(6, 0), pady=(0, 12))

        table_frame = ttk.Frame(body, style="Card.TFrame", padding=8)
        table_frame.grid(row=2, column=0, sticky="nsew", padx=(0, 6))
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(1, weight=1)
        ttk.Label(table_frame, text=t("pages.qc.table_title", default="质控点明细"), style="Section.TLabel").grid(
            row=0,
            column=0,
            sticky="w",
            pady=(0, 6),
        )
        self.tree.grid(in_=table_frame, row=1, column=0, sticky="nsew")
        tree_scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        tree_scroll_x = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        tree_scroll_y.grid(row=1, column=1, sticky="ns")
        tree_scroll_x.grid(row=2, column=0, sticky="ew", pady=(6, 0))
        self.tree.configure(yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set)
        headings = (
            t("pages.qc.table_point"),
            t("pages.qc.table_route"),
            t("pages.qc.table_temp"),
            t("pages.qc.table_co2"),
            t("pages.qc.table_score"),
            t("pages.qc.table_valid"),
            t("pages.qc.table_reason"),
        )
        widths = {"point_index": 70, "route": 90, "temperature_c": 80, "co2_ppm": 90, "quality_score": 90, "valid": 80, "reason": 220}
        for column, heading in zip(self.COLUMNS, headings, strict=False):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=widths[column], anchor="w")

        details_frame = ttk.Frame(body, style="Card.TFrame", padding=8)
        details_frame.grid(row=2, column=1, sticky="nsew", padx=(6, 0))
        details_frame.columnconfigure(0, weight=1)
        details_frame.rowconfigure(1, weight=1)
        ttk.Label(details_frame, text=t("pages.qc.details_title"), style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.details.grid(in_=details_frame, row=1, column=0, sticky="nsew")
        details_scroll = ttk.Scrollbar(details_frame, orient="vertical", command=self.details.yview)
        details_scroll.grid(row=1, column=1, sticky="ns", padx=(6, 0))
        self.details.configure(yscrollcommand=details_scroll.set)

    @staticmethod
    def _metric(parent: tk.Misc, column: int, label: str, variable: tk.StringVar) -> None:
        frame = ttk.Frame(parent, style="SoftCard.TFrame", padding=8)
        frame.grid(row=0, column=column, sticky="nsew", padx=4)
        ttk.Label(frame, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(frame, textvariable=variable, style="Section.TLabel").grid(row=1, column=0, sticky="w")

    def render(self, snapshot: dict[str, Any]) -> None:
        self.score_var.set(f"{float(snapshot.get('overall_score', 0.0) or 0.0):.2f}")
        self.grade_var.set(str(snapshot.get("grade", "--") or "--"))
        self.valid_var.set(str(snapshot.get("valid_points", 0) or 0))
        self.invalid_var.set(str(snapshot.get("invalid_points", 0) or 0))
        self.total_var.set(str(snapshot.get("total_points", 0) or 0))
        self.overview.render(dict(snapshot.get("overview", {}) or {}))
        self.reject_chart.render(dict(snapshot.get("reject_reasons_chart", {}) or {}))

        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in list(snapshot.get("point_rows", []) or []):
            self.tree.insert(
                "",
                "end",
                values=(
                    row.get("point_index", ""),
                    row.get("route", ""),
                    row.get("temperature_c", ""),
                    row.get("co2_ppm", ""),
                    f"{float(row.get('quality_score', 0.0) or 0.0):.2f}",
                    t("common.yes") if bool(row.get("valid", False)) else t("common.no"),
                    row.get("reason", ""),
                ),
            )

        reasons = list(snapshot.get("invalid_reasons", []) or [])
        recommendations = list(snapshot.get("recommendations", []) or [])
        decision_counts = dict(snapshot.get("decision_counts", {}) or {})
        run_gate = dict(snapshot.get("run_gate", {}) or {})
        reject_reason_taxonomy = list(snapshot.get("reject_reason_taxonomy", []) or [])
        failed_check_taxonomy = list(snapshot.get("failed_check_taxonomy", []) or [])
        point_gate_summary = dict(snapshot.get("point_gate_summary", {}) or {})
        route_decision_breakdown = dict(snapshot.get("route_decision_breakdown", {}) or {})
        reviewer_digest = dict(snapshot.get("reviewer_digest", {}) or {})
        rule_profile = dict(snapshot.get("rule_profile", {}) or {})
        threshold_profile = dict(snapshot.get("threshold_profile", {}) or {})
        evidence_boundary = dict(snapshot.get("evidence_boundary", {}) or {})
        lines = []
        if reviewer_digest:
            lines.append(t("pages.qc.digest_title", default="质控摘要"))
            lines.append(str(reviewer_digest.get("summary") or t("common.none")))
            lines.append(
                t(
                    "pages.qc.reviewer_card_title",
                    default="审阅卡片",
                )
            )
            lines.append(
                t(
                    "pages.qc.reviewer_card_summary",
                    summary=str(reviewer_digest.get("summary") or t("common.none")),
                    default=f"审阅结论: {str(reviewer_digest.get('summary') or t('common.none'))}",
                )
            )
            digest_lines = [
                str(item or "").strip()
                for item in list(reviewer_digest.get("lines") or [])
                if str(item or "").strip()
            ]
            for extra_line in digest_lines:
                if extra_line != str(reviewer_digest.get("summary") or "").strip():
                    lines.append(extra_line)
        if decision_counts:
            if lines:
                lines.append("")
            lines.append(t("pages.qc.result_levels", default="结果分级"))
            lines.extend(
                [
                    f"- {t('pages.qc.level.pass', default='通过')}: {int(decision_counts.get('pass', 0) or 0)}",
                    f"- {t('pages.qc.level.warn', default='预警')}: {int(decision_counts.get('warn', 0) or 0)}",
                    f"- {t('pages.qc.level.reject', default='拒绝')}: {int(decision_counts.get('reject', 0) or 0)}",
                    f"- {t('pages.qc.level.skipped', default='跳过')}: {int(decision_counts.get('skipped', 0) or 0)}",
                ]
            )
        if run_gate:
            if lines:
                lines.append("")
            lines.append(t("pages.qc.run_gate_title", default="运行门禁"))
            lines.append(
                f"- {t('pages.qc.run_gate_status', default='状态')}: {str(run_gate.get('status') or '--')}"
            )
            lines.append(
                f"- {t('pages.qc.run_gate_reason', default='原因')}: {str(run_gate.get('reason') or '--')}"
            )
        if point_gate_summary:
            if lines:
                lines.append("")
            lines.append(t("pages.qc.point_gate_title", default="点级门禁"))
            lines.append(
                f"- {t('pages.qc.point_gate_status', default='状态')}: {str(point_gate_summary.get('status') or '--')}"
            )
            lines.append(
                f"- {t('pages.qc.point_gate_flagged', default='关注点数')}: {int(point_gate_summary.get('flagged_point_count', 0) or 0)}"
            )
            lines.append(
                f"- {t('pages.qc.point_gate_routes', default='关注路由')}: "
                + (", ".join(str(item) for item in list(point_gate_summary.get("flagged_routes") or []) if str(item).strip()) or "--")
            )
        if route_decision_breakdown:
            if lines:
                lines.append("")
            lines.append(t("pages.qc.route_breakdown_title", default="路由分布"))
            for route_name, route_counts in sorted(route_decision_breakdown.items()):
                counts_payload = dict(route_counts or {})
                lines.append(
                    f"- {route_name}: "
                    f"{t('pages.qc.level.pass', default='通过')} {int(counts_payload.get('pass', 0) or 0)} / "
                    f"{t('pages.qc.level.warn', default='预警')} {int(counts_payload.get('warn', 0) or 0)} / "
                    f"{t('pages.qc.level.reject', default='拒绝')} {int(counts_payload.get('reject', 0) or 0)} / "
                    f"{t('pages.qc.level.skipped', default='跳过')} {int(counts_payload.get('skipped', 0) or 0)}"
                )
        if reject_reason_taxonomy:
            if lines:
                lines.append("")
            lines.append(t("pages.qc.taxonomy_title", default="拒绝原因分类"))
            lines.extend(
                f"- {str(item.get('code') or '--')} ({str(item.get('category') or 'other')}): {int(item.get('count', 0) or 0)}"
                for item in reject_reason_taxonomy
            )
        if failed_check_taxonomy:
            if lines:
                lines.append("")
            lines.append(t("pages.qc.failed_check_taxonomy_title", default="失败检查分类"))
            lines.extend(
                f"- {str(item.get('code') or '--')} ({str(item.get('category') or 'other')}): {int(item.get('count', 0) or 0)}"
                for item in failed_check_taxonomy
            )
        if rule_profile or threshold_profile:
            if lines:
                lines.append("")
            lines.append(t("pages.qc.profile_title", default="规则与阈值"))
            if rule_profile:
                lines.append(
                    f"- {t('pages.qc.rule_profile', default='规则配置')}: {str(rule_profile.get('name') or '--')}"
                )
            if threshold_profile:
                lines.append(
                    f"- {t('pages.qc.threshold_profile', default='阈值配置')}: "
                    f"min_sample={int(threshold_profile.get('min_sample_count', 0) or 0)}, "
                    f"pass={float(threshold_profile.get('pass_threshold', 0.0) or 0.0):.2f}, "
                    f"warn={float(threshold_profile.get('warn_threshold', 0.0) or 0.0):.2f}, "
                    f"reject={float(threshold_profile.get('reject_threshold', 0.0) or 0.0):.2f}"
                )
        if reasons:
            if lines:
                lines.append("")
            lines.append(t("pages.qc.invalid_reasons"))
            lines.extend(f"- {item}" for item in reasons)
        if recommendations:
            if lines:
                lines.append("")
            lines.append(t("pages.qc.recommendations"))
            lines.extend(f"- {item}" for item in recommendations)
        if evidence_boundary:
            if lines:
                lines.append("")
            lines.append(t("pages.qc.evidence_title", default="证据边界"))
            lines.append(
                f"- evidence_source: {str(evidence_boundary.get('evidence_source') or '--')}"
            )
            lines.append(
                f"- {t('pages.qc.evidence_disclaimer', default='仅限 simulation/offline 证据，不代表 real acceptance evidence')}"
            )
        if not lines:
            lines.append(t("pages.qc.no_detail"))
        self.details.configure(state="normal")
        self.details.delete("1.0", "end")
        self.details.insert("1.0", "\n".join(lines) + "\n")
        self.details.configure(state="disabled")
        self.page_scaffold._update_scroll_region()
