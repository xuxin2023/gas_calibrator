from __future__ import annotations

import time
import tkinter as tk
from tkinter import ttk
from typing import Any, Callable

from ..i18n import t
from ..review_center_presenter import (
    build_review_center_selection_snapshot,
    build_review_center_view,
)
from .collapsible_section import CollapsibleSection


class ReviewCenterPanel(ttk.LabelFrame):
    """Compact review center for operator/reviewer/approver evidence review."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        title: str | None = None,
        compact: bool = False,
        on_selection_changed: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        super().__init__(
            parent,
            text=title or t("results.review_center.title"),
            padding=8 if compact else 10,
            style="Card.TFrame",
        )
        self.compact = bool(compact)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(9, weight=1)
        self._payload: dict[str, Any] = {}
        self._items: list[dict[str, Any]] = []
        self._type_lookup: dict[str, str] = {}
        self._status_lookup: dict[str, str] = {}
        self._time_lookup: dict[str, str] = {}
        self._source_lookup: dict[str, str] = {}
        self._phase_lookup: dict[str, str] = {}
        self._artifact_role_lookup: dict[str, str] = {}
        self._standard_family_lookup: dict[str, str] = {}
        self._evidence_category_lookup: dict[str, str] = {}
        self._boundary_lookup: dict[str, str] = {}
        self._anchor_lookup: dict[str, str] = {}
        self._time_windows: dict[str, float | None] = {}
        self._active_view: dict[str, Any] = {}
        self._selected_source_id = "all"
        self._source_tree_lookup: dict[str, dict[str, Any]] = {}
        self._syncing_source_selection = False
        self._syncing_tree_selection = False
        self._selection_scope = "all"
        self._selected_item_key = ""
        self._selected_item: dict[str, Any] = {}
        self._selection_snapshot: dict[str, Any] = {}
        self._on_selection_changed_callback = on_selection_changed

        toolbar = ttk.Frame(self, style="Card.TFrame")
        toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        toolbar.columnconfigure(11, weight=1)
        self.type_filter_var = tk.StringVar(value="")
        self.status_filter_var = tk.StringVar(value="")
        self.time_filter_var = tk.StringVar(value="")
        self.source_filter_var = tk.StringVar(value="")
        self.phase_filter_var = tk.StringVar(value="")
        self.artifact_role_filter_var = tk.StringVar(value="")
        self.standard_family_filter_var = tk.StringVar(value="")
        self.evidence_category_filter_var = tk.StringVar(value="")
        self.boundary_filter_var = tk.StringVar(value="")
        self.anchor_filter_var = tk.StringVar(value="")
        self.count_var = tk.StringVar(value="")
        ttk.Label(toolbar, text=t("results.review_center.filter.type"), style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        self.type_filter = ttk.Combobox(toolbar, textvariable=self.type_filter_var, state="readonly", width=16)
        self.type_filter.grid(row=0, column=1, sticky="w", padx=(6, 12))
        self.type_filter.bind("<<ComboboxSelected>>", self._on_filter_changed, add="+")
        ttk.Label(toolbar, text=t("results.review_center.filter.status"), style="Muted.TLabel").grid(row=0, column=2, sticky="w")
        self.status_filter = ttk.Combobox(toolbar, textvariable=self.status_filter_var, state="readonly", width=14)
        self.status_filter.grid(row=0, column=3, sticky="w", padx=(6, 12))
        self.status_filter.bind("<<ComboboxSelected>>", self._on_filter_changed, add="+")
        ttk.Label(toolbar, text=t("results.review_center.filter.time"), style="Muted.TLabel").grid(row=0, column=4, sticky="w")
        self.time_filter = ttk.Combobox(toolbar, textvariable=self.time_filter_var, state="readonly", width=14)
        self.time_filter.grid(row=0, column=5, sticky="w", padx=(6, 12))
        self.time_filter.bind("<<ComboboxSelected>>", self._on_filter_changed, add="+")
        ttk.Label(toolbar, text=t("results.review_center.filter.source"), style="Muted.TLabel").grid(row=0, column=6, sticky="w")
        self.source_filter = ttk.Combobox(toolbar, textvariable=self.source_filter_var, state="readonly", width=16)
        self.source_filter.grid(row=0, column=7, sticky="w", padx=(6, 12))
        self.source_filter.bind("<<ComboboxSelected>>", self._on_filter_changed, add="+")
        ttk.Label(toolbar, textvariable=self.count_var, style="Muted.TLabel").grid(row=0, column=9, sticky="e")
        ttk.Label(toolbar, text=t("results.review_center.filter.phase", default="阶段"), style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(4, 0))
        self.phase_filter = ttk.Combobox(toolbar, textvariable=self.phase_filter_var, state="readonly", width=18)
        self.phase_filter.grid(row=1, column=1, sticky="w", padx=(6, 12), pady=(4, 0))
        self.phase_filter.bind("<<ComboboxSelected>>", self._on_filter_changed, add="+")
        ttk.Label(toolbar, text=t("results.review_center.filter.artifact_role", default="工件角色"), style="Muted.TLabel").grid(row=1, column=2, sticky="w", pady=(4, 0))
        self.artifact_role_filter = ttk.Combobox(toolbar, textvariable=self.artifact_role_filter_var, state="readonly", width=18)
        self.artifact_role_filter.grid(row=1, column=3, sticky="w", padx=(6, 12), pady=(4, 0))
        self.artifact_role_filter.bind("<<ComboboxSelected>>", self._on_filter_changed, add="+")
        ttk.Label(toolbar, text=t("results.review_center.filter.standard_family", default="标准家族"), style="Muted.TLabel").grid(row=1, column=4, sticky="w", pady=(4, 0))
        self.standard_family_filter = ttk.Combobox(toolbar, textvariable=self.standard_family_filter_var, state="readonly", width=24)
        self.standard_family_filter.grid(row=1, column=5, sticky="w", padx=(6, 12), pady=(4, 0))
        self.standard_family_filter.bind("<<ComboboxSelected>>", self._on_filter_changed, add="+")
        ttk.Label(toolbar, text=t("results.review_center.filter.evidence_category", default="证据类别"), style="Muted.TLabel").grid(row=1, column=6, sticky="w", pady=(4, 0))
        self.evidence_category_filter = ttk.Combobox(toolbar, textvariable=self.evidence_category_filter_var, state="readonly", width=24)
        self.evidence_category_filter.grid(row=1, column=7, sticky="w", padx=(6, 12), pady=(4, 0))
        self.evidence_category_filter.bind("<<ComboboxSelected>>", self._on_filter_changed, add="+")
        ttk.Label(toolbar, text=t("results.review_center.filter.boundary", default="边界"), style="Muted.TLabel").grid(row=1, column=8, sticky="w", pady=(4, 0))
        self.boundary_filter = ttk.Combobox(toolbar, textvariable=self.boundary_filter_var, state="readonly", width=22)
        self.boundary_filter.grid(row=1, column=9, sticky="w", padx=(6, 12), pady=(4, 0))
        self.boundary_filter.bind("<<ComboboxSelected>>", self._on_filter_changed, add="+")
        ttk.Label(toolbar, text=t("results.review_center.filter.anchor", default="锚点"), style="Muted.TLabel").grid(row=1, column=10, sticky="w", pady=(4, 0))
        self.anchor_filter = ttk.Combobox(toolbar, textvariable=self.anchor_filter_var, state="readonly", width=24)
        self.anchor_filter.grid(row=1, column=11, sticky="ew", padx=(6, 0), pady=(4, 0))
        self.anchor_filter.bind("<<ComboboxSelected>>", self._on_filter_changed, add="+")

        self.index_var = tk.StringVar(value="")
        ttk.Label(
            self,
            textvariable=self.index_var,
            wraplength=1120 if compact else 1320,
            justify="left",
            style="Muted.TLabel",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 6))

        summary = ttk.Frame(self, style="Card.TFrame")
        summary.grid(row=2, column=0, sticky="ew", pady=(0, 6))
        for column in range(4):
            summary.columnconfigure(column, weight=1)
        self.operator_var = tk.StringVar(value="")
        self.reviewer_var = tk.StringVar(value="")
        self.approver_var = tk.StringVar(value="")
        self.risk_var = tk.StringVar(value="")
        self.readiness_var = tk.StringVar(value="")
        self.analytics_var = tk.StringVar(value="")
        self.lineage_var = tk.StringVar(value="")
        self.phase_bridge_var = tk.StringVar(value="")
        self._summary_card(summary, 0, 0, t("results.review_center.role.operator"), self.operator_var)
        self._summary_card(summary, 0, 1, t("results.review_center.role.reviewer"), self.reviewer_var)
        self._summary_card(summary, 0, 2, t("results.review_center.role.approver"), self.approver_var)
        self._summary_card(summary, 0, 3, t("results.review_center.section.risk"), self.risk_var)
        self._summary_card(summary, 1, 0, t("results.review_center.section.readiness"), self.readiness_var)
        self._summary_card(summary, 1, 1, t("results.review_center.section.analytics"), self.analytics_var)
        self._summary_card(summary, 1, 2, t("results.review_center.section.lineage"), self.lineage_var)
        self._summary_card(summary, 1, 3, t("results.review_center.section.phase_bridge", default="阶段准入桥"), self.phase_bridge_var)

        self.phase_bridge_artifact_frame = ttk.Frame(self, style="Card.TFrame")
        self.phase_bridge_artifact_frame.grid(row=3, column=0, sticky="ew", pady=(0, 6))
        self.phase_bridge_artifact_frame.columnconfigure(0, weight=1)
        self.phase_bridge_artifact_title_var = tk.StringVar(value="")
        self.phase_bridge_artifact_status_var = tk.StringVar(value="")
        self.phase_bridge_artifact_path_var = tk.StringVar(value="")
        self.phase_bridge_artifact_note_var = tk.StringVar(value="")
        ttk.Label(
            self.phase_bridge_artifact_frame,
            text=t(
                "results.review_center.section.phase_bridge",
                default="阶段准入桥",
            ),
            style="Section.TLabel",
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        ttk.Label(
            self.phase_bridge_artifact_frame,
            textvariable=self.phase_bridge_artifact_title_var,
            justify="left",
            wraplength=1120 if compact else 1320,
        ).grid(row=1, column=0, sticky="ew")
        ttk.Label(
            self.phase_bridge_artifact_frame,
            textvariable=self.phase_bridge_artifact_status_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=2, column=0, sticky="ew", pady=(2, 0))
        ttk.Label(
            self.phase_bridge_artifact_frame,
            textvariable=self.phase_bridge_artifact_path_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=3, column=0, sticky="ew", pady=(2, 0))
        ttk.Label(
            self.phase_bridge_artifact_frame,
            textvariable=self.phase_bridge_artifact_note_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=4, column=0, sticky="ew", pady=(2, 0))
        self.phase_bridge_artifact_frame.grid_remove()

        self.stage_admission_review_pack_frame = ttk.Frame(self, style="Card.TFrame")
        self.stage_admission_review_pack_frame.grid(row=4, column=0, sticky="ew", pady=(0, 6))
        self.stage_admission_review_pack_frame.columnconfigure(0, weight=1)
        self.stage_admission_review_pack_title_var = tk.StringVar(value="")
        self.stage_admission_review_pack_status_var = tk.StringVar(value="")
        self.stage_admission_review_pack_path_var = tk.StringVar(value="")
        self.stage_admission_review_pack_note_var = tk.StringVar(value="")
        ttk.Label(
            self.stage_admission_review_pack_frame,
            textvariable=self.stage_admission_review_pack_title_var,
            style="Section.TLabel",
            wraplength=1120 if compact else 1320,
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        ttk.Label(
            self.stage_admission_review_pack_frame,
            textvariable=self.stage_admission_review_pack_status_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=1, column=0, sticky="ew")
        ttk.Label(
            self.stage_admission_review_pack_frame,
            textvariable=self.stage_admission_review_pack_path_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=2, column=0, sticky="ew", pady=(2, 0))
        ttk.Label(
            self.stage_admission_review_pack_frame,
            textvariable=self.stage_admission_review_pack_note_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=3, column=0, sticky="ew", pady=(2, 0))
        self.stage_admission_review_pack_frame.grid_remove()

        self.engineering_isolation_admission_checklist_frame = ttk.Frame(self, style="Card.TFrame")
        self.engineering_isolation_admission_checklist_frame.grid(row=5, column=0, sticky="ew", pady=(0, 6))
        self.engineering_isolation_admission_checklist_frame.columnconfigure(0, weight=1)
        self.engineering_isolation_admission_checklist_title_var = tk.StringVar(value="")
        self.engineering_isolation_admission_checklist_status_var = tk.StringVar(value="")
        self.engineering_isolation_admission_checklist_path_var = tk.StringVar(value="")
        self.engineering_isolation_admission_checklist_note_var = tk.StringVar(value="")
        ttk.Label(
            self.engineering_isolation_admission_checklist_frame,
            textvariable=self.engineering_isolation_admission_checklist_title_var,
            style="Section.TLabel",
            wraplength=1120 if compact else 1320,
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        ttk.Label(
            self.engineering_isolation_admission_checklist_frame,
            textvariable=self.engineering_isolation_admission_checklist_status_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=1, column=0, sticky="ew")
        ttk.Label(
            self.engineering_isolation_admission_checklist_frame,
            textvariable=self.engineering_isolation_admission_checklist_path_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=2, column=0, sticky="ew", pady=(2, 0))
        ttk.Label(
            self.engineering_isolation_admission_checklist_frame,
            textvariable=self.engineering_isolation_admission_checklist_note_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=3, column=0, sticky="ew", pady=(2, 0))
        self.engineering_isolation_admission_checklist_frame.grid_remove()

        self.stage3_real_validation_plan_frame = ttk.Frame(self, style="Card.TFrame")
        self.stage3_real_validation_plan_frame.grid(row=6, column=0, sticky="ew", pady=(0, 6))
        self.stage3_real_validation_plan_frame.columnconfigure(0, weight=1)
        self.stage3_real_validation_plan_title_var = tk.StringVar(value="")
        self.stage3_real_validation_plan_status_var = tk.StringVar(value="")
        self.stage3_real_validation_plan_path_var = tk.StringVar(value="")
        self.stage3_real_validation_plan_note_var = tk.StringVar(value="")
        ttk.Label(
            self.stage3_real_validation_plan_frame,
            textvariable=self.stage3_real_validation_plan_title_var,
            style="Section.TLabel",
            wraplength=1120 if compact else 1320,
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        ttk.Label(
            self.stage3_real_validation_plan_frame,
            textvariable=self.stage3_real_validation_plan_status_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=1, column=0, sticky="ew")
        ttk.Label(
            self.stage3_real_validation_plan_frame,
            textvariable=self.stage3_real_validation_plan_path_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=2, column=0, sticky="ew", pady=(2, 0))
        ttk.Label(
            self.stage3_real_validation_plan_frame,
            textvariable=self.stage3_real_validation_plan_note_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=3, column=0, sticky="ew", pady=(2, 0))
        self.stage3_real_validation_plan_frame.grid_remove()

        self.stage3_standards_alignment_matrix_frame = ttk.Frame(self, style="Card.TFrame")
        self.stage3_standards_alignment_matrix_frame.grid(row=7, column=0, sticky="ew", pady=(0, 6))
        self.stage3_standards_alignment_matrix_frame.columnconfigure(0, weight=1)
        self.stage3_standards_alignment_matrix_title_var = tk.StringVar(value="")
        self.stage3_standards_alignment_matrix_status_var = tk.StringVar(value="")
        self.stage3_standards_alignment_matrix_path_var = tk.StringVar(value="")
        self.stage3_standards_alignment_matrix_note_var = tk.StringVar(value="")
        ttk.Label(
            self.stage3_standards_alignment_matrix_frame,
            textvariable=self.stage3_standards_alignment_matrix_title_var,
            style="Section.TLabel",
            wraplength=1120 if compact else 1320,
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        ttk.Label(
            self.stage3_standards_alignment_matrix_frame,
            textvariable=self.stage3_standards_alignment_matrix_status_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=1, column=0, sticky="ew")
        ttk.Label(
            self.stage3_standards_alignment_matrix_frame,
            textvariable=self.stage3_standards_alignment_matrix_path_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=2, column=0, sticky="ew", pady=(2, 0))
        ttk.Label(
            self.stage3_standards_alignment_matrix_frame,
            textvariable=self.stage3_standards_alignment_matrix_note_var,
            justify="left",
            wraplength=1120 if compact else 1320,
            style="Muted.TLabel",
        ).grid(row=3, column=0, sticky="ew", pady=(2, 0))
        self.stage3_standards_alignment_matrix_frame.grid_remove()

        source_frame = ttk.Frame(self, style="Card.TFrame")
        source_frame.grid(row=8, column=0, sticky="ew", pady=(0, 6))
        source_frame.columnconfigure(0, weight=1)
        source_frame.columnconfigure(1, weight=1)
        source_frame.rowconfigure(1, weight=1)
        ttk.Label(source_frame, text=t("results.review_center.section.run_index"), style="Section.TLabel").grid(
            row=0,
            column=0,
            sticky="w",
            pady=(0, 6),
        )
        self.source_scope_var = tk.StringVar(
            value=t(
                "results.review_center.filter.active_source",
                source=t("results.review_center.filter.all_sources"),
                default=t("results.review_center.filter.all_sources"),
            )
        )
        ttk.Label(source_frame, textvariable=self.source_scope_var, style="Muted.TLabel").grid(
            row=0,
            column=1,
            sticky="e",
            padx=(12, 8),
            pady=(0, 6),
        )
        self.clear_source_button = ttk.Button(
            source_frame,
            text=t("results.review_center.filter.clear_source_drilldown"),
            command=self._clear_source_drilldown,
            state="disabled",
        )
        self.clear_source_button.grid(row=0, column=2, sticky="e", pady=(0, 6))
        self.source_tree = ttk.Treeview(
            source_frame,
            columns=("source", "latest", "coverage", "gaps"),
            show="headings",
            height=3 if compact else 4,
        )
        self.source_tree.grid(row=1, column=0, sticky="nsew")
        self.source_tree.heading("source", text=t("results.review_center.column.source"))
        self.source_tree.heading("latest", text=t("results.review_center.column.latest"))
        self.source_tree.heading("coverage", text=t("results.review_center.column.coverage"))
        self.source_tree.heading("gaps", text=t("results.review_center.column.gaps"))
        self.source_tree.column("source", width=160, anchor="w")
        self.source_tree.column("latest", width=120, anchor="w", stretch=False)
        self.source_tree.column("coverage", width=240 if compact else 300, anchor="w")
        self.source_tree.column("gaps", width=320 if compact else 380, anchor="w")
        source_scroll = ttk.Scrollbar(source_frame, orient="vertical", command=self.source_tree.yview)
        source_scroll.grid(row=1, column=1, sticky="ns", padx=(6, 0))
        self.source_tree.configure(yscrollcommand=source_scroll.set)
        self.source_tree.bind("<<TreeviewSelect>>", self._on_source_selected, add="+")

        list_frame = ttk.Frame(self, style="Card.TFrame")
        list_frame.grid(row=9, column=0, sticky="nsew")
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(1, weight=1)
        ttk.Label(list_frame, text=t("results.review_center.section.evidence_list"), style="Section.TLabel").grid(
            row=0,
            column=0,
            sticky="w",
            pady=(0, 6),
        )
        self.tree = ttk.Treeview(
            list_frame,
            columns=("time", "type", "status", "summary"),
            show="headings",
            height=5 if compact else 7,
        )
        self.tree.grid(row=1, column=0, sticky="nsew")
        self.tree.heading("time", text=t("results.review_center.column.time"))
        self.tree.heading("type", text=t("results.review_center.column.type"))
        self.tree.heading("status", text=t("results.review_center.column.status"))
        self.tree.heading("summary", text=t("results.review_center.column.summary"))
        self.tree.column("time", width=136, anchor="w", stretch=False)
        self.tree.column("type", width=90, anchor="w", stretch=False)
        self.tree.column("status", width=92, anchor="w", stretch=False)
        self.tree.column("summary", width=520 if compact else 680, anchor="w")
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_selected, add="+")
        tree_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.tree.yview)
        tree_scroll.grid(row=1, column=1, sticky="ns", padx=(6, 0))
        self.tree.configure(yscrollcommand=tree_scroll.set)

        self.detail_section = CollapsibleSection(
            self,
            title=t("results.review_center.section.evidence_detail"),
            expanded=not compact,
        )
        self.detail_section.grid(row=10, column=0, sticky="nsew", pady=(6, 0))
        self.detail_section.body.rowconfigure(8, weight=1)
        self.detail_section.body.columnconfigure(0, weight=1)
        detail_meta = ttk.Frame(self.detail_section.body, style="Card.TFrame")
        detail_meta.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        detail_meta.columnconfigure(1, weight=1)
        self.detail_summary_var = tk.StringVar(value="")
        self.detail_risk_var = tk.StringVar(value="")
        self.detail_key_fields_var = tk.StringVar(value="")
        self.detail_artifacts_var = tk.StringVar(value="")
        self.detail_acceptance_var = tk.StringVar(value="")
        self.detail_qc_var = tk.StringVar(value="")
        self.detail_analytics_var = tk.StringVar(value="")
        self.detail_spectral_var = tk.StringVar(value="")
        self.detail_lineage_var = tk.StringVar(value="")
        detail_rows = [
            ("results.review_center.detail_panel.summary", None, self.detail_summary_var),
            ("results.review_center.detail_panel.risk", None, self.detail_risk_var),
            ("results.review_center.detail_panel.key_fields", None, self.detail_key_fields_var),
            ("results.review_center.detail_panel.artifacts", None, self.detail_artifacts_var),
            ("results.review_center.detail_panel.acceptance", None, self.detail_acceptance_var),
            ("results.review_center.detail_panel.qc", t("shell.nav.qc"), self.detail_qc_var),
            ("results.review_center.detail_panel.analytics", None, self.detail_analytics_var),
            ("results.review_center.detail_panel.spectral_quality", None, self.detail_spectral_var),
            ("results.review_center.detail_panel.lineage", None, self.detail_lineage_var),
        ]
        for row, (label_key, label_default, value_var) in enumerate(detail_rows):
            ttk.Label(
                detail_meta,
                text=t(label_key, default=label_default),
                style="Muted.TLabel",
            ).grid(row=row, column=0, sticky="nw", padx=(0, 8), pady=2)
            ttk.Label(detail_meta, textvariable=value_var, justify="left", wraplength=1020 if compact else 1200).grid(
                row=row,
                column=1,
                sticky="ew",
                pady=2,
            )
        self.detail_text = tk.Text(self.detail_section.body, height=6 if compact else 8, wrap="word")
        self.detail_text.grid(row=8, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(self.detail_section.body, orient="vertical", command=self.detail_text.yview)
        detail_scroll.grid(row=8, column=1, sticky="ns", padx=(6, 0))
        self.detail_text.configure(yscrollcommand=detail_scroll.set, state="disabled")

        self.disclaimer_var = tk.StringVar(value="")
        ttk.Label(
            self,
            textvariable=self.disclaimer_var,
            wraplength=1120 if compact else 1320,
            justify="left",
            style="Muted.TLabel",
        ).grid(row=11, column=0, sticky="ew", pady=(6, 0))

    def _summary_card(
        self,
        parent: tk.Misc,
        row: int,
        column: int,
        label: str,
        value_var: tk.StringVar,
    ) -> None:
        frame = ttk.Frame(parent, style="SoftCard.TFrame", padding=8 if self.compact else 10)
        frame.grid(row=row, column=column, sticky="nsew", padx=4, pady=4)
        frame.columnconfigure(0, weight=1)
        ttk.Label(frame, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            frame,
            textvariable=value_var,
            wraplength=280 if self.compact else 340,
            justify="left",
        ).grid(row=1, column=0, sticky="ew", pady=(4, 0))

    def render(self, payload: dict[str, Any]) -> None:
        self._payload = dict(payload or {})
        self._items = [dict(item) for item in list(self._payload.get("evidence_items", []) or [])]
        self._render_filters()
        source_ids = {
            str(item.get("source_id") or "").strip()
            for item in list(dict(self._payload.get("index_summary", {}) or {}).get("sources", []) or [])
            if isinstance(item, dict)
        }
        if self._selected_source_id not in {"", "all"} and self._selected_source_id not in source_ids:
            self._selected_source_id = "all"
            if self._selection_scope in {"source", "evidence"}:
                self._selection_scope = "all"
                self._selected_item_key = ""
        self.disclaimer_var.set(str(self._payload.get("disclaimer") or ""))
        self.detail_section.set_summary(str(self._payload.get("detail_hint") or ""))
        self._apply_filters()

    def _render_sources(self, rows: list[dict[str, Any]]) -> None:
        self._source_tree_lookup = {}
        for item_id in self.source_tree.get_children():
            self.source_tree.delete(item_id)
        if not rows:
            self.source_tree.insert(
                "",
                "end",
                values=(
                    t("common.none"),
                    "--",
                    t("common.none"),
                    t("common.none"),
                ),
            )
            return
        for index, item in enumerate(rows):
            item_id = f"source-{index}"
            self._source_tree_lookup[item_id] = dict(item)
            self.source_tree.insert(
                "",
                "end",
                iid=item_id,
                values=(
                    item.get("source_label_display", item.get("source_label", "--")),
                    item.get("latest_display", "--"),
                    item.get("scope_count_display", item.get("coverage_display", "--")),
                    item.get("gaps_display", "--"),
                ),
            )
        self._sync_source_tree_selection()

    def _render_filters(self) -> None:
        filters = dict(self._payload.get("filters", {}) or {})
        type_options = [dict(item) for item in list(filters.get("type_options", []) or [])]
        status_options = [dict(item) for item in list(filters.get("status_options", []) or [])]
        time_options = [dict(item) for item in list(filters.get("time_options", []) or [])]
        source_options = [dict(item) for item in list(filters.get("source_options", []) or [])]
        phase_options = [dict(item) for item in list(filters.get("phase_options", []) or [])]
        artifact_role_options = [dict(item) for item in list(filters.get("artifact_role_options", []) or [])]
        standard_family_options = [dict(item) for item in list(filters.get("standard_family_options", []) or [])]
        evidence_category_options = [dict(item) for item in list(filters.get("evidence_category_options", []) or [])]
        boundary_options = [dict(item) for item in list(filters.get("boundary_options", []) or [])]
        anchor_options = [dict(item) for item in list(filters.get("anchor_options", []) or [])]
        self._type_lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in type_options}
        self._status_lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in status_options}
        self._time_lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in time_options}
        self._source_lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in source_options}
        self._phase_lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in phase_options}
        self._artifact_role_lookup = {
            str(item.get("label") or ""): str(item.get("id") or "")
            for item in artifact_role_options
        }
        self._standard_family_lookup = {
            str(item.get("label") or ""): str(item.get("id") or "")
            for item in standard_family_options
        }
        self._evidence_category_lookup = {
            str(item.get("label") or ""): str(item.get("id") or "")
            for item in evidence_category_options
        }
        self._boundary_lookup = {
            str(item.get("label") or ""): str(item.get("id") or "")
            for item in boundary_options
        }
        self._anchor_lookup = {
            str(item.get("label") or ""): str(item.get("id") or "")
            for item in anchor_options
        }
        self._time_windows = {
            str(item.get("id") or ""): (
                None
                if item.get("window_seconds") in ("", None)
                else float(item.get("window_seconds") or 0.0)
            )
            for item in time_options
        }
        type_labels = [str(item.get("label") or "") for item in type_options if str(item.get("label") or "").strip()]
        status_labels = [str(item.get("label") or "") for item in status_options if str(item.get("label") or "").strip()]
        time_labels = [str(item.get("label") or "") for item in time_options if str(item.get("label") or "").strip()]
        source_labels = [str(item.get("label") or "") for item in source_options if str(item.get("label") or "").strip()]
        phase_labels = [str(item.get("label") or "") for item in phase_options if str(item.get("label") or "").strip()]
        artifact_role_labels = [
            str(item.get("label") or "")
            for item in artifact_role_options
            if str(item.get("label") or "").strip()
        ]
        standard_family_labels = [
            str(item.get("label") or "")
            for item in standard_family_options
            if str(item.get("label") or "").strip()
        ]
        evidence_category_labels = [
            str(item.get("label") or "")
            for item in evidence_category_options
            if str(item.get("label") or "").strip()
        ]
        boundary_labels = [
            str(item.get("label") or "")
            for item in boundary_options
            if str(item.get("label") or "").strip()
        ]
        anchor_labels = [str(item.get("label") or "") for item in anchor_options if str(item.get("label") or "").strip()]
        self.type_filter.configure(values=type_labels)
        self.status_filter.configure(values=status_labels)
        self.time_filter.configure(values=time_labels)
        self.source_filter.configure(values=source_labels)
        self.phase_filter.configure(values=phase_labels)
        self.artifact_role_filter.configure(values=artifact_role_labels)
        self.standard_family_filter.configure(values=standard_family_labels)
        self.evidence_category_filter.configure(values=evidence_category_labels)
        self.boundary_filter.configure(values=boundary_labels)
        self.anchor_filter.configure(values=anchor_labels)
        default_type = next(
            (
                str(item.get("label") or "")
                for item in type_options
                if str(item.get("id") or "") == str(filters.get("selected_type") or "all")
            ),
            type_labels[0] if type_labels else "",
        )
        default_status = next(
            (
                str(item.get("label") or "")
                for item in status_options
                if str(item.get("id") or "") == str(filters.get("selected_status") or "all")
            ),
            status_labels[0] if status_labels else "",
        )
        if default_type:
            self.type_filter_var.set(default_type)
        if default_status:
            self.status_filter_var.set(default_status)
        default_time = next(
            (
                str(item.get("label") or "")
                for item in time_options
                if str(item.get("id") or "") == str(filters.get("selected_time") or "all")
            ),
            time_labels[0] if time_labels else "",
        )
        if default_time:
            self.time_filter_var.set(default_time)
        default_source = next(
            (
                str(item.get("label") or "")
                for item in source_options
                if str(item.get("id") or "") == str(filters.get("selected_source") or "all")
            ),
            source_labels[0] if source_labels else "",
        )
        if default_source:
            self.source_filter_var.set(default_source)
        default_phase = next(
            (
                str(item.get("label") or "")
                for item in phase_options
                if str(item.get("id") or "") == str(filters.get("selected_phase") or "all")
            ),
            phase_labels[0] if phase_labels else "",
        )
        default_artifact_role = next(
            (
                str(item.get("label") or "")
                for item in artifact_role_options
                if str(item.get("id") or "") == str(filters.get("selected_artifact_role") or "all")
            ),
            artifact_role_labels[0] if artifact_role_labels else "",
        )
        default_standard_family = next(
            (
                str(item.get("label") or "")
                for item in standard_family_options
                if str(item.get("id") or "") == str(filters.get("selected_standard_family") or "all")
            ),
            standard_family_labels[0] if standard_family_labels else "",
        )
        default_evidence_category = next(
            (
                str(item.get("label") or "")
                for item in evidence_category_options
                if str(item.get("id") or "") == str(filters.get("selected_evidence_category") or "all")
            ),
            evidence_category_labels[0] if evidence_category_labels else "",
        )
        default_boundary = next(
            (
                str(item.get("label") or "")
                for item in boundary_options
                if str(item.get("id") or "") == str(filters.get("selected_boundary") or "all")
            ),
            boundary_labels[0] if boundary_labels else "",
        )
        default_anchor = next(
            (
                str(item.get("label") or "")
                for item in anchor_options
                if str(item.get("id") or "") == str(filters.get("selected_anchor") or "all")
            ),
            anchor_labels[0] if anchor_labels else "",
        )
        if default_phase:
            self.phase_filter_var.set(default_phase)
        if default_artifact_role:
            self.artifact_role_filter_var.set(default_artifact_role)
        if default_standard_family:
            self.standard_family_filter_var.set(default_standard_family)
        if default_evidence_category:
            self.evidence_category_filter_var.set(default_evidence_category)
        if default_boundary:
            self.boundary_filter_var.set(default_boundary)
        if default_anchor:
            self.anchor_filter_var.set(default_anchor)

    def _on_filter_changed(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        self._apply_filters()

    def _apply_filters(self) -> None:
        selected_type = self._type_lookup.get(str(self.type_filter_var.get() or ""), "all")
        selected_status = self._status_lookup.get(str(self.status_filter_var.get() or ""), "all")
        selected_time = self._time_lookup.get(str(self.time_filter_var.get() or ""), "all")
        selected_source = self._source_lookup.get(str(self.source_filter_var.get() or ""), "all")
        selected_phase = self._phase_lookup.get(str(self.phase_filter_var.get() or ""), "all")
        selected_artifact_role = self._artifact_role_lookup.get(
            str(self.artifact_role_filter_var.get() or ""),
            "all",
        )
        selected_standard_family = self._standard_family_lookup.get(
            str(self.standard_family_filter_var.get() or ""),
            "all",
        )
        selected_evidence_category = self._evidence_category_lookup.get(
            str(self.evidence_category_filter_var.get() or ""),
            "all",
        )
        selected_boundary = self._boundary_lookup.get(str(self.boundary_filter_var.get() or ""), "all")
        selected_anchor = self._anchor_lookup.get(str(self.anchor_filter_var.get() or ""), "all")
        self._active_view = build_review_center_view(
            self._payload,
            selected_type=selected_type,
            selected_status=selected_status,
            selected_time=selected_time,
            selected_source_kind=selected_source,
            selected_source_id=self._selected_source_id,
            selected_phase=selected_phase,
            selected_artifact_role=selected_artifact_role,
            selected_standard_family=selected_standard_family,
            selected_evidence_category=selected_evidence_category,
            selected_boundary=selected_boundary,
            selected_anchor=selected_anchor,
            now_ts=time.time(),
        )
        rows = [dict(item) for item in list(self._active_view.get("items", []) or [])]
        self._render_sources([dict(item) for item in list(self._active_view.get("sources", []) or [])])
        self.operator_var.set(str(self._active_view.get("operator_summary") or t("common.none")))
        self.reviewer_var.set(str(self._active_view.get("reviewer_summary") or t("common.none")))
        self.approver_var.set(str(self._active_view.get("approver_summary") or t("common.none")))
        self.risk_var.set(str(self._active_view.get("risk_summary") or t("common.none")))
        self.readiness_var.set(str(self._active_view.get("readiness_summary") or t("common.none")))
        self.analytics_var.set(str(self._active_view.get("analytics_summary") or t("common.none")))
        self.lineage_var.set(str(self._active_view.get("lineage_summary") or t("common.none")))
        self.phase_bridge_var.set(str(self._active_view.get("phase_bridge_summary") or t("common.none")))
        phase_bridge_artifact_entry = dict(
            self._active_view.get("phase_bridge_reviewer_artifact_entry", {}) or {}
        )
        if bool(phase_bridge_artifact_entry.get("available", False)):
            self.phase_bridge_artifact_title_var.set(
                str(
                    phase_bridge_artifact_entry.get("name_text")
                    or phase_bridge_artifact_entry.get("title_text")
                    or t("common.none")
                )
            )
            self.phase_bridge_artifact_status_var.set(
                str(phase_bridge_artifact_entry.get("role_status_display") or t("common.none"))
            )
            self.phase_bridge_artifact_path_var.set(str(phase_bridge_artifact_entry.get("path") or t("common.none")))
            self.phase_bridge_artifact_note_var.set(
                str(
                    phase_bridge_artifact_entry.get("note_text")
                    or phase_bridge_artifact_entry.get("summary_text")
                    or t("common.none")
                )
            )
            self.phase_bridge_artifact_frame.grid()
        else:
            self.phase_bridge_artifact_title_var.set("")
            self.phase_bridge_artifact_status_var.set("")
            self.phase_bridge_artifact_path_var.set("")
            self.phase_bridge_artifact_note_var.set("")
            self.phase_bridge_artifact_frame.grid_remove()
        stage_admission_review_pack_entry = dict(
            self._active_view.get("stage_admission_review_pack_artifact_entry", {}) or {}
        )
        if bool(stage_admission_review_pack_entry.get("available", False)):
            self.stage_admission_review_pack_title_var.set(
                str(
                    stage_admission_review_pack_entry.get("name_text")
                    or stage_admission_review_pack_entry.get("title_text")
                    or t("common.none")
                )
            )
            self.stage_admission_review_pack_status_var.set(
                str(stage_admission_review_pack_entry.get("role_status_display") or t("common.none"))
            )
            self.stage_admission_review_pack_path_var.set(
                str(stage_admission_review_pack_entry.get("reviewer_path") or stage_admission_review_pack_entry.get("path") or t("common.none"))
            )
            self.stage_admission_review_pack_note_var.set(
                str(
                    stage_admission_review_pack_entry.get("note_text")
                    or stage_admission_review_pack_entry.get("summary_text")
                    or t("common.none")
                )
            )
            self.stage_admission_review_pack_frame.grid()
        else:
            self.stage_admission_review_pack_title_var.set("")
            self.stage_admission_review_pack_status_var.set("")
            self.stage_admission_review_pack_path_var.set("")
            self.stage_admission_review_pack_note_var.set("")
            self.stage_admission_review_pack_frame.grid_remove()
        engineering_isolation_admission_checklist_entry = dict(
            self._active_view.get("engineering_isolation_admission_checklist_artifact_entry", {}) or {}
        )
        if bool(engineering_isolation_admission_checklist_entry.get("available", False)):
            self.engineering_isolation_admission_checklist_title_var.set(
                str(
                    engineering_isolation_admission_checklist_entry.get("name_text")
                    or engineering_isolation_admission_checklist_entry.get("title_text")
                    or t("common.none")
                )
            )
            self.engineering_isolation_admission_checklist_status_var.set(
                str(
                    engineering_isolation_admission_checklist_entry.get("role_status_display")
                    or t("common.none")
                )
            )
            self.engineering_isolation_admission_checklist_path_var.set(
                str(
                    engineering_isolation_admission_checklist_entry.get("reviewer_path")
                    or engineering_isolation_admission_checklist_entry.get("path")
                    or t("common.none")
                )
            )
            self.engineering_isolation_admission_checklist_note_var.set(
                str(
                    engineering_isolation_admission_checklist_entry.get("note_text")
                    or engineering_isolation_admission_checklist_entry.get("summary_text")
                    or t("common.none")
                )
            )
            self.engineering_isolation_admission_checklist_frame.grid()
        else:
            self.engineering_isolation_admission_checklist_title_var.set("")
            self.engineering_isolation_admission_checklist_status_var.set("")
            self.engineering_isolation_admission_checklist_path_var.set("")
            self.engineering_isolation_admission_checklist_note_var.set("")
            self.engineering_isolation_admission_checklist_frame.grid_remove()
        stage3_real_validation_plan_entry = dict(
            self._active_view.get("stage3_real_validation_plan_artifact_entry", {}) or {}
        )
        if bool(stage3_real_validation_plan_entry.get("available", False)):
            self.stage3_real_validation_plan_title_var.set(
                str(
                    stage3_real_validation_plan_entry.get("name_text")
                    or stage3_real_validation_plan_entry.get("title_text")
                    or t("common.none")
                )
            )
            self.stage3_real_validation_plan_status_var.set(
                str(
                    stage3_real_validation_plan_entry.get("card_text")
                    or stage3_real_validation_plan_entry.get("entry_text")
                    or stage3_real_validation_plan_entry.get("role_status_display")
                    or t("common.none")
                )
            )
            self.stage3_real_validation_plan_path_var.set(
                str(
                    stage3_real_validation_plan_entry.get("artifact_paths_text")
                    or stage3_real_validation_plan_entry.get("reviewer_path")
                    or stage3_real_validation_plan_entry.get("path")
                    or t("common.none")
                )
            )
            self.stage3_real_validation_plan_note_var.set(
                str(
                    stage3_real_validation_plan_entry.get("reviewer_note_text")
                    or stage3_real_validation_plan_entry.get("note_text")
                    or stage3_real_validation_plan_entry.get("summary_text")
                    or t("common.none")
                )
            )
            self.stage3_real_validation_plan_frame.grid()
        else:
            self.stage3_real_validation_plan_title_var.set("")
            self.stage3_real_validation_plan_status_var.set("")
            self.stage3_real_validation_plan_path_var.set("")
            self.stage3_real_validation_plan_note_var.set("")
            self.stage3_real_validation_plan_frame.grid_remove()
        stage3_standards_alignment_matrix_entry = dict(
            self._active_view.get("stage3_standards_alignment_matrix_artifact_entry", {}) or {}
        )
        if bool(stage3_standards_alignment_matrix_entry.get("available", False)):
            self.stage3_standards_alignment_matrix_title_var.set(
                str(
                    stage3_standards_alignment_matrix_entry.get("name_text")
                    or stage3_standards_alignment_matrix_entry.get("title_text")
                    or t("common.none")
                )
            )
            self.stage3_standards_alignment_matrix_status_var.set(
                str(
                    stage3_standards_alignment_matrix_entry.get("card_text")
                    or stage3_standards_alignment_matrix_entry.get("entry_text")
                    or stage3_standards_alignment_matrix_entry.get("role_status_display")
                    or t("common.none")
                )
            )
            self.stage3_standards_alignment_matrix_path_var.set(
                str(
                    stage3_standards_alignment_matrix_entry.get("artifact_paths_text")
                    or stage3_standards_alignment_matrix_entry.get("reviewer_path")
                    or stage3_standards_alignment_matrix_entry.get("path")
                    or t("common.none")
                )
            )
            self.stage3_standards_alignment_matrix_note_var.set(
                str(
                    stage3_standards_alignment_matrix_entry.get("reviewer_note_text")
                    or stage3_standards_alignment_matrix_entry.get("note_text")
                    or stage3_standards_alignment_matrix_entry.get("summary_text")
                    or t("common.none")
                )
            )
            self.stage3_standards_alignment_matrix_frame.grid()
        else:
            self.stage3_standards_alignment_matrix_title_var.set("")
            self.stage3_standards_alignment_matrix_status_var.set("")
            self.stage3_standards_alignment_matrix_path_var.set("")
            self.stage3_standards_alignment_matrix_note_var.set("")
            self.stage3_standards_alignment_matrix_frame.grid_remove()
        self.index_var.set(str(self._active_view.get("index_text") or t("common.none")))
        self.source_scope_var.set(
            str(
                self._active_view.get("source_scope_label")
                or t(
                    "results.review_center.filter.active_source",
                    source=t("results.review_center.filter.all_sources"),
                    default=t("results.review_center.filter.all_sources"),
                )
            )
        )
        self.clear_source_button.configure(
            state="normal" if bool(self._active_view.get("source_scope_active", False)) else "disabled"
        )

        for item_id in self.tree.get_children():
            self.tree.delete(item_id)
        for index, item in enumerate(rows):
            self.tree.insert(
                "",
                "end",
                iid=str(index),
                values=(
                    item.get("generated_at_display", "--"),
                    item.get("type_display", "--"),
                    item.get("status_display", "--"),
                    item.get("summary", "--"),
                ),
            )
        self.count_var.set(
            t(
                "results.review_center.filter.count",
                visible=len(rows),
                total=len(self._items),
            )
        )
        if rows:
            selected_index = 0
            if self._selection_scope == "evidence" and self._selected_item_key:
                selected_index = next(
                    (
                        index
                        for index, item in enumerate(rows)
                        if self._item_key(item) == self._selected_item_key
                    ),
                    -1,
                )
                if selected_index < 0:
                    self._selection_scope = "source" if self._selected_source_id not in {"", "all"} else "all"
                    self._selected_item_key = ""
                    selected_index = 0
            selected_item = dict(rows[selected_index])
            self._syncing_tree_selection = True
            try:
                self.tree.selection_set(str(selected_index))
                self.tree.focus(str(selected_index))
            finally:
                self._syncing_tree_selection = False
            self._selected_item = dict(selected_item)
            self._render_detail(selected_item)
        else:
            if self._selection_scope == "evidence":
                self._selection_scope = "source" if self._selected_source_id not in {"", "all"} else "all"
                self._selected_item_key = ""
            self._selected_item = {}
            self._render_detail(
                {
                    "detail_text": str(self._payload.get("empty_detail") or t("results.review_center.empty")),
                    "detail_hint": str(self._payload.get("detail_hint") or t("results.review_center.detail_hint")),
                }
            )
        self._publish_selection_snapshot()

    def _on_tree_selected(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        if self._syncing_tree_selection:
            return
        selected = self.tree.selection()
        if not selected:
            return
        try:
            index = int(selected[0])
        except Exception:
            return
        filtered = [dict(item) for item in list(self._active_view.get("items", []) or [])]
        if 0 <= index < len(filtered):
            self._selection_scope = "evidence"
            self._selected_item = dict(filtered[index])
            self._selected_item_key = self._item_key(self._selected_item)
            self._render_detail(self._selected_item)
            self._publish_selection_snapshot()

    def _on_source_selected(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        if self._syncing_source_selection:
            return
        selected = self.source_tree.selection()
        if not selected:
            return
        row = dict(self._source_tree_lookup.get(str(selected[0]), {}) or {})
        source_id = str(row.get("source_id") or "").strip()
        if not source_id:
            return
        self._selected_source_id = source_id
        self._selection_scope = "source"
        self._selected_item_key = ""
        self._apply_filters()

    def _clear_source_drilldown(self) -> None:
        self._selected_source_id = "all"
        self._selection_scope = "all"
        self._selected_item_key = ""
        self._syncing_source_selection = True
        try:
            self.source_tree.selection_remove(self.source_tree.selection())
        finally:
            self._syncing_source_selection = False
        self._apply_filters()

    def clear_selection_scope(self) -> None:
        if self._selected_source_id not in {"", "all"}:
            self._clear_source_drilldown()
            return
        self._selection_scope = "all"
        self._selected_item_key = ""
        self._publish_selection_snapshot()

    def get_selection_snapshot(self) -> dict[str, Any]:
        return dict(self._selection_snapshot)

    def _sync_source_tree_selection(self) -> None:
        self._syncing_source_selection = True
        try:
            if self._selected_source_id in {"", "all"}:
                self.source_tree.selection_remove(self.source_tree.selection())
                return
            for item_id, row in self._source_tree_lookup.items():
                if str(row.get("source_id") or "").strip() == self._selected_source_id:
                    self.source_tree.selection_set(item_id)
                    self.source_tree.focus(item_id)
                    return
            self.source_tree.selection_remove(self.source_tree.selection())
        finally:
            self._syncing_source_selection = False

    def _render_detail(self, item: dict[str, Any]) -> None:
        detail = str(item.get("detail_text") or item.get("summary") or t("results.review_center.empty"))
        self.detail_section.set_summary(str(item.get("detail_hint") or item.get("type_display") or ""))
        self.detail_summary_var.set(str(item.get("detail_summary") or item.get("summary") or t("common.none")))
        self.detail_risk_var.set(str(item.get("detail_risk") or item.get("status_display") or t("common.none")))
        self.detail_key_fields_var.set(self._join_detail_lines(item.get("detail_key_fields")))
        self.detail_artifacts_var.set(self._join_detail_lines(item.get("detail_artifact_paths")))
        self.detail_acceptance_var.set(str(item.get("detail_acceptance_hint") or self._payload.get("disclaimer") or t("common.none")))
        self.detail_qc_var.set(
            "\n".join(
                line
                for line in (
                    [
                        (
                            f"{str(card.get('title') or '').strip()}: {str(card.get('summary') or '').strip()}".strip(": ")
                        )
                        for card in list(item.get("detail_qc_cards") or [])
                        if isinstance(card, dict) and (str(card.get("title") or "").strip() or str(card.get("summary") or "").strip())
                    ]
                    + [
                        str(line).strip()
                        for line in self._join_detail_lines(item.get("detail_qc_summary")).splitlines()
                        if str(line).strip()
                    ]
                )
                if line
            )
        )
        self.detail_analytics_var.set(self._join_detail_lines(item.get("detail_analytics_summary")))
        self.detail_spectral_var.set(self._join_detail_lines(item.get("detail_spectral_summary")))
        self.detail_lineage_var.set(self._join_detail_lines(item.get("detail_lineage_summary")))
        self.detail_text.configure(state="normal")
        self.detail_text.delete("1.0", "end")
        self.detail_text.insert("1.0", detail.strip() + "\n")
        self.detail_text.configure(state="disabled")

    def _publish_selection_snapshot(self) -> None:
        snapshot = build_review_center_selection_snapshot(
            self._active_view,
            scope=self._selection_scope,
            selected_item=dict(self._selected_item or {}) if self._selected_item else None,
        )
        self._selection_snapshot = dict(snapshot)
        if self._on_selection_changed_callback is not None:
            self._on_selection_changed_callback(dict(snapshot))

    @staticmethod
    def _item_key(item: dict[str, Any]) -> str:
        return "|".join(
            [
                str(item.get("source_id") or ""),
                str(item.get("type") or ""),
                str(item.get("path") or ""),
                str(item.get("generated_at") or ""),
                str(item.get("summary") or ""),
            ]
        )

    @staticmethod
    def _join_detail_lines(value: Any) -> str:
        if isinstance(value, (list, tuple)):
            lines = [str(item).strip() for item in value if str(item).strip()]
            return "\n".join(lines) if lines else t("common.none")
        text = str(value or "").strip()
        return text or t("common.none")
