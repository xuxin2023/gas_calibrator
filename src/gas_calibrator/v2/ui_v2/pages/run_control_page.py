from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import (
    display_acceptance_value,
    display_bool,
    display_compare_status,
    display_device_status,
    display_evidence_source,
    display_evidence_state,
    display_phase,
    display_reference_quality,
    display_route,
    display_run_mode,
    format_percent,
    format_pressure_hpa,
    format_temperature_c,
    t,
)
from ..widgets.route_progress_timeline import RouteProgressTimeline
from ..widgets.scrollable_page_frame import ScrollablePageFrame
from ..widgets.timeseries_chart import TimeSeriesChart


class RunControlPage(ttk.Frame):
    """Run control dashboard for the V2 main chain."""

    RUN_MODE_VALUES = (
        "auto_calibration",
        "co2_measurement",
        "h2o_measurement",
        "experiment_measurement",
    )

    def __init__(
        self,
        parent: tk.Misc,
        *,
        controller: Any,
        initial_points_path: str = "",
    ) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.controller = controller
        self.points_source_var = tk.StringVar(value="use_points_file")
        self.run_mode_var = tk.StringVar(value=display_run_mode("auto_calibration"))
        self.points_path_var = tk.StringVar(value=initial_points_path)
        self.command_status_var = tk.StringVar(value=t("common.ready"))
        self.run_id_var = tk.StringVar(value="--")
        self.phase_var = tk.StringVar(value="--")
        self.point_var = tk.StringVar(value="--")
        self.progress_var = tk.StringVar(value=format_percent(0.0))
        self.route_var = tk.StringVar(value="--")
        self.retry_var = tk.StringVar(value="0")
        self.message_var = tk.StringVar(value="--")
        self.validation_profile_var = tk.StringVar(value="--")
        self.validation_status_var = tk.StringVar(value="--")
        self.validation_failure_var = tk.StringVar(value="--")
        self.validation_evidence_var = tk.StringVar(value="--")
        self.validation_gate_var = tk.StringVar(value="--")
        self.readiness_var = tk.StringVar(value="--")
        self.analytics_var = tk.StringVar(value="--")
        self.lineage_var = tk.StringVar(value="--")
        self.points_preview_hint_var = tk.StringVar(value=t("run_control.points_preview_waiting"))
        self._device_text = tk.Text(self, height=8, wrap="word")
        self._route_text = tk.Text(self, height=8, wrap="word")
        self._validation_text = tk.Text(self, height=7, wrap="word")
        self._points_tree = ttk.Treeview(
            self,
            columns=("seq", "row", "temp", "route", "hgen", "co2", "pressure", "group", "status"),
            show="headings",
            height=7,
        )
        self.timeseries = TimeSeriesChart(self, max_points=60, height=170)
        self.route_timeline = RouteProgressTimeline(self)
        self._build()
        self._sync_points_source_widgets()
        self._refresh_points_preview()

    @classmethod
    def _run_mode_options(cls) -> dict[str, str]:
        return {display_run_mode(value): value for value in cls.RUN_MODE_VALUES}

    @staticmethod
    def _display_lookup(options: dict[str, str], value: Any, default: str) -> str:
        internal = str(value or "").strip() or default
        for label, current in options.items():
            if current == internal:
                return label
        return display_run_mode(internal, default=internal)

    @staticmethod
    def _reverse_lookup(options: dict[str, str], value: Any, default: str) -> str:
        text = str(value or "").strip()
        if not text:
            return default
        return options.get(text, text)

    def _current_run_mode_value(self) -> str:
        return self._reverse_lookup(self._run_mode_options(), self.run_mode_var.get(), "auto_calibration")

    def _build(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.page_scaffold = ScrollablePageFrame(self, padding=12)
        self.page_scaffold.grid(row=0, column=0, sticky="nsew")
        body = self.page_scaffold.content
        body.columnconfigure(0, weight=1)
        body.rowconfigure(3, weight=1)
        body.rowconfigure(5, weight=1)

        command_bar = ttk.Frame(body, style="Card.TFrame")
        command_bar.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        command_bar.columnconfigure(1, weight=1)
        ttk.Label(command_bar, text=t("run_control.source"), style="Muted.TLabel").grid(
            row=0,
            column=0,
            sticky="w",
            padx=(0, 8),
        )
        ttk.Radiobutton(
            command_bar,
            text=t("run_control.points_file"),
            variable=self.points_source_var,
            value="use_points_file",
            command=self._on_points_source_changed,
        ).grid(row=0, column=1, sticky="w")
        ttk.Radiobutton(
            command_bar,
            text=t("run_control.default_profile"),
            variable=self.points_source_var,
            value="use_default_profile",
            command=self._on_points_source_changed,
        ).grid(row=0, column=2, sticky="w", padx=(12, 0))
        ttk.Label(command_bar, text=t("run_control.mode"), style="Muted.TLabel").grid(
            row=0,
            column=3,
            sticky="e",
            padx=(12, 8),
        )
        self.run_mode_combo = ttk.Combobox(
            command_bar,
            textvariable=self.run_mode_var,
            values=tuple(self._run_mode_options().keys()),
            state="readonly",
            width=20,
        )
        self.run_mode_combo.grid(row=0, column=4, sticky="w")
        self.run_mode_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_points_preview())
        ttk.Label(command_bar, text=t("run_control.points"), style="Section.TLabel").grid(
            row=1,
            column=0,
            sticky="w",
            padx=(0, 8),
            pady=(6, 0),
        )
        self.points_entry = ttk.Entry(command_bar, textvariable=self.points_path_var)
        self.points_entry.grid(row=1, column=1, columnspan=2, sticky="ew", pady=(6, 0))
        ttk.Button(command_bar, text=t("run_control.preview"), command=self._refresh_points_preview).grid(
            row=1,
            column=3,
            padx=(8, 4),
            pady=(6, 0),
        )
        self.edit_points_button = ttk.Button(
            command_bar,
            text=t("run_control.edit_points"),
            command=self._on_edit_points,
        )
        self.edit_points_button.grid(row=1, column=4, padx=4, pady=(6, 0))
        ttk.Button(command_bar, text=t("run_control.start"), style="Accent.TButton", command=self._on_start).grid(
            row=1,
            column=5,
            padx=(8, 4),
            pady=(6, 0),
        )
        ttk.Button(command_bar, text=t("run_control.pause"), command=self._on_pause).grid(
            row=1,
            column=6,
            padx=4,
            pady=(6, 0),
        )
        ttk.Button(command_bar, text=t("run_control.resume"), command=self._on_resume).grid(
            row=1,
            column=7,
            padx=4,
            pady=(6, 0),
        )
        ttk.Button(command_bar, text=t("run_control.stop"), command=self._on_stop).grid(
            row=1,
            column=8,
            padx=(4, 0),
            pady=(6, 0),
        )

        summary = ttk.Frame(body, style="Card.TFrame")
        summary.grid(row=1, column=0, sticky="ew", pady=(0, 12))
        for column in range(4):
            summary.columnconfigure(column, weight=1)
        self._metric(summary, 0, 0, t("shell.metric.run_id"), self.run_id_var)
        self._metric(summary, 0, 1, t("shell.metric.phase"), self.phase_var)
        self._metric(summary, 0, 2, t("run_control.current_point"), self.point_var)
        self._metric(summary, 0, 3, t("shell.metric.progress"), self.progress_var)
        self._metric(summary, 1, 0, t("shell.metric.route"), self.route_var)
        self._metric(summary, 1, 1, t("run_control.retry"), self.retry_var)
        self._metric(summary, 1, 2, t("run_control.command"), self.command_status_var)
        self._metric(summary, 1, 3, t("shell.metric.message"), self.message_var)

        validation = ttk.Frame(body, style="Card.TFrame")
        validation.grid(row=2, column=0, sticky="ew", pady=(0, 12))
        for column in range(5):
            validation.columnconfigure(column, weight=1)
        ttk.Label(validation, text=t("run_control.validation_title"), style="Section.TLabel").grid(
            row=0,
            column=0,
            sticky="w",
            pady=(0, 6),
        )
        self._metric(validation, 1, 0, t("run_control.validation_profile"), self.validation_profile_var)
        self._metric(validation, 1, 1, t("run_control.compare_status"), self.validation_status_var)
        self._metric(validation, 1, 2, t("run_control.first_failure"), self.validation_failure_var)
        self._metric(validation, 1, 3, t("run_control.evidence"), self.validation_evidence_var)
        self._metric(validation, 1, 4, t("run_control.gate_state"), self.validation_gate_var)
        self._metric(validation, 2, 0, t("run_control.readiness"), self.readiness_var)
        self._metric(validation, 2, 1, t("run_control.analytics"), self.analytics_var)
        self._metric(validation, 2, 2, t("run_control.lineage"), self.lineage_var)
        self._validation_text.grid(in_=validation, row=3, column=0, columnspan=5, sticky="ew", pady=(8, 0))

        detail_body = ttk.Frame(body, style="Card.TFrame")
        detail_body.grid(row=3, column=0, sticky="nsew")
        detail_body.columnconfigure(0, weight=1)
        detail_body.columnconfigure(1, weight=1)
        detail_body.rowconfigure(1, weight=1)
        ttk.Label(detail_body, text=t("run_control.device_status"), style="Section.TLabel").grid(
            row=0,
            column=0,
            sticky="w",
            pady=(0, 6),
        )
        ttk.Label(detail_body, text=t("run_control.route_state"), style="Section.TLabel").grid(
            row=0,
            column=1,
            sticky="w",
            pady=(0, 6),
            padx=(12, 0),
        )
        self._device_text.grid(in_=detail_body, row=1, column=0, sticky="nsew")
        self._route_text.grid(in_=detail_body, row=1, column=1, sticky="nsew", padx=(12, 0))

        charts = ttk.Frame(body, style="Card.TFrame")
        charts.grid(row=4, column=0, sticky="nsew", pady=(12, 0))
        charts.columnconfigure(0, weight=1)
        charts.columnconfigure(1, weight=1)
        self.timeseries.grid(in_=charts, row=0, column=0, sticky="nsew", padx=(0, 6))
        self.route_timeline.grid(in_=charts, row=0, column=1, sticky="nsew", padx=(6, 0))

        preview = ttk.Frame(body, style="Card.TFrame")
        preview.grid(row=5, column=0, sticky="nsew", pady=(12, 0))
        preview.columnconfigure(0, weight=1)
        preview.rowconfigure(1, weight=1)
        ttk.Label(preview, text=t("run_control.execution_preview"), style="Section.TLabel").grid(
            row=0,
            column=0,
            sticky="w",
            pady=(0, 6),
        )
        ttk.Label(preview, textvariable=self.points_preview_hint_var, style="Muted.TLabel").grid(
            row=0,
            column=1,
            sticky="e",
            padx=(12, 0),
        )
        tree_shell = ttk.Frame(preview, style="Card.TFrame")
        tree_shell.grid(row=1, column=0, columnspan=2, sticky="nsew")
        tree_shell.columnconfigure(0, weight=1)
        tree_shell.rowconfigure(0, weight=1)
        tree_scroll_y = ttk.Scrollbar(tree_shell, orient="vertical", command=self._points_tree.yview)
        tree_scroll_x = ttk.Scrollbar(tree_shell, orient="horizontal", command=self._points_tree.xview)
        self._points_tree.configure(yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set)
        self._points_tree.grid(in_=tree_shell, row=0, column=0, sticky="nsew")
        tree_scroll_y.grid(row=0, column=1, sticky="ns")
        tree_scroll_x.grid(row=1, column=0, sticky="ew")
        for column, title, width, anchor in (
            ("seq", t("run_control.tree.seq"), 60, "center"),
            ("row", t("run_control.tree.row"), 70, "center"),
            ("temp", t("run_control.tree.temp"), 80, "center"),
            ("route", t("run_control.tree.route"), 70, "center"),
            ("hgen", t("run_control.tree.h2o_target"), 190, "w"),
            ("co2", "CO2", 100, "center"),
            ("pressure", t("run_control.tree.pressure"), 100, "center"),
            ("group", t("run_control.tree.group"), 70, "center"),
            ("status", t("run_control.tree.status"), 220, "w"),
        ):
            self._points_tree.heading(column, text=title)
            self._points_tree.column(column, width=width, anchor=anchor, stretch=column in {"hgen", "status"})

    @staticmethod
    def _metric(parent: tk.Misc, row: int, column: int, label: str, variable: tk.StringVar) -> None:
        frame = ttk.Frame(parent, style="SoftCard.TFrame", padding=8)
        frame.grid(row=row, column=column, sticky="nsew", padx=4, pady=4)
        frame.columnconfigure(0, weight=1)
        ttk.Label(frame, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(frame, textvariable=variable, style="Section.TLabel").grid(row=1, column=0, sticky="w")

    def render(self, snapshot: dict[str, Any]) -> None:
        results = dict(snapshot.get("results", {}) or {})
        self.run_id_var.set(str(snapshot.get("run_id", "--") or "--"))
        self.phase_var.set(str(snapshot.get("phase_display") or display_phase(snapshot.get("phase", "--")) or "--"))
        self.point_var.set(str(snapshot.get("current_point", "--") or "--"))
        self.progress_var.set(format_percent(float(snapshot.get("progress_pct", 0.0) or 0.0)))
        self.route_var.set(str(snapshot.get("route_display") or display_route(snapshot.get("route", "--")) or "--"))
        self.retry_var.set(str(snapshot.get("retry", 0) or 0))
        self.message_var.set(str(snapshot.get("message_display", snapshot.get("message", "--")) or "--"))

        validation = dict(snapshot.get("validation", {}) or {})
        gate_state = dict(validation.get("gate_state", {}) or {})
        gate_bits = [
            str(gate_state.get("checklist_gate_display") or gate_state.get("checklist_gate") or "--"),
            str(gate_state.get("target_route_display") or display_route(gate_state.get("target_route") or "--")),
            t("run_control.single_temp") if bool(gate_state.get("single_temp", False)) else t("run_control.multi_temp"),
        ]
        self.validation_profile_var.set(str(validation.get("validation_profile", "--") or "--"))
        self.validation_status_var.set(
            str(
                validation.get("compare_status_display")
                or display_compare_status(validation.get("compare_status", "--"))
                or "--"
            )
        )
        self.validation_failure_var.set(
            str(validation.get("first_failure_phase_display", validation.get("first_failure_phase", "--")) or "--")
        )
        evidence_bits = [
            str(
                validation.get("evidence_source_display")
                or display_evidence_source(validation.get("evidence_source", "--"))
                or "--"
            ),
            str(
                validation.get("evidence_state_display")
                or display_evidence_state(validation.get("evidence_state", "--"))
                or "--"
            ),
            display_acceptance_value("diagnostic" if bool(validation.get("diagnostic_only", False)) else "acceptance"),
        ]
        if bool(validation.get("primary_real_latest_missing", False) or validation.get("primary_latest_missing", False)):
            evidence_bits.append(t("run_control.primary_missing"))
        self.validation_evidence_var.set(" / ".join(evidence_bits))
        self.validation_gate_var.set(" / ".join(gate_bits))
        self.readiness_var.set(
            str(
                dict(validation.get("readiness_summary") or results.get("acceptance_readiness_summary") or {}).get(
                    "summary_display",
                    dict(validation.get("readiness_summary") or results.get("acceptance_readiness_summary") or {}).get(
                        "summary",
                        "--",
                    ),
                )
                or "--"
            )
        )
        self.analytics_var.set(
            str(
                dict(results.get("analytics_summary_digest") or {}).get(
                    "summary_display",
                    dict(results.get("analytics_summary_digest") or {}).get("summary", "--"),
                )
                or "--"
            )
        )
        lineage_digest = dict(results.get("lineage_digest") or {})
        self.lineage_var.set(
            " / ".join(
                [
                    str(lineage_digest.get("config_version") or "--"),
                    str(lineage_digest.get("points_version") or "--"),
                    str(lineage_digest.get("profile_version") or "--"),
                ]
            )
        )
        self._set_text(
            self._device_text,
            self._format_devices(snapshot.get("device_rows", []), snapshot.get("disabled_analyzers", [])),
        )
        self._set_text(self._route_text, self._format_route(snapshot))
        self._set_text(self._validation_text, self._format_validation(validation))
        self.timeseries.set_series(dict(snapshot.get("timeseries", {}).get("series", {}) or {}))
        self.route_timeline.render(dict(snapshot.get("route_progress", {}) or {}))
        self.page_scaffold._update_scroll_region()

    def _on_start(self) -> None:
        ok, message = self.controller.start(
            self.points_path_var.get().strip() or None,
            points_source=self.points_source_var.get(),
            run_mode=self._current_run_mode_value(),
        )
        self.command_status_var.set(t("run_control.command_status.start") if ok else t("run_control.command_status.start_failed"))
        self.message_var.set(message)

    def _on_edit_points(self) -> None:
        ok, message = self.controller.edit_points_file(
            self.points_path_var.get().strip() or None,
            points_source=self.points_source_var.get(),
        )
        self.command_status_var.set(t("run_control.command_status.edit") if ok else t("run_control.command_status.edit_failed"))
        self.message_var.set(message)

    def _on_pause(self) -> None:
        ok, message = self.controller.pause()
        self.command_status_var.set(t("run_control.command_status.paused") if ok else t("run_control.command_status.pause_failed"))
        self.message_var.set(message)

    def _on_resume(self) -> None:
        ok, message = self.controller.resume()
        self.command_status_var.set(t("run_control.command_status.resumed") if ok else t("run_control.command_status.resume_failed"))
        self.message_var.set(message)

    def _on_stop(self) -> None:
        ok, message = self.controller.stop()
        self.command_status_var.set(t("run_control.command_status.stopped") if ok else t("run_control.command_status.stop_failed"))
        self.message_var.set(message)

    def _refresh_points_preview(self) -> None:
        preview = self.controller.preview_points(
            self.points_path_var.get().strip() or None,
            points_source=self.points_source_var.get(),
            run_mode=self._current_run_mode_value(),
        )
        for item in self._points_tree.get_children():
            self._points_tree.delete(item)
        if not bool(preview.get("ok", False)):
            self.points_preview_hint_var.set(str(preview.get("summary", t("run_control.points_preview_failed"))))
            return
        if preview.get("run_mode"):
            self.run_mode_var.set(
                self._display_lookup(self._run_mode_options(), preview.get("run_mode"), "auto_calibration")
            )

        for row in list(preview.get("rows", []) or []):
            self._points_tree.insert(
                "",
                "end",
                values=(
                    row.get("seq", ""),
                    row.get("row", ""),
                    row.get("temp", ""),
                    row.get("route", ""),
                    row.get("hgen", ""),
                    row.get("co2", ""),
                    row.get("pressure", ""),
                    row.get("group", ""),
                    row.get("status", ""),
                ),
            )
        self.points_preview_hint_var.set(str(preview.get("summary", t("run_control.points_preview_refreshed"))))

    def _on_points_source_changed(self) -> None:
        self._sync_points_source_widgets()
        self._refresh_points_preview()

    def _sync_points_source_widgets(self) -> None:
        using_points_file = self.points_source_var.get() == "use_points_file"
        self.points_entry.configure(state="normal" if using_points_file else "disabled")
        self.edit_points_button.configure(state="normal" if using_points_file else "disabled")
        self.run_mode_combo.configure(state="readonly" if using_points_file else "disabled")

    @staticmethod
    def _set_text(widget: tk.Text, text: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", text.strip() + "\n")
        widget.configure(state="disabled")

    @staticmethod
    def _format_devices(rows: list[dict[str, Any]], disabled: list[str]) -> str:
        if not rows and not disabled:
            return t("run_control.no_device_data")
        lines = [
            f"{row.get('name', '--')}: {row.get('status_display') or display_device_status(row.get('status', '--'))} {row.get('port', '')}".rstrip()
            for row in rows
        ]
        if disabled:
            lines.append("")
            lines.append(t("run_control.disabled_analyzers"))
            lines.extend(f"- {item}" for item in disabled)
        return "\n".join(lines)

    def _format_route(self, snapshot: dict[str, Any]) -> str:
        lines = [
            f"{t('run_control.detail.route')}: {snapshot.get('route_display') or display_route(snapshot.get('route', '--')) or '--'}",
            f"{t('run_control.detail.phase')}: {snapshot.get('route_phase_display') or display_phase(snapshot.get('route_phase', '--')) or '--'}",
            f"{t('run_control.detail.source_point')}: {snapshot.get('source_point', '--') or '--'}",
            f"{t('run_control.detail.active_point')}: {snapshot.get('active_point', '--') or '--'}",
            f"{t('run_control.detail.point_tag')}: {snapshot.get('point_tag', '--') or '--'}",
            f"{t('run_control.detail.retry')}: {snapshot.get('retry', 0) or 0}",
        ]
        route_state = dict(snapshot.get("route_state", {}) or {})
        if not route_state:
            lines.append(f"{t('run_control.detail.route_state')}: {t('suite.none')}")
            return "\n".join(lines)
        lines.append(f"{t('run_control.detail.route_state')}:")
        for key, value in route_state.items():
            lines.append(
                f"- {self._route_state_key_label(str(key))}: {self._format_route_state_value(str(key), value)}"
            )
        return "\n".join(lines)

    def _format_validation(self, snapshot: dict[str, Any]) -> str:
        if not snapshot or not bool(snapshot.get("available", False)):
            return t("run_control.no_validation")

        lines = [
            f"{t('run_control.detail.profile')}: {snapshot.get('validation_profile', '--') or '--'}",
            f"{t('run_control.detail.compare_status')}: {snapshot.get('compare_status_display') or display_compare_status(snapshot.get('compare_status', '--')) or '--'}",
            f"{t('run_control.detail.evidence_source')}: {snapshot.get('evidence_source_display') or display_evidence_source(snapshot.get('evidence_source', '--')) or '--'}",
            f"{t('run_control.detail.evidence_state')}: {snapshot.get('evidence_state_display') or display_evidence_state(snapshot.get('evidence_state', '--')) or '--'}",
            f"{t('run_control.detail.acceptance_level')}: {snapshot.get('acceptance_level_display') or display_acceptance_value(snapshot.get('acceptance_level', '--')) or '--'}",
            f"{t('run_control.detail.acceptance_scope')}: {snapshot.get('acceptance_scope_display') or display_acceptance_value(snapshot.get('acceptance_scope', '--')) or '--'}",
            f"{t('run_control.detail.promotion_state')}: {snapshot.get('promotion_state_display') or display_acceptance_value(snapshot.get('promotion_state', '--')) or '--'}",
            f"{t('run_control.detail.review_state')}: {snapshot.get('review_state_display') or display_acceptance_value(snapshot.get('review_state', '--')) or '--'}",
            f"{t('run_control.detail.approval_state')}: {snapshot.get('approval_state_display') or display_acceptance_value(snapshot.get('approval_state', '--')) or '--'}",
            f"{t('run_control.detail.ready_for_promotion')}: {display_bool(bool(snapshot.get('ready_for_promotion', False)))}",
            f"{t('run_control.detail.first_failure_phase')}: {snapshot.get('first_failure_phase_display') or snapshot.get('first_failure_phase', '--') or '--'}",
            f"{t('run_control.detail.gate')}: {self.validation_gate_var.get() or '--'}",
        ]

        missing_conditions = list(snapshot.get("missing_conditions_display") or snapshot.get("missing_conditions") or [])
        if missing_conditions:
            lines.append(f"{t('run_control.detail.missing_conditions')}:")
            lines.extend(f"- {item}" for item in missing_conditions)
        else:
            lines.append(f"{t('run_control.detail.missing_conditions')}: {t('suite.none')}")

        reference_quality = dict(snapshot.get("reference_quality", {}) or {})
        if reference_quality:
            lines.append(
                f"{t('run_control.detail.reference_quality')}: "
                f"{display_reference_quality(reference_quality.get('reference_quality', '--'), default=str(reference_quality.get('reference_quality', '--')))}"
            )
            lines.append(
                f"{t('run_control.detail.thermometer_reference')}: "
                f"{display_reference_quality(reference_quality.get('thermometer_reference_status', '--'), default=str(reference_quality.get('thermometer_reference_status', '--')))}"
            )
            lines.append(
                f"{t('run_control.detail.pressure_reference')}: "
                f"{display_reference_quality(reference_quality.get('pressure_reference_status', '--'), default=str(reference_quality.get('pressure_reference_status', '--')))}"
            )

        route_validation = dict(snapshot.get("route_physical_validation", {}) or {})
        if route_validation:
            route_match = dict(route_validation.get("route_physical_state_match", {}) or {})
            relay_mismatch = dict(route_validation.get("relay_physical_mismatch", {}) or {})
            lines.append(
                f"{t('run_control.detail.route_physical_match')}: "
                f"V1={display_bool(bool(route_match.get('v1', False)))} / V2={display_bool(bool(route_match.get('v2', False)))}"
            )
            lines.append(
                f"{t('run_control.detail.relay_physical_mismatch')}: "
                f"V1={display_bool(bool(relay_mismatch.get('v1', False)))} / V2={display_bool(bool(relay_mismatch.get('v2', False)))}"
            )

        evidence_layers = list(snapshot.get("evidence_layers", []) or [])
        if evidence_layers:
            lines.append(f"{t('run_control.detail.evidence_layers')}:")
            for layer in evidence_layers:
                tier = str(layer.get("tier", "--") or "--")
                tier_display = display_acceptance_value(tier, default=tier)
                lines.append(
                    "- "
                    + t(
                        "run_control.detail.layer_line",
                        tier=tier_display,
                        profile=str(layer.get("validation_profile", "--") or "--"),
                        status=display_compare_status(layer.get("compare_status", "--"), default=str(layer.get("compare_status", "--"))),
                        evidence_source=display_evidence_source(layer.get("evidence_source", "--"), default=str(layer.get("evidence_source", "--"))),
                        evidence_state=display_evidence_state(layer.get("evidence_state", "--"), default=str(layer.get("evidence_state", "--"))),
                    )
                )

        fallback_candidates = list(snapshot.get("fallback_candidates", []) or [])
        if fallback_candidates:
            lines.append(f"{t('run_control.detail.fallback_candidates')}:")
            for item in fallback_candidates:
                lines.append(
                    "- "
                    + t(
                        "run_control.detail.fallback_line",
                        profile=str(item.get("validation_profile", "--") or "--"),
                        status=display_compare_status(item.get("compare_status", "--"), default=str(item.get("compare_status", "--"))),
                        evidence_state=display_evidence_state(item.get("evidence_state", "--"), default=str(item.get("evidence_state", "--"))),
                    )
                )

        artifact_bundle_path = str(snapshot.get("artifact_bundle_path", "") or "")
        if artifact_bundle_path:
            lines.append(f"{t('run_control.detail.artifact_bundle')}: {artifact_bundle_path}")
        report_dir = str(snapshot.get("report_dir", "") or "")
        if report_dir:
            lines.append(f"{t('run_control.detail.report_dir')}: {report_dir}")

        return "\n".join(lines)

    @staticmethod
    def _route_state_key_label(key: str) -> str:
        default = key.replace("_", " ")
        return t(f"run_control.route_state_key.{key}", default=default)

    @staticmethod
    def _format_route_state_value(key: str, value: Any) -> str:
        if isinstance(value, bool):
            return display_bool(value)
        if isinstance(value, list):
            return ", ".join(str(item) for item in value) if value else t("suite.none")
        if key in {"active_subroute"}:
            return display_route(value, default=str(value or "--"))
        if key.endswith("_hpa"):
            return format_pressure_hpa(value)
        if key.endswith("_c"):
            return format_temperature_c(value)
        return str(value if value not in (None, "") else "--")
