from __future__ import annotations

import copy
import math
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk
from typing import Any, Optional

from ...domain.mode_models import RunMode
from ..i18n import (
    display_analyzer_software_version,
    display_device_id_assignment,
    display_run_mode,
    t,
)
from ..widgets.scrollable_page_frame import ScrollablePageFrame


class PlanEditorPage(ttk.Frame):
    """Basic UI for editable calibration plan profiles and compile preview."""

    def __init__(self, parent: tk.Misc, *, facade: Any) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.facade = facade
        self.plan_gateway = facade.get_plan_gateway() if hasattr(facade, "get_plan_gateway") else getattr(facade, "plan_gateway")
        self.profile_name_var = tk.StringVar(value="")
        self.profile_version_var = tk.StringVar(value="1.0")
        self.description_var = tk.StringVar(value="")
        self.run_mode_var = tk.StringVar(value=display_run_mode(RunMode.AUTO_CALIBRATION.value))
        self.analyzer_version_var = tk.StringVar(value=display_analyzer_software_version("v5_plus"))
        self.device_id_assignment_var = tk.StringVar(value=display_device_id_assignment("automatic"))
        self.start_device_id_var = tk.StringVar(value="001")
        self.manual_device_ids_var = tk.StringVar(value="")
        self.selected_temps_var = tk.StringVar(value="")
        self.skip_co2_var = tk.StringVar(value="")
        self.water_first_var = tk.BooleanVar(value=False)
        self.water_first_temp_gte_var = tk.StringVar(value="")
        self.temperature_descending_var = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value=t("pages.plan_editor.status.ready"))
        self.default_status_var = tk.StringVar(value=t("pages.plan_editor.status.draft_profile"))
        self.preview_summary_var = tk.StringVar(value=t("pages.plan_editor.preview.not_generated"))

        self.temp_value_var = tk.StringVar(value="")
        self.temp_enabled_var = tk.BooleanVar(value=True)
        self.humidity_temp_var = tk.StringVar(value="")
        self.humidity_rh_var = tk.StringVar(value="")
        self.humidity_dew_var = tk.StringVar(value="")
        self.humidity_enabled_var = tk.BooleanVar(value=True)
        self.gas_value_var = tk.StringVar(value="")
        self.gas_group_var = tk.StringVar(value="A")
        self.gas_cylinder_var = tk.StringVar(value="")
        self.gas_enabled_var = tk.BooleanVar(value=True)
        self.pressure_value_var = tk.StringVar(value="")
        self.pressure_enabled_var = tk.BooleanVar(value=True)

        self._profile_names: list[str] = []
        self._temperature_rows: list[dict[str, Any]] = []
        self._humidity_rows: list[dict[str, Any]] = []
        self._gas_rows: list[dict[str, Any]] = []
        self._pressure_rows: list[dict[str, Any]] = []
        self._is_default_profile = False

        self._build()
        self._load_initial_state()

    def render(self, snapshot: dict[str, Any]) -> None:
        _ = snapshot

    @staticmethod
    def _reverse_lookup(options: dict[str, str], value: Any, default: str) -> str:
        text = str(value or "").strip()
        if not text:
            return default
        if text in options.values():
            return text
        return options.get(text, default)

    @staticmethod
    def _display_lookup(options: dict[str, str], value: Any, default: str) -> str:
        internal = str(value or "").strip() or default
        for label, current in options.items():
            if current == internal:
                return label
        return internal

    def _set_status(self, key: str, **kwargs: Any) -> None:
        self.status_var.set(t(f"pages.plan_editor.status.{key}", **kwargs))

    def _set_preview_summary(self, key: str, **kwargs: Any) -> None:
        self.preview_summary_var.set(t(f"pages.plan_editor.preview.{key}", **kwargs))

    @classmethod
    def _run_mode_options(cls) -> dict[str, str]:
        return {
            display_run_mode(RunMode.AUTO_CALIBRATION.value): RunMode.AUTO_CALIBRATION.value,
            display_run_mode(RunMode.CO2_MEASUREMENT.value): RunMode.CO2_MEASUREMENT.value,
            display_run_mode(RunMode.H2O_MEASUREMENT.value): RunMode.H2O_MEASUREMENT.value,
            display_run_mode(RunMode.EXPERIMENT_MEASUREMENT.value): RunMode.EXPERIMENT_MEASUREMENT.value,
        }

    @classmethod
    def _analyzer_version_options(cls) -> dict[str, str]:
        return {
            display_analyzer_software_version("pre_v5"): "pre_v5",
            display_analyzer_software_version("v5_plus"): "v5_plus",
        }

    @classmethod
    def _device_assignment_options(cls) -> dict[str, str]:
        return {
            display_device_id_assignment("automatic"): "automatic",
            display_device_id_assignment("manual"): "manual",
        }

    def _build(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.page_scaffold = ScrollablePageFrame(self, padding=12)
        self.page_scaffold.grid(row=0, column=0, sticky="nsew")
        body = self.page_scaffold.content
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)

        left = ttk.Frame(body, style="Card.TFrame")
        left.grid(row=0, column=0, sticky="nsw", padx=(0, 12))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)
        ttk.Label(left, text=t("pages.plan_editor.sidebar.profiles"), style="Section.TLabel").grid(
            row=0, column=0, sticky="w", pady=(0, 6)
        )
        self.profile_listbox = tk.Listbox(left, height=12, exportselection=False)
        self.profile_listbox.grid(row=1, column=0, sticky="nsew")
        self.profile_listbox.bind("<<ListboxSelect>>", self._on_profile_select)
        profile_scroll = ttk.Scrollbar(left, orient="vertical", command=self.profile_listbox.yview)
        profile_scroll.grid(row=1, column=1, sticky="ns")
        self.profile_listbox.configure(yscrollcommand=profile_scroll.set)

        profile_actions = ttk.Frame(left, style="Card.TFrame")
        profile_actions.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for column in range(2):
            profile_actions.columnconfigure(column, weight=1)
        ttk.Button(profile_actions, text=t("pages.plan_editor.actions.load"), command=self._load_selected_profile).grid(
            row=0, column=0, sticky="ew", padx=(0, 4), pady=2
        )
        ttk.Button(profile_actions, text=t("pages.plan_editor.actions.save"), command=self._save_profile).grid(
            row=0, column=1, sticky="ew", padx=(4, 0), pady=2
        )
        ttk.Button(profile_actions, text=t("pages.plan_editor.actions.save_as"), command=self._save_profile_as).grid(
            row=1, column=0, sticky="ew", padx=(0, 4), pady=2
        )
        ttk.Button(profile_actions, text=t("pages.plan_editor.actions.delete"), command=self._delete_selected_profile).grid(
            row=1, column=1, sticky="ew", padx=(4, 0), pady=2
        )
        ttk.Button(profile_actions, text=t("pages.plan_editor.actions.set_default"), command=self._set_default_profile).grid(
            row=2, column=0, columnspan=2, sticky="ew", pady=2
        )
        ttk.Button(profile_actions, text=t("pages.plan_editor.actions.new"), command=self._new_profile).grid(
            row=3, column=0, columnspan=2, sticky="ew", pady=2
        )
        ttk.Button(profile_actions, text=t("pages.plan_editor.actions.duplicate"), command=self._duplicate_selected_profile).grid(
            row=4, column=0, sticky="ew", padx=(0, 4), pady=2
        )
        ttk.Button(profile_actions, text=t("pages.plan_editor.actions.rename"), command=self._rename_selected_profile).grid(
            row=4, column=1, sticky="ew", padx=(4, 0), pady=2
        )
        ttk.Button(profile_actions, text=t("pages.plan_editor.actions.import"), command=self._import_profile).grid(
            row=5, column=0, sticky="ew", padx=(0, 4), pady=2
        )
        ttk.Button(profile_actions, text=t("pages.plan_editor.actions.export"), command=self._export_selected_profile).grid(
            row=5, column=1, sticky="ew", padx=(4, 0), pady=2
        )

        right = ttk.Frame(body, style="Card.TFrame")
        right.grid(row=0, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(4, weight=1)

        meta = ttk.Frame(right, style="Card.TFrame")
        meta.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        meta.columnconfigure(1, weight=1)
        ttk.Label(meta, text=t("pages.plan_editor.meta.title"), style="Title.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 8)
        )
        ttk.Label(meta, text=t("pages.plan_editor.meta.name"), style="Muted.TLabel").grid(
            row=1, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Entry(meta, textvariable=self.profile_name_var).grid(row=1, column=1, sticky="ew")
        ttk.Label(meta, text=t("pages.plan_editor.meta.version"), style="Muted.TLabel").grid(
            row=2, column=0, sticky="w", padx=(0, 8), pady=(6, 0)
        )
        ttk.Entry(meta, textvariable=self.profile_version_var).grid(row=2, column=1, sticky="ew", pady=(6, 0))
        ttk.Label(meta, text=t("pages.plan_editor.meta.description"), style="Muted.TLabel").grid(
            row=3, column=0, sticky="w", padx=(0, 8), pady=(6, 0)
        )
        ttk.Entry(meta, textvariable=self.description_var).grid(row=3, column=1, sticky="ew", pady=(6, 0))
        ttk.Label(meta, text=t("pages.plan_editor.meta.run_mode"), style="Muted.TLabel").grid(
            row=4, column=0, sticky="w", padx=(0, 8), pady=(6, 0)
        )
        ttk.Combobox(
            meta,
            textvariable=self.run_mode_var,
            values=tuple(self._run_mode_options().keys()),
            state="readonly",
        ).grid(row=4, column=1, sticky="ew", pady=(6, 0))
        ttk.Label(meta, textvariable=self.default_status_var, style="Muted.TLabel").grid(row=5, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Label(meta, textvariable=self.status_var, style="Muted.TLabel").grid(row=6, column=0, columnspan=2, sticky="w", pady=(4, 0))

        options = ttk.LabelFrame(right, text=t("pages.plan_editor.options.title"), style="Card.TFrame", padding=8)
        options.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        for column in range(4):
            options.columnconfigure(column, weight=1)
        ttk.Label(options, text=t("pages.plan_editor.options.selected_temps"), style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(options, textvariable=self.selected_temps_var).grid(row=0, column=1, sticky="ew", padx=(4, 12))
        ttk.Label(options, text=t("pages.plan_editor.options.skip_co2"), style="Muted.TLabel").grid(row=0, column=2, sticky="w")
        ttk.Entry(options, textvariable=self.skip_co2_var).grid(row=0, column=3, sticky="ew", padx=(4, 0))
        ttk.Checkbutton(options, text=t("pages.plan_editor.options.water_first"), variable=self.water_first_var).grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Label(options, text=t("pages.plan_editor.options.water_first_temp_gte"), style="Muted.TLabel").grid(
            row=1, column=1, sticky="w", pady=(8, 0)
        )
        ttk.Entry(options, textvariable=self.water_first_temp_gte_var).grid(row=1, column=2, sticky="ew", padx=(4, 12), pady=(8, 0))
        ttk.Checkbutton(options, text=t("pages.plan_editor.options.temperature_descending"), variable=self.temperature_descending_var).grid(row=1, column=3, sticky="w", pady=(8, 0))

        analyzer_setup = ttk.LabelFrame(right, text=t("pages.plan_editor.analyzer.title"), style="Card.TFrame", padding=8)
        analyzer_setup.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        for column in range(4):
            analyzer_setup.columnconfigure(column, weight=1)
        ttk.Label(analyzer_setup, text=t("pages.plan_editor.analyzer.software_version"), style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            analyzer_setup,
            textvariable=self.analyzer_version_var,
            values=tuple(self._analyzer_version_options().keys()),
            state="readonly",
        ).grid(row=0, column=1, sticky="ew", padx=(4, 12))
        ttk.Label(analyzer_setup, text=t("pages.plan_editor.analyzer.id_assignment"), style="Muted.TLabel").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            analyzer_setup,
            textvariable=self.device_id_assignment_var,
            values=tuple(self._device_assignment_options().keys()),
            state="readonly",
        ).grid(row=0, column=3, sticky="ew", padx=(4, 0))
        ttk.Label(analyzer_setup, text=t("pages.plan_editor.analyzer.start_id"), style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(analyzer_setup, textvariable=self.start_device_id_var).grid(row=1, column=1, sticky="ew", padx=(4, 12), pady=(8, 0))
        ttk.Label(analyzer_setup, text=t("pages.plan_editor.analyzer.manual_ids"), style="Muted.TLabel").grid(row=1, column=2, sticky="w", pady=(8, 0))
        ttk.Entry(analyzer_setup, textvariable=self.manual_device_ids_var).grid(row=1, column=3, sticky="ew", padx=(4, 0), pady=(8, 0))

        sections = ttk.Frame(right, style="Card.TFrame")
        sections.grid(row=3, column=0, sticky="ew", pady=(0, 10))
        sections.columnconfigure(0, weight=1)
        sections.columnconfigure(1, weight=1)
        self._build_temperature_section(sections).grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        self._build_humidity_section(sections).grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        self._build_gas_section(sections).grid(row=1, column=0, sticky="nsew", padx=(0, 6), pady=(12, 0))
        self._build_pressure_section(sections).grid(row=1, column=1, sticky="nsew", padx=(6, 0), pady=(12, 0))

        preview = ttk.LabelFrame(right, text=t("pages.plan_editor.preview.title"), style="Card.TFrame", padding=8)
        preview.grid(row=4, column=0, sticky="nsew")
        preview.columnconfigure(0, weight=1)
        preview.rowconfigure(2, weight=1)
        action_bar = ttk.Frame(preview, style="Card.TFrame")
        action_bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        action_bar.columnconfigure(0, weight=1)
        ttk.Button(action_bar, text=t("pages.plan_editor.preview.compile"), style="Accent.TButton", command=self._compile_preview).grid(row=0, column=0, sticky="w")
        ttk.Label(preview, textvariable=self.preview_summary_var, style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(0, 6))
        self.preview_tree = ttk.Treeview(
            preview,
            columns=("seq", "row", "temp", "route", "hgen", "co2", "pressure", "group", "cylinder", "status"),
            show="headings",
            height=7,
        )
        self.preview_tree.grid(row=2, column=0, sticky="nsew")
        preview_scroll = ttk.Scrollbar(preview, orient="vertical", command=self.preview_tree.yview)
        preview_scroll.grid(row=2, column=1, sticky="ns")
        self.preview_tree.configure(yscrollcommand=preview_scroll.set)
        for column, title, width, anchor in (
            ("seq", t("pages.plan_editor.preview_columns.seq"), 60, "center"),
            ("row", t("pages.plan_editor.preview_columns.row"), 60, "center"),
            ("temp", t("pages.plan_editor.preview_columns.temp"), 80, "center"),
            ("route", t("pages.plan_editor.preview_columns.route"), 80, "center"),
            ("hgen", t("pages.plan_editor.preview_columns.h2o_target"), 160, "w"),
            ("co2", t("pages.plan_editor.preview_columns.co2"), 100, "center"),
            ("pressure", t("pages.plan_editor.preview_columns.pressure"), 100, "center"),
            ("group", t("pages.plan_editor.preview_columns.group"), 70, "center"),
            ("cylinder", t("pages.plan_editor.preview_columns.cylinder"), 90, "center"),
            ("status", t("pages.plan_editor.preview_columns.status"), 100, "center"),
        ):
            self.preview_tree.heading(column, text=title)
            self.preview_tree.column(column, width=width, anchor=anchor, stretch=column in {"hgen"})

    def _build_temperature_section(self, parent: tk.Misc) -> ttk.LabelFrame:
        frame = ttk.LabelFrame(parent, text=t("pages.plan_editor.temperature.title"), style="Card.TFrame", padding=8)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)
        ttk.Entry(frame, textvariable=self.temp_value_var).grid(row=0, column=0, sticky="ew")
        ttk.Checkbutton(frame, text=t("pages.plan_editor.common.enabled"), variable=self.temp_enabled_var).grid(
            row=0, column=1, sticky="w", padx=(6, 0)
        )
        self.temperature_tree = ttk.Treeview(frame, columns=("order", "value", "enabled"), show="headings", height=5)
        self.temperature_tree.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(6, 0))
        self.temperature_tree.bind("<<TreeviewSelect>>", lambda _event: self._fill_temperature_form())
        for column, title, width in (
            ("order", t("pages.plan_editor.common.order"), 40),
            ("value", t("pages.plan_editor.temperature.value"), 80),
            ("enabled", t("pages.plan_editor.common.enabled"), 70),
        ):
            self.temperature_tree.heading(column, text=title)
            self.temperature_tree.column(column, width=width, anchor="center")
        actions = ttk.Frame(frame, style="Card.TFrame")
        actions.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        for idx in range(4):
            actions.columnconfigure(idx, weight=1)
        ttk.Button(actions, text=t("pages.plan_editor.actions.add"), command=self._add_temperature).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(actions, text=t("pages.plan_editor.actions.update"), command=self._update_temperature).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(actions, text=t("pages.plan_editor.actions.remove"), command=self._remove_temperature).grid(row=0, column=2, sticky="ew", padx=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.copy"), command=lambda: self._duplicate_selected(self.temperature_tree, self._temperature_rows, self._refresh_temperature_tree, self._fill_temperature_form)).grid(row=0, column=3, sticky="ew", padx=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.up"), command=lambda: self._move_selected(self.temperature_tree, self._temperature_rows, -1, self._refresh_temperature_tree)).grid(row=1, column=0, sticky="ew", padx=(0, 4), pady=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.down"), command=lambda: self._move_selected(self.temperature_tree, self._temperature_rows, 1, self._refresh_temperature_tree)).grid(row=1, column=1, sticky="ew", padx=4, pady=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.toggle"), command=lambda: self._toggle_selected(self.temperature_tree, self._temperature_rows, self._refresh_temperature_tree)).grid(row=1, column=2, sticky="ew", padx=(4, 0), pady=(4, 0))
        return frame

    def _build_humidity_section(self, parent: tk.Misc) -> ttk.LabelFrame:
        frame = ttk.LabelFrame(parent, text=t("pages.plan_editor.humidity.title"), style="Card.TFrame", padding=8)
        frame.columnconfigure(1, weight=1)
        frame.rowconfigure(3, weight=1)
        ttk.Label(frame, text=t("pages.plan_editor.humidity.hgen_temp"), style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(frame, textvariable=self.humidity_temp_var).grid(row=0, column=1, sticky="ew")
        ttk.Label(frame, text=t("pages.plan_editor.humidity.rh_pct"), style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(4, 0))
        ttk.Entry(frame, textvariable=self.humidity_rh_var).grid(row=1, column=1, sticky="ew", pady=(4, 0))
        ttk.Label(frame, text=t("pages.plan_editor.humidity.dew_point"), style="Muted.TLabel").grid(row=2, column=0, sticky="w", pady=(4, 0))
        ttk.Entry(frame, textvariable=self.humidity_dew_var).grid(row=2, column=1, sticky="ew", pady=(4, 0))
        ttk.Checkbutton(frame, text=t("pages.plan_editor.common.enabled"), variable=self.humidity_enabled_var).grid(row=0, column=2, sticky="w", padx=(6, 0))
        self.humidity_tree = ttk.Treeview(frame, columns=("order", "temp", "rh", "dew", "enabled"), show="headings", height=5)
        self.humidity_tree.grid(row=3, column=0, columnspan=3, sticky="nsew", pady=(6, 0))
        self.humidity_tree.bind("<<TreeviewSelect>>", lambda _event: self._fill_humidity_form())
        for column, title, width in (
            ("order", t("pages.plan_editor.common.order"), 40),
            ("temp", t("pages.plan_editor.temperature.value"), 70),
            ("rh", t("pages.plan_editor.humidity.rh_pct"), 70),
            ("dew", t("pages.plan_editor.humidity.dew_short"), 70),
            ("enabled", t("pages.plan_editor.common.enabled"), 70),
        ):
            self.humidity_tree.heading(column, text=title)
            self.humidity_tree.column(column, width=width, anchor="center")
        actions = ttk.Frame(frame, style="Card.TFrame")
        actions.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(6, 0))
        for idx in range(4):
            actions.columnconfigure(idx, weight=1)
        ttk.Button(actions, text=t("pages.plan_editor.actions.add"), command=self._add_humidity).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(actions, text=t("pages.plan_editor.actions.update"), command=self._update_humidity).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(actions, text=t("pages.plan_editor.actions.remove"), command=self._remove_humidity).grid(row=0, column=2, sticky="ew", padx=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.copy"), command=lambda: self._duplicate_selected(self.humidity_tree, self._humidity_rows, self._refresh_humidity_tree, self._fill_humidity_form)).grid(row=0, column=3, sticky="ew", padx=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.up"), command=lambda: self._move_selected(self.humidity_tree, self._humidity_rows, -1, self._refresh_humidity_tree)).grid(row=1, column=0, sticky="ew", padx=(0, 4), pady=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.down"), command=lambda: self._move_selected(self.humidity_tree, self._humidity_rows, 1, self._refresh_humidity_tree)).grid(row=1, column=1, sticky="ew", padx=4, pady=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.toggle"), command=lambda: self._toggle_selected(self.humidity_tree, self._humidity_rows, self._refresh_humidity_tree)).grid(row=1, column=2, sticky="ew", padx=(4, 0), pady=(4, 0))
        return frame

    def _build_gas_section(self, parent: tk.Misc) -> ttk.LabelFrame:
        frame = ttk.LabelFrame(parent, text=t("pages.plan_editor.gas.title"), style="Card.TFrame", padding=8)
        frame.columnconfigure(0, weight=1)
        frame.columnconfigure(4, weight=0)
        frame.rowconfigure(2, weight=1)
        ttk.Entry(frame, textvariable=self.gas_value_var).grid(row=0, column=0, sticky="ew")
        ttk.Label(frame, text=t("pages.plan_editor.gas.group"), style="Muted.TLabel").grid(row=0, column=1, sticky="w", padx=(6, 0))
        ttk.Entry(frame, textvariable=self.gas_group_var, width=8).grid(row=0, column=2, sticky="ew", padx=(4, 6))
        ttk.Label(frame, text=t("pages.plan_editor.gas.cylinder_ppm"), style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(4, 0))
        ttk.Entry(frame, textvariable=self.gas_cylinder_var).grid(row=1, column=1, sticky="ew", pady=(4, 0))
        ttk.Checkbutton(frame, text=t("pages.plan_editor.common.enabled"), variable=self.gas_enabled_var).grid(row=1, column=3, sticky="w", padx=(6, 0), pady=(4, 0))
        self.gas_tree = ttk.Treeview(frame, columns=("order", "value", "group", "cylinder", "enabled"), show="headings", height=5)
        self.gas_tree.grid(row=2, column=0, columnspan=4, sticky="nsew", pady=(6, 0))
        self.gas_tree.bind("<<TreeviewSelect>>", lambda _event: self._fill_gas_form())
        for column, title, width in (
            ("order", t("pages.plan_editor.common.order"), 40),
            ("value", t("pages.plan_editor.gas.co2_ppm"), 80),
            ("group", t("pages.plan_editor.gas.group"), 70),
            ("cylinder", t("pages.plan_editor.gas.cylinder"), 85),
            ("enabled", t("pages.plan_editor.common.enabled"), 70),
        ):
            self.gas_tree.heading(column, text=title)
            self.gas_tree.column(column, width=width, anchor="center")
        actions = ttk.Frame(frame, style="Card.TFrame")
        actions.grid(row=3, column=0, columnspan=4, sticky="ew", pady=(6, 0))
        for idx in range(4):
            actions.columnconfigure(idx, weight=1)
        ttk.Button(actions, text=t("pages.plan_editor.actions.add"), command=self._add_gas).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(actions, text=t("pages.plan_editor.actions.update"), command=self._update_gas).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(actions, text=t("pages.plan_editor.actions.remove"), command=self._remove_gas).grid(row=0, column=2, sticky="ew", padx=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.copy"), command=lambda: self._duplicate_selected(self.gas_tree, self._gas_rows, self._refresh_gas_tree, self._fill_gas_form)).grid(row=0, column=3, sticky="ew", padx=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.up"), command=lambda: self._move_selected(self.gas_tree, self._gas_rows, -1, self._refresh_gas_tree)).grid(row=1, column=0, sticky="ew", padx=(0, 4), pady=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.down"), command=lambda: self._move_selected(self.gas_tree, self._gas_rows, 1, self._refresh_gas_tree)).grid(row=1, column=1, sticky="ew", padx=4, pady=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.toggle"), command=lambda: self._toggle_selected(self.gas_tree, self._gas_rows, self._refresh_gas_tree)).grid(row=1, column=2, sticky="ew", padx=(4, 0), pady=(4, 0))
        return frame

    def _build_pressure_section(self, parent: tk.Misc) -> ttk.LabelFrame:
        frame = ttk.LabelFrame(parent, text=t("pages.plan_editor.pressure.title"), style="Card.TFrame", padding=8)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)
        ttk.Entry(frame, textvariable=self.pressure_value_var).grid(row=0, column=0, sticky="ew")
        ttk.Checkbutton(frame, text=t("pages.plan_editor.common.enabled"), variable=self.pressure_enabled_var).grid(
            row=0, column=1, sticky="w", padx=(6, 0)
        )
        self.pressure_tree = ttk.Treeview(frame, columns=("order", "value", "enabled"), show="headings", height=5)
        self.pressure_tree.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(6, 0))
        self.pressure_tree.bind("<<TreeviewSelect>>", lambda _event: self._fill_pressure_form())
        for column, title, width in (
            ("order", t("pages.plan_editor.common.order"), 40),
            ("value", t("pages.plan_editor.pressure.value"), 90),
            ("enabled", t("pages.plan_editor.common.enabled"), 70),
        ):
            self.pressure_tree.heading(column, text=title)
            self.pressure_tree.column(column, width=width, anchor="center")
        actions = ttk.Frame(frame, style="Card.TFrame")
        actions.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        for idx in range(4):
            actions.columnconfigure(idx, weight=1)
        ttk.Button(actions, text=t("pages.plan_editor.actions.add"), command=self._add_pressure).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(actions, text=t("pages.plan_editor.actions.update"), command=self._update_pressure).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(actions, text=t("pages.plan_editor.actions.remove"), command=self._remove_pressure).grid(row=0, column=2, sticky="ew", padx=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.copy"), command=lambda: self._duplicate_selected(self.pressure_tree, self._pressure_rows, self._refresh_pressure_tree, self._fill_pressure_form)).grid(row=0, column=3, sticky="ew", padx=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.up"), command=lambda: self._move_selected(self.pressure_tree, self._pressure_rows, -1, self._refresh_pressure_tree)).grid(row=1, column=0, sticky="ew", padx=(0, 4), pady=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.down"), command=lambda: self._move_selected(self.pressure_tree, self._pressure_rows, 1, self._refresh_pressure_tree)).grid(row=1, column=1, sticky="ew", padx=4, pady=(4, 0))
        ttk.Button(actions, text=t("pages.plan_editor.actions.toggle"), command=lambda: self._toggle_selected(self.pressure_tree, self._pressure_rows, self._refresh_pressure_tree)).grid(row=1, column=2, sticky="ew", padx=(4, 0), pady=(4, 0))
        return frame

    def _load_initial_state(self) -> None:
        self._refresh_profile_list()
        profile = self.plan_gateway.get_default_profile() or self.plan_gateway.create_empty_profile()
        self._apply_profile_payload(profile)
        profile_name = str(profile.get("name", "") or "")
        if profile_name:
            self._refresh_profile_list(select_name=profile_name)

    def _refresh_profile_list(self, *, select_name: Optional[str] = None) -> None:
        summaries = self.plan_gateway.list_profiles()
        self._profile_names = [str(item.get("name", "")) for item in summaries]
        self.profile_listbox.delete(0, "end")
        target_name = select_name or self.profile_name_var.get().strip()
        selected_index = None
        for index, item in enumerate(summaries):
            name = str(item.get("name", "") or "")
            prefix = "* " if bool(item.get("is_default", False)) else ""
            self.profile_listbox.insert("end", f"{prefix}{name}")
            if target_name and name == target_name:
                selected_index = index
        if selected_index is not None:
            self.profile_listbox.selection_clear(0, "end")
            self.profile_listbox.selection_set(selected_index)
            self.profile_listbox.activate(selected_index)

    def _selected_profile_name(self) -> Optional[str]:
        selection = self.profile_listbox.curselection()
        if not selection:
            return None
        index = int(selection[0])
        if index < 0 or index >= len(self._profile_names):
            return None
        return self._profile_names[index]

    def _on_profile_select(self, _event: Any) -> None:
        name = self._selected_profile_name()
        if name:
            self._set_status("selected_profile", name=name)

    def _load_selected_profile(self) -> None:
        name = self._selected_profile_name()
        if not name:
            self._set_status("select_profile_first")
            return
        payload = self.plan_gateway.load_profile(name)
        if payload is None:
            self._set_status("profile_not_found", name=name)
            return
        self._apply_profile_payload(payload)
        self._set_status("loaded_profile", name=name)

    def _new_profile(self) -> None:
        self._apply_profile_payload(self.plan_gateway.create_empty_profile())
        self._set_preview_summary("not_generated")
        self._set_status("new_profile_draft")

    def _collect_profile_payload(
        self,
        *,
        require_name: bool = False,
        for_compile: bool = False,
    ) -> Optional[dict[str, Any]]:
        try:
            payload = self._build_profile_payload()
        except ValueError as exc:
            self._set_status("invalid_plan_options", error=exc)
            return None
        if require_name and not str(payload.get("name", "") or "").strip():
            self._set_status("profile_name_required")
            return None
        profile_name = str(payload.get("name", "") or "").strip()
        if profile_name and any(char in profile_name for char in "\\/:"):
            self._set_status("profile_name_no_path_separators")
            return None
        profile_version = str(payload.get("profile_version", "") or "").strip()
        if not profile_version:
            self._set_status("profile_version_required")
            return None
        if any(char.isspace() for char in profile_version):
            self._set_status("profile_version_no_whitespace")
            return None
        if len(profile_version) > 32:
            self._set_status("profile_version_too_long")
            return None
        analyzer_setup = dict(payload.get("analyzer_setup", {}) or {})
        manual_ids = list(analyzer_setup.get("manual_device_ids", []) or [])
        if len(set(manual_ids)) != len(manual_ids):
            self._set_status("manual_ids_unique")
            return None
        if str(analyzer_setup.get("device_id_assignment_mode", "automatic")) == "manual" and not list(analyzer_setup.get("manual_device_ids", []) or []):
            self._set_status("manual_ids_required")
            return None
        selected_temps = list((payload.get("ordering", {}) or {}).get("selected_temps_c", []) or [])
        enabled_temps = {
            float(row.get("temperature_c"))
            for row in list(payload.get("temperatures", []) or [])
            if bool(row.get("enabled", True)) and row.get("temperature_c") is not None
        }
        if selected_temps and not enabled_temps:
            self._set_status("selected_temps_need_enabled_rows")
            return None
        missing_selected = [value for value in selected_temps if not any(abs(float(value) - item) < 1e-9 for item in enabled_temps)]
        if missing_selected:
            self._set_status("selected_temps_missing", values=self._format_numbers(missing_selected))
            return None
        if for_compile:
            if not any(bool(row.get("enabled", True)) for row in list(payload.get("temperatures", []) or [])):
                self._set_status("compile_need_temperature")
                return None
            has_h2o_points = any(bool(row.get("enabled", True)) for row in list(payload.get("humidities", []) or []))
            has_co2_points = any(
                bool(row.get("enabled", True))
                for row in list(payload.get("gas_points", []) or [])
            )
            run_mode = str(payload.get("run_mode", RunMode.AUTO_CALIBRATION.value) or RunMode.AUTO_CALIBRATION.value)
            if run_mode == RunMode.CO2_MEASUREMENT.value and not has_co2_points:
                self._set_status("compile_need_co2_point")
                return None
            if run_mode == RunMode.H2O_MEASUREMENT.value and not has_h2o_points:
                self._set_status("compile_need_h2o_point")
                return None
            if not (has_h2o_points or has_co2_points):
                self._set_status("compile_need_humidity_or_gas")
                return None
        return payload

    def _save_profile(self) -> None:
        if not self.profile_name_var.get().strip():
            self._save_profile_as()
            return
        payload = self._collect_profile_payload(require_name=True)
        if payload is None:
            return
        try:
            saved = self.plan_gateway.save_profile(payload)
        except Exception as exc:
            self._set_status("save_failed", error=exc)
            return
        self._apply_profile_payload(saved)
        self._refresh_profile_list(select_name=str(saved.get("name", "")))
        self._set_status("saved_profile", name=saved.get("name", "--"))

    def _save_profile_as(self) -> None:
        initial = self.profile_name_var.get().strip() or "new_profile"
        new_name = simpledialog.askstring(
            t("pages.plan_editor.dialog.save_as_title"),
            t("pages.plan_editor.dialog.profile_name"),
            initialvalue=initial,
            parent=self,
        )
        if not new_name:
            return
        self._save_profile_as_name(new_name)

    def _save_profile_as_name(self, name: str) -> None:
        payload = self._collect_profile_payload()
        if payload is None:
            return
        payload["name"] = str(name)
        try:
            saved = self.plan_gateway.save_profile(payload, name_override=str(name))
        except Exception as exc:
            self._set_status("save_failed", error=exc)
            return
        self._apply_profile_payload(saved)
        self._refresh_profile_list(select_name=str(saved.get("name", "")))
        self._set_status("saved_as", name=saved.get("name", "--"))

    def _duplicate_selected_profile(self) -> None:
        source_name = self._selected_profile_name() or self.profile_name_var.get().strip()
        if not source_name:
            self._set_status("load_profile_before_duplicate")
            return
        new_name = simpledialog.askstring(
            t("pages.plan_editor.dialog.duplicate_title"),
            t("pages.plan_editor.dialog.new_profile_name"),
            initialvalue=f"{source_name}_copy",
            parent=self,
        )
        if not new_name:
            return
        try:
            saved = self.plan_gateway.duplicate_profile(source_name, new_name)
        except Exception as exc:
            self._set_status("duplicate_failed", error=exc)
            return
        self._apply_profile_payload(saved)
        self._refresh_profile_list(select_name=str(saved.get("name", "")))
        self._set_status("duplicated_profile", name=saved.get("name", "--"))

    def _rename_selected_profile(self) -> None:
        source_name = self._selected_profile_name() or self.profile_name_var.get().strip()
        if not source_name:
            self._set_status("load_profile_before_rename")
            return
        new_name = simpledialog.askstring(
            t("pages.plan_editor.dialog.rename_title"),
            t("pages.plan_editor.dialog.new_profile_name"),
            initialvalue=source_name,
            parent=self,
        )
        if not new_name:
            return
        try:
            saved = self.plan_gateway.rename_profile(source_name, new_name)
        except Exception as exc:
            self._set_status("rename_failed", error=exc)
            return
        self._apply_profile_payload(saved)
        self._refresh_profile_list(select_name=str(saved.get("name", "")))
        self._set_status("renamed_profile", name=saved.get("name", "--"))

    def _delete_selected_profile(self) -> None:
        name = self._selected_profile_name() or self.profile_name_var.get().strip()
        if not name:
            self._set_status("select_profile_to_delete")
            return
        if not messagebox.askyesno(
            t("pages.plan_editor.dialog.delete_title"),
            t("pages.plan_editor.dialog.delete_prompt", name=name),
            parent=self,
        ):
            return
        deleted = self.plan_gateway.delete_profile(name)
        if deleted:
            self._refresh_profile_list()
            self._new_profile()
            self._set_status("deleted_profile", name=name)
        else:
            self._set_status("profile_not_found", name=name)

    def _set_default_profile(self) -> None:
        name = self.profile_name_var.get().strip() or self._selected_profile_name()
        if not name:
            self._set_status("save_or_load_first")
            return
        try:
            saved = self.plan_gateway.set_default_profile(name)
        except Exception as exc:
            self._set_status("set_default_failed", error=exc)
            return
        self._apply_profile_payload(saved)
        self._refresh_profile_list(select_name=name)
        self._set_status("default_profile", name=name)

    def _import_profile(self) -> None:
        source = filedialog.askopenfilename(
            title=t("pages.plan_editor.dialog.import_title"),
            parent=self,
            filetypes=((t("pages.plan_editor.dialog.json_files"), "*.json"), (t("pages.plan_editor.dialog.all_files"), "*.*")),
        )
        if not source:
            return
        try:
            saved = self.plan_gateway.import_profile(source)
        except Exception as exc:
            self._set_status("import_failed", error=exc)
            return
        self._apply_profile_payload(saved)
        self._refresh_profile_list(select_name=str(saved.get("name", "")))
        self._set_status("imported_profile", name=saved.get("name", "--"))

    def _export_selected_profile(self) -> None:
        name = self._selected_profile_name() or self.profile_name_var.get().strip()
        if not name:
            self._set_status("load_profile_before_export")
            return
        destination = filedialog.asksaveasfilename(
            title=t("pages.plan_editor.dialog.export_title"),
            parent=self,
            defaultextension=".json",
            initialfile=f"{name}.json",
            filetypes=((t("pages.plan_editor.dialog.json_files"), "*.json"), (t("pages.plan_editor.dialog.all_files"), "*.*")),
        )
        if not destination:
            return
        try:
            exported = self.plan_gateway.export_profile(name, destination)
        except Exception as exc:
            self._set_status("export_failed", error=exc)
            return
        self._set_status("exported_profile", path=exported)

    def _compile_preview(self) -> None:
        payload = self._collect_profile_payload(for_compile=True)
        if payload is None:
            return
        try:
            preview = self.plan_gateway.compile_profile_preview(payload)
        except Exception as exc:
            self._set_status("compile_failed", error=exc)
            return
        for item in self.preview_tree.get_children():
            self.preview_tree.delete(item)
        for row in list(preview.get("rows", []) or []):
            self.preview_tree.insert(
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
                    row.get("cylinder", ""),
                    row.get("status", ""),
                ),
            )
        self.preview_summary_var.set(str(preview.get("summary", t("pages.plan_editor.preview.ready"))))
        self._set_status("compiled_preview_for", name=preview.get("profile_name", "--"))

    def _apply_profile_payload(self, payload: dict[str, Any]) -> None:
        data = copy.deepcopy(dict(payload or {}))
        ordering = dict(data.get("ordering", {}) or {})
        self.profile_name_var.set(str(data.get("name", "") or ""))
        self.profile_version_var.set(str(data.get("profile_version", data.get("version", "1.0")) or "1.0"))
        self.description_var.set(str(data.get("description", "") or ""))
        mode_profile = dict(data.get("mode_profile", {}) or {})
        analyzer_setup = dict(data.get("analyzer_setup", {}) or {})
        self.run_mode_var.set(
            self._display_lookup(
                self._run_mode_options(),
                mode_profile.get("run_mode", data.get("run_mode", RunMode.AUTO_CALIBRATION.value)),
                RunMode.AUTO_CALIBRATION.value,
            )
        )
        self.analyzer_version_var.set(
            self._display_lookup(
                self._analyzer_version_options(),
                analyzer_setup.get("software_version", "v5_plus"),
                "v5_plus",
            )
        )
        self.device_id_assignment_var.set(
            self._display_lookup(
                self._device_assignment_options(),
                analyzer_setup.get("device_id_assignment_mode", "automatic"),
                "automatic",
            )
        )
        self.start_device_id_var.set(str(analyzer_setup.get("start_device_id", "001") or "001"))
        self.manual_device_ids_var.set(", ".join(str(item) for item in list(analyzer_setup.get("manual_device_ids", []) or [])))
        self._is_default_profile = bool(data.get("is_default", False))
        self._update_default_status()
        self.selected_temps_var.set(self._format_numbers(ordering.get("selected_temps_c", data.get("selected_temps", []))))
        self.skip_co2_var.set(", ".join(str(int(value)) for value in list(ordering.get("skip_co2_ppm", data.get("skip_co2_ppm", []))) or []))
        self.water_first_var.set(bool(ordering.get("water_first", data.get("water_first", False))))
        water_first_temp_gte = ordering.get("water_first_temp_gte", data.get("water_first_temp_gte"))
        self.water_first_temp_gte_var.set("" if water_first_temp_gte in (None, "") else self._format_float(water_first_temp_gte))
        self.temperature_descending_var.set(bool(ordering.get("temperature_descending", data.get("temperature_descending", True))))
        self._temperature_rows = self._with_order(list(data.get("temperatures", []) or []))
        self._humidity_rows = self._with_order(list(data.get("humidities", []) or []))
        self._gas_rows = self._with_order(list(data.get("gas_points", []) or []))
        self._pressure_rows = self._with_order(list(data.get("pressures", []) or []))
        self._refresh_temperature_tree()
        self._refresh_humidity_tree()
        self._refresh_gas_tree()
        self._refresh_pressure_tree()
        for fill_method in (
            self._fill_temperature_form,
            self._fill_humidity_form,
            self._fill_gas_form,
            self._fill_pressure_form,
        ):
            fill_method()

    def _update_default_status(self) -> None:
        self.default_status_var.set(
            t(
                "pages.plan_editor.status.default_profile_flag",
                value=t("common.yes") if self._is_default_profile else t("common.no"),
            )
        )

    def _build_profile_payload(self) -> dict[str, Any]:
        run_mode_value = self._reverse_lookup(
            self._run_mode_options(),
            self.run_mode_var.get(),
            RunMode.AUTO_CALIBRATION.value,
        )
        return {
            "name": self.profile_name_var.get().strip(),
            "profile_version": self.profile_version_var.get().strip() or "1.0",
            "description": self.description_var.get().strip(),
            "is_default": bool(self._is_default_profile),
            "run_mode": run_mode_value,
            "mode_profile": {
                "run_mode": run_mode_value,
            },
            "analyzer_setup": {
                "software_version": self._normalize_analyzer_version(
                    self._reverse_lookup(self._analyzer_version_options(), self.analyzer_version_var.get(), "v5_plus")
                ),
                "device_id_assignment_mode": self._normalize_device_id_assignment_mode(
                    self._reverse_lookup(self._device_assignment_options(), self.device_id_assignment_var.get(), "automatic")
                ),
                "start_device_id": self._normalize_device_id_text(
                    self.start_device_id_var.get(),
                    field_name=t("pages.plan_editor.validation.start_device_id"),
                    default="001",
                ),
                "manual_device_ids": self._parse_device_id_list(self.manual_device_ids_var.get()),
            },
            "temperatures": self._with_order(self._temperature_rows),
            "humidities": self._with_order(self._humidity_rows),
            "gas_points": self._with_order(self._gas_rows),
            "pressures": self._with_order(self._pressure_rows),
            "ordering": {
                "water_first": bool(self.water_first_var.get()),
                "water_first_temp_gte": self._parse_optional_float_text(self.water_first_temp_gte_var.get()),
                "selected_temps_c": self._parse_float_list(self.selected_temps_var.get()),
                "skip_co2_ppm": self._parse_int_list(self.skip_co2_var.get()),
                "temperature_descending": bool(self.temperature_descending_var.get()),
            },
        }

    @staticmethod
    def _with_order(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ordered: list[dict[str, Any]] = []
        for index, row in enumerate(list(rows), start=1):
            item = copy.deepcopy(dict(row))
            item["order"] = index
            item.setdefault("enabled", True)
            ordered.append(item)
        return ordered

    @staticmethod
    def _parse_float_list(raw: str) -> list[float]:
        text = str(raw or "").strip()
        if not text:
            return []
        for token in ("\n", "\r", "\t", ";", "，", "、"):
            text = text.replace(token, ",")
        values: list[float] = []
        for chunk in text.split(","):
            part = chunk.strip()
            if not part:
                continue
            values.append(float(part))
        return values

    @staticmethod
    def _ensure_finite(value: float, field_name: str) -> float:
        if not math.isfinite(float(value)):
            raise ValueError(t("pages.plan_editor.validation.finite_number", field=field_name))
        return float(value)

    @staticmethod
    def _parse_int_list(raw: str) -> list[int]:
        return [int(round(value)) for value in PlanEditorPage._parse_float_list(raw)]

    @staticmethod
    def _parse_optional_float_text(raw: str) -> Optional[float]:
        text = str(raw or "").strip()
        if not text:
            return None
        return float(text)

    @staticmethod
    def _normalize_analyzer_version(raw: Any) -> str:
        text = str(raw or "").strip().lower()
        if text in {"pre_v5", "pre-v5", "legacy", "v4"}:
            return "pre_v5"
        return "v5_plus"

    @staticmethod
    def _normalize_device_id_assignment_mode(raw: Any) -> str:
        text = str(raw or "").strip().lower()
        if text in {"manual", "manual_list", "fixed"}:
            return "manual"
        return "automatic"

    @staticmethod
    def _normalize_device_id_text(raw: Any, *, field_name: str, default: str = "") -> str:
        text = str(raw or "").strip()
        if not text:
            text = str(default or "").strip()
        if not text:
            return ""
        if text.isdigit():
            return f"{int(text):03d}"
        if len(text) > 8:
            raise ValueError(t("pages.plan_editor.validation.invalid_value", field=field_name, value=text))
        return text.upper()

    @classmethod
    def _parse_device_id_list(cls, raw: str) -> list[str]:
        text = str(raw or "").strip()
        if not text:
            return []
        values: list[str] = []
        for chunk in text.replace(";", ",").replace("\n", ",").split(","):
            part = chunk.strip()
            if not part:
                continue
            values.append(cls._normalize_device_id_text(part, field_name=t("pages.plan_editor.validation.device_id")))
        return values

    @staticmethod
    def _normalize_co2_group(raw: Any) -> str:
        text = str(raw or "").strip().upper()
        return text or "A"

    @staticmethod
    def _format_numbers(values: Any) -> str:
        return ", ".join(PlanEditorPage._format_float(value) for value in list(values or []))

    @staticmethod
    def _tree_index(tree: ttk.Treeview) -> Optional[int]:
        selection = tree.selection()
        if not selection:
            focus = tree.focus()
            selection = (focus,) if focus else ()
        if not selection:
            return None
        try:
            return int(selection[0])
        except (TypeError, ValueError):
            return None

    def _move_selected(
        self,
        tree: ttk.Treeview,
        rows: list[dict[str, Any]],
        delta: int,
        refresh,
    ) -> None:
        index = self._tree_index(tree)
        if index is None:
            self._set_status("select_row_first")
            return
        target = index + int(delta)
        if target < 0 or target >= len(rows):
            return
        rows[index], rows[target] = rows[target], rows[index]
        refresh(select_index=target)
        self._set_status("order_updated")

    def _toggle_selected(
        self,
        tree: ttk.Treeview,
        rows: list[dict[str, Any]],
        refresh,
    ) -> None:
        index = self._tree_index(tree)
        if index is None:
            self._set_status("select_row_first")
            return
        rows[index]["enabled"] = not bool(rows[index].get("enabled", True))
        refresh(select_index=index)
        self._set_status("enabled_flag_updated")

    def _duplicate_selected(
        self,
        tree: ttk.Treeview,
        rows: list[dict[str, Any]],
        refresh,
        fill_form,
    ) -> None:
        index = self._tree_index(tree)
        if index is None:
            self._set_status("select_row_first")
            return
        rows.append(copy.deepcopy(dict(rows[index])))
        refresh(select_index=len(rows) - 1)
        fill_form()
        self._set_status("row_copied")

    def _refresh_temperature_tree(self, *, select_index: Optional[int] = None) -> None:
        self._refresh_tree(
            self.temperature_tree,
            self._temperature_rows,
            lambda row: (row.get("order", ""), self._format_float(row.get("temperature_c")), self._enabled_text(row.get("enabled"))),
            select_index=select_index,
        )

    def _refresh_humidity_tree(self, *, select_index: Optional[int] = None) -> None:
        self._refresh_tree(
            self.humidity_tree,
            self._humidity_rows,
            lambda row: (
                row.get("order", ""),
                self._format_optional_float(row.get("hgen_temp_c")),
                self._format_optional_float(row.get("hgen_rh_pct")),
                self._format_optional_float(row.get("dewpoint_c")),
                self._enabled_text(row.get("enabled")),
            ),
            select_index=select_index,
        )

    def _refresh_gas_tree(self, *, select_index: Optional[int] = None) -> None:
        self._refresh_tree(
            self.gas_tree,
            self._gas_rows,
            lambda row: (
                row.get("order", ""),
                self._format_float(row.get("co2_ppm")),
                self._normalize_co2_group(row.get("co2_group")),
                self._format_optional_float(row.get("cylinder_nominal_ppm")),
                self._enabled_text(row.get("enabled")),
            ),
            select_index=select_index,
        )

    def _refresh_pressure_tree(self, *, select_index: Optional[int] = None) -> None:
        self._refresh_tree(
            self.pressure_tree,
            self._pressure_rows,
            lambda row: (row.get("order", ""), self._format_float(row.get("pressure_hpa")), self._enabled_text(row.get("enabled"))),
            select_index=select_index,
        )

    def _refresh_tree(
        self,
        tree: ttk.Treeview,
        rows: list[dict[str, Any]],
        value_builder,
        *,
        select_index: Optional[int] = None,
    ) -> None:
        current_index = self._tree_index(tree) if select_index is None else select_index
        rows[:] = self._with_order(rows)
        for item in tree.get_children():
            tree.delete(item)
        for index, row in enumerate(rows):
            tree.insert("", "end", iid=str(index), values=value_builder(row))
        if rows:
            target_index = 0 if current_index is None else max(0, min(int(current_index), len(rows) - 1))
            target = str(target_index)
            tree.selection_set(target)
            tree.focus(target)
        else:
            tree.selection_set(())
            tree.focus("")

    @staticmethod
    def _enabled_text(value: Any) -> str:
        return t("common.yes") if bool(value) else t("common.no")

    @staticmethod
    def _format_float(value: Any) -> str:
        return f"{float(value):g}"

    @staticmethod
    def _format_optional_float(value: Any) -> str:
        if value in (None, ""):
            return "--"
        return PlanEditorPage._format_float(value)

    def _fill_temperature_form(self) -> None:
        index = self._tree_index(self.temperature_tree)
        if index is None or index >= len(self._temperature_rows):
            self.temp_value_var.set("")
            self.temp_enabled_var.set(True)
            return
        row = self._temperature_rows[index]
        self.temp_value_var.set(self._format_float(row.get("temperature_c")))
        self.temp_enabled_var.set(bool(row.get("enabled", True)))

    def _fill_humidity_form(self) -> None:
        index = self._tree_index(self.humidity_tree)
        if index is None or index >= len(self._humidity_rows):
            self.humidity_temp_var.set("")
            self.humidity_rh_var.set("")
            self.humidity_dew_var.set("")
            self.humidity_enabled_var.set(True)
            return
        row = self._humidity_rows[index]
        self.humidity_temp_var.set("" if row.get("hgen_temp_c") is None else self._format_float(row.get("hgen_temp_c")))
        self.humidity_rh_var.set("" if row.get("hgen_rh_pct") is None else self._format_float(row.get("hgen_rh_pct")))
        self.humidity_dew_var.set("" if row.get("dewpoint_c") is None else self._format_float(row.get("dewpoint_c")))
        self.humidity_enabled_var.set(bool(row.get("enabled", True)))

    def _fill_gas_form(self) -> None:
        index = self._tree_index(self.gas_tree)
        if index is None or index >= len(self._gas_rows):
            self.gas_value_var.set("")
            self.gas_group_var.set("A")
            self.gas_cylinder_var.set("")
            self.gas_enabled_var.set(True)
            return
        row = self._gas_rows[index]
        self.gas_value_var.set(self._format_float(row.get("co2_ppm")))
        self.gas_group_var.set(self._normalize_co2_group(row.get("co2_group")))
        self.gas_cylinder_var.set("" if row.get("cylinder_nominal_ppm") is None else self._format_float(row.get("cylinder_nominal_ppm")))
        self.gas_enabled_var.set(bool(row.get("enabled", True)))

    def _fill_pressure_form(self) -> None:
        index = self._tree_index(self.pressure_tree)
        if index is None or index >= len(self._pressure_rows):
            self.pressure_value_var.set("")
            self.pressure_enabled_var.set(True)
            return
        row = self._pressure_rows[index]
        self.pressure_value_var.set(self._format_float(row.get("pressure_hpa")))
        self.pressure_enabled_var.set(bool(row.get("enabled", True)))

    def _add_temperature(self) -> None:
        try:
            values = self._parse_float_list(self.temp_value_var.get())
        except ValueError as exc:
            self._set_status("invalid_temperature", error=exc)
            return
        if not values:
            self._set_status("enter_temperature")
            return
        for value in values:
            self._temperature_rows.append(
                {
                    "temperature_c": self._ensure_finite(float(value), t("pages.plan_editor.validation.temperature")),
                    "enabled": bool(self.temp_enabled_var.get()),
                }
            )
        self._refresh_temperature_tree(select_index=len(self._temperature_rows) - 1)
        self._set_status("temperature_rows_added", count=len(values))

    def _update_temperature(self) -> None:
        index = self._tree_index(self.temperature_tree)
        if index is None:
            self._set_status("select_temperature_row_first")
            return
        try:
            self._temperature_rows[index] = {
                "temperature_c": self._ensure_finite(
                    float(self.temp_value_var.get().strip()),
                    t("pages.plan_editor.validation.temperature"),
                ),
                "enabled": bool(self.temp_enabled_var.get()),
            }
        except ValueError as exc:
            self._set_status("invalid_temperature", error=exc)
            return
        self._refresh_temperature_tree(select_index=index)
        self._set_status("temperature_updated")

    def _remove_temperature(self) -> None:
        self._remove_selected(self.temperature_tree, self._temperature_rows, self._refresh_temperature_tree, self._fill_temperature_form)

    def _add_humidity(self) -> None:
        try:
            row = {
                "hgen_temp_c": self._optional_float(self.humidity_temp_var.get(), t("pages.plan_editor.validation.hgen_temp")),
                "hgen_rh_pct": self._optional_float(self.humidity_rh_var.get(), t("pages.plan_editor.validation.humidity_target")),
                "dewpoint_c": self._optional_float(self.humidity_dew_var.get(), t("pages.plan_editor.validation.dew_point")),
                "enabled": bool(self.humidity_enabled_var.get()),
            }
        except ValueError as exc:
            self._set_status("humidity_error", error=exc)
            return
        if all(row.get(key) is None for key in ("hgen_temp_c", "hgen_rh_pct", "dewpoint_c")):
            self._set_status("humidity_need_target")
            return
        if row.get("hgen_rh_pct") is not None and not (0.0 <= float(row["hgen_rh_pct"]) <= 100.0):
            self._set_status("humidity_rh_range")
            return
        self._humidity_rows.append(row)
        self._refresh_humidity_tree(select_index=len(self._humidity_rows) - 1)
        self._set_status("humidity_added")

    def _update_humidity(self) -> None:
        index = self._tree_index(self.humidity_tree)
        if index is None:
            self._set_status("select_humidity_row_first")
            return
        try:
            row = {
                "hgen_temp_c": self._optional_float(self.humidity_temp_var.get(), t("pages.plan_editor.validation.hgen_temp")),
                "hgen_rh_pct": self._optional_float(self.humidity_rh_var.get(), t("pages.plan_editor.validation.humidity_target")),
                "dewpoint_c": self._optional_float(self.humidity_dew_var.get(), t("pages.plan_editor.validation.dew_point")),
                "enabled": bool(self.humidity_enabled_var.get()),
            }
        except ValueError as exc:
            self._set_status("humidity_error", error=exc)
            return
        if all(row.get(key) is None for key in ("hgen_temp_c", "hgen_rh_pct", "dewpoint_c")):
            self._set_status("humidity_need_target")
            return
        if row.get("hgen_rh_pct") is not None and not (0.0 <= float(row["hgen_rh_pct"]) <= 100.0):
            self._set_status("humidity_rh_range")
            return
        self._humidity_rows[index] = row
        self._refresh_humidity_tree(select_index=index)
        self._set_status("humidity_updated")

    def _remove_humidity(self) -> None:
        self._remove_selected(self.humidity_tree, self._humidity_rows, self._refresh_humidity_tree, self._fill_humidity_form)

    def _add_gas(self) -> None:
        try:
            values = self._parse_float_list(self.gas_value_var.get())
            cylinder_nominal = self._optional_float(self.gas_cylinder_var.get(), t("pages.plan_editor.validation.cylinder_nominal_ppm"))
        except ValueError as exc:
            self._set_status("invalid_gas_point", error=exc)
            return
        if not values:
            self._set_status("enter_co2_value")
            return
        if any(float(value) < 0.0 for value in values):
            self._set_status("co2_non_negative")
            return
        if cylinder_nominal is not None and float(cylinder_nominal) < 0.0:
            self._set_status("cylinder_non_negative")
            return
        for value in values:
            self._gas_rows.append(
                {
                    "co2_ppm": self._ensure_finite(float(value), t("pages.plan_editor.validation.co2_ppm")),
                    "co2_group": self._normalize_co2_group(self.gas_group_var.get()),
                    "cylinder_nominal_ppm": cylinder_nominal,
                    "enabled": bool(self.gas_enabled_var.get()),
                }
            )
        self._refresh_gas_tree(select_index=len(self._gas_rows) - 1)
        self._set_status("gas_rows_added", count=len(values))

    def _update_gas(self) -> None:
        index = self._tree_index(self.gas_tree)
        if index is None:
            self._set_status("select_gas_row_first")
            return
        try:
            self._gas_rows[index] = {
                "co2_ppm": self._ensure_finite(
                    float(self.gas_value_var.get().strip()),
                    t("pages.plan_editor.validation.co2_ppm"),
                ),
                "co2_group": self._normalize_co2_group(self.gas_group_var.get()),
                "cylinder_nominal_ppm": self._optional_float(self.gas_cylinder_var.get(), t("pages.plan_editor.validation.cylinder_nominal_ppm")),
                "enabled": bool(self.gas_enabled_var.get()),
            }
        except ValueError as exc:
            self._set_status("invalid_gas_point", error=exc)
            return
        if float(self._gas_rows[index]["co2_ppm"]) < 0.0:
            self._set_status("co2_non_negative")
            return
        if self._gas_rows[index].get("cylinder_nominal_ppm") is not None and float(self._gas_rows[index]["cylinder_nominal_ppm"]) < 0.0:
            self._set_status("cylinder_non_negative")
            return
        self._refresh_gas_tree(select_index=index)
        self._set_status("gas_updated")

    def _remove_gas(self) -> None:
        self._remove_selected(self.gas_tree, self._gas_rows, self._refresh_gas_tree, self._fill_gas_form)

    def _add_pressure(self) -> None:
        try:
            values = self._parse_float_list(self.pressure_value_var.get())
        except ValueError as exc:
            self._set_status("invalid_pressure", error=exc)
            return
        if not values:
            self._set_status("enter_pressure")
            return
        if any(float(value) <= 0.0 for value in values):
            self._set_status("pressure_positive")
            return
        for value in values:
            self._pressure_rows.append(
                {
                    "pressure_hpa": self._ensure_finite(float(value), t("pages.plan_editor.validation.pressure")),
                    "enabled": bool(self.pressure_enabled_var.get()),
                }
            )
        self._refresh_pressure_tree(select_index=len(self._pressure_rows) - 1)
        self._set_status("pressure_rows_added", count=len(values))

    def _update_pressure(self) -> None:
        index = self._tree_index(self.pressure_tree)
        if index is None:
            self._set_status("select_pressure_row_first")
            return
        try:
            self._pressure_rows[index] = {
                "pressure_hpa": self._ensure_finite(
                    float(self.pressure_value_var.get().strip()),
                    t("pages.plan_editor.validation.pressure"),
                ),
                "enabled": bool(self.pressure_enabled_var.get()),
            }
        except ValueError as exc:
            self._set_status("invalid_pressure", error=exc)
            return
        if float(self._pressure_rows[index]["pressure_hpa"]) <= 0.0:
            self._set_status("pressure_positive")
            return
        self._refresh_pressure_tree(select_index=index)
        self._set_status("pressure_updated")

    def _remove_pressure(self) -> None:
        self._remove_selected(self.pressure_tree, self._pressure_rows, self._refresh_pressure_tree, self._fill_pressure_form)

    def _remove_selected(
        self,
        tree: ttk.Treeview,
        rows: list[dict[str, Any]],
        refresh,
        fill_form,
    ) -> None:
        index = self._tree_index(tree)
        if index is None:
            self._set_status("select_row_first")
            return
        rows.pop(index)
        next_index = None if not rows else min(index, len(rows) - 1)
        refresh(select_index=next_index)
        fill_form()
        self._set_status("row_removed")

    @staticmethod
    def _optional_float(raw: str, field_name: str) -> Optional[float]:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError as exc:
            raise ValueError(t("pages.plan_editor.validation.invalid_number", field=field_name, error=exc)) from exc
