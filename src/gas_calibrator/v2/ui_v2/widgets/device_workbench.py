from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import t
from .collapsible_section import CollapsibleSection


class DeviceWorkbench(ttk.LabelFrame):
    """Simulation-only device workbench widget."""

    def __init__(self, parent: tk.Misc, *, facade: Any | None = None) -> None:
        super().__init__(parent, text=t("pages.devices.workbench.title"), padding=12, style="Card.TFrame")
        self.facade = facade
        self.columnconfigure(0, weight=1)
        self.rowconfigure(6, weight=1)
        self._quick_scenario_lookup: dict[str, str] = {}
        self._tab_device_kinds: dict[str, str] = {}
        self._preset_lookup: dict[str, dict[str, str]] = {}
        self._preset_group_lookup: dict[str, dict[str, Any]] = {}
        self._preset_center_lookup: dict[str, tuple[str, str]] = {}
        self._recent_preset_lookup: dict[str, tuple[str, str]] = {}
        self._preset_center_payload: dict[str, Any] = {}
        self._workbench_payload: dict[str, Any] = {}
        self._display_profile_lookup: dict[str, str] = {}
        self._display_profile_hint_lookup: dict[str, str] = {}
        self._history_device_lookup: dict[str, str] = {}
        self._history_result_lookup: dict[str, str] = {}
        self._snapshot_option_lookup: dict[str, int] = {}
        self._history_snapshot: dict[str, Any] = {}
        self._custom_group_lookup: dict[str, str] = {}
        self._custom_device_lookup: dict[str, str] = {}
        self._custom_step_preset_catalog: dict[str, list[dict[str, str]]] = {}
        self._custom_step_preset_lookup: list[dict[str, str]] = []
        self._custom_relay_lookup: dict[str, str] = {}
        self._preset_import_conflict_lookup: dict[str, str] = {}
        self._layout_mode = "compact"

        self.banner_var = tk.StringVar(value=t("pages.devices.workbench.banner.simulation_mode"))
        self.notice_var = tk.StringVar(value="")
        self.evidence_var = tk.StringVar(value="")
        self.message_var = tk.StringVar(value="")
        self.view_mode_var = tk.StringVar(value=t("pages.devices.workbench.view.operator_view"))
        self.layout_mode_var = tk.StringVar(value=t("pages.devices.workbench.layout.compact"))
        self.display_profile_var = tk.StringVar(value=t("pages.devices.workbench.display_profile.auto", default="自动"))
        self.display_profile_hint_var = tk.StringVar(value="")
        self.health_var = tk.StringVar(value="")
        self.faults_var = tk.StringVar(value="")
        self.reference_var = tk.StringVar(value="")
        self.route_var = tk.StringVar(value="")
        self.history_var = tk.StringVar(value="")
        self.risk_var = tk.StringVar(value="")
        self.last_evidence_var = tk.StringVar(value="")
        self.quick_scenario_var = tk.StringVar(value="")
        self.history_device_filter_var = tk.StringVar(value="")
        self.history_result_filter_var = tk.StringVar(value="")
        self.history_detail_var = tk.StringVar(value="")
        self.snapshot_left_var = tk.StringVar(value="")
        self.snapshot_right_var = tk.StringVar(value="")
        self.preset_group_var = tk.StringVar(value="")
        self.preset_center_var = tk.StringVar(value="")
        self.recent_preset_var = tk.StringVar(value="")
        self.preset_center_summary_var = tk.StringVar(value="")
        self.preset_detail_var = tk.StringVar(value="")
        self.custom_preset_summary_var = tk.StringVar(value="")
        self.preset_manager_summary_var = tk.StringVar(value="")
        self.preset_import_conflict_var = tk.StringVar(value="")
        self.history_snapshot_var = tk.StringVar(value="")
        self.history_evidence_var = tk.StringVar(value="")
        self.custom_preset_id_var = tk.StringVar(value="")
        self._loaded_custom_preset_id = ""
        self.custom_preset_group_var = tk.StringVar(value="")
        self.custom_preset_name_var = tk.StringVar(value="")
        self.custom_preset_description_var = tk.StringVar(value="")
        self.custom_preset_analyzer_index_var = tk.StringVar(value="1")
        self.custom_preset_pressure_var = tk.StringVar(value="1000")
        self.custom_preset_relay_var = tk.StringVar(value=t("pages.devices.workbench.device.relay"))
        self.custom_preset_channel_var = tk.StringVar(value="1")
        self.custom_step_device_vars = [tk.StringVar(value="") for _ in range(3)]
        self.custom_step_preset_vars = [tk.StringVar(value="") for _ in range(3)]
        self._preset_vars = {
            "analyzer": tk.StringVar(value=""),
            "pace": tk.StringVar(value=""),
            "grz": tk.StringVar(value=""),
            "chamber": tk.StringVar(value=""),
            "relay": tk.StringVar(value=""),
            "thermometer": tk.StringVar(value=""),
            "pressure_gauge": tk.StringVar(value=""),
        }
        self._preset_selectors: dict[str, ttk.Combobox] = {}

        ttk.Label(self, textvariable=self.banner_var, style="Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(self, textvariable=self.notice_var, wraplength=1020, justify="left").grid(row=1, column=0, sticky="ew", pady=(4, 0))

        toolbar = ttk.Frame(self)
        toolbar.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        toolbar.columnconfigure(15, weight=1)
        ttk.Label(toolbar, text=t("pages.devices.workbench.field.view_mode")).grid(row=0, column=0, sticky="w")
        ttk.Label(toolbar, textvariable=self.view_mode_var).grid(row=0, column=1, sticky="w", padx=(6, 12))
        self.operator_view_button = ttk.Button(
            toolbar,
            text=t("pages.devices.workbench.view.operator_view"),
            command=lambda: self._invoke("workbench", "set_view_mode", view_mode="operator_view"),
        )
        self.operator_view_button.grid(row=0, column=2, padx=4, pady=2)
        self.engineer_view_button = ttk.Button(
            toolbar,
            text=t("pages.devices.workbench.view.engineer_view"),
            command=lambda: self._invoke("workbench", "set_view_mode", view_mode="engineer_view"),
        )
        self.engineer_view_button.grid(row=0, column=3, padx=4, pady=2)
        ttk.Label(toolbar, text=t("pages.devices.workbench.field.quick_scenario")).grid(row=0, column=4, sticky="w", padx=(12, 0))
        self.quick_scenario_selector = ttk.Combobox(toolbar, textvariable=self.quick_scenario_var, state="readonly", width=24)
        self.quick_scenario_selector.grid(row=0, column=5, sticky="w", padx=(6, 4))
        self.run_quick_scenario_button = ttk.Button(toolbar, text=t("pages.devices.workbench.button.run_quick_scenario"), command=self._run_selected_quick_scenario)
        self.run_quick_scenario_button.grid(row=0, column=6, padx=4, pady=2)
        self.generate_evidence_button = ttk.Button(toolbar, text=t("pages.devices.workbench.button.generate_evidence"), command=self._generate_evidence)
        self.generate_evidence_button.grid(row=0, column=7, padx=4, pady=2)
        ttk.Label(toolbar, text=t("pages.devices.workbench.field.layout_mode")).grid(row=0, column=8, sticky="w", padx=(12, 0))
        ttk.Label(toolbar, textvariable=self.layout_mode_var).grid(row=0, column=9, sticky="w", padx=(6, 8))
        self.compact_layout_button = ttk.Button(
            toolbar,
            text=t("pages.devices.workbench.layout.compact"),
            command=lambda: self._set_layout_mode("compact"),
        )
        self.compact_layout_button.grid(row=0, column=10, padx=4, pady=2)
        self.standard_layout_button = ttk.Button(
            toolbar,
            text=t("pages.devices.workbench.layout.standard"),
            command=lambda: self._set_layout_mode("standard"),
        )
        self.standard_layout_button.grid(row=0, column=11, padx=4, pady=2)
        ttk.Label(toolbar, text=t("pages.devices.workbench.field.display_profile", default="显示档位")).grid(row=0, column=12, sticky="w", padx=(12, 0))
        self.display_profile_selector = ttk.Combobox(toolbar, textvariable=self.display_profile_var, state="readonly", width=16)
        self.display_profile_selector.grid(row=0, column=13, sticky="w", padx=(6, 4))
        self.display_profile_selector.bind("<<ComboboxSelected>>", self._apply_display_profile, add="+")
        ttk.Button(
            toolbar,
            text=t("pages.devices.workbench.button.apply_display_profile", default="应用档位"),
            command=self._apply_display_profile,
        ).grid(row=0, column=14, padx=4, pady=2)
        ttk.Label(
            toolbar,
            textvariable=self.display_profile_hint_var,
            style="Muted.TLabel",
            wraplength=240,
            justify="left",
        ).grid(row=0, column=15, sticky="w", padx=(12, 0))

        summary = ttk.Frame(self, style="Card.TFrame")
        summary.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        summary.columnconfigure(0, weight=1)
        summary.columnconfigure(1, weight=1)
        self._summary_card(summary, 0, 0, t("pages.devices.workbench.summary.health_label"), self.health_var)
        self._summary_card(summary, 0, 1, t("pages.devices.workbench.summary.reference_label"), self.reference_var)
        self._summary_card(summary, 1, 0, t("pages.devices.workbench.summary.faults_label"), self.faults_var)
        self._summary_card(summary, 1, 1, t("pages.devices.workbench.summary.route_label"), self.route_var)
        self._summary_card(summary, 2, 0, t("pages.devices.workbench.summary.history_label"), self.history_var)
        self._summary_card(summary, 2, 1, t("pages.devices.workbench.summary.risk_label"), self.risk_var)
        self._summary_card(summary, 3, 0, t("pages.devices.workbench.summary.last_evidence_label"), self.last_evidence_var)
        self._summary_card(summary, 3, 1, t("pages.devices.workbench.summary.evidence_label", default="证据状态"), self.evidence_var)

        message_frame = ttk.Frame(self)
        message_frame.grid(row=4, column=0, sticky="ew", pady=(8, 8))
        message_frame.columnconfigure(0, weight=1)
        message_frame.columnconfigure(1, weight=1)
        ttk.Label(
            message_frame,
            text=t("pages.devices.workbench.summary.operator_hint_label", default="当前动作"),
            style="Muted.TLabel",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            message_frame,
            text=t("pages.devices.workbench.summary.engineer_hint_label", default="诊断提示"),
            style="Muted.TLabel",
        ).grid(row=0, column=1, sticky="w", padx=(12, 0))
        ttk.Label(message_frame, textvariable=self.message_var, wraplength=500, justify="left").grid(row=1, column=0, sticky="ew")
        ttk.Label(
            message_frame,
            textvariable=self.notice_var,
            wraplength=500,
            justify="left",
        ).grid(row=1, column=1, sticky="ew", padx=(12, 0))

        self.preset_center_frame = ttk.LabelFrame(
            self,
            text=t("pages.devices.workbench.preset_center.title"),
            padding=8,
            style="Card.TFrame",
        )
        self.preset_center_frame.grid(row=5, column=0, sticky="ew")
        self.preset_center_frame.columnconfigure(0, weight=1)
        preset_toolbar = ttk.Frame(self.preset_center_frame)
        preset_toolbar.grid(row=0, column=0, sticky="ew")
        preset_toolbar.columnconfigure(10, weight=1)
        ttk.Label(preset_toolbar, text=t("pages.devices.workbench.preset_center.group")).grid(row=0, column=0, sticky="w")
        self.preset_group_selector = ttk.Combobox(
            preset_toolbar,
            textvariable=self.preset_group_var,
            state="readonly",
            width=18,
        )
        self.preset_group_selector.grid(row=0, column=1, sticky="w", padx=(6, 12))
        self.preset_group_selector.bind("<<ComboboxSelected>>", self._on_preset_group_changed, add="+")
        ttk.Label(preset_toolbar, text=t("pages.devices.workbench.preset_center.preset")).grid(row=0, column=2, sticky="w")
        self.preset_center_selector = ttk.Combobox(
            preset_toolbar,
            textvariable=self.preset_center_var,
            state="readonly",
            width=26,
        )
        self.preset_center_selector.grid(row=0, column=3, sticky="w", padx=(6, 4))
        self.preset_center_selector.bind("<<ComboboxSelected>>", self._on_center_preset_changed, add="+")
        self.run_preset_center_button = ttk.Button(
            preset_toolbar,
            text=t("pages.devices.workbench.button.run_preset"),
            command=self._run_selected_center_preset,
        )
        self.run_preset_center_button.grid(row=0, column=4, padx=4, pady=2)
        self.favorite_preset_button = ttk.Button(
            preset_toolbar,
            text=t("pages.devices.workbench.preset_center.favorite_button", default="收藏"),
            command=self._toggle_selected_preset_favorite,
        )
        self.favorite_preset_button.grid(row=0, column=5, padx=4, pady=2)
        self.pin_preset_button = ttk.Button(
            preset_toolbar,
            text=t("pages.devices.workbench.preset_center.pin_button", default="置顶"),
            command=self._toggle_selected_preset_pin,
        )
        self.pin_preset_button.grid(row=0, column=6, padx=4, pady=2)
        ttk.Label(preset_toolbar, text=t("pages.devices.workbench.preset_center.recent")).grid(row=0, column=7, sticky="w", padx=(12, 0))
        self.recent_preset_selector = ttk.Combobox(
            preset_toolbar,
            textvariable=self.recent_preset_var,
            state="readonly",
            width=24,
        )
        self.recent_preset_selector.grid(row=0, column=8, sticky="w", padx=(6, 4))
        self.run_recent_preset_button = ttk.Button(
            preset_toolbar,
            text=t("pages.devices.workbench.preset_center.run_recent"),
            command=self._run_recent_preset,
        )
        self.run_recent_preset_button.grid(row=0, column=9, sticky="e", padx=4, pady=2)
        self.pinned_preset_buttons = ttk.Frame(self.preset_center_frame, style="Card.TFrame")
        self.pinned_preset_buttons.grid(row=1, column=0, sticky="ew", pady=(8, 4))
        self.frequent_preset_buttons = ttk.Frame(self.preset_center_frame, style="Card.TFrame")
        self.frequent_preset_buttons.grid(row=2, column=0, sticky="ew", pady=(0, 4))
        self.custom_preset_buttons = ttk.Frame(self.preset_center_frame, style="Card.TFrame")
        self.custom_preset_buttons.grid(row=3, column=0, sticky="ew", pady=(0, 4))
        self.custom_preset_summary_label = ttk.Label(
            self.preset_center_frame,
            textvariable=self.custom_preset_summary_var,
            style="Muted.TLabel",
            wraplength=980,
            justify="left",
        )
        self.custom_preset_summary_label.grid(row=4, column=0, sticky="ew")
        self.custom_preset_editor = CollapsibleSection(
            self.preset_center_frame,
            title=t("pages.devices.workbench.preset_center.editor.title", default="自定义预置编辑器"),
            summary=t("pages.devices.workbench.preset_center.editor.summary", default="仅作用于 simulation/fake 动作组合"),
            expanded=False,
        )
        self.custom_preset_editor.grid(row=5, column=0, sticky="ew", pady=(4, 6))
        self.custom_preset_editor.body.columnconfigure(1, weight=1)
        self.custom_preset_editor.body.columnconfigure(3, weight=1)
        editor_actions = ttk.Frame(self.custom_preset_editor.body)
        editor_actions.grid(row=0, column=0, columnspan=4, sticky="ew", pady=(0, 6))
        ttk.Button(
            editor_actions,
            text=t("pages.devices.workbench.preset_center.editor.new_button", default="新建"),
            command=self._new_custom_preset,
        ).grid(row=0, column=0, padx=(0, 6), pady=2)
        ttk.Button(
            editor_actions,
            text=t("pages.devices.workbench.preset_center.editor.load_button", default="载入当前"),
            command=self._load_selected_preset_into_editor,
        ).grid(row=0, column=1, padx=6, pady=2)
        ttk.Button(
            editor_actions,
            text=t("pages.devices.workbench.preset_center.editor.save_button", default="保存"),
            command=self._save_custom_preset_from_editor,
        ).grid(row=0, column=2, padx=6, pady=2)
        ttk.Button(
            editor_actions,
            text=t("pages.devices.workbench.preset_center.editor.delete_button", default="删除"),
            command=self._delete_loaded_custom_preset,
        ).grid(row=0, column=3, padx=6, pady=2)
        ttk.Label(editor_actions, textvariable=self.custom_preset_id_var, style="Muted.TLabel").grid(
            row=0,
            column=4,
            padx=(12, 0),
            pady=2,
            sticky="w",
        )
        ttk.Label(
            self.custom_preset_editor.body,
            text=t("pages.devices.workbench.preset_center.editor.group", default="归属组"),
        ).grid(row=1, column=0, sticky="w", pady=2)
        self.custom_preset_group_selector = ttk.Combobox(
            self.custom_preset_editor.body,
            textvariable=self.custom_preset_group_var,
            state="readonly",
            width=16,
        )
        self.custom_preset_group_selector.grid(row=1, column=1, sticky="ew", padx=(6, 12), pady=2)
        ttk.Label(
            self.custom_preset_editor.body,
            text=t("pages.devices.workbench.preset_center.editor.name", default="名称"),
        ).grid(row=1, column=2, sticky="w", pady=2)
        ttk.Entry(self.custom_preset_editor.body, textvariable=self.custom_preset_name_var).grid(
            row=1,
            column=3,
            sticky="ew",
            padx=(6, 0),
            pady=2,
        )
        ttk.Label(
            self.custom_preset_editor.body,
            text=t("pages.devices.workbench.preset_center.editor.description", default="说明"),
        ).grid(row=2, column=0, sticky="w", pady=2)
        ttk.Entry(self.custom_preset_editor.body, textvariable=self.custom_preset_description_var).grid(
            row=2,
            column=1,
            columnspan=3,
            sticky="ew",
            padx=(6, 0),
            pady=2,
        )
        ttk.Label(
            self.custom_preset_editor.body,
            text=t("pages.devices.workbench.preset_center.editor.analyzer_index", default="分析仪编号"),
        ).grid(row=3, column=0, sticky="w", pady=2)
        ttk.Entry(self.custom_preset_editor.body, textvariable=self.custom_preset_analyzer_index_var, width=10).grid(
            row=3,
            column=1,
            sticky="w",
            padx=(6, 12),
            pady=2,
        )
        ttk.Label(
            self.custom_preset_editor.body,
            text=t("pages.devices.workbench.preset_center.editor.pressure_hpa", default="目标压力(hPa)"),
        ).grid(row=3, column=2, sticky="w", pady=2)
        ttk.Entry(self.custom_preset_editor.body, textvariable=self.custom_preset_pressure_var, width=12).grid(
            row=3,
            column=3,
            sticky="w",
            padx=(6, 0),
            pady=2,
        )
        ttk.Label(
            self.custom_preset_editor.body,
            text=t("pages.devices.workbench.preset_center.editor.relay_name", default="继电器"),
        ).grid(row=4, column=0, sticky="w", pady=2)
        self.custom_preset_relay_selector = ttk.Combobox(
            self.custom_preset_editor.body,
            textvariable=self.custom_preset_relay_var,
            state="readonly",
            width=16,
        )
        self.custom_preset_relay_selector.grid(row=4, column=1, sticky="w", padx=(6, 12), pady=2)
        ttk.Label(
            self.custom_preset_editor.body,
            text=t("pages.devices.workbench.preset_center.editor.channel", default="通道"),
        ).grid(row=4, column=2, sticky="w", pady=2)
        ttk.Entry(self.custom_preset_editor.body, textvariable=self.custom_preset_channel_var, width=12).grid(
            row=4,
            column=3,
            sticky="w",
            padx=(6, 0),
            pady=2,
        )
        self.custom_step_device_selectors: list[ttk.Combobox] = []
        self.custom_step_preset_selectors: list[ttk.Combobox] = []
        for index in range(3):
            row = 5 + index
            ttk.Label(
                self.custom_preset_editor.body,
                text=t(
                    "pages.devices.workbench.preset_center.editor.step",
                    index=index + 1,
                    default=f"步骤 {index + 1}",
                ),
            ).grid(row=row, column=0, sticky="w", pady=2)
            device_selector = ttk.Combobox(
                self.custom_preset_editor.body,
                textvariable=self.custom_step_device_vars[index],
                state="readonly",
                width=18,
            )
            device_selector.grid(row=row, column=1, sticky="ew", padx=(6, 12), pady=2)
            device_selector.bind(
                "<<ComboboxSelected>>",
                lambda _event, step_index=index: self._refresh_custom_step_options(step_index),
                add="+",
            )
            self.custom_step_device_selectors.append(device_selector)
            preset_selector = ttk.Combobox(
                self.custom_preset_editor.body,
                textvariable=self.custom_step_preset_vars[index],
                state="readonly",
                width=28,
            )
            preset_selector.grid(row=row, column=2, columnspan=2, sticky="ew", padx=(6, 0), pady=2)
            self.custom_step_preset_selectors.append(preset_selector)
            self._custom_step_preset_lookup.append({})
        ttk.Label(
            self.preset_center_frame,
            textvariable=self.preset_detail_var,
            wraplength=980,
            justify="left",
        ).grid(row=7, column=0, sticky="ew")
        ttk.Label(
            self.preset_center_frame,
            textvariable=self.preset_center_summary_var,
            style="Muted.TLabel",
            wraplength=980,
            justify="left",
        ).grid(row=8, column=0, sticky="ew")

        self.preset_manager_section = CollapsibleSection(
            self.preset_center_frame,
            title=t("pages.devices.workbench.preset_center.manager.title", default="预置管理器"),
            summary=t("pages.devices.workbench.preset_center.manager.summary", default="导入 / 导出 / 复制 simulation-only 预置"),
            expanded=False,
        )
        self.preset_manager_section.grid(row=6, column=0, sticky="ew", pady=(0, 6))
        self.preset_manager_section.body.columnconfigure(0, weight=1)
        manager_actions = ttk.Frame(self.preset_manager_section.body)
        manager_actions.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Button(
            manager_actions,
            text=t("pages.devices.workbench.preset_center.manager.duplicate_button", default="复制当前"),
            command=self._duplicate_selected_preset,
        ).grid(row=0, column=0, padx=(0, 6), pady=2)
        ttk.Button(
            manager_actions,
            text=t("pages.devices.workbench.preset_center.manager.export_selected_button", default="导出当前"),
            command=lambda: self._export_preset_bundle("selected"),
        ).grid(row=0, column=1, padx=6, pady=2)
        ttk.Button(
            manager_actions,
            text=t("pages.devices.workbench.preset_center.manager.export_group_button", default="导出当前组"),
            command=lambda: self._export_preset_bundle("group"),
        ).grid(row=0, column=2, padx=6, pady=2)
        ttk.Button(
            manager_actions,
            text=t("pages.devices.workbench.preset_center.manager.export_all_button", default="导出全部"),
            command=lambda: self._export_preset_bundle("all"),
        ).grid(row=0, column=3, padx=6, pady=2)
        ttk.Button(
            manager_actions,
            text=t("pages.devices.workbench.preset_center.manager.import_button", default="导入 JSON"),
            command=self._import_preset_bundle,
        ).grid(row=0, column=4, padx=6, pady=2)
        ttk.Label(
            manager_actions,
            text=t("pages.devices.workbench.preset_center.manager.conflict_policy_label", default="冲突处理"),
        ).grid(row=0, column=5, padx=(12, 4), pady=2, sticky="w")
        self.preset_import_conflict_selector = ttk.Combobox(
            manager_actions,
            textvariable=self.preset_import_conflict_var,
            state="readonly",
            width=18,
        )
        self.preset_import_conflict_selector.grid(row=0, column=6, padx=(0, 6), pady=2, sticky="w")
        ttk.Label(
            self.preset_manager_section.body,
            textvariable=self.preset_manager_summary_var,
            style="Muted.TLabel",
            wraplength=980,
            justify="left",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 6))
        self.preset_bundle_text = tk.Text(self.preset_manager_section.body, height=8, wrap="word")
        self.preset_bundle_text.grid(row=2, column=0, sticky="nsew")
        manager_scroll = ttk.Scrollbar(self.preset_manager_section.body, orient="vertical", command=self.preset_bundle_text.yview)
        manager_scroll.grid(row=2, column=1, sticky="ns", padx=(6, 0))
        self.preset_bundle_text.configure(yscrollcommand=manager_scroll.set)

        self.notebook = ttk.Notebook(self)
        self.notebook.grid(row=6, column=0, sticky="nsew", pady=(8, 0))
        self.notebook.bind("<<NotebookTabChanged>>", self._sync_preset_group_to_tab, add="+")

        self._build_analyzer_tab()
        self._build_pace_tab()
        self._build_grz_tab()
        self._build_chamber_tab()
        self._build_relay_tab()
        self._build_thermometer_tab()
        self._build_pressure_tab()

        self.operator_history_frame = ttk.LabelFrame(
            self,
            text=t("pages.devices.workbench.summary.history_label"),
            padding=8,
            style="Card.TFrame",
        )
        self.operator_history_frame.grid(row=7, column=0, sticky="nsew", pady=(8, 0))
        self.operator_history_frame.columnconfigure(0, weight=1)
        self.operator_history_frame.rowconfigure(1, weight=1)
        operator_filters = ttk.Frame(self.operator_history_frame)
        operator_filters.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(operator_filters, text=t("pages.devices.workbench.history.filter.device", default="设备筛选")).grid(row=0, column=0, sticky="w")
        self.history_device_filter_selector = ttk.Combobox(
            operator_filters,
            textvariable=self.history_device_filter_var,
            state="readonly",
            width=16,
        )
        self.history_device_filter_selector.grid(row=0, column=1, padx=(6, 12))
        self.history_device_filter_selector.bind("<<ComboboxSelected>>", self._apply_history_filters, add="+")
        ttk.Label(operator_filters, text=t("pages.devices.workbench.history.filter.result", default="结果筛选")).grid(row=0, column=2, sticky="w")
        self.history_result_filter_selector = ttk.Combobox(
            operator_filters,
            textvariable=self.history_result_filter_var,
            state="readonly",
            width=16,
        )
        self.history_result_filter_selector.grid(row=0, column=3, padx=(6, 0))
        self.history_result_filter_selector.bind("<<ComboboxSelected>>", self._apply_history_filters, add="+")
        operator_history_container = ttk.Frame(self.operator_history_frame)
        operator_history_container.grid(row=1, column=0, sticky="nsew")
        operator_history_container.columnconfigure(0, weight=1)
        operator_history_container.rowconfigure(0, weight=1)
        self.operator_history_tree = self._build_history_tree(
            operator_history_container,
            columns=("sequence", "time", "device", "action", "result"),
            height=4,
        )
        self.operator_history_tree.grid(row=0, column=0, sticky="nsew")
        operator_history_scroll = ttk.Scrollbar(operator_history_container, orient="vertical", command=self.operator_history_tree.yview)
        operator_history_scroll.grid(row=0, column=1, sticky="ns", padx=(6, 0))
        self.operator_history_tree.configure(yscrollcommand=operator_history_scroll.set)
        self.operator_history_tree.bind("<<TreeviewSelect>>", self._on_operator_history_selected, add="+")
        ttk.Label(self.operator_history_frame, textvariable=self.history_detail_var, wraplength=1020, justify="left").grid(row=2, column=0, sticky="ew", pady=(8, 0))
        history_links = ttk.Frame(self.operator_history_frame)
        history_links.grid(row=3, column=0, sticky="ew", pady=(6, 0))
        history_links.columnconfigure(1, weight=1)
        history_links.columnconfigure(3, weight=1)
        ttk.Button(
            history_links,
            text=t("pages.devices.workbench.history.jump_snapshot"),
            command=self._jump_history_snapshot,
        ).grid(row=0, column=0, padx=(0, 6), pady=2, sticky="w")
        ttk.Label(history_links, textvariable=self.history_snapshot_var, style="Muted.TLabel", wraplength=420, justify="left").grid(
            row=0,
            column=1,
            sticky="ew",
        )
        ttk.Button(
            history_links,
            text=t("pages.devices.workbench.history.jump_evidence"),
            command=self._jump_history_evidence,
        ).grid(row=0, column=2, padx=(12, 6), pady=2, sticky="w")
        ttk.Label(history_links, textvariable=self.history_evidence_var, style="Muted.TLabel", wraplength=420, justify="left").grid(
            row=0,
            column=3,
            sticky="ew",
        )

        self.engineer_frame = ttk.LabelFrame(self, text=t("pages.devices.workbench.view.engineer_view"), padding=8, style="Card.TFrame")
        self.engineer_frame.grid(row=8, column=0, sticky="nsew", pady=(8, 0))
        self.engineer_frame.columnconfigure(0, weight=1)
        self.engineer_frame.rowconfigure(0, weight=1)
        self.engineer_notebook = ttk.Notebook(self.engineer_frame)
        self.engineer_notebook.grid(row=0, column=0, sticky="nsew")

        self.engineer_overview_tab = ttk.Frame(self.engineer_notebook, padding=8)
        self.engineer_overview_tab.columnconfigure(0, weight=1)
        self.engineer_overview_tab.rowconfigure(2, weight=1)
        self.engineer_cards_frame = ttk.Frame(self.engineer_overview_tab, style="Card.TFrame")
        self.engineer_cards_frame.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        self.engineer_status_frame = ttk.Frame(self.engineer_overview_tab, style="Card.TFrame")
        self.engineer_status_frame.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        self.engineer_sections_frame = ttk.Frame(self.engineer_overview_tab, style="Card.TFrame")
        self.engineer_sections_frame.grid(row=2, column=0, sticky="nsew")
        self.engineer_sections_frame.columnconfigure(0, weight=1)
        self.engineer_notebook.add(self.engineer_overview_tab, text=t("pages.devices.workbench.engineer_overview"))

        self.context_tab = ttk.Frame(self.engineer_notebook, padding=8)
        self.context_tab.columnconfigure(0, weight=1)
        self.context_tab.rowconfigure(0, weight=1)
        self.context_text = tk.Text(self.context_tab, height=10, wrap="word")
        self.context_text.grid(row=0, column=0, sticky="nsew")
        self.context_scroll = ttk.Scrollbar(self.context_tab, orient="vertical", command=self.context_text.yview)
        self.context_scroll.grid(row=0, column=1, sticky="ns")
        self.context_text.configure(yscrollcommand=self.context_scroll.set)
        self.engineer_notebook.add(self.context_tab, text=t("pages.devices.workbench.summary.engineer_label"))

        self.diagnostic_tab = ttk.Frame(self.engineer_notebook, padding=8)
        self.diagnostic_tab.columnconfigure(0, weight=1)
        self.diagnostic_tab.rowconfigure(0, weight=1)
        self.diagnostic_text = tk.Text(self.diagnostic_tab, height=10, wrap="word")
        self.diagnostic_text.grid(row=0, column=0, sticky="nsew")
        self.diagnostic_scroll = ttk.Scrollbar(self.diagnostic_tab, orient="vertical", command=self.diagnostic_text.yview)
        self.diagnostic_scroll.grid(row=0, column=1, sticky="ns")
        self.diagnostic_text.configure(yscrollcommand=self.diagnostic_scroll.set)
        self.engineer_notebook.add(self.diagnostic_tab, text=t("pages.devices.workbench.summary.diagnostic_history"))

        self.history_tab = ttk.Frame(self.engineer_notebook, padding=8)
        self.history_tab.columnconfigure(0, weight=1)
        self.history_tab.rowconfigure(1, weight=1)
        self.history_tab.rowconfigure(2, weight=1)
        ttk.Label(self.history_tab, text=t("pages.devices.workbench.summary.diagnostic_history"), style="Muted.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        history_tree_frame = ttk.Frame(self.history_tab)
        history_tree_frame.grid(row=1, column=0, sticky="nsew")
        history_tree_frame.columnconfigure(0, weight=1)
        history_tree_frame.rowconfigure(0, weight=1)
        self.history_tree = self._build_history_tree(
            history_tree_frame,
            columns=("sequence", "time", "device", "action", "result", "fault"),
            height=6,
        )
        self.history_tree.grid(row=0, column=0, sticky="nsew")
        self.history_scroll = ttk.Scrollbar(history_tree_frame, orient="vertical", command=self.history_tree.yview)
        self.history_scroll.grid(row=0, column=1, sticky="ns", padx=(6, 0))
        self.history_tree.configure(yscrollcommand=self.history_scroll.set)
        self.history_tree.bind("<<TreeviewSelect>>", self._on_engineer_history_selected, add="+")
        self.history_detail_text = tk.Text(self.history_tab, height=7, wrap="word")
        self.history_detail_text.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        self.history_detail_scroll = ttk.Scrollbar(self.history_tab, orient="vertical", command=self.history_detail_text.yview)
        self.history_detail_scroll.grid(row=2, column=1, sticky="ns", pady=(8, 0))
        self.history_detail_text.configure(yscrollcommand=self.history_detail_scroll.set)
        self.engineer_notebook.add(self.history_tab, text=t("pages.devices.workbench.summary.history_label"))

        self.evidence_tab = ttk.Frame(self.engineer_notebook, padding=8)
        self.evidence_tab.columnconfigure(0, weight=1)
        self.evidence_tab.rowconfigure(0, weight=1)
        self.evidence_text = tk.Text(self.evidence_tab, height=10, wrap="word")
        self.evidence_text.grid(row=0, column=0, sticky="nsew")
        self.evidence_scroll = ttk.Scrollbar(self.evidence_tab, orient="vertical", command=self.evidence_text.yview)
        self.evidence_scroll.grid(row=0, column=1, sticky="ns")
        self.evidence_text.configure(yscrollcommand=self.evidence_scroll.set)
        self.engineer_notebook.add(self.evidence_tab, text=t("pages.devices.workbench.engineer_evidence"))

        self.compare_tab = ttk.Frame(self.engineer_notebook, padding=8)
        self.compare_tab.columnconfigure(0, weight=1)
        self.compare_tab.rowconfigure(1, weight=1)
        compare_toolbar = ttk.Frame(self.compare_tab)
        compare_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(compare_toolbar, text=t("pages.devices.workbench.snapshot.left", default="快照一")).grid(row=0, column=0, sticky="w")
        self.snapshot_left_selector = ttk.Combobox(compare_toolbar, textvariable=self.snapshot_left_var, state="readonly", width=28)
        self.snapshot_left_selector.grid(row=0, column=1, padx=(6, 12))
        ttk.Label(compare_toolbar, text=t("pages.devices.workbench.snapshot.right", default="快照二")).grid(row=0, column=2, sticky="w")
        self.snapshot_right_selector = ttk.Combobox(compare_toolbar, textvariable=self.snapshot_right_var, state="readonly", width=28)
        self.snapshot_right_selector.grid(row=0, column=3, padx=(6, 12))
        ttk.Button(compare_toolbar, text=t("pages.devices.workbench.snapshot.compare_button", default="对比快照"), command=self._apply_snapshot_compare).grid(row=0, column=4, padx=4)
        self.snapshot_compare_text = tk.Text(self.compare_tab, height=10, wrap="word")
        self.snapshot_compare_text.grid(row=1, column=0, sticky="nsew")
        self.snapshot_compare_scroll = ttk.Scrollbar(self.compare_tab, orient="vertical", command=self.snapshot_compare_text.yview)
        self.snapshot_compare_scroll.grid(row=1, column=1, sticky="ns")
        self.snapshot_compare_text.configure(yscrollcommand=self.snapshot_compare_scroll.set)
        self.engineer_notebook.add(self.compare_tab, text=t("pages.devices.workbench.snapshot.title", default="快照对比"))
        self.engineer_frame.grid_remove()

    def _summary_card(
        self,
        parent: tk.Misc,
        row: int,
        column: int,
        label: str,
        value_var: tk.StringVar,
    ) -> None:
        frame = ttk.Frame(parent, style="SoftCard.TFrame", padding=8)
        frame.grid(row=row, column=column, sticky="nsew", padx=4, pady=4)
        frame.columnconfigure(0, weight=1)
        ttk.Label(frame, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(frame, textvariable=value_var, wraplength=440, justify="left").grid(row=1, column=0, sticky="ew", pady=(4, 0))

    def _register_tab(self, frame: ttk.Frame, *, kind: str, title: str) -> None:
        self.notebook.add(frame, text=title)
        self._tab_device_kinds[str(frame)] = kind

    def _build_preset_bar(self, parent: tk.Misc, *, row: int, device_kind: str) -> None:
        frame = ttk.Frame(parent)
        frame.grid(row=row, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(frame, text=t("pages.devices.workbench.field.preset", default="常用预置"), style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        selector = ttk.Combobox(frame, textvariable=self._preset_vars[device_kind], state="readonly", width=24)
        selector.grid(row=0, column=1, padx=(6, 4))
        self._preset_selectors[device_kind] = selector
        ttk.Button(
            frame,
            text=t("pages.devices.workbench.button.run_preset", default="执行预置"),
            command=lambda kind=device_kind: self._run_selected_preset(kind),
        ).grid(row=0, column=2, padx=4)

    def _build_history_tree(self, parent: tk.Misc, *, columns: tuple[str, ...], height: int) -> ttk.Treeview:
        tree = ttk.Treeview(parent, columns=columns, show="headings", height=height)
        labels = {
            "sequence": (t("pages.devices.workbench.field.sequence"), 70),
            "time": (t("pages.devices.workbench.field.time"), 150),
            "device": (t("pages.devices.workbench.field.device"), 150),
            "action": (t("pages.devices.workbench.field.action_name"), 180),
            "result": (t("pages.devices.workbench.field.result"), 90),
            "fault": (t("pages.devices.workbench.field.fault_injection"), 90),
        }
        for column in columns:
            text, width = labels[column]
            tree.heading(column, text=text)
            tree.column(column, width=width, anchor="w")
        return tree

    def _analyzer_index(self) -> int:
        try:
            return max(0, int(self.analyzer_index_var.get()) - 1)
        except Exception:
            return 0

    def _current_device_kind(self) -> str:
        selected = self.notebook.select()
        return self._tab_device_kinds.get(str(selected), "workbench")

    def _run_selected_quick_scenario(self) -> None:
        label = str(self.quick_scenario_var.get() or "").strip()
        scenario_id = self._quick_scenario_lookup.get(label, "")
        if scenario_id:
            self._invoke("workbench", "run_quick_scenario", scenario_id=scenario_id, current_device=self._current_device_kind())

    def _generate_evidence(self) -> None:
        self._invoke("workbench", "generate_diagnostic_evidence", current_device=self._current_device_kind(), current_action="generate_diagnostic_evidence")

    def _run_selected_preset(self, device_kind: str) -> None:
        label = str(self._preset_vars[device_kind].get() or "").strip()
        preset_id = self._preset_lookup.get(device_kind, {}).get(label, "")
        if not preset_id:
            return
        params = self._preset_params(device_kind)
        self._invoke(device_kind, "run_preset", preset_id=preset_id, **params)

    def _run_selected_center_preset(self) -> None:
        label = str(self.preset_center_var.get() or "").strip()
        device_kind, preset_id = self._preset_center_lookup.get(label, ("", ""))
        if not preset_id:
            return
        self._run_preset_entry(device_kind, preset_id)

    def _run_recent_preset(self) -> None:
        label = str(self.recent_preset_var.get() or "").strip()
        device_kind, preset_id = self._recent_preset_lookup.get(label, ("", ""))
        if not preset_id:
            return
        self._run_preset_entry(device_kind, preset_id)

    def _run_preset_entry(self, device_kind: str, preset_id: str) -> None:
        params = self._preset_params(device_kind)
        self._invoke(device_kind, "run_preset", preset_id=preset_id, **params)

    def _on_preset_group_changed(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        self._refresh_preset_group_options()
        self._update_center_preset_detail()

    def _toggle_selected_preset_favorite(self) -> None:
        device_kind, preset_id = self._selected_center_preset()
        if not preset_id:
            return
        self._invoke("workbench", "toggle_preset_favorite", device_kind=device_kind, preset_id=preset_id)

    def _toggle_selected_preset_pin(self) -> None:
        device_kind, preset_id = self._selected_center_preset()
        if not preset_id:
            return
        self._invoke("workbench", "toggle_preset_pin", device_kind=device_kind, preset_id=preset_id)

    def _preset_params(self, device_kind: str) -> dict[str, Any]:
        if device_kind == "analyzer":
            return {"analyzer_index": self._analyzer_index()}
        if device_kind == "pace":
            return {"pressure_hpa": self.pace_pressure_var.get()}
        if device_kind == "relay":
            return {"relay_name": self.relay_target_var.get(), "channel": self.relay_channel_var.get()}
        return {}

    def _set_layout_mode(self, mode: str) -> None:
        normalized = "standard" if str(mode) == "standard" else "compact"
        self._invoke("workbench", "set_layout_mode", layout_mode=normalized)

    def _apply_display_profile(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        selected = str(self.display_profile_var.get() or "").strip()
        profile_id = self._display_profile_lookup.get(selected, "auto")
        self._invoke("workbench", "set_display_profile", display_profile=profile_id)

    def _selected_center_preset(self) -> tuple[str, str]:
        label = str(self.preset_center_var.get() or "").strip()
        return self._preset_center_lookup.get(label, ("", ""))

    def _apply_history_filters(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        device_id = self._history_device_lookup.get(str(self.history_device_filter_var.get() or ""), "all")
        result_id = self._history_result_lookup.get(str(self.history_result_filter_var.get() or ""), "all")
        self._invoke("workbench", "set_history_filters", device_filter=device_id, result_filter=result_id)

    def _on_operator_history_selected(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        self._select_history_detail_from_tree(self.operator_history_tree)

    def _on_engineer_history_selected(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        self._select_history_detail_from_tree(self.history_tree)

    def _select_history_detail_from_tree(self, tree: ttk.Treeview) -> None:
        selected = tree.selection()
        if not selected:
            return
        values = tree.item(selected[0], "values")
        if not values:
            return
        try:
            sequence = int(values[0])
        except Exception:
            return
        self._invoke("workbench", "select_history_detail", sequence=sequence)

    def _apply_snapshot_compare(self) -> None:
        left_sequence = self._snapshot_option_lookup.get(str(self.snapshot_left_var.get() or ""))
        right_sequence = self._snapshot_option_lookup.get(str(self.snapshot_right_var.get() or ""))
        self._invoke("workbench", "set_snapshot_compare", left_sequence=left_sequence, right_sequence=right_sequence)

    def _jump_history_snapshot(self) -> None:
        detail = dict(self._history_snapshot.get("detail", {}) or {})
        related_snapshot = dict(detail.get("related_snapshot", {}) or {})
        if not bool(related_snapshot.get("available", False)):
            return
        left_sequence = related_snapshot.get("sequence")
        right_sequence = related_snapshot.get("compare_sequence")
        self._invoke("workbench", "set_view_mode", view_mode="engineer_view")
        if left_sequence:
            self._invoke("workbench", "set_snapshot_compare", left_sequence=left_sequence, right_sequence=right_sequence)
        self.engineer_notebook.select(self.compare_tab)

    def _jump_history_evidence(self) -> None:
        detail = dict(self._history_snapshot.get("detail", {}) or {})
        related_evidence = dict(detail.get("related_evidence", {}) or {})
        if not bool(related_evidence.get("available", False)):
            return
        self._invoke("workbench", "set_view_mode", view_mode="engineer_view")
        self.engineer_notebook.select(self.evidence_tab)

    def _invoke(self, device_kind: str, action: str, **params: Any) -> dict[str, Any]:
        if self.facade is None or not hasattr(self.facade, "execute_device_workbench_action"):
            return {}
        result = self.facade.execute_device_workbench_action(device_kind, action, **params)
        self.render(dict(result.get("snapshot") or {}))
        return dict(result or {})

    @staticmethod
    def _preset_display_label(item: dict[str, Any]) -> str:
        label = str(item.get("label") or "").strip()
        if not label:
            return ""
        if not bool(item.get("is_custom", False)):
            return label
        source = str(item.get("source_display") or t("pages.devices.workbench.preset_center.source_custom", default="自定义"))
        step_count = int(item.get("step_count", len(list(item.get("steps", []) or []))) or 0)
        return t(
            "pages.devices.workbench.preset_center.custom_picker_label",
            label=label,
            source=source,
            steps=step_count,
            default=f"{label} · {source} {step_count} 步",
        )

    def _sync_preset_group_to_tab(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        current_group = self._group_for_device_kind(self._current_device_kind())
        if not current_group:
            return
        current_label = next(
            (
                label
                for label, item in self._preset_group_lookup.items()
                if str(item.get("id") or "") == current_group
            ),
            "",
        )
        if current_label and self.preset_group_var.get() != current_label:
            self.preset_group_var.set(current_label)
            self._refresh_preset_group_options()

    def _on_center_preset_changed(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        self._update_center_preset_detail()

    @staticmethod
    def _group_for_device_kind(device_kind: str) -> str:
        if str(device_kind) in {"pressure", "pressure_gauge"}:
            return "pressure"
        return str(device_kind or "")

    def _refresh_preset_group_options(self) -> None:
        group_label = str(self.preset_group_var.get() or "").strip()
        group_payload = dict(self._preset_group_lookup.get(group_label, {}) or {})
        presets = list(group_payload.get("presets", []) or [])
        preset_labels = [self._preset_display_label(item) for item in presets if self._preset_display_label(item)]
        self._preset_center_lookup = {
            self._preset_display_label(item): (
                str(item.get("device_kind") or ""),
                str(item.get("id") or ""),
            )
            for item in presets
            if self._preset_display_label(item)
        }
        self.preset_center_selector.configure(values=preset_labels)
        if preset_labels:
            current = str(self.preset_center_var.get() or "")
            if current not in preset_labels:
                self.preset_center_var.set(preset_labels[0])
        else:
            self.preset_center_var.set("")
        self._render_preset_strip(self.pinned_preset_buttons, list(group_payload.get("pinned_presets", []) or []))
        self._render_preset_strip(
            self.custom_preset_buttons,
            list(group_payload.get("custom_presets", []) or []),
            empty_text=t("pages.devices.workbench.preset_center.no_custom_presets", default="当前分组暂无自定义预置"),
        )
        for child in self.frequent_preset_buttons.winfo_children():
            child.destroy()
        frequent = list(group_payload.get("frequent_presets", []) or [])
        if not frequent:
            ttk.Label(self.frequent_preset_buttons, text=t("common.none"), style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        else:
            for index, item in enumerate(frequent):
                ttk.Button(
                    self.frequent_preset_buttons,
                    text=self._preset_display_label(item),
                    command=lambda device_kind=str(item.get("device_kind") or ""), preset_id=str(item.get("id") or ""): self._run_preset_entry(device_kind, preset_id),
                ).grid(row=0, column=index, padx=(0, 6), pady=2, sticky="w")
        self._update_center_preset_detail()

    def _render_preset_strip(self, parent: ttk.Frame, items: list[dict[str, Any]], *, empty_text: str | None = None) -> None:
        for child in parent.winfo_children():
            child.destroy()
        if not items:
            ttk.Label(parent, text=empty_text or t("common.none"), style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            return
        for index, item in enumerate(items):
            ttk.Button(
                parent,
                text=self._preset_display_label(item),
                command=lambda device_kind=str(item.get("device_kind") or ""), preset_id=str(item.get("id") or ""): self._run_preset_entry(device_kind, preset_id),
            ).grid(row=0, column=index, padx=(0, 6), pady=2, sticky="w")

    def _update_center_preset_detail(self) -> None:
        device_kind, preset_id = self._selected_center_preset()
        if not preset_id:
            self.preset_detail_var.set(t("common.none"))
            return
        group_label = str(self.preset_group_var.get() or "").strip()
        group_payload = dict(self._preset_group_lookup.get(group_label, {}) or {})
        preset_payload = next(
            (
                dict(item)
                for item in list(group_payload.get("presets", []) or [])
                if str(item.get("id") or "") == preset_id and str(item.get("device_kind") or "") == device_kind
            ),
            {},
        )
        description = str(preset_payload.get("description") or t("common.none"))
        pinned = (
            t("pages.devices.workbench.preset_center.pin_enabled", default="Pinned")
            if bool(preset_payload.get("is_pinned", False))
            else t("pages.devices.workbench.preset_center.pin_disabled", default="Not pinned")
        )
        favorite = (
            t("pages.devices.workbench.preset_center.favorite_enabled", default="Favorited")
            if bool(preset_payload.get("is_favorite", False))
            else t("pages.devices.workbench.preset_center.favorite_disabled", default="Not favorited")
        )
        usage = str(preset_payload.get("usage_count", 0) or 0)
        source = str(preset_payload.get("source_display") or t("pages.devices.workbench.preset_center.source_builtin", default="Built-in"))
        step_count = int(preset_payload.get("step_count", len(list(preset_payload.get("steps", []) or []))) or 0)
        metadata = str(preset_payload.get("metadata_summary") or t("common.none"))
        capability_summary = str(preset_payload.get("fake_capability_summary") or t("common.none"))
        parameters = dict(preset_payload.get("parameters", {}) or {})
        parameter_summary = ", ".join(
            f"{key}={value}"
            for key, value in parameters.items()
            if value not in ("", None, [], {})
        ) or t("common.none")
        detail_lines = [
            t(
                "pages.devices.workbench.preset_center.detail",
                description=description,
                pinned=pinned,
                favorite=favorite,
                source=source,
                steps=step_count,
                usage=usage,
                metadata=metadata,
                default=f"{source} | {description} | {pinned} | {favorite} | {step_count} steps | used {usage} | {metadata}",
            )
        ]
        detail_lines.append(
            t(
                "pages.devices.workbench.preset_center.detail_capabilities",
                capabilities=capability_summary,
                default=capability_summary,
            )
        )
        current_view_mode = str(dict(self._workbench_payload.get("meta", {}) or {}).get("view_mode") or "")
        if current_view_mode == "engineer_view":
            detail_lines.append(
                t(
                    "pages.devices.workbench.preset_center.detail_origin",
                    origin=str(preset_payload.get("origin_display") or t("common.none")),
                    imported_from=str(preset_payload.get("imported_from") or t("common.none")),
                    source_ref=str(preset_payload.get("source_ref") or t("common.none")),
                    default=(
                        f"{preset_payload.get('origin_display') or t('common.none')} | "
                        f"{preset_payload.get('imported_from') or t('common.none')} | "
                        f"{preset_payload.get('source_ref') or t('common.none')}"
                    ),
                )
            )
            detail_lines.append(
                t(
                    "pages.devices.workbench.preset_center.detail_parameters",
                    parameters=parameter_summary,
                    default=parameter_summary,
                )
            )
        self.preset_detail_var.set("\n".join(detail_lines))

    def _apply_layout_mode(self) -> None:
        compact = self._layout_mode == "compact"
        self.operator_history_tree.configure(height=4 if compact else 6)
        self.history_tree.configure(height=6 if compact else 8)
        self.context_text.configure(height=8 if compact else 10)
        self.diagnostic_text.configure(height=8 if compact else 10)
        self.history_detail_text.configure(height=6 if compact else 8)
        self.evidence_text.configure(height=8 if compact else 10)
        self.snapshot_compare_text.configure(height=8 if compact else 10)

    def _render_preset_center(self, payload: dict[str, Any]) -> None:
        self._preset_center_payload = dict(payload or {})
        self.preset_center_summary_var.set(str(payload.get("summary") or t("common.none")))
        manager = dict(payload.get("manager", {}) or {})
        manager_lines = [
            str(
                manager.get("summary")
                or t(
                    "pages.devices.workbench.preset_center.manager.summary",
                    default="导入 / 导出 / 复制 simulation-only 预置",
                )
            ).strip(),
            str(manager.get("directory_summary") or "").strip(),
            str(manager.get("bundle_format_summary") or "").strip(),
            str(manager.get("conflict_policy_summary") or "").strip(),
            str(manager.get("conflict_strategy_summary") or "").strip(),
            str(manager.get("sharing_reserved_fields_summary") or "").strip(),
            str(manager.get("bundle_profile_summary") or "").strip(),
            str(manager.get("sharing_ready_summary") or "").strip(),
            str(manager.get("selected_preset_metadata_summary") or "").strip(),
            (
                t(
                    "pages.devices.workbench.preset_center.detail_capabilities",
                    capabilities=str(manager.get("selected_preset_capability_summary") or t("common.none")),
                    default=str(manager.get("selected_preset_capability_summary") or t("common.none")),
                ).strip()
                if str(manager.get("selected_preset_capability_summary") or "").strip()
                else ""
            ),
        ]
        self.preset_manager_summary_var.set("\n".join(line for line in manager_lines if line) or t("common.none"))
        conflict_options = [dict(item) for item in list(manager.get("import_conflict_policy_options", []) or [])]
        self._preset_import_conflict_lookup = {
            str(item.get("label") or ""): str(item.get("id") or "")
            for item in conflict_options
            if str(item.get("label") or "").strip()
        }
        conflict_labels = list(self._preset_import_conflict_lookup.keys())
        self.preset_import_conflict_selector.configure(values=conflict_labels)
        selected_conflict_policy = next(
            (
                str(item.get("label") or "")
                for item in conflict_options
                if str(item.get("id") or "") == str(manager.get("selected_import_conflict_policy") or "")
            ),
            conflict_labels[0] if conflict_labels else "",
        )
        if selected_conflict_policy:
            self.preset_import_conflict_var.set(selected_conflict_policy)
        groups = [dict(item) for item in list(payload.get("groups", []) or [])]
        self._preset_group_lookup = {
            str(item.get("label") or ""): dict(item)
            for item in groups
        }
        group_labels = [str(item.get("label") or "") for item in groups if str(item.get("label") or "").strip()]
        self.preset_group_selector.configure(values=group_labels)
        current_group = str(self.preset_group_var.get() or "")
        if current_group not in group_labels:
            preferred_group = self._group_for_device_kind(self._current_device_kind())
            current_group = next(
                (
                    str(item.get("label") or "")
                    for item in groups
                    if str(item.get("id") or "") == preferred_group
                ),
                group_labels[0] if group_labels else "",
            )
            if current_group:
                self.preset_group_var.set(current_group)
        recent = [dict(item) for item in list(payload.get("recent_presets", []) or [])]
        self._recent_preset_lookup = {
            self._preset_display_label(item) or str(item.get("run_label") or ""): (
                str(item.get("device_kind") or ""),
                str(item.get("id") or ""),
            )
            for item in recent
        }
        recent_labels = [
            self._preset_display_label(item) or str(item.get("run_label") or "")
            for item in recent
            if (self._preset_display_label(item) or str(item.get("run_label") or "")).strip()
        ]
        self.recent_preset_selector.configure(values=recent_labels)
        if recent_labels:
            current_recent = str(self.recent_preset_var.get() or "")
            if current_recent not in recent_labels:
                self.recent_preset_var.set(recent_labels[0])
        else:
            self.recent_preset_var.set("")
        custom_count = len(list(payload.get("custom_presets", []) or []))
        self.custom_preset_summary_var.set(
            t(
                "pages.devices.workbench.preset_center.custom_summary",
                count=custom_count,
                default=f"当前共有 {custom_count} 个 simulation-only 自定义预置。",
            )
        )
        self._render_custom_preset_editor(payload)
        self._refresh_preset_group_options()

    def _render_custom_preset_editor(self, payload: dict[str, Any]) -> None:
        editor = dict(payload.get("editor", {}) or {})
        group_options = [dict(item) for item in list(editor.get("group_options", []) or [])]
        device_options = [dict(item) for item in list(editor.get("device_options", []) or [])]
        self._custom_group_lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in group_options}
        self._custom_device_lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in device_options}
        self._custom_step_preset_catalog = {
            str(key): [dict(row) for row in list(value or [])]
            for key, value in dict(editor.get("step_catalog", {}) or {}).items()
        }
        group_labels = [str(item.get("label") or "") for item in group_options if str(item.get("label") or "").strip()]
        device_labels = [str(item.get("label") or "") for item in device_options if str(item.get("label") or "").strip()]
        self.custom_preset_group_selector.configure(values=group_labels)
        if group_labels and self.custom_preset_group_var.get() not in group_labels:
            preferred_label = next(
                (
                    str(item.get("label") or "")
                    for item in group_options
                    if str(item.get("id") or "") == str(payload.get("selected_group_id") or "")
                ),
                group_labels[0],
            )
            self.custom_preset_group_var.set(preferred_label)
        for selector in self.custom_step_device_selectors:
            selector.configure(values=device_labels)
        relay_options = [
            ("relay", t("pages.devices.workbench.device.relay")),
            ("relay_8", t("pages.devices.workbench.device.relay_8")),
        ]
        self._custom_relay_lookup = {label: relay_id for relay_id, label in relay_options}
        self.custom_preset_relay_selector.configure(values=[label for _, label in relay_options])
        if self.custom_preset_relay_var.get() not in self._custom_relay_lookup:
            self.custom_preset_relay_var.set(t("pages.devices.workbench.device.relay"))
        for index in range(len(self.custom_step_device_vars)):
            self._refresh_custom_step_options(index)

    def _refresh_custom_step_options(self, index: int) -> None:
        if index < 0 or index >= len(self.custom_step_device_vars):
            return
        device_label = str(self.custom_step_device_vars[index].get() or "").strip()
        device_id = self._custom_device_lookup.get(device_label, "")
        preset_rows = list(self._custom_step_preset_catalog.get(device_id, []) or [])
        lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in preset_rows}
        self._custom_step_preset_lookup[index] = lookup
        labels = [str(item.get("label") or "") for item in preset_rows if str(item.get("label") or "").strip()]
        self.custom_step_preset_selectors[index].configure(values=labels)
        if self.custom_step_preset_vars[index].get() not in labels:
            self.custom_step_preset_vars[index].set(labels[0] if labels else "")

    def _selected_center_preset_payload(self) -> dict[str, Any]:
        device_kind, preset_id = self._selected_center_preset()
        if not preset_id:
            return {}
        group_label = str(self.preset_group_var.get() or "").strip()
        group_payload = dict(self._preset_group_lookup.get(group_label, {}) or {})
        return next(
            (
                dict(item)
                for item in list(group_payload.get("presets", []) or [])
                if str(item.get("id") or "") == preset_id and str(item.get("device_kind") or "") == device_kind
            ),
            {},
        )

    def _populate_custom_editor(self, payload: dict[str, Any]) -> None:
        group_id = str(payload.get("group_id") or "")
        group_label = next(
            (label for label, item_id in self._custom_group_lookup.items() if item_id == group_id),
            self.custom_preset_group_var.get(),
        )
        if group_label:
            self.custom_preset_group_var.set(group_label)
        preset_id = str(payload.get("id") or "")
        self._loaded_custom_preset_id = preset_id
        self.custom_preset_id_var.set(
            t(
                "pages.devices.workbench.preset_center.editor.loaded_id",
                preset_id=preset_id or t("pages.devices.workbench.preset_center.editor.unsaved", default="未保存"),
                default=f"预置 ID: {preset_id or t('pages.devices.workbench.preset_center.editor.unsaved', default='未保存')}",
            )
        )
        self.custom_preset_name_var.set(str(payload.get("label") or payload.get("name") or ""))
        self.custom_preset_description_var.set(str(payload.get("description") or ""))
        parameters = dict(payload.get("parameters", {}) or {})
        self.custom_preset_analyzer_index_var.set(str(parameters.get("analyzer_index", 1) or 1))
        self.custom_preset_pressure_var.set(str(parameters.get("pressure_hpa", 1000) or 1000))
        relay_id = str(parameters.get("relay_name") or "relay")
        relay_label = next((label for label, item_id in self._custom_relay_lookup.items() if item_id == relay_id), t("pages.devices.workbench.device.relay"))
        self.custom_preset_relay_var.set(relay_label)
        self.custom_preset_channel_var.set(str(parameters.get("channel", 1) or 1))
        steps = [dict(item) for item in list(payload.get("steps", []) or [])]
        for index in range(len(self.custom_step_device_vars)):
            step = steps[index] if index < len(steps) else {}
            device_id = str(step.get("device_kind") or "")
            device_label = next((label for label, item_id in self._custom_device_lookup.items() if item_id == device_id), "")
            self.custom_step_device_vars[index].set(device_label)
            self._refresh_custom_step_options(index)
            preset_id = str(step.get("preset_id") or "")
            preset_label = next(
                (label for label, item_id in self._custom_step_preset_lookup[index].items() if item_id == preset_id),
                "",
            )
            self.custom_step_preset_vars[index].set(preset_label)

    def _new_custom_preset(self) -> None:
        preferred_group = str(self.preset_group_var.get() or self.custom_preset_group_var.get() or "")
        self._loaded_custom_preset_id = ""
        self.custom_preset_id_var.set(
            t(
                "pages.devices.workbench.preset_center.editor.loaded_id",
                preset_id=t("pages.devices.workbench.preset_center.editor.unsaved", default="未保存"),
                default=f"预置 ID: {t('pages.devices.workbench.preset_center.editor.unsaved', default='未保存')}",
            )
        )
        if preferred_group:
            self.custom_preset_group_var.set(preferred_group)
        self.custom_preset_name_var.set("")
        self.custom_preset_description_var.set("")
        try:
            analyzer_index = max(1, int(self.analyzer_index_var.get() or 1))
        except Exception:
            analyzer_index = 1
        self.custom_preset_analyzer_index_var.set(str(analyzer_index))
        self.custom_preset_pressure_var.set(str(self.pace_pressure_var.get() or "1000"))
        self.custom_preset_relay_var.set(t("pages.devices.workbench.device.relay"))
        self.custom_preset_channel_var.set(str(self.relay_channel_var.get() or "1"))
        for index in range(len(self.custom_step_device_vars)):
            self.custom_step_device_vars[index].set("")
            self.custom_step_preset_vars[index].set("")
            self._refresh_custom_step_options(index)
        selected = self._selected_center_preset_payload()
        if selected:
            device_label = next(
                (label for label, item_id in self._custom_device_lookup.items() if item_id == str(selected.get("device_kind") or "")),
                "",
            )
            self.custom_step_device_vars[0].set(device_label)
            self._refresh_custom_step_options(0)
            preset_label = next(
                (label for label, item_id in self._custom_step_preset_lookup[0].items() if item_id == str(selected.get("id") or "")),
                "",
            )
            self.custom_step_preset_vars[0].set(preset_label)

    def _load_selected_preset_into_editor(self) -> None:
        selected = self._selected_center_preset_payload()
        if not selected:
            self._new_custom_preset()
            return
        if bool(selected.get("is_custom", False)):
            self._populate_custom_editor(selected)
            return
        self._new_custom_preset()
        self.custom_preset_name_var.set(str(selected.get("label") or ""))
        self.custom_preset_description_var.set(str(selected.get("description") or ""))

    def _editor_preset_id(self) -> str:
        return str(self._loaded_custom_preset_id or "").strip()

    def _collect_custom_step_payloads(self) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        for index in range(len(self.custom_step_device_vars)):
            device_label = str(self.custom_step_device_vars[index].get() or "").strip()
            preset_label = str(self.custom_step_preset_vars[index].get() or "").strip()
            device_id = self._custom_device_lookup.get(device_label, "")
            preset_id = self._custom_step_preset_lookup[index].get(preset_label, "")
            if not device_id or not preset_id:
                continue
            rows.append({"device_kind": device_id, "preset_id": preset_id})
        return rows

    def _set_center_selection(self, device_kind: str, preset_id: str) -> None:
        group_id = self._group_for_device_kind(device_kind)
        group_label = next(
            (
                label
                for label, payload in self._preset_group_lookup.items()
                if str(dict(payload).get("id") or "") == group_id
            ),
            "",
        )
        if group_label:
            self.preset_group_var.set(group_label)
            self._refresh_preset_group_options()
        selected_label = next(
            (
                label
                for label, entry in self._preset_center_lookup.items()
                if entry == (device_kind, preset_id)
            ),
            "",
        )
        if selected_label:
            self.preset_center_var.set(selected_label)
            self._update_center_preset_detail()

    def _save_custom_preset_from_editor(self) -> None:
        result = self._invoke(
            "workbench",
            "save_custom_preset",
            preset_id=self._editor_preset_id(),
            group_id=self._custom_group_lookup.get(str(self.custom_preset_group_var.get() or "").strip(), ""),
            label=str(self.custom_preset_name_var.get() or "").strip(),
            description=str(self.custom_preset_description_var.get() or "").strip(),
            analyzer_index=self.custom_preset_analyzer_index_var.get(),
            pressure_hpa=self.custom_preset_pressure_var.get(),
            relay_name=self._custom_relay_lookup.get(str(self.custom_preset_relay_var.get() or "").strip(), "relay"),
            channel=self.custom_preset_channel_var.get(),
            steps=self._collect_custom_step_payloads(),
        )
        custom_preset = dict(result.get("custom_preset", {}) or {})
        if custom_preset:
            self._populate_custom_editor(custom_preset)
            self._set_center_selection(str(custom_preset.get("device_kind") or ""), str(custom_preset.get("id") or ""))
        self._apply_preset_manager_feedback(result)

    def _delete_loaded_custom_preset(self) -> None:
        preset_id = self._editor_preset_id()
        if not preset_id:
            return
        self._invoke("workbench", "delete_custom_preset", preset_id=preset_id)
        self._new_custom_preset()

    def _duplicate_selected_preset(self) -> None:
        device_kind, preset_id = self._selected_center_preset()
        if not device_kind or not preset_id:
            return
        result = self._invoke(
            "workbench",
            "duplicate_preset",
            device_kind=device_kind,
            preset_id=preset_id,
        )
        custom_preset = dict(result.get("custom_preset", {}) or {})
        if custom_preset:
            self._populate_custom_editor(custom_preset)
            self._set_center_selection(str(custom_preset.get("device_kind") or ""), str(custom_preset.get("id") or ""))
        self._apply_preset_manager_feedback(result)

    def _export_preset_bundle(self, scope: str) -> None:
        group_label = str(self.preset_group_var.get() or "").strip()
        group_payload = dict(self._preset_group_lookup.get(group_label, {}) or {})
        device_kind, preset_id = self._selected_center_preset()
        result = self._invoke(
            "workbench",
            "export_preset_bundle",
            scope=scope,
            group_id=str(group_payload.get("id") or ""),
            device_kind=device_kind,
            preset_id=preset_id,
        )
        bundle_text = str(result.get("bundle_text") or "").strip()
        if bundle_text:
            self.preset_bundle_text.delete("1.0", "end")
            self.preset_bundle_text.insert("1.0", bundle_text + "\n")
        self._apply_preset_manager_feedback(result)

    def _import_preset_bundle(self) -> None:
        bundle_text = str(self.preset_bundle_text.get("1.0", "end") or "").strip()
        if not bundle_text:
            return
        result = self._invoke(
            "workbench",
            "import_preset_bundle",
            bundle_text=bundle_text,
            conflict_policy=self._preset_import_conflict_lookup.get(str(self.preset_import_conflict_var.get() or ""), ""),
        )
        imported = [dict(item) for item in list(result.get("custom_presets", []) or [])]
        if imported:
            first = dict(imported[0])
            self._populate_custom_editor(first)
            self._set_center_selection(str(first.get("device_kind") or ""), str(first.get("id") or ""))
        self._apply_preset_manager_feedback(result)

    def _apply_preset_manager_feedback(self, result: dict[str, Any]) -> None:
        base_text = str(self.preset_manager_summary_var.get() or "").strip()
        lines = [
            str(result.get("message") or "").strip(),
            str(result.get("conflict_summary") or "").strip(),
            str(result.get("conflict_policy_summary") or "").strip(),
            str(result.get("conflict_strategy_summary") or "").strip(),
            str(result.get("bundle_format_summary") or "").strip(),
            str(result.get("sharing_reserved_fields_summary") or "").strip(),
            str(result.get("bundle_profile_summary") or "").strip(),
            str(result.get("sharing_ready_summary") or "").strip(),
            base_text,
        ]
        seen: set[str] = set()
        merged: list[str] = []
        for line in lines:
            if not line or line in seen:
                continue
            seen.add(line)
            merged.append(line)
        self.preset_manager_summary_var.set("\n".join(merged) or t("common.none"))

    def _render_engineer_overview(self, engineer_summary: dict[str, Any]) -> None:
        for child in self.engineer_cards_frame.winfo_children():
            child.destroy()
        for child in self.engineer_status_frame.winfo_children():
            child.destroy()
        for child in self.engineer_sections_frame.winfo_children():
            child.destroy()
        cards = [dict(item) for item in list(engineer_summary.get("cards", []) or [])]
        for index, card in enumerate(cards):
            self.engineer_cards_frame.columnconfigure(index, weight=1)
            frame = ttk.Frame(self.engineer_cards_frame, style="SoftCard.TFrame", padding=8)
            frame.grid(row=0, column=index, sticky="nsew", padx=4)
            frame.columnconfigure(0, weight=1)
            ttk.Label(frame, text=str(card.get("title") or ""), style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(frame, text=str(card.get("summary") or ""), wraplength=260, justify="left").grid(row=1, column=0, sticky="ew", pady=(4, 0))
        status_blocks = [dict(item) for item in list(engineer_summary.get("status_blocks", []) or [])]
        trend_blocks = [dict(item) for item in list(engineer_summary.get("trend_blocks", []) or [])]
        for index, block in enumerate(status_blocks + trend_blocks):
            self.engineer_status_frame.columnconfigure(index, weight=1)
            frame = ttk.Frame(self.engineer_status_frame, style="SoftCard.TFrame", padding=8)
            frame.grid(row=0, column=index, sticky="nsew", padx=4)
            frame.columnconfigure(0, weight=1)
            ttk.Label(frame, text=str(block.get("title") or ""), style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            has_severity = bool(str(block.get("severity_display") or "").strip())
            if has_severity:
                ttk.Label(frame, text=str(block.get("severity_display") or ""), style="Muted.TLabel").grid(
                    row=1,
                    column=0,
                    sticky="w",
                    pady=(2, 0),
                )
            value_row = 2 if has_severity else 1
            note_row = value_row + 1
            ttk.Label(frame, text=str(block.get("value") or ""), style="Title.TLabel").grid(row=value_row, column=0, sticky="w", pady=(4, 0))
            ttk.Label(frame, text=str(block.get("note") or ""), wraplength=220, justify="left").grid(
                row=note_row,
                column=0,
                sticky="ew",
                pady=(4, 0),
            )
        sections = [dict(item) for item in list(engineer_summary.get("sections", []) or [])]
        for row, section in enumerate(sections):
            panel = CollapsibleSection(
                self.engineer_sections_frame,
                title=str(section.get("title") or ""),
                summary=str(section.get("summary") or ""),
                expanded=bool(section.get("expanded", False)),
            )
            panel.grid(row=row, column=0, sticky="ew", pady=(0, 8))
            text = tk.Text(panel.body, height=5 if self._layout_mode == "compact" else 7, wrap="word")
            text.grid(row=0, column=0, sticky="nsew")
            scroll = ttk.Scrollbar(panel.body, orient="vertical", command=text.yview)
            scroll.grid(row=0, column=1, sticky="ns", padx=(6, 0))
            text.configure(yscrollcommand=scroll.set)
            self._set_text(text, str(section.get("body_text") or t("common.none")))

    def _set_text(self, widget: tk.Text, text: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", text)
        widget.configure(state="disabled")

    def _toggle_engineer_view(self, view_mode: str) -> None:
        if str(view_mode) == "engineer_view":
            self.engineer_frame.grid()
            self.custom_preset_buttons.grid()
            self.custom_preset_summary_label.grid()
            self.custom_preset_editor.grid()
            self.preset_manager_section.grid()
        else:
            self.engineer_frame.grid_remove()
            self.custom_preset_buttons.grid_remove()
            self.custom_preset_summary_label.grid_remove()
            self.custom_preset_editor.grid_remove()
            self.preset_manager_section.grid_remove()

    def _build_analyzer_tab(self) -> None:
        self.analyzer_frame = ttk.Frame(self.notebook, padding=8)
        self.analyzer_frame.columnconfigure(0, weight=1)
        self.analyzer_index_var = tk.StringVar(value="1")
        self.analyzer_frequency_var = tk.StringVar(value="5")
        self.analyzer_fault_var = tk.StringVar(value="stable")
        self.analyzer_info_var = tk.StringVar(value="")
        self.analyzer_frames_var = tk.StringVar(value="")
        self._build_preset_bar(self.analyzer_frame, row=0, device_kind="analyzer")
        top = ttk.Frame(self.analyzer_frame)
        top.grid(row=1, column=0, sticky="ew")
        ttk.Label(top, text=t("pages.devices.workbench.field.analyzer_index")).grid(row=0, column=0, sticky="w")
        self.analyzer_selector = ttk.Combobox(top, textvariable=self.analyzer_index_var, values=[str(i) for i in range(1, 9)], width=6, state="readonly")
        self.analyzer_selector.grid(row=0, column=1, sticky="w", padx=(6, 12))
        ttk.Button(top, text=t("pages.devices.workbench.action.analyzer.select"), command=lambda: self._invoke("analyzer", "select", analyzer_index=self._analyzer_index())).grid(row=0, column=2, sticky="w")
        ttk.Label(self.analyzer_frame, textvariable=self.analyzer_info_var, wraplength=980, justify="left").grid(row=2, column=0, sticky="ew", pady=(8, 0))
        ttk.Label(self.analyzer_frame, textvariable=self.analyzer_frames_var, wraplength=980, justify="left").grid(row=3, column=0, sticky="ew", pady=(8, 0))
        buttons = ttk.Frame(self.analyzer_frame)
        buttons.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(buttons, text=t("pages.devices.workbench.button.analyzer.mode1"), command=lambda: self._invoke("analyzer", "set_mode", analyzer_index=self._analyzer_index(), mode=1)).grid(row=0, column=0, padx=4, pady=2)
        ttk.Button(buttons, text=t("pages.devices.workbench.button.analyzer.mode2"), command=lambda: self._invoke("analyzer", "set_mode", analyzer_index=self._analyzer_index(), mode=2)).grid(row=0, column=1, padx=4, pady=2)
        ttk.Button(buttons, text=t("pages.devices.workbench.button.analyzer.mode3"), command=lambda: self._invoke("analyzer", "set_mode", analyzer_index=self._analyzer_index(), mode=3)).grid(row=0, column=2, padx=4, pady=2)
        ttk.Button(buttons, text=t("pages.devices.workbench.button.analyzer.active"), command=lambda: self._invoke("analyzer", "set_active_state", analyzer_index=self._analyzer_index(), active=True)).grid(row=0, column=3, padx=4, pady=2)
        ttk.Button(buttons, text=t("pages.devices.workbench.button.analyzer.passive"), command=lambda: self._invoke("analyzer", "set_active_state", analyzer_index=self._analyzer_index(), active=False)).grid(row=0, column=4, padx=4, pady=2)
        ttk.Button(buttons, text=t("pages.devices.workbench.button.analyzer.read_frame"), command=lambda: self._invoke("analyzer", "read_frame", analyzer_index=self._analyzer_index())).grid(row=1, column=0, padx=4, pady=2)
        ttk.Button(buttons, text=t("pages.devices.workbench.button.analyzer.broadcast"), command=lambda: self._invoke("analyzer", "broadcast", analyzer_index=self._analyzer_index())).grid(row=1, column=1, padx=4, pady=2)
        ttk.Entry(buttons, textvariable=self.analyzer_frequency_var, width=8).grid(row=1, column=2, padx=4, pady=2)
        ttk.Button(buttons, text=t("pages.devices.workbench.button.analyzer.set_frequency"), command=lambda: self._invoke("analyzer", "set_frequency", analyzer_index=self._analyzer_index(), frequency_hz=self.analyzer_frequency_var.get())).grid(row=1, column=3, padx=4, pady=2)
        self.analyzer_fault_selector = ttk.Combobox(buttons, textvariable=self.analyzer_fault_var, values=["stable", "partial_frame", "truncated_frame", "corrupted_frame", "no_response", "sensor_precheck_fail"], state="readonly", width=22)
        self.analyzer_fault_selector.grid(row=1, column=4, padx=4, pady=2)
        ttk.Button(buttons, text=t("pages.devices.workbench.button.inject_fault"), command=lambda: self._invoke("analyzer", "inject_fault", analyzer_index=self._analyzer_index(), fault=self.analyzer_fault_var.get())).grid(row=1, column=5, padx=4, pady=2)
        self._register_tab(self.analyzer_frame, kind="analyzer", title=t("pages.devices.workbench.device.analyzer"))

    def _build_pace_tab(self) -> None:
        self.pace_frame = ttk.Frame(self.notebook, padding=8)
        self.pace_frame.columnconfigure(0, weight=1)
        self.pace_pressure_var = tk.StringVar(value="1000")
        self.pace_unit_var = tk.StringVar(value="HPA")
        self.pace_fault_var = tk.StringVar(value="stable")
        self.pace_info_var = tk.StringVar(value="")
        self._build_preset_bar(self.pace_frame, row=0, device_kind="pace")
        ttk.Label(self.pace_frame, textvariable=self.pace_info_var, wraplength=980, justify="left").grid(row=1, column=0, sticky="ew")
        controls = ttk.Frame(self.pace_frame)
        controls.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(controls, text=t("pages.devices.workbench.button.pace.vent_on"), command=lambda: self._invoke("pace", "set_vent", enabled=True)).grid(row=0, column=0, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pace.vent_off"), command=lambda: self._invoke("pace", "set_vent", enabled=False)).grid(row=0, column=1, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pace.output_on"), command=lambda: self._invoke("pace", "set_output", enabled=True)).grid(row=0, column=2, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pace.output_off"), command=lambda: self._invoke("pace", "set_output", enabled=False)).grid(row=0, column=3, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pace.isolation_on"), command=lambda: self._invoke("pace", "set_isolation", enabled=True)).grid(row=0, column=4, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pace.isolation_off"), command=lambda: self._invoke("pace", "set_isolation", enabled=False)).grid(row=0, column=5, padx=4, pady=2)
        ttk.Entry(controls, textvariable=self.pace_pressure_var, width=10).grid(row=1, column=0, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pace.set_pressure"), command=lambda: self._invoke("pace", "set_pressure", pressure_hpa=self.pace_pressure_var.get())).grid(row=1, column=1, padx=4, pady=2)
        ttk.Combobox(controls, textvariable=self.pace_unit_var, values=["HPA", "KPA", "BAR", "PSIA"], width=10, state="readonly").grid(row=1, column=2, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pace.set_unit"), command=lambda: self._invoke("pace", "set_unit", unit=self.pace_unit_var.get())).grid(row=1, column=3, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pace.read_pressure"), command=lambda: self._invoke("pace", "read_pressure")).grid(row=1, column=4, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pace.query_error"), command=lambda: self._invoke("pace", "query_error")).grid(row=1, column=5, padx=4, pady=2)
        ttk.Combobox(controls, textvariable=self.pace_fault_var, values=["stable", "no_response", "unsupported_header", "cleanup_no_response", "wrong_unit_configuration"], width=22, state="readonly").grid(row=2, column=0, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.inject_fault"), command=lambda: self._invoke("pace", "inject_fault", fault=self.pace_fault_var.get())).grid(row=2, column=1, padx=4, pady=2)
        self._register_tab(self.pace_frame, kind="pace", title=t("pages.devices.workbench.device.pace"))

    def _build_grz_tab(self) -> None:
        self.grz_frame = ttk.Frame(self.notebook, padding=8)
        self.grz_frame.columnconfigure(0, weight=1)
        self.grz_temp_var = tk.StringVar(value="25")
        self.grz_rh_var = tk.StringVar(value="35")
        self.grz_flow_var = tk.StringVar(value="1.0")
        self.grz_fault_var = tk.StringVar(value="stable")
        self.grz_info_var = tk.StringVar(value="")
        self._build_preset_bar(self.grz_frame, row=0, device_kind="grz")
        ttk.Label(self.grz_frame, textvariable=self.grz_info_var, wraplength=980, justify="left").grid(row=1, column=0, sticky="ew")
        controls = ttk.Frame(self.grz_frame)
        controls.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        ttk.Entry(controls, textvariable=self.grz_temp_var, width=10).grid(row=0, column=0, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.grz.set_temp"), command=lambda: self._invoke("grz", "set_target_temp", temperature_c=self.grz_temp_var.get())).grid(row=0, column=1, padx=4, pady=2)
        ttk.Entry(controls, textvariable=self.grz_rh_var, width=10).grid(row=0, column=2, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.grz.set_rh"), command=lambda: self._invoke("grz", "set_target_rh", humidity_pct=self.grz_rh_var.get())).grid(row=0, column=3, padx=4, pady=2)
        ttk.Entry(controls, textvariable=self.grz_flow_var, width=10).grid(row=0, column=4, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.grz.set_flow"), command=lambda: self._invoke("grz", "set_target_flow", flow_lpm=self.grz_flow_var.get())).grid(row=0, column=5, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.grz.cool_on"), command=lambda: self._invoke("grz", "set_cool", enabled=True)).grid(row=1, column=0, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.grz.cool_off"), command=lambda: self._invoke("grz", "set_cool", enabled=False)).grid(row=1, column=1, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.grz.heat_on"), command=lambda: self._invoke("grz", "set_heat", enabled=True)).grid(row=1, column=2, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.grz.heat_off"), command=lambda: self._invoke("grz", "set_heat", enabled=False)).grid(row=1, column=3, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.grz.ctrl_on"), command=lambda: self._invoke("grz", "set_control", enabled=True)).grid(row=1, column=4, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.grz.ctrl_off"), command=lambda: self._invoke("grz", "set_control", enabled=False)).grid(row=1, column=5, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.grz.fetch_all"), command=lambda: self._invoke("grz", "fetch_all")).grid(row=2, column=0, padx=4, pady=2)
        ttk.Combobox(controls, textvariable=self.grz_fault_var, values=["stable", "temperature_only_progress", "humidity_static_fault", "timeout"], width=24, state="readonly").grid(row=2, column=1, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.inject_fault"), command=lambda: self._invoke("grz", "inject_fault", fault=self.grz_fault_var.get())).grid(row=2, column=2, padx=4, pady=2)
        self._register_tab(self.grz_frame, kind="grz", title=t("pages.devices.workbench.device.grz"))

    def _build_chamber_tab(self) -> None:
        self.chamber_frame = ttk.Frame(self.notebook, padding=8)
        self.chamber_frame.columnconfigure(0, weight=1)
        self.chamber_temp_var = tk.StringVar(value="25")
        self.chamber_rh_var = tk.StringVar(value="40")
        self.chamber_mode_var = tk.StringVar(value="stable")
        self.chamber_info_var = tk.StringVar(value="")
        self._build_preset_bar(self.chamber_frame, row=0, device_kind="chamber")
        ttk.Label(self.chamber_frame, textvariable=self.chamber_info_var, wraplength=980, justify="left").grid(row=1, column=0, sticky="ew")
        controls = ttk.Frame(self.chamber_frame)
        controls.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        ttk.Entry(controls, textvariable=self.chamber_temp_var, width=10).grid(row=0, column=0, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.chamber.set_temp"), command=lambda: self._invoke("chamber", "set_temperature", temperature_c=self.chamber_temp_var.get())).grid(row=0, column=1, padx=4, pady=2)
        ttk.Entry(controls, textvariable=self.chamber_rh_var, width=10).grid(row=0, column=2, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.chamber.set_rh"), command=lambda: self._invoke("chamber", "set_humidity", humidity_pct=self.chamber_rh_var.get())).grid(row=0, column=3, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.chamber.run"), command=lambda: self._invoke("chamber", "run")).grid(row=1, column=0, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.chamber.stop"), command=lambda: self._invoke("chamber", "stop")).grid(row=1, column=1, padx=4, pady=2)
        ttk.Combobox(controls, textvariable=self.chamber_mode_var, values=["stable", "ramp_to_target", "soak_pending", "stalled", "alarm"], width=20, state="readonly").grid(row=1, column=2, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.chamber.set_mode"), command=lambda: self._invoke("chamber", "set_mode", mode=self.chamber_mode_var.get())).grid(row=1, column=3, padx=4, pady=2)
        self._register_tab(self.chamber_frame, kind="chamber", title=t("pages.devices.workbench.device.chamber"))

    def _build_relay_tab(self) -> None:
        self.relay_frame = ttk.Frame(self.notebook, padding=8)
        self.relay_frame.columnconfigure(0, weight=1)
        self.relay_frame.columnconfigure(1, weight=1)
        self.relay_target_var = tk.StringVar(value="relay")
        self.relay_channel_var = tk.StringVar(value="1")
        self.relay_batch_var = tk.StringVar(value="")
        self.relay_fault_var = tk.StringVar(value="stable")
        self.relay_info_var = tk.StringVar(value="")
        self._build_preset_bar(self.relay_frame, row=0, device_kind="relay")
        ttk.Label(self.relay_frame, textvariable=self.relay_info_var, wraplength=980, justify="left").grid(row=1, column=0, columnspan=2, sticky="ew")
        self.relay_tree = self._build_relay_tree(self.relay_frame)
        self.relay_tree.grid(row=2, column=0, sticky="nsew", padx=(0, 6), pady=(8, 0))
        self.relay8_tree = self._build_relay_tree(self.relay_frame)
        self.relay8_tree.grid(row=2, column=1, sticky="nsew", padx=(6, 0), pady=(8, 0))
        controls = ttk.Frame(self.relay_frame)
        controls.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        ttk.Combobox(controls, textvariable=self.relay_target_var, values=["relay", "relay_8"], width=10, state="readonly").grid(row=0, column=0, padx=4, pady=2)
        ttk.Entry(controls, textvariable=self.relay_channel_var, width=8).grid(row=0, column=1, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.relay.channel_on"), command=lambda: self._invoke("relay", "write_channel", relay_name=self.relay_target_var.get(), channel=self.relay_channel_var.get(), enabled=True)).grid(row=0, column=2, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.relay.channel_off"), command=lambda: self._invoke("relay", "write_channel", relay_name=self.relay_target_var.get(), channel=self.relay_channel_var.get(), enabled=False)).grid(row=0, column=3, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.relay.all_off"), command=lambda: self._invoke("relay", "all_off", relay_name=self.relay_target_var.get())).grid(row=0, column=4, padx=4, pady=2)
        ttk.Entry(controls, textvariable=self.relay_batch_var, width=18).grid(row=1, column=0, columnspan=2, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.relay.batch_write"), command=lambda: self._invoke("relay", "batch_write", relay_name=self.relay_target_var.get(), channels=self.relay_batch_var.get())).grid(row=1, column=2, padx=4, pady=2)
        ttk.Combobox(controls, textvariable=self.relay_fault_var, values=["stable", "stuck_channel", "read_fail", "write_fail"], width=18, state="readonly").grid(row=1, column=3, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.inject_fault"), command=lambda: self._invoke("relay", "inject_fault", relay_name=self.relay_target_var.get(), fault=self.relay_fault_var.get(), stuck_channels=self.relay_channel_var.get())).grid(row=1, column=4, padx=4, pady=2)
        self._register_tab(self.relay_frame, kind="relay", title=t("pages.devices.workbench.device.relay"))

    def _build_relay_tree(self, parent: tk.Misc) -> ttk.Treeview:
        tree = ttk.Treeview(parent, columns=("channel", "desired", "actual", "input", "mapping"), show="headings", height=8)
        for column, text, width in (
            ("channel", t("pages.devices.workbench.field.channel"), 70),
            ("desired", t("pages.devices.workbench.field.expected"), 80),
            ("actual", t("pages.devices.workbench.field.actual"), 80),
            ("input", t("pages.devices.workbench.field.input_status"), 90),
            ("mapping", t("pages.devices.workbench.field.valve_mapping"), 150),
        ):
            tree.heading(column, text=text)
            tree.column(column, width=width, anchor="w")
        return tree

    def _build_thermometer_tab(self) -> None:
        self.thermometer_frame = ttk.Frame(self.notebook, padding=8)
        self.thermometer_frame.columnconfigure(0, weight=1)
        self.thermometer_mode_var = tk.StringVar(value="stable")
        self.thermometer_info_var = tk.StringVar(value="")
        self._build_preset_bar(self.thermometer_frame, row=0, device_kind="thermometer")
        ttk.Label(self.thermometer_frame, textvariable=self.thermometer_info_var, wraplength=980, justify="left").grid(row=1, column=0, sticky="ew")
        controls = ttk.Frame(self.thermometer_frame)
        controls.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        ttk.Combobox(controls, textvariable=self.thermometer_mode_var, values=["stable", "drift", "stale", "no_response", "warmup_unstable", "plus_200_mode", "corrupted_ascii", "truncated_ascii"], width=24, state="readonly").grid(row=0, column=0, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.thermometer.set_mode"), command=lambda: self._invoke("thermometer", "set_mode", mode=self.thermometer_mode_var.get())).grid(row=0, column=1, padx=4, pady=2)
        self._register_tab(self.thermometer_frame, kind="thermometer", title=t("pages.devices.workbench.device.thermometer"))

    def _build_pressure_tab(self) -> None:
        self.pressure_frame = ttk.Frame(self.notebook, padding=8)
        self.pressure_frame.columnconfigure(0, weight=1)
        self.pressure_mode_var = tk.StringVar(value="single")
        self.pressure_unit_var = tk.StringVar(value="HPA")
        self.pressure_fault_var = tk.StringVar(value="stable")
        self.pressure_info_var = tk.StringVar(value="")
        self._build_preset_bar(self.pressure_frame, row=0, device_kind="pressure_gauge")
        ttk.Label(self.pressure_frame, textvariable=self.pressure_info_var, wraplength=980, justify="left").grid(row=1, column=0, sticky="ew")
        controls = ttk.Frame(self.pressure_frame)
        controls.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        ttk.Combobox(controls, textvariable=self.pressure_mode_var, values=["single", "continuous", "sample_hold"], width=16, state="readonly").grid(row=0, column=0, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pressure.set_mode"), command=lambda: self._invoke("pressure_gauge", "set_measurement_mode", measurement_mode=self.pressure_mode_var.get())).grid(row=0, column=1, padx=4, pady=2)
        ttk.Combobox(controls, textvariable=self.pressure_unit_var, values=["HPA", "KPA", "BAR", "PSIA"], width=12, state="readonly").grid(row=0, column=2, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.pressure.set_unit"), command=lambda: self._invoke("pressure_gauge", "set_unit", unit=self.pressure_unit_var.get())).grid(row=0, column=3, padx=4, pady=2)
        ttk.Combobox(controls, textvariable=self.pressure_fault_var, values=["stable", "no_response", "unsupported_command", "wrong_unit_configuration"], width=22, state="readonly").grid(row=1, column=0, padx=4, pady=2)
        ttk.Button(controls, text=t("pages.devices.workbench.button.inject_fault"), command=lambda: self._invoke("pressure_gauge", "inject_fault", fault=self.pressure_fault_var.get())).grid(row=1, column=1, padx=4, pady=2)
        self._register_tab(self.pressure_frame, kind="pressure_gauge", title=t("pages.devices.workbench.device.pressure_gauge"))

    def render(self, snapshot: dict[str, Any]) -> None:
        meta = dict(snapshot.get("meta", {}) or {})
        workbench = dict(snapshot.get("workbench", {}) or {})
        self._workbench_payload = dict(workbench)
        evidence = dict(snapshot.get("evidence", {}) or {})
        operator_summary = dict(snapshot.get("operator_summary", {}) or {})
        engineer_summary = dict(snapshot.get("engineer_summary", {}) or {})
        last_action = dict(meta.get("last_action", {}) or {})
        notices = list(meta.get("safety_notice", []) or [])
        route_validation = dict(evidence.get("route_physical_validation", {}) or {})
        reference_quality = dict(evidence.get("reference_quality", {}) or {})
        history_snapshot = dict(snapshot.get("history", {}) or {})
        snapshot_compare = dict(workbench.get("snapshot_compare", {}) or {})
        self._history_snapshot = history_snapshot

        self.banner_var.set(str(meta.get("simulation_mode_label") or t("pages.devices.workbench.banner.simulation_mode")))
        self.notice_var.set(" | ".join(str(item) for item in notices if str(item).strip()))
        self.view_mode_var.set(str(workbench.get("view_mode_display") or meta.get("view_mode_display") or t("pages.devices.workbench.view.operator_view")))
        self._layout_mode = str(workbench.get("layout_mode") or meta.get("layout_mode") or self._layout_mode or "compact")
        self.layout_mode_var.set(str(workbench.get("layout_mode_display") or meta.get("layout_mode_display") or t(f"pages.devices.workbench.layout.{self._layout_mode}")))
        display_profiles = [dict(item) for item in list(workbench.get("display_profiles", []) or [])]
        self._display_profile_lookup = {
            str(item.get("label") or ""): str(item.get("id") or "")
            for item in display_profiles
        }
        self.display_profile_selector.configure(values=list(self._display_profile_lookup.keys()))
        current_display_profile = next(
            (
                str(item.get("label") or "")
                for item in display_profiles
                if str(item.get("id") or "") == str(workbench.get("display_profile") or meta.get("display_profile") or "auto")
            ),
            str(workbench.get("display_profile_display") or meta.get("display_profile_display") or self.display_profile_var.get()),
        )
        if current_display_profile:
            self.display_profile_var.set(current_display_profile)
        display_profile_meta = dict(workbench.get("display_profile_meta") or meta.get("display_profile_meta") or {})
        self.display_profile_hint_var.set(
            t(
                "pages.devices.workbench.display_profile_hint",
                profile=str(display_profile_meta.get("resolved_label") or t("common.none")),
                family=str(display_profile_meta.get("profile_family_label") or t("common.none")),
                resolution=str(display_profile_meta.get("resolution") or "1920x1080"),
                layout=t(
                    f"pages.devices.workbench.layout.{str(display_profile_meta.get('layout_hint') or self._layout_mode)}",
                    default=str(display_profile_meta.get("layout_hint") or self._layout_mode),
                ),
                monitor=str(display_profile_meta.get("monitor_label") or t("common.none")),
                default=f"{display_profile_meta.get('resolved_label', t('common.none'))} | {display_profile_meta.get('profile_family_label', t('common.none'))} | {display_profile_meta.get('resolution', '1920x1080')} | {display_profile_meta.get('layout_hint', self._layout_mode)} | {display_profile_meta.get('monitor_label', t('common.none'))}",
            )
        )
        self.evidence_var.set(
            t(
                "pages.devices.workbench.summary.evidence",
                reference=t(
                    f"pages.devices.workbench.enum.reference_status.{str(reference_quality.get('reference_quality') or 'not_assessed')}",
                    default=str(reference_quality.get("reference_quality") or "not_assessed"),
                ),
                route=t(
                    "pages.devices.workbench.enum.route_match.match"
                    if bool(route_validation.get("route_physical_state_match", True))
                    else "pages.devices.workbench.enum.route_match.mismatch"
                ),
            )
        )
        self.message_var.set(str(last_action.get("message") or ""))
        self.health_var.set(str(operator_summary.get("health_summary") or t("common.none")))
        self.faults_var.set(str(operator_summary.get("fault_summary") or t("common.none")))
        self.reference_var.set(str(operator_summary.get("reference_summary") or t("common.none")))
        self.route_var.set(str(operator_summary.get("route_summary") or t("common.none")))
        self.history_var.set(str(operator_summary.get("history_summary") or t("common.none")))
        self.risk_var.set(str(operator_summary.get("risk_summary") or t("common.none")))
        self.last_evidence_var.set(str(operator_summary.get("last_evidence_summary") or t("pages.devices.workbench.summary.no_evidence")))

        quick_scenarios = list(workbench.get("quick_scenarios", []) or [])
        quick_labels = [str(item.get("label") or "") for item in quick_scenarios if str(item.get("label") or "").strip()]
        self._quick_scenario_lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in quick_scenarios}
        self.quick_scenario_selector.configure(values=quick_labels)
        if quick_labels and self.quick_scenario_var.get() not in quick_labels:
            self.quick_scenario_var.set(quick_labels[0])

        self._render_preset_options("analyzer", dict(snapshot.get("analyzer", {}) or {}))
        self._render_preset_options("pace", dict(snapshot.get("pace", {}) or {}))
        self._render_preset_options("grz", dict(snapshot.get("grz", {}) or {}))
        self._render_preset_options("chamber", dict(snapshot.get("chamber", {}) or {}))
        self._render_preset_options("relay", dict(snapshot.get("relay", {}) or {}))
        self._render_preset_options("thermometer", dict(snapshot.get("thermometer", {}) or {}))
        self._render_preset_options("pressure_gauge", dict(snapshot.get("pressure_gauge", {}) or {}))
        self._render_preset_center(dict(workbench.get("preset_center", {}) or {}))

        self._render_history_filters(history_snapshot)
        self._render_engineer_overview(engineer_summary)
        self._set_text(self.context_text, str(engineer_summary.get("simulation_context_text") or t("common.none")))
        self._set_text(self.diagnostic_text, str(engineer_summary.get("diagnostic_text") or t("common.none")))
        self._set_text(self.history_detail_text, str(engineer_summary.get("history_detail_json") or t("common.none")))
        self._set_text(self.evidence_text, str(engineer_summary.get("last_evidence_json") or engineer_summary.get("last_evidence_text") or t("common.none")))
        self._render_history(history_snapshot)
        self._render_snapshot_compare(snapshot_compare)
        self._apply_layout_mode()
        self._toggle_engineer_view(str(workbench.get("view_mode") or meta.get("view_mode") or "operator_view"))

        self._render_analyzer(dict(snapshot.get("analyzer", {}) or {}))
        self._render_pace(dict(snapshot.get("pace", {}) or {}))
        self._render_grz(dict(snapshot.get("grz", {}) or {}))
        self._render_chamber(dict(snapshot.get("chamber", {}) or {}))
        self._render_relay(dict(snapshot.get("relay", {}) or {}))
        self._render_thermometer(dict(snapshot.get("thermometer", {}) or {}))
        self._render_pressure(dict(snapshot.get("pressure_gauge", {}) or {}))

    def _render_preset_options(self, device_kind: str, snapshot: dict[str, Any]) -> None:
        presets = list(snapshot.get("presets", []) or [])
        labels = [str(item.get("label") or "") for item in presets if str(item.get("label") or "").strip()]
        self._preset_lookup[device_kind] = {
            str(item.get("label") or ""): str(item.get("id") or "")
            for item in presets
        }
        selector = self._preset_selectors.get(device_kind)
        if selector is None:
            return
        selector.configure(values=labels)
        if labels and self._preset_vars[device_kind].get() not in labels:
            self._preset_vars[device_kind].set(labels[0])

    def _render_history_filters(self, history_snapshot: dict[str, Any]) -> None:
        filters = dict(history_snapshot.get("filters", {}) or {})
        device_options = list(filters.get("device_options", []) or [])
        result_options = list(filters.get("result_options", []) or [])
        self._history_device_lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in device_options}
        self._history_result_lookup = {str(item.get("label") or ""): str(item.get("id") or "") for item in result_options}
        device_labels = [str(item.get("label") or "") for item in device_options if str(item.get("label") or "").strip()]
        result_labels = [str(item.get("label") or "") for item in result_options if str(item.get("label") or "").strip()]
        self.history_device_filter_selector.configure(values=device_labels)
        self.history_result_filter_selector.configure(values=result_labels)
        current_device = next(
            (
                str(item.get("label") or "")
                for item in device_options
                if str(item.get("id") or "") == str(filters.get("device") or "all")
            ),
            device_labels[0] if device_labels else "",
        )
        current_result = next(
            (
                str(item.get("label") or "")
                for item in result_options
                if str(item.get("id") or "") == str(filters.get("result") or "all")
            ),
            result_labels[0] if result_labels else "",
        )
        if current_device:
            self.history_device_filter_var.set(current_device)
        if current_result:
            self.history_result_filter_var.set(current_result)

    def _render_history(self, history_snapshot: dict[str, Any]) -> None:
        items = list(history_snapshot.get("items", []) or [])
        self.history_detail_var.set(str(history_snapshot.get("detail_text") or t("pages.devices.workbench.history.no_detail", default="暂无动作详情")))
        detail = dict(history_snapshot.get("detail", {}) or {})
        related_snapshot = dict(detail.get("related_snapshot", {}) or {})
        related_evidence = dict(detail.get("related_evidence", {}) or {})
        self.history_snapshot_var.set(
            str(related_snapshot.get("label") or t("pages.devices.workbench.history.no_related_snapshot"))
        )
        self.history_evidence_var.set(
            str(related_evidence.get("summary") or t("pages.devices.workbench.history.no_related_evidence"))
        )
        self._fill_history_tree(self.operator_history_tree, items, include_fault=False)
        self._fill_history_tree(self.history_tree, items, include_fault=True)

    def _fill_history_tree(self, tree: ttk.Treeview, rows: list[dict[str, Any]], *, include_fault: bool) -> None:
        for item in tree.get_children():
            tree.delete(item)
        for row in rows:
            values = (
                row.get("sequence"),
                row.get("timestamp"),
                row.get("device_display"),
                row.get("action_display"),
                row.get("result_display"),
            )
            if include_fault:
                values = values + (row.get("fault_injection_display"),)
            tree.insert("", "end", values=values)

    def _render_snapshot_compare(self, snapshot_compare: dict[str, Any]) -> None:
        options = list(snapshot_compare.get("options", []) or [])
        option_labels = [str(item.get("label") or "") for item in options if str(item.get("label") or "").strip()]
        self._snapshot_option_lookup = {str(item.get("label") or ""): int(item.get("sequence", 0) or 0) for item in options}
        self.snapshot_left_selector.configure(values=option_labels)
        self.snapshot_right_selector.configure(values=option_labels)
        left_label = str(snapshot_compare.get("left_label") or "")
        right_label = str(snapshot_compare.get("right_label") or "")
        if left_label:
            self.snapshot_left_var.set(left_label)
        elif option_labels:
            self.snapshot_left_var.set(option_labels[0])
        if right_label:
            self.snapshot_right_var.set(right_label)
        elif len(option_labels) > 1:
            self.snapshot_right_var.set(option_labels[1])
        elif option_labels:
            self.snapshot_right_var.set(option_labels[0])
        self._set_text(self.snapshot_compare_text, str(snapshot_compare.get("details_text") or t("pages.devices.workbench.snapshot.no_compare", default="暂无可对比快照")))

    def _render_analyzer(self, snapshot: dict[str, Any]) -> None:
        panel = dict(snapshot.get("panel_status", {}) or {})
        self.analyzer_index_var.set(str(panel.get("selected_analyzer", 1) or 1))
        self.analyzer_frequency_var.set(str(panel.get("frequency_hz", 5) or 5))
        self.analyzer_info_var.set(t("pages.devices.workbench.summary.analyzer", device_id=panel.get("device_id", "--"), mode=panel.get("mode_display", "--"), state=panel.get("active_send_display", "--"), frequency=panel.get("frequency_hz", "--"), status_bits=panel.get("status_bits", "--")))
        recent_frames = list(panel.get("recent_frames", []) or [])
        self.analyzer_frames_var.set(t("pages.devices.workbench.summary.recent_frames", frames=" | ".join(recent_frames) or t("common.none")))

    def _render_pace(self, snapshot: dict[str, Any]) -> None:
        panel = dict(snapshot.get("panel_status", {}) or {})
        self.pace_info_var.set(
            t(
                "pages.devices.workbench.summary.pace",
                pressure=panel.get("pressure_display", "--"),
                target=panel.get("target_pressure_display", "--"),
                vent=t("pages.devices.workbench.enum.on_off.on" if panel.get("vent_on") else "pages.devices.workbench.enum.on_off.off"),
                output=t("pages.devices.workbench.enum.on_off.on" if panel.get("output_on") else "pages.devices.workbench.enum.on_off.off"),
                isolation=t("pages.devices.workbench.enum.on_off.on" if panel.get("isolation_on") else "pages.devices.workbench.enum.on_off.off"),
                unit=panel.get("unit", "--"),
                slew=panel.get("slew_hpa_per_s", "--"),
                errors=" | ".join(str(item) for item in list(panel.get("error_queue", []) or [])) or t("common.none"),
            )
        )

    def _render_grz(self, snapshot: dict[str, Any]) -> None:
        panel = dict(snapshot.get("panel_status", {}) or {})
        self.grz_info_var.set(t("pages.devices.workbench.summary.grz", target_temp=panel.get("target_temp_display", "--"), target_rh=panel.get("target_rh_pct", "--"), target_flow=panel.get("target_flow_lpm", "--"), current_temp=panel.get("current_temp_display", "--"), current_rh=panel.get("current_rh_pct", "--"), dewpoint=panel.get("dewpoint_display", "--"), snapshot=panel.get("snapshot_raw", t("common.none"))))

    def _render_chamber(self, snapshot: dict[str, Any]) -> None:
        panel = dict(snapshot.get("panel_status", {}) or {})
        self.chamber_info_var.set(t("pages.devices.workbench.summary.chamber", temperature=panel.get("temperature_display", "--"), humidity=panel.get("humidity_pct", "--"), running=t("pages.devices.workbench.enum.on_off.on" if panel.get("running") else "pages.devices.workbench.enum.on_off.off"), target_temp=panel.get("setpoint_temp_display", "--"), target_rh=panel.get("setpoint_rh_pct", "--"), soak=panel.get("soak_state_display", "--")))

    def _render_relay(self, snapshot: dict[str, Any]) -> None:
        panel = dict(snapshot.get("panel_status", {}) or {})
        self.relay_info_var.set(str(panel.get("summary_line") or ""))
        self._fill_relay_tree(self.relay_tree, list(panel.get("relay", []) or []))
        self._fill_relay_tree(self.relay8_tree, list(panel.get("relay_8", []) or []))

    def _fill_relay_tree(self, tree: ttk.Treeview, rows: list[dict[str, Any]]) -> None:
        for item in tree.get_children():
            tree.delete(item)
        for row in rows:
            tree.insert("", "end", values=(row.get("channel"), row.get("desired_display"), row.get("actual_display"), row.get("input_display"), row.get("valve_mapping")))

    def _render_thermometer(self, snapshot: dict[str, Any]) -> None:
        panel = dict(snapshot.get("panel_status", {}) or {})
        self.thermometer_info_var.set(t("pages.devices.workbench.summary.thermometer", temperature=panel.get("temperature_display", "--"), status=panel.get("reference_status_display", "--"), preview=" | ".join(str(item) for item in list(panel.get("ascii_preview", []) or [])) or t("common.none")))

    def _render_pressure(self, snapshot: dict[str, Any]) -> None:
        panel = dict(snapshot.get("panel_status", {}) or {})
        self.pressure_info_var.set(t("pages.devices.workbench.summary.pressure", pressure=panel.get("pressure_display", "--"), unit=panel.get("unit", "--"), status=panel.get("reference_status_display", "--"), mode=panel.get("measurement_mode_display", "--"), preview=" | ".join(str(item) for item in list(panel.get("stream_preview", []) or [])) or t("common.none")))
