"""Main Tk application for calibration workflow control."""

from __future__ import annotations

import sys
import threading
import copy
import re
import csv
import io
import os
import json
import math
import subprocess
import time
from datetime import datetime, timedelta
from collections import deque
from pathlib import Path
from queue import Queue
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any, Dict, List, Tuple
from threading import Event

DEVICE_PORT_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("pressure_controller", "压力控制器"),
    ("pressure_gauge", "数字气压计"),
    ("dewpoint_meter", "露点仪"),
    ("humidity_generator", "湿度发生器"),
    ("temperature_chamber", "温度箱"),
    ("thermometer", "测温仪"),
    ("relay", "16路继电器"),
    ("relay_8", "8路继电器"),
)
DEFAULT_ANALYZER_PORTS: Tuple[str, ...] = tuple(f"COM{port}" for port in range(35, 43))
ANALYZER_MODE2_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("name", "分析仪"),
    ("port", "串口"),
    ("co2_ppm", "CO2(ppm)"),
    ("h2o_mmol", "H2O"),
    ("chamber_temp_c", "腔温(℃)"),
    ("pressure_kpa", "压力(kPa)"),
    ("case_temp_c", "机壳温(℃)"),
    ("co2_ratio_f", "CO2滤波比值"),
    ("h2o_ratio_f", "H2O滤波比值"),
    ("ref_signal", "参考信号"),
    ("co2_signal", "CO2信号"),
    ("h2o_signal", "H2O信号"),
    ("co2_ratio_raw", "CO2原始比值"),
    ("h2o_ratio_raw", "H2O原始比值"),
    ("co2_density", "CO2密度"),
    ("h2o_density", "H2O密度"),
    ("online", "在线"),
    ("status", "状态"),
    ("timestamp", "更新时间"),
)
AMBIENT_PRESSURE_TOKEN = "ambient"
AMBIENT_PRESSURE_LABEL = "当前大气压"
AMBIENT_PRESSURE_UI_LABEL = "当前大气压（不断路/不控压）"

if __package__ in (None, ""):
    SRC_ROOT = Path(__file__).resolve().parents[2]
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))

    from gas_calibrator.config import load_config
    from gas_calibrator.data.points import load_points_from_excel, reorder_points
    from gas_calibrator.devices import (
        DewpointMeter,
        GasAnalyzer,
        HumidityGenerator,
        Pace5000,
        ParoscientificGauge,
        RelayController,
        TemperatureChamber,
        Thermometer,
    )
    from gas_calibrator.diagnostics import run_self_test
    from gas_calibrator.logging_utils import RunLogger
    from gas_calibrator.modeling.config_loader import (
        find_latest_modeling_artifacts,
        load_modeling_config,
        save_modeling_config,
        summarize_modeling_config,
    )
    from gas_calibrator.senco_format import format_senco_values
    from gas_calibrator.tools.safe_stop import perform_safe_stop_with_retries
    from gas_calibrator.workflow.runner import CalibrationRunner
    from gas_calibrator.workflow.tuning import get_workflow_tunable_parameters
    from gas_calibrator.ui.dewpoint_page import DewpointPage
    from gas_calibrator.ui.humidity_page import HumidityPage
    from gas_calibrator.ui.thermometer_page import ThermometerPage
    from gas_calibrator.ui.valve_page import ValvePage
else:
    from ..config import load_config
    from ..data.points import load_points_from_excel, reorder_points
    from ..devices import (
        DewpointMeter,
        GasAnalyzer,
        HumidityGenerator,
        Pace5000,
        ParoscientificGauge,
        RelayController,
        TemperatureChamber,
        Thermometer,
    )
    from ..diagnostics import run_self_test
    from ..logging_utils import RunLogger
    from ..modeling.config_loader import (
        find_latest_modeling_artifacts,
        load_modeling_config,
        save_modeling_config,
        summarize_modeling_config,
    )
    from ..senco_format import format_senco_values
    from ..tools.safe_stop import perform_safe_stop_with_retries
    from ..workflow.runner import CalibrationRunner
    from ..workflow.tuning import get_workflow_tunable_parameters
    from .dewpoint_page import DewpointPage
    from .humidity_page import HumidityPage
    from .thermometer_page import ThermometerPage
    from .valve_page import ValvePage


class _Tooltip:
    """Small hover tooltip for read-only hints."""

    def __init__(self, widget: tk.Widget, text_getter):
        self.widget = widget
        self.text_getter = text_getter
        self.tip_window: tk.Toplevel | None = None
        self.label: tk.Label | None = None
        widget.bind("<Enter>", self._show, add="+")
        widget.bind("<Leave>", self._hide, add="+")

    def _show(self, _event=None) -> None:
        text = str(self.text_getter() or "").strip()
        if not text:
            return
        if self.tip_window is not None:
            return
        x = self.widget.winfo_rootx() + 12
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 6
        self.tip_window = tk.Toplevel(self.widget)
        self.tip_window.wm_overrideredirect(True)
        self.tip_window.wm_geometry(f"+{x}+{y}")
        self.label = tk.Label(
            self.tip_window,
            text=text,
            justify="left",
            anchor="w",
            bg="#111827",
            fg="white",
            padx=10,
            pady=8,
            wraplength=520,
        )
        self.label.pack(fill="both", expand=True)

    def _hide(self, _event=None) -> None:
        if self.tip_window is not None:
            self.tip_window.destroy()
            self.tip_window = None
            self.label = None


class App:
    """Main window controller."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("气体分析仪校准系统")
        screen_w = max(int(self.root.winfo_screenwidth()), 1440)
        screen_h = max(int(self.root.winfo_screenheight()), 900)
        default_w = min(screen_w - 40, max(1560, int(screen_w * 0.96)))
        default_h = min(screen_h - 60, max(920, int(screen_h * 0.92)))
        pos_x = max((screen_w - default_w) // 2, 0)
        pos_y = max((screen_h - default_h) // 2, 0)
        self.root.geometry(f"{default_w}x{default_h}+{pos_x}+{pos_y}")
        self.root.minsize(1280, 820)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close_request)
        self.ui_colors = {
            "bg": "#f2f6fb",
            "panel": "#eaf1fb",
            "card": "#ffffff",
            "line": "#d8e4f0",
            "text": "#0f2338",
            "muted": "#6f8094",
            "accent": "#0f8f90",
            "accent_dark": "#0f766e",
            "accent_soft": "#d8f8f3",
            "info_soft": "#e3efff",
            "warn_soft": "#fff3db",
            "danger_soft": "#ffe7ee",
            "hero": "#103d67",
            "hero_text": "#f7fbfd",
            "hero_subtle": "#d3e4f7",
            "hero_line": "#255783",
            "shadow": "#dfe8f3",
            "hero_layer": "#18466f",
            "hero_chip": "#eef6ff",
            "hero_chip_text": "#1e4668",
            "value_panel": "#fbfdff",
            "divider": "#dde7f1",
            "soft_layer": "#f8fbff",
            "chip_panel": "#eef4fb",
        }
        self.ui_metrics = {
            "shell_pad": 2,
            "card_pad_x": 4,
            "card_pad_y": 2,
            "section_gap_x": 4,
            "section_gap_y": 3,
            "selector_height": 78,
            "selector_panel_height": 148,
            "stripe_h": 4,
            "icon_font": ("Segoe UI Symbol", 13, "bold"),
            "badge_font": ("Microsoft YaHei UI", 9, "bold"),
            "title_font": ("Microsoft YaHei UI", 9, "bold"),
            "summary_value_font": ("Microsoft YaHei UI", 12, "bold"),
            "status_value_font": ("Microsoft YaHei UI", 9, "bold"),
            "pressure_value_font": ("Microsoft YaHei UI", 13, "bold"),
            "device_primary_font": ("Consolas", 11, "bold"),
            "device_secondary_font": ("Microsoft YaHei UI", 9),
            "card_label_font": ("Microsoft YaHei UI", 9, "bold"),
            "panel_header_font": ("Microsoft YaHei UI", 11, "bold"),
            "panel_icon_font": ("Segoe UI Symbol", 13, "bold"),
        }
        self.root.configure(bg=self.ui_colors["bg"])
        self._configure_styles()

        self.log_queue = Queue()
        self.runner = None
        self.worker = None
        self.startup_thread = None
        self.devices = {}
        self.logger = None
        self.temp_check_vars: Dict[float, tk.BooleanVar] = {}
        self.co2_check_vars: Dict[int, tk.BooleanVar] = {}
        self.pressure_check_vars: Dict[int, tk.BooleanVar] = {}
        self.temp_option_order: List[float] = []
        self.co2_option_order: List[int] = []
        self.pressure_option_order: List[int] = []
        self.ambient_pressure_var = tk.BooleanVar(value=False)
        self.ambient_pressure_var.trace_add("write", lambda *_args: self._on_ambient_pressure_change())
        self.ambient_pressure_check: ttk.Checkbutton | None = None
        self.temp_checkbuttons: List[tk.Checkbutton] = []
        self.co2_checkbuttons: List[tk.Checkbutton] = []
        self.pressure_checkbuttons: List[tk.Checkbutton] = []
        self.temp_listbox: tk.Listbox | None = None
        self.co2_listbox: tk.Listbox | None = None
        self.pressure_listbox: tk.Listbox | None = None
        self.points_tree: ttk.Treeview | None = None
        self.device_port_specs: List[Dict[str, Any]] = []
        self.device_port_vars: Dict[str, tk.StringVar] = {}
        self.device_port_grid: tk.Frame | None = None
        self.device_port_hint_var = tk.StringVar(value="设备串口配置：待加载")
        self.device_port_compat_var = tk.StringVar(value="兼容单分析仪端口：--")
        self.analyzer_value_vars: Dict[str, tk.StringVar] = {}
        self.analyzer_detail_vars: Dict[str, tk.StringVar] = {}
        self.analyzer_online_vars: Dict[str, tk.StringVar] = {}
        self.analyzer_update_vars: Dict[str, tk.StringVar] = {}
        self.analyzer_online_labels: Dict[str, tk.Label] = {}
        self.analyzer_table: ttk.Treeview | None = None
        self.analyzer_table_items: Dict[str, str] = {}
        self._live_analyzer_cache: Dict[str, Dict[str, str]] = {}
        self.analyzer_summary_var = tk.StringVar(value="在线分析仪：0 / 8")
        self.analyzer_update_summary_var = tk.StringVar(value="最近更新：--")
        self.analyzer_focus_var = tk.StringVar(value="当前活跃：--")
        self._poll_log_interval_ms = 200
        self._attach_run_refresh_interval_s = 2.0
        self._progress_refresh_interval_s = 1.0
        self._device_panel_refresh_interval_s = 0.8
        self._event_refresh_interval_s = 2.0
        self._modeling_refresh_interval_s = 2.0
        self._live_device_refresh_interval_s = 0.4
        self._live_analyzer_refresh_interval_s = 30.0
        self._last_attach_run_refresh_ts = 0.0
        self._last_progress_refresh_ts = 0.0
        self._last_device_panel_refresh_ts = 0.0
        self._last_event_refresh_ts = 0.0
        self._last_modeling_refresh_ts = 0.0
        self._last_live_device_refresh_ts = 0.0
        self._last_live_analyzer_refresh_ts = 0.0
        self._syncing_temp_listbox = False
        self._syncing_co2_listbox = False
        self._syncing_pressure_listbox = False
        self._points_preview_cache_key = None
        self._points_preview_cache: List[Any] = []
        self.last_runtime_cfg: Dict[str, Any] | None = None
        self.current_io_path: Path | None = None
        self.current_run_dir: Path | None = None
        self.current_workbook_path: Path | None = None
        self.current_summary_report_path: Path | None = None
        self.current_summary_report_paths: List[Path] = []
        self.current_latest_point_path: Path | None = None
        self.current_coefficient_report_path: Path | None = None
        self.current_temperature_compensation_report_path: Path | None = None
        self.current_temperature_compensation_csv_path: Path | None = None
        self.current_temperature_compensation_commands_path: Path | None = None
        self.current_modeling_result_path: Path | None = None
        self.current_modeling_run_dir: Path | None = None
        self.history_item_paths: Dict[str, Path] = {}
        self.history_items_cache: List[str] = []
        self._live_device_cache: Dict[str, Dict[str, str]] = {}
        self._live_device_cache_run_dir: Path | None = None
        self.current_workbook_name_var = tk.StringVar(value="Workbook：--")
        self.current_summary_report_name_var = tk.StringVar(value="汇总表：--")
        self.current_latest_point_name_var = tk.StringVar(value="最新点文件：--")
        self.current_coefficient_report_name_var = tk.StringVar(value="气体拟合报告：--")
        self.current_temperature_compensation_name_var = tk.StringVar(value="温度补偿结果：--")
        self.current_run_dir_name_var = tk.StringVar(value="Run目录：--")
        self.current_io_name_var = tk.StringVar(value="IO文件：--")
        self.current_modeling_config_name_var = tk.StringVar(value="离线建模配置：--")
        self.current_modeling_result_name_var = tk.StringVar(value="离线建模结果：--")
        self.temperature_compensation_apply_status_var = tk.StringVar(value="温度补偿下发：待命")
        self.modeling_input_path_var = tk.StringVar(value="")
        self.modeling_input_file_type_var = tk.StringVar(value="auto")
        self.modeling_input_sheet_var = tk.StringVar(value="0")
        self.modeling_save_status_var = tk.StringVar(value="保存状态：当前尚未保存离线建模输入文件")
        self.runtime_config_diff_var = tk.StringVar(value="配置差异：--")
        self._runtime_config_diff_cache_key: Tuple[Any, ...] | None = None
        self._runtime_config_diff_cache_text = "配置差异：--"
        self.start_readiness_var = tk.StringVar(value="启动校验：--")
        self.summary_mode_card_var = tk.StringVar(value="测量模式\n先水后气")
        self.summary_temp_card_var = tk.StringVar(value="温度点\n全部温度点")
        self.summary_gas_card_var = tk.StringVar(value="气点\n--")
        self.summary_cfg_card_var = tk.StringVar(value="配置状态\n未对比")
        self.current_target_ppm_var = tk.StringVar(value="当前标气：--")
        self.current_pressure_point_var = tk.StringVar(value="当前压力点：--")
        self.current_pressure_live_var = tk.StringVar(value="当前实压：--")
        self.current_pressure_stability_var = tk.StringVar(value="稳定标志：--")
        self.current_pressure_reapply_var = tk.StringVar(value="重发次数：--")
        self.current_route_group_detail_var = tk.StringVar(value="当前气路组：--")
        self.sample_progress_var = tk.StringVar(value="采样进度：--")
        self.data_freshness_var = tk.StringVar(value="数据刷新：--")
        self.points_preview_hint_var = tk.StringVar(value="点表预览：待加载")
        self.device_state_vars: Dict[str, tk.StringVar] = {}
        self.device_state_labels: Dict[str, tk.Label] = {}
        self.device_display_primary_vars: Dict[str, tk.StringVar] = {}
        self.device_display_secondary_vars: Dict[str, tk.StringVar] = {}
        self.device_online_vars: Dict[str, tk.StringVar] = {}
        self.device_online_labels: Dict[str, tk.Label] = {}
        self.device_issue_vars: Dict[str, tk.StringVar] = {}
        self.device_issue_labels: Dict[str, tk.Label] = {}
        self.device_issue_time_vars: Dict[str, tk.StringVar] = {}
        self.device_issue_time_labels: Dict[str, tk.Label] = {}
        self.device_trend_vars: Dict[str, tk.StringVar] = {}
        self.device_trend_detail_vars: Dict[str, tk.StringVar] = {}
        self.device_trend_labels: Dict[str, tk.Label] = {}
        self.device_trend_canvases: Dict[str, tk.Canvas] = {}
        self.current_events_cache: List[str] = []
        self._key_events_source_cache_key: Tuple[Any, ...] | None = None
        self._key_events_source_cache: List[str] = []
        self._key_events_render_cache_key: Tuple[Any, ...] | None = None
        self.safe_stop_in_progress = False
        self.temperature_compensation_apply_in_progress = False
        self._in_temp_scope_change = False
        self.safe_stop_countdown_var = tk.StringVar(value="恢复基线：待命")
        self.stage_icon_var = tk.StringVar(value="●")
        self._layout_after_id = None
        self.state_palette = {
            "idle": {"bg": "#f3f4f6", "fg": "#374151"},
            "ok": {"bg": "#dcfce7", "fg": "#166534"},
            "warn": {"bg": "#fef3c7", "fg": "#92400e"},
            "error": {"bg": "#fee2e2", "fg": "#991b1b"},
            "info": {"bg": "#dbeafe", "fg": "#1d4ed8"},
        }

        self._build_ui()
        try:
            self.load_config()
        except Exception as exc:
            self.temp_hint_var.set(f"温度点加载失败：{exc}")
        try:
            self._attach_latest_active_run()
        except Exception:
            pass

    def _configure_styles(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure(
            ".",
            background=self.ui_colors["bg"],
            foreground=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 10),
        )
        style.configure("TFrame", background=self.ui_colors["bg"])
        style.configure(
            "Card.TFrame",
            background=self.ui_colors["value_panel"],
            borderwidth=1,
            relief="solid",
        )
        style.configure(
            "TLabelframe",
            background=self.ui_colors["value_panel"],
            bordercolor=self.ui_colors["line"],
            borderwidth=1,
            relief="solid",
            padding=10,
        )
        style.configure(
            "TLabelframe.Label",
            background=self.ui_colors["value_panel"],
            foreground=self.ui_colors["hero_chip_text"],
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        style.configure(
            "HomeCard.TLabelframe",
            background=self.ui_colors["soft_layer"],
            bordercolor=self.ui_colors["divider"],
            borderwidth=1,
            relief="flat",
            padding=10,
        )
        style.configure(
            "HomeCard.TLabelframe.Label",
            background=self.ui_colors["soft_layer"],
            foreground=self.ui_colors["hero_chip_text"],
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        style.configure(
            "HomeBoard.TLabelframe",
            background=self.ui_colors["value_panel"],
            bordercolor=self.ui_colors["divider"],
            borderwidth=1,
            relief="flat",
            padding=10,
        )
        style.configure(
            "HomeBoard.TLabelframe.Label",
            background=self.ui_colors["value_panel"],
            foreground="#21486d",
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        style.configure("TLabel", background=self.ui_colors["bg"], foreground=self.ui_colors["text"])
        style.configure(
            "Muted.TLabel",
            background=self.ui_colors["bg"],
            foreground=self.ui_colors["muted"],
        )
        style.configure(
            "Header.TLabel",
            background=self.ui_colors["bg"],
            foreground=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 11, "bold"),
        )
        style.configure(
            "HeroTitle.TLabel",
            background=self.ui_colors["hero"],
            foreground=self.ui_colors["hero_text"],
            font=("Microsoft YaHei UI", 16, "bold"),
        )
        style.configure(
            "HeroSub.TLabel",
            background=self.ui_colors["hero"],
            foreground=self.ui_colors["hero_subtle"],
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        style.configure(
            "CardTitle.TLabel",
            background=self.ui_colors["card"],
            foreground=self.ui_colors["muted"],
            font=self.ui_metrics["title_font"],
        )
        style.configure(
            "CardValue.TLabel",
            background=self.ui_colors["card"],
            foreground=self.ui_colors["text"],
            font=self.ui_metrics["summary_value_font"],
        )
        style.configure(
            "TButton",
            padding=(11, 6),
            background="#f8fbff",
            foreground=self.ui_colors["text"],
            bordercolor="#d6e3f0",
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        style.map(
            "TButton",
            background=[("active", "#ffffff"), ("pressed", "#eef4fb")],
            foreground=[("disabled", "#94a3b8")],
        )
        style.configure(
            "Accent.TButton",
            background=self.ui_colors["accent"],
            foreground="#ffffff",
            bordercolor=self.ui_colors["accent"],
            focuscolor=self.ui_colors["accent"],
        )
        style.map(
            "Accent.TButton",
            background=[("active", self.ui_colors["accent_dark"]), ("disabled", "#94a3b8")],
            foreground=[("disabled", "#e2e8f0")],
        )
        style.configure("Info.TButton", background="#e5f0ff", foreground="#1d4ed8", bordercolor="#d7e4fb", padding=(10, 5))
        style.map("Info.TButton", background=[("active", "#dbeafe"), ("disabled", "#e5e7eb")], foreground=[("disabled", "#94a3b8")])
        style.configure("Warn.TButton", background="#fff1db", foreground="#b45309", bordercolor="#f5dfba", padding=(10, 5))
        style.map("Warn.TButton", background=[("active", "#ffe7bf"), ("disabled", "#ebecef")], foreground=[("disabled", "#94a3b8")])
        style.configure("Danger.TButton", background="#fee8ec", foreground="#b42318", bordercolor="#f5d4da", padding=(10, 5))
        style.map("Danger.TButton", background=[("active", "#ffd9df"), ("disabled", "#ebecef")], foreground=[("disabled", "#94a3b8")])
        style.configure("DangerStrong.TButton", background="#b42318", foreground="#ffffff", bordercolor="#b42318", padding=(12, 6))
        style.map("DangerStrong.TButton", background=[("active", "#991b1b"), ("disabled", "#d6d9de")], foreground=[("disabled", "#f8fafc")])
        style.configure("Subtle.TButton", background="#ffffff", foreground=self.ui_colors["text"], bordercolor=self.ui_colors["line"], padding=(10, 5))
        style.map("Subtle.TButton", background=[("active", "#f8fbff"), ("disabled", "#ebecef")], foreground=[("disabled", "#94a3b8")])
        style.configure("HumidityPage.TButton", background="#dff7f2", foreground="#0f766e", bordercolor="#c9ece4", padding=(10, 5), font=("Microsoft YaHei UI", 8, "bold"))
        style.map("HumidityPage.TButton", background=[("active", "#d0f1ea"), ("disabled", "#ebecef")], foreground=[("disabled", "#94a3b8")])
        style.configure("DewPage.TButton", background="#efe8ff", foreground="#6d28d9", bordercolor="#e0d4fb", padding=(10, 5), font=("Microsoft YaHei UI", 8, "bold"))
        style.map("DewPage.TButton", background=[("active", "#e6dbff"), ("disabled", "#ebecef")], foreground=[("disabled", "#94a3b8")])
        style.configure("ThermoPage.TButton", background="#fff0e5", foreground="#c2410c", bordercolor="#f4dac9", padding=(10, 5), font=("Microsoft YaHei UI", 8, "bold"))
        style.map("ThermoPage.TButton", background=[("active", "#ffe5d1"), ("disabled", "#ebecef")], foreground=[("disabled", "#94a3b8")])
        style.configure("ValvePage.TButton", background="#e8f2ff", foreground="#1d4f91", bordercolor="#d8e5f8", padding=(10, 5), font=("Microsoft YaHei UI", 8, "bold"))
        style.map("ValvePage.TButton", background=[("active", "#dbeafe"), ("disabled", "#ebecef")], foreground=[("disabled", "#94a3b8")])
        style.configure(
            "HomeTool.TButton",
            background="#f8fbff",
            foreground=self.ui_colors["text"],
            bordercolor="#d3deea",
            padding=(10, 5),
            font=("Microsoft YaHei UI", 8, "bold"),
        )
        style.map(
            "HomeTool.TButton",
            background=[("active", "#ffffff"), ("pressed", "#edf5ff"), ("disabled", "#ebecef")],
            foreground=[("disabled", "#94a3b8")],
        )
        style.configure(
            "TEntry",
            fieldbackground=self.ui_colors["card"],
            bordercolor=self.ui_colors["line"],
            padding=5,
        )
        style.configure(
            "TCombobox",
            fieldbackground=self.ui_colors["card"],
            background=self.ui_colors["card"],
            bordercolor=self.ui_colors["line"],
            padding=4,
        )
        style.map(
            "TCombobox",
            fieldbackground=[("readonly", self.ui_colors["card"])],
            selectbackground=[("readonly", self.ui_colors["card"])],
            selectforeground=[("readonly", self.ui_colors["text"])],
            foreground=[("readonly", self.ui_colors["text"])],
        )
        style.configure(
            "Home.TCombobox",
            fieldbackground="#f7fbff",
            background="#f7fbff",
            foreground=self.ui_colors["text"],
            arrowcolor=self.ui_colors["accent_dark"],
            bordercolor="#cfe0f2",
            lightcolor="#cfe0f2",
            darkcolor="#cfe0f2",
            insertcolor=self.ui_colors["text"],
            padding=(8, 5),
            relief="flat",
        )
        style.map(
            "Home.TCombobox",
            fieldbackground=[
                ("readonly", "#f7fbff"),
                ("focus", "#ffffff"),
                ("disabled", "#eef2f6"),
            ],
            background=[
                ("readonly", "#f7fbff"),
                ("focus", "#ffffff"),
                ("disabled", "#eef2f6"),
            ],
            selectbackground=[("readonly", "#f7fbff")],
            selectforeground=[("readonly", self.ui_colors["text"])],
            foreground=[
                ("readonly", self.ui_colors["text"]),
                ("disabled", "#94a3b8"),
            ],
            arrowcolor=[
                ("readonly", self.ui_colors["accent_dark"]),
                ("disabled", "#94a3b8"),
            ],
            bordercolor=[
                ("focus", self.ui_colors["accent_soft"]),
                ("readonly", "#cfe0f2"),
                ("disabled", "#dbe3ec"),
            ],
        )
        style.configure(
            "TNotebook",
            background=self.ui_colors["bg"],
            borderwidth=0,
            tabmargins=(0, 0, 0, 0),
        )
        style.configure(
            "TNotebook.Tab",
            background="#e6eef7",
            foreground=self.ui_colors["muted"],
            padding=(16, 8),
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", "#ffffff"), ("active", "#f4f8ff")],
            foreground=[("selected", self.ui_colors["accent_dark"]), ("active", self.ui_colors["text"])],
        )
        style.configure(
            "Home.TNotebook",
            background=self.ui_colors["soft_layer"],
            borderwidth=0,
            tabmargins=(0, 0, 0, 0),
        )
        style.configure(
            "Home.TNotebook.Tab",
            background="#edf4fd",
            foreground="#587089",
            borderwidth=0,
            padding=(14, 7),
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        style.map(
            "Home.TNotebook.Tab",
            background=[
                ("selected", "#ffffff"),
                ("active", "#f8fbff"),
            ],
            foreground=[
                ("selected", self.ui_colors["accent_dark"]),
                ("active", self.ui_colors["text"]),
            ],
        )
        style.configure(
            "Board.TNotebook",
            background="#edf4fd",
            borderwidth=0,
            tabmargins=(0, 0, 0, 0),
        )
        style.configure(
            "Board.TNotebook.Tab",
            background="#edf3fb",
            foreground="#5b7086",
            borderwidth=0,
            padding=(18, 9),
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        style.map(
            "Board.TNotebook.Tab",
            background=[
                ("selected", "#ffffff"),
                ("active", "#f8fbff"),
            ],
            foreground=[
                ("selected", "#103d67"),
                ("active", self.ui_colors["text"]),
            ],
        )
        style.configure(
            "Monitor.Treeview",
            background="#ffffff",
            fieldbackground="#ffffff",
            foreground=self.ui_colors["text"],
            bordercolor="#d7e3ef",
            lightcolor="#d7e3ef",
            darkcolor="#d7e3ef",
            rowheight=30,
            font=("Microsoft YaHei UI", 9),
        )
        style.map(
            "Monitor.Treeview",
            background=[("selected", "#dcecff")],
            foreground=[("selected", "#0f2338")],
        )
        style.configure(
            "Monitor.Treeview.Heading",
            background="#f3f7fc",
            foreground="#23425f",
            bordercolor="#d7e3ef",
            lightcolor="#f3f7fc",
            darkcolor="#f3f7fc",
            relief="flat",
            padding=(10, 8),
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        style.map(
            "Monitor.Treeview.Heading",
            background=[("active", "#edf4fb")],
            foreground=[("active", "#123a5d")],
        )
        style.configure(
            "Horizontal.TProgressbar",
            troughcolor="#dbe4ee",
            background=self.ui_colors["accent"],
            bordercolor=self.ui_colors["line"],
            lightcolor=self.ui_colors["accent"],
            darkcolor=self.ui_colors["accent"],
        )
        style.configure(
            "Vertical.TScrollbar",
            background="#d7e7f7",
            troughcolor="#f4f8fd",
            bordercolor="#d2dfec",
            arrowcolor="#57738f",
            darkcolor="#d7e7f7",
            lightcolor="#d7e7f7",
            gripcount=0,
            relief="flat",
            arrowsize=12,
        )
        style.map(
            "Vertical.TScrollbar",
            background=[("active", "#c8dcf1"), ("pressed", "#b8d1ec")],
            arrowcolor=[("active", "#33526d")],
        )

    def _call_on_ui_thread(self, func, *args, **kwargs):
        if not hasattr(self, "root") or self.root is None:
            return func(*args, **kwargs)
        result: Dict[str, Any] = {}
        done = Event()

        def _invoke() -> None:
            try:
                result["value"] = func(*args, **kwargs)
            except Exception as exc:  # pragma: no cover - defensive bridge
                result["error"] = exc
            finally:
                done.set()

        self.root.after(0, _invoke)
        done.wait()
        if "error" in result:
            raise result["error"]
        return result.get("value")

    def _build_ui(self) -> None:
        self.config_path = tk.StringVar(
            value=str(Path(__file__).resolve().parents[3] / "configs" / "default_config.json")
        )
        self.route_mode_var = tk.StringVar(value="先水后气")
        self.route_mode_var.trace_add("write", lambda *_args: self._on_route_mode_change())
        self.fit_enabled_var = tk.BooleanVar(value=True)
        self.fit_enabled_var.trace_add("write", lambda *_args: self._on_fit_mode_change())
        self.postrun_delivery_var = tk.BooleanVar(value=False)
        self.postrun_delivery_var.trace_add("write", lambda *_args: self._on_postrun_delivery_change())
        self.temp_scope_var = tk.StringVar(value="全部温度点")
        self.temp_scope_var.trace_add("write", lambda *_args: self._on_temp_scope_change())
        self.temperature_order_var = tk.StringVar(value="从高到低")
        self.temperature_order_var.trace_add("write", lambda *_args: self._on_temperature_order_change())
        self.config_file_brief_var = tk.StringVar(value="配置：default_config.json")
        self.route_mode_brief_var = tk.StringVar(value="模式：先水后气")
        self.fit_mode_brief_var = tk.StringVar(value="拟合：开启")
        self.temp_scope_brief_var = tk.StringVar(value="范围：全部温度点")
        self.temperature_order_brief_var = tk.StringVar(value="顺序：从高到低")

        main_pane = tk.PanedWindow(
            self.root,
            orient="vertical",
            sashwidth=12,
            showhandle=True,
            handlesize=10,
            handlepad=6,
            sashrelief="raised",
            bg=self.ui_colors["line"],
            bd=0,
            opaqueresize=True,
        )
        main_pane.pack(fill="both", expand=True)
        self.main_pane = main_pane

        top_shell = ttk.Frame(main_pane)
        bottom_shell = ttk.Frame(main_pane)
        main_pane.add(top_shell, stretch="always", minsize=470)
        main_pane.add(bottom_shell, stretch="always", minsize=240)
        self.top_shell = top_shell
        self.bottom_shell = bottom_shell

        top_parent = ttk.Frame(top_shell)
        top_parent.pack(fill="both", expand=True)
        self.top_parent = top_parent

        hero = tk.Frame(
            top_parent,
            bg=self.ui_colors["shadow"],
            highlightbackground=self.ui_colors["hero_line"],
            highlightthickness=1,
            padx=1,
            pady=1,
        )
        hero.pack(fill="x", padx=8, pady=(3, 1))
        hero_inner = tk.Frame(hero, bg=self.ui_colors["hero"], padx=10, pady=2)
        hero_inner.pack(fill="both", expand=True)
        hero_top = tk.Frame(hero_inner, bg=self.ui_colors["hero"])
        hero_top.pack(fill="x")
        hero_left = tk.Frame(hero_top, bg=self.ui_colors["hero"])
        hero_left.pack(side="left", fill="x", expand=True)
        hero_badges = tk.Frame(hero_left, bg=self.ui_colors["hero"])
        hero_badges.pack(anchor="w", pady=(0, 0))
        for text in ("气体校准", "HOME"):
            tk.Label(
                hero_badges,
                text=text,
                bg=self.ui_colors["hero_chip"],
                fg=self.ui_colors["hero_chip_text"],
                padx=7,
                pady=1,
                font=("Microsoft YaHei UI", 7, "bold"),
            ).pack(side="left", padx=(0, 8))
        hero_badges.pack_forget()
        ttk.Label(hero_left, text="气体分析仪校准控制台", style="HeroTitle.TLabel").pack(anchor="w")
        ttk.Label(hero_left, text="多设备联动校准 / 清晰控制流 / 1920x1080 桌面工作区", style="HeroSub.TLabel").pack(anchor="w", pady=(1, 0))
        hero_info_row = tk.Frame(hero_left, bg=self.ui_colors["hero"])
        hero_info_row.pack(anchor="w", pady=(4, 0))
        for text, bg, fg in (
            ("桌面首页", "#d8f8f3", "#0f766e"),
            ("联动控制", "#e6efff", "#1d4f91"),
            ("状态可视", "#fff3db", "#b45309"),
        ):
            tk.Label(
                hero_info_row,
                text=text,
                bg=bg,
                fg=fg,
                padx=8,
                pady=2,
                font=("Microsoft YaHei UI", 8, "bold"),
            ).pack(side="left", padx=(0, 8))
        hero_info_row.pack_forget()
        hero_right = tk.Frame(hero_top, bg=self.ui_colors["hero"])
        hero_right.pack(side="right", anchor="n")
        hero_status = tk.Frame(
            hero_right,
            bg="#18466f",
            highlightbackground="#2f638f",
            highlightthickness=1,
            padx=10,
            pady=7,
        )
        hero_status.pack(anchor="e", pady=(0, 2))
        hero_status_head = tk.Frame(hero_status, bg="#18466f")
        hero_status_head.pack(fill="x", pady=(0, 5))
        tk.Label(
            hero_status_head,
            text="HOME STATUS",
            bg="#dbeeff",
            fg="#1d4f91",
            padx=8,
            pady=2,
            font=("Consolas", 8, "bold"),
        ).pack(side="left")
        tk.Label(
            hero_status_head,
            text="READY",
            bg="#d8f8f3",
            fg="#0f766e",
            padx=8,
            pady=2,
            font=("Consolas", 8, "bold"),
        ).pack(side="right")
        hero_metrics = tk.Frame(hero_status, bg="#18466f")
        hero_metrics.pack(fill="x")
        for text, bg, fg in (
            ("1920x1080", "#e0f2fe", "#0c4a6e"),
            ("多设备", "#dcfce7", "#166534"),
        ):
            tk.Label(
                hero_metrics,
                text=text,
                bg=bg,
                fg=fg,
                padx=8,
                pady=2,
                font=("Consolas", 8, "bold"),
            ).pack(side="left", padx=(6, 0))
        hero_right.pack_forget()
        hero_bottom = tk.Frame(
            hero_inner,
            bg=self.ui_colors["hero_layer"],
            highlightbackground=self.ui_colors["hero_line"],
            highlightthickness=1,
            padx=8,
            pady=0,
        )
        hero_bottom.pack(fill="x", pady=(1, 0))
        hero_metrics = tk.Frame(hero_bottom, bg=self.ui_colors["hero_layer"])
        hero_metrics.pack(anchor="w")
        for text, bg, fg in (
            ("☼ 温度控制", "#fff7d6", "#92400e"),
            ("☁ 湿度调节", "#e0f2fe", "#075985"),
            ("◌ 气压闭环", "#dcfce7", "#166534"),
            ("◍ 露点监测", "#fee2e2", "#991b1b"),
        ):
            tk.Label(
                hero_metrics,
                text=text,
                bg=bg,
                fg=fg,
                padx=5,
                pady=0,
                font=("Microsoft YaHei UI", 7, "bold"),
            ).pack(side="left", padx=(0, 8))
        hero_bottom.pack_forget()

        content_area = tk.Frame(top_parent, bg=self.ui_colors["bg"])
        content_area.pack(fill="both", expand=True, padx=0, pady=0)
        content_area.grid_columnconfigure(0, weight=6)
        content_area.grid_columnconfigure(1, weight=5)
        content_area.grid_rowconfigure(0, weight=0)
        content_area.grid_rowconfigure(1, weight=1)
        self.content_area = content_area

        left_column = tk.Frame(content_area, bg=self.ui_colors["bg"])
        right_column = tk.Frame(content_area, bg=self.ui_colors["bg"])
        left_column.grid(row=0, column=0, sticky="nsew", padx=(6, 5), pady=(0, 0))
        right_column.grid(row=0, column=1, sticky="nsew", padx=(5, 6), pady=(0, 0))
        left_column.grid_columnconfigure(0, weight=1)
        left_column.grid_rowconfigure(1, weight=1)
        right_column.grid_columnconfigure(0, weight=1)
        right_column.grid_rowconfigure(0, weight=1)
        self.left_column = left_column
        self.right_column = right_column

        left_top_row = tk.Frame(left_column, bg=self.ui_colors["bg"])
        left_top_row.grid(row=0, column=0, sticky="ew", padx=6, pady=(0, 2))
        left_top_row.grid_columnconfigure(0, weight=4)
        left_top_row.grid_columnconfigure(1, weight=3)

        control_shell = tk.Frame(left_top_row, bg=self.ui_colors["shadow"], padx=1, pady=1)
        control_shell.grid(row=0, column=0, sticky="nsew", padx=(0, 6), pady=0)
        control_box = ttk.LabelFrame(control_shell, text="流程设置", style="HomeCard.TLabelframe")
        control_box.pack(fill="x")
        self.control_box = control_box
        control_intro = tk.Frame(control_box, bg=self.ui_colors["value_panel"])
        control_intro.pack(fill="x", padx=6, pady=(0, 4))
        tk.Label(
            control_intro,
            text="CONTROL CENTER",
            bg=self.ui_colors["accent_soft"],
            fg=self.ui_colors["accent_dark"],
            padx=10,
            pady=3,
            font=("Consolas", 8, "bold"),
        ).pack(side="left")
        for text, bg, fg in (
            ("配置", "#edf5ff", "#1d4ed8"),
            ("模式", "#f0fdf4", "#166534"),
        ):
            tk.Label(
                control_intro,
                text=text,
                bg=bg,
                fg=fg,
                padx=8,
                pady=3,
                font=("Microsoft YaHei UI", 8, "bold"),
            ).pack(side="left", padx=(8, 0))
        control_intro.pack_forget()

        config_panel = tk.Frame(
            control_box,
            bg=self.ui_colors["soft_layer"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=10,
            pady=8,
        )
        config_panel.pack(fill="x", padx=6, pady=(0, 4))
        top = tk.Frame(config_panel, bg=self.ui_colors["soft_layer"])
        top.pack(fill="x")

        tk.Label(top, text="配置文件", bg=self.ui_colors["soft_layer"], fg=self.ui_colors["text"], font=("Microsoft YaHei UI", 9, "bold")).pack(side="left", padx=(0, 10))
        self.config_entry = ttk.Entry(top, textvariable=self.config_path, width=54)
        self.config_entry.pack(side="left", padx=6, fill="x", expand=True)
        self.load_button = ttk.Button(top, text="加载配置", command=self.load_config, style="HomeTool.TButton", width=10)
        self.load_button.pack(side="left")
        config_meta = tk.Frame(config_panel, bg=self.ui_colors["soft_layer"])
        config_meta.pack(fill="x", pady=(6, 0))
        self.config_file_brief_label = tk.Label(
            config_meta,
            textvariable=self.config_file_brief_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.config_file_brief_label.pack(side="left")

        mode_panel = tk.Frame(
            control_box,
            bg=self.ui_colors["soft_layer"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=10,
            pady=6,
        )
        mode_panel.pack(fill="x", padx=6, pady=(0, 4))
        opts = tk.Frame(mode_panel, bg=self.ui_colors["soft_layer"])
        opts.pack(fill="x")
        opts.grid_columnconfigure(0, weight=1)
        opts.grid_columnconfigure(1, weight=1)
        opts.grid_columnconfigure(2, weight=1)
        opts.grid_columnconfigure(3, weight=1)
        opts.grid_columnconfigure(4, weight=1)
        route_field = tk.Frame(
            opts,
            bg="#f7fbff",
            highlightbackground="#d7e6f5",
            highlightthickness=1,
            padx=10,
            pady=7,
        )
        route_field.grid(row=0, column=0, sticky="ew", padx=(0, 5))
        tk.Label(route_field, text="测量模式", bg="#f7fbff", fg=self.ui_colors["muted"], font=("Microsoft YaHei UI", 8, "bold")).pack(anchor="w", pady=(0, 4))
        self.route_mode_combo = ttk.Combobox(
            route_field,
            textvariable=self.route_mode_var,
            values=["先水后气", "只测水路", "只测气路"],
            width=12,
            state="readonly",
            style="Home.TCombobox",
        )
        self.route_mode_combo.pack(fill="x")
        scope_field = tk.Frame(
            opts,
            bg="#f7fbff",
            highlightbackground="#d7e6f5",
            highlightthickness=1,
            padx=10,
            pady=7,
        )
        scope_field.grid(row=0, column=1, sticky="ew", padx=(5, 0))
        tk.Label(scope_field, text="温度点范围", bg="#f7fbff", fg=self.ui_colors["muted"], font=("Microsoft YaHei UI", 8, "bold")).pack(anchor="w", pady=(0, 4))
        self.temp_scope_combo = ttk.Combobox(
            scope_field,
            textvariable=self.temp_scope_var,
            values=["全部温度点", "指定温度点"],
            width=12,
            state="readonly",
            style="Home.TCombobox",
        )
        self.temp_scope_combo.pack(fill="x")
        order_field = tk.Frame(
            opts,
            bg="#f7fbff",
            highlightbackground="#d7e6f5",
            highlightthickness=1,
            padx=10,
            pady=7,
        )
        order_field.grid(row=0, column=2, sticky="ew", padx=(5, 0))
        tk.Label(order_field, text="温度顺序", bg="#f7fbff", fg=self.ui_colors["muted"], font=("Microsoft YaHei UI", 8, "bold")).pack(anchor="w", pady=(0, 4))
        self.temperature_order_combo = ttk.Combobox(
            order_field,
            textvariable=self.temperature_order_var,
            values=["从高到低", "从低到高"],
            width=12,
            state="readonly",
            style="Home.TCombobox",
        )
        self.temperature_order_combo.pack(fill="x")
        fit_field = tk.Frame(
            opts,
            bg="#f7fbff",
            highlightbackground="#d7e6f5",
            highlightthickness=1,
            padx=10,
            pady=7,
        )
        fit_field.grid(row=0, column=3, sticky="ew", padx=(5, 0))
        tk.Label(fit_field, text="校准拟合", bg="#f7fbff", fg=self.ui_colors["muted"], font=("Microsoft YaHei UI", 8, "bold")).pack(anchor="w", pady=(0, 4))
        self.fit_enabled_check = tk.Checkbutton(
            fit_field,
            text="运行后自动拟合",
            variable=self.fit_enabled_var,
            onvalue=True,
            offvalue=False,
            anchor="w",
            bg="#f7fbff",
            fg=self.ui_colors["text"],
            activebackground="#f7fbff",
            activeforeground=self.ui_colors["text"],
            selectcolor="#e6fffb",
            relief="flat",
            highlightthickness=0,
            font=("Microsoft YaHei UI", 9, "bold"),
            command=self._refresh_execution_summary,
        )
        self.fit_enabled_check.pack(fill="x")
        tk.Label(
            fit_field,
            text="关闭后仅采集和汇总，不做最终系数拟合",
            bg="#f7fbff",
            fg=self.ui_colors["muted"],
            anchor="w",
            justify="left",
            wraplength=180,
            font=("Microsoft YaHei UI", 8),
        ).pack(fill="x", pady=(4, 0))
        delivery_field = tk.Frame(
            opts,
            bg="#f7fbff",
            highlightbackground="#d7e6f5",
            highlightthickness=1,
            padx=10,
            pady=7,
        )
        delivery_field.grid(row=0, column=4, sticky="ew", padx=(5, 0))
        tk.Label(delivery_field, text="自动交付", bg="#f7fbff", fg=self.ui_colors["muted"], font=("Microsoft YaHei UI", 8, "bold")).pack(anchor="w", pady=(0, 4))
        self.postrun_delivery_check = tk.Checkbutton(
            delivery_field,
            text="完轮后自动算系数并写回",
            variable=self.postrun_delivery_var,
            onvalue=True,
            offvalue=False,
            anchor="w",
            bg="#f7fbff",
            fg=self.ui_colors["text"],
            activebackground="#f7fbff",
            activeforeground=self.ui_colors["text"],
            selectcolor="#e6fffb",
            relief="flat",
            highlightthickness=0,
            font=("Microsoft YaHei UI", 9, "bold"),
            command=self._refresh_execution_summary,
        )
        self.postrun_delivery_check.pack(fill="x")
        self.postrun_delivery_check.configure(state="disabled")
        tk.Label(
            delivery_field,
            text="自动计算系数、写入设备，并执行短验证；可随本轮关闭。",
            bg="#f7fbff",
            fg=self.ui_colors["muted"],
            anchor="w",
            justify="left",
            wraplength=190,
            font=("Microsoft YaHei UI", 8),
        ).pack(fill="x", pady=(4, 0))
        delivery_field.grid_remove()
        mode_meta = tk.Frame(mode_panel, bg=self.ui_colors["soft_layer"])
        mode_meta.pack(fill="x", pady=(4, 0))
        self.route_mode_brief_label = tk.Label(
            mode_meta,
            textvariable=self.route_mode_brief_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.route_mode_brief_label.pack(side="left")
        self.fit_mode_brief_label = tk.Label(
            mode_meta,
            textvariable=self.fit_mode_brief_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.fit_mode_brief_label.pack(side="left", padx=(6, 0))
        self.temp_scope_brief_label = tk.Label(
            mode_meta,
            textvariable=self.temp_scope_brief_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.temp_scope_brief_label.pack(side="left", padx=(6, 0))
        self.temp_scope_brief_label.pack_forget()
        self.temperature_order_brief_label = tk.Label(
            mode_meta,
            textvariable=self.temperature_order_brief_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.temperature_order_brief_label.pack(side="left", padx=(6, 0))

        action_shell = tk.Frame(left_top_row, bg=self.ui_colors["shadow"], padx=1, pady=1)
        action_shell.grid(row=0, column=1, sticky="nsew", pady=0)
        action_box = ttk.LabelFrame(action_shell, text="操作面板", style="HomeCard.TLabelframe")
        action_box.pack(fill="both", expand=True)
        action_intro = tk.Frame(action_box, bg=self.ui_colors["value_panel"])
        action_intro.pack(fill="x", padx=6, pady=(0, 4))
        tk.Label(
            action_intro,
            text="QUICK ACTIONS",
            bg="#fff4e8",
            fg="#c2410c",
            padx=10,
            pady=3,
            font=("Consolas", 8, "bold"),
        ).pack(side="left")
        tk.Label(
            action_intro,
            text="主页操作",
            bg="#eef6ff",
            fg="#1d4f91",
            padx=8,
            pady=3,
            font=("Microsoft YaHei UI", 8, "bold"),
        ).pack(side="left", padx=(8, 0))
        action_intro.pack_forget()

        btns = tk.Frame(
            action_box,
            bg="#ffffff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=6,
        )
        btns.pack(fill="x", padx=6, pady=(0, 0))
        flow_label = tk.Label(
            btns,
            text="流程控制",
            anchor="w",
            bg="#ffffff",
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 8, "bold"),
        )
        flow_label.pack(fill="x", pady=(0, 4))
        flow_label.pack_forget()
        btn_row1 = tk.Frame(btns, bg="#ffffff")
        btn_row1.pack(fill="x", pady=(0, 4))
        primary_actions = tk.Frame(btn_row1, bg="#ffffff")
        primary_actions.pack(side="left")
        secondary_actions = tk.Frame(btn_row1, bg="#ffffff")
        secondary_actions.pack(side="right")
        self.start_button = ttk.Button(primary_actions, text="开始", command=self.start, style="Accent.TButton", width=9)
        self.start_button.pack(side="left")
        self.pause_button = ttk.Button(primary_actions, text="暂停", command=self.pause, style="Warn.TButton", width=8)
        self.pause_button.pack(side="left", padx=6)
        self.resume_button = ttk.Button(primary_actions, text="继续", command=self.resume, style="Info.TButton", width=8)
        self.resume_button.pack(side="left")
        self.stop_button = ttk.Button(secondary_actions, text="停止", command=self.stop, style="Danger.TButton", width=9)
        self.stop_button.pack(side="right")
        self.self_test_button = ttk.Button(secondary_actions, text="自检", command=self.self_test, style="Info.TButton", width=7)
        self.self_test_button.pack(side="right", padx=(0, 6))
        safety_label = tk.Label(
            btns,
            text="安全恢复",
            anchor="w",
            bg="#ffffff",
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 8, "bold"),
        )
        safety_label.pack(fill="x", pady=(0, 4))
        safety_label.pack_forget()
        btn_row2 = tk.Frame(
            btns,
            bg="#fdf5f5",
            highlightbackground="#f0d5d5",
            highlightthickness=1,
            padx=6,
            pady=6,
        )
        btn_row2.pack(fill="x", pady=(0, 4))
        self.safe_stop_button = ttk.Button(
            btn_row2,
            text="一键恢复基线",
            command=self.safe_stop,
            style="DangerStrong.TButton",
            width=12,
        )
        self.safe_stop_button.pack(side="left")
        self.safe_stop_status_label = tk.Label(
            btn_row2,
            textvariable=self.safe_stop_countdown_var,
            anchor="w",
            padx=8,
            bg="#fffafa",
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9, "bold"),
            highlightthickness=1,
            highlightbackground="#ead8d8",
        )
        self.safe_stop_status_label.pack(side="left", padx=(8, 0), fill="x", expand=True)
        tool_label = tk.Label(
            btns,
            text="工具窗口",
            anchor="w",
            bg="#ffffff",
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 8, "bold"),
        )
        tool_label.pack(fill="x", pady=(0, 4))
        tool_label.pack_forget()
        btn_row3 = tk.Frame(btns, bg="#ffffff")
        btn_row3.pack(fill="x", pady=(0, 0))
        for col in range(4):
            btn_row3.grid_columnconfigure(col, weight=1)
        self.open_humidity_button = ttk.Button(btn_row3, text="湿度发生器", command=self.open_humidity_page, style="HumidityPage.TButton", width=10)
        self.open_humidity_button.grid(row=0, column=0, sticky="ew")
        self.open_dewpoint_button = ttk.Button(btn_row3, text="露点仪", command=self.open_dewpoint_page, style="DewPage.TButton", width=10)
        self.open_dewpoint_button.grid(row=0, column=1, sticky="ew", padx=(6, 0))
        self.open_thermometer_button = ttk.Button(btn_row3, text="测温仪", command=self.open_thermometer_page, style="ThermoPage.TButton", width=10)
        self.open_thermometer_button.grid(row=0, column=2, sticky="ew", padx=(6, 0))
        self.open_valve_button = ttk.Button(btn_row3, text="阀门控制", command=self.open_valve_page, style="ValvePage.TButton", width=10)
        self.open_valve_button.grid(row=0, column=3, sticky="ew", padx=(6, 0))
        nav_label = tk.Label(
            btns,
            text="快捷查看",
            anchor="w",
            bg="#ffffff",
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 8, "bold"),
        )
        nav_label.pack(fill="x", pady=(4, 4))
        nav_label.pack_forget()
        btn_row4 = tk.Frame(btns, bg="#ffffff")
        btn_row4.pack(fill="x", pady=(0, 0))
        for col in range(4):
            btn_row4.grid_columnconfigure(col, weight=1)
        self.open_monitor_view_button = ttk.Button(
            btn_row4,
            text="运行监控",
            command=self._show_run_monitor,
            style="HomeTool.TButton",
            width=10,
        )
        self.open_monitor_view_button.grid(row=0, column=0, sticky="ew")
        self.open_device_view_button = ttk.Button(
            btn_row4,
            text="设备总览",
            command=self._show_device_overview,
            style="HomeTool.TButton",
            width=10,
        )
        self.open_device_view_button.grid(row=0, column=1, sticky="ew", padx=(6, 0))
        self.open_points_view_button = ttk.Button(
            btn_row4,
            text="执行点预览",
            command=self._show_points_preview,
            style="HomeTool.TButton",
            width=10,
        )
        self.open_points_view_button.grid(row=0, column=2, sticky="ew", padx=(6, 0))
        self.open_logs_view_button = ttk.Button(
            btn_row4,
            text="日志工作台",
            command=self._show_workbench_view,
            style="HomeTool.TButton",
            width=10,
        )
        self.open_logs_view_button.grid(row=0, column=3, sticky="ew", padx=(6, 0))

        selection_shell = tk.Frame(left_column, bg=self.ui_colors["shadow"], padx=1, pady=1)
        selection_shell.grid(row=1, column=0, sticky="nsew", padx=6, pady=(0, 1))
        selection_area = tk.Frame(selection_shell, bg=self.ui_colors["soft_layer"])
        selection_area.pack(fill="both", expand=True)
        self.selection_area = selection_area
        selector_intro = tk.Frame(selection_area, bg=self.ui_colors["soft_layer"])
        selector_intro.pack(fill="x", padx=6, pady=(4, 2))
        tk.Label(
            selector_intro,
            text="POINT SELECTOR",
            bg="#eef6ff",
            fg="#1d4f91",
            padx=10,
            pady=3,
            font=("Consolas", 8, "bold"),
        ).pack(side="left")
        for text, bg, fg in (
            ("多选", "#edfdfb", "#0f766e"),
            ("滚动", "#f5f3ff", "#6d28d9"),
        ):
            tk.Label(
                selector_intro,
                text=text,
                bg=bg,
                fg=fg,
                padx=8,
                pady=3,
                font=("Microsoft YaHei UI", 8, "bold"),
            ).pack(side="left", padx=(8, 0))
        selector_intro.pack_forget()

        selector_tabs_shell = tk.Frame(
            selection_area,
            bg=self.ui_colors["chip_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=4,
            pady=4,
        )
        selector_tabs_shell.pack(fill="both", expand=True, padx=6, pady=(0, 0))
        selector_tabs = ttk.Notebook(selector_tabs_shell, style="Home.TNotebook")
        selector_tabs.pack(fill="both", expand=True)
        self.selector_tabs = selector_tabs

        temp_tab = ttk.Frame(selector_tabs)
        co2_tab = ttk.Frame(selector_tabs)
        pressure_tab = ttk.Frame(selector_tabs)
        points_tab = ttk.Frame(selector_tabs)
        selector_tabs.add(temp_tab, text="温度点")
        selector_tabs.add(co2_tab, text="气点")
        selector_tabs.add(pressure_tab, text="压力点")
        selector_tabs.add(points_tab, text="执行点预览")
        self.temp_tab = temp_tab
        self.co2_tab = co2_tab
        self.pressure_tab = pressure_tab
        self.points_tab = points_tab

        temp_box = ttk.LabelFrame(temp_tab, text="温度点选择", style="HomeCard.TLabelframe")
        temp_box.pack(fill="both", expand=True, padx=(0, 0), pady=0)
        self.temp_box = temp_box
        temp_actions = tk.Frame(temp_box, bg=self.ui_colors["soft_layer"], highlightbackground=self.ui_colors["divider"], highlightthickness=1, padx=8, pady=5)
        temp_actions.pack(fill="x", padx=6, pady=(0, 4))
        self.temp_select_all_button = ttk.Button(temp_actions, text="全选", command=self._select_all_temps, style="HomeTool.TButton", width=7)
        self.temp_select_all_button.pack(side="left")
        self.temp_clear_button = ttk.Button(temp_actions, text="清空", command=self._clear_all_temps, style="Subtle.TButton", width=7)
        self.temp_clear_button.pack(side="left", padx=6)
        self.temp_hint_var = tk.StringVar(value="温度点将根据 points.xlsx 自动加载")
        tk.Label(temp_actions, textvariable=self.temp_hint_var, bg=self.ui_colors["soft_layer"], fg=self.ui_colors["muted"], font=("Microsoft YaHei UI", 8)).pack(side="right")
        self.temp_checks_shell, self.temp_listbox = self._create_multiselect_listbox_panel(temp_box, 8)
        self.temp_checks_canvas = None
        self.temp_checks_inner = None
        self.temp_listbox.bind("<<ListboxSelect>>", self._on_temp_listbox_select, add="+")
        self.temp_listbox.bind("<Button-1>", self._on_temp_listbox_click, add="+")

        co2_box = ttk.LabelFrame(co2_tab, text="气点选择", style="HomeCard.TLabelframe")
        co2_box.pack(fill="both", expand=True, padx=(0, 0), pady=0)
        self.co2_box = co2_box
        co2_actions = tk.Frame(co2_box, bg=self.ui_colors["soft_layer"], highlightbackground=self.ui_colors["divider"], highlightthickness=1, padx=8, pady=5)
        co2_actions.pack(fill="x", padx=6, pady=(0, 4))
        self.co2_select_all_button = ttk.Button(co2_actions, text="全选", command=self._select_all_co2, style="HomeTool.TButton", width=7)
        self.co2_select_all_button.pack(side="left")
        self.co2_clear_button = ttk.Button(co2_actions, text="清空", command=self._clear_all_co2, style="Subtle.TButton", width=7)
        self.co2_clear_button.pack(side="left", padx=6)
        self.co2_hint_var = tk.StringVar(value="气点将根据阀门配置自动加载")
        tk.Label(co2_actions, textvariable=self.co2_hint_var, bg=self.ui_colors["soft_layer"], fg=self.ui_colors["muted"], font=("Microsoft YaHei UI", 8)).pack(side="right")
        self.co2_checks_shell, self.co2_listbox = self._create_multiselect_listbox_panel(co2_box, 8)
        self.co2_checks_canvas = None
        self.co2_checks_inner = None
        self.co2_listbox.bind("<<ListboxSelect>>", self._on_co2_listbox_select, add="+")
        self.co2_listbox.bind("<Button-1>", self._on_co2_listbox_click, add="+")

        pressure_box = ttk.LabelFrame(pressure_tab, text="压力点选择", style="HomeCard.TLabelframe")
        pressure_box.pack(fill="both", expand=True, padx=(0, 0), pady=0)
        self.pressure_box = pressure_box
        pressure_actions = tk.Frame(
            pressure_box,
            bg=self.ui_colors["soft_layer"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=8,
            pady=5,
        )
        pressure_actions.pack(fill="x", padx=6, pady=(0, 4))
        self.pressure_select_all_button = ttk.Button(
            pressure_actions,
            text="全选",
            command=self._select_all_pressures,
            style="HomeTool.TButton",
            width=7,
        )
        self.pressure_select_all_button.pack(side="left")
        self.pressure_clear_button = ttk.Button(
            pressure_actions,
            text="清空",
            command=self._clear_all_pressures,
            style="Subtle.TButton",
            width=7,
        )
        self.pressure_clear_button.pack(side="left", padx=6)
        self.ambient_pressure_check = ttk.Checkbutton(
            pressure_actions,
            text=AMBIENT_PRESSURE_UI_LABEL,
            variable=self.ambient_pressure_var,
            command=self._on_ambient_pressure_change,
        )
        self.ambient_pressure_check.pack(side="left", padx=(8, 4))
        self.pressure_hint_var = tk.StringVar(value="压力点固定为 7 个标准压力点 + 当前大气压")
        tk.Label(
            pressure_actions,
            textvariable=self.pressure_hint_var,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 8),
        ).pack(side="right")
        self.pressure_checks_shell, self.pressure_listbox = self._create_multiselect_listbox_panel(pressure_box, 8)
        self.pressure_checks_canvas = None
        self.pressure_checks_inner = None
        self.pressure_listbox.bind("<<ListboxSelect>>", self._on_pressure_listbox_select, add="+")
        self.pressure_listbox.bind("<Button-1>", self._on_pressure_listbox_click, add="+")

        self.selector_hint_var = tk.StringVar(value="温度点、气点与压力点支持多选；当前大气压为独立开路采样选项")
        tk.Label(selector_intro, textvariable=self.selector_hint_var, bg=self.ui_colors["soft_layer"], fg=self.ui_colors["muted"], font=("Microsoft YaHei UI", 8)).pack(side="right")
        selection_summary_strip = tk.Frame(
            selection_area,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=8,
            pady=6,
        )
        selection_summary_strip.pack(fill="x", padx=6, pady=(0, 4))
        selection_summary_strip.pack_forget()
        selection_summary_strip.grid_columnconfigure(0, weight=1)
        selection_summary_strip.grid_columnconfigure(1, weight=1)
        selection_summary_strip.grid_columnconfigure(2, weight=1)
        self.selection_temp_summary_label = tk.Label(
            selection_summary_strip,
            textvariable=self.temp_hint_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.selection_temp_summary_label.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        self.selection_co2_summary_label = tk.Label(
            selection_summary_strip,
            textvariable=self.co2_hint_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.selection_co2_summary_label.grid(row=0, column=1, sticky="ew")
        self.selection_pressure_summary_label = tk.Label(
            selection_summary_strip,
            textvariable=self.pressure_hint_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.selection_pressure_summary_label.grid(row=0, column=2, sticky="ew", padx=(6, 0))

        points_box = ttk.LabelFrame(points_tab, text="执行点预览", style="HomeCard.TLabelframe")
        points_box.pack(fill="both", expand=True, padx=(0, 0), pady=0)
        points_actions = tk.Frame(points_box, bg=self.ui_colors["soft_layer"], highlightbackground=self.ui_colors["divider"], highlightthickness=1, padx=8, pady=5)
        points_actions.pack(fill="x", padx=6, pady=(0, 4))
        ttk.Button(points_actions, text="刷新", command=self._refresh_points_preview, style="HomeTool.TButton", width=7).pack(side="left")
        ttk.Button(points_actions, text="打开点表", command=self._open_points_excel, style="Subtle.TButton", width=9).pack(side="left", padx=6)
        tk.Label(points_actions, textvariable=self.points_preview_hint_var, bg=self.ui_colors["soft_layer"], fg=self.ui_colors["muted"], font=("Microsoft YaHei UI", 8)).pack(side="right")

        points_tree_shell = tk.Frame(
            points_box,
            bg=self.ui_colors["card"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
        )
        points_tree_shell.pack(fill="both", expand=True, padx=6, pady=(0, 2))
        points_tree_scroll_y = ttk.Scrollbar(points_tree_shell, orient="vertical")
        points_tree_scroll_x = ttk.Scrollbar(points_tree_shell, orient="horizontal")
        self.points_tree = ttk.Treeview(
            points_tree_shell,
            columns=("seq", "row", "temp", "route", "hgen", "co2", "pressure", "group", "status"),
            show="headings",
            yscrollcommand=points_tree_scroll_y.set,
            xscrollcommand=points_tree_scroll_x.set,
            height=15,
            style="Monitor.Treeview",
        )
        points_tree_scroll_y.configure(command=self.points_tree.yview)
        points_tree_scroll_x.configure(command=self.points_tree.xview)
        for col, title, width, anchor in (
            ("seq", "顺序", 56, "center"),
            ("row", "点表行", 66, "center"),
            ("temp", "温度", 70, "center"),
            ("route", "路径", 64, "center"),
            ("hgen", "水路目标", 170, "w"),
            ("co2", "气点", 96, "center"),
            ("pressure", "压力", 96, "center"),
            ("group", "组别", 64, "center"),
            ("status", "本轮状态", 220, "w"),
        ):
            self.points_tree.heading(col, text=title)
            self.points_tree.column(col, width=width, anchor=anchor, stretch=(col in {"hgen", "status"}))
        self.points_tree.tag_configure("run", foreground="#166534", background="#f4fbf7")
        self.points_tree.tag_configure("skip", foreground="#92400e", background="#fff8ee")
        self.points_tree.pack(side="left", fill="both", expand=True)
        points_tree_scroll_y.pack(side="right", fill="y")
        points_tree_scroll_x.pack(side="bottom", fill="x")

        self.valve_hint_var = tk.StringVar(value="二氧化碳气路：--")
        right_top_row = tk.Frame(right_column, bg=self.ui_colors["bg"])
        right_top_row.grid(row=0, column=0, sticky="nsew", padx=0, pady=0)
        self.right_top_row = right_top_row

        right_tabs_shell = tk.Frame(
            right_top_row,
            bg="#eaf1fb",
            highlightbackground="#d6e2ef",
            highlightthickness=1,
            padx=5,
            pady=5,
        )
        right_tabs_shell.pack(fill="both", expand=True, padx=8, pady=(0, 1))
        right_tabs = ttk.Notebook(right_tabs_shell, style="Home.TNotebook")
        right_tabs.pack(fill="both", expand=True)
        self.right_tabs = right_tabs

        summary_tab = ttk.Frame(right_tabs)
        status_tab = ttk.Frame(right_tabs)
        coefficient_tab = ttk.Frame(right_tabs)
        summary_tab.pack_propagate(False)
        status_tab.pack_propagate(False)
        coefficient_tab.pack_propagate(False)
        right_tabs.add(summary_tab, text="启动前摘要")
        right_tabs.add(status_tab, text="运行监控")
        right_tabs.add(coefficient_tab, text="校准结果")
        right_tabs.select(status_tab)
        self.summary_tab = summary_tab
        self.status_tab = status_tab
        self.coefficient_tab = coefficient_tab

        def _make_scroll_tab(parent: ttk.Frame, bg: str | None = None) -> Tuple[tk.Frame, tk.Canvas, tk.Frame]:
            surface = bg or self.ui_colors["bg"]
            shell = tk.Frame(parent, bg=self.ui_colors["bg"])
            shell.pack(fill="both", expand=True)
            canvas = tk.Canvas(shell, bg=surface, highlightthickness=0, bd=0)
            scrollbar = ttk.Scrollbar(shell, orient="vertical", command=canvas.yview)
            inner = tk.Frame(canvas, bg=surface)
            window_id = canvas.create_window((0, 0), window=inner, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            canvas.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")

            def _sync_inner(_event=None) -> None:
                canvas.configure(scrollregion=canvas.bbox("all"))

            def _sync_width(event) -> None:
                canvas.itemconfigure(window_id, width=event.width)

            inner.bind("<Configure>", _sync_inner, add="+")
            canvas.bind("<Configure>", _sync_width, add="+")
            return shell, canvas, inner

        _summary_scroll_shell, self.summary_scroll_canvas, summary_inner = _make_scroll_tab(summary_tab)
        _status_scroll_shell, self.status_scroll_canvas, status_inner = _make_scroll_tab(status_tab)
        _coefficient_scroll_shell, self.coefficient_scroll_canvas, coefficient_inner = _make_scroll_tab(coefficient_tab)

        summary_shell = tk.Frame(summary_inner, bg=self.ui_colors["shadow"], padx=1, pady=1)
        summary_shell.pack(fill="both", expand=True, padx=0, pady=(0, 0))
        summary_box = ttk.LabelFrame(summary_shell, text="启动前摘要与流程说明", style="HomeBoard.TLabelframe")
        summary_box.pack(fill="both", expand=True, padx=0, pady=(0, 0))
        self.summary_box = summary_box
        summary_intro = tk.Frame(summary_box, bg=self.ui_colors["value_panel"])
        summary_intro.pack(fill="x", padx=8, pady=(4, 6))
        tk.Frame(summary_intro, bg="#dbeafe", height=3).pack(fill="x", pady=(0, 6))
        summary_intro_head = tk.Frame(summary_intro, bg=self.ui_colors["value_panel"])
        summary_intro_head.pack(fill="x")
        tk.Label(
            summary_intro_head,
            text="RUN OVERVIEW",
            bg="#e8f2ff",
            fg="#1d4f91",
            padx=10,
            pady=3,
            font=("Consolas", 8, "bold"),
        ).pack(side="left")
        for text, bg, fg in (
            ("摘要", "#edfdfb", "#0f766e"),
            ("流程", "#fff4e8", "#c2410c"),
        ):
            tk.Label(
                summary_intro_head,
                text=text,
                bg=bg,
                fg=fg,
                padx=8,
                pady=3,
                font=("Microsoft YaHei UI", 8, "bold"),
            ).pack(side="left", padx=(8, 0))
        tk.Label(
            summary_intro,
            text="主页摘要区集中展示当前配置、点位选择和启动前准备信息。",
            bg=self.ui_colors["value_panel"],
            fg=self.ui_colors["muted"],
            anchor="w",
            justify="left",
            font=("Microsoft YaHei UI", 9),
        ).pack(fill="x", pady=(5, 0))
        summary_intro.pack_forget()
        cards_shell = tk.Frame(
            summary_box,
            bg="#f7fbff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=10,
        )
        cards_shell.pack(fill="x", padx=6, pady=(2, 4))
        cards_row = tk.Frame(cards_shell, bg="#f7fbff")
        cards_row.pack(fill="x")
        self.summary_card_title_vars: Dict[str, tk.StringVar] = {}
        self.summary_card_value_vars: Dict[str, tk.StringVar] = {}
        summary_card_meta = (
            ("summary_mode_card_var", self.ui_colors["accent_soft"], "#f3fffc", "模式", "◫"),
            ("summary_temp_card_var", self.ui_colors["info_soft"], "#f4f9ff", "温度", "☼"),
            ("summary_gas_card_var", "#ede9fe", "#f7f4ff", "气点", "◌"),
            ("summary_cfg_card_var", self.ui_colors["warn_soft"], "#fffaf2", "配置", "⚙"),
        )
        for idx, (var_name, accent, surface, badge_text, icon_text) in enumerate(
            summary_card_meta
        ):
            shell = tk.Frame(
                cards_row,
                bg="#e8f0fa",
                padx=1,
                pady=1,
            )
            row = idx // 2
            col = idx % 2
            shell.grid(
                row=row,
                column=col,
                sticky="nsew",
                padx=(0, self.ui_metrics["section_gap_x"]) if col == 0 else 0,
                pady=(0, self.ui_metrics["section_gap_y"]) if row == 0 else 0,
            )
            cards_row.grid_columnconfigure(col, weight=1)
            card = tk.Frame(
                shell,
                bg=surface,
                highlightbackground="#e1eaf4",
                highlightthickness=1,
                padx=self.ui_metrics["card_pad_x"] + 4,
                pady=self.ui_metrics["card_pad_y"] + 4,
            )
            card.pack(fill="both", expand=True)
            tk.Frame(card, bg=accent, height=self.ui_metrics["stripe_h"]).pack(fill="x", pady=(0, 4))
            badge_row = tk.Frame(card, bg=surface)
            badge_row.pack(fill="x")
            tk.Label(
                badge_row,
                text=icon_text,
                bg=surface,
                fg=self.ui_colors["hero_chip_text"],
                padx=0,
                pady=0,
                font=self.ui_metrics["icon_font"],
                width=2,
            ).pack(side="left", padx=(0, 8))
            tk.Label(
                badge_row,
                text=badge_text,
                bg=accent,
                fg=self.ui_colors["hero_chip_text"],
                padx=7,
                pady=1,
                font=self.ui_metrics["badge_font"],
            ).pack(side="left")
            title_var = tk.StringVar()
            value_var = tk.StringVar()
            self.summary_card_title_vars[var_name] = title_var
            self.summary_card_value_vars[var_name] = value_var
            tk.Label(
                card,
                textvariable=title_var,
                anchor="w",
                bg=surface,
                fg=self.ui_colors["muted"],
                font=self.ui_metrics["card_label_font"],
            ).pack(fill="x", pady=(4, 1))
            tk.Label(
                card,
                textvariable=value_var,
                anchor="w",
                justify="left",
                bg=surface,
                fg=self.ui_colors["text"],
                font=self.ui_metrics["summary_value_font"],
            ).pack(fill="x")
        story_panel = tk.Frame(
            summary_box,
            bg="#f8fbff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=6,
        )
        story_panel.pack(fill="x", padx=6, pady=(0, 2))
        story_header = tk.Frame(story_panel, bg=self.ui_colors["value_panel"])
        story_header.pack(fill="x", pady=(0, 2))
        tk.Label(
            story_header,
            text="本次运行概览",
            bg=self.ui_colors["value_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 10, "bold"),
        ).pack(side="left")
        self.startup_summary_var = tk.StringVar(value="测量模式：先水后气 | 温度：全部温度点 | 气点：-- | 压力点：全部压力点")
        tk.Label(
            story_panel,
            textvariable=self.startup_summary_var,
            anchor="w",
            justify="left",
            wraplength=520,
            bg="#f8fbff",
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 9),
        ).pack(fill="x")
        self.current_selection_var = tk.StringVar(value="当前选择：先水后气 | 温度：全部温度点 | 气点：-- | 压力点：全部压力点")
        self.current_selection_summary_label = tk.Label(
            story_panel,
            textvariable=self.current_selection_var,
            anchor="w",
            justify="left",
            wraplength=520,
            bg="#f8fbff",
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 9),
        )
        self.current_selection_summary_label.pack(fill="x", pady=(3, 0))
        self.summary_valve_hint_label = tk.Label(
            story_panel,
            textvariable=self.valve_hint_var,
            anchor="w",
            justify="left",
            wraplength=520,
            bg="#edf8ff",
            fg=self.ui_colors["text"],
            padx=12,
            pady=8,
            font=("Microsoft YaHei UI", 9, "bold"),
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
        )
        self.summary_valve_hint_label.pack(fill="x", pady=(6, 0))
        self.current_selection_summary_label.pack_forget()
        flow_panel = tk.Frame(
            summary_box,
            bg="#f7fbff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=6,
        )
        flow_panel.pack(fill="both", expand=True, padx=6, pady=(0, 4))
        flow_header = tk.Frame(flow_panel, bg=self.ui_colors["soft_layer"])
        flow_header.pack(fill="x", pady=(0, 2))
        tk.Label(
            flow_header,
            text="流程说明",
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 10, "bold"),
        ).pack(side="left")
        self.flow_help_summary_var = tk.StringVar(value="")
        self.flow_help_summary_label = tk.Label(
            flow_panel,
            textvariable=self.flow_help_summary_var,
            anchor="w",
            justify="left",
            wraplength=520,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["accent_dark"],
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        self.flow_help_summary_label.pack(fill="x")
        flow_meta = tk.Frame(flow_panel, bg=self.ui_colors["soft_layer"])
        flow_meta.pack(fill="x", pady=(6, 2))
        self.flow_readiness_meta = tk.Label(
            flow_meta,
            textvariable=self.start_readiness_var,
            anchor="w",
            bg="#f3f8ff",
            fg=self.ui_colors["text"],
            padx=12,
            pady=6,
            font=("Microsoft YaHei UI", 9, "bold"),
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
        )
        self.flow_readiness_meta.pack(fill="x")
        self.flow_cfg_meta = tk.Label(
            flow_meta,
            textvariable=self.runtime_config_diff_var,
            anchor="w",
            bg=self.ui_colors["value_panel"],
            fg=self.ui_colors["muted"],
            padx=10,
            pady=4,
            font=("Microsoft YaHei UI", 9),
        )
        self.flow_cfg_meta.pack(fill="x", pady=(5, 0))
        self.flow_selector_meta = tk.Label(
            flow_meta,
            textvariable=self.selector_hint_var,
            anchor="w",
            justify="left",
            wraplength=520,
            bg=self.ui_colors["value_panel"],
            fg=self.ui_colors["muted"],
            padx=10,
            pady=5,
            font=("Microsoft YaHei UI", 9),
        )
        self.flow_selector_meta.pack(fill="x", pady=(5, 0))
        self.flow_help_expanded = tk.BooleanVar(value=False)
        self.flow_help_toggle_button = ttk.Button(flow_header, text="展开详细版", command=self._toggle_flow_help, style="Subtle.TButton")
        self.flow_help_toggle_button.pack(side="right")
        summary_actions = ttk.Frame(flow_panel)
        summary_actions.pack(fill="x", pady=(4, 1))
        self.refresh_button = ttk.Button(summary_actions, text="立即刷新", command=self._manual_refresh, style="HomeTool.TButton")
        self.refresh_button.pack(side="left")
        self.export_summary_button = ttk.Button(summary_actions, text="导出本次运行摘要", command=self._export_run_summary, style="HomeTool.TButton")
        self.export_summary_button.pack(side="right")
        self.flow_help_var = tk.StringVar(value="")
        self.flow_help_tooltip = _Tooltip(self.flow_help_summary_label, lambda: self.flow_help_var.get())
        self.flow_help_label = tk.Label(
            flow_panel,
            textvariable=self.flow_help_var,
            justify="left",
            anchor="w",
            padx=0,
            pady=6,
            wraplength=520,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
        )
        self.flow_help_label.pack(fill="x", pady=(4, 0))
        self.flow_cfg_meta.pack_forget()
        self.flow_selector_meta.pack_forget()
        self.flow_help_label.pack_forget()

        self.status_var = tk.StringVar(value="空闲")
        status_box = ttk.LabelFrame(status_inner, text="运行状态与进度", style="HomeBoard.TLabelframe")
        status_box.pack(fill="both", expand=True, padx=0, pady=(0, 0))
        self.status_box = status_box
        self.summary_var = tk.StringVar(value="执行摘要：未开始")
        self.stage_var = tk.StringVar(value="当前阶段：空闲")
        self.target_var = tk.StringVar(value="当前点位：--")
        summary_strip = tk.Frame(
            status_box,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=10,
            pady=6,
        )
        summary_strip.pack(fill="x", padx=6, pady=(0, 2))
        tk.Label(
            summary_strip,
            textvariable=self.summary_var,
            anchor="w",
            justify="left",
            wraplength=520,
            bg=self.ui_colors["value_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 10, "bold"),
        ).pack(fill="x")
        status_split = tk.Frame(status_box, bg=self.ui_colors["bg"])
        status_split.pack(fill="x", padx=6, pady=(0, 0))
        status_split.grid_columnconfigure(0, weight=3)
        status_split.grid_columnconfigure(1, weight=6)
        status_split.grid_columnconfigure(2, weight=4)

        stage_shell = tk.Frame(
            status_split,
            bg=self.ui_colors["shadow"],
            padx=self.ui_metrics["shell_pad"],
            pady=self.ui_metrics["shell_pad"],
        )
        stage_shell.grid(row=0, column=0, sticky="nsew", padx=(0, self.ui_metrics["section_gap_x"]))
        stage_card = tk.Frame(
            stage_shell,
            bg=self.ui_colors["card"],
            highlightthickness=0,
            padx=3,
            pady=2,
        )
        stage_card.pack(fill="both", expand=True)
        tk.Frame(stage_card, bg=self.ui_colors["accent_soft"], height=self.ui_metrics["stripe_h"]).pack(fill="x", pady=(0, 3))
        stage_title_row = tk.Frame(stage_card, bg=self.ui_colors["card"])
        stage_title_row.pack(fill="x")
        tk.Label(stage_title_row, text="◫", bg=self.ui_colors["card"], fg=self.ui_colors["hero_chip_text"], font=self.ui_metrics["icon_font"], width=2).pack(side="left")
        tk.Label(stage_title_row, text="当前步骤", bg=self.ui_colors["card"], fg=self.ui_colors["muted"], font=self.ui_metrics["title_font"]).pack(side="left")
        stage_panel = tk.Frame(
            stage_card,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=8,
            pady=3,
        )
        stage_panel.pack(fill="x", pady=(3, 1))
        stage_head = tk.Frame(stage_panel, bg=self.ui_colors["value_panel"])
        stage_head.pack(fill="x", pady=(0, 2))
        self.stage_light = tk.Canvas(stage_head, width=18, height=18, bg=self.ui_colors["value_panel"], highlightthickness=0)
        self.stage_light.pack(side="left", padx=(0, 6))
        self.stage_light_item = self.stage_light.create_oval(2, 2, 16, 16, fill="#94a3b8", outline="")
        self.stage_icon_label = tk.Label(
            stage_head,
            textvariable=self.stage_icon_var,
            font=("Microsoft YaHei UI", 12, "bold"),
            bg=self.ui_colors["value_panel"],
            fg=self.ui_colors["accent_dark"],
            width=2,
        )
        self.stage_icon_label.pack(side="left")
        self.stage_banner = tk.Label(
            stage_head,
            textvariable=self.stage_var,
            anchor="w",
            font=("Microsoft YaHei UI", 10, "bold"),
            padx=8,
            pady=2,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.stage_banner.pack(side="left", fill="x", expand=True)
        self.status_banner = tk.Label(
            stage_panel,
            textvariable=self.status_var,
            anchor="w",
            padx=8,
            pady=3,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.status_banner.pack(fill="x", pady=(1, 0))
        stage_detail = tk.Frame(stage_panel, bg=self.ui_colors["value_panel"])
        stage_detail.pack(fill="x", pady=(4, 0))
        self.start_readiness_label = tk.Label(
            stage_detail,
            textvariable=self.start_readiness_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.start_readiness_label.pack(fill="x", pady=(4, 0))

        point_shell = tk.Frame(
            status_split,
            bg=self.ui_colors["shadow"],
            padx=self.ui_metrics["shell_pad"],
            pady=self.ui_metrics["shell_pad"],
        )
        point_shell.grid(row=0, column=1, sticky="nsew", padx=(0, self.ui_metrics["section_gap_x"]))
        point_card = tk.Frame(
            point_shell,
            bg=self.ui_colors["card"],
            highlightthickness=0,
            padx=3,
            pady=2,
        )
        point_card.pack(fill="both", expand=True)
        tk.Frame(point_card, bg=self.ui_colors["info_soft"], height=self.ui_metrics["stripe_h"]).pack(fill="x", pady=(0, 3))
        point_title_row = tk.Frame(point_card, bg=self.ui_colors["card"])
        point_title_row.pack(fill="x")
        tk.Label(point_title_row, text="◌", bg=self.ui_colors["card"], fg="#4f46e5", font=self.ui_metrics["icon_font"], width=2).pack(side="left")
        tk.Label(point_title_row, text="当前点位", bg=self.ui_colors["card"], fg=self.ui_colors["muted"], font=self.ui_metrics["title_font"]).pack(side="left")
        point_panel = tk.Frame(
            point_card,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=8,
            pady=3,
        )
        point_panel.pack(fill="x", pady=(3, 1))
        point_row = tk.Frame(point_panel, bg=self.ui_colors["value_panel"])
        point_row.pack(fill="x", pady=(0, 2))
        self.point_banner = tk.Label(
            point_row,
            textvariable=self.target_var,
            anchor="w",
            font=("Microsoft YaHei UI", 11, "bold"),
            padx=8,
            pady=4,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.point_banner.pack(side="left", fill="x", expand=True)
        self.copy_point_button = ttk.Button(point_row, text="复制点位", command=self._copy_current_point, style="HomeTool.TButton")
        self.copy_point_button.pack(side="right", padx=(6, 0))

        detail_row = tk.Frame(point_panel, bg=self.ui_colors["value_panel"])
        detail_row.pack(fill="x", pady=(2, 0))
        self.gas_point_banner = tk.Label(
            detail_row,
            textvariable=self.current_target_ppm_var,
            anchor="w",
            padx=10,
            pady=4,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.gas_point_banner.pack(side="left", fill="x", expand=True)
        self.route_banner = tk.Label(
            detail_row,
            textvariable=self.current_route_group_detail_var,
            anchor="w",
            font=("Microsoft YaHei UI", 9, "bold"),
            padx=10,
            pady=4,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.route_banner.pack(side="left", padx=(6, 0))

        pressure_shell = tk.Frame(
            status_split,
            bg=self.ui_colors["shadow"],
            padx=self.ui_metrics["shell_pad"],
            pady=self.ui_metrics["shell_pad"],
        )
        pressure_shell.grid(row=0, column=2, sticky="nsew")
        pressure_card = tk.Frame(
            pressure_shell,
            bg=self.ui_colors["card"],
            highlightthickness=0,
            padx=3,
            pady=2,
        )
        pressure_card.pack(fill="both", expand=True)
        tk.Frame(pressure_card, bg=self.ui_colors["warn_soft"], height=self.ui_metrics["stripe_h"]).pack(fill="x", pady=(0, 3))
        pressure_title_row = tk.Frame(pressure_card, bg=self.ui_colors["card"])
        pressure_title_row.pack(fill="x")
        tk.Label(
            pressure_title_row,
            text="◍",
            bg=self.ui_colors["card"],
            fg="#b45309",
            font=self.ui_metrics["icon_font"],
            width=2,
        ).pack(side="left")
        tk.Label(
            pressure_title_row,
            text="当前压力与采样",
            bg=self.ui_colors["card"],
            fg=self.ui_colors["muted"],
            font=self.ui_metrics["title_font"],
        ).pack(side="left")
        pressure_panel = tk.Frame(
            pressure_card,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=8,
            pady=4,
        )
        pressure_panel.pack(fill="x", pady=(3, 1))
        pressure_header = tk.Frame(pressure_panel, bg=self.ui_colors["value_panel"])
        pressure_header.pack(fill="x", pady=(0, 4))
        self.data_freshness_label = tk.Label(
            pressure_header,
            textvariable=self.data_freshness_var,
            anchor="e",
            padx=8,
            pady=3,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
            font=("Microsoft YaHei UI", 8, "bold"),
        )
        self.data_freshness_label.pack(side="right")
        pressure_values = tk.Frame(pressure_panel, bg=self.ui_colors["value_panel"])
        pressure_values.pack(fill="x")
        pressure_values.grid_columnconfigure(0, weight=1)
        pressure_values.grid_columnconfigure(1, weight=1)
        self.pressure_point_banner = tk.Label(
            pressure_values,
            textvariable=self.current_pressure_point_var,
            anchor="w",
            font=("Microsoft YaHei UI", 10, "bold"),
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            padx=8,
            pady=5,
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.pressure_point_banner.grid(row=0, column=0, sticky="ew", padx=(0, 6), pady=(0, 6))
        self.pressure_live_label = tk.Label(
            pressure_values,
            textvariable=self.current_pressure_live_var,
            anchor="w",
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            padx=8,
            pady=5,
            font=("Microsoft YaHei UI", 11, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.pressure_live_label.grid(row=0, column=1, sticky="ew", pady=(0, 6))
        self.pressure_stability_label = tk.Label(
            pressure_values,
            textvariable=self.current_pressure_stability_var,
            anchor="w",
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            padx=8,
            pady=5,
            font=("Microsoft YaHei UI", 9, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.pressure_stability_label.grid(row=1, column=0, sticky="ew", padx=(0, 6))
        self.pressure_reapply_label = tk.Label(
            pressure_values,
            textvariable=self.current_pressure_reapply_var,
            anchor="w",
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            padx=8,
            pady=5,
            font=("Microsoft YaHei UI", 9),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.pressure_reapply_label.grid(row=1, column=1, sticky="ew")
        self.sample_progress_label = tk.Label(
            pressure_panel,
            textvariable=self.sample_progress_var,
            anchor="w",
            padx=8,
            pady=5,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["accent_dark"],
            font=("Microsoft YaHei UI", 9, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.sample_progress_label.pack(fill="x", pady=(6, 0))

        self.progress_summary_var = tk.StringVar(value="进度：--")
        self.progress_detail_var = tk.StringVar(value="已完成：0 | 已跳过：0 | 总点数：--")
        self.route_group_var = tk.StringVar(value="当前气路组别：--")
        self.last_issue_var = tk.StringVar(value="最近一次异常：--")
        self.progress_var = tk.DoubleVar(value=0.0)
        progress_box = tk.Frame(status_box, bg=self.ui_colors["bg"])
        progress_box.pack(fill="x", padx=6, pady=(6, 0))
        progress_box.grid_columnconfigure(0, weight=4)
        progress_box.grid_columnconfigure(1, weight=5)
        progress_box.grid_columnconfigure(2, weight=4)

        progress_stats_shell = tk.Frame(
            progress_box,
            bg=self.ui_colors["shadow"],
            padx=self.ui_metrics["shell_pad"],
            pady=self.ui_metrics["shell_pad"],
        )
        progress_stats_shell.grid(row=0, column=0, sticky="nsew", padx=(0, self.ui_metrics["section_gap_x"]))
        progress_stats_card = tk.Frame(
            progress_stats_shell,
            bg=self.ui_colors["card"],
            highlightthickness=0,
            padx=3,
            pady=2,
        )
        progress_stats_card.pack(fill="both", expand=True)
        tk.Frame(progress_stats_card, bg=self.ui_colors["accent_soft"], height=self.ui_metrics["stripe_h"]).pack(fill="x", pady=(0, 3))
        progress_stats_head = tk.Frame(progress_stats_card, bg=self.ui_colors["card"])
        progress_stats_head.pack(fill="x")
        tk.Label(progress_stats_head, text="◴", bg=self.ui_colors["card"], fg=self.ui_colors["hero_chip_text"], font=self.ui_metrics["icon_font"], width=2).pack(side="left")
        tk.Label(progress_stats_head, text="运行统计", bg=self.ui_colors["card"], fg=self.ui_colors["muted"], font=self.ui_metrics["title_font"]).pack(side="left")
        progress_stats_panel = tk.Frame(
            progress_stats_card,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=8,
            pady=5,
        )
        progress_stats_panel.pack(fill="x", pady=(4, 1))
        stats_row = tk.Frame(progress_stats_panel, bg=self.ui_colors["value_panel"])
        stats_row.pack(fill="x", pady=(0, 4))
        self.stat_completed_var = tk.StringVar(value="成功 0")
        self.stat_skipped_var = tk.StringVar(value="跳过 0")
        self.stat_failed_var = tk.StringVar(value="失败 0")
        self.stat_completed_label = tk.Label(stats_row, textvariable=self.stat_completed_var, padx=9, pady=4, font=("Microsoft YaHei UI", 8, "bold"), bg=self.ui_colors["chip_panel"], fg=self.ui_colors["text"], highlightthickness=1, highlightbackground=self.ui_colors["chip_panel"])
        self.stat_completed_label.pack(side="left", padx=(0, 6))
        self.stat_skipped_label = tk.Label(stats_row, textvariable=self.stat_skipped_var, padx=9, pady=4, font=("Microsoft YaHei UI", 8, "bold"), bg=self.ui_colors["chip_panel"], fg=self.ui_colors["text"], highlightthickness=1, highlightbackground=self.ui_colors["chip_panel"])
        self.stat_skipped_label.pack(side="left", padx=(0, 6))
        self.stat_failed_label = tk.Label(stats_row, textvariable=self.stat_failed_var, padx=9, pady=4, font=("Microsoft YaHei UI", 8, "bold"), bg=self.ui_colors["chip_panel"], fg=self.ui_colors["text"], highlightthickness=1, highlightbackground=self.ui_colors["chip_panel"])
        self.stat_failed_label.pack(side="left")
        self.progress_bar = ttk.Progressbar(
            progress_stats_panel,
            orient="horizontal",
            mode="determinate",
            maximum=100.0,
            variable=self.progress_var,
        )
        self.progress_bar.pack(fill="x", pady=(0, 0))

        progress_detail_shell = tk.Frame(
            progress_box,
            bg=self.ui_colors["shadow"],
            padx=self.ui_metrics["shell_pad"],
            pady=self.ui_metrics["shell_pad"],
        )
        progress_detail_shell.grid(row=0, column=1, sticky="nsew", padx=(0, self.ui_metrics["section_gap_x"]))
        progress_detail_card = tk.Frame(
            progress_detail_shell,
            bg=self.ui_colors["card"],
            highlightthickness=0,
            padx=3,
            pady=2,
        )
        progress_detail_card.pack(fill="both", expand=True)
        tk.Frame(progress_detail_card, bg=self.ui_colors["info_soft"], height=self.ui_metrics["stripe_h"]).pack(fill="x", pady=(0, 3))
        progress_detail_head = tk.Frame(progress_detail_card, bg=self.ui_colors["card"])
        progress_detail_head.pack(fill="x")
        tk.Label(progress_detail_head, text="◎", bg=self.ui_colors["card"], fg="#4f46e5", font=self.ui_metrics["icon_font"], width=2).pack(side="left")
        tk.Label(progress_detail_head, text="进度概览", bg=self.ui_colors["card"], fg=self.ui_colors["muted"], font=self.ui_metrics["title_font"]).pack(side="left")
        progress_detail_panel = tk.Frame(
            progress_detail_card,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=8,
            pady=5,
        )
        progress_detail_panel.pack(fill="x", pady=(4, 1))
        self.progress_summary_label = tk.Label(
            progress_detail_panel,
            textvariable=self.progress_summary_var,
            anchor="w",
            justify="left",
            padx=8,
            pady=5,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.progress_summary_label.pack(fill="x", pady=(0, 4))
        self.progress_detail_label = tk.Label(
            progress_detail_panel,
            textvariable=self.progress_detail_var,
            anchor="w",
            justify="left",
            padx=8,
            pady=5,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.progress_detail_label.pack(fill="x", pady=(0, 4))
        self.route_group_label = tk.Label(
            progress_detail_panel,
            textvariable=self.route_group_var,
            anchor="w",
            justify="left",
            padx=8,
            pady=5,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.route_group_label.pack(fill="x")

        issue_shell = tk.Frame(
            progress_box,
            bg=self.ui_colors["shadow"],
            padx=self.ui_metrics["shell_pad"],
            pady=self.ui_metrics["shell_pad"],
        )
        issue_shell.grid(row=0, column=2, sticky="nsew")
        issue_card = tk.Frame(
            issue_shell,
            bg=self.ui_colors["card"],
            highlightthickness=0,
            padx=3,
            pady=2,
        )
        issue_card.pack(fill="both", expand=True)
        tk.Frame(issue_card, bg=self.ui_colors["warn_soft"], height=self.ui_metrics["stripe_h"]).pack(fill="x", pady=(0, 3))
        issue_head = tk.Frame(issue_card, bg=self.ui_colors["card"])
        issue_head.pack(fill="x")
        tk.Label(issue_head, text="!", bg=self.ui_colors["card"], fg="#b45309", font=self.ui_metrics["icon_font"], width=2).pack(side="left")
        tk.Label(issue_head, text="最近异常", bg=self.ui_colors["card"], fg=self.ui_colors["muted"], font=self.ui_metrics["title_font"]).pack(side="left")
        issue_panel = tk.Frame(
            issue_card,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=8,
            pady=5,
        )
        issue_panel.pack(fill="both", expand=True, pady=(4, 1))
        issue_row = tk.Frame(issue_panel, bg=self.ui_colors["value_panel"])
        issue_row.pack(fill="both", expand=True)
        self.issue_banner = tk.Label(
            issue_row,
            textvariable=self.last_issue_var,
            anchor="w",
            padx=10,
            pady=6,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
            justify="left",
            wraplength=260,
        )
        self.issue_banner.pack(side="left", fill="x", expand=True)
        self.copy_issue_button = ttk.Button(issue_row, text="复制异常", command=self._copy_last_issue, style="HomeTool.TButton")
        self.copy_issue_button.pack(side="right", padx=(6, 0))

        status_coefficient_shell = tk.Frame(
            status_box,
            bg=self.ui_colors["shadow"],
            padx=self.ui_metrics["shell_pad"],
            pady=self.ui_metrics["shell_pad"],
        )
        status_coefficient_shell.pack(fill="x", padx=6, pady=(6, 0))
        status_coefficient_card = tk.Frame(
            status_coefficient_shell,
            bg=self.ui_colors["card"],
            highlightthickness=0,
            padx=3,
            pady=2,
        )
        status_coefficient_card.pack(fill="x", expand=True)
        tk.Frame(status_coefficient_card, bg=self.ui_colors["accent_soft"], height=self.ui_metrics["stripe_h"]).pack(fill="x", pady=(0, 3))
        status_coefficient_head = tk.Frame(status_coefficient_card, bg=self.ui_colors["card"])
        status_coefficient_head.pack(fill="x")
        tk.Label(
            status_coefficient_head,
            text="∑",
            bg=self.ui_colors["card"],
            fg=self.ui_colors["hero_chip_text"],
            font=self.ui_metrics["icon_font"],
            width=2,
        ).pack(side="left")
        tk.Label(
            status_coefficient_head,
            text="校准结果快照",
            bg=self.ui_colors["card"],
            fg=self.ui_colors["muted"],
            font=self.ui_metrics["title_font"],
        ).pack(side="left")
        ttk.Label(
            status_coefficient_head,
            textvariable=self.current_coefficient_report_name_var,
        ).pack(side="right")
        status_coefficient_panel = tk.Frame(
            status_coefficient_card,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=8,
            pady=5,
        )
        status_coefficient_panel.pack(fill="x", pady=(4, 1))
        status_coefficient_scroll = ttk.Scrollbar(status_coefficient_panel, orient="vertical")
        self.status_coefficient_text = tk.Text(
            status_coefficient_panel,
            height=10,
            yscrollcommand=status_coefficient_scroll.set,
            bg="#ffffff",
            fg=self.ui_colors["text"],
            insertbackground=self.ui_colors["text"],
            selectbackground="#dbeafe",
            selectforeground=self.ui_colors["text"],
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground="#d7e3f0",
            padx=10,
            pady=10,
            font=("Consolas", 10),
        )
        status_coefficient_scroll.configure(command=self.status_coefficient_text.yview)
        status_coefficient_scroll.pack(side="right", fill="y")
        self.status_coefficient_text.pack(side="left", fill="x", expand=True, padx=(0, 8))
        self.status_coefficient_text.configure(state="disabled")
        self._set_text_widget(self.status_coefficient_text, "当前没有可显示的气体拟合或温度补偿结果。")

        coefficient_page_shell = tk.Frame(coefficient_inner, bg=self.ui_colors["shadow"], padx=1, pady=1)
        coefficient_page_shell.pack(fill="both", expand=True, padx=0, pady=0)
        coefficient_page_box = ttk.LabelFrame(
            coefficient_page_shell,
            text="气体拟合与温度补偿",
            style="HomeBoard.TLabelframe",
        )
        coefficient_page_box.pack(fill="both", expand=True, padx=0, pady=0)
        coefficient_page_header = tk.Frame(
            coefficient_page_box,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=10,
            pady=8,
        )
        coefficient_page_header.pack(fill="x", padx=6, pady=(2, 4))
        tk.Label(
            coefficient_page_header,
            text="FIT RESULT",
            bg="#eef6ff",
            fg="#1d4f91",
            padx=10,
            pady=3,
            font=("Consolas", 8, "bold"),
        ).pack(side="left")
        ttk.Label(coefficient_page_header, textvariable=self.current_coefficient_report_name_var).pack(
            side="left",
            padx=(10, 0),
        )
        self.open_coefficient_tab_button = ttk.Button(
            coefficient_page_header,
            text="打开气体拟合报告",
            command=self._open_current_coefficient_report,
            style="HomeTool.TButton",
        )
        self.open_coefficient_tab_button.pack(side="right")
        self.apply_temperature_compensation_button = ttk.Button(
            coefficient_page_header,
            text="下发温度补偿",
            command=self._apply_current_temperature_compensation,
            style="Warn.TButton",
        )
        self.apply_temperature_compensation_button.pack(side="right", padx=(0, 6))
        self.open_temperature_compensation_button = ttk.Button(
            coefficient_page_header,
            text="打开温度补偿结果",
            command=self._open_current_temperature_compensation_report,
            style="HomeTool.TButton",
        )
        self.open_temperature_compensation_button.pack(side="right", padx=(0, 6))
        coefficient_page_meta = tk.Frame(
            coefficient_page_box,
            bg="#f8fbff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=8,
        )
        coefficient_page_meta.pack(fill="x", padx=6, pady=(0, 4))
        ttk.Label(coefficient_page_meta, textvariable=self.current_run_dir_name_var).pack(anchor="w")
        ttk.Label(coefficient_page_meta, textvariable=self.current_io_name_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(coefficient_page_meta, textvariable=self.current_temperature_compensation_name_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(coefficient_page_meta, textvariable=self.temperature_compensation_apply_status_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(
            coefficient_page_meta,
            text="这里分别显示气体分析仪校准拟合结果（CO2/H2O）和温度补偿结果，以及对应的下发命令摘要。",
            foreground=self.ui_colors["muted"],
            background="#f8fbff",
            wraplength=860,
            justify="left",
        ).pack(anchor="w", pady=(4, 0))
        coefficient_page_panel = tk.Frame(
            coefficient_page_box,
            bg="#ffffff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=8,
        )
        coefficient_page_panel.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        coefficient_page_scroll = ttk.Scrollbar(coefficient_page_panel, orient="vertical")
        self.coefficient_page_text = tk.Text(
            coefficient_page_panel,
            height=28,
            yscrollcommand=coefficient_page_scroll.set,
            bg="#ffffff",
            fg=self.ui_colors["text"],
            insertbackground=self.ui_colors["text"],
            selectbackground="#dbeafe",
            selectforeground=self.ui_colors["text"],
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground="#d7e3f0",
            padx=10,
            pady=10,
            font=("Consolas", 10),
        )
        coefficient_page_scroll.configure(command=self.coefficient_page_text.yview)
        coefficient_page_scroll.pack(side="right", fill="y")
        self.coefficient_page_text.pack(side="left", fill="both", expand=True, padx=(0, 8))
        self.coefficient_page_text.configure(state="disabled")
        self._set_text_widget(self.coefficient_page_text, "当前没有可显示的气体拟合或温度补偿结果。")

        bottom_tabs_shell = tk.Frame(bottom_shell, bg=self.ui_colors["shadow"], padx=1, pady=1)
        bottom_tabs_shell.pack(fill="both", expand=True, padx=6, pady=(0, 4))
        bottom_tabs = ttk.Notebook(bottom_tabs_shell, style="Board.TNotebook")
        bottom_tabs.pack(fill="both", expand=True)
        self.bottom_tabs = bottom_tabs
        self.bottom_tabs.bind("<<NotebookTabChanged>>", self._schedule_responsive_layout, add="+")

        device_shell = ttk.Frame(bottom_tabs)
        workbench_shell = ttk.Frame(bottom_tabs)
        ports_shell = ttk.Frame(bottom_tabs)
        bottom_tabs.add(device_shell, text="设备总览")
        bottom_tabs.add(workbench_shell, text="日志工作台")
        bottom_tabs.add(ports_shell, text="设备串口配置")
        self.device_shell = device_shell
        self.workbench_shell = workbench_shell
        self.ports_shell = ports_shell

        _device_scroll_shell, self.device_scroll_canvas, device_inner = _make_scroll_tab(
            device_shell, self.ui_colors["soft_layer"]
        )
        device_card_shell = tk.Frame(device_inner, bg=self.ui_colors["shadow"], padx=1, pady=1)
        device_card_shell.pack(fill="both", expand=True, padx=6, pady=4)
        device_box = ttk.LabelFrame(device_card_shell, text="设备与分析仪总览", style="HomeBoard.TLabelframe")
        device_box.pack(fill="both", expand=True)
        self.device_box = device_box
        device_summary_strip = tk.Frame(
            device_box,
            bg=self.ui_colors["value_panel"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            padx=8,
            pady=6,
        )
        device_summary_strip.pack(fill="x", padx=6, pady=(2, 3))
        device_summary_strip.grid_columnconfigure(0, weight=3)
        device_summary_strip.grid_columnconfigure(1, weight=6)
        device_summary_strip.grid_columnconfigure(2, weight=2)
        self.device_stage_summary_label = tk.Label(
            device_summary_strip,
            textvariable=self.stage_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.device_stage_summary_label.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        self.device_point_summary_label = tk.Label(
            device_summary_strip,
            textvariable=self.target_var,
            anchor="w",
            padx=8,
            pady=4,
            bg=self.ui_colors["soft_layer"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["soft_layer"],
        )
        self.device_point_summary_label.grid(row=0, column=1, sticky="ew", padx=(0, 6))
        self.device_refresh_summary_label = tk.Label(
            device_summary_strip,
            textvariable=self.data_freshness_var,
            anchor="center",
            padx=8,
            pady=4,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.device_refresh_summary_label.grid(row=0, column=2, sticky="ew")
        device_intro = tk.Frame(
            device_box,
            bg="#f8fbff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=9,
            pady=5,
        )
        device_intro.pack(fill="x", padx=6, pady=(2, 4))
        tk.Frame(device_intro, bg="#d8f8f3", height=3).pack(fill="x", pady=(0, 5))
        device_intro_head = tk.Frame(device_intro, bg="#f8fbff")
        device_intro_head.pack(fill="x")
        tk.Label(
            device_intro_head,
            text="LIVE DEVICE BOARD",
            bg="#edfdfb",
            fg=self.ui_colors["accent_dark"],
            padx=10,
            pady=3,
            font=("Consolas", 8, "bold"),
        ).pack(side="left")
        for text, bg, fg in (
            ("5 台设备", "#e8f2ff", "#1d4f91"),
            ("实时状态", "#f5f3ff", "#6d28d9"),
        ):
            tk.Label(
                device_intro_head,
                text=text,
                bg=bg,
                fg=fg,
                padx=8,
                pady=3,
                font=("Microsoft YaHei UI", 8, "bold"),
            ).pack(side="left", padx=(8, 0))
        tk.Label(
            device_intro_head,
            text="AUTO REFRESH",
            bg="#fff4e8",
            fg="#c2410c",
            padx=10,
            pady=3,
            font=("Consolas", 8, "bold"),
        ).pack(side="right")
        device_intro.pack_forget()
        self.device_vars = {
            "pace": tk.StringVar(value="压力控制器：--"),
            "gauge": tk.StringVar(value="数字气压计：--"),
            "chamber": tk.StringVar(value="温度箱：--"),
            "hgen": tk.StringVar(value="湿度发生器：--"),
            "dewpoint": tk.StringVar(value="露点仪：--"),
        }
        self.device_display_primary_vars = {
            "pace": tk.StringVar(value="--"),
            "gauge": tk.StringVar(value="--"),
            "chamber": tk.StringVar(value="--"),
            "hgen": tk.StringVar(value="--"),
            "dewpoint": tk.StringVar(value="--"),
        }
        self.device_display_secondary_vars = {
            "pace": tk.StringVar(value="压力控制器"),
            "gauge": tk.StringVar(value="数字气压计"),
            "chamber": tk.StringVar(value="温度箱"),
            "hgen": tk.StringVar(value="湿度发生器"),
            "dewpoint": tk.StringVar(value="露点仪"),
        }
        self.device_state_vars = {
            "pace": tk.StringVar(value="状态：--"),
            "gauge": tk.StringVar(value="状态：--"),
            "chamber": tk.StringVar(value="状态：--"),
            "hgen": tk.StringVar(value="状态：--"),
            "dewpoint": tk.StringVar(value="状态：--"),
        }
        self.device_online_vars = {
            "pace": tk.StringVar(value="○ 掉线"),
            "gauge": tk.StringVar(value="○ 掉线"),
            "chamber": tk.StringVar(value="○ 掉线"),
            "hgen": tk.StringVar(value="○ 掉线"),
            "dewpoint": tk.StringVar(value="○ 掉线"),
        }
        self.device_issue_vars = {
            "pace": tk.StringVar(value="异常摘要：无"),
            "gauge": tk.StringVar(value="异常摘要：无"),
            "chamber": tk.StringVar(value="异常摘要：无"),
            "hgen": tk.StringVar(value="异常摘要：无"),
            "dewpoint": tk.StringVar(value="异常摘要：无"),
        }
        self.device_issue_time_vars = {
            "pace": tk.StringVar(value="异常时间：--"),
            "gauge": tk.StringVar(value="异常时间：--"),
            "chamber": tk.StringVar(value="异常时间：--"),
            "hgen": tk.StringVar(value="异常时间：--"),
            "dewpoint": tk.StringVar(value="异常时间：--"),
        }
        self.device_update_vars = {
            "pace": tk.StringVar(value="更新：--"),
            "gauge": tk.StringVar(value="更新：--"),
            "chamber": tk.StringVar(value="更新：--"),
            "hgen": tk.StringVar(value="更新：--"),
            "dewpoint": tk.StringVar(value="更新：--"),
        }
        self.device_update_labels: Dict[str, tk.Label] = {}
        self.device_trend_vars = {
            "pace": tk.StringVar(value="30秒变化：--"),
            "gauge": tk.StringVar(value="30秒变化：--"),
            "chamber": tk.StringVar(value="30秒变化：--"),
            "hgen": tk.StringVar(value="30秒变化：--"),
            "dewpoint": tk.StringVar(value="30秒变化：--"),
        }
        self.device_trend_detail_vars = {
            "pace": tk.StringVar(value="最近30秒：无数据"),
            "gauge": tk.StringVar(value="最近30秒：无数据"),
            "chamber": tk.StringVar(value="最近30秒：无数据"),
            "hgen": tk.StringVar(value="最近30秒：无数据"),
            "dewpoint": tk.StringVar(value="最近30秒：无数据"),
        }
        titles = {
            "pace": "压力控制器",
            "gauge": "数字气压计",
            "chamber": "温度箱",
            "hgen": "湿度发生器",
            "dewpoint": "露点仪",
        }
        device_badges = {
            "pace": ("压", self.ui_colors["accent_soft"], "#115e59"),
            "gauge": ("气", self.ui_colors["info_soft"], "#1d4ed8"),
            "chamber": ("温", "#ede9fe", "#6d28d9"),
            "hgen": ("湿", self.ui_colors["warn_soft"], "#92400e"),
            "dewpoint": ("露", self.ui_colors["danger_soft"], "#991b1b"),
        }
        device_surfaces = {
            "pace": "#f3fffd",
            "gauge": "#f4f8ff",
            "chamber": "#f7f5ff",
            "hgen": "#fff9f0",
            "dewpoint": "#fff4f7",
        }
        accent_map = {
            "pace": self.ui_colors["accent_soft"],
            "gauge": self.ui_colors["info_soft"],
            "chamber": "#ede9fe",
            "hgen": self.ui_colors["warn_soft"],
            "dewpoint": self.ui_colors["danger_soft"],
        }
        device_icons = {
            "pace": "◌",
            "gauge": "◍",
            "chamber": "☼",
            "hgen": "☁",
            "dewpoint": "◔",
        }
        grid = tk.Frame(device_box, bg=self.ui_colors["soft_layer"])
        grid.pack(fill="both", expand=True, padx=6, pady=(0, 2))
        self.device_grid = grid
        self.device_order = ("pace", "gauge", "chamber", "hgen", "dewpoint")
        self.device_shells: Dict[str, tk.Frame] = {}
        self.device_issue_boxes: Dict[str, tk.Frame] = {}
        device_order = ("pace", "gauge", "chamber", "hgen", "dewpoint")
        for idx, key in enumerate(device_order):
            row = idx // 3
            col = idx % 3
            shell = tk.Frame(
                grid,
                bg=accent_map[key],
                padx=self.ui_metrics["shell_pad"],
                pady=self.ui_metrics["shell_pad"],
            )
            shell.grid(
                row=row,
                column=col,
                sticky="nsew",
                padx=(0, self.ui_metrics["section_gap_x"]) if col < 2 else 0,
                pady=(0, self.ui_metrics["section_gap_y"]) if row == 0 else 0,
            )
            grid.grid_columnconfigure(col, weight=1)
            self.device_shells[key] = shell
            card = tk.Frame(
                shell,
                bg=device_surfaces[key],
                highlightbackground=self.ui_colors["divider"],
                highlightthickness=1,
                padx=4,
                pady=4,
            )
            card.pack(fill="both", expand=True)
            title_row = tk.Frame(card, bg=device_surfaces[key])
            title_row.pack(fill="x", pady=(0, 1))
            badge_text, badge_bg, badge_fg = device_badges[key]
            tk.Label(
                title_row,
                text=device_icons[key],
                bg=device_surfaces[key],
                fg=badge_fg,
                padx=0,
                pady=0,
                font=self.ui_metrics["icon_font"],
                width=2,
            ).pack(side="left", padx=(0, 6))
            tk.Label(
                title_row,
                text=badge_text,
                bg=badge_bg,
                fg=badge_fg,
                padx=7,
                pady=3,
                font=self.ui_metrics["badge_font"],
            ).pack(side="left", padx=(0, 8))
            tk.Label(
                title_row,
                text=titles[key],
                font=("Microsoft YaHei UI", 8, "bold"),
                bg=device_surfaces[key],
                fg=self.ui_colors["text"],
            ).pack(anchor="w", side="left")
            online_label = tk.Label(
                title_row,
                textvariable=self.device_online_vars[key],
                anchor="e",
                padx=8,
                pady=2,
                bg=badge_bg,
                fg=self.ui_colors["muted"],
                font=("Microsoft YaHei UI", 7, "bold"),
                highlightthickness=1,
                highlightbackground=badge_bg,
            )
            online_label.pack(side="right")
            self.device_online_labels[key] = online_label
            _Tooltip(online_label, lambda k=key: self.device_update_vars[k].get())
            value_panel = tk.Frame(
                card,
                bg="#ffffff",
                highlightbackground=self.ui_colors["divider"],
                highlightthickness=1,
                padx=8,
                pady=4,
            )
            value_panel.pack(fill="x", pady=(3, 1))
            value_panel.grid_columnconfigure(0, weight=5)
            value_panel.grid_columnconfigure(1, weight=4)
            value_head = tk.Frame(value_panel, bg="#ffffff")
            value_head.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 4))
            tk.Label(
                value_head,
                text="实时读数",
                bg="#ffffff",
                fg=self.ui_colors["muted"],
                font=self.ui_metrics["card_label_font"],
            ).pack(side="left")
            primary_panel = tk.Frame(
                value_panel,
                bg="#ffffff",
                highlightbackground=self.ui_colors["divider"],
                highlightthickness=1,
                padx=6,
                pady=5,
            )
            primary_panel.grid(row=1, column=0, sticky="nsew", padx=(0, 5))
            tk.Label(
                primary_panel,
                textvariable=self.device_display_primary_vars[key],
                bg="#ffffff",
                fg=self.ui_colors["text"],
                anchor="w",
                font=("Consolas", 10, "bold"),
            ).pack(anchor="w", fill="x")
            secondary_panel = tk.Frame(
                value_panel,
                bg=device_surfaces[key],
                highlightbackground=self.ui_colors["divider"],
                highlightthickness=1,
                padx=6,
                pady=4,
            )
            secondary_panel.grid(row=1, column=1, sticky="nsew")
            tk.Label(
                secondary_panel,
                textvariable=self.device_display_secondary_vars[key],
                bg=device_surfaces[key],
                fg=self.ui_colors["text"],
                justify="left",
                wraplength=132,
                anchor="w",
                font=("Microsoft YaHei UI", 7),
            ).pack(anchor="w", fill="x")
            state_label = tk.Label(
                secondary_panel,
                textvariable=self.device_state_vars[key],
                anchor="w",
                padx=8,
                pady=3,
                bg=device_surfaces[key],
                font=("Microsoft YaHei UI", 7, "bold"),
                highlightthickness=1,
                highlightbackground=device_surfaces[key],
            )
            state_label.pack(fill="x", pady=(4, 0))
            state_label.pack_configure(pady=(3, 0))
            self.device_state_labels[key] = state_label
            trend_panel = tk.Frame(
                card,
                bg="#ffffff",
                highlightbackground=self.ui_colors["divider"],
                highlightthickness=1,
                padx=5,
                pady=1,
            )
            trend_panel.pack(fill="x", pady=(1, 1))
            trend_canvas = tk.Canvas(trend_panel, height=12, bg="#ffffff", highlightthickness=0)
            trend_canvas.pack(fill="x", pady=(1, 1))
            self.device_trend_canvases[key] = trend_canvas
            _Tooltip(trend_canvas, lambda k=key: self.device_trend_detail_vars[k].get())
            trend_label = tk.Label(
                trend_panel,
                textvariable=self.device_trend_vars[key],
                bg="#ffffff",
                fg=self.ui_colors["muted"],
                anchor="w",
                padx=8,
                pady=1,
                font=("Microsoft YaHei UI", 7),
                highlightthickness=1,
                highlightbackground="#ffffff",
            )
            trend_label.pack(fill="x")
            self.device_trend_labels[key] = trend_label
            issue_box = tk.Frame(
                card,
                bg="#ffffff",
                highlightbackground=self.ui_colors["divider"],
                highlightthickness=1,
                padx=7,
                pady=4,
            )
            issue_box.pack(fill="x", pady=(1, 0))
            self.device_issue_boxes[key] = issue_box
            issue_label = tk.Label(
                issue_box,
                textvariable=self.device_issue_vars[key],
                bg="#ffffff",
                fg=self.ui_colors["muted"],
                anchor="w",
                wraplength=180,
                justify="left",
                padx=8,
                pady=2,
                font=("Microsoft YaHei UI", 7),
                highlightthickness=1,
                highlightbackground="#ffffff",
            )
            issue_label.pack(fill="x", pady=(0, 0))
            self.device_issue_labels[key] = issue_label
            issue_time_label = tk.Label(
                issue_box,
                textvariable=self.device_issue_time_vars[key],
                bg="#ffffff",
                fg=self.ui_colors["muted"],
                anchor="w",
                padx=8,
                pady=1,
                font=("Microsoft YaHei UI", 7),
                highlightthickness=1,
                highlightbackground="#ffffff",
            )
            issue_time_label.pack(fill="x", pady=(2, 0))
            self.device_issue_time_labels[key] = issue_time_label
            issue_box.pack_forget()
            footer = tk.Frame(card, bg=device_surfaces[key])
            footer.pack(fill="x", pady=(1, 0))
            update_label = tk.Label(
                footer,
                textvariable=self.device_update_vars[key],
                bg=device_surfaces[key],
                fg=self.ui_colors["muted"],
                font=("Microsoft YaHei UI", 7),
            )
            update_label.pack(side="left", padx=(2, 0))
            self.device_update_labels[key] = update_label

        analyzer_shell = tk.Frame(device_box, bg=self.ui_colors["soft_layer"])
        analyzer_shell.pack(fill="x", padx=6, pady=(3, 2))
        analyzer_head = tk.Frame(
            analyzer_shell,
            bg="#f8fbff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=8,
            pady=4,
        )
        analyzer_head.pack(fill="x", pady=(0, 3))
        tk.Label(
            analyzer_head,
            text="8 台气体分析仪实时快照（mode2）",
            bg="#f8fbff",
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 10, "bold"),
        ).pack(side="left")
        analyzer_meta = tk.Frame(analyzer_head, bg="#f8fbff")
        analyzer_meta.pack(side="right")
        self.analyzer_summary_label = tk.Label(
            analyzer_meta,
            textvariable=self.analyzer_summary_var,
            anchor="e",
            padx=8,
            pady=3,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8, "bold"),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.analyzer_summary_label.pack(side="right")
        self.analyzer_update_summary_label = tk.Label(
            analyzer_meta,
            textvariable=self.analyzer_update_summary_var,
            anchor="e",
            padx=8,
            pady=3,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.analyzer_update_summary_label.pack(side="right", padx=(0, 6))
        self.analyzer_focus_label = tk.Label(
            analyzer_meta,
            textvariable=self.analyzer_focus_var,
            anchor="e",
            padx=8,
            pady=3,
            bg=self.ui_colors["chip_panel"],
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 8),
            highlightthickness=1,
            highlightbackground=self.ui_colors["chip_panel"],
        )
        self.analyzer_focus_label.pack(side="right", padx=(0, 6))
        analyzer_table_shell = tk.Frame(
            analyzer_shell,
            bg="#ffffff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=1,
            pady=1,
        )
        analyzer_table_shell.pack(fill="both", expand=True)
        analyzer_cols = [key for key, _label in ANALYZER_MODE2_COLUMNS]
        analyzer_table = ttk.Treeview(
            analyzer_table_shell,
            columns=analyzer_cols,
            show="headings",
            height=9,
            style="Monitor.Treeview",
        )
        self.analyzer_table = analyzer_table
        analyzer_x = ttk.Scrollbar(analyzer_table_shell, orient="horizontal", command=analyzer_table.xview)
        analyzer_y = ttk.Scrollbar(analyzer_table_shell, orient="vertical", command=analyzer_table.yview)
        analyzer_table.configure(xscrollcommand=analyzer_x.set, yscrollcommand=analyzer_y.set)
        analyzer_table.grid(row=0, column=0, sticky="nsew")
        analyzer_y.grid(row=0, column=1, sticky="ns")
        analyzer_x.grid(row=1, column=0, sticky="ew")
        analyzer_table_shell.grid_columnconfigure(0, weight=1)
        analyzer_table_shell.grid_rowconfigure(0, weight=1)
        for key, label in ANALYZER_MODE2_COLUMNS:
            width = 102
            if key == "name":
                width = 62
            elif key == "port":
                width = 68
            elif key == "online":
                width = 84
            elif key in {"status"}:
                width = 62
            elif key in {"co2_ppm", "h2o_mmol", "pressure_kpa"}:
                width = 88
            elif key in {"co2_density", "h2o_density", "ref_signal", "co2_signal", "h2o_signal"}:
                width = 88
            elif key in {"co2_ratio_f", "co2_ratio_raw", "h2o_ratio_f", "h2o_ratio_raw"}:
                width = 98
            elif key in {"chamber_temp_c", "case_temp_c"}:
                width = 90
            elif key == "timestamp":
                width = 132
            analyzer_table.heading(key, text=label)
            analyzer_table.column(key, width=width, minwidth=64, stretch=False, anchor="center")
        for idx in range(8):
            name = f"ga{idx + 1:02d}"
            row_tag = "even" if idx % 2 == 0 else "odd"
            item_id = analyzer_table.insert(
                "",
                "end",
                tags=(row_tag,),
                values=[name.upper(), "--", "○ 未读取"] + ["--"] * (len(ANALYZER_MODE2_COLUMNS) - 3),
            )
            self.analyzer_table_items[name] = item_id
        analyzer_table.tag_configure("online_even", background="#f3fffb")
        analyzer_table.tag_configure("online_odd", background="#ecfcf7")
        analyzer_table.tag_configure("stale_even", background="#fffaf0")
        analyzer_table.tag_configure("stale_odd", background="#fff6e6")
        analyzer_table.tag_configure("idle_even", background="#fbfdff")
        analyzer_table.tag_configure("idle_odd", background="#f5f9fd")

        _port_scroll_shell, self.port_scroll_canvas, port_inner = _make_scroll_tab(
            ports_shell, self.ui_colors["soft_layer"]
        )
        port_card_shell = tk.Frame(port_inner, bg=self.ui_colors["shadow"], padx=1, pady=1)
        port_card_shell.pack(fill="both", expand=True, padx=8, pady=8)
        port_box = ttk.LabelFrame(port_card_shell, text="设备串口配置", style="HomeBoard.TLabelframe")
        port_box.pack(fill="both", expand=True)
        self.port_box = port_box
        port_intro = tk.Frame(
            port_box,
            bg="#f8fbff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=9,
            pady=6,
        )
        port_intro.pack(fill="x", padx=6, pady=(2, 4))
        tk.Label(
            port_intro,
            text="DEVICE PORT CONFIG",
            bg="#eef6ff",
            fg="#1d4f91",
            padx=10,
            pady=3,
            font=("Consolas", 8, "bold"),
        ).pack(side="left")
        tk.Label(
            port_intro,
            textvariable=self.device_port_hint_var,
            bg="#f8fbff",
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 9),
        ).pack(side="left", padx=(10, 0))
        port_intro.pack_forget()

        port_action_row = tk.Frame(
            port_box,
            bg="#ffffff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=8,
        )
        port_action_row.pack(fill="x", padx=6, pady=(0, 4))
        self.device_port_save_button = ttk.Button(
            port_action_row,
            text="保存到当前配置",
            command=self._save_device_port_config,
            style="Accent.TButton",
            width=16,
        )
        self.device_port_save_button.pack(side="left")
        self.device_port_reload_button = ttk.Button(
            port_action_row,
            text="重新读取配置",
            command=self.load_config,
            style="HomeTool.TButton",
            width=12,
        )
        self.device_port_reload_button.pack(side="left", padx=6)
        self.device_port_default_button = ttk.Button(
            port_action_row,
            text="分析仪默认 COM35~42",
            command=self._apply_default_analyzer_ports,
            style="Info.TButton",
            width=20,
        )
        self.device_port_default_button.pack(side="left")
        tk.Label(
            port_action_row,
            text="仅对下次连接/下次启动生效",
            bg="#ffffff",
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 9),
        ).pack(side="right")

        compat_row = tk.Frame(
            port_box,
            bg="#fffaf1",
            highlightbackground="#f0d9b7",
            highlightthickness=1,
            padx=10,
            pady=7,
        )
        compat_row.pack(fill="x", padx=6, pady=(0, 4))
        tk.Label(
            compat_row,
            text="兼容旧单分析仪入口",
            bg="#fffaf1",
            fg="#92400e",
            font=("Microsoft YaHei UI", 9, "bold"),
        ).pack(side="left")
        tk.Label(
            compat_row,
            textvariable=self.device_port_compat_var,
            bg="#fffaf1",
            fg="#92400e",
            font=("Microsoft YaHei UI", 9),
        ).pack(side="left", padx=(10, 0))
        compat_row.pack_forget()

        port_grid_shell = tk.Frame(
            port_box,
            bg="#ffffff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=8,
            pady=8,
        )
        port_grid_shell.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        self.device_port_grid = tk.Frame(port_grid_shell, bg="#ffffff")
        self.device_port_grid.pack(fill="both", expand=True)

        workbench_card_shell = tk.Frame(workbench_shell, bg=self.ui_colors["shadow"], padx=1, pady=1)
        workbench_card_shell.pack(fill="both", expand=True, padx=8, pady=8)
        workbench = ttk.Notebook(workbench_card_shell, style="Board.TNotebook")
        workbench.pack(fill="both", expand=True)
        self.workbench = workbench

        log_tab = tk.Frame(workbench, bg=self.ui_colors["shadow"], padx=1, pady=1)
        events_tab = tk.Frame(workbench, bg=self.ui_colors["shadow"], padx=1, pady=1)
        history_tab = tk.Frame(workbench, bg=self.ui_colors["shadow"], padx=1, pady=1)
        workbench.add(log_tab, text="实时日志")
        workbench.add(events_tab, text="关键事件")
        workbench.add(history_tab, text="点位历史")

        log_panel = tk.Frame(log_tab, bg="#f8fbff")
        log_panel.pack(fill="both", expand=True)
        events_panel = tk.Frame(events_tab, bg="#f8fbff")
        events_panel.pack(fill="both", expand=True)
        history_panel = tk.Frame(history_tab, bg="#f8fbff")
        history_panel.pack(fill="both", expand=True)

        def add_panel_header(parent: tk.Frame, title: str, accent: str, icon: str, tone: str, note: str) -> tk.Frame:
            header = tk.Frame(
                parent,
                bg="#f8fbff",
                highlightbackground="#d9e6f2",
                highlightthickness=1,
                padx=12,
                pady=10,
            )
            header.pack(fill="x")
            tk.Frame(header, bg=accent, height=3).pack(fill="x", pady=(0, 8))
            top_row = tk.Frame(header, bg="#f8fbff")
            top_row.pack(fill="x")
            tk.Frame(top_row, bg=accent, width=4, height=26).pack(side="left", padx=(0, 10))
            chip = tk.Frame(top_row, bg=accent, padx=8, pady=3)
            chip.pack(side="left", padx=(0, 10))
            tk.Label(
                chip,
                text=icon,
                bg=accent,
                fg=self.ui_colors["accent_dark"],
                font=self.ui_metrics["panel_icon_font"],
                width=2,
            ).pack(side="left")
            tk.Label(
                top_row,
                text=title,
                bg="#f8fbff",
                fg=self.ui_colors["text"],
                font=self.ui_metrics["panel_header_font"],
            ).pack(side="left")
            tk.Label(
                top_row,
                text="PANEL",
                bg=tone,
                fg=self.ui_colors["muted"],
                padx=8,
                pady=2,
                font=("Consolas", 8, "bold"),
            ).pack(side="right")
            if note:
                tk.Label(
                    header,
                    text=note,
                    bg="#f8fbff",
                    fg=self.ui_colors["muted"],
                    anchor="w",
                    justify="left",
                    font=("Microsoft YaHei UI", 9),
                ).pack(fill="x", pady=(6, 0))
            return header

        add_panel_header(log_panel, "实时日志", self.ui_colors["info_soft"], "≡", "#edf4ff", "")
        add_panel_header(events_panel, "关键事件", self.ui_colors["warn_soft"], "!", "#fff5e6", "")
        add_panel_header(history_panel, "点位历史", self.ui_colors["accent_soft"], "◎", "#edfdfb", "")

        log_body = tk.Frame(log_panel, bg="#f8fbff", padx=8, pady=8)
        log_body.pack(fill="both", expand=True)
        event_body = tk.Frame(events_panel, bg="#f8fbff", padx=8, pady=8)
        event_body.pack(fill="both", expand=True)
        history_body = tk.Frame(history_panel, bg="#f8fbff", padx=8, pady=8)
        history_body.pack(fill="both", expand=True)

        log_scroll = ttk.Scrollbar(log_body, orient="vertical")
        self.log_text = tk.Text(
            log_body,
            height=22,
            yscrollcommand=log_scroll.set,
            bg="#ffffff",
            fg=self.ui_colors["text"],
            insertbackground=self.ui_colors["text"],
            selectbackground="#dbeafe",
            selectforeground=self.ui_colors["text"],
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground="#d7e3f0",
            padx=10,
            pady=10,
            font=("Consolas", 10),
        )
        log_scroll.configure(command=self.log_text.yview)
        log_scroll.pack(side="right", fill="y")
        self.log_text.pack(side="left", fill="both", expand=True, padx=(0, 8))

        event_header = tk.Frame(event_body, bg="#f8fbff")
        event_header.pack(fill="x")
        event_scroll = ttk.Scrollbar(event_body, orient="vertical")
        event_actions = tk.Frame(
            event_header,
            bg="#f8fbff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=8,
        )
        event_actions.pack(fill="x", pady=(0, 6))
        tk.Label(event_actions, text="EVENT TOOLBAR", bg="#fff5e6", fg="#b45309", padx=10, pady=3, font=("Consolas", 8, "bold")).pack(side="left")
        ttk.Label(event_actions, text="筛选").pack(side="left", padx=(10, 0))
        self.event_filter_var = tk.StringVar(value="全部")
        self.event_filter_var.trace_add("write", lambda *_args: self._refresh_key_events())
        ttk.Combobox(
            event_actions,
            textvariable=self.event_filter_var,
            values=["全部", "只看异常", "只看保存成功"],
            width=12,
            state="readonly",
            style="Home.TCombobox",
        ).pack(side="left", padx=(6, 0))
        ttk.Button(event_actions, text="复制选中事件", command=self._copy_selected_event, style="HomeTool.TButton").pack(side="left", padx=(12, 0))
        ttk.Button(event_actions, text="导出事件列表", command=self._export_event_list, style="HomeTool.TButton").pack(side="left", padx=(6, 0))
        self.event_text = tk.Text(
            event_body,
            height=16,
            yscrollcommand=event_scroll.set,
            bg="#ffffff",
            fg=self.ui_colors["text"],
            insertbackground=self.ui_colors["text"],
            selectbackground="#dbeafe",
            selectforeground=self.ui_colors["text"],
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground="#d7e3f0",
            padx=10,
            pady=10,
            font=("Consolas", 10),
        )
        event_scroll.configure(command=self.event_text.yview)
        event_scroll.pack(side="right", fill="y")
        self.event_text.pack(side="left", fill="both", expand=True, padx=(0, 8))
        self.event_text.tag_configure("timeline", foreground=self.ui_colors["line"], font=("Consolas", 10))
        self.event_text.tag_configure("event_ok", foreground="#166534")
        self.event_text.tag_configure("event_warn", foreground="#92400e")
        self.event_text.tag_configure("event_error", foreground="#991b1b")
        self.event_text.tag_configure("event_info", foreground="#1d4ed8")
        self.event_text.tag_configure("event_time", foreground=self.ui_colors["muted"], font=("Consolas", 9, "bold"))
        self.event_text.configure(state="disabled")
        self.event_text.bind("<Double-Button-1>", self._copy_event_from_double_click)
        self.event_text.bind("<Button-3>", self._show_event_context_menu)
        self.event_menu = tk.Menu(self.root, tearoff=0)
        self.event_menu.add_command(label="复制事件", command=self._copy_selected_event)
        self.event_menu.add_command(label="导出事件列表", command=self._export_event_list)
        self.event_menu.add_command(label="打开对应点文件", command=self._open_selected_event_point_file)

        history_box = tk.Frame(history_body, bg="#f8fbff")
        history_box.pack(fill="both", expand=True)
        history_actions = tk.Frame(
            history_box,
            bg="#f8fbff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=8,
        )
        history_actions.pack(fill="x", pady=(0, 6))
        tk.Label(history_actions, text="HISTORY TOOLBAR", bg="#edf4ff", fg="#1d4f91", padx=10, pady=3, font=("Consolas", 8, "bold")).pack(side="left")
        ttk.Label(history_actions, text="筛选").pack(side="left", padx=(10, 0))
        self.history_filter_var = tk.StringVar(value="全部")
        self.history_filter_var.trace_add("write", lambda *_args: self._refresh_history_list())
        ttk.Combobox(
            history_actions,
            textvariable=self.history_filter_var,
            values=["全部", "成功", "跳过", "当前"],
            width=8,
            state="readonly",
            style="Home.TCombobox",
        ).pack(side="left", padx=(6, 12))
        self.open_latest_point_button = ttk.Button(history_actions, text="打开最新点文件", command=self._open_latest_point_file, style="HomeTool.TButton")
        self.open_latest_point_button.pack(side="left")
        self.open_workbook_button = ttk.Button(history_actions, text="打开Workbook", command=self._open_current_workbook, style="HomeTool.TButton")
        self.open_workbook_button.pack(side="left", padx=(6, 0))
        self.open_summary_report_button = ttk.Button(
            history_actions,
            text="打开汇总表",
            command=self._open_current_summary_report,
            style="HomeTool.TButton",
        )
        self.open_summary_report_button.pack(side="left", padx=(6, 0))
        self.open_run_dir_button = ttk.Button(history_actions, text="打开Run目录", command=self._open_current_run_dir, style="HomeTool.TButton")
        self.open_run_dir_button.pack(side="left", padx=6)
        self.open_coefficient_report_button = ttk.Button(
            history_actions,
            text="打开气体拟合报告",
            command=self._open_current_coefficient_report,
            style="HomeTool.TButton",
        )
        self.open_coefficient_report_button.pack(side="left")
        self.open_modeling_config_button = ttk.Button(
            history_actions,
            text="打开离线建模配置",
            command=self._open_modeling_config,
            style="HomeTool.TButton",
        )
        self.open_modeling_config_button.pack(side="left", padx=(6, 0))
        self.run_modeling_button = ttk.Button(
            history_actions,
            text="运行离线建模",
            command=self._run_offline_modeling_analysis,
            style="HomeTool.TButton",
        )
        self.run_modeling_button.pack(side="left", padx=(6, 0))
        self.open_modeling_result_button = ttk.Button(
            history_actions,
            text="打开离线建模结果",
            command=self._open_modeling_result,
            style="HomeTool.TButton",
        )
        self.open_modeling_result_button.pack(side="left", padx=(6, 0))
        history_meta = tk.Frame(
            history_box,
            bg="#ffffff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=8,
        )
        history_meta.pack(fill="x", pady=(0, 6))
        ttk.Label(history_meta, textvariable=self.current_latest_point_name_var).pack(anchor="w")
        ttk.Label(history_meta, textvariable=self.current_workbook_name_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(history_meta, textvariable=self.current_summary_report_name_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(history_meta, textvariable=self.current_coefficient_report_name_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(history_meta, textvariable=self.current_run_dir_name_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(history_meta, textvariable=self.current_io_name_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(history_meta, textvariable=self.current_modeling_config_name_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(history_meta, textvariable=self.current_modeling_result_name_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(history_meta, textvariable=self.runtime_config_diff_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(
            history_meta,
            text="说明：离线建模分析功能默认不参与当前自动校准运行流程，仅用于旁路分析与系数生成。",
            foreground=self.ui_colors["muted"],
            background="#ffffff",
            wraplength=840,
            justify="left",
        ).pack(anchor="w", pady=(4, 0))
        coefficient_box = tk.Frame(
            history_box,
            bg="#ffffff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=8,
        )
        coefficient_box.pack(fill="x", pady=(0, 6))
        tk.Label(
            coefficient_box,
            text="校准结果预览",
            bg="#ffffff",
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9, "bold"),
        ).pack(anchor="w")
        coefficient_scroll = ttk.Scrollbar(coefficient_box, orient="vertical")
        self.coefficient_text = tk.Text(
            coefficient_box,
            height=8,
            yscrollcommand=coefficient_scroll.set,
            bg="#ffffff",
            fg=self.ui_colors["text"],
            insertbackground=self.ui_colors["text"],
            selectbackground="#dbeafe",
            selectforeground=self.ui_colors["text"],
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground="#d7e3f0",
            padx=10,
            pady=10,
            font=("Consolas", 10),
        )
        coefficient_scroll.configure(command=self.coefficient_text.yview)
        coefficient_scroll.pack(side="right", fill="y", pady=(8, 0))
        self.coefficient_text.pack(side="left", fill="x", expand=True, padx=(0, 8), pady=(8, 0))
        self.coefficient_text.configure(state="disabled")
        self._set_coefficient_text("当前没有可显示的气体拟合或温度补偿结果。")
        modeling_box = tk.Frame(
            history_box,
            bg="#ffffff",
            highlightbackground="#d9e6f2",
            highlightthickness=1,
            padx=10,
            pady=8,
        )
        modeling_box.pack(fill="x", pady=(0, 6))
        tk.Label(
            modeling_box,
            text="离线建模分析",
            bg="#ffffff",
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9, "bold"),
        ).pack(anchor="w")
        tk.Label(
            modeling_box,
            text="离线建模输入文件仅用于离线建模分析，不参与当前在线自动校准流程。",
            bg="#ffffff",
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 9),
            wraplength=820,
            justify="left",
        ).pack(anchor="w", pady=(4, 0))
        modeling_input_box = tk.Frame(modeling_box, bg="#ffffff")
        modeling_input_box.pack(fill="x", pady=(8, 0))
        tk.Label(
            modeling_input_box,
            text="离线建模输入文件",
            bg="#ffffff",
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9, "bold"),
        ).grid(row=0, column=0, sticky="w")
        self.modeling_input_entry = ttk.Entry(
            modeling_input_box,
            textvariable=self.modeling_input_path_var,
            width=68,
        )
        self.modeling_input_entry.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        self.modeling_input_browse_button = ttk.Button(
            modeling_input_box,
            text="浏览...",
            command=self._browse_modeling_input_file,
            style="HomeTool.TButton",
            width=10,
        )
        self.modeling_input_browse_button.grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(6, 0))
        tk.Label(
            modeling_input_box,
            text="文件类型",
            bg="#ffffff",
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9),
        ).grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.modeling_file_type_combo = ttk.Combobox(
            modeling_input_box,
            textvariable=self.modeling_input_file_type_var,
            values=("auto", "csv", "xlsx", "xls"),
            width=12,
            state="readonly",
        )
        self.modeling_file_type_combo.grid(row=2, column=0, sticky="w", padx=(56, 0), pady=(8, 0))
        tk.Label(
            modeling_input_box,
            text="Excel Sheet",
            bg="#ffffff",
            fg=self.ui_colors["text"],
            font=("Microsoft YaHei UI", 9),
        ).grid(row=2, column=0, sticky="w", padx=(180, 0), pady=(8, 0))
        self.modeling_sheet_entry = ttk.Entry(
            modeling_input_box,
            textvariable=self.modeling_input_sheet_var,
            width=16,
        )
        self.modeling_sheet_entry.grid(row=2, column=0, sticky="w", padx=(258, 0), pady=(8, 0))
        self.modeling_save_button = ttk.Button(
            modeling_input_box,
            text="保存离线建模配置",
            command=self._save_modeling_input_selection,
            style="Accent.TButton",
        )
        self.modeling_save_button.grid(row=2, column=1, sticky="e", padx=(8, 0), pady=(8, 0))
        tk.Label(
            modeling_input_box,
            textvariable=self.modeling_save_status_var,
            bg="#ffffff",
            fg=self.ui_colors["muted"],
            font=("Microsoft YaHei UI", 9),
            wraplength=820,
            justify="left",
        ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(6, 0))
        modeling_input_box.columnconfigure(0, weight=1)
        modeling_scroll = ttk.Scrollbar(modeling_box, orient="vertical")
        self.modeling_text = tk.Text(
            modeling_box,
            height=9,
            yscrollcommand=modeling_scroll.set,
            bg="#ffffff",
            fg=self.ui_colors["text"],
            insertbackground=self.ui_colors["text"],
            selectbackground="#dbeafe",
            selectforeground=self.ui_colors["text"],
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground="#d7e3f0",
            padx=10,
            pady=10,
            font=("Consolas", 10),
        )
        modeling_scroll.configure(command=self.modeling_text.yview)
        modeling_scroll.pack(side="right", fill="y", pady=(10, 0))
        self.modeling_text.pack(side="left", fill="x", expand=True, padx=(0, 8), pady=(10, 0))
        self.modeling_text.configure(state="disabled")
        self._set_modeling_text("离线建模分析默认关闭，不参与当前自动校准运行流程。")
        history_list_frame = tk.Frame(history_box, bg="#f8fbff")
        history_list_frame.pack(fill="both", expand=True)
        history_scroll = ttk.Scrollbar(history_list_frame, orient="vertical")
        self.history_list = tk.Listbox(
            history_list_frame,
            height=5,
            yscrollcommand=history_scroll.set,
            bg="#ffffff",
            fg=self.ui_colors["text"],
            selectbackground="#c7d2fe",
            selectforeground=self.ui_colors["text"],
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground="#d7e3f0",
            font=("Microsoft YaHei UI", 10),
        )
        history_scroll.configure(command=self.history_list.yview)
        history_scroll.pack(side="right", fill="y")
        self.history_list.pack(side="left", fill="both", expand=True)
        self.history_list.bind("<Double-Button-1>", self._open_selected_history_item)
        self.history_list.bind("<Button-3>", self._show_history_context_menu)
        self.history_menu = tk.Menu(self.root, tearoff=0)
        self.history_menu.add_command(label="复制历史项", command=self._copy_selected_history_item)
        self.history_menu.add_command(label="打开文件", command=self._open_selected_history_item)
        self.history_menu.add_command(label="打开所在目录", command=self._open_selected_history_parent)
        self.history_menu.add_command(label="复制文件路径", command=self._copy_selected_history_path)
        self._set_open_buttons_state()

        self.root.after(200, self._poll_log)
        self.root.bind("<Configure>", self._schedule_responsive_layout, add="+")
        self._on_temp_scope_change()
        self._refresh_execution_summary()
        self._refresh_flow_help()
        self._apply_banner_states()
        self._apply_control_lock()
        self.root.after(80, self._apply_responsive_layout)
        self.root.after(180, self._apply_responsive_layout)
        self.root.after(600, self._apply_responsive_layout)
        self.root.after(220, self._ensure_option_lists_loaded)
        self.root.after(900, self._ensure_option_lists_loaded)
        self.root.after(1600, self._ensure_option_lists_loaded)
        self.root.after(2600, self._ensure_option_lists_loaded)

    def _log_app_event(self, direction: str, command: Any = None, response: Any = None, error: Any = None) -> None:
        logger = getattr(self, "logger", None)
        if logger is None:
            return
        try:
            logger.log_io(
                port="UI",
                device="app",
                direction=direction,
                command=command,
                response=response,
                error=error,
            )
        except Exception:
            pass

    def _on_close_request(self) -> None:
        startup_alive = bool(self.startup_thread and self.startup_thread.is_alive())
        worker_alive = bool(self.worker and self.worker.is_alive())
        if startup_alive or worker_alive:
            decision = messagebox.askyesnocancel(
                "流程仍在运行",
                "当前流程仍在运行。\n\n"
                "是：隐藏窗口，流程在后台继续运行\n"
                "否：请求停止流程并关闭窗口\n"
                "取消：返回页面",
            )
            if decision is None:
                self._log_app_event("EVENT", command="close-window", response="cancelled")
                return
            if decision:
                self._log_app_event("EVENT", command="close-window", response="hidden-while-running")
                self.log("窗口已隐藏，流程继续在后台运行")
                self.root.withdraw()
                return
            self._log_app_event("EVENT", command="close-window", response="stop-and-close")
            if self.runner:
                self.runner.stop()
                self.log("已请求停止，等待流程结束...")
                if not self._wait_for_worker_shutdown(timeout_s=20.0):
                    self.log("警告：流程停止超时(20s)，强制关闭")
        else:
            self._log_app_event("EVENT", command="close-window", response="closed-idle")
        self.root.destroy()

    def _schedule_responsive_layout(self, _event=None) -> None:
        if self._layout_after_id is not None:
            try:
                self.root.after_cancel(self._layout_after_id)
            except Exception:
                pass
        self._layout_after_id = self.root.after(80, self._apply_responsive_layout)

    @staticmethod
    def _place_paned_sash(pane: tk.PanedWindow, index: int, pos: int) -> None:
        if hasattr(pane, "sashpos"):
            pane.sashpos(index, pos)
            return
        orient = str(pane.cget("orient"))
        x, y = pane.sash_coord(index)
        if orient == "vertical":
            pane.sash_place(index, x, pos)
        else:
            pane.sash_place(index, pos, y)

    def _apply_responsive_layout(self) -> None:
        self._layout_after_id = None
        width = max(int(self.root.winfo_width()), 1)
        height = max(int(self.root.winfo_height()), 1)

        wide = width >= 1360
        compact_selectors = width >= 1240
        device_view_active = False
        if hasattr(self, "bottom_tabs") and hasattr(self, "device_shell"):
            try:
                device_view_active = self.bottom_tabs.select() == str(self.device_shell)
            except Exception:
                device_view_active = False
        if device_view_active:
            selector_panel_height = 150 if height >= 1000 else 138 if height >= 920 else 126
            selector_area_height = selector_panel_height + 54
        else:
            selector_panel_height = 156 if height >= 1000 else 144 if height >= 920 else 130
            selector_area_height = selector_panel_height + 60

        if hasattr(self, "content_area"):
            if wide:
                self.left_column.grid(row=0, column=0, sticky="nsew", padx=(8, 6), pady=0)
                self.right_column.grid(row=0, column=1, sticky="nsew", padx=(6, 8), pady=0)
                self.content_area.grid_columnconfigure(0, weight=5, minsize=920)
                self.content_area.grid_columnconfigure(1, weight=4, minsize=700)
                self.content_area.grid_rowconfigure(0, weight=1)
                self.content_area.grid_rowconfigure(1, weight=0)
            else:
                self.left_column.grid(row=0, column=0, sticky="nsew", padx=8, pady=0)
                self.right_column.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 0))
                self.content_area.grid_columnconfigure(0, weight=1)
                self.content_area.grid_columnconfigure(1, weight=0)
                self.content_area.grid_rowconfigure(0, weight=0)
                self.content_area.grid_rowconfigure(1, weight=1)
                self.content_area.grid_rowconfigure(2, weight=0)

        if hasattr(self, "device_grid"):
            if width >= 1800:
                self._layout_device_cards(5)
            elif width >= 1500:
                self._layout_device_cards(4)
            else:
                self._layout_device_cards(3)

        for attr in ("temp_checks_shell", "co2_checks_shell"):
            shell = getattr(self, attr, None)
            if shell is not None:
                try:
                    shell.configure(height=selector_panel_height)
                except Exception:
                    pass
        if hasattr(self, "selection_area"):
            try:
                self.selection_area.configure(height=selector_area_height)
            except Exception:
                pass

        if hasattr(self, "main_pane"):
            try:
                if device_view_active:
                    if width >= 1800 and height >= 1000:
                        desired_top = int(height * 0.58)
                    elif wide:
                        desired_top = int(height * 0.60)
                    else:
                        desired_top = int(height * 0.64)
                else:
                    if width >= 1800 and height >= 1000:
                        desired_top = int(height * 0.66)
                    elif wide:
                        desired_top = int(height * 0.68)
                    else:
                        desired_top = int(height * 0.71)
                if height <= 920:
                    desired_top = int(height * ((0.63 if wide else 0.67) if device_view_active else (0.69 if wide else 0.72)))
                min_bottom = 280 if device_view_active else 190
                desired_top = max(520 if device_view_active else 560, min(desired_top, height - min_bottom))
                self._place_paned_sash(self.main_pane, 0, desired_top)
            except Exception:
                pass

        if hasattr(self, "workbench") and hasattr(self.workbench, "sashpos"):
            try:
                workbench_width = max(int(self.workbench.winfo_width()), 1)
                if workbench_width >= 1800:
                    log_width = int(workbench_width * 0.43)
                    event_width = int(workbench_width * 0.27)
                elif workbench_width >= 1500:
                    log_width = int(workbench_width * 0.45)
                    event_width = int(workbench_width * 0.28)
                else:
                    log_width = int(workbench_width * 0.48)
                    event_width = int(workbench_width * 0.27)
                first_sash = max(420, min(log_width, workbench_width - 620))
                second_sash = max(first_sash + 280, min(first_sash + event_width, workbench_width - 260))
                self.workbench.sashpos(0, first_sash)
                self.workbench.sashpos(1, second_sash)
            except Exception:
                pass

    def log(self, msg: str) -> None:
        self.log_queue.put(msg)

    def _apply_status(self, msg: str) -> None:
        self.status_var.set(msg)
        self._update_stage_from_status(msg)
        self._apply_banner_states()

    def set_status(self, msg: str) -> None:
        if threading.current_thread() is threading.main_thread():
            self._apply_status(msg)
            return

        self._call_on_ui_thread(self._apply_status, msg)

    def _poll_log(self) -> None:
        try:
            while not self.log_queue.empty():
                msg = self.log_queue.get()
                self.log_text.insert("end", msg + "\n")
                self.log_text.see("end")

            now_ts = time.time()
            try:
                if now_ts - self._last_attach_run_refresh_ts >= self._attach_run_refresh_interval_s:
                    self._attach_latest_active_run()
                    self._last_attach_run_refresh_ts = now_ts
            except Exception:
                pass

            try:
                if now_ts - self._last_progress_refresh_ts >= self._progress_refresh_interval_s:
                    self._refresh_progress_status()
                    self._last_progress_refresh_ts = now_ts
            except Exception:
                pass

            try:
                if now_ts - self._last_device_panel_refresh_ts >= self._device_panel_refresh_interval_s:
                    self._refresh_live_device_values()
                    self._last_device_panel_refresh_ts = now_ts
            except Exception:
                pass

            try:
                if now_ts - self._last_event_refresh_ts >= self._event_refresh_interval_s:
                    self._refresh_key_events()
                    self._last_event_refresh_ts = now_ts
            except Exception:
                pass

            try:
                if now_ts - self._last_modeling_refresh_ts >= self._modeling_refresh_interval_s:
                    self._refresh_modeling_panel()
                    self._last_modeling_refresh_ts = now_ts
            except Exception:
                pass

            try:
                self._apply_control_lock()
            except Exception:
                pass
        finally:
            try:
                self.root.after(self._poll_log_interval_ms, self._poll_log)
            except Exception:
                pass

    def _modeling_config_path(self) -> Path:
        cfg_path = Path(self.config_path.get()).resolve()
        if cfg_path.parent.name.lower() == "configs":
            return cfg_path.parent / "modeling_offline.json"
        return cfg_path.parent.parent / "configs" / "modeling_offline.json"

    @staticmethod
    def _normalize_modeling_sheet_name(raw_value: str) -> str | int:
        text = str(raw_value or "").strip()
        if not text:
            return 0
        if text.lstrip("+-").isdigit():
            return int(text)
        return text

    @staticmethod
    def _infer_modeling_file_type(path_text: str) -> str:
        suffix = Path(str(path_text or "")).suffix.lower()
        if suffix == ".csv":
            return "csv"
        if suffix == ".xlsx":
            return "xlsx"
        if suffix == ".xls":
            return "xls"
        return "auto"

    def _browse_modeling_input_file(self) -> None:
        current_path = str(self.modeling_input_path_var.get() or "").strip()
        initial_dir = ""
        if current_path:
            current = Path(current_path)
            if current.exists():
                initial_dir = str(current.parent)
        if not initial_dir:
            initial_dir = str(self._modeling_config_path().parent)
        selected = filedialog.askopenfilename(
            title="选择离线建模输入文件",
            initialdir=initial_dir,
            filetypes=[
                ("支持的文件", "*.csv *.xlsx *.xls"),
                ("CSV 文件", "*.csv"),
                ("Excel 文件", "*.xlsx *.xls"),
                ("所有文件", "*.*"),
            ],
        )
        if not selected:
            return
        selected_path = str(Path(selected).resolve())
        self.modeling_input_path_var.set(selected_path)
        self.modeling_input_file_type_var.set(self._infer_modeling_file_type(selected_path))
        if self.modeling_input_file_type_var.get() == "csv":
            self.modeling_input_sheet_var.set("0")
        self.modeling_save_status_var.set("保存状态：已选择新文件，请点击“保存离线建模配置”写入配置")

    def _save_modeling_input_selection(self) -> None:
        path_text = str(self.modeling_input_path_var.get() or "").strip()
        if not path_text:
            messagebox.showerror("保存失败", "请先选择离线建模输入文件。")
            self.modeling_save_status_var.set("保存状态：保存失败，尚未选择输入文件")
            return
        selected_type = str(self.modeling_input_file_type_var.get() or "auto").strip().lower() or "auto"
        sheet_name = self._normalize_modeling_sheet_name(self.modeling_input_sheet_var.get())
        try:
            target_path = save_modeling_config(
                modeling_config_path=self._modeling_config_path(),
                base_config_path=Path(self.config_path.get()).resolve(),
                path=path_text,
                file_type=selected_type,
                sheet_name=sheet_name,
            )
        except Exception as exc:
            messagebox.showerror("保存失败", f"保存离线建模配置失败：{exc}")
            self.modeling_save_status_var.set(f"保存状态：保存失败，{exc}")
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.modeling_save_status_var.set(f"保存状态：已保存到 {target_path.name}（{timestamp}）")
        self.log(f"离线建模输入文件已保存：{path_text}")
        self._refresh_modeling_panel()

    @staticmethod
    def _set_text_widget(widget: tk.Text, text: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", text)
        widget.configure(state="disabled")

    def _refresh_modeling_panel(self) -> None:
        try:
            loaded = load_modeling_config(
                base_config_path=Path(self.config_path.get()).resolve(),
                modeling_config_path=self._modeling_config_path(),
            )
        except Exception as exc:
            self.current_modeling_result_path = None
            self.current_modeling_run_dir = None
            self.current_modeling_config_name_var.set("离线建模配置：读取失败")
            self.current_modeling_result_name_var.set("离线建模结果：--")
            self.modeling_save_status_var.set("保存状态：离线建模配置读取失败")
            self._set_modeling_text(f"离线建模配置读取失败：{exc}")
            self._set_open_buttons_state()
            return

        modeling_cfg = loaded.get("modeling", {})
        data_source = modeling_cfg.get("data_source", {}) if isinstance(modeling_cfg.get("data_source", {}), dict) else {}
        config_path = Path(loaded.get("modeling_config_path", self._modeling_config_path()))
        self.current_modeling_config_name_var.set(f"离线建模配置：{config_path.name}")
        focused_widget = self.root.focus_get()
        editing_widgets = {
            getattr(self, "modeling_input_entry", None),
            getattr(self, "modeling_sheet_entry", None),
            getattr(self, "modeling_file_type_combo", None),
        }
        if focused_widget not in editing_widgets:
            source_path = str(data_source.get("path", "") or "").strip()
            self.modeling_input_path_var.set(source_path)
            file_type = str(data_source.get("file_type", data_source.get("format", "auto")) or "auto").strip().lower() or "auto"
            self.modeling_input_file_type_var.set(file_type)
            sheet_name = data_source.get("sheet_name", 0)
            self.modeling_input_sheet_var.set(str(sheet_name))
        current_status = str(self.modeling_save_status_var.get() or "")
        if not current_status.startswith("保存状态：已保存"):
            source_path = str(data_source.get("path", "") or "").strip()
            if not source_path:
                self.modeling_save_status_var.set("保存状态：当前尚未选择离线建模输入文件")
            else:
                self.modeling_save_status_var.set("保存状态：当前离线建模配置已加载")
        summary = summarize_modeling_config(loaded)
        latest = find_latest_modeling_artifacts(modeling_cfg.get("export", {}).get("output_dir", ""))
        run_dir = latest.get("run_dir")
        summary_txt = latest.get("summary_txt")
        summary_json = latest.get("summary_json")
        self.current_modeling_run_dir = run_dir if isinstance(run_dir, Path) else None
        self.current_modeling_result_path = summary_txt if isinstance(summary_txt, Path) else summary_json if isinstance(summary_json, Path) else None
        if self.current_modeling_result_path is not None:
            self.current_modeling_result_name_var.set(f"离线建模结果：{self.current_modeling_result_path.name}")
        else:
            self.current_modeling_result_name_var.set("离线建模结果：当前无文件")
        if isinstance(summary_txt, Path) and summary_txt.exists():
            try:
                text = summary_txt.read_text(encoding="utf-8")
                self._set_modeling_text(summary + "\n\n最近结果\n" + text)
            except Exception as exc:
                self._set_modeling_text(summary + f"\n\n最近结果读取失败：{exc}")
        else:
            self._set_modeling_text(summary + "\n\n最近结果\n当前还没有离线建模结果。")
        self._set_open_buttons_state()

    def load_config(self) -> None:
        self.cfg = self._load_runtime_base_config()
        workflow_cfg = self.cfg.get("workflow", {}) if isinstance(self.cfg.get("workflow", {}), dict) else {}
        coeff_cfg = self.cfg.get("coefficients", {}) if isinstance(self.cfg.get("coefficients", {}), dict) else {}
        route_text_map = {
            "h2o_then_co2": "先水后气",
            "h2o_only": "只测水路",
            "co2_only": "只测气路",
        }
        self.route_mode_var.set(route_text_map.get(str(workflow_cfg.get("route_mode", "h2o_then_co2")), "先水后气"))
        fit_enabled = (
            not bool(workflow_cfg.get("collect_only", False))
            and bool(coeff_cfg.get("enabled", True))
            and bool(coeff_cfg.get("auto_fit", True))
            and bool(coeff_cfg.get("fit_h2o", True))
        )
        self.fit_enabled_var.set(fit_enabled)
        self.postrun_delivery_var.set(False)
        self.temperature_order_var.set("从高到低" if bool(workflow_cfg.get("temperature_descending", True)) else "从低到高")
        selected_temps_raw = workflow_cfg.get("selected_temps_c")
        selected_temps: set[float] = set()
        if isinstance(selected_temps_raw, list):
            for item in selected_temps_raw:
                try:
                    selected_temps.add(round(float(item), 6))
                except Exception:
                    continue
        self.temp_scope_var.set("指定温度点" if selected_temps else "全部温度点")
        self._refresh_valve_hint()
        self._refresh_temperature_options()
        if selected_temps:
            for temp, var in self.temp_check_vars.items():
                var.set(round(float(temp), 6) in selected_temps)
            self._sync_temp_listbox_from_vars()
        self._refresh_co2_options()
        self._refresh_pressure_options()
        selected_pressures_raw = workflow_cfg.get("selected_pressure_points")
        selected_pressures: set[int] = set()
        ambient_selected = False
        if isinstance(selected_pressures_raw, list):
            for item in selected_pressures_raw:
                if self._is_ambient_pressure_token(item):
                    ambient_selected = True
                    continue
                try:
                    selected_pressures.add(int(round(float(item))))
                except Exception:
                    continue
        self.ambient_pressure_var.set(ambient_selected)
        if selected_pressures:
            for pressure_hpa, var in self.pressure_check_vars.items():
                var.set(int(pressure_hpa) in selected_pressures)
            self._sync_pressure_listbox_from_vars()
        self._refresh_device_port_editor()
        self._refresh_execution_summary()
        self._refresh_modeling_panel()
        self.log("配置已加载")
        self._attach_latest_active_run()
        self.root.after(50, self._apply_responsive_layout)
        self.root.after(150, self._ensure_option_lists_loaded)

    @staticmethod
    def _merge_nested_dicts(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        merged = copy.deepcopy(base)
        for key, value in overlay.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = App._merge_nested_dicts(merged[key], value)
            else:
                merged[key] = copy.deepcopy(value)
        return merged

    def _user_tuning_path(self, cfg: Dict[str, Any]) -> Path:
        base_dir = cfg.get("_base_dir")
        if base_dir:
            return Path(base_dir) / "configs" / "user_tuning.json"
        cfg_path = Path(self.config_path.get()).resolve()
        return cfg_path.parent.parent / "configs" / "user_tuning.json"

    def _load_runtime_base_config(self) -> Dict[str, Any]:
        cfg = load_config(self.config_path.get())
        tuning_path = self._user_tuning_path(cfg)
        if tuning_path.exists():
            try:
                overlay = json.loads(tuning_path.read_text(encoding="utf-8-sig"))
            except Exception as exc:
                self.log(f"用户调参覆盖加载失败：{exc}")
            else:
                if isinstance(overlay, dict):
                    cfg = self._merge_nested_dicts(cfg, overlay)
                    cfg["_user_tuning_path"] = str(tuning_path)
                    self.log(f"已加载用户调参覆盖：{tuning_path.name}")
        return cfg

    def _device_port_specs_from_cfg(self, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        specs: List[Dict[str, Any]] = []
        dcfg = cfg.get("devices", {}) if isinstance(cfg, dict) else {}
        for key, label in DEVICE_PORT_FIELDS:
            item = dcfg.get(key, {})
            specs.append(
                {
                    "key": key,
                    "label": label,
                    "port": str(item.get("port", "") or ""),
                    "kind": "device",
                }
            )

        gas_list_cfg = dcfg.get("gas_analyzers", [])
        if isinstance(gas_list_cfg, list) and gas_list_cfg:
            for idx, item in enumerate(gas_list_cfg, start=1):
                if not isinstance(item, dict):
                    continue
                specs.append(
                    {
                        "key": f"gas_analyzers.{idx - 1}.port",
                        "label": f"气体分析仪{idx} ({item.get('name', f'ga{idx:02d}')})",
                        "port": str(item.get("port", "") or ""),
                        "kind": "gas",
                        "index": idx - 1,
                    }
                )
        elif isinstance(dcfg.get("gas_analyzer"), dict):
            item = dcfg.get("gas_analyzer", {})
            specs.append(
                {
                    "key": "gas_analyzer.port",
                    "label": "气体分析仪（兼容）",
                    "port": str(item.get("port", "") or ""),
                    "kind": "single_gas",
                }
            )
        return specs

    def _refresh_device_port_editor(self) -> None:
        grid = self.device_port_grid
        if grid is None:
            return
        for child in grid.winfo_children():
            child.destroy()

        specs = self._device_port_specs_from_cfg(self.cfg if hasattr(self, "cfg") else {})
        self.device_port_specs = specs
        dcfg = self.cfg.get("devices", {}) if hasattr(self, "cfg") else {}
        compat_port = "--"
        if isinstance(dcfg.get("gas_analyzers"), list) and dcfg.get("gas_analyzers"):
            compat_port = str((dcfg.get("gas_analyzers") or [{}])[0].get("port", "--") or "--")
        elif isinstance(dcfg.get("gas_analyzer"), dict):
            compat_port = str(dcfg.get("gas_analyzer", {}).get("port", "--") or "--")
        self.device_port_compat_var.set(f"兼容单分析仪端口：{compat_port}")
        self.device_port_hint_var.set(
            f"共 {len(specs)} 项，保存到 {Path(self.config_path.get()).name}，下次启动生效"
        )

        for col in range(2):
            grid.grid_columnconfigure(col, weight=1)

        for idx, spec in enumerate(specs):
            row = idx // 2
            col = idx % 2
            field = tk.Frame(
                grid,
                bg="#f8fbff",
                highlightbackground="#d9e6f2",
                highlightthickness=1,
                padx=10,
                pady=8,
            )
            field.grid(row=row, column=col, sticky="nsew", padx=(0, 6) if col == 0 else 0, pady=(0, 6))
            tk.Label(
                field,
                text=spec["label"],
                bg="#f8fbff",
                fg=self.ui_colors["muted"],
                font=("Microsoft YaHei UI", 8, "bold"),
            ).pack(anchor="w", pady=(0, 4))
            var = self.device_port_vars.get(spec["key"])
            if var is None:
                var = tk.StringVar()
                self.device_port_vars[spec["key"]] = var
            var.set(spec["port"])
            ttk.Entry(field, textvariable=var, width=20).pack(fill="x")

    def _apply_default_analyzer_ports(self) -> None:
        gas_specs = [spec for spec in self.device_port_specs if spec.get("kind") == "gas"]
        for spec, port in zip(gas_specs, DEFAULT_ANALYZER_PORTS):
            var = self.device_port_vars.get(spec["key"])
            if var is not None:
                var.set(port)
        if gas_specs:
            self.device_port_compat_var.set(f"兼容单分析仪端口：{DEFAULT_ANALYZER_PORTS[0]}")
        self.log("分析仪串口已填充为默认 COM35~COM42")

    @staticmethod
    def _normalize_port_text(value: str) -> str:
        return str(value or "").strip().upper()

    def _save_device_port_config(self) -> None:
        cfg_path = Path(self.config_path.get()).resolve()
        try:
            raw_cfg = json.loads(cfg_path.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            messagebox.showerror("保存失败", f"读取配置失败：{exc}")
            return

        try:
            devices = raw_cfg.setdefault("devices", {})
            for spec in self.device_port_specs:
                key = spec["key"]
                label = spec["label"]
                var = self.device_port_vars.get(key)
                port = self._normalize_port_text(var.get() if var is not None else "")
                if not port:
                    raise ValueError(f"{label} 串口不能为空。")
                kind = spec.get("kind")
                if kind == "device":
                    item = devices.setdefault(key, {})
                    if not isinstance(item, dict):
                        raise ValueError(f"{label} 配置格式错误。")
                    item["port"] = port
                elif kind == "gas":
                    gas_list = devices.setdefault("gas_analyzers", [])
                    index = int(spec["index"])
                    if not isinstance(gas_list, list) or index >= len(gas_list) or not isinstance(gas_list[index], dict):
                        raise ValueError(f"{label} 配置不存在。")
                    gas_list[index]["port"] = port
                elif kind == "single_gas":
                    item = devices.setdefault("gas_analyzer", {})
                    if not isinstance(item, dict):
                        raise ValueError(f"{label} 配置格式错误。")
                    item["port"] = port

            gas_list = devices.get("gas_analyzers", [])
            if isinstance(gas_list, list) and gas_list and isinstance(gas_list[0], dict):
                compat = devices.setdefault("gas_analyzer", {})
                if isinstance(compat, dict):
                    compat["port"] = self._normalize_port_text(str(gas_list[0].get("port", "") or ""))

            cfg_path.write_text(json.dumps(raw_cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        except Exception as exc:
            messagebox.showerror("保存失败", str(exc))
            return

        self.log(f"设备串口配置已保存：{cfg_path.name}")
        self.load_config()
        messagebox.showinfo("保存成功", "设备串口配置已保存到当前配置文件，将于下次连接/下次启动生效。")

    def _ensure_option_lists_loaded(self) -> None:
        try:
            temp_size = len(getattr(self, "temp_check_vars", {}))
            co2_size = len(getattr(self, "co2_check_vars", {}))
        except Exception:
            temp_size = 0
            co2_size = 0
        if temp_size > 0 and co2_size > 0:
            return
        try:
            if hasattr(self, "cfg"):
                self._refresh_temperature_options()
                self._refresh_co2_options()
            else:
                self.load_config()
            if not getattr(self, "temp_check_vars", {}):
                self.load_config()
        except Exception as exc:
            self.temp_hint_var.set(f"温度点加载失败：{exc}")
            self.co2_hint_var.set(f"气点加载失败：{exc}")
            self._refresh_selector_hint_summary()
        self.root.after(60, self._apply_responsive_layout)

    @staticmethod
    def _find_latest_active_run_dir(output_dir: Path, freshness_s: int = 180) -> Tuple[Path | None, Path | None]:
        if not output_dir.exists():
            return None, None
        latest_run: Path | None = None
        latest_io: Path | None = None
        latest_mtime = 0.0
        threshold = freshness_s
        now_ts = datetime.now().timestamp()
        candidates = list(output_dir.glob("run_*")) + list(output_dir.glob("rerun_*"))
        for run_dir in candidates:
            if not run_dir.is_dir():
                continue
            io_candidates = sorted(run_dir.glob("io_*.csv"))
            if not io_candidates:
                continue
            io_path = max(io_candidates, key=lambda path: path.stat().st_mtime)
            mtime = io_path.stat().st_mtime
            if now_ts - mtime > threshold:
                continue
            if mtime > latest_mtime:
                latest_run = run_dir
                latest_io = io_path
                latest_mtime = mtime
        return latest_run, latest_io

    def _attach_latest_active_run(self) -> None:
        if self.worker and self.worker.is_alive():
            return
        if self.startup_thread and self.startup_thread.is_alive():
            return
        cfg = getattr(self, "cfg", None)
        if not cfg:
            return
        output_dir = Path(cfg["paths"]["output_dir"])
        run_dir, io_path = self._find_latest_active_run_dir(output_dir)
        if run_dir is None or io_path is None:
            return
        if self.current_run_dir == run_dir and self.current_io_path == io_path:
            return
        self.current_run_dir = run_dir
        self.current_io_path = io_path
        self._live_device_cache = {}
        self._live_device_cache_run_dir = run_dir
        self.current_run_dir_name_var.set(f"Run目录：{run_dir.name}")
        self.current_io_name_var.set(f"IO文件：{io_path.name}")
        self.log(f"已附加到运行目录：{run_dir}")
        self.set_status("已附加到运行中流程")

    def _create_scrollable_checks_panel(
        self, parent: tk.Widget, height: int
    ) -> Tuple[tk.Frame, tk.Canvas, ttk.Frame]:
        shell = tk.Frame(
            parent,
            bg=self.ui_colors["card"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            height=height,
        )
        shell.pack(fill="both", expand=True, padx=6, pady=(0, 4))
        shell.pack_propagate(False)
        canvas = tk.Canvas(
            shell,
            bg=self.ui_colors["card"],
            height=height,
            highlightthickness=0,
            bd=0,
        )
        scrollbar = ttk.Scrollbar(shell, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas)
        window_id = canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True, padx=(0, 2), pady=2)
        scrollbar.pack(side="right", fill="y", pady=2)

        def _sync_inner(_event=None) -> None:
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _sync_width(event) -> None:
            canvas.itemconfigure(window_id, width=event.width)

        inner.bind("<Configure>", _sync_inner, add="+")
        canvas.bind("<Configure>", _sync_width, add="+")
        return shell, canvas, inner

    def _rebuild_checks_panel(
        self,
        inner: ttk.Frame,
        widgets: List[tk.Checkbutton],
        items: List[Tuple[str, tk.BooleanVar]],
        command,
        columns: int = 2,
    ) -> None:
        for child in inner.winfo_children():
            child.destroy()
        widgets.clear()
        columns = max(1, columns)
        for col in range(columns):
            inner.grid_columnconfigure(col, weight=1)
        for idx, (label, var) in enumerate(items):
            row = idx // columns
            col = idx % columns
            btn = tk.Checkbutton(
                inner,
                text=label,
                variable=var,
                command=command,
                indicatoron=False,
                anchor="w",
                justify="left",
                bg="#ffffff",
                fg=self.ui_colors["text"],
                activebackground="#ffffff",
                activeforeground=self.ui_colors["text"],
                selectcolor="#ffffff",
                selectimage="",
                offrelief="flat",
                overrelief="flat",
                relief="flat",
                highlightthickness=1,
                highlightbackground=self.ui_colors["divider"],
                highlightcolor=self.ui_colors["accent"],
                bd=0,
                padx=8,
                pady=4,
                font=("Microsoft YaHei UI", 8, "bold"),
            )
            btn.grid(row=row, column=col, sticky="ew", padx=4, pady=4)
            widgets.append(btn)

    def _refresh_selector_button_styles(self) -> None:
        def _apply(
            buttons: List[tk.Checkbutton],
            order: List[float | int],
            vars_map: Dict[float | int, tk.BooleanVar],
            selected_bg: str,
            selected_fg: str,
            selected_line: str,
        ) -> None:
            for idx, button in enumerate(buttons):
                if idx >= len(order):
                    continue
                value = order[idx]
                var = vars_map.get(value)
                selected = bool(var.get()) if var is not None else False
                state = str(button.cget("state"))
                if state == "disabled":
                    bg = "#e8edf2"
                    fg = "#8a98a8"
                    line = "#d6dee7"
                elif selected:
                    bg = selected_bg
                    fg = selected_fg
                    line = selected_line
                else:
                    bg = "#ffffff"
                    fg = self.ui_colors["text"]
                    line = self.ui_colors["divider"]
                try:
                    button.configure(
                        bg=bg,
                        fg=fg,
                        activebackground=bg,
                        activeforeground=fg,
                        highlightbackground=line,
                    )
                except Exception:
                    pass

        _apply(
            self.temp_checkbuttons,
            [float(v) for v in self.temp_option_order],
            self.temp_check_vars,
            self.ui_colors["accent_soft"],
            self.ui_colors["accent_dark"],
            self.ui_colors["accent"],
        )
        _apply(
            self.co2_checkbuttons,
            [int(v) for v in self.co2_option_order],
            self.co2_check_vars,
            self.ui_colors["info_soft"],
            "#1d4ed8",
            "#93c5fd",
        )
        _apply(
            self.pressure_checkbuttons,
            [int(v) for v in self.pressure_option_order],
            self.pressure_check_vars,
            "#ecfeff",
            "#155e75",
            "#67e8f9",
        )

    def _create_multiselect_listbox_panel(self, parent: tk.Widget, height_rows: int) -> Tuple[tk.Frame, tk.Listbox]:
        shell = tk.Frame(
            parent,
            bg=self.ui_colors["card"],
            highlightbackground=self.ui_colors["divider"],
            highlightthickness=1,
            height=max(112, height_rows * 16 + 24),
        )
        shell.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        shell.pack_propagate(False)
        body = tk.Frame(shell, bg=self.ui_colors["card"])
        body.pack(fill="both", expand=True, padx=2, pady=2)
        scrollbar = ttk.Scrollbar(body, orient="vertical")
        listbox = tk.Listbox(
            body,
            selectmode=tk.EXTENDED,
            exportselection=False,
            activestyle="none",
            height=max(height_rows, 7),
            bg="#f8fafc",
            fg=self.ui_colors["text"],
            selectbackground="#c7d2fe",
            selectforeground=self.ui_colors["text"],
            disabledforeground=self.ui_colors["text"],
            relief="flat",
            bd=0,
            highlightthickness=0,
            font=("Microsoft YaHei UI", 8),
        )
        listbox.configure(yscrollcommand=scrollbar.set)
        scrollbar.configure(command=listbox.yview)
        scrollbar.pack(side="right", fill="y")
        listbox.pack(side="left", fill="both", expand=True)
        return shell, listbox

    def _populate_listbox_from_vars(
        self,
        listbox: tk.Listbox | None,
        order: List[float | int],
        vars_map: Dict[float | int, tk.BooleanVar],
        formatter,
        syncing_attr: str,
    ) -> None:
        if listbox is None:
            return
        setattr(self, syncing_attr, True)
        previous_state = str(listbox.cget("state"))
        try:
            if previous_state != "normal":
                listbox.configure(state="normal")
            listbox.delete(0, "end")
            for value in order:
                listbox.insert("end", formatter(value))
            for idx, value in enumerate(order):
                var = vars_map.get(value)
                if var is not None and var.get():
                    listbox.selection_set(idx)
        finally:
            if str(listbox.cget("state")) != previous_state:
                listbox.configure(state=previous_state)
            setattr(self, syncing_attr, False)

    def _sync_vars_from_listbox(
        self,
        listbox: tk.Listbox | None,
        order: List[float | int],
        vars_map: Dict[float | int, tk.BooleanVar],
        syncing_attr: str,
    ) -> bool:
        if listbox is None or getattr(self, syncing_attr, False):
            return False
        selected = set(listbox.curselection())
        changed = False
        for idx, value in enumerate(order):
            should_enable = idx in selected
            var = vars_map.get(value)
            if var is not None and var.get() != should_enable:
                var.set(should_enable)
                changed = True
        return changed

    def _sync_temp_listbox_from_vars(self) -> None:
        self._populate_listbox_from_vars(
            self.temp_listbox,
            self.temp_option_order,
            self.temp_check_vars,
            lambda value: f"{float(value):g}°C",
            "_syncing_temp_listbox",
        )

    def _sync_co2_listbox_from_vars(self) -> None:
        self._populate_listbox_from_vars(
            self.co2_listbox,
            self.co2_option_order,
            self.co2_check_vars,
            lambda value: f"{int(value)}ppm",
            "_syncing_co2_listbox",
        )

    def _sync_pressure_listbox_from_vars(self) -> None:
        self._populate_listbox_from_vars(
            self.pressure_listbox,
            self.pressure_option_order,
            self.pressure_check_vars,
            lambda value: f"{int(value)}hPa",
            "_syncing_pressure_listbox",
        )

    def _on_temp_listbox_select(self, _event=None) -> None:
        if self._sync_vars_from_listbox(
            self.temp_listbox,
            self.temp_option_order,
            self.temp_check_vars,
            "_syncing_temp_listbox",
        ):
            self._ensure_specific_temp_scope_for_manual_selection()
            self._refresh_execution_summary()

    def _on_co2_listbox_select(self, _event=None) -> None:
        if self._sync_vars_from_listbox(
            self.co2_listbox,
            self.co2_option_order,
            self.co2_check_vars,
            "_syncing_co2_listbox",
        ):
            self._refresh_execution_summary()

    def _on_pressure_listbox_select(self, _event=None) -> None:
        if self._sync_vars_from_listbox(
            self.pressure_listbox,
            self.pressure_option_order,
            self.pressure_check_vars,
            "_syncing_pressure_listbox",
        ):
            self._refresh_execution_summary()

    def _toggle_listbox_item(self, event, listbox: tk.Listbox | None, syncing_attr: str) -> str | None:
        if listbox is None or getattr(self, syncing_attr, False):
            return None
        try:
            if str(listbox.cget("state")) == "disabled":
                return "break"
            index = int(listbox.nearest(event.y))
            if index < 0:
                return "break"
            if index in set(listbox.curselection()):
                listbox.selection_clear(index)
            else:
                listbox.selection_set(index)
                listbox.activate(index)
            return "break"
        except Exception:
            return None

    def _on_temp_listbox_click(self, event) -> str | None:
        result = self._toggle_listbox_item(event, self.temp_listbox, "_syncing_temp_listbox")
        if result == "break":
            if self._sync_vars_from_listbox(
                self.temp_listbox,
                self.temp_option_order,
                self.temp_check_vars,
                "_syncing_temp_listbox",
            ):
                self._ensure_specific_temp_scope_for_manual_selection()
                self._refresh_execution_summary()
        return result

    def _on_co2_listbox_click(self, event) -> str | None:
        result = self._toggle_listbox_item(event, self.co2_listbox, "_syncing_co2_listbox")
        if result == "break":
            if self._sync_vars_from_listbox(
                self.co2_listbox,
                self.co2_option_order,
                self.co2_check_vars,
                "_syncing_co2_listbox",
            ):
                self._refresh_execution_summary()
        return result

    def _on_pressure_listbox_click(self, event) -> str | None:
        result = self._toggle_listbox_item(event, self.pressure_listbox, "_syncing_pressure_listbox")
        if result == "break":
            if self._sync_vars_from_listbox(
                self.pressure_listbox,
                self.pressure_option_order,
                self.pressure_check_vars,
                "_syncing_pressure_listbox",
            ):
                self._refresh_execution_summary()
        return result

    def _layout_device_cards(self, columns: int) -> None:
        grid = getattr(self, "device_grid", None)
        order = getattr(self, "device_order", ())
        if grid is None or not order:
            return
        columns = max(1, columns)
        max_cols = max(columns, 5)
        for col in range(max_cols):
            grid.grid_columnconfigure(col, weight=0)
        for col in range(columns):
            grid.grid_columnconfigure(col, weight=1)
        for idx, key in enumerate(order):
            shell = self.device_shells.get(key)
            if shell is None:
                continue
            row = idx // columns
            col = idx % columns
            shell.grid_configure(
                row=row,
                column=col,
                sticky="nsew",
                padx=(0, self.ui_metrics["section_gap_x"]) if col < columns - 1 else 0,
                pady=(0, self.ui_metrics["section_gap_y"]) if idx < len(order) - columns else 0,
            )

    def _on_temp_scope_change(self) -> None:
        if self._in_temp_scope_change:
            return
        self._in_temp_scope_change = True
        try:
            all_scope = self.temp_scope_var.get().strip() == "全部温度点"
            if all_scope:
                for var in self.temp_check_vars.values():
                    var.set(True)
            temp_state = "disabled" if self._controls_locked() else "normal"
            for btn in self.temp_checkbuttons:
                try:
                    btn.configure(state=temp_state)
                except Exception:
                    pass
            if self.temp_listbox is not None:
                try:
                    self.temp_listbox.configure(state=temp_state)
                except Exception:
                    pass
            button_state = "disabled" if self._controls_locked() else "normal"
            self.temp_select_all_button.configure(state=button_state)
            self.temp_clear_button.configure(state=button_state)
            self._refresh_selector_button_styles()
            self._refresh_current_selection_summary()
            ready, text, level = self._compute_start_readiness()
            self.start_readiness_var.set(text)
            self._set_card_style(self.start_readiness_label, level)
            cfg_text = self.runtime_config_diff_var.get().removeprefix("配置差异：").strip() or "未对比"
            self.summary_cfg_card_var.set(f"配置状态\n{cfg_text}")
            self._sync_summary_card_display()
        finally:
            self._in_temp_scope_change = False

    def _on_route_mode_change(self) -> None:
        self._refresh_execution_summary()
        self._refresh_flow_help()

    def _fit_mode_text(self) -> str:
        return "开启" if self.fit_enabled_var.get() else "关闭，仅采集"

    def _on_fit_mode_change(self) -> None:
        self._refresh_execution_summary()

    def _postrun_delivery_text(self) -> str:
        return "开启" if self.postrun_delivery_var.get() else "关闭"

    def _on_postrun_delivery_change(self) -> None:
        self._refresh_execution_summary()

    def _temperature_order_descending(self) -> bool:
        selection = self.temperature_order_var.get().strip()
        if selection == "从低到高":
            return False
        if selection == "从高到低":
            return True
        cfg = getattr(self, "cfg", {})
        workflow_cfg = cfg.get("workflow", {}) if isinstance(cfg, dict) else {}
        return bool(workflow_cfg.get("temperature_descending", True))

    def _temperature_order_text(self) -> str:
        return "从高到低" if self._temperature_order_descending() else "从低到高"

    def _on_temperature_order_change(self) -> None:
        self._refresh_execution_summary()

    @staticmethod
    def _flow_help_parts(route_text: str) -> Tuple[str, str]:
        water_summary = "水路：温箱稳 -> 湿度稳 -> 开水路 4 分钟 -> 封压 -> 控压采样。"
        water_detail = (
            "水路详细：\n"
            "1. 温度箱到设定值后稳定。\n"
            "2. 湿度发生器到目标温湿度。\n"
            "3. 开总路阀、水路阀和旁路阀，开路等待 4 分钟。\n"
            "4. 关闭压力控制器通大气，等待 5 秒后封压。\n"
            "5. 按各压力点控压；压力控制器自身稳定后直接采 10 条，1 秒 1 条，并计算平均值。"
        )
        gas_summary = "气路：通标准气 120 秒 -> 判稳 -> 封压 -> 控压采样。"
        gas_detail = (
            "气路详细：\n"
            "1. 气路基线为：水路阀关、保压阀关、旁路阀关、总气路阀关。\n"
            "2. 打开总路阀、总气路阀、对应气路阀和标准气阀，通气等待 120 秒。\n"
            "3. 封压前 8 台分析仪同步判稳。\n"
            "4. 关闭压力控制器通大气，等待 5 秒后封压。\n"
            "5. 按当前温度点的全部压力值控压；压力控制器自身稳定后直接采 10 条，1 秒 1 条，并计算平均值。"
        )
        if route_text == "只测水路":
            return water_summary, water_detail
        if route_text == "只测气路":
            return gas_summary, gas_detail
        return (
            "先水后气：同温度组先水后气。",
            f"{water_detail}\n\n{gas_detail}",
        )

    def _toggle_flow_help(self) -> None:
        expanded = not self.flow_help_expanded.get()
        self.flow_help_expanded.set(expanded)
        self._refresh_flow_help()

    def _refresh_flow_help(self) -> None:
        route_text = self.route_mode_var.get().strip() or "先水后气"
        summary, detail = self._flow_help_parts(route_text)
        self.flow_help_summary_var.set(summary)
        expanded = self.flow_help_expanded.get()
        self.flow_help_toggle_button.configure(text="收起详细版" if expanded else "展开详细版")
        self.flow_help_var.set(detail if expanded else "")
        if expanded:
            if not self.flow_help_label.winfo_manager():
                self.flow_help_label.pack(fill="x", padx=5, pady=4)
            if not self.flow_cfg_meta.winfo_manager():
                self.flow_cfg_meta.pack(fill="x", pady=(5, 0))
            if not self.flow_selector_meta.winfo_manager():
                self.flow_selector_meta.pack(fill="x", pady=(5, 0))
        else:
            if self.flow_help_label.winfo_manager():
                self.flow_help_label.pack_forget()
            if self.flow_cfg_meta.winfo_manager():
                self.flow_cfg_meta.pack_forget()
            if self.flow_selector_meta.winfo_manager():
                self.flow_selector_meta.pack_forget()

    def _refresh_selector_hint_summary(self) -> None:
        temp_hint = (self.temp_hint_var.get() or "").strip()
        co2_hint = (self.co2_hint_var.get() or "").strip()
        pressure_hint = (self.pressure_hint_var.get() or "").strip()
        parts = [item for item in (temp_hint, co2_hint, pressure_hint) if item]
        self.selector_hint_var.set(" | ".join(parts) if parts else "温度点、气点和压力点将按默认规则自动加载")

    def _resolve_points_excel_candidates(self) -> List[Path]:
        cfg = getattr(self, "cfg", None)
        if not isinstance(cfg, dict):
            return []
        points_path = cfg.get("paths", {}).get("points_excel")
        if not points_path:
            return []
        base_dir = Path(cfg.get("_base_dir", "."))
        candidates: List[Path] = []
        for raw in (points_path, base_dir / "points.xlsx"):
            try:
                candidate = Path(raw)
                if not candidate.is_absolute():
                    candidate = (base_dir / candidate).resolve()
                else:
                    candidate = candidate.resolve()
            except Exception:
                continue
            if candidate.exists() and candidate not in candidates:
                candidates.append(candidate)
        return candidates

    def _load_points_for_ui(self) -> List[Any]:
        cfg = getattr(self, "cfg", None)
        if not isinstance(cfg, dict):
            return []
        workflow_cfg = cfg.get("workflow", {})
        cache_key = (
            tuple(str(path) for path in self._resolve_points_excel_candidates()),
            workflow_cfg.get("missing_pressure_policy", "require"),
            bool(workflow_cfg.get("carry_forward_h2o", False)),
        )
        if self._points_preview_cache_key == cache_key:
            return list(self._points_preview_cache)

        last_error: Exception | None = None
        loaded: List[Any] = []
        for candidate in self._resolve_points_excel_candidates():
            try:
                loaded = list(
                    load_points_from_excel(
                        candidate,
                        missing_pressure_policy=workflow_cfg.get("missing_pressure_policy", "require"),
                        carry_forward_h2o=bool(workflow_cfg.get("carry_forward_h2o", False)),
                    )
                )
                if loaded:
                    break
            except Exception as exc:
                last_error = exc
        if not loaded and last_error is not None:
            raise last_error
        self._points_preview_cache_key = cache_key
        self._points_preview_cache = list(loaded)
        return loaded

    @staticmethod
    def _point_is_h2o_preview(point: Any) -> bool:
        marker = getattr(point, "is_h2o_point", None)
        if marker is not None:
            try:
                return bool(marker)
            except Exception:
                pass
        return getattr(point, "hgen_temp_c", None) is not None and getattr(point, "hgen_rh_pct", None) is not None

    def _preview_point_enabled(self, point: Any) -> Tuple[bool, str]:
        temp_c = getattr(point, "temp_chamber_c", None)
        route_mode = self.route_mode_var.get().strip() or "先水后气"
        is_h2o = self._point_is_h2o_preview(point)
        if temp_c is None:
            return False, "温度未定义"
        try:
            temp_value = float(temp_c)
        except Exception:
            return False, "温度未定义"

        if self.temp_scope_var.get().strip() == "指定温度点":
            selected_temps = {float(value) for value in self._selected_temp_values()}
            if temp_value not in selected_temps:
                return False, "温度未选"

        if temp_value < 0.0:
            if is_h2o:
                return False, "子零度固定只气"
            if route_mode == "只测水路":
                return False, "当前模式只测水路"
            return True, "执行：子零度气路"

        pressure_value = getattr(point, "target_pressure_hpa", None)
        try:
            pressure_hpa = int(round(float(pressure_value)))
        except Exception:
            pressure_hpa = None
        if self.pressure_check_vars:
            selected_pressures = set(self._selected_pressure_values())
            ambient_selected = self._ambient_pressure_selected()
            point_pressure_mode = str(getattr(point, "_pressure_mode", "") or "").strip()
            if not selected_pressures and not ambient_selected:
                return False, "压力点未选"
            if point_pressure_mode == "ambient_open":
                if not ambient_selected:
                    return False, "压力点未选"
            elif pressure_hpa is not None and pressure_hpa not in selected_pressures:
                return False, "压力点未选"

        if is_h2o:
            if route_mode == "只测气路":
                return False, "当前模式只测气路"
            return True, "执行：水路"

        if route_mode == "只测水路":
            return False, "当前模式只测水路"
        ppm = getattr(point, "co2_ppm", None)
        if ppm is None:
            return False, "无气点"
        try:
            ppm_value = int(round(float(ppm)))
        except Exception:
            return False, "无气点"
        if self.co2_check_vars:
            selected_ppm = set(self._selected_co2_values())
            if ppm_value not in selected_ppm:
                return False, "气点未选"
        return True, "执行：气路"

    @staticmethod
    def _preview_point_values(point: Any) -> Tuple[str, str, str, str, str]:
        is_h2o = App._point_is_h2o_preview(point)
        route_text = "水路" if is_h2o else "气路"
        hgen_temp = getattr(point, "hgen_temp_c", None)
        hgen_rh = getattr(point, "hgen_rh_pct", None)
        if hgen_temp is not None or hgen_rh is not None:
            temp_text = f"{float(hgen_temp):g}°C" if hgen_temp is not None else "--"
            rh_text = f"{float(hgen_rh):g}%RH" if hgen_rh is not None else "--"
            hgen_text = f"{temp_text} / {rh_text}"
        else:
            hgen_text = "--"
        ppm = getattr(point, "co2_ppm", None)
        co2_text = "--" if is_h2o else (f"{int(round(float(ppm)))}ppm" if ppm is not None else "--")
        pressure = getattr(point, "target_pressure_hpa", None)
        pressure_label = getattr(point, "_pressure_target_label", None)
        if pressure_label:
            pressure_text = str(pressure_label)
        else:
            pressure_text = f"{int(round(float(pressure)))}hPa" if pressure is not None else "--"
        group = str(getattr(point, "co2_group", "") or "").strip().upper()
        group_text = "--" if is_h2o else (group or "--")
        return route_text, hgen_text, co2_text, pressure_text, group_text

    @staticmethod
    def _preview_point_pressure_value(point: Any) -> float:
        pressure = getattr(point, "target_pressure_hpa", None)
        try:
            return float(pressure)
        except Exception:
            return float("-inf")

    @staticmethod
    def _preview_co2_group_rank(point: Any) -> int:
        group = str(getattr(point, "co2_group", "") or "").strip().upper()
        return 1 if group == "B" else 0

    def _preview_points_in_execution_order(self, points: List[Any]) -> List[Any]:
        cfg = getattr(self, "cfg", None)
        if not isinstance(cfg, dict):
            return list(points)
        if not points:
            return []
        required_attrs = ("temp_chamber_c", "target_pressure_hpa")
        for point in points:
            if any(not hasattr(point, attr) for attr in required_attrs):
                return list(points)

        try:
            ordered_points = reorder_points(
                list(points),
                0.0,
                descending_temperatures=self._temperature_order_descending(),
            )
        except Exception:
            ordered_points = list(points)

        preview_cfg = copy.deepcopy(cfg)
        workflow_cfg = preview_cfg.setdefault("workflow", {})
        route_mode_map = {
            "只测气路": "co2_only",
            "只测水路": "h2o_only",
            "先水后气": "h2o_then_co2",
        }
        workflow_cfg["route_mode"] = route_mode_map.get(self.route_mode_var.get().strip(), "h2o_then_co2")
        selected_pressure_tokens = self._selected_pressure_tokens()
        all_pressures = sorted((int(value) for value in self.pressure_check_vars.keys()), reverse=True)
        if selected_pressure_tokens and (
            self._ambient_pressure_selected()
            or self._selected_pressure_values() != all_pressures
        ):
            workflow_cfg["selected_pressure_points"] = list(selected_pressure_tokens)
        else:
            workflow_cfg.pop("selected_pressure_points", None)

        runner = CalibrationRunner(
            preview_cfg,
            {},
            getattr(self, "logger", None),
            lambda *_args, **_kwargs: None,
            lambda *_args, **_kwargs: None,
        )
        ordered_preview: List[Any] = []
        for temp_group in runner._group_points_by_temperature(ordered_points):
            if not temp_group:
                continue
            temp_value = getattr(temp_group[0], "temp_chamber_c", None)
            try:
                is_subzero = float(temp_value) < 0.0
            except Exception:
                is_subzero = False

            try:
                h2o_points = [point for point in temp_group if self._point_is_h2o_preview(point)]
                if not is_subzero:
                    h2o_pressure_points = runner._h2o_pressure_points_for_temperature(temp_group)
                    for h2o_group in runner._group_h2o_points(h2o_points):
                        if not h2o_group:
                            continue
                        lead = h2o_group[0]
                        ordered_preview.extend(
                            [
                                runner._build_h2o_pressure_point(lead, pressure_point)
                                for pressure_point in h2o_pressure_points
                            ]
                        )

                gas_sources = runner._co2_source_points(temp_group)
                co2_pressure_points = runner._co2_pressure_points_for_temperature(temp_group)
                for source_point in gas_sources:
                    ordered_preview.extend(
                        [
                            runner._build_co2_pressure_point(source_point, pressure_point)
                            for pressure_point in co2_pressure_points
                        ]
                    )
            except Exception:
                return list(points)
        return ordered_preview

    def _refresh_points_preview(self) -> None:
        tree = self.points_tree
        if tree is None:
            return
        try:
            points = self._load_points_for_ui()
        except Exception as exc:
            for item in tree.get_children():
                tree.delete(item)
            self.points_preview_hint_var.set(f"点表预览加载失败：{exc}")
            if hasattr(self, "selector_tabs"):
                self.selector_tabs.tab(self.points_tab, text="点表预览")
            return

        for item in tree.get_children():
            tree.delete(item)

        preview_points = self._preview_points_in_execution_order(points)
        run_count = 0
        skip_count = 0
        for seq, point in enumerate(preview_points, start=1):
            enabled, status = self._preview_point_enabled(point)
            route_text, hgen_text, co2_text, pressure_text, group_text = self._preview_point_values(point)
            row_id = getattr(point, "index", "--")
            temp_text = f"{float(getattr(point, 'temp_chamber_c', 0.0)):g}°C"
            status_text = status if enabled else f"跳过：{status}"
            tree.insert(
                "",
                "end",
                values=(
                    str(seq),
                    row_id,
                    temp_text,
                    route_text,
                    hgen_text,
                    co2_text,
                    pressure_text,
                    group_text,
                    status_text,
                ),
                tags=("run" if enabled else "skip",),
            )
            if enabled:
                run_count += 1
            else:
                skip_count += 1

        self.points_preview_hint_var.set(
            f"点表 {len(points)} 行 | 按真实执行顺序预览 | 本轮执行 {run_count} | 将跳过 {skip_count}"
        )
        if hasattr(self, "selector_tabs"):
            self.selector_tabs.tab(self.points_tab, text=f"点表预览 ({len(points)})")

    def _open_points_excel(self) -> None:
        candidates = self._resolve_points_excel_candidates()
        if not candidates:
            messagebox.showinfo("提示", "当前未找到点表文件。")
            return
        self._open_path(candidates[0])

    def _refresh_temperature_options(self) -> None:
        self.temp_check_vars = {}
        self.temp_option_order = []
        self.temp_checkbuttons.clear()

        try:
            points_path = self.cfg.get("paths", {}).get("points_excel")
            if not points_path:
                raise ValueError("未配置校准点文件")
            workflow_cfg = self.cfg.get("workflow", {})
            candidates: List[Path] = []
            for raw in (points_path, Path(self.cfg.get("_base_dir", ".")) / "points.xlsx"):
                try:
                    candidate = Path(raw).resolve()
                except Exception:
                    continue
                if candidate not in candidates and candidate.exists():
                    candidates.append(candidate)
            if not candidates:
                raise FileNotFoundError(f"未找到点表文件：{points_path}")
            last_error: Exception | None = None
            points = None
            for candidate in candidates:
                try:
                    points = load_points_from_excel(
                        candidate,
                        missing_pressure_policy=workflow_cfg.get("missing_pressure_policy", "require"),
                        carry_forward_h2o=bool(workflow_cfg.get("carry_forward_h2o", False)),
                    )
                    break
                except Exception as exc:
                    last_error = exc
            if points is None:
                raise last_error or RuntimeError("点表加载失败")
            temps = sorted(
                {
                    round(float(p.temp_chamber_c), 6)
                    for p in points
                    if p.temp_chamber_c is not None
                }
            )
        except Exception as exc:
            self.temp_hint_var.set(f"温度点加载失败：{exc}")
            self._refresh_selector_hint_summary()
            return

        if not temps:
            self.temp_hint_var.set("点表中未解析到温度点")
            self._refresh_selector_hint_summary()
            return

        self.temp_hint_var.set("可单选、多选或全选；仅在“指定温度点”时生效")
        for temp in temps:
            var = tk.BooleanVar(value=True)
            self.temp_check_vars[temp] = var
            self.temp_option_order.append(temp)
        if self.temp_listbox is not None:
            self._sync_temp_listbox_from_vars()
        elif self.temp_checks_inner is not None:
            self._rebuild_checks_panel(
                self.temp_checks_inner,
                self.temp_checkbuttons,
                [(f"{float(temp):g}°C", self.temp_check_vars[temp]) for temp in self.temp_option_order],
                self._refresh_execution_summary,
                columns=5,
            )
        self.temp_hint_var.set(f"已加载 {len(temps)} 个温度点，可单选、多选或全选")
        if hasattr(self, "selector_tabs"):
            self.selector_tabs.tab(self.temp_tab, text=f"温度点 ({len(temps)})")

        self._refresh_selector_hint_summary()
        self._on_temp_scope_change()
        self._refresh_execution_summary()
        self._apply_control_lock()
        self._refresh_selector_button_styles()

    def _selected_temp_values(self) -> List[float]:
        return [float(temp) for temp, var in sorted(self.temp_check_vars.items()) if var.get()]

    def _select_all_temps(self) -> None:
        self._ensure_specific_temp_scope_for_manual_selection()
        for var in self.temp_check_vars.values():
            var.set(True)
        self._sync_temp_listbox_from_vars()
        self._refresh_execution_summary()

    def _clear_all_temps(self) -> None:
        self._ensure_specific_temp_scope_for_manual_selection()
        for var in self.temp_check_vars.values():
            var.set(False)
        self._sync_temp_listbox_from_vars()
        self._refresh_execution_summary()

    def _ensure_specific_temp_scope_for_manual_selection(self) -> None:
        if self.temp_scope_var.get().strip() == "全部温度点":
            self.temp_scope_var.set("指定温度点")

    def _selected_temps_text(self) -> str:
        if self.temp_scope_var.get().strip() == "全部温度点":
            return "全部温度点"
        temps = [f"{temp:g}°C" for temp, var in sorted(self.temp_check_vars.items()) if var.get()]
        return "、".join(temps) if temps else "未选择温度点"

    def _compact_temps_text(self) -> str:
        if self.temp_scope_var.get().strip() == "全部温度点":
            total = len(self.temp_option_order)
            return f"全部 {total}项" if total else "全部温度点"
        selected = [f"{temp:g}°C" for temp, var in sorted(self.temp_check_vars.items()) if var.get()]
        if not selected:
            return "未选择"
        if len(selected) <= 2:
            return " / ".join(selected)
        return f"{len(selected)}项"

    def _refresh_co2_options(self) -> None:
        self.co2_check_vars = {}
        self.co2_option_order = []
        self.co2_checkbuttons.clear()

        workflow_cfg = self.cfg.get("workflow", {}) if hasattr(self, "cfg") else {}
        skip_ppm = {
            int(item)
            for item in workflow_cfg.get("skip_co2_ppm", [])
            if isinstance(item, (int, float, str)) and str(item).strip()
        }

        ppm_values: set[int] = set()
        source_hint = "点表"

        def _load_ppm_from_valves() -> set[int]:
            valves_cfg = self.cfg.get("valves", {}) if hasattr(self, "cfg") else {}
            fallback_values: set[int] = set()
            for map_name in ("co2_map", "co2_map_group2"):
                one_map = valves_cfg.get(map_name, {})
                if not isinstance(one_map, dict):
                    continue
                for key in one_map.keys():
                    try:
                        fallback_values.add(int(float(key)))
                    except Exception:
                        continue
            return fallback_values

        try:
            points_path = self.cfg.get("paths", {}).get("points_excel")
            if not points_path:
                raise ValueError("未配置校准点文件")
            candidates: List[Path] = []
            for raw in (points_path, Path(self.cfg.get("_base_dir", ".")) / "points.xlsx"):
                try:
                    candidate = Path(raw).resolve()
                except Exception:
                    continue
                if candidate not in candidates and candidate.exists():
                    candidates.append(candidate)
            if not candidates:
                raise FileNotFoundError(f"未找到点表文件：{points_path}")
            last_error: Exception | None = None
            points = None
            for candidate in candidates:
                try:
                    points = load_points_from_excel(
                        candidate,
                        missing_pressure_policy=workflow_cfg.get("missing_pressure_policy", "require"),
                        carry_forward_h2o=bool(workflow_cfg.get("carry_forward_h2o", False)),
                    )
                    break
                except Exception as exc:
                    last_error = exc
            if points is None:
                raise last_error or RuntimeError("点表加载失败")
            try:
                ordered_points = reorder_points(
                    list(points),
                    0.0,
                    descending_temperatures=self._temperature_order_descending(),
                )
                runner = CalibrationRunner(
                    self.cfg,
                    {},
                    getattr(self, "logger", None),
                    lambda *_args, **_kwargs: None,
                    lambda *_args, **_kwargs: None,
                )
                for temp_group in runner._group_points_by_temperature(ordered_points):
                    for point in runner._co2_source_points(temp_group):
                        try:
                            ppm = getattr(point, "co2_ppm", None)
                            if ppm is None:
                                continue
                            ppm_values.add(int(round(float(ppm))))
                        except Exception:
                            continue
            except Exception:
                ppm_values.clear()

            if not ppm_values:
                for point in points:
                    try:
                        ppm = getattr(point, "co2_ppm", None)
                        if ppm is None:
                            continue
                        ppm_values.add(int(round(float(ppm))))
                    except Exception:
                        continue
            if not ppm_values:
                ppm_values = _load_ppm_from_valves()
                source_hint = "阀门配置"
        except Exception:
            source_hint = "阀门配置"
            ppm_values = _load_ppm_from_valves()

        if not ppm_values:
            self.co2_hint_var.set("未从点表或阀门配置解析到气点")
            self._refresh_selector_hint_summary()
            return

        self.co2_hint_var.set(f"勾选本次要跑的标气点；未勾选的点将自动跳过（来源：{source_hint}）")
        for ppm in sorted(ppm_values):
            var = tk.BooleanVar(value=ppm not in skip_ppm)
            self.co2_check_vars[ppm] = var
            self.co2_option_order.append(ppm)
        if self.co2_listbox is not None:
            self._sync_co2_listbox_from_vars()
        elif self.co2_checks_inner is not None:
            self._rebuild_checks_panel(
                self.co2_checks_inner,
                self.co2_checkbuttons,
                [(f"{int(ppm)}ppm", self.co2_check_vars[ppm]) for ppm in self.co2_option_order],
                self._refresh_execution_summary,
                columns=5,
            )
        self.co2_hint_var.set(f"已加载 {len(ppm_values)} 个气点，未勾选项会自动跳过（来源：{source_hint}）")
        if hasattr(self, "selector_tabs"):
            self.selector_tabs.tab(self.co2_tab, text=f"气点 ({len(ppm_values)})")
        self._refresh_selector_hint_summary()
        self._refresh_execution_summary()
        self._apply_control_lock()
        self._refresh_selector_button_styles()

    def _selected_co2_values(self) -> List[int]:
        return [int(ppm) for ppm, var in sorted(self.co2_check_vars.items()) if var.get()]

    def _select_all_co2(self) -> None:
        for var in self.co2_check_vars.values():
            var.set(True)
        self._sync_co2_listbox_from_vars()
        self._refresh_execution_summary()

    def _clear_all_co2(self) -> None:
        for var in self.co2_check_vars.values():
            var.set(False)
        self._sync_co2_listbox_from_vars()
        self._refresh_execution_summary()

    def _selected_co2_text(self) -> str:
        if not self.co2_check_vars:
            return "--"
        selected = [f"{ppm}ppm" for ppm in self._selected_co2_values()]
        return "、".join(selected) if selected else "未选择气点"

    def _compact_co2_text(self) -> str:
        if not self.co2_check_vars:
            return "--"
        selected = [f"{ppm}ppm" for ppm in self._selected_co2_values()]
        if not selected:
            return "未选择"
        if len(selected) <= 3:
            return " / ".join(selected)
        return f"{len(selected)}项"

    def _refresh_pressure_options(self) -> None:
        self.pressure_check_vars = {}
        self.pressure_option_order = []
        self.pressure_checkbuttons.clear()

        workflow_cfg = self.cfg.get("workflow", {}) if hasattr(self, "cfg") else {}
        all_pressures = [1100, 1000, 900, 800, 700, 600, 500]
        allowed_pressures = set(all_pressures)
        selected_raw = workflow_cfg.get("selected_pressure_points")
        selected_values: set[int] = set()
        ambient_selected = False
        if isinstance(selected_raw, list):
            for item in selected_raw:
                if self._is_ambient_pressure_token(item):
                    ambient_selected = True
                    continue
                try:
                    selected_values.add(int(round(float(item))))
                except Exception:
                    continue
        self.ambient_pressure_var.set(ambient_selected)
        invalid_selected = sorted((value for value in selected_values if value not in allowed_pressures), reverse=True)
        if invalid_selected:
            invalid_text = "、".join(f"{value}hPa" for value in invalid_selected)
            allowed_text = "、".join([AMBIENT_PRESSURE_LABEL] + [f"{value}hPa" for value in all_pressures])
            self.log(f"压力点配置非法：{invalid_text}；允许值：{allowed_text}")
            self.pressure_hint_var.set(f"配置中的压力点非法：{invalid_text}；允许值：{allowed_text}")
        else:
            self.pressure_hint_var.set("勾选本次要跑的标准压力点；当前大气压通过独立开路采样选项控制")
        for pressure_hpa in all_pressures:
            enabled = (not selected_values) or (pressure_hpa in selected_values)
            var = tk.BooleanVar(value=enabled)
            self.pressure_check_vars[pressure_hpa] = var
            self.pressure_option_order.append(pressure_hpa)
        if self.pressure_listbox is not None:
            self._sync_pressure_listbox_from_vars()
        elif self.pressure_checks_inner is not None:
            self._rebuild_checks_panel(
                self.pressure_checks_inner,
                self.pressure_checkbuttons,
                [(f"{int(pressure_hpa)}hPa", self.pressure_check_vars[pressure_hpa]) for pressure_hpa in self.pressure_option_order],
                self._refresh_execution_summary,
                columns=4,
            )
        if not invalid_selected:
            self.pressure_hint_var.set("已加载 7 个标准压力点，可单选、多选或全选；可额外勾选当前大气压")
        if hasattr(self, "selector_tabs"):
            self.selector_tabs.tab(self.pressure_tab, text=f"压力点 ({len(all_pressures)})")
        self._refresh_selector_hint_summary()
        self._refresh_execution_summary()
        self._apply_control_lock()
        self._refresh_selector_button_styles()

    @staticmethod
    def _is_ambient_pressure_token(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        compact = re.sub(r"\s+", "", value.strip().lower())
        return compact in {AMBIENT_PRESSURE_TOKEN, "当前大气压", "大气压"}

    def _ambient_pressure_selected(self) -> bool:
        return bool(self.ambient_pressure_var.get())

    def _selected_pressure_tokens(self) -> List[Any]:
        selected: List[Any] = []
        if self._ambient_pressure_selected():
            selected.append(AMBIENT_PRESSURE_TOKEN)
        selected.extend(self._selected_pressure_values())
        return selected

    def _on_ambient_pressure_change(self) -> None:
        self._refresh_execution_summary()

    def _selected_pressure_values(self) -> List[int]:
        return [int(pressure_hpa) for pressure_hpa, var in sorted(self.pressure_check_vars.items(), reverse=True) if var.get()]

    def _select_all_pressures(self) -> None:
        for var in self.pressure_check_vars.values():
            var.set(True)
        self._sync_pressure_listbox_from_vars()
        self._refresh_execution_summary()

    def _clear_all_pressures(self) -> None:
        for var in self.pressure_check_vars.values():
            var.set(False)
        self._sync_pressure_listbox_from_vars()
        self._refresh_execution_summary()

    def _selected_pressure_text(self) -> str:
        if not self.pressure_check_vars and not self._ambient_pressure_selected():
            return "--"
        selected: List[str] = []
        if self._ambient_pressure_selected():
            selected.append(AMBIENT_PRESSURE_LABEL)
        selected.extend(f"{value}hPa" for value in self._selected_pressure_values())
        return "、".join(selected) if selected else "未选择压力点"

    def _compact_pressure_text(self) -> str:
        if not self.pressure_check_vars and not self._ambient_pressure_selected():
            return "--"
        selected: List[str] = []
        if self._ambient_pressure_selected():
            selected.append(AMBIENT_PRESSURE_LABEL)
        selected.extend(f"{value}hPa" for value in self._selected_pressure_values())
        if not selected:
            return "未选择"
        if len(selected) <= 3:
            return " / ".join(selected)
        return f"{len(selected)}项"

    def _refresh_current_selection_summary(self) -> None:
        route_text = self.route_mode_var.get().strip() or "先水后气"
        fit_text = self._fit_mode_text()
        temp_text = self._selected_temps_text()
        co2_text = self._selected_co2_text()
        pressure_text = self._selected_pressure_text()
        self.current_selection_var.set(
            f"当前选择：{route_text} | 拟合：{fit_text} | 温度：{temp_text} | 气点：{co2_text} | 压力点：{pressure_text}"
        )
        self.summary_mode_card_var.set(f"测量模式\n{route_text}")
        self.summary_temp_card_var.set(f"温度点\n{self._compact_temps_text()}")
        self.summary_gas_card_var.set(f"气点/压力\n{self._compact_co2_text()} | {self._compact_pressure_text()}")
        self._sync_summary_card_display()

    def _sync_summary_card_display(self) -> None:
        mapping = {
            "summary_mode_card_var": self.summary_mode_card_var.get(),
            "summary_temp_card_var": self.summary_temp_card_var.get(),
            "summary_gas_card_var": self.summary_gas_card_var.get(),
            "summary_cfg_card_var": self.summary_cfg_card_var.get(),
        }
        for key, raw in mapping.items():
            title, _, value = raw.partition("\n")
            title_var = self.summary_card_title_vars.get(key)
            value_var = self.summary_card_value_vars.get(key)
            if title_var is not None:
                title_var.set(title.strip() or "--")
            if value_var is not None:
                value_var.set(value.strip() or title.strip() or "--")

    def _compute_start_readiness(self) -> Tuple[bool, str, str]:
        route_text = self.route_mode_var.get().strip() or "先水后气"
        if not hasattr(self, "cfg"):
            return False, "启动校验：未加载配置", "warn"

        if self.temp_scope_var.get().strip() == "指定温度点":
            selected_temps = self._selected_temp_values()
            if not selected_temps:
                return False, "启动校验：请至少勾选一个温度点", "warn"

        if route_text in {"只测气路", "先水后气"}:
            if not self.co2_check_vars:
                return False, "启动校验：当前没有可用气点", "warn"
            selected_ppm = self._selected_co2_values()
            if not selected_ppm:
                return False, "启动校验：请至少勾选一个气点", "warn"

        if self.pressure_check_vars:
            selected_pressures = self._selected_pressure_values()
            if not selected_pressures and not self._ambient_pressure_selected():
                return False, "启动校验：请至少勾选一个压力点", "warn"

        return True, "启动校验：就绪", "ok"

    def _refresh_execution_summary(self) -> None:
        route_text = self.route_mode_var.get().strip() or "先水后气"
        fit_text = self._fit_mode_text()
        order_text = self._temperature_order_text()
        temp_text = self._selected_temps_text()
        co2_text = self._selected_co2_text()
        pressure_text = self._selected_pressure_text()
        self.config_file_brief_var.set(f"配置：{Path(self.config_path.get()).name}")
        self.route_mode_brief_var.set(f"模式：{route_text}")
        self.fit_mode_brief_var.set(f"拟合：{fit_text}")
        self.temp_scope_brief_var.set(f"范围：{self.temp_scope_var.get().strip() or '全部温度点'}")
        self.temperature_order_brief_var.set(f"顺序：{order_text}")
        self.summary_var.set(
            f"执行摘要：{route_text} | 拟合：{fit_text} | 顺序：{order_text} | 温度：{temp_text} | 气点：{co2_text} | 压力点：{pressure_text}"
        )
        self.startup_summary_var.set(
            f"测量模式：{route_text} | 校准拟合：{fit_text} | 温度顺序：{order_text} | 温度：{temp_text} | 气点：{co2_text} | 压力点：{pressure_text}"
        )
        self._refresh_current_selection_summary()
        ready, text, level = self._compute_start_readiness()
        self.start_readiness_var.set(text)
        self._set_card_style(self.start_readiness_label, level)
        cfg_text = self.runtime_config_diff_var.get().removeprefix("配置差异：").strip() or "未对比"
        self.summary_cfg_card_var.set(f"配置状态\n{cfg_text}")
        self._sync_summary_card_display()
        self._refresh_selector_button_styles()
        self._refresh_points_preview()
        self._apply_control_lock()
        self._refresh_points_preview()

    def _build_start_confirmation_text(self, runtime_cfg: Dict[str, Any]) -> str:
        workflow = runtime_cfg.get("workflow", {})
        coeff_cfg = runtime_cfg.get("coefficients", {}) if isinstance(runtime_cfg.get("coefficients", {}), dict) else {}
        route_mode = str(workflow.get("route_mode", "h2o_then_co2"))
        route_text_map = {
            "h2o_then_co2": "先水后气",
            "h2o_only": "只测水路",
            "co2_only": "只测气路",
        }
        route_text = route_text_map.get(route_mode, route_mode)
        fit_text = "开启" if (not bool(workflow.get("collect_only", False)) and bool(coeff_cfg.get("fit_h2o", True))) else "关闭，仅采集"
        order_text = "从高到低" if bool(workflow.get("temperature_descending", True)) else "从低到高"

        selected_temps = workflow.get("selected_temps_c")
        if isinstance(selected_temps, list) and selected_temps:
            temp_text = "、".join(f"{float(temp):g}°C" for temp in selected_temps)
        else:
            temp_text = "全部温度点"

        skip_ppm = {
            int(item)
            for item in workflow.get("skip_co2_ppm", [])
            if isinstance(item, (int, float, str)) and str(item).strip()
        }
        all_ppm = sorted(self.co2_check_vars.keys())
        selected_ppm = [f"{ppm}ppm" for ppm in all_ppm if ppm not in skip_ppm]
        skipped_ppm = [f"{ppm}ppm" for ppm in all_ppm if ppm in skip_ppm]
        selected_pressures_raw = workflow.get("selected_pressure_points")
        if isinstance(selected_pressures_raw, list) and selected_pressures_raw:
            pressure_parts: List[str] = []
            pressure_values = set()
            for item in selected_pressures_raw:
                if self._is_ambient_pressure_token(item):
                    pressure_parts.append(AMBIENT_PRESSURE_LABEL)
                    continue
                if not isinstance(item, (int, float, str)) or not str(item).strip():
                    continue
                try:
                    pressure_values.add(int(round(float(item))))
                except Exception:
                    continue
            pressure_parts.extend(f"{value}hPa" for value in sorted(pressure_values, reverse=True))
            pressure_text = "、".join(pressure_parts) if pressure_parts else "未选择"
        else:
            pressure_text = "全部压力点"

        lines = [
            "即将开始本次校准流程：",
            f"流程模式：{route_text}",
            f"校准拟合：{fit_text}",
            f"温度顺序：{order_text}",
            f"温度点：{temp_text}",
            f"气点：{'、'.join(selected_ppm) if selected_ppm else '未选择'}",
            f"跳过气点：{'、'.join(skipped_ppm) if skipped_ppm else '无'}",
            f"压力点：{pressure_text}",
        ]
        return "\n".join(lines)

    @staticmethod
    def _tail_file_lines(path: Path, count: int) -> List[str]:
        if count <= 0:
            return []
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            if file_size <= 0:
                return []
            block_size = 8192
            buffer = b""
            position = file_size
            # Read slightly past the requested line count so we can safely
            # drop a possibly partial first line after seeking into the tail.
            target_newlines = max(2, count + 2)
            while position > 0 and buffer.count(b"\n") < target_newlines:
                read_size = min(block_size, position)
                position -= read_size
                f.seek(position)
                buffer = f.read(read_size) + buffer
            text = buffer.decode("utf-8", errors="replace")
        return text.splitlines()[-count:]

    @staticmethod
    def _tail_csv_rows(path: Path, count: int = 120) -> List[Dict[str, str]]:
        if count <= 0:
            return []
        try:
            with path.open("rb") as f:
                header_line = f.readline().decode("utf-8", errors="replace").rstrip("\r\n")
            if not header_line:
                return []
            tail_lines = App._tail_file_lines(path, count=count + 8)
            if not tail_lines:
                return []
            if tail_lines and tail_lines[0] == header_line:
                data_lines = tail_lines[1:]
            else:
                # The first tail line may be a partial CSV row because we seek
                # into the middle of the file. Drop it and parse only complete rows.
                data_lines = tail_lines[1:] if len(tail_lines) > 1 else []
            if not data_lines:
                return []
            sample = header_line + "\n" + "\n".join(data_lines) + "\n"
            reader = csv.DictReader(io.StringIO(sample))
            rows = [
                row
                for row in reader
                if any(str(value or "").strip() for value in row.values())
            ]
            return rows[-count:]
        except Exception:
            rows: deque[Dict[str, str]] = deque(maxlen=count)
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
            return list(rows)

    @staticmethod
    def _tail_text_lines(path: Path, count: int = 200) -> List[str]:
        if count <= 0:
            return []
        try:
            return App._tail_file_lines(path, count)
        except Exception:
            lines: deque[str] = deque(maxlen=count)
            with path.open("r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    lines.append(line.rstrip("\r\n"))
            return list(lines)

    @staticmethod
    def _extract_key_events(lines: List[str], count: int = 12) -> List[str]:
        patterns = (
            "Temperature group ",
            "CO2 route opened;",
            "H2O route opened;",
            "Pressure controller vent=OFF",
            "Pressure in-limits at target",
            "Point ",
            " skipped:",
            "FAIL ",
            "INVALID_RESPONSE",
            "STARTUP_NO_ACK",
            "NO_ACK",
            "timeout",
            "Run finished",
            "恢复基线",
        )
        events: deque[str] = deque(maxlen=count)
        for line in lines:
            text = line.strip()
            if text and any(pattern in text for pattern in patterns):
                events.append(text)
        return list(events)

    @staticmethod
    def _latest_io_path(run_dir: Path) -> Path | None:
        candidates = sorted(run_dir.glob("io_*.csv"))
        if not candidates:
            return None
        return max(candidates, key=lambda path: path.stat().st_mtime)

    @staticmethod
    def _parse_row_timestamp(row: Dict[str, str]) -> datetime | None:
        text = str(row.get("ts", "") or row.get("timestamp", "") or "").strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _pressure_setpoint_command_prefixes(prefix: str) -> Tuple[str, ...]:
        normalized = str(prefix or "").strip().upper()
        if normalized in {":SOUR:PRES", ":SOUR:PRES:LEV:IMM:AMPL"}:
            return (":SOUR:PRES", ":SOUR:PRES:LEV:IMM:AMPL")
        return (str(prefix or "").strip(),)

    @staticmethod
    def _parse_pressure_setpoint_command(command: str, prefix: str = ":SOUR:PRES") -> Tuple[float | None, str | None]:
        text = str(command or "").strip()
        for candidate in App._pressure_setpoint_command_prefixes(prefix):
            m = re.match(re.escape(candidate) + r"\s*(-?\d+(?:\.\d+)?)$", text)
            if m:
                return float(m.group(1)), candidate
        return None, None

    @staticmethod
    def _parse_last_numeric_command(rows: List[Dict[str, str]], port: str, prefix: str) -> float | None:
        for row in reversed(rows):
            if str(row.get("port", "")).strip() != port:
                continue
            if str(row.get("direction", "")).strip().upper() != "TX":
                continue
            command = str(row.get("command", "") or "").strip()
            value, _matched_prefix = App._parse_pressure_setpoint_command(command, prefix)
            if value is not None:
                return value
        return None

    @staticmethod
    def _parse_pressure_reapply_info(rows: List[Dict[str, str]]) -> Tuple[int | None, int]:
        last_target: int | None = None
        total_count = 0
        for row in reversed(rows):
            if str(row.get("port", "")).strip() != "COM31":
                continue
            if str(row.get("direction", "")).strip().upper() != "TX":
                continue
            command = str(row.get("command", "") or "").strip()
            value, _matched_prefix = App._parse_pressure_setpoint_command(command)
            if value is None:
                continue
            target = int(round(float(value)))
            if last_target is None:
                last_target = target
                total_count = 1
                continue
            if target == last_target:
                total_count += 1
                continue
            break
        if last_target is None:
            return None, 0
        return last_target, max(0, total_count - 1)

    @staticmethod
    def _parse_dewpoint_frame(response: str) -> Dict[str, float]:
        text = str(response or "").strip()
        if "_GetCurData_" not in text or "_END" not in text:
            return {}

        payload_text = text.split("_GetCurData_", 1)[1].rsplit("_END", 1)[0]
        payload = payload_text.split("_")

        def _to_float(index: int) -> float | None:
            if index >= len(payload):
                return None
            try:
                return float(payload[index])
            except Exception:
                return None

        parsed: Dict[str, float] = {}
        dewpoint_c = _to_float(0)
        temp_c = _to_float(1)
        rh_pct = _to_float(7)
        if dewpoint_c is not None:
            parsed["dewpoint_c"] = dewpoint_c
        if temp_c is not None:
            parsed["temp_c"] = temp_c
        if rh_pct is not None:
            parsed["rh_pct"] = rh_pct
        return parsed

    @staticmethod
    def _compute_online_state(
        timestamp_text: str,
        payload_text: str,
        activity_timestamp_text: str = "--",
    ) -> Tuple[str, str]:
        payload = (payload_text or "").strip()

        def _parse_ts(raw: str) -> datetime | None:
            text = (raw or "").strip()
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(text, fmt)
                except ValueError:
                    continue
            return None

        ts = _parse_ts(timestamp_text)
        activity_ts = _parse_ts(activity_timestamp_text)
        ref_ts = ts or activity_ts

        if payload and not (payload.endswith("：--") or payload.endswith(": --") or payload.endswith("--")):
            if ts is None:
                return "◔ 在线", "warn"
            age_s = max(0, int((datetime.now() - ts).total_seconds()))
            if age_s <= 15:
                return "● 在线", "ok"
            if age_s <= 120:
                return "◔ 数据较旧", "warn"
            return "◑ 本阶段未轮询", "idle"

        if ref_ts is None:
            return "○ 未读取", "idle"
        age_s = max(0, int((datetime.now() - ref_ts).total_seconds()))
        if age_s <= 30:
            return "◔ 在线", "warn"
        if age_s <= 300:
            return "◑ 本阶段未轮询", "idle"
        return "○ 未读取", "idle"

    @staticmethod
    def _build_device_issue_summaries(rows: List[Dict[str, str]], seconds: int = 60) -> Dict[str, Dict[str, str]]:
        key_by_port = {
            "COM31": "pace",
            "COM30": "gauge",
            "COM27": "chamber",
            "COM24": "hgen",
            "COM25": "dewpoint",
        }
        latest_ts: datetime | None = None
        for row in reversed(rows):
            latest_ts = App._parse_row_timestamp(row)
            if latest_ts is not None:
                break
        cutoff = latest_ts - timedelta(seconds=seconds) if latest_ts is not None else None
        result: Dict[str, Dict[str, str]] = {
            key: {"text": "异常摘要：无", "level": "idle", "timestamp": "--"}
            for key in ("pace", "gauge", "chamber", "hgen", "dewpoint")
        }
        for row in reversed(rows):
            ts = App._parse_row_timestamp(row)
            if cutoff is not None and ts is not None and ts < cutoff:
                break
            port = str(row.get("port", "") or "").strip()
            key = key_by_port.get(port)
            if key is None:
                continue
            error = str(row.get("error", "") or "").strip()
            response = str(row.get("response", "") or "").strip()
            issue = ""
            level = "idle"
            if error:
                issue = error
                level = "error"
            elif "STARTUP_NO_ACK" in response:
                issue = f"启动告警：{response}"
                level = "info"
            elif any(token in response for token in ("INVALID_RESPONSE", "NO_ACK", "NO_RESPONSE", "timeout", "FAIL")):
                issue = response
                level = "warn" if "NO_ACK" in response or "timeout" in response else "error"
            if issue:
                ts_text = ts.strftime("%Y-%m-%d %H:%M:%S") if ts is not None else "--"
                result[key] = {"text": f"异常摘要：{issue[:48]}", "level": level, "timestamp": ts_text}
        return result

    @staticmethod
    def _latest_humidity_generator_state(rows: List[Dict[str, str]]) -> Dict[str, float] | None:
        for row in reversed(rows):
            if str(row.get("port", "")).strip() != "COM24":
                continue
            if str(row.get("direction", "")).strip().upper() != "RX":
                continue
            response = str(row.get("response", "") or "").strip()
            if "Uw=" not in response or "Tc=" not in response:
                continue
            out: Dict[str, float] = {}
            for key in ("Uw", "Tc", "Ts", "Td", "Flux"):
                m = re.search(rf"{key}=\s*(-?\d+(?:\.\d+)?)", response)
                if m:
                    out[key] = float(m.group(1))
            if out:
                return out
        return None

    @staticmethod
    def _replay_relay_states(rows: List[Dict[str, str]], port: str, channels: int) -> List[bool | None]:
        states: List[bool | None] = [None] * channels
        for row in rows:
            if str(row.get("port", "")).strip() != port:
                continue
            if str(row.get("direction", "")).strip().upper() != "TX":
                continue
            command = str(row.get("command", "") or "").strip()
            m = re.match(r"write_coil\((\d+),(True|False),addr=\d+\)", command)
            if not m:
                continue
            idx = int(m.group(1))
            if 0 <= idx < channels:
                states[idx] = m.group(2) == "True"
        return states

    @staticmethod
    def _as_int(value: Any) -> int | None:
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _device_port_from_cfg(cfg: Dict[str, Any] | None, device_name: str, default: str) -> str:
        if not isinstance(cfg, dict):
            return default
        devices_cfg = cfg.get("devices", {})
        if not isinstance(devices_cfg, dict):
            return default
        device_cfg = devices_cfg.get(device_name, {})
        if not isinstance(device_cfg, dict):
            return default
        port = str(device_cfg.get("port", "") or "").strip()
        return port or default

    @staticmethod
    def _logical_valve_state(
        rows: List[Dict[str, str]],
        cfg: Dict[str, Any] | None,
        valve: Any,
        *,
        fallback_port: str,
        fallback_channels: int,
        fallback_channel: int,
    ) -> bool | None:
        valve_num = App._as_int(valve)
        flow_switch = None
        if isinstance(cfg, dict):
            valves_cfg = cfg.get("valves", {})
            if isinstance(valves_cfg, dict):
                flow_switch = App._as_int(valves_cfg.get("flow_switch"))
        if isinstance(cfg, dict) and valve_num is not None:
            valves_cfg = cfg.get("valves", {})
            relay_map = valves_cfg.get("relay_map", {}) if isinstance(valves_cfg, dict) else {}
            entry = relay_map.get(str(valve_num)) if isinstance(relay_map, dict) else None
            if isinstance(entry, dict):
                device_name = str(entry.get("device", "") or "").strip()
                channel = App._as_int(entry.get("channel"))
                if device_name in {"relay", "relay_8"} and channel is not None and channel > 0:
                    default_port = "COM28" if device_name == "relay" else "COM29"
                    port = App._device_port_from_cfg(cfg, device_name, default_port)
                    channels = 16 if device_name == "relay" else 8
                    states = App._replay_relay_states(rows, port, channels)
                    idx = channel - 1
                    if 0 <= idx < len(states):
                        raw_state = states[idx]
                        return raw_state
        if fallback_channel > 0:
            states = App._replay_relay_states(rows, fallback_port, fallback_channels)
            idx = fallback_channel - 1
            if 0 <= idx < len(states):
                raw_state = states[idx]
                return raw_state
        return None

    @staticmethod
    def _infer_open_co2_ppm(rows: List[Dict[str, str]], cfg: Dict[str, Any] | None = None) -> int | None:
        if isinstance(cfg, dict):
            valves_cfg = cfg.get("valves", {})
            if isinstance(valves_cfg, dict):
                for map_key in ("co2_map", "co2_map_group2"):
                    mapping = valves_cfg.get(map_key, {})
                    if not isinstance(mapping, dict):
                        continue
                    for ppm_text, valve in mapping.items():
                        ppm = App._as_int(ppm_text)
                        if ppm is None:
                            continue
                        state = App._logical_valve_state(
                            rows,
                            cfg,
                            valve,
                            fallback_port="",
                            fallback_channels=0,
                            fallback_channel=0,
                        )
                        if state is True:
                            return ppm

        fallback_group1 = {0: 7, 200: 8, 400: 9, 600: 10, 800: 11, 1000: 12}
        fallback_group2 = {900: 1, 700: 2, 500: 3, 300: 4, 100: 5, 0: 6}
        relay16 = App._replay_relay_states(rows, "COM28", 16)
        for ppm, channel in fallback_group1.items():
            if relay16[channel - 1] is True:
                return ppm
        for ppm, channel in fallback_group2.items():
            if relay16[channel - 1] is True:
                return ppm
        return None

    @staticmethod
    def _planned_run_points(runtime_cfg: Dict[str, Any] | None) -> List[CalibrationPoint]:
        if not isinstance(runtime_cfg, dict):
            return []
        paths_cfg = runtime_cfg.get("paths", {})
        points_path = paths_cfg.get("points_excel") if isinstance(paths_cfg, dict) else None
        if not points_path:
            return []
        workflow_cfg = runtime_cfg.get("workflow", {}) if isinstance(runtime_cfg.get("workflow", {}), dict) else {}
        points_mtime = None
        try:
            points_mtime = Path(points_path).stat().st_mtime
        except Exception:
            pass
        cache_key = json.dumps(
            {
                "points_excel": str(points_path),
                "points_mtime": points_mtime,
                "route_mode": workflow_cfg.get("route_mode"),
                "temperature_descending": workflow_cfg.get("temperature_descending"),
                "selected_temps_c": workflow_cfg.get("selected_temps_c"),
                "skip_co2_ppm": workflow_cfg.get("skip_co2_ppm"),
                "selected_pressure_points": workflow_cfg.get("selected_pressure_points"),
                "missing_pressure_policy": workflow_cfg.get("missing_pressure_policy"),
                "h2o_carry_forward": workflow_cfg.get("h2o_carry_forward"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        cache = getattr(App, "_planned_run_points_cache", {})
        cached = cache.get(cache_key)
        if isinstance(cached, tuple):
            return list(cached)
        try:
            points = load_points_from_excel(
                points_path,
                missing_pressure_policy=str(workflow_cfg.get("missing_pressure_policy", "require") or "require"),
                carry_forward_h2o=bool(workflow_cfg.get("h2o_carry_forward", False)),
            )
            ordered_points = reorder_points(
                list(points),
                0.0,
                descending_temperatures=bool(workflow_cfg.get("temperature_descending", True)),
            )
            runner = CalibrationRunner(
                runtime_cfg,
                {},
                None,
                lambda *_args, **_kwargs: None,
                lambda *_args, **_kwargs: None,
            )
            filtered_points = runner._filter_selected_temperatures(ordered_points)
            route_mode = runner._route_mode()
            planned: List[CalibrationPoint] = []
            for temp_group in runner._group_points_by_temperature(filtered_points):
                if not temp_group:
                    continue
                temp_value = getattr(temp_group[0], "temp_chamber_c", None)
                try:
                    is_subzero = float(temp_value) < 0.0
                except Exception:
                    is_subzero = False
                if route_mode != "co2_only" and not is_subzero:
                    h2o_points = [point for point in temp_group if point.is_h2o_point]
                    h2o_pressure_points = runner._h2o_pressure_points_for_temperature(temp_group)
                    for h2o_group in runner._group_h2o_points(h2o_points):
                        if not h2o_group:
                            continue
                        lead = h2o_group[0]
                        planned.extend(
                            runner._build_h2o_pressure_point(lead, pressure_point)
                            for pressure_point in h2o_pressure_points
                        )
                if route_mode != "h2o_only":
                    gas_sources = runner._co2_source_points(temp_group)
                    co2_pressure_points = runner._co2_pressure_points_for_temperature(temp_group)
                    for source_point in gas_sources:
                        planned.extend(
                            runner._build_co2_pressure_point(source_point, pressure_point)
                            for pressure_point in co2_pressure_points
                        )
            cache[cache_key] = tuple(planned)
            setattr(App, "_planned_run_points_cache", cache)
            return planned
        except Exception:
            return []

    @staticmethod
    def _run_event_records(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for row in rows:
            port = str(row.get("port", "") or "").strip().upper()
            device = str(row.get("device", "") or "").strip().lower()
            direction = str(row.get("direction", "") or "").strip().upper()
            origin = ""
            if port == "RUN" and device == "runner" and direction == "EVENT":
                origin = "runner"
            elif port == "LOG" and device == "run_logger" and direction == "WARN":
                origin = "logger"
            else:
                continue
            response_text = str(row.get("response", "") or "").strip()
            payload: Dict[str, Any] | None = None
            if response_text.startswith("{"):
                try:
                    loaded = json.loads(response_text)
                except Exception:
                    loaded = None
                if isinstance(loaded, dict):
                    payload = loaded
            records.append(
                {
                    "origin": origin,
                    "command": str(row.get("command", "") or "").strip(),
                    "response": response_text,
                    "error": str(row.get("error", "") or "").strip(),
                    "payload": payload,
                    "ts": App._parse_row_timestamp(row),
                }
            )
        return records

    @staticmethod
    def _format_stage_payload_text(payload: Dict[str, Any] | None) -> str:
        if not isinstance(payload, dict):
            return "--"
        current = str(payload.get("current", "") or "").strip() or "--"
        wait_reason = str(payload.get("wait_reason", "") or "").strip()
        countdown = App._as_int(payload.get("countdown_s"))
        if wait_reason:
            suffix = wait_reason
            if countdown is not None:
                suffix += f"，剩余{countdown}s"
            return f"{current}（{suffix}）"
        if countdown is not None and current != "--":
            return f"{current}（剩余{countdown}s）"
        return current

    @staticmethod
    def _latest_stage_record(rows: List[Dict[str, str]]) -> Dict[str, Any] | None:
        for record in reversed(App._run_event_records(rows)):
            if record.get("command") != "stage":
                continue
            payload = record.get("payload")
            if isinstance(payload, dict):
                return record
        return None

    @staticmethod
    def _stage_wait_reason(payload: Dict[str, Any] | None) -> str:
        if not isinstance(payload, dict):
            return ""
        return str(payload.get("wait_reason", "") or "").strip()

    @staticmethod
    def _stage_allows_sample_inference(stage_record: Dict[str, Any] | None) -> bool:
        wait_reason = App._stage_wait_reason(stage_record.get("payload") if isinstance(stage_record, dict) else None)
        return wait_reason in {"", "采样中"}

    @staticmethod
    def _latest_stage_status(rows: List[Dict[str, str]]) -> Dict[str, str]:
        record = App._latest_stage_record(rows)
        if isinstance(record, dict):
            payload = record.get("payload")
            if isinstance(payload, dict):
                current = App._format_stage_payload_text(payload)
                route_group = str(payload.get("route_group", "") or "").strip() or "--"
                return {"current": current, "route_group": route_group}
        return {"current": "--", "route_group": "--"}

    @staticmethod
    def _sample_progress_from_events(rows: List[Dict[str, str]]) -> str:
        latest_stage = App._latest_stage_record(rows)
        latest_sample: Dict[str, Any] | None = None
        for record in reversed(App._run_event_records(rows)):
            if record.get("command") != "sample-progress":
                continue
            payload = record.get("payload")
            if not isinstance(payload, dict):
                continue
            latest_sample = record
            break

        if latest_sample is None:
            if not App._stage_allows_sample_inference(latest_stage):
                return "采样进度：等待开始"
            return "采样进度：--"

        if not App._stage_allows_sample_inference(latest_stage):
            stage_ts = latest_stage.get("ts") if isinstance(latest_stage, dict) else None
            sample_ts = latest_sample.get("ts")
            if stage_ts is None or sample_ts is None or stage_ts >= sample_ts:
                return "采样进度：等待开始"

        payload = latest_sample.get("payload")
        text = str(payload.get("text", "") or "").strip() if isinstance(payload, dict) else ""
        return text or "采样进度：--"

    @staticmethod
    def _format_run_event(record: Dict[str, Any]) -> str:
        origin = str(record.get("origin", "") or "").strip()
        command = str(record.get("command", "") or "").strip()
        payload = record.get("payload")
        response = str(record.get("response", "") or "").strip()
        error = str(record.get("error", "") or "").strip()
        if origin == "logger":
            detail = error or response or "xlsx 已降级"
            return f"报表告警：{command} {detail}".strip()
        if command == "stage":
            return App._format_stage_payload_text(payload)
        if command == "sample-progress" and isinstance(payload, dict):
            return str(payload.get("text", "") or "").strip() or "采样进度：--"
        if command == "run-start":
            return "流程已启动"
        if command == "run-finished":
            return "Run finished"
        if command == "run-cleanup":
            return "流程清理完成"
        if command == "run-aborted":
            detail = error or response or "unknown"
            return f"Run aborted: {detail}"
        if command == "stop-request":
            return "已请求停止"
        if command == "analyzer-config-warning" and isinstance(payload, dict):
            label = str(payload.get("label", "--") or "--").upper()
            phase = str(payload.get("phase", "") or "").strip().lower()
            warnings = payload.get("warnings", [])
            warning_text = "、".join(str(item) for item in warnings if str(item).strip()) or "配置 ACK 缺失"
            prefix = "分析仪启动提示" if phase == "startup" else "分析仪运行告警"
            return f"{prefix}：{label} {warning_text}（功能验证已通过）"
        if command == "analyzers-disabled" and isinstance(payload, dict):
            labels = "、".join(str(item).upper() for item in payload.get("labels", []) if str(item).strip()) or "--"
            reason = str(payload.get("reason", "") or "").strip()
            suffix = f"（{reason}）" if reason else ""
            return f"分析仪告警：已禁用 {labels}{suffix}"
        if command == "analyzers-restored" and isinstance(payload, dict):
            labels = "、".join(str(item).upper() for item in payload.get("labels", []) if str(item).strip()) or "--"
            return f"分析仪恢复：{labels}"
        if command == "analyzers-still-disabled" and isinstance(payload, dict):
            labels = "、".join(str(item).upper() for item in payload.get("labels", []) if str(item).strip()) or "--"
            return f"分析仪告警：仍禁用 {labels}"
        return response or error or command or "--"

    @staticmethod
    def _summarize_analyzer_health_issue(
        rows: List[Dict[str, str]],
        runtime_cfg: Dict[str, Any] | None = None,
        seconds: int = 90,
    ) -> str:
        analyzer_ports = App._gas_analyzer_port_map(runtime_cfg)
        if not analyzer_ports:
            return "--"
        latest_ts: datetime | None = None
        for row in reversed(rows):
            latest_ts = App._parse_row_timestamp(row)
            if latest_ts is not None:
                break
        cutoff = latest_ts - timedelta(seconds=seconds) if latest_ts is not None else None
        issues: List[str] = []
        for name, port in analyzer_ports.items():
            empty_count = 0
            protocol_count = 0
            suspicious_count = 0
            for row in rows:
                if str(row.get("port", "") or "").strip().upper() != port:
                    continue
                if str(row.get("direction", "") or "").strip().upper() != "RX":
                    continue
                ts = App._parse_row_timestamp(row)
                if cutoff is not None and ts is not None and ts < cutoff:
                    continue
                error = str(row.get("error", "") or "").strip()
                response = str(row.get("response", "") or "").strip()
                if error:
                    protocol_count += 1
                    continue
                if not response:
                    empty_count += 1
                    continue
                if any(token in response for token in ("INVALID_RESPONSE", "NO_RESPONSE", "FAIL")):
                    protocol_count += 1
                    continue
                parsed = None
                for candidate in GasAnalyzer._iter_frame_candidates(response):
                    parts = GasAnalyzer._split_frame_parts(candidate)
                    parsed = GasAnalyzer._parse_mode2(parts, response)
                    if parsed is None:
                        parsed = GasAnalyzer._parse_legacy(parts, response)
                    if parsed is not None:
                        break
                if not isinstance(parsed, dict):
                    continue
                try:
                    co2_ppm = float(parsed.get("co2_ppm")) if parsed.get("co2_ppm") is not None else None
                except Exception:
                    co2_ppm = None
                try:
                    h2o_mmol = float(parsed.get("h2o_mmol")) if parsed.get("h2o_mmol") is not None else None
                except Exception:
                    h2o_mmol = None
                if (
                    co2_ppm is not None
                    and h2o_mmol is not None
                    and co2_ppm >= 2999.0
                    and h2o_mmol >= 70.0
                    and not App._analyzer_frame_has_usable_ratio(parsed, runtime_cfg)
                ):
                    suspicious_count += 1
            if empty_count >= 3 or protocol_count >= 2 or suspicious_count >= 3:
                parts: List[str] = []
                if protocol_count:
                    parts.append(f"协议异常×{protocol_count}")
                if empty_count:
                    parts.append(f"空响应×{empty_count}")
                if suspicious_count:
                    parts.append(f"无效帧×{suspicious_count}")
                issues.append(f"{name.upper()} {'/'.join(parts)}")
        if not issues:
            return "--"
        return "分析仪告警：" + "；".join(issues[:2])

    @staticmethod
    def _analyzer_frame_has_usable_ratio(parsed: Dict[str, Any], runtime_cfg: Dict[str, Any] | None = None) -> bool:
        workflow_cfg = runtime_cfg.get("workflow", {}) if isinstance(runtime_cfg, dict) else {}
        frame_cfg = workflow_cfg.get("analyzer_frame_quality", {}) if isinstance(workflow_cfg, dict) else {}
        tolerance = abs(float(frame_cfg.get("invalid_sentinel_tolerance", 0.001) or 0.001))
        sentinels: List[float] = []
        for item in frame_cfg.get("invalid_sentinel_values", [-1001.0, -9999.0, 999999.0]) or []:
            try:
                numeric = float(item)
            except Exception:
                continue
            if math.isfinite(numeric):
                sentinels.append(numeric)

        for key in (
            "co2_ratio_f",
            "co2_ratio_raw",
            "h2o_ratio_f",
            "h2o_ratio_raw",
            "co2_sig",
            "h2o_sig",
        ):
            try:
                numeric = float(parsed.get(key)) if parsed.get(key) is not None else None
            except Exception:
                numeric = None
            if numeric is None or not math.isfinite(numeric) or numeric <= 0:
                continue
            if any(abs(numeric - sentinel) <= tolerance for sentinel in sentinels):
                continue
            return True
        return False

    @staticmethod
    def _last_issue_from_progress_rows(rows: List[Dict[str, str]], runtime_cfg: Dict[str, Any] | None = None) -> str:
        analyzer_issue = App._summarize_analyzer_health_issue(rows, runtime_cfg=runtime_cfg)
        for record in reversed(App._run_event_records(rows)):
            text = App._format_run_event(record)
            if record.get("command") == "run-aborted":
                return text
            if record.get("origin") == "logger":
                return text
            if any(token in text for token in ("skipped", "timeout", "ERROR", "FAIL", "aborted", "告警")):
                return text
        if analyzer_issue != "--":
            return analyzer_issue
        for row in reversed(rows):
            port = str(row.get("port", "") or "").strip().upper()
            direction = str(row.get("direction", "") or "").strip().upper()
            error = str(row.get("error", "") or "").strip()
            response = str(row.get("response", "") or "").strip()
            if port not in {"RUN", "UI", "LOG"} and direction not in {"EVENT", "WARN"}:
                continue
            if "STARTUP_NO_ACK" in response:
                continue
            if error:
                return error
            if any(token in response for token in ("skipped", "timeout", "ERROR", "FAIL", "aborted")):
                return response
        return "--"

    @staticmethod
    def _infer_progress_status_from_io(run_dir: Path) -> Dict[str, Any]:
        io_path = App._latest_io_path(run_dir)
        completed_files = sorted(run_dir.glob("point_*_samples.csv"))
        completed = len(completed_files)
        recent_points: deque[str] = deque(maxlen=5)
        for path in completed_files[-5:]:
            row_match = re.match(r"point_(\d+)_", path.name)
            if row_match:
                recent_points.append(f"Point {int(row_match.group(1))} samples saved: {path.name}")
            else:
                recent_points.append(f"Point samples saved: {path.name}")

        if io_path is None or not io_path.exists():
            return {
                "current": "--",
                "completed": completed,
                "skipped": 0,
                "failed": 0,
                "total": None,
                "percent": 0.0,
                "route_group": "--",
                "last_issue": "--",
                "recent_points": list(recent_points),
            }

        rows = App._tail_csv_rows(io_path, count=2500)
        runtime_cfg = App._load_runtime_config_snapshot(run_dir)
        planned_total = len(App._planned_run_points(runtime_cfg))
        hold_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("hold") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=1) is True
        flow_switch_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("flow_switch") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=2) is True
        gas_main_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("gas_main") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=3) is True
        h2o_path_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("h2o_path") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=8) is True
        co2_group1_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("co2_path") if isinstance(runtime_cfg, dict) else None, fallback_port="COM28", fallback_channels=16, fallback_channel=15) is True
        co2_group2_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("co2_path_group2") if isinstance(runtime_cfg, dict) else None, fallback_port="COM28", fallback_channels=16, fallback_channel=16) is True
        hgen = App._latest_humidity_generator_state(rows)
        pressure_target = App._parse_last_numeric_command(rows, "COM31", ":SOUR:PRES:LEV:IMM:AMPL")
        route_state = App._infer_route_state(rows, runtime_cfg)

        route_group = "--"
        if co2_group2_open:
            route_group = "第二组气路"
        elif co2_group1_open:
            route_group = "第一组气路"

        current = "--"
        if route_state == "开路" and hold_open and flow_switch_open and h2o_path_open and hgen is not None:
            current = f"H2O 开路等待 Tc={hgen.get('Tc', 0.0):.1f}°C Uw={hgen.get('Uw', 0.0):.1f}%"
            route_group = "水路"
        elif route_state == "开路" and h2o_path_open and not hold_open and not flow_switch_open and gas_main_open:
            ppm = App._infer_open_co2_ppm(rows, runtime_cfg)
            if ppm is not None:
                current = f"CO2 {ppm}ppm 通气等待"
        elif pressure_target is not None:
            ppm = App._infer_open_co2_ppm(rows, runtime_cfg)
            if ppm is not None:
                current = f"CO2 {ppm}ppm {int(round(pressure_target))}hPa"
            else:
                current = f"控压中 {int(round(pressure_target))}hPa"
        elif hgen is not None:
            current = f"H2O 前置 Tc={hgen.get('Tc', 0.0):.1f}°C Uw={hgen.get('Uw', 0.0):.1f}%"
            route_group = "水路"

        stage_status = App._latest_stage_status(rows)
        if stage_status["current"] != "--":
            current = stage_status["current"]
        if stage_status["route_group"] != "--":
            route_group = stage_status["route_group"]

        last_issue = App._last_issue_from_progress_rows(rows, runtime_cfg=runtime_cfg)

        if current != "--":
            recent_points.append(current)

        expected_samples = App._sampling_target_from_run_dir(run_dir)
        sample_progress = App._sample_progress_from_events(rows)
        latest_stage = App._latest_stage_record(rows)
        if sample_progress.endswith("--") and App._stage_allows_sample_inference(latest_stage):
            sample_progress = App._infer_sample_progress(rows, expected_count=expected_samples)
        elif sample_progress.endswith("--"):
            sample_progress = "采样进度：等待开始"
        freshness_text, freshness_level = App._compute_data_freshness(rows)
        done = completed
        total = planned_total if planned_total > 0 else None
        percent = min(100.0, (done / total) * 100.0) if total else 0.0

        return {
            "current": current,
            "completed": completed,
            "skipped": 0,
            "failed": 0,
            "total": total,
            "percent": percent,
            "route_group": route_group,
            "last_issue": last_issue,
            "recent_points": list(recent_points),
            "sample_progress": sample_progress,
            "freshness_text": freshness_text,
            "freshness_level": freshness_level,
        }

    @staticmethod
    def _infer_sample_progress(rows: List[Dict[str, str]], expected_count: int = 10) -> str:
        def _is_analyzer_sample(row: Dict[str, str]) -> bool:
            if str(row.get("direction", "")).strip().upper() != "RX":
                return False
            response = str(row.get("response", "") or "").strip()
            return response.startswith("YGAS,")

        def _is_sampling_boundary(row: Dict[str, str]) -> bool:
            port = str(row.get("port", "")).strip()
            direction = str(row.get("direction", "")).strip().upper()
            command = str(row.get("command", "") or "").strip()
            if port in {"COM28", "COM29"}:
                return True
            if port != "COM31" or direction != "TX":
                return False
            pressure_value, _matched_prefix = App._parse_pressure_setpoint_command(command)
            return any(
                token in command
                for token in (
                    ":OUTP 0",
                    ":SOUR:PRES:LEV:IMM:AMPL:VENT 1",
                    ":OUTP:MODE",
                )
            ) or pressure_value is not None

        latest_sample_index: Optional[int] = None
        for idx in range(len(rows) - 1, -1, -1):
            if _is_analyzer_sample(rows[idx]):
                latest_sample_index = idx
                break

        if latest_sample_index is None:
            return "采样进度：--"

        sample_start_index = 0
        for idx in range(latest_sample_index - 1, -1, -1):
            if _is_sampling_boundary(rows[idx]):
                sample_start_index = idx + 1
                break

        sample_counts_by_port: Dict[str, int] = {}
        for row in rows[sample_start_index : latest_sample_index + 1]:
            if not _is_analyzer_sample(row):
                continue
            port = str(row.get("port", "")).strip()
            if port:
                sample_counts_by_port[port] = sample_counts_by_port.get(port, 0) + 1
        sample_count = max(sample_counts_by_port.values(), default=0)
        if sample_count <= 0:
            return "采样进度：等待开始"
        target = max(1, int(expected_count))
        return f"采样进度：{min(sample_count, target)}/{target}"

    @staticmethod
    def _compute_data_freshness(rows: List[Dict[str, str]]) -> Tuple[str, str]:
        latest_ts: datetime | None = None
        for row in reversed(rows):
            latest_ts = App._parse_row_timestamp(row)
            if latest_ts is not None:
                break
        if latest_ts is None:
            return "数据刷新：--", "idle"
        delta_s = max(0, int((datetime.now() - latest_ts).total_seconds()))
        if delta_s <= 5:
            return f"数据刷新：{delta_s}s前", "ok"
        if delta_s <= 20:
            return f"数据刷新：{delta_s}s前", "warn"
        return f"数据刷新：{delta_s}s前（停滞）", "error"

    @staticmethod
    def _extract_device_trend_point(row: Dict[str, str]) -> Tuple[str | None, float | None]:
        port = str(row.get("port", "")).strip()
        direction = str(row.get("direction", "")).strip().upper()
        if direction != "RX":
            return None, None
        response = str(row.get("response", "") or "").strip()
        if not response:
            return None, None

        def _match(pattern: str) -> float | None:
            m = re.search(pattern, response)
            return float(m.group(1)) if m else None

        if port == "COM31":
            value = _match(r":SENS:PRES:INL\s+(-?\d+(?:\.\d+)?)")
            return ("pace", value) if value is not None else (None, None)
        if port == "COM30":
            value = _match(r"(-?\d+(?:\.\d+)?)")
            return ("gauge", value) if value is not None else (None, None)
        if port == "COM27":
            value = _match(r"(-?\d+(?:\.\d+)?)")
            return ("chamber", value) if value is not None else (None, None)
        if port == "COM24":
            value = _match(r"Uw=\s*(-?\d+(?:\.\d+)?)")
            return ("hgen", value) if value is not None else (None, None)
        if port == "COM25":
            value = _match(r"dewpoint\s*=\s*(-?\d+(?:\.\d+)?)")
            if value is None:
                value = App._parse_dewpoint_frame(response).get("dewpoint_c")
            if value is None:
                value = _match(r"(-?\d+(?:\.\d+)?)")
            return ("dewpoint", value) if value is not None else (None, None)
        return None, None

    @staticmethod
    def _build_device_trends(rows: List[Dict[str, str]], seconds: int = 30) -> Dict[str, Dict[str, Any]]:
        latest_ts: datetime | None = None
        for row in reversed(rows):
            latest_ts = App._parse_row_timestamp(row)
            if latest_ts is not None:
                break
        cutoff = latest_ts - timedelta(seconds=seconds) if latest_ts is not None else None
        series: Dict[str, List[float]] = {key: [] for key in ("pace", "gauge", "chamber", "hgen", "dewpoint")}
        for row in rows:
            ts = App._parse_row_timestamp(row)
            if cutoff is not None and ts is not None and ts < cutoff:
                continue
            key, value = App._extract_device_trend_point(row)
            if key is None or value is None:
                continue
            series[key].append(value)

        def _trend_level(device: str, delta: float | None) -> str:
            if delta is None:
                return "idle"
            abs_delta = abs(delta)
            if device in {"pace", "gauge"}:
                return "ok" if abs_delta <= 1.0 else "warn"
            if device == "chamber":
                return "ok" if abs_delta <= 0.2 else "warn"
            if device == "hgen":
                return "ok" if abs_delta <= 1.0 else "warn"
            if device == "dewpoint":
                return "ok" if abs_delta <= 0.3 else "warn"
            return "info"

        trends: Dict[str, Dict[str, Any]] = {}
        for key, values in series.items():
            delta = values[-1] - values[0] if len(values) >= 2 else None
            if delta is None:
                text = "30秒变化：--"
                detail = "最近30秒：无足够数据"
            else:
                text = f"30秒变化：{delta:+.2f}"
                detail = f"最近30秒：起始={values[0]:.2f} 结束={values[-1]:.2f} 最小={min(values):.2f} 最大={max(values):.2f}"
            trends[key] = {
                "values": values[-30:],
                "delta": delta,
                "text": text,
                "detail": detail,
                "level": _trend_level(key, delta),
            }
        return trends

    @staticmethod
    def _event_matches_filter(level: str, selected_filter: str) -> bool:
        if selected_filter == "只看异常":
            return level in {"warn", "error"}
        if selected_filter == "只看保存成功":
            return level == "ok"
        return True

    def _draw_sparkline(self, canvas: tk.Canvas, values: List[float], level: str) -> None:
        canvas.delete("all")
        width = max(canvas.winfo_width(), 180)
        height = max(canvas.winfo_height(), 28)
        canvas.create_line(0, height - 2, width, height - 2, fill="#e2e8f0")
        if len(values) < 2:
            canvas.create_text(6, height / 2, text="--", anchor="w", fill=self.ui_colors["muted"])
            return
        min_v = min(values)
        max_v = max(values)
        span = max(max_v - min_v, 1e-6)
        color = self.state_palette.get(level, self.state_palette["info"])["fg"]
        coords: List[float] = []
        for idx, value in enumerate(values):
            x = (width - 8) * idx / max(1, len(values) - 1) + 4
            y = height - 4 - ((value - min_v) / span) * (height - 8)
            coords.extend((x, y))
        canvas.create_line(*coords, fill=color, width=2, smooth=True)
        canvas.create_oval(coords[-2] - 2, coords[-1] - 2, coords[-2] + 2, coords[-1] + 2, fill=color, outline="")

    @staticmethod
    def _extract_key_events_from_io(
        rows: List[Dict[str, str]],
        count: int = 12,
        runtime_cfg: Dict[str, Any] | None = None,
    ) -> List[str]:
        events: deque[str] = deque(maxlen=count)
        latest_hgen = App._latest_humidity_generator_state(rows)
        if latest_hgen:
            events.append(
                f"湿度发生器前置：Tc={latest_hgen.get('Tc', 0.0):.2f}°C Uw={latest_hgen.get('Uw', 0.0):.1f}% Td={latest_hgen.get('Td', 0.0):.2f}°C"
            )
        hold_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("hold") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=1) is True
        bypass_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("flow_switch") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=2) is True
        gas_main_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("gas_main") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=3) is True
        h2o_path_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("h2o_path") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=8) is True
        if not hold_open and not bypass_open and not gas_main_open and not h2o_path_open:
            events.append("阀门基线：水路阀关 / 保压阀关 / 旁路阀关 / 总气路阀关")
        elif hold_open and bypass_open and not gas_main_open and h2o_path_open:
            events.append("水路已开：水路阀开 / 保压阀开 / 旁路阀开 / 总气路阀关")
        elif not hold_open and not bypass_open and gas_main_open and h2o_path_open:
            events.append("气路已开：水路阀关 / 保压阀关 / 旁路阀关 / 总气路阀开 / 总阀开")

        pressure_target = App._parse_last_numeric_command(rows, "COM31", ":SOUR:PRES:LEV:IMM:AMPL")
        if pressure_target is not None:
            events.append(f"压力控制目标：{int(round(pressure_target))} hPa")
        elif any(
            str(row.get("port", "")).strip() == "COM31"
            and str(row.get("direction", "")).strip().upper() == "TX"
            and ":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in str(row.get("command", "") or "")
            for row in rows[-50:]
        ):
            events.append("压力控制器：通大气保持中")
        analyzer_issue = App._summarize_analyzer_health_issue(rows, runtime_cfg=runtime_cfg)
        if analyzer_issue != "--":
            events.append(analyzer_issue)
        for record in App._run_event_records(rows):
            text = App._format_run_event(record)
            if not text or text == "--":
                continue
            if events and events[-1] == text:
                continue
            events.append(text)
        return list(events)

    @staticmethod
    def _classify_event_level(text: str) -> str:
        line = (text or "").strip()
        if not line:
            return "info"
        upper = line.upper()
        if "STARTUP_NO_ACK" in upper:
            return "info"
        if "分析仪恢复" in line:
            return "ok"
        if any(token in upper for token in ("FAIL", "INVALID_RESPONSE", "ERROR", "ABORTED")):
            return "error"
        if any(token in line for token in ("skipped", "timeout", "RUNTIME_NO_ACK", "NO_ACK", "NO_RESPONSE", "停滞", "告警", "已禁用", "仍禁用")):
            return "warn"
        if any(token in line for token in ("saved", "in-limits", "Run finished", "恢复基线", "采样进度")):
            return "ok"
        return "info"

    @staticmethod
    def _compute_progress_status(run_dir: Path) -> Dict[str, Any]:
        stdout_candidates = sorted(run_dir.glob("*_stdout.log"))
        stdout_path = max(stdout_candidates, key=lambda path: path.stat().st_mtime) if stdout_candidates else None
        if stdout_path is None or not stdout_path.exists():
            return App._infer_progress_status_from_io(run_dir)

        lines = App._tail_text_lines(stdout_path, count=4000)
        runtime_cfg = App._load_runtime_config_snapshot(run_dir)
        total = 0
        for line in lines:
            m = re.search(
                r"Temperature group .* CO2 sweep: sources=\[(.*?)\]\s+pressures=\[(.*?)\]",
                line,
            )
            if m:
                sources = [part.strip() for part in m.group(1).split(",") if part.strip()]
                pressures = [part.strip() for part in m.group(2).split(",") if part.strip()]
                total += len(sources) * len(pressures)
                continue
            m = re.search(r"H2O group rows=.*?rows=([0-9,]+)", line)
            if m:
                rows = [part.strip() for part in m.group(1).split(",") if part.strip()]
                total += len(rows)
        if total <= 0:
            total = len(App._planned_run_points(runtime_cfg))

        completed_files = sorted(run_dir.glob("point_*_samples.csv"))
        completed = len(completed_files)
        skipped = sum(1 for line in lines if " skipped:" in line or " skipped: " in line)
        failures = sum(
            1
            for line in lines
            if any(token in line for token in (" FAIL ", "Connectivity check failed", "INVALID_RESPONSE", "ERROR"))
        )
        current = "--"
        for line in reversed(lines):
            text = line.strip()
            if not text:
                continue
            if re.match(r"CO2\s+\d+ppm\s+\d+hPa$", text):
                current = text
                break
            if re.match(r"H2O row\s+\d+$", text):
                current = text
                break
            if re.match(r"CO2 row\s+\d+$", text):
                current = text
                break
            if text.startswith("Temperature group "):
                current = text
                break

        done = completed + skipped
        if total and total > 0:
            percent = min(100.0, (done / total) * 100.0)
        else:
            percent = 0.0

        route_group = "--"
        for line in reversed(lines):
            text = line.strip()
            if not text:
                continue
            m = re.match(r"CO2\s+(\d+)ppm\s+\d+hPa$", text)
            if m:
                ppm = int(m.group(1))
                if ppm in {100, 300, 500, 700, 900}:
                    route_group = "第二组气路"
                elif ppm in {0, 200, 400, 600, 800, 1000}:
                    route_group = "第一组气路"
                else:
                    route_group = f"{ppm}ppm"
                break

        last_issue = "--"
        issue_patterns = (
            " skipped:",
            " FAIL ",
            "Connectivity check failed",
            "INVALID_RESPONSE",
            "timeout",
            "ERROR",
            "失败",
        )
        for line in reversed(lines):
            text = line.strip()
            if text and any(pattern in text for pattern in issue_patterns):
                last_issue = text
                break

        recent_points: deque[str] = deque(maxlen=5)
        for line in lines:
            text = line.strip()
            if not text:
                continue
            if re.match(r"CO2\s+\d+ppm\s+\d+hPa$", text) or re.match(r"H2O row\s+\d+$", text):
                recent_points.append(text)
            elif "Point " in text and " samples saved" in text:
                recent_points.append(text)
            elif " skipped:" in text:
                recent_points.append(text)

        io_path = App._latest_io_path(run_dir)
        sample_progress = "采样进度：--"
        freshness_text = "数据刷新：--"
        freshness_level = "idle"
        if io_path is not None and io_path.exists():
            io_rows = App._tail_csv_rows(io_path, count=2500)
            stage_status = App._latest_stage_status(io_rows)
            if stage_status["current"] != "--":
                current = stage_status["current"]
            if stage_status["route_group"] != "--":
                route_group = stage_status["route_group"]
            expected_samples = App._sampling_target_from_run_dir(run_dir)
            sample_progress = App._sample_progress_from_events(io_rows)
            latest_stage = App._latest_stage_record(io_rows)
            if sample_progress.endswith("--") and App._stage_allows_sample_inference(latest_stage):
                sample_progress = App._infer_sample_progress(io_rows, expected_count=expected_samples)
            elif sample_progress.endswith("--"):
                sample_progress = "采样进度：等待开始"
            last_issue_from_rows = App._last_issue_from_progress_rows(io_rows, runtime_cfg=runtime_cfg)
            if last_issue_from_rows != "--":
                last_issue = last_issue_from_rows
            if current != "--" and (not recent_points or recent_points[-1] != current):
                recent_points.append(current)
            freshness_text, freshness_level = App._compute_data_freshness(io_rows)

        return {
            "current": current,
            "completed": completed,
            "skipped": skipped,
            "failed": failures,
            "total": total if total > 0 else None,
            "percent": percent,
            "route_group": route_group,
            "last_issue": last_issue,
            "recent_points": list(recent_points),
            "sample_progress": sample_progress,
            "freshness_text": freshness_text,
            "freshness_level": freshness_level,
        }

    @staticmethod
    def _parse_live_device_values(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        out = {
            "pace": {"text": "压力控制器：--", "timestamp": "--"},
            "gauge": {"text": "数字气压计：--", "timestamp": "--"},
            "chamber": {"text": "温度箱：--", "timestamp": "--"},
            "hgen": {"text": "湿度发生器：--", "timestamp": "--"},
            "dewpoint": {"text": "露点仪：--", "timestamp": "--"},
        }

        pace_rows = [
            row for row in rows
            if str(row.get("port", "") or "").strip() == "COM31"
            and str(row.get("direction", "") or "").strip().upper() == "RX"
            and str(row.get("response", "") or "").strip()
        ]
        for row in reversed(pace_rows):
            pace_text = str(row.get("response", "") or "").strip()
            m = re.search(r"(-?\d+(?:\.\d+)?),\s*(\d+)\s*$", pace_text)
            if not m:
                continue
            out["pace"]["text"] = f"压力控制器：{float(m.group(1)):.2f} hPa，稳定标志={m.group(2)}"
            out["pace"]["timestamp"] = str(row.get("ts", "") or row.get("timestamp", "") or "").strip() or "--"
            break

        gauge_rows = [
            row for row in rows
            if str(row.get("port", "") or "").strip() == "COM30"
            and str(row.get("direction", "") or "").strip().upper() == "RX"
            and str(row.get("response", "") or "").strip()
        ]
        for row in reversed(gauge_rows):
            gauge_text = str(row.get("response", "") or "").strip()
            m = re.search(r"(-?\d+(?:\.\d+)?)", gauge_text)
            if not m:
                continue
            out["gauge"]["text"] = f"数字气压计：{float(m.group(1)):.3f} hPa"
            out["gauge"]["timestamp"] = str(row.get("ts", "") or row.get("timestamp", "") or "").strip() or "--"
            break

        chamber_rows = [row for row in rows if row.get("port") == "COM27" and str(row.get("direction", "")).upper() == "RX"]
        chamber_temp = None
        chamber_rh = None
        chamber_ts = "--"
        for row in chamber_rows:
            response = str(row.get("response", "") or "")
            mt = re.search(r"temp_c=(-?\d+(?:\.\d+)?)", response)
            mr = re.search(r"rh_pct=(-?\d+(?:\.\d+)?)", response)
            if mt:
                chamber_temp = float(mt.group(1))
                chamber_ts = str(row.get("ts", "") or row.get("timestamp", "") or "").strip() or chamber_ts
            if mr:
                chamber_rh = float(mr.group(1))
                chamber_ts = str(row.get("ts", "") or row.get("timestamp", "") or "").strip() or chamber_ts
        if chamber_temp is not None or chamber_rh is not None:
            parts = []
            if chamber_temp is not None:
                parts.append(f"温度={chamber_temp:.1f}°C")
            if chamber_rh is not None:
                parts.append(f"湿度={chamber_rh:.1f}%")
            out["chamber"]["text"] = "温度箱：" + "，".join(parts)
            out["chamber"]["timestamp"] = chamber_ts

        hgen_rows = [
            row for row in rows
            if str(row.get("port", "") or "").strip() == "COM24"
            and str(row.get("direction", "") or "").strip().upper() == "RX"
            and str(row.get("response", "") or "").strip()
        ]
        for row in reversed(hgen_rows):
            hgen_text = str(row.get("response", "") or "").strip()
            tc = re.search(r"Tc=\s*(-?\d+(?:\.\d+)?)", hgen_text)
            uw = re.search(r"Uw=\s*(-?\d+(?:\.\d+)?)", hgen_text)
            td = re.search(r"Td=\s*(-?\d+(?:\.\d+)?)", hgen_text)
            flux = re.search(r"Flux=\s*(-?\d+(?:\.\d+)?)", hgen_text)
            parts = []
            if tc:
                parts.append(f"Tc={float(tc.group(1)):.2f}°C")
            if uw:
                parts.append(f"Uw={float(uw.group(1)):.1f}%")
            if td:
                parts.append(f"Td={float(td.group(1)):.2f}°C")
            if flux:
                parts.append(f"流量={float(flux.group(1)):.1f}")
            out["hgen"]["text"] = "湿度发生器：" + ("，".join(parts) if parts else hgen_text)
            out["hgen"]["timestamp"] = str(row.get("ts", "") or row.get("timestamp", "") or "").strip() or "--"
            break

        dew_rows = [
            row for row in rows
            if str(row.get("port", "") or "").strip() == "COM25"
            and str(row.get("direction", "") or "").strip().upper() == "RX"
            and str(row.get("response", "") or "").strip()
        ]
        for row in reversed(dew_rows):
            dew_text = str(row.get("response", "") or "").strip()
            parsed = App._parse_dewpoint_frame(dew_text)
            if parsed:
                out["dewpoint"]["text"] = (
                    f"露点仪：露点={float(parsed.get('dewpoint_c', 0.0)):.2f}°C，"
                    f"温度={float(parsed.get('temp_c', 0.0)):.2f}°C，"
                    f"湿度={float(parsed.get('rh_pct', 0.0)):.2f}"
                )
            else:
                out["dewpoint"]["text"] = f"露点仪：{dew_text}"
            out["dewpoint"]["timestamp"] = str(row.get("ts", "") or row.get("timestamp", "") or "").strip() or "--"
            break

        return out

    @staticmethod
    def _infer_route_state(rows: List[Dict[str, str]], runtime_cfg: Dict[str, Any] | None = None) -> str:
        total_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("h2o_path") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=8) is True
        hold_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("hold") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=1) is True
        bypass_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("flow_switch") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=2) is True
        gas_main_open = App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("gas_main") if isinstance(runtime_cfg, dict) else None, fallback_port="COM29", fallback_channels=8, fallback_channel=3) is True
        gas_route_open = bool(
            App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("co2_path") if isinstance(runtime_cfg, dict) else None, fallback_port="COM28", fallback_channels=16, fallback_channel=15) is True
            or App._logical_valve_state(rows, runtime_cfg, runtime_cfg.get("valves", {}).get("co2_path_group2") if isinstance(runtime_cfg, dict) else None, fallback_port="COM28", fallback_channels=16, fallback_channel=16) is True
        )
        recent = rows[-120:]
        output_on = any(
            str(row.get("port", "")).strip() == "COM31"
            and str(row.get("direction", "")).strip().upper() == "TX"
            and str(row.get("command", "") or "").strip() == ":OUTP 1"
            for row in recent
        )
        vent_off = any(
            str(row.get("port", "")).strip() == "COM31"
            and str(row.get("direction", "")).strip().upper() == "TX"
            and ":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in str(row.get("command", "") or "")
            for row in recent
        )
        vent_on = any(
            str(row.get("port", "")).strip() == "COM31"
            and str(row.get("direction", "")).strip().upper() == "TX"
            and ":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in str(row.get("command", "") or "")
            for row in recent
        )

        if output_on and vent_off:
            return "控压中"
        if vent_off and not total_open and not bypass_open and not hold_open:
            return "封压中"
        if total_open and hold_open and bypass_open:
            return "开路"
        if total_open and not bypass_open and not hold_open and gas_main_open and gas_route_open:
            return "开路"
        if vent_on:
            return "开路"
        return "待机"

    @staticmethod
    def _gas_analyzer_port_map(runtime_cfg: Dict[str, Any] | None) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        if not isinstance(runtime_cfg, dict):
            return mapping
        devices = runtime_cfg.get("devices", {})
        gas_list = devices.get("gas_analyzers", []) if isinstance(devices, dict) else []
        if isinstance(gas_list, list) and gas_list:
            for idx, item in enumerate(gas_list, start=1):
                if not isinstance(item, dict) or not item.get("enabled", True):
                    continue
                port = str(item.get("port", "") or "").strip().upper()
                if port:
                    mapping[f"ga{idx:02d}"] = port
            return mapping
        single = devices.get("gas_analyzer", {}) if isinstance(devices, dict) else {}
        if isinstance(single, dict):
            port = str(single.get("port", "") or "").strip().upper()
            if port:
                mapping["ga01"] = port
        return mapping

    @staticmethod
    def _device_activity_timestamps(rows: List[Dict[str, str]], runtime_cfg: Dict[str, Any] | None = None) -> Dict[str, str]:
        mapping = {
            "pace": "COM31",
            "gauge": "COM30",
            "chamber": "COM27",
            "hgen": "COM24",
            "dewpoint": "COM25",
        }
        mapping.update(App._gas_analyzer_port_map(runtime_cfg))
        latest: Dict[str, str] = {key: "--" for key in mapping}
        for row in rows:
            port = str(row.get("port", "") or "").strip()
            ts = str(row.get("ts", "") or row.get("timestamp", "") or "").strip()
            if not port or not ts:
                continue
            for key, expected_port in mapping.items():
                if port == expected_port:
                    latest[key] = ts
                    break
        return latest

    def _merge_live_device_values(self, values: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        merged: Dict[str, Dict[str, str]] = {}
        for key, payload in values.items():
            current_text = str(payload.get("text", "--") or "--")
            current_ts = str(payload.get("timestamp", "--") or "--")
            current_valid = not current_text.endswith("：--") and not current_text.endswith(": --") and not current_text.endswith("--")
            cached = self._live_device_cache.get(key, {})
            if current_valid:
                merged_payload = {"text": current_text, "timestamp": current_ts}
            elif cached:
                merged_payload = {
                    "text": str(cached.get("text", current_text) or current_text),
                    "timestamp": str(cached.get("timestamp", current_ts) or current_ts),
                }
            else:
                merged_payload = {"text": current_text, "timestamp": current_ts}
            merged[key] = merged_payload
        self._live_device_cache.update(merged)
        return merged

    def _merge_live_analyzer_values(self, values: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        merged: Dict[str, Dict[str, str]] = {}
        for key, payload in values.items():
            current_valid = self._analyzer_payload_has_live_values(payload)
            cached = self._live_analyzer_cache.get(key, {})
            if current_valid:
                merged_payload = dict(payload)
            elif cached:
                merged_payload = dict(cached)
            else:
                merged_payload = dict(payload)
            merged[key] = merged_payload
        self._live_analyzer_cache.update(merged)
        return merged

    @staticmethod
    def _analyzer_payload_has_live_values(payload: Dict[str, str]) -> bool:
        return any(
            str(payload.get(field, "") or "").strip() not in {"", "--"}
            for field in ("co2_ppm", "h2o_mmol", "chamber_temp_c")
        )

    @staticmethod
    def _format_analyzer_live_value(value: Any) -> str:
        try:
            number = float(value)
        except Exception:
            return "--"
        text = f"{number:.6f}".rstrip("0").rstrip(".")
        return text if text else "0"

    @staticmethod
    def _parse_live_analyzer_values(
        rows: List[Dict[str, str]], runtime_cfg: Dict[str, Any] | None = None
    ) -> Dict[str, Dict[str, str]]:
        values: Dict[str, Dict[str, str]] = {}
        port_to_name = {port: name for name, port in App._gas_analyzer_port_map(runtime_cfg).items()}
        if not port_to_name:
            return values
        for port, name in port_to_name.items():
            values[name] = {
                "name": name.upper(),
                "port": port,
                "online": "○ 未读取",
                "timestamp": "--",
                "status": "--",
            }
            for field, _label in ANALYZER_MODE2_COLUMNS:
                values[name].setdefault(field, "--")

        for row in rows:
            port = str(row.get("port", "") or "").strip().upper()
            direction = str(row.get("direction", "") or "").strip().upper()
            response = str(row.get("response", "") or "").strip()
            name = port_to_name.get(port)
            if name is None or direction != "RX" or not response:
                continue
            parsed = None
            for candidate in GasAnalyzer._iter_frame_candidates(response):
                parts = GasAnalyzer._split_frame_parts(candidate)
                parsed = GasAnalyzer._parse_mode2(parts, response)
                if parsed is None:
                    parsed = GasAnalyzer._parse_legacy(parts, response)
                if parsed is not None:
                    break
            if parsed is None:
                continue
            payload = {
                "name": name.upper(),
                "port": port,
                "online": "● 在线",
                "timestamp": str(row.get("ts", "") or row.get("timestamp", "") or "").strip() or "--",
                "status": str(parsed.get("status") or "--"),
            }
            for field, _label in ANALYZER_MODE2_COLUMNS:
                if field in {"name", "port", "online", "timestamp", "status"}:
                    continue
                payload[field] = App._format_analyzer_live_value(parsed.get(field))
            values[name] = {
                **values.get(name, {}),
                **payload,
            }
        return values

    def _refresh_live_device_values(self, force: bool = False) -> None:
        now_ts = datetime.now().timestamp()
        io_path = self.current_io_path
        if io_path is None and self.logger is not None:
            io_path = Path(self.logger.io_path)
        if io_path is None or not io_path.exists():
            return
        run_dir = self.current_run_dir or io_path.parent
        runtime_cfg = self._load_runtime_config_snapshot(run_dir) if run_dir is not None else None
        if self._live_device_cache_run_dir != run_dir:
            self._live_device_cache = {}
            self._live_analyzer_cache = {}
            self._live_device_cache_run_dir = run_dir
            self._last_live_device_refresh_ts = 0.0
            self._last_live_analyzer_refresh_ts = 0.0
        refresh_devices = force or (
            now_ts - self._last_live_device_refresh_ts >= self._live_device_refresh_interval_s
        )
        refresh_analyzers = force or (
            not self._live_analyzer_cache
            or now_ts - self._last_live_analyzer_refresh_ts >= self._live_analyzer_refresh_interval_s
        )
        if not refresh_devices and not refresh_analyzers:
            return
        try:
            rows: List[Dict[str, str]] = []
            context_rows: List[Dict[str, str]] = []
            values = self._live_device_cache
            activity_timestamps: Dict[str, str] = {}
            route_state = "--"
            trends: Dict[str, Dict[str, Any]] = {}
            issues: Dict[str, Dict[str, Any]] = {}

            if refresh_devices:
                rows = self._tail_csv_rows(io_path, count=160)
                context_limit = 4000 if not self._live_device_cache else 600
                context_rows = self._tail_csv_rows(io_path, count=context_limit)
                values = self._merge_live_device_values(self._parse_live_device_values(context_rows))
                activity_timestamps = self._device_activity_timestamps(context_rows, runtime_cfg)
                route_state = self._infer_route_state(rows, runtime_cfg)
                trends = self._build_device_trends(rows, seconds=30)
                issues = self._build_device_issue_summaries(rows, seconds=60)

            analyzer_values = self._live_analyzer_cache
            if refresh_analyzers:
                analyzer_context_limit = 4000 if not self._live_analyzer_cache else 600
                analyzer_rows = context_rows if context_rows else self._tail_csv_rows(io_path, count=analyzer_context_limit)
                parsed_analyzer_values = self._parse_live_analyzer_values(analyzer_rows, runtime_cfg)
                if (
                    not self._live_analyzer_cache
                    and not any(self._analyzer_payload_has_live_values(payload) for payload in parsed_analyzer_values.values())
                ):
                    analyzer_rows = self._tail_csv_rows(io_path, count=30000)
                    parsed_analyzer_values = self._parse_live_analyzer_values(analyzer_rows, runtime_cfg)
                analyzer_values = self._merge_live_analyzer_values(parsed_analyzer_values)
                if not activity_timestamps:
                    activity_timestamps = self._device_activity_timestamps(analyzer_rows, runtime_cfg)
        except Exception:
            return
        if refresh_devices:
            self._last_live_device_refresh_ts = now_ts
            for key, payload in values.items():
                text = payload.get("text", "--")
                if key in self.device_vars:
                    self.device_vars[key].set(text)
                    state_text, level = self._device_state_from_text(key, text)
                    if key in {"pace", "gauge", "hgen", "dewpoint"}:
                        state_text = f"{state_text} | 工艺：{route_state}"
                    if key in self.device_state_vars:
                        self.device_state_vars[key].set(state_text)
                    if key in self.device_update_vars:
                        timestamp = payload.get("timestamp", "--") or "--"
                        self.device_update_vars[key].set(f"更新：{timestamp}")
                    primary, secondary = self._format_device_readout(key, text)
                    if key in self.device_display_primary_vars:
                        self.device_display_primary_vars[key].set(primary)
                    if key in self.device_display_secondary_vars:
                        self.device_display_secondary_vars[key].set(secondary)
                    if key in self.device_state_labels:
                        self._set_card_style(self.device_state_labels[key], level)
                    if key in self.device_online_vars:
                        online_text, online_level = self._compute_online_state(
                            str(payload.get("timestamp", "--") or "--"),
                            text,
                            activity_timestamps.get(key, "--"),
                        )
                        self.device_online_vars[key].set(online_text)
                        if key in self.device_online_labels:
                            self._set_card_style(self.device_online_labels[key], online_level)
        if refresh_analyzers:
            self._last_live_analyzer_refresh_ts = now_ts
        if self.analyzer_table is not None:
            online_count = 0
            latest_update = "--"
            active_focus = "--"
            for idx, (key, payload) in enumerate(analyzer_values.items()):
                item_id = self.analyzer_table_items.get(key)
                if not item_id:
                    continue
                timestamp_text = str(payload.get("timestamp", "--") or "--")
                online_text, _online_level = self._compute_online_state(
                    timestamp_text,
                    str(payload.get("co2_ppm", "--")),
                    activity_timestamps.get(key, "--"),
                )
                if online_text.startswith(("●", "◔")):
                    online_count += 1
                if timestamp_text not in {"", "--"} and (latest_update == "--" or timestamp_text > latest_update):
                    latest_update = timestamp_text
                if active_focus == "--":
                    co2_ppm = str(payload.get("co2_ppm", "--") or "--")
                    chamber_temp = str(payload.get("chamber_temp_c", "--") or "--")
                    if co2_ppm != "--" or chamber_temp != "--":
                        active_focus = f"{key.upper()}  CO2={co2_ppm}  腔温={chamber_temp}"
                if online_text.startswith("●"):
                    tag_state = "online"
                elif online_text.startswith(("◔", "◑")):
                    tag_state = "stale"
                else:
                    tag_state = "idle"
                row_tag = f"{tag_state}_{'even' if idx % 2 == 0 else 'odd'}"
                row_values = []
                for field, _label in ANALYZER_MODE2_COLUMNS:
                    if field == "online":
                        row_values.append(online_text)
                    else:
                        row_values.append(str(payload.get(field, "--") or "--"))
                self.analyzer_table.item(item_id, values=row_values, tags=(row_tag,))
            self.analyzer_summary_var.set(f"在线分析仪：{online_count} / 8")
            self.analyzer_update_summary_var.set(f"最近更新：{latest_update}")
            self.analyzer_focus_var.set(f"当前活跃：{active_focus}")
        if refresh_devices:
            pace_text = values.get("pace", {}).get("text", "")
            m = re.search(r"压力控制器：(-?\d+(?:\.\d+)?)\s*hPa", pace_text)
            self.current_pressure_live_var.set(
                f"当前实压：{float(m.group(1)):.2f}hPa" if m else "当前实压：--"
            )
            stable_match = re.search(r"稳定标志=(\d+)", pace_text)
            self.current_pressure_stability_var.set(
                f"稳定标志：{stable_match.group(1)}" if stable_match else "稳定标志：--"
            )
            target_block, reapply_count = self._parse_pressure_reapply_info(rows)
            current_target_match = re.search(r"当前压力点：(\d+)hPa", self.current_pressure_point_var.get())
            current_target = int(current_target_match.group(1)) if current_target_match else None
            if target_block is not None and (current_target is None or current_target == target_block):
                self.current_pressure_reapply_var.set(f"重发次数：{reapply_count}")
            else:
                self.current_pressure_reapply_var.set("重发次数：--")
            for key, trend in trends.items():
                if key in self.device_trend_vars:
                    self.device_trend_vars[key].set(str(trend.get("text", "30秒变化：--")))
                if key in self.device_trend_detail_vars:
                    self.device_trend_detail_vars[key].set(str(trend.get("detail", "最近30秒：无数据")))
                if key in self.device_trend_labels:
                    self._set_card_style(self.device_trend_labels[key], str(trend.get("level", "idle")))
                if key in self.device_trend_canvases:
                    self._draw_sparkline(
                        self.device_trend_canvases[key],
                        list(trend.get("values", [])),
                        str(trend.get("level", "idle")),
                    )
            for key, issue in issues.items():
                if key in self.device_issue_vars:
                    self.device_issue_vars[key].set(str(issue.get("text", "异常摘要：无")))
                if key in self.device_issue_time_vars:
                    self.device_issue_time_vars[key].set(f"异常时间：{issue.get('timestamp', '--')}")
                if key in self.device_issue_labels:
                    self._set_card_style(self.device_issue_labels[key], str(issue.get("level", "idle")))
                if key in self.device_issue_time_labels:
                    self._set_card_style(self.device_issue_time_labels[key], str(issue.get("level", "idle")))
                issue_box = self.device_issue_boxes.get(key)
                issue_text = str(issue.get("text", "异常摘要：无")).strip()
                issue_level = str(issue.get("level", "idle"))
                visible = issue_level not in {"idle", ""} and issue_text not in {"", "异常摘要：无"}
                if issue_box is not None:
                    if visible and not issue_box.winfo_manager():
                        update_label = self.device_update_labels.get(key)
                        if update_label is not None:
                            issue_box.pack(fill="x", pady=(2, 0), before=update_label)
                        else:
                            issue_box.pack(fill="x", pady=(2, 0))
                    elif not visible and issue_box.winfo_manager():
                        issue_box.pack_forget()

    @staticmethod
    def _format_device_readout(key: str, text: str) -> Tuple[str, str]:
        value = str(text or "").strip()
        if not value or value.endswith("：--"):
            title_map = {
                "pace": "压力控制器",
                "gauge": "数字气压计",
                "chamber": "温度箱",
                "hgen": "湿度发生器",
                "dewpoint": "露点仪",
            }
            return "--", title_map.get(key, "--")

        if key == "pace":
            m = re.search(r"压力控制器：(-?\d+(?:\.\d+)?)\s*hPa，稳定标志=(\d+)", value)
            if m:
                return f"{float(m.group(1)):.2f} hPa", f"稳定标志 {m.group(2)}"
        elif key == "gauge":
            m = re.search(r"数字气压计：(-?\d+(?:\.\d+)?)\s*hPa", value)
            if m:
                return f"{float(m.group(1)):.3f} hPa", "数字气压计实时值"
        elif key == "chamber":
            mt = re.search(r"温度=(\-?\d+(?:\.\d+)?)°C", value)
            mr = re.search(r"湿度=(\-?\d+(?:\.\d+)?)%", value)
            primary = f"{float(mt.group(1)):.1f}°C" if mt else "--"
            secondary = f"湿度 {float(mr.group(1)):.1f}%" if mr else "温度箱实时值"
            return primary, secondary
        elif key == "hgen":
            mu = re.search(r"Uw=(\-?\d+(?:\.\d+)?)%", value)
            mt = re.search(r"Tc=(\-?\d+(?:\.\d+)?)°C", value)
            md = re.search(r"Td=(\-?\d+(?:\.\d+)?)°C", value)
            primary = f"{float(mu.group(1)):.1f}%RH" if mu else "--"
            detail_parts: List[str] = []
            if mt:
                detail_parts.append(f"Tc {float(mt.group(1)):.2f}°C")
            if md:
                detail_parts.append(f"Td {float(md.group(1)):.2f}°C")
            return primary, " · ".join(detail_parts) if detail_parts else "湿度发生器实时值"
        elif key == "dewpoint":
            md = re.search(r"露点=(\-?\d+(?:\.\d+)?)°C", value)
            mt = re.search(r"温度=(\-?\d+(?:\.\d+)?)°C", value)
            mr = re.search(r"湿度=(\-?\d+(?:\.\d+)?)", value)
            primary = f"{float(md.group(1)):.2f}°C" if md else "--"
            detail_parts: List[str] = []
            if mt:
                detail_parts.append(f"温度 {float(mt.group(1)):.2f}°C")
            if mr:
                detail_parts.append(f"湿度 {float(mr.group(1)):.2f}")
            return primary, " · ".join(detail_parts) if detail_parts else "露点仪实时值"

        stripped = value.split("：", 1)
        if len(stripped) == 2:
            return stripped[1].strip(), stripped[0].strip()
        return value, "--"

    def _build_run_summary_text(self) -> str:
        lines = [
            "本次运行摘要",
            "",
            self.startup_summary_var.get(),
            self.valve_hint_var.get(),
            self.current_run_dir_name_var.get(),
            self.current_io_name_var.get(),
            self.runtime_config_diff_var.get(),
            self.summary_var.get(),
            self.stage_var.get(),
            self.target_var.get(),
            self.current_target_ppm_var.get(),
            self.current_pressure_point_var.get(),
            self.current_pressure_reapply_var.get(),
            self.current_route_group_detail_var.get(),
            self.progress_summary_var.get(),
            self.progress_detail_var.get(),
            self.route_group_var.get(),
            self.last_issue_var.get(),
            self.current_workbook_name_var.get(),
            self.current_latest_point_name_var.get(),
            "",
            "设备实时值",
        ]
        for key in ("pace", "gauge", "chamber", "hgen", "dewpoint"):
            lines.append(self.device_vars[key].get())
            lines.append(self.device_state_vars[key].get())
            lines.append(self.device_update_vars[key].get())
        lines.append("")
        lines.append("关键事件")
        event_text = self.event_text.get("1.0", "end").strip()
        lines.append(event_text or "暂无关键事件")
        lines.append("")
        lines.append("最近点位历史")
        if self.history_list.size() == 0:
            lines.append("暂无历史")
        else:
            for idx in range(self.history_list.size()):
                lines.append(self.history_list.get(idx))
        return "\n".join(lines)

    def _export_run_summary(self) -> None:
        initial_dir = str(self.current_run_dir) if self.current_run_dir is not None else str(Path.cwd())
        initial_file = "run_summary.txt"
        if self.current_run_dir is not None:
            initial_file = f"{self.current_run_dir.name}_summary.txt"
        target = filedialog.asksaveasfilename(
            title="导出本次运行摘要",
            defaultextension=".txt",
            filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")],
            initialdir=initial_dir,
            initialfile=initial_file,
        )
        if not target:
            return
        summary = self._build_run_summary_text()
        Path(target).write_text(summary, encoding="utf-8")
        self.log(f"运行摘要已导出：{target}")

    def _manual_refresh(self) -> None:
        self._refresh_execution_summary()
        self._refresh_progress_status()
        self._refresh_key_events()
        self._refresh_live_device_values(force=True)
        self._apply_banner_states()
        self.log("界面已手动刷新")

    def _set_coefficient_text(self, text: str) -> None:
        self._set_text_widget(self.coefficient_text, text)
        if hasattr(self, "status_coefficient_text"):
            self._set_text_widget(self.status_coefficient_text, text)
        if hasattr(self, "coefficient_page_text"):
            self._set_text_widget(self.coefficient_page_text, text)

    @staticmethod
    def _find_temperature_compensation_paths(run_dir: Path) -> Dict[str, Path]:
        paths: Dict[str, Path] = {}
        candidates = {
            "workbook": run_dir / "temperature_compensation.xlsx",
            "results_csv": run_dir / "temperature_compensation_coefficients.csv",
            "observations_csv": run_dir / "temperature_calibration_observations.csv",
            "commands_txt": run_dir / "temperature_compensation_commands.txt",
        }
        for key, path in candidates.items():
            if path.exists():
                paths[key] = path
        return paths

    @staticmethod
    def _load_temperature_compensation_rows(path: Path | None) -> List[Dict[str, str]]:
        if path is None or not path.exists():
            return []
        with path.open("r", newline="", encoding="utf-8-sig") as handle:
            return list(csv.DictReader(handle))

    @classmethod
    def _build_temperature_compensation_report_text(cls, run_dir: Path) -> str | None:
        paths = cls._find_temperature_compensation_paths(run_dir)
        results_csv = paths.get("results_csv")
        if results_csv is None:
            return None
        rows = cls._load_temperature_compensation_rows(results_csv)
        if not rows:
            return f"[{results_csv.name}]\n温度补偿结果：当前没有可显示的数据。"

        commands_path = paths.get("commands_txt")
        command_lines: List[str] = []
        if commands_path is not None and commands_path.exists():
            try:
                command_lines = [line.strip() for line in commands_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            except Exception:
                command_lines = []

        lines = [f"[{results_csv.name}]", "温度补偿结果:"]
        grouped: Dict[str, List[Dict[str, str]]] = {}
        for row in rows:
            analyzer_id = str(row.get("analyzer_id") or "--").strip() or "--"
            grouped.setdefault(analyzer_id, []).append(row)

        for analyzer_id in sorted(grouped.keys()):
            lines.append(f"  {analyzer_id}")
            for row in sorted(grouped[analyzer_id], key=lambda item: str(item.get("fit_type") or "")):
                fit_type = str(row.get("fit_type") or "--").strip()
                senco_channel = str(row.get("senco_channel") or "--").strip()
                availability = str(row.get("availability") or "--").strip()
                fit_ok = str(row.get("fit_ok") or "").strip()
                parts = [fit_type, senco_channel, f"状态={availability}", f"fit_ok={fit_ok or '--'}"]
                for key, label in (
                    ("n_points", "n"),
                    ("rmse", "RMSE"),
                    ("max_abs_error", "MaxErr"),
                ):
                    value = str(row.get(key) or "").strip()
                    if value:
                        parts.append(f"{label}={value}")
                lines.append("    " + " | ".join(parts))
                coeff_parts: List[str] = []
                for key in ("A", "B", "C", "D"):
                    value = str(row.get(key) or "").strip()
                    if value:
                        coeff_parts.append(f"{key}={value}")
                if coeff_parts:
                    lines.append("      " + " | ".join(coeff_parts))
                command = str(row.get("command_string") or "").strip()
                if command:
                    lines.append(f"      cmd={command}")
        if command_lines:
            lines.append("温补命令预览:")
            lines.extend(f"  {line}" for line in command_lines[:12])
        return "\n".join(lines)

    def _refresh_temperature_compensation_summary(self, run_dir: Path) -> str | None:
        paths = self._find_temperature_compensation_paths(run_dir)
        self.current_temperature_compensation_csv_path = paths.get("results_csv")
        self.current_temperature_compensation_commands_path = paths.get("commands_txt")
        self.current_temperature_compensation_report_path = (
            paths.get("workbook")
            or paths.get("results_csv")
            or paths.get("commands_txt")
        )
        if self.current_temperature_compensation_report_path is not None:
            self.current_temperature_compensation_name_var.set(
                f"温度补偿结果：{self.current_temperature_compensation_report_path.name}"
            )
            if not self.temperature_compensation_apply_in_progress:
                self.temperature_compensation_apply_status_var.set("温度补偿下发：待命")
            try:
                return self._build_temperature_compensation_report_text(run_dir)
            except Exception as exc:
                return f"[temperature_compensation]\n读取温补结果失败: {exc}"
            self.current_temperature_compensation_name_var.set("温度补偿结果：当前无文件")
        if not self.temperature_compensation_apply_in_progress:
                self.temperature_compensation_apply_status_var.set("温度补偿下发：待命")
        return None

    def _set_modeling_text(self, text: str) -> None:
        self._set_text_widget(self.modeling_text, text)

    def _refresh_progress_status(self) -> None:
        run_dir = self.current_run_dir
        if run_dir is None:
            if self.current_io_path is not None:
                run_dir = Path(self.current_io_path).parent
            elif self.logger is not None:
                run_dir = Path(self.logger.run_dir)
        if run_dir is None or not run_dir.exists():
            self.progress_summary_var.set("进度：--")
            self.progress_detail_var.set("已完成：0 | 已跳过：0 | 总点数：--")
            self.route_group_var.set("当前气路组别：--")
            self.last_issue_var.set("最近一次异常：--")
            self.current_route_group_detail_var.set("当前气路组：--")
            self.progress_var.set(0.0)
            self.stat_completed_var.set("成功 0")
            self.stat_skipped_var.set("跳过 0")
            self.stat_failed_var.set("失败 0")
            self.sample_progress_var.set("采样进度：--")
            self.data_freshness_var.set("数据刷新：--")
            self.current_workbook_path = None
            self.current_summary_report_path = None
            self.current_summary_report_paths = []
            self.current_latest_point_path = None
            self.current_coefficient_report_path = None
            self.current_temperature_compensation_report_path = None
            self.current_temperature_compensation_csv_path = None
            self.current_temperature_compensation_commands_path = None
            self.history_item_paths = {}
            self.history_items_cache = []
            self.current_workbook_name_var.set("Workbook：--")
            self.current_summary_report_name_var.set("汇总表：--")
            self.current_latest_point_name_var.set("最新点文件：--")
            self.current_coefficient_report_name_var.set("气体拟合报告：--")
            self.current_temperature_compensation_name_var.set("温度补偿结果：--")
            self.temperature_compensation_apply_status_var.set("温度补偿下发：待命")
            self.current_run_dir_name_var.set("Run目录：当前无目录")
            self.current_io_name_var.set("IO文件：当前无文件")
            self.runtime_config_diff_var.set("配置差异：当前无运行")
            self.summary_cfg_card_var.set("配置状态\n当前无运行")
            self._sync_summary_card_display()
            self.current_pressure_live_var.set("当前实压：--")
            self.current_pressure_stability_var.set("稳定标志：--")
            self.current_pressure_reapply_var.set("重发次数：--")
            self.history_list.delete(0, "end")
            self._set_coefficient_text("当前没有可显示的气体拟合或温度补偿结果。")
            self._set_open_buttons_state()
            self._set_card_style(self.stat_completed_label, "idle")
            self._set_card_style(self.stat_skipped_label, "idle")
            self._set_card_style(self.stat_failed_label, "idle")
            self._set_card_style(self.sample_progress_label, "idle")
            self._set_card_style(self.data_freshness_label, "idle")
            self._apply_banner_states()
            return
        try:
            progress = self._compute_progress_status(run_dir)
        except Exception:
            return

        total_text = str(progress["total"]) if progress["total"] is not None else "--"
        self.current_run_dir = run_dir
        self.current_run_dir_name_var.set(f"Run目录：{run_dir.name}")
        self.current_io_path = self._latest_io_path(run_dir)
        if self.current_io_path is not None:
            self.current_io_name_var.set(f"IO文件：{self.current_io_path.name}")
        else:
            self.current_io_name_var.set("IO文件：当前无文件")
        self.runtime_config_diff_var.set(self._build_runtime_config_diff_text(run_dir))
        cfg_text = self.runtime_config_diff_var.get().removeprefix("配置差异：").strip() or "未对比"
        self.summary_cfg_card_var.set(f"配置状态\n{cfg_text}")
        self._sync_summary_card_display()
        self.current_workbook_path = self._find_latest_workbook(run_dir)
        if self.current_workbook_path is not None:
            self.current_workbook_name_var.set(f"Workbook：{self.current_workbook_path.name}")
        else:
            self.current_workbook_name_var.set("Workbook：当前无文件")
        self.current_summary_report_paths = self._find_summary_report_paths(run_dir)
        if self.current_summary_report_paths:
            self.current_summary_report_path = max(self.current_summary_report_paths, key=lambda path: path.stat().st_mtime)
            latest_names = [
                path.name
                for path in sorted(self.current_summary_report_paths, key=lambda path: path.stat().st_mtime, reverse=True)[:2]
            ]
            self.current_summary_report_name_var.set(
                f"汇总表：{'；'.join(latest_names)}（共 {len(self.current_summary_report_paths)} 份）"
            )
        else:
            self.current_summary_report_path = None
            self.current_summary_report_name_var.set("汇总表：当前无文件")
        self._refresh_coefficient_report_summary(run_dir)
        self.history_item_paths = self._build_history_item_paths(run_dir, progress.get("recent_points", []))
        self.current_latest_point_path = self._find_latest_point_path(progress.get("recent_points", []), self.history_item_paths)
        if self.current_latest_point_path is not None:
            self.current_latest_point_name_var.set(f"最新点文件：{self.current_latest_point_path.name}")
        else:
            self.current_latest_point_name_var.set("最新点文件：当前无文件")
        self.progress_summary_var.set(f"进度：当前={progress['current']}")
        self.progress_detail_var.set(
            f"已完成：{progress['completed']} | 已跳过：{progress['skipped']} | 总点数：{total_text}"
        )
        self.route_group_var.set(f"当前气路组别：{progress['route_group']}")
        self.last_issue_var.set(f"最近一次异常：{progress['last_issue']}")
        self.current_route_group_detail_var.set(f"当前气路组：{progress['route_group']}")
        self.progress_var.set(float(progress["percent"]))
        self.stat_completed_var.set(f"成功 {progress['completed']}")
        self.stat_skipped_var.set(f"跳过 {progress['skipped']}")
        self.stat_failed_var.set(f"失败 {progress['failed']}")
        self.sample_progress_var.set(progress.get("sample_progress", "采样进度：--"))
        self.data_freshness_var.set(progress.get("freshness_text", "数据刷新：--"))
        self._set_card_style(self.stat_completed_label, "ok")
        self._set_card_style(self.stat_skipped_label, "warn")
        self._set_card_style(self.stat_failed_label, "error" if progress["failed"] > 0 else "idle")
        self._set_card_style(
            self.sample_progress_label,
            "info" if "等待开始" not in self.sample_progress_var.get() and "--" not in self.sample_progress_var.get() else "idle",
        )
        self._set_card_style(self.data_freshness_label, progress.get("freshness_level", "idle"))
        self.history_items_cache = list(progress.get("recent_points", []))
        self._refresh_history_list(progress["current"])
        self._set_open_buttons_state()
        self._apply_banner_states()

    def _refresh_history_list(self, current: str | None = None) -> None:
        if current is None:
            current_text = self.progress_summary_var.get().removeprefix("进度：当前=").strip()
        else:
            current_text = current.strip()
        selected_filter = self.history_filter_var.get().strip() if hasattr(self, "history_filter_var") else "全部"
        self.history_list.delete(0, "end")
        for item in self.history_items_cache:
            level = self._history_item_level(item, current_text)
            if not self._history_item_matches_filter(level, selected_filter):
                continue
            self.history_list.insert("end", item)
            palette = self.state_palette.get(level, self.state_palette["idle"])
            self.history_list.itemconfig("end", bg=palette["bg"], fg=palette["fg"])

    @staticmethod
    def _history_item_matches_filter(level: str, selected_filter: str) -> bool:
        filter_map = {
            "全部": None,
            "成功": "ok",
            "跳过": "warn",
            "当前": "info",
        }
        expected = filter_map.get(selected_filter)
        if expected is None:
            return True
        return level == expected

    @staticmethod
    def _find_latest_workbook(run_dir: Path) -> Path | None:
        candidates = sorted(run_dir.glob("*_analyzer_sheets_*.xlsx"))
        if not candidates:
            return None
        return max(candidates, key=lambda path: path.stat().st_mtime)

    @staticmethod
    def _find_summary_report_paths(run_dir: Path) -> List[Path]:
        patterns = (
            "分析仪汇总_*.csv",
            "分析仪汇总_*.xlsx",
            "分析仪汇总_水路_*.csv",
            "分析仪汇总_水路_*.xlsx",
            "分析仪汇总_气路_*.csv",
            "分析仪汇总_气路_*.xlsx",
        )
        found: Dict[str, Path] = {}
        for pattern in patterns:
            for path in run_dir.glob(pattern):
                if path.is_file():
                    found[str(path.resolve())] = path
        return sorted(found.values(), key=lambda path: (path.stat().st_mtime, path.name))

    @staticmethod
    def _find_coefficient_report_paths(run_dir: Path) -> List[Path]:
        patterns = ("*_fit_*.json", "*_fit_*_residuals.csv")
        found: Dict[str, Path] = {}
        for pattern in patterns:
            for path in run_dir.glob(pattern):
                if path.is_file():
                    found[str(path.resolve())] = path
        return sorted(found.values(), key=lambda path: (path.stat().st_mtime, path.name))

    @staticmethod
    def _infer_coefficient_report_analyzer(path: Path, payload: Dict[str, Any]) -> str:
        analyzer = str(payload.get("analyzer") or payload.get("Analyzer") or "").strip()
        if analyzer:
            return analyzer
        match = re.match(r"^(co2|h2o)_([^_]+)_ratio_poly_fit_\d{8}_\d{6}$", path.stem, re.IGNORECASE)
        if match:
            return match.group(2)
        return "--"

    @staticmethod
    def _format_coefficient_number(value: Any) -> str:
        try:
            numeric = float(value)
        except Exception:
            return str(value)
        if numeric == 0.0:
            return "0"
        magnitude = abs(numeric)
        if magnitude >= 1000 or magnitude < 0.001:
            return f"{numeric:.6e}"
        return f"{numeric:.6f}".rstrip("0").rstrip(".")

    @classmethod
    def _coefficient_report_value(cls, payload: Any, *keys: str) -> Any:
        if not isinstance(payload, dict):
            return None
        for key in keys:
            value = payload.get(key)
            if value is not None:
                return value
        return None

    @classmethod
    def _append_coefficient_mapping(
        cls,
        lines: List[str],
        *,
        title: str,
        coefficients: Any,
        feature_terms: Dict[str, Any],
    ) -> None:
        if not isinstance(coefficients, dict) or not coefficients:
            return
        lines.append(title)
        for key, value in coefficients.items():
            term = feature_terms.get(str(key)) if isinstance(feature_terms, dict) else None
            if term:
                lines.append(f"  {key} ({term}) = {cls._format_coefficient_number(value)}")
            else:
                lines.append(f"  {key} = {cls._format_coefficient_number(value)}")

    @classmethod
    def _build_coefficient_metric_line(cls, title: str, metrics: Any) -> str | None:
        if not isinstance(metrics, dict):
            return None
        sample_count = cls._coefficient_report_value(metrics, "sample_count", "n", "Count")
        metric_block = metrics.get("simplified") if isinstance(metrics.get("simplified"), dict) else metrics
        parts = [title]
        if sample_count is not None:
            parts.append(f"n={sample_count}")
        for label, keys in (
            ("RMSE", ("RMSE", "rmse", "rmse_simplified")),
            ("R2", ("R2", "r2")),
            ("Bias", ("Bias", "bias")),
            ("MAE", ("MAE", "mae", "mae_simplified")),
            ("MaxErr", ("MaxError", "max_abs", "max_abs_simplified")),
        ):
            value = cls._coefficient_report_value(metric_block, *keys)
            if value is not None:
                parts.append(f"{label}={cls._format_coefficient_number(value)}")
        if len(parts) <= 1:
            return None
        return " | ".join(parts)

    @classmethod
    def _build_range_metric_lines(cls, title: str, ranges: Any) -> List[str]:
        if not isinstance(ranges, list):
            return []
        used_rows = [
            item for item in ranges if isinstance(item, dict) and int(item.get("Count") or 0) > 0
        ]
        if not used_rows:
            return []
        lines = [title]
        for item in used_rows[:6]:
            lines.append(
                "  "
                + " | ".join(
                    [
                        str(item.get("RangeLabel") or "--"),
                        f"n={item.get('Count', 0)}",
                        f"RMSE={cls._format_coefficient_number(item.get('RMSE'))}",
                        f"Bias={cls._format_coefficient_number(item.get('Bias'))}",
                        f"MaxErr={cls._format_coefficient_number(item.get('MaxError'))}",
                    ]
                )
            )
        return lines

    @classmethod
    def _build_coefficient_report_text(cls, path: Path) -> str:
        if path.suffix.lower() == ".csv":
            with path.open("r", newline="", encoding="utf-8-sig") as handle:
                reader = csv.DictReader(handle)
                fieldnames = list(reader.fieldnames or [])
                preview_rows: List[Dict[str, str]] = []
                row_count = 0
                for row in reader:
                    row_count += 1
                    if len(preview_rows) < 6:
                        preview_rows.append(row)

            lines = [
                f"[{path.name}]",
                f"类型: 残差表 | 行数: {row_count} | 列数: {len(fieldnames)}",
            ]
            if fieldnames:
                lines.append("列: " + ", ".join(fieldnames[:8]) + (" ..." if len(fieldnames) > 8 else ""))
            if preview_rows:
                lines.append("预览:")
                preview_fields = fieldnames[: min(5, len(fieldnames))]
                for row in preview_rows:
                    cells = [f"{key}={row.get(key, '')}" for key in preview_fields]
                    lines.append("  " + " | ".join(cells))
            else:
                lines.append("预览: 当前表内没有数据行")
            return "\n".join(lines)

        payload = json.loads(path.read_text(encoding="utf-8"))
        model = str(payload.get("model") or "--")
        gas = str(payload.get("gas") or "--").upper()
        analyzer = cls._infer_coefficient_report_analyzer(path, payload)
        sample_count = payload.get("n", "--")
        stats = payload.get("stats") if isinstance(payload.get("stats"), dict) else {}
        rmse_original = cls._coefficient_report_value(stats, "rmse_original", "rmse")
        rmse_simplified = cls._coefficient_report_value(stats, "rmse_simplified", "rmse")
        max_abs = cls._coefficient_report_value(stats, "max_abs_simplified", "max_abs")
        mae_simplified = cls._coefficient_report_value(stats, "mae_simplified", "mae")
        rmse_change = cls._coefficient_report_value(stats, "rmse_change")
        r2_value = cls._coefficient_report_value(stats, "r2")
        feature_terms = payload.get("feature_terms") if isinstance(payload.get("feature_terms"), dict) else {}
        coefficients = payload.get("simplified_coefficients")
        if not isinstance(coefficients, dict):
            coefficients = payload.get("coeffs") if isinstance(payload.get("coeffs"), dict) else {}
        original_coefficients = (
            payload.get("original_coefficients") if isinstance(payload.get("original_coefficients"), dict) else {}
        )
        fit_settings = stats.get("fit_settings") if isinstance(stats.get("fit_settings"), dict) else {}
        split_info = stats.get("dataset_split") if isinstance(stats.get("dataset_split"), dict) else {}
        simplification_summary = (
            stats.get("simplification_summary") if isinstance(stats.get("simplification_summary"), dict) else {}
        )
        model_features = stats.get("model_features") if isinstance(stats.get("model_features"), list) else []
        cross_coefficients = (
            payload.get("H2O_cross_coefficients") if isinstance(payload.get("H2O_cross_coefficients"), dict) else {}
        )

        lines = [
            f"[{path.name}]",
            f"模型: {model} | 气体: {gas} | 分析仪: {analyzer} | 样本: {sample_count}",
            (
                "RMSE(原始): "
                f"{cls._format_coefficient_number(rmse_original)} | "
                "RMSE(简化): "
                f"{cls._format_coefficient_number(rmse_simplified)} | "
                f"MaxAbs: {cls._format_coefficient_number(max_abs)}"
            ),
        ]
        summary_parts: List[str] = []
        if mae_simplified is not None:
            summary_parts.append(f"MAE(简化): {cls._format_coefficient_number(mae_simplified)}")
        if r2_value is not None:
            summary_parts.append(f"R2: {cls._format_coefficient_number(r2_value)}")
        if rmse_change is not None:
            summary_parts.append(f"RMSE变化: {cls._format_coefficient_number(rmse_change)}")
        if summary_parts:
            lines.append(" | ".join(summary_parts))

        split_parts: List[str] = []
        for key, label in (
            ("fit_count", "拟合"),
            ("train_count", "训练"),
            ("validation_count", "验证"),
            ("test_count", "测试"),
        ):
            value = split_info.get(key)
            if value is not None:
                split_parts.append(f"{label}={value}")
        if split_info.get("fit_scope"):
            split_parts.append(f"范围={split_info['fit_scope']}")
        if split_parts:
            lines.append("数据划分: " + " | ".join(split_parts))

        setting_parts: List[str] = []
        if fit_settings.get("fitting_method"):
            setting_parts.append(f"拟合={fit_settings['fitting_method']}")
        if fit_settings.get("simplification_method"):
            setting_parts.append(f"简化={fit_settings['simplification_method']}")
        selected_digits = simplification_summary.get("selected_digits")
        if selected_digits is not None:
            setting_parts.append(f"有效位={selected_digits}")
        if model_features:
            setting_parts.append("特征=" + ",".join(str(item) for item in model_features))
        if setting_parts:
            lines.append("拟合设置: " + " | ".join(setting_parts))

        metric_lines = [
            cls._build_coefficient_metric_line("训练集(简化)", stats.get("train_metrics")),
            cls._build_coefficient_metric_line("验证集(简化)", stats.get("validation_metrics")),
            cls._build_coefficient_metric_line("测试集(简化)", stats.get("test_metrics")),
        ]
        metric_lines = [line for line in metric_lines if line]
        if metric_lines:
            lines.append("结果表现:")
            lines.extend(f"  {line}" for line in metric_lines)

        range_lines: List[str] = []
        for title, metrics_key in (
            ("测试集分段表现(简化):", "test_metrics"),
            ("验证集分段表现(简化):", "validation_metrics"),
            ("训练集分段表现(简化):", "train_metrics"),
        ):
            metrics_payload = stats.get(metrics_key)
            if not isinstance(metrics_payload, dict):
                continue
            range_lines = cls._build_range_metric_lines(title, metrics_payload.get("range_simplified"))
            if range_lines:
                break
        if range_lines:
            lines.extend(range_lines)

        cls._append_coefficient_mapping(
            lines,
            title="最终系数(简化):",
            coefficients=coefficients,
            feature_terms=feature_terms,
        )
        if original_coefficients:
            cls._append_coefficient_mapping(
                lines,
                title="原始系数:",
                coefficients=original_coefficients,
                feature_terms=feature_terms,
            )
        if cross_coefficients:
            cls._append_coefficient_mapping(
                lines,
                title="H2O交叉系数:",
                coefficients=cross_coefficients,
                feature_terms={},
            )
        if not coefficients and not original_coefficients:
            lines.append("系数: 当前报告未提供可显示的系数项")
        return "\n".join(lines)

    def _refresh_coefficient_report_summary(self, run_dir: Path) -> None:
        report_paths = self._find_coefficient_report_paths(run_dir)
        temp_comp_text = self._refresh_temperature_compensation_summary(run_dir)
        if not report_paths and not temp_comp_text:
            self.current_coefficient_report_path = None
            self.current_coefficient_report_name_var.set("气体拟合报告：当前无文件")
            self._set_coefficient_text("当前运行目录里还没有气体拟合报告或温度补偿结果。")
            return

        sections: List[str] = []
        if report_paths:
            json_report_paths = [path for path in report_paths if path.suffix.lower() == ".json"]
            preferred_paths = json_report_paths or report_paths
            self.current_coefficient_report_path = max(preferred_paths, key=lambda path: path.stat().st_mtime)
            self.current_coefficient_report_name_var.set(
                f"气体拟合报告：{self.current_coefficient_report_path.name}（共 {len(report_paths)} 份）"
            )
            for path in sorted(report_paths, key=lambda item: item.stat().st_mtime, reverse=True):
                try:
                    sections.append(self._build_coefficient_report_text(path))
                except Exception as exc:
                    sections.append(f"[{path.name}]\n读取失败: {exc}")
        else:
            self.current_coefficient_report_path = None
            self.current_coefficient_report_name_var.set("气体拟合报告：当前无文件")
        if temp_comp_text:
            sections.append(temp_comp_text)
        self._set_coefficient_text("\n\n".join(sections))

    @staticmethod
    def _runtime_config_snapshot_path(run_dir: Path) -> Path:
        return run_dir / "runtime_config_snapshot.json"

    @staticmethod
    def _write_runtime_config_snapshot(run_dir: Path, runtime_cfg: Dict[str, Any]) -> None:
        App._runtime_config_snapshot_path(run_dir).write_text(
            json.dumps(runtime_cfg, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _path_cache_signature(path: Path | None) -> Tuple[str, int, int] | None:
        if path is None or not path.exists():
            return None
        try:
            stat = path.stat()
        except Exception:
            return None
        return (str(path.resolve()), int(stat.st_mtime_ns), int(stat.st_size))

    @staticmethod
    def _load_runtime_config_snapshot(run_dir: Path) -> Dict[str, Any] | None:
        path = App._runtime_config_snapshot_path(run_dir)
        if not path.exists():
            return None
        signature = App._path_cache_signature(path)
        cache = getattr(App, "_runtime_snapshot_cache", {})
        if signature is not None:
            cached = cache.get(signature[0])
            if isinstance(cached, tuple) and len(cached) == 2 and cached[0] == signature:
                return cached[1]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            data = None
        if signature is not None:
            cache[signature[0]] = (signature, data)
            App._runtime_snapshot_cache = cache
        return data

    @staticmethod
    def _sampling_target_from_run_dir(run_dir: Path) -> int:
        snapshot = App._load_runtime_config_snapshot(run_dir)
        if not isinstance(snapshot, dict):
            return 10
        workflow_cfg = snapshot.get("workflow", {})
        if not isinstance(workflow_cfg, dict):
            return 10
        sampling_cfg = workflow_cfg.get("sampling", {})
        if not isinstance(sampling_cfg, dict):
            return 10
        try:
            target = int(sampling_cfg.get("count", 10))
        except Exception:
            return 10
        return max(1, target)

    @staticmethod
    def _config_value_by_path(cfg: Dict[str, Any], path: str) -> Any:
        current: Any = cfg
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]
        return current

    def _build_runtime_config_diff_text(self, run_dir: Path) -> str:
        snapshot_path = self._runtime_config_snapshot_path(run_dir)
        snapshot_signature = self._path_cache_signature(snapshot_path)
        snapshot = self._load_runtime_config_snapshot(run_dir)
        current_cfg = self.last_runtime_cfg or getattr(self, "cfg", None)
        if snapshot is None:
            return "配置差异：本轮无快照，当前配置可能与本轮不同"
        if not isinstance(current_cfg, dict):
            return "配置差异：当前配置未加载"

        tracked = [
            ("workflow.route_mode", "模式"),
            ("workflow.selected_temps_c", "温度点"),
            ("workflow.skip_co2_ppm", "跳过气点"),
            ("workflow.selected_pressure_points", "压力点"),
        ]
        label_overrides = {
            "workflow.stability.h2o_route.preseal_soak_s": "水路开路等待",
            "workflow.stability.co2_route.preseal_soak_s": "气路通气等待",
            "workflow.stability.co2_route.first_point_preseal_soak_s": "首个气点通气等待",
            "workflow.pressure.stabilize_timeout_s": "压力稳定超时",
            "workflow.sampling.count": "采样条数",
            "workflow.sampling.h2o_interval_s": "水路采样间隔",
            "workflow.sampling.co2_interval_s": "气路采样间隔",
        }
        for spec in get_workflow_tunable_parameters():
            tracked.append((spec.path, label_overrides.get(spec.path, spec.label)))
        current_signature = tuple(
            (
                path,
                json.dumps(
                    self._config_value_by_path(current_cfg, path),
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                ),
            )
            for path, _label in tracked
        )
        cache_key = (str(run_dir.resolve()), snapshot_signature, current_signature)
        if cache_key == self._runtime_config_diff_cache_key:
            return self._runtime_config_diff_cache_text
        diffs: List[str] = []
        for path, label in tracked:
            old_val = self._config_value_by_path(snapshot, path)
            new_val = self._config_value_by_path(current_cfg, path)
            if old_val != new_val:
                diffs.append(f"{label} {old_val} -> {new_val}")
        if not diffs:
            text = "配置差异：当前配置与本轮一致"
        else:
            text = "配置差异：" + "；".join(diffs[:3])
        self._runtime_config_diff_cache_key = cache_key
        self._runtime_config_diff_cache_text = text
        return text

    @staticmethod
    def _build_history_item_paths(run_dir: Path, items: List[str]) -> Dict[str, Path]:
        mapping: Dict[str, Path] = {}
        for item in items:
            text = item.strip()
            if not text:
                continue
            saved_match = re.search(r"Point\s+\d+\s+samples saved:\s*(.+)$", text)
            if saved_match:
                candidate = Path(saved_match.group(1).strip())
                if not candidate.is_absolute():
                    candidate = run_dir / candidate
                if candidate.exists():
                    mapping[text] = candidate
                    continue

            co2_match = re.match(r"CO2\s+(\d+)ppm\s+(\d+)hPa$", text)
            if co2_match:
                ppm = co2_match.group(1)
                pressure = co2_match.group(2)
                candidates = sorted(run_dir.glob(f"point_*_co2_*_{ppm}ppm_{pressure}hpa_samples.csv"))
                if candidates:
                    mapping[text] = candidates[-1]
                    continue

            h2o_match = re.match(r"H2O row\s+(\d+)$", text)
            if h2o_match:
                row_id = int(h2o_match.group(1))
                candidate = run_dir / f"point_{row_id:04d}_h2o_samples.csv"
                if candidate.exists():
                    mapping[text] = candidate
        return mapping

    def _set_open_buttons_state(self) -> None:
        latest_point_state = "normal" if self.current_latest_point_path is not None else "disabled"
        workbook_state = "normal" if self.current_workbook_path is not None else "disabled"
        summary_state = "normal" if self.current_summary_report_path is not None else "disabled"
        run_dir_state = "normal" if self.current_run_dir is not None and self.current_run_dir.exists() else "disabled"
        coefficient_state = "normal" if self.current_coefficient_report_path is not None else "disabled"
        temperature_compensation_state = (
            "normal" if self.current_temperature_compensation_report_path is not None else "disabled"
        )
        temperature_compensation_apply_state = (
            "disabled"
            if self.temperature_compensation_apply_in_progress or self._controls_locked()
            else ("normal" if self.current_temperature_compensation_csv_path is not None else "disabled")
        )
        modeling_config_state = "normal" if self._modeling_config_path().exists() else "disabled"
        modeling_result_state = "normal" if self.current_modeling_result_path is not None else "disabled"
        modeling_runner_state = "normal" if (Path(__file__).resolve().parents[3] / "run_modeling_analysis.py").exists() else "disabled"
        self.open_latest_point_button.configure(state=latest_point_state)
        self.open_workbook_button.configure(state=workbook_state)
        self.open_summary_report_button.configure(state=summary_state)
        self.open_run_dir_button.configure(state=run_dir_state)
        self.open_coefficient_report_button.configure(state=coefficient_state)
        if hasattr(self, "open_coefficient_tab_button"):
            self.open_coefficient_tab_button.configure(state=coefficient_state)
        if hasattr(self, "open_temperature_compensation_button"):
            self.open_temperature_compensation_button.configure(state=temperature_compensation_state)
        if hasattr(self, "apply_temperature_compensation_button"):
            self.apply_temperature_compensation_button.configure(state=temperature_compensation_apply_state)
        self.open_modeling_config_button.configure(state=modeling_config_state)
        self.open_modeling_result_button.configure(state=modeling_result_state)
        self.run_modeling_button.configure(state=modeling_runner_state)
        self.export_summary_button.configure(state=run_dir_state)

    @staticmethod
    def _find_latest_point_path(items: List[str], mapping: Dict[str, Path]) -> Path | None:
        for item in reversed(items):
            path = mapping.get(item.strip())
            if path is not None and path.exists():
                return path
        return None

    @staticmethod
    def _history_item_level(item: str, current: str) -> str:
        text = item.strip()
        if not text:
            return "idle"
        if text == (current or "").strip():
            return "info"
        if " skipped:" in text:
            return "warn"
        if " samples saved" in text:
            return "ok"
        return "idle"

    @staticmethod
    def _open_path(path: Path) -> None:
        os.startfile(str(path))

    def _open_current_workbook(self) -> None:
        if self.current_workbook_path is None or not self.current_workbook_path.exists():
            messagebox.showinfo("提示", "当前没有可打开的 Workbook。")
            return
        self._open_path(self.current_workbook_path)

    def _open_current_summary_report(self) -> None:
        if self.current_summary_report_path is None or not self.current_summary_report_path.exists():
            messagebox.showinfo("提示", "当前没有可打开的汇总表。")
            return
        self._open_path(self.current_summary_report_path)

    def _open_latest_point_file(self) -> None:
        if self.current_latest_point_path is None or not self.current_latest_point_path.exists():
            messagebox.showinfo("提示", "当前没有可打开的最新点文件。")
            return
        self._open_path(self.current_latest_point_path)

    def _open_current_run_dir(self) -> None:
        if self.current_run_dir is None or not self.current_run_dir.exists():
            messagebox.showinfo("提示", "当前没有可打开的 Run 目录。")
            return
        self._open_path(self.current_run_dir)

    def _open_current_coefficient_report(self) -> None:
        if self.current_coefficient_report_path is None or not self.current_coefficient_report_path.exists():
            messagebox.showinfo("提示", "当前没有可打开的系数报告。")
            return
        self._open_path(self.current_coefficient_report_path)

    def _open_current_temperature_compensation_report(self) -> None:
        if (
            self.current_temperature_compensation_report_path is None
            or not self.current_temperature_compensation_report_path.exists()
        ):
            messagebox.showinfo("提示", "当前没有可打开的温补结果。")
            return
        self._open_path(self.current_temperature_compensation_report_path)

    @staticmethod
    def _parse_temperature_compensation_bool(value: Any) -> bool:
        text = str(value or "").strip().lower()
        return text in {"1", "true", "yes", "y"}

    def _temperature_compensation_apply_rows(self) -> List[Dict[str, str]]:
        return self._load_temperature_compensation_rows(self.current_temperature_compensation_csv_path)

    @staticmethod
    def _build_temperature_compensation_apply_plan(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        plan: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for row in rows:
            analyzer_id = str(row.get("analyzer_id") or "").strip().upper()
            fit_type = str(row.get("fit_type") or "").strip().lower()
            availability = str(row.get("availability") or "").strip().lower()
            fit_ok = App._parse_temperature_compensation_bool(row.get("fit_ok"))
            if not analyzer_id or fit_type not in {"cell", "shell"}:
                continue
            if availability != "available" or not fit_ok:
                continue
            try:
                coeffs = {
                    "A": float(row.get("A", 0.0)),
                    "B": float(row.get("B", 0.0)),
                    "C": float(row.get("C", 0.0)),
                    "D": float(row.get("D", 0.0)),
                }
            except Exception:
                continue
            plan.setdefault(analyzer_id, {})[fit_type] = coeffs
        return plan

    @staticmethod
    def _temperature_compensation_apply_summary(plan: Dict[str, Dict[str, Dict[str, Any]]]) -> str:
        if not plan:
            return "当前没有可下发的温补系数。"
        lines = ["即将下发以下温补系数："]
        for analyzer_id in sorted(plan.keys()):
            fit_types = []
            if "cell" in plan[analyzer_id]:
                fit_types.append("SENCO7(腔温)")
            if "shell" in plan[analyzer_id]:
                fit_types.append("SENCO8(壳温)")
            lines.append(f"- {analyzer_id}: {'、'.join(fit_types) if fit_types else '无'}")
        lines.append("")
        lines.append("将按当前配置临时连接分析仪，写入后立即断开。")
        return "\n".join(lines)

    @staticmethod
    def _build_gas_analyzers_for_temperature_compensation(cfg: Dict[str, Any], io_logger=None) -> Dict[str, GasAnalyzer]:
        devices: Dict[str, GasAnalyzer] = {}
        dcfg = cfg.get("devices", {}) if isinstance(cfg, dict) else {}
        gas_list_cfg = dcfg.get("gas_analyzers", [])
        if isinstance(gas_list_cfg, list) and gas_list_cfg:
            for idx, gcfg in enumerate(gas_list_cfg, start=1):
                if not isinstance(gcfg, dict) or not gcfg.get("enabled", True):
                    continue
                label = str(gcfg.get("name") or f"ga{idx:02d}").strip().upper()
                dev = GasAnalyzer(
                    gcfg["port"],
                    gcfg.get("baud", 115200),
                    device_id=gcfg.get("device_id", f"{idx:03d}"),
                    io_logger=io_logger,
                )
                dev.open()
                devices[label] = dev
            return devices

        gas_cfg = dcfg.get("gas_analyzer", {})
        if isinstance(gas_cfg, dict) and gas_cfg.get("enabled", False):
            dev = GasAnalyzer(
                gas_cfg["port"],
                gas_cfg.get("baud", 115200),
                device_id=gas_cfg.get("device_id", "000"),
                io_logger=io_logger,
            )
            dev.open()
            devices["GA01"] = dev
        return devices

    def _set_temperature_compensation_apply_state(self, in_progress: bool, text: str) -> None:
        self.temperature_compensation_apply_in_progress = in_progress
        self.temperature_compensation_apply_status_var.set(text)
        self._set_open_buttons_state()

    def _run_temperature_compensation_apply(self, cfg: Dict[str, Any], plan: Dict[str, Dict[str, Dict[str, Any]]]) -> None:
        tmp_logger: RunLogger | None = None
        devices: Dict[str, GasAnalyzer] = {}
        summary_lines: List[str] = []
        try:
            tmp_logger = RunLogger(Path(cfg["paths"]["output_dir"]), cfg=cfg)
            self.log(f"温度补偿下发日志目录：{tmp_logger.run_dir}")
            devices = self._build_gas_analyzers_for_temperature_compensation(cfg, io_logger=tmp_logger)
            if not devices:
                raise RuntimeError("当前配置下没有可连接的分析仪")

            success_count = 0
            warning_lines: List[str] = []
            for analyzer_id in sorted(plan.keys()):
                dev = devices.get(analyzer_id)
                if dev is None:
                    warning_lines.append(f"{analyzer_id}: 当前配置未启用或未找到对应分析仪")
                    continue
                fit_map = plan[analyzer_id]
                writes: List[Tuple[int, Dict[str, Any]]] = []
                if "cell" in fit_map:
                    writes.append((7, fit_map["cell"]))
                if "shell" in fit_map:
                    writes.append((8, fit_map["shell"]))
                if not writes:
                    continue
                mode_switch_attempted = False
                try:
                    mode_switch_attempted = True
                    if not dev.set_mode(2):
                        warning_lines.append(f"{analyzer_id}: MODE=2 未收到确认，已跳过温补写入")
                        continue
                    for group, coeffs in writes:
                        acked = dev.set_senco(group, coeffs["A"], coeffs["B"], coeffs["C"], coeffs["D"])
                        if acked:
                            summary_lines.append(f"{analyzer_id}: SENCO{group} 写入成功")
                            self.log(
                                f"{analyzer_id}: wrote SENCO{group} "
                                + ",".join(format_senco_values((coeffs['A'], coeffs['B'], coeffs['C'], coeffs['D'])))
                            )
                            success_count += 1
                        else:
                            warning_lines.append(f"{analyzer_id}: SENCO{group} 未收到确认")
                finally:
                    if mode_switch_attempted:
                        try:
                            if not dev.set_mode(1):
                                warning_lines.append(f"{analyzer_id}: MODE=1 未收到确认")
                        except Exception as exc:
                            warning_lines.append(f"{analyzer_id}: 退出 MODE=1 失败: {exc}")

            if warning_lines:
                message = "\n".join(summary_lines + warning_lines) if summary_lines else "\n".join(warning_lines)
                self.log("温度补偿下发完成（含告警）: " + " | ".join(summary_lines + warning_lines))
                self._call_on_ui_thread(
                    messagebox.showwarning,
                    "温度补偿下发完成",
                    message,
                    parent=self.root,
                )
                status_text = f"温度补偿下发：完成，成功 {success_count} 项，部分告警"
            else:
                message = "\n".join(summary_lines) if summary_lines else "没有执行任何温补写入。"
                self.log("温度补偿下发完成: " + " | ".join(summary_lines or [message]))
                self._call_on_ui_thread(
                    messagebox.showinfo,
                    "温度补偿下发完成",
                    message,
                    parent=self.root,
                )
                status_text = f"温度补偿下发：完成，成功 {success_count} 项"
        except Exception as exc:
            self.log(f"温度补偿下发失败：{exc}")
            self._call_on_ui_thread(
                messagebox.showerror,
                "温度补偿下发失败",
                str(exc),
                parent=self.root,
            )
            status_text = f"温度补偿下发：失败，{exc}"
        finally:
            self._close_devices(devices)
            if tmp_logger is not None:
                try:
                    tmp_logger.close()
                except Exception:
                    pass
            self._call_on_ui_thread(self._set_temperature_compensation_apply_state, False, status_text)

    def _apply_current_temperature_compensation(self) -> None:
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("提示", "流程运行中，暂不允许下发温度补偿系数。")
            return
        if self.startup_thread and self.startup_thread.is_alive():
            messagebox.showinfo("提示", "流程正在启动，暂不允许下发温度补偿系数。")
            return

        rows = self._temperature_compensation_apply_rows()
        plan = self._build_temperature_compensation_apply_plan(rows)
        if not plan:
            messagebox.showinfo("提示", "当前温补结果里没有 fit_ok 且可下发的系数。")
            return

        cfg = copy.deepcopy(self.last_runtime_cfg or getattr(self, "cfg", {}))
        if not cfg:
            messagebox.showerror("温度补偿下发失败", "当前没有可用配置。")
            return

        confirm_text = self._temperature_compensation_apply_summary(plan)
        if not messagebox.askokcancel("确认下发温度补偿", confirm_text):
            self.log("用户取消温度补偿下发")
            return

        self._set_temperature_compensation_apply_state(True, "温度补偿下发：执行中")
        threading.Thread(
            target=self._run_temperature_compensation_apply,
            args=(cfg, plan),
            daemon=True,
        ).start()

    def _open_modeling_config(self) -> None:
        path = self._modeling_config_path()
        if not path.exists():
            messagebox.showinfo("提示", "当前没有可打开的离线建模配置文件。")
            return
        self._open_path(path)

    def _open_modeling_result(self) -> None:
        if self.current_modeling_result_path is None or not self.current_modeling_result_path.exists():
            messagebox.showinfo("提示", "当前没有可打开的离线建模结果。")
            return
        self._open_path(self.current_modeling_result_path)

    def _run_offline_modeling_analysis(self) -> None:
        try:
            loaded = load_modeling_config(
                base_config_path=Path(self.config_path.get()).resolve(),
                modeling_config_path=self._modeling_config_path(),
            )
        except Exception as exc:
            messagebox.showerror("离线建模配置错误", f"读取离线建模配置失败：{exc}")
            return

        modeling_cfg = loaded.get("modeling", {})
        if not modeling_cfg.get("enabled"):
            messagebox.showinfo(
                "离线建模未启用",
                "离线建模分析当前默认关闭。\n\n请先在 configs/modeling_offline.json 或当前配置文件的 modeling 分组中启用后，再手动运行。\n\n本功能默认不参与在线自动校准流程。",
            )
            return

        script_path = Path(__file__).resolve().parents[3] / "run_modeling_analysis.py"
        if not script_path.exists():
            messagebox.showerror("启动失败", f"未找到离线建模入口：{script_path}")
            return

        command = [
            sys.executable,
            str(script_path),
            "--base-config",
            str(Path(self.config_path.get()).resolve()),
            "--modeling-config",
            str(self._modeling_config_path()),
        ]
        try:
            subprocess.Popen(
                command,
                cwd=str(script_path.parent),
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except Exception as exc:
            messagebox.showerror("启动失败", f"无法启动离线建模分析：{exc}")
            return
        self.log("已启动离线建模分析。当前不会影响自动校准在线流程。")


    def _open_selected_history_item(self, _event=None) -> None:
        selection = self.history_list.curselection()
        if not selection:
            return
        item = self.history_list.get(selection[0])
        path = self.history_item_paths.get(item)
        if path is None or not path.exists():
            messagebox.showinfo("提示", "该历史项当前没有对应的数据文件。")
            return
        self._open_path(path)

    def _open_selected_history_parent(self) -> None:
        selection = self.history_list.curselection()
        if not selection:
            return
        item = self.history_list.get(selection[0])
        path = self.history_item_paths.get(item)
        if path is None or not path.exists():
            messagebox.showinfo("提示", "该历史项当前没有对应的数据目录。")
            return
        self._open_path(path.parent)

    def _copy_selected_history_item(self) -> None:
        selection = self.history_list.curselection()
        if not selection:
            self.log("当前没有可复制的历史项")
            return
        item = self.history_list.get(selection[0]).strip()
        self._copy_text("历史项", item, "当前没有可复制的历史项")

    def _copy_selected_history_path(self) -> None:
        selection = self.history_list.curselection()
        if not selection:
            self.log("当前没有可复制的历史文件路径")
            return
        item = self.history_list.get(selection[0]).strip()
        path = self.history_item_paths.get(item)
        if path is None:
            self.log("该历史项当前没有对应的数据文件路径")
            return
        self._copy_text("历史文件路径", str(path), "当前没有可复制的历史文件路径")

    def _show_history_context_menu(self, event) -> None:
        if self.history_list.size() == 0:
            return
        try:
            index = self.history_list.nearest(event.y)
            if index < 0:
                return
            self.history_list.selection_clear(0, "end")
            self.history_list.selection_set(index)
            self.history_list.activate(index)
            self.history_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.history_menu.grab_release()

    def _refresh_key_events(self) -> None:
        run_dir = self.current_run_dir
        if run_dir is None:
            if self.current_io_path is not None:
                run_dir = Path(self.current_io_path).parent
            elif self.logger is not None:
                run_dir = Path(self.logger.run_dir)
        if run_dir is None or not run_dir.exists():
            return
        stdout_candidates = sorted(run_dir.glob("*_stdout.log"))
        stdout_path = max(stdout_candidates, key=lambda path: path.stat().st_mtime) if stdout_candidates else None
        io_path = self._latest_io_path(run_dir)
        source_key = (
            str(run_dir.resolve()),
            self._path_cache_signature(stdout_path),
            self._path_cache_signature(io_path),
        )
        if source_key == self._key_events_source_cache_key:
            events = list(self._key_events_source_cache)
        else:
            try:
                events = []
                if stdout_path is not None and stdout_path.exists():
                    lines = self._tail_text_lines(stdout_path, count=3000)
                    events.extend(self._extract_key_events(lines))
                if io_path is not None and io_path.exists():
                    rows = self._tail_csv_rows(io_path, count=2500)
                    runtime_cfg = self._load_runtime_config_snapshot(run_dir)
                    events.extend(self._extract_key_events_from_io(rows, runtime_cfg=runtime_cfg))
                deduped: List[str] = []
                seen: set[str] = set()
                for item in events:
                    text = item.strip()
                    if not text or text in seen:
                        continue
                    seen.add(text)
                    deduped.append(text)
                events = deduped[-12:]
                self._key_events_source_cache_key = source_key
                self._key_events_source_cache = list(events)
            except Exception:
                return
        selected_filter = self.event_filter_var.get().strip() if hasattr(self, "event_filter_var") else "全部"
        filtered_events = [event for event in events if self._event_matches_filter(self._classify_event_level(event), selected_filter)]
        render_key = (selected_filter, tuple(filtered_events))
        self.current_events_cache = list(filtered_events)
        if render_key == self._key_events_render_cache_key:
            return
        self._key_events_render_cache_key = render_key
        self.event_text.configure(state="normal")
        self.event_text.delete("1.0", "end")
        events = filtered_events
        if events:
            for idx, event in enumerate(events):
                level = self._classify_event_level(event)
                self.event_text.insert("end", "● ", ("timeline",))
                self.event_text.insert("end", event, (f"event_{level}",))
                if idx < len(events) - 1:
                    self.event_text.insert("end", "\n│\n", ("timeline",))
        else:
            self.event_text.insert("end", "暂无关键事件")
        self.event_text.configure(state="disabled")

    def _get_selected_event_text(self) -> str:
        try:
            return self.event_text.selection_get().strip()
        except tk.TclError:
            line = self.event_text.get("insert linestart", "insert lineend").strip()
            if line and line != "暂无关键事件":
                return line
        return ""

    def _copy_selected_event(self) -> None:
        selected = self._get_selected_event_text()
        if not selected and self.current_events_cache:
            selected = self.current_events_cache[-1]
        self._copy_text("关键事件", selected, "当前没有可复制的关键事件")

    def _copy_event_from_double_click(self, event=None) -> None:
        if event is not None:
            try:
                index = self.event_text.index(f"@{event.x},{event.y}")
                line_text = self.event_text.get(f"{index} linestart", f"{index} lineend").strip()
                if line_text and line_text not in {"●", "│", "暂无关键事件"}:
                    self._copy_text("关键事件", line_text, "当前没有可复制的关键事件")
                    return
            except Exception:
                pass
        self._copy_selected_event()

    def _export_event_list(self) -> None:
        if not self.current_events_cache:
            self.log("当前没有可导出的关键事件")
            return
        initial_dir = str(self.current_run_dir) if self.current_run_dir is not None else str(Path.cwd())
        initial_file = "event_list.txt"
        if self.current_run_dir is not None:
            initial_file = f"{self.current_run_dir.name}_events.txt"
        target = filedialog.asksaveasfilename(
            title="导出事件列表",
            defaultextension=".txt",
            filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")],
            initialdir=initial_dir,
            initialfile=initial_file,
        )
        if not target:
            return
        Path(target).write_text("\n".join(self.current_events_cache), encoding="utf-8")
        self.log(f"关键事件已导出：{target}")

    def _event_to_point_path(self, event_text: str) -> Path | None:
        if self.current_run_dir is None or not self.current_run_dir.exists():
            return None
        mapping = self._build_history_item_paths(self.current_run_dir, [event_text])
        path = mapping.get(event_text)
        if path is not None and path.exists():
            return path
        return None

    def _open_selected_event_point_file(self) -> None:
        event_text = self._get_selected_event_text()
        if not event_text and self.current_events_cache:
            event_text = self.current_events_cache[-1]
        path = self._event_to_point_path(event_text)
        if path is None:
            messagebox.showinfo("提示", "当前事件没有可打开的对应点文件。")
            return
        self._open_path(path)

    def _show_event_context_menu(self, event) -> None:
        try:
            index = self.event_text.index(f"@{event.x},{event.y}")
            self.event_text.tag_remove("sel", "1.0", "end")
            self.event_text.tag_add("sel", f"{index} linestart", f"{index} lineend")
            self.event_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.event_menu.grab_release()

    def _get_points_lookup(self) -> Dict[int, Any]:
        cfg = getattr(self, "cfg", None)
        if not isinstance(cfg, dict):
            return {}
        workflow_cfg = cfg.get("workflow", {})
        paths_cfg = cfg.get("paths", {})
        base_dir = Path(cfg.get("_base_dir", "."))
        points_raw = paths_cfg.get("points_excel")
        cache_key = (
            str((base_dir / str(points_raw)).resolve()) if points_raw else "",
            workflow_cfg.get("missing_pressure_policy", "require"),
            bool(workflow_cfg.get("carry_forward_h2o", False)),
        )
        if getattr(self, "_points_lookup_cache_key", None) == cache_key:
            return getattr(self, "_points_lookup_cache", {})
        lookup: Dict[int, Any] = {}
        candidates: List[Path] = []
        for raw in (points_raw, base_dir / "points.xlsx"):
            if not raw:
                continue
            try:
                candidate = Path(raw)
                if not candidate.is_absolute():
                    candidate = (base_dir / candidate).resolve()
                else:
                    candidate = candidate.resolve()
            except Exception:
                continue
            if candidate.exists() and candidate not in candidates:
                candidates.append(candidate)
        for candidate in candidates:
            try:
                points = load_points_from_excel(
                    candidate,
                    missing_pressure_policy=workflow_cfg.get("missing_pressure_policy", "require"),
                    carry_forward_h2o=bool(workflow_cfg.get("carry_forward_h2o", False)),
                )
            except Exception:
                continue
            lookup = {
                int(point.index): point
                for point in points
                if getattr(point, "index", None) is not None
            }
            if lookup:
                break
        self._points_lookup_cache_key = cache_key
        self._points_lookup_cache = lookup
        return lookup

    @staticmethod
    def _format_point_temp(value: Any) -> str:
        return f"{float(value):g}°C"

    @staticmethod
    def _format_point_rh(value: Any) -> str:
        return f"{float(value):g}%RH"

    @staticmethod
    def _format_point_pressure(value: Any) -> str:
        return f"{int(round(float(value)))}hPa"

    def _describe_row_target(self, route: str, row_id: int) -> Tuple[str, str, str]:
        point = self._get_points_lookup().get(int(row_id))
        if point is None:
            return (
                f"当前点位：{route.upper()} row {row_id}",
                "当前标气：--",
                "当前压力点：--",
            )

        chamber_text = self._format_point_temp(getattr(point, "temp_chamber_c", 0.0))
        pressure_value = getattr(point, "target_pressure_hpa", None)
        pressure_text = (
            f"当前压力点：{self._format_point_pressure(pressure_value)}"
            if pressure_value is not None
            else "当前压力点：--"
        )
        if route == "h2o":
            hgen_temp = getattr(point, "hgen_temp_c", None)
            hgen_rh = getattr(point, "hgen_rh_pct", None)
            hgen_parts: List[str] = []
            if hgen_temp is not None:
                hgen_parts.append(self._format_point_temp(hgen_temp))
            if hgen_rh is not None:
                hgen_parts.append(self._format_point_rh(hgen_rh))
            hgen_text = " / ".join(hgen_parts) if hgen_parts else "--"
            point_text = f"当前点位：水路 温箱{chamber_text} / 湿发{hgen_text}"
            if pressure_value is not None:
                point_text += f" / {self._format_point_pressure(pressure_value)}"
            return point_text, f"湿发设定：{hgen_text}", pressure_text

        co2_ppm = getattr(point, "co2_ppm", None)
        ppm_text = f"{int(round(float(co2_ppm)))}ppm" if co2_ppm is not None else "--"
        return (
            f"当前点位：气路 温箱{chamber_text} / {ppm_text}",
            f"当前标气：{ppm_text}",
            "当前压力点：--",
        )

    def _update_stage_from_status(self, msg: str) -> None:
        text = (msg or "").strip()
        stage = "当前阶段：运行中"
        target = "当前点位：--"
        target_ppm_text = "当前标气：--"
        pressure_text = "当前压力点：--"

        if not text or text == "空闲":
            stage = "当前阶段：空闲"
        elif "连接检查" in text:
            stage = "当前阶段：连接检查"
        elif "自检" in text:
            stage = "当前阶段：设备自检"
        elif text.startswith("CO2 row "):
            stage = "当前阶段：气路流程"
            row_match = re.match(r"CO2 row\s+(\d+)", text)
            if row_match:
                target, target_ppm_text, pressure_text = self._describe_row_target("co2", int(row_match.group(1)))
            else:
                target = f"当前点位：{text}"
        elif text.startswith("H2O row "):
            ambient_match = re.match(r"H2O row\s+(\d+)\s+当前大气压", text)
            if ambient_match:
                stage = "当前阶段：水路开路采样"
                target, target_ppm_text, _ = self._describe_row_target("h2o", int(ambient_match.group(1)))
                target = f"{target} / 当前大气压"
                pressure_text = "当前压力点：当前大气压"
                row_match = None
            else:
                stage = "当前阶段：水路流程"
                row_match = re.match(r"H2O row\s+(\d+)", text)
            if row_match:
                target, target_ppm_text, pressure_text = self._describe_row_target("h2o", int(row_match.group(1)))
            else:
                if not ambient_match:
                    target = f"当前点位：{text}"
        else:
            m = re.match(r"CO2\s+(\d+)ppm\s+(\d+)hPa", text)
            if m:
                stage = "当前阶段：气路控压/采样"
                target = f"当前点位：CO2 {m.group(1)}ppm / {m.group(2)}hPa"
                target_ppm_text = f"当前标气：{m.group(1)}ppm"
                pressure_text = f"当前压力点：{m.group(2)}hPa"
            else:
                m = re.match(r"CO2\s+(\d+)ppm\s+当前大气压", text)
                if m:
                    stage = "当前阶段：气路开路采样"
                    target = f"当前点位：CO2 {m.group(1)}ppm / 当前大气压"
                    target_ppm_text = f"当前标气：{m.group(1)}ppm"
                    pressure_text = "当前压力点：当前大气压"
                else:
                    m = re.match(r"H2O row\s+(\d+)", text)
                    if m:
                        stage = "当前阶段：水路控压/采样"
                        target, target_ppm_text, pressure_text = self._describe_row_target("h2o", int(m.group(1)))

        self.stage_var.set(stage)
        self.target_var.set(target)
        self.current_target_ppm_var.set(target_ppm_text)
        self.current_pressure_point_var.set(pressure_text)

    def _build_runtime_cfg(self) -> Dict[str, Any]:
        # Always rebuild from the latest on-disk config + user_tuning overlay.
        # This avoids stale in-memory settings when the user keeps a window open
        # while we adjust fast-test or formal-test tuning flags.
        cfg = self._load_runtime_base_config()
        self.cfg = copy.deepcopy(cfg)
        workflow = cfg.setdefault("workflow", {})
        coefficients = cfg.setdefault("coefficients", {})
        route_mode_map = {
            "先水后气": "h2o_then_co2",
            "只测水路": "h2o_only",
            "只测气路": "co2_only",
        }
        workflow["route_mode"] = route_mode_map.get(self.route_mode_var.get(), "h2o_then_co2")
        workflow["temperature_descending"] = self._temperature_order_descending()
        fit_enabled = bool(self.fit_enabled_var.get())
        workflow["collect_only"] = not fit_enabled
        if fit_enabled:
            coefficients["enabled"] = True
            coefficients["auto_fit"] = True
            coefficients["fit_h2o"] = True
        workflow.pop("postrun_corrected_delivery", None)

        scope = self.temp_scope_var.get().strip()
        if scope == "指定温度点":
            temps = [float(temp) for temp in self._selected_temp_values()]
            if not temps:
                raise ValueError("指定温度点时，至少勾选一个温度点。")
            workflow["selected_temps_c"] = temps
        else:
            workflow.pop("selected_temps_c", None)

        if self.co2_check_vars:
            selected_ppm = {int(ppm) for ppm in self._selected_co2_values()}
            all_ppm = {int(ppm) for ppm in self.co2_check_vars.keys()}
            workflow["skip_co2_ppm"] = sorted(all_ppm - selected_ppm)
        if self.pressure_check_vars:
            selected_pressures = self._selected_pressure_values()
            selected_pressure_tokens = self._selected_pressure_tokens()
            if not selected_pressure_tokens:
                raise ValueError("请至少勾选一个压力点。")
            all_pressures = sorted((int(value) for value in self.pressure_check_vars.keys()), reverse=True)
            if selected_pressures == all_pressures and not self._ambient_pressure_selected():
                workflow.pop("selected_pressure_points", None)
            else:
                workflow["selected_pressure_points"] = selected_pressure_tokens
        return cfg

    def _refresh_valve_hint(self) -> None:
        valves = self.cfg.get("valves", {}) if hasattr(self, "cfg") else {}
        map_a = valves.get("co2_map", {})
        map_b = valves.get("co2_map_group2", {})

        def _ppm_keys(one_map) -> str:
            if not isinstance(one_map, dict):
                return "--"
            keys = []
            for key in one_map.keys():
                try:
                    keys.append(int(key))
                except Exception:
                    continue
            if not keys:
                return "--"
            return "/".join(str(k) for k in sorted(keys))

        text_a = _ppm_keys(map_a)
        text_b = _ppm_keys(map_b)
        self.valve_hint_var.set(
            f"二氧化碳气路1：{text_a} ppm | 气路2：{text_b} ppm | 默认零气：气路1"
        )

    def _set_card_style(self, widget: tk.Label, level: str) -> None:
        palette = self.state_palette.get(level, self.state_palette["idle"])
        widget.configure(bg=palette["bg"], fg=palette["fg"])
        try:
            widget.configure(highlightbackground=palette["bg"], highlightcolor=palette["bg"])
        except Exception:
            pass

    def _controls_locked(self) -> bool:
        return bool(
            getattr(self, "safe_stop_in_progress", False)
            or (self.startup_thread and self.startup_thread.is_alive())
            or (self.worker and self.worker.is_alive())
        )

    def _apply_control_lock(self) -> None:
        locked = self._controls_locked()
        combo_state = "disabled" if locked else "readonly"
        input_state = "disabled" if locked else "normal"
        button_state = "disabled" if locked else "normal"

        self.config_entry.configure(state=input_state)
        self.load_button.configure(state=button_state)
        self.route_mode_combo.configure(state=combo_state)
        self.temp_scope_combo.configure(state=combo_state)
        self.temperature_order_combo.configure(state=combo_state)
        self.fit_enabled_check.configure(state=button_state)
        self.postrun_delivery_check.configure(state="disabled")
        self.temp_select_all_button.configure(state=button_state)
        self.temp_clear_button.configure(state=button_state)
        self.co2_select_all_button.configure(state=button_state)
        self.co2_clear_button.configure(state=button_state)
        self.pressure_select_all_button.configure(state=button_state)
        self.pressure_clear_button.configure(state=button_state)
        if self.ambient_pressure_check is not None:
            self.ambient_pressure_check.configure(state=button_state)
        if self.temp_listbox is not None:
            try:
                self.temp_listbox.configure(state="disabled" if locked else "normal")
            except Exception:
                pass
        if self.co2_listbox is not None:
            try:
                self.co2_listbox.configure(state="disabled" if locked else "normal")
            except Exception:
                pass
        if self.pressure_listbox is not None:
            try:
                self.pressure_listbox.configure(state="disabled" if locked else "normal")
            except Exception:
                pass
        for btn in self.co2_checkbuttons:
            try:
                btn.configure(state="disabled" if locked else "normal")
            except Exception:
                pass
        self._refresh_selector_button_styles()
        ready, _text, _level = self._compute_start_readiness()
        self.start_button.configure(state="disabled" if locked or not ready else "normal")
        self.self_test_button.configure(state="disabled" if locked else "normal")
        self.pause_button.configure(state="normal" if self.worker and self.worker.is_alive() else "disabled")
        self.resume_button.configure(state="normal" if self.worker and self.worker.is_alive() else "disabled")
        self.stop_button.configure(state="normal" if self.worker and self.worker.is_alive() else "disabled")
        if hasattr(self, "device_port_save_button"):
            self.device_port_save_button.configure(state=button_state)
        if hasattr(self, "device_port_reload_button"):
            self.device_port_reload_button.configure(state=button_state)
        if hasattr(self, "device_port_default_button"):
            self.device_port_default_button.configure(state=button_state)
        child_page_state = "disabled" if locked else "normal"
        self.open_humidity_button.configure(state=child_page_state)
        self.open_dewpoint_button.configure(state=child_page_state)
        self.open_thermometer_button.configure(state=child_page_state)
        self.open_valve_button.configure(state=child_page_state)

        self._on_temp_scope_change()

    def _copy_text(self, label: str, text: str, empty_hint: str) -> None:
        payload = (text or "").strip()
        if not payload or payload.endswith("：--") or payload.endswith(": --") or payload.endswith("--"):
            self.log(empty_hint)
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(payload)
        self.log(f"{label}已复制")

    def _copy_current_point(self) -> None:
        parts = [
            self.target_var.get(),
            self.current_target_ppm_var.get(),
            self.current_pressure_point_var.get(),
            self.current_route_group_detail_var.get(),
        ]
        text = " | ".join(part for part in parts if part and not part.endswith("：--"))
        self._copy_text("当前点位", text, "当前没有可复制的点位信息")

    def _copy_last_issue(self) -> None:
        self._copy_text("最近异常", self.last_issue_var.get(), "当前没有可复制的异常信息")

    def _show_run_monitor(self) -> None:
        if hasattr(self, "right_tabs") and hasattr(self, "status_tab"):
            self.right_tabs.select(self.status_tab)

    def _show_device_overview(self) -> None:
        if hasattr(self, "bottom_tabs") and hasattr(self, "device_shell"):
            self.bottom_tabs.select(self.device_shell)

    def _show_points_preview(self) -> None:
        if hasattr(self, "selector_tabs") and hasattr(self, "points_tab"):
            self.selector_tabs.select(self.points_tab)

    def _show_workbench_view(self) -> None:
        if hasattr(self, "bottom_tabs") and hasattr(self, "workbench_shell"):
            self.bottom_tabs.select(self.workbench_shell)

    def _apply_banner_states(self) -> None:
        status_text = self.status_var.get()
        issue_text = self.last_issue_var.get()
        route_text = self.current_route_group_detail_var.get()

        stage_level = "info"
        status_level = "idle"
        issue_level = "idle"
        route_level = "idle"
        if any(token in status_text for token in ("ERROR", "失败", "异常")):
            stage_level = "error"
            status_level = "error"
        elif any(token in status_text for token in ("空闲", "停止")):
            stage_level = "idle"
            status_level = "idle"
        elif any(token in status_text for token in ("运行", "校准", "自检", "连接", "恢复基线")):
            stage_level = "info"
            status_level = "ok"

        if issue_text.endswith("--"):
            issue_level = "idle"
        elif any(token in issue_text for token in ("FAIL", "INVALID", "timeout", "skipped", "失败", "ERROR")):
            issue_level = "error"
        else:
            issue_level = "warn"

        if "第一组" in route_text or "第二组" in route_text:
            route_level = "ok"

        widgets = (
            (self.stage_banner, stage_level),
            (self.point_banner, "info"),
            (self.route_banner, route_level),
            (self.gas_point_banner, "info"),
            (self.pressure_point_banner, "info"),
            (self.pressure_live_label, "info"),
            (self.pressure_stability_label, "ok" if self.current_pressure_stability_var.get().endswith("1") else "idle"),
            (self.pressure_reapply_label, "warn" if "--" not in self.current_pressure_reapply_var.get() and not self.current_pressure_reapply_var.get().endswith("：0") else "idle"),
            (self.status_banner, status_level),
            (self.issue_banner, issue_level),
        )
        for widget, level in widgets:
            self._set_card_style(widget, level)
        self._set_status_indicator(stage_level)

    def _set_status_indicator(self, level: str) -> None:
        palette = self.state_palette.get(level, self.state_palette["idle"])
        if hasattr(self, "stage_light"):
            self.stage_light.itemconfigure(self.stage_light_item, fill=palette["fg"])
        icon_map = {
            "idle": "○",
            "ok": "✓",
            "warn": "!",
            "error": "×",
            "info": "▶",
        }
        self.stage_icon_var.set(icon_map.get(level, "●"))
        if hasattr(self, "stage_icon_label"):
            self.stage_icon_label.configure(fg=palette["fg"], bg=self.ui_colors["value_panel"])

    @staticmethod
    def _device_state_from_text(key: str, text: str) -> Tuple[str, str]:
        if not text or text.endswith("：--"):
            return "状态：未读取", "idle"
        if key == "pace":
            if "稳定标志=1" in text:
                return "状态：稳定", "ok"
            if "稳定标志=0" in text:
                return "状态：未稳定", "warn"
        if key == "chamber":
            m = re.search(r"温度=(-?\d+(?:\.\d+)?)°C", text)
            if m:
                temp = float(m.group(1))
                if abs(temp - 20.0) <= 0.2:
                    return "状态：温度到位", "ok"
                return "状态：温度偏离", "warn"
        if key == "hgen":
            if "流量=0.0" in text:
                return "状态：已停机", "idle"
            return "状态：运行中", "ok"
        return "状态：在线", "ok"

    @staticmethod
    def _close_devices(devices) -> None:
        seen = set()
        for dev in devices.values():
            if isinstance(dev, dict):
                candidates = list(dev.values())
            elif isinstance(dev, (list, tuple, set)):
                candidates = list(dev)
            else:
                candidates = [dev]

            for item in candidates:
                if not hasattr(item, "close"):
                    continue
                obj_id = id(item)
                if obj_id in seen:
                    continue
                seen.add(obj_id)
                try:
                    item.close()
                except Exception:
                    pass

    @staticmethod
    def _enabled_failures(cfg: Dict[str, Any], results: Dict[str, Any]) -> List[Tuple[str, str]]:
        failures: List[Tuple[str, str]] = []
        dcfg = cfg.get("devices", {})
        for name, result in results.items():
            if name == "gas_analyzer":
                if isinstance(result, dict) and isinstance(result.get("items"), list):
                    for item in result.get("items", []):
                        if not isinstance(item, dict) or item.get("ok"):
                            continue
                        item_name = str(item.get("name") or "gas_analyzer").strip() or "gas_analyzer"
                        item_err = str(item.get("err", "UNKNOWN"))
                        failures.append((item_name, item_err))
                    continue
                single_enabled = bool(dcfg.get("gas_analyzer", {}).get("enabled", False))
                multi_cfg = dcfg.get("gas_analyzers", [])
                multi_enabled = any(
                    isinstance(item, dict) and item.get("enabled", True) for item in multi_cfg
                ) if isinstance(multi_cfg, list) else False
                enabled = single_enabled or multi_enabled
            else:
                enabled = bool(dcfg.get(name, {}).get("enabled", False))

            if not enabled:
                continue
            if isinstance(result, dict) and result.get("ok"):
                continue
            err = result.get("err", "UNKNOWN") if isinstance(result, dict) else "UNKNOWN"
            failures.append((name, str(err)))
        return failures

    @staticmethod
    def _is_gas_analyzer_failure(name: str) -> bool:
        text = str(name or "").strip().lower()
        return bool(re.fullmatch(r"(ga\d+|gas_analyzer(?:_\d+)?)", text))

    @staticmethod
    def _is_optional_startup_failure(name: str) -> bool:
        text = str(name or "").strip().lower()
        return text == "thermometer" or App._is_gas_analyzer_failure(text)

    def _remaining_enabled_gas_analyzers_after_skip(self, failure_names: List[str]) -> int:
        failed = {str(name or "").strip().lower() for name in failure_names}
        devices = self.cfg.get("devices", {}) if isinstance(getattr(self, "cfg", None), dict) else {}
        gas_list = devices.get("gas_analyzers", [])
        if isinstance(gas_list, list) and gas_list:
            remaining = 0
            for idx, item in enumerate(gas_list, start=1):
                if not isinstance(item, dict) or not item.get("enabled", True):
                    continue
                label = str(item.get("name") or f"ga{idx:02d}").strip().lower()
                if label in failed or f"gas_analyzer_{idx:02d}" in failed:
                    continue
                remaining += 1
            return remaining
        single_cfg = devices.get("gas_analyzer", {})
        if isinstance(single_cfg, dict) and single_cfg.get("enabled", False):
            return 0 if ("gas_analyzer" in failed or "ga01" in failed) else 1
        return 0

    def _disable_failed_gas_analyzers_in_cfg(self, failure_names: List[str]) -> List[str]:
        disabled: List[str] = []
        failed = {str(name or "").strip().lower() for name in failure_names}
        devices = self.cfg.get("devices", {}) if isinstance(getattr(self, "cfg", None), dict) else {}
        gas_list = devices.get("gas_analyzers", [])
        if isinstance(gas_list, list) and gas_list:
            for idx, item in enumerate(gas_list, start=1):
                if not isinstance(item, dict) or not item.get("enabled", True):
                    continue
                label = str(item.get("name") or f"ga{idx:02d}").strip()
                lowered = label.lower()
                if lowered in failed or f"gas_analyzer_{idx:02d}" in failed:
                    item["enabled"] = False
                    disabled.append(label)
            single_cfg = devices.get("gas_analyzer", {})
            if isinstance(single_cfg, dict):
                for idx, item in enumerate(gas_list, start=1):
                    if not isinstance(item, dict) or not item.get("enabled", True):
                        continue
                    single_cfg["enabled"] = True
                    single_cfg["port"] = item.get("port", single_cfg.get("port"))
                    single_cfg["baud"] = item.get("baud", single_cfg.get("baud", 115200))
                    single_cfg["device_id"] = item.get("device_id", single_cfg.get("device_id", "000"))
                    break
                else:
                    single_cfg["enabled"] = False
            return disabled

        single_cfg = devices.get("gas_analyzer", {})
        if isinstance(single_cfg, dict) and single_cfg.get("enabled", False):
            if "gas_analyzer" in failed or "ga01" in failed:
                single_cfg["enabled"] = False
                disabled.append("ga01")
        return disabled

    def _disable_failed_optional_devices_in_cfg(self, failure_names: List[str]) -> List[str]:
        disabled = self._disable_failed_gas_analyzers_in_cfg(failure_names)
        failed = {str(name or "").strip().lower() for name in failure_names}
        devices = self.cfg.get("devices", {}) if isinstance(getattr(self, "cfg", None), dict) else {}
        thermometer_cfg = devices.get("thermometer", {})
        if isinstance(thermometer_cfg, dict) and thermometer_cfg.get("enabled", False):
            if "thermometer" in failed:
                thermometer_cfg["enabled"] = False
                disabled.append("thermometer")
        return disabled

    def _startup_connectivity_check(self, io_logger: RunLogger) -> bool:
        scfg = self.cfg.get("workflow", {}).get("startup_connect_check", {})
        if not scfg or not scfg.get("enabled", False):
            return True

        attempt = 1
        retry_targets = None
        while True:
            self.set_status("连接检查中...")
            target_text = "全部启用设备" if not retry_targets else ",".join(sorted(retry_targets))
            self.log(f"连接检查第 {attempt} 次，目标设备：{target_text}")
            results = run_self_test(
                self.cfg,
                log_fn=self.log,
                io_logger=io_logger,
                only_devices=retry_targets,
            )
            failures = self._enabled_failures(self.cfg, results)
            if not failures:
                self.log("连接检查通过")
                return True

            lines = ["失败设备："]
            lines.extend(f"- {name}: {err}" for name, err in failures)
            detail = "\n".join(lines)
            self.log(detail.replace("\n", " | "))

            analyzer_failures = [(name, err) for name, err in failures if self._is_gas_analyzer_failure(name)]
            optional_failures = [(name, err) for name, err in failures if self._is_optional_startup_failure(name)]
            blocking_failures = [(name, err) for name, err in failures if not self._is_optional_startup_failure(name)]
            thermometer_failed = any(str(name or "").strip().lower() == "thermometer" for name, _ in optional_failures)
            if analyzer_failures and not blocking_failures and len(optional_failures) == len(failures):
                failed_names = [name for name, _ in optional_failures]
                remaining = self._remaining_enabled_gas_analyzers_after_skip(failed_names)
                if remaining > 0:
                    prompt = (
                        detail
                        + f"\n\n当前仍有 {remaining} 台分析仪可继续运行。"
                        + ("\n温度计也将被跳过，本轮温度参考会回退到温箱。" if thermometer_failed else "")
                        + "\n点击“是”重试，点击“否”跳过这些异常设备并继续，点击“取消”放弃启动。"
                    )
                else:
                    prompt = (
                        detail
                        + "\n\n当前没有可用分析仪。"
                        + "\n继续运行将不会采集分析仪数据，但水路/气路流程仍可继续。"
                        + ("\n温度计也将被跳过，本轮温度参考会回退到温箱。" if thermometer_failed else "")
                        + "\n点击“是”重试，点击“否”继续执行，点击“取消”放弃启动。"
                    )
                decision = self._call_on_ui_thread(
                    messagebox.askyesnocancel,
                    "气体分析仪连接检查失败",
                    prompt,
                    parent=self.root,
                )
                if decision is False:
                    disabled = self._disable_failed_optional_devices_in_cfg(failed_names)
                    disabled_text = "、".join(disabled) if disabled else "异常分析仪"
                    if remaining > 0:
                        self.log(f"用户选择跳过并继续：{disabled_text}")
                        self.set_status("连接检查通过（已跳过异常设备）")
                    else:
                        self.log(f"用户选择无分析仪数据继续运行：{disabled_text}")
                        self.set_status("连接检查通过（无分析仪数据）")
                    return True
                if decision is None:
                    self.log("用户取消启动")
                    self.set_status("空闲")
                    return False

            retry = self._call_on_ui_thread(
                messagebox.askretrycancel,
                "设备连接检查失败",
                detail + "\n\n修复连接后点击“重试”，或点击“取消”放弃启动。",
                parent=self.root,
            )
            if not retry:
                self.log("用户取消启动")
                self.set_status("空闲")
                return False

            retry_targets = [name for name, _ in failures]
            attempt += 1

    def _build_devices(self, io_logger=None) -> None:
        dcfg = self.cfg["devices"]
        built = {}

        try:
            if dcfg["pressure_controller"]["enabled"]:
                built["pace"] = Pace5000(
                    dcfg["pressure_controller"]["port"],
                    dcfg["pressure_controller"]["baud"],
                    timeout=float(dcfg["pressure_controller"].get("timeout", 1.0)),
                    line_ending=dcfg["pressure_controller"].get("line_ending"),
                    query_line_endings=dcfg["pressure_controller"].get("query_line_endings"),
                    pressure_queries=dcfg["pressure_controller"].get("pressure_queries"),
                    io_logger=io_logger,
                )
                built["pace"].open()

            if dcfg["pressure_gauge"]["enabled"]:
                built["pressure_gauge"] = ParoscientificGauge(
                    dcfg["pressure_gauge"]["port"],
                    dcfg["pressure_gauge"]["baud"],
                    timeout=float(dcfg["pressure_gauge"].get("timeout", 1.0)),
                    dest_id=dcfg["pressure_gauge"]["dest_id"],
                    response_timeout_s=dcfg["pressure_gauge"].get("response_timeout_s"),
                    io_logger=io_logger,
                )
                built["pressure_gauge"].open()

            if dcfg["dewpoint_meter"]["enabled"]:
                built["dewpoint"] = DewpointMeter(
                    dcfg["dewpoint_meter"]["port"],
                    dcfg["dewpoint_meter"]["baud"],
                    station=dcfg["dewpoint_meter"]["station"],
                    io_logger=io_logger,
                )
                built["dewpoint"].open()

            if dcfg["humidity_generator"]["enabled"]:
                built["humidity_gen"] = HumidityGenerator(
                    dcfg["humidity_generator"]["port"],
                    dcfg["humidity_generator"]["baud"],
                    io_logger=io_logger,
                )
                built["humidity_gen"].open()

            built_primary_ga = False
            gas_list_cfg = dcfg.get("gas_analyzers", [])
            if isinstance(gas_list_cfg, list) and gas_list_cfg:
                for idx, gcfg in enumerate(gas_list_cfg, start=1):
                    if not isinstance(gcfg, dict) or not gcfg.get("enabled", True):
                        continue
                    key = f"gas_analyzer_{idx:02d}"
                    dev = GasAnalyzer(
                        gcfg["port"],
                        gcfg.get("baud", 115200),
                        device_id=gcfg.get("device_id", f"{idx:03d}"),
                        io_logger=io_logger,
                    )
                    dev.open()
                    built[key] = dev
                    if not built_primary_ga:
                        built["gas_analyzer"] = dev
                        built_primary_ga = True
            elif dcfg["gas_analyzer"]["enabled"]:
                built["gas_analyzer"] = GasAnalyzer(
                    dcfg["gas_analyzer"]["port"],
                    dcfg["gas_analyzer"]["baud"],
                    device_id=dcfg["gas_analyzer"]["device_id"],
                    io_logger=io_logger,
                )
                built["gas_analyzer"].open()

            if dcfg["temperature_chamber"]["enabled"]:
                built["temp_chamber"] = TemperatureChamber(
                    dcfg["temperature_chamber"]["port"],
                    dcfg["temperature_chamber"]["baud"],
                    addr=dcfg["temperature_chamber"]["addr"],
                    io_logger=io_logger,
                )
                built["temp_chamber"].open()

            if dcfg["thermometer"]["enabled"]:
                built["thermometer"] = Thermometer(
                    dcfg["thermometer"]["port"],
                    dcfg["thermometer"]["baud"],
                    timeout=dcfg["thermometer"].get("timeout", 1.2),
                    parity=dcfg["thermometer"].get("parity", "N"),
                    stopbits=dcfg["thermometer"].get("stopbits", 1),
                    bytesize=dcfg["thermometer"].get("bytesize", 8),
                    io_logger=io_logger,
                )
                built["thermometer"].open()

            if dcfg["relay"]["enabled"]:
                built["relay"] = RelayController(
                    dcfg["relay"]["port"],
                    dcfg["relay"]["baud"],
                    addr=dcfg["relay"]["addr"],
                    io_logger=io_logger,
                )
                built["relay"].open()

            relay8_cfg = dcfg.get("relay_8", {})
            if relay8_cfg.get("enabled"):
                built["relay_8"] = RelayController(
                    relay8_cfg["port"],
                    relay8_cfg["baud"],
                    addr=relay8_cfg["addr"],
                    io_logger=io_logger,
                )
                built["relay_8"].open()
        except Exception:
            self._close_devices(built)
            raise

        self.devices = built

    def start(self) -> None:
        if self.startup_thread and self.startup_thread.is_alive():
            self.log("流程正在启动中")
            return
        if self.worker and self.worker.is_alive():
            self.log("流程已在运行")
            return

        try:
            runtime_cfg = self._build_runtime_cfg()
        except Exception as exc:
            messagebox.showerror("参数错误", str(exc))
            return

        confirm_text = self._build_start_confirmation_text(runtime_cfg)
        if not messagebox.askokcancel("启动确认", confirm_text):
            self.log("用户取消启动前确认")
            return

        self.cfg = runtime_cfg
        self.last_runtime_cfg = copy.deepcopy(runtime_cfg)
        self._refresh_execution_summary()
        self.set_status("启动中...")

        self.startup_thread = threading.Thread(
            target=self._start_run_background,
            args=(copy.deepcopy(runtime_cfg),),
            daemon=False,
        )
        self.startup_thread.start()

    def _start_run_background(self, runtime_cfg: Dict[str, Any]) -> None:
        try:
            self.cfg = runtime_cfg
            self.last_runtime_cfg = copy.deepcopy(runtime_cfg)

            self.logger = RunLogger(Path(self.cfg["paths"]["output_dir"]), cfg=self.cfg)
            self.current_io_path = Path(self.logger.io_path)
            self.current_run_dir = Path(self.logger.run_dir)
            self._live_device_cache = {}
            self._live_device_cache_run_dir = self.current_run_dir
            self._write_runtime_config_snapshot(self.current_run_dir, runtime_cfg)
            self.log(f"启动日志目录：{self.logger.run_dir}")

            if not self._startup_connectivity_check(self.logger):
                try:
                    self.logger.close()
                except Exception:
                    pass
                self.logger = None
                self.current_io_path = None
                self.current_run_dir = None
                self._live_device_cache = {}
                self._live_device_cache_run_dir = None
                return

            self._build_devices(io_logger=self.logger)
        except Exception as exc:
            self.log(f"启动失败：{exc}")
            self.set_status("ERROR")
            try:
                if self.logger:
                    self.logger.close()
            except Exception:
                pass
            self.logger = None
            self.current_io_path = None
            self.current_run_dir = None
            self._live_device_cache = {}
            self._live_device_cache_run_dir = None
            return
        finally:
            self.startup_thread = None

        self.log(f"Run folder: {self.logger.run_dir}")
        self._log_app_event("EVENT", command="run-start", response=str(self.logger.run_dir))
        self.runner = CalibrationRunner(self.cfg, self.devices, self.logger, self.log, self.set_status)
        self.worker = threading.Thread(target=self.runner.run, daemon=False)
        self.worker.start()
        self.log("校准已启动")

    def open_humidity_page(self) -> None:
        if self.worker and self.worker.is_alive():
            self.log("流程运行中，无法打开湿度发生器页面")
            return
        if not hasattr(self, "cfg"):
            self.load_config()
        hcfg = self.cfg["devices"].get("humidity_generator", {})
        HumidityPage(self.root, hcfg, log_fn=self.log)

    def open_dewpoint_page(self) -> None:
        if self.worker and self.worker.is_alive():
            self.log("流程运行中，无法打开露点仪页面")
            return
        if not hasattr(self, "cfg"):
            self.load_config()
        dcfg = self.cfg["devices"].get("dewpoint_meter", {})
        DewpointPage(self.root, dcfg, log_fn=self.log)

    def open_thermometer_page(self) -> None:
        if self.worker and self.worker.is_alive():
            self.log("流程运行中，无法打开测温仪页面")
            return
        if not hasattr(self, "cfg"):
            self.load_config()
        tcfg = self.cfg["devices"].get("thermometer", {})
        ThermometerPage(self.root, tcfg, log_fn=self.log)

    def open_valve_page(self) -> None:
        if self.worker and self.worker.is_alive():
            self.log("流程运行中，无法打开阀门控制页面")
            return
        if not hasattr(self, "cfg"):
            self.load_config()
        ValvePage(self.root, self.cfg, log_fn=self.log)

    def self_test(self) -> None:
        if self.worker and self.worker.is_alive():
            self.log("流程运行中，禁止执行自检")
            return
        try:
            runtime_cfg = self._build_runtime_cfg()
        except Exception as exc:
            messagebox.showerror("参数错误", str(exc))
            return

        def _run() -> None:
            self.set_status("自检中...")
            tmp_logger = RunLogger(Path(runtime_cfg["paths"]["output_dir"]), cfg=runtime_cfg)
            self.current_io_path = Path(tmp_logger.io_path)
            self.current_run_dir = Path(tmp_logger.run_dir)
            self.log(f"自检日志目录：{tmp_logger.run_dir}")
            try:
                run_self_test(runtime_cfg, log_fn=self.log, io_logger=tmp_logger)
            finally:
                tmp_logger.close()
                self.current_io_path = None
                self.current_run_dir = None
            self.set_status("自检完成")

        threading.Thread(target=_run, daemon=True).start()

    def pause(self) -> None:
        if self.runner:
            self.runner.pause()
            self.log("已暂停")

    def resume(self) -> None:
        if self.runner:
            self.runner.resume()
            self.log("已继续")

    def stop(self) -> None:
        if self.runner:
            self._log_app_event("EVENT", command="stop-request", response="ui-stop-button")
            self.runner.stop()
            self.log("已请求停止")

    def _build_devices_for_maintenance(self, cfg: Dict[str, Any], io_logger=None) -> Dict[str, Any]:
        original_cfg = getattr(self, "cfg", None)
        try:
            self.cfg = cfg
            self._build_devices(io_logger=io_logger)
            return self.devices
        finally:
            self.cfg = original_cfg if original_cfg is not None else cfg

    def _set_safe_stop_ui_state(self, in_progress: bool, remaining_s: int | None = None) -> None:
        self.safe_stop_in_progress = in_progress
        if in_progress:
            self.safe_stop_button.configure(state="disabled")
            if remaining_s is None:
                self.safe_stop_countdown_var.set("恢复基线：执行中")
            else:
                self.safe_stop_countdown_var.set(f"恢复基线：执行中，约 {remaining_s}s")
            self._set_card_style(self.safe_stop_status_label, "warn")
        else:
            self.safe_stop_button.configure(state="normal")
            self.safe_stop_countdown_var.set("恢复基线：待命")
            self._set_card_style(self.safe_stop_status_label, "idle")

    def _start_safe_stop_countdown(self, total_s: int) -> None:
        self._set_safe_stop_ui_state(True, total_s)

        def _tick(remaining: int) -> None:
            if not self.safe_stop_in_progress:
                return
            if remaining <= 0:
                self._set_safe_stop_ui_state(True, 0)
                return
            self._set_safe_stop_ui_state(True, remaining)
            self.root.after(1000, lambda: _tick(remaining - 1))

        _tick(total_s)

    def _wait_for_worker_shutdown(self, timeout_s: float = 20.0, poll_s: float = 0.1) -> bool:
        worker = getattr(self, "worker", None)
        if worker is None:
            return True

        is_alive = getattr(worker, "is_alive", None)
        if not callable(is_alive):
            return True

        deadline = time.time() + max(0.1, float(timeout_s))
        while is_alive():
            remaining = deadline - time.time()
            if remaining <= 0:
                return False

            joiner = getattr(worker, "join", None)
            if callable(joiner):
                try:
                    joiner(timeout=min(max(0.05, float(poll_s)), remaining))
                except Exception:
                    time.sleep(min(max(0.05, float(poll_s)), remaining))
            else:
                time.sleep(min(max(0.05, float(poll_s)), remaining))

        return True

    @staticmethod
    def _safe_stop_runtime_options(cfg: Dict[str, Any], *, wait_for_worker: bool) -> Dict[str, float | int]:
        safe_cfg = (cfg or {}).get("workflow", {}).get("safe_stop", {})
        wait_timeout_s = float(safe_cfg.get("wait_for_worker_timeout_s", 90.0 if wait_for_worker else 20.0))
        perform_attempts = int(safe_cfg.get("perform_attempts", 3) or 3)
        reopen_attempts = int(safe_cfg.get("reopen_attempts", 2) or 2)
        retry_delay_s = float(safe_cfg.get("retry_delay_s", 1.5) or 1.5)
        reopen_retry_delay_s = float(safe_cfg.get("reopen_retry_delay_s", max(2.0, retry_delay_s)) or max(2.0, retry_delay_s))
        countdown_s = int(
            max(
                20.0,
                (wait_timeout_s if wait_for_worker else 0.0)
                + max(1, reopen_attempts) * max(8.0, float(perform_attempts) * max(0.5, retry_delay_s) + 6.0),
            )
        )
        return {
            "wait_timeout_s": max(20.0, wait_timeout_s),
            "perform_attempts": max(1, perform_attempts),
            "reopen_attempts": max(1, reopen_attempts),
            "retry_delay_s": max(0.0, retry_delay_s),
            "reopen_retry_delay_s": max(0.0, reopen_retry_delay_s),
            "countdown_s": max(20, countdown_s),
        }

    def safe_stop(self) -> None:
        if getattr(self, "safe_stop_in_progress", False):
            self.log("恢复基线正在执行中")
            return
        wait_for_worker = False
        if self.worker and self.worker.is_alive():
            if not messagebox.askokcancel("确认恢复基线", "当前流程仍在运行。是否先请求停止并执行恢复基线？"):
                return
            if self.runner:
                self._log_app_event("EVENT", command="stop-request", response="safe-stop")
                self.runner.stop()
                self.log("已请求停止，等待流程释放设备后恢复基线")
            wait_for_worker = True

        cfg = copy.deepcopy(self.last_runtime_cfg or getattr(self, "cfg", {}))
        if not cfg:
            messagebox.showerror("恢复基线失败", "尚未加载配置。")
            return
        options = self._safe_stop_runtime_options(cfg, wait_for_worker=wait_for_worker)

        def _run() -> None:
            self._start_safe_stop_countdown(int(options["countdown_s"]))
            self.set_status("恢复基线中...")
            tmp_logger = None
            try:
                if wait_for_worker:
                    if not self._wait_for_worker_shutdown(timeout_s=float(options["wait_timeout_s"]), poll_s=0.2):
                        self.log(
                            f"流程停止超时（>{int(float(options['wait_timeout_s']))}s）；"
                            "未重新打开串口以避免与运行线程冲突，请稍后再次恢复基线"
                        )
                        self.set_status("恢复基线等待停止超时")
                        return
                    self.log("流程已停止，开始恢复基线")
                tmp_logger = RunLogger(Path(cfg["paths"]["output_dir"]), cfg=cfg)
                self.current_io_path = Path(tmp_logger.io_path)
                self.current_run_dir = Path(tmp_logger.run_dir)
                self.log(f"恢复基线日志目录：{tmp_logger.run_dir}")

                success = False
                last_result: Dict[str, Any] | None = None
                last_error: Exception | None = None
                for reopen_attempt in range(1, int(options["reopen_attempts"]) + 1):
                    devices: Dict[str, Any] = {}
                    try:
                        self.log(
                            f"恢复基线尝试 {reopen_attempt}/{int(options['reopen_attempts'])}：重新连接维护设备"
                        )
                        devices = self._build_devices_for_maintenance(cfg, io_logger=tmp_logger)
                        result = perform_safe_stop_with_retries(
                            devices,
                            log_fn=self.log,
                            cfg=cfg,
                            attempts=int(options["perform_attempts"]),
                            retry_delay_s=float(options["retry_delay_s"]),
                        )
                        last_result = result
                        if bool(result.get("safe_stop_verified", True)):
                            self.log(f"恢复基线完成：{result}")
                            self.set_status("恢复基线完成")
                            success = True
                            break
                        issues = ", ".join(str(item) for item in result.get("safe_stop_issues", []) if str(item).strip())
                        self.log(
                            "恢复基线校验未通过"
                            + (f"：{issues}" if issues else "")
                        )
                    except Exception as exc:
                        last_error = exc
                        self.log(f"恢复基线尝试失败：{exc}")
                    finally:
                        self._close_devices(devices)

                    if reopen_attempt < int(options["reopen_attempts"]):
                        delay_s = float(options["reopen_retry_delay_s"])
                        if delay_s > 0:
                            self.log(f"等待 {int(round(delay_s))}s 后重试恢复基线")
                            time.sleep(delay_s)

                if not success:
                    if last_result is not None:
                        self.log(f"恢复基线最终未通过校验：{last_result}")
                    if last_error is not None:
                        self.log(f"恢复基线最终失败：{last_error}")
                    self.set_status("恢复基线失败")
            except Exception as exc:
                self.log(f"恢复基线失败：{exc}")
                self.set_status("ERROR")
            finally:
                self.current_io_path = None
                self.current_run_dir = None
                if tmp_logger is not None:
                    tmp_logger.close()
                self._set_safe_stop_ui_state(False)

        threading.Thread(target=_run, daemon=True).start()


def main() -> None:
    root = tk.Tk()
    App(root)
    root.mainloop()


