"""Thermometer page."""

from __future__ import annotations

from collections import deque
from datetime import datetime
from statistics import mean, pstdev
import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, Optional

from ..devices import Thermometer


class ThermometerPage:
    """Thermometer debug window."""

    def __init__(self, parent: tk.Tk, cfg: Dict[str, Any], log_fn=None):
        self.parent = parent
        self.cfg = cfg or {}
        self.log_fn = log_fn
        self.dev: Optional[Thermometer] = None
        self._poll_job = None
        self._history = deque(maxlen=2000)
        self._max_rows = 80
        self._total_frames = 0
        self._ok_frames = 0
        self._status_palette = {
            "idle": {"bg": "#e5edf5", "fg": "#475569"},
            "ok": {"bg": "#dcfce7", "fg": "#166534"},
            "warn": {"bg": "#fef3c7", "fg": "#92400e"},
            "error": {"bg": "#fee2e2", "fg": "#991b1b"},
            "info": {"bg": "#dbeafe", "fg": "#1d4ed8"},
        }

        self.win = tk.Toplevel(parent)
        self.win.title("测温仪工作台")
        self.win.geometry("1380x860")
        self.win.minsize(1180, 760)
        self.win.configure(bg="#fbf3ef")
        self.win.protocol("WM_DELETE_WINDOW", self._on_close)

        self._configure_styles()
        self._build_ui()

    def _configure_styles(self) -> None:
        style = ttk.Style(self.win)
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("ThermoCard.TLabelframe", background="#fefaf8", bordercolor="#ead8cf", relief="flat", borderwidth=1)
        style.configure("ThermoCard.TLabelframe.Label", background="#fbf3ef", foreground="#8b3b1f", font=("Microsoft YaHei UI", 10, "bold"))
        style.configure("ThermoSub.TLabelframe", background="#fefaf8", bordercolor="#f0e0d8", relief="flat", borderwidth=1)
        style.configure("ThermoSub.TLabelframe.Label", background="#fefaf8", foreground="#8a6759", font=("Microsoft YaHei UI", 9, "bold"))
        style.configure("ThermoNotebook.TNotebook", background="#fbf3ef", borderwidth=0)
        style.configure("ThermoNotebook.TNotebook.Tab", padding=(10, 5), font=("Microsoft YaHei UI", 9, "bold"), background="#f3dfd4", foreground="#895f54")
        style.map("ThermoNotebook.TNotebook.Tab", background=[("selected", "#ffffff"), ("active", "#fff1ea")], foreground=[("selected", "#8b3b1f")])
        style.configure("ThermoAccent.TButton", font=("Microsoft YaHei UI", 9, "bold"), padding=(10, 4), background="#c2410c", foreground="white", borderwidth=0)
        style.map("ThermoAccent.TButton", background=[("active", "#9a3412"), ("disabled", "#e2d3cd")], foreground=[("disabled", "#92766b")])
        style.configure("ThermoSoft.TButton", font=("Microsoft YaHei UI", 9, "bold"), padding=(10, 4), background="#ffffff", foreground="#7c2d12", borderwidth=0)
        style.map("ThermoSoft.TButton", background=[("active", "#fff3ee"), ("disabled", "#f0ece9")], foreground=[("disabled", "#99867b")])
        style.configure("ThermoTree.Treeview", rowheight=28, font=("Microsoft YaHei UI", 9), background="#fbfdfe", fieldbackground="#fbfdfe")
        style.configure("ThermoTree.Treeview.Heading", font=("Microsoft YaHei UI", 9, "bold"))

    def _apply_layout(self) -> None:
        return None

    def _make_card_shell(self, parent: tk.Widget, tone: str) -> tk.Frame:
        return tk.Frame(parent, bg=tone, padx=1, pady=1)

    def _apply_status_chip(self, widget: tk.Label, level: str) -> None:
        palette = self._status_palette.get(level, self._status_palette["idle"])
        widget.configure(bg=palette["bg"], fg=palette["fg"], highlightbackground=palette["bg"], highlightcolor=palette["bg"])

    def _log(self, msg: str) -> None:
        if self.log_fn:
            self.log_fn(msg)
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")

    def _build_metric_card(
        self,
        parent: tk.Widget,
        row: int,
        column: int,
        title: str,
        textvariable: tk.StringVar,
        bg: str,
        value_font=("Microsoft YaHei UI", 11, "bold"),
    ) -> tk.Label:
        card = tk.Frame(parent, bg="#ffffff", highlightbackground=bg, highlightthickness=1, padx=1, pady=1)
        card.grid(row=row, column=column, sticky="nsew", padx=5, pady=5)
        inner = tk.Frame(card, bg=bg, padx=10, pady=7)
        inner.pack(fill="both", expand=True)
        tk.Frame(inner, bg="#ffffff", height=2).pack(fill="x", pady=(0, 6))
        tk.Label(inner, text=title, bg=bg, fg="#5b6473", font=("Microsoft YaHei UI", 9, "bold")).pack(anchor="w")
        value_label = tk.Label(inner, textvariable=textvariable, bg=bg, fg="#111827", font=value_font, anchor="w", justify="left")
        value_label.pack(anchor="w", pady=(6, 0))
        return value_label

    def _build_ui(self) -> None:
        shell = tk.Frame(self.win, bg="#fbf3ef")
        shell.pack(fill="both", expand=True, padx=12, pady=12)
        shell.grid_columnconfigure(0, weight=1)
        shell.grid_rowconfigure(3, weight=1)

        hero = tk.Frame(shell, bg="#9a3412", padx=18, pady=8)
        hero.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        hero_top = tk.Frame(hero, bg="#9a3412")
        hero_top.pack(fill="x")
        tk.Label(hero_top, text="测温仪工作台", bg="#9a3412", fg="white", font=("Microsoft YaHei UI", 15, "bold")).pack(side="left", anchor="w")
        tk.Label(hero_top, text="STABILITY", bg="#fff8f3", fg="#8b3b1f", padx=10, pady=3, font=("Consolas", 8, "bold")).pack(side="right", padx=(8, 0))
        tk.Label(hero_top, text="Thermometer", bg="#fff1df", fg="#8b3b1f", padx=10, pady=3, font=("Microsoft YaHei UI", 8, "bold")).pack(side="right")
        tk.Label(
            hero,
            text="串口参数、判稳修正和统计卡片集中在上方，底部保留样本、原始回包和日志。",
            bg="#9a3412",
            fg="#fde1d6",
            font=("Microsoft YaHei UI", 9),
        ).pack(anchor="w", pady=(2, 0))
        hero_strip = tk.Frame(hero, bg="#9a3412")
        hero_strip.pack(fill="x", pady=(6, 0))
        for text, bg, fg in (
            ("SERIAL INPUT", "#fff8f3", "#8b3b1f"),
            ("STABLE WINDOW", "#fff1df", "#c2410c"),
            ("SAMPLE LOG", "#fde8df", "#9a3412"),
        ):
            tk.Label(
                hero_strip,
                text=text,
                bg=bg,
                fg=fg,
                padx=10,
                pady=3,
                font=("Consolas", 8, "bold"),
            ).pack(side="left", padx=(0, 8))

        quick_strip = tk.Frame(shell, bg="#fbf3ef")
        quick_strip.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        for idx in range(3):
            quick_strip.grid_columnconfigure(idx, weight=1)
        self.quick_status_var = tk.StringVar(value="未连接")
        quick_cards = (
            ("设备角色", "温度采样 / 修正", "#fff8f3"),
            ("工作方式", "判稳 / 统计 / 轮询", "#fff1df"),
            ("通信状态", self.quick_status_var, "#fffaf7"),
        )
        for idx, (title, value, bg) in enumerate(quick_cards):
            card = tk.Frame(quick_strip, bg="#ffffff", highlightbackground="#f4dfd5", highlightthickness=1, padx=1, pady=1)
            card.grid(row=0, column=idx, sticky="ew", padx=(0, 8) if idx < 2 else 0)
            inner = tk.Frame(card, bg=bg, padx=10, pady=8)
            inner.pack(fill="both", expand=True)
            tk.Label(inner, text=title, bg=bg, fg="#8a6759", font=("Microsoft YaHei UI", 8, "bold")).pack(anchor="w")
            if isinstance(value, tk.StringVar):
                value_label = tk.Label(
                    inner,
                    textvariable=value,
                    bg=bg,
                    fg="#8b3b1f",
                    font=("Microsoft YaHei UI", 10, "bold"),
                    padx=8,
                    pady=4,
                    highlightthickness=1,
                    highlightbackground=bg,
                )
                value_label.pack(anchor="w", pady=(4, 0))
                if idx == 2:
                    self.quick_status_label = value_label
            else:
                tk.Label(inner, text=value, bg=bg, fg="#8b3b1f", font=("Microsoft YaHei UI", 10, "bold")).pack(anchor="w", pady=(4, 0))

        body = tk.Frame(shell, bg="#fbf3ef")
        body.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        body.grid_columnconfigure(0, weight=7)
        body.grid_columnconfigure(1, weight=4)

        left_shell = tk.Frame(body, bg="#fbf3ef")
        left_shell.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        right_shell = tk.Frame(body, bg="#fbf3ef")
        right_shell.grid(row=0, column=1, sticky="nsew")

        left_shell_card = self._make_card_shell(left_shell, "#f4dfd5")
        left_shell_card.pack(fill="both", expand=True)
        setup = ttk.LabelFrame(left_shell_card, text="连接与采样", style="ThermoCard.TLabelframe")
        setup.pack(fill="both", expand=True)

        conn = ttk.Frame(setup)
        conn.pack(fill="x", padx=10, pady=(10, 6))
        for col in range(8):
            conn.grid_columnconfigure(col, weight=1)
        ttk.Label(conn, text="COM").grid(row=0, column=0, sticky="w")
        self.port_var = tk.StringVar(value=self.cfg.get("port", "COM9"))
        self.port_entry = ttk.Entry(conn, textvariable=self.port_var)
        self.port_entry.grid(row=0, column=1, sticky="ew", padx=(6, 10))
        ttk.Label(conn, text="波特率").grid(row=0, column=2, sticky="w")
        self.baud_var = tk.StringVar(value=str(self.cfg.get("baud", 2400)))
        self.baud_entry = ttk.Entry(conn, textvariable=self.baud_var)
        self.baud_entry.grid(row=0, column=3, sticky="ew", padx=(6, 10))
        ttk.Label(conn, text="数据位").grid(row=0, column=4, sticky="w")
        self.bytesize_var = tk.StringVar(value=str(self.cfg.get("bytesize", 8)))
        self.bytesize_combo = ttk.Combobox(conn, textvariable=self.bytesize_var, values=["7", "8"], width=6, state="readonly")
        self.bytesize_combo.grid(row=0, column=5, sticky="ew", padx=(6, 10))
        ttk.Label(conn, text="校验").grid(row=0, column=6, sticky="w")
        self.parity_var = tk.StringVar(value=str(self.cfg.get("parity", "N")).upper())
        self.parity_combo = ttk.Combobox(conn, textvariable=self.parity_var, values=["N", "E", "O"], width=6, state="readonly")
        self.parity_combo.grid(row=0, column=7, sticky="ew", padx=(6, 0))
        for col in range(8):
            conn.grid_columnconfigure(col, minsize=88)

        serial2 = ttk.Frame(setup)
        serial2.pack(fill="x", padx=10, pady=(0, 6))
        for col in range(8):
            serial2.grid_columnconfigure(col, weight=1)
        ttk.Label(serial2, text="停止位").grid(row=0, column=0, sticky="w")
        self.stopbits_var = tk.StringVar(value=str(self.cfg.get("stopbits", 1)))
        self.stopbits_combo = ttk.Combobox(serial2, textvariable=self.stopbits_var, values=["1", "1.5", "2"], state="readonly")
        self.stopbits_combo.grid(row=0, column=1, sticky="ew", padx=(6, 10))
        ttk.Label(serial2, text="超时 (s)").grid(row=0, column=2, sticky="w")
        self.timeout_var = tk.StringVar(value=str(self.cfg.get("timeout", 1.2)))
        self.timeout_entry = ttk.Entry(serial2, textvariable=self.timeout_var)
        self.timeout_entry.grid(row=0, column=3, sticky="ew", padx=(6, 10))
        ttk.Label(serial2, text="轮询间隔").grid(row=0, column=4, sticky="w")
        self.interval_var = tk.StringVar(value="1.0")
        self.interval_entry = ttk.Entry(serial2, textvariable=self.interval_var)
        self.interval_entry.grid(row=0, column=5, sticky="ew", padx=(6, 10))
        self.connect_button = ttk.Button(serial2, text="连接", command=self.connect, style="ThermoAccent.TButton")
        self.connect_button.grid(row=0, column=6, padx=(0, 6))
        self.disconnect_button = ttk.Button(serial2, text="断开", command=self.disconnect, style="ThermoSoft.TButton")
        self.disconnect_button.grid(row=0, column=7)
        serial2.grid_columnconfigure(6, minsize=96)
        serial2.grid_columnconfigure(7, minsize=96)

        sample_box = ttk.LabelFrame(setup, text="判稳与修正", style="ThermoSub.TLabelframe")
        sample_box.pack(fill="x", padx=10, pady=6)
        for col in range(6):
            sample_box.grid_columnconfigure(col, weight=1)
            sample_box.grid_columnconfigure(col, minsize=92)
        ttk.Label(sample_box, text="窗口点数").grid(row=0, column=0, sticky="w", padx=8, pady=8)
        self.stable_window_var = tk.StringVar(value="20")
        self.stable_window_entry = ttk.Entry(sample_box, textvariable=self.stable_window_var)
        self.stable_window_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=8)
        ttk.Label(sample_box, text="阈值 (C)").grid(row=0, column=2, sticky="w", padx=8, pady=8)
        self.stable_tol_var = tk.StringVar(value="0.2")
        self.stable_tol_entry = ttk.Entry(sample_box, textvariable=self.stable_tol_var)
        self.stable_tol_entry.grid(row=0, column=3, sticky="ew", padx=(0, 8), pady=8)
        ttk.Label(sample_box, text="修正值 (C)").grid(row=0, column=4, sticky="w", padx=8, pady=8)
        self.corr_var = tk.StringVar(value="0.00")
        self.corr_entry = ttk.Entry(sample_box, textvariable=self.corr_var)
        self.corr_entry.grid(row=0, column=5, sticky="ew", padx=(0, 8), pady=8)
        self.plus200_var = tk.BooleanVar(value=False)
        self.plus200_check = ttk.Checkbutton(sample_box, text="+200C 模式", variable=self.plus200_var)
        self.plus200_check.grid(row=1, column=0, columnspan=2, sticky="w", padx=8, pady=(0, 8))
        self.read_once_button = ttk.Button(sample_box, text="读取一次", command=self.read_once, style="ThermoSoft.TButton")
        self.read_once_button.grid(row=1, column=2, sticky="ew", padx=8, pady=(0, 8))
        self.start_poll_button = ttk.Button(sample_box, text="开始轮询", command=self.start_poll, style="ThermoAccent.TButton")
        self.start_poll_button.grid(row=1, column=3, sticky="ew", padx=8, pady=(0, 8))
        self.stop_poll_button = ttk.Button(sample_box, text="停止轮询", command=self.stop_poll, style="ThermoSoft.TButton")
        self.stop_poll_button.grid(row=1, column=4, sticky="ew", padx=8, pady=(0, 8))
        self.clear_stats_button = ttk.Button(sample_box, text="清空统计", command=self.clear_stats, style="ThermoSoft.TButton")
        self.clear_stats_button.grid(row=1, column=5, sticky="ew", padx=8, pady=(0, 8))

        actions = ttk.LabelFrame(setup, text="运行提示", style="ThermoSub.TLabelframe")
        actions.pack(fill="x", padx=10, pady=(0, 10))
        bar = ttk.Frame(actions)
        bar.pack(fill="x", padx=8, pady=(8, 6))
        self.clear_log_button = ttk.Button(bar, text="清空日志", command=self.clear_log, style="ThermoSoft.TButton")
        self.clear_log_button.pack(side="left")
        tk.Label(
            actions,
            text="轮询时只保留停止按钮和日志清理，其余输入会自动锁定，避免采样过程误改参数。",
            justify="left",
            anchor="nw",
            bg="#fffaf7",
            fg="#334155",
            padx=12,
            pady=10,
            wraplength=560,
            font=("Microsoft YaHei UI", 9),
        ).pack(fill="x", padx=8, pady=(0, 8))

        right_shell_card = self._make_card_shell(right_shell, "#f4dfd5")
        right_shell_card.pack(fill="both", expand=True)
        stats = ttk.LabelFrame(right_shell_card, text="统计与状态", style="ThermoCard.TLabelframe")
        stats.pack(fill="both", expand=True)
        self.summary_vars = {
            "status": tk.StringVar(value="未连接"),
            "updated": tk.StringVar(value="--"),
            "raw_temp": tk.StringVar(value="--"),
            "actual_temp": tk.StringVar(value="--"),
            "count": tk.StringVar(value="0"),
            "ok_rate": tk.StringVar(value="0.0%"),
            "min": tk.StringVar(value="--"),
            "max": tk.StringVar(value="--"),
            "avg": tk.StringVar(value="--"),
            "std": tk.StringVar(value="--"),
            "span": tk.StringVar(value="--"),
            "stable": tk.StringVar(value="--"),
        }
        stat_grid = tk.Frame(stats, bg="#fefaf8")
        stat_grid.pack(fill="both", expand=True, padx=10, pady=10)
        for col in range(3):
            stat_grid.grid_columnconfigure(col, weight=1)
            stat_grid.grid_columnconfigure(col, minsize=118)
        cards = [
            ("通信状态", "status", "#ede9fe"),
            ("最新时间", "updated", "#dbeafe"),
            ("原始温度", "raw_temp", "#fff2cf"),
            ("修正温度", "actual_temp", "#dcfce7"),
            ("样本数", "count", "#fce7f3"),
            ("有效率", "ok_rate", "#d1fae5"),
            ("最小值", "min", "#f4f4f5"),
            ("最大值", "max", "#fee2e2"),
            ("平均值", "avg", "#dbeafe"),
            ("标准差", "std", "#ede9fe"),
            ("峰峰值", "span", "#fff2cf"),
            ("稳定性", "stable", "#dcfce7"),
        ]
        for idx, (title, key, bg) in enumerate(cards):
            label = self._build_metric_card(stat_grid, idx // 3, idx % 3, title, self.summary_vars[key], bg)
            if key == "status":
                self.summary_status_label = label

        tabs_shell = self._make_card_shell(shell, "#f4dfd5")
        tabs_shell.grid(row=3, column=0, sticky="nsew")
        tabs = ttk.Notebook(tabs_shell, style="ThermoNotebook.TNotebook")
        tabs.pack(fill="both", expand=True)
        recent_tab = ttk.Frame(tabs)
        raw_tab = ttk.Frame(tabs)
        log_tab = ttk.Frame(tabs)
        tabs.add(recent_tab, text="最近样本")
        tabs.add(raw_tab, text="原始回包")
        tabs.add(log_tab, text="日志")

        recent_tab.grid_columnconfigure(0, weight=1)
        recent_tab.grid_rowconfigure(0, weight=1)
        self.recent_view = ttk.Treeview(recent_tab, columns=("ts", "raw", "display", "actual", "state"), show="headings", style="ThermoTree.Treeview")
        self.recent_view.heading("ts", text="时间")
        self.recent_view.heading("raw", text="原始串")
        self.recent_view.heading("display", text="显示温度")
        self.recent_view.heading("actual", text="修正后")
        self.recent_view.heading("state", text="状态")
        self.recent_view.column("ts", width=100, anchor="center")
        self.recent_view.column("raw", width=360, anchor="w")
        self.recent_view.column("display", width=120, anchor="e")
        self.recent_view.column("actual", width=120, anchor="e")
        self.recent_view.column("state", width=100, anchor="center")
        recent_scroll = ttk.Scrollbar(recent_tab, orient="vertical", command=self.recent_view.yview)
        self.recent_view.configure(yscrollcommand=recent_scroll.set)
        self.recent_view.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)
        recent_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=10)

        raw_tab.grid_columnconfigure(0, weight=1)
        raw_tab.grid_rowconfigure(0, weight=1)
        self.raw_text = tk.Text(raw_tab, bg="#f7fafc", fg="#122131", insertbackground="#122131", font=("Consolas", 10), wrap="word")
        raw_scroll = ttk.Scrollbar(raw_tab, orient="vertical", command=self.raw_text.yview)
        self.raw_text.configure(yscrollcommand=raw_scroll.set)
        self.raw_text.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)
        raw_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=10)

        log_tab.grid_columnconfigure(0, weight=1)
        log_tab.grid_rowconfigure(0, weight=1)
        self.log_text = tk.Text(log_tab, bg="#f7fafc", fg="#122131", insertbackground="#122131", font=("Consolas", 10), wrap="word")
        log_scroll = ttk.Scrollbar(log_tab, orient="vertical", command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=log_scroll.set)
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)
        log_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=10)

        self._set_runtime_status("未连接", "idle")
        self._apply_button_states()
        self.win.after(80, self._apply_layout)
        self.win.after(260, self._apply_layout)

    def _ensure_dev(self) -> Thermometer:
        if not self.dev:
            raise RuntimeError("设备尚未连接")
        return self.dev

    @staticmethod
    def _fmt_temp(val: Optional[float]) -> str:
        if val is None:
            return "--"
        try:
            return f"{float(val):.2f} C"
        except Exception:
            return "--"

    @staticmethod
    def _fmt_number(val: Optional[float], digits: int = 3) -> str:
        if val is None:
            return "--"
        try:
            return f"{float(val):.{digits}f}"
        except Exception:
            return "--"

    def _set_raw(self, raw: str) -> None:
        self.raw_text.delete("1.0", "end")
        self.raw_text.insert("end", raw or "")

    def _set_connected(self, connected: bool) -> None:
        self.summary_vars["status"].set("已连接" if connected else "未连接")
        if hasattr(self, "quick_status_var"):
            self.quick_status_var.set("已连接" if connected else "未连接")
        level = "ok" if connected else "idle"
        if hasattr(self, "summary_status_label"):
            self._apply_status_chip(self.summary_status_label, level)
        if hasattr(self, "quick_status_label"):
            self._apply_status_chip(self.quick_status_label, level)
        self._apply_button_states()

    def _set_runtime_status(self, text: str, level: str) -> None:
        self.summary_vars["status"].set(text)
        if hasattr(self, "quick_status_var"):
            self.quick_status_var.set(text)
        if hasattr(self, "summary_status_label"):
            self._apply_status_chip(self.summary_status_label, level)
        if hasattr(self, "quick_status_label"):
            self._apply_status_chip(self.quick_status_label, level)

    def _apply_button_states(self) -> None:
        connected = self.dev is not None
        polling = self._poll_job is not None
        self.connect_button.configure(state="disabled" if connected else "normal")
        self.disconnect_button.configure(state="normal" if connected else "disabled")
        serial_disabled = "disabled" if connected else "normal"
        self.port_entry.configure(state=serial_disabled)
        self.baud_entry.configure(state=serial_disabled)
        self.timeout_entry.configure(state=serial_disabled)
        combo_state = "disabled" if connected else "readonly"
        self.bytesize_combo.configure(state=combo_state)
        self.parity_combo.configure(state=combo_state)
        self.stopbits_combo.configure(state=combo_state)
        tune_state = "disabled" if polling else "normal"
        for widget in (self.interval_entry, self.stable_window_entry, self.stable_tol_entry, self.corr_entry):
            widget.configure(state=tune_state)
        self.plus200_check.configure(state="disabled" if polling else "normal")
        self.read_once_button.configure(state="normal" if connected and not polling else "disabled")
        self.start_poll_button.configure(state="normal" if connected and not polling else "disabled")
        self.stop_poll_button.configure(state="normal" if polling else "disabled")
        self.clear_stats_button.configure(state="normal" if connected and not polling else "disabled")

    def _get_correction(self) -> float:
        try:
            return float(self.corr_var.get().strip())
        except Exception:
            return 0.0

    def _apply_correction(self, temp: float) -> float:
        value = float(temp) + self._get_correction()
        if self.plus200_var.get():
            value += 200.0
        return value

    def _insert_recent(self, ts: str, raw: str, display: Optional[float], actual: Optional[float], state: str) -> None:
        self.recent_view.insert(
            "",
            0,
            values=(
                ts,
                raw,
                self._fmt_number(display, 2) if display is not None else "--",
                self._fmt_number(actual, 2) if actual is not None else "--",
                state,
            ),
        )
        children = self.recent_view.get_children()
        if len(children) > self._max_rows:
            self.recent_view.delete(children[-1])

    def _update_stats(self) -> None:
        values = [v for v in self._history if isinstance(v, (int, float))]
        self.summary_vars["count"].set(str(len(values)))
        self.summary_vars["ok_rate"].set(f"{(self._ok_frames / self._total_frames) * 100:.1f}%" if self._total_frames else "0.0%")
        if not values:
            for key in ("min", "max", "avg", "std", "span", "stable"):
                self.summary_vars[key].set("--")
            return
        min_v = min(values)
        max_v = max(values)
        avg_v = mean(values)
        std_v = pstdev(values) if len(values) > 1 else 0.0
        span_v = max_v - min_v
        self.summary_vars["min"].set(self._fmt_temp(min_v))
        self.summary_vars["max"].set(self._fmt_temp(max_v))
        self.summary_vars["avg"].set(self._fmt_temp(avg_v))
        self.summary_vars["std"].set(self._fmt_number(std_v) + " C")
        self.summary_vars["span"].set(self._fmt_number(span_v) + " C")
        try:
            win = max(2, int(self.stable_window_var.get().strip()))
        except Exception:
            win = 20
        try:
            tol = max(0.0, float(self.stable_tol_var.get().strip()))
        except Exception:
            tol = 0.2
        segment = values[-win:]
        if len(segment) < 2:
            self.summary_vars["stable"].set("采集不足")
            return
        seg_span = max(segment) - min(segment)
        self.summary_vars["stable"].set(f"稳定 (Δ={seg_span:.3f}C)" if seg_span <= tol else f"波动 (Δ={seg_span:.3f}C)")

    def _on_sample(self, data: Dict[str, Any]) -> None:
        now = datetime.now().strftime("%H:%M:%S")
        raw = data.get("raw", "")
        temp = data.get("temp_c")
        ok = bool(data.get("ok"))
        self._total_frames += 1
        self._set_raw(raw)
        self.summary_vars["updated"].set(now)
        if ok and isinstance(temp, (int, float)):
            display_temp = float(temp)
            actual_temp = self._apply_correction(display_temp)
            self._ok_frames += 1
            self.summary_vars["raw_temp"].set(self._fmt_temp(display_temp))
            self.summary_vars["actual_temp"].set(self._fmt_temp(actual_temp))
            self._history.append(actual_temp)
            self._insert_recent(now, raw, display_temp, actual_temp, "有效")
            self._log(f"温度读取成功: 显示={display_temp:.2f}C 修正后={actual_temp:.2f}C")
        else:
            self.summary_vars["raw_temp"].set("--")
            self.summary_vars["actual_temp"].set("--")
            self._insert_recent(now, raw, None, None, "无效")
            self._log(f"解析失败，原始串: {raw!r}")
        self._update_stats()

    def connect(self) -> None:
        if self.dev:
            self._log("设备已连接")
            return
        try:
            port = self.port_var.get().strip()
            baud = int(self.baud_var.get().strip())
            bytesize = int(self.bytesize_var.get().strip())
            parity = self.parity_var.get().strip().upper() or "N"
            stopbits = float(self.stopbits_var.get().strip())
            timeout = float(self.timeout_var.get().strip())
            self.dev = Thermometer(port, baudrate=baud, timeout=timeout, parity=parity, stopbits=stopbits, bytesize=bytesize)
            self.dev.open()
            self.dev.flush_input()
            self._set_connected(True)
            self._log(f"连接成功 {port} @ {baud}, {bytesize}{parity}{stopbits:g}, timeout={timeout:.2f}s")
        except Exception as exc:
            self.dev = None
            self._set_runtime_status("连接失败", "error")
            self._apply_button_states()
            self._log(f"连接失败: {exc}")

    def disconnect(self) -> None:
        self.stop_poll()
        if self.dev:
            try:
                self.dev.close()
            except Exception:
                pass
            self.dev = None
        self._set_connected(False)
        self._log("已断开")

    def read_once(self) -> None:
        try:
            data = self._ensure_dev().read_current()
            self._on_sample(data)
            self._set_runtime_status("读取完成", "info")
        except Exception as exc:
            self._set_runtime_status("读取失败", "error")
            self._log(f"读取失败: {exc}")

    def _poll_once(self) -> None:
        self.read_once()
        try:
            interval = max(0.2, float(self.interval_var.get()))
        except Exception:
            interval = 1.0
        self._poll_job = self.win.after(int(interval * 1000), self._poll_once)

    def start_poll(self) -> None:
        try:
            self._ensure_dev()
            if self._poll_job is not None:
                self._log("轮询已启动")
                return
            self._poll_once()
            self._set_runtime_status("轮询中", "info")
            self._apply_button_states()
            self._log("轮询开始")
        except Exception as exc:
            self._log(f"启动轮询失败: {exc}")

    def stop_poll(self) -> None:
        if self._poll_job is not None:
            self.win.after_cancel(self._poll_job)
            self._poll_job = None
            self._set_runtime_status("轮询停止", "warn")
            self._apply_button_states()
            self._log("轮询已停止")

    def clear_stats(self) -> None:
        self._history.clear()
        self._total_frames = 0
        self._ok_frames = 0
        self.summary_vars["updated"].set("--")
        self.summary_vars["raw_temp"].set("--")
        self.summary_vars["actual_temp"].set("--")
        for item in self.recent_view.get_children():
            self.recent_view.delete(item)
        self._update_stats()
        self._log("统计已清空")

    def clear_log(self) -> None:
        self.log_text.delete("1.0", "end")
        self._log("日志已清空")

    def _on_close(self) -> None:
        self.disconnect()
        self.win.destroy()
