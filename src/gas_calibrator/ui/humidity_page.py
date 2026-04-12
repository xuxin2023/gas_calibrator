"""Humidity generator control page."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..devices import HumidityGenerator


FIELD_DESC = {
    "Br": "Baud rate",
    "Fa": "Flow target (L/min)",
    "Fl": "Flow actual (L/min)",
    "Psa": "Saturation pressure target (hPa)",
    "Ps": "Saturation pressure (hPa)",
    "Pc": "Test pressure (hPa)",
    "st": "Stable time (humidity,temp)",
    "Ts": "Saturation temp (C)",
    "Tc": "Test temp (C)",
    "Tda": "Dewpoint target (C)",
    "Td": "Dewpoint actual (C)",
    "Tfa": "Frostpoint target (C)",
    "Tf": "Frostpoint actual (C)",
    "Ta": "Control temp target (C)",
    "Tm": "Control temp actual (C)",
    "Ver": "Firmware",
    "UwA": "Water RH target (%RH)",
    "Uw": "Water RH actual (%RH)",
    "UiA": "Ice RH target (%RH)",
    "Ui": "Ice RH actual (%RH)",
}

INVALID_MEASUREMENT_SENTINEL = -999.0


class HumidityPage:
    """Humidity generator debug window."""

    def __init__(self, parent: tk.Tk, cfg: dict, log_fn=None):
        self.parent = parent
        self.cfg = cfg or {}
        self.log_fn = log_fn
        self.dev: HumidityGenerator | None = None
        self.last_data: dict = {}
        self._poll_job: str | None = None
        try:
            self._poll_interval_ms = max(300, int(float(self.cfg.get("poll_interval_ms", 1500))))
        except Exception:
            self._poll_interval_ms = 1500

        self.win = tk.Toplevel(parent)
        self.win.title("湿度发生器工作台")
        self.win.geometry("1360x860")
        self.win.minsize(1180, 760)
        self.win.configure(bg="#f1f7f8")
        self.win.protocol("WM_DELETE_WINDOW", self._on_close)

        self.status_var = tk.StringVar(value="未连接")
        self.snapshot_var = tk.StringVar(value="等待读取设备数据")
        self._status_palette = {
            "idle": {"bg": "#e5edf5", "fg": "#475569"},
            "ok": {"bg": "#dcfce7", "fg": "#166534"},
            "warn": {"bg": "#fef3c7", "fg": "#92400e"},
            "error": {"bg": "#fee2e2", "fg": "#991b1b"},
            "info": {"bg": "#dbeafe", "fg": "#1d4ed8"},
        }
        self._configure_styles()
        self._build_ui()

    def _configure_styles(self) -> None:
        style = ttk.Style(self.win)
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure(
            "HumidityCard.TLabelframe",
            background="#f9fcfd",
            bordercolor="#cfe1e6",
            relief="flat",
            borderwidth=1,
        )
        style.configure(
            "HumidityCard.TLabelframe.Label",
            background="#f9fcfd",
            foreground="#0f4f5e",
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        style.configure("HumiditySub.TLabelframe", background="#f9fcfd", bordercolor="#deeaee", relief="flat", borderwidth=1)
        style.configure(
            "HumiditySub.TLabelframe.Label",
            background="#f9fcfd",
            foreground="#4f7380",
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        style.configure("HumidityNotebook.TNotebook", background="#f1f7f8", borderwidth=0)
        style.configure(
            "HumidityNotebook.TNotebook.Tab",
            padding=(12, 6),
            font=("Microsoft YaHei UI", 9, "bold"),
            background="#e3f4f5",
            foreground="#4d6d78",
        )
        style.map(
            "HumidityNotebook.TNotebook.Tab",
            background=[("selected", "#ffffff"), ("active", "#f4fbfc")],
            foreground=[("selected", "#0f4f5e"), ("active", "#123247")],
        )
        style.configure(
            "HumidityAccent.TButton",
            font=("Microsoft YaHei UI", 9, "bold"),
            padding=(10, 4),
            background="#0f8b8d",
            foreground="white",
            borderwidth=0,
        )
        style.map(
            "HumidityAccent.TButton",
            background=[("active", "#0d7475"), ("disabled", "#cbd5dc")],
            foreground=[("disabled", "#7b8794")],
        )
        style.configure(
            "HumiditySoft.TButton",
            font=("Microsoft YaHei UI", 9, "bold"),
            padding=(10, 4),
            background="#ffffff",
            foreground="#123247",
            borderwidth=0,
        )
        style.map(
            "HumiditySoft.TButton",
            background=[("active", "#eef7f8"), ("disabled", "#e6ebf0")],
            foreground=[("disabled", "#95a3b2")],
        )
        style.configure(
            "HumidityWarn.TButton",
            font=("Microsoft YaHei UI", 9, "bold"),
            padding=(10, 4),
            background="#b45309",
            foreground="white",
            borderwidth=0,
        )
        style.map(
            "HumidityWarn.TButton",
            background=[("active", "#92400e"), ("disabled", "#dfd4c7")],
            foreground=[("disabled", "#8f7d70")],
        )
        style.configure(
            "HumidityTree.Treeview",
            rowheight=28,
            font=("Microsoft YaHei UI", 9),
            background="#fbfdfe",
            fieldbackground="#fbfdfe",
        )
        style.configure("HumidityTree.Treeview.Heading", font=("Microsoft YaHei UI", 9, "bold"))

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
        bg: str = "#f4f8fb",
    ) -> None:
        card = tk.Frame(parent, bg="#ffffff", highlightbackground=bg, highlightthickness=1, padx=1, pady=1)
        card.grid(row=row, column=column, sticky="nsew", padx=5, pady=5)
        inner = tk.Frame(card, bg=bg, padx=10, pady=7)
        inner.pack(fill="both", expand=True)
        tk.Frame(inner, bg="#ffffff", height=2).pack(fill="x", pady=(0, 6))
        tk.Label(inner, text=title, bg=bg, fg="#5f7388", font=("Microsoft YaHei UI", 9, "bold")).pack(anchor="w")
        tk.Label(
            inner,
            textvariable=textvariable,
            bg=bg,
            fg="#112133",
            justify="left",
            anchor="w",
            font=("Microsoft YaHei UI", 10, "bold"),
        ).pack(anchor="w", pady=(5, 0))

    def _build_ui(self) -> None:
        shell = tk.Frame(self.win, bg="#f1f7f8")
        shell.pack(fill="both", expand=True, padx=12, pady=12)
        shell.grid_columnconfigure(0, weight=1)
        shell.grid_rowconfigure(3, weight=1)

        hero = tk.Frame(shell, bg="#155e75", padx=18, pady=8)
        hero.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        hero_top = tk.Frame(hero, bg="#155e75")
        hero_top.pack(fill="x")
        tk.Label(hero_top, text="湿度发生器工作台", bg="#155e75", fg="white", font=("Microsoft YaHei UI", 15, "bold")).pack(side="left", anchor="w")
        tk.Label(hero_top, text="LIVE I/O", bg="#edfdfb", fg="#0f4f5e", padx=10, pady=3, font=("Consolas", 8, "bold")).pack(side="right", padx=(8, 0))
        tk.Label(hero_top, text="Humidity", bg="#d9f7f4", fg="#0f4f5e", padx=10, pady=3, font=("Microsoft YaHei UI", 8, "bold")).pack(side="right")
        tk.Label(
            hero,
            text="连接、设定和设备动作集中在上方，底部保留结构化数据、原始回包和日志。",
            bg="#155e75",
            fg="#d7eef3",
            font=("Microsoft YaHei UI", 9),
        ).pack(anchor="w", pady=(3, 0))
        hero_strip = tk.Frame(hero, bg="#155e75")
        hero_strip.pack(fill="x", pady=(6, 0))
        for text, bg, fg in (
            ("CONNECT READY", "#edfdfb", "#0f4f5e"),
            ("SETPOINT PANEL", "#d9f7f4", "#0f766e"),
            ("FIELD LOG", "#e3f5ff", "#155e75"),
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

        quick_strip = tk.Frame(shell, bg="#f1f7f8")
        quick_strip.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        for idx in range(3):
            quick_strip.grid_columnconfigure(idx, weight=1)
        quick_cards = (
            ("设备角色", "湿度源 / 控制器", "#edfdfb"),
            ("操作焦点", "设定 / 读取 / 动作", "#eef6ff"),
            ("当前状态", self.status_var, "#f6fbff"),
        )
        for idx, (title, value, bg) in enumerate(quick_cards):
            card = tk.Frame(quick_strip, bg="#ffffff", highlightbackground="#d7ecef", highlightthickness=1, padx=1, pady=1)
            card.grid(row=0, column=idx, sticky="ew", padx=(0, 8) if idx < 2 else 0)
            inner = tk.Frame(card, bg=bg, padx=10, pady=8)
            inner.pack(fill="both", expand=True)
            tk.Label(inner, text=title, bg=bg, fg="#5f7388", font=("Microsoft YaHei UI", 8, "bold")).pack(anchor="w")
            if isinstance(value, tk.StringVar):
                value_label = tk.Label(
                    inner,
                    textvariable=value,
                    bg=bg,
                    fg="#0f4f5e",
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
                tk.Label(inner, text=value, bg=bg, fg="#0f4f5e", font=("Microsoft YaHei UI", 10, "bold")).pack(anchor="w", pady=(4, 0))

        body = tk.Frame(shell, bg="#f1f7f8")
        body.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        body.grid_columnconfigure(0, weight=7)
        body.grid_columnconfigure(1, weight=3)

        left_shell = tk.Frame(body, bg="#f1f7f8")
        left_shell.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        right_shell = tk.Frame(body, bg="#f1f7f8")
        right_shell.grid(row=0, column=1, sticky="nsew")

        control_shell = self._make_card_shell(left_shell, "#d7ecef")
        control_shell.pack(fill="both", expand=True)
        control_card = ttk.LabelFrame(control_shell, text="连接与控制", style="HumidityCard.TLabelframe")
        control_card.pack(fill="both", expand=True)

        conn = ttk.Frame(control_card)
        conn.pack(fill="x", padx=10, pady=(10, 6))
        for col in range(8):
            conn.grid_columnconfigure(col, weight=1 if col in {1, 3} else 0)
        ttk.Label(conn, text="COM").grid(row=0, column=0, sticky="w")
        self.port_var = tk.StringVar(value=self.cfg.get("port", "COM8"))
        self.port_entry = ttk.Entry(conn, textvariable=self.port_var, width=14)
        self.port_entry.grid(row=0, column=1, sticky="ew", padx=(6, 10))
        ttk.Label(conn, text="波特率").grid(row=0, column=2, sticky="w")
        self.baud_var = tk.StringVar(value=str(self.cfg.get("baud", 9600)))
        self.baud_entry = ttk.Entry(conn, textvariable=self.baud_var, width=12)
        self.baud_entry.grid(row=0, column=3, sticky="ew", padx=(6, 10))
        self.connect_button = ttk.Button(conn, text="连接", command=self.connect, style="HumidityAccent.TButton")
        self.connect_button.grid(row=0, column=4, padx=(0, 6))
        self.disconnect_button = ttk.Button(conn, text="断开", command=self.disconnect, style="HumiditySoft.TButton")
        self.disconnect_button.grid(row=0, column=5)
        conn.grid_columnconfigure(4, minsize=96)
        conn.grid_columnconfigure(5, minsize=96)
        conn.grid_columnconfigure(6, minsize=96)
        self.status_label = tk.Label(
            conn,
            textvariable=self.status_var,
            bg="#dff7f2",
            fg="#0f4f5e",
            padx=12,
            pady=5,
            font=("Microsoft YaHei UI", 10, "bold"),
            highlightthickness=1,
            highlightbackground="#dff7f2",
        )
        self.status_label.grid(row=0, column=6, columnspan=2, sticky="e")

        tk.Label(
            control_card,
            textvariable=self.snapshot_var,
            anchor="w",
            justify="left",
            bg="#f2fbfc",
            fg="#2b5561",
            padx=14,
            pady=8,
            font=("Consolas", 9),
            highlightbackground="#d7ecef",
            highlightthickness=1,
        ).pack(fill="x", padx=10, pady=(0, 6))

        setpoints = ttk.LabelFrame(control_card, text="常用设定", style="HumiditySub.TLabelframe")
        setpoints.pack(fill="x", padx=10, pady=6)
        for col in range(6):
            setpoints.grid_columnconfigure(col, weight=1)
            setpoints.grid_columnconfigure(col, minsize=98)
        ttk.Label(setpoints, text="温度 (C)").grid(row=0, column=0, sticky="w", padx=8, pady=8)
        self.temp_var = tk.StringVar()
        self.temp_entry = ttk.Entry(setpoints, textvariable=self.temp_var)
        self.temp_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=8)
        self.set_temp_button = ttk.Button(setpoints, text="设温度", command=self.set_temp, style="HumiditySoft.TButton")
        self.set_temp_button.grid(row=0, column=2, padx=(0, 8), pady=8)
        ttk.Label(setpoints, text="湿度 (%RH)").grid(row=0, column=3, sticky="w", padx=8, pady=8)
        self.rh_var = tk.StringVar()
        self.rh_entry = ttk.Entry(setpoints, textvariable=self.rh_var)
        self.rh_entry.grid(row=0, column=4, sticky="ew", padx=(0, 8), pady=8)
        self.set_rh_button = ttk.Button(setpoints, text="设湿度", command=self.set_rh, style="HumiditySoft.TButton")
        self.set_rh_button.grid(row=0, column=5, pady=8)
        ttk.Label(setpoints, text="流量 (L/min)").grid(row=1, column=0, sticky="w", padx=8, pady=(0, 8))
        self.flow_var = tk.StringVar()
        self.flow_entry = ttk.Entry(setpoints, textvariable=self.flow_var)
        self.flow_entry.grid(row=1, column=1, sticky="ew", padx=(0, 8), pady=(0, 8))
        self.set_flow_button = ttk.Button(setpoints, text="设流量", command=self.set_flow, style="HumiditySoft.TButton")
        self.set_flow_button.grid(row=1, column=2, padx=(0, 8), pady=(0, 8))
        ttk.Label(setpoints, text="露点 (C)").grid(row=1, column=3, sticky="w", padx=8, pady=(0, 8))
        self.dewpoint_var = tk.StringVar()
        self.dewpoint_entry = ttk.Entry(setpoints, textvariable=self.dewpoint_var)
        self.dewpoint_entry.grid(row=1, column=4, sticky="ew", padx=(0, 8), pady=(0, 8))
        self.set_dewpoint_button = ttk.Button(
            setpoints,
            text="按露点设定",
            command=self.set_dewpoint,
            style="HumiditySoft.TButton",
        )
        self.set_dewpoint_button.grid(row=1, column=5, pady=(0, 8))

        actions = ttk.LabelFrame(control_card, text="设备动作", style="HumiditySub.TLabelframe")
        actions.pack(fill="x", padx=10, pady=(6, 10))
        for col in range(4):
            actions.grid_columnconfigure(col, weight=1)
            actions.grid_columnconfigure(col, minsize=120)
        self.ctrl_on_button = ttk.Button(actions, text="控制开", command=lambda: self.ctrl(True), style="HumidityAccent.TButton")
        self.ctrl_on_button.grid(row=0, column=0, sticky="ew", padx=6, pady=6)
        self.ctrl_off_button = ttk.Button(actions, text="控制关", command=lambda: self.ctrl(False), style="HumiditySoft.TButton")
        self.ctrl_off_button.grid(row=0, column=1, sticky="ew", padx=6, pady=6)
        self.cool_on_button = ttk.Button(actions, text="制冷开", command=lambda: self.cool(True), style="HumidityAccent.TButton")
        self.cool_on_button.grid(row=0, column=2, sticky="ew", padx=6, pady=6)
        self.cool_off_button = ttk.Button(actions, text="制冷关", command=lambda: self.cool(False), style="HumiditySoft.TButton")
        self.cool_off_button.grid(row=0, column=3, sticky="ew", padx=6, pady=6)
        self.heat_on_button = ttk.Button(actions, text="加热开", command=lambda: self.heat(True), style="HumidityAccent.TButton")
        self.heat_on_button.grid(row=1, column=0, sticky="ew", padx=6, pady=6)
        self.heat_off_button = ttk.Button(actions, text="加热关", command=lambda: self.heat(False), style="HumiditySoft.TButton")
        self.heat_off_button.grid(row=1, column=1, sticky="ew", padx=6, pady=6)
        self.ensure_run_button = ttk.Button(actions, text="确保运行", command=self.ensure_run, style="HumidityAccent.TButton")
        self.ensure_run_button.grid(row=1, column=2, sticky="ew", padx=6, pady=6)
        self.safe_stop_button = ttk.Button(actions, text="安全停止", command=self.safe_stop, style="HumidityWarn.TButton")
        self.safe_stop_button.grid(row=1, column=3, sticky="ew", padx=6, pady=6)
        self.read_all_button = ttk.Button(actions, text="读取全部", command=self.read_all, style="HumiditySoft.TButton")
        self.read_all_button.grid(row=2, column=0, sticky="ew", padx=6, pady=6)
        self.tag_var = tk.StringVar(value="All")
        tags = ["All", "Br", "Fa", "Fl", "Psa", "Ps", "Pc", "st", "Ts", "Tc", "Tda", "Td", "Tfa", "Tf", "Ta", "Tm", "Ver", "UwA", "Uw", "UiA", "Ui"]
        self.tag_combo = ttk.Combobox(actions, textvariable=self.tag_var, values=tags, width=10, state="readonly")
        self.tag_combo.grid(row=2, column=1, sticky="ew", padx=6, pady=6)
        self.read_tag_button = ttk.Button(actions, text="读取标签", command=self.read_tag, style="HumiditySoft.TButton")
        self.read_tag_button.grid(row=2, column=2, sticky="ew", padx=6, pady=6)
        tk.Label(
            actions,
            text="连接后再执行设定。露点设定会自动换算为湿度发生器温度/湿度目标。摘要固定在上方，避免顶区被重复信息挤满。",
            anchor="w",
            justify="left",
            wraplength=360,
            bg="#f5f9fc",
            fg="#41576b",
            padx=10,
            pady=8,
            font=("Microsoft YaHei UI", 9),
        ).grid(row=2, column=3, sticky="nsew", padx=6, pady=6)

        summary_shell = self._make_card_shell(right_shell, "#d7ecef")
        summary_shell.pack(fill="both", expand=True)
        summary_card = ttk.LabelFrame(summary_shell, text="关键状态", style="HumidityCard.TLabelframe")
        summary_card.pack(fill="both", expand=True)
        self.summary_vars = {
            "water_rh": tk.StringVar(value="--"),
            "ice_rh": tk.StringVar(value="--"),
            "test_temp": tk.StringVar(value="--"),
            "sat_temp": tk.StringVar(value="--"),
            "dewpoint": tk.StringVar(value="--"),
            "frost": tk.StringVar(value="--"),
            "flow": tk.StringVar(value="--"),
            "pressure": tk.StringVar(value="--"),
            "stable_time": tk.StringVar(value="--"),
            "ver_br": tk.StringVar(value="--"),
        }
        summary_grid = tk.Frame(summary_card, bg="#f9fcfd")
        summary_grid.pack(fill="both", expand=True, padx=10, pady=10)
        for col in range(2):
            summary_grid.grid_columnconfigure(col, weight=1)
            summary_grid.grid_columnconfigure(col, minsize=152)
        items = [
            ("水面湿度", "water_rh", "#f3fbfb"),
            ("冰面湿度", "ice_rh", "#f3f8fc"),
            ("测试温度", "test_temp", "#f9f7ef"),
            ("饱和温度", "sat_temp", "#eef8f3"),
            ("露点", "dewpoint", "#f6f1fb"),
            ("霜点", "frost", "#f8f2f8"),
            ("流量", "flow", "#eff7fb"),
            ("压力", "pressure", "#f9f4ef"),
            ("稳定时间", "stable_time", "#f4f8fb"),
            ("版本 / 波特率", "ver_br", "#f4f8fb"),
        ]
        for idx, (label, key, bg) in enumerate(items):
            self._build_metric_card(summary_grid, idx // 2, idx % 2, label, self.summary_vars[key], bg)

        tabs_shell = self._make_card_shell(shell, "#d7ecef")
        tabs_shell.grid(row=3, column=0, sticky="nsew")
        tabs = ttk.Notebook(tabs_shell, style="HumidityNotebook.TNotebook")
        tabs.pack(fill="both", expand=True)

        data_tab = ttk.Frame(tabs)
        raw_tab = ttk.Frame(tabs)
        log_tab = ttk.Frame(tabs)
        tabs.add(data_tab, text="结构化字段")
        tabs.add(raw_tab, text="原始回包")
        tabs.add(log_tab, text="日志")

        data_tab.grid_columnconfigure(0, weight=1)
        data_tab.grid_rowconfigure(0, weight=1)
        self.data_view = ttk.Treeview(data_tab, columns=("key", "desc", "value"), show="headings", style="HumidityTree.Treeview")
        self.data_view.heading("key", text="字段")
        self.data_view.heading("desc", text="说明")
        self.data_view.heading("value", text="值")
        self.data_view.column("key", width=120, anchor="w")
        self.data_view.column("desc", width=260, anchor="w")
        self.data_view.column("value", width=220, anchor="w")
        data_scroll = ttk.Scrollbar(data_tab, orient="vertical", command=self.data_view.yview)
        self.data_view.configure(yscrollcommand=data_scroll.set)
        self.data_view.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)
        data_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=10)

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

        self._set_status("未连接", "idle")
        self._apply_button_states()
        self.win.after(80, self._apply_layout)
        self.win.after(260, self._apply_layout)

    def _set_status(self, text: str, level: str) -> None:
        self.status_var.set(text)
        self._apply_status_chip(self.status_label, level)
        if hasattr(self, "quick_status_label"):
            self._apply_status_chip(self.quick_status_label, level)

    def _apply_button_states(self) -> None:
        connected = self.dev is not None
        self.connect_button.configure(state="disabled" if connected else "normal")
        self.disconnect_button.configure(state="normal" if connected else "disabled")
        entry_state = "disabled" if connected else "normal"
        self.port_entry.configure(state=entry_state)
        self.baud_entry.configure(state=entry_state)
        action_state = "normal" if connected else "disabled"
        for widget in (
            self.temp_entry,
            self.rh_entry,
            self.flow_entry,
            self.dewpoint_entry,
            self.set_temp_button,
            self.set_rh_button,
            self.set_flow_button,
            self.set_dewpoint_button,
            self.ctrl_on_button,
            self.ctrl_off_button,
            self.cool_on_button,
            self.cool_off_button,
            self.heat_on_button,
            self.heat_off_button,
            self.ensure_run_button,
            self.safe_stop_button,
            self.read_all_button,
            self.read_tag_button,
        ):
            widget.configure(state=action_state)
        self.tag_combo.configure(state="readonly" if connected else "disabled")

    def _ensure_dev(self) -> HumidityGenerator:
        if not self.dev:
            raise RuntimeError("设备尚未连接")
        return self.dev

    def _render_data(self, data: dict) -> None:
        for item in self.data_view.get_children():
            self.data_view.delete(item)
        for key in sorted(data.keys()):
            self.data_view.insert("", "end", values=(key, FIELD_DESC.get(key, ""), data.get(key)))

    def _fmt(self, val, digits: int = 2) -> str:
        if val is None:
            return "--"
        try:
            numeric = float(val)
        except Exception:
            return str(val)
        if numeric <= INVALID_MEASUREMENT_SENTINEL:
            return "--"
        return f"{numeric:.{digits}f}"

    def _update_snapshot(self, data: dict) -> None:
        tc = self._fmt(data.get("Tc"))
        uw = self._fmt(data.get("Uw"))
        td = self._fmt(data.get("Td"))
        fl = self._fmt(data.get("Fl"))
        self.snapshot_var.set(f"Tc={tc} C | Uw={uw}%RH | Td={td} C | Fl={fl} L/min")

    def _update_summary(self, data: dict) -> None:
        uw = self._fmt(data.get("Uw"))
        uwa = self._fmt(data.get("UwA"))
        ui = self._fmt(data.get("Ui"))
        uia = self._fmt(data.get("UiA"))
        tc = self._fmt(data.get("Tc"))
        ta = self._fmt(data.get("Ta"))
        ts = self._fmt(data.get("Ts"))
        td = self._fmt(data.get("Td"))
        tda = self._fmt(data.get("Tda"))
        tf = self._fmt(data.get("Tf"))
        tfa = self._fmt(data.get("Tfa"))
        fl = self._fmt(data.get("Fl"))
        fa = self._fmt(data.get("Fa"))
        psa = self._fmt(data.get("Psa"))
        ps = self._fmt(data.get("Ps"))
        pc = self._fmt(data.get("Pc"))
        ver = self._fmt(data.get("Ver"), 0)
        br = self._fmt(data.get("Br"), 0)
        st = data.get("st")
        st_h, st_t = "--", "--"
        if isinstance(st, str) and "," in st:
            st_h, st_t = [part.strip() or "--" for part in st.split(",", 1)]
        elif st:
            st_h = str(st)

        self.summary_vars["water_rh"].set(f"实测 {uw} / 目标 {uwa} %RH")
        self.summary_vars["ice_rh"].set(f"实测 {ui} / 目标 {uia} %RH")
        self.summary_vars["test_temp"].set(f"实测 {tc} / 目标 {ta} C")
        self.summary_vars["sat_temp"].set(f"{ts} C")
        self.summary_vars["dewpoint"].set(f"实测 {td} / 目标 {tda} C")
        self.summary_vars["frost"].set(f"实测 {tf} / 目标 {tfa} C")
        self.summary_vars["flow"].set(f"实测 {fl} / 目标 {fa} L/min")
        self.summary_vars["pressure"].set(f"{psa} / {ps} / {pc} hPa")
        self.summary_vars["stable_time"].set(f"{st_h} / {st_t}")
        self.summary_vars["ver_br"].set(f"{ver} / {br}")
        self._update_snapshot(data)

    def _set_raw(self, raw: str) -> None:
        self.raw_text.delete("1.0", "end")
        self.raw_text.insert("end", raw or "")

    def _cancel_poll(self) -> None:
        if self._poll_job is not None:
            try:
                self.win.after_cancel(self._poll_job)
            except Exception:
                pass
            self._poll_job = None

    def _schedule_poll(self) -> None:
        self._cancel_poll()
        if self.dev and self.win.winfo_exists():
            self._poll_job = self.win.after(self._poll_interval_ms, self._poll_once)

    def _refresh_live(self, *, update_status: bool = False, log_message: str | None = None) -> None:
        dev = self._ensure_dev()
        snap = dev.fetch_all()
        raw = snap.get("raw") or ""
        data = snap.get("data") or {}
        if raw or not data:
            self._set_raw(raw)
        if data:
            merged = dict(self.last_data)
            merged.update(data)
            self.last_data = merged
            self._render_data(self.last_data)
            self._update_summary(self.last_data)
        if update_status:
            self._set_status("读取完成", "info")
        if log_message:
            self._log(log_message)

    def _poll_once(self) -> None:
        self._poll_job = None
        if not self.dev or not self.win.winfo_exists():
            return
        try:
            self._refresh_live(update_status=False)
        except Exception as exc:
            self._set_status(f"刷新失败: {exc}", "error")
            self._log(f"实时刷新失败: {exc}")
        finally:
            if self.dev and self.win.winfo_exists():
                self._schedule_poll()

    def connect(self) -> None:
        if self.dev:
            self._log("设备已连接")
            return
        try:
            port = self.port_var.get().strip()
            baud = int(self.baud_var.get().strip())
            self.dev = HumidityGenerator(port, baud)
            self.dev.open()
            self._set_status(f"已连接 {port} @ {baud}", "ok")
            self._apply_button_states()
            self._refresh_live(update_status=False)
            self._schedule_poll()
            self._log(f"连接成功 {port} @ {baud}")
        except Exception as exc:
            self.dev = None
            self._set_status("连接失败", "error")
            self._apply_button_states()
            self._log(f"连接失败: {exc}")

    def disconnect(self) -> None:
        self._cancel_poll()
        if self.dev:
            try:
                self.dev.close()
            except Exception:
                pass
            self.dev = None
        self._set_status("未连接", "idle")
        self._apply_button_states()
        self._log("已断开")

    def set_temp(self) -> None:
        dev = self._ensure_dev()
        val = float(self.temp_var.get())
        dev.set_target_temp(val)
        self.last_data["Ta"] = val
        self._refresh_live(update_status=False)
        self._log(f"设温度 {val}")

    def set_rh(self) -> None:
        dev = self._ensure_dev()
        val = float(self.rh_var.get())
        dev.set_target_rh(val)
        self.last_data["UwA"] = val
        self._refresh_live(update_status=False)
        self._log(f"设湿度 {val}")

    def set_flow(self) -> None:
        dev = self._ensure_dev()
        val = float(self.flow_var.get())
        dev.set_flow_target(val)
        self.last_data["Fa"] = val
        self._refresh_live(update_status=False)
        self._log(f"设流量 {val}")

    def set_dewpoint(self) -> None:
        dev = self._ensure_dev()
        dewpoint_c = float(self.dewpoint_var.get())
        result = dev.set_target_dewpoint(dewpoint_c)
        target_temp_c = float(result["target_temp_c"])
        target_rh_pct = float(result["target_rh_pct"])
        self.temp_var.set(f"{target_temp_c:g}")
        self.rh_var.set(f"{target_rh_pct:g}")
        self.last_data["Ta"] = target_temp_c
        self.last_data["Tda"] = dewpoint_c
        self.last_data["UwA"] = target_rh_pct
        self._refresh_live(update_status=False)
        self._log(
            f"按露点设定 {dewpoint_c:g}C -> 温度 {target_temp_c:g}C / 湿度 {target_rh_pct:g}%RH"
        )

    def ctrl(self, on: bool) -> None:
        dev = self._ensure_dev()
        dev.enable_control(on)
        self._refresh_live(update_status=False)
        self._log(f"控制 {'开' if on else '关'}")

    def cool(self, on: bool) -> None:
        dev = self._ensure_dev()
        dev.cool_on() if on else dev.cool_off()
        self._refresh_live(update_status=False)
        self._log(f"制冷 {'开' if on else '关'}")

    def heat(self, on: bool) -> None:
        dev = self._ensure_dev()
        dev.heat_on() if on else dev.heat_off()
        self._refresh_live(update_status=False)
        self._log(f"加热 {'开' if on else '关'}")

    def ensure_run(self) -> None:
        dev = self._ensure_dev()
        res = dev.ensure_run()
        self._refresh_live(update_status=False)
        self._log(f"ensure_run: {res}")

    def safe_stop(self) -> None:
        dev = self._ensure_dev()
        dev.safe_stop()
        self._refresh_live(update_status=False)
        self._log("安全停止")

    def read_all(self) -> None:
        self._refresh_live(update_status=True, log_message="读取全部完成")

    def read_tag(self) -> None:
        dev = self._ensure_dev()
        tag = self.tag_var.get().strip() or "All"
        if tag == "All":
            self.read_all()
            return
        snap = dev.fetch_tag_value(tag)
        self._set_raw(snap.get("raw_pick") or "")
        value = snap.get("value")
        if value is not None:
            self.last_data[tag] = value
            self._render_data({tag: value})
            self._update_summary(self.last_data)
        self._set_status(f"标签 {tag} 已读取", "info")
        self._log(f"读取标签 {tag} 完成")

    def _on_close(self) -> None:
        self.disconnect()
        self.win.destroy()
