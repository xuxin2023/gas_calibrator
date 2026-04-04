"""Dewpoint meter page."""

from __future__ import annotations

from collections import deque
import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, Optional

from ..devices import DewpointMeter


PAYLOAD_DESC = {
    1: "Dewpoint (C)",
    2: "Temperature (C)",
    8: "Relative humidity (%RH)",
    9: "Flag 1",
    10: "Flag 2",
    11: "Flag 3",
    12: "Flag 4",
}


class DewpointPage:
    """Dewpoint meter debug window."""

    def __init__(self, parent: tk.Tk, cfg: Dict[str, Any], log_fn=None):
        self.parent = parent
        self.cfg = cfg or {}
        self.log_fn = log_fn
        self.dev: Optional[DewpointMeter] = None
        self._poll_job = None
        self._dp_history = deque(maxlen=20)

        self.win = tk.Toplevel(parent)
        self.win.title("露点仪工作台")
        self.win.geometry("1280x820")
        self.win.minsize(1120, 720)
        self.win.configure(bg="#f5f1fb")
        self.win.protocol("WM_DELETE_WINDOW", self._on_close)

        self.status_var = tk.StringVar(value="未连接")
        self.snapshot_var = tk.StringVar(value="未读取")
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
        style.configure("DewCard.TLabelframe", background="#fbf9fe", bordercolor="#ddd9ec", relief="flat", borderwidth=1)
        style.configure("DewCard.TLabelframe.Label", background="#f5f1fb", foreground="#4c2583", font=("Microsoft YaHei UI", 10, "bold"))
        style.configure("DewSub.TLabelframe", background="#fbf9fe", bordercolor="#ebe4f3", relief="flat", borderwidth=1)
        style.configure("DewSub.TLabelframe.Label", background="#fbf9fe", foreground="#6a5a82", font=("Microsoft YaHei UI", 9, "bold"))
        style.configure("DewNotebook.TNotebook", background="#f5f1fb", borderwidth=0)
        style.configure("DewNotebook.TNotebook.Tab", padding=(10, 5), font=("Microsoft YaHei UI", 9, "bold"), background="#eadfff", foreground="#644b86")
        style.map("DewNotebook.TNotebook.Tab", background=[("selected", "#ffffff"), ("active", "#f4eeff")], foreground=[("selected", "#4c2583")])
        style.configure("DewAccent.TButton", font=("Microsoft YaHei UI", 9, "bold"), padding=(10, 4), background="#6d28d9", foreground="white", borderwidth=0)
        style.map("DewAccent.TButton", background=[("active", "#5b21b6"), ("disabled", "#d4cbe7")], foreground=[("disabled", "#8c7ea5")])
        style.configure("DewSoft.TButton", font=("Microsoft YaHei UI", 9, "bold"), padding=(10, 4), background="#ffffff", foreground="#412268", borderwidth=0)
        style.map("DewSoft.TButton", background=[("active", "#f4eeff"), ("disabled", "#eeebf4")], foreground=[("disabled", "#9a8fa9")])
        style.configure("DewTree.Treeview", rowheight=28, font=("Microsoft YaHei UI", 9), background="#fbfdfe", fieldbackground="#fbfdfe")
        style.configure("DewTree.Treeview.Heading", font=("Microsoft YaHei UI", 9, "bold"))

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
    ) -> None:
        card = tk.Frame(parent, bg="#ffffff", highlightbackground=bg, highlightthickness=1, padx=1, pady=1)
        card.grid(row=row, column=column, sticky="nsew", padx=5, pady=5)
        inner = tk.Frame(card, bg=bg, padx=10, pady=7)
        inner.pack(fill="both", expand=True)
        tk.Frame(inner, bg="#ffffff", height=2).pack(fill="x", pady=(0, 6))
        tk.Label(inner, text=title, bg=bg, fg="#5b6473", font=("Microsoft YaHei UI", 9, "bold")).pack(anchor="w")
        tk.Label(inner, textvariable=textvariable, bg=bg, fg="#111827", font=("Microsoft YaHei UI", 11, "bold"), anchor="w", justify="left").pack(anchor="w", pady=(6, 0))

    def _build_ui(self) -> None:
        shell = tk.Frame(self.win, bg="#f5f1fb")
        shell.pack(fill="both", expand=True, padx=12, pady=12)
        shell.grid_columnconfigure(0, weight=1)
        shell.grid_rowconfigure(3, weight=1)

        hero = tk.Frame(shell, bg="#4c1d95", padx=18, pady=8)
        hero.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        hero_top = tk.Frame(hero, bg="#4c1d95")
        hero_top.pack(fill="x")
        tk.Label(hero_top, text="露点仪工作台", bg="#4c1d95", fg="white", font=("Microsoft YaHei UI", 15, "bold")).pack(side="left", anchor="w")
        tk.Label(hero_top, text="POLLING", bg="#f7f1ff", fg="#4c2583", padx=10, pady=3, font=("Consolas", 8, "bold")).pack(side="right", padx=(8, 0))
        tk.Label(hero_top, text="Dewpoint", bg="#efe7ff", fg="#4c2583", padx=10, pady=3, font=("Microsoft YaHei UI", 8, "bold")).pack(side="right")
        tk.Label(
            hero,
            text="连接、轮询和状态集中在上方，载荷解析与原始回包放到底部工作区。",
            bg="#4c1d95",
            fg="#e7d9ff",
            font=("Microsoft YaHei UI", 9),
        ).pack(anchor="w", pady=(2, 0))
        hero_strip = tk.Frame(hero, bg="#4c1d95")
        hero_strip.pack(fill="x", pady=(6, 0))
        for text, bg, fg in (
            ("SERIAL LINK", "#f7f1ff", "#4c2583"),
            ("POLL LOOP", "#efe7ff", "#6d28d9"),
            ("PAYLOAD VIEW", "#e8f2ff", "#1d4ed8"),
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

        quick_strip = tk.Frame(shell, bg="#f5f1fb")
        quick_strip.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        for idx in range(3):
            quick_strip.grid_columnconfigure(idx, weight=1)
        quick_cards = (
            ("设备角色", "露点 / 湿度采集", "#f7f1ff"),
            ("工作方式", "轮询 / 判稳 / 解析", "#efe7ff"),
            ("当前状态", self.status_var, "#faf7ff"),
        )
        for idx, (title, value, bg) in enumerate(quick_cards):
            card = tk.Frame(quick_strip, bg="#ffffff", highlightbackground="#e8def8", highlightthickness=1, padx=1, pady=1)
            card.grid(row=0, column=idx, sticky="ew", padx=(0, 8) if idx < 2 else 0)
            inner = tk.Frame(card, bg=bg, padx=10, pady=8)
            inner.pack(fill="both", expand=True)
            tk.Label(inner, text=title, bg=bg, fg="#6a5a82", font=("Microsoft YaHei UI", 8, "bold")).pack(anchor="w")
            if isinstance(value, tk.StringVar):
                value_label = tk.Label(
                    inner,
                    textvariable=value,
                    bg=bg,
                    fg="#4c2583",
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
                tk.Label(inner, text=value, bg=bg, fg="#4c2583", font=("Microsoft YaHei UI", 10, "bold")).pack(anchor="w", pady=(4, 0))

        body = tk.Frame(shell, bg="#f5f1fb")
        body.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        body.grid_columnconfigure(0, weight=7)
        body.grid_columnconfigure(1, weight=3)

        left_shell = tk.Frame(body, bg="#f5f1fb")
        left_shell.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        right_shell = tk.Frame(body, bg="#f5f1fb")
        right_shell.grid(row=0, column=1, sticky="nsew")

        left_shell_card = self._make_card_shell(left_shell, "#e8def8")
        left_shell_card.pack(fill="both", expand=True)
        left = ttk.LabelFrame(left_shell_card, text="连接与采集", style="DewCard.TLabelframe")
        left.pack(fill="both", expand=True)
        conn = ttk.Frame(left)
        conn.pack(fill="x", padx=10, pady=(10, 6))
        for col in range(8):
            conn.grid_columnconfigure(col, weight=1 if col in {1, 3, 5} else 0)
        ttk.Label(conn, text="COM").grid(row=0, column=0, sticky="w")
        self.port_var = tk.StringVar(value=self.cfg.get("port", "COM13"))
        self.port_entry = ttk.Entry(conn, textvariable=self.port_var)
        self.port_entry.grid(row=0, column=1, sticky="ew", padx=(6, 10))
        ttk.Label(conn, text="波特率").grid(row=0, column=2, sticky="w")
        self.baud_var = tk.StringVar(value=str(self.cfg.get("baud", 9600)))
        self.baud_entry = ttk.Entry(conn, textvariable=self.baud_var)
        self.baud_entry.grid(row=0, column=3, sticky="ew", padx=(6, 10))
        ttk.Label(conn, text="站号").grid(row=0, column=4, sticky="w")
        self.station_var = tk.StringVar(value=str(self.cfg.get("station", "001")))
        self.station_entry = ttk.Entry(conn, textvariable=self.station_var)
        self.station_entry.grid(row=0, column=5, sticky="ew", padx=(6, 10))
        self.connect_button = ttk.Button(conn, text="连接", command=self.connect, style="DewAccent.TButton")
        self.connect_button.grid(row=0, column=6, padx=(0, 6))
        self.disconnect_button = ttk.Button(conn, text="断开", command=self.disconnect, style="DewSoft.TButton")
        self.disconnect_button.grid(row=0, column=7)
        conn.grid_columnconfigure(6, minsize=96)
        conn.grid_columnconfigure(7, minsize=96)

        poll_box = ttk.LabelFrame(left, text="轮询与判稳", style="DewSub.TLabelframe")
        poll_box.pack(fill="x", padx=10, pady=6)
        for col in range(6):
            poll_box.grid_columnconfigure(col, weight=1)
        ttk.Label(poll_box, text="间隔 (s)").grid(row=0, column=0, sticky="w", padx=8, pady=8)
        self.interval_var = tk.StringVar(value="1.0")
        self.interval_entry = ttk.Entry(poll_box, textvariable=self.interval_var)
        self.interval_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=8)
        ttk.Label(poll_box, text="阈值 (C)").grid(row=0, column=2, sticky="w", padx=8, pady=8)
        self.stable_tol_var = tk.StringVar(value="0.2")
        self.stable_tol_entry = ttk.Entry(poll_box, textvariable=self.stable_tol_var)
        self.stable_tol_entry.grid(row=0, column=3, sticky="ew", padx=(0, 8), pady=8)
        ttk.Label(poll_box, text="窗口点数").grid(row=0, column=4, sticky="w", padx=8, pady=8)
        self.window_var = tk.StringVar(value="20")
        self.window_entry = ttk.Entry(poll_box, textvariable=self.window_var)
        self.window_entry.grid(row=0, column=5, sticky="ew", padx=(0, 8), pady=8)
        self.read_once_button = ttk.Button(poll_box, text="读取一次", command=self.read_once, style="DewSoft.TButton")
        self.read_once_button.grid(row=1, column=0, columnspan=2, sticky="ew", padx=8, pady=(0, 8))
        self.start_poll_button = ttk.Button(poll_box, text="开始轮询", command=self.start_poll, style="DewAccent.TButton")
        self.start_poll_button.grid(row=1, column=2, columnspan=2, sticky="ew", padx=8, pady=(0, 8))
        self.stop_poll_button = ttk.Button(poll_box, text="停止轮询", command=self.stop_poll, style="DewSoft.TButton")
        self.stop_poll_button.grid(row=1, column=4, columnspan=2, sticky="ew", padx=8, pady=(0, 8))
        for col in range(6):
            poll_box.grid_columnconfigure(col, minsize=94)

        monitor = ttk.LabelFrame(left, text="当前状态", style="DewSub.TLabelframe")
        monitor.pack(fill="x", padx=10, pady=(6, 10))
        self.status_label = tk.Label(
            monitor,
            textvariable=self.status_var,
            bg="#efe7ff",
            fg="#4c2583",
            padx=12,
            pady=6,
            font=("Microsoft YaHei UI", 10, "bold"),
            highlightthickness=1,
            highlightbackground="#efe7ff",
        )
        self.status_label.pack(fill="x", padx=10, pady=(10, 8))
        tk.Label(monitor, textvariable=self.snapshot_var, justify="left", anchor="nw", bg="#faf7ff", fg="#334155", padx=12, pady=9, font=("Consolas", 9), highlightbackground="#e8def8", highlightthickness=1).pack(fill="x", padx=10, pady=(0, 10))

        right_shell_card = self._make_card_shell(right_shell, "#e8def8")
        right_shell_card.pack(fill="both", expand=True)
        right = ttk.LabelFrame(right_shell_card, text="关键参数", style="DewCard.TLabelframe")
        right.pack(fill="both", expand=True)
        self.summary_vars = {
            "dewpoint": tk.StringVar(value="--"),
            "temp": tk.StringVar(value="--"),
            "rh": tk.StringVar(value="--"),
            "stable": tk.StringVar(value="--"),
            "flags": tk.StringVar(value="--"),
        }
        grid = tk.Frame(right, bg="#fbf9fe")
        grid.pack(fill="both", expand=True, padx=10, pady=10)
        for col in range(2):
            grid.grid_columnconfigure(col, weight=1)
            grid.grid_columnconfigure(col, minsize=154)
        cards = [
            ("露点", "dewpoint", "#efe7ff"),
            ("温度", "temp", "#e0f2fe"),
            ("湿度", "rh", "#dcfce7"),
            ("稳定性", "stable", "#fff2cf"),
            ("标志位", "flags", "#fee2e2"),
        ]
        for idx, (title, key, bg) in enumerate(cards):
            self._build_metric_card(grid, idx // 2, idx % 2, title, self.summary_vars[key], bg)

        tabs_shell = self._make_card_shell(shell, "#e8def8")
        tabs_shell.grid(row=3, column=0, sticky="nsew")
        tabs = ttk.Notebook(tabs_shell, style="DewNotebook.TNotebook")
        tabs.pack(fill="both", expand=True)
        table_tab = ttk.Frame(tabs)
        raw_tab = ttk.Frame(tabs)
        log_tab = ttk.Frame(tabs)
        tabs.add(table_tab, text="载荷解析")
        tabs.add(raw_tab, text="原始回包")
        tabs.add(log_tab, text="日志")

        table_tab.grid_columnconfigure(0, weight=1)
        table_tab.grid_rowconfigure(0, weight=1)
        self.table = ttk.Treeview(table_tab, columns=("idx", "desc", "value"), show="headings", style="DewTree.Treeview")
        self.table.heading("idx", text="序号")
        self.table.heading("desc", text="说明")
        self.table.heading("value", text="值")
        self.table.column("idx", width=80, anchor="center")
        self.table.column("desc", width=260, anchor="w")
        self.table.column("value", width=320, anchor="w")
        table_scroll = ttk.Scrollbar(table_tab, orient="vertical", command=self.table.yview)
        self.table.configure(yscrollcommand=table_scroll.set)
        self.table.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)
        table_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=10)

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
        polling = self._poll_job is not None
        self.connect_button.configure(state="disabled" if connected else "normal")
        self.disconnect_button.configure(state="normal" if connected else "disabled")
        serial_state = "disabled" if connected else "normal"
        for widget in (self.port_entry, self.baud_entry, self.station_entry):
            widget.configure(state=serial_state)
        poll_state = "disabled" if polling else "normal"
        for widget in (self.interval_entry, self.stable_tol_entry, self.window_entry):
            widget.configure(state=poll_state)
        self.read_once_button.configure(state="normal" if connected and not polling else "disabled")
        self.start_poll_button.configure(state="normal" if connected and not polling else "disabled")
        self.stop_poll_button.configure(state="normal" if polling else "disabled")

    def _ensure_dev(self) -> DewpointMeter:
        if not self.dev:
            raise RuntimeError("设备尚未连接")
        return self.dev

    @staticmethod
    def _fmt_float(val: Any, digits: int = 2) -> str:
        try:
            return f"{float(val):.{digits}f}"
        except Exception:
            return "--"

    def _update_stability(self, dewpoint: Optional[float]) -> str:
        if dewpoint is None:
            return "--"
        try:
            win_size = max(2, int(self.window_var.get()))
        except Exception:
            win_size = 20
        self._dp_history = deque(self._dp_history, maxlen=win_size)
        self._dp_history.append(float(dewpoint))
        if len(self._dp_history) < 2:
            return "采集不足"
        span = max(self._dp_history) - min(self._dp_history)
        try:
            tol = float(self.stable_tol_var.get())
        except Exception:
            tol = 0.2
        return f"稳定 (Δ={span:.3f}C)" if span <= tol else f"波动 (Δ={span:.3f}C)"

    def _set_raw(self, raw: str) -> None:
        self.raw_text.delete("1.0", "end")
        self.raw_text.insert("end", raw or "")

    def _render_data(self, data: Dict[str, Any]) -> None:
        for item in self.table.get_children():
            self.table.delete(item)
        payload = data.get("payload") or []
        for idx, token in enumerate(payload, start=1):
            self.table.insert("", "end", values=(idx, PAYLOAD_DESC.get(idx, f"Field {idx}"), token))

    def _update_summary(self, data: Dict[str, Any]) -> None:
        dp = data.get("dewpoint_c")
        temp = data.get("temp_c")
        rh = data.get("rh_pct")
        stable = self._update_stability(dp if isinstance(dp, (int, float)) else None)
        flags = data.get("flags")
        flags_str = ", ".join(str(v) for v in flags) if isinstance(flags, list) else "--"
        self.summary_vars["dewpoint"].set(f"{self._fmt_float(dp)} C")
        self.summary_vars["temp"].set(f"{self._fmt_float(temp)} C")
        self.summary_vars["rh"].set(f"{self._fmt_float(rh)} %RH")
        self.summary_vars["stable"].set(stable)
        self.summary_vars["flags"].set(flags_str)
        self.snapshot_var.set(
            f"Dewpoint: {self._fmt_float(dp)} C\n"
            f"Temp: {self._fmt_float(temp)} C\n"
            f"RH: {self._fmt_float(rh)} %RH\n"
            f"Stable: {stable}\n"
            f"Flags: {flags_str}"
        )

    def connect(self) -> None:
        if self.dev:
            self._log("设备已连接")
            return
        try:
            port = self.port_var.get().strip()
            baud = int(self.baud_var.get().strip())
            station = self.station_var.get().strip() or "001"
            self.dev = DewpointMeter(port, baudrate=baud, station=station, timeout=0.6)
            self.dev.open()
            self._set_status(f"已连接 {port} @ {baud} / station {station}", "ok")
            self._apply_button_states()
            self._log(f"连接成功 {port} @ {baud}, station={station}")
        except Exception as exc:
            self.dev = None
            self._set_status("连接失败", "error")
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
        self._set_status("未连接", "idle")
        self._apply_button_states()
        self._log("已断开")

    def read_once(self) -> None:
        try:
            dev = self._ensure_dev()
            data = dev.get_current(timeout_s=2.2, attempts=2)
            self._set_raw(data.get("raw", ""))
            self._render_data(data)
            self._update_summary(data)
            self._log(f"发送 {data.get('cmd', '')!r}")
            if not data.get("ok"):
                self._log("未收到完整数据，请检查 COM、站号、波特率和接线。")
            else:
                self._log(f"收到: {data.get('raw', '')}")
            lines = data.get("lines") or []
            self._log(f"串口回包行数: {len(lines)}")
            if lines:
                self._log(f"最后一行: {lines[-1]}")
            self._set_status("读取完成", "info")
        except Exception as exc:
            self._log(f"读取失败: {exc}")
            self._set_status("读取失败", "error")

    def _poll_once(self) -> None:
        try:
            self.read_once()
        except Exception as exc:
            self._log(f"轮询失败: {exc}")
            self.stop_poll()
            return
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
            self._set_status("轮询中", "info")
            self._apply_button_states()
            self._log("轮询开始")
        except Exception as exc:
            self._log(f"启动轮询失败: {exc}")

    def stop_poll(self) -> None:
        if self._poll_job is not None:
            self.win.after_cancel(self._poll_job)
            self._poll_job = None
            self._set_status("轮询停止", "warn")
            self._apply_button_states()
            self._log("轮询已停止")

    def _on_close(self) -> None:
        self.disconnect()
        self.win.destroy()
