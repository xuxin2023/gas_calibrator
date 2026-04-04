"""Manual valve control page."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, Iterable, Optional, Tuple

from ..devices import RelayController


class ValvePage:
    """Manual valve routing helper for commissioning and troubleshooting."""

    def __init__(self, parent: tk.Tk, cfg: Dict[str, Any], log_fn=None):
        self.parent = parent
        self.cfg = cfg or {}
        self.log_fn = log_fn
        self.relays: Dict[str, RelayController] = {}
        self._core_entries: list[Dict[str, Any]] = self._core_valve_entries()
        self._manual_entries: list[Dict[str, Any]] = self._managed_valve_entries()
        self._manual_open_set: set[int] = set(self._baseline_open())
        self._manual_status_labels: Dict[int, list[tk.Label]] = {}
        self._manual_buttons: list[ttk.Button] = []

        self.win = tk.Toplevel(parent)
        self.win.title("阀门控制工作台")
        self.win.geometry("1240x760")
        self.win.minsize(1080, 680)
        self.win.configure(bg="#edf3f8")
        self.win.protocol("WM_DELETE_WINDOW", self._on_close)

        self.status_var = tk.StringVar(value="未连接")
        self.route_hint_var = tk.StringVar(value="等待执行切换")
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
        style.configure("ValveCard.TLabelframe", background="#f9fbfd", bordercolor="#d6e2eb", relief="flat", borderwidth=1)
        style.configure("ValveCard.TLabelframe.Label", background="#edf3f8", foreground="#214565", font=("Microsoft YaHei UI", 10, "bold"))
        style.configure("ValveSub.TLabelframe", background="#f9fbfd", bordercolor="#e3ebf2", relief="flat", borderwidth=1)
        style.configure("ValveSub.TLabelframe.Label", background="#f9fbfd", foreground="#607080", font=("Microsoft YaHei UI", 9, "bold"))
        style.configure("ValveAccent.TButton", font=("Microsoft YaHei UI", 9, "bold"), padding=(10, 4), background="#214565", foreground="white", borderwidth=0)
        style.map("ValveAccent.TButton", background=[("active", "#163750"), ("disabled", "#d4dae2")], foreground=[("disabled", "#8b96a3")])
        style.configure("ValveSoft.TButton", font=("Microsoft YaHei UI", 9, "bold"), padding=(10, 4), background="#ffffff", foreground="#1f2937", borderwidth=0)
        style.map("ValveSoft.TButton", background=[("active", "#eef5fb"), ("disabled", "#eceff3")], foreground=[("disabled", "#97a2ad")])
        style.configure("ValveWarn.TButton", font=("Microsoft YaHei UI", 9, "bold"), padding=(10, 4), background="#9f1239", foreground="white", borderwidth=0)
        style.map("ValveWarn.TButton", background=[("active", "#881337"), ("disabled", "#e2cfd6")], foreground=[("disabled", "#9d7c86")])

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

    @staticmethod
    def _as_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None

    def _build_ui(self) -> None:
        dcfg = self.cfg.get("devices", {})
        relay_cfg = dcfg.get("relay", {})
        relay8_cfg = dcfg.get("relay_8", {})

        shell = tk.Frame(self.win, bg="#edf3f8")
        shell.pack(fill="both", expand=True, padx=12, pady=12)
        shell.grid_columnconfigure(0, weight=1)
        shell.grid_rowconfigure(3, weight=1)

        hero = tk.Frame(shell, bg="#214565", padx=18, pady=8)
        hero.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        hero_top = tk.Frame(hero, bg="#214565")
        hero_top.pack(fill="x")
        tk.Label(hero_top, text="阀门控制工作台", bg="#214565", fg="white", font=("Microsoft YaHei UI", 15, "bold")).pack(side="left", anchor="w")
        tk.Label(hero_top, text="RELAY", bg="#f2f8ff", fg="#214565", padx=10, pady=3, font=("Consolas", 8, "bold")).pack(side="right", padx=(8, 0))
        tk.Label(hero_top, text="Routing", bg="#e2f0ff", fg="#214565", padx=10, pady=3, font=("Microsoft YaHei UI", 8, "bold")).pack(side="right")
        tk.Label(
            hero,
            text="连接与状态在上方，快速切换与操作日志分区展示，路由信息单独收口。",
            bg="#214565",
            fg="#dceaf7",
            font=("Microsoft YaHei UI", 9),
        ).pack(anchor="w", pady=(2, 0))
        hero_strip = tk.Frame(hero, bg="#214565")
        hero_strip.pack(fill="x", pady=(6, 0))
        for text, bg, fg in (
            ("RELAY LINK", "#f2f8ff", "#214565"),
            ("ROUTE SWITCH", "#e2f0ff", "#1d4f91"),
            ("FLOW HOLD", "#eaf7ff", "#155e75"),
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

        quick_strip = tk.Frame(shell, bg="#edf3f8")
        quick_strip.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        for idx in range(3):
            quick_strip.grid_columnconfigure(idx, weight=1)
        quick_cards = (
            ("控制核心", "继电器 / 路由", "#f2f8ff"),
            ("操作方式", "切换 / 复位 / 日志", "#eaf7ff"),
            ("连接状态", self.status_var, "#f7fbff"),
        )
        for idx, (title, value, bg) in enumerate(quick_cards):
            card = tk.Frame(quick_strip, bg="#ffffff", highlightbackground="#dbe8f1", highlightthickness=1, padx=1, pady=1)
            card.grid(row=0, column=idx, sticky="ew", padx=(0, 8) if idx < 2 else 0)
            inner = tk.Frame(card, bg=bg, padx=10, pady=8)
            inner.pack(fill="both", expand=True)
            tk.Label(inner, text=title, bg=bg, fg="#607080", font=("Microsoft YaHei UI", 8, "bold")).pack(anchor="w")
            if isinstance(value, tk.StringVar):
                value_label = tk.Label(
                    inner,
                    textvariable=value,
                    bg=bg,
                    fg="#214565",
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
                tk.Label(inner, text=value, bg=bg, fg="#214565", font=("Microsoft YaHei UI", 10, "bold")).pack(anchor="w", pady=(4, 0))

        top_body = tk.Frame(shell, bg="#edf3f8")
        top_body.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        top_body.grid_columnconfigure(0, weight=7)
        top_body.grid_columnconfigure(1, weight=3)

        left_shell = tk.Frame(top_body, bg="#edf3f8")
        left_shell.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        right_shell = tk.Frame(top_body, bg="#edf3f8")
        right_shell.grid(row=0, column=1, sticky="nsew")

        left_shell_card = self._make_card_shell(left_shell, "#dbe8f1")
        left_shell_card.pack(fill="both", expand=True)
        config_card = ttk.LabelFrame(left_shell_card, text="继电器连接", style="ValveCard.TLabelframe")
        config_card.pack(fill="both", expand=True)

        top = ttk.Frame(config_card)
        top.pack(fill="x", padx=10, pady=10)
        for col in range(8):
            top.grid_columnconfigure(col, weight=1)
        ttk.Label(top, text="16路 COM").grid(row=0, column=0, sticky="w")
        self.relay_port_var = tk.StringVar(value=str(relay_cfg.get("port", "COM28")))
        self.relay_port_entry = ttk.Entry(top, textvariable=self.relay_port_var)
        self.relay_port_entry.grid(row=0, column=1, sticky="ew", padx=(6, 10))
        ttk.Label(top, text="波特率").grid(row=0, column=2, sticky="w")
        self.relay_baud_var = tk.StringVar(value=str(relay_cfg.get("baud", 38400)))
        self.relay_baud_entry = ttk.Entry(top, textvariable=self.relay_baud_var)
        self.relay_baud_entry.grid(row=0, column=3, sticky="ew", padx=(6, 10))
        ttk.Label(top, text="地址").grid(row=0, column=4, sticky="w")
        self.relay_addr_var = tk.StringVar(value=str(relay_cfg.get("addr", 1)))
        self.relay_addr_entry = ttk.Entry(top, textvariable=self.relay_addr_var)
        self.relay_addr_entry.grid(row=0, column=5, sticky="ew", padx=(6, 10))
        self.connect_button = ttk.Button(top, text="连接", command=self.connect, style="ValveAccent.TButton")
        self.connect_button.grid(row=0, column=6, padx=(0, 6))
        self.disconnect_button = ttk.Button(top, text="断开", command=self.disconnect, style="ValveSoft.TButton")
        self.disconnect_button.grid(row=0, column=7)
        top.grid_columnconfigure(6, minsize=96)
        top.grid_columnconfigure(7, minsize=96)

        second = ttk.Frame(config_card)
        second.pack(fill="x", padx=10, pady=(0, 10))
        for col in range(8):
            second.grid_columnconfigure(col, weight=1)
        ttk.Label(second, text="8路 COM").grid(row=0, column=0, sticky="w")
        self.relay8_port_var = tk.StringVar(value=str(relay8_cfg.get("port", "COM29")))
        self.relay8_port_entry = ttk.Entry(second, textvariable=self.relay8_port_var)
        self.relay8_port_entry.grid(row=0, column=1, sticky="ew", padx=(6, 10))
        ttk.Label(second, text="波特率").grid(row=0, column=2, sticky="w")
        self.relay8_baud_var = tk.StringVar(value=str(relay8_cfg.get("baud", 38400)))
        self.relay8_baud_entry = ttk.Entry(second, textvariable=self.relay8_baud_var)
        self.relay8_baud_entry.grid(row=0, column=3, sticky="ew", padx=(6, 10))
        ttk.Label(second, text="地址").grid(row=0, column=4, sticky="w")
        self.relay8_addr_var = tk.StringVar(value=str(relay8_cfg.get("addr", 1)))
        self.relay8_addr_entry = ttk.Entry(second, textvariable=self.relay8_addr_var)
        self.relay8_addr_entry.grid(row=0, column=5, sticky="ew", padx=(6, 10))
        self.status_label = tk.Label(
            second,
            textvariable=self.status_var,
            bg="#e2f0ff",
            fg="#214565",
            width=16,
            anchor="center",
            padx=12,
            pady=5,
            font=("Microsoft YaHei UI", 10, "bold"),
            highlightthickness=1,
            highlightbackground="#e2f0ff",
        )
        self.status_label.grid(row=0, column=6, columnspan=2, sticky="e")
        second.grid_columnconfigure(6, minsize=96)
        second.grid_columnconfigure(7, minsize=96)

        quick = ttk.LabelFrame(config_card, text="切换说明", style="ValveSub.TLabelframe")
        quick.pack(fill="x", padx=10, pady=(0, 10))
        tk.Label(
            quick,
            text="完成连接后再执行切换。当前旁路阀规则为：基线关闭、通水打开、通气关闭、封压后关闭。",
            justify="left",
            anchor="nw",
            bg="#f7fbff",
            fg="#334155",
            padx=12,
            pady=8,
            wraplength=760,
            font=("Microsoft YaHei UI", 9),
        ).pack(fill="x", padx=6, pady=4)

        right_shell_card = self._make_card_shell(right_shell, "#dbe8f1")
        right_shell_card.pack(fill="both", expand=True)
        route_card = ttk.LabelFrame(right_shell_card, text="当前路由", style="ValveCard.TLabelframe")
        route_card.pack(fill="both", expand=True)
        tk.Label(
            route_card,
            textvariable=self.route_hint_var,
            justify="left",
            anchor="nw",
            bg="#f7fbff",
            fg="#334155",
            padx=14,
            pady=14,
            font=("Consolas", 10),
            wraplength=320,
        ).pack(fill="both", expand=True, padx=10, pady=10)

        lower_shell = tk.Frame(shell, bg="#edf3f8")
        lower_shell.grid(row=3, column=0, sticky="nsew")
        lower_shell.grid_columnconfigure(0, weight=1)
        lower_shell.grid_rowconfigure(1, weight=1)

        route_shell = self._make_card_shell(lower_shell, "#dbe8f1")
        route_shell.grid(row=0, column=0, sticky="ew")
        route_shell.grid_columnconfigure(0, weight=1)
        route_panel = ttk.LabelFrame(route_shell, text="快速切换", style="ValveCard.TLabelframe")
        route_panel.grid(row=0, column=0, sticky="ew")
        route_panel.grid_columnconfigure(0, weight=1)
        route_panel.grid_columnconfigure(1, weight=1)

        co2a = ttk.LabelFrame(route_panel, text="CO2 组 A", style="ValveSub.TLabelframe")
        co2a.grid(row=0, column=0, sticky="nsew", padx=(10, 6), pady=10)
        for col in range(3):
            co2a.grid_columnconfigure(col, weight=1)
        self.group_a_buttons = []
        for idx, ppm in enumerate((0, 200, 400, 600, 800, 1000)):
            btn = ttk.Button(co2a, text=f"{ppm} ppm", command=lambda p=ppm: self.set_co2_group("A", p), width=10, style="ValveSoft.TButton")
            btn.grid(row=idx // 3, column=idx % 3, padx=6, pady=6, sticky="ew")
            self.group_a_buttons.append(btn)

        co2b = ttk.LabelFrame(route_panel, text="CO2 组 B", style="ValveSub.TLabelframe")
        co2b.grid(row=0, column=1, sticky="nsew", padx=(6, 10), pady=10)
        for col in range(3):
            co2b.grid_columnconfigure(col, weight=1)
        self.group_b_buttons = []
        for idx, ppm in enumerate((0, 100, 300, 500, 700, 900)):
            btn = ttk.Button(co2b, text=f"{ppm} ppm", command=lambda p=ppm: self.set_co2_group("B", p), width=10, style="ValveSoft.TButton")
            btn.grid(row=idx // 3, column=idx % 3, padx=6, pady=6, sticky="ew")
            self.group_b_buttons.append(btn)

        h2o = ttk.LabelFrame(route_panel, text="水路与总关闭", style="ValveSub.TLabelframe")
        h2o.grid(row=1, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))
        self.h2o_on_button = ttk.Button(h2o, text="打开水路", command=self.set_h2o_on, style="ValveAccent.TButton")
        self.h2o_on_button.pack(side="left", padx=8, pady=8)
        self.all_close_button = ttk.Button(h2o, text="恢复基线", command=self.all_close, style="ValveWarn.TButton")
        self.all_close_button.pack(side="left", padx=8, pady=8)
        ttk.Label(h2o, text="建议先完成气路切换，再决定是否切到水路。").pack(side="left", padx=12)

        core_manual = ttk.LabelFrame(route_panel, text="核心阀门独立开关", style="ValveSub.TLabelframe")
        core_manual.grid(row=2, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))
        self._build_core_controls(core_manual)

        manual = ttk.LabelFrame(route_panel, text="全部阀门单阀控制", style="ValveSub.TLabelframe")
        manual.grid(row=3, column=0, columnspan=2, sticky="nsew", padx=10, pady=(0, 10))
        manual.grid_columnconfigure(0, weight=1)
        manual.grid_rowconfigure(1, weight=1)

        manual_toolbar = ttk.Frame(manual)
        manual_toolbar.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 0))
        manual_toolbar.grid_columnconfigure(0, weight=1)
        ttk.Label(
            manual_toolbar,
            text="每个逻辑阀门都可以独立打开或关闭。",
        ).grid(row=0, column=0, sticky="w")
        self.manual_refresh_button = ttk.Button(
            manual_toolbar,
            text="刷新状态",
            command=self.refresh_manual_states,
            style="ValveSoft.TButton",
        )
        self.manual_refresh_button.grid(row=0, column=1, sticky="e")

        manual_body = tk.Frame(manual, bg="#f7fbff")
        manual_body.grid(row=1, column=0, sticky="nsew", padx=8, pady=8)
        manual_body.grid_columnconfigure(0, weight=1)
        manual_body.grid_rowconfigure(0, weight=1)
        self.manual_canvas = tk.Canvas(
            manual_body,
            bg="#f7fbff",
            height=220,
            highlightthickness=0,
            bd=0,
        )
        manual_scroll = ttk.Scrollbar(manual_body, orient="vertical", command=self.manual_canvas.yview)
        self.manual_canvas.configure(yscrollcommand=manual_scroll.set)
        self.manual_canvas.grid(row=0, column=0, sticky="nsew")
        manual_scroll.grid(row=0, column=1, sticky="ns")
        self.manual_list_frame = tk.Frame(self.manual_canvas, bg="#f7fbff")
        self._manual_canvas_window = self.manual_canvas.create_window(
            (0, 0),
            window=self.manual_list_frame,
            anchor="nw",
        )
        self.manual_list_frame.bind(
            "<Configure>",
            lambda _event: self.manual_canvas.configure(scrollregion=self.manual_canvas.bbox("all")),
        )
        self.manual_canvas.bind(
            "<Configure>",
            lambda event: self.manual_canvas.itemconfigure(self._manual_canvas_window, width=event.width),
        )
        self._build_manual_controls(self.manual_list_frame)

        log_shell = self._make_card_shell(lower_shell, "#dbe8f1")
        log_shell.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        log_shell.grid_columnconfigure(0, weight=1)
        log_shell.grid_rowconfigure(0, weight=1)
        log_box = ttk.LabelFrame(log_shell, text="操作日志", style="ValveCard.TLabelframe")
        log_box.grid(row=0, column=0, sticky="nsew")
        log_box.grid_columnconfigure(0, weight=1)
        log_box.grid_rowconfigure(0, weight=1)
        self.log_text = tk.Text(log_box, bg="#f7fafc", fg="#122131", insertbackground="#122131", font=("Consolas", 10), wrap="word")
        log_scroll = ttk.Scrollbar(log_box, orient="vertical", command=self.log_text.yview)
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
        connected = bool(self.relays)
        self.connect_button.configure(state="disabled" if connected else "normal")
        self.disconnect_button.configure(state="normal" if connected else "disabled")
        entry_state = "disabled" if connected else "normal"
        for widget in (
            self.relay_port_entry,
            self.relay_baud_entry,
            self.relay_addr_entry,
            self.relay8_port_entry,
            self.relay8_baud_entry,
            self.relay8_addr_entry,
        ):
            widget.configure(state=entry_state)
        route_state = "normal" if connected else "disabled"
        for button in self.group_a_buttons + self.group_b_buttons + [self.h2o_on_button, self.all_close_button, self.manual_refresh_button] + self._manual_buttons:
            button.configure(state=route_state)
        self._refresh_manual_state(prefer_device=connected, log_errors=False)

    def _core_valve_entries(self) -> list[Dict[str, Any]]:
        valves_cfg = self.cfg.get("valves", {})
        entries: list[Dict[str, Any]] = []
        for key, label in (
            ("h2o_path", "总阀门"),
            ("flow_switch", "旁路阀"),
            ("hold", "水路阀"),
            ("gas_main", "总气路阀"),
            ("co2_path", "A组总气路阀"),
            ("co2_path_group2", "B组总气路阀"),
        ):
            valve = self._as_int(valves_cfg.get(key))
            if valve is None:
                continue
            relay_name, channel = self._resolve_target(valve)
            entries.append(
                {
                    "key": key,
                    "valve": valve,
                    "label": label,
                    "relay_name": relay_name,
                    "channel": channel,
                }
            )
        return entries

    def _managed_valve_entries(self) -> list[Dict[str, Any]]:
        valves_cfg = self.cfg.get("valves", {})
        entries: list[Dict[str, Any]] = []
        seen: set[int] = set()

        def add_entry(raw_value: Any, label: str) -> None:
            valve = self._as_int(raw_value)
            if valve is None or valve in seen:
                return
            relay_name, channel = self._resolve_target(valve)
            entries.append(
                {
                    "valve": valve,
                    "label": label,
                    "relay_name": relay_name,
                    "channel": channel,
                }
            )
            seen.add(valve)

        for entry in self._core_entries:
            add_entry(entry["valve"], entry["label"])

        for map_name, prefix in (("co2_map", "CO2-A"), ("co2_map_group2", "CO2-B")):
            one_map = valves_cfg.get(map_name, {})
            if not isinstance(one_map, dict):
                continue
            sortable: list[tuple[int, Any]] = []
            for ppm_text, raw_valve in one_map.items():
                ppm_value = self._as_int(ppm_text)
                sortable.append((ppm_value if ppm_value is not None else 10**9, raw_valve, ppm_text))
            for _, raw_valve, ppm_text in sorted(sortable, key=lambda item: (item[0], str(item[2]))):
                add_entry(raw_valve, f"{prefix} {ppm_text} ppm")

        for valve in self._managed_valves():
            add_entry(valve, f"Valve {valve}")

        return entries

    def _register_status_label(self, valve: int, widget: tk.Label) -> None:
        labels = self._manual_status_labels.setdefault(valve, [])
        labels.append(widget)

    def _build_core_controls(self, parent: tk.Widget) -> None:
        grid = tk.Frame(parent, bg="#f9fbfd")
        grid.pack(fill="x", padx=8, pady=8)
        for col in range(3):
            grid.grid_columnconfigure(col, weight=1)
        for idx, entry in enumerate(self._core_entries):
            card = tk.Frame(grid, bg="#f7fbff", highlightbackground="#d8e5ef", highlightthickness=1, padx=10, pady=8)
            card.grid(row=idx // 3, column=idx % 3, sticky="ew", padx=6, pady=6)
            card.grid_columnconfigure(0, weight=1)
            tk.Label(
                card,
                text=entry["label"],
                bg="#f7fbff",
                fg="#122131",
                anchor="w",
                font=("Microsoft YaHei UI", 9, "bold"),
            ).grid(row=0, column=0, sticky="w")
            tk.Label(
                card,
                text=f"L{entry['valve']}  {entry['relay_name']}/CH{entry['channel']}",
                bg="#f7fbff",
                fg="#607080",
                anchor="w",
                font=("Consolas", 8),
            ).grid(row=1, column=0, sticky="w", pady=(2, 6))
            status = tk.Label(
                card,
                text="--",
                bg="#e5edf5",
                fg="#475569",
                padx=8,
                pady=4,
                font=("Microsoft YaHei UI", 8, "bold"),
            )
            status.grid(row=0, column=1, rowspan=2, sticky="e", padx=(8, 0))
            self._register_status_label(entry["valve"], status)

            btn_row = tk.Frame(card, bg="#f7fbff")
            btn_row.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(2, 0))
            btn_row.grid_columnconfigure(0, weight=1)
            btn_row.grid_columnconfigure(1, weight=1)
            open_btn = ttk.Button(
                btn_row,
                text="打开",
                command=lambda valve=entry["valve"]: self.set_manual_valve_state(valve, True),
                style="ValveAccent.TButton",
            )
            open_btn.grid(row=0, column=0, sticky="ew", padx=(0, 4))
            close_btn = ttk.Button(
                btn_row,
                text="关闭",
                command=lambda valve=entry["valve"]: self.set_manual_valve_state(valve, False),
                style="ValveSoft.TButton",
            )
            close_btn.grid(row=0, column=1, sticky="ew", padx=(4, 0))
            self._manual_buttons.extend([open_btn, close_btn])

    def _build_manual_controls(self, parent: tk.Widget) -> None:
        for idx, entry in enumerate(self._manual_entries):
            row = tk.Frame(parent, bg="#f7fbff", highlightbackground="#d8e5ef", highlightthickness=1, padx=8, pady=6)
            row.grid(row=idx, column=0, sticky="ew", pady=(0, 6))
            row.grid_columnconfigure(0, weight=3)
            row.grid_columnconfigure(1, weight=2)
            row.grid_columnconfigure(2, weight=1)

            tk.Label(
                row,
                text=f"{entry['label']}  [L{entry['valve']}]",
                bg="#f7fbff",
                fg="#122131",
                anchor="w",
                font=("Microsoft YaHei UI", 9, "bold"),
            ).grid(row=0, column=0, sticky="w")
            tk.Label(
                row,
                text=f"{entry['relay_name']} / CH{entry['channel']}",
                bg="#f7fbff",
                fg="#607080",
                anchor="w",
                font=("Consolas", 9),
            ).grid(row=0, column=1, sticky="w", padx=(10, 0))

            status = tk.Label(
                row,
                text="--",
                bg="#e5edf5",
                fg="#475569",
                padx=8,
                pady=4,
                font=("Microsoft YaHei UI", 8, "bold"),
            )
            status.grid(row=0, column=2, sticky="e", padx=(8, 0))
            self._register_status_label(entry["valve"], status)

            btn_box = tk.Frame(row, bg="#f7fbff")
            btn_box.grid(row=0, column=3, sticky="e", padx=(12, 0))
            open_btn = ttk.Button(
                btn_box,
                text="打开",
                command=lambda valve=entry["valve"]: self.set_manual_valve_state(valve, True),
                style="ValveAccent.TButton",
                width=8,
            )
            open_btn.pack(side="left", padx=(0, 6))
            close_btn = ttk.Button(
                btn_box,
                text="关闭",
                command=lambda valve=entry["valve"]: self.set_manual_valve_state(valve, False),
                style="ValveSoft.TButton",
                width=8,
            )
            close_btn.pack(side="left")
            self._manual_buttons.extend([open_btn, close_btn])

    def _managed_valves(self) -> list[int]:
        valves_cfg = self.cfg.get("valves", {})
        managed = set()
        for key in ("co2_path", "co2_path_group2", "gas_main", "h2o_path", "hold", "flow_switch"):
            iv = self._as_int(valves_cfg.get(key))
            if iv is not None:
                managed.add(iv)
        for key in ("co2_map", "co2_map_group2"):
            one_map = valves_cfg.get(key, {})
            if isinstance(one_map, dict):
                for val in one_map.values():
                    iv = self._as_int(val)
                    if iv is not None:
                        managed.add(iv)
        return sorted(managed)

    def _apply_open(self, logical_open: Iterable[int]) -> None:
        if not self.relays:
            raise RuntimeError("Relays not connected")
        open_set = {int(v) for v in logical_open if self._as_int(v) is not None}
        states: Dict[Tuple[str, int], bool] = {}
        for valve in self._managed_valves():
            relay_name, channel = self._resolve_target(valve)
            key = (relay_name, channel)
            desired_state = valve in open_set
            states[key] = states.get(key, False) or desired_state
        for (relay_name, channel), is_open in sorted(states.items()):
            relay = self.relays.get(relay_name)
            if relay:
                relay.set_valve(channel, is_open)

    def _baseline_open(self) -> list[int]:
        return []

    def _read_logical_states(self) -> Dict[int, Optional[bool]]:
        states: Dict[int, Optional[bool]] = {}
        raw_by_relay: Dict[str, list[bool]] = {}
        for relay_name, relay in self.relays.items():
            count = 8 if relay_name == "relay_8" else 16
            raw = relay.read_coils(0, count)
            raw_by_relay[relay_name] = [bool(item) for item in list(raw or [])[:count]]
        for entry in self._manual_entries:
            bits = raw_by_relay.get(entry["relay_name"])
            channel = int(entry["channel"])
            raw_state: Optional[bool] = None
            if bits is not None and 1 <= channel <= len(bits):
                raw_state = bool(bits[channel - 1])
            if raw_state is None:
                states[entry["valve"]] = None
            else:
                states[entry["valve"]] = raw_state
        return states

    def _set_manual_status_chip(self, valve: int, state: Optional[bool]) -> None:
        labels = self._manual_status_labels.get(valve)
        if not labels:
            return
        for label in labels:
            if state is True:
                label.configure(text="开启", bg="#dcfce7", fg="#166534")
            elif state is False:
                label.configure(text="关闭", bg="#e5edf5", fg="#475569")
            else:
                label.configure(text="--", bg="#fef3c7", fg="#92400e")

    def _refresh_manual_state(self, *, prefer_device: bool, log_errors: bool) -> None:
        if prefer_device and self.relays:
            try:
                states = self._read_logical_states()
                self._manual_open_set = {valve for valve, state in states.items() if state is True}
            except Exception as exc:
                if log_errors:
                    self._log(f"Refresh valve states failed: {exc}")
                states = {entry["valve"]: (entry["valve"] in self._manual_open_set if self.relays else None) for entry in self._manual_entries}
        else:
            states = {entry["valve"]: (entry["valve"] in self._manual_open_set if self.relays else None) for entry in self._manual_entries}
        for valve, state in states.items():
            self._set_manual_status_chip(valve, state)

    def _manual_summary(self) -> str:
        open_labels = [entry["label"] for entry in self._manual_entries if entry["valve"] in self._manual_open_set]
        if not open_labels:
            return "单阀控制：当前全部关闭"
        return "单阀控制已开启：\n" + "\n".join(open_labels)

    def _resolve_target(self, logical_valve: int) -> Tuple[str, int]:
        valves_cfg = self.cfg.get("valves", {})
        relay_map = valves_cfg.get("relay_map", {})
        entry = relay_map.get(str(logical_valve)) if isinstance(relay_map, dict) else None
        if isinstance(entry, dict):
            relay_name = str(entry.get("device", "relay") or "relay")
            channel = self._as_int(entry.get("channel"))
            if channel and channel > 0:
                return relay_name, channel
        return "relay", logical_valve

    def connect(self) -> None:
        if self.relays:
            self._log("继电器已连接")
            return
        connected: Dict[str, RelayController] = {}
        try:
            relay = RelayController(self.relay_port_var.get().strip(), int(self.relay_baud_var.get().strip()), addr=int(float(self.relay_addr_var.get().strip())))
            relay.open()
            connected["relay"] = relay
            relay8 = RelayController(self.relay8_port_var.get().strip(), int(self.relay8_baud_var.get().strip()), addr=int(float(self.relay8_addr_var.get().strip())))
            relay8.open()
            connected["relay_8"] = relay8
        except Exception as exc:
            for dev in connected.values():
                try:
                    dev.close()
                except Exception:
                    pass
            self._set_status("连接失败", "error")
            self._apply_button_states()
            self._log(f"连接失败: {exc}")
            return
        self.relays = connected
        self._set_status("16路 + 8路已连接", "ok")
        self._apply_button_states()
        self.refresh_manual_states()
        self._log("继电器连接成功")

    def disconnect(self) -> None:
        for dev in self.relays.values():
            try:
                dev.close()
            except Exception:
                pass
        self.relays = {}
        self._manual_open_set = set(self._baseline_open())
        self._set_status("未连接", "idle")
        self._apply_button_states()
        self._log("已断开")

    def _apply_named_state(self, logical_open: Iterable[int], *, route_hint: str, status_text: str, status_level: str, log_text: str) -> None:
        open_set = {int(v) for v in logical_open if self._as_int(v) is not None}
        self._apply_open(open_set)
        self._manual_open_set = open_set
        self._refresh_manual_state(prefer_device=True, log_errors=True)
        self.route_hint_var.set(route_hint)
        self._set_status(status_text, status_level)
        self._log(log_text)

    def set_manual_valve_state(self, valve: int, is_open: bool) -> None:
        label = next((entry["label"] for entry in self._manual_entries if entry["valve"] == valve), f"Valve {valve}")
        open_set = set(self._manual_open_set)
        if is_open:
            open_set.add(int(valve))
        else:
            open_set.discard(int(valve))
        try:
            self._apply_open(open_set)
            self._manual_open_set = open_set
            self._refresh_manual_state(prefer_device=True, log_errors=True)
            action = "打开" if is_open else "关闭"
            self.route_hint_var.set(self._manual_summary())
            self._set_status(f"{label} {action}", "info")
            self._log(f"{label} 已{action}")
        except Exception as exc:
            self._log(f"{label} 控制失败: {exc}")

    def refresh_manual_states(self) -> None:
        self._refresh_manual_state(prefer_device=True, log_errors=True)
        self.route_hint_var.set(self._manual_summary())

    def _co2_map(self, group: str) -> Dict[str, Any]:
        valves_cfg = self.cfg.get("valves", {})
        return valves_cfg.get("co2_map_group2", {}) if group.upper() == "B" else valves_cfg.get("co2_map", {})

    def _co2_path(self, group: str) -> Optional[int]:
        valves_cfg = self.cfg.get("valves", {})
        if group.upper() == "B":
            return self._as_int(valves_cfg.get("co2_path_group2", valves_cfg.get("co2_path")))
        return self._as_int(valves_cfg.get("co2_path"))

    def set_co2_group(self, group: str, ppm: int) -> None:
        one_map = self._co2_map(group) or {}
        source = self._as_int(one_map.get(str(int(ppm))))
        if source is None:
            self._log(f"CO2 组 {group} 的 {ppm} ppm 未配置")
            return
        valves_cfg = self.cfg.get("valves", {})
        path = self._co2_path(group)
        total = self._as_int(valves_cfg.get("h2o_path"))
        gas_main = self._as_int(valves_cfg.get("gas_main"))
        flow_switch = self._as_int(valves_cfg.get("flow_switch"))
        open_list = []
        if total is not None:
            open_list.append(total)
        if gas_main is not None:
            open_list.append(gas_main)
        if path is not None:
            open_list.append(path)
        open_list.append(source)
        try:
            self._apply_named_state(
                open_list,
                route_hint=f"CO2 组 {group}\n目标浓度: {ppm} ppm\n总阀: 开\n组阀: {path}\n源阀: {source}\n旁路: 关",
                status_text=f"组 {group} / {ppm} ppm",
                status_level="info",
                log_text=f"CO2 组 {group} 已切换到 {ppm} ppm",
            )
        except Exception as exc:
            self._log(f"CO2 切换失败: {exc}")

    def set_h2o_on(self) -> None:
        valves_cfg = self.cfg.get("valves", {})
        open_list = []
        for key in ("h2o_path", "hold", "flow_switch"):
            valve = self._as_int(valves_cfg.get(key))
            if valve is not None:
                open_list.append(valve)
        try:
            self._apply_named_state(
                open_list,
                route_hint=f"水路已打开\n开启阀门: {open_list}\n旁路: 开",
                status_text="水路已打开",
                status_level="info",
                log_text="水路已打开",
            )
        except Exception as exc:
            self._log(f"水路切换失败: {exc}")

    def all_close(self) -> None:
        try:
            self._apply_named_state(
                self._baseline_open(),
                route_hint="已恢复基线\n所有受管阀门恢复到关闭状态",
                status_text="恢复基线",
                status_level="warn",
                log_text="所有通路已恢复到基线状态",
            )
        except Exception as exc:
            self._log(f"全部关闭失败: {exc}")

    def _on_close(self) -> None:
        self.disconnect()
        self.win.destroy()
