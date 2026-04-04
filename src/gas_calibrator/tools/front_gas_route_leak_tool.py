"""Standalone UI launcher for the front gas-route leak diagnostic."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Dict, Iterable, List, Optional, Sequence


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_LAUNCHER = REPO_ROOT / "run_gas_route_ratio_leak_check.py"
DEFAULT_CONFIG_PATH = "configs/default_config.json"
DEFAULT_GAS_PPM = "0,200,400,600,800,1000"


@dataclass(frozen=True)
class FrontGasRouteLeakToolState:
    config_path: str = DEFAULT_CONFIG_PATH
    analyzer: str = "ga02"
    gas_ppm: str = DEFAULT_GAS_PPM
    co2_group: str = "A"
    point_duration_s: str = "180"
    stable_window_s: str = "5"
    tail_window_s: str = "10"
    sample_poll_s: str = "0.1"
    print_every_s: str = "1.0"
    source_close_first_delay_s: str = "1"
    configure_analyzer_stream: bool = False
    allow_live_hardware: bool = False


def build_cli_arguments(
    state: FrontGasRouteLeakToolState,
    *,
    run_id: Optional[str] = None,
) -> List[str]:
    argv = [
        "--config",
        state.config_path,
        "--analyzer",
        state.analyzer,
        "--gas-ppm",
        state.gas_ppm,
        "--co2-group",
        state.co2_group,
        "--point-duration-s",
        state.point_duration_s,
        "--stable-window-s",
        state.stable_window_s,
        "--tail-window-s",
        state.tail_window_s,
        "--sample-poll-s",
        state.sample_poll_s,
        "--print-every-s",
        state.print_every_s,
        "--source-close-first-delay-s",
        state.source_close_first_delay_s,
    ]
    if run_id:
        argv.extend(["--run-id", run_id])
    if state.allow_live_hardware:
        argv.append("--allow-live-hardware")
    if state.configure_analyzer_stream:
        argv.append("--configure-analyzer-stream")
    return argv


def build_cli_command(
    state: FrontGasRouteLeakToolState,
    *,
    run_id: Optional[str] = None,
) -> List[str]:
    return [sys.executable, str(CLI_LAUNCHER), *build_cli_arguments(state, run_id=run_id)]


class FrontGasRouteLeakToolApp:
    """Small independent UI for front gas-route leak diagnostics."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("前端气路 CO2 比值查漏")
        self.root.geometry("1120x820")
        self.root.minsize(980, 720)
        self.root.configure(bg="#eef4fb")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self.config_path_var = tk.StringVar(value=DEFAULT_CONFIG_PATH)
        self.analyzer_var = tk.StringVar(value="ga02")
        self.gas_ppm_var = tk.StringVar(value=DEFAULT_GAS_PPM)
        self.co2_group_var = tk.StringVar(value="A")
        self.point_duration_var = tk.StringVar(value="180")
        self.stable_window_var = tk.StringVar(value="5")
        self.tail_window_var = tk.StringVar(value="10")
        self.sample_poll_var = tk.StringVar(value="0.1")
        self.print_every_var = tk.StringVar(value="1.0")
        self.source_close_delay_var = tk.StringVar(value="1")
        self.allow_live_var = tk.BooleanVar(value=False)
        self.configure_stream_var = tk.BooleanVar(value=False)
        self.run_state_var = tk.StringVar(value="状态：待开始")
        self.summary_var = tk.StringVar(value="结果：未运行")
        self.output_dir_var = tk.StringVar(value="结果目录：--")

        self._worker: threading.Thread | None = None
        self._process: subprocess.Popen[str] | None = None
        self._latest_output_dir: Path | None = None
        self._latest_plot_paths: Dict[str, Path] = {}
        self._latest_report_path: Path | None = None

        self._build_ui()

    def _build_ui(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        outer = ttk.Frame(self.root, padding=14)
        outer.pack(fill="both", expand=True)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(3, weight=1)

        hero = ttk.Frame(outer, padding=(16, 14))
        hero.grid(row=0, column=0, sticky="ew")
        hero.columnconfigure(0, weight=1)
        ttk.Label(hero, text="前端气路 CO2 比值查漏", font=("Microsoft YaHei UI", 16, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            hero,
            text="独立诊断，不写系数、不发 SENCO、不改设备 ID；测试结束后自动导出 CSV / JSON / TXT / 曲线图。",
            foreground="#32506a",
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))

        form = ttk.LabelFrame(outer, text="诊断参数", padding=12)
        form.grid(row=1, column=0, sticky="ew", pady=(10, 10))
        for col in range(4):
            form.columnconfigure(col, weight=1)

        self._labeled_entry(form, "配置文件", self.config_path_var, 0, 0, width=48)
        ttk.Button(form, text="浏览...", command=self._browse_config).grid(row=0, column=3, sticky="ew", padx=(8, 0))
        self._labeled_entry(form, "分析仪", self.analyzer_var, 1, 0)
        self._labeled_entry(form, "气点(ppm)", self.gas_ppm_var, 1, 1)
        self._labeled_combo(form, "组别", self.co2_group_var, ("A", "B", "auto"), 1, 2)
        self._labeled_entry(form, "点时长(s)", self.point_duration_var, 1, 3)
        self._labeled_entry(form, "稳定窗口(s)", self.stable_window_var, 2, 0)
        self._labeled_entry(form, "尾部窗口(s)", self.tail_window_var, 2, 1)
        self._labeled_entry(form, "采样间隔(s)", self.sample_poll_var, 2, 2)
        self._labeled_entry(form, "打印间隔(s)", self.print_every_var, 2, 3)
        self._labeled_entry(form, "关阀后等待(s)", self.source_close_delay_var, 3, 0)

        ttk.Checkbutton(
            form,
            text="允许实时硬件",
            variable=self.allow_live_var,
        ).grid(row=3, column=1, sticky="w")
        ttk.Checkbutton(
            form,
            text="重新配置分析仪流式模式",
            variable=self.configure_stream_var,
        ).grid(row=3, column=2, columnspan=2, sticky="w")

        tips = ttk.LabelFrame(outer, text="运行说明", padding=12)
        tips.grid(row=2, column=0, sticky="ew")
        ttk.Label(
            tips,
            text=(
                "默认流程：先关上一点源阀，等待设定秒数，再开下一路源阀；"
                "总闸和总气阀通路保持打开。"
            ),
            foreground="#35556e",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            tips,
            text="曲线图会自动生成：总览图、拟合图、切换总图、每段切换细图。",
            foreground="#35556e",
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))

        control = ttk.Frame(outer)
        control.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        control.columnconfigure(0, weight=1)
        control.rowconfigure(1, weight=1)

        action_row = ttk.Frame(control)
        action_row.grid(row=0, column=0, sticky="ew")
        self.start_button = ttk.Button(action_row, text="开始查漏诊断", command=self._start_run)
        self.start_button.pack(side="left")
        ttk.Button(action_row, text="打开结果目录", command=self._open_output_dir).pack(side="left", padx=(8, 0))
        ttk.Button(action_row, text="打开总览图", command=lambda: self._open_plot("ratio_overview_plot")).pack(
            side="left", padx=(8, 0)
        )
        ttk.Button(action_row, text="打开拟合图", command=lambda: self._open_plot("stable_mean_fit_plot")).pack(
            side="left", padx=(8, 0)
        )
        ttk.Button(action_row, text="打开切换总图", command=lambda: self._open_plot("transition_windows_plot")).pack(
            side="left", padx=(8, 0)
        )
        ttk.Button(action_row, text="打开文本报告", command=self._open_report).pack(side="left", padx=(8, 0))

        summary_card = ttk.Frame(control, padding=(0, 10, 0, 8))
        summary_card.grid(row=1, column=0, sticky="ew")
        ttk.Label(summary_card, textvariable=self.run_state_var, font=("Microsoft YaHei UI", 10, "bold")).pack(
            anchor="w"
        )
        ttk.Label(summary_card, textvariable=self.summary_var).pack(anchor="w", pady=(4, 0))
        ttk.Label(summary_card, textvariable=self.output_dir_var, foreground="#516b80").pack(anchor="w", pady=(4, 0))

        self.log_text = scrolledtext.ScrolledText(control, height=24, font=("Consolas", 10))
        self.log_text.grid(row=2, column=0, sticky="nsew")
        self.log_text.insert("end", "日志输出将在这里显示。\n")
        self.log_text.configure(state="disabled")

    def _labeled_entry(self, parent: ttk.Frame, label: str, variable: tk.StringVar, row: int, column: int, *, width: int = 16) -> None:
        frame = ttk.Frame(parent)
        frame.grid(row=row, column=column, sticky="ew", padx=4, pady=4)
        frame.columnconfigure(0, weight=1)
        ttk.Label(frame, text=label).grid(row=0, column=0, sticky="w")
        ttk.Entry(frame, textvariable=variable, width=width).grid(row=1, column=0, sticky="ew", pady=(4, 0))

    def _labeled_combo(
        self,
        parent: ttk.Frame,
        label: str,
        variable: tk.StringVar,
        values: Sequence[str],
        row: int,
        column: int,
    ) -> None:
        frame = ttk.Frame(parent)
        frame.grid(row=row, column=column, sticky="ew", padx=4, pady=4)
        frame.columnconfigure(0, weight=1)
        ttk.Label(frame, text=label).grid(row=0, column=0, sticky="w")
        ttk.Combobox(frame, textvariable=variable, values=list(values), state="readonly").grid(
            row=1, column=0, sticky="ew", pady=(4, 0)
        )

    def _browse_config(self) -> None:
        selected = filedialog.askopenfilename(
            title="选择配置文件",
            filetypes=[("JSON 配置", "*.json"), ("所有文件", "*.*")],
            initialdir=str(REPO_ROOT / "configs"),
        )
        if selected:
            try:
                self.config_path_var.set(str(Path(selected).relative_to(REPO_ROOT)))
            except Exception:
                self.config_path_var.set(selected)

    def _append_log(self, text: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", text.rstrip() + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _collect_state(self) -> FrontGasRouteLeakToolState:
        return FrontGasRouteLeakToolState(
            config_path=self.config_path_var.get().strip() or DEFAULT_CONFIG_PATH,
            analyzer=self.analyzer_var.get().strip() or "ga02",
            gas_ppm=self.gas_ppm_var.get().strip() or DEFAULT_GAS_PPM,
            co2_group=self.co2_group_var.get().strip() or "A",
            point_duration_s=self.point_duration_var.get().strip() or "180",
            stable_window_s=self.stable_window_var.get().strip() or "5",
            tail_window_s=self.tail_window_var.get().strip() or "10",
            sample_poll_s=self.sample_poll_var.get().strip() or "0.1",
            print_every_s=self.print_every_var.get().strip() or "1.0",
            source_close_first_delay_s=self.source_close_delay_var.get().strip() or "1",
            configure_analyzer_stream=bool(self.configure_stream_var.get()),
            allow_live_hardware=bool(self.allow_live_var.get()),
        )

    def _set_running(self, running: bool) -> None:
        self.start_button.configure(state="disabled" if running else "normal")
        self.run_state_var.set("状态：诊断进行中" if running else "状态：待开始")

    def _start_run(self) -> None:
        if self._worker and self._worker.is_alive():
            messagebox.showinfo("正在运行", "当前已有一轮前端气路查漏在运行，请等待结束。")
            return
        state = self._collect_state()
        if not state.allow_live_hardware:
            messagebox.showwarning("安全限制", "请先勾选“允许实时硬件”，再启动查漏诊断。")
            return

        self._latest_output_dir = None
        self._latest_plot_paths = {}
        self._latest_report_path = None
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        command = build_cli_command(state, run_id=run_id)
        self._append_log("")
        self._append_log(f"启动命令：{' '.join(command)}")
        self._set_running(True)

        worker = threading.Thread(target=self._run_subprocess, args=(command, run_id), daemon=True)
        self._worker = worker
        worker.start()

    def _run_subprocess(self, command: Sequence[str], run_id: str) -> None:
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        process: subprocess.Popen[str] | None = None
        output_dir: Path | None = None
        try:
            process = subprocess.Popen(
                list(command),
                cwd=str(REPO_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
            )
            self._process = process
            if process.stdout is not None:
                for line in process.stdout:
                    stripped = line.rstrip()
                    self.root.after(0, self._append_log, stripped)
                    if stripped.startswith("Leak check output dir:"):
                        raw_path = stripped.split(":", 1)[1].strip()
                        if raw_path:
                            output_dir = Path(raw_path)
            return_code = process.wait()
        except Exception as exc:
            self.root.after(0, self._append_log, f"前端气路查漏启动失败：{exc}")
            return_code = 1
        finally:
            self._process = None

        if output_dir is None:
            output_dir = REPO_ROOT / "results" / "gas_route_ratio_leak_check" / run_id
        self.root.after(0, self._handle_finished, return_code, output_dir)

    def _handle_finished(self, return_code: int, output_dir: Path) -> None:
        self._set_running(False)
        self._latest_output_dir = output_dir if output_dir.exists() else None
        self.output_dir_var.set(f"结果目录：{output_dir}" if output_dir else "结果目录：--")

        if output_dir.exists():
            fit_path = output_dir / "fit_summary.json"
            report_path = output_dir / "readable_report.txt"
            if report_path.exists():
                self._latest_report_path = report_path
            if fit_path.exists():
                try:
                    payload = json.loads(fit_path.read_text(encoding="utf-8"))
                except Exception as exc:
                    self.summary_var.set(f"结果：完成，但结果摘要读取失败：{exc}")
                else:
                    classification = str(payload.get("classification") or "unknown").upper()
                    self.summary_var.set(
                        "结果："
                        f"{classification} | monotonic={payload.get('monotonic_ok')} | "
                        f"R2={payload.get('linear_r2')} | "
                        f"max_residual={payload.get('max_abs_normalized_residual')}"
                    )
                    plot_files = payload.get("plot_files", {})
                    for key in ("ratio_overview_plot", "stable_mean_fit_plot", "transition_windows_plot"):
                        raw_path = plot_files.get(key)
                        if raw_path:
                            self._latest_plot_paths[key] = Path(raw_path)
            else:
                self.summary_var.set("结果：运行已结束，但未找到 fit_summary.json")
        else:
            self.summary_var.set("结果：运行已结束，但结果目录不存在")

        if return_code == 0:
            self.run_state_var.set("状态：诊断完成")
        elif return_code == 2:
            self.run_state_var.set("状态：被安全门阻止")
        else:
            self.run_state_var.set(f"状态：诊断失败 (rc={return_code})")

    def _open_path(self, path: Path | None, *, missing_title: str) -> None:
        if path is None or not path.exists():
            messagebox.showinfo(missing_title, "目标文件尚未生成，请先完成一次诊断。")
            return
        os.startfile(str(path))

    def _open_output_dir(self) -> None:
        self._open_path(self._latest_output_dir, missing_title="结果目录不可用")

    def _open_plot(self, key: str) -> None:
        self._open_path(self._latest_plot_paths.get(key), missing_title="曲线图不可用")

    def _open_report(self) -> None:
        self._open_path(self._latest_report_path, missing_title="报告不可用")

    def _on_close(self) -> None:
        if self._worker and self._worker.is_alive():
            if not messagebox.askyesno("诊断进行中", "查漏诊断仍在运行。现在关闭窗口不会主动中止子进程，是否仍然关闭窗口？"):
                return
        self.root.destroy()


def main(argv: Optional[Iterable[str]] = None) -> int:
    _unused = list(argv) if argv is not None else []
    root = tk.Tk()
    FrontGasRouteLeakToolApp(root)
    root.mainloop()
    return 0
