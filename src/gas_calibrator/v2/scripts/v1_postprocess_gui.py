from __future__ import annotations

import queue
import sys
import threading
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd

V2_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = Path(__file__).resolve().parents[3]
REPO_ROOT = Path(__file__).resolve().parents[4]

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

DEFAULT_CONFIG_PATH = REPO_ROOT / "configs" / "default_config.json"
_PREVIEW_SHEET = "简化系数"
_PREVIEW_METADATA_COLUMNS = ("分析仪", "气体", "数据范围")


@dataclass(frozen=True)
class PostprocessJobOptions:
    run_dir: str
    config_path: str
    output_dir: str = ""
    download: bool = True


def _load_default_runner() -> Callable[..., dict[str, str]]:
    # Delay optional analytics/storage imports until the job is actually started.
    from gas_calibrator.v2.adapters.v1_postprocess_runner import run_from_cli

    return run_from_cli


def run_postprocess_job(
    options: PostprocessJobOptions,
    *,
    runner: Optional[Callable[..., dict[str, str]]] = None,
) -> dict[str, str]:
    run_dir = str(options.run_dir or "").strip()
    config_path = str(options.config_path or "").strip()
    output_dir = str(options.output_dir or "").strip()
    if not run_dir:
        raise ValueError("run_dir is required")
    if not config_path:
        raise ValueError("config_path is required")
    effective_runner = runner or _load_default_runner()
    return effective_runner(
        run_dir=run_dir,
        config_path=config_path,
        output_dir=output_dir or None,
        download=bool(options.download),
    )


def _format_display_coefficient(value: Any) -> str:
    try:
        numeric = float(value)
    except Exception:
        return "" if value is None else str(value)
    return format(numeric, ".5E")


def build_coefficient_preview(report_path: str | Path, *, max_rows: int = 16) -> str:
    path = Path(report_path)
    if not path.exists():
        return f"系数预览不可用：报告不存在 {path}"
    try:
        frame = pd.read_excel(path, sheet_name=_PREVIEW_SHEET)
    except Exception as exc:
        return f"系数预览不可用：无法读取 {_PREVIEW_SHEET} - {exc}"
    if frame.empty:
        return "系数预览不可用：简化系数页为空"

    preview = frame.head(max_rows).copy()
    coefficient_columns = [
        column
        for column in preview.columns
        if str(column or "").strip() not in _PREVIEW_METADATA_COLUMNS
        and not str(column or "").strip().endswith("_term")
    ]
    display_columns = [column for column in _PREVIEW_METADATA_COLUMNS if column in preview.columns] + coefficient_columns
    preview = preview.loc[:, display_columns]
    for column in coefficient_columns:
        preview[column] = preview[column].map(_format_display_coefficient)
    return preview.to_string(index=False)


class V1PostprocessGui:
    def __init__(self) -> None:
        import tkinter as tk
        from tkinter import ttk

        self._tk = tk
        self.root = tk.Tk()
        self.root.title("V1 后处理与系数下发")
        self.root.geometry("920x700")
        self.root.minsize(860, 620)

        self._queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._worker: Optional[threading.Thread] = None

        self.run_dir_var = tk.StringVar()
        self.config_var = tk.StringVar(value=str(DEFAULT_CONFIG_PATH))
        self.output_var = tk.StringVar()
        self.download_var = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value="请选择运行目录和配置文件。")

        frame = ttk.Frame(self.root, padding=16)
        frame.pack(fill="both", expand=True)
        frame.columnconfigure(1, weight=1)

        ttk.Label(frame, text="运行目录").grid(row=0, column=0, sticky="w", padx=(0, 12), pady=(0, 10))
        ttk.Entry(frame, textvariable=self.run_dir_var).grid(row=0, column=1, sticky="ew", pady=(0, 10))
        ttk.Button(frame, text="选择...", command=self._choose_run_dir).grid(row=0, column=2, padx=(12, 0), pady=(0, 10))

        ttk.Label(frame, text="配置文件").grid(row=1, column=0, sticky="w", padx=(0, 12), pady=(0, 10))
        ttk.Entry(frame, textvariable=self.config_var).grid(row=1, column=1, sticky="ew", pady=(0, 10))
        ttk.Button(frame, text="选择...", command=self._choose_config).grid(row=1, column=2, padx=(12, 0), pady=(0, 10))

        ttk.Label(frame, text="输出目录").grid(row=2, column=0, sticky="w", padx=(0, 12), pady=(0, 10))
        ttk.Entry(frame, textvariable=self.output_var).grid(row=2, column=1, sticky="ew", pady=(0, 10))
        ttk.Button(frame, text="选择...", command=self._choose_output_dir).grid(row=2, column=2, padx=(12, 0), pady=(0, 10))

        ttk.Checkbutton(frame, text="生成报告后自动下发到分析仪", variable=self.download_var).grid(
            row=3, column=0, columnspan=3, sticky="w", pady=(0, 14)
        )

        ttk.Button(frame, text="开始执行", command=self._start).grid(row=4, column=0, sticky="w")
        ttk.Label(frame, textvariable=self.status_var).grid(row=4, column=1, columnspan=2, sticky="w", padx=(12, 0))

        ttk.Label(frame, text="执行日志").grid(row=5, column=0, columnspan=3, sticky="w", pady=(14, 6))
        self.log_text = tk.Text(frame, height=14, wrap="word", state="disabled")
        self.log_text.grid(row=6, column=0, columnspan=3, sticky="nsew")
        frame.rowconfigure(6, weight=1)

        ttk.Label(frame, text="系数预览").grid(row=7, column=0, columnspan=3, sticky="w", pady=(14, 6))
        self.preview_text = tk.Text(frame, height=12, wrap="none", state="disabled")
        self.preview_text.grid(row=8, column=0, columnspan=3, sticky="nsew")
        frame.rowconfigure(8, weight=1)

        self.root.after(120, self._drain_queue)

    def _append_log(self, message: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message.rstrip() + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _set_preview(self, message: str) -> None:
        self.preview_text.configure(state="normal")
        self.preview_text.delete("1.0", "end")
        if message:
            self.preview_text.insert("end", message.rstrip() + "\n")
        self.preview_text.configure(state="disabled")

    def _choose_run_dir(self) -> None:
        from tkinter import filedialog

        selected = filedialog.askdirectory(title="选择 V1 运行目录")
        if selected:
            self.run_dir_var.set(selected)

    def _choose_config(self) -> None:
        from tkinter import filedialog

        selected = filedialog.askopenfilename(
            title="选择配置文件",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            initialfile=self.config_var.get() or str(DEFAULT_CONFIG_PATH),
        )
        if selected:
            self.config_var.set(selected)

    def _choose_output_dir(self) -> None:
        from tkinter import filedialog

        selected = filedialog.askdirectory(title="选择输出目录")
        if selected:
            self.output_var.set(selected)

    def _set_busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        for child in self.root.winfo_children():
            self._set_state_recursive(child, state)
        self.log_text.configure(state="disabled")
        self.preview_text.configure(state="disabled")

    def _set_state_recursive(self, widget: Any, state: str) -> None:
        try:
            if widget not in {self.log_text, self.preview_text}:
                widget.configure(state=state)
        except Exception:
            pass
        for child in widget.winfo_children():
            self._set_state_recursive(child, state)

    def _start(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        options = PostprocessJobOptions(
            run_dir=self.run_dir_var.get(),
            config_path=self.config_var.get(),
            output_dir=self.output_var.get(),
            download=bool(self.download_var.get()),
        )
        self._append_log("开始执行后处理任务...")
        self.status_var.set("执行中...")
        self._set_busy(True)
        self._worker = threading.Thread(target=self._run_worker, args=(options,), daemon=True)
        self._worker.start()

    def _run_worker(self, options: PostprocessJobOptions) -> None:
        try:
            result = run_postprocess_job(options)
            self._queue.put(("success", result))
        except Exception as exc:
            self._queue.put(("error", {"message": str(exc), "traceback": traceback.format_exc()}))

    def _drain_queue(self) -> None:
        while True:
            try:
                event, payload = self._queue.get_nowait()
            except queue.Empty:
                break
            if event == "success":
                self.status_var.set("执行完成。")
                self._append_log("执行完成。")
                report_path = ""
                for key in ("report", "summary", "download_summary", "io_log"):
                    value = payload.get(key)
                    if value:
                        self._append_log(f"{key}: {value}")
                    if key == "report":
                        report_path = str(value or "")
                if report_path:
                    self._set_preview(build_coefficient_preview(report_path))
            elif event == "error":
                self.status_var.set("执行失败。")
                self._append_log(f"执行失败: {payload.get('message', '')}")
                tb = str(payload.get("traceback") or "").strip()
                if tb:
                    self._append_log(tb)
                self._set_preview("")
            self._set_busy(False)
        self.root.after(120, self._drain_queue)

    def run(self) -> None:
        self.root.mainloop()


def main() -> int:
    gui = V1PostprocessGui()
    gui.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
