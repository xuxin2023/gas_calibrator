"""Minimal tkinter wrapper for the offline absorbance debugger."""

from __future__ import annotations

import threading
import webbrowser
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, StringVar, Tk, filedialog, messagebox
from tkinter import ttk

from .app import run_debugger
from .options import (
    normalize_model_selection_strategy,
    normalize_pressure_source,
    normalize_ratio_source,
    normalize_temp_source,
)


class AbsorbanceDebuggerGui:
    """Thin desktop wrapper around the existing debugger API."""

    def __init__(self, root: Tk) -> None:
        self.root = root
        self.root.title("Absorbance Debugger")
        self.root.geometry("780x500")

        self.input_path = StringVar()
        self.output_dir = StringVar(value=str(Path(__file__).resolve().parents[2] / "output" / "absorbance_debugger"))
        self.p_ref = StringVar(value="1013.25")
        self.pressure_source = StringVar(value="P_corr")
        self.temperature_source = StringVar(value="T_corr")
        self.ratio_source = StringVar(value="raw")
        self.model_selection_strategy = StringVar(value="auto")
        self.enable_composite_score = StringVar(value="1")
        self.auto_open_report = StringVar(value="1")
        self.status_text = StringVar(value="Ready.")
        self.last_report_path: Path | None = None

        self.ga_vars = {
            "GA01": StringVar(value="1"),
            "GA02": StringVar(value="1"),
            "GA03": StringVar(value="1"),
        }

        self._build_ui()

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=12)
        frame.pack(fill=BOTH, expand=True)

        self._path_row(frame, 0, "Input zip/run", self.input_path, self._browse_input)
        self._path_row(frame, 1, "Output dir", self.output_dir, self._browse_output)

        ttk.Label(frame, text="Analyzers").grid(row=2, column=0, sticky="w", pady=(10, 4))
        analyzer_frame = ttk.Frame(frame)
        analyzer_frame.grid(row=2, column=1, columnspan=2, sticky="w", pady=(10, 4))
        for idx, analyzer_id in enumerate(("GA01", "GA02", "GA03")):
            ttk.Checkbutton(
                analyzer_frame,
                text=analyzer_id,
                variable=self.ga_vars[analyzer_id],
                onvalue="1",
                offvalue="0",
            ).grid(row=0, column=idx, padx=(0, 12), sticky="w")

        self._entry_row(frame, 3, "P_ref (hPa)", self.p_ref)
        self._combo_row(frame, 4, "Pressure source", self.pressure_source, ("P_std", "P_corr"))
        self._combo_row(frame, 5, "Temperature source", self.temperature_source, ("T_std", "T_corr"))
        self._combo_row(frame, 6, "Ratio source", self.ratio_source, ("raw", "filt"))
        self._combo_row(frame, 7, "Model strategy", self.model_selection_strategy, ("auto", "grouped_loo", "grouped_kfold"))

        option_frame = ttk.Frame(frame)
        option_frame.grid(row=8, column=0, columnspan=3, sticky="w", pady=(10, 0))
        ttk.Checkbutton(
            option_frame,
            text="Enable composite score",
            variable=self.enable_composite_score,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT)
        ttk.Checkbutton(
            option_frame,
            text="Open report after run",
            variable=self.auto_open_report,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT, padx=(14, 0))

        action_frame = ttk.Frame(frame)
        action_frame.grid(row=9, column=0, columnspan=3, sticky="ew", pady=(18, 0))
        self.start_button = ttk.Button(action_frame, text="Start analysis", command=self._start_analysis)
        self.start_button.pack(side=LEFT)
        self.open_button = ttk.Button(action_frame, text="Open report.html", command=self._open_report, state="disabled")
        self.open_button.pack(side=LEFT, padx=(10, 0))

        ttk.Label(frame, textvariable=self.status_text, wraplength=700).grid(
            row=10,
            column=0,
            columnspan=3,
            sticky="w",
            pady=(16, 0),
        )

        frame.columnconfigure(1, weight=1)

    def _path_row(self, parent: ttk.Frame, row: int, label: str, variable: StringVar, command) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=4)
        entry = ttk.Entry(parent, textvariable=variable, width=72)
        entry.grid(row=row, column=1, sticky="ew", pady=4, padx=(8, 8))
        ttk.Button(parent, text="Browse", command=command).grid(row=row, column=2, sticky="e", pady=4)

    def _entry_row(self, parent: ttk.Frame, row: int, label: str, variable: StringVar) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=variable, width=16).grid(row=row, column=1, sticky="w", pady=4, padx=(8, 8))

    def _combo_row(self, parent: ttk.Frame, row: int, label: str, variable: StringVar, values: tuple[str, ...]) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=4)
        combo = ttk.Combobox(parent, textvariable=variable, values=values, state="readonly", width=14)
        combo.grid(row=row, column=1, sticky="w", pady=4, padx=(8, 8))

    def _browse_input(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Select run zip",
            filetypes=[("Zip files", "*.zip"), ("All files", "*.*")],
        )
        if file_path:
            self.input_path.set(file_path)
            return
        dir_path = filedialog.askdirectory(title="Select extracted run directory")
        if dir_path:
            self.input_path.set(dir_path)

    def _browse_output(self) -> None:
        dir_path = filedialog.askdirectory(title="Select output directory")
        if dir_path:
            self.output_dir.set(dir_path)

    def _selected_analyzers(self) -> tuple[str, ...]:
        selected = tuple(analyzer_id for analyzer_id, flag in self.ga_vars.items() if flag.get() == "1")
        if not selected:
            raise ValueError("Select at least one analyzer.")
        return selected

    def _start_analysis(self) -> None:
        try:
            input_path = self.input_path.get().strip()
            if not input_path:
                raise ValueError("Choose a run zip or extracted run directory.")
            analyzers = self._selected_analyzers()
            p_ref_hpa = float(self.p_ref.get().strip())
            ratio_source = normalize_ratio_source(self.ratio_source.get())
            temperature_source = normalize_temp_source(self.temperature_source.get())
            pressure_source = normalize_pressure_source(self.pressure_source.get())
            model_selection_strategy = normalize_model_selection_strategy(self.model_selection_strategy.get())
            enable_composite_score = self.enable_composite_score.get() == "1"
        except Exception as exc:
            messagebox.showerror("Absorbance Debugger", str(exc))
            return

        output_dir = Path(self.output_dir.get().strip())
        input_path_obj = Path(input_path)
        if output_dir.name.lower() != input_path_obj.stem.lower():
            output_dir = output_dir / input_path_obj.stem

        self.start_button.configure(state="disabled")
        self.open_button.configure(state="disabled")
        self.status_text.set("Running analysis...")

        def _worker() -> None:
            try:
                result = run_debugger(
                    input_path,
                    output_dir=output_dir,
                    analyzers=analyzers,
                    ratio_source=ratio_source,
                    temperature_source=temperature_source,
                    pressure_source=pressure_source,
                    model_selection_strategy=model_selection_strategy,
                    enable_composite_score=enable_composite_score,
                    p_ref_hpa=p_ref_hpa,
                )
                report_path = Path(result["output_dir"]) / "report.html"
                self.last_report_path = report_path
                self.root.after(0, lambda: self._finish_success(report_path))
            except Exception as exc:  # pragma: no cover - UI error surface
                self.root.after(0, lambda: self._finish_error(exc))

        threading.Thread(target=_worker, daemon=True).start()

    def _finish_success(self, report_path: Path) -> None:
        self.start_button.configure(state="normal")
        self.open_button.configure(state="normal")
        self.status_text.set(f"Analysis finished. Report: {report_path}")
        if self.auto_open_report.get() == "1" and report_path.exists():
            webbrowser.open(report_path.resolve().as_uri())

    def _finish_error(self, exc: Exception) -> None:
        self.start_button.configure(state="normal")
        self.status_text.set(f"Analysis failed: {exc}")
        messagebox.showerror("Absorbance Debugger", str(exc))

    def _open_report(self) -> None:
        if self.last_report_path is None or not self.last_report_path.exists():
            messagebox.showinfo("Absorbance Debugger", "No report.html is available yet.")
            return
        webbrowser.open(self.last_report_path.resolve().as_uri())


def main() -> int:
    root = Tk()
    AbsorbanceDebuggerGui(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
