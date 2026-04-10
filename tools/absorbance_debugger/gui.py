"""Minimal tkinter wrapper for the offline absorbance debugger."""

from __future__ import annotations

import threading
import webbrowser
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, StringVar, Tk, filedialog, messagebox
from tkinter import ttk

from .app import run_debugger, run_debugger_batch
from .options import (
    normalize_absorbance_order_mode,
    normalize_model_selection_strategy,
    normalize_pressure_source,
    normalize_ratio_source,
    normalize_temp_source,
    parse_path_list,
)


class AbsorbanceDebuggerGui:
    """Thin desktop wrapper around the existing debugger API."""

    def __init__(self, root: Tk) -> None:
        self.root = root
        self.root.title("Absorbance Debugger")
        self.root.geometry("840x580")

        self.input_path = StringVar()
        self.output_dir = StringVar(value=str(Path(__file__).resolve().parents[2] / "output" / "absorbance_debugger"))
        self.p_ref = StringVar(value="1013.25")
        self.pressure_source = StringVar(value="P_corr")
        self.temperature_source = StringVar(value="T_corr")
        self.ratio_source = StringVar(value="raw")
        self.absorbance_order_mode = StringVar(value="samplewise_log_first")
        self.model_selection_strategy = StringVar(value="auto")
        self.enable_zero_residual_correction = StringVar(value="1")
        self.zero_residual_models = StringVar(value="linear,quadratic")
        self.enable_piecewise_model = StringVar(value="1")
        self.piecewise_boundary_ppm = StringVar(value="200")
        self.invalid_pressure_targets_hpa = StringVar(value="500")
        self.invalid_pressure_tolerance_hpa = StringVar(value="30")
        self.enable_composite_score = StringVar(value="1")
        self.run_source_consistency_compare = StringVar(value="1")
        self.run_pressure_branch_compare = StringVar(value="1")
        self.run_upper_bound_compare = StringVar(value="1")
        self.hard_invalid_pressure_exclude = StringVar(value="1")
        self.use_valid_only_main_conclusion = StringVar(value="1")
        self.auto_open_report = StringVar(value="1")
        self.status_text = StringVar(value="Ready.")
        self.selected_sources_text = StringVar(value="Selected sources: not run yet.")
        self.last_report_path: Path | None = None
        self.last_summary_path: Path | None = None

        self.ga_vars = {
            "GA01": StringVar(value="1"),
            "GA02": StringVar(value="1"),
            "GA03": StringVar(value="1"),
            "GA04": StringVar(value="1"),
        }

        self._build_ui()

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=12)
        frame.pack(fill=BOTH, expand=True)

        self._path_row(frame, 0, "Input zip/run(s)", self.input_path, self._browse_input)
        self._path_row(frame, 1, "Output dir", self.output_dir, self._browse_output)

        ttk.Label(frame, text="Analyzers").grid(row=2, column=0, sticky="w", pady=(10, 4))
        analyzer_frame = ttk.Frame(frame)
        analyzer_frame.grid(row=2, column=1, columnspan=2, sticky="w", pady=(10, 4))
        for idx, analyzer_id in enumerate(("GA01", "GA02", "GA03", "GA04")):
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
        self._combo_row(frame, 7, "Order mode", self.absorbance_order_mode, ("samplewise_log_first", "mean_first_log", "compare_both"))
        self._combo_row(frame, 8, "Model strategy", self.model_selection_strategy, ("auto", "grouped_loo", "grouped_kfold"))
        self._combo_row(frame, 9, "ΔA0(T) model(s)", self.zero_residual_models, ("linear,quadratic", "linear", "quadratic", "piecewise_linear"))
        self._entry_row(frame, 10, "Piecewise boundary", self.piecewise_boundary_ppm)
        self._entry_row(frame, 11, "Invalid pressures", self.invalid_pressure_targets_hpa)
        self._entry_row(frame, 12, "Invalid tol (hPa)", self.invalid_pressure_tolerance_hpa)

        option_frame = ttk.Frame(frame)
        option_frame.grid(row=13, column=0, columnspan=3, sticky="w", pady=(10, 0))
        ttk.Checkbutton(
            option_frame,
            text="Enable composite score",
            variable=self.enable_composite_score,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT)
        ttk.Checkbutton(
            option_frame,
            text="Enable zero residual correction",
            variable=self.enable_zero_residual_correction,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT, padx=(14, 0))
        ttk.Checkbutton(
            option_frame,
            text="Enable piecewise model",
            variable=self.enable_piecewise_model,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT, padx=(14, 0))
        ttk.Checkbutton(
            option_frame,
            text="R0 source consistency compare",
            variable=self.run_source_consistency_compare,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT, padx=(14, 0))
        ttk.Checkbutton(
            option_frame,
            text="Pressure branch compare",
            variable=self.run_pressure_branch_compare,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT, padx=(14, 0))
        ttk.Checkbutton(
            option_frame,
            text="Upper bound vs deployable",
            variable=self.run_upper_bound_compare,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT, padx=(14, 0))

        option_frame_2 = ttk.Frame(frame)
        option_frame_2.grid(row=14, column=0, columnspan=3, sticky="w", pady=(10, 0))
        ttk.Checkbutton(
            option_frame_2,
            text="Hard-exclude invalid pressure bins",
            variable=self.hard_invalid_pressure_exclude,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT)
        ttk.Checkbutton(
            option_frame_2,
            text="Use valid-only as main conclusion",
            variable=self.use_valid_only_main_conclusion,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT, padx=(14, 0))
        ttk.Checkbutton(
            option_frame_2,
            text="Open report after run",
            variable=self.auto_open_report,
            onvalue="1",
            offvalue="0",
        ).pack(side=LEFT, padx=(14, 0))

        action_frame = ttk.Frame(frame)
        action_frame.grid(row=15, column=0, columnspan=3, sticky="ew", pady=(18, 0))
        self.start_button = ttk.Button(action_frame, text="Start analysis", command=self._start_analysis)
        self.start_button.pack(side=LEFT)
        self.open_button = ttk.Button(action_frame, text="Open report/summary", command=self._open_report, state="disabled")
        self.open_button.pack(side=LEFT, padx=(10, 0))

        ttk.Label(frame, textvariable=self.status_text, wraplength=700).grid(
            row=16,
            column=0,
            columnspan=3,
            sticky="w",
            pady=(16, 0),
        )
        ttk.Label(frame, textvariable=self.selected_sources_text, wraplength=700).grid(
            row=17,
            column=0,
            columnspan=3,
            sticky="w",
            pady=(10, 0),
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
        file_paths = filedialog.askopenfilenames(
            title="Select one or more run zips",
            filetypes=[("Zip files", "*.zip"), ("All files", "*.*")],
        )
        if file_paths:
            self.input_path.set(";".join(file_paths))
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
            input_paths = parse_path_list(self.input_path.get().strip())
            if not input_paths:
                raise ValueError("Choose a run zip or extracted run directory.")
            analyzers = self._selected_analyzers()
            p_ref_hpa = float(self.p_ref.get().strip())
            ratio_source = normalize_ratio_source(self.ratio_source.get())
            temperature_source = normalize_temp_source(self.temperature_source.get())
            pressure_source = normalize_pressure_source(self.pressure_source.get())
            absorbance_order_mode = normalize_absorbance_order_mode(self.absorbance_order_mode.get())
            model_selection_strategy = normalize_model_selection_strategy(self.model_selection_strategy.get())
            zero_residual_models = self.zero_residual_models.get().strip()
            piecewise_boundary_ppm = float(self.piecewise_boundary_ppm.get().strip())
            invalid_pressure_targets_hpa = self.invalid_pressure_targets_hpa.get().strip()
            invalid_pressure_tolerance_hpa = float(self.invalid_pressure_tolerance_hpa.get().strip())
            enable_composite_score = self.enable_composite_score.get() == "1"
            enable_zero_residual_correction = self.enable_zero_residual_correction.get() == "1"
            enable_piecewise_model = self.enable_piecewise_model.get() == "1"
            run_source_consistency_compare = self.run_source_consistency_compare.get() == "1"
            run_pressure_branch_compare = self.run_pressure_branch_compare.get() == "1"
            run_upper_bound_compare = self.run_upper_bound_compare.get() == "1"
            hard_invalid_pressure_exclude = self.hard_invalid_pressure_exclude.get() == "1"
            use_valid_only_main_conclusion = self.use_valid_only_main_conclusion.get() == "1"
        except Exception as exc:
            messagebox.showerror("Absorbance Debugger", str(exc))
            return

        output_dir = Path(self.output_dir.get().strip())
        input_path_obj = Path(input_paths[0])
        if len(input_paths) == 1 and output_dir.name.lower() != input_path_obj.stem.lower():
            output_dir = output_dir / input_path_obj.stem

        self.start_button.configure(state="disabled")
        self.open_button.configure(state="disabled")
        self.status_text.set("Running analysis...")

        def _worker() -> None:
            try:
                common_kwargs = dict(
                    output_dir=output_dir,
                    analyzers=analyzers,
                    ratio_source=ratio_source,
                    temperature_source=temperature_source,
                    pressure_source=pressure_source,
                    absorbance_order_mode=absorbance_order_mode,
                    model_selection_strategy=model_selection_strategy,
                    enable_composite_score=enable_composite_score,
                    enable_zero_residual_correction=enable_zero_residual_correction,
                    zero_residual_models=zero_residual_models,
                    enable_piecewise_model=enable_piecewise_model,
                    piecewise_boundary_ppm=piecewise_boundary_ppm,
                    run_r0_source_consistency_compare=run_source_consistency_compare,
                    run_pressure_branch_compare=run_pressure_branch_compare,
                    run_upper_bound_compare=run_upper_bound_compare,
                    invalid_pressure_targets_hpa=invalid_pressure_targets_hpa,
                    invalid_pressure_tolerance_hpa=invalid_pressure_tolerance_hpa,
                    invalid_pressure_mode="hard_exclude" if hard_invalid_pressure_exclude else "diagnostic_only",
                    use_valid_only_main_conclusion=use_valid_only_main_conclusion,
                    p_ref_hpa=p_ref_hpa,
                )
                result = (
                    run_debugger(input_paths[0], **common_kwargs)
                    if len(input_paths) == 1
                    else run_debugger_batch(tuple(input_paths), **common_kwargs)
                )
                artifact_path = (
                    Path(result["cross_run_summary_path"])
                    if "cross_run_summary_path" in result
                    else Path(result["output_dir"]) / "report.html"
                )
                self.last_report_path = Path(result["output_dir"]) / "report.html" if len(input_paths) == 1 else None
                self.last_summary_path = artifact_path if "cross_run_summary_path" in result else None
                self.root.after(0, lambda: self._finish_success(artifact_path, result))
            except Exception as exc:  # pragma: no cover - UI error surface
                self.root.after(0, lambda: self._finish_error(exc))

        threading.Thread(target=_worker, daemon=True).start()

    def _finish_success(self, artifact_path: Path, result: dict[str, object]) -> None:
        self.start_button.configure(state="normal")
        self.open_button.configure(state="normal")
        self.status_text.set(f"Analysis finished. Output: {artifact_path}")
        selection = result.get("selected_source_summary")
        if selection is not None and hasattr(selection, "empty") and not selection.empty:
            parts = [
                f"{row['analyzer_id']}={row['selected_source_pair']}"
                for row in selection.to_dict(orient="records")
            ]
            self.selected_sources_text.set("Selected sources: " + ", ".join(parts))
        elif "reproducibility_note" in result:
            lines = [str(result["reproducibility_note"])]
            for run_result in result.get("run_results", []):
                invalid_summary = run_result.get("invalid_pressure_summary")
                if invalid_summary is not None and hasattr(invalid_summary, "empty") and not invalid_summary.empty:
                    overall = invalid_summary[invalid_summary["summary_scope"] == "overall"]
                    if not overall.empty:
                        lines.append(
                            f"{run_result.get('run_name', '')}: invalid pressure excluded {int(overall.iloc[0]['invalid_point_count'])} point(s)"
                        )
            self.selected_sources_text.set(" | ".join(lines))
        else:
            invalid_summary = result.get("invalid_pressure_summary")
            if invalid_summary is not None and hasattr(invalid_summary, "empty") and not invalid_summary.empty:
                overall = invalid_summary[invalid_summary["summary_scope"] == "overall"]
                if not overall.empty:
                    self.selected_sources_text.set(
                        f"Selected sources: not available. Invalid pressure excluded {int(overall.iloc[0]['invalid_point_count'])} point(s)."
                    )
                    return
            self.selected_sources_text.set("Selected sources: not available.")
        if self.auto_open_report.get() == "1" and artifact_path.exists():
            webbrowser.open(artifact_path.resolve().as_uri())

    def _finish_error(self, exc: Exception) -> None:
        self.start_button.configure(state="normal")
        self.status_text.set(f"Analysis failed: {exc}")
        messagebox.showerror("Absorbance Debugger", str(exc))

    def _open_report(self) -> None:
        target = self.last_summary_path if self.last_summary_path is not None else self.last_report_path
        if target is None or not target.exists():
            messagebox.showinfo("Absorbance Debugger", "No report.html is available yet.")
            return
        webbrowser.open(target.resolve().as_uri())


def main() -> int:
    root = Tk()
    AbsorbanceDebuggerGui(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
