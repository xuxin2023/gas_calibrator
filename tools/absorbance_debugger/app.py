"""Application wrapper for the offline absorbance debugger."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

from .analysis.merged_zero_anchor import build_merged_zero_anchor_compare
from .analysis.pipeline import execute_pipeline
from .analysis.cross_run import build_cross_run_summary
from .plots.charts import plot_cross_run_summary
from .models.config import DebuggerConfig
from .options import (
    normalize_absorbance_order_mode,
    normalize_invalid_pressure_mode,
    normalize_model_selection_strategy,
    normalize_zero_residual_model,
    parse_numeric_csv,
    normalize_pressure_source,
    normalize_ratio_source,
    normalize_temp_source,
)


def run_debugger(
    input_path: str | Path,
    *,
    output_dir: str | Path | None = None,
    analyzers: tuple[str, ...] = ("GA01", "GA02", "GA03"),
    warning_only_analyzers: tuple[str, ...] = ("GA04",),
    enable_base_final: bool = False,
    ratio_source: str = "ratio_co2_raw",
    temperature_source: str = "temp_corr_c",
    pressure_source: str = "pressure_corr_hpa",
    absorbance_order_mode: str = "samplewise_log_first",
    model_selection_strategy: str = "auto_grouped",
    enable_composite_score: bool = True,
    run_r0_source_consistency_compare: bool = True,
    run_pressure_branch_compare: bool = True,
    run_upper_bound_compare: bool = True,
    enable_zero_residual_correction: bool = True,
    zero_residual_models: str | tuple[str, ...] = ("linear", "quadratic"),
    enable_piecewise_model: bool = True,
    piecewise_boundary_ppm: float = 200.0,
    invalid_pressure_targets_hpa: str | tuple[float, ...] = (500.0,),
    invalid_pressure_tolerance_hpa: float = 30.0,
    invalid_pressure_mode: str = "hard_exclude",
    use_valid_only_main_conclusion: bool = True,
    eps: float = 1.0e-9,
    p_min_hpa: float = 100.0,
    p_ref_hpa: float = 1013.25,
    overwrite_output: bool = True,
) -> dict:
    """Execute the offline debugger with a convenient Python API."""

    input_path = Path(input_path).resolve()
    resolved_output = (
        Path(output_dir).resolve()
        if output_dir is not None
        else Path(__file__).resolve().parents[2] / "output" / "absorbance_debugger" / input_path.stem
    )
    invalid_targets = (
        parse_numeric_csv(invalid_pressure_targets_hpa)
        if isinstance(invalid_pressure_targets_hpa, str)
        else tuple(float(value) for value in invalid_pressure_targets_hpa)
    )
    if isinstance(zero_residual_models, str):
        zero_model_tokens = [item.strip() for item in str(zero_residual_models).split(",") if item.strip()]
    else:
        zero_model_tokens = [str(item).strip() for item in zero_residual_models if str(item).strip()]
    zero_models = tuple(normalize_zero_residual_model(item) for item in zero_model_tokens) if zero_model_tokens else ("linear", "quadratic")
    config = DebuggerConfig(
        input_path=input_path,
        output_dir=resolved_output,
        analyzer_whitelist=tuple(analyzers),
        warning_only_analyzers=tuple(warning_only_analyzers),
        enable_base_final=enable_base_final,
        default_ratio_source=normalize_ratio_source(ratio_source),
        default_temp_source=normalize_temp_source(temperature_source),
        default_pressure_source=normalize_pressure_source(pressure_source),
        absorbance_order_mode=normalize_absorbance_order_mode(absorbance_order_mode),
        model_selection_strategy=normalize_model_selection_strategy(model_selection_strategy),
        enable_composite_score=bool(enable_composite_score),
        run_r0_source_consistency_compare=bool(run_r0_source_consistency_compare),
        run_pressure_branch_compare=bool(run_pressure_branch_compare),
        run_upper_bound_compare=bool(run_upper_bound_compare),
        enable_zero_residual_correction=bool(enable_zero_residual_correction),
        zero_residual_candidate_models=zero_models,
        enable_piecewise_model=bool(enable_piecewise_model),
        piecewise_boundary_ppm=float(piecewise_boundary_ppm),
        invalid_pressure_targets_hpa=invalid_targets,
        invalid_pressure_tolerance_hpa=float(invalid_pressure_tolerance_hpa),
        invalid_pressure_mode=normalize_invalid_pressure_mode(invalid_pressure_mode),
        use_valid_only_main_conclusion=bool(use_valid_only_main_conclusion),
        eps=eps,
        p_min_hpa=p_min_hpa,
        p_ref_hpa=p_ref_hpa,
        overwrite_output=overwrite_output,
    )
    return execute_pipeline(config)


def _default_batch_output_dir(input_paths: tuple[Path, ...]) -> Path:
    root = Path(__file__).resolve().parents[2] / "output" / "absorbance_debugger"
    if not input_paths:
        return root / "cross_run_empty"
    if len(input_paths) == 1:
        return root / input_paths[0].stem
    return root / f"cross_run_{input_paths[0].stem}_plus_{len(input_paths) - 1}"


def run_debugger_batch(
    input_paths: tuple[str | Path, ...] | list[str | Path],
    *,
    output_dir: str | Path | None = None,
    **kwargs,
) -> dict:
    """Execute the debugger on multiple runs and emit a cross-run summary."""

    resolved_inputs = tuple(Path(path).resolve() for path in input_paths)
    resolved_output = Path(output_dir).resolve() if output_dir is not None else _default_batch_output_dir(resolved_inputs)
    overwrite_output = bool(kwargs.get("overwrite_output", True))
    run_kwargs = dict(kwargs)
    run_kwargs.pop("output_dir", None)
    if resolved_output.exists() and overwrite_output:
        shutil.rmtree(resolved_output)
    resolved_output.mkdir(parents=True, exist_ok=True)

    run_results: list[dict] = []
    failure_rows: list[dict[str, object]] = []
    for input_path in resolved_inputs:
        run_output = resolved_output / input_path.stem
        try:
            result = run_debugger(
                input_path,
                output_dir=run_output,
                **run_kwargs,
            )
            result["run_name"] = input_path.stem
            run_results.append(result)
        except Exception as exc:
            failure_rows.append(
                {
                    "run_name": input_path.stem,
                    "analyzer_id": "",
                    "run_status": "failed",
                    "failure_reason": str(exc),
                }
            )

    if not run_results and failure_rows:
        raise RuntimeError(f"All cross-run analyses failed: {failure_rows[0]['failure_reason']}")

    summary, by_analyzer, auto_conclusions, reproducibility_note = build_cross_run_summary(run_results)
    if failure_rows:
        summary = pd.concat([summary, pd.DataFrame(failure_rows)], ignore_index=True, sort=False) if not summary.empty else pd.DataFrame(failure_rows)
    summary_path = resolved_output / "step_09_cross_run_summary.csv"
    by_analyzer_path = resolved_output / "step_09_cross_run_by_analyzer.csv"
    auto_conclusions_path = resolved_output / "step_09_cross_run_auto_conclusions.csv"
    plot_path = resolved_output / "step_09_cross_run_plots.png"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    by_analyzer.to_csv(by_analyzer_path, index=False, encoding="utf-8-sig")
    auto_conclusions.to_csv(auto_conclusions_path, index=False, encoding="utf-8-sig")
    plot_cross_run_summary(summary[summary["analyzer_id"].ne("")].copy() if "analyzer_id" in summary.columns else summary, plot_path)
    (resolved_output / "step_09_cross_run_note.json").write_text(
        json.dumps({"reproducibility_note": reproducibility_note}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    merged_zero_anchor_compare = pd.DataFrame()
    base_result = next((item for item in run_results if str(item.get("run_name", "")) == "run_20260407_185002"), None)
    anchor_result = next(
        (
            item
            for item in run_results
            if str(item.get("run_name", "")) != "run_20260407_185002"
            and not getattr(item.get("run_role_assessment", pd.DataFrame()), "empty", True)
            and bool(
                item["run_role_assessment"][
                    item["run_role_assessment"]["assessment_scope"] == "run_summary"
                ]["has_high_temp_zero_anchor_candidate"].iloc[0]
            )
        ),
        None,
    )
    if base_result is not None and anchor_result is not None:
        merged_zero_anchor_compare = build_merged_zero_anchor_compare(
            base_result=base_result,
            anchor_result=anchor_result,
            output_dir=resolved_output,
        )
        if not merged_zero_anchor_compare.empty:
            merged_zero_anchor_compare.to_csv(
                resolved_output / "step_05z_merged_zero_anchor_compare.csv",
                index=False,
                encoding="utf-8-sig",
            )
    return {
        "output_dir": resolved_output,
        "run_results": run_results,
        "cross_run_summary": summary,
        "cross_run_by_analyzer": by_analyzer,
        "cross_run_auto_conclusions": auto_conclusions,
        "merged_zero_anchor_compare": merged_zero_anchor_compare,
        "reproducibility_note": reproducibility_note,
        "cross_run_summary_path": summary_path,
    }
