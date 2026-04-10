"""Application wrapper for the offline absorbance debugger."""

from __future__ import annotations

from pathlib import Path

from .analysis.pipeline import execute_pipeline
from .models.config import DebuggerConfig
from .options import (
    normalize_model_selection_strategy,
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
    model_selection_strategy: str = "auto_grouped",
    enable_composite_score: bool = True,
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
    config = DebuggerConfig(
        input_path=input_path,
        output_dir=resolved_output,
        analyzer_whitelist=tuple(analyzers),
        warning_only_analyzers=tuple(warning_only_analyzers),
        enable_base_final=enable_base_final,
        default_ratio_source=normalize_ratio_source(ratio_source),
        default_temp_source=normalize_temp_source(temperature_source),
        default_pressure_source=normalize_pressure_source(pressure_source),
        model_selection_strategy=normalize_model_selection_strategy(model_selection_strategy),
        enable_composite_score=bool(enable_composite_score),
        eps=eps,
        p_min_hpa=p_min_hpa,
        p_ref_hpa=p_ref_hpa,
        overwrite_output=overwrite_output,
    )
    return execute_pipeline(config)
