"""Application wrapper for the offline absorbance debugger."""

from __future__ import annotations

from pathlib import Path

from .analysis.pipeline import execute_pipeline
from .models.config import DebuggerConfig


def run_debugger(
    input_path: str | Path,
    *,
    output_dir: str | Path | None = None,
    analyzers: tuple[str, ...] = ("GA01", "GA02", "GA03"),
    warning_only_analyzers: tuple[str, ...] = ("GA04",),
    enable_base_final: bool = False,
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
        else input_path.parent / "output" / "absorbance_debugger" / input_path.stem
    )
    config = DebuggerConfig(
        input_path=input_path,
        output_dir=resolved_output,
        analyzer_whitelist=tuple(analyzers),
        warning_only_analyzers=tuple(warning_only_analyzers),
        enable_base_final=enable_base_final,
        eps=eps,
        p_min_hpa=p_min_hpa,
        p_ref_hpa=p_ref_hpa,
        overwrite_output=overwrite_output,
    )
    return execute_pipeline(config)
