"""Configuration models for the offline absorbance debugger."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class DebuggerConfig:
    """Runtime options for one offline absorbance reconstruction run."""

    input_path: Path
    output_dir: Path
    analyzer_whitelist: tuple[str, ...] = ("GA01", "GA02", "GA03")
    warning_only_analyzers: tuple[str, ...] = ("GA04",)
    ratio_sources: tuple[str, ...] = ("ratio_co2_raw", "ratio_co2_filt")
    temp_sources: tuple[str, ...] = ("temp_std_c", "temp_corr_c")
    pressure_sources: tuple[str, ...] = ("pressure_std_hpa", "pressure_corr_hpa")
    r0_models: tuple[str, ...] = ("linear", "quadratic", "cubic")
    default_ratio_source: str = "ratio_co2_raw"
    default_temp_source: str = "temp_corr_c"
    default_pressure_source: str = "pressure_corr_hpa"
    default_r0_model: str = "quadratic"
    p_ref_hpa: float = 1013.25
    eps: float = 1.0e-9
    p_min_hpa: float = 100.0
    enable_base_final: bool = False
    overwrite_output: bool = True

    def default_branch_id(self) -> str:
        """Return the default absorbance branch identifier."""

        return "__".join(
            (
                self.default_ratio_source,
                self.default_temp_source,
                self.default_pressure_source,
                self.default_r0_model,
            )
        )


@dataclass(frozen=True)
class RunArtifacts:
    """Resolved input artifact names for a completed run bundle."""

    run_name: str
    root_prefix: str
    files: Mapping[str, str] = field(default_factory=dict)
