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
    model_selection_strategy: str = "auto_grouped"
    enable_composite_score: bool = True
    composite_weights: tuple[tuple[str, float], ...] = (
        ("overall_rmse", 0.35),
        ("zero_rmse", 0.30),
        ("temp_bias_spread", 0.20),
        ("max_abs_error", 0.15),
    )
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

    def default_pressure_label(self) -> str:
        """Return a short report label for the selected pressure source."""

        return "P_corr" if self.default_pressure_source == "pressure_corr_hpa" else "P_std"

    def default_temperature_label(self) -> str:
        """Return a short report label for the selected temperature source."""

        return "T_corr" if self.default_temp_source == "temp_corr_c" else "T_std"

    def default_ratio_label(self) -> str:
        """Return a short report label for the selected ratio source."""

        return "raw" if self.default_ratio_source == "ratio_co2_raw" else "filt"

    def composite_weight_map(self) -> dict[str, float]:
        """Return composite score weights as a regular mapping."""

        return {key: float(value) for key, value in self.composite_weights}


@dataclass(frozen=True)
class RunArtifacts:
    """Resolved input artifact names for a completed run bundle."""

    run_name: str
    root_prefix: str
    files: Mapping[str, str] = field(default_factory=dict)
