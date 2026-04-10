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
    absorbance_order_mode: str = "samplewise_log_first"
    default_absorbance_order: str = "samplewise_log_first"
    default_source_policy: str = "matched_only"
    matched_selection_policy: str = "per_analyzer_best_among_matched"
    model_selection_strategy: str = "auto_grouped"
    enable_composite_score: bool = True
    run_r0_source_consistency_compare: bool = True
    run_pressure_branch_compare: bool = True
    run_upper_bound_compare: bool = True
    enable_zero_residual_correction: bool = True
    zero_residual_candidate_models: tuple[str, ...] = ("linear", "quadratic")
    zero_residual_piecewise_break_temp_c: float = 20.0
    enable_piecewise_model: bool = True
    piecewise_boundary_ppm: float = 200.0
    invalid_pressure_targets_hpa: tuple[float, ...] = (500.0,)
    invalid_pressure_tolerance_hpa: float = 30.0
    invalid_pressure_mode: str = "hard_exclude"
    use_valid_only_main_conclusion: bool = True
    composite_weights: tuple[tuple[str, float], ...] = (
        ("overall_rmse", 0.25),
        ("zero_rmse", 0.35),
        ("temp_bias_spread", 0.25),
        ("max_abs_error", 0.15),
    )
    legacy_composite_weights: tuple[tuple[str, float], ...] = (
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

    def default_pressure_branch_label(self) -> str:
        """Return the multiply-norm pressure branch label for the selected source."""

        return "pressure_corr" if self.default_pressure_source == "pressure_corr_hpa" else "pressure_std"

    def default_alt_pressure_branch_label(self) -> str:
        """Return the divide-only pressure branch label for the selected source."""

        return "alt_divide_only_corr" if self.default_pressure_source == "pressure_corr_hpa" else "alt_divide_only_std"

    def composite_weight_map(self) -> dict[str, float]:
        """Return composite score weights as a regular mapping."""

        return {key: float(value) for key, value in self.composite_weights}

    def legacy_composite_weight_map(self) -> dict[str, float]:
        """Return the legacy composite score weights used before this round."""

        return {key: float(value) for key, value in self.legacy_composite_weights}

    def matched_source_pair_label(self, ratio_source: str) -> str:
        """Return the matched source-pair label for one ratio source."""

        return "raw/raw" if ratio_source == "ratio_co2_raw" else "filt/filt"

    def matched_ratio_sources(self) -> tuple[str, ...]:
        """Return the ratio sources that are eligible for the main matched-only chain."""

        return tuple(source for source in self.ratio_sources if source in {"ratio_co2_raw", "ratio_co2_filt"})


@dataclass(frozen=True)
class RunArtifacts:
    """Resolved input artifact names for a completed run bundle."""

    run_name: str
    root_prefix: str
    files: Mapping[str, str] = field(default_factory=dict)
