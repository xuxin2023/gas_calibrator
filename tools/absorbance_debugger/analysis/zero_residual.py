"""Zero-residual absorbance correction helpers."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ZeroResidualModelSpec:
    """One candidate model for the residual zero absorbance surface."""

    model_id: str
    model_label: str
    formula: str
    terms: tuple[str, ...]


@dataclass(frozen=True)
class ZeroResidualFit:
    """Fitted zero-residual correction model."""

    spec: ZeroResidualModelSpec
    coefficients: tuple[float, ...]
    sample_count: int
    rmse: float
    mae: float
    max_abs_error: float
    breakpoint_temp_c: float | None = None

    def evaluate(self, values: pd.Series | np.ndarray | list[float]) -> np.ndarray:
        matrix = _design_matrix(np.asarray(values, dtype=float), self.spec, self.breakpoint_temp_c)
        return matrix @ np.asarray(self.coefficients, dtype=float)

    def formula_text(self, precision: int = 8) -> str:
        if self.spec.model_id == "piecewise_linear":
            breakpoint = 0.0 if self.breakpoint_temp_c is None else float(self.breakpoint_temp_c)
            coeffs = [f"{value:.{precision}g}" for value in self.coefficients]
            return (
                f"{coeffs[0]} + {coeffs[1]}*T + {coeffs[2]}*max(T - {breakpoint:.{precision}g}, 0)"
            ).replace("+ -", "- ")
        coeffs = [f"{value:.{precision}g}" for value in self.coefficients]
        if self.spec.model_id == "linear":
            return f"{coeffs[0]} + {coeffs[1]}*T".replace("+ -", "- ")
        if self.spec.model_id == "quadratic":
            return f"{coeffs[0]} + {coeffs[1]}*T + {coeffs[2]}*T^2".replace("+ -", "- ")
        return self.spec.formula


ZERO_RESIDUAL_MODEL_SPECS: dict[str, ZeroResidualModelSpec] = {
    "linear": ZeroResidualModelSpec(
        model_id="linear",
        model_label="Linear zero residual",
        formula="ΔA0(T) = z0 + z1*T",
        terms=("intercept", "T"),
    ),
    "quadratic": ZeroResidualModelSpec(
        model_id="quadratic",
        model_label="Quadratic zero residual",
        formula="ΔA0(T) = z0 + z1*T + z2*T^2",
        terms=("intercept", "T", "T^2"),
    ),
    "piecewise_linear": ZeroResidualModelSpec(
        model_id="piecewise_linear",
        model_label="Piecewise-linear zero residual",
        formula="ΔA0(T) = z0 + z1*T + z2*max(T-T_break,0)",
        terms=("intercept", "T", "H(T-T_break)"),
    ),
}

ZERO_RESIDUAL_NONE_LABEL = "No zero residual correction"


def available_zero_residual_modes(config: Any) -> tuple[str, ...]:
    """Return the zero-residual modes evaluated in this run."""

    modes = ["none"]
    if getattr(config, "enable_zero_residual_correction", True):
        modes.extend(list(getattr(config, "zero_residual_candidate_models", ("linear", "quadratic"))))
    return tuple(dict.fromkeys(modes))


def _metrics(errors: pd.Series | np.ndarray) -> dict[str, float]:
    clean = pd.to_numeric(pd.Series(errors), errors="coerce").dropna()
    if clean.empty:
        return {
            "rmse": math.nan,
            "mae": math.nan,
            "max_abs_error": math.nan,
            "bias": math.nan,
        }
    values = clean.to_numpy(dtype=float)
    abs_values = np.abs(values)
    return {
        "rmse": float(np.sqrt(np.mean(np.square(values)))),
        "mae": float(np.mean(abs_values)),
        "max_abs_error": float(np.max(abs_values)),
        "bias": float(np.mean(values)),
    }


def _design_matrix(values: np.ndarray, spec: ZeroResidualModelSpec, breakpoint_temp_c: float | None) -> np.ndarray:
    columns: list[np.ndarray] = []
    positive = np.maximum(values - float(breakpoint_temp_c or 0.0), 0.0)
    for term in spec.terms:
        if term == "intercept":
            columns.append(np.ones(len(values), dtype=float))
        elif term == "T":
            columns.append(values)
        elif term == "T^2":
            columns.append(np.square(values))
        elif term == "H(T-T_break)":
            columns.append(positive)
        else:  # pragma: no cover - bounded by ZERO_RESIDUAL_MODEL_SPECS
            raise ValueError(f"Unsupported zero residual term: {term}")
    return np.column_stack(columns)


def _fit_zero_model(
    observations: pd.DataFrame,
    spec: ZeroResidualModelSpec,
    breakpoint_temp_c: float | None,
) -> ZeroResidualFit:
    x = observations["temp_use_c"].to_numpy(dtype=float)
    y = observations["A_zero_obs"].to_numpy(dtype=float)
    if len(observations) < len(spec.terms):
        raise ValueError(f"{spec.model_id} needs at least {len(spec.terms)} zero observations")
    matrix = _design_matrix(x, spec, breakpoint_temp_c)
    coeffs, _, _, _ = np.linalg.lstsq(matrix, y, rcond=None)
    predicted = matrix @ coeffs
    metrics = _metrics(predicted - y)
    return ZeroResidualFit(
        spec=spec,
        coefficients=tuple(float(item) for item in coeffs),
        sample_count=int(len(observations)),
        rmse=metrics["rmse"],
        mae=metrics["mae"],
        max_abs_error=metrics["max_abs_error"],
        breakpoint_temp_c=breakpoint_temp_c,
    )


def fit_zero_residual_models(
    absorbance_points: pd.DataFrame,
    config: Any,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[tuple[str, str, str], ZeroResidualFit]]:
    """Fit ΔA0(T) candidates from valid-only zero points on the deployable main branch."""

    main_points = absorbance_points[
        (absorbance_points["temp_source"] == config.default_temp_source)
        & (absorbance_points["pressure_source"] == config.default_pressure_source)
        & (absorbance_points["r0_model"] == config.default_r0_model)
        & (absorbance_points["ratio_source"].isin(config.matched_ratio_sources()))
    ].copy()
    zero_points = main_points[main_points["target_co2_ppm"] == 0].copy()
    if zero_points.empty:
        empty_cols = [
            "analyzer_id",
            "ratio_source",
            "selected_source_pair",
            "point_title",
            "point_row",
            "temp_use_c",
            "A_zero_obs",
        ]
        return (
            pd.DataFrame(columns=empty_cols),
            pd.DataFrame(),
            pd.DataFrame(),
            {},
        )

    zero_points["selected_source_pair"] = zero_points["ratio_source"].map(config.matched_source_pair_label)
    zero_points = zero_points.rename(
        columns={
            "analyzer": "analyzer_id",
            "A_mean": "A_zero_obs",
        }
    )
    observation_cols = [
        "analyzer_id",
        "ratio_source",
        "selected_source_pair",
        "point_title",
        "point_row",
        "temp_set_c",
        "temp_use_mean_c",
        "A_zero_obs",
        "A_std",
        "sample_count",
    ]
    observations = zero_points[observation_cols].rename(columns={"temp_use_mean_c": "temp_use_c"}).copy()
    observations["zero_anchor_missing_at_40c"] = 40.0 not in sorted(observations["temp_set_c"].dropna().unique().tolist())

    fit_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []
    fit_lookup: dict[tuple[str, str, str], ZeroResidualFit] = {}
    requested_models = [
        ZERO_RESIDUAL_MODEL_SPECS[model_id]
        for model_id in getattr(config, "zero_residual_candidate_models", ("linear", "quadratic"))
        if model_id in ZERO_RESIDUAL_MODEL_SPECS
    ]

    for (analyzer_id, ratio_source), subset in observations.groupby(["analyzer_id", "ratio_source"], dropna=False):
        source_pair_label = config.matched_source_pair_label(str(ratio_source))
        baseline_metrics = _metrics(subset["A_zero_obs"])
        candidate_rows = [
            {
                "analyzer_id": analyzer_id,
                "ratio_source": ratio_source,
                "selected_source_pair": source_pair_label,
                "zero_residual_model": "none",
                "zero_residual_model_label": ZERO_RESIDUAL_NONE_LABEL,
                "formula": "ΔA0(T) = 0",
                "term_count": 0,
                "sample_count": int(len(subset)),
                "rmse_zero_absorbance": baseline_metrics["rmse"],
                "mae_zero_absorbance": baseline_metrics["mae"],
                "max_abs_zero_absorbance": baseline_metrics["max_abs_error"],
                "bias_zero_absorbance": baseline_metrics["bias"],
                "coefficients_desc": json.dumps([]),
                "breakpoint_temp_c": math.nan,
                "with_zero_residual_correction": False,
            }
        ]
        for spec in requested_models:
            breakpoint_temp_c = (
                float(getattr(config, "zero_residual_piecewise_break_temp_c", 20.0))
                if spec.model_id == "piecewise_linear"
                else None
            )
            try:
                fit = _fit_zero_model(subset, spec, breakpoint_temp_c)
            except Exception:
                continue
            fit_lookup[(str(analyzer_id), str(ratio_source), spec.model_id)] = fit
            candidate_rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "ratio_source": ratio_source,
                    "selected_source_pair": source_pair_label,
                    "zero_residual_model": spec.model_id,
                    "zero_residual_model_label": spec.model_label,
                    "formula": fit.formula_text(),
                    "term_count": len(spec.terms),
                    "sample_count": fit.sample_count,
                    "rmse_zero_absorbance": fit.rmse,
                    "mae_zero_absorbance": fit.mae,
                    "max_abs_zero_absorbance": fit.max_abs_error,
                    "bias_zero_absorbance": _metrics(fit.evaluate(subset["temp_use_c"]) - subset["A_zero_obs"])["bias"],
                    "coefficients_desc": json.dumps(list(fit.coefficients)),
                    "breakpoint_temp_c": fit.breakpoint_temp_c,
                    "with_zero_residual_correction": True,
                }
            )

        candidate_df = pd.DataFrame(candidate_rows).sort_values(
            ["rmse_zero_absorbance", "term_count", "zero_residual_model"],
            ignore_index=True,
        )
        selected = candidate_df.iloc[0]
        for _, row in candidate_df.iterrows():
            fit_rows.append(
                {
                    **row.to_dict(),
                    "is_selected_zero_residual_model": row["zero_residual_model"] == selected["zero_residual_model"],
                }
            )
        selection_rows.append(
            {
                "analyzer_id": analyzer_id,
                "ratio_source": ratio_source,
                "selected_source_pair": source_pair_label,
                "without_zero_residual_correction_rmse": float(
                    candidate_df[candidate_df["zero_residual_model"] == "none"].iloc[0]["rmse_zero_absorbance"]
                ),
                "with_zero_residual_correction_rmse": float(selected["rmse_zero_absorbance"]),
                "zero_residual_rmse_gain": float(
                    candidate_df[candidate_df["zero_residual_model"] == "none"].iloc[0]["rmse_zero_absorbance"]
                    - selected["rmse_zero_absorbance"]
                ),
                "selected_zero_residual_model": selected["zero_residual_model"],
                "selected_zero_residual_model_label": selected["zero_residual_model_label"],
                "selection_reason": (
                    f"Selected {selected['zero_residual_model']} by lowest zero-absorbance RMSE="
                    f"{float(selected['rmse_zero_absorbance']):.6g} on valid-only 0 ppm points."
                ),
                "with_zero_residual_correction": bool(selected["with_zero_residual_correction"]),
                "zero_anchor_missing_at_40c": bool(subset["zero_anchor_missing_at_40c"].any()),
            }
        )

    models = pd.DataFrame(fit_rows).sort_values(
        ["analyzer_id", "ratio_source", "rmse_zero_absorbance", "zero_residual_model"],
        ignore_index=True,
    )
    selection = pd.DataFrame(selection_rows).sort_values(
        ["analyzer_id", "selected_source_pair"],
        ignore_index=True,
    )
    return observations, models, selection, fit_lookup


def build_zero_residual_point_variants(
    absorbance_points: pd.DataFrame,
    fit_lookup: dict[tuple[str, str, str], ZeroResidualFit],
    config: Any,
) -> pd.DataFrame:
    """Create point-level absorbance variants with and without ΔA0(T) correction."""

    main_points = absorbance_points[
        (absorbance_points["temp_source"] == config.default_temp_source)
        & (absorbance_points["pressure_source"] == config.default_pressure_source)
        & (absorbance_points["r0_model"] == config.default_r0_model)
        & (absorbance_points["ratio_source"].isin(config.matched_ratio_sources()))
    ].copy()
    modes = available_zero_residual_modes(config)
    if main_points.empty:
        return main_points

    rows: list[pd.DataFrame] = []
    for mode in modes:
        mode_rows: list[pd.DataFrame] = []
        for (analyzer_id, ratio_source), subset in main_points.groupby(["analyzer", "ratio_source"], dropna=False):
            work = subset.copy()
            work["selected_source_pair"] = config.matched_source_pair_label(str(ratio_source))
            work["zero_residual_mode"] = mode
            work["zero_residual_model_label"] = (
                ZERO_RESIDUAL_NONE_LABEL
                if mode == "none"
                else ZERO_RESIDUAL_MODEL_SPECS[mode].model_label
            )
            work["with_zero_residual_correction"] = mode != "none"
            if mode == "none":
                delta = np.zeros(len(work), dtype=float)
            else:
                fit = fit_lookup.get((str(analyzer_id), str(ratio_source), mode))
                if fit is None:
                    continue
                delta = fit.evaluate(work["temp_use_mean_c"])
            work["delta_a0_t"] = delta
            work["A_uncorrected_mean"] = work["A_mean"]
            work["A_mean"] = work["A_mean"] - delta
            if "A_from_mean" in work.columns:
                work["A_from_mean_uncorrected"] = work["A_from_mean"]
                work["A_from_mean"] = work["A_from_mean"] - delta
            if "A_alt_mean" in work.columns:
                work["A_alt_mean_uncorrected"] = work["A_alt_mean"]
                work["A_alt_mean"] = work["A_alt_mean"] - (delta / float(config.p_ref_hpa))
            mode_rows.append(work)
        if mode_rows:
            rows.append(pd.concat(mode_rows, ignore_index=True))
    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True)
    return combined.sort_values(
        ["analyzer", "ratio_source", "zero_residual_mode", "point_row", "point_title"],
        ignore_index=True,
    )
