"""Water zero-anchor diagnostic correction helpers."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class WaterZeroAnchorFit:
    """One fitted diagnostic water-anchor correction model."""

    model_id: str
    model_label: str
    active_terms: tuple[str, ...]
    coefficients: tuple[float, ...]
    sample_count: int
    rmse: float
    mae: float
    max_abs_error: float

    def evaluate(self, frame: pd.DataFrame) -> np.ndarray:
        matrix = _design_matrix(frame, self.active_terms)
        return matrix @ np.asarray(self.coefficients, dtype=float)

    def formula_text(self, precision: int = 8) -> str:
        parts: list[str] = []
        for coefficient, term in zip(self.coefficients, self.active_terms, strict=False):
            coeff_text = f"{float(coefficient):.{precision}g}"
            if term == "intercept":
                parts.append(coeff_text)
                continue
            term_text = {
                "delta_sub": "d_sub",
                "delta_zero": "d_zeroC",
                "temp_use_c": "T",
                "delta_sub_sq": "d_sub^2",
                "delta_zero_sq": "d_zeroC^2",
                "temp_use_c_sq": "T^2",
            }.get(term, term)
            parts.append(f"{coeff_text}*{term_text}")
        return " + ".join(parts).replace("+ -", "- ")


WATER_ZERO_ANCHOR_MODEL_LABELS: dict[str, str] = {
    "linear": "Linear water zero-anchor correction",
    "quadratic": "Quadratic water zero-anchor correction",
}
WATER_ZERO_ANCHOR_NONE_LABEL = "No water zero-anchor correction"


def available_water_zero_anchor_modes(config: Any) -> tuple[str, ...]:
    """Return water-anchor modes evaluated in this run."""

    modes = ["none"]
    if getattr(config, "enable_water_zero_anchor_correction", True):
        modes.extend(list(getattr(config, "water_zero_anchor_candidate_models", ("linear", "quadratic"))))
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


def _active_terms(frame: pd.DataFrame, model_id: str) -> tuple[str, ...]:
    base_terms = ["intercept"]
    if pd.to_numeric(frame.get("delta_h2o_ratio_vs_subzero_anchor"), errors="coerce").notna().any():
        base_terms.append("delta_sub")
    if pd.to_numeric(frame.get("delta_h2o_ratio_vs_zeroC_anchor"), errors="coerce").notna().any():
        base_terms.append("delta_zero")
    if len(base_terms) == 1:
        return tuple()
    base_terms.append("temp_use_c")
    if model_id == "quadratic":
        quadratic_terms: list[str] = []
        if "delta_sub" in base_terms:
            quadratic_terms.append("delta_sub_sq")
        if "delta_zero" in base_terms:
            quadratic_terms.append("delta_zero_sq")
        quadratic_terms.append("temp_use_c_sq")
        base_terms.extend(quadratic_terms)
    return tuple(base_terms)


def _design_matrix(frame: pd.DataFrame, terms: tuple[str, ...]) -> np.ndarray:
    columns: list[np.ndarray] = []
    for term in terms:
        if term == "intercept":
            columns.append(np.ones(len(frame), dtype=float))
        elif term == "delta_sub":
            columns.append(pd.to_numeric(frame["delta_h2o_ratio_vs_subzero_anchor"], errors="coerce").to_numpy(dtype=float))
        elif term == "delta_zero":
            columns.append(pd.to_numeric(frame["delta_h2o_ratio_vs_zeroC_anchor"], errors="coerce").to_numpy(dtype=float))
        elif term == "temp_use_c":
            columns.append(pd.to_numeric(frame["temp_use_c"], errors="coerce").to_numpy(dtype=float))
        elif term == "delta_sub_sq":
            columns.append(np.square(pd.to_numeric(frame["delta_h2o_ratio_vs_subzero_anchor"], errors="coerce").to_numpy(dtype=float)))
        elif term == "delta_zero_sq":
            columns.append(np.square(pd.to_numeric(frame["delta_h2o_ratio_vs_zeroC_anchor"], errors="coerce").to_numpy(dtype=float)))
        elif term == "temp_use_c_sq":
            columns.append(np.square(pd.to_numeric(frame["temp_use_c"], errors="coerce").to_numpy(dtype=float)))
        else:  # pragma: no cover - bounded by _active_terms
            raise ValueError(f"Unsupported water-anchor term: {term}")
    return np.column_stack(columns)


def _fit_model(
    observations: pd.DataFrame,
    model_id: str,
) -> WaterZeroAnchorFit:
    active_terms = _active_terms(observations, model_id)
    if not active_terms:
        raise ValueError("water-anchor fit needs at least one water-delta feature")
    required_columns = ["A_zero_obs", "temp_use_c"]
    if "delta_sub" in active_terms or "delta_sub_sq" in active_terms:
        required_columns.append("delta_h2o_ratio_vs_subzero_anchor")
    if "delta_zero" in active_terms or "delta_zero_sq" in active_terms:
        required_columns.append("delta_h2o_ratio_vs_zeroC_anchor")
    working = observations.dropna(subset=required_columns).copy()
    if len(working) < len(active_terms):
        raise ValueError(f"{model_id} water-anchor fit needs at least {len(active_terms)} zero observations")
    matrix = _design_matrix(working, active_terms)
    y = pd.to_numeric(working["A_zero_obs"], errors="coerce").to_numpy(dtype=float)
    coefficients, _, _, _ = np.linalg.lstsq(matrix, y, rcond=None)
    predicted = matrix @ coefficients
    metrics = _metrics(predicted - y)
    return WaterZeroAnchorFit(
        model_id=model_id,
        model_label=WATER_ZERO_ANCHOR_MODEL_LABELS.get(model_id, model_id),
        active_terms=active_terms,
        coefficients=tuple(float(item) for item in coefficients),
        sample_count=int(len(working)),
        rmse=metrics["rmse"],
        mae=metrics["mae"],
        max_abs_error=metrics["max_abs_error"],
    )


def build_water_zero_anchor_features(
    point_variants: pd.DataFrame,
    filtered_samples: pd.DataFrame,
) -> pd.DataFrame:
    """Build point-level water-anchor features on top of the zero-residual variants."""

    if point_variants.empty:
        return pd.DataFrame()

    point_water = (
        filtered_samples.groupby(
            ["analyzer", "point_title", "point_row", "temp_set_c", "target_co2_ppm"],
            dropna=False,
        )
        .agg(
            h2o_ratio_raw_mean=("ratio_h2o_raw", "mean"),
            h2o_ratio_filt_mean=("ratio_h2o_filt", "mean"),
            h2o_signal_mean=("h2o_signal", "mean"),
            h2o_density_mean=("h2o_density", "mean"),
            h2o_sample_count=("sample_index", "count"),
        )
        .reset_index()
    )
    merged = point_variants.merge(
        point_water,
        on=["analyzer", "point_title", "point_row", "temp_set_c", "target_co2_ppm"],
        how="left",
    ).copy()
    merged = merged.rename(
        columns={
            "analyzer": "analyzer_id",
            "A_mean": "A_zero_ready",
            "target_co2_ppm": "target_ppm",
            "temp_use_mean_c": "temp_use_c",
        }
    )

    rows: list[pd.DataFrame] = []
    for analyzer_id, analyzer_df in merged.groupby("analyzer_id", dropna=False):
        anchor_base = analyzer_df[
            [
                "point_title",
                "point_row",
                "temp_set_c",
                "target_ppm",
                "h2o_ratio_raw_mean",
                "h2o_ratio_filt_mean",
            ]
        ].drop_duplicates().copy()
        zero_base = anchor_base[pd.to_numeric(anchor_base["target_ppm"], errors="coerce") == 0].copy()
        subzero = zero_base[pd.to_numeric(zero_base["temp_set_c"], errors="coerce") < 0].copy()
        zero_c = zero_base[pd.to_numeric(zero_base["temp_set_c"], errors="coerce") == 0].sort_values(
            ["point_row", "point_title"],
            ignore_index=True,
        )
        subzero_raw = pd.to_numeric(subzero["h2o_ratio_raw_mean"], errors="coerce").dropna()
        subzero_filt = pd.to_numeric(subzero["h2o_ratio_filt_mean"], errors="coerce").dropna()
        zero_c_raw = pd.to_numeric(zero_c["h2o_ratio_raw_mean"], errors="coerce").dropna()
        zero_c_filt = pd.to_numeric(zero_c["h2o_ratio_filt_mean"], errors="coerce").dropna()

        subzero_raw_anchor = float(subzero_raw.mean()) if not subzero_raw.empty else math.nan
        subzero_filt_anchor = float(subzero_filt.mean()) if not subzero_filt.empty else math.nan
        zero_c_raw_anchor = float(zero_c_raw.iloc[0]) if not zero_c_raw.empty else math.nan
        zero_c_filt_anchor = float(zero_c_filt.iloc[0]) if not zero_c_filt.empty else math.nan
        zero_c_point_row = float(zero_c.iloc[0]["point_row"]) if not zero_c.empty and pd.notna(zero_c.iloc[0]["point_row"]) else math.nan
        subzero_rows = "|".join(str(int(row)) for row in pd.to_numeric(subzero["point_row"], errors="coerce").dropna().astype(int).tolist())

        work = analyzer_df.copy()
        work["water_ratio_source"] = work["ratio_source"].map(
            {
                "ratio_co2_raw": "ratio_h2o_raw",
                "ratio_co2_filt": "ratio_h2o_filt",
            }
        )
        work["water_ratio_mean"] = np.where(
            work["ratio_source"] == "ratio_co2_raw",
            pd.to_numeric(work["h2o_ratio_raw_mean"], errors="coerce"),
            pd.to_numeric(work["h2o_ratio_filt_mean"], errors="coerce"),
        )
        work["subzero_zero_water_ratio_anchor"] = np.where(
            work["ratio_source"] == "ratio_co2_raw",
            subzero_raw_anchor,
            subzero_filt_anchor,
        )
        work["zeroC_first_zero_water_ratio_anchor"] = np.where(
            work["ratio_source"] == "ratio_co2_raw",
            zero_c_raw_anchor,
            zero_c_filt_anchor,
        )
        work["delta_h2o_ratio_vs_subzero_anchor"] = work["water_ratio_mean"] - work["subzero_zero_water_ratio_anchor"]
        work["delta_h2o_ratio_vs_zeroC_anchor"] = work["water_ratio_mean"] - work["zeroC_first_zero_water_ratio_anchor"]
        work["subzero_anchor_available"] = pd.notna(work["subzero_zero_water_ratio_anchor"])
        work["zeroC_anchor_available"] = pd.notna(work["zeroC_first_zero_water_ratio_anchor"])
        work["zeroC_first_zero_point_row"] = zero_c_point_row
        work["subzero_anchor_point_rows"] = subzero_rows
        work["feature_status"] = np.select(
            [
                work["subzero_anchor_available"] & work["zeroC_anchor_available"],
                work["subzero_anchor_available"],
                work["zeroC_anchor_available"],
            ],
            [
                "subzero_and_zeroC_available",
                "subzero_only",
                "zeroC_only",
            ],
            default="feature_unavailable",
        )
        work["anchor_note"] = np.where(
            work["feature_status"] == "feature_unavailable",
            "feature unavailable because no valid water zero anchor was identified for this analyzer/source pair",
            "water zero-anchor features were derived from valid-only zero-gas points in the same run",
        )
        rows.append(work)

    combined = pd.concat(rows, ignore_index=True) if rows else merged
    combined = combined.rename(columns={"A_zero_ready": "A_mean"})
    combined["A_zero_obs"] = np.where(
        pd.to_numeric(combined["target_ppm"], errors="coerce") == 0,
        pd.to_numeric(combined["A_mean"], errors="coerce"),
        np.nan,
    )
    return combined.sort_values(
        ["analyzer_id", "ratio_source", "zero_residual_mode", "point_row", "point_title"],
        ignore_index=True,
    )


def fit_water_zero_anchor_models(
    feature_frame: pd.DataFrame,
    config: Any,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[tuple[str, str, str, str], WaterZeroAnchorFit]]:
    """Fit water-anchor diagnostic models from valid-only zero points."""

    if feature_frame.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    zero_features = feature_frame[pd.to_numeric(feature_frame["target_ppm"], errors="coerce") == 0].copy()
    fit_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []
    fit_lookup: dict[tuple[str, str, str, str], WaterZeroAnchorFit] = {}

    candidate_modes = [
        mode
        for mode in getattr(config, "water_zero_anchor_candidate_models", ("linear", "quadratic"))
        if mode in WATER_ZERO_ANCHOR_MODEL_LABELS
    ]

    for (analyzer_id, ratio_source, zero_mode), subset in zero_features.groupby(
        ["analyzer_id", "ratio_source", "zero_residual_mode"],
        dropna=False,
    ):
        source_pair_label = str(subset["selected_source_pair"].iloc[0]) if "selected_source_pair" in subset.columns else ""
        feature_status = str(subset["feature_status"].iloc[0]) if "feature_status" in subset.columns else "feature_unavailable"
        baseline_metrics = _metrics(subset["A_zero_obs"])
        candidate_rows = [
            {
                "analyzer_id": analyzer_id,
                "ratio_source": ratio_source,
                "selected_source_pair": source_pair_label,
                "zero_residual_mode": zero_mode,
                "water_zero_anchor_model": "none",
                "water_zero_anchor_model_label": WATER_ZERO_ANCHOR_NONE_LABEL,
                "formula": "g(water_zero_features) = 0",
                "term_count": 0,
                "active_terms": json.dumps([]),
                "sample_count": int(len(subset)),
                "rmse_zero_absorbance": baseline_metrics["rmse"],
                "mae_zero_absorbance": baseline_metrics["mae"],
                "max_abs_zero_absorbance": baseline_metrics["max_abs_error"],
                "bias_zero_absorbance": baseline_metrics["bias"],
                "coefficients_desc": json.dumps([]),
                "feature_status": feature_status,
                "fit_status": "ok",
                "with_water_zero_anchor_correction": False,
            }
        ]
        for model_id in candidate_modes:
            try:
                fit = _fit_model(subset, model_id)
            except Exception as exc:
                candidate_rows.append(
                    {
                        "analyzer_id": analyzer_id,
                        "ratio_source": ratio_source,
                        "selected_source_pair": source_pair_label,
                        "zero_residual_mode": zero_mode,
                        "water_zero_anchor_model": model_id,
                        "water_zero_anchor_model_label": WATER_ZERO_ANCHOR_MODEL_LABELS[model_id],
                        "formula": "",
                        "term_count": math.nan,
                        "active_terms": json.dumps([]),
                        "sample_count": int(len(subset)),
                        "rmse_zero_absorbance": math.nan,
                        "mae_zero_absorbance": math.nan,
                        "max_abs_zero_absorbance": math.nan,
                        "bias_zero_absorbance": math.nan,
                        "coefficients_desc": json.dumps([]),
                        "feature_status": feature_status,
                        "fit_status": str(exc),
                        "with_water_zero_anchor_correction": True,
                    }
                )
                continue
            fit_lookup[(str(analyzer_id), str(ratio_source), str(zero_mode), model_id)] = fit
            fit_rows_metrics = _metrics(fit.evaluate(subset.dropna(subset=["temp_use_c"])) - pd.to_numeric(subset.dropna(subset=["temp_use_c"])["A_zero_obs"], errors="coerce"))
            candidate_rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "ratio_source": ratio_source,
                    "selected_source_pair": source_pair_label,
                    "zero_residual_mode": zero_mode,
                    "water_zero_anchor_model": model_id,
                    "water_zero_anchor_model_label": fit.model_label,
                    "formula": fit.formula_text(),
                    "term_count": len(fit.active_terms),
                    "active_terms": json.dumps(list(fit.active_terms)),
                    "sample_count": fit.sample_count,
                    "rmse_zero_absorbance": fit.rmse,
                    "mae_zero_absorbance": fit.mae,
                    "max_abs_zero_absorbance": fit.max_abs_error,
                    "bias_zero_absorbance": fit_rows_metrics["bias"],
                    "coefficients_desc": json.dumps(list(fit.coefficients)),
                    "feature_status": feature_status,
                    "fit_status": "ok",
                    "with_water_zero_anchor_correction": True,
                }
            )

        candidate_df = pd.DataFrame(candidate_rows).sort_values(
            ["rmse_zero_absorbance", "term_count", "water_zero_anchor_model"],
            ignore_index=True,
            na_position="last",
        )
        selected = candidate_df.iloc[0]
        for _, row in candidate_df.iterrows():
            fit_rows.append(
                {
                    **row.to_dict(),
                    "is_selected_water_zero_anchor_model": row["water_zero_anchor_model"] == selected["water_zero_anchor_model"],
                }
            )
        none_row = candidate_df[candidate_df["water_zero_anchor_model"] == "none"].iloc[0]
        selection_rows.append(
            {
                "analyzer_id": analyzer_id,
                "ratio_source": ratio_source,
                "selected_source_pair": source_pair_label,
                "zero_residual_mode": zero_mode,
                "without_water_zero_anchor_correction_rmse": float(none_row["rmse_zero_absorbance"]),
                "with_water_zero_anchor_correction_rmse": float(selected["rmse_zero_absorbance"]) if pd.notna(selected["rmse_zero_absorbance"]) else math.nan,
                "water_zero_anchor_rmse_gain": (
                    float(none_row["rmse_zero_absorbance"]) - float(selected["rmse_zero_absorbance"])
                    if pd.notna(selected["rmse_zero_absorbance"])
                    else math.nan
                ),
                "selected_water_zero_anchor_model": selected["water_zero_anchor_model"],
                "selected_water_zero_anchor_model_label": selected["water_zero_anchor_model_label"],
                "feature_status": feature_status,
                "selection_reason": (
                    f"Selected {selected['water_zero_anchor_model']} by lowest zero-absorbance RMSE="
                    f"{float(selected['rmse_zero_absorbance']):.6g} on valid-only 0 ppm points."
                    if pd.notna(selected["rmse_zero_absorbance"])
                    else "No usable water-anchor fit was available; debugger kept the uncorrected branch."
                ),
                "with_water_zero_anchor_correction": bool(selected["with_water_zero_anchor_correction"]),
            }
        )

    models = pd.DataFrame(fit_rows).sort_values(
        ["analyzer_id", "ratio_source", "zero_residual_mode", "water_zero_anchor_model"],
        ignore_index=True,
    )
    selection = pd.DataFrame(selection_rows).sort_values(
        ["analyzer_id", "ratio_source", "zero_residual_mode"],
        ignore_index=True,
    )
    return models, selection, fit_lookup


def build_water_zero_anchor_point_variants(
    feature_frame: pd.DataFrame,
    fit_lookup: dict[tuple[str, str, str, str], WaterZeroAnchorFit],
    config: Any,
) -> pd.DataFrame:
    """Create point variants with and without the diagnostic water-anchor correction."""

    if feature_frame.empty:
        return feature_frame

    rows: list[pd.DataFrame] = []
    for mode in available_water_zero_anchor_modes(config):
        mode_rows: list[pd.DataFrame] = []
        for (analyzer_id, ratio_source, zero_mode), subset in feature_frame.groupby(
            ["analyzer_id", "ratio_source", "zero_residual_mode"],
            dropna=False,
        ):
            work = subset.copy()
            work["water_zero_anchor_mode"] = mode
            work["water_zero_anchor_model_label"] = (
                WATER_ZERO_ANCHOR_NONE_LABEL if mode == "none" else WATER_ZERO_ANCHOR_MODEL_LABELS.get(mode, mode)
            )
            work["with_water_zero_anchor_correction"] = mode != "none"
            if mode == "none":
                delta = np.zeros(len(work), dtype=float)
            else:
                fit = fit_lookup.get((str(analyzer_id), str(ratio_source), str(zero_mode), mode))
                if fit is None:
                    continue
                required_columns = ["temp_use_c"]
                if "delta_sub" in fit.active_terms or "delta_sub_sq" in fit.active_terms:
                    required_columns.append("delta_h2o_ratio_vs_subzero_anchor")
                if "delta_zero" in fit.active_terms or "delta_zero_sq" in fit.active_terms:
                    required_columns.append("delta_h2o_ratio_vs_zeroC_anchor")
                if work[required_columns].dropna().empty:
                    continue
                delta = fit.evaluate(work)
            work["water_zero_anchor_delta"] = delta
            work["A_before_water_zero_anchor"] = work["A_mean"]
            work["A_mean"] = pd.to_numeric(work["A_mean"], errors="coerce") - delta
            if "A_from_mean" in work.columns:
                work["A_from_mean_before_water_zero_anchor"] = work["A_from_mean"]
                work["A_from_mean"] = pd.to_numeric(work["A_from_mean"], errors="coerce") - delta
            if "A_alt_mean" in work.columns:
                work["A_alt_mean_before_water_zero_anchor"] = work["A_alt_mean"]
                work["A_alt_mean"] = pd.to_numeric(work["A_alt_mean"], errors="coerce") - (delta / float(config.p_ref_hpa))
            mode_rows.append(work)
        if mode_rows:
            rows.append(pd.concat(mode_rows, ignore_index=True))

    combined = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if combined.empty:
        return combined
    return combined.sort_values(
        ["analyzer_id", "ratio_source", "zero_residual_mode", "water_zero_anchor_mode", "point_row", "point_title"],
        ignore_index=True,
    )


def _winner(base_value: float, candidate_value: float) -> str:
    if math.isnan(base_value) and math.isnan(candidate_value):
        return "tie"
    if math.isnan(base_value):
        return "water_zero_anchor_chain"
    if math.isnan(candidate_value):
        return "baseline_chain"
    if abs(base_value - candidate_value) <= 1.0e-12:
        return "tie"
    return "baseline_chain" if base_value < candidate_value else "water_zero_anchor_chain"


def _lookup_range_rmse(frame: pd.DataFrame, analyzer_id: str, range_name: str) -> float:
    subset = frame[
        (frame["analyzer_id"] == analyzer_id)
        & (frame["concentration_range"] == range_name)
    ]
    if subset.empty:
        return math.nan
    value = pd.to_numeric(subset.iloc[0]["new_rmse"], errors="coerce")
    return float(value) if pd.notna(value) else math.nan


def _lookup_temp_rmse(frame: pd.DataFrame, analyzer_id: str, temp_c: float) -> float:
    subset = frame[
        (frame["analyzer_id"] == analyzer_id)
        & (pd.to_numeric(frame["temp_c"], errors="coerce") == float(temp_c))
    ]
    if subset.empty:
        return math.nan
    value = pd.to_numeric(subset.iloc[0]["new_rmse"], errors="coerce")
    return float(value) if pd.notna(value) else math.nan


def _selection_value(selection: pd.DataFrame, analyzer_id: str, column: str) -> Any:
    subset = selection[selection["analyzer_id"] == analyzer_id]
    if subset.empty or column not in subset.columns:
        return ""
    value = subset.iloc[0][column]
    return "" if pd.isna(value) else value


def build_water_anchor_compare(
    baseline_outputs: dict[str, Any],
    water_outputs: dict[str, Any],
    baseline_selection: pd.DataFrame,
    water_selection: pd.DataFrame,
) -> pd.DataFrame:
    """Compare the baseline chain against the diagnostic water-anchor branch."""

    baseline_overview = baseline_outputs.get("overview_summary", pd.DataFrame())
    water_overview = water_outputs.get("overview_summary", pd.DataFrame())
    analyzers = sorted(
        set(baseline_overview.get("analyzer_id", pd.Series(dtype=str)).dropna().astype(str).tolist())
        | set(water_overview.get("analyzer_id", pd.Series(dtype=str)).dropna().astype(str).tolist())
    )
    rows: list[dict[str, Any]] = []
    for analyzer_id in analyzers:
        base_row = baseline_overview[baseline_overview["analyzer_id"] == analyzer_id]
        water_row = water_overview[water_overview["analyzer_id"] == analyzer_id]
        if base_row.empty and water_row.empty:
            continue
        base_row = base_row.iloc[0] if not base_row.empty else pd.Series(dtype=object)
        water_row = water_row.iloc[0] if not water_row.empty else pd.Series(dtype=object)

        baseline_overall = float(pd.to_numeric(base_row.get("new_chain_rmse"), errors="coerce")) if not base_row.empty else math.nan
        water_overall = float(pd.to_numeric(water_row.get("new_chain_rmse"), errors="coerce")) if not water_row.empty else math.nan
        baseline_zero = float(pd.to_numeric(base_row.get("new_zero_rmse"), errors="coerce")) if not base_row.empty else math.nan
        water_zero = float(pd.to_numeric(water_row.get("new_zero_rmse"), errors="coerce")) if not water_row.empty else math.nan
        baseline_temp = float(pd.to_numeric(base_row.get("new_temp_stability_metric"), errors="coerce")) if not base_row.empty else math.nan
        water_temp = float(pd.to_numeric(water_row.get("new_temp_stability_metric"), errors="coerce")) if not water_row.empty else math.nan
        baseline_max = float(pd.to_numeric(base_row.get("new_chain_max_abs_error"), errors="coerce")) if not base_row.empty else math.nan
        water_max = float(pd.to_numeric(water_row.get("new_chain_max_abs_error"), errors="coerce")) if not water_row.empty else math.nan

        rows.append(
            {
                "analyzer_id": analyzer_id,
                "old_chain_rmse": float(pd.to_numeric(base_row.get("old_chain_rmse"), errors="coerce")) if not base_row.empty else math.nan,
                "old_zero_rmse": float(pd.to_numeric(base_row.get("old_zero_rmse"), errors="coerce")) if not base_row.empty else math.nan,
                "old_temp_stability_metric": float(pd.to_numeric(base_row.get("old_temp_stability_metric"), errors="coerce")) if not base_row.empty else math.nan,
                "baseline_overall_rmse": baseline_overall,
                "water_anchor_overall_rmse": water_overall,
                "baseline_zero_rmse": baseline_zero,
                "water_anchor_zero_rmse": water_zero,
                "baseline_temp_bias_spread": baseline_temp,
                "water_anchor_temp_bias_spread": water_temp,
                "baseline_max_abs_error": baseline_max,
                "water_anchor_max_abs_error": water_max,
                "winner_overall": _winner(baseline_overall, water_overall),
                "winner_zero": _winner(baseline_zero, water_zero),
                "winner_temp_stability": _winner(baseline_temp, water_temp),
                "winner_max_abs_error": _winner(baseline_max, water_max),
                "baseline_gap_to_old_overall": baseline_overall - float(pd.to_numeric(base_row.get("old_chain_rmse"), errors="coerce")) if not base_row.empty else math.nan,
                "water_anchor_gap_to_old_overall": water_overall - float(pd.to_numeric(base_row.get("old_chain_rmse"), errors="coerce")) if not base_row.empty else math.nan,
                "baseline_gap_to_old_zero": baseline_zero - float(pd.to_numeric(base_row.get("old_zero_rmse"), errors="coerce")) if not base_row.empty else math.nan,
                "water_anchor_gap_to_old_zero": water_zero - float(pd.to_numeric(base_row.get("old_zero_rmse"), errors="coerce")) if not base_row.empty else math.nan,
                "baseline_gap_to_old_temp": baseline_temp - float(pd.to_numeric(base_row.get("old_temp_stability_metric"), errors="coerce")) if not base_row.empty else math.nan,
                "water_anchor_gap_to_old_temp": water_temp - float(pd.to_numeric(base_row.get("old_temp_stability_metric"), errors="coerce")) if not base_row.empty else math.nan,
                "baseline_low_range_rmse": _lookup_range_rmse(baseline_outputs.get("by_concentration_range", pd.DataFrame()), analyzer_id, "0~200 ppm"),
                "water_anchor_low_range_rmse": _lookup_range_rmse(water_outputs.get("by_concentration_range", pd.DataFrame()), analyzer_id, "0~200 ppm"),
                "baseline_main_range_rmse": _lookup_range_rmse(baseline_outputs.get("by_concentration_range", pd.DataFrame()), analyzer_id, "200~1000 ppm"),
                "water_anchor_main_range_rmse": _lookup_range_rmse(water_outputs.get("by_concentration_range", pd.DataFrame()), analyzer_id, "200~1000 ppm"),
                "baseline_new_rmse_at_40c": _lookup_temp_rmse(baseline_outputs.get("by_temperature", pd.DataFrame()), analyzer_id, 40.0),
                "water_anchor_new_rmse_at_40c": _lookup_temp_rmse(water_outputs.get("by_temperature", pd.DataFrame()), analyzer_id, 40.0),
                "baseline_selected_source_pair": _selection_value(baseline_selection, analyzer_id, "selected_source_pair"),
                "baseline_zero_residual_mode": _selection_value(baseline_selection, analyzer_id, "zero_residual_mode"),
                "water_anchor_selected_source_pair": _selection_value(water_selection, analyzer_id, "selected_source_pair"),
                "water_anchor_zero_residual_mode": _selection_value(water_selection, analyzer_id, "zero_residual_mode"),
                "water_zero_anchor_mode": _selection_value(water_selection, analyzer_id, "water_zero_anchor_mode"),
                "water_zero_anchor_model_label": _selection_value(water_selection, analyzer_id, "water_zero_anchor_model_label"),
                "water_feature_status": _selection_value(water_selection, analyzer_id, "water_feature_status"),
                "water_selection_reason": _selection_value(water_selection, analyzer_id, "selection_reason"),
            }
        )
    return pd.DataFrame(rows).sort_values(["analyzer_id"], ignore_index=True)
