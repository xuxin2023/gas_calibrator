"""Absorbance-to-ppm candidate models, grouped validation, and selection."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class AbsorbanceModelSpec:
    """One bounded absorbance calibration candidate."""

    model_id: str
    model_label: str
    formula: str
    terms: tuple[str, ...]


MODEL_SPECS: tuple[AbsorbanceModelSpec, ...] = (
    AbsorbanceModelSpec(
        model_id="model_a_linear",
        model_label="Model A: linear",
        formula="ppm = b0 + b1*A",
        terms=("intercept", "A"),
    ),
    AbsorbanceModelSpec(
        model_id="model_b_quadratic",
        model_label="Model B: quadratic",
        formula="ppm = b0 + b1*A + b2*A^2",
        terms=("intercept", "A", "A^2"),
    ),
    AbsorbanceModelSpec(
        model_id="model_c_cubic",
        model_label="Model C: cubic",
        formula="ppm = b0 + b1*A + b2*A^2 + b3*A^3",
        terms=("intercept", "A", "A^2", "A^3"),
    ),
    AbsorbanceModelSpec(
        model_id="model_d_temp_terms",
        model_label="Model D: temperature terms",
        formula="ppm = b0 + b1*A + b2*A^2 + b3*T + b4*T^2",
        terms=("intercept", "A", "A^2", "T", "T^2"),
    ),
    AbsorbanceModelSpec(
        model_id="model_e_cross_term",
        model_label="Model E: cross term",
        formula="ppm = b0 + b1*A + b2*A^2 + b3*T + b4*A*T",
        terms=("intercept", "A", "A^2", "T", "A*T"),
    ),
)


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


def _temp_bias_spread(frame: pd.DataFrame, error_column: str) -> float:
    if frame.empty or error_column not in frame.columns:
        return math.nan
    grouped = (
        frame.dropna(subset=["temp_c", error_column])
        .groupby("temp_c", dropna=False)[error_column]
        .mean()
    )
    if grouped.empty:
        return math.nan
    return float(np.std(grouped.to_numpy(dtype=float)))


def _score_metric(value: float) -> float:
    if value is None or not math.isfinite(value):
        return 1.0e9
    return float(value)


def _composite_score(metric_source: dict[str, float], weights: dict[str, float]) -> float:
    total = 0.0
    for metric_name, weight in weights.items():
        total += float(weight) * _score_metric(metric_source.get(metric_name, math.nan))
    return total


def _design_matrix(frame: pd.DataFrame, spec: AbsorbanceModelSpec) -> np.ndarray:
    absorbance = frame["A_mean"].to_numpy(dtype=float)
    temperature = frame["temp_model_c"].to_numpy(dtype=float)
    columns: list[np.ndarray] = []
    for term in spec.terms:
        if term == "intercept":
            columns.append(np.ones(len(frame), dtype=float))
        elif term == "A":
            columns.append(absorbance)
        elif term == "A^2":
            columns.append(np.square(absorbance))
        elif term == "A^3":
            columns.append(np.power(absorbance, 3))
        elif term == "T":
            columns.append(temperature)
        elif term == "T^2":
            columns.append(np.square(temperature))
        elif term == "A*T":
            columns.append(absorbance * temperature)
        else:  # pragma: no cover - bounded by MODEL_SPECS
            raise ValueError(f"Unsupported model term: {term}")
    return np.column_stack(columns)


def _fit_lstsq(frame: pd.DataFrame, spec: AbsorbanceModelSpec) -> tuple[np.ndarray, np.ndarray]:
    x = _design_matrix(frame, spec)
    y = frame["target_ppm"].to_numpy(dtype=float)
    coeffs, _, _, _ = np.linalg.lstsq(x, y, rcond=None)
    return coeffs, x @ coeffs


def _folds(group_ids: list[str], strategy: str) -> tuple[str, list[list[str]]]:
    if len(group_ids) < 2:
        return "none", []
    if strategy == "grouped_loo":
        return "grouped_loo", [[group_id] for group_id in group_ids]
    if strategy == "grouped_kfold":
        fold_count = min(5, len(group_ids))
        return "grouped_kfold", [list(chunk) for chunk in np.array_split(np.asarray(group_ids, dtype=object), fold_count)]
    if strategy == "auto_grouped":
        if len(group_ids) >= 5:
            fold_count = min(5, len(group_ids))
            return "grouped_kfold", [list(chunk) for chunk in np.array_split(np.asarray(group_ids, dtype=object), fold_count)]
        return "grouped_loo", [[group_id] for group_id in group_ids]
    raise ValueError(f"Unsupported model selection strategy: {strategy}")


def _fit_one_candidate(
    analyzer_df: pd.DataFrame,
    spec: AbsorbanceModelSpec,
    strategy: str,
    score_weights: dict[str, float],
    enable_composite_score: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    candidate_df = analyzer_df.dropna(subset=["A_mean", "temp_model_c", "target_ppm"]).copy()
    candidate_df = candidate_df.sort_values(["temp_c", "target_ppm", "point_row", "point_title"]).reset_index(drop=True)
    if len(candidate_df) < len(spec.terms):
        raise ValueError(f"{spec.model_id} needs at least {len(spec.terms)} rows, got {len(candidate_df)}")

    coeffs, overall_pred = _fit_lstsq(candidate_df, spec)
    overall_errors = overall_pred - candidate_df["target_ppm"].to_numpy(dtype=float)
    candidate_df["overall_fit_pred_ppm"] = overall_pred
    candidate_df["overall_fit_error_ppm"] = overall_errors

    group_ids = candidate_df["group_key"].dropna().astype(str).drop_duplicates().tolist()
    strategy_used, folds = _folds(group_ids, strategy)
    validation_pred = np.full(len(candidate_df), np.nan, dtype=float)
    train_frames: list[pd.DataFrame] = []

    for fold_index, val_groups in enumerate(folds, start=1):
        validation_mask = candidate_df["group_key"].isin(val_groups)
        train_df = candidate_df[~validation_mask].copy()
        validation_df = candidate_df[validation_mask].copy()
        if train_df.empty or validation_df.empty or len(train_df) < len(spec.terms):
            continue
        fold_coeffs, train_pred = _fit_lstsq(train_df, spec)
        validation_pred_local = _design_matrix(validation_df, spec) @ fold_coeffs
        validation_pred[validation_df.index.to_numpy()] = validation_pred_local
        fold_train = train_df.copy()
        fold_train["prediction_scope"] = f"train_fold_{fold_index}"
        fold_train["predicted_ppm"] = train_pred
        fold_train["error_ppm"] = train_pred - fold_train["target_ppm"].to_numpy(dtype=float)
        train_frames.append(fold_train)

    candidate_df["validation_oof_pred_ppm"] = validation_pred
    candidate_df["validation_oof_error_ppm"] = candidate_df["validation_oof_pred_ppm"] - candidate_df["target_ppm"]

    validation_frame = candidate_df.dropna(subset=["validation_oof_pred_ppm"]).copy()
    train_concat = pd.concat(train_frames, ignore_index=True) if train_frames else pd.DataFrame()

    train_metrics = _metrics(train_concat["error_ppm"]) if not train_concat.empty else _metrics(pd.Series(dtype=float))
    validation_metrics = _metrics(validation_frame["validation_oof_error_ppm"])
    overall_metrics = _metrics(candidate_df["overall_fit_error_ppm"])

    score_source_name = "validation_oof" if not validation_frame.empty else "overall_fit"
    score_source_frame = validation_frame if not validation_frame.empty else candidate_df
    score_source_error_col = "validation_oof_error_ppm" if not validation_frame.empty else "overall_fit_error_ppm"
    zero_frame = score_source_frame[score_source_frame["target_ppm"] == 0].copy()
    zero_rmse = _metrics(zero_frame[score_source_error_col])["rmse"]
    temp_bias_spread = _temp_bias_spread(score_source_frame, score_source_error_col)
    max_abs_error = _metrics(score_source_frame[score_source_error_col])["max_abs_error"]

    composite_inputs = {
        "overall_rmse": _metrics(score_source_frame[score_source_error_col])["rmse"],
        "zero_rmse": zero_rmse,
        "temp_bias_spread": temp_bias_spread,
        "max_abs_error": max_abs_error,
    }
    composite_score = (
        _composite_score(composite_inputs, score_weights)
        if enable_composite_score
        else _score_metric(validation_metrics["rmse"] if not validation_frame.empty else overall_metrics["rmse"])
    )

    score_row = {
        "analyzer_id": str(candidate_df["analyzer"].iloc[0]),
        "model_id": spec.model_id,
        "model_label": spec.model_label,
        "formula": spec.formula,
        "term_count": len(spec.terms),
        "sample_count": int(len(candidate_df)),
        "group_count": int(len(group_ids)),
        "validation_fold_count": int(len(folds)),
        "selection_strategy_used": strategy_used,
        "score_source": score_source_name,
        "train_rmse": train_metrics["rmse"],
        "train_mae": train_metrics["mae"],
        "train_max_abs_error": train_metrics["max_abs_error"],
        "validation_rmse": validation_metrics["rmse"],
        "validation_mae": validation_metrics["mae"],
        "validation_max_abs_error": validation_metrics["max_abs_error"],
        "overall_rmse": overall_metrics["rmse"],
        "overall_mae": overall_metrics["mae"],
        "overall_max_abs_error": overall_metrics["max_abs_error"],
        "overall_bias": overall_metrics["bias"],
        "zero_rmse": zero_rmse,
        "temp_bias_spread": temp_bias_spread,
        "max_abs_error_for_score": max_abs_error,
        "composite_score": composite_score,
        "validation_available": not validation_frame.empty,
    }

    coefficient_rows: list[dict[str, Any]] = []
    for order, (term_name, coeff) in enumerate(zip(spec.terms, coeffs, strict=False), start=1):
        coefficient_rows.append(
            {
                "analyzer_id": str(candidate_df["analyzer"].iloc[0]),
                "model_id": spec.model_id,
                "model_label": spec.model_label,
                "term_order": order,
                "term_name": term_name,
                "coefficient": float(coeff),
                "formula": spec.formula,
            }
        )

    residual_rows: list[dict[str, Any]] = []
    for _, row in candidate_df.iterrows():
        base = {
            "analyzer_id": row["analyzer"],
            "model_id": spec.model_id,
            "model_label": spec.model_label,
            "formula": spec.formula,
            "point_title": row["point_title"],
            "point_row": row["point_row"],
            "group_key": row["group_key"],
            "temp_c": row["temp_c"],
            "target_ppm": row["target_ppm"],
            "A_mean": row["A_mean"],
            "A_std": row.get("A_std"),
            "temp_model_c": row["temp_model_c"],
        }
        residual_rows.append(
            {
                **base,
                "prediction_scope": "overall_fit",
                "predicted_ppm": row["overall_fit_pred_ppm"],
                "error_ppm": row["overall_fit_error_ppm"],
            }
        )
        if pd.notna(row["validation_oof_pred_ppm"]):
            residual_rows.append(
                {
                    **base,
                    "prediction_scope": "validation_oof",
                    "predicted_ppm": row["validation_oof_pred_ppm"],
                    "error_ppm": row["validation_oof_error_ppm"],
                }
            )

    return score_row, coefficient_rows, residual_rows


def evaluate_absorbance_models(points: pd.DataFrame, config: Any) -> dict[str, pd.DataFrame]:
    """Fit bounded absorbance candidates per analyzer and select the best model."""

    branch_points = points.copy()
    branch_points["target_ppm"] = pd.to_numeric(branch_points["target_co2_ppm"], errors="coerce")
    branch_points["temp_c"] = pd.to_numeric(branch_points["temp_set_c"], errors="coerce")
    branch_points["temp_model_c"] = pd.to_numeric(branch_points["temp_use_mean_c"], errors="coerce")
    branch_points["group_key"] = (
        branch_points["temp_c"].map(lambda value: "nan" if pd.isna(value) else f"{float(value):g}")
        + "|"
        + branch_points["target_ppm"].map(lambda value: "nan" if pd.isna(value) else f"{float(value):g}")
        + "|"
        + branch_points["point_title"].fillna("").astype(str)
    )

    score_rows: list[dict[str, Any]] = []
    coefficient_rows: list[dict[str, Any]] = []
    residual_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []

    weight_map = config.composite_weight_map()

    for analyzer_id, analyzer_df in branch_points.groupby("analyzer"):
        analyzer_scores: list[dict[str, Any]] = []
        for spec in MODEL_SPECS:
            candidate_rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "model_id": spec.model_id,
                    "model_label": spec.model_label,
                    "formula": spec.formula,
                    "terms": "|".join(spec.terms),
                    "selection_strategy_requested": config.model_selection_strategy,
                    "selection_strategy_used": "",
                    "composite_score_enabled": config.enable_composite_score,
                    "ratio_source": config.default_ratio_source,
                    "temperature_source": config.default_temp_source,
                    "pressure_source": config.default_pressure_source,
                    "branch_id": config.default_branch_id(),
                }
            )
            try:
                score_row, coeffs, residuals = _fit_one_candidate(
                    analyzer_df=analyzer_df,
                    spec=spec,
                    strategy=config.model_selection_strategy,
                    score_weights=weight_map,
                    enable_composite_score=config.enable_composite_score,
                )
            except Exception:
                continue
            analyzer_scores.append(score_row)
            score_rows.append(score_row)
            coefficient_rows.extend(coeffs)
            residual_rows.extend(residuals)

        if not analyzer_scores:
            continue

        analyzer_scores_df = pd.DataFrame(analyzer_scores).sort_values(
            ["composite_score", "validation_rmse", "overall_rmse", "overall_max_abs_error", "model_id"],
            ignore_index=True,
        )
        analyzer_scores_df["model_rank"] = np.arange(1, len(analyzer_scores_df) + 1, dtype=int)
        best_row = analyzer_scores_df.iloc[0]
        reason = (
            f"Selected by lowest composite_score={best_row['composite_score']:.6g} on {best_row['score_source']} "
            f"with weights overall_rmse={weight_map['overall_rmse']:.2f}, "
            f"zero_rmse={weight_map['zero_rmse']:.2f}, "
            f"temp_bias_spread={weight_map['temp_bias_spread']:.2f}, "
            f"max_abs_error={weight_map['max_abs_error']:.2f}."
            if config.enable_composite_score
            else f"Selected by lowest validation_rmse on {best_row['score_source']} because composite score was disabled."
        )
        selection_rows.append(
            {
                "analyzer_id": analyzer_id,
                "best_absorbance_model": best_row["model_id"],
                "best_absorbance_model_label": best_row["model_label"],
                "formula": best_row["formula"],
                "composite_score": best_row["composite_score"],
                "model_rank": 1,
                "selection_strategy_requested": config.model_selection_strategy,
                "selection_strategy_used": best_row["selection_strategy_used"],
                "composite_score_enabled": config.enable_composite_score,
                "selected_prediction_scope": best_row["score_source"],
                "group_count": best_row["group_count"],
                "validation_available": best_row["validation_available"],
                "selection_reason": reason,
                "ratio_source": config.default_ratio_source,
                "temperature_source": config.default_temp_source,
                "pressure_source": config.default_pressure_source,
                "branch_id": config.default_branch_id(),
            }
        )

        rank_map = analyzer_scores_df.set_index("model_id")["model_rank"].to_dict()
        for score in score_rows:
            if score["analyzer_id"] == analyzer_id:
                score["model_rank"] = int(rank_map.get(score["model_id"], 0))
                score["is_selected_model"] = score["model_id"] == best_row["model_id"]

        for row in coefficient_rows:
            if row["analyzer_id"] == analyzer_id:
                row["is_selected_model"] = row["model_id"] == best_row["model_id"]

        for row in residual_rows:
            if row["analyzer_id"] == analyzer_id:
                row["is_selected_model"] = row["model_id"] == best_row["model_id"]

        for row in candidate_rows:
            if row["analyzer_id"] == analyzer_id:
                row["selection_strategy_used"] = str(best_row["selection_strategy_used"])
                row["is_selected_model"] = row["model_id"] == best_row["model_id"]

    scores = pd.DataFrame(score_rows)
    if not scores.empty:
        scores = scores.sort_values(["analyzer_id", "model_rank", "model_id"], ignore_index=True)
    candidates = pd.DataFrame(candidate_rows)
    if not candidates.empty:
        candidates = candidates.sort_values(["analyzer_id", "model_id"], ignore_index=True)
    coefficients = pd.DataFrame(coefficient_rows)
    if not coefficients.empty:
        coefficients = coefficients.sort_values(["analyzer_id", "model_id", "term_order"], ignore_index=True)
    residuals = pd.DataFrame(residual_rows)
    if not residuals.empty:
        residuals = residuals.sort_values(["analyzer_id", "model_id", "prediction_scope", "point_row"], ignore_index=True)
    selection = pd.DataFrame(selection_rows)
    if not selection.empty:
        selection = selection.sort_values(["analyzer_id"], ignore_index=True)

    best_predictions = pd.DataFrame()
    if not residuals.empty and not selection.empty:
        overall_rows = residuals[residuals["prediction_scope"] == "overall_fit"].copy()
        validation_rows = residuals[residuals["prediction_scope"] == "validation_oof"].copy()
        overall_rows = overall_rows.merge(
            selection[["analyzer_id", "best_absorbance_model"]],
            on="analyzer_id",
            how="inner",
        )
        overall_rows = overall_rows[overall_rows["model_id"] == overall_rows["best_absorbance_model"]].copy()
        overall_rows = overall_rows.rename(
            columns={
                "predicted_ppm": "best_overall_fit_pred_ppm",
                "error_ppm": "best_overall_fit_error_ppm",
            }
        )
        keep_cols = [
            "analyzer_id",
            "model_id",
            "model_label",
            "formula",
            "point_title",
            "point_row",
            "group_key",
            "temp_c",
            "target_ppm",
            "A_mean",
            "A_std",
            "temp_model_c",
            "best_overall_fit_pred_ppm",
            "best_overall_fit_error_ppm",
        ]
        overall_rows = overall_rows[keep_cols].copy()
        if not validation_rows.empty:
            validation_rows = validation_rows.merge(
                selection[["analyzer_id", "best_absorbance_model"]],
                on="analyzer_id",
                how="inner",
            )
            validation_rows = validation_rows[validation_rows["model_id"] == validation_rows["best_absorbance_model"]].copy()
            validation_rows = validation_rows.rename(
                columns={
                    "predicted_ppm": "best_validation_pred_ppm",
                    "error_ppm": "best_validation_error_ppm",
                }
            )
            validation_rows = validation_rows[
                [
                    "analyzer_id",
                    "point_title",
                    "point_row",
                    "best_validation_pred_ppm",
                    "best_validation_error_ppm",
                ]
            ].copy()
            best_predictions = overall_rows.merge(
                validation_rows,
                on=["analyzer_id", "point_title", "point_row"],
                how="left",
            )
        else:
            best_predictions = overall_rows.copy()
            best_predictions["best_validation_pred_ppm"] = np.nan
            best_predictions["best_validation_error_ppm"] = np.nan

        best_predictions = best_predictions.merge(
            selection[
                [
                    "analyzer_id",
                    "best_absorbance_model",
                    "best_absorbance_model_label",
                    "composite_score",
                    "selection_strategy_used",
                    "selected_prediction_scope",
                    "validation_available",
                    "selection_reason",
                ]
            ],
            on="analyzer_id",
            how="left",
        )
        best_predictions["selected_pred_ppm"] = best_predictions["best_validation_pred_ppm"].combine_first(
            best_predictions["best_overall_fit_pred_ppm"]
        )
        best_predictions["selected_error_ppm"] = best_predictions["best_validation_error_ppm"].combine_first(
            best_predictions["best_overall_fit_error_ppm"]
        )
        best_predictions["selected_prediction_scope"] = np.where(
            best_predictions["best_validation_pred_ppm"].notna(),
            "validation_oof",
            "overall_fit",
        )

    return {
        "candidates": candidates,
        "scores": scores,
        "selection": selection,
        "coefficients": coefficients,
        "residuals": residuals,
        "best_predictions": best_predictions,
    }
