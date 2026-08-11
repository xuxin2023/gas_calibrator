"""基于分析仪汇总行的比值多项式系数拟合。"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

from .coefficient_analysis import analyze_coefficient_stability
from .data_loader import records_to_dataframe, resolve_column_name
from .dataset_splitter import split_dataset
from .feature_builder import build_feature_dataset, default_model_features
from .model_fit import SUPPORTED_FIT_METHODS, fit_linear_model, fit_least_squares, predict_with_coefficients
from .model_metrics import analyze_error_by_range, compute_metrics
from .outlier_detector import filter_outliers


DEFAULT_CO2_RATIO_KEYS = ("R_CO2", "co2_ratio_f", "co2_ratio_raw")
DEFAULT_H2O_RATIO_KEYS = ("R_H2O", "h2o_ratio_f", "h2o_ratio_raw")
DEFAULT_TEMP_KEYS = ("T1", "Temp", "chamber_temp_c", "temp_c", "temp_set_c")
DEFAULT_PRESSURE_KEYS = ("BAR", "P", "pressure_kpa", "pressure_hpa")
DEFAULT_HUMIDITY_KEYS = ("ppm_H2O_Dew", "H2O", "h2o_mmol")
SUPPORTED_SIMPLIFICATION_METHODS = ("column_norm", "standardize", "range_scale")


@dataclass
class RatioPolyFitResult:
    """比值多项式拟合结果。"""

    model: str
    gas: str
    ratio_degree: int
    n: int
    feature_names: List[str]
    feature_terms: Dict[str, str]
    original_coefficients: Dict[str, float]
    simplified_coefficients: Dict[str, float]
    stats: Dict[str, Any]
    residuals: List[Dict[str, Any]]


def _emit_log(log_fn: Optional[Callable[[str], None]], message: str) -> None:
    if log_fn is not None:
        log_fn(message)


def _solve_least_squares(x_matrix: np.ndarray, y_vector: np.ndarray) -> np.ndarray:
    return fit_least_squares(x_matrix, y_vector).coefficients


def _solve_linear_model(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    *,
    fitting_method: str,
    ridge_lambda: float,
) -> np.ndarray:
    return fit_linear_model(
        x_matrix,
        y_vector,
        method=fitting_method,
        ridge_lambda=ridge_lambda,
    ).coefficients


def _round_coefficients(coefficients: np.ndarray, target_digits: int) -> np.ndarray:
    rounded = np.asarray(coefficients, dtype=float).copy()
    for idx, value in enumerate(rounded):
        if value == 0:
            rounded[idx] = 0.0
            continue
        magnitude = math.floor(math.log10(abs(value)))
        decimals = int(target_digits - 1 - magnitude)
        decimals = max(0, min(15, decimals))
        rounded[idx] = round(float(value), decimals)
    return rounded


def _simplify_with_column_norm(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    *,
    target_digits: int,
    add_intercept: bool,
    fitting_method: str,
    ridge_lambda: float,
) -> np.ndarray:
    column_norms = np.sqrt(np.sum(x_matrix**2, axis=0))
    if add_intercept and column_norms.size:
        column_norms[0] = 1.0
    magnitude = np.floor(np.log10(np.abs(column_norms) + 1e-100))
    scale_factors = 10 ** (magnitude - (target_digits - 2))
    scale_factors = np.clip(scale_factors, 1e-10, 1e10)
    inverse_scaling = np.diag(1.0 / scale_factors)
    x_scaled = x_matrix @ inverse_scaling
    scaled_coefficients = _solve_linear_model(
        x_scaled,
        y_vector,
        fitting_method=fitting_method,
        ridge_lambda=ridge_lambda,
    )
    return inverse_scaling @ scaled_coefficients


def _simplify_with_standardize(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    *,
    add_intercept: bool,
    fitting_method: str,
    ridge_lambda: float,
) -> np.ndarray:
    means = np.mean(x_matrix, axis=0)
    stds = np.std(x_matrix, axis=0)
    if add_intercept and means.size:
        means[0] = 0.0
        stds[0] = 1.0
    stds[stds == 0] = 1.0
    standardized = (x_matrix - means) / stds
    standardized_coefficients = _solve_linear_model(
        standardized,
        y_vector,
        fitting_method=fitting_method,
        ridge_lambda=ridge_lambda,
    )
    original = standardized_coefficients.copy()
    for idx in range(original.shape[0]):
        original[idx] = standardized_coefficients[idx] / stds[idx]
    if add_intercept and original.size:
        original[0] = float(np.mean(y_vector)) - float(np.sum(original[1:] * means[1:]))
    return original


def _simplify_with_range(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    *,
    add_intercept: bool,
    fitting_method: str,
    ridge_lambda: float,
) -> np.ndarray:
    mins = np.min(x_matrix, axis=0)
    ranges = np.max(x_matrix, axis=0) - mins
    if add_intercept and mins.size:
        mins[0] = 0.0
        ranges[0] = 1.0
    ranges[ranges == 0] = 1.0
    scaled = (x_matrix - mins) / ranges
    scaled_coefficients = _solve_linear_model(
        scaled,
        y_vector,
        fitting_method=fitting_method,
        ridge_lambda=ridge_lambda,
    )
    original = scaled_coefficients.copy()
    for idx in range(original.shape[0]):
        original[idx] = scaled_coefficients[idx] / ranges[idx]
    if add_intercept and original.size:
        original[0] = float(np.mean(y_vector)) - float(np.sum(original[1:] * mins[1:]))
    return original


def _build_simplified_coefficients(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    *,
    simplification_method: str,
    target_digits: int,
    add_intercept: bool,
    fitting_method: str,
    ridge_lambda: float,
) -> np.ndarray:
    if simplification_method == "column_norm":
        simplified = _simplify_with_column_norm(
            x_matrix,
            y_vector,
            target_digits=target_digits,
            add_intercept=add_intercept,
            fitting_method=fitting_method,
            ridge_lambda=ridge_lambda,
        )
    elif simplification_method == "standardize":
        simplified = _simplify_with_standardize(
            x_matrix,
            y_vector,
            add_intercept=add_intercept,
            fitting_method=fitting_method,
            ridge_lambda=ridge_lambda,
        )
    else:
        simplified = _simplify_with_range(
            x_matrix,
            y_vector,
            add_intercept=add_intercept,
            fitting_method=fitting_method,
            ridge_lambda=ridge_lambda,
        )
    return _round_coefficients(simplified, target_digits)


def _search_simplified_coefficients(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    *,
    selection_matrix: np.ndarray,
    selection_target: np.ndarray,
    original_coefficients: np.ndarray,
    simplification_method: str,
    target_digits: int,
    add_intercept: bool,
    fitting_method: str,
    ridge_lambda: float,
    auto_target_digits: bool,
    digit_candidates: Optional[Sequence[int]],
    simplify_rmse_tolerance: float,
) -> Dict[str, Any]:
    baseline_rmse = compute_metrics(
        selection_target,
        predict_with_coefficients(selection_matrix, original_coefficients),
    )["RMSE"]

    requested_digits = list(digit_candidates or [8, 7, 6, 5, 4])
    requested_digits = sorted({int(item) for item in requested_digits if int(item) > 0}, reverse=True)
    if not auto_target_digits:
        requested_digits = [int(target_digits)]

    history: List[Dict[str, float]] = []
    selected_coefficients: Optional[np.ndarray] = None
    selected_digits = requested_digits[0]

    for digits in requested_digits:
        candidate = _build_simplified_coefficients(
            x_matrix,
            y_vector,
            simplification_method=simplification_method,
            target_digits=digits,
            add_intercept=add_intercept,
            fitting_method=fitting_method,
            ridge_lambda=ridge_lambda,
        )
        candidate_rmse = compute_metrics(
            selection_target,
            predict_with_coefficients(selection_matrix, candidate),
        )["RMSE"]
        rmse_delta = float(candidate_rmse - baseline_rmse)
        history.append({"digits": float(digits), "selection_rmse": float(candidate_rmse), "rmse_delta": rmse_delta})

        if selected_coefficients is None:
            selected_coefficients = candidate
            selected_digits = digits
            continue

        if auto_target_digits and rmse_delta <= float(simplify_rmse_tolerance):
            selected_coefficients = candidate
            selected_digits = digits
            continue
        if auto_target_digits:
            break

    assert selected_coefficients is not None
    return {
        "coefficients": selected_coefficients,
        "selected_digits": int(selected_digits),
        "baseline_rmse": float(baseline_rmse),
        "digit_history": history,
    }


def _compute_stats(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    original_coefficients: np.ndarray,
    simplified_coefficients: np.ndarray,
) -> Dict[str, float]:
    prediction_original = x_matrix @ original_coefficients
    prediction_simplified = x_matrix @ simplified_coefficients
    residual_original = y_vector - prediction_original
    residual_simplified = y_vector - prediction_simplified

    mse_original = float(np.mean(residual_original**2))
    mse_simplified = float(np.mean(residual_simplified**2))
    rmse_original = math.sqrt(mse_original)
    rmse_simplified = math.sqrt(mse_simplified)
    mae_simplified = float(np.mean(np.abs(residual_simplified)))
    max_abs_simplified = float(np.max(np.abs(residual_simplified)))
    prediction_delta = np.abs(prediction_original - prediction_simplified)
    rmse_change = rmse_simplified - rmse_original
    rmse_relative_change_pct = 0.0
    if rmse_original > 0:
        rmse_relative_change_pct = abs(rmse_change) / rmse_original * 100.0

    return {
        "mse_original": mse_original,
        "mse_simplified": mse_simplified,
        "rmse_original": rmse_original,
        "rmse_simplified": rmse_simplified,
        "mae_simplified": mae_simplified,
        "max_abs_simplified": max_abs_simplified,
        "rmse_change": rmse_change,
        "rmse_relative_change_pct": rmse_relative_change_pct,
        "max_prediction_delta": float(np.max(prediction_delta)),
        "mean_prediction_delta": float(np.mean(prediction_delta)),
    }


def _coefficients_to_mapping(names: Sequence[str], values: np.ndarray) -> Dict[str, float]:
    return {name: float(values[idx]) for idx, name in enumerate(names)}


def _default_evaluation_bins(gas: str) -> List[float]:
    if str(gas or "").strip().lower() == "h2o":
        return [0, 2000, 5000, 10000, 20000, 40000]
    return [0, 200, 400, 800, 1200, 2000]


def _model_uses_humidity_features(model_features: Sequence[str]) -> bool:
    return any(str(token).strip().upper() in {"H", "H2", "RH"} for token in model_features)


def _extract_cross_interference_summary(
    feature_names: Sequence[str],
    feature_terms: Dict[str, str],
    feature_tokens: Sequence[str],
    original_coefficients: np.ndarray,
    simplified_coefficients: np.ndarray,
) -> Dict[str, Any]:
    token_to_export_name = {
        "H": "a_H",
        "H2": "a_H2",
        "RH": "a_RH",
    }
    original_map = _coefficients_to_mapping(feature_names, original_coefficients)
    simplified_map = _coefficients_to_mapping(feature_names, simplified_coefficients)
    export_terms: Dict[str, str] = {}
    export_original: Dict[str, float] = {}
    export_simplified: Dict[str, float] = {}
    source_coefficients: Dict[str, str] = {}
    for index, token in enumerate(feature_tokens):
        export_name = token_to_export_name.get(str(token).strip().upper())
        if export_name is None:
            continue
        coefficient_name = str(feature_names[index])
        export_terms[export_name] = feature_terms.get(coefficient_name, "")
        export_original[export_name] = float(original_map[coefficient_name])
        export_simplified[export_name] = float(simplified_map[coefficient_name])
        source_coefficients[export_name] = coefficient_name
    return {
        "enabled": bool(export_simplified),
        "feature_tokens": [token for token in feature_tokens if str(token).strip().upper() in token_to_export_name],
        "feature_terms": export_terms,
        "source_coefficients": source_coefficients,
        "original_coefficients": export_original,
        "simplified_coefficients": export_simplified,
    }


def _prepare_ratio_poly_dataframe(
    rows: Iterable[Dict[str, Any]],
    *,
    gas: str,
    target_key: str,
    ratio_keys: Sequence[str],
    temp_keys: Sequence[str],
    pressure_keys: Sequence[str],
    humidity_keys: Optional[Sequence[str]],
    model_features: Sequence[str],
    pressure_scale: float,
) -> tuple[pd.DataFrame, str, str, str, str, str | None]:
    dataframe = records_to_dataframe(rows)
    if dataframe.empty:
        raise ValueError("No rows were provided for fit")

    target_column = resolve_column_name(dataframe, (target_key,), label=f"{gas} target")
    ratio_column = resolve_column_name(dataframe, ratio_keys, label=f"{gas} ratio")
    temp_column = resolve_column_name(dataframe, temp_keys, label=f"{gas} temperature")
    pressure_column = resolve_column_name(dataframe, pressure_keys, label=f"{gas} pressure")
    humidity_column: str | None = None
    if _model_uses_humidity_features(model_features):
        if not humidity_keys:
            raise ValueError("model_features includes H/H2/RH but humidity_keys were not provided")
        humidity_column = resolve_column_name(dataframe, humidity_keys, label=f"{gas} humidity cross term")

    working = dataframe.copy()
    selected_columns = [target_column, ratio_column, temp_column, pressure_column]
    if humidity_column is not None:
        selected_columns.append(humidity_column)
    for column in selected_columns:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    working = working.dropna(subset=selected_columns).copy()
    working["P_fit"] = working[pressure_column].astype(float) * float(pressure_scale)
    return working, target_column, ratio_column, temp_column, "P_fit", humidity_column


def _normalize_simplification_selection_scope(value: str | None) -> str:
    text = str(value or "train").strip().lower().replace("-", "_").replace("+", "_plus_")
    if text in {"train", "fit_train"}:
        return "train"
    if text in {"train_plus_val", "train_plus_validation", "train_validation", "train_val"}:
        return "train_plus_val"
    raise ValueError("simplification_selection_scope must be 'train' or 'train_plus_val'")


def _resolve_selection_frame(
    filtered_train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    *,
    selection_scope: str,
) -> tuple[pd.DataFrame, str]:
    if selection_scope == "train_plus_val" and not val_df.empty:
        return pd.concat([filtered_train_df, val_df], axis=0).copy(), "train+validation"
    return filtered_train_df.copy(), "train"


def _evaluate_dataset(
    dataset_name: str,
    dataset_frame: pd.DataFrame,
    *,
    target_column: str,
    ratio_column: str,
    temp_column: str,
    pressure_column: str,
    humidity_column: str | None,
    temperature_offset_c: float,
    model_features: Sequence[str],
    original_coefficients: np.ndarray,
    simplified_coefficients: np.ndarray,
    bins: Sequence[float],
    log_fn: Optional[Callable[[str], None]],
) -> Dict[str, Any]:
    if dataset_frame.empty:
        _emit_log(log_fn, f"{dataset_name} 集为空，跳过评估")
        return {
            "sample_count": 0,
            "original": {},
            "simplified": {},
            "range_original": [],
            "range_simplified": [],
        }

    dataset = build_feature_dataset(
        dataset_frame,
        target_column=target_column,
        ratio_column=ratio_column,
        temperature_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        temperature_offset_c=temperature_offset_c,
        model_features=model_features,
    )
    prediction_original = predict_with_coefficients(dataset.feature_matrix, original_coefficients)
    prediction_simplified = predict_with_coefficients(dataset.feature_matrix, simplified_coefficients)
    original_metrics = compute_metrics(dataset.target_vector, prediction_original)
    simplified_metrics = compute_metrics(dataset.target_vector, prediction_simplified)

    _emit_log(log_fn, f"{dataset_name} RMSE = {original_metrics['RMSE']:.6g}")
    _emit_log(log_fn, f"{dataset_name} R2 = {original_metrics['R2']:.6g}")
    _emit_log(log_fn, f"{dataset_name} Bias = {original_metrics['Bias']:.6g}")
    _emit_log(log_fn, f"简化后 {dataset_name} RMSE = {simplified_metrics['RMSE']:.6g}")

    return {
        "sample_count": int(dataset.feature_matrix.shape[0]),
        "original": original_metrics,
        "simplified": simplified_metrics,
        "range_original": analyze_error_by_range(dataset.target_vector, prediction_original, bins),
        "range_simplified": analyze_error_by_range(dataset.target_vector, prediction_simplified, bins),
    }


def fit_ratio_poly_rt_p(
    rows: Iterable[Dict[str, Any]],
    *,
    gas: str,
    target_key: str,
    ratio_keys: Optional[Sequence[str]] = None,
    temp_keys: Optional[Sequence[str]] = None,
    pressure_keys: Optional[Sequence[str]] = None,
    humidity_keys: Optional[Sequence[str]] = None,
    ratio_degree: int = 3,
    temperature_offset_c: float = 273.15,
    pressure_scale: float = 1.0,
    add_intercept: bool = True,
    model_features: Optional[Sequence[str]] = None,
    fitting_method: str = "least_squares",
    ridge_lambda: float = 1e-6,
    simplify_coefficients: bool = True,
    simplification_method: str = "column_norm",
    target_digits: int = 6,
    auto_target_digits: bool = False,
    digit_candidates: Optional[Sequence[int]] = None,
    simplify_rmse_tolerance: float = 0.0,
    simplification_selection_scope: str = "train",
    outlier_methods: Optional[Sequence[str]] = None,
    iqr_factor: float = 1.5,
    residual_std_multiplier: float = 3.0,
    min_samples: int = 0,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    random_seed: int = 42,
    shuffle_dataset: bool = True,
    evaluation_bins: Optional[Sequence[float]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> RatioPolyFitResult:
    """拟合 R/T/P 模型，并执行三阶段评估。"""
    if target_digits < 1:
        raise ValueError("target_digits must be >= 1")
    if simplification_method not in SUPPORTED_SIMPLIFICATION_METHODS:
        raise ValueError(
            "simplification_method must be one of: " + ", ".join(SUPPORTED_SIMPLIFICATION_METHODS)
        )
    if fitting_method not in SUPPORTED_FIT_METHODS:
        raise ValueError("fitting_method must be one of: " + ", ".join(SUPPORTED_FIT_METHODS))

    gas_lower = str(gas or "").strip().lower()
    if ratio_keys is None:
        ratio_keys = DEFAULT_CO2_RATIO_KEYS if gas_lower == "co2" else DEFAULT_H2O_RATIO_KEYS
    if temp_keys is None:
        temp_keys = DEFAULT_TEMP_KEYS
    if pressure_keys is None:
        pressure_keys = DEFAULT_PRESSURE_KEYS
    if humidity_keys is None:
        humidity_keys = DEFAULT_HUMIDITY_KEYS

    active_model_features = list(model_features or default_model_features(ratio_degree, add_intercept))
    _emit_log(log_fn, f"加载数据：开始准备 {gas_lower.upper()} 拟合数据")
    working, target_column, ratio_column, temp_column, pressure_column, humidity_column = _prepare_ratio_poly_dataframe(
        rows,
        gas=gas_lower,
        target_key=target_key,
        ratio_keys=ratio_keys,
        temp_keys=temp_keys,
        pressure_keys=pressure_keys,
        humidity_keys=humidity_keys,
        model_features=active_model_features,
        pressure_scale=pressure_scale,
    )
    _emit_log(log_fn, f"加载数据：{len(working)} 条有效记录")
    _emit_log(log_fn, f"模型特征：{active_model_features}")
    _emit_log(log_fn, f"拟合方法：{fitting_method}")

    normalized_selection_scope = _normalize_simplification_selection_scope(simplification_selection_scope)

    required = max(min_samples, len(active_model_features))
    if len(working) < required:
        raise ValueError(f"Not enough rows for fit: {len(working)} < {required}")

    _emit_log(log_fn, "数据拆分：")
    train_df, val_df, test_df, split_metadata = split_dataset(
        working,
        train_ratio=train_ratio,
        val_ratio=val_ratio,
        random_seed=random_seed,
        shuffle=shuffle_dataset,
        min_train_size=required,
        log_fn=log_fn,
        return_metadata=True,
    )

    outlier_result = filter_outliers(
        train_df,
        target_column=target_column,
        ratio_column=ratio_column,
        temperature_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        model_features=active_model_features,
        temperature_offset_c=temperature_offset_c,
        fit_method=fitting_method,
        ridge_lambda=ridge_lambda,
        methods=outlier_methods,
        iqr_factor=iqr_factor,
        residual_std_multiplier=residual_std_multiplier,
        log_fn=log_fn,
    )
    filtered_fit_df = outlier_result.kept_frame
    if len(filtered_fit_df) < required:
        raise ValueError(f"Fit rows are not enough after outlier filtering: {len(filtered_fit_df)} < {required}")

    fit_dataset = build_feature_dataset(
        filtered_fit_df,
        target_column=target_column,
        ratio_column=ratio_column,
        temperature_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        temperature_offset_c=temperature_offset_c,
        model_features=active_model_features,
    )
    x_fit = fit_dataset.feature_matrix
    y_fit = fit_dataset.target_vector
    names = fit_dataset.feature_names

    _emit_log(log_fn, f"构造特征：全量拟合集 {x_fit.shape[0]} 条样本，{x_fit.shape[1]} 个特征")
    _emit_log(log_fn, "开始全量拟合模型")
    original_fit = fit_linear_model(
        x_fit,
        y_fit,
        method=fitting_method,
        ridge_lambda=ridge_lambda,
    )
    original = original_fit.coefficients
    _emit_log(log_fn, "全量拟合完成")

    selection_df, selection_scope_label = _resolve_selection_frame(
        filtered_fit_df,
        val_df,
        selection_scope=normalized_selection_scope,
    )
    selection_dataset = build_feature_dataset(
        selection_df,
        target_column=target_column,
        ratio_column=ratio_column,
        temperature_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        temperature_offset_c=temperature_offset_c,
        model_features=active_model_features,
    )

    _emit_log(log_fn, "开始系数简化")
    if not simplify_coefficients:
        simplified = np.asarray(original, dtype=float).copy()
        simplify_info = {
            "selected_digits": int(target_digits),
            "baseline_rmse": compute_metrics(
                selection_dataset.target_vector,
                predict_with_coefficients(selection_dataset.feature_matrix, original),
            )["RMSE"],
            "digit_history": [],
        }
    else:
        simplify_info = _search_simplified_coefficients(
            x_fit,
            y_fit,
            selection_matrix=selection_dataset.feature_matrix,
            selection_target=selection_dataset.target_vector,
            original_coefficients=original,
            simplification_method=simplification_method,
            target_digits=target_digits,
            add_intercept="intercept" in active_model_features,
            fitting_method=fitting_method,
            ridge_lambda=ridge_lambda,
            auto_target_digits=auto_target_digits,
            digit_candidates=digit_candidates,
            simplify_rmse_tolerance=simplify_rmse_tolerance,
        )
        simplified = np.asarray(simplify_info["coefficients"], dtype=float)
    _emit_log(log_fn, f"系数简化完成：最优有效数字 = {simplify_info['selected_digits']}")

    full_dataset = build_feature_dataset(
        working,
        target_column=target_column,
        ratio_column=ratio_column,
        temperature_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        temperature_offset_c=temperature_offset_c,
        model_features=active_model_features,
    )
    x_full = full_dataset.feature_matrix
    y_full = full_dataset.target_vector
    stats: Dict[str, Any] = _compute_stats(x_full, y_full, original, simplified)
    stats["dataset_split"] = {
        "train_ratio": float(train_ratio),
        "val_ratio": float(val_ratio),
        "test_ratio": float(1.0 - train_ratio - val_ratio),
        "random_seed": int(random_seed),
        "shuffle": bool(shuffle_dataset),
        "split_strategy": str(split_metadata.get("split_strategy", "random")),
        "split_strategy_label": str(split_metadata.get("split_strategy_label", "random")),
        "group_columns": list(split_metadata.get("group_columns", [])),
        "group_count": int(split_metadata.get("group_count", 0)),
        "raw_train_count": int(len(train_df)),
        "train_count": int(len(filtered_fit_df)),
        "validation_count": int(len(val_df)),
        "test_count": int(len(test_df)),
        "fit_count": int(len(filtered_fit_df)),
        "selection_count": int(len(selection_df)),
        "raw_train_indices": [int(index) for index in train_df.index.tolist()],
        "fit_indices": [int(index) for index in filtered_fit_df.index.tolist()],
        "validation_indices": [int(index) for index in val_df.index.tolist()],
        "test_indices": [int(index) for index in test_df.index.tolist()],
        "selection_indices": [int(index) for index in selection_df.index.tolist()],
        "fit_scope": "train",
    }
    stats["fit_scope"] = "train"
    stats["outlier_scope"] = "train"
    stats["simplification_scope"] = "train"
    stats["selection_scope"] = selection_scope_label
    stats["leakage_safe"] = bool(selection_scope_label == "train")
    stats["model_features"] = list(active_model_features)
    stats["fit_settings"] = {
        "fitting_method": fitting_method,
        "ridge_lambda": float(ridge_lambda),
        "simplification_method": simplification_method,
        "simplification_selection_scope": normalized_selection_scope,
    }
    stats["outlier_detection"] = {
        "methods": [str(item) for item in (outlier_methods or [])],
        "original_count": outlier_result.original_count,
        "outlier_count": outlier_result.outlier_count,
        "final_count": outlier_result.final_count,
        "details": outlier_result.details,
        "outlier_scope": "train",
    }
    stats["original_coefficient_analysis"] = analyze_coefficient_stability(x_fit, original)
    stats["simplified_coefficient_analysis"] = analyze_coefficient_stability(x_fit, simplified)
    stats["simplification_summary"] = {
        "selected_digits": int(simplify_info["selected_digits"]),
        "auto_target_digits": bool(auto_target_digits),
        "digit_history": simplify_info["digit_history"],
        "baseline_rmse": float(simplify_info["baseline_rmse"]),
        "rmse_tolerance": float(simplify_rmse_tolerance),
        "simplification_scope": "train",
        "selection_scope": selection_scope_label,
    }
    stats["cross_interference"] = _extract_cross_interference_summary(
        names,
        fit_dataset.feature_terms,
        fit_dataset.feature_tokens,
        original,
        simplified,
    )

    bins = list(evaluation_bins or _default_evaluation_bins(gas_lower))
    stats["train_metrics"] = _evaluate_dataset(
        "Train",
        filtered_fit_df,
        target_column=target_column,
        ratio_column=ratio_column,
        temp_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        temperature_offset_c=temperature_offset_c,
        model_features=active_model_features,
        original_coefficients=original,
        simplified_coefficients=simplified,
        bins=bins,
        log_fn=log_fn,
    )
    stats["validation_metrics"] = _evaluate_dataset(
        "Validation",
        val_df,
        target_column=target_column,
        ratio_column=ratio_column,
        temp_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        temperature_offset_c=temperature_offset_c,
        model_features=active_model_features,
        original_coefficients=original,
        simplified_coefficients=simplified,
        bins=bins,
        log_fn=log_fn,
    )
    stats["test_metrics"] = _evaluate_dataset(
        "Test",
        test_df,
        target_column=target_column,
        ratio_column=ratio_column,
        temp_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        temperature_offset_c=temperature_offset_c,
        model_features=active_model_features,
        original_coefficients=original,
        simplified_coefficients=simplified,
        bins=bins,
        log_fn=log_fn,
    )

    _emit_log(log_fn, f"RMSE(原始)={stats['rmse_original']:.6g}")
    _emit_log(log_fn, f"RMSE(简化)={stats['rmse_simplified']:.6g}")
    _emit_log(log_fn, f"RMSE变化={stats['rmse_change']:.6g}")

    prediction_original = predict_with_coefficients(x_full, original)
    prediction_simplified = predict_with_coefficients(x_full, simplified)
    filtered_fit_indices = set(filtered_fit_df.index.tolist())
    raw_train_indices = set(train_df.index.tolist())
    validation_indices = set(val_df.index.tolist())
    residuals: List[Dict[str, Any]] = []
    for position, (idx, row) in enumerate(full_dataset.working_frame.iterrows()):
        if idx in filtered_fit_indices:
            split_name = "train"
        elif idx in raw_train_indices:
            split_name = "train_removed_outlier"
        elif idx in validation_indices:
            split_name = "validation"
        else:
            split_name = "test"
        residuals.append(
            {
                "dataset_split": split_name,
                "Analyzer": row.get("Analyzer"),
                "PointRow": row.get("PointRow"),
                "PointPhase": row.get("PointPhase"),
                "PointTag": row.get("PointTag"),
                "PointTitle": row.get("PointTitle"),
                "R": float(row[ratio_column]),
                "T_c": float(row[temp_column]),
                "T_k": float(row["T_k"]),
                "P": float(row[pressure_column]),
                "H2O": float(row[humidity_column]) if humidity_column is not None else None,
                "target": float(row[target_column]),
                "prediction_original": float(prediction_original[position]),
                "prediction_simplified": float(prediction_simplified[position]),
                "error_original": float(prediction_original[position] - y_full[position]),
                "error_simplified": float(prediction_simplified[position] - y_full[position]),
            }
        )

    return RatioPolyFitResult(
        model="ratio_poly_rt_p",
        gas=gas_lower,
        ratio_degree=ratio_degree,
        n=x_full.shape[0],
        feature_names=names,
        feature_terms=fit_dataset.feature_terms,
        original_coefficients=_coefficients_to_mapping(names, original),
        simplified_coefficients=_coefficients_to_mapping(names, simplified),
        stats=stats,
        residuals=residuals,
    )


def save_ratio_poly_report(
    result: RatioPolyFitResult,
    out_dir: Path,
    prefix: str,
    include_residuals: bool = True,
) -> Dict[str, Path]:
    """将比值多项式拟合结果落盘为 JSON 和 CSV。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"{prefix}_fit_{stamp}.json"
    payload = {
        "model": result.model,
        "gas": result.gas,
        "ratio_degree": result.ratio_degree,
        "n": result.n,
        "feature_names": result.feature_names,
        "feature_terms": result.feature_terms,
        "original_coefficients": result.original_coefficients,
        "simplified_coefficients": result.simplified_coefficients,
        "H2O_cross_coefficients": result.stats.get("cross_interference", {}).get("simplified_coefficients", {}),
        "cross_interference": result.stats.get("cross_interference", {}),
        "stats": result.stats,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    paths: Dict[str, Path] = {"json": json_path}
    if include_residuals:
        csv_path = out_dir / f"{prefix}_fit_{stamp}_residuals.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            if result.residuals:
                writer = csv.DictWriter(handle, fieldnames=list(result.residuals[0].keys()))
                writer.writeheader()
                writer.writerows(result.residuals)
            else:
                handle.write("")
        paths["csv"] = csv_path
    return paths
