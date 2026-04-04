"""进化版校准方程拟合与系数简化。"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

import numpy as np

from .coefficient_analysis import analyze_coefficient_stability
from .dataset_splitter import split_dataset
from .feature_builder import build_feature_dataset, default_model_features
from .fit_ratio_poly import (
    DEFAULT_CO2_RATIO_KEYS,
    DEFAULT_H2O_RATIO_KEYS,
    DEFAULT_HUMIDITY_KEYS,
    DEFAULT_PRESSURE_KEYS,
    DEFAULT_TEMP_KEYS,
    RatioPolyFitResult,
    SUPPORTED_SIMPLIFICATION_METHODS,
    _coefficients_to_mapping,
    _compute_stats,
    _default_evaluation_bins,
    _emit_log,
    _evaluate_dataset,
    _extract_cross_interference_summary,
    _prepare_ratio_poly_dataframe,
    _search_simplified_coefficients,
)
from .model_fit import SUPPORTED_FIT_METHODS, fit_linear_model, predict_with_coefficients
from .model_metrics import compute_metrics
from .outlier_detector import filter_outliers


def _weighted_linear_fit(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    weights: np.ndarray,
    *,
    fitting_method: str,
    ridge_lambda: float,
) -> np.ndarray:
    sqrt_weights = np.sqrt(np.asarray(weights, dtype=float))
    weighted_x = x_matrix * sqrt_weights[:, None]
    weighted_y = y_vector * sqrt_weights
    return fit_linear_model(
        weighted_x,
        weighted_y,
        method=fitting_method,
        ridge_lambda=ridge_lambda,
    ).coefficients


def _robust_scale(residuals: np.ndarray) -> float:
    centered = residuals - np.median(residuals)
    mad = float(np.median(np.abs(centered)))
    if mad > 0:
        return 1.4826 * mad
    fallback = float(np.std(residuals))
    return fallback if fallback > 0 else 1.0


def _huber_weights(
    residuals: np.ndarray,
    *,
    scale: float,
    delta: float,
    min_weight: float,
) -> np.ndarray:
    if scale <= 0:
        return np.ones_like(residuals, dtype=float)
    weights = np.ones_like(residuals, dtype=float)
    cutoff = float(delta) * float(scale)
    abs_residuals = np.abs(residuals)
    large = abs_residuals > cutoff
    if np.any(large):
        weights[large] = cutoff / abs_residuals[large]
    weights = np.clip(weights, float(min_weight), 1.0)
    return weights


def fit_ratio_poly_rt_p_evolved(
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
    outlier_methods: Optional[Sequence[str]] = None,
    iqr_factor: float = 1.5,
    residual_std_multiplier: float = 3.0,
    min_samples: int = 0,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    random_seed: int = 42,
    shuffle_dataset: bool = True,
    evaluation_bins: Optional[Sequence[float]] = None,
    robust_iterations: int = 8,
    robust_huber_delta: float = 1.5,
    robust_min_weight: float = 0.05,
    candidate_simplification_methods: Optional[Sequence[str]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> RatioPolyFitResult:
    """进化版拟合：异常点筛选 + 鲁棒重加权 + 三阶段评估。"""
    if target_digits < 1:
        raise ValueError("target_digits must be >= 1")
    if robust_iterations < 0:
        raise ValueError("robust_iterations must be >= 0")
    if robust_huber_delta <= 0:
        raise ValueError("robust_huber_delta must be > 0")
    if not (0 < robust_min_weight <= 1):
        raise ValueError("robust_min_weight must be within (0, 1]")
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
    _emit_log(log_fn, f"加载数据：开始准备 {gas_lower.upper()} 进化版拟合数据")
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

    required = max(min_samples, len(active_model_features))
    if len(working) < required:
        raise ValueError(f"Not enough rows for fit: {len(working)} < {required}")

    _emit_log(log_fn, "数据拆分：")
    train_df, val_df, test_df = split_dataset(
        working,
        train_ratio=train_ratio,
        val_ratio=val_ratio,
        random_seed=random_seed,
        shuffle=shuffle_dataset,
        min_train_size=required,
        log_fn=log_fn,
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
    filtered_train_df = outlier_result.kept_frame
    if len(filtered_train_df) < required:
        raise ValueError(f"Training rows are not enough after outlier filtering: {len(filtered_train_df)} < {required}")

    train_dataset = build_feature_dataset(
        filtered_train_df,
        target_column=target_column,
        ratio_column=ratio_column,
        temperature_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        temperature_offset_c=temperature_offset_c,
        model_features=active_model_features,
    )
    x_train = train_dataset.feature_matrix
    y_train = train_dataset.target_vector
    names = train_dataset.feature_names

    _emit_log(log_fn, f"构造特征：训练集 {x_train.shape[0]} 条样本，{x_train.shape[1]} 个特征")
    _emit_log(log_fn, "开始训练模型")
    original = fit_linear_model(
        x_train,
        y_train,
        method=fitting_method,
        ridge_lambda=ridge_lambda,
    ).coefficients

    weights = np.ones(x_train.shape[0], dtype=float)
    robust_scale = 0.0
    robust_steps = 0
    if robust_iterations > 0:
        for _step in range(robust_iterations):
            residuals = y_train - (x_train @ original)
            robust_scale = _robust_scale(residuals)
            new_weights = _huber_weights(
                residuals,
                scale=robust_scale,
                delta=robust_huber_delta,
                min_weight=robust_min_weight,
            )
            updated = _weighted_linear_fit(
                x_train,
                y_train,
                new_weights,
                fitting_method=fitting_method,
                ridge_lambda=ridge_lambda,
            )
            robust_steps += 1
            if np.allclose(updated, original, rtol=1e-9, atol=1e-9):
                original = updated
                weights = new_weights
                break
            original = updated
            weights = new_weights
    _emit_log(log_fn, f"训练完成：鲁棒迭代 {robust_steps} 次，降权样本 {int(np.sum(weights < 0.999999))} 个")

    selection_df = val_df if not val_df.empty else filtered_train_df
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
    weighted_x = x_train * np.sqrt(weights)[:, None]
    weighted_y = y_train * np.sqrt(weights)
    methods = list(candidate_simplification_methods or SUPPORTED_SIMPLIFICATION_METHODS)
    if simplification_method not in {"auto", *SUPPORTED_SIMPLIFICATION_METHODS}:
        raise ValueError("simplification_method must be auto or one of supported methods")

    if not simplify_coefficients:
        simplified = np.asarray(original, dtype=float).copy()
        simplify_info = {
            "selected_method": "none",
            "selected_digits": int(target_digits),
            "baseline_rmse": compute_metrics(
                selection_dataset.target_vector,
                predict_with_coefficients(selection_dataset.feature_matrix, original),
            )["RMSE"],
            "digit_history": [],
        }
    else:
        selected_methods = methods if simplification_method == "auto" else [simplification_method]
        best_method = selected_methods[0]
        best_info = _search_simplified_coefficients(
            weighted_x,
            weighted_y,
            selection_matrix=selection_dataset.feature_matrix,
            selection_target=selection_dataset.target_vector,
            original_coefficients=original,
            simplification_method=best_method,
            target_digits=target_digits,
            add_intercept="intercept" in active_model_features,
            fitting_method=fitting_method,
            ridge_lambda=ridge_lambda,
            auto_target_digits=auto_target_digits,
            digit_candidates=digit_candidates,
            simplify_rmse_tolerance=simplify_rmse_tolerance,
        )
        best_predictions = predict_with_coefficients(selection_dataset.feature_matrix, best_info["coefficients"])
        best_metrics = compute_metrics(selection_dataset.target_vector, best_predictions)
        best_key = (best_metrics["RMSE"], best_metrics["MaxError"])

        for method_name in selected_methods[1:]:
            candidate_info = _search_simplified_coefficients(
                weighted_x,
                weighted_y,
                selection_matrix=selection_dataset.feature_matrix,
                selection_target=selection_dataset.target_vector,
                original_coefficients=original,
                simplification_method=method_name,
                target_digits=target_digits,
                add_intercept="intercept" in active_model_features,
                fitting_method=fitting_method,
                ridge_lambda=ridge_lambda,
                auto_target_digits=auto_target_digits,
                digit_candidates=digit_candidates,
                simplify_rmse_tolerance=simplify_rmse_tolerance,
            )
            candidate_predictions = predict_with_coefficients(selection_dataset.feature_matrix, candidate_info["coefficients"])
            candidate_metrics = compute_metrics(selection_dataset.target_vector, candidate_predictions)
            candidate_key = (candidate_metrics["RMSE"], candidate_metrics["MaxError"])
            if candidate_key < best_key:
                best_method = method_name
                best_info = candidate_info
                best_key = candidate_key

        simplified = np.asarray(best_info["coefficients"], dtype=float)
        simplify_info = {
            "selected_method": best_method,
            "selected_digits": int(best_info["selected_digits"]),
            "baseline_rmse": float(best_info["baseline_rmse"]),
            "digit_history": best_info["digit_history"],
        }
    _emit_log(log_fn, f"系数简化完成：选用 {simplify_info['selected_method']}，最优有效数字 = {simplify_info['selected_digits']}")

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
        "raw_train_count": int(len(train_df)),
        "train_count": int(len(filtered_train_df)),
        "validation_count": int(len(val_df)),
        "test_count": int(len(test_df)),
    }
    stats["model_features"] = list(active_model_features)
    stats["fit_settings"] = {
        "fitting_method": fitting_method,
        "ridge_lambda": float(ridge_lambda),
        "simplification_method": simplify_info["selected_method"],
    }
    stats["outlier_detection"] = {
        "methods": [str(item) for item in (outlier_methods or [])],
        "original_count": outlier_result.original_count,
        "outlier_count": outlier_result.outlier_count,
        "final_count": outlier_result.final_count,
        "details": outlier_result.details,
    }
    stats["original_coefficient_analysis"] = analyze_coefficient_stability(x_train, original)
    stats["simplified_coefficient_analysis"] = analyze_coefficient_stability(x_train, simplified)
    stats["simplification_summary"] = {
        "selected_method": simplify_info["selected_method"],
        "selected_digits": int(simplify_info["selected_digits"]),
        "auto_target_digits": bool(auto_target_digits),
        "digit_history": simplify_info["digit_history"],
        "baseline_rmse": float(simplify_info["baseline_rmse"]),
        "rmse_tolerance": float(simplify_rmse_tolerance),
    }
    stats["cross_interference"] = _extract_cross_interference_summary(
        names,
        train_dataset.feature_terms,
        train_dataset.feature_tokens,
        original,
        simplified,
    )
    stats["robust_iterations"] = float(robust_steps)
    stats["robust_scale"] = float(robust_scale)
    stats["downweighted_samples"] = float(np.sum(weights < 0.999999))
    stats["min_weight"] = float(np.min(weights))

    bins = list(evaluation_bins or _default_evaluation_bins(gas_lower))
    stats["train_metrics"] = _evaluate_dataset(
        "Train",
        filtered_train_df,
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

    prediction_original = x_full @ original
    prediction_simplified = x_full @ simplified
    weight_by_index = {idx: float(weight) for idx, weight in zip(filtered_train_df.index.tolist(), weights)}
    filtered_train_indices = set(filtered_train_df.index.tolist())
    raw_train_indices = set(train_df.index.tolist())
    validation_indices = set(val_df.index.tolist())
    residuals_rows: List[Dict[str, Any]] = []
    for position, (idx, row) in enumerate(full_dataset.working_frame.iterrows()):
        if idx in filtered_train_indices:
            split_name = "train"
        elif idx in raw_train_indices:
            split_name = "train_removed_outlier"
        elif idx in validation_indices:
            split_name = "validation"
        else:
            split_name = "test"
        residuals_rows.append(
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
                "weight": weight_by_index.get(idx, 1.0),
                "prediction_original": float(prediction_original[position]),
                "prediction_simplified": float(prediction_simplified[position]),
                "error_original": float(prediction_original[position] - y_full[position]),
                "error_simplified": float(prediction_simplified[position] - y_full[position]),
            }
        )

    return RatioPolyFitResult(
        model="ratio_poly_rt_p_evolved",
        gas=gas_lower,
        ratio_degree=ratio_degree,
        n=x_full.shape[0],
        feature_names=names,
        feature_terms=train_dataset.feature_terms,
        original_coefficients=_coefficients_to_mapping(names, original),
        simplified_coefficients=_coefficients_to_mapping(names, simplified),
        stats=stats,
        residuals=residuals_rows,
    )
