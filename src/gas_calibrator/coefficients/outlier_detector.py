"""异常点检测与剔除模块。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from .feature_builder import build_feature_dataset
from .model_fit import fit_linear_model, predict_with_coefficients


@dataclass
class OutlierDetectionResult:
    """异常点检测结果。"""

    original_count: int
    outlier_count: int
    final_count: int
    kept_frame: pd.DataFrame
    removed_indices: List[int]
    details: Dict[str, int]


def _emit_log(log_fn: Optional[Callable[[str], None]], message: str) -> None:
    if log_fn is not None:
        log_fn(message)


def detect_iqr_outliers(values: Sequence[float], *, factor: float = 1.5) -> np.ndarray:
    """使用 IQR 规则识别异常点。"""
    array = np.asarray(values, dtype=float)
    if array.size == 0:
        return np.zeros(0, dtype=bool)
    q1 = float(np.percentile(array, 25))
    q3 = float(np.percentile(array, 75))
    iqr = q3 - q1
    lower = q1 - float(factor) * iqr
    upper = q3 + float(factor) * iqr
    return (array < lower) | (array > upper)


def detect_residual_outliers(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    *,
    fit_method: str = "least_squares",
    ridge_lambda: float = 1e-6,
    std_multiplier: float = 3.0,
) -> np.ndarray:
    """通过初始拟合后的残差识别异常点。"""
    if y_vector.size == 0:
        return np.zeros(0, dtype=bool)
    fitted = fit_linear_model(
        x_matrix,
        y_vector,
        method=fit_method,
        ridge_lambda=ridge_lambda,
    )
    residuals = np.asarray(y_vector - predict_with_coefficients(x_matrix, fitted.coefficients), dtype=float)
    std = float(np.std(residuals))
    if std == 0.0:
        return np.zeros_like(residuals, dtype=bool)
    return np.abs(residuals) > float(std_multiplier) * std


def filter_outliers(
    dataframe: pd.DataFrame,
    *,
    target_column: str,
    ratio_column: str,
    temperature_column: str,
    pressure_column: str,
    humidity_column: str | None = None,
    model_features: Sequence[str],
    temperature_offset_c: float,
    fit_method: str = "least_squares",
    ridge_lambda: float = 1e-6,
    methods: Optional[Sequence[str]] = None,
    iqr_columns: Optional[Sequence[str]] = None,
    iqr_factor: float = 1.5,
    residual_std_multiplier: float = 3.0,
    log_fn: Optional[Callable[[str], None]] = None,
) -> OutlierDetectionResult:
    """按 IQR 和残差法组合识别训练集异常点。"""
    requested_methods = [str(item).strip().lower() for item in (methods or []) if str(item).strip()]
    if not requested_methods:
        return OutlierDetectionResult(
            original_count=len(dataframe),
            outlier_count=0,
            final_count=len(dataframe),
            kept_frame=dataframe.copy(),
            removed_indices=[],
            details={"iqr_outliers": 0, "residual_outliers": 0},
        )

    original = dataframe.copy()
    outlier_mask = np.zeros(len(original), dtype=bool)
    details = {"iqr_outliers": 0, "residual_outliers": 0}

    if "iqr" in requested_methods:
        columns = list(iqr_columns or [target_column, ratio_column, temperature_column, pressure_column])
        iqr_mask = np.zeros(len(original), dtype=bool)
        for column in columns:
            iqr_mask |= detect_iqr_outliers(original[column].astype(float).to_numpy(), factor=iqr_factor)
        details["iqr_outliers"] = int(np.sum(iqr_mask))
        outlier_mask |= iqr_mask

    kept_after_iqr = original.loc[~outlier_mask].copy()
    if "residual_sigma" in requested_methods and not kept_after_iqr.empty:
        residual_dataset = build_feature_dataset(
            kept_after_iqr,
            target_column=target_column,
            ratio_column=ratio_column,
            temperature_column=temperature_column,
            pressure_column=pressure_column,
            humidity_column=humidity_column,
            temperature_offset_c=temperature_offset_c,
            model_features=model_features,
        )
        residual_mask = detect_residual_outliers(
            residual_dataset.feature_matrix,
            residual_dataset.target_vector,
            fit_method=fit_method,
            ridge_lambda=ridge_lambda,
            std_multiplier=residual_std_multiplier,
        )
        residual_indices = kept_after_iqr.index.to_numpy()[residual_mask]
        details["residual_outliers"] = int(np.sum(residual_mask))
        if residual_indices.size:
            outlier_mask |= original.index.isin(residual_indices)

    kept_frame = original.loc[~outlier_mask].copy()
    removed_indices = original.index[outlier_mask].astype(int).tolist()

    _emit_log(log_fn, "异常点检测：")
    _emit_log(log_fn, f"原始样本 = {len(original)}")
    _emit_log(log_fn, f"IQR异常点 = {details['iqr_outliers']}")
    _emit_log(log_fn, f"残差异常点 = {details['residual_outliers']}")
    _emit_log(log_fn, f"最终异常点 = {int(np.sum(outlier_mask))}")
    _emit_log(log_fn, f"最终样本 = {len(kept_frame)}")

    return OutlierDetectionResult(
        original_count=len(original),
        outlier_count=int(np.sum(outlier_mask)),
        final_count=len(kept_frame),
        kept_frame=kept_frame,
        removed_indices=removed_indices,
        details=details,
    )
