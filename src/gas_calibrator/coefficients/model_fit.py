"""线性拟合与回代预测工具。"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


SUPPORTED_FIT_METHODS = ("least_squares", "ridge_like")


@dataclass
class LeastSquaresFitResult:
    """线性拟合结果。"""

    coefficients: np.ndarray
    predictions: np.ndarray


def fit_least_squares(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
) -> LeastSquaresFitResult:
    """使用 numpy 最小二乘求解原始系数。"""
    coefficients, _, _, _ = np.linalg.lstsq(x_matrix, y_vector, rcond=None)
    coefficients = np.asarray(coefficients, dtype=float)
    predictions = predict_with_coefficients(x_matrix, coefficients)
    return LeastSquaresFitResult(coefficients=coefficients, predictions=predictions)


def fit_ridge_like(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    *,
    ridge_lambda: float,
) -> LeastSquaresFitResult:
    """使用类岭回归公式求解系数，增强相关特征下的稳定性。"""
    if ridge_lambda < 0:
        raise ValueError("ridge_lambda must be >= 0")
    xtx = np.asarray(x_matrix.T @ x_matrix, dtype=float)
    xty = np.asarray(x_matrix.T @ y_vector, dtype=float)
    identity = np.eye(xtx.shape[0], dtype=float)
    coefficients = np.linalg.solve(xtx + float(ridge_lambda) * identity, xty)
    predictions = predict_with_coefficients(x_matrix, coefficients)
    return LeastSquaresFitResult(coefficients=np.asarray(coefficients, dtype=float), predictions=predictions)


def fit_linear_model(
    x_matrix: np.ndarray,
    y_vector: np.ndarray,
    *,
    method: str = "least_squares",
    ridge_lambda: float = 1e-6,
) -> LeastSquaresFitResult:
    """按指定方法执行线性拟合。"""
    method_name = str(method or "least_squares").strip().lower()
    if method_name == "least_squares":
        return fit_least_squares(x_matrix, y_vector)
    if method_name == "ridge_like":
        return fit_ridge_like(x_matrix, y_vector, ridge_lambda=ridge_lambda)
    raise ValueError(f"Unsupported fit method: {method}")


def predict_with_coefficients(
    x_matrix: np.ndarray,
    coefficients: np.ndarray,
) -> np.ndarray:
    """根据系数回代预测值。"""
    return np.asarray(x_matrix @ coefficients, dtype=float)
