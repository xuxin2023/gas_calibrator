"""Numeric fitting helpers for the offline absorbance debugger."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

import numpy as np


@dataclass(frozen=True)
class PolynomialFit:
    """Simple polynomial fit container."""

    model_name: str
    degree: int
    coefficients_desc: tuple[float, ...]
    sample_count: int
    rmse: float
    mae: float
    r2: float | None

    def evaluate(self, values: Sequence[float] | np.ndarray) -> np.ndarray:
        """Evaluate the fitted polynomial on one or more x values."""

        return np.polyval(self.coefficients_desc, values)

    def formula(self, variable: str = "x", precision: int = 8) -> str:
        """Render the polynomial as a compact formula string."""

        terms: list[str] = []
        order = self.degree
        for idx, coeff in enumerate(self.coefficients_desc):
            power = order - idx
            coeff_text = f"{coeff:.{precision}g}"
            if power == 0:
                terms.append(coeff_text)
            elif power == 1:
                terms.append(f"{coeff_text}*{variable}")
            else:
                terms.append(f"{coeff_text}*{variable}^{power}")
        return " + ".join(terms).replace("+ -", "- ")


@dataclass(frozen=True)
class LinearFit:
    """Linear fit in y = intercept + slope * x form."""

    intercept: float
    slope: float
    sample_count: int
    rmse: float
    mae: float
    r2: float | None

    def evaluate(self, values: Sequence[float] | np.ndarray) -> np.ndarray:
        """Evaluate the fitted line."""

        arr = np.asarray(values, dtype=float)
        return self.intercept + self.slope * arr

    def formula(self, x_name: str = "x", precision: int = 8) -> str:
        """Render the line formula."""

        return (
            f"{self.intercept:.{precision}g}"
            f" + {self.slope:.{precision}g}*{x_name}"
        ).replace("+ -", "- ")


def _metrics(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[float, float, float | None]:
    residual = y_pred - y_true
    rmse = float(np.sqrt(np.mean(np.square(residual)))) if residual.size else math.nan
    mae = float(np.mean(np.abs(residual))) if residual.size else math.nan
    r2: float | None = None
    if y_true.size:
        centered = y_true - np.mean(y_true)
        ss_tot = float(np.sum(np.square(centered)))
        if ss_tot > 0:
            ss_res = float(np.sum(np.square(residual)))
            r2 = 1.0 - (ss_res / ss_tot)
    return rmse, mae, r2


def fit_polynomial(x: Sequence[float], y: Sequence[float], degree: int, model_name: str) -> PolynomialFit:
    """Fit a polynomial model using least squares."""

    x_arr = np.asarray(list(x), dtype=float)
    y_arr = np.asarray(list(y), dtype=float)
    if x_arr.size <= degree:
        raise ValueError(f"Need at least {degree + 1} samples for a degree-{degree} fit")
    coefficients = np.polyfit(x_arr, y_arr, deg=degree)
    predicted = np.polyval(coefficients, x_arr)
    rmse, mae, r2 = _metrics(y_arr, predicted)
    return PolynomialFit(
        model_name=model_name,
        degree=degree,
        coefficients_desc=tuple(float(item) for item in coefficients),
        sample_count=int(x_arr.size),
        rmse=rmse,
        mae=mae,
        r2=r2,
    )


def fit_linear(x: Sequence[float], y: Sequence[float]) -> LinearFit:
    """Fit a straight line."""

    poly = fit_polynomial(x, y, degree=1, model_name="linear")
    slope = float(poly.coefficients_desc[0])
    intercept = float(poly.coefficients_desc[1])
    return LinearFit(
        intercept=intercept,
        slope=slope,
        sample_count=poly.sample_count,
        rmse=poly.rmse,
        mae=poly.mae,
        r2=poly.r2,
    )


def rolling_lowpass(values: Sequence[float], window: int = 3) -> np.ndarray:
    """Apply a small centered moving average."""

    arr = np.asarray(list(values), dtype=float)
    if arr.size == 0:
        return arr
    width = max(1, int(window))
    kernel = np.ones(width, dtype=float) / width
    padded = np.pad(arr, pad_width=(width // 2, width - 1 - width // 2), mode="edge")
    return np.convolve(padded, kernel, mode="valid")


def clamp_positive(values: Sequence[float] | np.ndarray, floor: float) -> np.ndarray:
    """Clamp one or more values to a positive lower bound."""

    return np.clip(np.asarray(values, dtype=float), floor, None)
