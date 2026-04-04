from __future__ import annotations

import numpy as np

from .base import AlgorithmBase
from .result_types import FitResult, ValidationResult
from .validator import BackValidator


class PolynomialAlgorithm(AlgorithmBase):
    """Polynomial fit: y = a*x^n + ... + c."""

    def __init__(self, name: str = "polynomial", config: dict | None = None):
        super().__init__(name, config)
        self.degree = int(self.config.get("degree", 2))

    def fit(self, samples, point_results) -> FitResult:
        x_values, y_values = self._extract_point_pairs(point_results)
        x = np.asarray(x_values, dtype=float)
        y = np.asarray(y_values, dtype=float)
        if len(x) < self.degree + 1:
            return FitResult(
                algorithm_name=self.name,
                coefficients={},
                r_squared=0.0,
                rmse=0.0,
                valid=False,
                message="Insufficient data points",
            )

        coeff_array = np.polyfit(x, y, self.degree)
        y_pred = np.polyval(coeff_array, x)
        residuals = y - y_pred
        ss_res = float(np.sum(residuals**2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r_squared = 1.0 - (ss_res / ss_tot if ss_tot > 0 else 0.0)
        rmse = float(np.sqrt(np.mean(residuals**2)))
        mae = float(np.mean(np.abs(residuals)))
        max_error = float(np.max(np.abs(residuals))) if len(residuals) else 0.0
        coefficients = {
            f"coef_{self.degree - index}": float(value)
            for index, value in enumerate(coeff_array)
        }
        p = len(coefficients)
        adjusted_r_squared = r_squared
        if len(x) > p:
            adjusted_r_squared = 1.0 - (1.0 - r_squared) * (len(x) - 1) / (len(x) - p)
        return FitResult(
            algorithm_name=self.name,
            algorithm_spec=self.get_spec(),
            coefficients=coefficients,
            coefficient_names=list(coefficients.keys()),
            r_squared=r_squared,
            adjusted_r_squared=adjusted_r_squared,
            rmse=rmse,
            mae=mae,
            max_error=max_error,
            valid=True,
            residuals=residuals.tolist(),
        )

    def validate(self, fit_result, samples) -> ValidationResult:
        return BackValidator().validate(
            fit_result,
            samples,
            tolerance=float(self.config.get("tolerance", 0.05)),
        )

    def predict(self, coefficients, inputs) -> float:
        x = float(inputs.get("x", 0.0))
        ordered = sorted(
            ((int(key.split("_", 1)[1]), float(value)) for key, value in coefficients.items() if key.startswith("coef_")),
            key=lambda item: item[0],
            reverse=True,
        )
        if not ordered:
            return 0.0
        coeff_array = [value for _, value in ordered]
        return float(np.polyval(coeff_array, x))
