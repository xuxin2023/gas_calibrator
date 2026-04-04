from __future__ import annotations

import numpy as np

from .base import AlgorithmBase
from .result_types import FitResult, ValidationResult
from .validator import BackValidator


class LinearAlgorithm(AlgorithmBase):
    """Linear fit: y = a*x + b."""

    def fit(self, samples, point_results) -> FitResult:
        x_values, y_values = self._extract_point_pairs(point_results)
        x = np.asarray(x_values, dtype=float)
        y = np.asarray(y_values, dtype=float)
        if len(x) < 2:
            return FitResult(
                algorithm_name=self.name,
                coefficients={},
                r_squared=0.0,
                rmse=0.0,
                valid=False,
                message="Insufficient data points",
            )

        coeffs = np.polyfit(x, y, 1)
        y_pred = np.polyval(coeffs, x)
        residuals = y - y_pred
        ss_res = float(np.sum(residuals**2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r_squared = 1.0 - (ss_res / ss_tot if ss_tot > 0 else 0.0)
        rmse = float(np.sqrt(np.mean(residuals**2)))
        mae = float(np.mean(np.abs(residuals)))
        max_error = float(np.max(np.abs(residuals))) if len(residuals) else 0.0
        adjusted_r_squared = r_squared if len(x) <= 2 else 1.0 - (1.0 - r_squared) * (len(x) - 1) / (len(x) - 2)
        return FitResult(
            algorithm_name=self.name,
            algorithm_spec=self.get_spec(),
            coefficients={"slope": float(coeffs[0]), "intercept": float(coeffs[1])},
            coefficient_names=["slope", "intercept"],
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
        return float(coefficients["slope"]) * float(inputs.get("x", 0.0)) + float(coefficients["intercept"])
