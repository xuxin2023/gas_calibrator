from __future__ import annotations

import numpy as np

from .base import AlgorithmBase
from .linear import LinearAlgorithm
from .result_types import FitResult, ValidationResult
from .validator import BackValidator


class RobustAlgorithm(AlgorithmBase):
    """Robust linear fit with simple MAD-based outlier rejection."""

    def fit(self, samples, point_results) -> FitResult:
        x_values, y_values = self._extract_point_pairs(point_results)
        x = np.asarray(x_values, dtype=float)
        y = np.asarray(y_values, dtype=float)
        if len(x) < 3:
            return FitResult(
                algorithm_name=self.name,
                coefficients={},
                r_squared=0.0,
                rmse=0.0,
                valid=False,
                message="Insufficient data points",
            )

        median_y = float(np.median(y))
        mad = float(np.median(np.abs(y - median_y))) or 1.0
        threshold = float(self.config.get("mad_scale", 3.5)) * mad
        mask = np.abs(y - median_y) <= threshold
        filtered_points = [
            {"mean_co2": float(x[index]), "mean_h2o": float(y[index])}
            for index in range(len(x))
            if bool(mask[index])
        ]
        linear = LinearAlgorithm(name=self.name, config=self.config)
        fit_result = linear.fit(samples, filtered_points)
        fit_result.algorithm_spec = self.get_spec()
        fit_result.message = "Robust fit after MAD filtering"
        if len(filtered_points) < len(point_results):
            fit_result.warnings.append(f"Filtered {len(point_results) - len(filtered_points)} outlier candidates before fit")
        return fit_result

    def validate(self, fit_result, samples) -> ValidationResult:
        return BackValidator().validate(
            fit_result,
            samples,
            tolerance=float(self.config.get("tolerance", 0.05)),
        )

    def predict(self, coefficients, inputs) -> float:
        return float(coefficients.get("slope", 0.0)) * float(inputs.get("x", 0.0)) + float(coefficients.get("intercept", 0.0))
