from __future__ import annotations

from typing import Any

import numpy as np

from .result_types import FitResult, ValidationResult


class BackValidator:
    """Back-validator for fitted coefficients."""

    def validate(
        self,
        fit_result: FitResult,
        samples: list[Any],
        tolerance: float = 0.05,
    ) -> ValidationResult:
        xs: list[float] = []
        ys: list[float] = []
        for item in samples:
            extra = getattr(item, "extra", None)
            x = extra.get("x") if isinstance(extra, dict) else None
            y = extra.get("y") if isinstance(extra, dict) else None
            if x is None:
                x = getattr(item, "co2", None)
            if y is None:
                y = getattr(item, "h2o", None)
            if x is None or y is None:
                continue
            xs.append(float(x))
            ys.append(float(y))

        if not xs:
            fit_result.confidence = 0.0
            fit_result.confidence_level = self.get_confidence_level(fit_result.confidence)
            return ValidationResult(
                algorithm_name=fit_result.algorithm_name,
                passed=False,
                r_squared=0.0,
                rmse=0.0,
                mae=0.0,
                sample_count=0,
                message="No validation samples",
            )

        y_true = np.asarray(ys, dtype=float)
        y_pred = np.asarray([self._predict(fit_result, x) for x in xs], dtype=float)
        residuals = y_true - y_pred
        mae = float(np.mean(np.abs(residuals)))
        rmse = float(np.sqrt(np.mean(residuals**2)))
        ss_res = float(np.sum(residuals**2))
        ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
        r_squared = 1.0 - (ss_res / ss_tot if ss_tot > 0 else 0.0)
        outliers = [index for index, value in enumerate(np.abs(residuals)) if float(value) > tolerance]
        passed = rmse <= tolerance and mae <= tolerance
        fit_result.confidence = self.assess_confidence(fit_result)
        fit_result.confidence_level = self.get_confidence_level(fit_result.confidence)
        return ValidationResult(
            algorithm_name=fit_result.algorithm_name,
            passed=passed,
            r_squared=r_squared,
            rmse=rmse,
            mae=mae,
            sample_count=len(xs),
            outliers=outliers,
            message="ok" if passed else "validation tolerance exceeded",
        )

    def assess_confidence(self, fit_result: FitResult) -> float:
        r2_component = max(0.0, min(1.0, float(fit_result.r_squared)))
        residuals = fit_result.residuals or []
        point_count = len(residuals)
        sample_component = min(1.0, point_count / 10.0) if point_count else 0.5
        rmse_component = 1.0 / (1.0 + max(0.0, float(fit_result.rmse)))
        warning_penalty = min(0.2, len(getattr(fit_result, "warnings", [])) * 0.05)
        confidence = (r2_component * 0.5) + (sample_component * 0.2) + (rmse_component * 0.3) - warning_penalty
        return max(0.0, min(1.0, confidence))

    @staticmethod
    def get_confidence_level(confidence: float) -> str:
        if confidence >= 0.9:
            return "high"
        if confidence >= 0.7:
            return "medium"
        return "low"

    @staticmethod
    def _predict(fit_result: FitResult, x: float) -> float:
        coeffs = fit_result.coefficients
        if "slope" in coeffs and "intercept" in coeffs:
            return float(coeffs["slope"]) * x + float(coeffs["intercept"])

        polynomial_keys = [key for key in coeffs if key.startswith("coef_")]
        if polynomial_keys:
            ordered = sorted(polynomial_keys, key=lambda item: int(item.split("_", 1)[1]), reverse=True)
            coeff_list = [float(coeffs[key]) for key in ordered]
            return float(np.polyval(coeff_list, x))

        if "amt_a" in coeffs and "amt_b" in coeffs and "amt_c" in coeffs:
            return float(coeffs["amt_a"]) * x**2 + float(coeffs["amt_b"]) * x + float(coeffs["amt_c"])

        return 0.0
