"""Temperature compensation fitting helpers for V2."""

from __future__ import annotations

import math
import warnings
from typing import Any, Iterable, Sequence

import numpy as np

from ...senco_format import format_senco_coeffs as _format_senco_coeffs


DEFAULT_TEMPERATURE_COMPENSATION_COEFFS: tuple[float, float, float, float] = (0.0, 1.0, 0.0, 0.0)


def _clean_pairs(raw_temps: Iterable[Any], ref_temps: Iterable[Any]) -> tuple[np.ndarray, np.ndarray]:
    pairs: list[tuple[float, float]] = []
    for raw_value, ref_value in zip(list(raw_temps), list(ref_temps)):
        try:
            raw_float = float(raw_value)
            ref_float = float(ref_value)
        except Exception:
            continue
        if not (math.isfinite(raw_float) and math.isfinite(ref_float)):
            continue
        pairs.append((raw_float, ref_float))
    if not pairs:
        return np.asarray([], dtype=float), np.asarray([], dtype=float)
    raw = np.asarray([pair[0] for pair in pairs], dtype=float)
    ref = np.asarray([pair[1] for pair in pairs], dtype=float)
    return raw, ref


def _coefficients_from_polyfit(polyfit_coeffs: Sequence[float], degree: int) -> tuple[float, float, float, float]:
    coeffs = [float(value) for value in polyfit_coeffs]
    if degree == 3:
        d, c, b, a = coeffs
        return a, b, c, d
    if degree == 2:
        c, b, a = coeffs
        return a, b, c, 0.0
    if degree == 1:
        b, a = coeffs
        return a, b, 0.0, 0.0
    raise ValueError(f"Unsupported polynomial degree: {degree}")


def fit_temperature_compensation(
    raw_temps: Iterable[Any],
    ref_temps: Iterable[Any],
    polynomial_order: int = 3,
) -> dict[str, Any]:
    """Fit `Y = A + B*T + C*T^2 + D*T^3` with downgrade-on-low-sample logic."""

    raw, ref = _clean_pairs(raw_temps, ref_temps)
    n_points = int(raw.size)
    default_result = {
        "A": float(DEFAULT_TEMPERATURE_COMPENSATION_COEFFS[0]),
        "B": float(DEFAULT_TEMPERATURE_COMPENSATION_COEFFS[1]),
        "C": float(DEFAULT_TEMPERATURE_COMPENSATION_COEFFS[2]),
        "D": float(DEFAULT_TEMPERATURE_COMPENSATION_COEFFS[3]),
        "rmse": None,
        "max_abs_error": None,
        "n_points": n_points,
        "fit_ok": False,
        "polynomial_degree_used": 0,
    }
    if n_points < 2:
        return default_result

    requested_degree = max(1, int(polynomial_order or 3))
    degree = min(requested_degree, 3, n_points - 1)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            polyfit_coeffs = np.polyfit(raw, ref, deg=degree)
        a, b, c, d = _coefficients_from_polyfit(polyfit_coeffs, degree)
        predicted = a + b * raw + c * (raw**2) + d * (raw**3)
        residuals = predicted - ref
        rmse = float(np.sqrt(np.mean(residuals**2))) if residuals.size else 0.0
        max_abs_error = float(np.max(np.abs(residuals))) if residuals.size else 0.0
        return {
            "A": float(a),
            "B": float(b),
            "C": float(c),
            "D": float(d),
            "rmse": rmse,
            "max_abs_error": max_abs_error,
            "n_points": n_points,
            "fit_ok": True,
            "polynomial_degree_used": degree,
        }
    except Exception:
        return default_result


def format_senco_coeffs(coeffs: Sequence[Any]) -> tuple[str, str, str, str]:
    """Format A/B/C/D with scientific notation for SENCO export."""

    return _format_senco_coeffs(coeffs)
