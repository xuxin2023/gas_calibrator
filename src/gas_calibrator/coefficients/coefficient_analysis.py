"""系数稳定性分析。"""

from __future__ import annotations

import math
from typing import Any, Dict

import numpy as np


def analyze_coefficient_stability(
    x_matrix: np.ndarray,
    coefficients: np.ndarray,
) -> Dict[str, Any]:
    """输出用于判断数值稳定性的统计指标。"""
    coeffs = np.asarray(coefficients, dtype=float)
    abs_coeffs = np.abs(coeffs)
    nonzero = abs_coeffs[abs_coeffs > 0]
    order_stats: Dict[str, int] = {}
    for value in nonzero:
        order = int(math.floor(math.log10(value)))
        key = f"1e{order}"
        order_stats[key] = order_stats.get(key, 0) + 1

    return {
        "condition_number": float(np.linalg.cond(np.asarray(x_matrix, dtype=float))),
        "max_abs_coefficient": float(np.max(abs_coeffs)) if abs_coeffs.size else 0.0,
        "min_nonzero_coefficient": float(np.min(nonzero)) if nonzero.size else 0.0,
        "nonzero_count": int(nonzero.size),
        "coefficient_order_stats": order_stats,
    }
