"""模型评估指标与分区间误差分析。"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Sequence

import numpy as np


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """计算 RMSE、R2、Bias 和 MaxError。"""
    truth = np.asarray(y_true, dtype=float)
    pred = np.asarray(y_pred, dtype=float)
    if truth.shape != pred.shape:
        raise ValueError("y_true and y_pred must have the same shape")
    if truth.size == 0:
        raise ValueError("Cannot compute metrics for an empty dataset")

    residuals = pred - truth
    sse = float(np.sum((truth - pred) ** 2))
    centered = truth - float(np.mean(truth))
    sst = float(np.sum(centered**2))
    if sst == 0.0:
        r2 = 1.0 if sse == 0.0 else 0.0
    else:
        r2 = 1.0 - sse / sst

    return {
        "RMSE": math.sqrt(float(np.mean((truth - pred) ** 2))),
        "R2": float(r2),
        "Bias": float(np.mean(residuals)),
        "MaxError": float(np.max(np.abs(residuals))),
    }


def analyze_error_by_range(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    bins: Sequence[float],
) -> List[Dict[str, Any]]:
    """按目标值区间统计误差指标。"""
    truth = np.asarray(y_true, dtype=float)
    pred = np.asarray(y_pred, dtype=float)
    if truth.shape != pred.shape:
        raise ValueError("y_true and y_pred must have the same shape")
    if len(bins) < 2:
        raise ValueError("bins must contain at least two edges")

    results: List[Dict[str, Any]] = []
    last_index = len(bins) - 2
    for idx, (start, end) in enumerate(zip(bins[:-1], bins[1:])):
        if idx == last_index:
            mask = (truth >= start) & (truth <= end)
        else:
            mask = (truth >= start) & (truth < end)

        count = int(np.sum(mask))
        if count == 0:
            results.append(
                {
                    "RangeLabel": f"{start}-{end}",
                    "RangeStart": float(start),
                    "RangeEnd": float(end),
                    "Count": 0,
                    "RMSE": None,
                    "R2": None,
                    "Bias": None,
                    "MaxError": None,
                }
            )
            continue

        metrics = compute_metrics(truth[mask], pred[mask])
        results.append(
            {
                "RangeLabel": f"{start}-{end}",
                "RangeStart": float(start),
                "RangeEnd": float(end),
                "Count": count,
                **metrics,
            }
        )
    return results
