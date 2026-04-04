"""逐点回代验证与对账分析。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Sequence

import numpy as np
import pandas as pd

from .model_fit import predict_with_coefficients
from .model_metrics import compute_metrics


@dataclass
class PredictionAnalysisResult:
    """保存逐点预测分析结果。"""

    point_table: pd.DataFrame
    summary: Dict[str, Any]
    range_table: pd.DataFrame
    top_error_orig: pd.DataFrame
    top_error_simple: pd.DataFrame
    top_pred_diff: pd.DataFrame


def _safe_relative_error(errors: np.ndarray, truth: np.ndarray) -> np.ndarray:
    """计算相对误差，真实值为 0 时返回 NaN。"""
    result = np.full(truth.shape, np.nan, dtype=float)
    nonzero_mask = np.abs(truth) > 1e-12
    result[nonzero_mask] = errors[nonzero_mask] / truth[nonzero_mask] * 100.0
    return result


def _max_abs_ignore_nan(values: np.ndarray) -> float:
    """返回忽略 NaN 后的最大绝对值。"""
    valid = values[~np.isnan(values)]
    if valid.size == 0:
        return 0.0
    return float(np.max(np.abs(valid)))


def _mean_abs(values: np.ndarray) -> float:
    """返回平均绝对误差。"""
    return float(np.mean(np.abs(values))) if values.size else 0.0


def _top_by_column(point_table: pd.DataFrame, column: str, *, top_n: int) -> pd.DataFrame:
    """按指定列降序取 Top N。"""
    if point_table.empty:
        return point_table.copy()
    return point_table.sort_values(column, ascending=False).head(int(top_n)).reset_index(drop=True)


def analyze_predictions(
    x_matrix: np.ndarray,
    y_true: np.ndarray,
    a_orig: np.ndarray,
    a_simple: np.ndarray,
    *,
    top_n: int = 10,
    sample_index: Sequence[Any] | None = None,
) -> PredictionAnalysisResult:
    """对原始系数和简化系数做逐点回代分析。"""
    x = np.asarray(x_matrix, dtype=float)
    truth = np.asarray(y_true, dtype=float).reshape(-1)
    orig = np.asarray(a_orig, dtype=float).reshape(-1)
    simple = np.asarray(a_simple, dtype=float).reshape(-1)

    if x.ndim != 2:
        raise ValueError("x_matrix must be a 2D array")
    if truth.ndim != 1:
        raise ValueError("y_true must be a 1D array")
    if x.shape[0] != truth.shape[0]:
        raise ValueError("x_matrix and y_true must contain the same sample count")
    if x.shape[1] != orig.shape[0] or x.shape[1] != simple.shape[0]:
        raise ValueError("Coefficient count must match feature count")

    point_ids = list(sample_index) if sample_index is not None else list(range(len(truth)))
    if len(point_ids) != len(truth):
        raise ValueError("sample_index length must match y_true length")

    pred_orig = predict_with_coefficients(x, orig)
    pred_simple = predict_with_coefficients(x, simple)
    error_orig = pred_orig - truth
    error_simple = pred_simple - truth
    rel_error_orig = _safe_relative_error(error_orig, truth)
    rel_error_simple = _safe_relative_error(error_simple, truth)
    pred_diff = pred_simple - pred_orig

    point_table = pd.DataFrame(
        {
            "index": point_ids,
            "Y_true": truth,
            "Y_pred_orig": pred_orig,
            "error_orig": error_orig,
            "rel_error_orig_pct": rel_error_orig,
            "Y_pred_simple": pred_simple,
            "error_simple": error_simple,
            "rel_error_simple_pct": rel_error_simple,
            "pred_diff": pred_diff,
        }
    )
    point_table["abs_error_orig"] = np.abs(point_table["error_orig"])
    point_table["abs_error_simple"] = np.abs(point_table["error_simple"])
    point_table["abs_pred_diff"] = np.abs(point_table["pred_diff"])

    summary = {
        "sample_count": int(len(truth)),
        "rmse_orig": float(compute_metrics(truth, pred_orig)["RMSE"]),
        "rmse_simple": float(compute_metrics(truth, pred_simple)["RMSE"]),
        "mae_orig": _mean_abs(error_orig),
        "mae_simple": _mean_abs(error_simple),
        "max_abs_error_orig": float(np.max(np.abs(error_orig))) if error_orig.size else 0.0,
        "max_abs_error_simple": float(np.max(np.abs(error_simple))) if error_simple.size else 0.0,
        "max_rel_error_orig_pct": _max_abs_ignore_nan(rel_error_orig),
        "max_rel_error_simple_pct": _max_abs_ignore_nan(rel_error_simple),
    }

    top_error_orig = _top_by_column(point_table, "abs_error_orig", top_n=top_n)
    top_error_simple = _top_by_column(point_table, "abs_error_simple", top_n=top_n)
    top_pred_diff = _top_by_column(point_table, "abs_pred_diff", top_n=top_n)

    return PredictionAnalysisResult(
        point_table=point_table,
        summary=summary,
        range_table=pd.DataFrame(),
        top_error_orig=top_error_orig,
        top_error_simple=top_error_simple,
        top_pred_diff=top_pred_diff,
    )


def analyze_by_range(
    y_true: np.ndarray,
    error_orig: np.ndarray,
    error_simple: np.ndarray,
    bins: Sequence[float],
) -> pd.DataFrame:
    """按真实值区间统计原始/简化模型误差。"""
    truth = np.asarray(y_true, dtype=float).reshape(-1)
    orig = np.asarray(error_orig, dtype=float).reshape(-1)
    simple = np.asarray(error_simple, dtype=float).reshape(-1)

    if truth.shape != orig.shape or truth.shape != simple.shape:
        raise ValueError("y_true, error_orig and error_simple must share the same shape")
    if len(bins) < 2:
        raise ValueError("bins must contain at least two edges")

    rows = []
    last_index = len(bins) - 2
    for idx, (start, end) in enumerate(zip(bins[:-1], bins[1:])):
        if idx == last_index:
            mask = (truth >= start) & (truth <= end)
        else:
            mask = (truth >= start) & (truth < end)
        count = int(np.sum(mask))
        if count == 0:
            rows.append(
                {
                    "range_label": f"{start}-{end}",
                    "range_start": float(start),
                    "range_end": float(end),
                    "count": 0,
                    "rmse_orig": None,
                    "rmse_simple": None,
                    "mean_error_orig": None,
                    "mean_error_simple": None,
                    "max_abs_error_orig": None,
                    "max_abs_error_simple": None,
                }
            )
            continue
        orig_slice = orig[mask]
        simple_slice = simple[mask]
        rows.append(
            {
                "range_label": f"{start}-{end}",
                "range_start": float(start),
                "range_end": float(end),
                "count": count,
                "rmse_orig": float(np.sqrt(np.mean(orig_slice**2))),
                "rmse_simple": float(np.sqrt(np.mean(simple_slice**2))),
                "mean_error_orig": float(np.mean(orig_slice)),
                "mean_error_simple": float(np.mean(simple_slice)),
                "max_abs_error_orig": float(np.max(np.abs(orig_slice))),
                "max_abs_error_simple": float(np.max(np.abs(simple_slice))),
            }
        )
    return pd.DataFrame(rows)
