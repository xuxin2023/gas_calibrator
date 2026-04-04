"""
AMT 拟合模块。

职责：
1. 从采样记录构建设计矩阵；
2. 使用最小二乘求解 AMT 方程系数；
3. 输出统计指标与残差；
4. 保存 JSON/CSV 拟合报告。
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import numpy as np

# 默认候选键：用于从不同来源样本中自动挑选可用字段。
DEFAULT_TEMP_KEYS = ("chamber_temp_c", "case_temp_c", "temp_c", "temp_set_c")
DEFAULT_PRESSURE_KEYS = (
    "pressure_hpa",
    "pressure_kpa",
    "pressure_gauge_raw",
    "pressure_target_hpa",
)
DEFAULT_CO2_SIGNAL_KEYS = ("co2_signal", "co2_ratio_raw", "co2_ratio_f", "co2_sig")
DEFAULT_H2O_SIGNAL_KEYS = ("h2o_signal", "h2o_ratio_raw", "h2o_ratio_f", "h2o_sig")


@dataclass
class FitResult:
    """拟合结果结构体。"""

    model: str
    gas: str
    order: int
    n: int
    coeffs: Dict[str, float]
    stats: Dict[str, float]
    residuals: List[Dict[str, Any]]


def _first_float(sample: Dict[str, Any], keys: Sequence[str]) -> Optional[float]:
    """按候选键顺序返回首个可转换为 float 的值。"""
    for key in keys:
        if not key:
            continue
        val = sample.get(key)
        if val is None:
            continue
        try:
            return float(val)
        except Exception:
            continue
    return None


def _pressure_hpa(sample: Dict[str, Any], keys: Sequence[str]) -> Optional[float]:
    """
    提取压力并统一换算到 hPa。

    约定：若键名包含 `kpa`，则自动乘以 10 转换为 hPa。
    """
    for key in keys:
        if not key:
            continue
        val = sample.get(key)
        if val is None:
            continue
        try:
            val = float(val)
        except Exception:
            continue
        if "kpa" in key.lower():
            return val * 10.0
        return val
    return None


def _build_row(I1: float, T_k: float, p_hpa: float, order: int, p0_hpa: float) -> List[float]:
    """
    构建设计矩阵单行。

    特征顺序：
    [1, ln(I1), T, T^2..., T/I1, T^2/I1..., (p-p0)/p0]
    """
    row = [1.0, math.log(I1)]
    for i in range(1, order + 1):
        row.append(T_k**i)
    for i in range(1, order + 1):
        row.append((T_k**i) / I1)
    row.append((p_hpa - p0_hpa) / p0_hpa)
    return row


def fit_amt_eq4(
    samples: Iterable[Dict[str, Any]],
    *,
    gas: str,
    target_key: str,
    signal_keys: Optional[Sequence[str]] = None,
    temp_keys: Optional[Sequence[str]] = None,
    pressure_keys: Optional[Sequence[str]] = None,
    order: int = 2,
    p0_hpa: float = 1013.25,
    t0_k: float = 273.15,
    dry_air_correction: bool = False,
    h2o_source: str = "target",
    h2o_target_key: str = "h2o_mmol_target",
    h2o_meas_key: str = "h2o_mmol",
    min_samples: int = 0,
) -> FitResult:
    """
    执行 AMT EQ4 拟合。

    参数说明（核心）：
    - `target_key`：目标浓度字段；
    - `signal_keys`：原始信号字段候选；
    - `order`：温度项阶次；
    - `dry_air_correction`：是否按干空气进行目标修正。
    """
    temp_keys = tuple(temp_keys) if temp_keys else DEFAULT_TEMP_KEYS
    pressure_keys = tuple(pressure_keys) if pressure_keys else DEFAULT_PRESSURE_KEYS
    if signal_keys is None:
        signal_keys = DEFAULT_CO2_SIGNAL_KEYS if gas.lower() == "co2" else DEFAULT_H2O_SIGNAL_KEYS
    else:
        signal_keys = tuple(signal_keys)

    # 未知数数量：k0,k1 + u1..un + v1..vn + w1
    num_coeffs = 3 + 2 * order
    required = max(min_samples, num_coeffs)

    rows: List[List[float]] = []
    y: List[float] = []
    meta: List[Dict[str, Any]] = []

    for sample in samples:
        target = _first_float(sample, (target_key,))
        if target is None:
            continue

        I1 = _first_float(sample, signal_keys)
        if I1 is None or I1 <= 0:
            continue

        T_c = _first_float(sample, temp_keys)
        if T_c is None:
            continue
        T_k = T_c + 273.15

        p_hpa = _pressure_hpa(sample, pressure_keys)
        if p_hpa is None or p_hpa <= 0:
            continue

        # 目标浓度可选进行干空气修正。
        chi = float(target)
        h2o_mmol = None
        if dry_air_correction:
            if h2o_source == "measured":
                h2o_mmol = _first_float(sample, (h2o_meas_key,))
            else:
                h2o_mmol = _first_float(sample, (h2o_target_key,))
            if h2o_mmol:
                chi_h2o = h2o_mmol / 1000.0
                denom = max(1e-6, 1.0 - chi_h2o)
                chi = chi / denom

        # 按模型定义将浓度目标映射到回归目标 y。
        y_val = chi * p_hpa * t0_k / (p0_hpa * T_k)
        rows.append(_build_row(I1, T_k, p_hpa, order, p0_hpa))
        y.append(y_val)
        meta.append(
            {
                "target": float(target),
                "chi_corr": float(chi),
                "p_hpa": float(p_hpa),
                "T_k": float(T_k),
                "signal": float(I1),
                "h2o_mmol": None if h2o_mmol is None else float(h2o_mmol),
            }
        )

    if len(rows) < required:
        raise ValueError(f"Not enough samples for fit: {len(rows)} < {required}")

    # 最小二乘求解系数向量。
    X = np.asarray(rows, dtype=float)
    Y = np.asarray(y, dtype=float)
    coef, _, _, _ = np.linalg.lstsq(X, Y, rcond=None)

    coeffs: Dict[str, float] = {"k0": float(coef[0]), "k1": float(coef[1])}
    idx = 2
    for i in range(1, order + 1):
        coeffs[f"u{i}"] = float(coef[idx])
        idx += 1
    for i in range(1, order + 1):
        coeffs[f"v{i}"] = float(coef[idx])
        idx += 1
    coeffs["w1"] = float(coef[idx])

    # 回代得到预测浓度并计算残差。
    y_hat = X @ coef
    chi_pred: List[float] = []
    for i, m in enumerate(meta):
        pred = y_hat[i] * (p0_hpa * m["T_k"]) / (m["p_hpa"] * t0_k)
        chi_pred.append(pred)

    target_corr = np.asarray([m["chi_corr"] for m in meta], dtype=float)
    pred_corr = np.asarray(chi_pred, dtype=float)
    err = pred_corr - target_corr

    rmse = float(np.sqrt(np.mean(err**2)))
    mae = float(np.mean(np.abs(err)))
    max_abs = float(np.max(np.abs(err)))
    r2 = 0.0
    if len(target_corr) > 1:
        ss_res = float(np.sum(err**2))
        ss_tot = float(np.sum((target_corr - np.mean(target_corr)) ** 2))
        r2 = 1.0 - (ss_res / ss_tot if ss_tot > 0 else 0.0)

    residuals: List[Dict[str, Any]] = []
    for i, m in enumerate(meta):
        residuals.append(
            {
                "target": m["target"],
                "target_corr": m["chi_corr"],
                "pred_corr": float(pred_corr[i]),
                "error_corr": float(err[i]),
                "signal": m["signal"],
                "p_hpa": m["p_hpa"],
                "T_k": m["T_k"],
                "h2o_mmol": m["h2o_mmol"],
            }
        )

    stats = {"rmse": rmse, "mae": mae, "max_abs": max_abs, "r2": r2}
    return FitResult(model="amt_eq4", gas=gas, order=order, n=len(rows), coeffs=coeffs, stats=stats, residuals=residuals)


def save_fit_report(
    result: FitResult,
    out_dir: Path,
    prefix: str,
    include_residuals: bool = True,
) -> Dict[str, Path]:
    """
    保存拟合报告。

    输出：
    - JSON：核心系数与统计；
    - CSV（可选）：逐样本残差明细。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"{prefix}_fit_{stamp}.json"

    payload = {
        "model": result.model,
        "gas": result.gas,
        "order": result.order,
        "n": result.n,
        "coeffs": result.coeffs,
        "stats": result.stats,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    paths: Dict[str, Path] = {"json": json_path}
    if include_residuals:
        csv_path = out_dir / f"{prefix}_fit_{stamp}_residuals.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            if result.residuals:
                writer = csv.DictWriter(f, fieldnames=list(result.residuals[0].keys()))
                writer.writeheader()
                writer.writerows(result.residuals)
            else:
                f.write("")
        paths["csv"] = csv_path
    return paths

