"""
离线点表异常点筛选、重拟合与审计导出核心逻辑。

该模块只处理离线点表，不依赖原始秒级时序数据。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from ..config.offline_modeling import OfflineColumnConfig, OfflineRefitConfig, RefitFilteringConfig, SimplificationConfig
from ..exceptions import ConfigurationInvalidError, DataValidationError


FEATURE_NAMES: List[str] = ["a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"]
FEATURE_TERMS: List[str] = ["1", "R", "R^2", "R^3", "T_k", "T_k^2", "R*T_k", "P", "R*T_k*P"]
SUPPORTED_SIMPLIFICATION_METHODS = ("column_norm", "standardize", "range_scale")


@dataclass
class LinearFitMetrics:
    """单次拟合的核心指标。"""

    rmse: float
    r2: float
    bias: float
    max_error: float
    mae: float
    max_prediction_delta: float
    mean_prediction_delta: float
    condition_number: float


@dataclass
class LinearFitBundle:
    """单次拟合结果。"""

    original_coefficients: np.ndarray
    simplified_coefficients: np.ndarray
    metrics_original: LinearFitMetrics
    metrics_simplified: LinearFitMetrics
    predictions_original: np.ndarray
    predictions_simplified: np.ndarray
    residuals_original: np.ndarray
    residuals_simplified: np.ndarray
    coefficient_summary: str


@dataclass
class RefitFilteringResult:
    """筛选重算完整结果。"""

    audit_frame: pd.DataFrame
    summary_frame: pd.DataFrame
    compare_frame: pd.DataFrame
    original_coefficients_frame: pd.DataFrame
    simplified_coefficients_frame: pd.DataFrame


def _emit_log(log_fn: Optional[Callable[[str], None]], message: str) -> None:
    if log_fn is not None:
        log_fn(message)


def _resolve_existing_column(frame: pd.DataFrame, candidates: Sequence[str], label: str) -> str:
    for candidate in candidates:
        if candidate and candidate in frame.columns:
            return candidate
    raise ConfigurationInvalidError(label, list(candidates), reason="未在输入表中找到对应列")


def _resolve_optional_column(frame: pd.DataFrame, candidates: Sequence[str]) -> str:
    for candidate in candidates:
        if candidate and candidate in frame.columns:
            return candidate
    return ""


def resolve_column_mapping(
    frame: pd.DataFrame,
    *,
    gas_type: str,
    columns: OfflineColumnConfig,
) -> OfflineColumnConfig:
    """自动适配真实字段名，优先使用显式配置。"""
    gas_lower = str(gas_type).strip().lower()
    target_candidates = [columns.target]
    ratio_candidates = [columns.ratio]
    if gas_lower == "co2":
        target_candidates.extend(["Y_true", "ppm_CO2_Tank", "ppm_CO2"])
        ratio_candidates.extend(["R", "R_CO2"])
    else:
        target_candidates.extend(["Y_true", "ppm_H2O_Dew", "ppm_H2O"])
        ratio_candidates.extend(["R", "R_H2O"])
    temperature_candidates = [columns.temperature, "T1", "Temp", "T_k"]
    pressure_candidates = [columns.pressure, "BAR", "P"]

    return OfflineColumnConfig(
        analyzer_id=_resolve_optional_column(frame, [columns.analyzer_id, "Analyzer", "analyzer_id"]),
        row_index=_resolve_optional_column(frame, [columns.row_index, "PointRow", "NUM", "index"]),
        phase=_resolve_optional_column(frame, [columns.phase, "PointPhase"]),
        point_tag=_resolve_optional_column(frame, [columns.point_tag, "PointTag"]),
        point_title=_resolve_optional_column(frame, [columns.point_title, "PointTitle"]),
        target=_resolve_existing_column(frame, target_candidates, "target"),
        ratio=_resolve_existing_column(frame, ratio_candidates, "ratio"),
        temperature=_resolve_existing_column(frame, temperature_candidates, "temperature"),
        pressure=_resolve_existing_column(frame, pressure_candidates, "pressure"),
        tk=columns.tk if columns.tk in frame.columns else "T_k",
    )


def _build_feature_matrix(frame: pd.DataFrame) -> np.ndarray:
    ratio = frame["R"].to_numpy(dtype=float)
    tk = frame["T_k"].to_numpy(dtype=float)
    pressure = frame["P"].to_numpy(dtype=float)
    columns = [
        np.ones_like(ratio, dtype=float),
        ratio,
        ratio**2,
        ratio**3,
        tk,
        tk**2,
        ratio * tk,
        pressure,
        ratio * tk * pressure,
    ]
    return np.column_stack(columns).astype(float, copy=False)


def _fit_least_squares(x_matrix: np.ndarray, y_true: np.ndarray) -> np.ndarray:
    coefficients, _, _, _ = np.linalg.lstsq(x_matrix, y_true, rcond=None)
    return np.asarray(coefficients, dtype=float)


def _round_significant(values: np.ndarray, digits: int) -> np.ndarray:
    rounded = np.asarray(values, dtype=float).copy()
    for index, value in enumerate(rounded):
        if value == 0:
            rounded[index] = 0.0
            continue
        magnitude = int(np.floor(np.log10(abs(value))))
        decimals = max(0, min(15, digits - 1 - magnitude))
        rounded[index] = round(float(value), decimals)
    return rounded


def _simplify_coefficients(
    x_matrix: np.ndarray,
    y_true: np.ndarray,
    *,
    method: str,
    digits: int,
) -> np.ndarray:
    method_name = str(method).strip().lower()
    if method_name not in SUPPORTED_SIMPLIFICATION_METHODS:
        raise ConfigurationInvalidError("simplification.method", method, reason="不支持的系数简化方法")
    if method_name == "column_norm":
        norms = np.sqrt(np.sum(x_matrix**2, axis=0))
        norms[0] = 1.0
        magnitude = np.floor(np.log10(np.abs(norms) + 1e-100))
        scale = np.clip(10 ** (magnitude - (digits - 2)), 1e-10, 1e10)
        inverse = np.diag(1.0 / scale)
        scaled = x_matrix @ inverse
        coefficients = inverse @ _fit_least_squares(scaled, y_true)
        return _round_significant(coefficients, digits)
    if method_name == "standardize":
        means = np.mean(x_matrix, axis=0)
        stds = np.std(x_matrix, axis=0)
        means[0] = 0.0
        stds[0] = 1.0
        stds[stds == 0] = 1.0
        scaled = (x_matrix - means) / stds
        coefficients = _fit_least_squares(scaled, y_true)
        original = coefficients.copy()
        for idx in range(original.shape[0]):
            original[idx] = coefficients[idx] / stds[idx]
        original[0] = float(np.mean(y_true)) - float(np.sum(original[1:] * means[1:]))
        return _round_significant(original, digits)
    mins = np.min(x_matrix, axis=0)
    ranges = np.max(x_matrix, axis=0) - mins
    mins[0] = 0.0
    ranges[0] = 1.0
    ranges[ranges == 0] = 1.0
    scaled = (x_matrix - mins) / ranges
    coefficients = _fit_least_squares(scaled, y_true)
    original = coefficients.copy()
    for idx in range(original.shape[0]):
        original[idx] = coefficients[idx] / ranges[idx]
    original[0] = coefficients[0] - float(np.sum(original[1:] * mins[1:]))
    return _round_significant(original, digits)


def _predict(x_matrix: np.ndarray, coefficients: np.ndarray) -> np.ndarray:
    return np.asarray(x_matrix @ coefficients, dtype=float)


def _compute_metrics(
    y_true: np.ndarray,
    prediction_original: np.ndarray,
    prediction_simplified: np.ndarray,
    *,
    x_matrix: np.ndarray,
) -> Tuple[LinearFitMetrics, LinearFitMetrics]:
    error_orig = prediction_original - y_true
    error_simple = prediction_simplified - y_true
    sse_orig = float(np.sum(error_orig**2))
    sse_simple = float(np.sum(error_simple**2))
    sst = float(np.sum((y_true - np.mean(y_true)) ** 2))
    condition_number = float(np.linalg.cond(x_matrix))
    delta = prediction_simplified - prediction_original

    def _metrics(error: np.ndarray, sse: float) -> LinearFitMetrics:
        return LinearFitMetrics(
            rmse=float(np.sqrt(np.mean(error**2))),
            r2=float(1.0 - sse / sst) if sst > 0 else 1.0,
            bias=float(np.mean(error)),
            max_error=float(np.max(np.abs(error))) if error.size else 0.0,
            mae=float(np.mean(np.abs(error))) if error.size else 0.0,
            max_prediction_delta=float(np.max(np.abs(delta))) if delta.size else 0.0,
            mean_prediction_delta=float(np.mean(np.abs(delta))) if delta.size else 0.0,
            condition_number=condition_number,
        )

    return _metrics(error_orig, sse_orig), _metrics(error_simple, sse_simple)


def _fit_bundle(frame: pd.DataFrame, simplification: SimplificationConfig) -> LinearFitBundle:
    x_matrix = _build_feature_matrix(frame)
    y_true = frame["Y_true"].to_numpy(dtype=float)
    original = _fit_least_squares(x_matrix, y_true)
    simplified = original.copy()
    if simplification.enabled:
        simplified = _simplify_coefficients(
            x_matrix,
            y_true,
            method=simplification.method,
            digits=simplification.target_digits,
        )
    prediction_original = _predict(x_matrix, original)
    prediction_simplified = _predict(x_matrix, simplified)
    metrics_original, metrics_simplified = _compute_metrics(
        y_true,
        prediction_original,
        prediction_simplified,
        x_matrix=x_matrix,
    )
    coefficient_shift = np.abs(simplified - original)
    summary = (
        f"最大系数变化={float(np.max(coefficient_shift)):.6g}; "
        f"平均系数变化={float(np.mean(coefficient_shift)):.6g}"
    )
    return LinearFitBundle(
        original_coefficients=original,
        simplified_coefficients=simplified,
        metrics_original=metrics_original,
        metrics_simplified=metrics_simplified,
        predictions_original=prediction_original,
        predictions_simplified=prediction_simplified,
        residuals_original=prediction_original - y_true,
        residuals_simplified=prediction_simplified - y_true,
        coefficient_summary=summary,
    )


def _validate_bounds(series: pd.Series, lower: Optional[float], upper: Optional[float]) -> pd.Series:
    mask = pd.Series(True, index=series.index)
    if lower is not None:
        mask &= series >= float(lower)
    if upper is not None:
        mask &= series <= float(upper)
    return mask


def _assign_range_bin(values: pd.Series, bins: Sequence[float]) -> pd.Categorical:
    edges = sorted(float(item) for item in bins)
    if len(edges) < 2:
        maximum = float(values.max()) if not values.empty else 0.0
        edges = [0.0, maximum]
    if edges[-1] <= float(values.max()):
        edges.append(float(values.max()) + 1e-9)
    labels: List[str] = []
    for start, end in zip(edges[:-1], edges[1:]):
        labels.append(f"{start:g}-{end:g}")
    return pd.cut(values, bins=edges, labels=labels, include_lowest=True, right=False)


def _mad(series: pd.Series, minimum: float) -> float:
    median = float(series.median())
    mad = float(np.median(np.abs(series.to_numpy(dtype=float) - median)))
    return max(mad, float(minimum))


def _build_working_frame(
    frame: pd.DataFrame,
    *,
    gas_type: str,
    columns: OfflineColumnConfig,
    filtering: RefitFilteringConfig,
) -> pd.DataFrame:
    mapping = resolve_column_mapping(frame, gas_type=gas_type, columns=columns)
    working = frame.copy().reset_index(drop=False).rename(columns={"index": "source_index"})
    if mapping.analyzer_id:
        working["analyzer_id"] = working[mapping.analyzer_id].astype(str)
    else:
        working["analyzer_id"] = "UNKNOWN"
    if mapping.row_index:
        working["row_index"] = working[mapping.row_index]
    else:
        working["row_index"] = working["source_index"]
    working["gas_type"] = str(gas_type).strip().lower()
    working["Y_true"] = pd.to_numeric(working[mapping.target], errors="coerce")
    working["R"] = pd.to_numeric(working[mapping.ratio], errors="coerce")
    working["T_input"] = pd.to_numeric(working[mapping.temperature], errors="coerce")
    working["P"] = pd.to_numeric(working[mapping.pressure], errors="coerce")
    if mapping.temperature == mapping.tk:
        working["T_k"] = working["T_input"]
        working["T_c"] = working["T_input"] - float(filtering.temperature_offset_c)
    else:
        working["T_c"] = working["T_input"]
        working["T_k"] = working["T_input"] + float(filtering.temperature_offset_c)
    working["target_bin"] = _assign_range_bin(
        working["Y_true"],
        filtering.target_bins_co2 if gas_type == "co2" else filtering.target_bins_h2o,
    ).astype(str)
    working["temp_bin"] = (np.floor(working["T_c"] / filtering.temp_bin_size) * filtering.temp_bin_size).round(3)
    working["press_bin"] = (np.floor(working["P"] / filtering.press_bin_size) * filtering.press_bin_size).round(3)
    working["keep_after_basic"] = True
    working["keep_after_group"] = True
    working["keep_final"] = True
    working["remove_stage"] = ""
    working["remove_reason"] = ""
    working["first_fit_pred"] = np.nan
    working["first_fit_residual"] = np.nan
    working["second_fit_pred"] = np.nan
    working["second_fit_residual"] = np.nan
    return working


def _apply_basic_cleaning(frame: pd.DataFrame, filtering: RefitFilteringConfig) -> pd.DataFrame:
    required = frame[["Y_true", "R", "T_k", "P"]].notna().all(axis=1)
    bounds = filtering.bounds
    valid = required.copy()
    valid &= _validate_bounds(frame["Y_true"], bounds.y_min, bounds.y_max)
    valid &= _validate_bounds(frame["R"], bounds.r_min, bounds.r_max)
    valid &= _validate_bounds(frame["T_k"], bounds.tk_min, bounds.tk_max)
    valid &= _validate_bounds(frame["P"], bounds.p_min, bounds.p_max)
    frame.loc[~required, "keep_after_basic"] = False
    frame.loc[~required, "keep_final"] = False
    frame.loc[~required, "remove_stage"] = "basic_clean"
    frame.loc[~required, "remove_reason"] = "missing_required"
    invalid = required & ~valid
    frame.loc[invalid, "keep_after_basic"] = False
    frame.loc[invalid, "keep_final"] = False
    frame.loc[invalid, "remove_stage"] = "basic_clean"
    frame.loc[invalid, "remove_reason"] = "invalid_value"
    return frame


def _apply_group_filter(frame: pd.DataFrame, filtering: RefitFilteringConfig) -> pd.DataFrame:
    eligible = frame["keep_after_basic"].fillna(False)
    grouped = frame.loc[eligible].groupby(["target_bin", "temp_bin", "press_bin"], dropna=False)
    for _, group in grouped:
        if len(group) < filtering.min_group_size:
            continue
        median_r = float(group["R"].median())
        mad_r = _mad(group["R"], filtering.mad_min_group)
        outliers = group[np.abs(group["R"] - median_r) > filtering.mad_multiplier_group * mad_r]
        if outliers.empty:
            continue
        frame.loc[outliers.index, "keep_after_group"] = False
        frame.loc[outliers.index, "keep_final"] = False
        frame.loc[outliers.index, "remove_stage"] = "group_consistency"
        frame.loc[outliers.index, "remove_reason"] = "group_R_outlier"
    return frame


def _apply_residual_filter(frame: pd.DataFrame, filtering: RefitFilteringConfig) -> pd.DataFrame:
    gas_type = str(frame["gas_type"].iloc[0]).strip().lower()
    active = frame["keep_after_group"].fillna(False)
    kept = frame.loc[active].copy()
    if kept.empty:
        return frame

    total_limit = int(np.floor(len(kept) * (filtering.max_remove_ratio_co2 if gas_type == "co2" else filtering.max_remove_ratio_h2o)))
    if total_limit <= 0:
        return frame

    removed_total = 0
    for _, group in kept.groupby("target_bin", dropna=False):
        if removed_total >= total_limit:
            break
        current_indices = [idx for idx in group.index if frame.at[idx, "keep_final"]]
        if len(current_indices) <= filtering.min_points_per_bin:
            continue
        current = frame.loc[current_indices]
        residuals = current["first_fit_residual"].astype(float)
        median_residual = float(residuals.median())
        mad_residual = _mad(residuals, filtering.mad_min_residual)
        threshold = filtering.mad_multiplier_residual * mad_residual

        scored: List[Tuple[float, int]] = []
        for idx, row in current.iterrows():
            effective_threshold = threshold
            if gas_type == "co2" and float(row["Y_true"]) <= filtering.low_range_protect_threshold_co2:
                effective_threshold *= filtering.low_range_extra_multiplier
            deviation = abs(float(row["first_fit_residual"]) - median_residual)
            if deviation > effective_threshold:
                scored.append((deviation, idx))
        if not scored:
            continue

        scored.sort(reverse=True)
        removable_slots = min(
            filtering.max_remove_per_bin,
            total_limit - removed_total,
            max(0, len(current_indices) - filtering.min_points_per_bin),
        )
        for _, idx in scored[:removable_slots]:
            frame.at[idx, "keep_final"] = False
            frame.at[idx, "remove_stage"] = "residual_refit"
            frame.at[idx, "remove_reason"] = "residual_outlier_bin"
            removed_total += 1
            if removed_total >= total_limit:
                break
    return frame


def _coefficient_frame(
    analyzer_id: str,
    gas_type: str,
    stage: str,
    coefficients: np.ndarray,
) -> pd.DataFrame:
    payload: Dict[str, Any] = {"analyzer_id": analyzer_id, "gas_type": gas_type, "stage": stage}
    for name, term, value in zip(FEATURE_NAMES, FEATURE_TERMS, coefficients):
        payload[name] = float(value)
        payload[f"{name}_term"] = term
    return pd.DataFrame([payload])


def _build_compare_row(
    analyzer_id: str,
    gas_type: str,
    before_fit: LinearFitBundle,
    after_fit: LinearFitBundle,
) -> Dict[str, Any]:
    bias_risk = abs(after_fit.metrics_simplified.bias) > abs(before_fit.metrics_simplified.bias)
    max_error_risk = after_fit.metrics_simplified.max_error > before_fit.metrics_simplified.max_error
    condition_risk = after_fit.metrics_simplified.condition_number > before_fit.metrics_simplified.condition_number
    improved_rmse = after_fit.metrics_simplified.rmse < before_fit.metrics_simplified.rmse
    risk_flag = improved_rmse and (bias_risk or max_error_risk or condition_risk)
    coefficient_delta = np.abs(after_fit.simplified_coefficients - before_fit.simplified_coefficients)
    return {
        "analyzer_id": analyzer_id,
        "gas_type": gas_type,
        "RMSE_before": before_fit.metrics_simplified.rmse,
        "RMSE_after": after_fit.metrics_simplified.rmse,
        "R2_before": before_fit.metrics_simplified.r2,
        "R2_after": after_fit.metrics_simplified.r2,
        "Bias_before": before_fit.metrics_simplified.bias,
        "Bias_after": after_fit.metrics_simplified.bias,
        "MaxError_before": before_fit.metrics_simplified.max_error,
        "MaxError_after": after_fit.metrics_simplified.max_error,
        "Condition_before": before_fit.metrics_simplified.condition_number,
        "Condition_after": after_fit.metrics_simplified.condition_number,
        "原始与简化预测最大差值_before": before_fit.metrics_simplified.max_prediction_delta,
        "原始与简化预测最大差值_after": after_fit.metrics_simplified.max_prediction_delta,
        "原始与简化预测平均差值_before": before_fit.metrics_simplified.mean_prediction_delta,
        "原始与简化预测平均差值_after": after_fit.metrics_simplified.mean_prediction_delta,
        "系数变化摘要": f"最大变化={float(np.max(coefficient_delta)):.6g}; 平均变化={float(np.mean(coefficient_delta)):.6g}",
        "是否推荐采用": "谨慎" if risk_flag else ("推荐" if improved_rmse else "维持原模型"),
        "风险标记": "RMSE下降但 MaxError/Bias/条件数变差" if risk_flag else "",
    }


def _build_filter_summary(frame: pd.DataFrame) -> pd.DataFrame:
    remaining = frame.loc[frame["keep_final"]].copy()
    summary = {
        "总点数": int(len(frame)),
        "基础清洗删除数": int((frame["remove_stage"] == "basic_clean").sum()),
        "组内R离群删除数": int((frame["remove_reason"] == "group_R_outlier").sum()),
        "残差离群删除数": int((frame["remove_reason"] == "residual_outlier_bin").sum()),
        "最终保留数": int(frame["keep_final"].sum()),
        "删除比例": float(1.0 - frame["keep_final"].mean()) if len(frame) else 0.0,
    }
    by_bin = frame.groupby("target_bin", dropna=False)["keep_final"].agg(["sum", "count"]).reset_index()
    by_bin["remove_count"] = by_bin["count"] - by_bin["sum"]
    summary["各区间保留数"] = "; ".join(f"{row.target_bin}:{int(row.sum)}" for row in by_bin.itertuples(index=False))
    summary["各区间删除数"] = "; ".join(f"{row.target_bin}:{int(row.remove_count)}" for row in by_bin.itertuples(index=False))
    summary["最终保留区间数"] = int(remaining["target_bin"].nunique(dropna=False)) if not remaining.empty else 0
    return pd.DataFrame([summary])


def run_refit_filtering(
    rows: Iterable[Dict[str, Any]],
    *,
    config: OfflineRefitConfig,
    analyzer_id: Optional[str] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> RefitFilteringResult:
    """执行离线点表筛选、重拟合与审计。"""
    frame = pd.DataFrame(list(rows))
    if frame.empty:
        raise DataValidationError("rows", "empty", expected="至少包含一行离线点表数据")
    _emit_log(log_fn, f"离线筛选重算：输入 {len(frame)} 条记录")
    working = _build_working_frame(
        frame,
        gas_type=config.gas_type,
        columns=config.columns,
        filtering=config.filtering,
    )
    if analyzer_id is not None and set(working["analyzer_id"].astype(str)) == {"UNKNOWN"}:
        working["analyzer_id"] = str(analyzer_id)
    if analyzer_id is not None and "analyzer_id" in working.columns:
        working = working.loc[working["analyzer_id"].astype(str) == str(analyzer_id)].copy()
    if working.empty:
        raise DataValidationError("analyzer_id", analyzer_id, expected="输入表中存在对应分析仪数据")

    _emit_log(log_fn, "开始基础清洗")
    working = _apply_basic_cleaning(working, config.filtering)
    _emit_log(log_fn, f"基础清洗后保留 {int(working['keep_after_basic'].sum())} 条")

    _emit_log(log_fn, "开始组内一致性筛选")
    working = _apply_group_filter(working, config.filtering)
    first_fit_frame = working.loc[working["keep_after_group"]].copy()
    if len(first_fit_frame) < len(FEATURE_NAMES):
        raise DataValidationError("first_fit_frame", len(first_fit_frame), expected=f">={len(FEATURE_NAMES)} 条有效记录")
    _emit_log(log_fn, f"第一次拟合样本数 {len(first_fit_frame)}")
    first_fit = _fit_bundle(first_fit_frame, config.simplification)
    working.loc[first_fit_frame.index, "first_fit_pred"] = first_fit.predictions_original
    working.loc[first_fit_frame.index, "first_fit_residual"] = first_fit.residuals_original

    if config.filtering.enable_refit_filtering:
        _emit_log(log_fn, "开始按量程区间执行残差离群筛选")
        working = _apply_residual_filter(working, config.filtering)
    final_frame = working.loc[working["keep_final"]].copy()
    if len(final_frame) < len(FEATURE_NAMES):
        raise DataValidationError("final_frame", len(final_frame), expected=f">={len(FEATURE_NAMES)} 条最终保留记录")

    _emit_log(log_fn, f"第二次重拟合样本数 {len(final_frame)}")
    second_fit = _fit_bundle(final_frame, config.simplification)
    working.loc[final_frame.index, "second_fit_pred"] = second_fit.predictions_original
    working.loc[final_frame.index, "second_fit_residual"] = second_fit.residuals_original

    analyzer_value = str(final_frame["analyzer_id"].iloc[0])
    compare_frame = pd.DataFrame(
        [_build_compare_row(analyzer_value, config.gas_type, first_fit, second_fit)]
    )
    summary_frame = _build_filter_summary(working)
    original_coefficients_frame = pd.concat(
        [
            _coefficient_frame(analyzer_value, config.gas_type, "before_refit", first_fit.original_coefficients),
            _coefficient_frame(analyzer_value, config.gas_type, "after_refit", second_fit.original_coefficients),
        ],
        ignore_index=True,
    )
    simplified_coefficients_frame = pd.concat(
        [
            _coefficient_frame(analyzer_value, config.gas_type, "before_refit", first_fit.simplified_coefficients),
            _coefficient_frame(analyzer_value, config.gas_type, "after_refit", second_fit.simplified_coefficients),
        ],
        ignore_index=True,
    )
    _emit_log(log_fn, f"第一次 RMSE={first_fit.metrics_simplified.rmse:.6g}")
    _emit_log(log_fn, f"第二次 RMSE={second_fit.metrics_simplified.rmse:.6g}")
    _emit_log(log_fn, f"最终删除比例={float(summary_frame.iloc[0]['删除比例']):.2%}")

    audit_frame = working[
        [
            "analyzer_id",
            "gas_type",
            "source_index",
            "row_index",
            "Y_true",
            "R",
            "T_k",
            "P",
            "target_bin",
            "temp_bin",
            "press_bin",
            "first_fit_pred",
            "first_fit_residual",
            "second_fit_pred",
            "second_fit_residual",
            "keep_final",
            "remove_stage",
            "remove_reason",
        ]
    ].copy()
    return RefitFilteringResult(
        audit_frame=audit_frame,
        summary_frame=summary_frame,
        compare_frame=compare_frame,
        original_coefficients_frame=original_coefficients_frame,
        simplified_coefficients_frame=simplified_coefficients_frame,
    )


def export_refit_filtering_result(
    result: RefitFilteringResult,
    out_dir: Path,
    *,
    prefix: str,
) -> Dict[str, Path]:
    """导出筛选重算审计文件。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    audit_csv = out_dir / f"{prefix}_筛选明细.csv"
    summary_csv = out_dir / f"{prefix}_筛选统计摘要.csv"
    compare_csv = out_dir / f"{prefix}_重拟合前后对比.csv"
    excel_path = out_dir / f"{prefix}_筛选重算报告.xlsx"

    result.audit_frame.to_csv(audit_csv, index=False, encoding="utf-8-sig")
    result.summary_frame.to_csv(summary_csv, index=False, encoding="utf-8-sig")
    result.compare_frame.to_csv(compare_csv, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        result.audit_frame.to_excel(writer, sheet_name="逐点筛选明细表", index=False)
        result.summary_frame.to_excel(writer, sheet_name="筛选统计摘要表", index=False)
        result.compare_frame.to_excel(writer, sheet_name="重拟合前后对比摘要表", index=False)
        result.original_coefficients_frame.to_excel(writer, sheet_name="原始系数", index=False)
        result.simplified_coefficients_frame.to_excel(writer, sheet_name="简化系数", index=False)

    return {
        "audit_csv": audit_csv,
        "summary_csv": summary_csv,
        "compare_csv": compare_csv,
        "excel": excel_path,
    }
