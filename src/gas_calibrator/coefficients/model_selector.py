"""候选模型比较与推荐。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from .fit_ratio_poly import RatioPolyFitResult, fit_ratio_poly_rt_p
from .fit_ratio_poly_evolved import fit_ratio_poly_rt_p_evolved


DEFAULT_CANDIDATE_MODELS: Dict[str, List[str]] = {
    "Model_A": ["intercept", "R", "R2", "T", "P"],
    "Model_B": ["intercept", "R", "R2", "R3", "T", "T2", "RT", "P"],
    "Model_C": ["intercept", "R", "R2", "R3", "T", "T2", "RT", "P", "RTP"],
}
H2O_CROSS_CANDIDATE_MODELS: Dict[str, List[str]] = {
    "Model_D": ["intercept", "R", "R2", "T", "P", "H", "H2"],
    "Model_E": ["intercept", "R", "R2", "R3", "T", "T2", "RT", "P", "H", "H2", "RH"],
}


@dataclass
class ModelSelectionResult:
    """多模型比较结果。"""

    base_model: str
    comparison_rows: List[Dict[str, Any]]
    recommended_model: str
    recommendation_reason: str
    results: Dict[str, RatioPolyFitResult]


def _emit_log(log_fn: Optional[Callable[[str], None]], message: str) -> None:
    if log_fn is not None:
        log_fn(message)


def _comparison_row(name: str, result: RatioPolyFitResult) -> Dict[str, Any]:
    validation = result.stats.get("validation_metrics", {})
    test = result.stats.get("test_metrics", {})
    validation_simplified = validation.get("simplified", {})
    test_simplified = test.get("simplified", {})
    cross = result.stats.get("cross_interference", {})
    return {
        "CandidateModel": name,
        "FitModel": result.model,
        "FeatureCount": len(result.feature_names),
        "FeatureTokens": ",".join(result.stats.get("model_features", [])),
        "HasCrossInterference": bool(cross.get("enabled")),
        "CrossFeatureCount": len(cross.get("feature_tokens", [])),
        "CrossFeatureTokens": ",".join(cross.get("feature_tokens", [])),
        "ValidationRMSE": float(validation_simplified.get("RMSE", float("inf"))),
        "TestRMSE": float(test_simplified.get("RMSE", float("inf"))),
        "ValidationR2": float(validation_simplified.get("R2", float("-inf"))),
        "TestR2": float(test_simplified.get("R2", float("-inf"))),
        "ConditionNumber": float(result.stats.get("original_coefficient_analysis", {}).get("condition_number", float("inf"))),
        "RMSEChange": float(result.stats.get("rmse_change", 0.0)),
        "SelectedDigits": int(result.stats.get("simplification_summary", {}).get("selected_digits", 0)),
        "OutlierCount": int(result.stats.get("outlier_detection", {}).get("outlier_count", 0)),
    }


def compare_ratio_poly_models(
    rows: Iterable[Dict[str, Any]],
    *,
    gas: str,
    target_key: str,
    ratio_keys: Sequence[str],
    temp_keys: Sequence[str],
    pressure_keys: Sequence[str],
    humidity_keys: Sequence[str] | None = None,
    base_model: str = "ratio_poly_rt_p",
    candidate_models: Optional[Mapping[str, Sequence[str]]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    **fit_kwargs: Any,
) -> ModelSelectionResult:
    """对多个候选模型执行拟合、评估并输出推荐结果。"""
    model_name = str(base_model or "ratio_poly_rt_p").strip().lower()
    fit_fn = fit_ratio_poly_rt_p_evolved if model_name in {"ratio_poly_rt_p_evolved", "poly_rt_p_evolved"} else fit_ratio_poly_rt_p
    candidates = dict(candidate_models or DEFAULT_CANDIDATE_MODELS)

    _emit_log(log_fn, f"开始模型比较，共{len(candidates)}个模型")
    results: Dict[str, RatioPolyFitResult] = {}
    comparison_rows: List[Dict[str, Any]] = []
    for candidate_name, features in candidates.items():
        _emit_log(log_fn, candidate_name)
        result = fit_fn(
            rows,
            gas=gas,
            target_key=target_key,
            ratio_keys=ratio_keys,
            temp_keys=temp_keys,
            pressure_keys=pressure_keys,
            humidity_keys=humidity_keys,
            model_features=list(features),
            log_fn=log_fn,
            **fit_kwargs,
        )
        row = _comparison_row(candidate_name, result)
        _emit_log(log_fn, f"{candidate_name} Validation RMSE = {row['ValidationRMSE']:.6g}")
        _emit_log(log_fn, f"{candidate_name} Test RMSE = {row['TestRMSE']:.6g}")
        results[candidate_name] = result
        comparison_rows.append(row)

    comparison_rows.sort(
        key=lambda item: (
            item["ValidationRMSE"],
            item["TestRMSE"],
            item["ConditionNumber"],
            item["FeatureCount"],
            abs(item["RMSEChange"]),
        )
    )
    recommended = comparison_rows[0]
    reason = (
        f"{recommended['CandidateModel']} 的 Validation/Test RMSE 最优，"
        f"条件数更低，特征数量为 {recommended['FeatureCount']}，"
        f"简化后 RMSE 变化 {recommended['RMSEChange']:.6g}"
    )
    return ModelSelectionResult(
        base_model=model_name,
        comparison_rows=comparison_rows,
        recommended_model=str(recommended["CandidateModel"]),
        recommendation_reason=reason,
        results=results,
    )
