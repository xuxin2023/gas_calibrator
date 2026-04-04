"""校准方程拟合主流程调度模块。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Sequence

from .data_loader import load_excel_dataframe
from .exporter import export_model_comparison
from .fit_ratio_poly import fit_ratio_poly_rt_p
from .fit_ratio_poly_evolved import fit_ratio_poly_rt_p_evolved
from .model_selector import compare_ratio_poly_models


def _emit_log(log_fn: Optional[Callable[[str], None]], message: str) -> None:
    if log_fn is not None:
        log_fn(message)


def run_ratio_poly_fit_workflow(
    rows: Iterable[Dict[str, Any]],
    *,
    gas: str,
    target_key: str,
    ratio_keys: Sequence[str],
    temp_keys: Sequence[str],
    pressure_keys: Sequence[str],
    humidity_keys: Sequence[str] | None = None,
    model: str = "ratio_poly_rt_p",
    compare_models: bool = False,
    candidate_models: Optional[Dict[str, Sequence[str]]] = None,
    export_dir: Optional[str | Path] = None,
    export_prefix: Optional[str] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    **kwargs: Any,
):
    """主流程调度：读取记录、拆分、筛异常、拟合、评估和可选模型比较。"""
    model_name = str(model or "ratio_poly_rt_p").strip().lower()
    _emit_log(log_fn, f"主流程：准备执行 {model_name} 拟合")
    if compare_models or candidate_models:
        selection = compare_ratio_poly_models(
            rows,
            gas=gas,
            target_key=target_key,
            ratio_keys=ratio_keys,
            temp_keys=temp_keys,
            pressure_keys=pressure_keys,
            humidity_keys=humidity_keys,
            base_model=model_name,
            candidate_models=candidate_models,
            log_fn=log_fn,
            **kwargs,
        )
        _emit_log(log_fn, f"推荐模型：{selection.recommended_model}")
        _emit_log(log_fn, f"推荐原因：{selection.recommendation_reason}")
        if export_dir is not None:
            recommended = selection.results[selection.recommended_model]
            payload = {
                "base_model": selection.base_model,
                "recommended_model": selection.recommended_model,
                "recommendation_reason": selection.recommendation_reason,
                "comparison_rows": selection.comparison_rows,
                "cross_interference": recommended.stats.get("cross_interference", {}),
                "H2O_cross_coefficients": recommended.stats.get("cross_interference", {}).get("simplified_coefficients", {}),
            }
            export_model_comparison(
                payload,
                selection.comparison_rows,
                Path(export_dir),
                prefix=export_prefix or f"{gas}_{selection.base_model}",
            )
        return selection

    if model_name in {"ratio_poly_rt_p", "poly_rt_p"}:
        result = fit_ratio_poly_rt_p(
            rows,
            gas=gas,
            target_key=target_key,
            ratio_keys=ratio_keys,
            temp_keys=temp_keys,
            pressure_keys=pressure_keys,
            humidity_keys=humidity_keys,
            log_fn=log_fn,
            **kwargs,
        )
    elif model_name in {"ratio_poly_rt_p_evolved", "poly_rt_p_evolved"}:
        result = fit_ratio_poly_rt_p_evolved(
            rows,
            gas=gas,
            target_key=target_key,
            ratio_keys=ratio_keys,
            temp_keys=temp_keys,
            pressure_keys=pressure_keys,
            humidity_keys=humidity_keys,
            log_fn=log_fn,
            **kwargs,
        )
    else:
        raise ValueError(f"Unknown ratio-poly model: {model}")
    _emit_log(log_fn, f"主流程：{result.model} 拟合结束")
    return result


def run_ratio_poly_fit_excel(
    excel_path: str | Path,
    *,
    gas: str,
    target_key: str,
    ratio_keys: Sequence[str],
    temp_keys: Sequence[str],
    pressure_keys: Sequence[str],
    humidity_keys: Sequence[str] | None = None,
    sheet_name: str | int = 0,
    model: str = "ratio_poly_rt_p",
    compare_models: bool = False,
    candidate_models: Optional[Dict[str, Sequence[str]]] = None,
    export_dir: Optional[str | Path] = None,
    export_prefix: Optional[str] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    **kwargs: Any,
):
    """离线 Excel 主流程：先读取 Excel，再调度拟合。"""
    _emit_log(log_fn, f"读取数据：{excel_path}")
    dataframe = load_excel_dataframe(excel_path, sheet_name=sheet_name)
    _emit_log(log_fn, f"读取完成：{len(dataframe)} 条")
    return run_ratio_poly_fit_workflow(
        dataframe.to_dict(orient="records"),
        gas=gas,
        target_key=target_key,
        ratio_keys=ratio_keys,
        temp_keys=temp_keys,
        pressure_keys=pressure_keys,
        humidity_keys=humidity_keys,
        model=model,
        compare_models=compare_models,
        candidate_models=candidate_models,
        export_dir=export_dir,
        export_prefix=export_prefix,
        log_fn=log_fn,
        **kwargs,
    )
