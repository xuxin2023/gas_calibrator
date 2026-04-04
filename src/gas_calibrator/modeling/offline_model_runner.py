"""独立离线建模分析入口。"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from ..coefficients.exporter import export_model_comparison, export_prediction_analysis
from ..coefficients.feature_builder import build_feature_dataset
from ..coefficients.fit_ratio_poly import _prepare_ratio_poly_dataframe, save_ratio_poly_report
from ..coefficients.model_selector import compare_ratio_poly_models
from ..coefficients.outlier_detector import filter_outliers
from ..coefficients.prediction_analysis import PredictionAnalysisResult, analyze_by_range, analyze_predictions
from .config_loader import load_modeling_config, validate_modeling_input_source


def _emit_log(log_fn: Optional[Callable[[str], None]], message: str) -> None:
    if log_fn is not None:
        log_fn(message)


def _read_input_frame(source_path: Path, *, file_type: str, sheet_name: str | int) -> pd.DataFrame:
    if file_type in {"xlsx", "xls"}:
        return pd.read_excel(source_path, sheet_name=sheet_name)
    if file_type == "csv":
        return pd.read_csv(source_path)
    raise ValueError(f"不支持的离线建模输入文件类型：{file_type}")


def _build_summary_text(selection, exported_paths: Dict[str, Path], data_path: Path, *, file_type: str, sheet_name: str | int) -> str:
    best_result = selection.results[selection.recommended_model]
    validation = best_result.stats.get("validation_metrics", {}).get("simplified", {})
    test = best_result.stats.get("test_metrics", {}).get("simplified", {})
    outlier = best_result.stats.get("outlier_detection", {})
    cross = best_result.stats.get("cross_interference", {})
    lines = [
        "离线建模分析结果",
        f"输入文件：{data_path}",
        f"文件类型：{file_type}",
        f"Excel Sheet：{sheet_name if file_type in {'xlsx', 'xls'} else '--'}",
        f"推荐模型：{selection.recommended_model}",
        f"推荐原因：{selection.recommendation_reason}",
        f"Validation RMSE：{validation.get('RMSE', '--')}",
        f"Test RMSE：{test.get('RMSE', '--')}",
        f"异常点数量：{outlier.get('outlier_count', 0)}",
        f"H2O交叉干扰：{'启用' if cross.get('enabled') else '未启用'}",
        f"H2O交叉系数：{cross.get('simplified_coefficients', {}) if cross.get('enabled') else '--'}",
        f"原始系数数量：{len(best_result.original_coefficients)}",
        f"导出文件：{', '.join(str(path) for path in exported_paths.values())}",
        "",
        "本功能默认不参与在线自动校准流程，仅用于离线建模分析与系数生成。",
    ]
    return "\n".join(lines)


def _build_prediction_analysis(
    frame: pd.DataFrame,
    *,
    modeling: Dict[str, Any],
    data_source: Dict[str, Any],
    recommended: Any,
) -> PredictionAnalysisResult:
    """按推荐模型重建全量拟合集，并执行逐点回代分析。"""
    model_features = list(recommended.stats.get("model_features", []))
    working, target_column, ratio_column, temp_column, pressure_column, humidity_column = _prepare_ratio_poly_dataframe(
        frame.to_dict(orient="records"),
        gas=str(data_source.get("gas", "co2")),
        target_key=str(data_source.get("target_key", "ppm_CO2_Tank")),
        ratio_keys=tuple(data_source.get("ratio_keys", ["R_CO2"])),
        temp_keys=tuple(data_source.get("temp_keys", ["T1"])),
        pressure_keys=tuple(data_source.get("pressure_keys", ["BAR"])),
        humidity_keys=tuple(data_source.get("humidity_keys", ["ppm_H2O_Dew", "H2O", "h2o_mmol"])),
        model_features=model_features,
        pressure_scale=1.0,
    )

    outlier_cfg = dict(modeling.get("outlier_filter", {}))
    outlier_methods: List[str] = []
    if outlier_cfg.get("enabled"):
        outlier_methods.append(str(outlier_cfg.get("method", "iqr")))

    outlier_result = filter_outliers(
        working,
        target_column=target_column,
        ratio_column=ratio_column,
        temperature_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        model_features=model_features,
        temperature_offset_c=273.15,
        fit_method=str(modeling.get("fit_method", "least_squares")),
        ridge_lambda=float(modeling.get("ridge_lambda", 1e-6)),
        methods=tuple(outlier_methods),
        iqr_factor=float(outlier_cfg.get("threshold", 1.5)) if "iqr" in outlier_methods else 1.5,
        residual_std_multiplier=float(outlier_cfg.get("threshold", 3.0)) if "residual_sigma" in outlier_methods else 3.0,
    )
    fit_frame = outlier_result.kept_frame.reset_index(drop=False).rename(columns={"index": "source_index"})
    dataset = build_feature_dataset(
        fit_frame,
        target_column=target_column,
        ratio_column=ratio_column,
        temperature_column=temp_column,
        pressure_column=pressure_column,
        humidity_column=humidity_column,
        temperature_offset_c=273.15,
        model_features=model_features,
    )

    original = [recommended.original_coefficients[name] for name in recommended.feature_names]
    simplified = [recommended.simplified_coefficients[name] for name in recommended.feature_names]
    analysis = analyze_predictions(
        dataset.feature_matrix,
        dataset.target_vector,
        original,
        simplified,
        sample_index=fit_frame["source_index"].tolist(),
    )

    metadata_columns = [
        column
        for column in ["Analyzer", "PointRow", "PointPhase", "PointTag", "PointTitle", target_column, ratio_column, temp_column]
        if column in fit_frame.columns
    ]
    metadata = fit_frame[["source_index", *metadata_columns]].rename(columns={"source_index": "index"}).copy()
    point_table = analysis.point_table.merge(metadata, on="index", how="left")
    point_table["target_column"] = target_column
    point_table["ratio_column"] = ratio_column
    point_table["temperature_column"] = temp_column
    point_table["pressure_column"] = pressure_column
    point_table["pressure_value"] = fit_frame[pressure_column].to_numpy()

    bins = modeling.get("evaluation_bins") or [0, 200, 400, 800, 1200, 2000]
    range_table = analyze_by_range(
        point_table["Y_true"].to_numpy(float),
        point_table["error_orig"].to_numpy(float),
        point_table["error_simple"].to_numpy(float),
        bins=bins,
    )

    analysis.point_table = point_table
    analysis.range_table = range_table
    analysis.top_error_orig = point_table.sort_values("abs_error_orig", ascending=False).head(10).reset_index(drop=True)
    analysis.top_error_simple = point_table.sort_values("abs_error_simple", ascending=False).head(10).reset_index(drop=True)
    analysis.top_pred_diff = point_table.sort_values("abs_pred_diff", ascending=False).head(10).reset_index(drop=True)
    analysis.summary["outlier_count"] = int(outlier_result.outlier_count)
    analysis.summary["fit_sample_count"] = int(len(fit_frame))
    return analysis


def run_offline_modeling_analysis(
    *,
    base_config_path: str | Path | None = None,
    modeling_config_path: str | Path | None = None,
    input_path: str | Path | None = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """执行独立离线建模分析。"""
    loaded = load_modeling_config(
        base_config_path=base_config_path,
        modeling_config_path=modeling_config_path,
    )
    modeling = dict(loaded["modeling"])
    _emit_log(log_fn, "已加载离线建模配置")
    _emit_log(log_fn, "离线建模分析已启用")
    _emit_log(log_fn, "当前不会影响自动校准在线流程")

    if not modeling.get("enabled"):
        raise ValueError("modeling.enabled = false，当前离线建模分析默认关闭")

    data_source = dict(modeling.get("data_source", {}))
    if input_path is not None:
        data_source["path"] = str(input_path)
    validated_source = validate_modeling_input_source(
        data_source,
        project_root=loaded["project_root"],
    )
    source_path = Path(validated_source["path"])
    file_type = str(validated_source["file_type"])
    sheet_name = validated_source["sheet_name"]

    output_root = Path(modeling.get("export", {}).get("output_dir", "")).resolve()
    run_dir = output_root / f"modeling_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)

    _emit_log(log_fn, f"离线建模输入文件：{source_path}")
    _emit_log(log_fn, f"文件类型：{file_type}")
    _emit_log(log_fn, f"Excel Sheet：{sheet_name if file_type in {'xlsx', 'xls'} else '--'}")
    _emit_log(log_fn, "该配置仅用于离线建模分析，不影响在线自动校准流程")
    _emit_log(log_fn, f"候选模型数量：{len(modeling.get('candidate_models', {}))}")
    _emit_log(log_fn, f"拟合方法：{modeling.get('fit_method')}")
    _emit_log(log_fn, f"异常点筛选：{'开启' if modeling.get('outlier_filter', {}).get('enabled') else '关闭'}")
    _emit_log(log_fn, f"系数简化：{'开启' if modeling.get('simplification', {}).get('enabled') else '关闭'}")
    _emit_log(log_fn, f"结果导出目录：{run_dir}")

    frame = _read_input_frame(source_path, file_type=file_type, sheet_name=sheet_name)
    rows = frame.to_dict(orient="records")

    outlier_cfg = dict(modeling.get("outlier_filter", {}))
    outlier_methods: List[str] = []
    if outlier_cfg.get("enabled"):
        outlier_methods.append(str(outlier_cfg.get("method", "iqr")))

    simplification_cfg = dict(modeling.get("simplification", {}))
    selection = compare_ratio_poly_models(
        rows,
        gas=str(data_source.get("gas", "co2")),
        target_key=str(data_source.get("target_key", "ppm_CO2_Tank")),
        ratio_keys=tuple(data_source.get("ratio_keys", ["R_CO2"])),
        temp_keys=tuple(data_source.get("temp_keys", ["T1"])),
        pressure_keys=tuple(data_source.get("pressure_keys", ["BAR"])),
        humidity_keys=tuple(data_source.get("humidity_keys", ["ppm_H2O_Dew", "H2O", "h2o_mmol"])),
        base_model="ratio_poly_rt_p",
        candidate_models=modeling.get("candidate_models"),
        fitting_method=str(modeling.get("fit_method", "least_squares")),
        ridge_lambda=float(modeling.get("ridge_lambda", 1e-6)),
        outlier_methods=tuple(outlier_methods),
        iqr_factor=float(outlier_cfg.get("threshold", 1.5)) if str(outlier_cfg.get("method", "iqr")) == "iqr" else 1.5,
        residual_std_multiplier=float(outlier_cfg.get("threshold", 3.0)) if str(outlier_cfg.get("method", "")) == "residual_sigma" else 3.0,
        simplify_coefficients=bool(simplification_cfg.get("enabled", True)),
        simplification_method=str(simplification_cfg.get("method", "column_norm")),
        auto_target_digits=bool(simplification_cfg.get("auto_digits", True)),
        target_digits=int(simplification_cfg.get("target_digits", 6)),
        digit_candidates=tuple(simplification_cfg.get("digit_candidates", [8, 7, 6, 5, 4])),
        simplify_rmse_tolerance=float(simplification_cfg.get("rmse_tolerance", 0.0)),
        train_ratio=float(modeling.get("dataset_split", {}).get("train_ratio", 0.7)),
        val_ratio=float(modeling.get("dataset_split", {}).get("val_ratio", 0.15)),
        random_seed=int(modeling.get("dataset_split", {}).get("random_seed", 42)),
        shuffle_dataset=bool(modeling.get("dataset_split", {}).get("shuffle", True)),
        log_fn=log_fn,
    )

    exported: Dict[str, Path] = {}
    export_cfg = dict(modeling.get("export", {}))
    formats = {str(item).strip().lower() for item in export_cfg.get("formats", ["json", "csv"])}
    if export_cfg.get("enabled", True):
        compare_paths = export_model_comparison(
            {
                "recommended_model": selection.recommended_model,
                "recommendation_reason": selection.recommendation_reason,
                "comparison_rows": selection.comparison_rows,
                "cross_interference": selection.results[selection.recommended_model].stats.get("cross_interference", {}),
                "H2O_cross_coefficients": selection.results[selection.recommended_model].stats.get("cross_interference", {}).get("simplified_coefficients", {}),
            },
            selection.comparison_rows,
            run_dir,
            prefix=str(data_source.get("gas", "co2")),
        )
        for key, path in compare_paths.items():
            if key in formats:
                exported[f"comparison_{key}"] = path
            else:
                path.unlink(missing_ok=True)

        recommended = selection.results[selection.recommended_model]
        report_paths = save_ratio_poly_report(
            recommended,
            run_dir,
            prefix=f"recommended_{str(data_source.get('gas', 'co2'))}",
            include_residuals="csv" in formats,
        )
        if "json" in formats:
            exported["recommended_json"] = report_paths["json"]
        else:
            report_paths["json"].unlink(missing_ok=True)
        if "csv" in formats and "csv" in report_paths:
            exported["recommended_csv"] = report_paths["csv"]
        elif "csv" in report_paths:
            report_paths["csv"].unlink(missing_ok=True)

        prediction_analysis = _build_prediction_analysis(
            frame,
            modeling=modeling,
            data_source=data_source,
            recommended=recommended,
        )
        _emit_log(log_fn, f"原始模型 RMSE = {prediction_analysis.summary['rmse_orig']:.6g}")
        _emit_log(log_fn, f"简化模型 RMSE = {prediction_analysis.summary['rmse_simple']:.6g}")
        _emit_log(log_fn, f"最大绝对误差（orig）= {prediction_analysis.summary['max_abs_error_orig']:.6g}")
        _emit_log(log_fn, f"最大绝对误差（simple）= {prediction_analysis.summary['max_abs_error_simple']:.6g}")
        if not prediction_analysis.top_error_simple.empty:
            _emit_log(log_fn, f"最大偏差点 index = {prediction_analysis.top_error_simple.iloc[0]['index']}")
        if not prediction_analysis.top_pred_diff.empty:
            _emit_log(log_fn, f"简化影响最大点 index = {prediction_analysis.top_pred_diff.iloc[0]['index']}")
        prediction_paths = export_prediction_analysis(
            prediction_analysis,
            run_dir,
            prefix=f"recommended_{str(data_source.get('gas', 'co2'))}",
        )
        for key, path in prediction_paths.items():
            exported[f"prediction_{key}"] = path
        _emit_log(log_fn, f"逐点对账表已导出: {prediction_paths['excel']}")

    summary_text = _build_summary_text(
        selection,
        exported,
        source_path,
        file_type=file_type,
        sheet_name=sheet_name,
    )
    summary_txt_path = run_dir / "summary.txt"
    summary_json_path = run_dir / "summary.json"
    summary_txt_path.write_text(summary_text, encoding="utf-8")
    summary_json_path.write_text(
        json.dumps(
            {
                "recommended_model": selection.recommended_model,
                "recommendation_reason": selection.recommendation_reason,
                "comparison_rows": selection.comparison_rows,
                "exported_paths": {key: str(value) for key, value in exported.items()},
                "input_path": str(source_path),
                "file_type": file_type,
                "sheet_name": sheet_name,
                "cross_interference": selection.results[selection.recommended_model].stats.get("cross_interference", {}),
                "H2O_cross_coefficients": selection.results[selection.recommended_model].stats.get("cross_interference", {}).get("simplified_coefficients", {}),
                "prediction_analysis_paths": {key: str(value) for key, value in exported.items() if key.startswith("prediction_")},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    exported["summary_txt"] = summary_txt_path
    exported["summary_json"] = summary_json_path
    _emit_log(log_fn, f"推荐模型：{selection.recommended_model}")
    _emit_log(log_fn, f"推荐原因：{selection.recommendation_reason}")
    _emit_log(log_fn, f"摘要文件：{summary_txt_path}")

    return {
        "run_dir": run_dir,
        "recommended_model": selection.recommended_model,
        "recommendation_reason": selection.recommendation_reason,
        "comparison_rows": selection.comparison_rows,
        "exported_paths": exported,
    }
