from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from gas_calibrator.coefficients.model_feature_policy import AMBIENT_ONLY_MODEL_FEATURES
from gas_calibrator.export import corrected_water_points_report as report_module


def test_select_corrected_fit_rows_applies_h2o_rule() -> None:
    rows = [
        {"Analyzer": "GA01", "PointPhase": "水路", "PointTag": "h2o_20", "Temp": 20.0, "EnvTempC": 20.0, "ppm_CO2_Tank": None, "ppm_H2O_Dew": 7.0},
        {"Analyzer": "GA01", "PointPhase": "H2O", "PointTag": "h2o_30", "Temp": 30.0, "EnvTempC": 30.0, "ppm_CO2_Tank": None, "ppm_H2O_Dew": 9.0},
        {"Analyzer": "GA01", "PointPhase": "气路", "PointTag": "co2_m20_0", "Temp": -20.0, "EnvTempC": -20.0, "ppm_CO2_Tank": 0.0, "ppm_H2O_Dew": 0.04},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_m20_400", "Temp": -20.0, "EnvTempC": -20.0, "ppm_CO2_Tank": 400.0, "ppm_H2O_Dew": 0.05},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_m10_0", "Temp": -10.0, "EnvTempC": -10.0, "ppm_CO2_Tank": 0.0, "ppm_H2O_Dew": 0.06},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_0_400", "Temp": 0.0, "EnvTempC": 0.0, "ppm_CO2_Tank": 400.0, "ppm_H2O_Dew": 0.3},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_0_0", "Temp": 0.0, "EnvTempC": 0.0, "ppm_CO2_Tank": 0.0, "ppm_H2O_Dew": 0.93},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_10_0", "Temp": 10.1, "EnvTempC": 10.0, "ppm_CO2_Tank": 0.0, "ppm_H2O_Dew": 0.1},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_10_400", "Temp": 10.2, "EnvTempC": 10.0, "ppm_CO2_Tank": 400.0, "ppm_H2O_Dew": 0.1},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_20_0", "Temp": 20.0, "EnvTempC": 20.0, "ppm_CO2_Tank": 0.0, "ppm_H2O_Dew": 0.1},
    ]
    frame = pd.DataFrame(rows)

    selection = report_module.select_corrected_fit_rows_with_diagnostics(frame, gas="h2o", temperature_key="Temp")
    selected = selection["selected_frame"]
    gate_hits = selection["h2o_anchor_gate_hits"]

    assert selected["PointTag"].tolist() == [
        "h2o_20",
        "h2o_30",
        "co2_m20_0",
    ]
    assert gate_hits["PointTag"].tolist() == ["co2_m10_0", "co2_0_0"]
    assert gate_hits["GateReason"].tolist() == [
        "anchor_h2o_dew_above_limit",
        "anchor_h2o_dew_above_limit",
    ]


def test_build_corrected_water_points_report_creates_expected_sheets(
    monkeypatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "summary.xlsx"
    frame = pd.DataFrame(
        [
            {
                "PointPhase": "气路",
                "PointTag": "co2_pt",
                "PointTitle": "0°C环境，二氧化碳0ppm，气压1100hPa",
                "Temp": 18.0,
                "ppm_CO2_Tank": 0.0,
                "ppm_H2O_Dew": 0.5,
                "R_CO2": 1.4,
                "R_H2O": 0.77,
                "BAR": 108.0,
            },
            {
                "PointPhase": "水路",
                "PointTag": "h2o_pt",
                "PointTitle": "20°C环境，湿度发生器20°C/50%RH，气压1100hPa",
                "Temp": 20.0,
                "ppm_CO2_Tank": None,
                "ppm_H2O_Dew": 7.0,
                "R_CO2": 1.3,
                "R_H2O": 0.8,
                "BAR": 108.0,
            },
        ]
    )
    with pd.ExcelWriter(source_path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="GA01", index=False)

    fake_bundle = report_module.CorrectedFitBundle(
        analyzer="GA01",
        gas="CO2",
        data_scope="按水路纠正规则",
        selected_frame=frame,
        summary_row={"分析仪": "GA01", "气体": "CO2", "数据范围": "按水路纠正规则"},
        simplified_row={"分析仪": "GA01", "气体": "CO2", "数据范围": "按水路纠正规则", "a0": 1.0, "a0_term": "1"},
        original_row={"分析仪": "GA01", "气体": "CO2", "数据范围": "按水路纠正规则", "a0": 1.0, "a0_term": "1"},
        point_table=pd.DataFrame(
            [
                {
                    "分析仪": "GA01",
                    "气体": "CO2",
                    "数据范围": "按水路纠正规则",
                    "index": 0,
                    "点位行号": 1,
                    "点位相位": "气路",
                    "点位标签": "co2_pt",
                    "点位标题": "pt",
                    "Y_true": 0.0,
                    "Y_pred_orig": 0.0,
                    "error_orig": 0.0,
                    "rel_error_orig_pct": None,
                    "Y_pred_simple": 0.0,
                    "error_simple": 0.0,
                    "rel_error_simple_pct": None,
                    "pred_diff": 0.0,
                    "abs_error_orig": 0.0,
                    "abs_error_simple": 0.0,
                    "abs_pred_diff": 0.0,
                    "R": 1.4,
                    "Temp": 18.0,
                    "P_fit": 108.0,
                }
            ]
        ),
        range_table=pd.DataFrame(
            [
                {
                    "分析仪": "GA01",
                    "气体": "CO2",
                    "数据范围": "按水路纠正规则",
                    "range_label": "0-200",
                    "count": 1,
                    "rmse_orig": 0.0,
                    "rmse_simple": 0.0,
                    "mean_error_orig": 0.0,
                    "mean_error_simple": 0.0,
                    "max_abs_error_orig": 0.0,
                    "max_abs_error_simple": 0.0,
                }
            ]
        ),
        top_error_orig=pd.DataFrame([{"分析仪": "GA01", "气体": "CO2", "排序维度": "原始误差", "rank": 1, "index": 0, "点位行号": 1, "点位相位": "气路", "点位标签": "co2_pt", "Y_true": 0.0, "Y_pred_orig": 0.0, "Y_pred_simple": 0.0, "error_orig": 0.0, "error_simple": 0.0, "pred_diff": 0.0, "abs_error_orig": 0.0, "abs_error_simple": 0.0, "abs_pred_diff": 0.0}]),
        top_error_simple=pd.DataFrame([{"分析仪": "GA01", "气体": "CO2", "排序维度": "简化误差", "rank": 1, "index": 0, "点位行号": 1, "点位相位": "气路", "点位标签": "co2_pt", "Y_true": 0.0, "Y_pred_orig": 0.0, "Y_pred_simple": 0.0, "error_orig": 0.0, "error_simple": 0.0, "pred_diff": 0.0, "abs_error_orig": 0.0, "abs_error_simple": 0.0, "abs_pred_diff": 0.0}]),
        top_pred_diff=pd.DataFrame([{"分析仪": "GA01", "气体": "CO2", "排序维度": "预测差值", "rank": 1, "index": 0, "点位行号": 1, "点位相位": "气路", "点位标签": "co2_pt", "Y_true": 0.0, "Y_pred_orig": 0.0, "Y_pred_simple": 0.0, "error_orig": 0.0, "error_simple": 0.0, "pred_diff": 0.0, "abs_error_orig": 0.0, "abs_error_simple": 0.0, "abs_pred_diff": 0.0}]),
    )

    monkeypatch.setattr(report_module, "_build_bundle", lambda *args, **kwargs: fake_bundle)

    out_path = tmp_path / "report.xlsx"
    result = report_module.build_corrected_water_points_report([source_path], output_path=out_path)

    assert out_path.exists()
    assert result["summary"].shape[0] == 2
    assert "h2o_selected_rows" in result

    workbook = pd.ExcelFile(out_path)
    assert workbook.sheet_names == ["说明", "汇总", "简化系数", "原始系数", "逐点对账", "分区间分析", "误差TopN"]


def test_build_bundle_ambient_only_passes_model_features(monkeypatch) -> None:
    selected = pd.DataFrame(
        [
            {
                "PointRow": 1,
                "PointPhase": "气路",
                "PointTag": "co2_ambient_1",
                "PointTitle": "ambient-1",
                "FitTemp": 20.0,
                "ppm_CO2_Tank": 400.0,
                "R_CO2": 1.0,
                "P_fit": 101.0,
            },
            {
                "PointRow": 2,
                "PointPhase": "气路",
                "PointTag": "co2_ambient_2",
                "PointTitle": "ambient-2",
                "FitTemp": 21.0,
                "ppm_CO2_Tank": 500.0,
                "R_CO2": 1.1,
                "P_fit": 101.1,
            },
        ]
    )
    captured: dict[str, object] = {}

    def fake_fit(rows, **kwargs):
        captured["model_features"] = kwargs.get("model_features")
        return SimpleNamespace(
            residuals=[
                {
                    "target": 400.0,
                    "prediction_original": 400.0,
                    "error_original": 0.0,
                    "prediction_simplified": 400.0,
                    "error_simplified": 0.0,
                    "R": 1.0,
                    "T_c": 20.0,
                    "P": 101.0,
                    "PointRow": 1,
                    "PointPhase": "气路",
                    "PointTag": "co2_ambient_1",
                    "PointTitle": "ambient-1",
                },
                {
                    "target": 500.0,
                    "prediction_original": 500.0,
                    "error_original": 0.0,
                    "prediction_simplified": 500.0,
                    "error_simplified": 0.0,
                    "R": 1.1,
                    "T_c": 21.0,
                    "P": 101.1,
                    "PointRow": 2,
                    "PointPhase": "气路",
                    "PointTag": "co2_ambient_2",
                    "PointTitle": "ambient-2",
                },
            ],
            feature_terms={name: name for name in AMBIENT_ONLY_MODEL_FEATURES},
            feature_names=list(AMBIENT_ONLY_MODEL_FEATURES),
            simplified_coefficients={name: float(index) for index, name in enumerate(AMBIENT_ONLY_MODEL_FEATURES)},
            original_coefficients={name: float(index) for index, name in enumerate(AMBIENT_ONLY_MODEL_FEATURES)},
            stats={"mae_simplified": 0.0},
            model="ratio_poly_rt_p",
            n=2,
        )

    monkeypatch.setattr(report_module, "fit_ratio_poly_rt_p", fake_fit)

    bundle = report_module._build_bundle(
        "GA01",
        "co2",
        "按水路纠正规则",
        selected,
        target_key="ppm_CO2_Tank",
        ratio_key="R_CO2",
        temperature_key="Temp",
        pressure_key="P_fit",
        coeff_cfg={"selected_pressure_points": [" ambient "]},
    )

    assert captured["model_features"] == AMBIENT_ONLY_MODEL_FEATURES
    assert bundle.summary_row["模型特征策略"] == "ambient_only_fallback"
    assert bundle.summary_row["模型特征列表"] == ",".join(AMBIENT_ONLY_MODEL_FEATURES)


def test_build_bundle_mixed_pressure_keeps_default_full_model(monkeypatch) -> None:
    selected = pd.DataFrame(
        [
            {
                "PointRow": 1,
                "PointPhase": "气路",
                "PointTag": "co2_mixed_1",
                "PointTitle": "mixed-1",
                "FitTemp": 20.0,
                "ppm_CO2_Tank": 400.0,
                "R_CO2": 1.0,
                "P_fit": 101.0,
            },
            {
                "PointRow": 2,
                "PointPhase": "气路",
                "PointTag": "co2_mixed_2",
                "PointTitle": "mixed-2",
                "FitTemp": 21.0,
                "ppm_CO2_Tank": 500.0,
                "R_CO2": 1.1,
                "P_fit": 50.0,
            },
        ]
    )
    captured: dict[str, object] = {}

    def fake_fit(rows, **kwargs):
        captured["model_features"] = kwargs.get("model_features")
        return SimpleNamespace(
            residuals=[
                {
                    "target": 400.0,
                    "prediction_original": 400.0,
                    "error_original": 0.0,
                    "prediction_simplified": 400.0,
                    "error_simplified": 0.0,
                    "R": 1.0,
                    "T_c": 20.0,
                    "P": 101.0,
                    "PointRow": 1,
                    "PointPhase": "气路",
                    "PointTag": "co2_mixed_1",
                    "PointTitle": "mixed-1",
                },
                {
                    "target": 500.0,
                    "prediction_original": 500.0,
                    "error_original": 0.0,
                    "prediction_simplified": 500.0,
                    "error_simplified": 0.0,
                    "R": 1.1,
                    "T_c": 21.0,
                    "P": 50.0,
                    "PointRow": 2,
                    "PointPhase": "气路",
                    "PointTag": "co2_mixed_2",
                    "PointTitle": "mixed-2",
                },
            ],
            feature_terms={"intercept": "intercept"},
            feature_names=["intercept"],
            simplified_coefficients={"intercept": 1.0},
            original_coefficients={"intercept": 1.0},
            stats={"mae_simplified": 0.0},
            model="ratio_poly_rt_p",
            n=2,
        )

    monkeypatch.setattr(report_module, "fit_ratio_poly_rt_p", fake_fit)

    bundle = report_module._build_bundle(
        "GA01",
        "co2",
        "按水路纠正规则",
        selected,
        target_key="ppm_CO2_Tank",
        ratio_key="R_CO2",
        temperature_key="Temp",
        pressure_key="P_fit",
        coeff_cfg={"selected_pressure_points": ["ambient", 500]},
    )

    assert captured["model_features"] is None
    assert bundle.summary_row["模型特征策略"] == "default_full_model"


def test_build_corrected_water_points_report_uses_summary_column_temperature_by_gas(
    monkeypatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "summary_temp_keys.xlsx"
    frame = pd.DataFrame(
        [
            {
                "PointPhase": "气路",
                "PointTag": "co2_pt",
                "PointTitle": "0°C环境，二氧化碳 0 ppm",
                "Temp": 18.0,
                "thermometer_temp_c": 11.5,
                "h2o_temp_c": 9.9,
                "ppm_CO2_Tank": 0.0,
                "ppm_H2O_Dew": 0.5,
                "R_CO2": 1.4,
                "R_H2O": 0.77,
                "BAR": 108.0,
            },
            {
                "PointPhase": "水路",
                "PointTag": "h2o_pt",
                "PointTitle": "20°C环境，湿度发生器 20°C/50%RH",
                "Temp": 20.0,
                "thermometer_temp_c": 21.5,
                "h2o_temp_c": 22.5,
                "ppm_CO2_Tank": None,
                "ppm_H2O_Dew": 7.0,
                "R_CO2": 1.3,
                "R_H2O": 0.8,
                "BAR": 108.0,
            },
        ]
    )
    with pd.ExcelWriter(source_path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="GA01", index=False)

    captured: dict[str, str] = {}

    def fake_build_bundle(analyzer, gas, data_scope, selected_frame, **kwargs):
        temperature_key = str(kwargs["temperature_key"])
        captured[gas] = temperature_key
        gas_upper = gas.upper()
        return report_module.CorrectedFitBundle(
            analyzer=analyzer,
            gas=gas_upper,
            data_scope=data_scope,
            selected_frame=selected_frame.drop(columns=["Analyzer", "Gas", "DataScope"], errors="ignore"),
            summary_row={
                "Analyzer": analyzer,
                "Gas": gas_upper,
                "DataScope": data_scope,
                "temperature_column_used": temperature_key,
            },
            simplified_row={"Analyzer": analyzer, "Gas": gas_upper, "DataScope": data_scope, "a0": 1.0},
            original_row={"Analyzer": analyzer, "Gas": gas_upper, "DataScope": data_scope, "a0": 1.0},
            point_table=pd.DataFrame(),
            range_table=pd.DataFrame(),
            top_error_orig=pd.DataFrame(),
            top_error_simple=pd.DataFrame(),
            top_pred_diff=pd.DataFrame(),
        )

    monkeypatch.setattr(report_module, "_build_bundle", fake_build_bundle)

    result = report_module.build_corrected_water_points_report(
        [source_path],
        output_path=tmp_path / "report_temp_keys.xlsx",
        coeff_cfg={
            "summary_columns": {
                "co2": {"temperature": "thermometer_temp_c"},
                "h2o": {"temperature": "h2o_temp_c"},
            }
        },
    )

    assert captured == {"co2": "thermometer_temp_c", "h2o": "h2o_temp_c"}
    assert set(result["summary"]["temperature_column_used"].tolist()) == {"thermometer_temp_c", "h2o_temp_c"}
    assert set(result["h2o_selected_rows"]["TemperatureColumnUsed"].tolist()) == {"h2o_temp_c"}


def test_build_corrected_water_points_report_falls_back_to_temp_when_temperature_not_configured(
    monkeypatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "summary_temp_fallback.xlsx"
    frame = pd.DataFrame(
        [
            {
                "PointPhase": "气路",
                "PointTag": "co2_pt",
                "PointTitle": "0°C环境，二氧化碳 0 ppm",
                "Temp": 18.0,
                "ppm_CO2_Tank": 0.0,
                "ppm_H2O_Dew": 0.5,
                "R_CO2": 1.4,
                "R_H2O": 0.77,
                "BAR": 108.0,
            },
            {
                "PointPhase": "水路",
                "PointTag": "h2o_pt",
                "PointTitle": "20°C环境，湿度发生器 20°C/50%RH",
                "Temp": 20.0,
                "ppm_CO2_Tank": None,
                "ppm_H2O_Dew": 7.0,
                "R_CO2": 1.3,
                "R_H2O": 0.8,
                "BAR": 108.0,
            },
        ]
    )
    with pd.ExcelWriter(source_path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="GA01", index=False)

    captured: dict[str, str] = {}

    def fake_build_bundle(analyzer, gas, data_scope, selected_frame, **kwargs):
        temperature_key = str(kwargs["temperature_key"])
        captured[gas] = temperature_key
        gas_upper = gas.upper()
        return report_module.CorrectedFitBundle(
            analyzer=analyzer,
            gas=gas_upper,
            data_scope=data_scope,
            selected_frame=selected_frame.drop(columns=["Analyzer", "Gas", "DataScope"], errors="ignore"),
            summary_row={
                "Analyzer": analyzer,
                "Gas": gas_upper,
                "DataScope": data_scope,
                "temperature_column_used": temperature_key,
            },
            simplified_row={"Analyzer": analyzer, "Gas": gas_upper, "DataScope": data_scope, "a0": 1.0},
            original_row={"Analyzer": analyzer, "Gas": gas_upper, "DataScope": data_scope, "a0": 1.0},
            point_table=pd.DataFrame(),
            range_table=pd.DataFrame(),
            top_error_orig=pd.DataFrame(),
            top_error_simple=pd.DataFrame(),
            top_pred_diff=pd.DataFrame(),
        )

    monkeypatch.setattr(report_module, "_build_bundle", fake_build_bundle)

    result = report_module.build_corrected_water_points_report(
        [source_path],
        output_path=tmp_path / "report_temp_fallback.xlsx",
        coeff_cfg={},
    )

    assert captured == {"co2": "Temp", "h2o": "Temp"}
    assert set(result["summary"]["temperature_column_used"].tolist()) == {"Temp"}
    assert set(result["h2o_selected_rows"]["TemperatureColumnUsed"].tolist()) == {"Temp"}


def test_build_bundle_marks_bad_ratio_input_in_summary(monkeypatch) -> None:
    selected = pd.DataFrame(
        [
            {
                "PointRow": 1,
                "PointPhase": "气路",
                "PointTag": "co2_bad_1",
                "PointTitle": "bad-1",
                "FitTemp": 20.0,
                "ppm_CO2_Tank": 0.0,
                "R_CO2": 1.0,
                "P_fit": 101.0,
            },
            {
                "PointRow": 2,
                "PointPhase": "气路",
                "PointTag": "co2_bad_2",
                "PointTitle": "bad-2",
                "FitTemp": 20.5,
                "ppm_CO2_Tank": 200.0,
                "R_CO2": 1.0,
                "P_fit": 101.0,
            },
            {
                "PointRow": 3,
                "PointPhase": "气路",
                "PointTag": "co2_bad_3",
                "PointTitle": "bad-3",
                "FitTemp": 21.0,
                "ppm_CO2_Tank": 400.0,
                "R_CO2": 1.0,
                "P_fit": 101.0,
            },
            {
                "PointRow": 4,
                "PointPhase": "气路",
                "PointTag": "co2_bad_4",
                "PointTitle": "bad-4",
                "FitTemp": 21.5,
                "ppm_CO2_Tank": 800.0,
                "R_CO2": 1.0,
                "P_fit": 101.0,
            },
        ]
    )

    def fake_fit(rows, **kwargs):
        residuals = []
        for index, row in enumerate(rows, start=1):
            residuals.append(
                {
                    "target": row["ppm_CO2_Tank"],
                    "prediction_original": row["ppm_CO2_Tank"],
                    "error_original": 0.0,
                    "prediction_simplified": row["ppm_CO2_Tank"],
                    "error_simplified": 0.0,
                    "R": row["R_CO2"],
                    "T_c": row["FitTemp"],
                    "P": row["P_fit"],
                    "PointRow": index,
                    "PointPhase": row["PointPhase"],
                    "PointTag": row["PointTag"],
                    "PointTitle": row["PointTitle"],
                }
            )
        return SimpleNamespace(
            residuals=residuals,
            feature_terms={"intercept": "intercept"},
            feature_names=["intercept"],
            simplified_coefficients={"intercept": 1.0},
            original_coefficients={"intercept": 1.0},
            stats={"mae_simplified": 0.0},
            model="ratio_poly_rt_p",
            n=len(rows),
        )

    monkeypatch.setattr(report_module, "fit_ratio_poly_rt_p", fake_fit)

    bundle = report_module._build_bundle(
        "GA03",
        "co2",
        "按水路纠正规则",
        selected,
        target_key="ppm_CO2_Tank",
        ratio_key="R_CO2",
        temperature_key="Temp",
        pressure_key="P_fit",
        coeff_cfg={},
    )

    assert bundle.summary_row["fit_input_quality"] == "fail"
    assert "ratio_unique_count_too_low" in bundle.summary_row["fit_input_warning"]
    assert "ratio_span_too_small" in bundle.summary_row["fit_input_warning"]
    assert bundle.summary_row["ratio_unique_count"] == 1
    assert bundle.summary_row["ratio_span"] == 0.0
    assert bundle.summary_row["delivery_recommendation"] == "forbid_download"


def test_build_corrected_water_points_report_h2o_selected_rows_tolerates_existing_metadata_columns(
    monkeypatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "summary_h2o.xlsx"
    frame = pd.DataFrame(
        [
            {
                "PointPhase": "姘磋矾",
                "PointTag": "h2o_pt",
                "PointTitle": "h2o-point",
                "Temp": 20.0,
                "ppm_CO2_Tank": None,
                "ppm_H2O_Dew": 7.0,
                "R_CO2": 1.3,
                "R_H2O": 0.8,
                "BAR": 108.0,
            }
        ]
    )
    with pd.ExcelWriter(source_path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="GA03", index=False)

    fake_bundle = report_module.CorrectedFitBundle(
        analyzer="GA03",
        gas="H2O",
        data_scope="ambient_only",
        selected_frame=pd.DataFrame(
            [
                {
                    "Analyzer": "GA03",
                    "Gas": "H2O",
                    "DataScope": "ambient_only",
                    "SelectionOrigin": "h2o_phase",
                    "PointRow": 9,
                    "PointPhase": "姘磋矾",
                    "PointTag": "h2o_pt",
                    "PointTitle": "h2o-point",
                    "EnvTempC": 20.0,
                    "Temp": 20.0,
                    "FitTemp": 20.0,
                    "TemperatureColumnUsed": "Temp",
                    "ppm_CO2_Tank": None,
                    "ppm_H2O_Dew": 7.0,
                    "R_CO2": 1.3,
                    "R_H2O": 0.8,
                    "BAR": 108.0,
                    "SourceFile": "summary_h2o.xlsx",
                    "SourceStamp": "20260420_000000",
                }
            ]
        ),
        summary_row={"Analyzer": "GA03", "Gas": "H2O", "DataScope": "ambient_only"},
        simplified_row={"Analyzer": "GA03", "Gas": "H2O", "DataScope": "ambient_only", "a0": 1.0},
        original_row={"Analyzer": "GA03", "Gas": "H2O", "DataScope": "ambient_only", "a0": 1.0},
        point_table=pd.DataFrame([{"Analyzer": "GA03", "Gas": "H2O", "DataScope": "ambient_only", "index": 0}]),
        range_table=pd.DataFrame([{"Analyzer": "GA03", "Gas": "H2O", "DataScope": "ambient_only", "range_label": "0-10"}]),
        top_error_orig=pd.DataFrame([{"Analyzer": "GA03", "Gas": "H2O", "rank": 1, "index": 0}]),
        top_error_simple=pd.DataFrame([{"Analyzer": "GA03", "Gas": "H2O", "rank": 1, "index": 0}]),
        top_pred_diff=pd.DataFrame([{"Analyzer": "GA03", "Gas": "H2O", "rank": 1, "index": 0}]),
    )

    monkeypatch.setattr(
        report_module,
        "select_corrected_fit_rows_with_diagnostics",
        lambda *_args, **_kwargs: {
            "selected_frame": pd.DataFrame([{"Temp": 20.0, "PointPhase": "姘磋矾", "PointTag": "h2o_pt"}]),
            "h2o_anchor_gate_hits": pd.DataFrame(),
        },
    )
    monkeypatch.setattr(report_module, "_build_bundle", lambda *args, **kwargs: fake_bundle)

    out_path = tmp_path / "report_h2o.xlsx"
    result = report_module.build_corrected_water_points_report([source_path], output_path=out_path)

    selected_rows = pd.DataFrame(result["h2o_selected_rows"])
    assert out_path.exists()
    assert selected_rows.shape[0] >= 1
    assert list(selected_rows.columns[:3]) == ["Analyzer", "Gas", "DataScope"]
    assert selected_rows["Analyzer"].eq("GA03").all()
