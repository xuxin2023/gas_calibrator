from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from gas_calibrator.export import corrected_water_points_report as report_module


def test_select_corrected_fit_rows_applies_h2o_rule() -> None:
    rows = [
        {"Analyzer": "GA01", "PointPhase": "水路", "PointTag": "h2o_20", "Temp": 20.0, "EnvTempC": 20.0, "ppm_CO2_Tank": None},
        {"Analyzer": "GA01", "PointPhase": "H2O", "PointTag": "h2o_30", "Temp": 30.0, "EnvTempC": 30.0, "ppm_CO2_Tank": None},
        {"Analyzer": "GA01", "PointPhase": "气路", "PointTag": "co2_m20_0", "Temp": -20.0, "EnvTempC": -20.0, "ppm_CO2_Tank": 0.0},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_m20_400", "Temp": -20.0, "EnvTempC": -20.0, "ppm_CO2_Tank": 400.0},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_m10_0", "Temp": -10.0, "EnvTempC": -10.0, "ppm_CO2_Tank": 0.0},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_0_400", "Temp": 0.0, "EnvTempC": 0.0, "ppm_CO2_Tank": 400.0},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_0_0", "Temp": 0.0, "EnvTempC": 0.0, "ppm_CO2_Tank": 0.0},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_10_0", "Temp": 10.1, "EnvTempC": 10.0, "ppm_CO2_Tank": 0.0},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_10_400", "Temp": 10.2, "EnvTempC": 10.0, "ppm_CO2_Tank": 400.0},
        {"Analyzer": "GA01", "PointPhase": "CO2", "PointTag": "co2_20_0", "Temp": 20.0, "EnvTempC": 20.0, "ppm_CO2_Tank": 0.0},
    ]
    frame = pd.DataFrame(rows)

    selected = report_module.select_corrected_fit_rows(frame, gas="h2o", temperature_key="Temp")

    assert selected["PointTag"].tolist() == [
        "h2o_20",
        "h2o_30",
        "co2_m20_0",
        "co2_m10_0",
        "co2_0_0",
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

    workbook = pd.ExcelFile(out_path)
    assert workbook.sheet_names == ["说明", "汇总", "简化系数", "原始系数", "逐点对账", "分区间分析", "误差TopN"]
