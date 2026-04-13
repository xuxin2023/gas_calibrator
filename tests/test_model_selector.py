from pathlib import Path

from gas_calibrator.coefficients.exporter import export_model_comparison
from gas_calibrator.coefficients.main import run_ratio_poly_fit_workflow
from gas_calibrator.coefficients.model_selector import compare_ratio_poly_models


def _rows():
    rows = []
    for index in range(24):
        ratio = 0.7 + 0.03 * index
        temp = 10.0 + index * 0.5
        pressure = 98.0 + (index % 5)
        temp_k = temp + 273.15
        target = 20 + 30 * ratio - 2 * (ratio**2) + 0.1 * temp_k - 0.5 * pressure
        rows.append(
            {
                "Analyzer": "GA01",
                "PointRow": index,
                "PointPhase": "CO2",
                "PointTag": f"p{index}",
                "PointTitle": f"Point {index}",
                "ppm_CO2_Tank": target,
                "R_CO2": ratio,
                "T1": temp,
                "BAR": pressure,
            }
        )
    return rows


def _cross_rows():
    rows = []
    for index in range(30):
        ratio = 0.72 + 0.02 * index
        temp = 11.0 + index * 0.4
        pressure = 97.0 + (index % 4)
        h2o = 1000.0 + (index % 6) * 160.0 + index * 10.0
        temp_k = temp + 273.15
        target = 10 + 22 * ratio - 4 * (ratio**2) + 0.06 * temp_k - 0.35 * pressure + 0.03 * h2o + 0.02 * ratio * h2o
        rows.append(
            {
                "Analyzer": "GA04",
                "PointRow": index,
                "PointPhase": "CO2",
                "PointTag": f"p{index}",
                "PointTitle": f"Point {index}",
                "ppm_CO2_Tank": target,
                "ppm_H2O_Dew": h2o,
                "R_CO2": ratio,
                "T1": temp,
                "BAR": pressure,
            }
        )
    return rows


def test_compare_ratio_poly_models_returns_recommendation() -> None:
    result = compare_ratio_poly_models(
        _rows(),
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
    )

    assert result.recommended_model in {"Model_A", "Model_B", "Model_C"}
    assert len(result.comparison_rows) == 3
    recommended = result.results[result.recommended_model]
    assert recommended.stats["leakage_safe"] is True
    assert recommended.stats["selection_scope"] == "train"


def test_run_ratio_poly_fit_workflow_can_compare_models(tmp_path: Path) -> None:
    selection = run_ratio_poly_fit_workflow(
        _rows(),
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        compare_models=True,
        export_dir=tmp_path,
        export_prefix="demo",
    )

    assert selection.recommended_model in {"Model_A", "Model_B", "Model_C"}
    assert list(tmp_path.glob("demo_model_compare_*.json"))
    assert list(tmp_path.glob("demo_model_compare_*.csv"))


def test_export_model_comparison_writes_outputs(tmp_path: Path) -> None:
    payload = {"recommended_model": "Model_A", "comparison_rows": [{"CandidateModel": "Model_A", "ValidationRMSE": 1.0}]}
    outputs = export_model_comparison(payload, payload["comparison_rows"], tmp_path, prefix="compare")

    assert outputs["json"].exists()
    assert outputs["csv"].exists()


def test_compare_ratio_poly_models_supports_h2o_cross_models() -> None:
    result = compare_ratio_poly_models(
        _cross_rows(),
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        humidity_keys=("ppm_H2O_Dew",),
        candidate_models={
            "Model_A": ["intercept", "R", "R2", "T", "P"],
            "Model_E": ["intercept", "R", "R2", "R3", "T", "T2", "RT", "P", "H", "H2", "RH"],
        },
    )

    assert result.recommended_model == "Model_E"
    recommended_row = next(row for row in result.comparison_rows if row["CandidateModel"] == "Model_E")
    assert recommended_row["HasCrossInterference"] is True
    assert recommended_row["CrossFeatureCount"] == 3
