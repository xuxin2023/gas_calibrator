import json
from pathlib import Path

import numpy as np

from gas_calibrator.coefficients.data_loader import records_to_dataframe
from gas_calibrator.coefficients.dataset_splitter import split_dataset
from gas_calibrator.coefficients.feature_builder import build_feature_matrix, build_feature_terms
from gas_calibrator.coefficients.fit_ratio_poly import fit_ratio_poly_rt_p, save_ratio_poly_report
from gas_calibrator.coefficients.fit_ratio_poly_evolved import fit_ratio_poly_rt_p_evolved
from gas_calibrator.coefficients.main import run_ratio_poly_fit_workflow
from gas_calibrator.coefficients.model_fit import fit_least_squares
from gas_calibrator.coefficients.model_metrics import analyze_error_by_range, compute_metrics


def _synthetic_summary_rows():
    coefficients = {
        "a0": 42.5,
        "a1": 120.0,
        "a2": -35.0,
        "a3": 6.0,
        "a4": 0.11,
        "a5": -0.00012,
        "a6": 0.32,
        "a7": -0.75,
        "a8": 0.0045,
    }
    rows = []
    for index in range(30):
        ratio_value = 0.75 + 0.03 * index
        temp_c = 5.0 + (index % 10) * 4.0 + index * 0.2
        pressure_value = 94.0 + (index % 7) * 2.0 + index * 0.3
        temp_k = temp_c + 273.15
        target = (
            coefficients["a0"]
            + coefficients["a1"] * ratio_value
            + coefficients["a2"] * (ratio_value**2)
            + coefficients["a3"] * (ratio_value**3)
            + coefficients["a4"] * temp_k
            + coefficients["a5"] * (temp_k**2)
            + coefficients["a6"] * ratio_value * temp_k
            + coefficients["a7"] * pressure_value
            + coefficients["a8"] * ratio_value * temp_k * pressure_value
        )
        rows.append(
            {
                "Analyzer": "GA07",
                "PointRow": index,
                "PointPhase": "CO2",
                "PointTag": f"co2_point_{index}",
                "PointTitle": f"Point {index}",
                "ppm_CO2_Tank": target,
                "R_CO2": ratio_value,
                "T1": temp_c,
                "BAR": pressure_value,
            }
        )
    return rows


def _synthetic_cross_rows():
    rows = []
    for index in range(36):
        ratio_value = 0.65 + 0.025 * index
        temp_c = 8.0 + (index % 9) * 3.5
        pressure_value = 95.0 + (index % 6) * 2.5
        h2o_value = 1200.0 + (index % 8) * 180.0 + index * 12.0
        temp_k = temp_c + 273.15
        target = (
            25.0
            + 55.0 * ratio_value
            - 12.0 * (ratio_value**2)
            + 0.08 * temp_k
            - 0.35 * pressure_value
            + 0.045 * h2o_value
            - 0.00001 * (h2o_value**2)
            + 0.018 * ratio_value * h2o_value
        )
        rows.append(
            {
                "Analyzer": "GA03",
                "PointRow": index,
                "PointPhase": "CO2",
                "PointTag": f"cross_point_{index}",
                "PointTitle": f"Cross Point {index}",
                "ppm_CO2_Tank": target,
                "ppm_H2O_Dew": h2o_value,
                "R_CO2": ratio_value,
                "T1": temp_c,
                "BAR": pressure_value,
            }
        )
    return rows


def test_fit_ratio_poly_rt_p_recovers_coefficients() -> None:
    rows = _synthetic_summary_rows()
    result = fit_ratio_poly_rt_p(
        rows,
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        ratio_degree=3,
        simplify_coefficients=False,
        random_seed=5,
    )

    assert result.model == "ratio_poly_rt_p"
    assert result.n == len(rows)
    assert result.stats["rmse_original"] < 1e-8
    assert result.stats["rmse_simplified"] < 1e-8
    assert result.feature_terms["a1"] == "R"
    assert result.feature_terms["a8"] == "R*T_k*P"
    assert abs(result.original_coefficients["a0"] - 42.5) < 1e-5
    assert abs(result.original_coefficients["a8"] - 0.0045) < 1e-8
    assert result.stats["dataset_split"]["train_count"] >= 9
    assert result.stats["dataset_split"]["fit_scope"] == "train"
    assert result.stats["selection_scope"] == "train"
    assert result.stats["leakage_safe"] is True
    assert "validation_metrics" in result.stats
    assert "test_metrics" in result.stats


def test_fit_ratio_poly_rt_p_uses_train_dataset_for_coefficients() -> None:
    rows = _synthetic_summary_rows()
    dataframe = records_to_dataframe(rows)
    train_df, _val_df, _test_df, _metadata = split_dataset(
        dataframe,
        random_seed=13,
        return_metadata=True,
    )
    x_matrix, _terms = build_feature_matrix(
        train_df,
        ratio_column="R_CO2",
        temperature_column="T1",
        pressure_column="BAR",
        ratio_degree=3,
        temperature_offset_c=273.15,
        add_intercept=True,
    )
    y_vector = train_df["ppm_CO2_Tank"].astype(float).to_numpy()
    expected, *_ = np.linalg.lstsq(x_matrix, y_vector, rcond=None)

    result = fit_ratio_poly_rt_p(
        rows,
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        simplify_coefficients=False,
        random_seed=13,
    )

    actual = np.array([result.original_coefficients[f"a{index}"] for index in range(len(expected))], dtype=float)
    assert np.allclose(actual, expected)
    assert set(result.stats["dataset_split"]["fit_indices"]).issubset(set(result.stats["dataset_split"]["raw_train_indices"]))
    assert set(result.stats["dataset_split"]["fit_indices"]).isdisjoint(set(result.stats["dataset_split"]["validation_indices"]))
    assert set(result.stats["dataset_split"]["fit_indices"]).isdisjoint(set(result.stats["dataset_split"]["test_indices"]))


def test_fit_ratio_poly_rt_p_simplification_selection_never_reads_test() -> None:
    result = fit_ratio_poly_rt_p(
        _synthetic_summary_rows(),
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        auto_target_digits=True,
        digit_candidates=(8, 7, 6, 5),
        simplify_rmse_tolerance=0.0,
        simplification_selection_scope="train",
        random_seed=17,
    )

    selection_indices = set(result.stats["dataset_split"]["selection_indices"])
    test_indices = set(result.stats["dataset_split"]["test_indices"])
    assert result.stats["selection_scope"] == "train"
    assert selection_indices.isdisjoint(test_indices)
    assert result.stats["simplification_summary"]["selection_scope"] == "train"


def test_fit_ratio_poly_supports_ridge_like_and_outlier_filtering() -> None:
    rows = _synthetic_summary_rows()
    rows.append(
        {
            "Analyzer": "GA07",
            "PointRow": 999,
            "PointPhase": "CO2",
            "PointTag": "outlier",
            "PointTitle": "Outlier",
            "ppm_CO2_Tank": 9000.0,
            "R_CO2": 1.1,
            "T1": 25.0,
            "BAR": 101.0,
        }
    )
    result = fit_ratio_poly_rt_p(
        rows,
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        fitting_method="ridge_like",
        ridge_lambda=1e-3,
        outlier_methods=("iqr", "residual_sigma"),
    )

    assert result.stats["fit_settings"]["fitting_method"] == "ridge_like"
    assert result.stats["outlier_detection"]["outlier_count"] >= 1
    assert result.stats["original_coefficient_analysis"]["condition_number"] > 0


def test_save_ratio_poly_report_writes_outputs(tmp_path: Path) -> None:
    rows = _synthetic_summary_rows()
    result = fit_ratio_poly_rt_p(
        rows,
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        ratio_degree=3,
        simplify_coefficients=True,
        simplification_method="column_norm",
        target_digits=6,
    )

    outputs = save_ratio_poly_report(result, tmp_path, prefix="co2_ga07", include_residuals=True)
    assert outputs["json"].exists()
    assert outputs["csv"].exists()

    payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
    assert payload["model"] == "ratio_poly_rt_p"
    assert payload["feature_terms"]["a4"] == "T_k"
    assert "simplified_coefficients" in payload
    assert "validation_metrics" in payload["stats"]


def test_fit_ratio_poly_rt_p_evolved_is_more_robust_with_outlier() -> None:
    rows = _synthetic_summary_rows()
    rows.append(
        {
            "Analyzer": "GA07",
            "PointRow": 99,
            "PointPhase": "CO2",
            "PointTag": "co2_outlier",
            "PointTitle": "Outlier",
            "ppm_CO2_Tank": 6000.0,
            "R_CO2": 1.12,
            "T1": 22.0,
            "BAR": 101.0,
        }
    )

    basic = fit_ratio_poly_rt_p(
        rows,
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        ratio_degree=3,
        simplify_coefficients=False,
    )
    evolved = fit_ratio_poly_rt_p_evolved(
        rows,
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        ratio_degree=3,
        simplify_coefficients=False,
    )

    assert evolved.model == "ratio_poly_rt_p_evolved"
    assert evolved.stats["downweighted_samples"] >= 1
    assert "validation_metrics" in evolved.stats
    assert "test_metrics" in evolved.stats
    assert basic.stats["dataset_split"]["fit_scope"] == "train"


def test_build_feature_matrix_keeps_fixed_feature_order() -> None:
    dataframe = records_to_dataframe(_synthetic_summary_rows())
    x_matrix, terms = build_feature_matrix(
        dataframe,
        ratio_column="R_CO2",
        temperature_column="T1",
        pressure_column="BAR",
        ratio_degree=3,
        temperature_offset_c=273.15,
        add_intercept=True,
    )

    assert x_matrix.shape[1] == 9
    assert terms == build_feature_terms(3, True)
    assert terms == ["1", "R", "R^2", "R^3", "T_k", "T_k^2", "R*T_k", "P", "R*T_k*P"]


def test_build_feature_matrix_supports_configured_model_features() -> None:
    dataframe = records_to_dataframe(_synthetic_summary_rows())
    x_matrix, terms = build_feature_matrix(
        dataframe,
        ratio_column="R_CO2",
        temperature_column="T1",
        pressure_column="BAR",
        model_features=["intercept", "R", "T", "P"],
    )

    assert x_matrix.shape[1] == 4
    assert terms == ["1", "R", "T_k", "P"]


def test_build_feature_matrix_supports_h2o_cross_features() -> None:
    dataframe = records_to_dataframe(_synthetic_cross_rows())
    x_matrix, terms = build_feature_matrix(
        dataframe,
        ratio_column="R_CO2",
        temperature_column="T1",
        pressure_column="BAR",
        humidity_column="ppm_H2O_Dew",
        model_features=["intercept", "R", "H", "H2", "RH"],
    )

    assert x_matrix.shape[1] == 5
    assert terms == ["1", "R", "H2O", "H2O^2", "R*H2O"]


def test_fit_ratio_poly_exports_h2o_cross_coefficients(tmp_path: Path) -> None:
    rows = _synthetic_cross_rows()
    result = fit_ratio_poly_rt_p(
        rows,
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        humidity_keys=("ppm_H2O_Dew",),
        model_features=["intercept", "R", "R2", "T", "P", "H", "H2", "RH"],
        simplify_coefficients=False,
        random_seed=9,
    )

    cross = result.stats["cross_interference"]
    assert cross["enabled"] is True
    assert set(cross["simplified_coefficients"].keys()) == {"a_H", "a_H2", "a_RH"}

    outputs = save_ratio_poly_report(result, tmp_path, prefix="co2_cross", include_residuals=False)
    payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
    assert set(payload["H2O_cross_coefficients"].keys()) == {"a_H", "a_H2", "a_RH"}


def test_fit_least_squares_returns_coefficients_and_predictions() -> None:
    dataframe = records_to_dataframe(_synthetic_summary_rows())
    x_matrix, _terms = build_feature_matrix(
        dataframe,
        ratio_column="R_CO2",
        temperature_column="T1",
        pressure_column="BAR",
        ratio_degree=3,
        temperature_offset_c=273.15,
        add_intercept=True,
    )
    y_vector = dataframe["ppm_CO2_Tank"].astype(float).to_numpy()

    result = fit_least_squares(x_matrix, y_vector)

    assert result.coefficients.shape[0] == x_matrix.shape[1]
    assert result.predictions.shape[0] == y_vector.shape[0]
    assert abs(result.coefficients[0] - 42.5) < 1e-5


def test_run_ratio_poly_fit_workflow_dispatches_to_selected_model() -> None:
    messages: list[str] = []
    result = run_ratio_poly_fit_workflow(
        _synthetic_summary_rows(),
        gas="co2",
        target_key="ppm_CO2_Tank",
        ratio_keys=("R_CO2",),
        temp_keys=("T1",),
        pressure_keys=("BAR",),
        model="ratio_poly_rt_p_evolved",
        simplify_coefficients=False,
        log_fn=messages.append,
    )

    assert result.model == "ratio_poly_rt_p_evolved"
    assert any("主流程：准备执行 ratio_poly_rt_p_evolved 拟合" in message for message in messages)


def test_split_dataset_is_deterministic_with_seed() -> None:
    dataframe = records_to_dataframe([{"value": idx} for idx in range(20)])
    train_a, val_a, test_a = split_dataset(dataframe, random_seed=7)
    train_b, val_b, test_b = split_dataset(dataframe, random_seed=7)

    assert len(train_a) == 14
    assert len(val_a) == 3
    assert len(test_a) == 3
    assert train_a.index.tolist() == train_b.index.tolist()
    assert val_a.index.tolist() == val_b.index.tolist()
    assert test_a.index.tolist() == test_b.index.tolist()


def test_compute_metrics_and_range_analysis() -> None:
    y_true = [100.0, 200.0, 500.0, 900.0]
    y_pred = [110.0, 195.0, 490.0, 920.0]

    metrics = compute_metrics(y_true, y_pred)
    ranges = analyze_error_by_range(y_true, y_pred, [0, 200, 800, 1200])

    assert round(metrics["RMSE"], 6) == round((((10**2 + 5**2 + 10**2 + 20**2) / 4) ** 0.5), 6)
    assert "R2" in metrics
    assert metrics["MaxError"] == 20.0
    assert ranges[0]["Count"] == 1
    assert ranges[1]["Count"] == 2
    assert ranges[2]["Count"] == 1
