import numpy as np

from gas_calibrator.coefficients.exporter import export_prediction_analysis
from gas_calibrator.coefficients.prediction_analysis import analyze_by_range, analyze_predictions


def test_analyze_predictions_returns_point_level_metrics(tmp_path) -> None:
    x_matrix = np.array(
        [
            [1.0, 0.0],
            [1.0, 1.0],
            [1.0, 2.0],
            [1.0, 3.0],
        ]
    )
    y_true = np.array([1.0, 3.0, 5.0, 7.0])
    a_orig = np.array([1.0, 2.0])
    a_simple = np.array([1.0, 1.8])

    analysis = analyze_predictions(
        x_matrix,
        y_true,
        a_orig,
        a_simple,
        sample_index=["p0", "p1", "p2", "p3"],
    )

    assert list(analysis.point_table.columns[:9]) == [
        "index",
        "Y_true",
        "Y_pred_orig",
        "error_orig",
        "rel_error_orig_pct",
        "Y_pred_simple",
        "error_simple",
        "rel_error_simple_pct",
        "pred_diff",
    ]
    assert analysis.summary["rmse_orig"] == 0.0
    assert analysis.summary["rmse_simple"] > 0.0
    assert analysis.top_error_simple.iloc[0]["index"] == "p3"

    analysis.range_table = analyze_by_range(
        analysis.point_table["Y_true"].to_numpy(float),
        analysis.point_table["error_orig"].to_numpy(float),
        analysis.point_table["error_simple"].to_numpy(float),
        bins=[0, 2, 4, 8],
    )
    paths = export_prediction_analysis(analysis, tmp_path, prefix="demo")
    assert paths["excel"].exists()
    assert paths["points_csv"].exists()
    assert paths["summary_csv"].exists()
    assert paths["ranges_csv"].exists()


def test_analyze_by_range_computes_expected_fields() -> None:
    y_true = np.array([100.0, 150.0, 450.0, 900.0])
    error_orig = np.array([1.0, -2.0, 5.0, -10.0])
    error_simple = np.array([2.0, -1.0, 7.0, -8.0])

    table = analyze_by_range(y_true, error_orig, error_simple, bins=[0, 200, 800, 1200])

    assert list(table.columns) == [
        "range_label",
        "range_start",
        "range_end",
        "count",
        "rmse_orig",
        "rmse_simple",
        "mean_error_orig",
        "mean_error_simple",
        "max_abs_error_orig",
        "max_abs_error_simple",
    ]
    assert table.iloc[0]["count"] == 2
    assert table.iloc[1]["count"] == 1
    assert table.iloc[2]["count"] == 1
