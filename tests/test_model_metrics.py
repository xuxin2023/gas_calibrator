from gas_calibrator.coefficients.model_metrics import analyze_error_by_range, compute_metrics


def test_compute_metrics_returns_expected_values() -> None:
    metrics = compute_metrics([100.0, 200.0, 300.0], [98.0, 203.0, 301.0])

    assert round(metrics["RMSE"], 6) == round(((4 + 9 + 1) / 3) ** 0.5, 6)
    assert round(metrics["Bias"], 6) == round(((98 - 100) + (203 - 200) + (301 - 300)) / 3, 6)
    assert metrics["MaxError"] == 3.0
    assert "R2" in metrics


def test_analyze_error_by_range_splits_into_bins() -> None:
    results = analyze_error_by_range(
        [100.0, 180.0, 350.0, 900.0],
        [102.0, 182.0, 340.0, 890.0],
        [0, 200, 800, 1200],
    )

    assert len(results) == 3
    assert results[0]["Count"] == 2
    assert results[1]["Count"] == 1
    assert results[2]["Count"] == 1
