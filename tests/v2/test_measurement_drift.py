from gas_calibrator.v2.analytics.measurement.marts.measurement_drift import build_measurement_drift


def test_measurement_drift_happy_path() -> None:
    features = {
        "analyzer_features": [
            {
                "analyzer_label": "ga01",
                "run_count": 2,
                "history": [
                    {
                        "run_id": "run_alpha",
                        "mean_co2_ppm": 400.0,
                        "mean_h2o_mmol": 0.7,
                        "mean_co2_ratio_f": 1.000,
                        "mean_h2o_ratio_f": 0.700,
                        "mean_ref_signal": 3500.0,
                        "mean_co2_signal": 4500.0,
                        "mean_h2o_signal": 2500.0,
                        "mean_rmse": 0.02,
                    },
                    {
                        "run_id": "run_beta",
                        "mean_co2_ppm": 430.0,
                        "mean_h2o_mmol": 0.9,
                        "mean_co2_ratio_f": 1.030,
                        "mean_h2o_ratio_f": 0.730,
                        "mean_ref_signal": 3900.0,
                        "mean_co2_signal": 5000.0,
                        "mean_h2o_signal": 2900.0,
                        "mean_rmse": 0.05,
                    },
                ],
            }
        ]
    }

    report = build_measurement_drift(features)
    assert report["analyzer_count"] == 1
    assert report["analyzers"][0]["status"] in {"watch", "alert"}
    assert report["analyzers"][0]["co2_ppm_delta"] == 30.0
