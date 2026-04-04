from gas_calibrator.v2.analytics.marts.drift_metrics import build_drift_metrics


def test_drift_metrics_happy_path() -> None:
    features = {
        "analyzers": [
            {
                "analyzer_id": "ga01",
                "history": [
                    {
                        "run_id": "run_a",
                        "mean_co2_ppm": 400.0,
                        "mean_h2o_mmol": 0.20,
                        "mean_rmse": 0.02,
                    },
                    {
                        "run_id": "run_b",
                        "mean_co2_ppm": 405.0,
                        "mean_h2o_mmol": 0.25,
                        "mean_rmse": 0.03,
                    },
                ],
            }
        ]
    }

    report = build_drift_metrics(features)
    assert report["analyzer_count"] == 1
    analyzer = report["analyzers"][0]
    assert analyzer["status"] == "ok"
    assert analyzer["mean_co2_ppm_delta"] == 5.0
    assert analyzer["rmse_delta"] == 0.01
