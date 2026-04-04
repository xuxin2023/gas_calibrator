from gas_calibrator.v2.analytics.measurement.marts.instrument_health import build_instrument_health


def test_instrument_health_happy_path() -> None:
    features = {
        "analyzer_features": [
            {
                "analyzer_label": "ga01",
                "frame_count": 10,
                "run_count": 2,
                "usable_rate": 0.95,
                "abnormal_status_count": 0,
                "point_count": 4,
                "qc_fail_count": 0,
                "history": [
                    {"mean_co2_ratio_f": 1.00, "mean_h2o_ratio_f": 0.70, "mean_rmse": 0.02},
                    {"mean_co2_ratio_f": 1.01, "mean_h2o_ratio_f": 0.71, "mean_rmse": 0.02},
                ],
            },
            {
                "analyzer_label": "ga02",
                "frame_count": 10,
                "run_count": 2,
                "usable_rate": 0.40,
                "abnormal_status_count": 4,
                "point_count": 4,
                "qc_fail_count": 2,
                "history": [
                    {"mean_co2_ratio_f": 1.00, "mean_h2o_ratio_f": 0.70, "mean_rmse": 0.02},
                    {"mean_co2_ratio_f": 1.05, "mean_h2o_ratio_f": 0.76, "mean_rmse": 0.06},
                ],
            },
        ]
    }

    report = build_instrument_health(features)
    assert report["analyzer_count"] == 2
    assert report["analyzers"][0]["health_score"] > report["analyzers"][1]["health_score"]
    assert report["average_health_score"] is not None
