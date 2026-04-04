from gas_calibrator.v2.analytics.marts.analyzer_health import build_analyzer_health


def test_analyzer_health_happy_path() -> None:
    features = {
        "analyzers": [
            {
                "analyzer_id": "ga_bad",
                "analyzer_serial": "SN02",
                "run_count": 2,
                "sample_count": 10,
                "fit_result_count": 1,
                "mean_rmse": 0.08,
                "mean_r_squared": 0.96,
                "qc_fail_count": 3,
                "alarm_count": 4,
            },
            {
                "analyzer_id": "ga_good",
                "analyzer_serial": "SN01",
                "run_count": 3,
                "sample_count": 30,
                "fit_result_count": 3,
                "mean_rmse": 0.02,
                "mean_r_squared": 0.998,
                "qc_fail_count": 1,
                "alarm_count": 0,
            },
        ]
    }

    report = build_analyzer_health(features)
    assert report["analyzer_count"] == 2
    assert report["analyzers"][0]["analyzer_id"] == "ga_bad"
    assert report["analyzers"][0]["health_score"] < report["analyzers"][1]["health_score"]
    assert report["analyzers"][1]["status"] == "healthy"
