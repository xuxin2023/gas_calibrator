from gas_calibrator.v2.analytics.marts.control_charts import build_control_charts


def test_control_charts_happy_path() -> None:
    features = {
        "analyzers": [
            {
                "analyzer_id": "ga01",
                "analyzer_serial": "SN01",
                "history": [
                    {"run_id": "run_a", "start_time": "2026-03-20T00:00:00", "mean_co2_ppm": 400.0, "mean_h2o_mmol": 0.2, "mean_rmse": 0.02},
                    {"run_id": "run_b", "start_time": "2026-03-21T00:00:00", "mean_co2_ppm": 402.0, "mean_h2o_mmol": 0.3, "mean_rmse": 0.03},
                    {"run_id": "run_c", "start_time": "2026-03-22T00:00:00", "mean_co2_ppm": 401.0, "mean_h2o_mmol": 0.4, "mean_rmse": 0.025},
                ],
            }
        ]
    }

    report = build_control_charts(features)
    assert report["analyzer_count"] == 1
    chart = report["charts"][0]["charts"][0]
    assert chart["metric"] == "mean_co2_ppm"
    assert chart["sample_count"] == 3
    assert chart["status"] == "ok"
    assert chart["center_line"] == 401.0
