from gas_calibrator.v2.analytics.measurement.marts.signal_anomaly import build_signal_anomaly


def test_signal_anomaly_happy_path() -> None:
    features = {
        "frame_features": [
            {
                "run_id": "run_alpha",
                "run_uuid": "uuid-alpha",
                "point_id": "point-1",
                "point_sequence": 1,
                "analyzer_label": "ga01",
                "sample_index": 1,
                "sample_ts": "2026-03-20T00:00:00+00:00",
                "frame_has_data": True,
                "frame_usable": True,
                "analyzer_status": "",
                "co2_ratio_f": 1.00,
                "co2_ratio_raw": 1.00,
                "h2o_ratio_f": 0.70,
                "h2o_ratio_raw": 0.70,
                "pressure_hpa": 1000.0,
                "pressure_kpa": 100.0,
                "ref_signal": 3500.0,
                "co2_signal": 4500.0,
                "h2o_signal": 2500.0,
            },
            {
                "run_id": "run_alpha",
                "run_uuid": "uuid-alpha",
                "point_id": "point-1",
                "point_sequence": 1,
                "analyzer_label": "ga01",
                "sample_index": 2,
                "sample_ts": "2026-03-20T00:00:05+00:00",
                "frame_has_data": False,
                "frame_usable": False,
                "analyzer_status": "fault",
                "co2_ratio_f": 1.05,
                "co2_ratio_raw": 1.00,
                "h2o_ratio_f": 0.74,
                "h2o_ratio_raw": 0.70,
                "pressure_hpa": 1125.0,
                "pressure_kpa": 108.0,
                "ref_signal": 4500.0,
                "co2_signal": 5600.0,
                "h2o_signal": 3300.0,
            },
        ]
    }

    report = build_signal_anomaly(features)
    assert report["anomaly_count"] >= 1
    assert report["category_counts"]["missing_frame"] >= 1
    assert report["category_counts"]["analyzer_status"] >= 1
