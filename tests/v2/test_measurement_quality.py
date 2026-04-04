from gas_calibrator.v2.analytics.measurement.marts.measurement_quality import build_measurement_quality


def test_measurement_quality_happy_path() -> None:
    features = {
        "run_features": [
            {
                "run_id": "run_alpha",
                "status": "completed",
                "frame_count": 3,
                "usable_frame_count": 2,
                "frame_has_data_count": 3,
                "frame_usable_rate": 2 / 3,
                "frame_has_data_rate": 1.0,
                "mean_coverage_ratio": 0.8,
            }
        ],
        "frame_features": [
            {"frame_usable": True, "frame_has_data": True},
            {"frame_usable": True, "frame_has_data": True},
            {"frame_usable": False, "frame_has_data": True},
        ],
        "analyzer_features": [
            {
                "analyzer_label": "ga01",
                "run_count": 1,
                "frame_count": 2,
                "usable_frame_count": 2,
                "missing_frame_count": 0,
                "usable_rate": 1.0,
                "has_data_rate": 1.0,
                "mean_coverage_ratio": 1.0,
            },
            {
                "analyzer_label": "ga02",
                "run_count": 1,
                "frame_count": 1,
                "usable_frame_count": 0,
                "missing_frame_count": 0,
                "usable_rate": 0.0,
                "has_data_rate": 1.0,
                "mean_coverage_ratio": 0.5,
            },
        ],
    }

    report = build_measurement_quality(features)
    assert report["frame_count"] == 3
    assert report["usable_rate"] == 2 / 3
    assert report["analyzer_count"] == 2
    assert report["overall_quality_score"] > 0
    assert report["analyzer_breakdown"][0]["analyzer_label"] == "ga01"
