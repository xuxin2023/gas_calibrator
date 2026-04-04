from gas_calibrator.v2.analytics.measurement.marts.context_attribution import build_context_attribution


def test_context_attribution_happy_path() -> None:
    features = {
        "frame_features": [
            {
                "run_id": "run_beta",
                "point_id": "point-2",
                "point_sequence": 2,
                "analyzer_label": "ga01",
                "sample_index": 1,
                "sample_ts": "2026-03-20T00:00:10+00:00",
                "frame_usable": False,
                "analyzer_status": "fault",
                "qc_fail_count": 1,
                "failed_qc_rule_names": ["humidity_stability"],
                "failed_qc_messages": ["humidity stability timeout"],
                "pressure_hpa": 1125.0,
                "pressure_kpa": 108.0,
                "context_chamber_temp_c": 36.0,
                "stability_time_s": 95.0,
                "total_time_s": 210.0,
                "analyzer_expected_count": 2,
                "analyzer_usable_count": 1,
                "analyzer_missing_labels": "ga02",
                "hgen_Uw": 60.0,
            }
        ]
    }

    report = build_context_attribution(features)
    assert report["affected_frame_count"] == 1
    assert report["category_counts"]["coverage_context"] >= 1
    assert report["category_counts"]["pressure_context"] >= 1
    assert report["category_counts"]["humidity_context"] >= 1
