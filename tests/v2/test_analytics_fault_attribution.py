from gas_calibrator.v2.analytics.marts.fault_attribution import build_fault_attribution


def test_fault_attribution_happy_path() -> None:
    features = {
        "runs": [
            {
                "run_id": "run_alpha",
                "status": "failed",
                "alarm_categories": {"humidity_generator": 1},
            },
            {
                "run_id": "run_beta",
                "status": "failed",
                "alarm_categories": {"pressure_leak": 2},
            },
        ],
        "points": [
            {
                "run_id": "run_alpha",
                "qc_fail_count": 1,
                "failed_qc_rule_names": ["humidity_stability"],
                "failed_qc_messages": ["humidity stability timeout"],
            },
            {
                "run_id": "run_beta",
                "qc_fail_count": 1,
                "failed_qc_rule_names": ["pressure_leak"],
                "failed_qc_messages": ["pressure leak detected"],
            },
        ],
    }

    report = build_fault_attribution(features)
    assert report["run_count"] == 2
    assert report["overall_category_counts"]["humidity_path"] >= 1
    assert report["overall_category_counts"]["pressure_path"] >= 1
    assert report["runs"][0]["dominant_category"] == "humidity_path"
