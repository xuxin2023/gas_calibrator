from gas_calibrator.v2.analytics.marts.point_kpis import build_point_kpis


def test_point_kpis_happy_path() -> None:
    features = {
        "points": [
            {"route_type": "co2", "status": "completed", "total_time_s": 20.0, "stability_time_s": 8.0, "retry_count": 0},
            {"route_type": "co2", "status": "failed", "total_time_s": 40.0, "stability_time_s": 12.0, "retry_count": 1},
            {"route_type": "h2o", "status": "completed", "total_time_s": 30.0, "stability_time_s": 10.0, "retry_count": 0},
        ]
    }

    report = build_point_kpis(features)
    assert report["point_count"] == 3
    assert report["completed_point_count"] == 2
    assert report["point_success_rate"] == 2 / 3
    assert report["average_total_time_s"] == 30.0
    assert len(report["route_breakdown"]) == 2
    assert report["route_breakdown"][0]["route_type"] == "co2"
