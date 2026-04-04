from gas_calibrator.v2.analytics.marts.run_kpis import build_run_kpis


def test_run_kpis_happy_path() -> None:
    features = {
        "runs": [
            {
                "status": "completed",
                "duration_s": 120.0,
                "total_points": 3,
                "warnings": 1,
                "errors": 0,
                "raw_manifest_present": True,
                "enrich_qc_status": "loaded",
                "enrich_fit_imported_results": 2,
                "ai_summary_status": "completed",
                "postprocess_summary_status": "loaded",
            },
            {
                "status": "failed",
                "duration_s": 180.0,
                "total_points": 2,
                "warnings": 2,
                "errors": 1,
                "raw_manifest_present": False,
                "enrich_qc_status": "missing",
                "enrich_fit_imported_results": 0,
                "ai_summary_status": None,
                "postprocess_summary_status": None,
            },
        ]
    }

    report = build_run_kpis(features)
    assert report["run_count"] == 2
    assert report["completed_run_count"] == 1
    assert report["run_success_rate"] == 0.5
    assert report["average_duration_s"] == 150.0
    assert report["manifest_coverage"] == 0.5
    assert report["qc_enrich_coverage"] == 0.5
    assert report["fit_enrich_coverage"] == 0.5
