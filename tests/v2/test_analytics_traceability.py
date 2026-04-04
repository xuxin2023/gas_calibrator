from gas_calibrator.v2.analytics.marts.traceability import build_traceability


def test_traceability_happy_path() -> None:
    features = {
        "runs": [
            {
                "run_id": "run_alpha",
                "run_uuid": "uuid-alpha",
                "status": "completed",
                "software_version": "2.1.0",
                "raw_manifest_present": True,
                "raw_source_points_file": "points_alpha.xlsx",
                "manifest_schema_version": "1.0",
                "sample_count": 5,
                "total_points": 2,
                "enrich_qc_status": "loaded",
                "enrich_fit_imported_results": 1,
                "postprocess_summary_status": "loaded",
                "ai_summary_status": "completed",
                "coefficient_report_status": "completed",
                "skipped_artifacts": [],
            },
            {
                "run_id": "run_beta",
                "run_uuid": "uuid-beta",
                "status": "failed",
                "software_version": "2.1.0",
                "raw_manifest_present": False,
                "raw_source_points_file": None,
                "manifest_schema_version": None,
                "sample_count": 0,
                "total_points": 1,
                "enrich_qc_status": "missing",
                "enrich_fit_imported_results": 0,
                "postprocess_summary_status": None,
                "ai_summary_status": None,
                "coefficient_report_status": None,
                "skipped_artifacts": ["qc_report.json"],
            },
        ]
    }

    report = build_traceability(features)
    assert report["run_count"] == 2
    assert report["runs"][0]["enrich_complete"] is True
    assert "manifest" in report["runs"][1]["missing_sections"]
    assert "qc" in report["runs"][1]["missing_sections"]
