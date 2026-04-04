import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")
from gas_calibrator.v2.analytics.measurement import MeasurementAnalyticsService, export_csv
from gas_calibrator.v2.storage.database import DatabaseManager, StorageSettings, stable_uuid
from gas_calibrator.v2.storage.models import (
    FitResultRecord,
    MeasurementFrameRecord,
    PointRecord,
    QCResultRecord,
    RunRecord,
)


def _database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager(StorageSettings(backend="sqlite", database=str(tmp_path / "measurement_analytics.sqlite")))
    database.initialize()
    return database


def _seed_database(database: DatabaseManager) -> None:
    started = datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc)
    run_alpha = stable_uuid("run", "run_alpha")
    run_beta = stable_uuid("run", "run_beta")
    point_alpha_1 = stable_uuid("point", run_alpha, 1)
    point_alpha_2 = stable_uuid("point", run_alpha, 2)
    point_beta_1 = stable_uuid("point", run_beta, 1)

    with database.session_scope() as session:
        session.add_all(
            [
                RunRecord(
                    id=run_alpha,
                    start_time=started,
                    end_time=started.replace(minute=12),
                    status="completed",
                    software_version="2.3.0",
                    total_points=2,
                    successful_points=1,
                    failed_points=1,
                    warnings=1,
                    errors=0,
                    notes=json.dumps(
                        {
                            "source_run_id": "run_alpha",
                            "raw": {
                                "manifest_present": True,
                                "source_points_file": "points_alpha.xlsx",
                                "manifest_schema_version": "1.0",
                            },
                            "enrich": {
                                "ai_summary_metadata": {"status": "completed"},
                                "postprocess_summary_metadata": {"status": "loaded"},
                                "coefficient_metadata": {"report_status": "completed"},
                                "skipped_artifacts": [],
                            },
                        }
                    ),
                ),
                RunRecord(
                    id=run_beta,
                    start_time=started.replace(hour=1),
                    end_time=started.replace(hour=1, minute=9),
                    status="failed",
                    software_version="2.3.0",
                    total_points=1,
                    successful_points=0,
                    failed_points=1,
                    warnings=2,
                    errors=1,
                    notes=json.dumps(
                        {
                            "source_run_id": "run_beta",
                            "raw": {"manifest_present": True},
                            "enrich": {
                                "ai_summary_metadata": {"status": "fallback"},
                                "postprocess_summary_metadata": {"status": "loaded"},
                                "coefficient_metadata": {"report_status": "completed"},
                                "skipped_artifacts": ["ai_anomaly_note.md"],
                            },
                        }
                    ),
                ),
                PointRecord(
                    id=point_alpha_1,
                    run_id=run_alpha,
                    sequence=1,
                    route_type="co2",
                    status="completed",
                    total_time_s=30.0,
                    stability_time_s=10.0,
                ),
                PointRecord(
                    id=point_alpha_2,
                    run_id=run_alpha,
                    sequence=2,
                    route_type="h2o",
                    status="failed",
                    total_time_s=95.0,
                    stability_time_s=70.0,
                ),
                PointRecord(
                    id=point_beta_1,
                    run_id=run_beta,
                    sequence=1,
                    route_type="co2",
                    status="failed",
                    total_time_s=180.0,
                    stability_time_s=90.0,
                ),
                MeasurementFrameRecord(
                    id=stable_uuid("measurement_frame", run_alpha, 1, "ga01", 1, "a"),
                    run_id=run_alpha,
                    point_id=point_alpha_1,
                    sample_index=1,
                    sample_ts=started.replace(minute=1),
                    analyzer_label="ga01",
                    analyzer_id="010",
                    frame_has_data=True,
                    frame_usable=True,
                    mode="2",
                    mode2_field_count=16,
                    co2_ppm=400.0,
                    h2o_mmol=0.70,
                    co2_ratio_f=1.000,
                    h2o_ratio_f=0.700,
                    co2_ratio_raw=0.999,
                    h2o_ratio_raw=0.699,
                    ref_signal=3500.0,
                    co2_signal=4500.0,
                    h2o_signal=2500.0,
                    chamber_temp_c=25.0,
                    case_temp_c=26.0,
                    pressure_kpa=100.0,
                    raw_payload={"ga01_raw": "frame_a"},
                    context_payload={
                        "pressure_hpa": 1000.0,
                        "dewpoint_c": 4.5,
                        "chamber_temp_c": 25.0,
                        "chamber_rh_pct": 45.0,
                        "analyzer_expected_count": 2,
                        "analyzer_with_frame_count": 2,
                        "analyzer_usable_count": 2,
                        "analyzer_coverage_text": "2/2",
                        "analyzer_integrity": "complete",
                        "stability_time_s": 10.0,
                        "total_time_s": 30.0,
                    },
                ),
                MeasurementFrameRecord(
                    id=stable_uuid("measurement_frame", run_alpha, 1, "ga02", 1, "b"),
                    run_id=run_alpha,
                    point_id=point_alpha_1,
                    sample_index=1,
                    sample_ts=started.replace(minute=1),
                    analyzer_label="ga02",
                    analyzer_id="028",
                    frame_has_data=True,
                    frame_usable=False,
                    analyzer_status="warning",
                    mode="2",
                    mode2_field_count=16,
                    co2_ppm=398.0,
                    h2o_mmol=0.69,
                    co2_ratio_f=0.998,
                    h2o_ratio_f=0.698,
                    co2_ratio_raw=0.998,
                    h2o_ratio_raw=0.698,
                    ref_signal=3480.0,
                    co2_signal=4480.0,
                    h2o_signal=2480.0,
                    chamber_temp_c=25.0,
                    case_temp_c=26.0,
                    pressure_kpa=100.0,
                    raw_payload={"ga02_raw": "frame_b"},
                    context_payload={
                        "pressure_hpa": 1000.0,
                        "analyzer_expected_count": 2,
                        "analyzer_with_frame_count": 2,
                        "analyzer_usable_count": 1,
                        "analyzer_unusable_labels": "ga02",
                        "analyzer_coverage_text": "1/2",
                        "analyzer_integrity": "partial",
                    },
                ),
                MeasurementFrameRecord(
                    id=stable_uuid("measurement_frame", run_alpha, 2, "ga01", 1, "c"),
                    run_id=run_alpha,
                    point_id=point_alpha_2,
                    sample_index=1,
                    sample_ts=started.replace(minute=5),
                    analyzer_label="ga01",
                    analyzer_id="010",
                    frame_has_data=True,
                    frame_usable=True,
                    mode="2",
                    mode2_field_count=16,
                    co2_ppm=401.0,
                    h2o_mmol=0.75,
                    co2_ratio_f=1.002,
                    h2o_ratio_f=0.705,
                    co2_ratio_raw=1.001,
                    h2o_ratio_raw=0.704,
                    ref_signal=3520.0,
                    co2_signal=4520.0,
                    h2o_signal=2520.0,
                    chamber_temp_c=25.2,
                    case_temp_c=26.1,
                    pressure_kpa=100.0,
                    raw_payload={"ga01_raw": "frame_c"},
                    context_payload={
                        "pressure_hpa": 1002.0,
                        "dewpoint_c": 8.0,
                        "chamber_temp_c": 25.1,
                        "chamber_rh_pct": 75.0,
                        "hgen_Uw": 52.7,
                        "analyzer_expected_count": 2,
                        "analyzer_with_frame_count": 2,
                        "analyzer_usable_count": 2,
                        "analyzer_coverage_text": "2/2",
                        "analyzer_integrity": "complete",
                        "stability_time_s": 70.0,
                        "total_time_s": 95.0,
                    },
                ),
                MeasurementFrameRecord(
                    id=stable_uuid("measurement_frame", run_beta, 1, "ga01", 1, "d"),
                    run_id=run_beta,
                    point_id=point_beta_1,
                    sample_index=1,
                    sample_ts=started.replace(hour=1, minute=2),
                    analyzer_label="ga01",
                    analyzer_id="010",
                    frame_has_data=True,
                    frame_usable=True,
                    mode="2",
                    mode2_field_count=16,
                    co2_ppm=430.0,
                    h2o_mmol=0.90,
                    co2_ratio_f=1.030,
                    h2o_ratio_f=0.730,
                    co2_ratio_raw=1.010,
                    h2o_ratio_raw=0.710,
                    ref_signal=3900.0,
                    co2_signal=5000.0,
                    h2o_signal=2900.0,
                    chamber_temp_c=27.0,
                    case_temp_c=28.0,
                    pressure_kpa=108.0,
                    raw_payload={"ga01_raw": "frame_d"},
                    context_payload={
                        "pressure_hpa": 1080.0,
                        "dewpoint_c": 10.0,
                        "chamber_temp_c": 28.0,
                        "chamber_rh_pct": 55.0,
                        "analyzer_expected_count": 2,
                        "analyzer_with_frame_count": 2,
                        "analyzer_usable_count": 2,
                        "analyzer_coverage_text": "2/2",
                        "analyzer_integrity": "complete",
                        "stability_time_s": 90.0,
                        "total_time_s": 180.0,
                    },
                ),
                MeasurementFrameRecord(
                    id=stable_uuid("measurement_frame", run_beta, 1, "ga01", 2, "e"),
                    run_id=run_beta,
                    point_id=point_beta_1,
                    sample_index=2,
                    sample_ts=started.replace(hour=1, minute=2, second=5),
                    analyzer_label="ga01",
                    analyzer_id="010",
                    frame_has_data=False,
                    frame_usable=False,
                    analyzer_status="fault",
                    mode="2",
                    mode2_field_count=16,
                    co2_ppm=435.0,
                    h2o_mmol=0.95,
                    co2_ratio_f=1.050,
                    h2o_ratio_f=0.740,
                    co2_ratio_raw=1.000,
                    h2o_ratio_raw=0.700,
                    ref_signal=4500.0,
                    co2_signal=5600.0,
                    h2o_signal=3300.0,
                    chamber_temp_c=27.5,
                    case_temp_c=28.5,
                    pressure_kpa=108.0,
                    raw_payload={"ga01_raw": "frame_e"},
                    context_payload={
                        "pressure_hpa": 1125.0,
                        "dewpoint_c": 12.0,
                        "chamber_temp_c": 36.0,
                        "chamber_rh_pct": 58.0,
                        "hgen_Uw": 60.0,
                        "analyzer_expected_count": 2,
                        "analyzer_with_frame_count": 1,
                        "analyzer_usable_count": 1,
                        "analyzer_coverage_text": "1/2",
                        "analyzer_integrity": "partial",
                        "analyzer_missing_labels": "ga02",
                        "stability_time_s": 95.0,
                        "total_time_s": 210.0,
                    },
                ),
                QCResultRecord(
                    id=stable_uuid("qc", point_alpha_2, "humidity_stability"),
                    point_id=point_alpha_2,
                    rule_name="humidity_stability",
                    passed=False,
                    message="humidity stability timeout",
                ),
                QCResultRecord(
                    id=stable_uuid("qc", point_beta_1, "pressure_leak"),
                    point_id=point_beta_1,
                    rule_name="pressure_leak",
                    passed=False,
                    message="pressure leak detected",
                ),
                FitResultRecord(
                    id=stable_uuid("fit", run_alpha, "ga01", "linear"),
                    run_id=run_alpha,
                    analyzer_id="ga01",
                    algorithm="linear",
                    coefficients={"slope": 1.0},
                    rmse=0.02,
                    r_squared=0.998,
                    n_points=2,
                ),
                FitResultRecord(
                    id=stable_uuid("fit", run_beta, "ga01", "linear"),
                    run_id=run_beta,
                    analyzer_id="ga01",
                    algorithm="linear",
                    coefficients={"slope": 1.1},
                    rmse=0.05,
                    r_squared=0.990,
                    n_points=1,
                ),
            ]
        )


def test_measurement_analytics_service_builds_reports_and_exports(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _seed_database(database)
    service = MeasurementAnalyticsService(database)

    features = service.build_features(analyzer_id="ga01")
    assert features["schema_version"] == "1.0"
    assert len(features["run_features"]) == 2
    assert len(features["frame_features"]) == 4
    assert len(features["analyzer_features"]) == 1
    assert features["analyzer_features"][0]["analyzer_label"] == "ga01"

    quality_report = service.run_report("measurement_quality")
    assert quality_report["schema_version"] == "1.0"
    assert quality_report["report_name"] == "measurement_quality"
    assert quality_report["data"]["frame_count"] == 5
    assert quality_report["data"]["analyzer_count"] == 2

    all_reports = service.run_all()
    assert {
        "measurement_quality",
        "measurement_drift",
        "signal_anomaly",
        "context_attribution",
        "instrument_health",
    } <= set(all_reports)
    assert all_reports["signal_anomaly"]["data"]["anomaly_count"] >= 1

    report_path = service.export_report("instrument_health", tmp_path / "instrument_health.json")
    assert report_path.exists()

    csv_path = export_csv(
        tmp_path / "measurement_quality.csv",
        all_reports["measurement_quality"]["data"]["analyzer_breakdown"],
    )
    assert csv_path.exists()
    assert "analyzer_label" in csv_path.read_text(encoding="utf-8")
