import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")
from gas_calibrator.v2.analytics import AnalyticsService, export_csv
from gas_calibrator.v2.storage.database import DatabaseManager, StorageSettings, stable_uuid
from gas_calibrator.v2.storage.models import (
    AlarmIncidentRecord,
    DeviceEventRecord,
    FitResultRecord,
    PointRecord,
    QCResultRecord,
    RunRecord,
    SampleRecord,
)


def _database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager(StorageSettings(backend="sqlite", database=str(tmp_path / "analytics.sqlite")))
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
                    end_time=started.replace(minute=10),
                    status="completed",
                    software_version="2.1.0",
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
                                "qc": {"imported_results": 2},
                                "fit": {"imported_results": 1},
                                "coefficient_metadata": {"report_status": "completed"},
                                "ai_summary_metadata": {"status": "completed"},
                                "postprocess_summary_metadata": {"status": "loaded"},
                                "skipped_artifacts": [],
                            },
                        }
                    ),
                ),
                RunRecord(
                    id=run_beta,
                    start_time=started.replace(hour=1),
                    end_time=started.replace(hour=1, minute=8),
                    status="failed",
                    software_version="2.1.0",
                    total_points=1,
                    successful_points=0,
                    failed_points=1,
                    warnings=2,
                    errors=1,
                    notes=json.dumps(
                        {
                            "source_run_id": "run_beta",
                            "raw": {
                                "manifest_present": False,
                            },
                            "enrich": {
                                "qc": {"imported_results": 1},
                                "fit": {"imported_results": 1},
                                "coefficient_metadata": {"report_status": "completed"},
                                "ai_summary_metadata": {"status": "fallback"},
                                "postprocess_summary_metadata": {"status": "loaded"},
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
                    total_time_s=32.0,
                    stability_time_s=10.0,
                    retry_count=0,
                    temperature_c=25.0,
                    pressure_hpa=1000.0,
                    co2_target_ppm=400.0,
                ),
                PointRecord(
                    id=point_alpha_2,
                    run_id=run_alpha,
                    sequence=2,
                    route_type="h2o",
                    status="failed",
                    total_time_s=40.0,
                    stability_time_s=16.0,
                    retry_count=1,
                    temperature_c=25.0,
                    pressure_hpa=1000.0,
                ),
                PointRecord(
                    id=point_beta_1,
                    run_id=run_beta,
                    sequence=1,
                    route_type="co2",
                    status="failed",
                    total_time_s=28.0,
                    stability_time_s=9.0,
                    retry_count=0,
                    temperature_c=26.0,
                    pressure_hpa=995.0,
                    co2_target_ppm=420.0,
                ),
                SampleRecord(
                    id=stable_uuid("sample", run_alpha, 1, "ga01", "SN01", 1),
                    point_id=point_alpha_1,
                    analyzer_id="ga01",
                    analyzer_serial="SN01",
                    sample_index=1,
                    timestamp=started.replace(minute=1),
                    co2_ppm=401.0,
                    h2o_mmol=0.2,
                    co2_ratio_f=1.001,
                    h2o_ratio_f=0.201,
                ),
                SampleRecord(
                    id=stable_uuid("sample", run_alpha, 1, "ga01", "SN01", 2),
                    point_id=point_alpha_1,
                    analyzer_id="ga01",
                    analyzer_serial="SN01",
                    sample_index=2,
                    timestamp=started.replace(minute=1, second=5),
                    co2_ppm=402.0,
                    h2o_mmol=0.21,
                    co2_ratio_f=1.002,
                    h2o_ratio_f=0.202,
                ),
                SampleRecord(
                    id=stable_uuid("sample", run_alpha, 2, "ga02", "SN02", 1),
                    point_id=point_alpha_2,
                    analyzer_id="ga02",
                    analyzer_serial="SN02",
                    sample_index=1,
                    timestamp=started.replace(minute=5),
                    co2_ppm=2.0,
                    h2o_mmol=9.2,
                    co2_ratio_f=1.01,
                    h2o_ratio_f=0.41,
                ),
                SampleRecord(
                    id=stable_uuid("sample", run_beta, 1, "ga01", "SN01", 1),
                    point_id=point_beta_1,
                    analyzer_id="ga01",
                    analyzer_serial="SN01",
                    sample_index=1,
                    timestamp=started.replace(hour=1, minute=1),
                    co2_ppm=405.0,
                    h2o_mmol=0.3,
                    co2_ratio_f=1.005,
                    h2o_ratio_f=0.205,
                ),
                QCResultRecord(
                    id=stable_uuid("qc", point_alpha_1, "overall_quality"),
                    point_id=point_alpha_1,
                    rule_name="overall_quality",
                    passed=True,
                    value=0.98,
                    message="keep",
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
                    r_squared=0.994,
                    n_points=1,
                ),
                DeviceEventRecord(
                    id=stable_uuid("device_event", run_alpha, "humidifier", "tx", started, "SET"),
                    run_id=run_alpha,
                    device_name="humidifier",
                    event_type="tx",
                    event_data={"data": "SET"},
                    timestamp=started.replace(minute=2),
                ),
                DeviceEventRecord(
                    id=stable_uuid("device_event", run_beta, "pressure_controller", "tx", started, "READ"),
                    run_id=run_beta,
                    device_name="pressure_controller",
                    event_type="tx",
                    event_data={"data": "READ"},
                    timestamp=started.replace(hour=1, minute=2),
                ),
                AlarmIncidentRecord(
                    id=stable_uuid("alarm", run_alpha, "warning", "humidity unstable", started),
                    run_id=run_alpha,
                    severity="warning",
                    category="humidity_generator",
                    message="humidity unstable",
                    details={},
                    timestamp=started.replace(minute=5),
                    resolved=False,
                ),
                AlarmIncidentRecord(
                    id=stable_uuid("alarm", run_beta, "error", "pressure leak", started),
                    run_id=run_beta,
                    severity="error",
                    category="pressure_leak",
                    message="pressure leak",
                    details={},
                    timestamp=started.replace(hour=1, minute=4),
                    resolved=False,
                ),
            ]
        )


def test_analytics_service_builds_reports_and_exports(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _seed_database(database)
    service = AnalyticsService(database)

    features = service.build_features(analyzer_id="ga01")
    assert features["schema_version"] == "1.0"
    assert len(features["runs"]) == 2
    assert len(features["analyzers"]) == 1
    assert features["analyzers"][0]["analyzer_id"] == "ga01"

    run_report = service.run_report("run_kpis")
    assert run_report["schema_version"] == "1.0"
    assert run_report["report_name"] == "run_kpis"
    assert run_report["data"]["run_count"] == 2
    assert run_report["data"]["manifest_coverage"] == 0.5

    all_reports = service.run_all()
    assert {"run_kpis", "traceability", "fault_attribution"} <= set(all_reports)
    assert all_reports["traceability"]["data"]["run_count"] == 2

    report_path = service.export_report("run_kpis", tmp_path / "run_kpis.json")
    assert report_path.exists()

    csv_path = export_csv(
        tmp_path / "point_kpis.csv",
        all_reports["point_kpis"]["data"]["route_breakdown"],
    )
    assert csv_path.exists()
    assert "route_type" in csv_path.read_text(encoding="utf-8")
