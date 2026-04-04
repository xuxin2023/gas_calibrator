from datetime import datetime, timezone
import json
from pathlib import Path

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.models import CalibrationPhase, CalibrationPoint, CalibrationStatus, SamplingResult
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.session import RunSession


def _sample() -> SamplingResult:
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    return SamplingResult(
        point=point,
        analyzer_id="ga01",
        timestamp=datetime(2026, 3, 18, 8, 0, 0, tzinfo=timezone.utc),
        h2o_signal=10.0,
        co2_signal=400.0,
        temperature_c=25.0,
        pressure_hpa=1000.0,
        pressure_gauge_hpa=998.5,
        pressure_reference_status="healthy",
        thermometer_temp_c=24.4,
        thermometer_reference_status="healthy",
        dew_point_c=5.0,
    )


def test_result_store_exports_artifacts(tmp_path: Path) -> None:
    store = ResultStore(tmp_path, "run_test")
    sample = _sample()
    store.save_sample(sample)
    store.save_point_summary(
        sample.point,
        {
            "point_phase": "co2",
            "point_tag": "co2_1",
            "sample_count": 1,
            "valid": True,
            "recommendation": "use",
            "reason": "passed",
            "dewpoint_gate_result": "pass",
            "dewpoint_rebound_detected": True,
            "rebound_rise_c": 1.25,
            "rebound_note": "late rebound observed",
        },
    )

    csv_path = store.export_csv()
    excel_path = store.export_excel()
    json_path = store.export_json()
    readable_path = store.export_points_readable()

    assert csv_path.exists()
    assert excel_path.exists()
    assert json_path.exists()
    assert readable_path is not None
    assert readable_path.exists()
    readable_text = readable_path.read_text(encoding="utf-8")
    assert "AnalyzerCoverage" in readable_text
    assert "reference_quality" in readable_text
    assert "dewpoint_gate_result" in readable_text
    assert "dewpoint_rebound_detected" in readable_text
    assert "rebound_rise_c" in readable_text
    assert "late rebound observed" in readable_text
    assert len(store.get_samples()) == 1


def test_result_store_writes_run_summary(tmp_path: Path) -> None:
    store = ResultStore(tmp_path, "run_test")
    config = AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})
    session = RunSession(config)
    session.start()
    session.add_warning("warn-1")
    session.end("done")
    status = CalibrationStatus(
        phase=CalibrationPhase.COMPLETED,
        total_points=3,
        completed_points=2,
        progress=2 / 3,
        message="done",
        elapsed_s=12.5,
    )

    startup_pressure_precheck = {
        "passed": True,
        "route": "co2",
        "warning_count": 0,
        "error_count": 0,
        "details": {"source": "pressure_meter", "samples": 3},
    }

    store.save_run_summary(session, status, startup_pressure_precheck=startup_pressure_precheck)

    assert store.data_writer.summary_path.exists()
    payload = json.loads(store.data_writer.summary_path.read_text(encoding="utf-8"))
    assert payload["points_total"] == 3
    assert payload["points_completed"] == 2
    assert payload["progress"] == 2 / 3
    assert payload["warnings"] == 1
    assert payload["errors"] == 0
    assert payload["started_at"] is not None
    assert payload["ended_at"] is not None
    assert payload["status"]["phase"] == "completed"
    assert payload["status"]["total_points"] == 3
    assert payload["status"]["completed_points"] == 2
    assert payload["status"]["progress"] == 2 / 3
    assert payload["startup_pressure_precheck"] == startup_pressure_precheck
    assert payload["stats"]["startup_pressure_precheck"] == startup_pressure_precheck


def test_result_store_summarizes_artifact_roles(tmp_path: Path) -> None:
    store = ResultStore(tmp_path, "run_test")
    config = AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})
    session = RunSession(config)
    session.start()
    session.end("done")

    store.save_run_summary(
        session,
        export_statuses={
            "run_summary": {"role": "execution_summary", "status": "ok", "path": "summary.json", "error": ""},
            "points_readable": {"role": "execution_summary", "status": "ok", "path": "points_readable.csv", "error": ""},
            "samples_csv": {"role": "execution_rows", "status": "ok", "path": "samples.csv", "error": ""},
            "qc_report": {"role": "diagnostic_analysis", "status": "error", "path": "", "error": "boom"},
        },
    )

    payload = json.loads(store.data_writer.summary_path.read_text(encoding="utf-8"))
    role_summary = payload["stats"]["artifact_role_summary"]
    assert role_summary["execution_summary"]["count"] == 2
    assert role_summary["execution_rows"]["count"] == 1
    assert role_summary["diagnostic_analysis"]["status_counts"]["error"] == 1


def test_result_store_writes_manifest(tmp_path: Path) -> None:
    store = ResultStore(tmp_path, "run_test")
    config = AppConfig.from_dict(
        {
            "devices": {
                "pressure_controller": {"port": "COM1", "enabled": True},
                "gas_analyzers": [{"port": "COM3", "enabled": True}],
            },
            "paths": {"output_dir": str(tmp_path), "points_excel": "points.xlsx"},
            "storage": {"password": "top-secret"},
            "ai": {"api_key": "secret-key"},
        }
    )
    session = RunSession(config)
    session.run_id = "run_test"

    startup_pressure_precheck = {
        "passed": False,
        "route": "co2",
        "warning_count": 0,
        "error_count": 1,
        "details": {"error": "hold drift too large"},
    }

    manifest_path = store.save_run_manifest(
        session,
        source_points_file="runtime_points.xlsx",
        startup_pressure_precheck=startup_pressure_precheck,
    )

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_path.exists()
    assert manifest_path.name == "manifest.json"
    assert payload["run_id"] == "run_test"
    assert payload["source_points_file"] == "runtime_points.xlsx"
    assert payload["config_snapshot"]["storage"]["password"] == "***REDACTED***"
    assert payload["config_snapshot"]["ai"]["api_key"] == "***REDACTED***"
    assert payload["startup_pressure_precheck"] == startup_pressure_precheck
    assert "points_readable" in payload["artifacts"]["role_catalog"]["execution_summary"]


def test_result_store_exports_offline_acceptance_and_analytics_artifacts(tmp_path: Path) -> None:
    store = ResultStore(tmp_path, "run_test")
    config = AppConfig.from_dict(
        {
            "devices": {"gas_analyzers": [{"id": "GA01", "enabled": True}]},
            "paths": {"output_dir": str(tmp_path), "points_excel": str(tmp_path / "points.xlsx")},
            "features": {"simulation_mode": True},
            "workflow": {"profile_name": "bench", "profile_version": "2.5"},
        }
    )
    Path(config.paths.points_excel).write_text("points", encoding="utf-8")
    session = RunSession(config)
    sample = _sample()
    store.save_sample(sample)
    store.save_point_summary(
        sample.point,
        {
            "point_phase": "co2",
            "point_tag": "co2_1",
            "sample_count": 1,
            "valid": True,
            "recommendation": "use",
            "reason": "passed",
        },
    )

    payload = store.export_offline_artifacts(
        session,
        source_points_file=config.paths.points_excel,
        output_files=[],
        export_statuses={
            "run_summary": {"role": "execution_summary", "status": "ok", "path": str(store.run_dir / "summary.json"), "error": ""},
            "results_json": {"role": "execution_rows", "status": "ok", "path": str(store.run_dir / "results.json"), "error": ""},
        },
    )

    assert (store.run_dir / "acceptance_plan.json").exists()
    assert (store.run_dir / "analytics_summary.json").exists()
    assert (store.run_dir / "trend_registry.json").exists()
    assert (store.run_dir / "lineage_summary.json").exists()
    assert (store.run_dir / "evidence_registry.json").exists()
    assert (store.run_dir / "coefficient_registry.json").exists()
    assert payload["summary_stats"]["acceptance_plan"]["ready_for_promotion"] is False
    assert payload["summary_stats"]["analytics_summary"]["analyzer_coverage"]["coverage_text"] == "1/1"
