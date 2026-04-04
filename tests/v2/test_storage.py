import csv
import json
from pathlib import Path
import shutil

import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import func, inspect, select

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.calibration_service import CalibrationPhase, CalibrationService
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.stability_checker import StabilityResult
from gas_calibrator.v2.storage import (
    ArtifactImporter,
    CoefficientVersionStore,
    DatabaseManager,
    HistoryQueryService,
    StorageExporter,
    StorageSettings,
)
from gas_calibrator.v2.storage.database import resolve_run_uuid
from gas_calibrator.v2.storage.models import (
    AlarmIncidentRecord,
    CoefficientVersionRecord,
    DeviceEventRecord,
    FitResultRecord,
    MeasurementFrameRecord,
    PointRecord,
    QCResultRecord,
    RunRecord,
    SampleRecord,
    SensorRecord,
)


def _storage_settings(tmp_path: Path) -> StorageSettings:
    return StorageSettings(backend="sqlite", database=str(tmp_path / "storage.sqlite"), auto_import=True)


def _write_run_artifacts(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run_20260320_001000"
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "run_20260320_001000",
                "generated_at": "2026-03-20T00:10:00+00:00",
                "status": {
                    "phase": "completed",
                    "total_points": 2,
                    "completed_points": 1,
                    "elapsed_s": 120.0,
                },
                "stats": {
                    "warning_count": 1,
                    "error_count": 0,
                    "enabled_devices": ["ga01", "ga02", "temperature_chamber"],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "run_id": "run_20260320_001000",
                "software_version": "2.2.0",
                "run_mode": "auto_calibration",
                "route_mode": "h2o_then_co2",
                "profile_name": "bench_profile",
                "profile_version": "2.5",
                "report_family": "v2_product_report_family",
                "report_templates": {
                    "report_family": "v2_product_report_family",
                    "template_keys": [
                        "co2_test_report",
                        "co2_calibration_report",
                        "h2o_test_report",
                        "h2o_calibration_report",
                    ],
                    "per_device_output": True,
                    "templates": [
                        {"key": "co2_test_report"},
                        {"key": "co2_calibration_report"},
                        {"key": "h2o_test_report"},
                        {"key": "h2o_calibration_report"},
                    ],
                },
                "analyzer_setup": {
                    "software_version": "v5_plus",
                    "device_id_assignment_mode": "manual",
                    "start_device_id": "021",
                    "manual_device_ids": ["021", "022"],
                },
                "source_points_file": "points/batch_a.xlsx",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    (run_dir / "points.csv").write_text(
        "\n".join(
            [
                "timestamp,point_index,point_tag,temperature_c,co2_ppm,co2_group,cylinder_nominal_ppm,humidity_pct,pressure_hpa,route,status,stability_time_s,total_time_s",
                "2026-03-20T00:08:00+00:00,1,,25.0,400.0,B,405.0,,1000.0,co2,completed,12.5,30.0",
                "2026-03-20T00:09:30+00:00,2,,25.0,,,,45.0,1000.0,h2o,failed,14.0,34.0",
            ]
        ),
        encoding="utf-8",
    )

    (run_dir / "samples.csv").write_text(
        "\n".join(
            [
                "timestamp,point_index,temperature_c,co2_ppm,co2_group,cylinder_nominal_ppm,humidity_pct,pressure_hpa,route,analyzer_id,analyzer_serial,sample_index,sample_co2_ppm,sample_h2o_mmol,co2_ratio_f,h2o_ratio_f,co2_ratio_raw,h2o_ratio_raw,chamber_temp_c,case_temp_c,dewpoint_c",
                "2026-03-20T00:08:10+00:00,1,25.0,400.0,B,405.0,,1000.0,co2,ga01,SN01,1,401.2,0.2,1.001,0.201,1.001,0.201,25.1,26.0,4.1",
                "2026-03-20T00:08:11+00:00,1,25.0,400.0,B,405.0,,1000.0,co2,ga01,SN01,2,401.0,0.2,1.000,0.202,1.000,0.202,25.1,26.1,4.0",
                "2026-03-20T00:09:40+00:00,2,25.0,,,,45.0,1000.0,h2o,ga02,SN02,1,2.0,9.1,1.010,0.410,1.010,0.410,25.2,26.2,8.0",
            ]
        ),
        encoding="utf-8",
    )

    (run_dir / "qc_report.json").write_text(
        json.dumps(
            {
                "run_id": "run_20260320_001000",
                "point_details": [
                    {
                        "point_index": 1,
                        "quality_score": 0.98,
                        "valid": True,
                        "recommendation": "keep",
                        "reason": "",
                    },
                    {
                        "point_index": 2,
                        "quality_score": 0.45,
                        "valid": False,
                        "recommendation": "exclude",
                        "reason": "usable_sample_count_insufficient,missing_count=2",
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    (run_dir / "results.json").write_text(
        json.dumps(
            {
                "fit_results": [
                    {
                        "analyzer_id": "ga01",
                        "algorithm": "linear",
                        "coefficients": {"slope": 1.1, "intercept": 0.2},
                        "rmse": 0.03,
                        "r_squared": 0.999,
                        "n_points": 2,
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    (run_dir / "io_log.csv").write_text(
        "\n".join(
            [
                "timestamp,device,direction,data",
                "2026-03-20T00:08:05+00:00,ga01,tx,READ",
                "2026-03-20T00:08:05+00:00,ga01,rx,OK",
            ]
        ),
        encoding="utf-8",
    )

    (run_dir / "run.log").write_text(
        "\n".join(
            [
                json.dumps({"timestamp": "2026-03-20T00:07:59+00:00", "level": "INFO", "message": "run start", "context": {}}),
                json.dumps({"timestamp": "2026-03-20T00:09:41+00:00", "level": "WARNING", "message": "humidity unstable", "context": {"point": 2}}),
            ]
        ),
        encoding="utf-8",
    )

    (run_dir / "ai_run_summary.md").write_text(
        "# AI Run Summary\n\n本次运行已完成，建议复核异常点位。\n",
        encoding="utf-8",
    )
    (run_dir / "ai_anomaly_note.md").write_text(
        "# AI Anomaly Note\n\n湿度点位存在稳定性异常。\n",
        encoding="utf-8",
    )
    (run_dir / "calibration_coefficients_postprocess_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-20T00:11:00+00:00",
                "flags": {"import_db": True, "skip_qc": False, "skip_refit": False, "skip_ai": False},
                "manifest": {"status": "completed", "path": str(run_dir / "manifest.json")},
                "database_import": {"status": "completed"},
                "qc": {"status": "completed", "json": str(run_dir / "qc_report.json")},
                "report": {"status": "completed", "path": str(run_dir / "calibration_coefficients.xlsx")},
                "refit": {
                    "status": "completed",
                    "runs": [
                        {"analyzer": "GA01", "gas": "co2", "status": "completed"},
                        {"analyzer": "GA01", "gas": "h2o", "status": "completed"},
                    ],
                },
                "ai": {
                    "status": "completed",
                    "run_summary": {"status": "completed", "path": str(run_dir / "ai_run_summary.md")},
                    "anomaly_note": {"status": "completed", "path": str(run_dir / "ai_anomaly_note.md")},
                },
                "download": {"status": "skipped", "reason": "download disabled"},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    return run_dir


def _repo_runtime_run_dir() -> Path:
    return (
        Path(__file__).resolve().parents[2]
        / "src"
        / "gas_calibrator"
        / "v2"
        / "output"
        / "v1_v2_compare"
        / "v2_collect_0c"
        / "run_20260320_043540"
    )


def _expected_measurement_frame_count(samples_runtime_path: Path) -> int:
    presence_suffixes = (
        "frame_has_data",
        "frame_usable",
        "raw",
        "co2_ppm",
        "h2o_mmol",
        "co2_ratio_f",
        "h2o_ratio_f",
        "co2_ratio_raw",
        "h2o_ratio_raw",
        "ref_signal",
        "co2_signal",
        "h2o_signal",
        "pressure_kpa",
        "chamber_temp_c",
        "case_temp_c",
        "co2_density",
        "h2o_density",
    )
    with samples_runtime_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        labels = sorted(
            {
                field.split("_", 1)[0].lower()
                for field in (reader.fieldnames or [])
                if field.startswith("ga")
                and "_" in field
                and len(field.split("_", 1)[0]) == 4
                and field[2:4].isdigit()
            }
        )
        expected = 0
        for row in reader:
            for label in labels:
                if any(row.get(f"{label}_{suffix}") not in (None, "") for suffix in presence_suffixes):
                    expected += 1
        return expected


def _rewrite_runtime_without_columns(path: Path, columns_to_drop: set[str]) -> None:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = [field for field in (reader.fieldnames or []) if field not in columns_to_drop]
        rows = [{key: value for key, value in row.items() if key in fieldnames} for row in reader]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_storage_schema_creation_and_health_check(tmp_path: Path) -> None:
    database = DatabaseManager(_storage_settings(tmp_path))
    assert database.initialize() is True

    table_names = set(inspect(database.engine).get_table_names())
    assert {"runs", "points", "samples", "measurement_frames", "qc_results", "fit_results", "coefficient_versions", "sensors"} <= table_names
    assert database.health_check()["ok"] is True


def test_artifact_import_queries_and_export(tmp_path: Path) -> None:
    database = DatabaseManager(_storage_settings(tmp_path))
    database.initialize()
    run_dir = _write_run_artifacts(tmp_path)
    importer = ArtifactImporter(database)

    importer.import_run_directory(run_dir)
    importer.import_run_directory(run_dir)

    with database.session_scope() as session:
        assert session.execute(select(func.count(RunRecord.id))).scalar_one() == 1
        assert session.execute(select(func.count(PointRecord.id))).scalar_one() == 2
        assert session.execute(select(func.count(SampleRecord.id))).scalar_one() == 3
        assert session.execute(select(func.count(SensorRecord.sensor_id))).scalar_one() == 2

    queries = HistoryQueryService(database)
    runs = queries.runs_by_time_range(limit=10)
    assert len(runs) == 1
    assert runs[0]["status"] == "completed"
    assert runs[0]["total_points"] == 2

    device_history = queries.runs_by_device("ga01")
    assert len(device_history) == 1
    assert device_history[0]["sample_count"] == 2

    sensors = queries.sensors()
    assert len(sensors) == 2
    ga01_sensor = next(item for item in sensors if item["analyzer_id"] == "ga01")
    assert ga01_sensor["software_version"] == "v5_plus"

    sensor_runs = queries.runs_by_sensor(ga01_sensor["sensor_id"])
    assert len(sensor_runs) == 1
    assert sensor_runs[0]["profile_name"] == "bench_profile"

    sensor_samples = queries.samples_by_sensor(ga01_sensor["sensor_id"])
    assert len(sensor_samples) == 2
    assert all(item["sensor_id"] == ga01_sensor["sensor_id"] for item in sensor_samples)

    sensor_fit_results = queries.fit_results_by_sensor(ga01_sensor["sensor_id"])
    assert len(sensor_fit_results) == 1
    assert sensor_fit_results[0]["analyzer_id"] == "ga01"

    store = CoefficientVersionStore(database)
    store.save_new_version(
        sensor_id=ga01_sensor["sensor_id"],
        analyzer_id="ga01",
        analyzer_serial="SN01",
        coefficients={"slope": 1.23},
        created_by="qa",
    )
    sensor_coefficients = queries.coefficient_versions_by_sensor(ga01_sensor["sensor_id"])
    assert len(sensor_coefficients) == 1
    assert sensor_coefficients[0]["analyzer_serial"] == "SN01"

    point_samples = queries.samples_by_point(run_id="run_20260320_001000", sequence=1)
    assert [item["sample_index"] for item in point_samples] == [1, 2]

    stats = queries.statistics()
    assert stats["run_count"] == 1
    assert stats["point_count"] == 2
    assert stats["point_success_rate"] == 0.5

    exporter = StorageExporter(database)
    run_bundle = exporter.export_run_bundle("run_20260320_001000", tmp_path / "exported")
    assert run_bundle["summary"].exists()
    assert run_bundle["samples"].exists()
    assert run_bundle["product_report_manifest"].exists()
    exported_summary = json.loads(run_bundle["summary"].read_text(encoding="utf-8"))
    product_manifest = json.loads(run_bundle["product_report_manifest"].read_text(encoding="utf-8"))
    assert exported_summary["run_id"] == "run_20260320_001000"
    assert exported_summary["report_family"] == "v2_product_report_family"
    assert exported_summary["evidence_source"] == "diagnostic"
    assert exported_summary["not_real_acceptance_evidence"] is True
    assert exported_summary["acceptance_level"] == "diagnostic"
    assert exported_summary["promotion_state"] == "dry_run_only"
    assert len(product_manifest["generated_reports"]) == 1
    generated_report = product_manifest["generated_reports"][0]
    assert generated_report["template_key"] == "h2o_calibration_report"
    generated_path = Path(generated_report["path"])
    assert generated_path.exists()
    report_payload = json.loads(generated_path.read_text(encoding="utf-8"))
    assert report_payload["report_key"] == "h2o_calibration_report"
    assert report_payload["run_id"] == "run_20260320_001000"
    assert report_payload["point_count"] == 1

    sensor_bundle = exporter.export_sensor_bundle(ga01_sensor["sensor_id"], tmp_path / "sensor_export")
    assert sensor_bundle["sensor"].exists()
    assert sensor_bundle["samples"].exists()

    runs_csv = exporter.export_runs_csv(tmp_path / "exported" / "runs.csv")
    assert runs_csv.exists()


def test_artifact_import_supports_raw_and_enrich_stages(tmp_path: Path) -> None:
    database = DatabaseManager(_storage_settings(tmp_path))
    database.initialize()
    run_dir = _write_run_artifacts(tmp_path)
    importer = ArtifactImporter(database)

    raw = importer.import_raw_run_directory(run_dir)
    with database.session_scope() as session:
        run_record = session.get(RunRecord, resolve_run_uuid("run_20260320_001000"))
        assert run_record is not None
        assert run_record.software_version == "2.2.0"
        assert session.execute(select(func.count(RunRecord.id))).scalar_one() == 1
        assert session.execute(select(func.count(PointRecord.id))).scalar_one() == 2
        assert session.execute(select(func.count(SampleRecord.id))).scalar_one() == 3
        assert session.execute(select(func.count(DeviceEventRecord.id))).scalar_one() == 2
        assert session.execute(select(func.count(AlarmIncidentRecord.id))).scalar_one() == 2
        assert session.execute(select(func.count(MeasurementFrameRecord.id))).scalar_one() == 0
        assert session.execute(select(func.count(QCResultRecord.id))).scalar_one() == 0
        assert session.execute(select(func.count(FitResultRecord.id))).scalar_one() == 0

    enrich = importer.import_enrich_run_directory(run_dir)
    importer.import_enrich_run_directory(run_dir)

    assert raw["stage"] == "raw"
    assert enrich["stage"] == "enrich"
    assert enrich["metadata_sections"]["ai_summary_metadata"] == "loaded"
    assert "qc_report.json" not in enrich["skipped_artifacts"]

    with database.session_scope() as session:
        run_record = session.get(RunRecord, resolve_run_uuid("run_20260320_001000"))
        notes = json.loads(run_record.notes or "{}")
        assert session.execute(select(func.count(QCResultRecord.id))).scalar_one() == 4
        assert session.execute(select(func.count(FitResultRecord.id))).scalar_one() == 1
        assert notes["enrich"]["qc"]["imported_results"] == 4
        assert notes["enrich"]["fit"]["imported_results"] == 1
        assert notes["enrich"]["coefficient_metadata"]["report_status"] == "completed"
        assert notes["enrich"]["ai_summary_metadata"]["status"] == "completed"


def test_measurement_frames_imports_real_samples_runtime_as_long_table(tmp_path: Path) -> None:
    database = DatabaseManager(_storage_settings(tmp_path))
    database.initialize()
    run_dir = _repo_runtime_run_dir()
    assert (run_dir / "samples_runtime.csv").exists()
    importer = ArtifactImporter(database)

    raw = importer.import_raw_run_directory(run_dir)
    expected_count = _expected_measurement_frame_count(run_dir / "samples_runtime.csv")

    assert raw["measurement_frames"] == expected_count

    with database.session_scope() as session:
        count = session.execute(select(func.count(MeasurementFrameRecord.id))).scalar_one()
        frame = session.execute(select(MeasurementFrameRecord).order_by(MeasurementFrameRecord.sample_ts)).scalars().first()
        assert count == expected_count
        assert frame is not None
        assert frame.run_id == resolve_run_uuid(run_dir.name)
        assert frame.analyzer_label == "ga01"
        assert "pressure_hpa" in frame.context_payload
        assert "analyzer_expected_count" in frame.context_payload
        assert "raw_payload" not in frame.context_payload

    queries = HistoryQueryService(database)
    ga01_sensor = next(
        item
        for item in queries.sensors()
        if (item.get("metadata") or {}).get("legacy", {}).get("analyzer_label") == "ga01"
    )
    ga01_frames = queries.measurement_frames_by_sensor(ga01_sensor["sensor_id"], run_id=run_dir.name)
    assert ga01_frames
    assert all(item["sensor_id"] == ga01_sensor["sensor_id"] for item in ga01_frames)


def test_measurement_frames_import_is_idempotent_for_real_run(tmp_path: Path) -> None:
    database = DatabaseManager(_storage_settings(tmp_path))
    database.initialize()
    run_dir = _repo_runtime_run_dir()
    importer = ArtifactImporter(database)
    expected_count = _expected_measurement_frame_count(run_dir / "samples_runtime.csv")

    importer.import_raw_run_directory(run_dir)
    importer.import_raw_run_directory(run_dir)

    with database.session_scope() as session:
        count = session.execute(select(func.count(MeasurementFrameRecord.id))).scalar_one()
        assert count == expected_count


def test_measurement_frames_import_tolerates_missing_analyzer_columns(tmp_path: Path) -> None:
    database = DatabaseManager(_storage_settings(tmp_path))
    database.initialize()
    source_run_dir = _repo_runtime_run_dir()
    run_dir = tmp_path / source_run_dir.name
    shutil.copytree(source_run_dir, run_dir)
    samples_runtime_path = run_dir / "samples_runtime.csv"
    _rewrite_runtime_without_columns(
        samples_runtime_path,
        {
            "ga02_mode2_field_count",
            "ga02_pressure_kpa",
            "ga02_co2_ratio_raw",
            "ga02_h2o_ratio_raw",
        },
    )
    expected_count = _expected_measurement_frame_count(samples_runtime_path)
    importer = ArtifactImporter(database)

    raw = importer.import_raw_run_directory(run_dir)

    assert raw["measurement_frames"] == expected_count

    with database.session_scope() as session:
        count = session.execute(select(func.count(MeasurementFrameRecord.id))).scalar_one()
        ga02_count = session.execute(
            select(func.count(MeasurementFrameRecord.id)).where(MeasurementFrameRecord.analyzer_label == "ga02")
        ).scalar_one()
        assert count == expected_count
        assert ga02_count > 0


def test_coefficient_store_save_approve_deploy_and_rollback(tmp_path: Path) -> None:
    database = DatabaseManager(_storage_settings(tmp_path))
    database.initialize()
    store = CoefficientVersionStore(database)
    with database.session_scope() as session:
        sensor = SensorRecord(
            device_key="ga01-main",
            analyzer_id="ga01",
            analyzer_serial="SN01",
            software_version="v5_plus",
            model="GA",
            channel_type="co2_h2o_dual",
            metadata_json={},
        )
        session.add(sensor)
        session.flush()
        sensor_id = str(sensor.sensor_id)

    v1 = store.save_new_version(
        analyzer_id="ga01",
        analyzer_serial="SN01",
        coefficients={"slope": 1.0},
        created_by="alice",
    )
    v2 = store.save_new_version(
        sensor_id=sensor_id,
        coefficients={"slope": 1.2},
        created_by="bob",
    )
    assert v1.version == 1
    assert v2.version == 2

    approved = store.approve_version(v2.id, approved_by="lead")
    store.deploy_version(v1.id)
    deployed = store.deploy_version(v2.id)
    current = store.get_current_version(sensor_id=sensor_id, deployed_only=True)
    legacy_current = store.get_current_version(analyzer_id="ga01", analyzer_serial="SN01", deployed_only=True)
    history = store.list_versions(sensor_id=sensor_id)
    rollback = store.rollback_to_version(
        sensor_id=sensor_id,
        version=1,
        created_by="ops",
        notes="restore baseline",
    )

    assert approved.approved is True
    assert deployed.deployed is True
    assert current is not None and current.version == 2
    assert legacy_current is not None and legacy_current.version == 2
    assert [item.version for item in history] == [2, 1]
    assert rollback.version == 3
    assert str(rollback.sensor_id) == sensor_id
    assert rollback.coefficients == {"slope": 1.0}


class _FakeTemperatureChamber:
    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def selftest(self):
        return {"ok": True}

    def set_temp_c(self, value: float) -> None:
        self.value = value

    def start(self) -> None:
        return None

    def read_temp_c(self) -> float:
        return 25.0


class _FakeGasAnalyzer:
    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def selftest(self):
        return {"ok": True}

    def fetch_all(self):
        return {
            "data": {
                "co2_signal": 400.0,
                "h2o_signal": 10.0,
                "temperature_c": 25.0,
                "pressure_hpa": 1000.0,
                "dewpoint_c": 5.0,
                "co2_ppm": 400.0,
                "h2o_mmol": 0.2,
            }
        }


class _ImmediateStabilityChecker:
    def wait_for_stability(self, stability_type, read_func, stop_event, **kwargs):
        value = read_func() if read_func is not None else None
        return StabilityResult(
            stability_type=stability_type,
            stable=True,
            readings=[] if value is None else [float(value)],
            range_value=0.0,
            tolerance=1.0,
            elapsed_s=0.0,
            window_s=0.0,
            timeout_s=1.0,
            sample_count=1 if value is not None else 0,
            last_value=None if value is None else float(value),
        )


def _write_points_file(tmp_path: Path) -> Path:
    path = tmp_path / "points.json"
    path.write_text(
        json.dumps({"points": [{"index": 1, "temperature_c": 25.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0, "route": "co2"}]}),
        encoding="utf-8",
    )
    return path


def _make_service(tmp_path: Path) -> CalibrationService:
    points_path = _write_points_file(tmp_path)
    config = AppConfig.from_dict(
        {
            "devices": {
                "temperature_chamber": {"port": "COM1", "enabled": True},
                "gas_analyzers": [{"port": "COM2", "enabled": True}],
            },
            "workflow": {
                "sampling": {"count": 1, "interval_s": 0.0, "discard_first_n": 0},
                "precheck": {"enabled": True, "device_connection": True, "sensor_check": False, "pressure_leak_test": False},
            },
            "paths": {"points_excel": str(points_path), "output_dir": str(tmp_path / "output")},
            "storage": {"backend": "sqlite", "database": str(tmp_path / "storage.sqlite")},
        }
    )
    device_manager = DeviceManager(config.devices)
    device_manager.register_device("temperature_chamber", _FakeTemperatureChamber())
    device_manager.register_device("gas_analyzer_0", _FakeGasAnalyzer())
    return CalibrationService(
        config=config,
        device_manager=device_manager,
        stability_checker=_ImmediateStabilityChecker(),
    )


def test_calibration_service_storage_failure_falls_back_to_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _make_service(tmp_path)

    def _fail_sync(self) -> None:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(CalibrationService, "_sync_results_to_storage_impl", _fail_sync)

    service.session.start()
    service.session.end("")
    service.result_store.save_run_summary(service.session)
    service._sync_results_to_storage()

    assert service.result_store.data_writer.summary_path.exists()
    assert any("Storage warning" in warning for warning in service.session.warnings)
