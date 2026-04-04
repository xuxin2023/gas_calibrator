import json
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import func, select

from gas_calibrator.v2.storage.database import DatabaseManager, StorageSettings, resolve_run_uuid
from gas_calibrator.v2.storage.import_run import main
from gas_calibrator.v2.storage.models import FitResultRecord, PointRecord, QCResultRecord, RunRecord, SampleRecord


def _write_run_artifacts(
    base_dir: Path,
    *,
    include_manifest: bool,
    include_postprocess: bool = True,
    include_ai: bool = True,
) -> Path:
    run_dir = base_dir / "run_20260320_001000"
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
                    "enabled_devices": ["ga01", "ga02"],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    if include_manifest:
        (run_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "run_id": "run_20260320_001000",
                    "software_version": "2.1.7",
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
                "timestamp,point_index,point_tag,temperature_c,co2_ppm,humidity_pct,pressure_hpa,route,status,stability_time_s,total_time_s",
                "2026-03-20T00:08:00+00:00,1,,25.0,400.0,,1000.0,co2,completed,12.5,30.0",
                "2026-03-20T00:09:30+00:00,2,,25.0,,45.0,1000.0,h2o,failed,14.0,34.0",
            ]
        ),
        encoding="utf-8",
    )

    (run_dir / "samples.csv").write_text(
        "\n".join(
            [
                "timestamp,point_index,temperature_c,co2_ppm,humidity_pct,pressure_hpa,route,analyzer_id,analyzer_serial,sample_index,sample_co2_ppm,sample_h2o_mmol,co2_ratio_f,h2o_ratio_f,co2_ratio_raw,h2o_ratio_raw,chamber_temp_c,case_temp_c,dewpoint_c",
                "2026-03-20T00:08:10+00:00,1,25.0,400.0,,1000.0,co2,ga01,SN01,1,401.2,0.2,1.001,0.201,1.001,0.201,25.1,26.0,4.1",
                "2026-03-20T00:08:11+00:00,1,25.0,400.0,,1000.0,co2,ga01,SN01,2,401.0,0.2,1.000,0.202,1.000,0.202,25.1,26.1,4.0",
                "2026-03-20T00:09:40+00:00,2,25.0,,45.0,1000.0,h2o,ga02,SN02,1,2.0,9.1,1.010,0.410,1.010,0.410,25.2,26.2,8.0",
            ]
        ),
        encoding="utf-8",
    )

    (run_dir / "qc_report.json").write_text(
        json.dumps(
            {
                "run_id": "run_20260320_001000",
                "point_details": [
                    {"point_index": 1, "quality_score": 0.98, "valid": True, "recommendation": "keep", "reason": ""},
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

    if include_ai:
        (run_dir / "ai_run_summary.md").write_text(
            "# AI Run Summary\n\n本次运行 AI 解释已生成。\n",
            encoding="utf-8",
        )
        (run_dir / "ai_anomaly_note.md").write_text(
            "# AI Anomaly Note\n\n存在湿度异常。\n",
            encoding="utf-8",
        )

    if include_postprocess:
        (run_dir / "calibration_coefficients_postprocess_summary.json").write_text(
            json.dumps(
                {
                    "generated_at": "2026-03-20T00:11:00+00:00",
                    "flags": {"import_db": True, "skip_qc": False, "skip_refit": False, "skip_ai": False},
                    "manifest": {"status": "completed", "path": str(run_dir / "manifest.json")},
                    "database_import": {"status": "completed"},
                    "qc": {"status": "completed", "json": str(run_dir / "qc_report.json")},
                    "report": {"status": "completed", "path": str(run_dir / "calibration_coefficients.xlsx")},
                    "refit": {"status": "completed", "runs": [{"analyzer": "GA01", "gas": "co2", "status": "completed"}]},
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


def _database(db_path: Path) -> DatabaseManager:
    return DatabaseManager(StorageSettings(backend="sqlite", database=str(db_path)))


def test_import_run_cli_imports_manifest_metadata_into_sqlite(tmp_path: Path, capsys) -> None:
    run_dir = _write_run_artifacts(tmp_path, include_manifest=True)
    db_path = tmp_path / "storage.sqlite"

    exit_code = main(
        [
            "--run-dir",
            str(run_dir),
            "--backend",
            "sqlite",
            "--database",
            str(db_path),
            "--init-schema",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "\"backend\": \"sqlite\"" in captured.out
    assert "\"run_id\"" in captured.out

    database = _database(db_path)
    try:
        with database.session_scope() as session:
            record = session.get(RunRecord, resolve_run_uuid("run_20260320_001000"))
            assert record is not None
            assert record.software_version == "2.1.7"
            notes = json.loads(record.notes or "{}")
            assert notes["source_points_file"] == "points/batch_a.xlsx"
            assert notes["manifest_schema_version"] == "1.0"
            assert session.execute(select(func.count(PointRecord.id))).scalar_one() == 2
            assert session.execute(select(func.count(SampleRecord.id))).scalar_one() == 3
            assert session.execute(select(func.count(QCResultRecord.id))).scalar_one() == 4
            assert session.execute(select(func.count(FitResultRecord.id))).scalar_one() == 1
    finally:
        database.dispose()


def test_import_run_cli_is_idempotent_without_manifest_using_sqlite_dsn(tmp_path: Path) -> None:
    run_dir = _write_run_artifacts(tmp_path, include_manifest=False)
    db_path = tmp_path / "storage.sqlite"
    dsn = f"sqlite:///{db_path.as_posix()}"

    assert main(["--run-dir", str(run_dir), "--dsn", dsn, "--init-schema"]) == 0
    assert main(["--run-dir", str(run_dir), "--dsn", dsn]) == 0

    database = _database(db_path)
    try:
        with database.session_scope() as session:
            record = session.get(RunRecord, resolve_run_uuid("run_20260320_001000"))
            assert record is not None
            assert record.software_version == "v2"
            notes = json.loads(record.notes or "{}")
            assert notes["source_points_file"] is None
            assert session.execute(select(func.count(RunRecord.id))).scalar_one() == 1
            assert session.execute(select(func.count(PointRecord.id))).scalar_one() == 2
            assert session.execute(select(func.count(SampleRecord.id))).scalar_one() == 3
            assert session.execute(select(func.count(QCResultRecord.id))).scalar_one() == 4
            assert session.execute(select(func.count(FitResultRecord.id))).scalar_one() == 1
    finally:
        database.dispose()


def test_import_run_cli_supports_raw_then_enrich_stages(tmp_path: Path) -> None:
    run_dir = _write_run_artifacts(tmp_path, include_manifest=True)
    db_path = tmp_path / "storage.sqlite"

    assert main(["--run-dir", str(run_dir), "--backend", "sqlite", "--database", str(db_path), "--init-schema", "--stage", "raw"]) == 0

    database = _database(db_path)
    try:
        with database.session_scope() as session:
            assert session.execute(select(func.count(RunRecord.id))).scalar_one() == 1
            assert session.execute(select(func.count(PointRecord.id))).scalar_one() == 2
            assert session.execute(select(func.count(SampleRecord.id))).scalar_one() == 3
            assert session.execute(select(func.count(QCResultRecord.id))).scalar_one() == 0
            assert session.execute(select(func.count(FitResultRecord.id))).scalar_one() == 0
    finally:
        database.dispose()

    assert main(["--run-dir", str(run_dir), "--backend", "sqlite", "--database", str(db_path), "--stage", "enrich"]) == 0
    assert main(["--run-dir", str(run_dir), "--backend", "sqlite", "--database", str(db_path), "--stage", "enrich"]) == 0

    database = _database(db_path)
    try:
        with database.session_scope() as session:
            record = session.get(RunRecord, resolve_run_uuid("run_20260320_001000"))
            notes = json.loads(record.notes or "{}")
            assert session.execute(select(func.count(QCResultRecord.id))).scalar_one() == 4
            assert session.execute(select(func.count(FitResultRecord.id))).scalar_one() == 1
            assert notes["enrich"]["qc"]["imported_results"] == 4
            assert notes["enrich"]["ai_summary_metadata"]["status"] == "completed"
    finally:
        database.dispose()


def test_import_run_cli_enrich_gracefully_skips_missing_artifacts(tmp_path: Path, capsys) -> None:
    run_dir = _write_run_artifacts(
        tmp_path,
        include_manifest=False,
        include_postprocess=False,
        include_ai=False,
    )
    (run_dir / "qc_report.json").unlink()
    db_path = tmp_path / "storage.sqlite"

    assert main(["--run-dir", str(run_dir), "--backend", "sqlite", "--database", str(db_path), "--init-schema", "--stage", "raw"]) == 0
    exit_code = main(["--run-dir", str(run_dir), "--backend", "sqlite", "--database", str(db_path), "--stage", "enrich"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "\"stage\": \"enrich\"" in captured.out
    assert "qc_report.json" in captured.out
    assert "ai_run_summary.md" in captured.out

    database = _database(db_path)
    try:
        with database.session_scope() as session:
            record = session.get(RunRecord, resolve_run_uuid("run_20260320_001000"))
            notes = json.loads(record.notes or "{}")
            assert session.execute(select(func.count(QCResultRecord.id))).scalar_one() == 0
            assert session.execute(select(func.count(FitResultRecord.id))).scalar_one() == 1
            assert "qc_report.json" in notes["enrich"]["skipped_artifacts"]
            assert notes["enrich"]["postprocess_summary_metadata"]["status"] == "missing"
    finally:
        database.dispose()
