from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from gas_calibrator.v2.core.data_writer import DataWriter
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.sim.resilience import build_export_resilience_report


def test_run_logger_expands_headers_without_losing_existing_rows(tmp_path: Path) -> None:
    logger = RunLogger(str(tmp_path), "dynamic_header")
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    try:
        logger.log_sample({"timestamp": "2026-03-26T10:00:00", "point_index": 1, "route": "co2"})
        logger.log_sample(
            {
                "timestamp": "2026-03-26T10:00:01",
                "point_index": 1,
                "route": "co2",
                "pressure_gauge_hpa": 998.0,
                "thermometer_temp_c": 25.0,
                "frame_has_data": True,
                "frame_usable": True,
                "frame_status": "ok",
                "sample_index": 2,
            }
        )
        logger.log_point(point, "done", extra_fields={"points_readable": True})
    finally:
        logger.finalize()

    sample_lines = logger.samples_path.read_text(encoding="utf-8").splitlines()
    point_lines = logger.points_path.read_text(encoding="utf-8").splitlines()

    assert "pressure_gauge_hpa" in sample_lines[0]
    assert "thermometer_temp_c" in sample_lines[0]
    assert "frame_status" in sample_lines[0]
    assert len(sample_lines) == 3
    assert "points_readable" in point_lines[0]


def test_data_writer_merges_existing_header_with_new_sample_fields(tmp_path: Path) -> None:
    writer = DataWriter(str(tmp_path), "writer_dynamic")
    writer.samples_csv_path.write_text("timestamp,point_index,route\nlegacy,1,co2\n", encoding="utf-8")
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")

    writer.write_samples(
        [
            SamplingResult(
                point=point,
                analyzer_id="ga01",
                timestamp=datetime(2026, 3, 26, 10, 0, 2),
                co2_ppm=401.0,
                pressure_hpa=1000.0,
                pressure_gauge_hpa=999.0,
                thermometer_temp_c=25.1,
                frame_has_data=True,
                frame_usable=True,
                frame_status="ok",
                sample_index=1,
            )
        ]
    )

    header = writer.samples_csv_path.read_text(encoding="utf-8").splitlines()[0]
    assert "pressure_gauge_hpa" in header
    assert "thermometer_temp_c" in header
    assert "frame_status" in header


def test_export_resilience_report_captures_failure_isolation_and_reporting(tmp_path: Path) -> None:
    result = build_export_resilience_report(report_root=tmp_path, run_name="resilience_report")

    report = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))
    markdown = Path(result["report_markdown"]).read_text(encoding="utf-8")
    cases = {item["name"]: item for item in report["cases"]}

    assert result["status"] == "MATCH"
    assert cases["dynamic_header_expansion"]["status"] == "MATCH"
    assert cases["points_readable_execution_summary"]["status"] == "MATCH"
    assert cases["export_failure_isolation"]["status"] == "MATCH"
    readable_exports = cases["points_readable_execution_summary"]["details"]["artifact_exports"]
    assert readable_exports["points_readable"]["role"] == "execution_summary"
    assert readable_exports["points_readable"]["status"] == "ok"
    artifact_exports = cases["export_failure_isolation"]["details"]["artifact_exports"]
    assert artifact_exports["qc_report"]["status"] == "error"
    assert artifact_exports["run_summary"]["status"] == "ok"
    assert "导出韧性" in markdown
    assert "证据来源" in markdown


def test_export_resilience_report_avoids_real_driver_imports_when_optional_deps_are_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def _guard_import(module_name: str):
        raise AssertionError(f"offline resilience should not import real driver: {module_name}")

    monkeypatch.setattr("gas_calibrator.v2.core.device_factory.import_module", _guard_import)

    result = build_export_resilience_report(report_root=tmp_path, run_name="resilience_no_driver")

    assert result["status"] == "MATCH"
