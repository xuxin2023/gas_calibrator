"""
Data output module.
"""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from openpyxl import Workbook

try:  # pragma: no cover - defensive import
    from ... import __version__ as SOFTWARE_VERSION
except Exception:  # pragma: no cover - defensive
    SOFTWARE_VERSION = ""

from .acceptance_model import build_user_visible_evidence_boundary
from .models import CalibrationStatus, SamplingResult
from .csv_resilience import load_csv_rows, merge_csv_headers, save_csv_atomic


SUMMARY_SCHEMA_VERSION = "2.2"


class DataWriter:
    """Writes V2 run artifacts."""

    SAMPLE_COLUMNS = [
        "timestamp",
        "point_index",
        "point_phase",
        "point_tag",
        "temperature_c",
        "co2_ppm",
        "co2_group",
        "cylinder_nominal_ppm",
        "analyzer_id",
        "humidity_pct",
        "route",
        "sample_co2_ppm",
        "sample_h2o_mmol",
        "h2o_signal",
        "co2_signal",
        "co2_ratio_f",
        "co2_ratio_raw",
        "h2o_ratio_f",
        "h2o_ratio_raw",
        "ref_signal",
        "pressure_hpa",
        "pressure_gauge_hpa",
        "pressure_reference_status",
        "thermometer_temp_c",
        "thermometer_reference_status",
        "dew_point_c",
        "analyzer_pressure_kpa",
        "analyzer_chamber_temp_c",
        "case_temp_c",
        "frame_has_data",
        "frame_usable",
        "frame_status",
        "sample_index",
        "stability_time_s",
        "total_time_s",
    ]
    POINTS_READABLE_COLUMNS = [
        "point_index",
        "point_phase",
        "point_tag",
        "route",
        "temperature_c",
        "hgen_temp_c",
        "humidity_pct",
        "co2_ppm",
        "co2_group",
        "cylinder_nominal_ppm",
        "pressure_target_hpa",
        "execution_status",
        "qc_valid",
        "recommendation",
        "reason",
        "raw_sample_count",
        "cleaned_sample_count",
        "usable_sample_count",
        "removed_sample_count",
        "total_frames",
        "valid_frames",
        "frames_with_data",
        "frame_status",
        "pressure_gauge_hpa_mean",
        "pressure_reference_status",
        "thermometer_temp_c_mean",
        "thermometer_reference_status",
        "reference_quality",
        "dew_point_c_mean",
        "AnalyzerCoverage",
        "UsableAnalyzers",
        "ExpectedAnalyzers",
        "PointIntegrity",
        "MissingAnalyzers",
        "UnusableAnalyzers",
        "stability_time_s",
        "total_time_s",
    ]

    def __init__(self, output_dir: str, run_id: str):
        self.output_dir = Path(output_dir)
        self.run_id = str(run_id)
        self.run_dir = self.output_dir / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.samples_csv_path = self.run_dir / "samples.csv"
        self.samples_excel_path = self.run_dir / "samples.xlsx"
        self.points_readable_csv_path = self.run_dir / "points_readable.csv"
        self.summary_path = self.run_dir / "summary.json"
        self.log_path = self.run_dir / "run.log"

    def write_samples(self, results: List[SamplingResult]) -> str:
        """Persist samples to CSV."""
        rows = [self._result_to_row(result) for result in results]
        existing_header, _ = load_csv_rows(self.samples_csv_path)
        fieldnames = merge_csv_headers(
            self.SAMPLE_COLUMNS,
            existing_header,
            *(row.keys() for row in rows),
        )
        save_csv_atomic(self.samples_csv_path, fieldnames, rows)
        return str(self.samples_csv_path)

    def write_samples_excel(self, results: List[SamplingResult]) -> str:
        """Persist samples to Excel."""
        rows = [self._result_to_row(result) for result in results]
        fieldnames = merge_csv_headers(self.SAMPLE_COLUMNS, *(row.keys() for row in rows))
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "samples"
        sheet.append(list(fieldnames))
        for row in rows:
            sheet.append([row.get(column) for column in fieldnames])
        workbook.save(self.samples_excel_path)
        workbook.close()
        return str(self.samples_excel_path)

    def write_points_readable(self, rows: List[Dict[str, Any]]) -> str:
        """Persist execution-level readable point summaries to CSV."""
        existing_header, _ = load_csv_rows(self.points_readable_csv_path)
        fieldnames = merge_csv_headers(
            self.POINTS_READABLE_COLUMNS,
            existing_header,
            *(row.keys() for row in rows),
        )
        save_csv_atomic(self.points_readable_csv_path, fieldnames, rows)
        return str(self.points_readable_csv_path)

    def write_summary(
        self,
        status: CalibrationStatus,
        stats: dict,
        *,
        started_at: Optional[str] = None,
        ended_at: Optional[str] = None,
        warnings: Optional[int] = None,
        errors: Optional[int] = None,
        software_version: Optional[str] = None,
        startup_pressure_precheck: Optional[dict[str, Any]] = None,
        reporting: Optional[dict[str, Any]] = None,
        simulation_mode: Optional[bool] = None,
        evidence_source: Any = None,
        not_real_acceptance_evidence: Optional[bool] = None,
        acceptance_level: Optional[str] = None,
        promotion_state: Optional[str] = None,
    ) -> str:
        """Persist run summary JSON."""
        points_total = int(status.total_points)
        points_completed = int(status.completed_points)
        progress = float(status.progress)
        warning_count = int(0 if warnings is None else warnings)
        error_count = int(0 if errors is None else errors)
        stats_payload = dict(stats)
        boundary = build_user_visible_evidence_boundary(
            evidence_source=evidence_source,
            simulation_mode=simulation_mode,
            not_real_acceptance_evidence=not_real_acceptance_evidence,
            acceptance_level=acceptance_level,
            promotion_state=promotion_state,
        )
        payload = {
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "run_id": self.run_id,
            "software_version": software_version if software_version is not None else SOFTWARE_VERSION,
            "software_build_id": software_version if software_version is not None else SOFTWARE_VERSION,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "started_at": started_at,
            "ended_at": ended_at,
            "points_total": points_total,
            "points_completed": points_completed,
            "total_points": points_total,
            "completed_points": points_completed,
            "warnings": warning_count,
            "errors": error_count,
            "progress": progress,
            "status": {
                "phase": status.phase.value,
                "total_points": points_total,
                "completed_points": points_completed,
                "progress": progress,
                "message": status.message,
                "elapsed_s": status.elapsed_s,
                "error": status.error,
            },
            "stats": stats_payload,
            **boundary,
        }
        config_safety = stats_payload.get("config_safety")
        if isinstance(config_safety, dict) and config_safety:
            payload["config_safety"] = dict(config_safety)
        config_safety_review = stats_payload.get("config_safety_review")
        if isinstance(config_safety_review, dict) and config_safety_review:
            payload["config_safety_review"] = dict(config_safety_review)
        if startup_pressure_precheck is not None:
            payload["startup_pressure_precheck"] = dict(startup_pressure_precheck)
        if reporting is not None:
            payload["reporting"] = dict(reporting)
        self.summary_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return str(self.summary_path)

    def write_log(self, level: str, message: str, **kwargs) -> None:
        """Append one JSON log entry."""
        entry = {
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "level": str(level).upper(),
            "message": str(message),
            "context": dict(kwargs),
        }
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    @staticmethod
    def _result_to_row(result: SamplingResult) -> Dict[str, Any]:
        return {
            "timestamp": result.timestamp.isoformat(),
            "point_index": result.point.index,
            "point_phase": result.point_phase or result.point.route,
            "point_tag": result.point_tag,
            "temperature_c": result.point.temperature_c,
            "co2_ppm": result.point.co2_ppm,
            "co2_group": result.point.co2_group,
            "cylinder_nominal_ppm": result.point.cylinder_nominal_ppm,
            "humidity_pct": result.point.humidity_pct,
            "route": result.point.route,
            "analyzer_id": result.analyzer_id,
            "sample_co2_ppm": result.co2_ppm,
            "sample_h2o_mmol": result.h2o_mmol,
            "h2o_signal": result.h2o_signal,
            "co2_signal": result.co2_signal,
            "co2_ratio_f": result.co2_ratio_f,
            "co2_ratio_raw": result.co2_ratio_raw,
            "h2o_ratio_f": result.h2o_ratio_f,
            "h2o_ratio_raw": result.h2o_ratio_raw,
            "ref_signal": result.ref_signal,
            "pressure_hpa": result.pressure_hpa,
            "pressure_gauge_hpa": result.pressure_gauge_hpa,
            "pressure_reference_status": result.pressure_reference_status,
            "thermometer_temp_c": result.thermometer_temp_c,
            "thermometer_reference_status": result.thermometer_reference_status,
            "dew_point_c": result.dew_point_c,
            "analyzer_pressure_kpa": result.analyzer_pressure_kpa,
            "analyzer_chamber_temp_c": result.analyzer_chamber_temp_c,
            "case_temp_c": result.case_temp_c,
            "frame_has_data": result.frame_has_data,
            "frame_usable": result.frame_usable,
            "frame_status": result.frame_status,
            "sample_index": result.sample_index,
            "stability_time_s": result.stability_time_s,
            "total_time_s": result.total_time_s,
        }
