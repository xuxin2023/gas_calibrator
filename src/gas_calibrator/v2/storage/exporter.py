from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import mean, stdev
from typing import Any

from sqlalchemy import select

from ..core.acceptance_model import build_user_visible_evidence_boundary
from .database import DatabaseManager, resolve_run_uuid
from .models import PointRecord, QCResultRecord, RunRecord, SampleRecord
from .queries import HistoryQueryService


def _extract_source_run_id(notes: str | None, fallback: str) -> str:
    if not notes:
        return fallback
    try:
        payload = json.loads(notes)
    except json.JSONDecodeError:
        return fallback
    if isinstance(payload, dict):
        return str(payload.get("source_run_id") or fallback)
    return fallback


def _safe_slug(value: str) -> str:
    text = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value or "").strip())
    return text.strip("_") or "device"


def _numeric_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(float(mean(values)), 6)


def _numeric_std(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return round(float(stdev(values)), 6)


class StorageExporter:
    def __init__(self, database: DatabaseManager):
        self.database = database
        self.queries = HistoryQueryService(database)

    def export_runs_csv(
        self,
        path: str | Path,
        *,
        start_time=None,
        end_time=None,
        status: str | None = None,
    ) -> Path:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        runs = self.queries.runs_by_time_range(start_time=start_time, end_time=end_time, status=status, limit=10000)
        with destination.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = [
                "id",
                "start_time",
                "end_time",
                "status",
                "software_version",
                "run_mode",
                "route_mode",
                "profile_name",
                "profile_version",
                "report_family",
                "operator",
                "total_points",
                "successful_points",
                "failed_points",
                "warnings",
                "errors",
            ]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows([{key: row.get(key) for key in fieldnames} for row in runs])
        return destination

    def export_samples_json(self, run_id: str, path: str | Path) -> Path:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        samples = self.queries.samples_by_point(run_id=run_id)
        destination.write_text(json.dumps({"run_id": run_id, "samples": samples}, ensure_ascii=False, indent=2), encoding="utf-8")
        return destination

    def export_run_bundle(self, run_id: str, output_dir: str | Path) -> dict[str, Path]:
        destination = Path(output_dir)
        destination.mkdir(parents=True, exist_ok=True)
        run_uuid = resolve_run_uuid(run_id)

        with self.database.session_scope() as session:
            run = session.get(RunRecord, run_uuid)
            if run is None:
                raise ValueError(f"run not found: {run_id}")

            points = session.execute(
                select(PointRecord).where(PointRecord.run_id == run_uuid).order_by(PointRecord.sequence.asc())
            ).scalars().all()
            samples = session.execute(
                select(SampleRecord)
                .join(PointRecord, SampleRecord.point_id == PointRecord.id)
                .where(PointRecord.run_id == run_uuid)
                .order_by(PointRecord.sequence.asc(), SampleRecord.sample_index.asc())
            ).scalars().all()
            qc_results = session.execute(
                select(QCResultRecord)
                .join(PointRecord, QCResultRecord.point_id == PointRecord.id)
                .where(PointRecord.run_id == run_uuid)
                .order_by(PointRecord.sequence.asc(), QCResultRecord.rule_name.asc())
            ).scalars().all()

            exported_run_id = _extract_source_run_id(run.notes, str(run.id))
            summary_boundary = build_user_visible_evidence_boundary(
                evidence_source="diagnostic",
                not_real_acceptance_evidence=True,
                acceptance_level="diagnostic",
                promotion_state="dry_run_only",
            )
            summary_path = destination / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "run_id": exported_run_id,
                        "status": {
                            "phase": run.status,
                            "total_points": run.total_points,
                            "completed_points": run.successful_points,
                            "elapsed_s": None
                            if run.start_time is None or run.end_time is None
                            else (run.end_time - run.start_time).total_seconds(),
                        },
                        "stats": {
                            "software_version": run.software_version,
                            "run_mode": run.run_mode,
                            "route_mode": run.route_mode,
                            "profile_name": run.profile_name,
                            "profile_version": run.profile_version,
                            "report_family": run.report_family,
                            "successful_points": run.successful_points,
                            "failed_points": run.failed_points,
                            "warning_count": run.warnings,
                            "error_count": run.errors,
                        },
                        "report_family": run.report_family,
                        "report_templates": dict(run.report_templates or {}),
                        "analyzer_setup": dict(run.analyzer_setup or {}),
                        "generated_at": None if run.end_time is None else run.end_time.isoformat(),
                        **summary_boundary,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            points_path = destination / "points.csv"
            with points_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "timestamp",
                        "point_index",
                        "point_tag",
                        "temperature_c",
                        "co2_ppm",
                        "co2_group",
                        "cylinder_nominal_ppm",
                        "humidity_pct",
                        "pressure_hpa",
                        "route",
                        "status",
                        "stability_time_s",
                        "total_time_s",
                    ],
                )
                writer.writeheader()
                for point in points:
                    writer.writerow(
                        {
                            "timestamp": "",
                            "point_index": point.sequence,
                            "point_tag": "",
                            "temperature_c": point.temperature_c,
                            "co2_ppm": point.co2_target_ppm,
                            "co2_group": point.co2_group,
                            "cylinder_nominal_ppm": point.cylinder_nominal_ppm,
                            "humidity_pct": point.humidity_rh,
                            "pressure_hpa": point.pressure_hpa,
                            "route": point.route_type,
                            "status": point.status,
                            "stability_time_s": point.stability_time_s,
                            "total_time_s": point.total_time_s,
                        }
                    )

            point_lookup = {point.id: point for point in points}
            samples_path = destination / "samples.csv"
            with samples_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "timestamp",
                        "point_index",
                        "temperature_c",
                        "co2_ppm",
                        "humidity_pct",
                        "pressure_hpa",
                        "route",
                        "sensor_id",
                        "analyzer_id",
                        "analyzer_serial",
                        "sample_index",
                        "sample_co2_ppm",
                        "sample_h2o_mmol",
                        "co2_ratio_f",
                        "h2o_ratio_f",
                        "co2_ratio_raw",
                        "h2o_ratio_raw",
                        "chamber_temp_c",
                        "case_temp_c",
                        "dewpoint_c",
                    ],
                )
                writer.writeheader()
                for sample in samples:
                    point = point_lookup[sample.point_id]
                    writer.writerow(
                        {
                            "timestamp": None if sample.timestamp is None else sample.timestamp.isoformat(),
                            "point_index": point.sequence,
                            "temperature_c": point.temperature_c,
                            "co2_ppm": point.co2_target_ppm,
                            "humidity_pct": point.humidity_rh,
                            "pressure_hpa": sample.pressure_hpa or point.pressure_hpa,
                            "route": point.route_type,
                            "sensor_id": None if sample.sensor_id is None else str(sample.sensor_id),
                            "analyzer_id": sample.analyzer_id,
                            "analyzer_serial": sample.analyzer_serial,
                            "sample_index": sample.sample_index,
                            "sample_co2_ppm": sample.co2_ppm,
                            "sample_h2o_mmol": sample.h2o_mmol,
                            "co2_ratio_f": sample.co2_ratio_f,
                            "h2o_ratio_f": sample.h2o_ratio_f,
                            "co2_ratio_raw": sample.co2_ratio_raw,
                            "h2o_ratio_raw": sample.h2o_ratio_raw,
                            "chamber_temp_c": sample.chamber_temp_c,
                            "case_temp_c": sample.case_temp_c,
                            "dewpoint_c": sample.dewpoint_c,
                        }
                    )

            qc_path = destination / "qc_report.json"
            point_qc_rows: list[dict[str, Any]] = []
            for result in qc_results:
                point = point_lookup[result.point_id]
                point_qc_rows.append(
                    {
                        "point_index": point.sequence,
                        "rule_name": result.rule_name,
                        "passed": result.passed,
                        "value": result.value,
                        "threshold": result.threshold,
                        "message": result.message,
                    }
                )
            qc_path.write_text(
                json.dumps({"run_id": exported_run_id, "point_details": point_qc_rows}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            run_sensor_ids = sorted(
                {
                    str(sample.sensor_id)
                    for sample in samples
                    if sample.sensor_id is not None
                }
            )
            sensors_by_id = {
                item["sensor_id"]: item
                for item in self.queries.sensors(limit=10000)
            }
            generated_product_reports = self._export_h2o_calibration_reports(
                destination=destination,
                exported_run_id=exported_run_id,
                run=run,
                points=points,
                samples=samples,
                sensors_by_id=sensors_by_id,
                run_sensor_ids=run_sensor_ids,
            )
            product_report_manifest_path = destination / "product_report_manifest.json"
            product_report_manifest_path.write_text(
                json.dumps(
                    {
                        "run_id": exported_run_id,
                        "report_family": run.report_family,
                        "report_templates": dict(run.report_templates or {}),
                        "generated_reports": generated_product_reports,
                        "per_device_outputs": [
                            {
                                **sensors_by_id[sensor_id],
                                "reports": [
                                    item
                                    for item in generated_product_reports
                                    if item.get("sensor_id") == sensor_id
                                ],
                            }
                            for sensor_id in run_sensor_ids
                            if sensor_id in sensors_by_id
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

        return {
            "summary": summary_path,
            "points": points_path,
            "samples": samples_path,
            "qc_report": qc_path,
            "product_report_manifest": product_report_manifest_path,
        }

    @staticmethod
    def _h2o_calibration_template_enabled(run: RunRecord) -> bool:
        templates = dict(run.report_templates or {})
        for item in templates.get("templates") or []:
            if str(item.get("key") or "") == "h2o_calibration_report":
                if "enabled" in item:
                    return bool(item.get("enabled", False))
                break
        run_mode = str(run.run_mode or "auto_calibration").strip().lower()
        route_mode = str(run.route_mode or "h2o_then_co2").strip().lower()
        return run_mode == "auto_calibration" and route_mode != "co2_only"

    def _export_h2o_calibration_reports(
        self,
        *,
        destination: Path,
        exported_run_id: str,
        run: RunRecord,
        points: list[PointRecord],
        samples: list[SampleRecord],
        sensors_by_id: dict[str, dict[str, Any]],
        run_sensor_ids: list[str],
    ) -> list[dict[str, Any]]:
        if not self._h2o_calibration_template_enabled(run):
            return []

        h2o_points = [point for point in points if str(point.route_type or "").strip().lower() == "h2o"]
        if not h2o_points or not run_sensor_ids:
            return []

        report_dir = destination / "reports" / "h2o_calibration"
        report_dir.mkdir(parents=True, exist_ok=True)
        outputs: list[dict[str, Any]] = []
        for sensor_id in run_sensor_ids:
            sensor_payload = sensors_by_id.get(sensor_id)
            if sensor_payload is None:
                continue

            point_rows: list[dict[str, Any]] = []
            for point in h2o_points:
                sensor_samples = [
                    sample
                    for sample in samples
                    if sample.point_id == point.id and sample.sensor_id is not None and str(sample.sensor_id) == sensor_id
                ]
                if not sensor_samples:
                    continue
                h2o_values = [float(sample.h2o_mmol) for sample in sensor_samples if sample.h2o_mmol is not None]
                dewpoint_values = [float(sample.dewpoint_c) for sample in sensor_samples if sample.dewpoint_c is not None]
                chamber_values = [float(sample.chamber_temp_c) for sample in sensor_samples if sample.chamber_temp_c is not None]
                point_rows.append(
                    {
                        "point_sequence": point.sequence,
                        "temperature_c": point.temperature_c,
                        "humidity_rh": point.humidity_rh,
                        "pressure_hpa": point.pressure_hpa,
                        "sample_count": len(sensor_samples),
                        "h2o_mmol_mean": _numeric_mean(h2o_values),
                        "h2o_mmol_std": _numeric_std(h2o_values),
                        "dewpoint_c_mean": _numeric_mean(dewpoint_values),
                        "chamber_temp_c_mean": _numeric_mean(chamber_values),
                    }
                )

            if not point_rows:
                continue

            report_payload = {
                "report_key": "h2o_calibration_report",
                "title": "H2O Calibration Report",
                "run_id": exported_run_id,
                "report_family": run.report_family,
                "profile_name": run.profile_name,
                "profile_version": run.profile_version,
                "sensor": dict(sensor_payload),
                "analyzer_setup": dict(run.analyzer_setup or {}),
                "generated_at": None if run.end_time is None else run.end_time.isoformat(),
                "point_count": len(point_rows),
                "points": point_rows,
            }
            file_stem = _safe_slug(
                str(sensor_payload.get("device_key") or sensor_payload.get("analyzer_serial") or sensor_payload.get("analyzer_id") or sensor_id)
            )
            report_path = report_dir / f"{file_stem}.json"
            report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            outputs.append(
                {
                    "sensor_id": sensor_id,
                    "device_key": sensor_payload.get("device_key"),
                    "template_key": "h2o_calibration_report",
                    "format": "json",
                    "path": str(report_path),
                    "implementation_status": "first_exporter_available",
                    "point_count": len(point_rows),
                }
            )
        return outputs

    def export_sensor_bundle(self, sensor_id: str, output_dir: str | Path) -> dict[str, Path]:
        destination = Path(output_dir)
        destination.mkdir(parents=True, exist_ok=True)
        sensor_path = destination / "sensor.json"
        runs_path = destination / "runs.json"
        samples_path = destination / "samples.json"
        frames_path = destination / "measurement_frames.json"
        fit_path = destination / "fit_results.json"
        coefficients_path = destination / "coefficient_versions.json"

        sensors = {item["sensor_id"]: item for item in self.queries.sensors(limit=10000)}
        sensor_payload = sensors.get(sensor_id)
        if sensor_payload is None:
            raise ValueError(f"sensor not found: {sensor_id}")

        sensor_path.write_text(json.dumps(sensor_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        runs_path.write_text(
            json.dumps({"sensor_id": sensor_id, "runs": self.queries.runs_by_sensor(sensor_id)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        samples_path.write_text(
            json.dumps({"sensor_id": sensor_id, "samples": self.queries.samples_by_sensor(sensor_id)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        frames_path.write_text(
            json.dumps(
                {"sensor_id": sensor_id, "measurement_frames": self.queries.measurement_frames_by_sensor(sensor_id)},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        fit_path.write_text(
            json.dumps({"sensor_id": sensor_id, "fit_results": self.queries.fit_results_by_sensor(sensor_id)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        coefficients_path.write_text(
            json.dumps(
                {"sensor_id": sensor_id, "coefficient_versions": self.queries.coefficient_versions_by_sensor(sensor_id)},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return {
            "sensor": sensor_path,
            "runs": runs_path,
            "samples": samples_path,
            "measurement_frames": frames_path,
            "fit_results": fit_path,
            "coefficient_versions": coefficients_path,
        }
