from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from .database import DatabaseManager, stable_uuid
from .models import (
    AlarmIncidentRecord,
    DeviceEventRecord,
    FitResultRecord,
    MeasurementFrameRecord,
    PointRecord,
    QCResultRecord,
    RunRecord,
    SampleRecord,
    SensorRecord,
)


RUNTIME_ANALYZER_FIELD_PATTERN = re.compile(r"^(ga\d{2})_(.+)$")
RUNTIME_FRAME_PRESENCE_SUFFIXES = {
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
}
RUNTIME_CONTEXT_KEYS = {
    "point_phase",
    "point_tag",
    "temp_set_c",
    "pressure_target_hpa",
    "co2_ppm_target",
    "h2o_mmol_target",
    "point_is_h2o",
    "pressure_hpa",
    "dewpoint_c",
    "dew_temp_c",
    "dew_rh_pct",
    "dew_pressure_hpa",
    "dewpoint_sample_ts",
    "chamber_temp_c",
    "chamber_rh_pct",
    "analyzer_expected_count",
    "analyzer_with_frame_count",
    "analyzer_usable_count",
    "analyzer_coverage_text",
    "analyzer_integrity",
    "analyzer_missing_labels",
    "analyzer_unusable_labels",
    "stability_time_s",
    "total_time_s",
}
RUNTIME_CONTEXT_FLOAT_KEYS = {
    "temp_set_c",
    "pressure_target_hpa",
    "co2_ppm_target",
    "h2o_mmol_target",
    "pressure_hpa",
    "dewpoint_c",
    "dew_temp_c",
    "dew_rh_pct",
    "dew_pressure_hpa",
    "chamber_temp_c",
    "chamber_rh_pct",
    "stability_time_s",
    "total_time_s",
}
RUNTIME_CONTEXT_INT_KEYS = {
    "analyzer_expected_count",
    "analyzer_with_frame_count",
    "analyzer_usable_count",
}
RUNTIME_CONTEXT_BOOL_KEYS = {"point_is_h2o"}


def _coerce_float(value: Any) -> float | None:
    if value in (None, "", "null", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any, *, default: int | None = None) -> int | None:
    if value in (None, "", "null", "None"):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return default
    text_value = str(value).strip().lower()
    if text_value in {"1", "true", "yes", "y", "passed", "pass"}:
        return True
    if text_value in {"0", "false", "no", "n", "failed", "fail"}:
        return False
    return default


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, "", "null", "None"):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text_value = str(value).strip()
    if not text_value:
        return None
    if text_value.endswith("Z"):
        text_value = f"{text_value[:-1]}+00:00"
    parsed = datetime.fromisoformat(text_value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _normalize_route(value: Any) -> str:
    route = str(value or "co2").strip().lower()
    return "h2o" if route in {"h2o", "water", "humidity"} else "co2"


def _normalize_run_status(value: Any) -> str:
    status = str(value or "running").strip().lower()
    if status in {"completed", "done", "success"}:
        return "completed"
    if status in {"failed", "error"}:
        return "failed"
    if status in {"aborted", "stopped", "cancelled"}:
        return "aborted"
    return "running"


def _normalize_point_status(value: Any) -> str:
    status = str(value or "pending").strip().lower()
    if status in {"done", "success"}:
        return "completed"
    if status in {"error"}:
        return "failed"
    if status in {"pending", "running", "completed", "failed", "skipped"}:
        return status
    return "pending"


def _normalize_alarm_severity(value: Any) -> str:
    severity = str(value or "info").strip().lower()
    if severity not in {"info", "warning", "error", "critical"}:
        return "info"
    return severity


def _json_notes(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _string_or_none(value: Any) -> str | None:
    if value in (None, "", "null", "None"):
        return None
    text_value = str(value).strip()
    return text_value or None


def _normalize_channel_type(value: Any, *, default: str = "co2_h2o_dual") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"co2_only", "co2"}:
        return "co2_only"
    if normalized in {"h2o_only", "h2o", "water_only"}:
        return "h2o_only"
    if normalized in {"co2_h2o_dual", "dual", "co2+h2o"}:
        return "co2_h2o_dual"
    return str(default or "co2_h2o_dual").strip().lower() or "co2_h2o_dual"


def _normalized_identity_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _build_sensor_device_key(
    *,
    analyzer_id: Any,
    analyzer_serial: Any,
    channel_type: str,
) -> str:
    serial_token = _normalized_identity_token(analyzer_serial)
    analyzer_token = _normalized_identity_token(analyzer_id)
    identity_token = serial_token or analyzer_token or "unknown"
    return f"{_normalize_channel_type(channel_type)}:{identity_token}"


def _runtime_payload_value(value: Any) -> Any:
    if value in (None, ""):
        return None
    return value


def _runtime_context_value(key: str, value: Any) -> Any:
    if value in (None, ""):
        return None
    if key in RUNTIME_CONTEXT_BOOL_KEYS:
        return _coerce_bool(value, default=False)
    if key in RUNTIME_CONTEXT_INT_KEYS:
        return _coerce_int(value)
    if key in RUNTIME_CONTEXT_FLOAT_KEYS:
        return _coerce_float(value)
    if key.startswith("hgen_"):
        coerced = _coerce_float(value)
        return coerced if coerced is not None else _string_or_none(value)
    return _string_or_none(value)


def _runtime_analyzer_fields(fieldnames: list[str] | None) -> dict[str, dict[str, str]]:
    labels: dict[str, dict[str, str]] = {}
    for fieldname in fieldnames or []:
        match = RUNTIME_ANALYZER_FIELD_PATTERN.match(fieldname)
        if match is None:
            continue
        label, suffix = match.groups()
        labels.setdefault(label.lower(), {})[suffix] = fieldname
    return {label: labels[label] for label in sorted(labels)}


def _runtime_frame_present(row: dict[str, Any], field_map: dict[str, str]) -> bool:
    for suffix in RUNTIME_FRAME_PRESENCE_SUFFIXES:
        field_name = field_map.get(suffix)
        if field_name is None:
            continue
        if row.get(field_name) not in (None, ""):
            return True
    return False


class ArtifactImporter:
    def __init__(self, database: DatabaseManager):
        self.database = database

    @staticmethod
    def _sensor_cache(session: Session) -> dict[str, SensorRecord]:
        cache = session.info.get("_sensor_record_cache")
        if isinstance(cache, dict):
            return cache
        cache = {}
        session.info["_sensor_record_cache"] = cache
        return cache

    def import_raw_run_directory(
        self,
        run_dir: str | Path,
        *,
        operator: str | None = None,
        batch_size: int = 500,
    ) -> dict[str, Any]:
        directory = Path(run_dir)
        summary_path = directory / "summary.json"
        manifest_path = directory / "manifest.json"
        if summary_path.exists():
            summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        else:
            summary_payload = {"run_id": directory.name, "status": {"phase": "completed"}, "stats": {}}
        manifest_payload = {}
        if manifest_path.exists():
            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        with self.database.session_scope() as session:
            run_id = self._import_run_summary(
                session,
                summary_payload=summary_payload,
                summary_path=summary_path if summary_path.exists() else None,
                manifest_payload=manifest_payload or None,
                operator=operator,
            )
            point_map = self._import_points(session, run_id, directory)
            sample_count = self._import_samples(
                session,
                run_id,
                point_map,
                directory / "samples.csv",
                batch_size=batch_size,
                manifest_payload=manifest_payload or None,
            )
            measurement_frame_count = self._import_measurement_frames(
                session,
                run_id,
                point_map,
                directory / "samples_runtime.csv",
                batch_size=batch_size,
                manifest_payload=manifest_payload or None,
            )
            event_count = self._import_device_events(session, run_id, directory / "io_log.csv")
            alarm_count = self._import_alarm_incidents(session, run_id, directory / "run.log")
            self._refresh_run_counters(session, run_id)
            return {
                "run_id": str(run_id),
                "stage": "raw",
                "points": len(point_map),
                "samples": sample_count,
                "measurement_frames": measurement_frame_count,
                "device_events": event_count,
                "alarms_incidents": alarm_count,
                "manifest_loaded": bool(manifest_payload),
            }

    def import_enrich_run_directory(
        self,
        run_dir: str | Path,
        *,
        artifact_dir: str | Path | None = None,
        operator: str | None = None,
    ) -> dict[str, Any]:
        directory = Path(run_dir)
        enrich_directory = Path(artifact_dir) if artifact_dir is not None else directory
        summary_path = directory / "summary.json"
        manifest_path = directory / "manifest.json"
        if summary_path.exists():
            summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        else:
            summary_payload = {"run_id": directory.name, "status": {"phase": "completed"}, "stats": {}}
        manifest_payload = {}
        if manifest_path.exists():
            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        with self.database.session_scope() as session:
            run_id = self._import_run_summary(
                session,
                summary_payload=summary_payload,
                summary_path=summary_path if summary_path.exists() else None,
                manifest_payload=manifest_payload or None,
                operator=operator,
            )
            point_map = self._import_points(session, run_id, directory)
            qc_count = self._import_qc_results(session, point_map, enrich_directory / "qc_report.json")
            fit_count = self._import_fit_results(
                session,
                run_id,
                directory / "results.json",
                manifest_payload=manifest_payload or None,
            )
            metadata = self._import_enrich_metadata(
                session,
                run_id,
                run_dir=directory,
                artifact_dir=enrich_directory,
                qc_results_count=qc_count,
                fit_results_count=fit_count,
            )
            self._refresh_run_counters(session, run_id)
            return {
                "run_id": str(run_id),
                "stage": "enrich",
                "qc_results": qc_count,
                "fit_results": fit_count,
                **metadata,
            }

    def import_run_directory(
        self,
        run_dir: str | Path,
        *,
        operator: str | None = None,
        batch_size: int = 500,
        artifact_dir: str | Path | None = None,
    ) -> dict[str, Any]:
        raw_result = self.import_raw_run_directory(
            run_dir,
            operator=operator,
            batch_size=batch_size,
        )
        enrich_result = self.import_enrich_run_directory(
            run_dir,
            artifact_dir=artifact_dir,
            operator=operator,
        )
        return {
            "run_id": raw_result["run_id"],
            "points": raw_result["points"],
            "samples": raw_result["samples"],
            "measurement_frames": raw_result["measurement_frames"],
            "qc_results": enrich_result["qc_results"],
            "fit_results": enrich_result["fit_results"],
            "device_events": raw_result["device_events"],
            "alarms_incidents": raw_result["alarms_incidents"],
            "raw": raw_result,
            "enrich": enrich_result,
        }

    @staticmethod
    def _manifest_workflow_payload(manifest_payload: dict[str, Any] | None) -> dict[str, Any]:
        manifest = manifest_payload or {}
        config_snapshot = manifest.get("config_snapshot")
        if isinstance(config_snapshot, dict):
            workflow = config_snapshot.get("workflow")
            if isinstance(workflow, dict):
                return workflow
        return {}

    @classmethod
    def _manifest_value(
        cls,
        manifest_payload: dict[str, Any] | None,
        key: str,
        default: Any = None,
    ) -> Any:
        manifest = manifest_payload or {}
        value = manifest.get(key)
        if value not in (None, ""):
            return value
        workflow = cls._manifest_workflow_payload(manifest_payload)
        if workflow.get(key) not in (None, ""):
            return workflow.get(key)
        return default

    @classmethod
    def _manifest_run_metadata(cls, manifest_payload: dict[str, Any] | None) -> dict[str, Any]:
        report_templates = cls._manifest_value(manifest_payload, "report_templates", {})
        analyzer_setup = cls._manifest_value(manifest_payload, "analyzer_setup", {})
        return {
            "run_mode": _string_or_none(cls._manifest_value(manifest_payload, "run_mode")),
            "route_mode": _string_or_none(cls._manifest_value(manifest_payload, "route_mode")),
            "profile_name": _string_or_none(cls._manifest_value(manifest_payload, "profile_name")),
            "profile_version": _string_or_none(cls._manifest_value(manifest_payload, "profile_version")),
            "report_family": _string_or_none(cls._manifest_value(manifest_payload, "report_family")),
            "report_templates": report_templates if isinstance(report_templates, dict) else {},
            "analyzer_setup": analyzer_setup if isinstance(analyzer_setup, dict) else {},
        }

    @classmethod
    def _sensor_context_from_manifest(cls, manifest_payload: dict[str, Any] | None) -> dict[str, Any]:
        run_metadata = cls._manifest_run_metadata(manifest_payload)
        analyzer_setup = dict(run_metadata.get("analyzer_setup") or {})
        return {
            "software_version": _string_or_none(
                analyzer_setup.get("software_version") or cls._manifest_value(manifest_payload, "software_version")
            ),
            "channel_type": "co2_h2o_dual",
            "metadata": {
                "legacy": {
                    "profile_name": run_metadata.get("profile_name"),
                    "profile_version": run_metadata.get("profile_version"),
                },
                "analyzer_setup": analyzer_setup,
            },
        }

    def _resolve_sensor_id(
        self,
        session: Session,
        *,
        analyzer_id: Any,
        analyzer_serial: Any,
        software_version: Any = None,
        model: Any = None,
        channel_type: str = "co2_h2o_dual",
        metadata_patch: dict[str, Any] | None = None,
    ):
        analyzer_text = _string_or_none(analyzer_id)
        serial_text = _string_or_none(analyzer_serial)
        normalized_channel_type = _normalize_channel_type(channel_type)
        cache = self._sensor_cache(session)
        existing = None
        if serial_text is None and analyzer_text:
            cached_candidates = [
                record
                for record in cache.values()
                if isinstance(record, SensorRecord)
                and record.channel_type == normalized_channel_type
                and record.analyzer_id == analyzer_text
            ]
            if len({str(record.sensor_id) for record in cached_candidates}) == 1 and cached_candidates:
                existing = cached_candidates[0]
            if existing is None:
                pending_candidates = [
                    record
                    for record in session.new
                    if isinstance(record, SensorRecord)
                    and record.channel_type == normalized_channel_type
                    and record.analyzer_id == analyzer_text
                ]
                if len({str(record.sensor_id) for record in pending_candidates}) == 1 and pending_candidates:
                    existing = pending_candidates[0]
            if existing is None:
                db_candidates = session.execute(
                    select(SensorRecord).where(
                        SensorRecord.channel_type == normalized_channel_type,
                        SensorRecord.analyzer_id == analyzer_text,
                    )
                ).scalars().all()
                if len({str(record.sensor_id) for record in db_candidates}) == 1 and db_candidates:
                    existing = db_candidates[0]

        device_key = (
            existing.device_key
            if existing is not None
            else _build_sensor_device_key(
                analyzer_id=analyzer_text,
                analyzer_serial=serial_text,
                channel_type=normalized_channel_type,
            )
        )
        sensor_id = existing.sensor_id if existing is not None else stable_uuid("sensor", device_key)
        existing = existing or cache.get(device_key) or cache.get(str(sensor_id))
        if existing is None:
            for pending in session.new:
                if isinstance(pending, SensorRecord) and (
                    pending.device_key == device_key or pending.sensor_id == sensor_id
                ):
                    existing = pending
                    break
        if existing is None:
            existing = session.execute(
                select(SensorRecord).where(
                    (SensorRecord.sensor_id == sensor_id) | (SensorRecord.device_key == device_key)
                )
            ).scalars().first()
        if existing is not None:
            cache[device_key] = existing
            cache[str(existing.sensor_id)] = existing
        base_metadata = dict(getattr(existing, "metadata_json", {}) or {})
        metadata = self._merge_notes(
            base_metadata,
            dict(metadata_patch or {}),
        )
        if existing is None:
            existing = SensorRecord(
                sensor_id=sensor_id,
                device_key=device_key,
                analyzer_id=analyzer_text,
                analyzer_serial=serial_text,
                software_version=_string_or_none(software_version),
                model=_string_or_none(model),
                channel_type=normalized_channel_type,
                metadata_json=metadata,
            )
            session.add(existing)
        else:
            existing.device_key = device_key
            existing.channel_type = normalized_channel_type
            if analyzer_text:
                existing.analyzer_id = analyzer_text
            if serial_text:
                existing.analyzer_serial = serial_text
            if _string_or_none(software_version):
                existing.software_version = _string_or_none(software_version)
            if _string_or_none(model):
                existing.model = _string_or_none(model)
            existing.metadata_json = metadata
        cache[device_key] = existing
        cache[str(existing.sensor_id)] = existing
        return sensor_id

    def _resolve_sensor_id_for_analyzer(
        self,
        session: Session,
        *,
        analyzer_id: Any,
        analyzer_serial: Any = None,
        manifest_payload: dict[str, Any] | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ):
        context = self._sensor_context_from_manifest(manifest_payload)
        merged_metadata = self._merge_notes(
            dict(context.get("metadata") or {}),
            dict(metadata_patch or {}),
        )
        return self._resolve_sensor_id(
            session,
            analyzer_id=analyzer_id,
            analyzer_serial=analyzer_serial,
            software_version=context.get("software_version"),
            channel_type=str(context.get("channel_type", "co2_h2o_dual") or "co2_h2o_dual"),
            metadata_patch=merged_metadata,
        )

    def _import_run_summary(
        self,
        session: Session,
        *,
        summary_payload: dict[str, Any],
        summary_path: Path | None,
        manifest_payload: dict[str, Any] | None,
        operator: str | None,
    ):
        source_run_id = str(summary_payload.get("run_id") or "unknown_run")
        run_id = stable_uuid("run", source_run_id)
        status_payload = summary_payload.get("status") or {}
        stats_payload = summary_payload.get("stats") or {}
        manifest = manifest_payload or {}
        run_metadata = self._manifest_run_metadata(manifest_payload)
        generated_at = _parse_datetime(summary_payload.get("generated_at")) or datetime.now(timezone.utc)
        elapsed_s = max(0.0, _coerce_float(status_payload.get("elapsed_s")) or 0.0)
        existing = session.get(RunRecord, run_id)
        merged_notes = self._merge_notes(
            self._load_notes(None if existing is None else existing.notes),
            {
                "source_run_id": source_run_id,
                "summary_notes": summary_payload.get("notes"),
                "enabled_devices": stats_payload.get("enabled_devices", []),
                "source_points_file": manifest.get("source_points_file"),
                "manifest_schema_version": manifest.get("schema_version"),
                "run_mode": run_metadata.get("run_mode"),
                "route_mode": run_metadata.get("route_mode"),
                "profile_name": run_metadata.get("profile_name"),
                "profile_version": run_metadata.get("profile_version"),
                "report_family": run_metadata.get("report_family"),
                "report_templates": run_metadata.get("report_templates"),
                "analyzer_setup": run_metadata.get("analyzer_setup"),
                "raw": {
                    "source_run_id": source_run_id,
                    "summary_notes": summary_payload.get("notes"),
                    "enabled_devices": stats_payload.get("enabled_devices", []),
                    "source_points_file": manifest.get("source_points_file"),
                    "manifest_schema_version": manifest.get("schema_version"),
                    "manifest_present": bool(manifest),
                    "config_snapshot_present": bool(manifest.get("config_snapshot")),
                    "device_snapshot_present": bool(manifest.get("device_snapshot")),
                    "run_mode": run_metadata.get("run_mode"),
                    "route_mode": run_metadata.get("route_mode"),
                    "profile_name": run_metadata.get("profile_name"),
                    "profile_version": run_metadata.get("profile_version"),
                    "report_family": run_metadata.get("report_family"),
                },
            },
        )
        record = RunRecord(
            id=run_id,
            start_time=generated_at - timedelta(seconds=elapsed_s),
            end_time=generated_at,
            status=_normalize_run_status(status_payload.get("phase")),
            config_hash=self._file_hash(summary_path),
            software_version=str(
                summary_payload.get("software_version")
                or stats_payload.get("software_version")
                or manifest.get("software_version")
                or "v2"
            ),
            run_mode=run_metadata.get("run_mode"),
            route_mode=run_metadata.get("route_mode"),
            profile_name=run_metadata.get("profile_name"),
            profile_version=run_metadata.get("profile_version"),
            report_family=run_metadata.get("report_family"),
            report_templates=dict(run_metadata.get("report_templates") or {}),
            analyzer_setup=dict(run_metadata.get("analyzer_setup") or {}),
            operator=operator or summary_payload.get("operator") or stats_payload.get("operator"),
            total_points=_coerce_int(stats_payload.get("total_points"), default=_coerce_int(status_payload.get("total_points"), default=0)) or 0,
            successful_points=_coerce_int(stats_payload.get("successful_points"), default=_coerce_int(status_payload.get("completed_points"), default=0)) or 0,
            failed_points=_coerce_int(stats_payload.get("failed_points"), default=0) or 0,
            warnings=_coerce_int(stats_payload.get("warning_count"), default=0) or 0,
            errors=_coerce_int(stats_payload.get("error_count"), default=0) or 0,
            notes=_json_notes(merged_notes),
        )
        session.merge(record)
        session.flush()
        return run_id

    def _import_points(self, session: Session, run_id, run_dir: Path) -> dict[int, Any]:
        candidates = self._collect_point_candidates(run_dir)
        point_map: dict[int, Any] = {}
        for sequence, payload in sorted(candidates.items()):
            point_id = stable_uuid("point", run_id, sequence)
            record = PointRecord(
                id=point_id,
                run_id=run_id,
                sequence=sequence,
                temperature_c=_coerce_float(payload.get("temperature_c")),
                humidity_rh=_coerce_float(payload.get("humidity_pct", payload.get("humidity_rh"))),
                pressure_hpa=_coerce_float(payload.get("pressure_hpa")),
                route_type=_normalize_route(payload.get("route", payload.get("route_type"))),
                co2_target_ppm=_coerce_float(payload.get("co2_ppm", payload.get("co2_target_ppm"))),
                co2_group=_string_or_none(payload.get("co2_group")),
                cylinder_nominal_ppm=_coerce_float(payload.get("cylinder_nominal_ppm", payload.get("nominal_ppm"))),
                status=_normalize_point_status(payload.get("status")),
                stability_time_s=_coerce_float(payload.get("stability_time_s")),
                total_time_s=_coerce_float(payload.get("total_time_s")),
                retry_count=_coerce_int(payload.get("retry_count"), default=0) or 0,
            )
            session.merge(record)
            point_map[sequence] = point_id
        session.flush()
        return point_map

    def _collect_point_candidates(self, run_dir: Path) -> dict[int, dict[str, Any]]:
        candidates: dict[int, dict[str, Any]] = {}

        point_summary_path = run_dir / "point_summaries.json"
        if point_summary_path.exists():
            payload = json.loads(point_summary_path.read_text(encoding="utf-8"))
            for item in payload:
                point = dict(item.get("point") or {})
                stats = dict(item.get("stats") or {})
                sequence = _coerce_int(point.get("index"))
                if sequence is None:
                    continue
                candidates.setdefault(sequence, {}).update(point)
                candidates[sequence].update(
                    {
                        "status": "completed" if _coerce_bool(stats.get("valid"), default=True) else "failed",
                        "stability_time_s": stats.get("stability_time_s"),
                        "total_time_s": stats.get("total_time_s"),
                    }
                )

        points_csv = run_dir / "points.csv"
        if points_csv.exists():
            with points_csv.open("r", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    sequence = _coerce_int(row.get("point_index"))
                    if sequence is None:
                        continue
                    candidates.setdefault(sequence, {}).update(row)

        samples_csv = run_dir / "samples.csv"
        if samples_csv.exists():
            with samples_csv.open("r", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    sequence = _coerce_int(row.get("point_index"))
                    if sequence is None or sequence in candidates:
                        continue
                    candidates[sequence] = {
                        "temperature_c": row.get("temperature_c"),
                        "co2_ppm": row.get("co2_ppm"),
                        "co2_group": row.get("co2_group"),
                        "cylinder_nominal_ppm": row.get("cylinder_nominal_ppm"),
                        "humidity_pct": row.get("humidity_pct"),
                        "pressure_hpa": row.get("pressure_hpa"),
                        "route": row.get("route"),
                        "status": "completed",
                    }
        return candidates

    def _import_samples(
        self,
        session: Session,
        run_id,
        point_map: dict[int, Any],
        samples_path: Path,
        *,
        batch_size: int,
        manifest_payload: dict[str, Any] | None = None,
    ) -> int:
        if not samples_path.exists():
            return 0

        processed = 0
        sample_counters: dict[tuple[int, str, str], int] = {}
        with samples_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                point_index = _coerce_int(row.get("point_index"))
                if point_index is None or point_index not in point_map:
                    continue
                analyzer_id = str(row.get("analyzer_id") or "unknown_analyzer")
                analyzer_serial = str(row.get("analyzer_serial") or "").strip()
                counter_key = (point_index, analyzer_id, analyzer_serial)
                sample_index = _coerce_int(row.get("sample_index"))
                if sample_index is None:
                    sample_counters[counter_key] = sample_counters.get(counter_key, 0) + 1
                    sample_index = sample_counters[counter_key]
                point_id = point_map[point_index]
                sensor_id = self._resolve_sensor_id_for_analyzer(
                    session,
                    analyzer_id=analyzer_id,
                    analyzer_serial=analyzer_serial or None,
                    manifest_payload=manifest_payload,
                    metadata_patch={
                        "legacy": {
                            "analyzer_id": analyzer_id,
                            "analyzer_serial": analyzer_serial or None,
                        },
                        "source": {
                            "artifact": "samples.csv",
                            "run_id": str(run_id),
                        },
                    },
                )
                record = SampleRecord(
                    id=stable_uuid("sample", run_id, point_index, analyzer_id, analyzer_serial, sample_index),
                    point_id=point_id,
                    sensor_id=sensor_id,
                    analyzer_id=analyzer_id,
                    analyzer_serial=analyzer_serial or None,
                    sample_index=sample_index,
                    timestamp=_parse_datetime(row.get("timestamp") or row.get("sample_ts")),
                    co2_ppm=_coerce_float(row.get("sample_co2_ppm", row.get("co2_ppm"))),
                    h2o_mmol=_coerce_float(row.get("sample_h2o_mmol", row.get("h2o_mmol"))),
                    pressure_hpa=_coerce_float(row.get("pressure_hpa")),
                    co2_ratio_f=_coerce_float(row.get("co2_ratio_f")),
                    h2o_ratio_f=_coerce_float(row.get("h2o_ratio_f")),
                    co2_ratio_raw=_coerce_float(row.get("co2_ratio_raw")),
                    h2o_ratio_raw=_coerce_float(row.get("h2o_ratio_raw")),
                    chamber_temp_c=_coerce_float(row.get("analyzer_chamber_temp_c", row.get("chamber_temp_c"))),
                    case_temp_c=_coerce_float(row.get("case_temp_c")),
                    dewpoint_c=_coerce_float(row.get("dew_point_c", row.get("dewpoint_c"))),
                )
                session.merge(record)
                processed += 1
                if processed % batch_size == 0:
                    session.flush()
        session.flush()
        return processed

    def _import_measurement_frames(
        self,
        session: Session,
        run_id,
        point_map: dict[int, Any],
        samples_runtime_path: Path,
        *,
        batch_size: int,
        manifest_payload: dict[str, Any] | None = None,
    ) -> int:
        if not samples_runtime_path.exists():
            return 0

        processed = 0
        with samples_runtime_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            analyzer_fields = _runtime_analyzer_fields(reader.fieldnames)
            if not analyzer_fields:
                return 0
            for row in reader:
                point_index = _coerce_int(row.get("point_index"))
                if point_index is None or point_index not in point_map:
                    continue
                point_id = point_map[point_index]
                sample_index = _coerce_int(row.get("sample_index"), default=0) or 0
                sample_ts = _parse_datetime(row.get("sample_ts"))
                context_payload = self._build_runtime_context_payload(row)
                for analyzer_label, field_map in analyzer_fields.items():
                    if not _runtime_frame_present(row, field_map):
                        continue
                    analyzer_id = _string_or_none(self._runtime_group_value(row, field_map, "id")) or analyzer_label
                    analyzer_serial = _string_or_none(self._runtime_group_value(row, field_map, "serial"))
                    sensor_id = self._resolve_sensor_id_for_analyzer(
                        session,
                        analyzer_id=analyzer_id,
                        analyzer_serial=analyzer_serial,
                        manifest_payload=manifest_payload,
                        metadata_patch={
                            "legacy": {
                                "analyzer_id": analyzer_id,
                                "analyzer_serial": analyzer_serial,
                                "analyzer_label": analyzer_label,
                            },
                            "source": {
                                "artifact": "samples_runtime.csv",
                                "run_id": str(run_id),
                            },
                        },
                    )
                    record = MeasurementFrameRecord(
                        id=stable_uuid(
                            "measurement_frame",
                            run_id,
                            point_index,
                            analyzer_label,
                            sample_index,
                            sample_ts.isoformat() if sample_ts is not None else "",
                        ),
                        run_id=run_id,
                        point_id=point_id,
                        sensor_id=sensor_id,
                        sample_index=sample_index,
                        sample_ts=sample_ts,
                        analyzer_label=analyzer_label,
                        analyzer_id=analyzer_id,
                        analyzer_serial=analyzer_serial,
                        frame_has_data=_coerce_bool(self._runtime_group_value(row, field_map, "frame_has_data")),
                        frame_usable=_coerce_bool(self._runtime_group_value(row, field_map, "frame_usable")),
                        analyzer_status=_string_or_none(self._runtime_group_value(row, field_map, "status")),
                        mode=_string_or_none(self._runtime_group_value(row, field_map, "mode")),
                        mode2_field_count=_coerce_int(self._runtime_group_value(row, field_map, "mode2_field_count")),
                        co2_ppm=_coerce_float(
                            self._runtime_group_value(row, field_map, "co2_ppm", fallback_keys=("co2_ppm",))
                        ),
                        h2o_mmol=_coerce_float(
                            self._runtime_group_value(row, field_map, "h2o_mmol", fallback_keys=("h2o_mmol",))
                        ),
                        co2_ratio_f=_coerce_float(
                            self._runtime_group_value(row, field_map, "co2_ratio_f", fallback_keys=("co2_ratio_f",))
                        ),
                        h2o_ratio_f=_coerce_float(
                            self._runtime_group_value(row, field_map, "h2o_ratio_f", fallback_keys=("h2o_ratio_f",))
                        ),
                        co2_ratio_raw=_coerce_float(self._runtime_group_value(row, field_map, "co2_ratio_raw")),
                        h2o_ratio_raw=_coerce_float(self._runtime_group_value(row, field_map, "h2o_ratio_raw")),
                        ref_signal=_coerce_float(
                            self._runtime_group_value(row, field_map, "ref_signal", fallback_keys=("ref_signal",))
                        ),
                        co2_signal=_coerce_float(
                            self._runtime_group_value(row, field_map, "co2_signal", fallback_keys=("co2_signal",))
                        ),
                        h2o_signal=_coerce_float(
                            self._runtime_group_value(row, field_map, "h2o_signal", fallback_keys=("h2o_signal",))
                        ),
                        chamber_temp_c=_coerce_float(
                            self._runtime_group_value(
                                row,
                                field_map,
                                "chamber_temp_c",
                                fallback_keys=("analyzer_chamber_temp_c", "chamber_temp_c"),
                            )
                        ),
                        case_temp_c=_coerce_float(
                            self._runtime_group_value(row, field_map, "case_temp_c", fallback_keys=("case_temp_c",))
                        ),
                        pressure_kpa=_coerce_float(self._runtime_group_value(row, field_map, "pressure_kpa")),
                        raw_payload=self._build_runtime_raw_payload(row, field_map),
                        context_payload=dict(context_payload),
                    )
                    session.merge(record)
                    processed += 1
                    if processed % batch_size == 0:
                        session.flush()
        session.flush()
        return processed

    def _import_qc_results(self, session: Session, point_map: dict[int, Any], report_path: Path) -> int:
        if not report_path.exists():
            return 0
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        processed = 0
        for detail in payload.get("point_details", []):
            point_index = _coerce_int(detail.get("point_index"))
            if point_index is None or point_index not in point_map:
                continue
            point_id = point_map[point_index]
            session.merge(
                QCResultRecord(
                    id=stable_uuid("qc", point_id, "overall_quality"),
                    point_id=point_id,
                    rule_name="overall_quality",
                    passed=_coerce_bool(detail.get("valid"), default=False),
                    value=_coerce_float(detail.get("quality_score")),
                    threshold=None,
                    message=str(detail.get("recommendation") or detail.get("reason") or ""),
                )
            )
            processed += 1
            reason = str(detail.get("reason") or "").strip()
            for token in [item.strip() for item in reason.split(",") if item.strip()]:
                session.merge(
                    QCResultRecord(
                        id=stable_uuid("qc", point_id, token),
                        point_id=point_id,
                        rule_name=token.split("<", 1)[0].split("=", 1)[0],
                        passed=False,
                        value=None,
                        threshold=None,
                        message=token,
                    )
                )
                processed += 1
        session.flush()
        return processed

    def _import_fit_results(
        self,
        session: Session,
        run_id,
        results_path: Path,
        *,
        manifest_payload: dict[str, Any] | None = None,
    ) -> int:
        if not results_path.exists():
            return 0
        payload = json.loads(results_path.read_text(encoding="utf-8"))
        fit_payloads = self._collect_fit_payloads(payload)

        processed = 0
        for item in fit_payloads:
            coefficients = item.get("coefficients")
            if not isinstance(coefficients, dict):
                continue
            analyzer_id = str(item.get("analyzer_id") or "aggregate")
            analyzer_serial = _string_or_none(item.get("analyzer_serial"))
            algorithm = str(item.get("algorithm") or item.get("algorithm_name") or "unknown")
            sensor_id = self._resolve_sensor_id_for_analyzer(
                session,
                analyzer_id=analyzer_id,
                analyzer_serial=analyzer_serial,
                manifest_payload=manifest_payload,
                metadata_patch={
                    "legacy": {
                        "analyzer_id": analyzer_id,
                        "analyzer_serial": analyzer_serial,
                    },
                    "source": {
                        "artifact": "results.json",
                        "run_id": str(run_id),
                    },
                },
            )
            session.merge(
                FitResultRecord(
                    id=stable_uuid("fit", run_id, analyzer_id, algorithm),
                    run_id=run_id,
                    sensor_id=sensor_id,
                    analyzer_id=analyzer_id,
                    algorithm=algorithm,
                    coefficients=coefficients,
                    rmse=_coerce_float(item.get("rmse")),
                    r_squared=_coerce_float(item.get("r_squared")),
                    n_points=_coerce_int(item.get("n_points"), default=0),
                )
            )
            processed += 1
        session.flush()
        return processed

    def _import_device_events(self, session: Session, run_id, io_log_path: Path) -> int:
        if not io_log_path.exists():
            return 0
        processed = 0
        with io_log_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                session.merge(
                    DeviceEventRecord(
                        id=stable_uuid("device_event", run_id, row.get("device"), row.get("direction"), row.get("timestamp"), row.get("data")),
                        run_id=run_id,
                        device_name=str(row.get("device") or "unknown_device"),
                        event_type=str(row.get("direction") or "io").strip().lower() or "io",
                        event_data={"data": row.get("data")},
                        timestamp=_parse_datetime(row.get("timestamp")),
                    )
                )
                processed += 1
        session.flush()
        return processed

    def _import_alarm_incidents(self, session: Session, run_id, log_path: Path) -> int:
        if not log_path.exists():
            return 0
        processed = 0
        for line in log_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            message = str(payload.get("message") or "").strip()
            if not message:
                continue
            session.merge(
                AlarmIncidentRecord(
                    id=stable_uuid("alarm", run_id, payload.get("level"), message, payload.get("timestamp")),
                    run_id=run_id,
                    severity=_normalize_alarm_severity(payload.get("level")),
                    category="runtime_log",
                    message=message,
                    details=payload.get("context") if isinstance(payload.get("context"), dict) else {},
                    timestamp=_parse_datetime(payload.get("timestamp")),
                    resolved=False,
                    resolved_at=None,
                )
            )
            processed += 1
        session.flush()
        return processed

    def _refresh_run_counters(self, session: Session, run_id) -> None:
        run = session.get(RunRecord, run_id)
        if run is None:
            return
        points = session.execute(select(PointRecord).where(PointRecord.run_id == run_id)).scalars().all()
        run.total_points = len(points) or run.total_points
        run.successful_points = sum(1 for point in points if point.status == "completed")
        run.failed_points = sum(1 for point in points if point.status == "failed")
        session.flush()

    @staticmethod
    def _file_hash(path: Path | None) -> str | None:
        if path is None or not path.exists():
            return None
        return sha256(path.read_bytes()).hexdigest()

    @staticmethod
    def _load_notes(value: Any) -> dict[str, Any]:
        if not value:
            return {}
        if isinstance(value, dict):
            return dict(value)
        try:
            payload = json.loads(str(value))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _runtime_group_value(
        row: dict[str, Any],
        field_map: dict[str, str],
        suffix: str,
        *,
        fallback_keys: tuple[str, ...] = (),
    ) -> Any:
        field_name = field_map.get(suffix)
        if field_name is not None and row.get(field_name) not in (None, ""):
            return row.get(field_name)
        for fallback_key in fallback_keys:
            if row.get(fallback_key) not in (None, ""):
                return row.get(fallback_key)
        return None

    @staticmethod
    def _build_runtime_raw_payload(row: dict[str, Any], field_map: dict[str, str]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for field_name in sorted(field_map.values()):
            value = _runtime_payload_value(row.get(field_name))
            if value is not None:
                payload[field_name] = value
        return payload

    @staticmethod
    def _build_runtime_context_payload(row: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for key in sorted(row):
            if key in RUNTIME_CONTEXT_KEYS or key.startswith("hgen_"):
                payload[key] = _runtime_context_value(key, row.get(key))
        return payload

    @classmethod
    def _merge_notes(cls, base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base)
        for key, value in patch.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = cls._merge_notes(dict(merged[key]), value)
            else:
                merged[key] = value
        return merged

    @staticmethod
    def _collect_fit_payloads(payload: dict[str, Any]) -> list[dict[str, Any]]:
        if isinstance(payload.get("fit_results"), list):
            return [item for item in payload["fit_results"] if isinstance(item, dict)]
        if isinstance(payload.get("fit_result"), dict):
            return [payload["fit_result"]]
        if isinstance(payload.get("coefficients"), dict):
            return [payload]
        return []

    def _import_enrich_metadata(
        self,
        session: Session,
        run_id,
        *,
        run_dir: Path,
        artifact_dir: Path,
        qc_results_count: int,
        fit_results_count: int,
    ) -> dict[str, Any]:
        record = session.get(RunRecord, run_id)
        if record is None:
            return {
                "metadata_sections": {},
                "skipped_artifacts": ["run record missing"],
            }

        skipped: list[str] = []
        qc_payload = self._read_json_if_exists(artifact_dir / "qc_report.json")
        postprocess_payload = self._read_json_if_exists(artifact_dir / "calibration_coefficients_postprocess_summary.json")
        ai_run_summary_path = artifact_dir / "ai_run_summary.md"
        ai_anomaly_note_path = artifact_dir / "ai_anomaly_note.md"
        results_path = run_dir / "results.json"
        fit_payloads = self._collect_fit_payloads(self._read_json_if_exists(results_path))

        if not qc_payload:
            skipped.append("qc_report.json")
        if not postprocess_payload:
            skipped.append("calibration_coefficients_postprocess_summary.json")
        if not ai_run_summary_path.exists():
            skipped.append("ai_run_summary.md")
        if not ai_anomaly_note_path.exists():
            skipped.append("ai_anomaly_note.md")
        if not results_path.exists():
            skipped.append("results.json")

        qc_metadata = (
            {
                "path": str((artifact_dir / "qc_report.json").resolve()),
                "total_points": qc_payload.get("total_points"),
                "valid_points": qc_payload.get("valid_points"),
                "invalid_points": qc_payload.get("invalid_points"),
                "overall_score": qc_payload.get("overall_score"),
                "grade": qc_payload.get("grade"),
                "imported_results": qc_results_count,
            }
            if qc_payload
            else {"status": "missing", "imported_results": qc_results_count}
        )

        coefficient_metadata = {
            "report_status": None,
            "report_path": None,
            "refit_status": None,
            "refit_run_count": 0,
            "completed_refit_count": 0,
        }
        postprocess_metadata: dict[str, Any] = {"status": "missing"}
        ai_metadata = {
            "status": "missing",
            "run_summary_path": None,
            "anomaly_note_path": None,
            "run_summary_preview": None,
        }
        if postprocess_payload:
            refit_runs = list(((postprocess_payload.get("refit") or {}).get("runs")) or [])
            coefficient_metadata = {
                "report_status": (postprocess_payload.get("report") or {}).get("status"),
                "report_path": (postprocess_payload.get("report") or {}).get("path"),
                "refit_status": (postprocess_payload.get("refit") or {}).get("status"),
                "refit_run_count": len(refit_runs),
                "completed_refit_count": sum(
                    1 for item in refit_runs if isinstance(item, dict) and item.get("status") == "completed"
                ),
            }
            ai_section = postprocess_payload.get("ai") or {}
            ai_metadata = {
                "status": ai_section.get("status", "missing"),
                "run_summary_path": (ai_section.get("run_summary") or {}).get("path"),
                "anomaly_note_path": (ai_section.get("anomaly_note") or {}).get("path"),
                "run_summary_preview": self._markdown_preview(ai_run_summary_path),
            }
            postprocess_metadata = {
                "status": "loaded",
                "path": str((artifact_dir / "calibration_coefficients_postprocess_summary.json").resolve()),
                "generated_at": postprocess_payload.get("generated_at"),
                "flags": postprocess_payload.get("flags", {}),
                "stages": {
                    name: ((postprocess_payload.get(name) or {}).get("status") if isinstance(postprocess_payload.get(name), dict) else None)
                    for name in ("manifest", "database_import", "qc", "report", "refit", "ai", "download")
                },
            }
        elif ai_run_summary_path.exists():
            ai_metadata = {
                "status": "loaded",
                "run_summary_path": str(ai_run_summary_path.resolve()),
                "anomaly_note_path": str(ai_anomaly_note_path.resolve()) if ai_anomaly_note_path.exists() else None,
                "run_summary_preview": self._markdown_preview(ai_run_summary_path),
            }

        enrich_patch = {
            "enrich": {
                "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "artifact_dir": str(artifact_dir.resolve()),
                "qc": qc_metadata,
                "fit": {
                    "results_path": str(results_path.resolve()) if results_path.exists() else None,
                    "fit_payload_count": len(fit_payloads),
                    "imported_results": fit_results_count,
                },
                "coefficient_metadata": coefficient_metadata,
                "ai_summary_metadata": ai_metadata,
                "postprocess_summary_metadata": postprocess_metadata,
                "skipped_artifacts": skipped,
            }
        }
        record.notes = _json_notes(self._merge_notes(self._load_notes(record.notes), enrich_patch))
        session.flush()
        return {
            "metadata_sections": {
                "qc": "loaded" if qc_payload else "missing",
                "fit": "loaded" if results_path.exists() else "missing",
                "coefficient_metadata": "loaded" if postprocess_payload else "missing",
                "ai_summary_metadata": "loaded" if ai_run_summary_path.exists() else "missing",
                "postprocess_summary_metadata": "loaded" if postprocess_payload else "missing",
            },
            "skipped_artifacts": skipped,
        }

    @staticmethod
    def _read_json_if_exists(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _markdown_preview(path: Path, *, max_chars: int = 240) -> str | None:
        if not path.exists():
            return None
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return None
        preview = " ".join(line.strip() for line in text.splitlines() if line.strip())
        if len(preview) <= max_chars:
            return preview
        return preview[:max_chars].rstrip() + "..."
