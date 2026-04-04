from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import Any

from sqlalchemy import and_, func, or_, select

from .database import DatabaseManager, resolve_run_uuid
from .models import (
    CoefficientVersionRecord,
    FitResultRecord,
    MeasurementFrameRecord,
    PointRecord,
    RunRecord,
    SampleRecord,
    SensorRecord,
)


def _serialize_run(record: RunRecord) -> dict[str, Any]:
    return {
        "id": str(record.id),
        "start_time": None if record.start_time is None else record.start_time.isoformat(),
        "end_time": None if record.end_time is None else record.end_time.isoformat(),
        "status": record.status,
        "config_hash": record.config_hash,
        "software_version": record.software_version,
        "run_mode": record.run_mode,
        "route_mode": record.route_mode,
        "profile_name": record.profile_name,
        "profile_version": record.profile_version,
        "report_family": record.report_family,
        "report_templates": dict(record.report_templates or {}),
        "analyzer_setup": dict(record.analyzer_setup or {}),
        "operator": record.operator,
        "total_points": record.total_points,
        "successful_points": record.successful_points,
        "failed_points": record.failed_points,
        "warnings": record.warnings,
        "errors": record.errors,
        "notes": record.notes,
    }


def _serialize_sample(record: SampleRecord) -> dict[str, Any]:
    return {
        "id": str(record.id),
        "point_id": str(record.point_id),
        "sensor_id": None if record.sensor_id is None else str(record.sensor_id),
        "analyzer_id": record.analyzer_id,
        "analyzer_serial": record.analyzer_serial,
        "sample_index": record.sample_index,
        "timestamp": None if record.timestamp is None else record.timestamp.isoformat(),
        "co2_ppm": record.co2_ppm,
        "h2o_mmol": record.h2o_mmol,
        "pressure_hpa": record.pressure_hpa,
        "co2_ratio_f": record.co2_ratio_f,
        "h2o_ratio_f": record.h2o_ratio_f,
        "co2_ratio_raw": record.co2_ratio_raw,
        "h2o_ratio_raw": record.h2o_ratio_raw,
        "chamber_temp_c": record.chamber_temp_c,
        "case_temp_c": record.case_temp_c,
        "dewpoint_c": record.dewpoint_c,
    }


def _serialize_sensor(record: SensorRecord) -> dict[str, Any]:
    return {
        "sensor_id": str(record.sensor_id),
        "device_key": record.device_key,
        "analyzer_id": record.analyzer_id,
        "analyzer_serial": record.analyzer_serial,
        "software_version": record.software_version,
        "model": record.model,
        "channel_type": record.channel_type,
        "metadata": dict(record.metadata_json or {}),
    }


def _serialize_measurement_frame(record: MeasurementFrameRecord) -> dict[str, Any]:
    return {
        "id": str(record.id),
        "run_id": str(record.run_id),
        "point_id": str(record.point_id),
        "sensor_id": None if record.sensor_id is None else str(record.sensor_id),
        "sample_index": record.sample_index,
        "sample_ts": None if record.sample_ts is None else record.sample_ts.isoformat(),
        "analyzer_label": record.analyzer_label,
        "analyzer_id": record.analyzer_id,
        "analyzer_serial": record.analyzer_serial,
        "frame_has_data": record.frame_has_data,
        "frame_usable": record.frame_usable,
        "analyzer_status": record.analyzer_status,
        "mode": record.mode,
        "co2_ppm": record.co2_ppm,
        "h2o_mmol": record.h2o_mmol,
        "co2_ratio_f": record.co2_ratio_f,
        "h2o_ratio_f": record.h2o_ratio_f,
        "pressure_kpa": record.pressure_kpa,
        "context_payload": dict(record.context_payload or {}),
    }


def _serialize_fit_result(record: FitResultRecord) -> dict[str, Any]:
    return {
        "id": str(record.id),
        "run_id": str(record.run_id),
        "sensor_id": None if record.sensor_id is None else str(record.sensor_id),
        "analyzer_id": record.analyzer_id,
        "algorithm": record.algorithm,
        "coefficients": dict(record.coefficients or {}),
        "rmse": record.rmse,
        "r_squared": record.r_squared,
        "n_points": record.n_points,
    }


def _serialize_coefficient_version(record: CoefficientVersionRecord) -> dict[str, Any]:
    return {
        "id": str(record.id),
        "sensor_id": None if record.sensor_id is None else str(record.sensor_id),
        "analyzer_id": record.analyzer_id,
        "analyzer_serial": record.analyzer_serial,
        "version": record.version,
        "coefficients": dict(record.coefficients or {}),
        "created_at": None if record.created_at is None else record.created_at.isoformat(),
        "created_by": record.created_by,
        "approved": record.approved,
        "deployed": record.deployed,
        "notes": record.notes,
    }


class HistoryQueryService:
    def __init__(self, database: DatabaseManager):
        self.database = database

    def runs_by_time_range(
        self,
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with self.database.session_scope() as session:
            stmt = select(RunRecord).order_by(RunRecord.start_time.desc().nullslast())
            if start_time is not None:
                stmt = stmt.where(RunRecord.start_time >= start_time)
            if end_time is not None:
                stmt = stmt.where(RunRecord.end_time <= end_time)
            if status:
                stmt = stmt.where(RunRecord.status == status)
            rows = session.execute(stmt.limit(limit)).scalars().all()
            return [_serialize_run(row) for row in rows]

    def query_runs_by_time_range(self, **kwargs) -> list[dict[str, Any]]:
        return self.runs_by_time_range(**kwargs)

    def runs_by_device(
        self,
        analyzer_id: str,
        *,
        analyzer_serial: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with self.database.session_scope() as session:
            stmt = (
                select(
                    RunRecord,
                    func.count(SampleRecord.id).label("sample_count"),
                    func.max(SampleRecord.timestamp).label("last_sample_time"),
                )
                .join(PointRecord, PointRecord.run_id == RunRecord.id)
                .join(SampleRecord, SampleRecord.point_id == PointRecord.id)
                .where(SampleRecord.analyzer_id == analyzer_id)
                .group_by(RunRecord.id)
                .order_by(RunRecord.start_time.desc().nullslast())
                .limit(limit)
            )
            if analyzer_serial is not None:
                stmt = stmt.where(SampleRecord.analyzer_serial == analyzer_serial)
            rows = session.execute(stmt).all()
            return [
                {
                    **_serialize_run(run),
                    "sample_count": int(sample_count or 0),
                    "last_sample_time": None if last_sample_time is None else last_sample_time.isoformat(),
                }
                for run, sample_count, last_sample_time in rows
            ]

    def query_runs_by_device(self, analyzer_id: str, **kwargs) -> list[dict[str, Any]]:
        return self.runs_by_device(analyzer_id, **kwargs)

    def sensors(self, *, limit: int = 1000) -> list[dict[str, Any]]:
        with self.database.session_scope() as session:
            rows = session.execute(
                select(SensorRecord).order_by(SensorRecord.device_key.asc()).limit(limit)
            ).scalars().all()
            return [_serialize_sensor(row) for row in rows]

    def _load_sensor(self, session, sensor_id: str) -> SensorRecord:
        record = session.execute(
            select(SensorRecord).where(SensorRecord.sensor_id == sensor_id)
        ).scalars().first()
        if record is None:
            raise ValueError(f"sensor not found: {sensor_id}")
        return record

    @staticmethod
    def _sensor_match_clause(record_cls, sensor: SensorRecord):
        direct = getattr(record_cls, "sensor_id") == sensor.sensor_id
        legacy_terms = []
        analyzer_id = sensor.analyzer_id
        analyzer_serial = sensor.analyzer_serial
        if analyzer_id not in (None, ""):
            legacy = and_(
                getattr(record_cls, "sensor_id").is_(None),
                getattr(record_cls, "analyzer_id") == analyzer_id,
            )
            if analyzer_serial not in (None, "") and hasattr(record_cls, "analyzer_serial"):
                legacy = and_(legacy, getattr(record_cls, "analyzer_serial") == analyzer_serial)
            legacy_terms.append(legacy)
        elif analyzer_serial not in (None, "") and hasattr(record_cls, "analyzer_serial"):
            legacy_terms.append(
                and_(
                    getattr(record_cls, "sensor_id").is_(None),
                    getattr(record_cls, "analyzer_serial") == analyzer_serial,
                )
            )
        if legacy_terms:
            return or_(direct, *legacy_terms)
        return direct

    def runs_by_sensor(self, sensor_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.database.session_scope() as session:
            sensor = self._load_sensor(session, sensor_id)
            run_ids = set()
            sample_run_ids = session.execute(
                select(PointRecord.run_id)
                .join(SampleRecord, SampleRecord.point_id == PointRecord.id)
                .where(self._sensor_match_clause(SampleRecord, sensor))
            ).scalars().all()
            run_ids.update(sample_run_ids)
            frame_run_ids = session.execute(
                select(MeasurementFrameRecord.run_id).where(self._sensor_match_clause(MeasurementFrameRecord, sensor))
            ).scalars().all()
            run_ids.update(frame_run_ids)
            fit_run_ids = session.execute(
                select(FitResultRecord.run_id).where(self._sensor_match_clause(FitResultRecord, sensor))
            ).scalars().all()
            run_ids.update(fit_run_ids)
            if not run_ids:
                return []
            rows = session.execute(
                select(RunRecord)
                .where(RunRecord.id.in_(sorted(run_ids)))
                .order_by(RunRecord.start_time.desc().nullslast())
                .limit(limit)
            ).scalars().all()
            return [_serialize_run(row) for row in rows]

    def samples_by_sensor(
        self,
        sensor_id: str,
        *,
        run_id: str | None = None,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        with self.database.session_scope() as session:
            sensor = self._load_sensor(session, sensor_id)
            stmt = select(SampleRecord).join(PointRecord, SampleRecord.point_id == PointRecord.id)
            stmt = stmt.where(self._sensor_match_clause(SampleRecord, sensor))
            if run_id is not None:
                stmt = stmt.where(PointRecord.run_id == resolve_run_uuid(run_id))
            rows = session.execute(
                stmt.order_by(SampleRecord.timestamp.asc().nullslast(), SampleRecord.sample_index.asc()).limit(limit)
            ).scalars().all()
            return [_serialize_sample(row) for row in rows]

    def measurement_frames_by_sensor(
        self,
        sensor_id: str,
        *,
        run_id: str | None = None,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        with self.database.session_scope() as session:
            sensor = self._load_sensor(session, sensor_id)
            stmt = select(MeasurementFrameRecord).where(self._sensor_match_clause(MeasurementFrameRecord, sensor))
            if run_id is not None:
                stmt = stmt.where(MeasurementFrameRecord.run_id == resolve_run_uuid(run_id))
            rows = session.execute(
                stmt.order_by(MeasurementFrameRecord.sample_ts.asc().nullslast(), MeasurementFrameRecord.sample_index.asc()).limit(limit)
            ).scalars().all()
            return [_serialize_measurement_frame(row) for row in rows]

    def fit_results_by_sensor(
        self,
        sensor_id: str,
        *,
        run_id: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        with self.database.session_scope() as session:
            sensor = self._load_sensor(session, sensor_id)
            stmt = select(FitResultRecord).where(self._sensor_match_clause(FitResultRecord, sensor))
            if run_id is not None:
                stmt = stmt.where(FitResultRecord.run_id == resolve_run_uuid(run_id))
            rows = session.execute(
                stmt.order_by(FitResultRecord.algorithm.asc()).limit(limit)
            ).scalars().all()
            return [_serialize_fit_result(row) for row in rows]

    def coefficient_versions_by_sensor(
        self,
        sensor_id: str,
        *,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        with self.database.session_scope() as session:
            sensor = self._load_sensor(session, sensor_id)
            stmt = select(CoefficientVersionRecord).where(self._sensor_match_clause(CoefficientVersionRecord, sensor))
            rows = session.execute(
                stmt.order_by(CoefficientVersionRecord.version.desc()).limit(limit)
            ).scalars().all()
            return [_serialize_coefficient_version(row) for row in rows]

    def samples_by_point(
        self,
        *,
        point_id: str | None = None,
        run_id: str | None = None,
        sequence: int | None = None,
        analyzer_id: str | None = None,
    ) -> list[dict[str, Any]]:
        with self.database.session_scope() as session:
            stmt = select(SampleRecord).join(PointRecord, SampleRecord.point_id == PointRecord.id)
            if point_id is not None:
                stmt = stmt.where(PointRecord.id == point_id)
            if run_id is not None:
                stmt = stmt.where(PointRecord.run_id == resolve_run_uuid(run_id))
            if sequence is not None:
                stmt = stmt.where(PointRecord.sequence == sequence)
            if analyzer_id is not None:
                stmt = stmt.where(SampleRecord.analyzer_id == analyzer_id)
            rows = session.execute(
                stmt.order_by(SampleRecord.sample_index.asc(), SampleRecord.timestamp.asc().nullslast())
            ).scalars().all()
            return [_serialize_sample(row) for row in rows]

    def query_samples_by_point(self, **kwargs) -> list[dict[str, Any]]:
        return self.samples_by_point(**kwargs)

    def statistics(
        self,
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> dict[str, Any]:
        runs = self.runs_by_time_range(start_time=start_time, end_time=end_time, limit=10000)
        with self.database.session_scope() as session:
            stmt = select(PointRecord).join(RunRecord, PointRecord.run_id == RunRecord.id)
            if start_time is not None:
                stmt = stmt.where(RunRecord.start_time >= start_time)
            if end_time is not None:
                stmt = stmt.where(RunRecord.end_time <= end_time)
            points = session.execute(stmt).scalars().all()

        run_durations = []
        for run in runs:
            start = datetime.fromisoformat(run["start_time"]) if run["start_time"] else None
            end = datetime.fromisoformat(run["end_time"]) if run["end_time"] else None
            if start is not None and end is not None:
                run_durations.append((end - start).total_seconds())

        point_times = [point.total_time_s for point in points if point.total_time_s is not None]
        total_runs = len(runs)
        completed_runs = sum(1 for run in runs if run["status"] == "completed")
        total_points = len(points)
        completed_points = sum(1 for point in points if point.status == "completed")

        return {
            "run_count": total_runs,
            "completed_run_count": completed_runs,
            "run_success_rate": 0.0 if total_runs == 0 else completed_runs / total_runs,
            "point_count": total_points,
            "completed_point_count": completed_points,
            "point_success_rate": 0.0 if total_points == 0 else completed_points / total_points,
            "average_run_time_s": None if not run_durations else mean(run_durations),
            "average_point_time_s": None if not point_times else mean(point_times),
        }

    def query_statistics(self, **kwargs) -> dict[str, Any]:
        return self.statistics(**kwargs)
