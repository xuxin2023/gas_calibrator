from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import json
from statistics import mean
from typing import Any

from sqlalchemy import select

from ..storage.database import DatabaseManager, resolve_run_uuid
from ..storage.models import (
    AlarmIncidentRecord,
    DeviceEventRecord,
    FitResultRecord,
    PointRecord,
    QCResultRecord,
    RunRecord,
    SampleRecord,
)


ANALYTICS_FEATURE_SCHEMA_VERSION = "1.0"


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _mean(values: list[float | None]) -> float | None:
    numbers = [float(item) for item in values if item is not None]
    if not numbers:
        return None
    return mean(numbers)


def _load_notes(payload: Any) -> dict[str, Any]:
    if not payload:
        return {}
    if isinstance(payload, dict):
        return dict(payload)
    try:
        data = json.loads(str(payload))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


class FeatureBuilder:
    """Builds run-, point-, and analyzer-level features from storage records."""

    def __init__(self, database: DatabaseManager):
        self.database = database

    def build_features(
        self,
        *,
        run_id: str | None = None,
        analyzer_id: str | None = None,
    ) -> dict[str, Any]:
        with self.database.session_scope() as session:
            scoped_run_ids, scoped_point_ids = self._resolve_scope(session, run_id=run_id, analyzer_id=analyzer_id)
            if scoped_run_ids is not None and not scoped_run_ids:
                return self._empty_features(run_id=run_id, analyzer_id=analyzer_id)

            run_stmt = select(RunRecord).order_by(RunRecord.start_time.asc().nullsfirst(), RunRecord.id.asc())
            if scoped_run_ids is not None:
                run_stmt = run_stmt.where(RunRecord.id.in_(scoped_run_ids))
            runs = session.execute(run_stmt).scalars().all()
            run_ids = [record.id for record in runs]
            if not run_ids:
                return self._empty_features(run_id=run_id, analyzer_id=analyzer_id)

            point_stmt = select(PointRecord).where(PointRecord.run_id.in_(run_ids)).order_by(PointRecord.sequence.asc())
            if scoped_point_ids is not None:
                point_stmt = point_stmt.where(PointRecord.id.in_(scoped_point_ids))
            points = session.execute(point_stmt).scalars().all()
            point_ids = [record.id for record in points]

            samples = []
            qc_results = []
            if point_ids:
                sample_stmt = select(SampleRecord).where(SampleRecord.point_id.in_(point_ids))
                if analyzer_id is not None:
                    sample_stmt = sample_stmt.where(SampleRecord.analyzer_id == analyzer_id)
                samples = session.execute(sample_stmt).scalars().all()
                qc_results = session.execute(select(QCResultRecord).where(QCResultRecord.point_id.in_(point_ids))).scalars().all()

            fit_stmt = select(FitResultRecord).where(FitResultRecord.run_id.in_(run_ids))
            if analyzer_id is not None:
                fit_stmt = fit_stmt.where(FitResultRecord.analyzer_id == analyzer_id)
            fit_results = session.execute(fit_stmt).scalars().all()

            device_events = session.execute(select(DeviceEventRecord).where(DeviceEventRecord.run_id.in_(run_ids))).scalars().all()
            alarms = session.execute(select(AlarmIncidentRecord).where(AlarmIncidentRecord.run_id.in_(run_ids))).scalars().all()

        return self._build_feature_payload(
            runs=runs,
            points=points,
            samples=samples,
            qc_results=qc_results,
            fit_results=fit_results,
            device_events=device_events,
            alarms=alarms,
            scope={"run_id": run_id, "analyzer_id": analyzer_id},
        )

    def _resolve_scope(
        self,
        session,
        *,
        run_id: str | None,
        analyzer_id: str | None,
    ) -> tuple[set[Any] | None, set[Any] | None]:
        scoped_run_ids: set[Any] | None = None
        scoped_point_ids: set[Any] | None = None
        if run_id is not None:
            scoped_run_ids = {resolve_run_uuid(run_id)}
        if analyzer_id is None:
            return scoped_run_ids, scoped_point_ids

        rows = session.execute(
            select(SampleRecord.point_id, PointRecord.run_id)
            .join(PointRecord, SampleRecord.point_id == PointRecord.id)
            .where(SampleRecord.analyzer_id == analyzer_id)
        ).all()
        analyzer_point_ids = {point_id for point_id, _ in rows}
        analyzer_run_ids = {run_ref for _, run_ref in rows}
        scoped_point_ids = analyzer_point_ids
        if scoped_run_ids is None:
            scoped_run_ids = analyzer_run_ids
        else:
            scoped_run_ids = scoped_run_ids & analyzer_run_ids
        return scoped_run_ids, scoped_point_ids

    def _build_feature_payload(
        self,
        *,
        runs: list[RunRecord],
        points: list[PointRecord],
        samples: list[SampleRecord],
        qc_results: list[QCResultRecord],
        fit_results: list[FitResultRecord],
        device_events: list[DeviceEventRecord],
        alarms: list[AlarmIncidentRecord],
        scope: dict[str, Any],
    ) -> dict[str, Any]:
        points_by_run: dict[Any, list[PointRecord]] = defaultdict(list)
        for point in points:
            points_by_run[point.run_id].append(point)

        samples_by_point: dict[Any, list[SampleRecord]] = defaultdict(list)
        for sample in samples:
            samples_by_point[sample.point_id].append(sample)

        qc_by_point: dict[Any, list[QCResultRecord]] = defaultdict(list)
        for item in qc_results:
            qc_by_point[item.point_id].append(item)

        fit_by_run: dict[Any, list[FitResultRecord]] = defaultdict(list)
        for item in fit_results:
            fit_by_run[item.run_id].append(item)

        events_by_run: dict[Any, list[DeviceEventRecord]] = defaultdict(list)
        for item in device_events:
            events_by_run[item.run_id].append(item)

        alarms_by_run: dict[Any, list[AlarmIncidentRecord]] = defaultdict(list)
        for item in alarms:
            alarms_by_run[item.run_id].append(item)

        run_features: list[dict[str, Any]] = []
        point_features: list[dict[str, Any]] = []
        point_by_id: dict[Any, PointRecord] = {}
        point_run_lookup: dict[Any, Any] = {}
        source_run_ids: dict[Any, str] = {}

        for run in runs:
            notes = _load_notes(run.notes)
            raw_notes = notes.get("raw") if isinstance(notes.get("raw"), dict) else {}
            enrich_notes = notes.get("enrich") if isinstance(notes.get("enrich"), dict) else {}
            run_points = points_by_run.get(run.id, [])
            run_samples = [sample for point in run_points for sample in samples_by_point.get(point.id, [])]
            run_qc = [item for point in run_points for item in qc_by_point.get(point.id, [])]
            run_fit = list(fit_by_run.get(run.id, []))
            run_events = list(events_by_run.get(run.id, []))
            run_alarms = list(alarms_by_run.get(run.id, []))
            duration_s = None
            if run.start_time is not None and run.end_time is not None:
                duration_s = max(0.0, (run.end_time - run.start_time).total_seconds())
            source_run_id = str(notes.get("source_run_id") or raw_notes.get("source_run_id") or run.id)
            source_run_ids[run.id] = source_run_id
            run_features.append(
                {
                    "run_id": source_run_id,
                    "run_uuid": str(run.id),
                    "status": run.status,
                    "start_time": _iso(run.start_time),
                    "end_time": _iso(run.end_time),
                    "duration_s": duration_s,
                    "config_hash": run.config_hash,
                    "software_version": run.software_version,
                    "operator": run.operator,
                    "total_points": run.total_points,
                    "successful_points": run.successful_points,
                    "failed_points": run.failed_points,
                    "warnings": run.warnings,
                    "errors": run.errors,
                    "sample_count": len(run_samples),
                    "qc_result_count": len(run_qc),
                    "fit_result_count": len(run_fit),
                    "device_event_count": len(run_events),
                    "alarm_count": len(run_alarms),
                    "alarm_categories": dict(Counter(str(item.category or "uncategorized") for item in run_alarms)),
                    "event_devices": dict(Counter(str(item.device_name or "unknown") for item in run_events)),
                    "sample_analyzers": sorted({sample.analyzer_id for sample in run_samples if sample.analyzer_id}),
                    "raw_manifest_present": bool(
                        raw_notes.get("manifest_present")
                        or raw_notes.get("manifest_schema_version")
                        or raw_notes.get("source_points_file")
                    ),
                    "raw_source_points_file": raw_notes.get("source_points_file") or notes.get("source_points_file"),
                    "manifest_schema_version": raw_notes.get("manifest_schema_version") or notes.get("manifest_schema_version"),
                    "enrich_qc_status": "loaded" if enrich_notes.get("qc") else "missing",
                    "enrich_qc_imported_results": int(((enrich_notes.get("qc") or {}).get("imported_results")) or 0),
                    "enrich_fit_imported_results": int(((enrich_notes.get("fit") or {}).get("imported_results")) or 0),
                    "coefficient_report_status": ((enrich_notes.get("coefficient_metadata") or {}).get("report_status")),
                    "ai_summary_status": ((enrich_notes.get("ai_summary_metadata") or {}).get("status")),
                    "postprocess_summary_status": ((enrich_notes.get("postprocess_summary_metadata") or {}).get("status")),
                    "skipped_artifacts": list(enrich_notes.get("skipped_artifacts") or []),
                    "notes": notes,
                }
            )

        for point in points:
            point_by_id[point.id] = point
            point_run_lookup[point.id] = point.run_id
            point_samples = list(samples_by_point.get(point.id, []))
            point_qc = list(qc_by_point.get(point.id, []))
            point_features.append(
                {
                    "point_id": str(point.id),
                    "run_id": source_run_ids.get(point.run_id, str(point.run_id)),
                    "run_uuid": str(point.run_id),
                    "sequence": point.sequence,
                    "route_type": point.route_type,
                    "status": point.status,
                    "temperature_c": point.temperature_c,
                    "humidity_rh": point.humidity_rh,
                    "pressure_hpa": point.pressure_hpa,
                    "co2_target_ppm": point.co2_target_ppm,
                    "stability_time_s": point.stability_time_s,
                    "total_time_s": point.total_time_s,
                    "retry_count": point.retry_count,
                    "sample_count": len(point_samples),
                    "analyzer_count": len({sample.analyzer_id for sample in point_samples if sample.analyzer_id}),
                    "mean_co2_ppm": _mean([sample.co2_ppm for sample in point_samples]),
                    "mean_h2o_mmol": _mean([sample.h2o_mmol for sample in point_samples]),
                    "qc_result_count": len(point_qc),
                    "qc_fail_count": sum(1 for item in point_qc if not item.passed),
                    "qc_pass_count": sum(1 for item in point_qc if item.passed),
                    "failed_qc_rule_names": [item.rule_name for item in point_qc if not item.passed],
                    "failed_qc_messages": [item.message for item in point_qc if (not item.passed and item.message)],
                }
            )

        analyzer_samples: dict[tuple[str, str | None], list[SampleRecord]] = defaultdict(list)
        analyzer_point_ids: dict[tuple[str, str | None], set[Any]] = defaultdict(set)
        analyzer_run_ids: dict[tuple[str, str | None], set[Any]] = defaultdict(set)
        for sample in samples:
            key = (sample.analyzer_id, sample.analyzer_serial)
            analyzer_samples[key].append(sample)
            analyzer_point_ids[key].add(sample.point_id)
            analyzer_run_ids[key].add(point_run_lookup.get(sample.point_id))

        analyzer_fit: dict[str, list[FitResultRecord]] = defaultdict(list)
        for item in fit_results:
            analyzer_fit[item.analyzer_id].append(item)

        analyzer_features: list[dict[str, Any]] = []
        for (analyzer_id, analyzer_serial), analyzer_sample_rows in sorted(analyzer_samples.items()):
            run_ids = {run_ref for run_ref in analyzer_run_ids[(analyzer_id, analyzer_serial)] if run_ref is not None}
            point_ids = set(analyzer_point_ids[(analyzer_id, analyzer_serial)])
            related_qc = [item for point_id in point_ids for item in qc_by_point.get(point_id, [])]
            related_fit = list(analyzer_fit.get(analyzer_id, []))
            history: list[dict[str, Any]] = []
            for run in runs:
                if run.id not in run_ids:
                    continue
                per_run_samples = [
                    sample
                    for sample in analyzer_sample_rows
                    if point_run_lookup.get(sample.point_id) == run.id
                ]
                per_run_point_ids = {sample.point_id for sample in per_run_samples}
                per_run_qc = [item for point_id in per_run_point_ids for item in qc_by_point.get(point_id, [])]
                per_run_fit = [item for item in related_fit if item.run_id == run.id]
                history.append(
                    {
                        "run_id": source_run_ids.get(run.id, str(run.id)),
                        "start_time": _iso(run.start_time),
                        "sample_count": len(per_run_samples),
                        "point_count": len(per_run_point_ids),
                        "mean_co2_ppm": _mean([sample.co2_ppm for sample in per_run_samples]),
                        "mean_h2o_mmol": _mean([sample.h2o_mmol for sample in per_run_samples]),
                        "mean_co2_ratio_f": _mean([sample.co2_ratio_f for sample in per_run_samples]),
                        "mean_h2o_ratio_f": _mean([sample.h2o_ratio_f for sample in per_run_samples]),
                        "qc_fail_count": sum(1 for item in per_run_qc if not item.passed),
                        "mean_rmse": _mean([item.rmse for item in per_run_fit]),
                        "mean_r_squared": _mean([item.r_squared for item in per_run_fit]),
                        "alarm_count": len(alarms_by_run.get(run.id, [])),
                    }
                )
            history.sort(key=lambda item: item.get("start_time") or "")
            analyzer_features.append(
                {
                    "analyzer_id": analyzer_id,
                    "analyzer_serial": analyzer_serial,
                    "run_count": len(run_ids),
                    "point_count": len(point_ids),
                    "sample_count": len(analyzer_sample_rows),
                    "mean_co2_ppm": _mean([sample.co2_ppm for sample in analyzer_sample_rows]),
                    "mean_h2o_mmol": _mean([sample.h2o_mmol for sample in analyzer_sample_rows]),
                    "mean_co2_ratio_f": _mean([sample.co2_ratio_f for sample in analyzer_sample_rows]),
                    "mean_h2o_ratio_f": _mean([sample.h2o_ratio_f for sample in analyzer_sample_rows]),
                    "qc_fail_count": sum(1 for item in related_qc if not item.passed),
                    "qc_result_count": len(related_qc),
                    "fit_result_count": len(related_fit),
                    "mean_rmse": _mean([item.rmse for item in related_fit]),
                    "mean_r_squared": _mean([item.r_squared for item in related_fit]),
                    "alarm_count": sum(len(alarms_by_run.get(run_ref, [])) for run_ref in run_ids),
                    "latest_sample_time": max((_iso(sample.timestamp) for sample in analyzer_sample_rows if sample.timestamp is not None), default=None),
                    "history": history,
                }
            )

        return {
            "schema_version": ANALYTICS_FEATURE_SCHEMA_VERSION,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "scope": dict(scope),
            "runs": run_features,
            "points": point_features,
            "analyzers": analyzer_features,
        }

    def _empty_features(self, *, run_id: str | None, analyzer_id: str | None) -> dict[str, Any]:
        return {
            "schema_version": ANALYTICS_FEATURE_SCHEMA_VERSION,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "scope": {"run_id": run_id, "analyzer_id": analyzer_id},
            "runs": [],
            "points": [],
            "analyzers": [],
        }
