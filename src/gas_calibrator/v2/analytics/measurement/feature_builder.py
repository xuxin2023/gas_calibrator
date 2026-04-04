from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import json
from statistics import mean
from typing import Any

from sqlalchemy import or_, select

from ...storage.database import DatabaseManager, resolve_run_uuid
from ...storage.models import (
    FitResultRecord,
    MeasurementFrameRecord,
    PointRecord,
    QCResultRecord,
    RunRecord,
)
from .schemas import MEASUREMENT_FEATURE_SCHEMA_VERSION, build_measurement_feature_payload


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


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


def _mean(values: list[float | int | None]) -> float | None:
    numbers = [float(item) for item in values if item is not None]
    if not numbers:
        return None
    return mean(numbers)


def _safe_div(numerator: float | int, denominator: float | int) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def _context_value(payload: Any, key: str) -> Any:
    if not isinstance(payload, dict):
        return None
    return payload.get(key)


def _is_status_abnormal(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    return text not in {"ok", "normal", "ready", "usable"}


def _coverage_ratio(frame: dict[str, Any]) -> float | None:
    expected = frame.get("analyzer_expected_count")
    usable = frame.get("analyzer_usable_count")
    if expected in (None, 0):
        return None
    try:
        return _safe_div(float(usable or 0), float(expected))
    except (TypeError, ValueError):
        return None


class MeasurementFeatureBuilder:
    """Builds measurement-focused run, frame, and analyzer features."""

    def __init__(self, database: DatabaseManager):
        self.database = database

    def build_features(
        self,
        *,
        run_id: str | None = None,
        analyzer_id: str | None = None,
    ) -> dict[str, Any]:
        with self.database.session_scope() as session:
            frame_stmt = select(MeasurementFrameRecord).order_by(
                MeasurementFrameRecord.sample_ts.asc().nullsfirst(),
                MeasurementFrameRecord.sample_index.asc(),
                MeasurementFrameRecord.analyzer_label.asc(),
            )
            if run_id is not None:
                frame_stmt = frame_stmt.where(MeasurementFrameRecord.run_id == resolve_run_uuid(run_id))
            if analyzer_id is not None:
                frame_stmt = frame_stmt.where(
                    or_(
                        MeasurementFrameRecord.analyzer_label == analyzer_id,
                        MeasurementFrameRecord.analyzer_id == analyzer_id,
                    )
                )
            frames = session.execute(frame_stmt).scalars().all()
            if not frames:
                return self._empty_features(run_id=run_id, analyzer_id=analyzer_id)

            point_ids = {frame.point_id for frame in frames}
            run_ids = {frame.run_id for frame in frames}
            points = session.execute(select(PointRecord).where(PointRecord.id.in_(point_ids))).scalars().all()
            runs = session.execute(
                select(RunRecord)
                .where(RunRecord.id.in_(run_ids))
                .order_by(RunRecord.start_time.asc().nullsfirst(), RunRecord.id.asc())
            ).scalars().all()
            qc_results = session.execute(select(QCResultRecord).where(QCResultRecord.point_id.in_(point_ids))).scalars().all()
            fit_stmt = select(FitResultRecord).where(FitResultRecord.run_id.in_(run_ids))
            fit_results = session.execute(fit_stmt).scalars().all()

        return self._build_feature_payload(
            runs=runs,
            points=points,
            frames=frames,
            qc_results=qc_results,
            fit_results=fit_results,
            scope={"run_id": run_id, "analyzer_id": analyzer_id},
        )

    def _build_feature_payload(
        self,
        *,
        runs: list[RunRecord],
        points: list[PointRecord],
        frames: list[MeasurementFrameRecord],
        qc_results: list[QCResultRecord],
        fit_results: list[FitResultRecord],
        scope: dict[str, Any],
    ) -> dict[str, Any]:
        run_lookup = {record.id: record for record in runs}
        point_lookup = {record.id: record for record in points}
        qc_by_point: dict[Any, list[QCResultRecord]] = defaultdict(list)
        for item in qc_results:
            qc_by_point[item.point_id].append(item)

        fit_by_run: dict[Any, list[FitResultRecord]] = defaultdict(list)
        for item in fit_results:
            fit_by_run[item.run_id].append(item)

        source_run_ids: dict[Any, str] = {}
        for run in runs:
            notes = _load_notes(run.notes)
            raw_notes = notes.get("raw") if isinstance(notes.get("raw"), dict) else {}
            source_run_ids[run.id] = str(notes.get("source_run_id") or raw_notes.get("source_run_id") or run.id)

        frame_features: list[dict[str, Any]] = []
        frames_by_run: dict[Any, list[dict[str, Any]]] = defaultdict(list)
        frames_by_analyzer: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for frame in frames:
            point = point_lookup.get(frame.point_id)
            run = run_lookup.get(frame.run_id)
            point_qc = list(qc_by_point.get(frame.point_id, []))
            context = frame.context_payload if isinstance(frame.context_payload, dict) else {}
            feature = {
                "run_id": source_run_ids.get(frame.run_id, str(frame.run_id)),
                "run_uuid": str(frame.run_id),
                "run_status": None if run is None else run.status,
                "software_version": None if run is None else run.software_version,
                "point_id": str(frame.point_id),
                "point_sequence": None if point is None else point.sequence,
                "point_status": None if point is None else point.status,
                "route_type": None if point is None else point.route_type,
                "sample_index": frame.sample_index,
                "sample_ts": _iso(frame.sample_ts),
                "analyzer_label": frame.analyzer_label,
                "analyzer_id": frame.analyzer_id,
                "analyzer_serial": frame.analyzer_serial,
                "frame_has_data": bool(frame.frame_has_data),
                "frame_usable": bool(frame.frame_usable),
                "analyzer_status": frame.analyzer_status,
                "mode": frame.mode,
                "mode2_field_count": frame.mode2_field_count,
                "co2_ppm": frame.co2_ppm,
                "h2o_mmol": frame.h2o_mmol,
                "co2_ratio_f": frame.co2_ratio_f,
                "h2o_ratio_f": frame.h2o_ratio_f,
                "co2_ratio_raw": frame.co2_ratio_raw,
                "h2o_ratio_raw": frame.h2o_ratio_raw,
                "ref_signal": frame.ref_signal,
                "co2_signal": frame.co2_signal,
                "h2o_signal": frame.h2o_signal,
                "frame_chamber_temp_c": frame.chamber_temp_c,
                "frame_case_temp_c": frame.case_temp_c,
                "pressure_kpa": frame.pressure_kpa,
                "pressure_hpa": _context_value(context, "pressure_hpa"),
                "dewpoint_c": _context_value(context, "dewpoint_c"),
                "dew_temp_c": _context_value(context, "dew_temp_c"),
                "dew_rh_pct": _context_value(context, "dew_rh_pct"),
                "dew_pressure_hpa": _context_value(context, "dew_pressure_hpa"),
                "context_chamber_temp_c": _context_value(context, "chamber_temp_c"),
                "context_chamber_rh_pct": _context_value(context, "chamber_rh_pct"),
                "analyzer_expected_count": _context_value(context, "analyzer_expected_count"),
                "analyzer_with_frame_count": _context_value(context, "analyzer_with_frame_count"),
                "analyzer_usable_count": _context_value(context, "analyzer_usable_count"),
                "analyzer_coverage_text": _context_value(context, "analyzer_coverage_text"),
                "analyzer_integrity": _context_value(context, "analyzer_integrity"),
                "analyzer_missing_labels": _context_value(context, "analyzer_missing_labels"),
                "analyzer_unusable_labels": _context_value(context, "analyzer_unusable_labels"),
                "stability_time_s": _context_value(context, "stability_time_s"),
                "total_time_s": _context_value(context, "total_time_s"),
                "raw_payload_present": bool(frame.raw_payload),
                "qc_fail_count": sum(1 for item in point_qc if not item.passed),
                "failed_qc_rule_names": [item.rule_name for item in point_qc if not item.passed],
                "failed_qc_messages": [item.message for item in point_qc if (not item.passed and item.message)],
            }
            for key, value in context.items():
                if str(key).startswith("hgen_"):
                    feature[str(key)] = value
            frame_features.append(feature)
            frames_by_run[frame.run_id].append(feature)
            frames_by_analyzer[frame.analyzer_label].append(feature)

        run_features: list[dict[str, Any]] = []
        for run in runs:
            notes = _load_notes(run.notes)
            raw_notes = notes.get("raw") if isinstance(notes.get("raw"), dict) else {}
            enrich_notes = notes.get("enrich") if isinstance(notes.get("enrich"), dict) else {}
            run_frames = list(frames_by_run.get(run.id, []))
            run_point_ids = {frame["point_id"] for frame in run_frames}
            run_qc = [item for point_id, items in qc_by_point.items() if str(point_id) in run_point_ids for item in items]
            coverage_values = [_coverage_ratio(frame) for frame in run_frames]
            duration_s = None
            if run.start_time is not None and run.end_time is not None:
                duration_s = max(0.0, (run.end_time - run.start_time).total_seconds())
            run_features.append(
                {
                    "run_id": source_run_ids.get(run.id, str(run.id)),
                    "run_uuid": str(run.id),
                    "status": run.status,
                    "start_time": _iso(run.start_time),
                    "end_time": _iso(run.end_time),
                    "duration_s": duration_s,
                    "software_version": run.software_version,
                    "operator": run.operator,
                    "total_points": run.total_points,
                    "successful_points": run.successful_points,
                    "failed_points": run.failed_points,
                    "warnings": run.warnings,
                    "errors": run.errors,
                    "frame_count": len(run_frames),
                    "usable_frame_count": sum(1 for frame in run_frames if frame.get("frame_usable")),
                    "frame_has_data_count": sum(1 for frame in run_frames if frame.get("frame_has_data")),
                    "frame_usable_rate": _safe_div(
                        sum(1 for frame in run_frames if frame.get("frame_usable")),
                        len(run_frames),
                    ),
                    "frame_has_data_rate": _safe_div(
                        sum(1 for frame in run_frames if frame.get("frame_has_data")),
                        len(run_frames),
                    ),
                    "analyzer_count": len({frame["analyzer_label"] for frame in run_frames}),
                    "point_count": len(run_point_ids),
                    "sample_count": len({(frame["point_id"], frame["sample_index"]) for frame in run_frames}),
                    "measurement_start_time": min((frame["sample_ts"] for frame in run_frames if frame.get("sample_ts")), default=None),
                    "measurement_end_time": max((frame["sample_ts"] for frame in run_frames if frame.get("sample_ts")), default=None),
                    "mean_co2_ppm": _mean([frame.get("co2_ppm") for frame in run_frames]),
                    "mean_h2o_mmol": _mean([frame.get("h2o_mmol") for frame in run_frames]),
                    "mean_ref_signal": _mean([frame.get("ref_signal") for frame in run_frames]),
                    "mean_coverage_ratio": _mean([value for value in coverage_values if value is not None]),
                    "qc_fail_count": sum(1 for item in run_qc if not item.passed),
                    "fit_result_count": len(fit_by_run.get(run.id, [])),
                    "raw_manifest_present": bool(
                        raw_notes.get("manifest_present")
                        or raw_notes.get("manifest_schema_version")
                        or raw_notes.get("source_points_file")
                    ),
                    "raw_source_points_file": raw_notes.get("source_points_file") or notes.get("source_points_file"),
                    "manifest_schema_version": raw_notes.get("manifest_schema_version") or notes.get("manifest_schema_version"),
                    "ai_summary_status": (enrich_notes.get("ai_summary_metadata") or {}).get("status"),
                    "postprocess_summary_status": (enrich_notes.get("postprocess_summary_metadata") or {}).get("status"),
                    "coefficient_report_status": (enrich_notes.get("coefficient_metadata") or {}).get("report_status"),
                    "skipped_artifacts": list(enrich_notes.get("skipped_artifacts") or []),
                    "notes": notes,
                }
            )

        analyzer_features: list[dict[str, Any]] = []
        for analyzer_label in sorted(frames_by_analyzer):
            analyzer_frames = list(frames_by_analyzer[analyzer_label])
            analyzer_run_ids = {frame["run_uuid"] for frame in analyzer_frames}
            analyzer_point_ids = {frame["point_id"] for frame in analyzer_frames}
            analyzer_ids = sorted({str(frame.get("analyzer_id")) for frame in analyzer_frames if frame.get("analyzer_id")})
            analyzer_serials = sorted(
                {str(frame.get("analyzer_serial")) for frame in analyzer_frames if frame.get("analyzer_serial")}
            )
            abnormal_status_count = sum(1 for frame in analyzer_frames if _is_status_abnormal(frame.get("analyzer_status")))
            coverage_values = [_coverage_ratio(frame) for frame in analyzer_frames]
            related_qc = [
                item
                for point_id, items in qc_by_point.items()
                if str(point_id) in analyzer_point_ids
                for item in items
            ]
            related_fit = [
                item
                for item in fit_results
                if item.analyzer_id == analyzer_label and str(item.run_id) in analyzer_run_ids
            ]
            history: list[dict[str, Any]] = []
            for run in runs:
                run_frames = [frame for frame in analyzer_frames if frame["run_uuid"] == str(run.id)]
                if not run_frames:
                    continue
                run_point_ids = {frame["point_id"] for frame in run_frames}
                run_qc = [
                    item
                    for point_id, items in qc_by_point.items()
                    if str(point_id) in run_point_ids
                    for item in items
                ]
                run_fit = [item for item in related_fit if item.run_id == run.id]
                history.append(
                    {
                        "run_id": source_run_ids.get(run.id, str(run.id)),
                        "run_uuid": str(run.id),
                        "start_time": _iso(run.start_time),
                        "frame_count": len(run_frames),
                        "usable_rate": _safe_div(
                            sum(1 for frame in run_frames if frame.get("frame_usable")),
                            len(run_frames),
                        ),
                        "mean_co2_ppm": _mean([frame.get("co2_ppm") for frame in run_frames]),
                        "mean_h2o_mmol": _mean([frame.get("h2o_mmol") for frame in run_frames]),
                        "mean_co2_ratio_f": _mean([frame.get("co2_ratio_f") for frame in run_frames]),
                        "mean_h2o_ratio_f": _mean([frame.get("h2o_ratio_f") for frame in run_frames]),
                        "mean_ref_signal": _mean([frame.get("ref_signal") for frame in run_frames]),
                        "mean_co2_signal": _mean([frame.get("co2_signal") for frame in run_frames]),
                        "mean_h2o_signal": _mean([frame.get("h2o_signal") for frame in run_frames]),
                        "mean_pressure_hpa": _mean([frame.get("pressure_hpa") for frame in run_frames]),
                        "qc_fail_count": sum(1 for item in run_qc if not item.passed),
                        "mean_rmse": _mean([item.rmse for item in run_fit]),
                        "mean_r_squared": _mean([item.r_squared for item in run_fit]),
                    }
                )
            history.sort(key=lambda item: item.get("start_time") or "")
            analyzer_features.append(
                {
                    "analyzer_label": analyzer_label,
                    "analyzer_ids": analyzer_ids,
                    "analyzer_serials": analyzer_serials,
                    "run_count": len(analyzer_run_ids),
                    "point_count": len(analyzer_point_ids),
                    "frame_count": len(analyzer_frames),
                    "usable_frame_count": sum(1 for frame in analyzer_frames if frame.get("frame_usable")),
                    "frame_has_data_count": sum(1 for frame in analyzer_frames if frame.get("frame_has_data")),
                    "missing_frame_count": sum(1 for frame in analyzer_frames if not frame.get("frame_has_data")),
                    "abnormal_status_count": abnormal_status_count,
                    "usable_rate": _safe_div(
                        sum(1 for frame in analyzer_frames if frame.get("frame_usable")),
                        len(analyzer_frames),
                    ),
                    "has_data_rate": _safe_div(
                        sum(1 for frame in analyzer_frames if frame.get("frame_has_data")),
                        len(analyzer_frames),
                    ),
                    "mean_co2_ppm": _mean([frame.get("co2_ppm") for frame in analyzer_frames]),
                    "mean_h2o_mmol": _mean([frame.get("h2o_mmol") for frame in analyzer_frames]),
                    "mean_co2_ratio_f": _mean([frame.get("co2_ratio_f") for frame in analyzer_frames]),
                    "mean_h2o_ratio_f": _mean([frame.get("h2o_ratio_f") for frame in analyzer_frames]),
                    "mean_ref_signal": _mean([frame.get("ref_signal") for frame in analyzer_frames]),
                    "mean_co2_signal": _mean([frame.get("co2_signal") for frame in analyzer_frames]),
                    "mean_h2o_signal": _mean([frame.get("h2o_signal") for frame in analyzer_frames]),
                    "mean_pressure_hpa": _mean([frame.get("pressure_hpa") for frame in analyzer_frames]),
                    "mean_dewpoint_c": _mean([frame.get("dewpoint_c") for frame in analyzer_frames]),
                    "mean_coverage_ratio": _mean([value for value in coverage_values if value is not None]),
                    "qc_fail_count": sum(1 for item in related_qc if not item.passed),
                    "qc_result_count": len(related_qc),
                    "fit_result_count": len(related_fit),
                    "mean_rmse": _mean([item.rmse for item in related_fit]),
                    "mean_r_squared": _mean([item.r_squared for item in related_fit]),
                    "latest_sample_time": max(
                        (frame["sample_ts"] for frame in analyzer_frames if frame.get("sample_ts")),
                        default=None,
                    ),
                    "history": history,
                }
            )

        return build_measurement_feature_payload(
            run_id=scope.get("run_id"),
            analyzer_id=scope.get("analyzer_id"),
            run_features=run_features,
            frame_features=frame_features,
            analyzer_features=analyzer_features,
        )

    def _empty_features(self, *, run_id: str | None, analyzer_id: str | None) -> dict[str, Any]:
        return {
            "schema_version": MEASUREMENT_FEATURE_SCHEMA_VERSION,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "scope": {"run_id": run_id, "analyzer_id": analyzer_id},
            "run_features": [],
            "frame_features": [],
            "analyzer_features": [],
        }
