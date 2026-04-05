from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
import json
from typing import Any, Optional

from ..config import (
    build_step2_config_governance_handoff,
    build_step2_config_safety_review,
    summarize_step2_config_safety,
)
from ..domain.pressure_selection import effective_pressure_mode, normalize_pressure_selection_token, pressure_target_label
from .data_writer import DataWriter
from .models import CalibrationPoint, CalibrationStatus, SamplingResult
from .offline_artifacts import build_point_taxonomy_handoff, export_run_offline_artifacts
from .run_manifest import write_run_manifest
from .session import RunSession

try:  # pragma: no cover - defensive import
    from ... import __version__ as SOFTWARE_VERSION
except Exception:  # pragma: no cover - defensive
    SOFTWARE_VERSION = ""


class ResultStore:
    """Unified run result storage."""

    def __init__(self, output_dir: Path, run_id: str):
        self.output_dir = Path(output_dir)
        self.run_id = str(run_id)
        self.data_writer = DataWriter(str(self.output_dir), self.run_id)
        self.run_dir = self.data_writer.run_dir
        self.json_path = self.run_dir / "results.json"
        self.point_summary_path = self.run_dir / "point_summaries.json"
        self.points_readable_path = self.run_dir / "points_readable.csv"
        self.manifest_path = self.run_dir / "manifest.json"
        self._samples: list[SamplingResult] = []
        self._point_summaries: list[dict[str, Any]] = []

    def save_sample(self, result: SamplingResult) -> None:
        self._samples.append(result)

    def save_point_summary(self, point: CalibrationPoint, stats: dict[str, Any]) -> None:
        payload = {
            "point": asdict(point),
            "stats": dict(stats),
        }
        self._point_summaries.append(payload)
        self.point_summary_path.write_text(
            json.dumps(self._point_summaries, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def save_run_summary(
        self,
        session: RunSession,
        status: Optional[CalibrationStatus] = None,
        *,
        output_files: Optional[list[str]] = None,
        startup_pressure_precheck: Optional[dict[str, Any]] = None,
        export_statuses: Optional[dict[str, dict[str, Any]]] = None,
        extra_stats: Optional[dict[str, Any]] = None,
    ) -> None:
        effective_status = status or self._fallback_status(session)
        reporting = self._reporting_payload(session)
        artifact_exports = dict(export_statuses or {})
        config = getattr(session, "config", None)
        config_safety = dict(getattr(config, "_config_safety", {}) or {})
        if not config_safety and config is not None:
            config_safety = summarize_step2_config_safety(config)
        config_safety_review = build_step2_config_safety_review(config_safety) if config_safety else {}
        config_governance_handoff = (
            build_step2_config_governance_handoff(config_safety_review or config_safety)
            if (config_safety_review or config_safety)
            else {}
        )
        stats = {
            "run_id": session.run_id,
            "sample_count": len(self._samples),
            "warning_count": len(session.warnings),
            "error_count": len(session.errors),
            "enabled_devices": sorted(session.enabled_devices),
            "point_summaries": list(self._point_summaries),
            "output_files": list(output_files or []),
            "artifact_exports": artifact_exports,
            "artifact_role_summary": self._artifact_role_summary(artifact_exports),
            "reporting_mode": reporting,
        }
        if self._point_summaries:
            stats["point_taxonomy_summary"] = build_point_taxonomy_handoff(self._point_summaries)
        if config_safety:
            stats["config_safety"] = config_safety
            stats["config_safety_review"] = config_safety_review
            stats["config_governance_handoff"] = config_governance_handoff
        if startup_pressure_precheck is not None:
            stats["startup_pressure_precheck"] = startup_pressure_precheck
        if extra_stats:
            stats.update(dict(extra_stats))
        features = getattr(getattr(session, "config", None), "features", None)
        self.data_writer.write_summary(
            effective_status,
            stats,
            started_at=self._format_dt(session.started_at),
            ended_at=self._format_dt(session.ended_at),
            warnings=len(session.warnings),
            errors=len(session.errors),
            startup_pressure_precheck=startup_pressure_precheck,
            reporting=reporting,
            simulation_mode=bool(getattr(features, "simulation_mode", False)),
        )
        self._promote_summary_handoffs(stats)

    def save_run_manifest(
        self,
        session: RunSession,
        *,
        source_points_file: Optional[str | Path] = None,
        output_files: Optional[list[str]] = None,
        startup_pressure_precheck: Optional[dict[str, Any]] = None,
        extra_sections: Optional[dict[str, Any]] = None,
    ) -> Path:
        self.manifest_path = write_run_manifest(
            self.run_dir,
            session,
            source_points_file=source_points_file,
            output_files=output_files,
            startup_pressure_precheck=startup_pressure_precheck,
            extra_sections=extra_sections,
        )
        return self.manifest_path

    def get_samples(self) -> list[SamplingResult]:
        return list(self._samples)

    def export_csv(self) -> Path:
        return Path(self.data_writer.write_samples(self._samples))

    def export_excel(self) -> Path:
        return Path(self.data_writer.write_samples_excel(self._samples))

    def export_json(self) -> Path:
        payload = {
            "run_id": self.run_id,
            "samples": [self._serialize_sample(sample) for sample in self._samples],
            "point_summaries": list(self._point_summaries),
        }
        self.json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return self.json_path

    def export_points_readable(self, session: Optional[RunSession] = None) -> Optional[Path]:
        rows = self._build_points_readable_rows(session)
        if not rows:
            return None
        return Path(self.data_writer.write_points_readable(rows))

    def export_offline_artifacts(
        self,
        session: RunSession,
        *,
        source_points_file: Optional[str | Path] = None,
        output_files: Optional[list[str]] = None,
        export_statuses: Optional[dict[str, dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        return export_run_offline_artifacts(
            run_dir=self.run_dir,
            output_dir=self.output_dir,
            run_id=self.run_id,
            session=session,
            samples=list(self._samples),
            point_summaries=list(self._point_summaries),
            output_files=list(output_files or []),
            export_statuses=dict(export_statuses or {}),
            source_points_file=source_points_file,
            software_build_id=SOFTWARE_VERSION,
            config_safety=dict(getattr(getattr(session, "config", None), "_config_safety", {}) or {}),
            config_safety_review=build_step2_config_safety_review(
                dict(getattr(getattr(session, "config", None), "_config_safety", {}) or {})
            )
            if dict(getattr(getattr(session, "config", None), "_config_safety", {}) or {})
            else {},
        )

    def _promote_summary_handoffs(self, stats: dict[str, Any]) -> None:
        if not self.data_writer.summary_path.exists():
            return
        try:
            payload = json.loads(self.data_writer.summary_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return

        promoted = False
        for key in (
            "point_taxonomy_summary",
            "artifact_role_summary",
            "reporting_mode",
            "config_safety",
            "config_safety_review",
            "config_governance_handoff",
            "offline_diagnostic_adapter_summary",
            "workbench_evidence_summary",
        ):
            value = stats.get(key)
            if not isinstance(value, dict) or not value:
                continue
            payload[key] = dict(value)
            promoted = True
        if not promoted:
            return
        self.data_writer.summary_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _reporting_payload(session: RunSession) -> dict[str, Any]:
        workflow = getattr(getattr(session, "config", None), "workflow", None)
        reporting_cfg = dict(getattr(workflow, "reporting", {}) or {}) if workflow is not None else {}
        include_fleet_stats = bool(reporting_cfg.get("include_fleet_stats", False))
        return {
            "include_fleet_stats": include_fleet_stats,
            "mode": "diagnostic_plus_fleet" if include_fleet_stats else "formal_default",
        }

    @staticmethod
    def _artifact_role_summary(export_statuses: dict[str, dict[str, Any]]) -> dict[str, Any]:
        summary: dict[str, dict[str, Any]] = {}
        for name, payload in dict(export_statuses or {}).items():
            role = str(dict(payload or {}).get("role", "") or "unclassified")
            status = str(dict(payload or {}).get("status", "") or "unknown")
            item = summary.setdefault(
                role,
                {
                    "count": 0,
                    "artifacts": [],
                    "status_counts": {},
                },
            )
            item["count"] += 1
            item["artifacts"].append(str(name))
            item["status_counts"][status] = int(item["status_counts"].get(status, 0)) + 1
        for payload in summary.values():
            payload["artifacts"] = sorted(payload["artifacts"])
        return summary

    def _fallback_status(self, session: RunSession) -> CalibrationStatus:
        total_points = int(getattr(session, "total_points", 0) or 0)
        completed_points = int(getattr(session, "completed_points", 0) or 0)
        progress = float(getattr(session, "progress", 0.0) or 0.0)
        return CalibrationStatus(
            phase=session.phase,
            current_point=session.current_point,
            total_points=total_points,
            completed_points=completed_points,
            progress=progress,
            message=session.stop_reason or session.phase.value,
            elapsed_s=self._elapsed_s(session),
            error=session.errors[-1] if session.errors else None,
        )

    @staticmethod
    def _serialize_sample(result: SamplingResult) -> dict[str, Any]:
        payload = asdict(result)
        payload["timestamp"] = result.timestamp.isoformat()
        return payload

    def _build_points_readable_rows(self, session: Optional[RunSession]) -> list[dict[str, Any]]:
        summaries_by_key: dict[tuple[int, str, str], dict[str, Any]] = {}
        for payload in self._point_summaries:
            point_payload = dict(payload.get("point", {}) or {})
            stats_payload = dict(payload.get("stats", {}) or {})
            key = self._summary_key(point_payload, stats_payload)
            summaries_by_key[key] = {
                "point": point_payload,
                "stats": stats_payload,
            }

        samples_by_key: dict[tuple[int, str, str], list[SamplingResult]] = defaultdict(list)
        for sample in self._samples:
            samples_by_key[self._sample_key(sample)].append(sample)

        keys = sorted(set(summaries_by_key) | set(samples_by_key), key=lambda item: (item[0], item[1], item[2]))
        if not keys:
            return []

        expected_analyzers = self._expected_analyzers(session)
        expected_count = len(expected_analyzers)
        reporting = self._reporting_payload(session) if session is not None else {"include_fleet_stats": False}
        include_fleet_stats = bool(reporting.get("include_fleet_stats", False))
        rows: list[dict[str, Any]] = []
        for key in keys:
            summary_payload = summaries_by_key.get(key, {})
            point_payload = dict(summary_payload.get("point", {}) or {})
            stats_payload = dict(summary_payload.get("stats", {}) or {})
            samples = list(samples_by_key.get(key, []))
            route = str(point_payload.get("route") or key[1] or "")
            present_analyzers = {
                str(sample.analyzer_id).strip().upper()
                for sample in samples
                if str(sample.analyzer_id).strip()
            }
            usable_analyzers = {
                str(sample.analyzer_id).strip().upper()
                for sample in samples
                if str(sample.analyzer_id).strip() and bool(sample.frame_usable)
            }
            missing_analyzers = sorted(set(expected_analyzers) - present_analyzers)
            unusable_analyzers = sorted(present_analyzers - usable_analyzers)
            frame_statuses = sorted(
                {
                    str(sample.frame_status).strip()
                    for sample in samples
                    if str(sample.frame_status).strip()
                }
            )
            row = {
                "point_index": point_payload.get("index", key[0]),
                "point_phase": stats_payload.get("point_phase") or key[1] or route,
                "point_tag": stats_payload.get("point_tag") or key[2],
                "route": route,
                "temperature_c": point_payload.get("temperature_c"),
                "hgen_temp_c": point_payload.get("humidity_generator_temp_c"),
                "humidity_pct": point_payload.get("humidity_pct"),
                "co2_ppm": point_payload.get("co2_ppm"),
                "co2_group": point_payload.get("co2_group"),
                "cylinder_nominal_ppm": point_payload.get("cylinder_nominal_ppm"),
                "pressure_target_hpa": point_payload.get("pressure_hpa"),
                "pressure_mode": effective_pressure_mode(
                    pressure_hpa=point_payload.get("pressure_hpa"),
                    pressure_mode=point_payload.get("pressure_mode"),
                    pressure_selection_token=point_payload.get("pressure_selection_token"),
                ),
                "pressure_target_label": pressure_target_label(
                    pressure_hpa=point_payload.get("pressure_hpa"),
                    pressure_mode=point_payload.get("pressure_mode"),
                    pressure_selection_token=point_payload.get("pressure_selection_token"),
                    explicit_label=point_payload.get("pressure_target_label"),
                ),
                "pressure_selection_token": normalize_pressure_selection_token(point_payload.get("pressure_selection_token")),
                "execution_status": self._execution_status(stats_payload, samples),
                "qc_valid": stats_payload.get("valid"),
                "recommendation": stats_payload.get("recommendation"),
                "reason": stats_payload.get("reason"),
                "raw_sample_count": stats_payload.get("raw_sample_count", len(samples)),
                "cleaned_sample_count": stats_payload.get("cleaned_sample_count", len(samples)),
                "usable_sample_count": stats_payload.get(
                    "usable_sample_count",
                    sum(1 for sample in samples if bool(sample.frame_usable)),
                ),
                "removed_sample_count": stats_payload.get("removed_sample_count", 0),
                "total_frames": len(samples),
                "valid_frames": sum(1 for sample in samples if bool(sample.frame_usable)),
                "frames_with_data": sum(1 for sample in samples if bool(sample.frame_has_data)),
                "frame_status": self._format_frame_status(frame_statuses),
                "pressure_gauge_hpa_mean": self._mean(samples, "pressure_gauge_hpa"),
                "pressure_reference_status": self._dominant_text(samples, "pressure_reference_status"),
                "thermometer_temp_c_mean": self._mean(samples, "thermometer_temp_c"),
                "thermometer_reference_status": self._dominant_text(samples, "thermometer_reference_status"),
                "reference_quality": self._reference_quality_text(samples),
                "dew_point_c_mean": self._mean(samples, "dew_point_c"),
                "postseal_expected_dewpoint_c": stats_payload.get("postseal_expected_dewpoint_c"),
                "postseal_actual_dewpoint_c": stats_payload.get("postseal_actual_dewpoint_c"),
                "postseal_physical_delta_c": stats_payload.get("postseal_physical_delta_c"),
                "postseal_physical_qc_status": stats_payload.get("postseal_physical_qc_status"),
                "postseal_physical_qc_reason": stats_payload.get("postseal_physical_qc_reason"),
                "postseal_guard_status": stats_payload.get("postseal_guard_status"),
                "postseal_guard_flags": stats_payload.get("postseal_guard_flags"),
                "postseal_timeout_policy": stats_payload.get("postseal_timeout_policy"),
                "postseal_timeout_blocked": stats_payload.get("postseal_timeout_blocked"),
                "postsample_late_rebound_status": stats_payload.get("postsample_late_rebound_status"),
                "postsample_late_rebound_reason": stats_payload.get("postsample_late_rebound_reason"),
                "dewpoint_gate_result": stats_payload.get("dewpoint_gate_result"),
                "flush_gate_status": stats_payload.get("flush_gate_status", stats_payload.get("dewpoint_gate_result")),
                "flush_gate_reason": stats_payload.get(
                    "flush_gate_reason",
                    stats_payload.get("postsample_late_rebound_reason", stats_payload.get("rebound_note")),
                ),
                "dewpoint_rebound_detected": stats_payload.get("dewpoint_rebound_detected"),
                "rebound_rise_c": stats_payload.get("rebound_rise_c"),
                "rebound_note": stats_payload.get("rebound_note"),
                "preseal_dewpoint_c": stats_payload.get("preseal_dewpoint_c"),
                "preseal_temp_c": stats_payload.get("preseal_temp_c"),
                "preseal_rh_pct": stats_payload.get("preseal_rh_pct"),
                "preseal_pressure_hpa": stats_payload.get("preseal_pressure_hpa"),
                "preseal_trigger_overshoot_hpa": stats_payload.get("preseal_trigger_overshoot_hpa"),
                "preseal_vent_off_begin_to_route_sealed_ms": stats_payload.get(
                    "preseal_vent_off_begin_to_route_sealed_ms"
                ),
                "pressure_gauge_stale_count": stats_payload.get("pressure_gauge_stale_count"),
                "pressure_gauge_total_count": stats_payload.get("pressure_gauge_total_count"),
                "pressure_gauge_stale_ratio": stats_payload.get("pressure_gauge_stale_ratio"),
                "AnalyzerCoverage": f"{len(usable_analyzers)}/{expected_count}" if expected_count else "0/0",
                "UsableAnalyzers": len(usable_analyzers),
                "ExpectedAnalyzers": expected_count,
                "PointIntegrity": self._point_integrity_text(
                    expected_count=expected_count,
                    present=present_analyzers,
                    usable=usable_analyzers,
                ),
                "MissingAnalyzers": ",".join(missing_analyzers) if include_fleet_stats else "",
                "UnusableAnalyzers": ",".join(unusable_analyzers) if include_fleet_stats else "",
                "stability_time_s": stats_payload.get("stability_time_s"),
                "total_time_s": stats_payload.get("total_time_s"),
            }
            rows.append(row)
        return rows

    @staticmethod
    def _summary_key(point_payload: dict[str, Any], stats_payload: dict[str, Any]) -> tuple[int, str, str]:
        return (
            int(point_payload.get("index", 0) or 0),
            str(stats_payload.get("point_phase") or point_payload.get("route") or "").strip().lower(),
            str(stats_payload.get("point_tag") or "").strip(),
        )

    @staticmethod
    def _sample_key(sample: SamplingResult) -> tuple[int, str, str]:
        return (
            int(sample.point.index),
            str(sample.point_phase or sample.point.route or "").strip().lower(),
            str(sample.point_tag or "").strip(),
        )

    @staticmethod
    def _mean(samples: list[SamplingResult], field: str) -> Optional[float]:
        values = [getattr(sample, field) for sample in samples if getattr(sample, field) is not None]
        if not values:
            return None
        numeric = [float(value) for value in values]
        return sum(numeric) / len(numeric)

    @staticmethod
    def _dominant_text(samples: list[SamplingResult], field: str) -> str:
        counts: dict[str, int] = {}
        for sample in samples:
            text = str(getattr(sample, field, "") or "").strip()
            if not text:
                continue
            counts[text] = int(counts.get(text, 0)) + 1
        if not counts:
            return ""
        return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]

    @classmethod
    def _reference_quality_text(cls, samples: list[SamplingResult]) -> str:
        thermometer = cls._dominant_text(samples, "thermometer_reference_status")
        pressure = cls._dominant_text(samples, "pressure_reference_status")
        failed = {"no_response", "corrupted_ascii", "truncated_ascii", "display_interrupted", "unsupported_command"}
        degraded = {"stale", "drift", "warmup_unstable", "wrong_unit_configuration"}
        statuses = {item for item in (thermometer, pressure) if item}
        if not samples or not statuses:
            if any(getattr(sample, "thermometer_temp_c", None) is not None or getattr(sample, "pressure_gauge_hpa", None) is not None for sample in samples):
                return "degraded"
            return "missing"
        if any(item in failed for item in statuses):
            return "failed"
        if any(item in degraded for item in statuses):
            return "degraded"
        if statuses <= {"healthy", "skipped_by_profile"}:
            return "healthy"
        return "degraded"

    @staticmethod
    def _format_frame_status(statuses: list[str]) -> str:
        if not statuses:
            return ""
        if len(statuses) == 1:
            return statuses[0]
        return ",".join(statuses)

    @staticmethod
    def _execution_status(stats_payload: dict[str, Any], samples: list[SamplingResult]) -> str:
        if "valid" in stats_payload:
            return "usable" if bool(stats_payload.get("valid")) else "rejected"
        if samples and any(bool(sample.frame_usable) for sample in samples):
            return "sampled"
        if samples:
            return "captured_no_usable_frames"
        return "pending"

    @staticmethod
    def _expected_analyzers(session: Optional[RunSession]) -> list[str]:
        if session is None:
            return []
        devices = getattr(getattr(session, "config", None), "devices", None)
        analyzers = getattr(devices, "gas_analyzers", []) if devices is not None else []
        values = []
        for index, item in enumerate(analyzers or []):
            if not bool(getattr(item, "enabled", True)):
                continue
            label = str(getattr(item, "id", "") or f"GA{index + 1:02d}").strip().upper()
            values.append(label)
        return values

    @staticmethod
    def _point_integrity_text(*, expected_count: int, present: set[str], usable: set[str]) -> str:
        if expected_count <= 0:
            return "not_configured"
        if not present:
            return "missing_all"
        if len(usable) == expected_count:
            return "complete"
        if len(usable) == 0:
            return "present_but_unusable"
        missing = expected_count - len(present)
        unusable = len(present - usable)
        if missing > 0 and unusable > 0:
            return "missing_and_unusable"
        if missing > 0:
            return "missing"
        if unusable > 0:
            return "unusable"
        return "partial"

    @staticmethod
    def _format_dt(value) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat(timespec="seconds")

    @staticmethod
    def _elapsed_s(session: RunSession) -> float:
        if session.started_at is None:
            return 0.0
        ended_at = session.ended_at or session.started_at
        return max(0.0, (ended_at - session.started_at).total_seconds())
