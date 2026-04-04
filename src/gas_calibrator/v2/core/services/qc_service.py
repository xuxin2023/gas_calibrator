from __future__ import annotations

from typing import Any, Optional

from ....validation.dewpoint_flush_gate import predict_pressure_scaled_dewpoint_c
from ..acceptance_model import build_user_visible_evidence_boundary
from ..event_bus import EventType
from ..models import CalibrationPoint, SamplingResult
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState


class QCService:
    """QC orchestration helpers backed by the existing QCPipeline/QCReporter."""

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def get_cleaned_results(self, point_index: Optional[int] = None) -> list[SamplingResult]:
        cleaned = self.run_state.qc.cleaned_point_samples
        if point_index is None:
            combined: list[SamplingResult] = []
            for samples in cleaned.values():
                combined.extend(samples)
            return list(combined)
        return list(cleaned.get(point_index, []))

    def run_point_qc(
        self,
        point: CalibrationPoint,
        *,
        phase: str = "",
        point_tag: str = "",
    ) -> None:
        resolved_phase = str(phase or ("h2o" if point.is_h2o_point else "co2")).strip().lower()
        resolved_tag = str(point_tag or "").strip()
        samples = self.host._samples_for_point(point, phase=resolved_phase, point_tag=resolved_tag)
        if not samples:
            return
        cleaned_samples, validation, point_score = self.host.qc_pipeline.process_point(
            point,
            samples,
            point_index=point.index,
            return_cleaned=True,
        )
        self.run_state.qc.cleaned_point_samples[point.index] = list(cleaned_samples)
        setattr(self.host, "_cleaned_point_samples", self.run_state.qc.cleaned_point_samples)
        validations = [item for item in self.run_state.qc.point_validations if item.point_index != point.index]
        validations.append(validation)
        self._set_point_validations(validations)
        qc_inputs = [item for item in self.run_state.qc.point_qc_inputs if item[0].index != point.index]
        qc_inputs.append((point, samples))
        self._set_point_qc_inputs(qc_inputs)
        postseal_guard_stats = self._build_postseal_guard_stats(
            point,
            samples=samples,
            cleaned_samples=cleaned_samples,
            phase=resolved_phase,
        )
        self.context.result_store.save_point_summary(
            point,
            {
                "point_phase": resolved_phase,
                "point_tag": resolved_tag,
                "usable_sample_count": validation.usable_sample_count,
                "outlier_ratio": validation.outlier_ratio,
                "quality_score": validation.quality_score,
                "point_quality_score": point_score,
                "valid": validation.valid,
                "recommendation": validation.recommendation,
                "reason": validation.reason,
                "failed_checks": list(getattr(validation, "failed_checks", []) or []),
                "ai_explanation": str(getattr(validation, "ai_explanation", "") or ""),
                "raw_sample_count": len(samples),
                "cleaned_sample_count": len(cleaned_samples),
                "removed_sample_count": max(0, len(samples) - len(cleaned_samples)),
                "stability_time_s": samples[0].stability_time_s,
                "total_time_s": samples[0].total_time_s,
                **postseal_guard_stats,
            },
        )
        if resolved_tag:
            self.host._clear_point_timing(point, phase=resolved_phase, point_tag=resolved_tag)
        if not validation.valid:
            warning = f"QC rejected point {point.index}: {validation.reason}"
            self.context.session.add_warning(warning)
            self.context.event_bus.publish(EventType.WARNING_RAISED, {"message": warning, "point_index": point.index})

    def export_qc_report(self) -> dict[str, str]:
        if not self.run_state.qc.point_qc_inputs:
            return {"status": "skipped", "error": "no qc point inputs"}
        validations, run_score, report = self.host.qc_pipeline.process_run(
            self.run_state.qc.point_qc_inputs
        )
        pipeline = getattr(self.host, "qc_pipeline", None)
        reporter = getattr(pipeline, "reporter", None)
        if reporter is None:
            return {"status": "error", "error": "qc reporter is unavailable"}
        rule_profile = self._rule_profile()
        threshold_profile = self._threshold_profile()
        evidence_boundary = self._evidence_boundary()
        point_results = []
        for (point, _samples), validation in zip(list(self.run_state.qc.point_qc_inputs or []), list(validations or []), strict=False):
            point_results.append((point, validation))
        report = reporter.generate(
            point_results,
            run_score,
            rule_profile=rule_profile,
            threshold_profile=threshold_profile,
            evidence_boundary=evidence_boundary,
        )
        self._set_point_validations(validations)
        self._set_run_quality_score(run_score)
        self._set_qc_report(report)
        qc_json_path = self.context.result_store.run_dir / "qc_report.json"
        qc_csv_path = self.context.result_store.run_dir / "qc_report.csv"
        qc_summary_path = self.context.result_store.run_dir / "qc_summary.json"
        qc_manifest_path = self.context.result_store.run_dir / "qc_manifest.json"
        qc_reviewer_digest_path = self.context.result_store.run_dir / "qc_reviewer_digest.md"
        reporter.export_json(report, qc_json_path)
        reporter.export_csv(report, qc_csv_path)
        reporter.export_summary_json(report, qc_summary_path)
        reporter.export_reviewer_digest_markdown(report, qc_reviewer_digest_path)
        reporter.export_manifest_json(
            report,
            qc_manifest_path,
            report_json_path=qc_json_path,
            report_csv_path=qc_csv_path,
            summary_path=qc_summary_path,
            reviewer_digest_path=qc_reviewer_digest_path,
        )
        for path in (
            qc_json_path,
            qc_csv_path,
            qc_summary_path,
            qc_manifest_path,
            qc_reviewer_digest_path,
        ):
            self.host._remember_output_file(str(path))
        return {
            "status": "ok",
            "path": str(qc_json_path),
            "summary_path": str(qc_summary_path),
            "manifest_path": str(qc_manifest_path),
        }

    def _set_point_validations(self, validations: list[Any]) -> None:
        self.run_state.qc.point_validations = list(validations)
        setattr(self.host, "_point_validations", self.run_state.qc.point_validations)

    def _set_point_qc_inputs(self, inputs: list[tuple[CalibrationPoint, list[SamplingResult]]]) -> None:
        self.run_state.qc.point_qc_inputs = list(inputs)
        setattr(self.host, "_point_qc_inputs", self.run_state.qc.point_qc_inputs)

    def _set_run_quality_score(self, run_score: Any) -> None:
        self.run_state.qc.run_quality_score = run_score
        setattr(self.host, "_run_quality_score", run_score)

    def _set_qc_report(self, report: Any) -> None:
        self.run_state.qc.qc_report = report
        setattr(self.host, "_qc_report", report)

    def _rule_profile(self) -> dict[str, Any]:
        pipeline = getattr(self.host, "qc_pipeline", None)
        current_rule = getattr(pipeline, "_current_rule", None)
        return {
            "name": str(getattr(current_rule, "name", "") or "default"),
            "route": str(getattr(current_rule, "route", "") or "both"),
            "mode": str(getattr(current_rule, "mode", "") or "normal"),
            "source": "custom_rule" if current_rule is not None else "default",
        }

    def _threshold_profile(self) -> dict[str, Any]:
        pipeline = getattr(self.host, "qc_pipeline", None)
        validator = getattr(pipeline, "point_validator", None)
        return {
            "min_sample_count": int(getattr(validator, "min_sample_count", 0) or 0),
            "quality_threshold": float(getattr(validator, "min_score", 0.0) or 0.0),
            "pass_threshold": float(getattr(validator, "pass_threshold", 0.0) or 0.0),
            "warn_threshold": float(getattr(validator, "warn_threshold", 0.0) or 0.0),
            "reject_threshold": float(getattr(validator, "reject_threshold", 0.0) or 0.0),
            "max_outlier_ratio": float(getattr(validator, "max_outlier_ratio", 0.0) or 0.0),
        }

    def _evidence_boundary(self) -> dict[str, Any]:
        features = getattr(self.context.config, "features", None)
        return build_user_visible_evidence_boundary(
            simulation_mode=bool(getattr(features, "simulation_mode", False)),
        )

    def _build_postseal_guard_stats(
        self,
        point: CalibrationPoint,
        *,
        samples: list[SamplingResult],
        cleaned_samples: list[SamplingResult],
        phase: str,
    ) -> dict[str, Any]:
        cfg = self._postseal_guard_config(point)
        stats = {
            "co2_postseal_quality_guards_enabled": bool(cfg.get("enabled", False)),
            "postseal_guard_scope": "simulation_offline_review",
            "postseal_guard_status": "skipped",
            "postseal_guard_flags": "",
            "postseal_guard_reason": "guard_disabled",
            "postseal_rebound_veto": False,
            "preseal_dewpoint_c": self._sample_value(samples, "preseal_dewpoint_c"),
            "preseal_pressure_hpa": self._sample_value(samples, "preseal_pressure_hpa"),
            "postseal_expected_dewpoint_c": self._sample_value(samples, "postseal_expected_dewpoint_c"),
            "postseal_actual_dewpoint_c": self._sample_value(samples, "postseal_actual_dewpoint_c", fallback_keys=("dew_point_c", "dewpoint_live_c")),
            "postseal_physical_delta_c": self._sample_value(samples, "postseal_physical_delta_c"),
            "postseal_physical_qc_status": str(self._sample_value(samples, "postseal_physical_qc_status") or "skipped"),
            "postseal_physical_qc_reason": str(self._sample_value(samples, "postseal_physical_qc_reason") or ""),
            "dewpoint_gate_pass_live_c": self._sample_value(samples, "dewpoint_gate_pass_live_c"),
            "first_effective_sample_dewpoint_c": self._sample_value(
                cleaned_samples or samples,
                "first_effective_sample_dewpoint_c",
                fallback_keys=("dew_point_c", "dewpoint_live_c"),
            ),
            "postgate_to_first_effective_dewpoint_rise_c": self._sample_value(samples, "postgate_to_first_effective_dewpoint_rise_c"),
            "postsample_late_rebound_status": str(self._sample_value(samples, "postsample_late_rebound_status") or "skipped"),
            "postsample_late_rebound_reason": str(self._sample_value(samples, "postsample_late_rebound_reason") or ""),
            "pressure_gauge_stale_count": 0,
            "pressure_gauge_total_count": 0,
            "pressure_gauge_stale_ratio": None,
        }

        stale_count = 0
        total_count = 0
        for sample in list(samples or []):
            stale_marker = str(
                self._sample_value([sample], "pressure_gauge_error", fallback_keys=("pressure_reference_status",)) or ""
            ).strip().lower()
            if not stale_marker:
                continue
            total_count += 1
            if stale_marker in {"fast_signal_stale", "stale"}:
                stale_count += 1
        stats["pressure_gauge_stale_count"] = stale_count
        stats["pressure_gauge_total_count"] = total_count
        stats["pressure_gauge_stale_ratio"] = round(stale_count / total_count, 6) if total_count else None

        if not bool(cfg.get("enabled", False)):
            return stats
        if not self._is_co2_low_pressure_sealed_point(point, cfg=cfg, phase=phase):
            stats["postseal_guard_reason"] = "not_co2_low_pressure_sealed_point"
            return stats

        flags: list[str] = []
        reasons: list[str] = []
        guard_status = "pass"

        def _flag(flag: str, severity: str, reason: str) -> None:
            nonlocal guard_status
            if flag not in flags:
                flags.append(flag)
            if reason:
                reasons.append(reason)
            if severity == "fail":
                guard_status = "fail"
            elif severity == "warn" and guard_status != "fail":
                guard_status = "warn"

        gate_result = str(self._sample_value(samples, "dewpoint_gate_result") or "").strip().lower()
        if gate_result == "rebound_veto":
            stats["postseal_rebound_veto"] = True
            _flag("postseal_rebound_veto", "fail", "dewpoint_gate_result=rebound_veto")

        if stats["postseal_expected_dewpoint_c"] is None:
            stats["postseal_expected_dewpoint_c"] = predict_pressure_scaled_dewpoint_c(
                stats.get("preseal_dewpoint_c"),
                stats.get("preseal_pressure_hpa"),
                point.target_pressure_hpa,
            )
        if (
            stats.get("postseal_expected_dewpoint_c") is not None
            and stats.get("postseal_actual_dewpoint_c") is not None
            and stats.get("postseal_physical_delta_c") is None
        ):
            stats["postseal_physical_delta_c"] = round(
                float(stats["postseal_actual_dewpoint_c"]) - float(stats["postseal_expected_dewpoint_c"]),
                6,
            )
        if (
            str(stats.get("postseal_physical_qc_status") or "").strip().lower() == "skipped"
            and stats.get("postseal_physical_delta_c") is not None
            and stats.get("postseal_expected_dewpoint_c") is not None
        ):
            max_abs_delta_c = float(cfg.get("physical_qc_max_abs_delta_c", 0.0) or 0.0)
            delta_c = float(stats["postseal_physical_delta_c"])
            if abs(delta_c) <= max_abs_delta_c:
                stats["postseal_physical_qc_status"] = "pass"
                stats["postseal_physical_qc_reason"] = ""
            else:
                policy = str(cfg.get("physical_qc_policy") or "off")
                stats["postseal_physical_qc_status"] = "fail"
                stats["postseal_physical_qc_reason"] = (
                    f"abs_delta_c={abs(delta_c):.3f}>max_abs_delta_c={max_abs_delta_c:.3f};policy={policy}"
                )
        if str(stats.get("postseal_physical_qc_status") or "").strip().lower() == "fail":
            policy = str(cfg.get("physical_qc_policy") or "off").lower()
            if policy == "warn":
                _flag(
                    "postseal_physical_qc",
                    "warn",
                    str(stats.get("postseal_physical_qc_reason") or "postseal_physical_qc(policy=warn)"),
                )
            elif policy == "reject":
                _flag(
                    "postseal_physical_qc",
                    "fail",
                    str(stats.get("postseal_physical_qc_reason") or "postseal_physical_qc(policy=reject)"),
                )

        if stats.get("dewpoint_gate_pass_live_c") is not None and stats.get("first_effective_sample_dewpoint_c") is not None:
            rise_c = float(stats["first_effective_sample_dewpoint_c"]) - float(stats["dewpoint_gate_pass_live_c"])
            stats["postgate_to_first_effective_dewpoint_rise_c"] = round(rise_c, 6)
            if str(stats.get("postsample_late_rebound_status") or "").strip().lower() == "skipped":
                max_rise_c = float(cfg.get("postsample_late_rebound_max_rise_c", 0.0) or 0.0)
                if rise_c <= max_rise_c:
                    stats["postsample_late_rebound_status"] = "pass"
                    stats["postsample_late_rebound_reason"] = ""
                else:
                    policy = str(cfg.get("postsample_late_rebound_policy") or "off").lower()
                    stats["postsample_late_rebound_status"] = "fail" if policy == "reject" else "warn"
                    stats["postsample_late_rebound_reason"] = (
                        f"rise_c={rise_c:.3f}>max_rise_c={max_rise_c:.3f};policy={policy}"
                    )
        late_status = str(stats.get("postsample_late_rebound_status") or "").strip().lower()
        if late_status == "warn":
            _flag(
                "postsample_late_rebound",
                "warn",
                str(stats.get("postsample_late_rebound_reason") or "postsample_late_rebound"),
            )
        elif late_status == "fail":
            _flag(
                "postsample_late_rebound",
                "fail",
                str(stats.get("postsample_late_rebound_reason") or "postsample_late_rebound"),
            )

        stale_ratio = stats.get("pressure_gauge_stale_ratio")
        if stale_ratio is not None:
            stale_reject_max = cfg.get("pressure_gauge_stale_ratio_reject_max")
            stale_warn_max = cfg.get("pressure_gauge_stale_ratio_warn_max")
            if stale_reject_max is not None and float(stale_ratio) > float(stale_reject_max):
                _flag(
                    "pressure_gauge_stale_ratio",
                    "fail",
                    f"pressure_gauge_stale_ratio={float(stale_ratio):.3f}>reject_max={float(stale_reject_max):.3f}",
                )
            elif stale_warn_max is not None and float(stale_ratio) > float(stale_warn_max):
                _flag(
                    "pressure_gauge_stale_ratio",
                    "warn",
                    f"pressure_gauge_stale_ratio={float(stale_ratio):.3f}>warn_max={float(stale_warn_max):.3f}",
                )

        stats["postseal_guard_status"] = guard_status
        stats["postseal_guard_flags"] = ",".join(flags)
        stats["postseal_guard_reason"] = ";".join(reasons) if reasons else "passed"
        return stats

    def _postseal_guard_config(self, point: CalibrationPoint) -> dict[str, Any]:
        pressure_cfg = dict(getattr(getattr(self.context.config, "workflow", None), "pressure", {}) or {})
        return {
            "enabled": bool(pressure_cfg.get("co2_postseal_quality_guards_enabled", False)),
            "low_pressure_max_hpa": float(pressure_cfg.get("co2_postseal_low_pressure_max_hpa", 900.0) or 900.0),
            "physical_qc_policy": self._normalized_policy(
                pressure_cfg.get("co2_postseal_physical_qc_policy", "off"),
                allowed={"off", "warn", "reject"},
                default="off",
            ),
            "physical_qc_max_abs_delta_c": float(
                pressure_cfg.get("co2_postseal_physical_qc_max_abs_delta_c", 0.35) or 0.35
            ),
            "postsample_late_rebound_policy": self._normalized_policy(
                pressure_cfg.get("co2_postsample_late_rebound_policy", "off"),
                allowed={"off", "warn", "reject"},
                default="off",
            ),
            "postsample_late_rebound_max_rise_c": float(
                pressure_cfg.get("co2_postsample_late_rebound_max_rise_c", 0.25) or 0.25
            ),
            "pressure_gauge_stale_ratio_warn_max": self._as_float(
                pressure_cfg.get("pressure_gauge_stale_ratio_warn_max")
            ),
            "pressure_gauge_stale_ratio_reject_max": self._as_float(
                pressure_cfg.get("pressure_gauge_stale_ratio_reject_max")
            ),
            "target_pressure_hpa": point.target_pressure_hpa,
        }

    @staticmethod
    def _normalized_policy(value: Any, *, allowed: set[str], default: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized in allowed:
            return normalized
        return default

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _sample_value(
        self,
        samples: list[Any],
        key: str,
        *,
        fallback_keys: tuple[str, ...] = (),
    ) -> Any:
        for sample in list(samples or []):
            for candidate in (key, *fallback_keys):
                if isinstance(sample, dict):
                    value = sample.get(candidate)
                else:
                    value = getattr(sample, candidate, None)
                if value not in (None, ""):
                    return value
        return None

    @staticmethod
    def _is_co2_low_pressure_sealed_point(
        point: CalibrationPoint,
        *,
        cfg: dict[str, Any],
        phase: str,
    ) -> bool:
        if str(phase or "").strip().lower() != "co2":
            return False
        if point.is_h2o_point or point.is_ambient_pressure_point:
            return False
        if point.target_pressure_hpa is None:
            return False
        return float(point.target_pressure_hpa) < float(cfg.get("low_pressure_max_hpa", 900.0) or 900.0)
