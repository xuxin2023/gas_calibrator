from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace
import threading

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventBus, EventType
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import QCService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager
from gas_calibrator.v2.qc.point_validator import PointValidationResult
from gas_calibrator.v2.qc.qc_report import QCReport, QCReporter
from gas_calibrator.v2.qc.quality_scorer import RunQualityScore


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}, "features": {"simulation_mode": True}})


def _build_service(tmp_path: Path) -> tuple[QCService, OrchestrationContext, RunState, SimpleNamespace, CalibrationPoint, list[SamplingResult]]:
    config = _config(tmp_path)
    session = RunSession(config)
    event_bus = EventBus()
    state_manager = StateManager(event_bus)
    result_store = ResultStore(tmp_path, session.run_id)
    run_logger = RunLogger(str(tmp_path), session.run_id)
    device_manager = DeviceManager(config.devices)
    stability_checker = StabilityChecker(config.workflow.stability)
    stop_event = threading.Event()
    pause_event = threading.Event()
    pause_event.set()
    context = OrchestrationContext(
        config=config,
        session=session,
        state_manager=state_manager,
        event_bus=event_bus,
        result_store=result_store,
        run_logger=run_logger,
        device_manager=device_manager,
        stability_checker=stability_checker,
        stop_event=stop_event,
        pause_event=pause_event,
    )
    run_state = RunState()
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    samples = [
        SamplingResult(
            point=point,
            analyzer_id="ga01",
            timestamp=datetime.now(),
            co2_ppm=401.0,
            h2o_mmol=9.0,
            pressure_hpa=1000.0,
            stability_time_s=1.0,
            total_time_s=2.0,
            point_phase="co2",
            point_tag="co2_tag",
        )
    ]
    validation = PointValidationResult(
        valid=False,
        point_index=1,
        usable_sample_count=1,
        outlier_ratio=0.0,
        quality_score=0.65,
        recommendation="review",
        reason="sample_qc_failed",
        failed_checks=[{"rule_name": "signal_span", "actual": 2.0, "threshold": 1.0, "message": "wide"}],
        ai_explanation="QC explanation",
    )
    run_score = RunQualityScore(overall_score=0.65, grade="D", recommendations=["Review invalid points before fitting."])
    report = QCReport(
        run_id=session.run_id,
        timestamp=datetime.now(),
        total_points=1,
        valid_points=0,
        invalid_points=1,
        overall_score=0.65,
        grade="D",
        point_details=[
            {
                "point_index": 1,
                "route": "co2",
                "valid": False,
                "quality_score": 0.65,
                "recommendation": "review",
                "reason": "sample_qc_failed",
                "failed_checks": [{"rule_name": "signal_span", "actual": 2.0, "threshold": 1.0, "message": "wide"}],
                "ai_explanation": "QC explanation",
            }
        ],
        recommendations=["Review invalid points before fitting."],
    )

    class FakePipeline:
        def __init__(self):
            self.reporter = QCReporter(run_id=session.run_id)
            self.point_validator = SimpleNamespace(
                min_sample_count=3,
                min_score=0.8,
                pass_threshold=0.8,
                warn_threshold=0.6,
                reject_threshold=0.4,
                max_outlier_ratio=0.2,
            )
            self._current_rule = None

        def process_point(self, point_arg, samples_arg, point_index=None, return_cleaned=None):
            assert point_arg == point
            assert samples_arg == samples
            return list(samples_arg), validation, 0.65

        def process_run(self, all_data):
            assert len(all_data) == 1
            return [validation], run_score, report

    warnings: list[str] = []
    context.event_bus.subscribe(EventType.WARNING_RAISED, lambda event: warnings.append(str(event.data["message"])))
    remembered: list[str] = []

    host = SimpleNamespace(
        qc_pipeline=FakePipeline(),
        _samples_for_point=lambda point_arg, phase="", point_tag="": list(samples),
        _clear_point_timing=lambda point_arg, phase="", point_tag="": None,
        _remember_output_file=remembered.append,
        remembered=remembered,
    )
    service = QCService(context, run_state, host=host)
    return service, context, run_state, host, point, samples


def test_qc_service_updates_run_state_and_exports_report(tmp_path: Path) -> None:
    service, context, run_state, host, point, samples = _build_service(tmp_path)

    service.run_point_qc(point, phase="co2", point_tag="co2_tag")
    service.export_qc_report()

    assert run_state.qc.cleaned_point_samples[1] == samples
    assert len(run_state.qc.point_qc_inputs) == 1
    assert len(run_state.qc.point_validations) == 1
    assert run_state.qc.run_quality_score is not None
    assert run_state.qc.qc_report is not None
    assert context.session.warnings == ["QC rejected point 1: sample_qc_failed"]
    assert service.get_cleaned_results(1) == samples
    assert (context.result_store.run_dir / "qc_report.json").exists()
    assert (context.result_store.run_dir / "qc_report.csv").exists()
    assert (context.result_store.run_dir / "qc_summary.json").exists()
    assert (context.result_store.run_dir / "qc_manifest.json").exists()
    assert (context.result_store.run_dir / "qc_reviewer_digest.md").exists()
    assert str(context.result_store.run_dir / "qc_report.json") in host.remembered
    assert str(context.result_store.run_dir / "qc_report.csv") in host.remembered
    assert str(context.result_store.run_dir / "qc_summary.json") in host.remembered
    qc_summary = json.loads((context.result_store.run_dir / "qc_summary.json").read_text(encoding="utf-8"))
    qc_manifest = json.loads((context.result_store.run_dir / "qc_manifest.json").read_text(encoding="utf-8"))
    reviewer_digest = (context.result_store.run_dir / "qc_reviewer_digest.md").read_text(encoding="utf-8")
    assert qc_summary["decision_counts"]["warn"] == 1
    assert qc_summary["run_gate"]["status"] == "warn"
    assert qc_summary["point_gate_summary"]["status"] == "warn"
    assert qc_summary["route_decision_breakdown"]["co2"]["warn"] == 1
    assert qc_summary["reject_reason_taxonomy"][0]["code"] == "sample_qc_failed"
    assert qc_summary["failed_check_taxonomy"][0]["code"] == "signal_span"
    assert qc_summary["reviewer_card"]["title"] == "质控审阅卡"
    assert qc_summary["reviewer_card"]["lines"]
    assert qc_summary["review_card_lines"]
    assert qc_summary["evidence_section"]["cards"]
    assert any(section["id"] == "gates" for section in qc_summary["review_sections"])
    assert qc_summary["evidence_source"] == "simulated_protocol"
    assert qc_manifest["artifacts"][0]["name"] == "qc_report_json"
    assert qc_manifest["point_gate_summary"]["status"] == "warn"
    assert qc_manifest["route_decision_breakdown"]["co2"]["warn"] == 1
    assert qc_manifest["reviewer_card"]["title"] == "质控审阅卡"
    assert "质控复核摘要" in reviewer_digest
    assert "点级门禁" in reviewer_digest
    assert "signal_span" in reviewer_digest
    csv_text = (context.result_store.run_dir / "qc_report.csv").read_text(encoding="utf-8")
    json_text = (context.result_store.run_dir / "qc_report.json").read_text(encoding="utf-8")
    assert "failed_checks" in csv_text
    assert "signal_span" in csv_text
    assert "result_level" in csv_text
    assert "\"result_level\": \"warn\"" in json_text

    context.run_logger.finalize()


def test_qc_service_records_postseal_quality_guard_stats_and_offline_review(tmp_path: Path) -> None:
    service, context, run_state, host, _point, _samples = _build_service(tmp_path)
    context.config.workflow.pressure.update(
        {
            "co2_postseal_quality_guards_enabled": True,
            "co2_postseal_low_pressure_max_hpa": 900.0,
            "co2_postseal_physical_qc_policy": "reject",
            "co2_postseal_physical_qc_max_abs_delta_c": 0.2,
            "co2_postsample_late_rebound_policy": "warn",
            "co2_postsample_late_rebound_max_rise_c": 0.15,
            "pressure_gauge_stale_ratio_warn_max": 0.1,
            "pressure_gauge_stale_ratio_reject_max": 0.8,
        }
    )

    point = CalibrationPoint(index=7, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=850.0, route="co2")
    sample = SimpleNamespace(
        point=point,
        analyzer_id="ga01",
        timestamp=datetime.now(),
        co2_ppm=401.5,
        h2o_mmol=7.1,
        pressure_hpa=850.0,
        stability_time_s=1.0,
        total_time_s=2.0,
        point_phase="co2",
        point_tag="co2_low_pressure",
        preseal_dewpoint_c=4.0,
        preseal_pressure_hpa=1000.0,
        dew_point_c=5.0,
        dewpoint_gate_result="rebound_veto",
        dewpoint_gate_pass_live_c=4.6,
        pressure_reference_status="fast_signal_stale",
    )
    validation = PointValidationResult(
        valid=False,
        point_index=point.index,
        usable_sample_count=1,
        outlier_ratio=0.0,
        quality_score=0.41,
        recommendation="review",
        reason="postseal_guard_review",
        failed_checks=[],
        ai_explanation="guard review",
    )

    host._samples_for_point = lambda point_arg, phase="", point_tag="": [sample]
    host._clear_point_timing = lambda point_arg, phase="", point_tag="": None
    host.qc_pipeline.process_point = (
        lambda point_arg, samples_arg, point_index=None, return_cleaned=None: ([sample], validation, 0.41)
    )

    service.run_point_qc(point, phase="co2", point_tag="co2_low_pressure")

    point_summaries = json.loads(context.result_store.point_summary_path.read_text(encoding="utf-8"))
    stats = dict(point_summaries[-1]["stats"])

    assert stats["co2_postseal_quality_guards_enabled"] is True
    assert stats["postseal_guard_status"] == "fail"
    assert stats["postseal_rebound_veto"] is True
    assert "postseal_rebound_veto" in stats["postseal_guard_flags"]
    assert "pressure_gauge_stale_ratio" in stats["postseal_guard_flags"]
    assert stats["postseal_expected_dewpoint_c"] is not None
    assert stats["postseal_physical_qc_status"] == "fail"
    assert stats["postsample_late_rebound_status"] == "warn"
    assert stats["pressure_gauge_stale_ratio"] == 1.0

    context.result_store.export_offline_artifacts(
        context.session,
        output_files=list(run_state.artifacts.output_files),
        export_statuses=dict(run_state.artifacts.export_statuses),
    )
    analytics_summary = json.loads(
        (context.result_store.run_dir / "analytics_summary.json").read_text(encoding="utf-8")
    )
    guard_review = dict(analytics_summary["qc_overview"]["postseal_guard_review"])

    assert guard_review["active_point_count"] == 1
    assert guard_review["rebound_veto_count"] == 1
    assert guard_review["physical_flagged_count"] == 1
    assert guard_review["late_rebound_flagged_count"] == 1
    assert guard_review["stale_flagged_count"] == 1
    assert any(section["id"] == "co2_postseal_quality" for section in analytics_summary["qc_overview"]["review_sections"])
    assert "simulation/offline/headless" in "\n".join(guard_review["lines"])

    context.run_logger.finalize()
