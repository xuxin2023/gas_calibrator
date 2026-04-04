from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import threading

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import AIExplanationService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager


def _build_service(tmp_path: Path) -> tuple[AIExplanationService, OrchestrationContext, RunState, SimpleNamespace]:
    config = AppConfig.from_dict(
        {
            "paths": {"output_dir": str(tmp_path)},
            "ai": {
                "enabled": True,
                "features": {"run_summary": True, "anomaly_diagnosis": True},
            },
        }
    )
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
    remembered: list[str] = []
    logs: list[str] = []
    host = SimpleNamespace(
        _remember_output_file=remembered.append,
        _log=logs.append,
        remembered=remembered,
        logs=logs,
        service=SimpleNamespace(ai_runtime=None),
    )
    return AIExplanationService(context, run_state, host=host), context, run_state, host


def test_ai_explanation_service_generates_anomaly_and_summary_outputs(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    run_state.qc.qc_report = SimpleNamespace(point_details=[{"point_index": 1, "valid": False, "reason": "humidity unstable"}])
    context.session.add_warning("humidity unstable")
    context.run_logger.log_io("ga01", "TX", "READ")

    class FakeAdvisor:
        def diagnose_run(self, *, failed_points, device_events, alarms):
            assert failed_points
            assert alarms
            return "诊断结论：湿度发生器响应慢。"

    class FakeSummarizer:
        def write_summary(self, run_dir, *, anomaly_diagnosis=""):
            path = Path(run_dir) / "run_summary.txt"
            path.write_text(f"AI summary\n{anomaly_diagnosis}\n", encoding="utf-8")
            return path.read_text(encoding="utf-8")

    host.service.ai_runtime = SimpleNamespace(
        anomaly_advisor=FakeAdvisor(),
        summarizer=FakeSummarizer(),
    )

    service.generate_ai_outputs()

    anomaly_txt = context.result_store.run_dir / "anomaly_diagnosis.txt"
    anomaly_json = context.result_store.run_dir / "anomaly_diagnosis.json"
    summary_txt = context.result_store.run_dir / "run_summary.txt"
    assert anomaly_txt.exists()
    assert anomaly_json.exists()
    assert summary_txt.exists()
    assert str(anomaly_txt) in host.remembered
    assert str(anomaly_json) in host.remembered
    assert any("AI anomaly diagnosis generated" in message for message in host.logs)
    assert any("AI run summary generated" in message for message in host.logs)

    context.run_logger.finalize()


def test_ai_explanation_service_swallow_failures_during_generate_outputs(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    run_state.qc.qc_report = SimpleNamespace(point_details=[{"point_index": 1, "valid": False, "reason": "humidity unstable"}])

    class FailingAdvisor:
        def diagnose_run(self, **kwargs):
            raise RuntimeError("advisor unavailable")

    class FailingSummarizer:
        def write_summary(self, run_dir, *, anomaly_diagnosis=""):
            raise RuntimeError("summary unavailable")

    host.service.ai_runtime = SimpleNamespace(
        anomaly_advisor=FailingAdvisor(),
        summarizer=FailingSummarizer(),
    )

    service.generate_ai_outputs()

    assert any("AI anomaly diagnosis warning: advisor unavailable" in message for message in host.logs)
    assert any("AI summary warning: summary unavailable" in message for message in host.logs)

    context.run_logger.finalize()
