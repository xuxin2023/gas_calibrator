from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import threading
import time
import json

import pytest

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventBus, EventType
from gas_calibrator.v2.core.models import CalibrationPhase, CalibrationPoint
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import StatusService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager
from gas_calibrator.v2.exceptions import WorkflowInterruptedError


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})


def _build_service(tmp_path: Path) -> tuple[StatusService, OrchestrationContext, RunState, SimpleNamespace]:
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
    host = SimpleNamespace(
        _log_callback=None,
        _timing_key=lambda point, phase="", point_tag="": (
            f"{str(phase or point.route or '').strip().lower()}:{point_tag or point.index}"
        ),
    )
    return StatusService(context, run_state, host=host), context, run_state, host


def test_status_service_updates_status_logs_and_tracks_timing(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    warnings: list[str] = []
    context.event_bus.subscribe(EventType.WARNING_RAISED, lambda event: warnings.append(str(event.data["message"])))
    callback_messages: list[str] = []
    host._log_callback = callback_messages.append

    context.state_manager.prepare_run(1)
    context.state_manager.start()
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    service.update_status(phase=CalibrationPhase.SAMPLING, current_point=point, message="sampling")
    service.log("warn: pressure drift observed")
    service.remember_output_file("summary.json")
    service.begin_point_timing(point, phase="co2", point_tag="co2_1")
    time.sleep(0.01)
    service.mark_point_stable_for_sampling(point, phase="co2", point_tag="co2_1")
    timing = service.finish_point_timing(point, phase="co2", point_tag="co2_1")
    service.mark_point_completed(point, point_tag="co2_1", stability_time_s=timing["stability_time_s"], total_time_s=timing["total_time_s"])
    service.clear_point_timing(point, phase="co2", point_tag="co2_1")

    status = context.state_manager.status
    assert status.phase is CalibrationPhase.SAMPLING
    assert context.session.phase is CalibrationPhase.SAMPLING
    assert context.session.current_point == point
    assert "warn: pressure drift observed" in context.session.warnings
    assert warnings == ["warn: pressure drift observed"]
    assert callback_messages == ["warn: pressure drift observed"]
    assert run_state.artifacts.output_files == ["summary.json"]
    assert timing["stability_time_s"] is not None
    assert timing["total_time_s"] is not None
    assert timing["total_time_s"] >= timing["stability_time_s"]
    assert run_state.timing.point_contexts == {}
    assert context.state_manager.status.completed_points == 1

    context.run_logger.finalize()


def test_status_service_check_stop_and_pause_semantics(tmp_path: Path) -> None:
    service, context, _, _ = _build_service(tmp_path)

    context.stop_event.set()
    with pytest.raises(WorkflowInterruptedError):
        service.check_stop()

    context.stop_event.clear()
    context.pause_event.clear()
    releaser = threading.Timer(0.05, context.pause_event.set)
    releaser.start()
    started = time.monotonic()
    service.check_stop()
    elapsed = time.monotonic() - started

    assert elapsed >= 0.04
    context.run_logger.finalize()


def test_status_service_record_route_trace_writes_jsonl_and_tracks_artifact(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    point = CalibrationPoint(index=3, temperature_c=25.0, pressure_hpa=1000.0, route="co2", co2_ppm=400.0)
    host.route_context = SimpleNamespace(current_route="co2", active_point=point, current_point=point, source_point=point, point_tag="")
    host.route_planner = SimpleNamespace(
        co2_point_tag=lambda item: f"co2_groupa_{int(item.co2_ppm)}ppm_{int(item.target_pressure_hpa)}hpa",
        h2o_point_tag=lambda item: f"h2o_{int(item.temp_chamber_c)}c",
    )

    service.record_route_trace(
        action="sample_start",
        point=point,
        target={"pressure_hpa": 1000.0},
        actual={"pressure_hpa": 999.8},
        relay_state={"relay_a": {"3": True}},
        result="ok",
        message="sampling",
    )

    trace_path = context.result_store.run_dir / "route_trace.jsonl"
    assert trace_path.exists()
    payload = json.loads(trace_path.read_text(encoding="utf-8").strip())
    assert payload["run_id"] == context.session.run_id
    assert payload["route"] == "co2"
    assert payload["point_index"] == 3
    assert payload["point_tag"] == "co2_groupa_400ppm_1000hpa"
    assert payload["action"] == "sample_start"
    assert payload["target"]["pressure_hpa"] == 1000.0
    assert payload["actual"]["pressure_hpa"] == 999.8
    assert payload["relay_state"]["relay_a"]["3"] is True
    assert payload["result"] == "ok"
    assert str(trace_path) in run_state.artifacts.output_files

    context.run_logger.finalize()
