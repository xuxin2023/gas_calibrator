from __future__ import annotations

from pathlib import Path
import threading

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})


def test_orchestration_context_collects_runtime_dependencies(tmp_path: Path) -> None:
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

    assert context.config is config
    assert context.session is session
    assert context.state_manager is state_manager
    assert context.event_bus is event_bus
    assert context.result_store is result_store
    assert context.run_logger is run_logger
    assert context.device_manager is device_manager
    assert context.stability_checker is stability_checker
    assert context.stop_event is stop_event
    assert context.pause_event is pause_event
    assert context.data_writer is result_store.data_writer

    run_logger.finalize()
