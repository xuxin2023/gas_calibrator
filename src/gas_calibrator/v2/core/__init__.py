from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from importlib import import_module
import threading
from typing import TYPE_CHECKING, Any, Callable, DefaultDict


class EventType(Enum):
    WORKFLOW_STARTED = "workflow_started"
    WORKFLOW_COMPLETED = "workflow_completed"
    PHASE_CHANGED = "phase_changed"
    POINT_STARTED = "point_started"
    POINT_COMPLETED = "point_completed"
    STABILITY_PASSED = "stability_passed"
    SAMPLE_COLLECTED = "sample_collected"
    DEVICE_ERROR = "device_error"
    WARNING_RAISED = "warning_raised"


@dataclass(frozen=True)
class Event:
    type: EventType
    data: Any
    timestamp: datetime


class EventBus:
    """Thread-safe in-process event bus."""

    def __init__(self) -> None:
        self._handlers: DefaultDict[EventType, list[Callable[[Event], None]]] = defaultdict(list)
        self._lock = threading.RLock()

    def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        with self._lock:
            handlers = self._handlers[event_type]
            if handler not in handlers:
                handlers.append(handler)

    def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        with self._lock:
            handlers = self._handlers.get(event_type)
            if not handlers:
                return
            self._handlers[event_type] = [item for item in handlers if item is not handler]
            if not self._handlers[event_type]:
                self._handlers.pop(event_type, None)

    def publish(self, event_type: EventType, data: Any = None) -> None:
        event = Event(type=event_type, data=data, timestamp=datetime.now())
        with self._lock:
            handlers = list(self._handlers.get(event_type, []))
        for handler in handlers:
            handler(event)

    def clear(self) -> None:
        with self._lock:
            self._handlers.clear()

if TYPE_CHECKING:
    from ..config.models import AppConfig
    from .device_manager import DeviceManager
    from .result_store import ResultStore
    from .run_logger import RunLogger
    from .models import RunSession
    from gas_calibrator.validation.simulation.stability_checker import StabilityChecker
    from .state_manager import StateManager


@dataclass(frozen=True)
class OrchestrationContext:
    """Shared runtime dependencies for orchestration services."""

    config: AppConfig
    session: RunSession
    state_manager: StateManager
    event_bus: EventBus
    result_store: ResultStore
    run_logger: RunLogger
    device_manager: DeviceManager
    stability_checker: StabilityChecker
    stop_event: Any
    pause_event: Any

    @property
    def data_writer(self) -> Any:
        return self.result_store.data_writer

__all__ = [
    "CalibrationPhase",
    "CalibrationPoint",
    "CalibrationService",
    "CalibrationStatus",
    "DataWriter",
    "DeviceFactory",
    "DeviceInfo",
    "DeviceManager",
    "DeviceStatus",
    "DeviceType",
    "Event",
    "EventBus",
    "EventType",
    "OrchestrationContext",
    "CompiledPlan",
    "PlanCompiler",
    "PointFilter",
    "PointParser",
    "ResultStore",
    "RunLogger",
    "RunSession",
    "SamplingResult",
    "StateManager",
    "StabilityChecker",
    "StabilityResult",
    "StabilityType",
    "TemperatureGroup",
    "WorkflowOrchestrator",
    "Co2RouteStep",
    "FinalizeStep",
    "H2oRouteStep",
    "PrecheckStep",
    "SamplingStep",
    "StartupStep",
    "TemperatureGroupStep",
]

_EXPORT_MAP = {
    "CalibrationPhase": (".calibration_service", "CalibrationPhase"),
    "CalibrationPoint": (".models", "CalibrationPoint"),
    "CalibrationService": (".calibration_service", "CalibrationService"),
    "CalibrationStatus": (".models", "CalibrationStatus"),
    "DataWriter": (".data_writer", "DataWriter"),
    "DeviceFactory": (".device_factory", "DeviceFactory"),
    "DeviceInfo": (".device_manager", "DeviceInfo"),
    "DeviceManager": (".device_manager", "DeviceManager"),
    "DeviceStatus": (".device_manager", "DeviceStatus"),
    "DeviceType": (".device_factory", "DeviceType"),
    "CompiledPlan": (".plan_compiler", "CompiledPlan"),
    "PlanCompiler": (".plan_compiler", "PlanCompiler"),
    "PointFilter": (
        "gas_calibrator.validation.simulation.point_parser",
        "PointFilter",
    ),
    "PointParser": (
        "gas_calibrator.validation.simulation.point_parser",
        "PointParser",
    ),
    "ResultStore": (".result_store", "ResultStore"),
    "RunLogger": (".run_logger", "RunLogger"),
    "RunSession": (".models", "RunSession"),
    "SamplingResult": (".models", "SamplingResult"),
    "StateManager": (".state_manager", "StateManager"),
    "StabilityChecker": (
        "gas_calibrator.validation.simulation.stability_checker",
        "StabilityChecker",
    ),
    "StabilityResult": (
        "gas_calibrator.validation.simulation.stability_checker",
        "StabilityResult",
    ),
    "StabilityType": (
        "gas_calibrator.validation.simulation.stability_checker",
        "StabilityType",
    ),
    "TemperatureGroup": (
        "gas_calibrator.validation.simulation.point_parser",
        "TemperatureGroup",
    ),
    "WorkflowOrchestrator": (".orchestrator", "WorkflowOrchestrator"),
    "Co2RouteStep": (".workflow_steps", "Co2RouteStep"),
    "FinalizeStep": (".workflow_steps", "FinalizeStep"),
    "H2oRouteStep": (".workflow_steps", "H2oRouteStep"),
    "PrecheckStep": (".workflow_steps", "PrecheckStep"),
    "SamplingStep": (".workflow_steps", "SamplingStep"),
    "StartupStep": (".workflow_steps", "StartupStep"),
    "TemperatureGroupStep": (".workflow_steps", "TemperatureGroupStep"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORT_MAP.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attr_name = target
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
