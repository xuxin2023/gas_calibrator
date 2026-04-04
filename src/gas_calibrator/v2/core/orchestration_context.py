from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..config import AppConfig
from .device_manager import DeviceManager
from .event_bus import EventBus
from .result_store import ResultStore
from .run_logger import RunLogger
from .session import RunSession
from .stability_checker import StabilityChecker
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
