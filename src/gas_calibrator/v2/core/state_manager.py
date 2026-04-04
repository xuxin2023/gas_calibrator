from __future__ import annotations

from dataclasses import replace
import threading
import time
from typing import Callable, Optional

from .event_bus import EventBus, EventType
from .models import CalibrationPhase, CalibrationPoint, CalibrationStatus


class StateManager:
    """Maintains calibration status and progress state."""

    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self._status = CalibrationStatus()
        self._started_at: Optional[float] = None
        self._lock = threading.RLock()
        self._progress_callback: Optional[Callable[[CalibrationStatus], None]] = None
        self._planned_point_keys: tuple[str, ...] = ()
        self._completed_point_keys: set[str] = set()

    @property
    def status(self) -> CalibrationStatus:
        with self._lock:
            return replace(self._status)

    def set_progress_callback(self, callback: Optional[Callable[[CalibrationStatus], None]]) -> None:
        with self._lock:
            self._progress_callback = callback

    def load_points(self, total_points: int, message: str, *, point_keys: Optional[list[str]] = None) -> None:
        effective_total, planned_keys = self._normalize_progress_inputs(total_points, point_keys=point_keys)
        with self._lock:
            self._planned_point_keys = planned_keys
            self._completed_point_keys = set()
            self._status = replace(
                self._status,
                total_points=effective_total,
                completed_points=0,
                progress=0.0,
                current_point=None,
                message=message,
                error=None,
            )
        self._emit()

    def prepare_run(
        self,
        total_points: int,
        message: str = "Calibration service starting",
        *,
        point_keys: Optional[list[str]] = None,
    ) -> None:
        effective_total, planned_keys = self._normalize_progress_inputs(total_points, point_keys=point_keys)
        with self._lock:
            self._planned_point_keys = planned_keys
            self._completed_point_keys = set()
            self._status = replace(
                self._status,
                phase=CalibrationPhase.IDLE,
                total_points=effective_total,
                completed_points=0,
                progress=0.0,
                current_point=None,
                message=message,
                elapsed_s=0.0,
                error=None,
            )
        self._emit()

    def start(self) -> None:
        with self._lock:
            self._started_at = time.monotonic()
            self._status = replace(self._status, elapsed_s=0.0)
        self._emit()

    def update_status(
        self,
        *,
        phase: Optional[CalibrationPhase] = None,
        current_point: Optional[CalibrationPoint] = None,
        message: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        with self._lock:
            old_phase = self._status.phase
            self._status = replace(
                self._status,
                phase=phase or self._status.phase,
                current_point=current_point,
                message=message if message is not None else self._status.message,
                elapsed_s=self._elapsed_s(),
                error=error,
            )
        if self._status.phase is not old_phase:
            self.event_bus.publish(
                EventType.PHASE_CHANGED,
                {"from": old_phase.value, "to": self._status.phase.value},
            )
        self._emit()

    def mark_point_completed(self, point: CalibrationPoint, *, point_key: str = "") -> None:
        with self._lock:
            resolved_key = str(point_key or self._default_point_key(point)).strip()
            if resolved_key:
                self._completed_point_keys.add(resolved_key)
            completed_points = len(self._completed_point_keys) if self._completed_point_keys else self._status.completed_points + 1
            total_points = len(self._planned_point_keys) if self._planned_point_keys else self._status.total_points
            progress = min(1.0, (completed_points / total_points)) if total_points else 0.0
            self._status = replace(
                self._status,
                current_point=point,
                completed_points=completed_points,
                progress=progress,
                message=f"Completed point {point.index}",
                elapsed_s=self._elapsed_s(),
            )
        self._emit()

    def pause(self) -> None:
        self.update_status(message="Calibration paused")

    def resume(self) -> None:
        self.update_status(message="Calibration resumed")

    def complete(self) -> None:
        self.update_status(
            phase=CalibrationPhase.COMPLETED,
            current_point=None,
            message="Calibration completed",
        )

    def stop(self, message: str = "Calibration stopped") -> None:
        self.update_status(
            phase=CalibrationPhase.STOPPED,
            current_point=None,
            message=message,
        )

    def set_error(self, error: str) -> None:
        self.update_status(
            phase=CalibrationPhase.ERROR,
            current_point=None,
            message=f"Calibration failed: {error}",
            error=error,
        )

    def _emit(self) -> None:
        with self._lock:
            callback = self._progress_callback
            status = replace(self._status)
        if callback is None:
            return
        try:
            callback(status)
        except Exception:
            pass

    def _elapsed_s(self) -> float:
        if self._started_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self._started_at)

    def _normalize_progress_inputs(
        self,
        total_points: int,
        *,
        point_keys: Optional[list[str]],
    ) -> tuple[int, tuple[str, ...]]:
        normalized_keys = tuple(dict.fromkeys(str(item).strip() for item in (point_keys or []) if str(item).strip()))
        if normalized_keys:
            return len(normalized_keys), normalized_keys
        return int(total_points), ()

    @staticmethod
    def _default_point_key(point: CalibrationPoint) -> str:
        route = str(getattr(point, "route", "") or "").strip().lower()
        return f"{route}:{int(point.index)}"
