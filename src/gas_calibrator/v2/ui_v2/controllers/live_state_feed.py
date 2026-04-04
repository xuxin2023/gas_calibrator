from __future__ import annotations

import tkinter as tk
from typing import Any, Callable, Optional

from .app_facade import AppFacade


class LiveStateFeed:
    """Polling-based live feed for the Tk shell."""

    def __init__(self, facade: AppFacade, *, interval_ms: int = 750) -> None:
        self.facade = facade
        self.interval_ms = max(100, int(interval_ms))
        self._sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._devices_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._algorithms_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._reports_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._timeseries_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._qc_overview_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._winner_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._export_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._route_progress_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._reject_reason_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._residual_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._analyzer_health_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._error_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._busy_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._notification_sink: Optional[Callable[[dict[str, Any]], None]] = None
        self._root: Optional[tk.Misc] = None
        self._after_id: Optional[str] = None
        self._running = False

    def set_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._sink = sink

    def set_devices_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._devices_sink = sink

    def set_algorithms_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._algorithms_sink = sink

    def set_reports_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._reports_sink = sink

    def set_timeseries_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._timeseries_sink = sink

    def set_qc_overview_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._qc_overview_sink = sink

    def set_winner_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._winner_sink = sink

    def set_export_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._export_sink = sink

    def set_route_progress_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._route_progress_sink = sink

    def set_reject_reason_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._reject_reason_sink = sink

    def set_residual_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._residual_sink = sink

    def set_analyzer_health_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._analyzer_health_sink = sink

    def set_error_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._error_sink = sink

    def set_busy_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._busy_sink = sink

    def set_notification_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._notification_sink = sink

    def bind_root(self, root: tk.Misc) -> None:
        self._root = root

    def start(self) -> None:
        if self._root is None:
            raise RuntimeError("LiveStateFeed root is not bound")
        if self._running:
            return
        self._running = True
        self._tick()

    def stop(self) -> None:
        self._running = False
        if self._root is not None and self._after_id is not None:
            try:
                self._root.after_cancel(self._after_id)
            except Exception:
                pass
        self._after_id = None

    def poll_once(self) -> dict[str, Any]:
        snapshot = self.facade.build_snapshot()
        self._publish(self._sink, snapshot)
        self._publish(self._devices_sink, dict(snapshot.get("devices", {}) or {}))
        self._publish(self._algorithms_sink, dict(snapshot.get("algorithms", {}) or {}))
        self._publish(self._reports_sink, dict(snapshot.get("reports", {}) or {}))
        self._publish(self._timeseries_sink, dict(snapshot.get("timeseries", {}) or {}))
        self._publish(self._qc_overview_sink, dict(snapshot.get("qc_overview", {}) or {}))
        self._publish(self._winner_sink, dict(snapshot.get("winner", {}) or {}))
        self._publish(self._export_sink, dict(snapshot.get("export", {}) or {}))
        self._publish(self._route_progress_sink, dict(snapshot.get("route_progress", {}) or {}))
        self._publish(self._reject_reason_sink, dict(snapshot.get("reject_reasons_chart", {}) or {}))
        self._publish(self._residual_sink, dict(snapshot.get("residuals", {}) or {}))
        self._publish(self._analyzer_health_sink, dict(snapshot.get("analyzer_health", {}) or {}))
        self._publish(self._error_sink, dict(snapshot.get("error", {}) or {}))
        self._publish(self._busy_sink, dict(snapshot.get("busy", {}) or {}))
        self._publish(self._notification_sink, dict(snapshot.get("notifications", {}) or {}))
        return snapshot

    def _tick(self) -> None:
        if not self._running or self._root is None:
            return
        self.poll_once()
        self._after_id = self._root.after(self.interval_ms, self._tick)

    @staticmethod
    def _publish(sink: Optional[Callable[[dict[str, Any]], None]], payload: dict[str, Any]) -> None:
        if sink is not None:
            sink(payload)
