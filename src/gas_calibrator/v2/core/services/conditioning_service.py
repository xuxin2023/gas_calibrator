from __future__ import annotations

from typing import Any


class ConditioningService:

    def __init__(self, *, host: Any) -> None:
        self.host = host

    def _a2_cfg_bool(self, path: str, default: bool) -> bool:
        val = self.host._cfg_get(path, None)
        if val is None:
            return default
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.strip().lower() in ("true", "1", "yes", "on")
        if isinstance(val, (int, float)):
            return bool(val)
        return default

    def _a2_route_open_transient_window_enabled(self) -> bool:
        return self._a2_cfg_bool("workflow.pressure.route_open_transient_window_enabled", True)

    def _a2_route_open_transient_recovery_timeout_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get("workflow.pressure.route_open_transient_recovery_timeout_s", 10.0)
        )
        return max(0.1, float(10.0 if value is None else value))

    def _a2_route_open_transient_recovery_band_hpa(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get("workflow.pressure.route_open_transient_recovery_band_hpa", 10.0)
        )
        return max(0.1, float(10.0 if value is None else value))

    def _a2_route_open_transient_stable_hold_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get("workflow.pressure.route_open_transient_stable_hold_s", 2.0)
        )
        return max(0.0, float(2.0 if value is None else value))

    def _a2_route_open_transient_stable_span_hpa(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_open_transient_stable_pressure_span_hpa",
                self.host._cfg_get(
                    "workflow.pressure.route_open_transient_stable_span_hpa",
                    self._a2_route_open_transient_recovery_band_hpa(),
                ),
            )
        )
        return max(0.1, float(self._a2_route_open_transient_recovery_band_hpa() if value is None else value))

    def _a2_route_open_transient_stable_slope_hpa_per_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get("workflow.pressure.route_open_transient_stable_slope_hpa_per_s", 1.0)
        )
        return max(0.0, float(1.0 if value is None else value))

    def _a2_route_open_transient_sustained_rise_min_samples(self) -> int:
        value = self.host._as_float(
            self.host._cfg_get("workflow.pressure.route_open_transient_sustained_rise_min_samples", 3)
        )
        return max(2, int(3 if value is None else value))
