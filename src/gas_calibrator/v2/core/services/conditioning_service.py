from __future__ import annotations

from typing import Any, Optional


class ConditioningService:
    """Config accessors and lightweight helpers for A2 route conditioning."""

    def __init__(self, *, host: Any) -> None:
        self.host = host

    def _a2_conditioning_vent_heartbeat_interval_s(self) -> float:

    def _a2_conditioning_vent_max_gap_s(self) -> float:

    def _a2_conditioning_high_frequency_vent_max_gap_s(self) -> float:

    def _a2_conditioning_vent_maintenance_interval_s(self) -> float:

    def _a2_conditioning_vent_maintenance_max_gap_s(self) -> float:

    def _a2_conditioning_scheduler_sleep_step_s(self) -> float:

    def _a2_conditioning_defer_reschedule_latency_budget_ms(self) -> float:

    def _a2_conditioning_pressure_monitor_interval_s(self) -> float:

    def _a2_conditioning_diagnostic_budget_ms(self) -> float:

    def _a2_conditioning_pressure_monitor_budget_ms(self) -> float:

    def _a2_conditioning_continuous_latest_fresh_budget_ms(self) -> float:

    def _a2_conditioning_selected_pressure_sample_stale_budget_ms(self) -> float:

    def _a2_conditioning_monitor_pressure_max_defer_ms(self) -> float:

    def _a2_conditioning_trace_write_budget_ms(self) -> float:

    def _a2_conditioning_digital_gauge_max_age_s(self) -> float:

    def _a2_conditioning_pressure_abort_hpa(self) -> float:

    def _a2_route_conditioning_hard_abort_pressure_hpa(self) -> float:

    def _a2_route_open_transient_window_enabled(self) -> bool:

    def _a2_route_open_transient_recovery_timeout_s(self) -> float:

    def _a2_route_open_transient_recovery_band_hpa(self) -> float:

    def _a2_route_open_transient_stable_hold_s(self) -> float:

    def _a2_route_open_transient_stable_span_hpa(self) -> float:

    def _a2_route_open_transient_stable_slope_hpa_per_s(self) -> float:

    def _a2_route_open_transient_sustained_rise_min_samples(self) -> int:

    def _a2_conditioning_high_frequency_vent_window_s(self) -> float:

    def _a2_conditioning_high_frequency_vent_interval_s(self) -> float:

    def _a2_conditioning_fast_vent_max_duration_s(self) -> float:

    def _a2_route_open_transition_block_threshold_s(self) -> float:

    def _a2_route_open_settle_wait_s(self) -> float:

    def _a2_route_open_settle_wait_slice_s(self) -> float:

    def _a2_conditioning_pressure_rise_vent_trigger_hpa(self) -> float:

    def _a2_conditioning_pressure_rise_vent_min_interval_s(self) -> float:

    def _a2_prearm_route_conditioning_baseline_max_age_s(self) -> float:

    def _a2_prearm_baseline_freshness_max_s(self) -> float:

    def _a2_prearm_baseline_atmosphere_band_hpa(self) -> float:

    def _a2_conditioning_pressure_source_mode(self) -> str:

    def _a2_cfg_bool(self, path: str, default: bool) -> bool:

