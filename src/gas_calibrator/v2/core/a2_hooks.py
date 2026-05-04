from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class A2Hooks:

    high_pressure_first_point_mode_enabled: bool = False

    co2_route_conditioning_at_atmosphere_active: bool = False

    co2_route_conditioning_at_atmosphere_context: dict[str, Any] = field(default_factory=dict)

    co2_route_conditioning_completed: bool = False
    co2_route_conditioning_completed_at: str = ""

    co2_route_open_monotonic_s: Optional[float] = None
    co2_route_open_pressure_hpa: Optional[float] = None

    high_pressure_first_point_context: dict[str, Any] = field(default_factory=dict)
    high_pressure_first_point_vent_preclosed: bool = False
    high_pressure_first_point_initial_decision: str = ""

    preseal_analyzer_gate_passed: bool = False
    preseal_vent_close_arm_context: Optional[dict[str, Any]] = None
    preseal_last_pressure_hpa: Optional[float] = None
    preseal_pressure_rise_detected: bool = False

    pressure_points_started: bool = False
    pressure_control_active: bool = False

    seal_allowed: bool = False
    seal_trigger_reason: str = ""

    route_open_pressure_first_sample_recorded: bool = False

    callbacks: dict[str, Optional[Callable[..., Any]]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.co2_route_conditioning_at_atmosphere_context is None:
            self.co2_route_conditioning_at_atmosphere_context = {}
        if self.high_pressure_first_point_context is None:
            self.high_pressure_first_point_context = {}
        if self.callbacks is None:
            self.callbacks = {}

    def marker_route_open_started(self) -> Optional[Callable[..., Any]]:
        return self.callbacks.get("mark_route_open_started")

    def marker_route_open_completed(self) -> Optional[Callable[..., Any]]:
        return self.callbacks.get("mark_route_open_completed")

    def refresh_after_route_open(self) -> Optional[Callable[..., Any]]:
        return self.callbacks.get("refresh_after_route_open")

    def fail_route_open_transition(self) -> Optional[Callable[..., Any]]:
        return self.callbacks.get("fail_route_open_transition")

    def wait_route_open_settle(self) -> Optional[Callable[..., Any]]:
        return self.callbacks.get("wait_route_open_settle")

    def complete_route_open_transition(self) -> Optional[Callable[..., Any]]:
        return self.callbacks.get("complete_route_open_transition")

    def record_a2_conditioning_workflow_timing(self) -> Optional[Callable[..., Any]]:
        return self.callbacks.get("record_a2_conditioning_workflow_timing")
