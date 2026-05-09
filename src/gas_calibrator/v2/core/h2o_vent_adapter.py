from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Callable, Any

from .vent_manager import VentManager


VentCommand = Callable[[bool], None]


@dataclass(frozen=True)
class H2OVentAdapterResult:
    route: str
    state: str
    allowed: bool
    blocked_reason: str
    reason: str
    source: str
    event: str
    requested_on: bool | None = None
    observed_on: bool | None = None
    hardware_command_sent: bool = False
    behavior_changed: bool = False
    gate_applied: bool = False
    fail_closed_applied: bool = False
    not_real_acceptance_evidence: bool = True
    keepalive_requested: bool = False
    keepalive_started: bool = False
    keepalive_stopped: bool = False
    thread_created: bool = False
    thread_joined: bool = False
    interval_s: float | None = None
    already_stopped: bool = False
    stop_result: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class H2OVentAdapter:
    def __init__(self, vent_command: VentCommand | None = None, manager: VentManager | None = None) -> None:
        self.vent_command = vent_command
        self.manager = manager or VentManager()
        self.events: list[dict[str, Any]] = []
        self.keepalive_active = False
        self.thread_created = False
        self.thread_joined = False
        self.stop_count = 0

    def request_vent(
        self,
        route: str,
        state: str,
        on: bool,
        reason: str = "",
        source: str = "H2OVentAdapter.request_vent",
    ) -> H2OVentAdapterResult:
        policy = self.manager.request_vent(route, state, on, reason=reason, source=source)
        hardware_command_sent = False
        if policy.allowed and self.vent_command is not None:
            self.vent_command(bool(on))
            hardware_command_sent = True
        return self._record(
            H2OVentAdapterResult(
                route=policy.route,
                state=policy.state,
                allowed=policy.allowed,
                blocked_reason=policy.blocked_reason,
                reason=policy.reason,
                source=policy.source,
                event="vent_on" if on else "vent_off",
                requested_on=bool(on),
                hardware_command_sent=hardware_command_sent,
            )
        )

    def start_keepalive(
        self,
        route: str,
        state: str,
        interval_s: float,
        reason: str = "",
        source: str = "H2OVentAdapter.start_keepalive",
    ) -> H2OVentAdapterResult:
        policy = self.manager.start_vent_keepalive(route, state, interval_s, reason=reason, source=source)
        if policy.allowed:
            self.keepalive_active = True
        return self._record(
            H2OVentAdapterResult(
                route=policy.route,
                state=policy.state,
                allowed=policy.allowed,
                blocked_reason=policy.blocked_reason,
                reason=policy.reason,
                source=policy.source,
                event="start_keepalive",
                requested_on=True,
                keepalive_requested=policy.keepalive_requested,
                keepalive_started=False,
                thread_created=False,
                interval_s=policy.interval_s,
            )
        )

    def stop_keepalive(
        self, route: str, state: str, reason: str = "", source: str = "H2OVentAdapter.stop_keepalive"
    ) -> H2OVentAdapterResult:
        policy = self.manager.stop_vent_keepalive(route, state, reason=reason, source=source)
        already_stopped = not self.keepalive_active
        if self.keepalive_active:
            self.keepalive_active = False
        self.stop_count += 1
        return self._record(
            H2OVentAdapterResult(
                route=policy.route,
                state=policy.state,
                allowed=policy.allowed,
                blocked_reason=policy.blocked_reason,
                reason=policy.reason,
                source=policy.source,
                event="stop_keepalive",
                keepalive_requested=policy.keepalive_requested,
                keepalive_stopped=policy.allowed,
                thread_created=False,
                thread_joined=False,
                already_stopped=already_stopped,
                stop_result="already_stopped" if already_stopped else "stopped",
            )
        )

    def cleanup(self, reason: str = "", source: str = "H2OVentAdapter.cleanup") -> H2OVentAdapterResult:
        already_stopped = not self.keepalive_active
        if self.keepalive_active:
            self.keepalive_active = False
        return self._record(
            H2OVentAdapterResult(
                route="h2o",
                state="CLEANUP",
                allowed=True,
                blocked_reason="",
                reason=str(reason or ""),
                source=str(source or ""),
                event="cleanup",
                keepalive_stopped=True,
                thread_created=False,
                thread_joined=False,
                already_stopped=already_stopped,
                stop_result="already_stopped" if already_stopped else "stopped",
            )
        )

    def _record(self, result: H2OVentAdapterResult) -> H2OVentAdapterResult:
        self.events.append(result.as_dict())
        return result
