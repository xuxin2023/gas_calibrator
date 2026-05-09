from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

from .route_state_shadow import ShadowState


class VentRoute(StrEnum):
    CO2 = "co2"
    H2O = "h2o"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class VentPolicyResult:
    route: str
    state: str
    allowed: bool
    blocked_reason: str
    severity: str
    policy_name: str
    reason: str
    source: str
    hardware_command_sent: bool = False
    behavior_changed: bool = False
    gate_applied: bool = False
    fail_closed_applied: bool = False
    not_real_acceptance_evidence: bool = True
    vent_on_requested: bool | None = None
    observed_on: bool | None = None
    keepalive_requested: bool = False
    keepalive_started: bool = False
    keepalive_stopped: bool = False
    thread_created: bool = False
    thread_joined: bool = False
    interval_s: float | None = None
    architecture_debt_observed: bool = False
    observation_only: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class VentManager:
    policy_name = "v2_route_state_vent_policy_model"
    future_common_default_interval_s = 2.0
    legacy_h2o_keepalive_interval_s = 1.0

    def assert_vent_allowed(
        self,
        route: str | VentRoute,
        state: str | ShadowState,
        on: bool,
        reason: str = "",
        source: str = "",
        ambient_block_enabled: bool = False,
    ) -> VentPolicyResult:
        normalized_route = _normalize_route(route)
        normalized_state = _normalize_state(state)
        allowed, blocked_reason, severity = self._policy_decision(
            normalized_route,
            normalized_state,
            bool(on),
            reason,
            ambient_block_enabled=ambient_block_enabled,
        )
        return VentPolicyResult(
            route=normalized_route,
            state=normalized_state,
            allowed=allowed,
            blocked_reason=blocked_reason,
            severity=severity,
            policy_name=self.policy_name,
            reason=str(reason or ""),
            source=str(source or ""),
            vent_on_requested=bool(on),
        )

    def request_vent(
        self,
        route: str | VentRoute,
        state: str | ShadowState,
        on: bool,
        reason: str = "",
        source: str = "",
        ambient_block_enabled: bool = False,
    ) -> VentPolicyResult:
        result = self.assert_vent_allowed(
            route,
            state,
            on,
            reason=reason,
            source=source,
            ambient_block_enabled=ambient_block_enabled,
        )
        return VentPolicyResult(
            **{
                **result.as_dict(),
                "hardware_command_sent": False,
                "behavior_changed": False,
                "gate_applied": False,
                "fail_closed_applied": False,
            }
        )

    def start_vent_keepalive(
        self,
        route: str | VentRoute,
        state: str | ShadowState,
        interval_s: float,
        reason: str = "",
        source: str = "",
    ) -> VentPolicyResult:
        normalized_route = _normalize_route(route)
        normalized_state = _normalize_state(state)
        interval = _coerce_interval(interval_s)
        if interval is None:
            return VentPolicyResult(
                route=normalized_route,
                state=normalized_state,
                allowed=False,
                blocked_reason="invalid_keepalive_interval",
                severity="blocked",
                policy_name=self.policy_name,
                reason=str(reason or ""),
                source=str(source or ""),
                vent_on_requested=True,
                keepalive_requested=True,
                interval_s=None,
            )
        result = self.assert_vent_allowed(normalized_route, normalized_state, True, reason=reason, source=source)
        keepalive_allowed = result.allowed and normalized_route == VentRoute.H2O.value and normalized_state in {
            ShadowState.OPEN_CONDITIONING.value,
            ShadowState.AMBIENT_OPEN_SAMPLING.value,
        }
        blocked_reason = result.blocked_reason
        severity = result.severity
        if result.allowed and not keepalive_allowed:
            blocked_reason = "keepalive_route_state_not_allowed"
            severity = "blocked"
        return VentPolicyResult(
            route=normalized_route,
            state=normalized_state,
            allowed=keepalive_allowed,
            blocked_reason=blocked_reason,
            severity=severity,
            policy_name=self.policy_name,
            reason=str(reason or ""),
            source=str(source or ""),
            vent_on_requested=True,
            keepalive_requested=True,
            keepalive_started=False,
            thread_created=False,
            interval_s=interval,
        )

    def stop_vent_keepalive(
        self,
        route: str | VentRoute,
        state: str | ShadowState,
        reason: str = "",
        source: str = "",
    ) -> VentPolicyResult:
        normalized_route = _normalize_route(route)
        normalized_state = _normalize_state(state)
        allowed = normalized_route == VentRoute.H2O.value and normalized_state in {
            ShadowState.OPEN_CONDITIONING.value,
            ShadowState.AMBIENT_OPEN_SAMPLING.value,
            ShadowState.SEAL_TRANSITION.value,
            ShadowState.CLEANUP.value,
            ShadowState.EMERGENCY_SAFE_STOP.value,
        }
        return VentPolicyResult(
            route=normalized_route,
            state=normalized_state,
            allowed=allowed,
            blocked_reason="" if allowed else "keepalive_stop_route_state_not_allowed",
            severity="ok" if allowed else "blocked",
            policy_name=self.policy_name,
            reason=str(reason or ""),
            source=str(source or ""),
            keepalive_requested=True,
            keepalive_stopped=False,
            thread_joined=False,
        )

    def record_vent_observation(
        self,
        route: str | VentRoute,
        state: str | ShadowState,
        observed_on: bool,
        reason: str = "",
        source: str = "",
        architecture_debt_observed: bool = False,
    ) -> VentPolicyResult:
        normalized_route = _normalize_route(route)
        normalized_state = _normalize_state(state)
        return VentPolicyResult(
            route=normalized_route,
            state=normalized_state,
            allowed=True,
            blocked_reason="",
            severity="info",
            policy_name="v2_route_state_vent_observation_model",
            reason=str(reason or ""),
            source=str(source or ""),
            observed_on=bool(observed_on),
            hardware_command_sent=False,
            behavior_changed=False,
            gate_applied=False,
            fail_closed_applied=False,
            not_real_acceptance_evidence=True,
            architecture_debt_observed=bool(architecture_debt_observed),
            observation_only=True,
        )

    def _policy_decision(
        self,
        route: str,
        state: str,
        on: bool,
        reason: str,
        *,
        ambient_block_enabled: bool = False,
    ) -> tuple[bool, str, str]:
        if route not in {VentRoute.CO2.value, VentRoute.H2O.value}:
            return (False, "unknown_route", "blocked" if on else "warning")
        if state == ShadowState.UNKNOWN.value:
            return (False, "unknown_state", "blocked" if on else "warning")
        if state == ShadowState.SEALED_PRESSURE_CONTROL.value and on:
            return (False, "sealed_pressure_control_vent_on_blocked", "blocked")
        if route == VentRoute.H2O.value:
            if state in {ShadowState.OPEN_CONDITIONING.value, ShadowState.AMBIENT_OPEN_SAMPLING.value} and on:
                return (True, "", "ok")
            if state == ShadowState.SEAL_TRANSITION.value and not on:
                return (True, "", "ok")
        if route == VentRoute.CO2.value:
            if state == ShadowState.OPEN_CONDITIONING.value and on:
                return (True, "", "ok")
            if state == ShadowState.AMBIENT_OPEN_SAMPLING.value and on:
                if ambient_block_enabled:
                    return (True, "", "ok")
                return (False, "co2_ambient_block_not_explicitly_enabled", "blocked")
        if state in {ShadowState.CLEANUP.value, ShadowState.EMERGENCY_SAFE_STOP.value}:
            if reason:
                return (True, "", "warning")
            return (False, "cleanup_or_emergency_requires_reason", "warning")
        if on:
            return (False, "route_state_vent_on_not_allowed", "blocked")
        return (False, "route_state_vent_off_not_allowed", "warning")


def assert_vent_allowed(
    route: str | VentRoute,
    state: str | ShadowState,
    on: bool,
    reason: str = "",
    source: str = "",
    ambient_block_enabled: bool = False,
) -> VentPolicyResult:
    return VentManager().assert_vent_allowed(
        route,
        state,
        on,
        reason=reason,
        source=source,
        ambient_block_enabled=ambient_block_enabled,
    )


def request_vent(
    route: str | VentRoute,
    state: str | ShadowState,
    on: bool,
    reason: str = "",
    source: str = "",
    ambient_block_enabled: bool = False,
) -> VentPolicyResult:
    return VentManager().request_vent(
        route,
        state,
        on,
        reason=reason,
        source=source,
        ambient_block_enabled=ambient_block_enabled,
    )


def start_vent_keepalive(
    route: str | VentRoute,
    state: str | ShadowState,
    interval_s: float,
    reason: str = "",
    source: str = "",
) -> VentPolicyResult:
    return VentManager().start_vent_keepalive(route, state, interval_s, reason=reason, source=source)


def stop_vent_keepalive(
    route: str | VentRoute,
    state: str | ShadowState,
    reason: str = "",
    source: str = "",
) -> VentPolicyResult:
    return VentManager().stop_vent_keepalive(route, state, reason=reason, source=source)


def record_vent_observation(
    route: str | VentRoute,
    state: str | ShadowState,
    observed_on: bool,
    reason: str = "",
    source: str = "",
    architecture_debt_observed: bool = False,
) -> VentPolicyResult:
    return VentManager().record_vent_observation(
        route,
        state,
        observed_on,
        reason=reason,
        source=source,
        architecture_debt_observed=architecture_debt_observed,
    )


def _normalize_route(route: str | VentRoute) -> str:
    value = str(route.value if isinstance(route, VentRoute) else route or "").strip().lower()
    if value in {VentRoute.CO2.value, VentRoute.H2O.value}:
        return value
    return VentRoute.UNKNOWN.value


def _normalize_state(state: str | ShadowState) -> str:
    value = str(state.value if isinstance(state, ShadowState) else state or "").strip().upper()
    if value in {item.value for item in ShadowState}:
        return value
    return ShadowState.UNKNOWN.value


def _coerce_interval(interval_s: float) -> float | None:
    try:
        interval = float(interval_s)
    except (TypeError, ValueError):
        return None
    if interval <= 0:
        return None
    return interval
