from __future__ import annotations

from enum import StrEnum
from typing import Any, Mapping


class ShadowState(StrEnum):
    BASELINE = "BASELINE"
    OPEN_CONDITIONING = "OPEN_CONDITIONING"
    AMBIENT_OPEN_SAMPLING = "AMBIENT_OPEN_SAMPLING"
    SEAL_TRANSITION = "SEAL_TRANSITION"
    SEALED_PRESSURE_CONTROL = "SEALED_PRESSURE_CONTROL"
    CLEANUP = "CLEANUP"
    EMERGENCY_SAFE_STOP = "EMERGENCY_SAFE_STOP"
    UNKNOWN = "UNKNOWN"


CONTRACT_SHADOW_STATES = tuple(state.value for state in ShadowState)


def infer_shadow_state(source: Mapping[str, Any] | None = None, **kwargs: Any) -> str:
    payload = _merged_payload(source, kwargs)
    action = _text(payload.get("source_action") or payload.get("action"))
    trace_action = _text(payload.get("source_trace_action") or payload.get("trace_action"))
    source_function = _text(payload.get("source_function"))
    route = _text(payload.get("route"))
    combined = " ".join(part for part in (action, trace_action, source_function) if part)

    if _is_ambient_source(payload, combined):
        return ShadowState.AMBIENT_OPEN_SAMPLING.value
    if any(token in combined for token in ("emergency", "safe_stop", "final_safe_stop", "workflow_validation_error")):
        return ShadowState.EMERGENCY_SAFE_STOP.value
    if any(token in combined for token in ("cleanup", "route_complete")):
        return ShadowState.CLEANUP.value
    if any(
        token in combined
        for token in (
            "seal_transition",
            "pressurize_and_hold",
            "preseal",
            "stop_keepalive",
            "vent_false",
            "bare_controller_vent_false",
            "settle",
            "gauge_read",
            "close_h2o_path",
            "set_h2o_path_false",
            "route_close",
            "route_seal",
        )
    ):
        return ShadowState.SEAL_TRANSITION.value
    if any(
        token in combined
        for token in (
            "set_pressure_to_target",
            "set_setpoint",
            "wait_after_pressure_stable",
            "wait_post_pressure",
            "pressure_stable",
            "pressure_point",
            "dry_air_correction",
            "dry_air_corrected",
        )
    ):
        return ShadowState.SEALED_PRESSURE_CONTROL.value
    if "sample" in combined and not _is_ambient_source(payload, combined):
        return ShadowState.SEALED_PRESSURE_CONTROL.value
    if any(
        token in combined
        for token in (
            "open_conditioning",
            "route_conditioning",
            "co2_route_open",
            "set_valves_for_co2",
            "open_co2_route",
            "wait_route_soak",
            "open_h2o_route_and_wait_ready",
            "wait_route_ready",
            "start_h2o_vent_keepalive",
            "wait_dewpoint",
        )
    ):
        return ShadowState.OPEN_CONDITIONING.value
    if any(
        token in combined
        for token in (
            "baseline",
            "route_context_enter",
            "apply_route_baseline",
            "set_co2_route_baseline",
            "prepare_humidity_generator",
            "wait_humidity",
            "wait_temperature",
            "prepare_pressure_for_h2o",
        )
    ):
        return ShadowState.BASELINE.value
    if route in {"co2", "h2o"} and payload.get("route_context_enter") is True:
        return ShadowState.BASELINE.value
    return ShadowState.UNKNOWN.value


def build_shadow_event(source: Mapping[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
    payload = _merged_payload(source, kwargs)
    inferred_state = str(payload.get("shadow_state") or infer_shadow_state(payload))
    shadow_state = inferred_state if inferred_state in CONTRACT_SHADOW_STATES else ShadowState.UNKNOWN.value
    target_pressure = _pressure_value(payload, "target")
    actual_pressure = _pressure_value(payload, "actual")
    vent_state = _vent_state(payload)
    route_valve_state = _route_valve_state(payload)
    action = str(payload.get("source_action") or payload.get("action") or "")
    trace_action = str(payload.get("source_trace_action") or payload.get("trace_action") or "")
    set_pressure_seen = bool(
        payload.get(
            "set_pressure_seen",
            _contains_any(action, trace_action, tokens=("set_pressure", "set_setpoint")),
        )
    )
    sample_seen = bool(payload.get("sample_seen", _contains_any(action, trace_action, tokens=("sample",))))
    unknown_fields: list[str] = list(payload.get("unknown_fields") or [])
    missing_source: list[str] = list(payload.get("missing_source") or [])

    if vent_state == "unknown":
        _append_once(unknown_fields, "vent_state_observed")
        _append_once(missing_source, "vent_evidence")
    if route_valve_state == "unknown":
        _append_once(unknown_fields, "route_valve_state_observed")
        _append_once(missing_source, "route_valve_evidence")
    if target_pressure is None and _needs_target_pressure(shadow_state, action, trace_action):
        _append_once(unknown_fields, "target_pressure_hpa")
        _append_once(missing_source, "target_pressure_evidence")
    if actual_pressure is None and _needs_actual_pressure(shadow_state, action, trace_action):
        _append_once(unknown_fields, "actual_pressure_hpa")
        _append_once(missing_source, "actual_pressure_evidence")
    if sample_seen and not any(key in payload for key in ("sample_count", "sample_schema_version", "sample_schema")):
        _append_once(unknown_fields, "sample_evidence")
        _append_once(missing_source, "sample_schema_evidence")

    event = {
        "route": str(payload.get("route") or "unknown").strip().lower() or "unknown",
        "shadow_state": shadow_state,
        "source_action": action,
        "source_function": str(payload.get("source_function") or ""),
        "source_trace_action": trace_action,
        "point_index": _optional_int(payload.get("point_index") or _point_attr(payload.get("point"), "index")),
        "point_tag": str(payload.get("point_tag") or ""),
        "target_pressure_hpa": target_pressure,
        "actual_pressure_hpa": actual_pressure,
        "vent_state_observed": vent_state,
        "route_valve_state_observed": route_valve_state,
        "set_pressure_seen": set_pressure_seen,
        "sample_seen": sample_seen,
        "inferred": bool(payload.get("inferred", True)),
        "inference_confidence": str(payload.get("inference_confidence") or _confidence(shadow_state, unknown_fields)),
        "unknown_fields": unknown_fields,
        "missing_source": missing_source,
        "observation_only": True,
        "behavior_changed": False,
        "gate_applied": False,
        "fail_closed_applied": False,
        "not_real_acceptance_evidence": True,
    }

    for key in (
        "ambient_block_enabled",
        "is_ambient_pressure_point",
        "direct_vent_call_observed",
        "architecture_debt_observed",
        "dry_air_corrected_h2o_ppm",
        "settle_s",
        "pressure_after_settle_hpa",
        "keepalive_state",
        "sample_count",
        "sample_schema_version",
        "no_write_guard_observed",
    ):
        if key in payload:
            event[key] = payload[key]
    return event


def build_shadow_trace(sources: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]) -> list[dict[str, Any]]:
    return [build_shadow_event(source) for source in sources]


def _merged_payload(source: Mapping[str, Any] | None, kwargs: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if isinstance(source, Mapping):
        payload.update(dict(source))
    payload.update(dict(kwargs))
    return payload


def _text(value: Any) -> str:
    return str(value or "").strip().lower()


def _contains_any(*values: str, tokens: tuple[str, ...]) -> bool:
    combined = " ".join(str(value or "").lower() for value in values)
    return any(token in combined for token in tokens)


def _is_ambient_source(payload: Mapping[str, Any], combined: str) -> bool:
    if bool(payload.get("is_ambient_pressure_point")):
        return True
    if _text(payload.get("pressure_selection_token")) == "ambient_open":
        return True
    target = payload.get("target")
    if isinstance(target, Mapping) and target.get("pressure_hpa") is None and target.get("vent_on") is True:
        return True
    if "ambient_open" in combined:
        return True
    return "pressure_skip" in combined and "vent_on" in combined


def _pressure_value(payload: Mapping[str, Any], kind: str) -> float | None:
    direct_key = f"{kind}_pressure_hpa"
    if direct_key in payload:
        return _optional_float(payload.get(direct_key))
    nested = payload.get(kind)
    if isinstance(nested, Mapping):
        for key in ("pressure_hpa", "target_pressure_hpa", "actual_pressure_hpa", "pressure_after_settle_hpa"):
            if key in nested:
                return _optional_float(nested.get(key))
    if kind == "target" and "pressure_hpa" in payload and not _is_ambient_source(payload, ""):
        return _optional_float(payload.get("pressure_hpa"))
    if kind == "actual" and "pressure_after_settle_hpa" in payload:
        return _optional_float(payload.get("pressure_after_settle_hpa"))
    return None


def _vent_state(payload: Mapping[str, Any]) -> str:
    for key in ("vent_state_observed", "vent_state"):
        if key in payload:
            return _normalize_on_off(payload.get(key))
    for container_key in ("actual", "target"):
        container = payload.get(container_key)
        if isinstance(container, Mapping) and "vent_on" in container:
            if container.get("vent_on") is True:
                return "ON"
            if container.get("vent_on") is False:
                return "OFF"
            return "unknown"
    if "vent_on" in payload:
        if payload.get("vent_on") is True:
            return "ON"
        if payload.get("vent_on") is False:
            return "OFF"
        return "unknown"
    return "unknown"


def _route_valve_state(payload: Mapping[str, Any]) -> str:
    for key in ("route_valve_state_observed", "route_valve_state", "route_state_observed"):
        if key in payload:
            value = _text(payload.get(key))
            return value or "unknown"
    for key in ("route_open", "route_sealed"):
        if key in payload:
            if key == "route_open":
                if payload.get(key) is True:
                    return "open"
                if payload.get(key) is False:
                    return "closed"
                return "unknown"
            if payload.get(key) is True:
                return "sealed"
            return "unknown"
    return "unknown"


def _normalize_on_off(value: Any) -> str:
    if isinstance(value, bool):
        return "ON" if value else "OFF"
    text = _text(value)
    if text in {"on", "true", "1", "open"}:
        return "ON"
    if text in {"off", "false", "0", "closed", "sealed"}:
        return "OFF"
    return "unknown"


def _needs_target_pressure(shadow_state: str, action: str, trace_action: str) -> bool:
    if shadow_state != ShadowState.SEALED_PRESSURE_CONTROL.value:
        return False
    return _contains_any(action, trace_action, tokens=("set_pressure", "set_setpoint", "pressure_point", "wait_post_pressure"))


def _needs_actual_pressure(shadow_state: str, action: str, trace_action: str) -> bool:
    if shadow_state not in {ShadowState.SEALED_PRESSURE_CONTROL.value, ShadowState.SEAL_TRANSITION.value}:
        return False
    return _contains_any(action, trace_action, tokens=("wait_after_pressure", "wait_post_pressure", "gauge_read", "settle"))


def _confidence(shadow_state: str, unknown_fields: list[str]) -> str:
    if shadow_state == ShadowState.UNKNOWN.value:
        return "unknown"
    if not unknown_fields:
        return "high"
    if len(unknown_fields) <= 2:
        return "medium"
    return "low"


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _point_attr(point: Any, name: str) -> Any:
    if point is None:
        return None
    return getattr(point, name, None)


def _append_once(items: list[str], value: str) -> None:
    if value not in items:
        items.append(value)
