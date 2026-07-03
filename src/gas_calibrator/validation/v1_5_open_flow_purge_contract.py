"""V1.5 open-flow purge-time contract.

This module is pure/offline. It only resolves minimum purge durations from a
queue row and never opens COM ports, controls routes, or writes coefficients.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional


CO2_NORMAL_PURGE_S = 360.0
CO2_CONSERVATIVE_PURGE_S = 600.0
H2O_NORMAL_PURGE_S = 720.0
H2O_CONSERVATIVE_PURGE_S = 900.0


_TRUE_TEXT = {"1", "true", "yes", "y", "on"}

_CONSERVATIVE_COMMON = {
    "unknown",
    "unknown_route",
    "first",
    "first_point",
    "startup",
    "power_cycle",
    "after_restart",
    "long_idle",
    "route_recovery",
    "recovery",
    "abnormal_recovery",
}

_CO2_CONSERVATIVE = _CONSERVATIVE_COMMON | {
    "after_wet",
    "after_wet_route",
    "after_water",
    "after_water_route",
    "after_h2o",
    "after_h2o_route",
    "wet_to_dry",
    "post_wet",
}

_H2O_CONSERVATIVE = _CONSERVATIVE_COMMON | {
    "after_high_humidity",
    "high_to_low",
    "dry_anchor",
    "low_water_anchor",
    "dry_gas_anchor",
}


@dataclass(frozen=True)
class PurgeResolution:
    component: str
    purge_s: float
    minimum_purge_s: float
    profile: str
    explicit_override: bool
    reasons: tuple[str, ...] = field(default_factory=tuple)


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if numeric != numeric:
        return None
    return numeric


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in _TRUE_TEXT


def _token(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace("-", "_")


def _row_tokens(row: Mapping[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for key in (
        "purge_profile",
        "route_profile",
        "route_state",
        "initial_state",
        "route_initial_state",
        "line_state",
        "pre_purge_state",
        "previous_route_state",
        "sample_role",
    ):
        token = _token(row.get(key))
        if token:
            tokens.add(token)
    flag_map = {
        "first_point": ("first_point", "is_first_point"),
        "route_recovery": ("route_recovery", "after_route_recovery", "route_abnormal_recovery"),
        "after_wet_route": ("after_wet_route", "after_water_route", "after_h2o_route"),
        "after_high_humidity": ("after_high_humidity", "high_to_low_humidity"),
        "low_water_anchor": ("low_water_anchor", "dry_anchor", "dry_gas_anchor"),
    }
    for token, keys in flag_map.items():
        if any(_truthy(row.get(key)) for key in keys):
            tokens.add(token)
    return tokens


def resolve_v1_5_open_flow_purge(
    *,
    component: str,
    row: Mapping[str, Any],
    explicit_purge_s: Any = None,
) -> PurgeResolution:
    """Resolve the V1.5 minimum purge duration for a queue point.

    Explicit CLI/row purge values are preserved for backward compatibility. If
    no explicit value is supplied, the function upgrades the minimum only when
    the queue row declares a physically higher-risk route state.
    """

    component_key = _token(component)
    if component_key not in {"co2", "h2o"}:
        raise ValueError("component must be 'co2' or 'h2o'")

    row_purge = _safe_float(row.get("purge_s"))
    cli_purge = _safe_float(explicit_purge_s)
    explicit = cli_purge if cli_purge is not None else row_purge
    if explicit is not None:
        normal = CO2_NORMAL_PURGE_S if component_key == "co2" else H2O_NORMAL_PURGE_S
        return PurgeResolution(
            component=component_key,
            purge_s=float(explicit),
            minimum_purge_s=normal,
            profile="explicit_override",
            explicit_override=True,
            reasons=("explicit_purge_s_preserved",),
        )

    tokens = _row_tokens(row)
    if component_key == "co2":
        conservative_hits = sorted(tokens & _CO2_CONSERVATIVE)
        minimum = CO2_CONSERVATIVE_PURGE_S if conservative_hits else CO2_NORMAL_PURGE_S
    else:
        conservative_hits = sorted(tokens & _H2O_CONSERVATIVE)
        minimum = H2O_CONSERVATIVE_PURGE_S if conservative_hits else H2O_NORMAL_PURGE_S

    profile = "conservative" if conservative_hits else "normal"
    return PurgeResolution(
        component=component_key,
        purge_s=minimum,
        minimum_purge_s=minimum,
        profile=profile,
        explicit_override=False,
        reasons=tuple(conservative_hits or ("normal_known_route",)),
    )


def nitrogen_prepurge_formal_role() -> Mapping[str, Any]:
    """Describe the formal role of optional nitrogen pre-purge evidence."""

    return {
        "role": "diagnostic_or_conditioning_prepurge",
        "may_reduce_residual_co2": True,
        "may_help_dry_route": True,
        "is_formal_co2_zero_anchor": False,
        "is_formal_h2o_dry_anchor": False,
        "requires_own_reference_evidence_for_anchor_use": True,
    }
