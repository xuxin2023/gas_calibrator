"""Shared humidity and dewpoint conversion helpers."""

from __future__ import annotations

import math
from typing import Any


def _require_float(value: Any, name: str) -> float:
    try:
        numeric = float(value)
    except Exception as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(numeric):
        raise ValueError(f"{name} must be finite")
    return numeric


def saturation_vapor_pressure_hpa(temp_c: Any) -> float:
    temp = _require_float(temp_c, "temp_c")
    if temp >= 0.0:
        return 6.1121 * math.exp((18.678 - temp / 234.5) * (temp / (257.14 + temp)))
    return 6.1115 * math.exp((23.036 - temp / 333.7) * (temp / (279.82 + temp)))


def dewpoint_to_h2o_mmol_per_mol(dewpoint_c: Any, pressure_hpa: Any) -> float:
    pressure = _require_float(pressure_hpa, "pressure_hpa")
    if pressure <= 0.0:
        raise ValueError("pressure_hpa must be positive")
    return round(1000.0 * saturation_vapor_pressure_hpa(dewpoint_c) / pressure, 6)


def rh_pct_from_dewpoint(temp_c: Any, dewpoint_c: Any) -> float:
    return round(
        100.0 * saturation_vapor_pressure_hpa(dewpoint_c) / saturation_vapor_pressure_hpa(temp_c),
        6,
    )


def derive_humidity_generator_setpoint(
    dewpoint_c: Any,
    *,
    min_temp_c: float = 20.0,
    headroom_c: float = 5.0,
    temp_step_c: float = 5.0,
    max_rh_pct: float = 95.0,
) -> dict[str, float]:
    dewpoint = _require_float(dewpoint_c, "dewpoint_c")
    step = _require_float(temp_step_c, "temp_step_c")
    if step <= 0.0:
        raise ValueError("temp_step_c must be positive")

    target_temp_c = max(
        _require_float(min_temp_c, "min_temp_c"),
        math.ceil((dewpoint + _require_float(headroom_c, "headroom_c")) / step) * step,
    )
    target_rh_pct = min(
        _require_float(max_rh_pct, "max_rh_pct"),
        rh_pct_from_dewpoint(target_temp_c, dewpoint),
    )
    return {
        "hgen_temp_c": round(target_temp_c, 3),
        "hgen_rh_pct": round(target_rh_pct, 3),
    }
