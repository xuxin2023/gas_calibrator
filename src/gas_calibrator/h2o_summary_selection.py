from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence


DEFAULT_H2O_TEMPERATURE_BUCKETS_C: tuple[float, ...] = (-20.0, -10.0, 0.0, 10.0, 20.0, 30.0, 40.0)
DEFAULT_H2O_TEMPERATURE_BUCKET_TOLERANCE_C: float = 6.0
DEFAULT_H2O_INCLUDE_CO2_TEMP_GROUPS_C: tuple[float, ...] = ()
DEFAULT_H2O_INCLUDE_CO2_ZERO_PPM_TEMP_GROUPS_C: tuple[float, ...] = (-20.0, -10.0, 0.0)
DEFAULT_H2O_INCLUDE_CO2_ZERO_PPM_ROWS: bool = True
DEFAULT_H2O_CO2_ZERO_PPM_TARGET: float = 0.0
DEFAULT_H2O_CO2_ZERO_PPM_TOLERANCE: float = 0.5
DEFAULT_H2O_TEMP_TOLERANCE_C: float = 0.6


def _float_list(raw: Any, default: Sequence[float]) -> list[float]:
    if not isinstance(raw, (list, tuple)):
        return [float(value) for value in default]

    values: list[float] = []
    for item in raw:
        try:
            values.append(float(item))
        except Exception:
            continue
    return values if values else [float(value) for value in default]


def default_h2o_summary_selection() -> Dict[str, Any]:
    return {
        "include_h2o_phase": True,
        "temperature_buckets_c": [float(value) for value in DEFAULT_H2O_TEMPERATURE_BUCKETS_C],
        "temperature_bucket_tolerance_c": float(DEFAULT_H2O_TEMPERATURE_BUCKET_TOLERANCE_C),
        "include_co2_temp_groups_c": [float(value) for value in DEFAULT_H2O_INCLUDE_CO2_TEMP_GROUPS_C],
        "include_co2_zero_ppm_rows": bool(DEFAULT_H2O_INCLUDE_CO2_ZERO_PPM_ROWS),
        "co2_zero_ppm_target": float(DEFAULT_H2O_CO2_ZERO_PPM_TARGET),
        "co2_zero_ppm_tolerance": float(DEFAULT_H2O_CO2_ZERO_PPM_TOLERANCE),
        "include_co2_zero_ppm_temp_groups_c": [
            float(value) for value in DEFAULT_H2O_INCLUDE_CO2_ZERO_PPM_TEMP_GROUPS_C
        ],
        "temp_tolerance_c": float(DEFAULT_H2O_TEMP_TOLERANCE_C),
    }


def normalize_h2o_summary_selection(selection: Mapping[str, Any] | None) -> Dict[str, Any]:
    payload = dict(selection or {})
    defaults = default_h2o_summary_selection()
    return {
        "include_h2o_phase": bool(payload.get("include_h2o_phase", defaults["include_h2o_phase"])),
        "temperature_buckets_c": _float_list(
            payload.get("temperature_buckets_c"),
            defaults["temperature_buckets_c"],
        ),
        "temperature_bucket_tolerance_c": float(
            payload.get("temperature_bucket_tolerance_c", defaults["temperature_bucket_tolerance_c"])
        ),
        "include_co2_temp_groups_c": _float_list(
            payload.get("include_co2_temp_groups_c"),
            defaults["include_co2_temp_groups_c"],
        ),
        "include_co2_zero_ppm_rows": bool(
            payload.get("include_co2_zero_ppm_rows", defaults["include_co2_zero_ppm_rows"])
        ),
        "co2_zero_ppm_target": float(payload.get("co2_zero_ppm_target", defaults["co2_zero_ppm_target"])),
        "co2_zero_ppm_tolerance": float(
            payload.get("co2_zero_ppm_tolerance", defaults["co2_zero_ppm_tolerance"])
        ),
        "include_co2_zero_ppm_temp_groups_c": _float_list(
            payload.get("include_co2_zero_ppm_temp_groups_c"),
            defaults["include_co2_zero_ppm_temp_groups_c"],
        ),
        "temp_tolerance_c": float(payload.get("temp_tolerance_c", defaults["temp_tolerance_c"])),
    }
