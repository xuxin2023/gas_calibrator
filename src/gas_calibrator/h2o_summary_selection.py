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
DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_QUALITY_GATE_ENABLED: bool = True
DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_REQUIRE_H2O_DEW: bool = True
DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_MAX_PPM_H2O_DEW_DEFAULT: float = 0.5
DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_MAX_PPM_H2O_DEW_BY_TEMP_C: Mapping[str, float] = {
    "-20": 0.2,
    "-10": 0.05,
    "0": 0.5,
}


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


def _float_mapping(raw: Any, default: Mapping[str, float]) -> Dict[str, float]:
    source = raw if isinstance(raw, Mapping) else default
    values: Dict[str, float] = {}
    for key, value in dict(source).items():
        try:
            key_numeric = float(key)
            value_numeric = float(value)
        except Exception:
            continue
        values[f"{key_numeric:g}"] = value_numeric
    if values:
        return values
    return {str(key): float(value) for key, value in dict(default).items()}


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
        "co2_zero_ppm_anchor_quality_gate_enabled": bool(
            DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_QUALITY_GATE_ENABLED
        ),
        "co2_zero_ppm_anchor_require_h2o_dew": bool(
            DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_REQUIRE_H2O_DEW
        ),
        "co2_zero_ppm_anchor_max_ppm_h2o_dew_default": float(
            DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_MAX_PPM_H2O_DEW_DEFAULT
        ),
        "co2_zero_ppm_anchor_max_ppm_h2o_dew_by_temp_c": {
            str(key): float(value)
            for key, value in DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_MAX_PPM_H2O_DEW_BY_TEMP_C.items()
        },
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
        "co2_zero_ppm_anchor_quality_gate_enabled": bool(
            payload.get(
                "co2_zero_ppm_anchor_quality_gate_enabled",
                defaults["co2_zero_ppm_anchor_quality_gate_enabled"],
            )
        ),
        "co2_zero_ppm_anchor_require_h2o_dew": bool(
            payload.get(
                "co2_zero_ppm_anchor_require_h2o_dew",
                defaults["co2_zero_ppm_anchor_require_h2o_dew"],
            )
        ),
        "co2_zero_ppm_anchor_max_ppm_h2o_dew_default": float(
            payload.get(
                "co2_zero_ppm_anchor_max_ppm_h2o_dew_default",
                defaults["co2_zero_ppm_anchor_max_ppm_h2o_dew_default"],
            )
        ),
        "co2_zero_ppm_anchor_max_ppm_h2o_dew_by_temp_c": _float_mapping(
            payload.get("co2_zero_ppm_anchor_max_ppm_h2o_dew_by_temp_c"),
            defaults["co2_zero_ppm_anchor_max_ppm_h2o_dew_by_temp_c"],
        ),
    }
