"""Humidity and dewpoint diagnostics for V1.5 open-flow evidence."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence

from ._math import slope, values


def classify_humidity_behavior(
    rows: Sequence[Mapping[str, Any]],
    *,
    dewpoint_key: str = "dewpoint_c",
    dry_ppmv_key: str = "h2o_dry_ppmv",
    wet_ppmv_key: str = "h2o_wet_ppmv",
    co2_key: str = "co2_ppm",
    stable_slope_abs_max: float = 0.02,
    rising_slope_min: float = 0.05,
) -> Dict[str, Any]:
    dewpoint = values(rows, dewpoint_key)
    dry = values(rows, dry_ppmv_key)
    wet = values(rows, wet_ppmv_key)
    co2 = values(rows, co2_key)
    dewpoint_slope = slope(dewpoint)
    dry_slope = slope(dry)
    wet_slope = slope(wet)
    co2_slope = slope(co2)

    reasons = []
    classification = "humidity_stable_or_insufficient_evidence"
    if dewpoint_slope is not None and dry_slope is not None:
        if dewpoint_slope > rising_slope_min and abs(dry_slope) <= stable_slope_abs_max:
            classification = "pressure_effect_possible"
            reasons.append("raw_dewpoint_rising_but_dry_ppmv_stable")
        elif dry_slope > rising_slope_min:
            classification = "real_moisture_release"
            reasons.append("dry_ppmv_rising")
            if co2_slope is not None and co2_slope < -rising_slope_min:
                classification = "wet_dilution_or_contamination_suspect"
                reasons.append("dry_ppmv_rising_and_co2_falling")

    return {
        "status": "pass" if classification in {"humidity_stable_or_insufficient_evidence", "pressure_effect_possible"} else "fail",
        "classification": classification,
        "reasons": reasons,
        "dewpoint_slope": dewpoint_slope,
        "h2o_dry_ppmv_slope": dry_slope,
        "h2o_wet_ppmv_slope": wet_slope,
        "co2_slope": co2_slope,
    }
