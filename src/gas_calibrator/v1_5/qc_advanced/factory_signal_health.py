"""Factory-mode signal health diagnostics."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence

from ._math import slope, values


def evaluate_factory_signal_health(
    rows: Sequence[Mapping[str, Any]],
    *,
    co2_key: str = "co2_ppm",
    h2o_key: str = "h2o_mmol",
    co2_ratio_key: str = "co2_ratio",
    h2o_ratio_key: str = "h2o_ratio",
    ref_signal_key: str = "ref_signal",
    concentration_slope_min: float = 0.05,
    ratio_stable_abs_max: float = 0.0005,
    ref_signal_slope_min: float = 0.5,
) -> Dict[str, Any]:
    co2_slope = slope(values(rows, co2_key))
    h2o_slope = slope(values(rows, h2o_key))
    co2_ratio_slope = slope(values(rows, co2_ratio_key))
    h2o_ratio_slope = slope(values(rows, h2o_ratio_key))
    ref_signal_slope = slope(values(rows, ref_signal_key))
    findings: List[str] = []

    if ref_signal_slope is not None and abs(ref_signal_slope) > ref_signal_slope_min:
        findings.append("optical_reference_signal_drift")
    if co2_slope is not None and abs(co2_slope) > concentration_slope_min:
        if co2_ratio_slope is not None and abs(co2_ratio_slope) <= ratio_stable_abs_max:
            findings.append("co2_pressure_or_temperature_compensation_suspect")
        else:
            findings.append("co2_ratio_or_gas_instability")
    if h2o_slope is not None and abs(h2o_slope) > concentration_slope_min:
        if h2o_ratio_slope is not None and abs(h2o_ratio_slope) <= ratio_stable_abs_max:
            findings.append("h2o_pressure_or_temperature_compensation_suspect")
        else:
            findings.append("h2o_ratio_or_humidity_instability")

    return {
        "status": "pass" if not findings else "review",
        "findings": findings,
        "co2_slope": co2_slope,
        "h2o_slope": h2o_slope,
        "co2_ratio_slope": co2_ratio_slope,
        "h2o_ratio_slope": h2o_ratio_slope,
        "ref_signal_slope": ref_signal_slope,
    }
