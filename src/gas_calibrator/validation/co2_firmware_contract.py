"""CO2 firmware-output contract helpers for V1.5 offline review.

The analyzer's displayed CO2 ppm is not only the SENCO1/SENCO3 ratio model.
Recorded MODE2 frames show that the firmware applies a water-vapor dry-basis
correction after the raw CO2 chain:

    displayed_co2_ppm ~= raw_senco13_co2_ppm / (1 - H2O_mmol_mol / 1000)

These helpers keep that layer explicit so an offline model is compared with the
same physical quantity that the firmware reports.
"""

from __future__ import annotations

import math
from typing import Optional, Sequence


def _finite(value: float | int | str | None) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def co2_h2o_dry_correction_factor(h2o_mmol_mol: float | int | str | None) -> Optional[float]:
    """Return the firmware dry-basis correction factor for CO2 final ppm.

    ``h2o_mmol_mol`` is the analyzer H2O output in mmol/mol. The denominator is
    only valid while the water mole fraction is physically below 1.
    """

    h2o = _finite(h2o_mmol_mol)
    if h2o is None:
        return None
    denominator = 1.0 - h2o / 1000.0
    if denominator <= 0.0:
        return None
    return 1.0 / denominator


def co2_raw_to_firmware_final_ppm(
    raw_senco13_co2_ppm: float | int | str | None,
    h2o_mmol_mol: float | int | str | None,
) -> Optional[float]:
    """Predict displayed CO2 ppm from the raw SENCO1/SENCO3 CO2 result."""

    raw = _finite(raw_senco13_co2_ppm)
    factor = co2_h2o_dry_correction_factor(h2o_mmol_mol)
    if raw is None or factor is None:
        return None
    return raw * factor


def co2_senco1_ratio_polynomial_ppm(
    coefficients: Sequence[float | int | str],
    ratio: float | int | str | None,
) -> Optional[float]:
    """Evaluate the firmware SENCO1 ratio polynomial layer.

    The analyzer manual maps CO2 density/concentration ratio coefficients to
    SENCO1. In MODE2 evidence this layer is the ratio-only polynomial:

    ``C0 + C1*R + C2*R^2 + C3*R^3``.
    """

    r = _finite(ratio)
    if r is None:
        return None
    values = [_finite(item) for item in coefficients]
    if len(values) < 4 or any(value is None for value in values[:4]):
        return None
    c0, c1, c2, c3 = (float(value) for value in values[:4])
    return c0 + c1 * r + c2 * (r**2) + c3 * (r**3)


def co2_senco3_temperature_compensation_ppm(
    coefficients: Sequence[float | int | str],
    ratio: float | int | str | None,
    temperature_c: float | int | str | None,
    pressure_hpa: float | int | str | None = 0.0,
) -> Optional[float]:
    """Evaluate the observed SENCO3 contribution to raw CO2 ppm.

    The V1.5 firmware evidence shows the displayed raw CO2 layer contains the
    SENCO1 ratio polynomial plus a SENCO3 compensation layer using absolute
    Kelvin temperature:

    ``C0*T + C1*T^2 + C2*R*T + C3*P + C4*R*T*P``.

    ``C5`` is intentionally not used here because current manual/evidence
    review has not identified a live contribution for that slot.
    """

    r = _finite(ratio)
    temp = _finite(temperature_c)
    pressure = _finite(pressure_hpa)
    if r is None or temp is None:
        return None
    if pressure is None:
        pressure = 0.0
    values = [_finite(item) for item in coefficients]
    if len(values) < 5:
        values = values + [0.0] * (5 - len(values))
    if any(value is None for value in values[:5]):
        return None
    c0, c1, c2, c3, c4 = (float(value) for value in values[:5])
    temp_k = temp + 273.15
    return c0 * temp_k + c1 * (temp_k**2) + c2 * r * temp_k + c3 * pressure + c4 * r * temp_k * pressure


def co2_senco13_raw_ppm(
    senco1_coefficients: Sequence[float | int | str],
    senco3_coefficients: Sequence[float | int | str],
    ratio: float | int | str | None,
    temperature_c: float | int | str | None,
    pressure_hpa: float | int | str | None = 0.0,
) -> Optional[float]:
    """Evaluate the raw CO2 ppm layer before firmware H2O dry correction."""

    primary = co2_senco1_ratio_polynomial_ppm(senco1_coefficients, ratio)
    secondary = co2_senco3_temperature_compensation_ppm(
        senco3_coefficients,
        ratio,
        temperature_c,
        pressure_hpa,
    )
    if primary is None or secondary is None:
        return None
    return primary + secondary


def co2_firmware_final_to_raw_ppm(
    firmware_final_co2_ppm: float | int | str | None,
    h2o_mmol_mol: float | int | str | None,
) -> Optional[float]:
    """Remove the firmware H2O correction from displayed CO2 ppm."""

    final = _finite(firmware_final_co2_ppm)
    factor = co2_h2o_dry_correction_factor(h2o_mmol_mol)
    if final is None or factor is None:
        return None
    return final / factor


def co2_dry_route_h2o_status(
    h2o_mmol_mol: float | int | str | None,
    *,
    warning_mmol_mol: float = 10.0,
    block_mmol_mol: float = 50.0,
) -> str:
    """Classify analyzer H2O output for dry CO2 open-flow checks."""

    h2o = _finite(h2o_mmol_mol)
    if h2o is None:
        return "h2o_missing"
    if h2o >= block_mmol_mol:
        return "h2o_severe_bias_blocks_co2_acceptance"
    if h2o >= warning_mmol_mol:
        return "h2o_high_bias_explains_co2_final_shift"
    return "h2o_low_for_dry_co2_route"
