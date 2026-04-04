"""Pure helpers for formatting SENCO coefficient payloads."""

from __future__ import annotations

import math
from typing import Any, Sequence, Tuple


def format_senco_value(value: Any) -> str:
    """Format one SENCO coefficient with 6 significant digits.

    Output rules:
    - mantissa uses one integer digit plus five decimals
    - exponent always uses two digits
    - positive exponents omit the ``+`` sign
    - zero and negative zero normalize to ``0.00000e00``
    """

    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError("SENCO coefficient must be finite")
    if numeric == 0.0:
        return "0.00000e00"
    mantissa, exponent = format(numeric, ".5e").split("e", 1)
    exponent_sign = "-" if exponent.startswith("-") else ""
    exponent_digits = exponent.lstrip("+-").zfill(2)
    return f"{mantissa}e{exponent_sign}{exponent_digits}"


def format_senco_values(values: Sequence[Any]) -> Tuple[str, ...]:
    return tuple(format_senco_value(value) for value in values)


def format_senco_coeffs(coeffs: Sequence[Any]) -> Tuple[str, str, str, str]:
    """Format the 4-coefficient A/B/C/D payload used by SENCO7/8."""

    if len(coeffs) != 4:
        raise ValueError("coeffs must contain exactly 4 values")
    return tuple(format_senco_values(coeffs))  # type: ignore[return-value]

