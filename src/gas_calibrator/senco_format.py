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


def rounded_senco_values(values: Sequence[Any]) -> Tuple[float, ...]:
    """Round values the same way SENCO payload formatting does, then parse back to float."""

    return tuple(float(text) for text in format_senco_values(values))


def senco_readback_matches(expected: Sequence[Any], actual: Sequence[Any], *, atol: float = 1e-9) -> bool:
    """Compare device readback against SENCO-rounded expected values."""

    if len(expected) != len(actual):
        return False
    try:
        rounded_expected = rounded_senco_values(expected)
        actual_values = tuple(float(value) for value in actual)
    except Exception:
        return False
    return all(
        math.isfinite(exp) and math.isfinite(got) and abs(got - exp) <= float(atol)
        for exp, got in zip(rounded_expected, actual_values)
    )


def format_senco_coeffs(coeffs: Sequence[Any]) -> Tuple[str, str, str, str]:
    """Format the 4-coefficient A/B/C/D payload used by SENCO7/8."""

    if len(coeffs) != 4:
        raise ValueError("coeffs must contain exactly 4 values")
    return tuple(format_senco_values(coeffs))  # type: ignore[return-value]
