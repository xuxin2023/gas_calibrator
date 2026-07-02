"""Shared serial pacing guards for V1.5 analyzer coefficient tools."""

from __future__ import annotations

from argparse import Namespace
from typing import Iterable


MIN_ANALYZER_COMMAND_GAP_S = 1.0
FRAGILE_SERIAL_TIMING_FIELDS = (
    "readback_retry_delay_s",
    "restore_command_gap_s",
    "coefficient_read_delay_s",
    "post_write_settle_s",
)


def _flag_name(field: str) -> str:
    return "--" + field.replace("_", "-")


def require_fragile_serial_timing(
    args: Namespace,
    *,
    tool_name: str,
    fields: Iterable[str] = FRAGILE_SERIAL_TIMING_FIELDS,
    min_gap_s: float = MIN_ANALYZER_COMMAND_GAP_S,
) -> None:
    """Reject analyzer command/readback pacing below the V1.5 fragile-serial limit.

    GETCO/readback commands stress the same analyzer firmware path as SENCO writes,
    so coefficient tools must not silently accept sub-second retry/read delays.
    """

    failures: list[str] = []
    for field in fields:
        if not hasattr(args, field):
            continue
        raw = getattr(args, field)
        if raw is None:
            continue
        try:
            value = float(raw)
        except Exception:
            failures.append(f"{_flag_name(field)}={raw!r} is not numeric")
            continue
        if value < float(min_gap_s):
            failures.append(f"{_flag_name(field)}={value:g}s < {float(min_gap_s):g}s")
    if failures:
        joined = "; ".join(failures)
        raise ValueError(
            f"{tool_name} refuses analyzer serial pacing below {float(min_gap_s):g}s: {joined}"
        )
