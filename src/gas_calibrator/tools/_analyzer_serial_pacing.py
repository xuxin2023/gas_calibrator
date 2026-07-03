"""Shared serial command pacing helpers for analyzer configuration writes."""

from __future__ import annotations

from contextlib import contextmanager
import time
from typing import Any, Callable


MIN_ANALYZER_SERIAL_COMMAND_GAP_S = 1.0


def _coerce_serial_command_gap(value: Any) -> float:
    try:
        gap = float(value)
    except Exception:
        gap = MIN_ANALYZER_SERIAL_COMMAND_GAP_S
    return max(MIN_ANALYZER_SERIAL_COMMAND_GAP_S, gap)


def _serial_for_command_pacing(analyzer: Any) -> Any:
    current = analyzer
    seen: set[int] = set()
    for _ in range(4):
        if current is None or id(current) in seen:
            return None
        seen.add(id(current))
        ser = getattr(current, "ser", None)
        if callable(getattr(ser, "write", None)):
            return ser
        current = getattr(current, "_analyzer", None)
    return None


@contextmanager
def _enforce_serial_command_gap(
    analyzer: Any,
    min_gap_s: Any,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
):
    """Ensure writes to an analyzer serial object are spaced by at least 1s."""

    events: list[dict[str, Any]] = []
    ser = _serial_for_command_pacing(analyzer)
    original_write = getattr(ser, "write", None)
    if ser is None or not callable(original_write):
        yield events
        return

    gap = _coerce_serial_command_gap(min_gap_s)
    if bool(getattr(ser, "_v1_5_serial_command_gap_active", False)):
        yield events
        return

    previous_flag = getattr(ser, "_v1_5_serial_command_gap_active", None)
    last_write_at: float | None = None

    def paced_write(data: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal last_write_at
        if last_write_at is not None:
            elapsed_s = max(0.0, time.monotonic() - last_write_at)
            wait_s = max(0.0, gap - elapsed_s)
            if wait_s > 0:
                sleep_fn(wait_s)
                events.append(
                    {
                        "min_gap_s": round(gap, 6),
                        "elapsed_before_wait_s": round(elapsed_s, 6),
                        "wait_s": round(wait_s, 6),
                    }
                )
        result = original_write(data, *args, **kwargs)
        last_write_at = time.monotonic()
        return result

    setattr(ser, "_v1_5_serial_command_gap_active", True)
    setattr(ser, "write", paced_write)
    try:
        yield events
    finally:
        setattr(ser, "write", original_write)
        if previous_flag is None:
            try:
                delattr(ser, "_v1_5_serial_command_gap_active")
            except Exception:
                pass
        else:
            setattr(ser, "_v1_5_serial_command_gap_active", previous_flag)
