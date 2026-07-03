"""Small numeric helpers for V1.5 advanced QC."""

from __future__ import annotations

import math
from typing import Any, Iterable, List, Mapping, Optional, Sequence


def safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def values(rows: Sequence[Mapping[str, Any]], key: str) -> List[float]:
    out: List[float] = []
    for row in rows:
        number = safe_float(row.get(key))
        if number is not None:
            out.append(number)
    return out


def mean(items: Iterable[float]) -> Optional[float]:
    data = list(items)
    if not data:
        return None
    return sum(data) / len(data)


def stddev(items: Sequence[float]) -> Optional[float]:
    if len(items) < 2:
        return 0.0 if items else None
    avg = sum(items) / len(items)
    return math.sqrt(sum((item - avg) ** 2 for item in items) / (len(items) - 1))


def slope(items: Sequence[float]) -> Optional[float]:
    if len(items) < 2:
        return 0.0 if items else None
    return (items[-1] - items[0]) / (len(items) - 1)


def data_range(items: Sequence[float]) -> Optional[float]:
    if not items:
        return None
    return max(items) - min(items)
