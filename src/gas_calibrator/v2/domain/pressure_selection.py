from __future__ import annotations

import re
from typing import Any, Optional


AMBIENT_PRESSURE_TOKEN = "ambient"
AMBIENT_PRESSURE_LABEL = "当前大气压"
AMBIENT_OPEN_MODE = "ambient_open"
SEALED_CONTROLLED_MODE = "sealed_controlled"

_AMBIENT_ALIASES = {
    AMBIENT_PRESSURE_TOKEN,
    "ambientopen",
    "ambient_open",
    "ambient-open",
    "ambientpressure",
    "ambient_pressure",
    "当前大气压",
    "大气压",
}


def _compact_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip().lower())


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def is_ambient_pressure_selection_value(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return _compact_text(value) in _AMBIENT_ALIASES


def normalize_pressure_selection_token(value: Any) -> str:
    if is_ambient_pressure_selection_value(value):
        return AMBIENT_PRESSURE_TOKEN
    return str(value or "").strip()


def normalize_pressure_mode(value: Any) -> str:
    compact = _compact_text(value)
    if compact in {"", "none"}:
        return ""
    if compact in _AMBIENT_ALIASES:
        return AMBIENT_OPEN_MODE
    if compact in {"sealedcontrolled", "sealed_controlled", "sealed-controlled", "sealed", "controlled"}:
        return SEALED_CONTROLLED_MODE
    return str(value or "").strip().lower()


def effective_pressure_mode(
    *,
    pressure_hpa: Any,
    pressure_mode: Any = "",
    pressure_selection_token: Any = "",
) -> str:
    normalized_mode = normalize_pressure_mode(pressure_mode)
    if normalized_mode:
        return normalized_mode
    if is_ambient_pressure_selection_value(pressure_selection_token):
        return AMBIENT_OPEN_MODE
    if _safe_float(pressure_hpa) is not None:
        return SEALED_CONTROLLED_MODE
    return AMBIENT_OPEN_MODE


def pressure_target_label(
    *,
    pressure_hpa: Any,
    pressure_mode: Any = "",
    pressure_selection_token: Any = "",
    explicit_label: Any = None,
) -> Optional[str]:
    label = str(explicit_label or "").strip()
    if label:
        return label
    mode = effective_pressure_mode(
        pressure_hpa=pressure_hpa,
        pressure_mode=pressure_mode,
        pressure_selection_token=pressure_selection_token,
    )
    if mode == AMBIENT_OPEN_MODE:
        return AMBIENT_PRESSURE_LABEL
    numeric = _safe_float(pressure_hpa)
    if numeric is None:
        return None
    return f"{float(numeric):g}hPa"


def normalize_pressure_selection_value(value: Any) -> Optional[float | str]:
    if is_ambient_pressure_selection_value(value):
        return AMBIENT_PRESSURE_TOKEN
    numeric = _safe_float(value)
    if numeric is not None:
        return float(numeric)
    text = str(value or "").strip()
    if not text:
        return None
    return text


def normalize_selected_pressure_points(values: Any) -> list[float | str]:
    if values in (None, ""):
        return []
    raw_values = list(values) if isinstance(values, (list, tuple, set)) else [values]
    normalized: list[float | str] = []
    seen: set[str] = set()
    for item in raw_values:
        resolved = normalize_pressure_selection_value(item)
        if resolved is None:
            continue
        key = f"ambient:{resolved}" if isinstance(resolved, str) else f"pressure:{round(float(resolved), 6)}"
        if key in seen:
            continue
        seen.add(key)
        normalized.append(resolved)
    return normalized


def pressure_selection_key(
    *,
    pressure_hpa: Any,
    pressure_mode: Any = "",
    pressure_selection_token: Any = "",
) -> Optional[float | str]:
    mode = effective_pressure_mode(
        pressure_hpa=pressure_hpa,
        pressure_mode=pressure_mode,
        pressure_selection_token=pressure_selection_token,
    )
    if mode == AMBIENT_OPEN_MODE:
        return AMBIENT_PRESSURE_TOKEN
    numeric = _safe_float(pressure_hpa)
    if numeric is None:
        return None
    return round(float(numeric), 6)
