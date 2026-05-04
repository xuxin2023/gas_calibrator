"""Pure no-500 hPa summary filtering helper shared by boundary-safe tools.

This module intentionally stays free of any V2 imports so V1 runtime
entrypoints can reuse the filtering logic without dragging in offline
V2 postprocess code.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional

import pandas as pd


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _first_present(row: pd.Series, keys: Iterable[str]) -> Any:
    for key in keys:
        if key in row.index:
            value = row.get(key)
            if value not in (None, ""):
                return value
    return None


def _is_500hpa_row(row: pd.Series) -> bool:
    pressure_label = str(
        _first_present(row, ("PressureTargetLabel", "鍘嬪姏鐩爣鏍囩", "pressure_target_label")) or ""
    ).strip().lower()
    if "500" in pressure_label:
        return True

    pressure_target = _safe_float(
        _first_present(row, ("PressureTarget", "鐩爣鍘嬪姏hPa", "pressure_target_hpa"))
    )
    if pressure_target is not None and abs(pressure_target - 500.0) <= 0.5:
        return True

    pressure_mode = str(
        _first_present(row, ("PressureMode", "鍘嬪姏鎵ц妯″紡", "pressure_mode")) or ""
    ).strip().lower()
    if pressure_mode == "sealed_controlled" and pressure_target is not None and abs(pressure_target - 500.0) <= 5.0:
        return True

    return False


def filter_no_500_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    if frame.empty:
        return frame.copy(), {"original_rows": 0, "removed_rows": 0, "kept_rows": 0}
    mask = frame.apply(_is_500hpa_row, axis=1)
    filtered = frame.loc[~mask].copy()
    return filtered, {
        "original_rows": int(len(frame)),
        "removed_rows": int(mask.sum()),
        "kept_rows": int(len(filtered)),
    }
