from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

AMBIENT_ONLY_MODEL_FEATURES = ["intercept", "R", "R2", "R3", "T", "T2", "RT"]


def _is_ambient_pressure_token(value: Any) -> bool:
    return isinstance(value, str) and value.strip().lower() == "ambient"


def _is_numeric_pressure_token(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    try:
        numeric = float(text)
    except Exception:
        return False
    return math.isfinite(numeric)


def resolve_ratio_poly_model_features(
    coeff_cfg: Mapping[str, Any] | None,
    selected_pressure_points: Sequence[Any] | None = None,
) -> tuple[list[str] | None, str]:
    explicit_features = (coeff_cfg or {}).get("model_features")
    if isinstance(explicit_features, list) and explicit_features:
        return list(explicit_features), "explicit_config"

    if selected_pressure_points is None:
        return None, "default_full_model"

    points = (
        [selected_pressure_points]
        if isinstance(selected_pressure_points, (str, bytes))
        else list(selected_pressure_points)
    )
    if not points:
        return None, "default_full_model"

    has_ambient = False
    has_numeric = False
    has_other = False
    for item in points:
        if _is_ambient_pressure_token(item):
            has_ambient = True
        elif _is_numeric_pressure_token(item):
            has_numeric = True
        else:
            has_other = True

    if has_ambient and not has_numeric and not has_other:
        return list(AMBIENT_ONLY_MODEL_FEATURES), "ambient_only_fallback"
    return None, "default_full_model"
