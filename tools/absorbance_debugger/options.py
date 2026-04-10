"""Shared option normalization for CLI and GUI wrappers."""

from __future__ import annotations


RATIO_SOURCE_MAP = {
    "raw": "ratio_co2_raw",
    "ratio_co2_raw": "ratio_co2_raw",
    "filt": "ratio_co2_filt",
    "filtered": "ratio_co2_filt",
    "ratio_co2_filt": "ratio_co2_filt",
}

TEMP_SOURCE_MAP = {
    "std": "temp_std_c",
    "t_std": "temp_std_c",
    "temp_std_c": "temp_std_c",
    "corr": "temp_corr_c",
    "t_corr": "temp_corr_c",
    "temp_corr_c": "temp_corr_c",
}

PRESSURE_SOURCE_MAP = {
    "std": "pressure_std_hpa",
    "p_std": "pressure_std_hpa",
    "pressure_std_hpa": "pressure_std_hpa",
    "corr": "pressure_corr_hpa",
    "p_corr": "pressure_corr_hpa",
    "pressure_corr_hpa": "pressure_corr_hpa",
}

MODEL_SELECTION_STRATEGY_MAP = {
    "auto": "auto_grouped",
    "auto_grouped": "auto_grouped",
    "grouped_loo": "grouped_loo",
    "grouped_kfold": "grouped_kfold",
}

ABSORBANCE_ORDER_MODE_MAP = {
    "samplewise": "samplewise_log_first",
    "samplewise_log_first": "samplewise_log_first",
    "mean_first": "mean_first_log",
    "mean_first_log": "mean_first_log",
    "compare": "compare_both",
    "compare_both": "compare_both",
}

INVALID_PRESSURE_MODE_MAP = {
    "hard_exclude": "hard_exclude",
    "hard": "hard_exclude",
    "diagnostic_only": "diagnostic_only",
    "diagnostic": "diagnostic_only",
}


def _normalize(text: str, mapping: dict[str, str], label: str) -> str:
    key = str(text or "").strip().lower()
    if key in mapping:
        return mapping[key]
    valid = ", ".join(sorted(mapping))
    raise ValueError(f"Unsupported {label}: {text!r}. Valid values: {valid}")


def normalize_ratio_source(text: str) -> str:
    """Normalize CLI or GUI ratio source text."""

    return _normalize(text, RATIO_SOURCE_MAP, "ratio source")


def normalize_temp_source(text: str) -> str:
    """Normalize CLI or GUI temperature source text."""

    return _normalize(text, TEMP_SOURCE_MAP, "temperature source")


def normalize_pressure_source(text: str) -> str:
    """Normalize CLI or GUI pressure source text."""

    return _normalize(text, PRESSURE_SOURCE_MAP, "pressure source")


def normalize_model_selection_strategy(text: str) -> str:
    """Normalize CLI or GUI model-selection strategy text."""

    return _normalize(text, MODEL_SELECTION_STRATEGY_MAP, "model selection strategy")


def normalize_absorbance_order_mode(text: str) -> str:
    """Normalize CLI or GUI absorbance order mode text."""

    return _normalize(text, ABSORBANCE_ORDER_MODE_MAP, "absorbance order mode")


def normalize_invalid_pressure_mode(text: str) -> str:
    """Normalize invalid-pressure handling mode text."""

    return _normalize(text, INVALID_PRESSURE_MODE_MAP, "invalid pressure mode")


def parse_numeric_csv(text: str) -> tuple[float, ...]:
    """Parse a comma-separated numeric list."""

    values: list[float] = []
    for part in str(text or "").split(","):
        item = part.strip()
        if not item:
            continue
        values.append(float(item))
    return tuple(values)
