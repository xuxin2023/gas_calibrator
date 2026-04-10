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
