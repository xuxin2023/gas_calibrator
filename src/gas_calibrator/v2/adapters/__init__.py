"""
Adapter package entrypoints.

Exports are resolved lazily so importing ``gas_calibrator.v2.adapters`` does
not eagerly pull offline analytics, storage, or postprocess chains into
headless/runtime paths that do not use them.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_EXPORTS = {
    "LegacyCalibrationRunner": ("gas_calibrator.v2.adapters.legacy_runner", "LegacyCalibrationRunner"),
    "download_coefficients_to_analyzers": (
        "gas_calibrator.v2.adapters.analyzer_coefficient_downloader",
        "download_coefficients_to_analyzers",
    ),
    "run_from_cli": ("gas_calibrator.v2.adapters.offline_refit_runner", "run_from_cli"),
    "run_v1_postprocess": ("gas_calibrator.v2.adapters.v1_postprocess_runner", "run_from_cli"),
}

__all__ = list(_LAZY_EXPORTS)


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
