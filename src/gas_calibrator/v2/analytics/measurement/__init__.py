from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_EXPORTS = {
    "MEASUREMENT_ANALYTICS_SCHEMA_VERSION": (".schemas", "MEASUREMENT_ANALYTICS_SCHEMA_VERSION"),
    "MEASUREMENT_FEATURE_SCHEMA_VERSION": (".feature_builder", "MEASUREMENT_FEATURE_SCHEMA_VERSION"),
    "MeasurementAnalyticsService": (".service", "MeasurementAnalyticsService"),
    "MeasurementFeatureBuilder": (".feature_builder", "MeasurementFeatureBuilder"),
    "export_csv": (".exporters", "export_csv"),
    "export_json": (".exporters", "export_json"),
}

__all__ = list(_LAZY_EXPORTS)


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    value = getattr(import_module(module_name, __name__), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
