from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_EXPORTS = {
    "ANALYTICS_FEATURE_SCHEMA_VERSION": (".feature_builder", "ANALYTICS_FEATURE_SCHEMA_VERSION"),
    "ANALYTICS_REPORT_SCHEMA_VERSION": (".service", "ANALYTICS_REPORT_SCHEMA_VERSION"),
    "AnalyticsService": (".service", "AnalyticsService"),
    "FeatureBuilder": (".feature_builder", "FeatureBuilder"),
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
