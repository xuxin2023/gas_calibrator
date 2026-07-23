"""Shared persistence primitives for V1.5, V2, and evidence sidecars.

This package owns storage mechanics only. Calibration algorithms and route
selection remain in their version-specific domains.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


__all__ = [
    "DatabaseManager",
    "StorageSettings",
    "load_storage_config_file",
    "resolve_run_uuid",
    "stable_uuid",
]

_EXPORT_MAP = {
    "DatabaseManager": (".database", "DatabaseManager"),
    "StorageSettings": (".database", "StorageSettings"),
    "load_storage_config_file": (".database", "load_storage_config_file"),
    "resolve_run_uuid": (".database", "resolve_run_uuid"),
    "stable_uuid": (".database", "stable_uuid"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORT_MAP.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attr_name = target
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
