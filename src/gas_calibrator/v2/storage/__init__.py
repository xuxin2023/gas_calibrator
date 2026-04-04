from __future__ import annotations

from importlib import import_module
from typing import Any


__all__ = [
    "ArtifactImporter",
    "CoefficientVersionStore",
    "DatabaseManager",
    "HistoryQueryService",
    "ProfileStore",
    "ProfileSummary",
    "StorageExporter",
    "StorageSettings",
    "load_storage_config_file",
    "resolve_run_uuid",
    "stable_uuid",
]

_EXPORT_MAP = {
    "ArtifactImporter": (".importer", "ArtifactImporter"),
    "CoefficientVersionStore": (".coefficient_store", "CoefficientVersionStore"),
    "DatabaseManager": (".database", "DatabaseManager"),
    "HistoryQueryService": (".queries", "HistoryQueryService"),
    "ProfileStore": (".profile_store", "ProfileStore"),
    "ProfileSummary": (".profile_store", "ProfileSummary"),
    "StorageExporter": (".exporter", "StorageExporter"),
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
