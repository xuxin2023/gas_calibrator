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
    "SidecarIndexStore",
    "StorageExporter",
    "StorageSettings",
    "index_run",
    "load_storage_config_file",
    "normalize_sidecar_record",
    "resolve_run_uuid",
    "SIDECAR_COLLECTIONS",
    "stable_uuid",
]

_EXPORT_MAP = {
    "ArtifactImporter": (".importer", "ArtifactImporter"),
    "CoefficientVersionStore": (".coefficient_store", "CoefficientVersionStore"),
    "DatabaseManager": (".database", "DatabaseManager"),
    "HistoryQueryService": (".queries", "HistoryQueryService"),
    "ProfileStore": (".profile_store", "ProfileStore"),
    "ProfileSummary": (".profile_store", "ProfileSummary"),
    "SidecarIndexStore": (".sidecar_index", "SidecarIndexStore"),
    "StorageExporter": (".exporter", "StorageExporter"),
    "StorageSettings": (".database", "StorageSettings"),
    "index_run": (".indexer", "index_run"),
    "normalize_sidecar_record": (".sidecar_index", "normalize_sidecar_record"),
    "load_storage_config_file": (".database", "load_storage_config_file"),
    "resolve_run_uuid": (".database", "resolve_run_uuid"),
    "SIDECAR_COLLECTIONS": (".sidecar_index", "SIDECAR_COLLECTIONS"),
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
