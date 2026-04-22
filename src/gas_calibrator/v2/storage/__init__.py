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
    "load_storage_config_file",
    "normalize_sidecar_record",
    "resolve_run_uuid",
    "SIDECAR_COLLECTIONS",
    "stable_uuid",
    # V1.3 governance: tamper-proof, retention, archival
    "TamperProofStore",
    "IntegritySeal",
    "RetentionPolicy",
    "ArchivalStrategy",
    "ArchiveFormat",
    "DEFAULT_RETENTION_POLICIES",
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
    "normalize_sidecar_record": (".sidecar_index", "normalize_sidecar_record"),
    "load_storage_config_file": (".database", "load_storage_config_file"),
    "resolve_run_uuid": (".database", "resolve_run_uuid"),
    "SIDECAR_COLLECTIONS": (".sidecar_index", "SIDECAR_COLLECTIONS"),
    "stable_uuid": (".database", "stable_uuid"),
    # V1.3 governance: tamper-proof, retention, archival (re-exported from core)
    "TamperProofStore": ("..core.conformity_and_governance_objects", "TamperProofStore"),
    "IntegritySeal": ("..core.conformity_and_governance_objects", "IntegritySeal"),
    "RetentionPolicy": ("..core.conformity_and_governance_objects", "RetentionPolicy"),
    "ArchivalStrategy": ("..core.conformity_and_governance_objects", "ArchivalStrategy"),
    "ArchiveFormat": ("..core.conformity_and_governance_objects", "ArchiveFormat"),
    "DEFAULT_RETENTION_POLICIES": ("..core.conformity_and_governance_objects", "DEFAULT_RETENTION_POLICIES"),
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
