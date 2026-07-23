"""Compatibility exports for the shared storage database layer.

The implementation is owned by ``gas_calibrator.storage`` so V1.5 does not
depend on the V2 namespace. Existing V2 imports remain valid.
"""

from __future__ import annotations

from ...storage.database import (
    DatabaseManager,
    STORAGE_NAMESPACE,
    StorageSettings,
    load_storage_config_file,
    resolve_run_uuid,
    stable_uuid,
)


__all__ = [
    "DatabaseManager",
    "STORAGE_NAMESPACE",
    "StorageSettings",
    "load_storage_config_file",
    "resolve_run_uuid",
    "stable_uuid",
]
