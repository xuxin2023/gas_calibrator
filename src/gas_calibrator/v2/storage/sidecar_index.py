"""Compatibility exports for the shared sidecar index storage layer."""

from __future__ import annotations

from ...storage.sidecar_index import (
    SIDECAR_COLLECTIONS,
    SidecarIndexStore,
    normalize_sidecar_record,
)


__all__ = [
    "SIDECAR_COLLECTIONS",
    "SidecarIndexStore",
    "normalize_sidecar_record",
]
