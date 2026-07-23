from __future__ import annotations

import ast
from pathlib import Path

from gas_calibrator.storage.database import DatabaseManager as SharedDatabaseManager
from gas_calibrator.storage.importer import ArtifactImporter as SharedArtifactImporter
from gas_calibrator.storage.models import Base as SharedBase
from gas_calibrator.storage.queries import HistoryQueryService as SharedHistoryQueryService
from gas_calibrator.storage.sidecar_index import (
    SIDECAR_COLLECTIONS as SHARED_SIDECAR_COLLECTIONS,
)
from gas_calibrator.storage.sidecar_index import SidecarIndexStore as SharedSidecarIndexStore
from gas_calibrator.storage.sidecar_index import (
    normalize_sidecar_record as shared_normalize_sidecar_record,
)
from gas_calibrator.v2.storage.database import DatabaseManager as V2DatabaseManager
from gas_calibrator.v2.storage.importer import ArtifactImporter as V2ArtifactImporter
from gas_calibrator.v2.storage.models import Base as V2Base
from gas_calibrator.v2.storage.queries import HistoryQueryService as V2HistoryQueryService
from gas_calibrator.v2.storage.sidecar_index import (
    SIDECAR_COLLECTIONS as V2_SIDECAR_COLLECTIONS,
)
from gas_calibrator.v2.storage.sidecar_index import SidecarIndexStore as V2SidecarIndexStore
from gas_calibrator.v2.storage.sidecar_index import (
    normalize_sidecar_record as v2_normalize_sidecar_record,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "gas_calibrator"


def _v1_5_storage_consumers() -> list[Path]:
    files = [
        *sorted((SOURCE_ROOT / "v1_5").rglob("*.py")),
        *sorted((SOURCE_ROOT / "storage" / "v1_5_evidence").rglob("*.py")),
        *sorted((SOURCE_ROOT / "tools").glob("run_v1_5*.py")),
        *sorted((SOURCE_ROOT / "validation").glob("v1_5*.py")),
    ]
    return list(dict.fromkeys(files))


def _v2_storage_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(
                alias.name
                for alias in node.names
                if alias.name == "gas_calibrator.v2.storage"
                or alias.name.startswith("gas_calibrator.v2.storage.")
            )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "v2.storage" or module.startswith("v2.storage."):
                imports.append("." * node.level + module)
            elif module == "gas_calibrator.v2.storage" or module.startswith(
                "gas_calibrator.v2.storage."
            ):
                imports.append(module)
    return imports


def test_v1_5_storage_consumers_do_not_import_v2_storage() -> None:
    offenders = {
        path.relative_to(REPO_ROOT).as_posix(): _v2_storage_imports(path)
        for path in _v1_5_storage_consumers()
        if _v2_storage_imports(path)
    }
    assert offenders == {}


def test_v2_storage_database_compatibility_exports_shared_types() -> None:
    assert V2DatabaseManager is SharedDatabaseManager
    assert V2Base is SharedBase


def test_v2_artifact_importer_compatibility_exports_shared_type() -> None:
    assert V2ArtifactImporter is SharedArtifactImporter


def test_v2_sidecar_index_compatibility_exports_shared_types() -> None:
    assert V2SidecarIndexStore is SharedSidecarIndexStore
    assert v2_normalize_sidecar_record is shared_normalize_sidecar_record
    assert V2_SIDECAR_COLLECTIONS is SHARED_SIDECAR_COLLECTIONS


def test_v2_history_query_compatibility_exports_shared_type() -> None:
    assert V2HistoryQueryService is SharedHistoryQueryService


def test_shared_storage_modules_do_not_import_v2() -> None:
    offenders = {
        path.relative_to(REPO_ROOT).as_posix(): _v2_storage_imports(path)
        for path in sorted((SOURCE_ROOT / "storage").glob("*.py"))
        if _v2_storage_imports(path)
    }
    assert offenders == {}
