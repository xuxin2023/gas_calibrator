from __future__ import annotations

import ast
from pathlib import Path

from gas_calibrator.storage.models import Base as SharedBase

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


def test_shared_storage_models_own_the_database_schema() -> None:
    assert SharedBase.__module__ == "gas_calibrator.storage.models"


def test_shared_storage_modules_do_not_import_v2() -> None:
    offenders = {
        path.relative_to(REPO_ROOT).as_posix(): _v2_storage_imports(path)
        for path in sorted((SOURCE_ROOT / "storage").glob("*.py"))
        if _v2_storage_imports(path)
    }
    assert offenders == {}
