from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
V2_RUNTIME_ROOTS = [
    REPO_ROOT / "src/gas_calibrator/v2/config",
    REPO_ROOT / "src/gas_calibrator/v2/core",
    REPO_ROOT / "src/gas_calibrator/v2/ui_v2",
    REPO_ROOT / "src/gas_calibrator/v2/storage",
    REPO_ROOT / "src/gas_calibrator/v2/analytics",
    REPO_ROOT / "src/gas_calibrator/v2/qc",
    REPO_ROOT / "src/gas_calibrator/v2/domain",
]
V2_RUNTIME_FILES = [
    REPO_ROOT / "src/gas_calibrator/v2/entry.py",
]
FORBIDDEN_V1_MODULES = {
    "gas_calibrator.workflow.runner",
    "gas_calibrator.logging_utils",
    "gas_calibrator.tools.run_headless",
    "workflow.runner",
    "logging_utils",
    "tools.run_headless",
}


def _iter_v2_runtime_files() -> list[Path]:
    files = list(V2_RUNTIME_FILES)
    for root in V2_RUNTIME_ROOTS:
        files.extend(sorted(root.rglob("*.py")))
    return files


def _forbidden_v1_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in FORBIDDEN_V1_MODULES or alias.name.startswith("gas_calibrator.tools.run_v1_"):
                    hits.append(f"{path}:{node.lineno} import {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            module = str(node.module or "")
            if module in FORBIDDEN_V1_MODULES:
                hits.append(f"{path}:{node.lineno} from {'.' * node.level}{module}")
            if module.startswith("gas_calibrator.tools.run_v1_"):
                hits.append(f"{path}:{node.lineno} from {'.' * node.level}{module}")
    return hits


def test_v2_runtime_has_no_direct_v1_runtime_imports() -> None:
    violations: list[str] = []
    for path in _iter_v2_runtime_files():
        violations.extend(_forbidden_v1_imports(path))
    assert violations == []
