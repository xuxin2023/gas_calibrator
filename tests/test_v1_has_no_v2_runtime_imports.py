from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
V1_RUNTIME_FILES = [
    REPO_ROOT / "src/gas_calibrator/config.py",
    REPO_ROOT / "src/gas_calibrator/workflow/runner.py",
    REPO_ROOT / "src/gas_calibrator/devices/gas_analyzer.py",
    REPO_ROOT / "src/gas_calibrator/logging_utils.py",
    REPO_ROOT / "src/gas_calibrator/tools/run_headless.py",
    REPO_ROOT / "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py",
    REPO_ROOT / "src/gas_calibrator/tools/run_v1_online_acceptance.py",
]


def _forbidden_v2_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("gas_calibrator.v2"):
                    hits.append(f"{path}:{node.lineno} import {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            module = str(node.module or "")
            if module.startswith("gas_calibrator.v2") or module.startswith("v2."):
                hits.append(f"{path}:{node.lineno} from {'.' * node.level}{module}")
    return hits


def test_v1_runtime_has_no_direct_v2_imports() -> None:
    violations: list[str] = []
    for path in V1_RUNTIME_FILES:
        violations.extend(_forbidden_v2_imports(path))
    assert violations == []
