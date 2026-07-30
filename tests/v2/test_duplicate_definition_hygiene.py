from __future__ import annotations

import ast
import hashlib
import os
from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
UNIQUE_FUNCTIONS = {
    REPO_ROOT / "src" / "gas_calibrator" / "v2" / "core" / "artifact_compatibility.py": {
        "build_artifact_compatibility_overview",
        "regenerate_artifact_compatibility_sidecars",
    },
    REPO_ROOT / "src" / "gas_calibrator" / "v2" / "core" / "wp6_builder.py": {
        "_comparison_digest_artifact",
        "_comparison_evidence_pack_artifact",
        "_comparison_rollup_artifact",
        "_importer_artifact",
        "_normalize_comparison_payload",
        "_pt_ilc_registry_artifact",
        "_scope_comparison_view_artifact",
        "build_wp6_artifacts",
        "import_comparison_from_csv",
        "import_comparison_from_json",
    },
}
WP6_FINAL_FUNCTION_HASHES = {
    "_pt_ilc_registry_artifact": "5a9ddf133af8d3c50e3b7065e28974c187b9f603262ab0c6b550510ad5b2facb",
    "_importer_artifact": "3d7b942a9fba94b8c4cd322dc5365ddafd6401288cadc5d7e1cbe6aff4e0fc25",
    "_comparison_evidence_pack_artifact": "a88a9c4a49c1e8ca205806c2fc25f3ed422c99ab2ddce83467bf937af9dfa1c4",
    "_scope_comparison_view_artifact": "56fc8a0fb327f350be60ce27a08026a087b76d19caf5402b3f49e5cdc6c292c5",
    "_comparison_digest_artifact": "db904b5f26a6caf7fc0fab5ec4d26b76cf5ac124d53873a9ef50122513fc4b57",
    "_comparison_rollup_artifact": "8104fedc8b0e2d95681348a502d335edc685f0fc0d11400a7a45f68bc9a634c7",
    "_normalize_comparison_payload": "a5ed562a44dcf59c9f2327046aab523f9935174c47a45bd2115235e45503608c",
}


def test_public_artifact_functions_have_one_definition_each() -> None:
    duplicates: list[tuple[str, str, int]] = []

    for path, expected_names in UNIQUE_FUNCTIONS.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for name in sorted(expected_names):
            count = sum(
                1
                for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
            )
            if count != 1:
                duplicates.append((path.name, name, count))

    assert duplicates == []


def test_wp6_retained_private_implementations_are_unchanged() -> None:
    path = REPO_ROOT / "src" / "gas_calibrator" / "v2" / "core" / "wp6_builder.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    assert {
        name: hashlib.sha256(
            ast.dump(functions[name], include_attributes=False).encode()
        ).hexdigest()
        for name in WP6_FINAL_FUNCTION_HASHES
    } == WP6_FINAL_FUNCTION_HASHES


def test_config_path_symbol_has_one_import_binding() -> None:
    path = REPO_ROOT / "src" / "gas_calibrator" / "v2" / "config" / "models.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    path_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "pathlib"
        and any(alias.name == "Path" for alias in node.names)
    ]

    assert len(path_imports) == 1


def test_wp6_builder_imports_in_a_clean_interpreter() -> None:
    env = os.environ.copy()
    source_root = str(REPO_ROOT / "src")
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (source_root, env.get("PYTHONPATH", "")) if part
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from gas_calibrator.v2.core.wp6_builder import "
                "build_wp6_artifacts, import_comparison_from_csv, import_comparison_from_json; "
                "print(build_wp6_artifacts.__module__)"
            ),
        ],
        cwd=REPO_ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "gas_calibrator.v2.core.wp6_builder"
