from __future__ import annotations

import ast
import os
from pathlib import Path
import subprocess
import sys

from gas_calibrator.v2.core import recognition_readiness_artifacts
from gas_calibrator.v2.core import reviewer_surface_contracts


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = REPO_ROOT / "src"
AFFECTED_MODULE_PATHS = (
    "gas_calibrator/v2/adapters/analyzer_coefficient_downloader.py",
    "gas_calibrator/v2/adapters/legacy_runner.py",
    "gas_calibrator/v2/config/models.py",
    "gas_calibrator/v2/core/artifact_compatibility.py",
    "gas_calibrator/v2/core/csv_resilience.py",
    "gas_calibrator/v2/core/engineering_isolation_admission_checklist.py",
    "gas_calibrator/v2/core/offline_artifacts.py",
    "gas_calibrator/validation/simulation/plan_models.py",
    "gas_calibrator/validation/simulation/plan_preview.py",
    "gas_calibrator/validation/simulation/plan_rows.py",
    "gas_calibrator/validation/simulation/point_parser.py",
    "gas_calibrator/validation/simulation/point_preparation.py",
    "gas_calibrator/validation/simulation/route_planner.py",
    "gas_calibrator/validation/simulation/runtime_point.py",
    "gas_calibrator/validation/simulation/sampling_contracts.py",
    "gas_calibrator/v2/core/recognition_readiness_artifacts.py",
    "gas_calibrator/v2/core/reviewer_summary_builders.py",
    "gas_calibrator/v2/core/run_logger.py",
    "gas_calibrator/v2/core/services/sampling_service.py",
    "gas_calibrator/v2/core/stage_admission_review_pack.py",
    "gas_calibrator/v2/core/step2_closeout_package_builder.py",
    "gas_calibrator/v2/core/step2_closeout_readiness_builder.py",
    "gas_calibrator/v2/core/step2_final_closure_matrix.py",
    "gas_calibrator/v2/core/step2_freeze_audit_builder.py",
    "gas_calibrator/v2/core/step2_freeze_seal_builder.py",
    "gas_calibrator/v2/core/step3_admission_dossier_builder.py",
    "gas_calibrator/v2/entry.py",
    "gas_calibrator/v2/intelligence/summarizer.py",
    "gas_calibrator/v2/scripts/historical_artifacts.py",
    "gas_calibrator/v2/scripts/run_simulation_suite.py",
    "gas_calibrator/v2/sim/devices/analyzer_fake.py",
)
AFFECTED_MODULES = tuple(
    path.removesuffix(".py").replace("/", ".") for path in AFFECTED_MODULE_PATHS
)


def test_affected_import_bindings_are_used_or_explicitly_reexported() -> None:
    unused_bindings: list[str] = []
    for relative_path in AFFECTED_MODULE_PATHS:
        path = SOURCE_ROOT / relative_path
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        loaded_names = {
            node.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
        }
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                continue
            if isinstance(node, ast.ImportFrom) and node.module == "__future__":
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                local_name = alias.asname or (
                    alias.name if isinstance(node, ast.ImportFrom) else alias.name.split(".")[0]
                )
                explicit_reexport = (
                    isinstance(node, ast.ImportFrom) and alias.asname == alias.name
                )
                if local_name not in loaded_names and not explicit_reexport:
                    unused_bindings.append(
                        f"{path.relative_to(REPO_ROOT)}:{node.lineno}:{local_name}"
                    )

    assert unused_bindings == []


def test_step2_closeout_markdown_filename_remains_an_explicit_reexport() -> None:
    assert (
        recognition_readiness_artifacts.STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME
        == reviewer_surface_contracts.STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME
    )


def test_affected_modules_import_in_a_clean_interpreter() -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(SOURCE_ROOT), env.get("PYTHONPATH", "")) if part
    )
    module_list = repr(AFFECTED_MODULES)
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import importlib; "
                f"modules = {module_list}; "
                "[importlib.import_module(name) for name in modules]; "
                "print(len(modules))"
            ),
        ],
        cwd=REPO_ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == str(len(AFFECTED_MODULES))
