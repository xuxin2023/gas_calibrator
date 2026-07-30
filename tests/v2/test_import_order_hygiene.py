from __future__ import annotations

import ast
import os
from pathlib import Path
import subprocess
import sys

from gas_calibrator.v2.core import recognition_readiness_artifacts
from gas_calibrator.v2.core import services
from gas_calibrator.v2.core.software_validation_builder import (
    build_software_validation_wp5_artifacts,
)
from gas_calibrator.v2.core.uncertainty_builder import build_uncertainty_wp3_artifacts
from gas_calibrator.v2.core.wp6_builder import (
    build_step2_closeout_digest,
    build_wp6_artifacts,
)
from gas_calibrator.v2.core.services.sampling_service import SamplingService
from gas_calibrator.v2.sim import devices
from gas_calibrator.v2.sim.devices.analyzer_fake import AnalyzerFake


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = REPO_ROOT / "src"
IMPORT_ORDER_MODULE_PATHS = (
    "gas_calibrator/v2/core/recognition_readiness_artifacts.py",
    "gas_calibrator/v2/core/services/__init__.py",
    "gas_calibrator/v2/sim/devices/__init__.py",
)
IMPORT_FAMILIES = (
    (
        "gas_calibrator.v2.core.recognition_readiness_artifacts",
        "gas_calibrator.v2.core.software_validation_builder",
        "gas_calibrator.v2.core.uncertainty_builder",
        "gas_calibrator.v2.core.wp6_builder",
    ),
    (
        "gas_calibrator.v2.core.services",
        "gas_calibrator.v2.core.services.sampling_service",
        "gas_calibrator.v2.core.services.pressure_control_service",
    ),
    (
        "gas_calibrator.v2.sim.devices",
        "gas_calibrator.v2.sim.devices.analyzer_fake",
        "gas_calibrator.v2.sim.devices.temp_chamber_fake",
    ),
    (
        "gas_calibrator.v2.algorithms",
        "gas_calibrator.v2.algorithms.base",
        "gas_calibrator.validation.simulation.domain",
    ),
    (
        "gas_calibrator.validation.simulation.domain",
        "gas_calibrator.v2.config",
        "gas_calibrator.v2.core.plan_compiler",
    ),
)


def test_imports_precede_runtime_definitions() -> None:
    misplaced_imports: list[str] = []
    for relative_path in IMPORT_ORDER_MODULE_PATHS:
        path = SOURCE_ROOT / relative_path
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        runtime_definition_seen = False
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if runtime_definition_seen:
                    misplaced_imports.append(
                        f"{path.relative_to(REPO_ROOT)}:{node.lineno}"
                    )
                continue
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                continue
            runtime_definition_seen = True

    assert misplaced_imports == []


def test_public_reexports_preserve_object_identity() -> None:
    assert (
        recognition_readiness_artifacts.build_software_validation_wp5_artifacts
        is build_software_validation_wp5_artifacts
    )
    assert (
        recognition_readiness_artifacts.build_uncertainty_wp3_artifacts
        is build_uncertainty_wp3_artifacts
    )
    assert (
        recognition_readiness_artifacts.build_step2_closeout_digest
        is build_step2_closeout_digest
    )
    assert recognition_readiness_artifacts.build_wp6_artifacts is build_wp6_artifacts
    assert services.SamplingService is SamplingService
    assert devices.AnalyzerFake is AnalyzerFake


def test_import_families_load_forward_and_reverse_in_clean_interpreters() -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(SOURCE_ROOT), env.get("PYTHONPATH", "")) if part
    )
    for family in IMPORT_FAMILIES:
        for order in (family, tuple(reversed(family))):
            module_list = repr(order)
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "import importlib; "
                        f"modules = {module_list}; "
                        "[importlib.import_module(name) for name in modules]"
                    ),
                ],
                cwd=REPO_ROOT,
                env=env,
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )

            assert completed.returncode == 0, (
                f"import order failed: {order}\n{completed.stderr}"
            )


def test_domain_services_facade_and_spectral_engine_are_retired() -> None:
    assert not (
        SOURCE_ROOT / "gas_calibrator/v2/domain/services/__init__.py"
    ).exists()
    assert not (
        SOURCE_ROOT
        / "gas_calibrator/v2/domain/services/spectral_quality_engine.py"
    ).exists()
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(SOURCE_ROOT), env.get("PYTHONPATH", "")) if part
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "from gas_calibrator.v2.core import offline_artifacts; "
                "assert not hasattr(offline_artifacts, "
                "'build_run_spectral_quality_summary'); "
                "loaded = sorted(name for name in sys.modules "
                "if name.startswith('gas_calibrator.v2.domain.services.')); "
                "assert loaded == [], loaded"
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
