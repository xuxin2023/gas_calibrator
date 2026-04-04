from __future__ import annotations

import importlib
import sys


def test_sim_package_import_does_not_pull_runtime_stack() -> None:
    module = importlib.import_module("gas_calibrator.v2.sim")
    for name in (
        "gas_calibrator.v2.entry",
        "gas_calibrator.v2.core.calibration_service",
        "gas_calibrator.v2.core.orchestrator",
    ):
        sys.modules.pop(name, None)

    importlib.reload(module)

    assert "gas_calibrator.v2.entry" not in sys.modules
    assert "gas_calibrator.v2.core.calibration_service" not in sys.modules
    assert "gas_calibrator.v2.core.orchestrator" not in sys.modules


def test_simulation_suite_script_import_stays_lightweight() -> None:
    module = importlib.import_module("gas_calibrator.v2.scripts.run_simulation_suite")
    for name in (
        "gas_calibrator.v2.entry",
        "gas_calibrator.v2.core.calibration_service",
        "gas_calibrator.v2.core.orchestrator",
    ):
        sys.modules.pop(name, None)

    importlib.reload(module)

    assert "gas_calibrator.v2.entry" not in sys.modules
    assert "gas_calibrator.v2.core.calibration_service" not in sys.modules
    assert "gas_calibrator.v2.core.orchestrator" not in sys.modules
