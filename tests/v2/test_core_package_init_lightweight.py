from __future__ import annotations

import importlib
import sys


def test_core_package_import_is_lazy() -> None:
    module = importlib.import_module("gas_calibrator.v2.core")
    for name in (
        "gas_calibrator.v2.core.calibration_service",
        "gas_calibrator.v2.core.device_manager",
        "gas_calibrator.v2.core.orchestrator",
    ):
        sys.modules.pop(name, None)

    importlib.reload(module)

    assert "gas_calibrator.v2.core.calibration_service" not in sys.modules
    assert "gas_calibrator.v2.core.device_manager" not in sys.modules
    assert "gas_calibrator.v2.core.orchestrator" not in sys.modules
