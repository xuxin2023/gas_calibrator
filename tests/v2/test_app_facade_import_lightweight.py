from __future__ import annotations

import importlib
import sys


def test_app_facade_import_does_not_pull_runtime_stack() -> None:
    module = importlib.import_module("gas_calibrator.v2.ui_v2.controllers.app_facade")
    for name in (
        "gas_calibrator.v2.entry",
        "gas_calibrator.v2.core.calibration_service",
    ):
        sys.modules.pop(name, None)

    importlib.reload(module)

    assert "gas_calibrator.v2.entry" not in sys.modules
    assert "gas_calibrator.v2.core.calibration_service" not in sys.modules
