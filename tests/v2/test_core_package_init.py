from __future__ import annotations

import importlib
import sys


def test_core_package_init_is_lazy_for_calibration_service_import() -> None:
    sys.modules.pop("gas_calibrator.v2.core", None)
    sys.modules.pop("gas_calibrator.v2.core.calibration_service", None)

    module = importlib.import_module("gas_calibrator.v2.core")

    assert module.__all__
    assert "gas_calibrator.v2.core.calibration_service" not in sys.modules
