from __future__ import annotations

import importlib
import sys


def test_v2_package_init_is_lazy_for_entry_import() -> None:
    sys.modules.pop("gas_calibrator.v2", None)
    sys.modules.pop("gas_calibrator.v2.entry", None)

    module = importlib.import_module("gas_calibrator.v2")

    assert hasattr(module, "create_calibration_service")
    assert "gas_calibrator.v2.entry" not in sys.modules
