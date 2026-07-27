from __future__ import annotations

import importlib
import sys


def test_app_facade_import_does_not_pull_runtime_stack(monkeypatch) -> None:
    import gas_calibrator.v2 as v2_package
    import gas_calibrator.v2.core as core_package

    module = importlib.import_module("gas_calibrator.v2.ui_v2.controllers.app_facade")
    monkeypatch.delattr(v2_package, "entry", raising=False)
    monkeypatch.delattr(core_package, "calibration_service", raising=False)
    monkeypatch.delitem(sys.modules, "gas_calibrator.v2.entry", raising=False)
    monkeypatch.delitem(
        sys.modules,
        "gas_calibrator.v2.core.calibration_service",
        raising=False,
    )

    importlib.reload(module)

    assert "gas_calibrator.v2.entry" not in sys.modules
    assert "gas_calibrator.v2.core.calibration_service" not in sys.modules
