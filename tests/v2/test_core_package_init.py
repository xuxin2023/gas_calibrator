from __future__ import annotations

import importlib
import sys


def test_core_package_init_is_lazy_for_calibration_service_import(monkeypatch) -> None:
    import gas_calibrator.v2 as v2_package

    monkeypatch.delattr(v2_package, "core", raising=False)
    monkeypatch.delitem(sys.modules, "gas_calibrator.v2.core", raising=False)
    monkeypatch.delitem(
        sys.modules,
        "gas_calibrator.v2.core.calibration_service",
        raising=False,
    )

    module = importlib.import_module("gas_calibrator.v2.core")

    assert module.__all__
    assert "gas_calibrator.v2.core.calibration_service" not in sys.modules
