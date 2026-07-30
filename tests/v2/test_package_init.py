from __future__ import annotations

import importlib
from pathlib import Path
import sys

import gas_calibrator
import pytest


def test_v2_package_root_is_a_namespace_without_product_entry_facade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    had_parent_attribute = hasattr(gas_calibrator, "v2")
    with monkeypatch.context() as isolated:
        isolated.delitem(sys.modules, "gas_calibrator.v2", raising=False)
        isolated.delitem(sys.modules, "gas_calibrator.v2.entry", raising=False)
        isolated.delattr(gas_calibrator, "v2", raising=False)

        module = importlib.import_module("gas_calibrator.v2")
        package_init = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "gas_calibrator"
            / "v2"
            / "__init__.py"
        )

        assert not package_init.exists()
        assert module.__spec__.submodule_search_locations is not None
        assert not hasattr(module, "create_calibration_service")
        assert not hasattr(module, "run_calibration")
        assert not hasattr(module, "__version__")
        assert "gas_calibrator.v2.entry" not in sys.modules

    if not had_parent_attribute:
        delattr(gas_calibrator, "v2")
