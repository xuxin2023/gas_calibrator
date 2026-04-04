from __future__ import annotations

import importlib
import sys


def test_storage_package_init_is_lazy_for_database_import() -> None:
    sys.modules.pop("gas_calibrator.v2.storage", None)
    sys.modules.pop("gas_calibrator.v2.storage.database", None)

    module = importlib.import_module("gas_calibrator.v2.storage")

    assert module.__all__
    assert "gas_calibrator.v2.storage.database" not in sys.modules
