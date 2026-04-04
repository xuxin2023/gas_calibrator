from __future__ import annotations

import importlib
import sys


def test_compare_import_does_not_pull_entry_or_v1_trace_module() -> None:
    module = importlib.import_module("gas_calibrator.v2.scripts.compare_v1_v2_control_flow")
    for name in (
        "gas_calibrator.v2.entry",
        "gas_calibrator.v2.scripts.run_v1_route_trace",
    ):
        sys.modules.pop(name, None)

    importlib.reload(module)

    assert "gas_calibrator.v2.entry" not in sys.modules
    assert "gas_calibrator.v2.scripts.run_v1_route_trace" not in sys.modules
