from __future__ import annotations

import importlib
import sys


def test_compare_import_does_not_pull_entry_or_v1_trace_module(monkeypatch) -> None:
    import gas_calibrator.v2 as v2_package
    import gas_calibrator.v2.scripts as scripts_package

    module = importlib.import_module("gas_calibrator.v2.scripts.compare_v1_v2_control_flow")
    monkeypatch.delattr(v2_package, "entry", raising=False)
    monkeypatch.delattr(scripts_package, "run_v1_route_trace", raising=False)
    monkeypatch.delitem(sys.modules, "gas_calibrator.v2.entry", raising=False)
    monkeypatch.delitem(
        sys.modules,
        "gas_calibrator.v2.scripts.run_v1_route_trace",
        raising=False,
    )

    importlib.reload(module)

    assert "gas_calibrator.v2.entry" not in sys.modules
    assert "gas_calibrator.v2.scripts.run_v1_route_trace" not in sys.modules
