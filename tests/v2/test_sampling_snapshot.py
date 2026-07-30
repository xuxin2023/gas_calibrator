from __future__ import annotations

import ast
from pathlib import Path

from gas_calibrator.validation.simulation.sampling_contracts import (
    normalize_snapshot,
    pick_humidity_value,
    pick_numeric,
    pick_text,
    sanitize_humidity_value,
    snapshot_has_data,
    snapshot_retry_reason,
)
from gas_calibrator.v2.core.services.sampling_service import (
    SamplingService,
)
from gas_calibrator.v2.core.orchestrator import WorkflowOrchestrator


def test_sampling_snapshot_helpers_have_shared_owner_and_no_v2_import() -> None:
    helpers = (
        normalize_snapshot,
        pick_humidity_value,
        pick_numeric,
        pick_text,
        sanitize_humidity_value,
        snapshot_has_data,
        snapshot_retry_reason,
    )
    assert all(
        helper.__module__
        == "gas_calibrator.validation.simulation.sampling_contracts"
        for helper in helpers
    )

    path = (
        Path(__file__).resolve().parents[2]
        / "src/gas_calibrator/validation/simulation/sampling_contracts.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported_modules = {
        str(node.module or "")
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert all(
        not module.startswith("gas_calibrator.v2")
        for module in imported_modules
    )


def test_sampling_snapshot_behavior_preserves_nested_and_sensor_values() -> None:
    nested = normalize_snapshot(
        {
            "data": {
                "co2_ppm": "401.25",
                "status": "inner",
            },
            "status": "outer",
        }
    )

    assert nested == {
        "co2_ppm": "401.25",
        "data": {
            "co2_ppm": "401.25",
            "status": "inner",
        },
        "status": "outer",
    }
    assert normalize_snapshot(["bad"]) == {}
    assert pick_numeric({"x": "bad", "y": " 12.5 "}, "x", "y") == 12.5
    assert pick_text({"a": "  ", "b": 23}, "a", "b") == "23"
    assert pick_humidity_value({"Uw": "44.5"}) == 44.5
    assert sanitize_humidity_value(0.0) == 0.0
    assert sanitize_humidity_value(100.0) == 100.0
    assert sanitize_humidity_value(100.01) is None


def test_sampling_snapshot_retry_contract_and_direct_consumer_surface() -> None:
    empty = {"data": {}, "status": "  "}
    missing = {"value": "x"}
    ready = {"data": {"co2_ppm": "399.0"}}

    assert snapshot_has_data(normalize_snapshot(empty)) is False
    assert snapshot_retry_reason(
        empty,
        required_keys=(),
        retry_on_empty=True,
    ) == "empty snapshot"
    assert snapshot_retry_reason(
        missing,
        required_keys=("co2_ppm",),
        retry_on_empty=False,
    ) == "missing numeric data for keys=co2_ppm"
    assert (
        snapshot_retry_reason(
            ready,
            required_keys=("co2_ppm",),
            retry_on_empty=True,
        )
        is None
    )

    assert WorkflowOrchestrator._normalize_snapshot(ready) == (
        normalize_snapshot(ready)
    )
    assert WorkflowOrchestrator._pick_numeric(
        {"co2_ppm": "399.0"},
        "co2_ppm",
    ) == pick_numeric({"co2_ppm": "399.0"}, "co2_ppm")
    assert WorkflowOrchestrator._pick_humidity_value({"Uw": "44.5"}) == (
        pick_humidity_value({"Uw": "44.5"})
    )
    retired_delegates = {
        "snapshot_retry_reason",
        "_snapshot_has_data",
        "normalize_snapshot",
        "pick_numeric",
        "pick_text",
        "pick_humidity_value",
        "sanitize_humidity_value",
        "standard_analyzer_row_values",
        "sampling_result_to_row",
        "span",
        "evaluate_sample_quality",
        "summarize_analyzer_integrity",
        "sensor_read_retry_settings",
    }
    assert retired_delegates.isdisjoint(SamplingService.__dict__)
