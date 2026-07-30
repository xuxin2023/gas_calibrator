from __future__ import annotations

import ast
from pathlib import Path

from gas_calibrator.validation.simulation.sampling_contracts import (
    evaluate_sample_quality,
    sample_span,
)
from gas_calibrator.v2.core.services.sampling_service import SamplingService


def test_sampling_quality_helpers_have_shared_owner_and_no_v2_import() -> None:
    assert sample_span.__module__ == (
        "gas_calibrator.validation.simulation.sampling_contracts"
    )
    assert evaluate_sample_quality.__module__ == (
        "gas_calibrator.validation.simulation.sampling_contracts"
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


def test_sampling_quality_preserves_limits_and_strict_boundary() -> None:
    rows = [
        {
            "co2_ppm": 1.0,
            "h2o_mmol": 4.0,
            "pressure_hpa": 900.0,
            "dewpoint_c": -20.0,
        },
        {
            "co2_ppm": 3.0,
            "h2o_mmol": 5.0,
            "pressure_hpa": 905.0,
            "dewpoint_c": -19.5,
        },
    ]
    exact_limits = {
        "enabled": True,
        "max_span_co2_ppm": 2.0,
        "max_span_h2o_mmol": 1.0,
        "max_span_pressure_hpa": 5.0,
        "max_span_dewpoint_c": 0.5,
    }

    assert evaluate_sample_quality(
        rows,
        quality_config=exact_limits,
    ) == (
        True,
        {
            "co2_ppm": 2.0,
            "h2o_mmol": 1.0,
            "pressure_hpa": 5.0,
            "dewpoint_c": 0.5,
        },
    )
    assert evaluate_sample_quality(
        rows,
        quality_config={
            **exact_limits,
            "max_span_co2_ppm": 1.9,
        },
    ) == (
        False,
        {
            "co2_ppm": 2.0,
            "h2o_mmol": 1.0,
            "pressure_hpa": 5.0,
            "dewpoint_c": 0.5,
        },
    )


def test_sampling_quality_disabled_missing_and_no_duplicate_service_owner() -> None:
    assert sample_span([]) == 0.0
    assert sample_span([5.0]) == 0.0
    assert sample_span([-2.0, 3.5, 1.0]) == 5.5
    assert evaluate_sample_quality(
        [{"co2_ppm": 1.0}, {"co2_ppm": 9.0}],
        quality_config={"enabled": False},
    ) == (True, {})
    assert evaluate_sample_quality(
        [{"co2_ppm": None}, {"co2_ppm": 2.0}],
        quality_config={
            "enabled": True,
            "max_span_co2_ppm": 1.0,
        },
    ) == (True, {"co2_ppm": 0.0})

    assert "span" not in SamplingService.__dict__
    assert "evaluate_sample_quality" not in SamplingService.__dict__
