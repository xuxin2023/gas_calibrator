from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.validation.simulation.sampling_contracts import (
    sampling_result_to_row,
    standard_analyzer_row_values,
)
from gas_calibrator.v2.core.orchestrator import WorkflowOrchestrator
from gas_calibrator.v2.core.services.sampling_service import (
    SamplingService,
)


def _result() -> SimpleNamespace:
    point = SimpleNamespace(
        index=7,
        temperature_c=25.5,
        co2_ppm=401.0,
        co2_group="B",
        cylinder_nominal_ppm=405.2,
        humidity_pct=48.0,
        route="co2",
    )
    return SimpleNamespace(
        point=point,
        analyzer_id="GA03",
        timestamp=datetime(
            2026,
            7,
            30,
            1,
            2,
            3,
            456789,
            tzinfo=timezone.utc,
        ),
        co2_ppm=402.1,
        h2o_mmol=9.8,
        h2o_signal=11.1,
        co2_signal=22.2,
        co2_ratio_f=0.111,
        co2_ratio_raw=0.112,
        h2o_ratio_f=0.221,
        h2o_ratio_raw=0.222,
        ref_signal=88.8,
        pressure_hpa=998.6,
        pressure_gauge_hpa=998.4,
        pressure_reference_status="healthy",
        thermometer_temp_c=24.95,
        thermometer_reference_status="healthy",
        dew_point_c=-12.3,
        analyzer_pressure_kpa=99.7,
        analyzer_chamber_temp_c=25.1,
        case_temp_c=26.2,
        frame_has_data=True,
        frame_usable=False,
        frame_status="partial",
        sample_index=4,
    )


def test_sampling_rows_have_shared_owner_and_no_v2_import() -> None:
    assert standard_analyzer_row_values.__module__ == (
        "gas_calibrator.validation.simulation.sampling_contracts"
    )
    assert sampling_result_to_row.__module__ == (
        "gas_calibrator.validation.simulation.sampling_contracts"
    )
    assert "STANDARD_ANALYZER_ROW_FIELDS" not in SamplingService.__dict__

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


def test_sampling_rows_preserve_standard_field_order_and_missing_values() -> None:
    result = _result()
    values = standard_analyzer_row_values(result)

    assert list(values) == [
        "co2_ppm",
        "h2o_mmol",
        "co2_ratio_f",
        "h2o_ratio_f",
        "co2_signal",
        "h2o_signal",
        "ref_signal",
        "analyzer_chamber_temp_c",
        "case_temp_c",
    ]
    assert values == {
        "co2_ppm": 402.1,
        "h2o_mmol": 9.8,
        "co2_ratio_f": 0.111,
        "h2o_ratio_f": 0.221,
        "co2_signal": 22.2,
        "h2o_signal": 11.1,
        "ref_signal": 88.8,
        "analyzer_chamber_temp_c": 25.1,
        "case_temp_c": 26.2,
    }
    assert standard_analyzer_row_values(
        SimpleNamespace(co2_ppm=1.0)
    )["case_temp_c"] is None


def test_sampling_rows_preserve_full_schema_values_and_direct_consumer() -> None:
    result = _result()
    row = sampling_result_to_row(result)

    assert list(row) == [
        "timestamp",
        "point_index",
        "temperature_c",
        "co2_ppm",
        "co2_group",
        "cylinder_nominal_ppm",
        "humidity_pct",
        "route",
        "analyzer_id",
        "sample_co2_ppm",
        "sample_h2o_mmol",
        "h2o_signal",
        "co2_signal",
        "co2_ratio_f",
        "co2_ratio_raw",
        "h2o_ratio_f",
        "h2o_ratio_raw",
        "ref_signal",
        "pressure_hpa",
        "pressure_gauge_hpa",
        "pressure_reference_status",
        "thermometer_temp_c",
        "thermometer_reference_status",
        "dew_point_c",
        "analyzer_pressure_kpa",
        "analyzer_chamber_temp_c",
        "case_temp_c",
        "frame_has_data",
        "frame_usable",
        "frame_status",
        "sample_index",
    ]
    assert row == {
        "timestamp": "2026-07-30T01:02:03.456789+00:00",
        "point_index": 7,
        "temperature_c": 25.5,
        "co2_ppm": 401.0,
        "co2_group": "B",
        "cylinder_nominal_ppm": 405.2,
        "humidity_pct": 48.0,
        "route": "co2",
        "analyzer_id": "GA03",
        "sample_co2_ppm": 402.1,
        "sample_h2o_mmol": 9.8,
        "h2o_signal": 11.1,
        "co2_signal": 22.2,
        "co2_ratio_f": 0.111,
        "co2_ratio_raw": 0.112,
        "h2o_ratio_f": 0.221,
        "h2o_ratio_raw": 0.222,
        "ref_signal": 88.8,
        "pressure_hpa": 998.6,
        "pressure_gauge_hpa": 998.4,
        "pressure_reference_status": "healthy",
        "thermometer_temp_c": 24.95,
        "thermometer_reference_status": "healthy",
        "dew_point_c": -12.3,
        "analyzer_pressure_kpa": 99.7,
        "analyzer_chamber_temp_c": 25.1,
        "case_temp_c": 26.2,
        "frame_has_data": True,
        "frame_usable": False,
        "frame_status": "partial",
        "sample_index": 4,
    }
    assert WorkflowOrchestrator._sampling_result_to_row(result) == row
    assert "standard_analyzer_row_values" not in SamplingService.__dict__
    assert "sampling_result_to_row" not in SamplingService.__dict__
