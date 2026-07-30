from __future__ import annotations

import pandas as pd

from gas_calibrator.tools._no500_filter import filter_no_500_frame


def test_filter_no_500_frame_removes_only_500hpa_rows() -> None:
    frame = pd.DataFrame(
        [
            {
                "Analyzer": "GA01",
                "PressureMode": "ambient_open",
                "PressureTargetLabel": "ambient",
                "PressureTarget": None,
            },
            {
                "Analyzer": "GA01",
                "PressureMode": "sealed_controlled",
                "PressureTargetLabel": "500hPa",
                "PressureTarget": 500.0,
            },
            {
                "Analyzer": "GA01",
                "PressureMode": "sealed_controlled",
                "PressureTargetLabel": "700hPa",
                "PressureTarget": 700.0,
            },
        ]
    )

    filtered, stats = filter_no_500_frame(frame)

    assert list(filtered["PressureTargetLabel"]) == ["ambient", "700hPa"]
    assert stats == {"original_rows": 3, "removed_rows": 1, "kept_rows": 2}


def test_filter_no_500_frame_preserves_mode_dependent_tolerance() -> None:
    frame = pd.DataFrame(
        [
            {
                "PressureMode": "ambient_open",
                "PressureTargetLabel": "504hPa",
                "PressureTarget": 504.0,
            },
            {
                "PressureMode": "sealed_controlled",
                "PressureTargetLabel": "504hPa",
                "PressureTarget": 504.0,
            },
            {
                "PressureMode": "sealed_controlled",
                "PressureTargetLabel": "506hPa",
                "PressureTarget": 506.0,
            },
        ]
    )

    filtered, stats = filter_no_500_frame(frame)

    assert list(filtered["PressureTarget"]) == [504.0, 506.0]
    assert stats == {"original_rows": 3, "removed_rows": 1, "kept_rows": 2}


def test_filter_no_500_frame_keeps_empty_contract() -> None:
    frame = pd.DataFrame()

    filtered, stats = filter_no_500_frame(frame)

    assert filtered.empty
    assert stats == {"original_rows": 0, "removed_rows": 0, "kept_rows": 0}
