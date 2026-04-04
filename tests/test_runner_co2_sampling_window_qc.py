from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


def _point_co2_low_pressure() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=500.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=700.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="B",
    )


def _sampling_rows(values: list[float]) -> list[dict]:
    start = datetime(2026, 4, 4, 10, 0, 0)
    rows: list[dict] = []
    for idx, value in enumerate(values):
        ts = start + timedelta(seconds=idx)
        rows.append(
            {
                "sample_ts": ts.isoformat(timespec="milliseconds"),
                "sample_start_ts": ts.isoformat(timespec="milliseconds"),
                "dewpoint_live_c": value,
            }
        )
    return rows


def test_evaluate_co2_sampling_window_qc_passes_for_stable_window(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_sampling_window_qc_enabled": True,
                    "co2_sampling_window_qc_max_range_c": 0.20,
                    "co2_sampling_window_qc_max_rise_c": 0.12,
                    "co2_sampling_window_qc_max_abs_slope_c_per_s": 0.02,
                    "co2_sampling_window_qc_policy": "reject",
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()

    result = runner._evaluate_co2_sampling_window_qc(
        point,
        phase="co2",
        samples=_sampling_rows(
            [-24.50, -24.49, -24.48, -24.47, -24.47, -24.46, -24.45, -24.45, -24.44, -24.43]
        ),
    )
    logger.close()

    assert result["sampling_window_dewpoint_first_c"] == -24.5
    assert result["sampling_window_dewpoint_last_c"] == -24.43
    assert result["sampling_window_dewpoint_range_c"] == 0.07
    assert result["sampling_window_dewpoint_rise_c"] == 0.07
    assert pytest.approx(result["sampling_window_dewpoint_slope_c_per_s"], abs=1e-6) == 0.07 / 9.0
    assert result["sampling_window_qc_status"] == "pass"
    assert result["sampling_window_qc_reason"] == ""


@pytest.mark.parametrize(
    ("policy", "expected_status"),
    [
        ("warn", "warn"),
        ("reject", "fail"),
    ],
)
def test_evaluate_co2_sampling_window_qc_warns_or_fails_for_drifting_window(
    tmp_path: Path,
    policy: str,
    expected_status: str,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_sampling_window_qc_enabled": True,
                    "co2_sampling_window_qc_max_range_c": 0.20,
                    "co2_sampling_window_qc_max_rise_c": 0.12,
                    "co2_sampling_window_qc_max_abs_slope_c_per_s": 0.02,
                    "co2_sampling_window_qc_policy": policy,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()

    result = runner._evaluate_co2_sampling_window_qc(
        point,
        phase="co2",
        samples=_sampling_rows(
            [-24.50, -24.44, -24.38, -24.31, -24.25, -24.18, -24.11, -24.05, -23.99, -23.90]
        ),
    )
    logger.close()

    assert result["sampling_window_dewpoint_first_c"] == -24.5
    assert result["sampling_window_dewpoint_last_c"] == -23.9
    assert result["sampling_window_dewpoint_range_c"] == 0.6
    assert result["sampling_window_dewpoint_rise_c"] == 0.6
    assert pytest.approx(result["sampling_window_dewpoint_slope_c_per_s"], abs=1e-6) == 0.6 / 9.0
    assert result["sampling_window_qc_status"] == expected_status
    assert "range_c=0.600>max_range_c=0.200" in result["sampling_window_qc_reason"]
    assert "rise_c=0.600>max_rise_c=0.120" in result["sampling_window_qc_reason"]
    assert "abs_slope_c_per_s=0.0667>max_abs_slope_c_per_s=0.0200" in result["sampling_window_qc_reason"]
    assert f"policy={policy}" in result["sampling_window_qc_reason"]
