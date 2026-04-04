import csv
import types
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger, _field_label
from gas_calibrator.workflow.runner import CalibrationRunner


def _point() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=400.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def test_sample_and_log_writes_non_analyzer_device_means(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 2,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    rows = [
        {
            "co2_ppm": 400.0,
            "pressure_hpa": 1000.1,
            "pressure_gauge_hpa": 1000.0,
            "dewpoint_c": 1.0,
            "dew_temp_c": 20.0,
            "dew_rh_pct": 29.0,
            "chamber_temp_c": 20.0,
            "chamber_rh_pct": 98.0,
            "hgen_Td": 0.8,
            "hgen_Tc": 20.1,
            "hgen_Uw": 30.0,
            "point_row": 1,
        },
        {
            "co2_ppm": 402.0,
            "pressure_hpa": 1000.3,
            "pressure_gauge_hpa": 1000.2,
            "dewpoint_c": 1.2,
            "dew_temp_c": 20.2,
            "dew_rh_pct": 31.0,
            "chamber_temp_c": 20.4,
            "chamber_rh_pct": 99.0,
            "hgen_Td": 1.0,
            "hgen_Tc": 20.3,
            "hgen_Uw": 32.0,
            "point_row": 1,
        },
    ]
    runner._collect_samples = types.MethodType(lambda self, *_args, **_kwargs: list(rows), runner)

    runner._sample_and_log(_point())
    logger.close()

    with logger.points_path.open("r", encoding="utf-8", newline="") as f:
        rr = list(csv.DictReader(f))
    assert len(rr) == 1
    row = rr[0]

    assert float(row[_field_label("pressure_gauge_hpa_mean")]) == pytest.approx(1000.1)
    assert float(row[_field_label("dewpoint_c_mean")]) == pytest.approx(1.1)
    assert float(row[_field_label("dew_temp_c_mean")]) == pytest.approx(20.1)
    assert float(row[_field_label("dew_rh_pct_mean")]) == pytest.approx(30.0)
    assert float(row[_field_label("chamber_rh_pct_mean")]) == pytest.approx(98.5)
    assert float(row[_field_label("hgen_Td_mean")]) == pytest.approx(0.9)
    assert float(row[_field_label("hgen_Tc_mean")]) == pytest.approx(20.2)
    assert float(row[_field_label("hgen_Uw_mean")]) == pytest.approx(31.0)
