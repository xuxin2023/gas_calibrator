from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


def test_subzero_point_forces_gas_path(tmp_path: Path) -> None:
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=-10.0,
        co2_ppm=400.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=50.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=0.0,
        h2o_mmol=6.0,
        raw_h2o="demo",
    )
    logger = RunLogger(tmp_path)
    calls = {"h2o": 0, "co2": 0}
    try:
        runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
        runner._set_temperature = lambda *_: None  # type: ignore[method-assign]
        runner._run_h2o_point = lambda *_: calls.__setitem__("h2o", calls["h2o"] + 1)  # type: ignore[method-assign]
        runner._run_co2_point = lambda *_: calls.__setitem__("co2", calls["co2"] + 1)  # type: ignore[method-assign]
        runner._run_point(point)
    finally:
        logger.close()

    assert calls["h2o"] == 0
    assert calls["co2"] == 1
