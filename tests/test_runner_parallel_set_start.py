from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


def _h2o_point() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=1.0,
        h2o_mmol=7.0,
        raw_h2o="demo",
    )


def _co2_point() -> CalibrationPoint:
    return CalibrationPoint(
        index=2,
        temp_chamber_c=20.0,
        co2_ppm=400.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def test_run_point_h2o_prepares_humidity_before_temperature(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    try:
        runner._run_h2o_point = lambda point, prepared=False: calls.append(f"run_h2o_prepared={prepared}")  # type: ignore[method-assign]
        runner._run_co2_point = lambda point: calls.append("run_co2")  # type: ignore[method-assign]
        runner._run_point(_h2o_point())
    finally:
        logger.close()

    assert calls == ["run_h2o_prepared=False", "run_co2"]


def test_run_point_co2_does_not_prepare_humidity(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    try:
        runner._run_h2o_point = lambda point, prepared=False: calls.append(f"run_h2o_prepared={prepared}")  # type: ignore[method-assign]
        runner._run_co2_point = lambda point: calls.append("run_co2")  # type: ignore[method-assign]
        runner._run_point(_co2_point())
    finally:
        logger.close()

    assert calls == ["run_co2"]
