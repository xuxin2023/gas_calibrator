from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _PressureReader:
    def __init__(self, values):
        self.values = list(values)
        self.calls = 0

    def read_pressure(self):
        self.calls += 1
        if self.values:
            return float(self.values.pop(0))
        return 1000.0


def _co2_point() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=400.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=800.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def test_pressure_stable_starts_sampling_immediately_when_capture_then_hold_disabled(tmp_path: Path) -> None:
    logs = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": False, "co2_post_stable_sample_delay_s": 0.0}}},
        {},
        logger,
        logs.append,
        lambda *_: None,
    )

    def should_not_run(_point):
        raise AssertionError("output-off hold should never run after branch removal")

    runner._observe_pressure_hold_after_output_off = should_not_run

    assert runner._wait_after_pressure_stable_before_sampling(_co2_point()) is True
    logger.close()
    assert any("start sampling immediately" in msg.lower() for msg in logs)

def test_pressure_stable_ignores_capture_then_hold_config_and_samples_immediately(tmp_path: Path) -> None:
    logs = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "capture_then_hold_enabled": True,
                    "disable_output_during_sampling": True,
                    "co2_output_off_hold_s": 0.0,
                    "output_off_retry_count": 1,
                }
            }
        },
        {},
        logger,
        logs.append,
        lambda *_: None,
    )
    disable_reasons = []
    recaptures = {"count": 0}
    runner._disable_pressure_controller_output = lambda reason="": disable_reasons.append(reason)

    def should_not_run(_point):
        raise AssertionError("capture_then_hold helper should be ignored even when config=true")

    runner._observe_pressure_hold_after_output_off = should_not_run
    runner._set_pressure_to_target = lambda point: recaptures.__setitem__("count", recaptures["count"] + 1) or True

    assert runner._wait_after_pressure_stable_before_sampling(_co2_point()) is True
    logger.close()

    assert disable_reasons == []
    assert recaptures["count"] == 0
    assert any("retired in v1 and will be ignored" in msg.lower() for msg in logs)
    assert any("start sampling immediately" in msg.lower() for msg in logs)

def test_pressure_stable_logs_capture_then_hold_ignored_once(tmp_path: Path) -> None:
    logs = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": True}}},
        {},
        logger,
        logs.append,
        lambda *_: None,
    )

    runner._observe_pressure_hold_after_output_off = lambda _point: (_ for _ in ()).throw(
        AssertionError("capture_then_hold helper should not be called")
    )

    assert runner._wait_after_pressure_stable_before_sampling(_co2_point()) is True
    assert runner._wait_after_pressure_stable_before_sampling(_co2_point()) is True
    logger.close()

    ignored_logs = [msg for msg in logs if "retired in v1 and will be ignored" in str(msg).lower()]
    assert len(ignored_logs) == 1


def test_output_off_hold_falls_back_to_pace_when_gauge_missing(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _PressureReader([805.5])
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": True}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    value, source = runner._read_best_pressure_for_output_off_hold(True)
    logger.close()

    assert value == 805.5
    assert source == "pace"
    assert pace.calls == 1


def test_pressure_point_order_remains_high_to_low_with_feature_enabled(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": True}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    points = [
        CalibrationPoint(index=1, temp_chamber_c=20.0, co2_ppm=200.0, hgen_temp_c=None, hgen_rh_pct=None, target_pressure_hpa=500.0, dewpoint_c=None, h2o_mmol=None, raw_h2o=None),
        CalibrationPoint(index=2, temp_chamber_c=20.0, co2_ppm=200.0, hgen_temp_c=None, hgen_rh_pct=None, target_pressure_hpa=1100.0, dewpoint_c=None, h2o_mmol=None, raw_h2o=None),
        CalibrationPoint(index=3, temp_chamber_c=20.0, co2_ppm=200.0, hgen_temp_c=None, hgen_rh_pct=None, target_pressure_hpa=800.0, dewpoint_c=None, h2o_mmol=None, raw_h2o=None),
    ]

    ordered = runner._co2_pressure_points_for_temperature(points)
    logger.close()

    assert [int(point.target_pressure_hpa or 0) for point in ordered] == [1100, 800, 500]
