from datetime import datetime, timedelta, timezone

from gas_calibrator.v2.config import QCConfig
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.qc.sample_checker import SampleChecker


def _sample(index: int, seconds: int, *, co2: float | None = 400.0, h2o: float | None = 10.0) -> SamplingResult:
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")
    return SamplingResult(
        point=point,
        analyzer_id=f"ga{index}",
        timestamp=datetime(2026, 3, 18, 9, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds),
        co2_signal=co2,
        h2o_signal=h2o,
    )


def test_sample_checker_passes_good_samples() -> None:
    checker = SampleChecker(QCConfig(min_sample_count=3))
    result = checker.check([_sample(1, 0), _sample(2, 1), _sample(3, 2)])

    assert result.passed is True
    assert result.sample_count == 3
    assert result.score > 0.9


def test_sample_checker_flags_missing_and_comm_error() -> None:
    checker = SampleChecker(QCConfig(min_sample_count=4))
    result = checker.check([_sample(1, 0), _sample(2, 5, co2=None, h2o=None)])

    assert result.passed is False
    assert result.missing_count == 2
    assert result.has_comm_error is True
    assert "communication_error" in result.issues
