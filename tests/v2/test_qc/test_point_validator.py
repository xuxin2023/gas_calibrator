from datetime import datetime, timedelta, timezone

from gas_calibrator.v2.config import QCConfig
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.qc.outlier_detector import OutlierResult
from gas_calibrator.v2.qc.point_validator import PointValidator
from gas_calibrator.v2.qc.sample_checker import SampleQCResult


def _samples(count: int) -> list[SamplingResult]:
    point = CalibrationPoint(index=7, temperature_c=25.0, route="co2")
    base = datetime(2026, 3, 18, 9, 0, 0, tzinfo=timezone.utc)
    return [
        SamplingResult(point=point, analyzer_id="ga01", timestamp=base + timedelta(seconds=index), co2_signal=100.0 + index)
        for index in range(count)
    ]


def test_point_validator_accepts_clean_point() -> None:
    validator = PointValidator(QCConfig(min_sample_count=3))
    result = validator.validate(
        CalibrationPoint(index=7, temperature_c=25.0, route="co2"),
        _samples(4),
        SampleQCResult(True, 4, 3, 0, False, True, 0.95, []),
        OutlierResult(),
    )

    assert result.valid is True
    assert result.recommendation == "use"


def test_point_validator_rejects_high_outlier_ratio() -> None:
    validator = PointValidator(QCConfig(min_sample_count=3, max_outlier_ratio=0.2))
    result = validator.validate(
        CalibrationPoint(index=7, temperature_c=25.0, route="co2"),
        _samples(5),
        SampleQCResult(True, 5, 3, 0, False, True, 0.9, []),
        OutlierResult(outlier_indices={0, 1}, reasons={0: "spike", 1: "step"}),
    )

    assert result.valid is False
    assert result.recommendation in {"review", "exclude"}
