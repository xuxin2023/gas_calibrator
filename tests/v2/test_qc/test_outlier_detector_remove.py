from datetime import datetime, timedelta

from gas_calibrator.v2.config import QCConfig
from gas_calibrator.v2.domain.sample_models import RawSample
from gas_calibrator.v2.qc.outlier_detector import OutlierDetector


def _samples(values: list[float]) -> list[RawSample]:
    base = datetime(2026, 3, 18, 9, 0, 0)
    return [
        RawSample(
            timestamp=base + timedelta(seconds=index),
            point_index=1,
            analyzer_name="ga01",
            co2=value,
            h2o=10.0,
        )
        for index, value in enumerate(values)
    ]


def test_detect_and_remove_returns_cleaned_copy() -> None:
    detector = OutlierDetector(QCConfig())
    samples = _samples([10.0, 10.1, 30.0, 9.9, 10.2])

    cleaned, result = detector.detect_and_remove(samples, field="co2", z_thresh=1.5)

    assert len(samples) == 5
    assert len(cleaned) == 4
    assert result.outlier_count == 1
    assert result.outlier_indices == {2}
    assert samples is not cleaned


def test_detect_and_remove_keeps_samples_when_variance_zero() -> None:
    detector = OutlierDetector(QCConfig())
    samples = _samples([10.0, 10.0, 10.0, 10.0])

    cleaned, result = detector.detect_and_remove(samples, field="co2")

    assert len(cleaned) == 4
    assert result.outlier_count == 0
    assert "No variance" in result.message
