from datetime import datetime, timedelta, timezone

from gas_calibrator.v2.config import QCConfig
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.qc.outlier_detector import OutlierDetector


def _samples(values: list[float]) -> list[SamplingResult]:
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")
    base = datetime(2026, 3, 18, 9, 0, 0, tzinfo=timezone.utc)
    return [
        SamplingResult(point=point, analyzer_id="ga01", timestamp=base + timedelta(seconds=index), co2_signal=value)
        for index, value in enumerate(values)
    ]


def test_outlier_detector_detects_spike() -> None:
    detector = OutlierDetector(QCConfig(spike_threshold=2.0))
    result = detector.detect(_samples([100.0, 101.0, 180.0, 102.0, 103.0]))

    assert 2 in result.outlier_indices
    assert 2 in result.spike_indices


def test_outlier_detector_marks_outliers() -> None:
    detector = OutlierDetector(QCConfig())
    samples = _samples([100.0, 101.0, 180.0, 102.0])
    marked = detector.mark_outliers(samples, {2})

    assert getattr(marked[2], "qc_outlier") is True
