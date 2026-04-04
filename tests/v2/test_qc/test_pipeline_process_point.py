from datetime import datetime, timedelta

from gas_calibrator.v2.config import QCConfig
from gas_calibrator.v2.domain.point_models import CalibrationPoint
from gas_calibrator.v2.domain.sample_models import RawSample
from gas_calibrator.v2.qc.pipeline import QCPipeline


def _samples(values: list[float]) -> list[RawSample]:
    base = datetime(2026, 3, 18, 9, 0, 0)
    return [
        RawSample(
            timestamp=base + timedelta(seconds=index),
            point_index=3,
            analyzer_name="ga01",
            co2=value,
            h2o=5.0 + index * 0.01,
        )
        for index, value in enumerate(values)
    ]


def test_process_point_returns_cleaned_samples_for_algorithm() -> None:
    pipeline = QCPipeline(QCConfig(min_sample_count=3, spike_threshold=1.5), run_id="run_clean")
    point = CalibrationPoint(index=3, name="P3", target_co2=10.0)

    cleaned, validation, quality_score = pipeline.process_point(
        point,
        _samples([10.0, 10.1, 30.0, 9.9, 10.2]),
        point_index=3,
        return_cleaned=True,
    )

    assert len(cleaned) == 4
    assert validation.point_index == 3
    assert 0.0 <= quality_score <= 1.0
    assert pipeline.last_cleaned[3].cleaned_count == 4
