from datetime import datetime, timedelta

from gas_calibrator.v2.config import QCConfig
from gas_calibrator.v2.domain.point_models import CalibrationPoint
from gas_calibrator.v2.domain.sample_models import RawSample
from gas_calibrator.v2.qc.pipeline import QCPipeline


def _samples(point_index: int, values: list[float]) -> list[RawSample]:
    base = datetime(2026, 3, 18, 9, 0, 0)
    return [
        RawSample(
            timestamp=base + timedelta(seconds=index),
            point_index=point_index,
            analyzer_name="ga01",
            co2=value,
            h2o=4.0 + index * 0.02,
        )
        for index, value in enumerate(values)
    ]


def test_process_run_returns_cleaned_mapping_and_scores() -> None:
    pipeline = QCPipeline(QCConfig(min_sample_count=3, spike_threshold=1.5), run_id="run_map")
    points = [
        CalibrationPoint(index=1, name="P1", target_co2=10.0),
        CalibrationPoint(index=2, name="P2", target_co2=20.0),
    ]
    all_samples = {
        1: _samples(1, [10.0, 10.1, 9.9, 10.2]),
        2: _samples(2, [20.0, 20.1, 45.0, 19.9, 20.2]),
    }

    cleaned_all, validations, run_score = pipeline.process_run(
        points=points,
        all_samples=all_samples,
        return_cleaned=True,
    )

    assert set(cleaned_all) == {1, 2}
    assert len(cleaned_all[1]) == 4
    assert len(cleaned_all[2]) == 4
    assert len(validations) == 2
    assert 0.0 <= run_score.overall_score <= 1.0
