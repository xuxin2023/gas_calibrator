from datetime import datetime

from gas_calibrator.v2.domain.sample_models import RawSample, SampleWindow


def test_raw_sample_defaults() -> None:
    timestamp = datetime(2026, 3, 18, 10, 0, 0)
    sample = RawSample(timestamp=timestamp, point_index=1, analyzer_name="ga01")

    assert sample.co2 is None
    assert sample.extra == {}


def test_sample_window_collects_samples() -> None:
    started_at = datetime(2026, 3, 18, 10, 0, 0)
    window = SampleWindow(point_index=1, started_at=started_at)
    sample = RawSample(timestamp=started_at, point_index=1, analyzer_name="ga01", co2=400.0)
    window.samples.append(sample)

    assert len(window.samples) == 1
    assert window.samples[0].co2 == 400.0
