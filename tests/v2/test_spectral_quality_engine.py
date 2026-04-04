from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import numpy as np

from gas_calibrator.v2.domain.services.spectral_quality_engine import (
    SpectralQualityEngine,
    build_run_spectral_quality_summary,
    build_sample_timeseries_channels,
)


def test_spectral_quality_engine_detects_valid_periodic_series() -> None:
    engine = SpectralQualityEngine(min_samples=64, min_duration_s=30.0, low_freq_max_hz=0.05)
    timestamps = [datetime(2026, 3, 28, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=index) for index in range(96)]
    values = [float(np.sin(2.0 * np.pi * 0.125 * index)) for index in range(96)]

    result = engine.analyze_series(
        {
            "timestamps": timestamps,
            "values": values,
            "channel_name": "GA01.co2_signal",
        }
    )

    assert result["status"] == "ok"
    assert result["sample_count"] == 96
    assert abs(float(result["dominant_frequency_hz"])) > 0.1
    assert "constant_series" not in list(result["anomaly_flags"] or [])


def test_spectral_quality_engine_treats_constant_series_as_stable() -> None:
    engine = SpectralQualityEngine(min_samples=32, min_duration_s=20.0, low_freq_max_hz=0.05)

    result = engine.analyze_series(
        {
            "timestamps": [datetime(2026, 3, 28, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=index) for index in range(48)],
            "values": [10.0] * 48,
            "channel_name": "GA01.temperature_c",
        }
    )

    assert result["status"] == "ok"
    assert result["stability_score"] == 1.0
    assert result["low_freq_energy_ratio"] == 1.0
    assert "constant_series" in list(result["anomaly_flags"] or [])


def test_spectral_quality_engine_returns_insufficient_for_short_series() -> None:
    engine = SpectralQualityEngine(min_samples=32, min_duration_s=30.0)

    result = engine.analyze_series(
        {
            "timestamps": [datetime(2026, 3, 28, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=index) for index in range(12)],
            "values": list(range(12)),
            "channel_name": "GA01.pressure_hpa",
        }
    )

    assert result["status"] == "insufficient_data"
    assert "low_sample_count" in list(result["anomaly_flags"] or [])


def test_spectral_quality_engine_soft_fails_invalid_timestamps() -> None:
    engine = SpectralQualityEngine(min_samples=8, min_duration_s=2.0)

    result = engine.analyze_series(
        {
            "timestamps": [
                "2026-03-28T12:00:05+00:00",
                "2026-03-28T12:00:04+00:00",
                "2026-03-28T12:00:03+00:00",
                "2026-03-28T12:00:02+00:00",
                "2026-03-28T12:00:01+00:00",
                "2026-03-28T12:00:00+00:00",
                "2026-03-28T11:59:59+00:00",
                "2026-03-28T11:59:58+00:00",
            ],
            "values": list(range(8)),
            "channel_name": "GA01.co2_ppm",
        }
    )

    assert result["status"] == "invalid_series"
    assert "invalid_timestamps" in list(result["anomaly_flags"] or [])


def test_spectral_quality_engine_drops_missing_values_without_failing() -> None:
    engine = SpectralQualityEngine(min_samples=16, min_duration_s=10.0)
    timestamps = [datetime(2026, 3, 28, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=index) for index in range(24)]
    values = [float(index) if index not in {3, 7, 11} else None for index in range(24)]

    result = engine.analyze_series(
        {
            "timestamps": timestamps,
            "values": values,
            "channel_name": "GA01.pressure_gauge_hpa",
        }
    )

    assert result["status"] == "ok"
    assert "missing_values_dropped" in list(result["anomaly_flags"] or [])
    assert result["sample_count"] == 21


def test_run_spectral_quality_summary_captures_multiple_channels() -> None:
    samples = []
    base = datetime(2026, 3, 28, 12, 0, 0, tzinfo=timezone.utc)
    for index in range(80):
        samples.append(
            SimpleNamespace(
                analyzer_id="GA01",
                timestamp=base + timedelta(seconds=index),
                co2_signal=float(np.sin(2.0 * np.pi * 0.1 * index)),
                temperature_c=25.0,
                pressure_hpa=1000.0 + (0.1 * index),
            )
        )

    channels = build_sample_timeseries_channels(samples)
    summary = build_run_spectral_quality_summary(
        run_id="run_test",
        samples=samples,
        simulation_mode=True,
        min_samples=32,
        min_duration_s=20.0,
        low_freq_max_hz=0.05,
    )

    assert any(str(item.get("channel_name") or "") == "GA01.co2_signal" for item in channels)
    assert summary["artifact_type"] == "spectral_quality_summary"
    assert summary["channel_count"] >= 3
    assert summary["status"] == "ok"
    assert summary["not_real_acceptance_evidence"] is True
    assert "GA01.co2_signal" in dict(summary.get("channels") or {})
