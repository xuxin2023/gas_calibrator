from __future__ import annotations

from collections import Counter
from datetime import datetime
from math import isfinite
from typing import Any

import numpy as np

DEFAULT_SPECTRAL_CHANNEL_FIELDS: tuple[str, ...] = (
    "co2_signal",
    "co2_ppm",
    "h2o_signal",
    "h2o_mmol",
    "ref_signal",
    "temperature_c",
    "pressure_hpa",
    "thermometer_temp_c",
    "pressure_gauge_hpa",
    "analyzer_chamber_temp_c",
    "case_temp_c",
)
_EPSILON = 1e-12


class SpectralQualityEngine:
    def __init__(
        self,
        *,
        min_samples: int = 64,
        min_duration_s: float = 30.0,
        low_freq_max_hz: float = 0.01,
        detrend: bool = True,
    ) -> None:
        self.min_samples = max(4, int(min_samples))
        self.min_duration_s = max(0.0, float(min_duration_s))
        self.low_freq_max_hz = max(0.0, float(low_freq_max_hz))
        self.detrend = bool(detrend)

    def analyze_series(self, series_payload: dict[str, Any]) -> dict[str, Any]:
        channel_name = str(series_payload.get("channel_name") or "").strip()
        metadata = dict(series_payload.get("metadata", {}) or {})
        raw_values = list(series_payload.get("values") or [])
        raw_timestamps = list(series_payload.get("timestamps") or [])
        raw_sample_count = len(raw_values)
        diagnostics: dict[str, Any] = {
            "raw_sample_count": raw_sample_count,
            "dropped_sample_count": 0,
            "timestamp_source": "",
            "sample_interval_s": None,
            "sampling_frequency_hz": None,
            "welch_segment_size": 0,
            "welch_window_count": 0,
            "detrended": bool(self.detrend),
        }
        anomaly_flags: list[str] = []
        timestamps, values, dropped_count = _clean_series(raw_timestamps, raw_values)
        diagnostics["dropped_sample_count"] = dropped_count
        if dropped_count > 0:
            anomaly_flags.append("missing_values_dropped")
        sample_interval_s = _coerce_positive_float(series_payload.get("sample_interval_s"))
        if sample_interval_s is not None:
            diagnostics["timestamp_source"] = "sample_interval"
        elif timestamps:
            parsed_times = [_coerce_timestamp_seconds(item) for item in timestamps]
            if any(item is None for item in parsed_times):
                return _result_payload(
                    status="invalid_series",
                    channel_name=channel_name,
                    metadata=metadata,
                    sample_count=len(values),
                    duration_s=0.0,
                    low_freq_energy_ratio=None,
                    dominant_frequency_hz=None,
                    stability_score=None,
                    anomaly_flags=anomaly_flags + ["invalid_timestamps"],
                    diagnostics={**diagnostics, "timestamp_source": "timestamps", "error": "unparseable_timestamps"},
                )
            relative_times = np.asarray(parsed_times, dtype=float)
            relative_times = relative_times - float(relative_times[0])
            deltas = np.diff(relative_times)
            if len(deltas) == 0 or np.any(deltas <= 0.0):
                return _result_payload(
                    status="invalid_series",
                    channel_name=channel_name,
                    metadata=metadata,
                    sample_count=len(values),
                    duration_s=0.0,
                    low_freq_energy_ratio=None,
                    dominant_frequency_hz=None,
                    stability_score=None,
                    anomaly_flags=anomaly_flags + ["invalid_timestamps"],
                    diagnostics={**diagnostics, "timestamp_source": "timestamps", "error": "non_monotonic_timestamps"},
                )
            sample_interval_s = float(np.median(deltas))
            diagnostics["timestamp_source"] = "timestamps"
            if sample_interval_s <= 0.0:
                return _result_payload(
                    status="invalid_series",
                    channel_name=channel_name,
                    metadata=metadata,
                    sample_count=len(values),
                    duration_s=0.0,
                    low_freq_energy_ratio=None,
                    dominant_frequency_hz=None,
                    stability_score=None,
                    anomaly_flags=anomaly_flags + ["invalid_timestamps"],
                    diagnostics={**diagnostics, "timestamp_source": "timestamps", "error": "non_positive_interval"},
                )
            if len(deltas) > 1:
                jitter = float(np.std(deltas) / max(np.mean(deltas), _EPSILON))
                diagnostics["interval_jitter_ratio"] = round(jitter, 6)
                if jitter > 0.2:
                    anomaly_flags.append("irregular_sampling")
        else:
            return _result_payload(
                status="invalid_series",
                channel_name=channel_name,
                metadata=metadata,
                sample_count=len(values),
                duration_s=0.0,
                low_freq_energy_ratio=None,
                dominant_frequency_hz=None,
                stability_score=None,
                anomaly_flags=anomaly_flags + ["invalid_timestamps"],
                diagnostics={**diagnostics, "error": "missing_timestamps"},
            )

        diagnostics["sample_interval_s"] = sample_interval_s
        diagnostics["sampling_frequency_hz"] = round(1.0 / max(sample_interval_s, _EPSILON), 6)
        duration_s = float(max(0.0, (len(values) - 1) * sample_interval_s))
        if len(values) < self.min_samples or duration_s < self.min_duration_s:
            short_flags = list(anomaly_flags)
            if len(values) < self.min_samples:
                short_flags.append("low_sample_count")
            if duration_s < self.min_duration_s:
                short_flags.append("insufficient_duration")
            return _result_payload(
                status="insufficient_data",
                channel_name=channel_name,
                metadata=metadata,
                sample_count=len(values),
                duration_s=duration_s,
                low_freq_energy_ratio=None,
                dominant_frequency_hz=None,
                stability_score=None,
                anomaly_flags=short_flags,
                diagnostics=diagnostics,
            )

        series = np.asarray(values, dtype=float)
        centered = series - float(np.mean(series))
        processed = _linear_detrend(centered) if self.detrend and len(series) >= 3 else centered
        freqs, power, segment_size, window_count = _welch_psd(processed, sampling_frequency_hz=1.0 / sample_interval_s)
        diagnostics["welch_segment_size"] = segment_size
        diagnostics["welch_window_count"] = window_count
        if freqs.size == 0 or power.size == 0:
            return _result_payload(
                status="insufficient_data",
                channel_name=channel_name,
                metadata=metadata,
                sample_count=len(values),
                duration_s=duration_s,
                low_freq_energy_ratio=None,
                dominant_frequency_hz=None,
                stability_score=None,
                anomaly_flags=anomaly_flags + ["insufficient_frequency_bins"],
                diagnostics=diagnostics,
            )

        spectral_power = power[1:] if power.size > 1 else power
        spectral_freqs = freqs[1:] if freqs.size > 1 else freqs
        total_power = float(np.sum(spectral_power))
        signal_std = float(np.std(processed))
        diff_std = float(np.std(np.diff(processed))) if len(processed) > 1 else 0.0
        if total_power <= _EPSILON or signal_std <= _EPSILON:
            anomaly_flags.append("constant_series")
            return _result_payload(
                status="ok",
                channel_name=channel_name,
                metadata=metadata,
                sample_count=len(values),
                duration_s=duration_s,
                low_freq_energy_ratio=1.0,
                dominant_frequency_hz=0.0,
                stability_score=1.0,
                anomaly_flags=anomaly_flags,
                diagnostics=diagnostics,
            )

        low_freq_mask = spectral_freqs <= self.low_freq_max_hz if spectral_freqs.size else np.asarray([], dtype=bool)
        low_freq_power = float(np.sum(spectral_power[low_freq_mask])) if low_freq_mask.size else 0.0
        dominant_index = int(np.argmax(spectral_power)) if spectral_power.size else 0
        dominant_frequency_hz = float(spectral_freqs[dominant_index]) if spectral_freqs.size else 0.0
        low_freq_energy_ratio = float(low_freq_power / max(total_power, _EPSILON))
        stability_score = _stability_score(signal_std=signal_std, diff_std=diff_std, low_freq_energy_ratio=low_freq_energy_ratio)
        if low_freq_energy_ratio >= 0.65:
            anomaly_flags.append("low_frequency_drift")
        elif dominant_frequency_hz > max(self.low_freq_max_hz * 4.0, 0.05) and stability_score < 0.55:
            anomaly_flags.append("oscillatory_peak")

        return _result_payload(
            status="ok",
            channel_name=channel_name,
            metadata=metadata,
            sample_count=len(values),
            duration_s=duration_s,
            low_freq_energy_ratio=round(low_freq_energy_ratio, 6),
            dominant_frequency_hz=round(dominant_frequency_hz, 6),
            stability_score=round(stability_score, 6),
            anomaly_flags=anomaly_flags,
            diagnostics=diagnostics,
        )


def build_sample_timeseries_channels(
    samples: list[Any],
    *,
    channel_fields: tuple[str, ...] = DEFAULT_SPECTRAL_CHANNEL_FIELDS,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for sample in list(samples or []):
        analyzer_id = str(_read_value(sample, "analyzer_id") or "").strip().upper() or "RUN"
        timestamp = _read_value(sample, "timestamp")
        for field_name in channel_fields:
            value = _read_value(sample, field_name)
            channel_name = f"{analyzer_id}.{field_name}"
            bucket = grouped.setdefault(
                channel_name,
                {
                    "channel_name": channel_name,
                    "timestamps": [],
                    "values": [],
                    "metadata": {
                        "analyzer_id": analyzer_id,
                        "field_name": field_name,
                    },
                },
            )
            bucket["timestamps"].append(timestamp)
            bucket["values"].append(value)
    return [dict(payload) for payload in grouped.values()]


def build_run_spectral_quality_summary(
    *,
    run_id: str,
    samples: list[Any],
    simulation_mode: bool,
    min_samples: int = 64,
    min_duration_s: float = 30.0,
    low_freq_max_hz: float = 0.01,
) -> dict[str, Any]:
    engine = SpectralQualityEngine(
        min_samples=min_samples,
        min_duration_s=min_duration_s,
        low_freq_max_hz=low_freq_max_hz,
    )
    channel_payloads = build_sample_timeseries_channels(samples)
    analyses: dict[str, dict[str, Any]] = {}
    status_counts: Counter[str] = Counter()
    flags: set[str] = set()
    scores: list[float] = []
    for payload in channel_payloads:
        analysis = engine.analyze_series(payload)
        channel_name = str(analysis.get("channel_name") or payload.get("channel_name") or "")
        analyses[channel_name] = analysis
        status_counts[str(analysis.get("status") or "invalid_series")] += 1
        flags.update(str(item) for item in list(analysis.get("anomaly_flags") or []) if str(item).strip())
        score = analysis.get("stability_score")
        if isinstance(score, (int, float)):
            scores.append(float(score))
    if not analyses:
        overall_status = "insufficient_data"
    elif status_counts.get("ok", 0) > 0:
        overall_status = "ok"
    elif status_counts.get("invalid_series", 0) > 0 and status_counts.get("insufficient_data", 0) == 0:
        overall_status = "invalid_series"
    else:
        overall_status = "insufficient_data"
    overall_score = round(sum(scores) / len(scores), 6) if scores else None
    return {
        "artifact_type": "spectral_quality_summary",
        "schema_version": "1.0",
        "run_id": str(run_id or ""),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": overall_status,
        "evidence_source": "simulated_protocol" if simulation_mode else "diagnostic",
        "evidence_state": "collected",
        "not_real_acceptance_evidence": True,
        "channel_count": len(analyses),
        "ok_channel_count": int(status_counts.get("ok", 0)),
        "overall_score": overall_score,
        "flags": sorted(flags),
        "status_counts": dict(status_counts),
        "config": {
            "min_samples": int(min_samples),
            "min_duration_s": float(min_duration_s),
            "low_freq_max_hz": float(low_freq_max_hz),
        },
        "channels": analyses,
    }


def _clean_series(timestamps: list[Any], values: list[Any]) -> tuple[list[Any], list[float], int]:
    cleaned_timestamps: list[Any] = []
    cleaned_values: list[float] = []
    dropped_count = 0
    if timestamps and len(timestamps) != len(values):
        length = min(len(timestamps), len(values))
        timestamps = timestamps[:length]
        values = values[:length]
        dropped_count += abs(len(timestamps) - len(values))
    for index, raw_value in enumerate(values):
        numeric_value = _coerce_finite_float(raw_value)
        if numeric_value is None:
            dropped_count += 1
            continue
        cleaned_values.append(numeric_value)
        if timestamps:
            cleaned_timestamps.append(timestamps[index] if index < len(timestamps) else None)
    return cleaned_timestamps, cleaned_values, dropped_count


def _result_payload(
    *,
    status: str,
    channel_name: str,
    metadata: dict[str, Any],
    sample_count: int,
    duration_s: float,
    low_freq_energy_ratio: float | None,
    dominant_frequency_hz: float | None,
    stability_score: float | None,
    anomaly_flags: list[str],
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    unique_flags: list[str] = []
    for flag in anomaly_flags:
        text = str(flag or "").strip()
        if text and text not in unique_flags:
            unique_flags.append(text)
    return {
        "status": str(status or "invalid_series"),
        "channel_name": str(channel_name or ""),
        "sample_count": int(sample_count),
        "duration_s": round(float(duration_s or 0.0), 6),
        "low_freq_energy_ratio": low_freq_energy_ratio,
        "dominant_frequency_hz": dominant_frequency_hz,
        "stability_score": stability_score,
        "anomaly_flags": unique_flags,
        "diagnostics": dict(diagnostics or {}),
        "metadata": dict(metadata or {}),
    }


def _read_value(sample: Any, field_name: str) -> Any:
    if isinstance(sample, dict):
        return sample.get(field_name)
    return getattr(sample, field_name, None)


def _coerce_timestamp_seconds(value: Any) -> float | None:
    if isinstance(value, datetime):
        return float(value.timestamp())
    if isinstance(value, (int, float)) and isfinite(float(value)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def _coerce_positive_float(value: Any) -> float | None:
    numeric = _coerce_finite_float(value)
    if numeric is None or numeric <= 0.0:
        return None
    return numeric


def _coerce_finite_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if isfinite(numeric) else None


def _linear_detrend(values: np.ndarray) -> np.ndarray:
    positions = np.arange(values.size, dtype=float)
    slope, intercept = np.polyfit(positions, values, 1)
    return values - (slope * positions + intercept)


def _welch_psd(values: np.ndarray, *, sampling_frequency_hz: float) -> tuple[np.ndarray, np.ndarray, int, int]:
    sample_count = int(values.size)
    if sample_count < 8:
        return np.asarray([]), np.asarray([]), 0, 0
    segment_size = min(256, sample_count)
    segment_size = max(16, 2 ** int(np.floor(np.log2(segment_size))))
    segment_size = min(segment_size, sample_count)
    if segment_size < 8:
        return np.asarray([]), np.asarray([]), 0, 0
    overlap = segment_size // 2
    step = max(1, segment_size - overlap)
    window = np.hanning(segment_size)
    scale = float(sampling_frequency_hz * np.sum(window ** 2))
    psd_sum: np.ndarray | None = None
    window_count = 0
    for start in range(0, sample_count - segment_size + 1, step):
        segment = np.asarray(values[start : start + segment_size], dtype=float)
        segment = segment - float(np.mean(segment))
        fft = np.fft.rfft(segment * window)
        spectrum = (np.abs(fft) ** 2) / max(scale, _EPSILON)
        if segment_size > 2:
            spectrum[1:-1] *= 2.0
        psd_sum = spectrum if psd_sum is None else psd_sum + spectrum
        window_count += 1
    if psd_sum is None or window_count <= 0:
        return np.asarray([]), np.asarray([]), segment_size, 0
    frequencies = np.fft.rfftfreq(segment_size, d=1.0 / sampling_frequency_hz)
    return frequencies, psd_sum / float(window_count), segment_size, window_count


def _stability_score(*, signal_std: float, diff_std: float, low_freq_energy_ratio: float) -> float:
    if signal_std <= _EPSILON and diff_std <= _EPSILON:
        return 1.0
    smoothness = 1.0 - min(1.0, diff_std / max(signal_std + diff_std, _EPSILON))
    drift_penalty = min(0.35, max(0.0, low_freq_energy_ratio - 0.75))
    return max(0.0, min(1.0, smoothness - drift_penalty))
