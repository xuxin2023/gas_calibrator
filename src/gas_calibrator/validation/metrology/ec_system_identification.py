"""Empirical EC transfer-function identification for offline simulated fixtures."""


from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from gas_calibrator.utils.converters import finite_float as _finite_or_none


_EPSILON = 1e-12


def identify_empirical_transfer(
    series: Mapping[str, Any],
    *,
    target_frequencies_hz: Sequence[float],
    warmup_s: float = 10.0,
    segment_size: int = 512,
) -> dict[str, Any]:
    """Estimate reference-to-DUT and command-to-DUT H1 transfer functions.

    The upstream reference is the required input for the acceptance path.  The
    command-to-DUT result is retained only to prove that source dynamics are
    not silently attributed to the DUT.
    """

    metadata = dict(series.get("metadata") or {})
    protocol = dict(series.get("protocol") or {})
    synchronization = dict(series.get("synchronization") or {})
    base = {
        "artifact_type": "ec_dynamic_system_identification",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "ec_dynamic_system_identification_v1",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "analyzer_id": str(metadata.get("analyzer_id") or ""),
        "gas": str(metadata.get("gas") or "").lower(),
        "metadata": metadata,
        "protocol": protocol,
        "synchronization": synchronization,
        "method": "welch_h1_reference_to_dut",
        "status": "invalid",
        "flags": [],
    }
    timestamps = _finite_array(series.get("timestamps_s") or [])
    command = _finite_array(series.get("command_values") or [])
    reference = _finite_array(series.get("upstream_reference_values") or [])
    dut = _finite_array(series.get("dut_values") or [])
    if not len(reference):
        return {**base, "flags": ["upstream_reference_missing"]}
    if min(len(timestamps), len(command), len(reference), len(dut)) < 32:
        return {**base, "flags": ["insufficient_samples"]}
    if len({len(timestamps), len(command), len(reference), len(dut)}) != 1:
        return {**base, "flags": ["misaligned_series_lengths"]}
    deltas = np.diff(timestamps)
    if np.any(deltas <= 0.0):
        return {**base, "flags": ["non_monotonic_timestamps"]}
    median_interval = float(np.median(deltas))
    sample_rate_hz = 1.0 / max(median_interval, _EPSILON)
    jitter_ratio = float(np.std(deltas) / max(median_interval, _EPSILON))
    start_index = int(np.searchsorted(timestamps, float(warmup_s), side="left"))
    prbs_order = int(protocol.get("prbs_order") or 0)
    chip_rate_hz = _finite_or_none(protocol.get("chip_rate_hz"))
    post_warmup_duration_s = max(0.0, float(timestamps[-1]) - float(warmup_s))
    prbs_period_count = (
        None
        if prbs_order <= 0 or chip_rate_hz is None
        else post_warmup_duration_s * chip_rate_hz / float(2**prbs_order - 1)
    )
    if len(timestamps) - start_index < max(32, segment_size):
        return {
            **base,
            "flags": ["insufficient_post_warmup_samples"],
            "timing": _timing_payload(sample_rate_hz, jitter_ratio, median_interval, len(timestamps)),
        }

    command_view = command[start_index:]
    reference_view = reference[start_index:]
    dut_view = dut[start_index:]
    relative_spectrum = _welch_h1(
        reference_view,
        dut_view,
        sample_rate_hz=sample_rate_hz,
        segment_size=segment_size,
    )
    total_spectrum = _welch_h1(
        command_view,
        dut_view,
        sample_rate_hz=sample_rate_hz,
        segment_size=segment_size,
    )
    if relative_spectrum is None or total_spectrum is None:
        return {
            **base,
            "flags": ["spectral_estimation_failed"],
            "timing": _timing_payload(sample_rate_hz, jitter_ratio, median_interval, len(timestamps)),
        }

    truth_points = {
        round(float(item.get("frequency_hz")), 9): dict(item)
        for item in list(dict(series.get("synthetic_truth") or {}).get("relative_transfer_points") or [])
        if _finite_or_none(item.get("frequency_hz")) is not None
    }
    relative_points = _select_transfer_points(
        relative_spectrum,
        target_frequencies_hz=target_frequencies_hz,
        truth_points=truth_points,
    )
    total_points = _select_transfer_points(
        total_spectrum,
        target_frequencies_hz=target_frequencies_hz,
        truth_points={},
    )
    total_by_frequency = {
        round(float(item["requested_frequency_hz"]), 9): item
        for item in total_points
    }
    source_separation_rows: list[dict[str, Any]] = []
    for point in relative_points:
        key = round(float(point["requested_frequency_hz"]), 9)
        total_point = total_by_frequency.get(key, {})
        source_separation_rows.append(
            {
                "frequency_hz": point["requested_frequency_hz"],
                "reference_to_dut_amplitude_ratio": point.get("amplitude_ratio"),
                "command_to_dut_amplitude_ratio": total_point.get("amplitude_ratio"),
                "difference_ratio": _relative_difference(
                    total_point.get("amplitude_ratio"),
                    point.get("amplitude_ratio"),
                ),
            }
        )

    return {
        **base,
        "status": "ok" if relative_points else "incomplete",
        "flags": [] if relative_points else ["no_target_frequency_points"],
        "input_source": "upstream_reference",
        "source_transfer_separated": True,
        "time_alignment_verified": (
            synchronization.get("clock_domain") == "shared_simulated_sample_clock"
            and synchronization.get("reference_and_dut_time_aligned") is True
        ),
        "timing": _timing_payload(sample_rate_hz, jitter_ratio, median_interval, len(timestamps)),
        "warmup_s": float(warmup_s),
        "segment_size": int(relative_spectrum["segment_size"]),
        "window_count": int(relative_spectrum["window_count"]),
        "prbs_period_count_after_warmup": _round_or_none(prbs_period_count),
        "target_frequency_count": len(relative_points),
        "relative_transfer_points": relative_points,
        "command_to_dut_points": total_points,
        "source_separation": source_separation_rows,
    }


def build_system_identification_acceptance(
    analyses: Iterable[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
    protocol_id: str,
) -> dict[str, Any]:
    rows = [dict(item) for item in analyses]
    common = dict(contract.get("common_fixture_limits") or {})
    gas_limits = dict(contract.get("gas_fixture_limits") or {})
    required_path_metadata = [
        str(item)
        for item in list(contract.get("required_path_metadata") or [])
    ]
    gates: list[dict[str, Any]] = []
    channel_results: list[dict[str, Any]] = []
    for analysis in rows:
        gas = str(analysis.get("gas") or "").lower()
        analyzer_id = str(analysis.get("analyzer_id") or "")
        limits = {**common, **dict(gas_limits.get(gas) or {})}
        points = list(analysis.get("relative_transfer_points") or [])
        channel_gates = [
            _gate("analysis_complete", analysis.get("status") == "ok", analysis.get("status"), "status == ok"),
            _gate(
                "upstream_reference_used",
                analysis.get("input_source") == "upstream_reference",
                analysis.get("input_source"),
                "upstream_reference",
            ),
            _gate(
                "shared_clock_alignment",
                analysis.get("time_alignment_verified") is True,
                {
                    "verified": analysis.get("time_alignment_verified"),
                    "clock_domain": dict(analysis.get("synchronization") or {}).get("clock_domain"),
                },
                "shared_simulated_sample_clock",
            ),
            _gate(
                "required_path_metadata",
                not [
                    name
                    for name in required_path_metadata
                    if analysis.get("metadata", {}).get(name) in {None, ""}
                ],
                {
                    "missing": [
                        name
                        for name in required_path_metadata
                        if analysis.get("metadata", {}).get(name) in {None, ""}
                    ]
                },
                {"missing": []},
            ),
            _gate(
                "source_transfer_separated",
                analysis.get("source_transfer_separated") is True,
                analysis.get("source_transfer_separated"),
                True,
            ),
            _min_gate(
                "source_dynamics_observable",
                max(
                    (
                        float(item.get("difference_ratio"))
                        for item in list(analysis.get("source_separation") or [])
                        if _finite_or_none(item.get("difference_ratio")) is not None
                    ),
                    default=None,
                ),
                limits.get("min_source_separation_ratio"),
            ),
            _min_gate(
                "target_frequency_coverage",
                len(points),
                limits.get("min_target_frequency_count"),
            ),
            _min_gate(
                "prbs_period_coverage_after_warmup",
                analysis.get("prbs_period_count_after_warmup"),
                limits.get("min_prbs_period_count_after_warmup"),
            ),
            _max_gate(
                "timestamp_jitter_ratio",
                dict(analysis.get("timing") or {}).get("interval_jitter_ratio"),
                limits.get("max_timestamp_jitter_ratio"),
            ),
        ]
        for point in points:
            frequency = point.get("requested_frequency_hz")
            channel_gates.extend(
                [
                    _min_gate(
                        f"coherence@{frequency}Hz",
                        point.get("coherence"),
                        limits.get("min_coherence"),
                    ),
                    _max_gate(
                        f"amplitude_relative_error@{frequency}Hz",
                        point.get("amplitude_relative_error"),
                        limits.get("max_amplitude_relative_error"),
                    ),
                    _max_gate(
                        f"phase_absolute_error_deg@{frequency}Hz",
                        point.get("phase_absolute_error_deg"),
                        limits.get("max_phase_absolute_error_deg"),
                    ),
                    _max_gate(
                        f"amplitude_ci95_width_db@{frequency}Hz",
                        point.get("amplitude_ci95_width_db"),
                        limits.get("max_amplitude_ci95_width_db"),
                    ),
                ]
            )
        for gate in channel_gates:
            gate["gas"] = gas
            gate["analyzer_id"] = analyzer_id
        gates.extend(channel_gates)
        channel_results.append(
            {
                "gas": gas,
                "analyzer_id": analyzer_id,
                "status": "pass" if all(bool(item.get("passed")) for item in channel_gates) else "fail",
                "gates": channel_gates,
            }
        )
    passed = bool(rows) and all(bool(item.get("passed")) for item in gates)
    failed = [
        f"{item.get('gas')}:{item.get('analyzer_id')}:{item.get('name')}"
        for item in gates
        if not bool(item.get("passed"))
    ]
    return {
        "artifact_type": "ec_dynamic_system_identification_acceptance",
        "artifact_role": "diagnostic_analysis",
        "schema_version": str(contract.get("schema_version") or "ec_dynamic_system_identification_contract_v1"),
        "protocol_id": str(protocol_id or ""),
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "acceptance_scope": "ec_dynamic_system_identification_fixture",
        "promotion_state": "blocked",
        "ready_for_promotion": False,
        "static_calibration_status": "not_evaluated",
        "ec_dynamic_status": "simulation_system_id_pass" if passed else "simulation_system_id_fail",
        "real_acceptance_status": "blocked",
        "all_fixture_gates_passed": passed,
        "failed_gate_names": failed,
        "required_gates": gates,
        "channels": channel_results,
        "boundary_note": (
            "经验传递函数、相干性和区间只验证 simulation fixture 的系统辨识实现；"
            "不代表真实分析仪频响或 EC 通量验收。"
        ),
    }


def _welch_h1(
    input_values: np.ndarray,
    output_values: np.ndarray,
    *,
    sample_rate_hz: float,
    segment_size: int,
) -> dict[str, Any] | None:
    sample_count = min(len(input_values), len(output_values))
    size = min(max(32, int(segment_size)), sample_count)
    size = 2 ** int(np.floor(np.log2(size)))
    if size < 32:
        return None
    step = size // 2
    window = np.hanning(size)
    sxx: np.ndarray | None = None
    syy: np.ndarray | None = None
    syx: np.ndarray | None = None
    segment_transfers: list[np.ndarray] = []
    for start in range(0, sample_count - size + 1, step):
        x = np.asarray(input_values[start : start + size], dtype=float)
        y = np.asarray(output_values[start : start + size], dtype=float)
        x = x - float(np.mean(x))
        y = y - float(np.mean(y))
        x_fft = np.fft.rfft(x * window)
        y_fft = np.fft.rfft(y * window)
        current_sxx = x_fft * np.conjugate(x_fft)
        current_syy = y_fft * np.conjugate(y_fft)
        current_syx = y_fft * np.conjugate(x_fft)
        sxx = current_sxx if sxx is None else sxx + current_sxx
        syy = current_syy if syy is None else syy + current_syy
        syx = current_syx if syx is None else syx + current_syx
        segment_transfers.append(y_fft / np.where(np.abs(x_fft) > _EPSILON, x_fft, np.nan + 0j))
    window_count = len(segment_transfers)
    if sxx is None or syy is None or syx is None or window_count < 2:
        return None
    sxx = sxx / window_count
    syy = syy / window_count
    syx = syx / window_count
    transfer = syx / np.where(np.abs(sxx) > _EPSILON, sxx, np.nan + 0j)
    coherence = (np.abs(syx) ** 2) / np.maximum(np.real(sxx) * np.real(syy), _EPSILON)
    frequencies = np.fft.rfftfreq(size, d=1.0 / sample_rate_hz)
    return {
        "frequencies_hz": frequencies,
        "transfer": transfer,
        "coherence": np.clip(np.real(coherence), 0.0, 1.0),
        "segment_transfers": np.asarray(segment_transfers),
        "segment_size": size,
        "window_count": window_count,
    }


def _select_transfer_points(
    spectrum: Mapping[str, Any],
    *,
    target_frequencies_hz: Sequence[float],
    truth_points: Mapping[float, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    frequencies = np.asarray(spectrum["frequencies_hz"], dtype=float)
    transfer = np.asarray(spectrum["transfer"], dtype=complex)
    coherence = np.asarray(spectrum["coherence"], dtype=float)
    segment_transfers = np.asarray(spectrum["segment_transfers"], dtype=complex)
    rows: list[dict[str, Any]] = []
    for requested in target_frequencies_hz:
        requested_frequency = float(requested)
        if requested_frequency <= 0.0 or requested_frequency > float(frequencies[-1]):
            continue
        index = int(np.argmin(np.abs(frequencies - requested_frequency)))
        h_value = complex(transfer[index])
        if not np.isfinite(h_value.real) or not np.isfinite(h_value.imag):
            continue
        amplitude = abs(h_value)
        phase_deg = float(np.angle(h_value, deg=True))
        segment_values = segment_transfers[:, index]
        segment_values = segment_values[np.isfinite(segment_values.real) & np.isfinite(segment_values.imag)]
        amplitude_db_values = 20.0 * np.log10(np.maximum(np.abs(segment_values), _EPSILON))
        amp_low, amp_high = _quantile_pair(amplitude_db_values)
        phase_low, phase_high = _phase_interval(segment_values)
        truth = dict(truth_points.get(round(requested_frequency, 9)) or {})
        truth_amplitude = _finite_or_none(truth.get("amplitude_ratio"))
        truth_phase = _finite_or_none(truth.get("phase_deg"))
        amplitude_error = (
            None
            if truth_amplitude is None or abs(truth_amplitude) <= _EPSILON
            else abs(amplitude - truth_amplitude) / abs(truth_amplitude)
        )
        phase_error = (
            None
            if truth_phase is None
            else abs(_wrapped_phase_difference_deg(phase_deg, truth_phase))
        )
        rows.append(
            {
                "requested_frequency_hz": round(requested_frequency, 9),
                "frequency_hz": round(float(frequencies[index]), 9),
                "amplitude_ratio": round(amplitude, 9),
                "amplitude_db": round(20.0 * np.log10(max(amplitude, _EPSILON)), 9),
                "phase_deg": round(phase_deg, 9),
                "coherence": round(float(coherence[index]), 9),
                "amplitude_ci95_db": {
                    "low": _round_or_none(amp_low),
                    "high": _round_or_none(amp_high),
                    "method": "welch_segment_percentile_interval",
                },
                "amplitude_ci95_width_db": _round_or_none(
                    None if amp_low is None or amp_high is None else amp_high - amp_low
                ),
                "phase_ci95_deg": {
                    "low": _round_or_none(phase_low),
                    "high": _round_or_none(phase_high),
                    "method": "wrapped_welch_segment_percentile_interval",
                },
                "truth_amplitude_ratio": truth_amplitude,
                "truth_phase_deg": truth_phase,
                "amplitude_relative_error": _round_or_none(amplitude_error),
                "phase_absolute_error_deg": _round_or_none(phase_error),
            }
        )
    return rows


def _phase_interval(values: np.ndarray) -> tuple[float | None, float | None]:
    if not len(values):
        return None, None
    unit = values / np.maximum(np.abs(values), _EPSILON)
    center = float(np.angle(np.mean(unit), deg=True))
    differences = np.asarray(
        [_wrapped_phase_difference_deg(float(np.angle(item, deg=True)), center) for item in values],
        dtype=float,
    )
    low, high = _quantile_pair(differences)
    if low is None or high is None:
        return None, None
    return center + low, center + high


def _quantile_pair(values: np.ndarray) -> tuple[float | None, float | None]:
    finite = np.asarray(values, dtype=float)
    finite = finite[np.isfinite(finite)]
    if not len(finite):
        return None, None
    low, high = np.quantile(finite, [0.025, 0.975])
    return float(low), float(high)


def _timing_payload(
    sample_rate_hz: float,
    jitter_ratio: float,
    median_interval_s: float,
    sample_count: int,
) -> dict[str, Any]:
    return {
        "sample_rate_hz": round(float(sample_rate_hz), 9),
        "median_sample_interval_s": round(float(median_interval_s), 9),
        "interval_jitter_ratio": round(float(jitter_ratio), 9),
        "sample_count": int(sample_count),
    }


def _gate(name: str, passed: bool, value: Any, limit: Any) -> dict[str, Any]:
    return {
        "name": str(name),
        "required": True,
        "passed": bool(passed),
        "value": value,
        "limit": limit,
    }


def _min_gate(name: str, value: Any, minimum: Any) -> dict[str, Any]:
    numeric_value = _finite_or_none(value)
    numeric_minimum = _finite_or_none(minimum)
    return _gate(
        name,
        numeric_value is not None and numeric_minimum is not None and numeric_value >= numeric_minimum,
        numeric_value,
        {"min": numeric_minimum},
    )


def _max_gate(name: str, value: Any, maximum: Any) -> dict[str, Any]:
    numeric_value = _finite_or_none(value)
    numeric_maximum = _finite_or_none(maximum)
    return _gate(
        name,
        numeric_value is not None and numeric_maximum is not None and numeric_value <= numeric_maximum,
        numeric_value,
        {"max": numeric_maximum},
    )


def _relative_difference(left: Any, right: Any) -> float | None:
    left_value = _finite_or_none(left)
    right_value = _finite_or_none(right)
    if left_value is None or right_value is None or abs(right_value) <= _EPSILON:
        return None
    return round(abs(left_value - right_value) / abs(right_value), 9)


def _wrapped_phase_difference_deg(value: float, reference: float) -> float:
    return (float(value) - float(reference) + 180.0) % 360.0 - 180.0


def _finite_array(values: Sequence[Any]) -> np.ndarray:
    try:
        array = np.asarray(list(values), dtype=float)
    except (TypeError, ValueError):
        return np.asarray([], dtype=float)
    if array.ndim != 1 or np.any(~np.isfinite(array)):
        return np.asarray([], dtype=float)
    return array


def _round_or_none(value: float | None, digits: int = 9) -> float | None:
    return None if value is None else round(float(value), digits)


__all__ = [
    "build_system_identification_acceptance",
    "identify_empirical_transfer",
]
