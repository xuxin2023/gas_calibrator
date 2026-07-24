"""Offline EC dynamic metrology analysis and acceptance contracts.

This module is deliberately independent from the production V1 runtime.  It
turns simulation/replay step-response series into reviewer-facing dynamic
metrics and never promotes the result to real acceptance evidence.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from math import atan, isfinite, log, pi, sqrt
from typing import Any, Iterable, Mapping, Sequence

import numpy as np


SUPPORTED_GASES = {"co2", "h2o"}
DEFAULT_TRANSFER_FREQUENCIES_HZ = (0.01, 0.1, 1.0)
_EPSILON = 1e-12


@dataclass(frozen=True)
class DynamicPathMetadata:
    """Physical and timing metadata required for one analyzer path."""

    analyzer_id: str
    gas: str
    serial_position: int
    sample_rate_hz: float
    tube_length_m: float
    tube_inner_diameter_mm: float
    tube_material: str
    flow_slpm: float
    cell_pressure_hpa: float
    cell_temperature_c: float
    relative_humidity_pct: float
    heated_tube: bool
    filter_id: str
    transport_delay_s: float
    fast_rise_tau_s: float
    fast_fall_tau_s: float
    memory_fraction: float = 0.0
    memory_rise_tau_s: float = 1.0
    memory_fall_tau_s: float = 1.0
    timestamp_source: str = "simulated_sample_clock"

    def validate(self) -> None:
        gas = str(self.gas or "").strip().lower()
        if gas not in SUPPORTED_GASES:
            raise ValueError(f"unsupported gas: {self.gas}")
        if not str(self.analyzer_id or "").strip():
            raise ValueError("analyzer_id is required")
        if int(self.serial_position) < 1:
            raise ValueError("serial_position must be >= 1")
        positive_values = {
            "sample_rate_hz": self.sample_rate_hz,
            "tube_inner_diameter_mm": self.tube_inner_diameter_mm,
            "flow_slpm": self.flow_slpm,
            "cell_pressure_hpa": self.cell_pressure_hpa,
            "fast_rise_tau_s": self.fast_rise_tau_s,
            "fast_fall_tau_s": self.fast_fall_tau_s,
            "memory_rise_tau_s": self.memory_rise_tau_s,
            "memory_fall_tau_s": self.memory_fall_tau_s,
        }
        for name, value in positive_values.items():
            if not _is_positive_finite(value):
                raise ValueError(f"{name} must be positive and finite")
        if not _is_nonnegative_finite(self.tube_length_m):
            raise ValueError("tube_length_m must be non-negative and finite")
        if not _is_nonnegative_finite(self.transport_delay_s):
            raise ValueError("transport_delay_s must be non-negative and finite")
        if not 0.0 <= float(self.relative_humidity_pct) <= 100.0:
            raise ValueError("relative_humidity_pct must be within [0, 100]")
        if not 0.0 <= float(self.memory_fraction) < 1.0:
            raise ValueError("memory_fraction must be within [0, 1)")
        if gas == "co2" and float(self.memory_fraction) > 0.0:
            raise ValueError("CO2 fixture paths must not use H2O sorption memory")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        payload = asdict(self)
        payload["gas"] = str(self.gas).lower()
        return payload


def analyze_dynamic_channel(
    channel: Mapping[str, Any],
    *,
    protocol: Mapping[str, Any],
    transfer_frequencies_hz: Sequence[float] = DEFAULT_TRANSFER_FREQUENCIES_HZ,
) -> dict[str, Any]:
    """Estimate step metrics, effective transfer response, timing, and noise."""

    metadata = dict(channel.get("metadata") or {})
    gas = str(metadata.get("gas") or channel.get("gas") or "").strip().lower()
    analyzer_id = str(metadata.get("analyzer_id") or channel.get("analyzer_id") or "").strip()
    timestamps = _finite_array(channel.get("timestamps_s") or [])
    values = _finite_array(channel.get("values") or [])
    sample_indices = _integer_array(channel.get("sample_indices") or [])
    expected_sample_count = int(channel.get("expected_sample_count") or len(values))

    base_result: dict[str, Any] = {
        "analyzer_id": analyzer_id,
        "gas": gas,
        "serial_position": int(metadata.get("serial_position") or 0),
        "metadata": metadata,
        "status": "invalid",
        "flags": [],
    }
    if gas not in SUPPORTED_GASES:
        return {**base_result, "flags": ["unsupported_gas"]}
    if len(timestamps) != len(values) or len(values) < 8:
        return {**base_result, "flags": ["insufficient_or_misaligned_samples"]}
    if np.any(np.diff(timestamps) <= 0.0):
        return {**base_result, "flags": ["non_monotonic_timestamps"]}

    interval_metrics = _timestamp_metrics(
        timestamps,
        sample_indices=sample_indices if len(sample_indices) == len(timestamps) else None,
        expected_sample_count=expected_sample_count,
    )
    step_up_s = float(protocol.get("step_up_s"))
    step_down_s = float(protocol.get("step_down_s"))
    baseline_input = float(protocol.get("baseline_value"))
    high_input = float(protocol.get("step_value"))
    input_span = high_input - baseline_input
    if step_up_s <= 0.0 or step_down_s <= step_up_s or abs(input_span) <= _EPSILON:
        return {**base_result, "flags": ["invalid_protocol_definition"]}

    pre_values = values[timestamps < step_up_s]
    high_window_start = step_up_s + 0.7 * (step_down_s - step_up_s)
    high_values = values[(timestamps >= high_window_start) & (timestamps < step_down_s)]
    final_window_start = step_down_s + 0.7 * max(float(timestamps[-1]) - step_down_s, 0.0)
    final_values = values[timestamps >= final_window_start]
    if min(len(pre_values), len(high_values), len(final_values)) < 3:
        return {
            **base_result,
            "flags": ["insufficient_plateau_samples"],
            "timing": interval_metrics,
        }

    response_low = float(np.median(pre_values))
    response_high = float(np.median(high_values))
    response_final = float(np.median(final_values))
    rise = _transition_metrics(
        timestamps,
        values,
        edge_time_s=step_up_s,
        end_time_s=step_down_s,
        start_value=response_low,
        end_value=response_high,
    )
    fall = _transition_metrics(
        timestamps,
        values,
        edge_time_s=step_down_s,
        end_time_s=float(timestamps[-1]),
        start_value=response_high,
        end_value=response_final,
    )
    flags: list[str] = []
    if rise is None:
        flags.append("rise_transition_not_resolved")
    if fall is None:
        flags.append("fall_transition_not_resolved")

    rise_tau = _effective_tau(rise)
    fall_tau = _effective_tau(fall)
    rise_delay = _effective_delay(rise, rise_tau)
    fall_delay = _effective_delay(fall, fall_tau)
    gain = (response_high - response_low) / input_span
    tau_asymmetry = _symmetric_ratio(rise_tau, fall_tau)
    sample_rate_hz = float(interval_metrics.get("sample_rate_hz") or 0.0)
    transfer_function = _effective_transfer_function(
        gain=gain,
        tau_s=rise_tau,
        delay_s=rise_delay,
        sample_rate_hz=sample_rate_hz,
        frequencies_hz=transfer_frequencies_hz,
    )
    stable_values = np.concatenate((pre_values, final_values))
    noise_std = float(np.std(stable_values)) if stable_values.size else None
    allan = _allan_deviation(
        stable_values,
        sample_rate_hz=sample_rate_hz,
        averaging_seconds=(1.0, 2.0, 5.0),
    )

    return {
        **base_result,
        "status": "ok" if not flags else "incomplete",
        "flags": flags,
        "plateaus": {
            "baseline": response_low,
            "high": response_high,
            "final": response_final,
            "input_baseline": baseline_input,
            "input_high": high_input,
        },
        "gain": round(float(gain), 9),
        "rise": rise,
        "fall": fall,
        "effective_rise_tau_s": _round_or_none(rise_tau),
        "effective_fall_tau_s": _round_or_none(fall_tau),
        "effective_rise_delay_s": _round_or_none(rise_delay),
        "effective_fall_delay_s": _round_or_none(fall_delay),
        "rise_fall_tau_ratio": _round_or_none(tau_asymmetry),
        "noise_std": _round_or_none(noise_std),
        "allan_deviation": allan,
        "timing": interval_metrics,
        "effective_transfer_function": transfer_function,
    }


def build_dynamic_acceptance(
    analyses: Iterable[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
    protocol_id: str,
) -> dict[str, Any]:
    """Evaluate offline fixture gates while keeping real promotion blocked."""

    rows = [dict(item) for item in analyses]
    common_limits = dict(contract.get("common_fixture_limits") or {})
    gas_limits = dict(contract.get("gas_fixture_limits") or {})
    required_metadata = [
        str(item)
        for item in list(contract.get("required_path_metadata") or [])
        if str(item).strip()
    ]
    channel_results: list[dict[str, Any]] = []
    required_gates: list[dict[str, Any]] = []

    for analysis in rows:
        gas = str(analysis.get("gas") or "").lower()
        limits = {**common_limits, **dict(gas_limits.get(gas) or {})}
        timing = dict(analysis.get("timing") or {})
        rise = dict(analysis.get("rise") or {})
        metadata = dict(analysis.get("metadata") or {})
        missing_metadata = [
            name
            for name in required_metadata
            if name not in metadata or metadata.get(name) is None or metadata.get(name) == ""
        ]
        gates = [
            _gate(
                "required_path_metadata",
                not missing_metadata,
                {"missing": missing_metadata},
                {"required": required_metadata},
            ),
            _gate(
                "analysis_complete",
                analysis.get("status") == "ok",
                analysis.get("status"),
                "status == ok",
            ),
            _max_gate(
                "timestamp_jitter_ratio",
                timing.get("interval_jitter_ratio"),
                limits.get("max_timestamp_jitter_ratio"),
            ),
            _max_gate(
                "dropout_fraction",
                timing.get("dropout_fraction"),
                limits.get("max_dropout_fraction"),
            ),
            _relative_gate(
                "sample_rate",
                timing.get("sample_rate_hz"),
                metadata.get("sample_rate_hz"),
                limits.get("max_sample_rate_relative_error"),
            ),
            _absolute_error_gate(
                "dynamic_gain",
                analysis.get("gain"),
                1.0,
                limits.get("max_gain_absolute_error"),
            ),
            _max_gate(
                "t90",
                rise.get("t90_s"),
                limits.get("max_t90_s"),
            ),
            _max_gate(
                "rise_fall_tau_asymmetry",
                analysis.get("rise_fall_tau_ratio"),
                limits.get("max_rise_fall_tau_ratio"),
            ),
            _absolute_error_gate(
                "synthetic_delay_recovery",
                analysis.get("effective_rise_delay_s"),
                float(metadata.get("transport_delay_s") or 0.0),
                limits.get("max_synthetic_delay_absolute_error_s"),
            ),
        ]
        if float(metadata.get("memory_fraction") or 0.0) == 0.0:
            gates.append(
                _relative_gate(
                    "synthetic_tau_recovery",
                    analysis.get("effective_rise_tau_s"),
                    metadata.get("fast_rise_tau_s"),
                    limits.get("max_synthetic_tau_relative_error"),
                )
            )
        channel_passed = all(bool(item.get("passed")) for item in gates)
        required_gates.extend(
            {
                **item,
                "analyzer_id": analysis.get("analyzer_id"),
                "gas": gas,
            }
            for item in gates
        )
        channel_results.append(
            {
                "analyzer_id": analysis.get("analyzer_id"),
                "gas": gas,
                "serial_position": analysis.get("serial_position"),
                "status": "pass" if channel_passed else "fail",
                "gates": gates,
            }
        )

    serial_order_gates = _serial_delay_order_gates(rows)
    required_gates.extend(serial_order_gates)
    all_passed = bool(rows) and all(bool(item.get("passed")) for item in required_gates)
    failed_gate_names = [
        f"{item.get('gas', '')}:{item.get('analyzer_id', '')}:{item.get('name', '')}".strip(":")
        for item in required_gates
        if not bool(item.get("passed"))
    ]
    return {
        "artifact_type": "ec_dynamic_acceptance",
        "artifact_role": "diagnostic_analysis",
        "schema_version": str(contract.get("schema_version") or "ec_dynamic_acceptance_contract_v1"),
        "protocol_id": str(protocol_id or ""),
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "acceptance_scope": "ec_dynamic_simulation_contract",
        "promotion_state": "blocked",
        "ready_for_promotion": False,
        "static_calibration_status": "not_evaluated",
        "ec_dynamic_status": "simulation_contract_pass" if all_passed else "simulation_contract_fail",
        "real_acceptance_status": "blocked",
        "all_fixture_gates_passed": all_passed,
        "failed_gate_names": failed_gate_names,
        "required_gates": required_gates,
        "channels": channel_results,
        "boundary_note": (
            "本结果仅验证离线动态计量算法对合成真值和异常的识别能力；"
            "不代表真实分析仪、真实流路或涡动协方差系统验收。"
        ),
    }


def _timestamp_metrics(
    timestamps: np.ndarray,
    *,
    sample_indices: np.ndarray | None,
    expected_sample_count: int,
) -> dict[str, Any]:
    deltas = np.diff(timestamps)
    if sample_indices is not None and len(sample_indices) == len(timestamps):
        index_steps = np.diff(sample_indices)
        if np.any(index_steps <= 0):
            return {
                "sample_rate_hz": None,
                "interval_jitter_ratio": None,
                "dropout_fraction": 1.0,
                "non_monotonic_sample_indices": True,
            }
        per_sample_deltas = deltas / index_steps
        observed_missing = max(0, int(sample_indices[-1] - sample_indices[0] + 1 - len(sample_indices)))
    else:
        per_sample_deltas = deltas
        observed_missing = max(0, int(expected_sample_count - len(timestamps)))
    median_interval = float(np.median(per_sample_deltas))
    jitter_ratio = float(np.std(per_sample_deltas) / max(median_interval, _EPSILON))
    denominator = max(int(expected_sample_count), len(timestamps), 1)
    dropout_fraction = max(observed_missing, denominator - len(timestamps)) / denominator
    return {
        "sample_rate_hz": round(1.0 / max(median_interval, _EPSILON), 9),
        "median_sample_interval_s": round(median_interval, 9),
        "interval_jitter_ratio": round(jitter_ratio, 9),
        "dropout_fraction": round(float(dropout_fraction), 9),
        "observed_sample_count": int(len(timestamps)),
        "expected_sample_count": int(expected_sample_count),
    }


def _transition_metrics(
    timestamps: np.ndarray,
    values: np.ndarray,
    *,
    edge_time_s: float,
    end_time_s: float,
    start_value: float,
    end_value: float,
) -> dict[str, float] | None:
    span = end_value - start_value
    if abs(span) <= _EPSILON:
        return None
    mask = (timestamps >= edge_time_s) & (timestamps <= end_time_s)
    segment_t = timestamps[mask]
    segment_v = values[mask]
    if len(segment_t) < 3:
        return None
    progress = (segment_v - start_value) / span
    crossings: dict[str, float] = {}
    for label, fraction in (("t10_s", 0.1), ("t50_s", 0.5), ("t90_s", 0.9)):
        crossing = _first_crossing(segment_t, progress, fraction)
        if crossing is None:
            return None
        crossings[label] = round(max(0.0, crossing - edge_time_s), 9)
    crossings["rise_time_10_90_s"] = round(crossings["t90_s"] - crossings["t10_s"], 9)
    return crossings


def _first_crossing(timestamps: np.ndarray, progress: np.ndarray, threshold: float) -> float | None:
    candidates = np.flatnonzero(progress >= threshold)
    if candidates.size == 0:
        return None
    index = int(candidates[0])
    if index == 0:
        return float(timestamps[0])
    p0 = float(progress[index - 1])
    p1 = float(progress[index])
    t0 = float(timestamps[index - 1])
    t1 = float(timestamps[index])
    if abs(p1 - p0) <= _EPSILON:
        return t1
    fraction = (threshold - p0) / (p1 - p0)
    return t0 + fraction * (t1 - t0)


def _effective_tau(metrics: Mapping[str, Any] | None) -> float | None:
    if not metrics:
        return None
    t10 = _finite_or_none(metrics.get("t10_s"))
    t90 = _finite_or_none(metrics.get("t90_s"))
    if t10 is None or t90 is None or t90 <= t10:
        return None
    return (t90 - t10) / log(9.0)


def _effective_delay(metrics: Mapping[str, Any] | None, tau_s: float | None) -> float | None:
    if not metrics or tau_s is None:
        return None
    t50 = _finite_or_none(metrics.get("t50_s"))
    if t50 is None:
        return None
    return max(0.0, t50 - log(2.0) * tau_s)


def _effective_transfer_function(
    *,
    gain: float,
    tau_s: float | None,
    delay_s: float | None,
    sample_rate_hz: float,
    frequencies_hz: Sequence[float],
) -> list[dict[str, float]]:
    if tau_s is None or delay_s is None or sample_rate_hz <= 0.0:
        return []
    rows: list[dict[str, float]] = []
    nyquist = 0.5 * sample_rate_hz
    for raw_frequency in frequencies_hz:
        frequency = float(raw_frequency)
        if frequency <= 0.0 or frequency >= nyquist:
            continue
        omega_tau = 2.0 * pi * frequency * tau_s
        amplitude = abs(gain) / sqrt(1.0 + omega_tau * omega_tau)
        phase_rad = -(2.0 * pi * frequency * delay_s) - atan(omega_tau)
        rows.append(
            {
                "frequency_hz": round(frequency, 9),
                "amplitude_ratio": round(amplitude, 9),
                "phase_deg": round(phase_rad * 180.0 / pi, 9),
            }
        )
    return rows


def _allan_deviation(
    values: np.ndarray,
    *,
    sample_rate_hz: float,
    averaging_seconds: Sequence[float],
) -> list[dict[str, Any]]:
    if len(values) < 4 or sample_rate_hz <= 0.0:
        return []
    rows: list[dict[str, Any]] = []
    for seconds in averaging_seconds:
        block_size = max(1, int(round(float(seconds) * sample_rate_hz)))
        block_count = len(values) // block_size
        if block_count < 2:
            continue
        trimmed = values[: block_count * block_size]
        block_means = np.mean(trimmed.reshape(block_count, block_size), axis=1)
        deviation = sqrt(0.5 * float(np.mean(np.diff(block_means) ** 2)))
        rows.append(
            {
                "averaging_time_s": round(block_size / sample_rate_hz, 9),
                "allan_deviation": round(deviation, 12),
                "block_count": int(block_count),
            }
        )
    return rows


def _serial_delay_order_gates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []
    for gas in sorted(SUPPORTED_GASES):
        gas_rows = sorted(
            [item for item in rows if str(item.get("gas") or "").lower() == gas],
            key=lambda item: int(item.get("serial_position") or 0),
        )
        if len(gas_rows) < 2:
            continue
        delays = [_finite_or_none(item.get("effective_rise_delay_s")) for item in gas_rows]
        passed = all(
            left is not None and right is not None and right > left
            for left, right in zip(delays, delays[1:])
        )
        gates.append(
            _gate(
                "serial_position_delay_order",
                passed,
                delays,
                "estimated delay must increase with serial position",
                gas=gas,
                analyzer_id="chain",
            )
        )
    return gates


def _gate(
    name: str,
    passed: bool,
    value: Any,
    limit: Any,
    *,
    gas: str | None = None,
    analyzer_id: str | None = None,
) -> dict[str, Any]:
    payload = {
        "name": name,
        "required": True,
        "passed": bool(passed),
        "value": value,
        "limit": limit,
    }
    if gas is not None:
        payload["gas"] = gas
    if analyzer_id is not None:
        payload["analyzer_id"] = analyzer_id
    return payload


def _max_gate(name: str, value: Any, maximum: Any) -> dict[str, Any]:
    numeric_value = _finite_or_none(value)
    numeric_maximum = _finite_or_none(maximum)
    passed = numeric_value is not None and numeric_maximum is not None and numeric_value <= numeric_maximum
    return _gate(name, passed, numeric_value, {"max": numeric_maximum})


def _relative_gate(name: str, value: Any, target: Any, maximum_relative_error: Any) -> dict[str, Any]:
    numeric_value = _finite_or_none(value)
    numeric_target = _finite_or_none(target)
    numeric_limit = _finite_or_none(maximum_relative_error)
    error = None
    if numeric_value is not None and numeric_target is not None and abs(numeric_target) > _EPSILON:
        error = abs(numeric_value - numeric_target) / abs(numeric_target)
    passed = error is not None and numeric_limit is not None and error <= numeric_limit
    return _gate(
        name,
        passed,
        {"measured": numeric_value, "target": numeric_target, "relative_error": error},
        {"max_relative_error": numeric_limit},
    )


def _absolute_error_gate(name: str, value: Any, target: float, maximum_absolute_error: Any) -> dict[str, Any]:
    numeric_value = _finite_or_none(value)
    numeric_limit = _finite_or_none(maximum_absolute_error)
    error = None if numeric_value is None else abs(numeric_value - target)
    passed = error is not None and numeric_limit is not None and error <= numeric_limit
    return _gate(
        name,
        passed,
        {"measured": numeric_value, "target": target, "absolute_error": error},
        {"max_absolute_error": numeric_limit},
    )


def _symmetric_ratio(left: float | None, right: float | None) -> float | None:
    if left is None or right is None or left <= 0.0 or right <= 0.0:
        return None
    return max(left, right) / min(left, right)


def _finite_array(values: Sequence[Any]) -> np.ndarray:
    try:
        array = np.asarray(list(values), dtype=float)
    except (TypeError, ValueError):
        return np.asarray([], dtype=float)
    if array.ndim != 1 or np.any(~np.isfinite(array)):
        return np.asarray([], dtype=float)
    return array


def _integer_array(values: Sequence[Any]) -> np.ndarray:
    try:
        array = np.asarray(list(values), dtype=int)
    except (TypeError, ValueError):
        return np.asarray([], dtype=int)
    return array if array.ndim == 1 else np.asarray([], dtype=int)


def _finite_or_none(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if isfinite(numeric) else None


def _round_or_none(value: float | None, digits: int = 9) -> float | None:
    return None if value is None else round(float(value), digits)


def _is_positive_finite(value: Any) -> bool:
    numeric = _finite_or_none(value)
    return numeric is not None and numeric > 0.0


def _is_nonnegative_finite(value: Any) -> bool:
    numeric = _finite_or_none(value)
    return numeric is not None and numeric >= 0.0


__all__ = [
    "DEFAULT_TRANSFER_FREQUENCIES_HZ",
    "DynamicPathMetadata",
    "SUPPORTED_GASES",
    "analyze_dynamic_channel",
    "build_dynamic_acceptance",
]
