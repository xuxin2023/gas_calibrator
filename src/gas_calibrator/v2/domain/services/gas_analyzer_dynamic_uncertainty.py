"""Gas-analyzer-only dynamic bandwidth and engineering uncertainty analysis."""

from __future__ import annotations

from math import isfinite, pi, sqrt
from typing import Any, Iterable, Mapping

import numpy as np


_EPSILON = 1e-12


def analyze_gas_analyzer_dynamic_performance(
    system_identification: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Derive analyzer bandwidth, phase delay, and an engineering uncertainty budget.

    Dynamic attenuation is reported as a systematic response bias. It is not
    folded into the uncertainty budget and no inverse correction is produced.
    """

    source = dict(system_identification)
    metadata = dict(source.get("metadata") or {})
    protocol = dict(source.get("protocol") or {})
    synchronization = dict(source.get("synchronization") or {})
    gas = str(source.get("gas") or metadata.get("gas") or "").lower()
    analyzer_id = str(source.get("analyzer_id") or metadata.get("analyzer_id") or "")
    base = {
        "artifact_type": "gas_analyzer_dynamic_performance",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "gas_analyzer_dynamic_performance_v1",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "gas_analyzer_only": True,
        "ec_flux_in_scope": False,
        "formal_metrological_uncertainty": False,
        "automatic_dynamic_correction_applied": False,
        "correction_factor_output": None,
        "analyzer_id": analyzer_id,
        "gas": gas,
        "metadata": metadata,
        "protocol": protocol,
        "synchronization": synchronization,
        "status": "invalid",
        "flags": [],
    }
    if source.get("status") != "ok":
        return {
            **base,
            "flags": ["system_identification_not_complete", *list(source.get("flags") or [])],
        }
    points = _valid_points(source.get("relative_transfer_points") or [])
    if len(points) < 3:
        return {**base, "flags": ["insufficient_transfer_points"]}

    thresholds = dict(contract.get("bandwidth_thresholds") or {})
    uncertainty_model = dict(contract.get("uncertainty_model") or {})
    coverage_factor = _positive(uncertainty_model.get("coverage_factor"), default=2.0)
    independent_fraction = _positive(
        uncertainty_model.get("welch_overlap_independent_fraction"),
        default=0.5,
    )
    window_count = max(1, int(source.get("window_count") or 1))
    effective_average_count = max(1.0, window_count * independent_fraction)
    point_budgets = [
        _point_uncertainty_budget(
            point,
            uncertainty_model=uncertainty_model,
            effective_average_count=effective_average_count,
            coverage_factor=coverage_factor,
        )
        for point in points
    ]

    empirical_bandwidths = {
        name: _bandwidth_at_threshold(
            points,
            threshold=float(threshold),
            amplitude_field="amplitude_ratio",
        )
        for name, threshold in thresholds.items()
    }
    truth_bandwidths = {
        name: _bandwidth_at_threshold(
            points,
            threshold=float(threshold),
            amplitude_field="truth_amplitude_ratio",
        )
        for name, threshold in thresholds.items()
    }
    bandwidth_rows: dict[str, dict[str, Any]] = {}
    frequency_resolution_hz = _frequency_resolution(points)
    decision_relative_limit = _positive(
        dict(contract.get("common_fixture_limits") or {}).get(
            "max_decision_bandwidth_expanded_relative_uncertainty"
        ),
        default=0.35,
    )
    bin_divisor = _positive(
        uncertainty_model.get("bandwidth_bin_standard_uncertainty_divisor"),
        default=sqrt(12.0),
    )
    for name, threshold in thresholds.items():
        empirical = dict(empirical_bandwidths[name])
        truth = dict(truth_bandwidths[name])
        frequency = _finite_or_none(empirical.get("frequency_hz"))
        truth_frequency = _finite_or_none(truth.get("frequency_hz"))
        relative_error = _relative_error(frequency, truth_frequency)
        bandwidth_uncertainty = _bandwidth_uncertainty(
            empirical,
            point_budgets=point_budgets,
            threshold=float(threshold),
            frequency_resolution_hz=frequency_resolution_hz,
            bin_standard_uncertainty_divisor=bin_divisor,
            coverage_factor=coverage_factor,
        )
        expanded_relative_uncertainty = _relative_uncertainty(
            bandwidth_uncertainty.get("expanded_uncertainty_hz"),
            frequency,
        )
        bandwidth_rows[name] = {
            "amplitude_threshold": float(threshold),
            "frequency_hz": frequency,
            "truth_frequency_hz": truth_frequency,
            "relative_error": relative_error,
            "censored": bool(empirical.get("censored")),
            "uncertainty": bandwidth_uncertainty,
            "expanded_relative_uncertainty": expanded_relative_uncertainty,
            "decision_grade": (
                "qualified"
                if expanded_relative_uncertainty is not None
                and expanded_relative_uncertainty <= decision_relative_limit
                else "diagnostic_only"
            ),
        }

    usable_bandwidth_hz = _finite_or_none(
        dict(bandwidth_rows.get("ten_percent_attenuation") or {}).get("frequency_hz")
    )
    budgets_in_usable_band = [
        item
        for item in point_budgets
        if usable_bandwidth_hz is not None
        and float(item["frequency_hz"]) <= usable_bandwidth_hz + _EPSILON
    ]
    max_expanded_amplitude = _max_or_none(
        item.get("expanded_amplitude_relative_uncertainty")
        for item in budgets_in_usable_band
    )
    max_expanded_phase = _max_or_none(
        item.get("expanded_phase_uncertainty_deg")
        for item in budgets_in_usable_band
    )
    phase_delay_rows = _phase_delay_rows(points)
    low_frequency_delay = _median_or_none(
        item.get("effective_phase_delay_s")
        for item in phase_delay_rows[: min(3, len(phase_delay_rows))]
    )
    top_contributors = _top_contributors(budgets_in_usable_band)

    return {
        **base,
        "status": "ok",
        "flags": [],
        "input_source": source.get("input_source"),
        "source_transfer_separated": source.get("source_transfer_separated"),
        "source_separation": list(source.get("source_separation") or []),
        "time_alignment_verified": source.get("time_alignment_verified"),
        "timing": dict(source.get("timing") or {}),
        "window_count": window_count,
        "effective_average_count": round(effective_average_count, 9),
        "prbs_period_count_after_warmup": source.get("prbs_period_count_after_warmup"),
        "evaluation_frequency_count": len(points),
        "evaluation_frequency_range_hz": {
            "minimum": points[0]["frequency_hz"],
            "maximum": points[-1]["frequency_hz"],
            "resolution": frequency_resolution_hz,
        },
        "bandwidths": bandwidth_rows,
        "usable_bandwidth_definition": "ten_percent_attenuation",
        "usable_bandwidth_hz": usable_bandwidth_hz,
        "low_frequency_effective_phase_delay_s": low_frequency_delay,
        "phase_delay_by_frequency": phase_delay_rows,
        "dynamic_response_points": [
            {
                "frequency_hz": point["frequency_hz"],
                "amplitude_ratio": point["amplitude_ratio"],
                "dynamic_amplitude_bias_relative": round(
                    1.0 - float(point["amplitude_ratio"]),
                    9,
                ),
                "phase_deg": point["phase_deg"],
                "coherence": point["coherence"],
            }
            for point in points
        ],
        "uncertainty_budget": {
            "budget_type": "offline_engineering_dynamic_uncertainty",
            "coverage_factor": coverage_factor,
            "formal_metrological_uncertainty": False,
            "dynamic_attenuation_included_as_uncertainty": False,
            "dynamic_attenuation_treatment": "reported_separately_as_systematic_bias",
            "points": point_budgets,
            "qualified_point_count": len(budgets_in_usable_band),
            "max_expanded_amplitude_relative_uncertainty": max_expanded_amplitude,
            "max_expanded_phase_uncertainty_deg": max_expanded_phase,
            "top_contributors": top_contributors,
        },
    }


def build_gas_analyzer_dynamic_uncertainty_acceptance(
    performances: Iterable[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
    protocol_id: str,
) -> dict[str, Any]:
    rows = [dict(item) for item in performances]
    common = dict(contract.get("common_fixture_limits") or {})
    gas_limits = dict(contract.get("gas_fixture_limits") or {})
    required_metadata = [str(item) for item in list(contract.get("required_path_metadata") or [])]
    required_contributors = {
        "welch_coherence_random",
        "upstream_reference_amplitude",
        "upstream_reference_phase",
        "shared_clock_timing",
        "spectral_leakage",
    }
    gates: list[dict[str, Any]] = []
    channels: list[dict[str, Any]] = []
    for performance in rows:
        gas = str(performance.get("gas") or "").lower()
        analyzer_id = str(performance.get("analyzer_id") or "")
        limits = {**common, **dict(gas_limits.get(gas) or {})}
        bandwidths = dict(performance.get("bandwidths") or {})
        budget = dict(performance.get("uncertainty_budget") or {})
        missing_metadata = [
            name
            for name in required_metadata
            if dict(performance.get("metadata") or {}).get(name) in {None, ""}
        ]
        source_difference = _max_or_none(
            item.get("difference_ratio")
            for item in list(performance.get("source_separation") or [])
        )
        observed_contributors = {
            str(item.get("component"))
            for item in list(budget.get("top_contributors") or [])
            if str(item.get("component") or "")
        }
        point_contributors = {
            str(component.get("component"))
            for point in list(budget.get("points") or [])
            for component in list(point.get("components") or [])
        }
        observed_contributors.update(point_contributors)
        ten_percent = dict(bandwidths.get("ten_percent_attenuation") or {})
        minus_3db = dict(bandwidths.get("minus_3db") or {})
        five_percent = dict(bandwidths.get("five_percent_attenuation") or {})
        channel_gates = [
            _gate("analysis_complete", performance.get("status") == "ok", performance.get("status"), "ok"),
            _gate(
                "gas_analyzer_only_scope",
                performance.get("gas_analyzer_only") is True
                and performance.get("ec_flux_in_scope") is False,
                {
                    "gas_analyzer_only": performance.get("gas_analyzer_only"),
                    "ec_flux_in_scope": performance.get("ec_flux_in_scope"),
                },
                {"gas_analyzer_only": True, "ec_flux_in_scope": False},
            ),
            _gate(
                "upstream_reference_used",
                performance.get("input_source") == "upstream_reference",
                performance.get("input_source"),
                "upstream_reference",
            ),
            _gate(
                "shared_clock_alignment",
                performance.get("time_alignment_verified") is True,
                performance.get("time_alignment_verified"),
                True,
            ),
            _gate(
                "required_path_metadata",
                not missing_metadata,
                {"missing": missing_metadata},
                {"missing": []},
            ),
            _min_gate(
                "source_dynamics_observable",
                source_difference,
                limits.get("min_source_separation_ratio"),
            ),
            _min_gate(
                "evaluation_frequency_coverage",
                performance.get("evaluation_frequency_count"),
                limits.get("min_evaluation_frequency_count"),
            ),
            _min_gate(
                "prbs_period_coverage_after_warmup",
                performance.get("prbs_period_count_after_warmup"),
                limits.get("min_prbs_period_count_after_warmup"),
            ),
            _max_gate(
                "timestamp_jitter_ratio",
                dict(performance.get("timing") or {}).get("interval_jitter_ratio"),
                limits.get("max_timestamp_jitter_ratio"),
            ),
            _min_gate(
                "minimum_coherence",
                _min_or_none(
                    point.get("coherence")
                    for point in list(performance.get("dynamic_response_points") or [])
                ),
                limits.get("min_coherence"),
            ),
            _min_gate(
                "ten_percent_bandwidth_hz",
                ten_percent.get("frequency_hz"),
                limits.get("min_ten_percent_bandwidth_hz"),
            ),
            _min_gate(
                "minus_3db_bandwidth_hz",
                minus_3db.get("frequency_hz"),
                limits.get("min_minus_3db_bandwidth_hz"),
            ),
            _max_gate(
                "five_percent_bandwidth_truth_error",
                five_percent.get("relative_error"),
                limits.get("max_bandwidth_relative_error"),
            ),
            _max_gate(
                "ten_percent_bandwidth_truth_error",
                ten_percent.get("relative_error"),
                limits.get("max_bandwidth_relative_error"),
            ),
            _max_gate(
                "ten_percent_bandwidth_expanded_relative_uncertainty",
                ten_percent.get("expanded_relative_uncertainty"),
                limits.get(
                    "max_decision_bandwidth_expanded_relative_uncertainty"
                ),
            ),
            _max_gate(
                "minus_3db_bandwidth_expanded_relative_uncertainty",
                minus_3db.get("expanded_relative_uncertainty"),
                limits.get(
                    "max_decision_bandwidth_expanded_relative_uncertainty"
                ),
            ),
            _max_gate(
                "minus_3db_bandwidth_truth_error",
                minus_3db.get("relative_error"),
                limits.get("max_bandwidth_relative_error"),
            ),
            _max_gate(
                "expanded_amplitude_relative_uncertainty",
                budget.get("max_expanded_amplitude_relative_uncertainty"),
                limits.get("max_expanded_amplitude_relative_uncertainty"),
            ),
            _max_gate(
                "expanded_phase_uncertainty_deg",
                budget.get("max_expanded_phase_uncertainty_deg"),
                limits.get("max_expanded_phase_uncertainty_deg"),
            ),
            _gate(
                "uncertainty_contributors_complete",
                required_contributors.issubset(observed_contributors),
                {"missing": sorted(required_contributors - observed_contributors)},
                {"missing": []},
            ),
            _gate(
                "no_automatic_dynamic_correction",
                performance.get("automatic_dynamic_correction_applied") is False
                and performance.get("correction_factor_output") is None,
                {
                    "applied": performance.get("automatic_dynamic_correction_applied"),
                    "correction_factor_output": performance.get("correction_factor_output"),
                },
                {"applied": False, "correction_factor_output": None},
            ),
            _gate(
                "not_formal_metrological_uncertainty",
                performance.get("formal_metrological_uncertainty") is False,
                performance.get("formal_metrological_uncertainty"),
                False,
            ),
        ]
        for gate in channel_gates:
            gate["gas"] = gas
            gate["analyzer_id"] = analyzer_id
        gates.extend(channel_gates)
        channels.append(
            {
                "gas": gas,
                "analyzer_id": analyzer_id,
                "status": "pass" if all(bool(item.get("passed")) for item in channel_gates) else "fail",
                "gates": channel_gates,
            }
        )
    passed = bool(rows) and all(bool(item.get("passed")) for item in gates)
    return {
        "artifact_type": "gas_analyzer_dynamic_uncertainty_acceptance",
        "artifact_role": "diagnostic_analysis",
        "schema_version": str(
            contract.get("schema_version")
            or "gas_analyzer_dynamic_uncertainty_contract_v1"
        ),
        "protocol_id": str(protocol_id or ""),
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "acceptance_scope": "gas_analyzer_dynamic_uncertainty_fixture",
        "promotion_state": "blocked",
        "ready_for_promotion": False,
        "static_calibration_status": "not_evaluated",
        "gas_analyzer_dynamic_status": (
            "simulation_dynamic_uncertainty_pass"
            if passed
            else "simulation_dynamic_uncertainty_fail"
        ),
        "ec_flux_status": "not_in_scope",
        "real_acceptance_status": "blocked",
        "all_fixture_gates_passed": passed,
        "failed_gate_names": [
            f"{item.get('gas')}:{item.get('analyzer_id')}:{item.get('name')}"
            for item in gates
            if not bool(item.get("passed"))
        ],
        "required_gates": gates,
        "channels": channels,
        "boundary_note": (
            "本工件只验证气体分析仪动态带宽、延迟和离线工程不确定度计算；"
            "不包含涡动相关协谱修正、通量闭合或真实分析仪验收。"
        ),
    }


def _valid_points(values: Iterable[Mapping[str, Any]]) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    for value in values:
        frequency = _finite_or_none(value.get("frequency_hz"))
        amplitude = _finite_or_none(value.get("amplitude_ratio"))
        phase = _finite_or_none(value.get("phase_deg"))
        coherence = _finite_or_none(value.get("coherence"))
        truth_amplitude = _finite_or_none(value.get("truth_amplitude_ratio"))
        if (
            frequency is None
            or amplitude is None
            or phase is None
            or coherence is None
            or frequency <= 0.0
            or amplitude <= 0.0
        ):
            continue
        rows.append(
            {
                "frequency_hz": frequency,
                "amplitude_ratio": amplitude,
                "phase_deg": phase,
                "coherence": coherence,
                "truth_amplitude_ratio": truth_amplitude,
            }
        )
    return sorted(rows, key=lambda item: item["frequency_hz"])


def _point_uncertainty_budget(
    point: Mapping[str, float],
    *,
    uncertainty_model: Mapping[str, Any],
    effective_average_count: float,
    coverage_factor: float,
) -> dict[str, Any]:
    frequency = float(point["frequency_hz"])
    coherence = min(1.0, max(_EPSILON, float(point["coherence"])))
    random_standard = sqrt(
        max(0.0, 1.0 - coherence)
        / max(2.0 * coherence * effective_average_count, _EPSILON)
    )
    reference_amplitude = _nonnegative(
        uncertainty_model.get("reference_amplitude_relative_standard_uncertainty")
    )
    leakage_amplitude = _nonnegative(
        uncertainty_model.get("spectral_leakage_amplitude_relative_standard_uncertainty")
    )
    combined_amplitude = sqrt(
        random_standard**2
        + reference_amplitude**2
        + leakage_amplitude**2
    )
    reference_phase = _nonnegative(
        uncertainty_model.get("reference_phase_standard_uncertainty_deg")
    )
    clock_timing_s = _nonnegative(
        uncertainty_model.get("clock_timing_standard_uncertainty_s")
    )
    clock_phase_deg = 360.0 * frequency * clock_timing_s
    leakage_phase = _nonnegative(
        uncertainty_model.get("spectral_leakage_phase_standard_uncertainty_deg")
    )
    random_phase_deg = random_standard * 180.0 / pi
    combined_phase = sqrt(
        random_phase_deg**2
        + reference_phase**2
        + clock_phase_deg**2
        + leakage_phase**2
    )
    return {
        "frequency_hz": frequency,
        "coherence": coherence,
        "components": [
            {
                "component": "welch_coherence_random",
                "amplitude_relative_standard_uncertainty": round(random_standard, 9),
                "phase_standard_uncertainty_deg": round(random_phase_deg, 9),
            },
            {
                "component": "upstream_reference_amplitude",
                "amplitude_relative_standard_uncertainty": round(reference_amplitude, 9),
                "phase_standard_uncertainty_deg": 0.0,
            },
            {
                "component": "upstream_reference_phase",
                "amplitude_relative_standard_uncertainty": 0.0,
                "phase_standard_uncertainty_deg": round(reference_phase, 9),
            },
            {
                "component": "shared_clock_timing",
                "amplitude_relative_standard_uncertainty": 0.0,
                "phase_standard_uncertainty_deg": round(clock_phase_deg, 9),
            },
            {
                "component": "spectral_leakage",
                "amplitude_relative_standard_uncertainty": round(leakage_amplitude, 9),
                "phase_standard_uncertainty_deg": round(leakage_phase, 9),
            },
        ],
        "combined_amplitude_relative_standard_uncertainty": round(combined_amplitude, 9),
        "expanded_amplitude_relative_uncertainty": round(
            coverage_factor * combined_amplitude,
            9,
        ),
        "combined_phase_standard_uncertainty_deg": round(combined_phase, 9),
        "expanded_phase_uncertainty_deg": round(
            coverage_factor * combined_phase,
            9,
        ),
    }


def _bandwidth_at_threshold(
    points: list[dict[str, float]],
    *,
    threshold: float,
    amplitude_field: str,
) -> dict[str, Any]:
    previous_frequency = 0.0
    previous_amplitude = 1.0
    for index, point in enumerate(points):
        frequency = float(point["frequency_hz"])
        amplitude = _finite_or_none(point.get(amplitude_field))
        if amplitude is None:
            return {
                "frequency_hz": None,
                "censored": False,
                "lower_index": None,
                "upper_index": None,
                "local_slope_per_hz": None,
            }
        if amplitude < threshold:
            slope = (amplitude - previous_amplitude) / max(
                frequency - previous_frequency,
                _EPSILON,
            )
            crossing = (
                previous_frequency
                if abs(slope) <= _EPSILON
                else previous_frequency + (threshold - previous_amplitude) / slope
            )
            crossing = min(frequency, max(previous_frequency, crossing))
            return {
                "frequency_hz": round(crossing, 9),
                "censored": False,
                "lower_index": index - 1,
                "upper_index": index,
                "local_slope_per_hz": round(slope, 9),
            }
        previous_frequency = frequency
        previous_amplitude = amplitude
    return {
        "frequency_hz": points[-1]["frequency_hz"],
        "censored": True,
        "lower_index": len(points) - 1,
        "upper_index": None,
        "local_slope_per_hz": None,
    }


def _bandwidth_uncertainty(
    bandwidth: Mapping[str, Any],
    *,
    point_budgets: list[dict[str, Any]],
    threshold: float,
    frequency_resolution_hz: float | None,
    bin_standard_uncertainty_divisor: float,
    coverage_factor: float,
) -> dict[str, Any]:
    if bool(bandwidth.get("censored")):
        return {
            "status": "censored_at_evaluation_limit",
            "combined_standard_uncertainty_hz": None,
            "expanded_uncertainty_hz": None,
        }
    slope = _finite_or_none(bandwidth.get("local_slope_per_hz"))
    upper_index = bandwidth.get("upper_index")
    if (
        slope is None
        or abs(slope) <= _EPSILON
        or not isinstance(upper_index, int)
        or upper_index < 0
        or upper_index >= len(point_budgets)
    ):
        return {
            "status": "insufficient_local_slope",
            "combined_standard_uncertainty_hz": None,
            "expanded_uncertainty_hz": None,
        }
    point_standard = float(
        point_budgets[upper_index][
            "combined_amplitude_relative_standard_uncertainty"
        ]
    )
    amplitude_component_hz = threshold * point_standard / abs(slope)
    bin_component_hz = (
        0.0
        if frequency_resolution_hz is None
        else frequency_resolution_hz / bin_standard_uncertainty_divisor
    )
    combined = sqrt(amplitude_component_hz**2 + bin_component_hz**2)
    return {
        "status": "estimated",
        "amplitude_component_standard_uncertainty_hz": round(amplitude_component_hz, 9),
        "frequency_bin_standard_uncertainty_hz": round(bin_component_hz, 9),
        "combined_standard_uncertainty_hz": round(combined, 9),
        "expanded_uncertainty_hz": round(coverage_factor * combined, 9),
        "coverage_factor": coverage_factor,
    }


def _phase_delay_rows(points: list[dict[str, float]]) -> list[dict[str, float]]:
    frequencies = np.asarray([item["frequency_hz"] for item in points], dtype=float)
    phases = np.unwrap(
        np.deg2rad(np.asarray([item["phase_deg"] for item in points], dtype=float))
    )
    rows: list[dict[str, float]] = []
    for frequency, phase in zip(frequencies, phases):
        rows.append(
            {
                "frequency_hz": round(float(frequency), 9),
                "unwrapped_phase_deg": round(float(np.rad2deg(phase)), 9),
                "effective_phase_delay_s": round(
                    float(-phase / (2.0 * pi * frequency)),
                    9,
                ),
            }
        )
    return rows


def _top_contributors(point_budgets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    maxima: dict[str, dict[str, float]] = {}
    for point in point_budgets:
        for component in list(point.get("components") or []):
            name = str(component.get("component") or "")
            if not name:
                continue
            row = maxima.setdefault(
                name,
                {
                    "amplitude_relative_standard_uncertainty": 0.0,
                    "phase_standard_uncertainty_deg": 0.0,
                },
            )
            row["amplitude_relative_standard_uncertainty"] = max(
                row["amplitude_relative_standard_uncertainty"],
                _nonnegative(component.get("amplitude_relative_standard_uncertainty")),
            )
            row["phase_standard_uncertainty_deg"] = max(
                row["phase_standard_uncertainty_deg"],
                _nonnegative(component.get("phase_standard_uncertainty_deg")),
            )
    return sorted(
        (
            {
                "component": name,
                "max_amplitude_relative_standard_uncertainty": round(
                    values["amplitude_relative_standard_uncertainty"],
                    9,
                ),
                "max_phase_standard_uncertainty_deg": round(
                    values["phase_standard_uncertainty_deg"],
                    9,
                ),
            }
            for name, values in maxima.items()
        ),
        key=lambda item: max(
            item["max_amplitude_relative_standard_uncertainty"],
            item["max_phase_standard_uncertainty_deg"] / 180.0,
        ),
        reverse=True,
    )


def _frequency_resolution(points: list[dict[str, float]]) -> float | None:
    if len(points) < 2:
        return None
    deltas = np.diff(np.asarray([item["frequency_hz"] for item in points], dtype=float))
    return round(float(np.median(deltas)), 9)


def _relative_error(value: Any, reference: Any) -> float | None:
    left = _finite_or_none(value)
    right = _finite_or_none(reference)
    if left is None or right is None or abs(right) <= _EPSILON:
        return None
    return round(abs(left - right) / abs(right), 9)


def _relative_uncertainty(value: Any, reference: Any) -> float | None:
    numeric = _finite_or_none(value)
    denominator = _finite_or_none(reference)
    if numeric is None or denominator is None or abs(denominator) <= _EPSILON:
        return None
    return round(abs(numeric) / abs(denominator), 9)


def _gate(name: str, passed: bool, value: Any, limit: Any) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "value": value,
        "limit": limit,
    }


def _min_gate(name: str, value: Any, minimum: Any) -> dict[str, Any]:
    numeric = _finite_or_none(value)
    threshold = _finite_or_none(minimum)
    return _gate(
        name,
        numeric is not None and threshold is not None and numeric >= threshold,
        numeric,
        {"minimum": threshold},
    )


def _max_gate(name: str, value: Any, maximum: Any) -> dict[str, Any]:
    numeric = _finite_or_none(value)
    threshold = _finite_or_none(maximum)
    return _gate(
        name,
        numeric is not None and threshold is not None and numeric <= threshold,
        numeric,
        {"maximum": threshold},
    )


def _max_or_none(values: Iterable[Any]) -> float | None:
    finite = [item for item in (_finite_or_none(value) for value in values) if item is not None]
    return None if not finite else max(finite)


def _min_or_none(values: Iterable[Any]) -> float | None:
    finite = [item for item in (_finite_or_none(value) for value in values) if item is not None]
    return None if not finite else min(finite)


def _median_or_none(values: Iterable[Any]) -> float | None:
    finite = [item for item in (_finite_or_none(value) for value in values) if item is not None]
    return None if not finite else round(float(np.median(finite)), 9)


def _positive(value: Any, *, default: float) -> float:
    numeric = _finite_or_none(value)
    return default if numeric is None or numeric <= 0.0 else numeric


def _nonnegative(value: Any) -> float:
    numeric = _finite_or_none(value)
    return 0.0 if numeric is None else max(0.0, numeric)


def _finite_or_none(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if isfinite(numeric) else None


__all__ = [
    "analyze_gas_analyzer_dynamic_performance",
    "build_gas_analyzer_dynamic_uncertainty_acceptance",
]
