"""Integrated gas-analyzer static, environmental, and dynamic envelope analysis."""

from __future__ import annotations

from collections import defaultdict
from itertools import product
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from gas_calibrator.utils.converters import finite_float as _finite_or_none


_EPSILON = 1e-12


def analyze_gas_analyzer_operating_envelope(
    measurement_rows: Iterable[Mapping[str, Any]],
    interference_rows: Iterable[Mapping[str, Any]],
    dynamic_performances: Iterable[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    measurements = [dict(item) for item in measurement_rows]
    interference = [dict(item) for item in interference_rows]
    dynamic_rows = [dict(item) for item in dynamic_performances]
    dynamics = {str(item.get("gas") or "").lower(): dict(item) for item in dynamic_rows}
    supported_gases = {"co2", "h2o"}
    unexpected_measurement_gas_row_count = sum(
        1
        for item in measurements
        if str(item.get("gas") or "").lower() not in supported_gases
    )
    unexpected_interference_gas_row_count = sum(
        1
        for item in interference
        if str(item.get("gas") or "").lower() not in supported_gases
    )
    dynamic_gas_counts: dict[str, int] = defaultdict(int)
    for item in dynamic_rows:
        dynamic_gas_counts[str(item.get("gas") or "").lower()] += 1
    unexpected_dynamic_gases = sorted(set(dynamic_gas_counts) - supported_gases)
    duplicate_dynamic_gas_count = sum(
        max(0, count - 1) for count in dynamic_gas_counts.values()
    )
    gas_contracts = dict(contract.get("gas_contracts") or {})
    interference_contracts = dict(contract.get("interference_contracts") or {})
    common_limits = dict(contract.get("common_fixture_limits") or {})
    grid = dict(contract.get("environment_grid") or {})
    required_fields = [
        str(item) for item in list(contract.get("required_measurement_fields") or [])
    ]
    required_interference_fields = [
        str(item) for item in list(contract.get("required_interference_fields") or [])
    ]
    channels: list[dict[str, Any]] = []
    for gas in ("co2", "h2o"):
        gas_rows = [
            item for item in measurements if str(item.get("gas") or "").lower() == gas
        ]
        gas_interference = [
            item for item in interference if str(item.get("gas") or "").lower() == gas
        ]
        channels.append(
            _analyze_gas_channel(
                gas,
                gas_rows,
                gas_interference,
                dynamics.get(gas, {}),
                gas_contract=dict(gas_contracts.get(gas) or {}),
                interference_contract=dict(interference_contracts.get(gas) or {}),
                common_limits=common_limits,
                grid=grid,
                required_fields=required_fields,
                required_interference_fields=required_interference_fields,
            )
        )
    observed_anchor_roles = {
        str(channel.get("anchor_role") or "")
        for channel in channels
        if str(channel.get("anchor_role") or "")
    }
    anchors_separated = observed_anchor_roles == {
        "co2_zero_gas",
        "h2o_dry_gas",
    }
    return {
        "artifact_type": "gas_analyzer_operating_envelope",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "gas_analyzer_operating_envelope_v1",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "gas_analyzer_only": True,
        "ec_flux_in_scope": False,
        "unexpected_measurement_gas_row_count": (unexpected_measurement_gas_row_count),
        "unexpected_interference_gas_row_count": (
            unexpected_interference_gas_row_count
        ),
        "unexpected_dynamic_gases": unexpected_dynamic_gases,
        "duplicate_dynamic_gas_count": duplicate_dynamic_gas_count,
        "coefficient_fitting_in_scope": False,
        "coefficient_writeback_in_scope": False,
        "automatic_dynamic_correction_applied": False,
        "co2_zero_gas_and_h2o_dry_gas_separated": anchors_separated,
        "observed_anchor_roles": sorted(observed_anchor_roles),
        "channels": channels,
        "status": (
            "ok"
            if len(channels) == 2
            and all(channel.get("status") == "ok" for channel in channels)
            else "incomplete"
        ),
    }


def build_gas_analyzer_operating_envelope_acceptance(
    envelope: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    protocol_id: str,
) -> dict[str, Any]:
    common = dict(contract.get("common_fixture_limits") or {})
    gas_contracts = dict(contract.get("gas_contracts") or {})
    gates: list[dict[str, Any]] = [
        _gate(
            "analysis_complete",
            envelope.get("status") == "ok",
            envelope.get("status"),
            "ok",
        ),
        _gate(
            "gas_analyzer_only_scope",
            envelope.get("gas_analyzer_only") is True
            and envelope.get("ec_flux_in_scope") is False,
            {
                "gas_analyzer_only": envelope.get("gas_analyzer_only"),
                "ec_flux_in_scope": envelope.get("ec_flux_in_scope"),
            },
            {"gas_analyzer_only": True, "ec_flux_in_scope": False},
        ),
        _gate(
            "supported_gas_scope_only",
            int(envelope.get("unexpected_measurement_gas_row_count") or 0) == 0
            and int(envelope.get("unexpected_interference_gas_row_count") or 0) == 0
            and not list(envelope.get("unexpected_dynamic_gases") or [])
            and int(envelope.get("duplicate_dynamic_gas_count") or 0) == 0,
            {
                "unexpected_measurements": envelope.get(
                    "unexpected_measurement_gas_row_count"
                ),
                "unexpected_interference": envelope.get(
                    "unexpected_interference_gas_row_count"
                ),
                "unexpected_dynamic_gases": list(
                    envelope.get("unexpected_dynamic_gases") or []
                ),
                "duplicate_dynamic_gases": envelope.get("duplicate_dynamic_gas_count"),
            },
            {
                "unexpected_measurements": 0,
                "unexpected_interference": 0,
                "unexpected_dynamic_gases": [],
                "duplicate_dynamic_gases": 0,
            },
        ),
        _gate(
            "co2_zero_and_h2o_dry_anchor_separation",
            envelope.get("co2_zero_gas_and_h2o_dry_gas_separated") is True,
            envelope.get("observed_anchor_roles"),
            ["co2_zero_gas", "h2o_dry_gas"],
        ),
        _gate(
            "no_coefficient_fit_or_writeback",
            envelope.get("coefficient_fitting_in_scope") is False
            and envelope.get("coefficient_writeback_in_scope") is False,
            {
                "fitting": envelope.get("coefficient_fitting_in_scope"),
                "writeback": envelope.get("coefficient_writeback_in_scope"),
            },
            {"fitting": False, "writeback": False},
        ),
        _gate(
            "no_automatic_dynamic_correction",
            envelope.get("automatic_dynamic_correction_applied") is False,
            envelope.get("automatic_dynamic_correction_applied"),
            False,
        ),
    ]
    static_gates = [
        gate for gate in gates if gate["name"] != "no_automatic_dynamic_correction"
    ]
    dynamic_gates = [
        gate
        for gate in gates
        if gate["name"]
        in {
            "analysis_complete",
            "gas_analyzer_only_scope",
            "supported_gas_scope_only",
            "no_coefficient_fit_or_writeback",
            "no_automatic_dynamic_correction",
        }
    ]
    dynamic_gate_names = {
        "dynamic_dependency_complete",
        "dynamic_analyzer_identity_match",
        "ten_percent_bandwidth_hz",
        "low_frequency_effective_phase_delay_s",
        "dynamic_decision_band_qualified",
    }
    channel_results: list[dict[str, Any]] = []
    for channel in list(envelope.get("channels") or []):
        gas = str(channel.get("gas") or "").lower()
        analyzer_id = str(channel.get("analyzer_id") or "")
        limits = {**common, **dict(gas_contracts.get(gas) or {})}
        metrics = dict(channel.get("metrics") or {})
        coverage = dict(channel.get("coverage") or {})
        interference_coverage = dict(channel.get("interference_coverage") or {})
        dynamic = dict(channel.get("dynamic_performance") or {})
        qualified = dict(channel.get("qualified_operating_envelope") or {})
        channel_gates = [
            _gate(
                "channel_complete",
                channel.get("status") == "ok",
                channel.get("status"),
                "ok",
            ),
            _gate(
                "analyzer_identity_unambiguous",
                channel.get("analyzer_identity_valid") is True,
                {
                    "measurement_ids": list(channel.get("analyzer_ids") or []),
                    "interference_ids": list(
                        channel.get("interference_analyzer_ids") or []
                    ),
                },
                {"one_common_analyzer_id": True},
            ),
            _gate(
                "required_fields_complete",
                not list(channel.get("missing_required_fields") or []),
                {"missing": list(channel.get("missing_required_fields") or [])},
                {"missing": []},
            ),
            _gate(
                "reference_quality_healthy",
                int(channel.get("degraded_reference_row_count") or 0) == 0,
                channel.get("degraded_reference_row_count"),
                0,
            ),
            _gate(
                "measurement_frames_usable",
                int(channel.get("unusable_measurement_row_count") or 0) == 0,
                channel.get("unusable_measurement_row_count"),
                0,
            ),
            _gate(
                "measurement_numeric_values_finite",
                int(channel.get("invalid_numeric_measurement_row_count") or 0) == 0,
                channel.get("invalid_numeric_measurement_row_count"),
                0,
            ),
            _gate(
                "interference_fields_complete",
                not list(channel.get("missing_interference_fields") or []),
                {"missing": list(channel.get("missing_interference_fields") or [])},
                {"missing": []},
            ),
            _gate(
                "interference_reference_quality_healthy",
                int(channel.get("degraded_interference_reference_row_count") or 0) == 0,
                channel.get("degraded_interference_reference_row_count"),
                0,
            ),
            _gate(
                "interference_frames_usable",
                int(channel.get("unusable_interference_row_count") or 0) == 0,
                channel.get("unusable_interference_row_count"),
                0,
            ),
            _gate(
                "interference_numeric_values_finite",
                int(channel.get("invalid_numeric_interference_row_count") or 0) == 0,
                channel.get("invalid_numeric_interference_row_count"),
                0,
            ),
            _gate(
                "complete_interference_sweep",
                int(interference_coverage.get("missing_row_count") or 0) == 0
                and int(interference_coverage.get("duplicate_row_count") or 0) == 0
                and int(interference_coverage.get("unexpected_row_count") or 0) == 0,
                {
                    "missing": interference_coverage.get("missing_row_count"),
                    "duplicates": interference_coverage.get("duplicate_row_count"),
                    "unexpected": interference_coverage.get("unexpected_row_count"),
                },
                {"missing": 0, "duplicates": 0, "unexpected": 0},
            ),
            _gate(
                "complete_rectangular_grid",
                int(coverage.get("missing_row_count") or 0) == 0
                and int(coverage.get("duplicate_row_count") or 0) == 0
                and int(coverage.get("unexpected_row_count") or 0) == 0,
                {
                    "missing": coverage.get("missing_row_count"),
                    "duplicates": coverage.get("duplicate_row_count"),
                    "unexpected": coverage.get("unexpected_row_count"),
                },
                {"missing": 0, "duplicates": 0, "unexpected": 0},
            ),
            _gate(
                "anchor_role_and_target",
                channel.get("anchor_contract_valid") is True,
                {
                    "role": channel.get("anchor_role"),
                    "target": channel.get("anchor_target"),
                },
                {
                    "role": limits.get("anchor_role"),
                    "target": limits.get("anchor_target"),
                },
            ),
            _max_gate(
                "anchor_absolute_error",
                metrics.get("max_anchor_absolute_error"),
                limits.get("max_anchor_absolute_error"),
            ),
            _max_gate(
                "span_normalized_error",
                metrics.get("max_span_normalized_error"),
                limits.get("max_span_normalized_error"),
            ),
            _max_gate(
                "repeatability",
                metrics.get("max_repeatability_sigma_span_fraction"),
                limits.get("max_repeatability_sigma_span_fraction"),
            ),
            _max_gate(
                "hysteresis",
                metrics.get("max_hysteresis_span_fraction"),
                limits.get("max_hysteresis_span_fraction"),
            ),
            _max_gate(
                "drift",
                metrics.get("max_drift_span_fraction"),
                limits.get("max_drift_span_fraction"),
            ),
            _max_gate(
                "linearity_residual",
                metrics.get("max_linearity_residual_span_fraction"),
                limits.get("max_linearity_residual_span_fraction"),
            ),
            _max_gate(
                "pressure_sensitivity",
                metrics.get("max_pressure_sensitivity_span_fraction_per_100_hpa"),
                limits.get("max_pressure_sensitivity_span_fraction_per_100_hpa"),
            ),
            _max_gate(
                "temperature_sensitivity",
                metrics.get("max_temperature_sensitivity_span_fraction_per_10_c"),
                limits.get("max_temperature_sensitivity_span_fraction_per_10_c"),
            ),
            _max_gate(
                "flow_sensitivity",
                metrics.get("max_flow_sensitivity_span_fraction_per_slpm"),
                limits.get("max_flow_sensitivity_span_fraction_per_slpm"),
            ),
            _max_gate(
                "interference_effect",
                metrics.get("max_interference_effect_span_fraction"),
                limits.get("max_interference_effect_span_fraction"),
            ),
            _gate(
                "all_environment_cells_qualified",
                int(qualified.get("failed_cell_count") or 0) == 0,
                {
                    "failed": qualified.get("failed_cell_count"),
                    "total": qualified.get("total_cell_count"),
                },
                {"failed": 0},
            ),
            _gate(
                "dynamic_dependency_complete",
                dynamic.get("status") == "ok",
                dynamic.get("status"),
                "ok",
            ),
            _gate(
                "dynamic_analyzer_identity_match",
                str(dynamic.get("analyzer_id") or "") == analyzer_id
                and bool(analyzer_id),
                dynamic.get("analyzer_id"),
                analyzer_id,
            ),
            _min_gate(
                "ten_percent_bandwidth_hz",
                dynamic.get("usable_bandwidth_hz"),
                limits.get("min_ten_percent_bandwidth_hz"),
            ),
            _max_gate(
                "low_frequency_effective_phase_delay_s",
                dynamic.get("low_frequency_effective_phase_delay_s"),
                limits.get("max_low_frequency_effective_phase_delay_s"),
            ),
            _gate(
                "dynamic_decision_band_qualified",
                dict(
                    dict(dynamic.get("bandwidths") or {}).get("ten_percent_attenuation")
                    or {}
                ).get("decision_grade")
                == "qualified",
                dict(
                    dict(dynamic.get("bandwidths") or {}).get("ten_percent_attenuation")
                    or {}
                ).get("decision_grade"),
                "qualified",
            ),
        ]
        for gate in channel_gates:
            gate["gas"] = gas
            gate["analyzer_id"] = analyzer_id
        gates.extend(channel_gates)
        static_gates.extend(
            gate for gate in channel_gates if gate["name"] not in dynamic_gate_names
        )
        dynamic_gates.extend(
            gate for gate in channel_gates if gate["name"] in dynamic_gate_names
        )
        channel_results.append(
            {
                "gas": gas,
                "analyzer_id": analyzer_id,
                "status": (
                    "pass"
                    if all(bool(item.get("passed")) for item in channel_gates)
                    else "fail"
                ),
                "gates": channel_gates,
            }
        )
    passed = bool(channel_results) and all(bool(item.get("passed")) for item in gates)
    static_passed = bool(channel_results) and all(
        bool(item.get("passed")) for item in static_gates
    )
    dynamic_passed = bool(channel_results) and all(
        bool(item.get("passed")) for item in dynamic_gates
    )
    qualified_envelopes = {
        str(channel.get("gas") or ""): dict(
            channel.get("qualified_operating_envelope") or {}
        )
        for channel in list(envelope.get("channels") or [])
    }
    return {
        "artifact_type": "gas_analyzer_operating_envelope_acceptance",
        "artifact_role": "diagnostic_analysis",
        "schema_version": str(
            contract.get("schema_version")
            or "gas_analyzer_operating_envelope_contract_v1"
        ),
        "protocol_id": str(protocol_id or ""),
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "acceptance_scope": "gas_analyzer_integrated_operating_envelope_fixture",
        "promotion_state": "blocked",
        "ready_for_promotion": False,
        "static_calibration_status": (
            "simulation_static_envelope_pass"
            if static_passed
            else "simulation_static_envelope_fail"
        ),
        "gas_analyzer_dynamic_status": (
            "simulation_dynamic_dependency_pass"
            if dynamic_passed
            else "simulation_dynamic_dependency_fail"
        ),
        "operating_envelope_status": (
            "simulation_operating_envelope_pass"
            if passed
            else "simulation_operating_envelope_fail"
        ),
        "ec_flux_status": "not_in_scope",
        "real_acceptance_status": "blocked",
        "all_fixture_gates_passed": passed,
        "failed_gate_names": [
            f"{item.get('gas', 'global')}:{item.get('analyzer_id', 'all')}:{item.get('name')}"
            for item in gates
            if not bool(item.get("passed"))
        ],
        "required_gates": gates,
        "channels": channel_results,
        "qualified_operating_envelopes": qualified_envelopes,
        "boundary_note": (
            "本工件只整合气体分析仪静态、环境、干扰和动态性能的 simulation fixture；"
            "不拟合或写入系数，不代表真实产品规格或 EC 通量验收。"
        ),
    }


def _analyze_gas_channel(
    gas: str,
    rows: list[dict[str, Any]],
    interference_rows: list[dict[str, Any]],
    dynamic: dict[str, Any],
    *,
    gas_contract: dict[str, Any],
    interference_contract: dict[str, Any],
    common_limits: dict[str, Any],
    grid: dict[str, Any],
    required_fields: list[str],
    required_interference_fields: list[str],
) -> dict[str, Any]:
    measurement_numeric_fields = (
        "temperature_c",
        "pressure_hpa",
        "flow_slpm",
        "target_value",
        "reference_value",
        "measured_value",
        "replicate",
    )
    interference_numeric_fields = (
        "interferent_value",
        "target_value",
        "reference_value",
        "measured_value",
        "replicate",
    )
    missing_fields = sorted(
        {
            field
            for row in rows
            for field in required_fields
            if field not in row or row.get(field) is None
        }
    )
    invalid_numeric_measurement_count = sum(
        1
        for row in rows
        if any(
            _finite_or_none(row.get(field)) is None
            for field in measurement_numeric_fields
        )
    )
    usable = [
        row
        for row in rows
        if row.get("frame_usable") is True
        and all(
            _finite_or_none(row.get(field)) is not None
            for field in measurement_numeric_fields
        )
    ]
    missing_interference_fields = sorted(
        {
            field
            for row in interference_rows
            for field in required_interference_fields
            if field not in row or row.get(field) is None
        }
    )
    invalid_numeric_interference_count = sum(
        1
        for row in interference_rows
        if any(
            _finite_or_none(row.get(field)) is None
            for field in interference_numeric_fields
        )
    )
    usable_interference = [
        row
        for row in interference_rows
        if row.get("frame_usable") is True
        and str(row.get("reference_quality") or "") == "healthy"
        and all(
            _finite_or_none(row.get(field)) is not None
            for field in interference_numeric_fields
        )
    ]
    degraded_interference_reference_count = sum(
        1
        for row in interference_rows
        if str(row.get("reference_quality") or "") != "healthy"
    )
    unusable_interference_count = sum(
        1 for row in interference_rows if row.get("frame_usable") is not True
    )
    degraded_reference_count = sum(
        1 for row in rows if str(row.get("reference_quality") or "") != "healthy"
    )
    unusable_measurement_count = sum(
        1 for row in rows if row.get("frame_usable") is not True
    )
    span = _positive(gas_contract.get("span_value"))
    anchor_role = str(gas_contract.get("anchor_role") or "")
    anchor_target = _finite_or_none(gas_contract.get("anchor_target"))
    observed_anchor_rows = [
        row for row in usable if str(row.get("anchor_role") or "") == anchor_role
    ]
    anchor_target_rows = [
        row for row in usable if _close(row.get("target_value"), anchor_target)
    ]
    wrong_anchor_rows = [
        row
        for row in usable
        if str(row.get("anchor_role") or "")
        and str(row.get("anchor_role") or "") != anchor_role
    ]
    anchor_contract_valid = (
        bool(anchor_target_rows)
        and not wrong_anchor_rows
        and all(
            str(row.get("anchor_role") or "") == anchor_role
            for row in anchor_target_rows
        )
        and all(
            _close(row.get("target_value"), anchor_target)
            for row in observed_anchor_rows
        )
    )
    expected_keys = _expected_measurement_keys(gas, gas_contract, grid)
    observed_counts: dict[tuple[Any, ...], int] = defaultdict(int)
    for row in rows:
        observed_counts[_measurement_key(row)] += 1
    missing_keys = sorted(expected_keys - set(observed_counts))
    duplicate_count = sum(max(0, count - 1) for count in observed_counts.values())
    unexpected_count = len(set(observed_counts) - expected_keys)
    expected_interference_keys = _expected_interference_keys(
        gas,
        interference_contract,
    )
    observed_interference_counts: dict[tuple[Any, ...], int] = defaultdict(int)
    for row in interference_rows:
        observed_interference_counts[_interference_key(row)] += 1
    missing_interference_keys = sorted(
        expected_interference_keys - set(observed_interference_counts)
    )
    duplicate_interference_count = sum(
        max(0, count - 1) for count in observed_interference_counts.values()
    )
    unexpected_interference_count = len(
        set(observed_interference_counts) - expected_interference_keys
    )
    metrics = _static_metrics(
        usable,
        usable_interference,
        span=span,
        grid=grid,
    )
    cells = _environment_cells(
        usable,
        span=span,
        anchor_role=anchor_role,
        anchor_limit=_positive(gas_contract.get("max_anchor_absolute_error")),
        max_span_error=_positive(common_limits.get("max_span_normalized_error")),
        grid=grid,
    )
    failed_cells = [item for item in cells if item["status"] != "qualified"]
    analyzer_ids = sorted(
        {
            str(row.get("analyzer_id") or "")
            for row in rows
            if str(row.get("analyzer_id") or "")
        }
    )
    interference_analyzer_ids = sorted(
        {
            str(row.get("analyzer_id") or "")
            for row in interference_rows
            if str(row.get("analyzer_id") or "")
        }
    )
    analyzer_identity_valid = (
        len(analyzer_ids) == 1 and interference_analyzer_ids == analyzer_ids
    )
    envelope_status = (
        "qualified"
        if not missing_keys
        and duplicate_count == 0
        and unexpected_count == 0
        and not failed_cells
        else "incomplete_or_unqualified"
    )
    return {
        "gas": gas,
        "analyzer_id": analyzer_ids[0] if len(analyzer_ids) == 1 else "",
        "analyzer_ids": analyzer_ids,
        "interference_analyzer_ids": interference_analyzer_ids,
        "analyzer_identity_valid": analyzer_identity_valid,
        "status": "ok" if rows and not missing_fields else "incomplete",
        "missing_required_fields": missing_fields,
        "degraded_reference_row_count": degraded_reference_count,
        "unusable_measurement_row_count": unusable_measurement_count,
        "invalid_numeric_measurement_row_count": (invalid_numeric_measurement_count),
        "missing_interference_fields": missing_interference_fields,
        "degraded_interference_reference_row_count": (
            degraded_interference_reference_count
        ),
        "unusable_interference_row_count": unusable_interference_count,
        "invalid_numeric_interference_row_count": (invalid_numeric_interference_count),
        "anchor_role": anchor_role,
        "anchor_target": anchor_target,
        "anchor_contract_valid": anchor_contract_valid,
        "coverage": {
            "expected_row_count": len(expected_keys),
            "observed_unique_row_count": len(observed_counts),
            "missing_row_count": len(missing_keys),
            "duplicate_row_count": duplicate_count,
            "unexpected_row_count": unexpected_count,
            "missing_key_preview": [list(item) for item in missing_keys[:10]],
        },
        "interference_coverage": {
            "expected_row_count": len(expected_interference_keys),
            "observed_unique_row_count": len(observed_interference_counts),
            "missing_row_count": len(missing_interference_keys),
            "duplicate_row_count": duplicate_interference_count,
            "unexpected_row_count": unexpected_interference_count,
            "missing_key_preview": [
                list(item) for item in missing_interference_keys[:10]
            ],
        },
        "metrics": metrics,
        "environment_cells": cells,
        "qualified_operating_envelope": {
            "status": envelope_status,
            "temperature_range_c": _range(grid.get("temperatures_c") or []),
            "pressure_range_hpa": _range(grid.get("pressures_hpa") or []),
            "flow_range_slpm": _range(grid.get("flows_slpm") or []),
            "target_range": _range(gas_contract.get("target_levels") or []),
            "total_cell_count": len(cells),
            "qualified_cell_count": len(cells) - len(failed_cells),
            "failed_cell_count": len(failed_cells),
            "failed_cell_preview": failed_cells[:10],
            "rectangular_grid_complete": (
                not missing_keys and duplicate_count == 0 and unexpected_count == 0
            ),
        },
        "dynamic_performance": dynamic,
    }


def _static_metrics(
    rows: list[dict[str, Any]],
    interference_rows: list[dict[str, Any]],
    *,
    span: float,
    grid: dict[str, Any],
) -> dict[str, Any]:
    errors = [
        abs(float(row["measured_value"]) - float(row["reference_value"]))
        for row in rows
        if _finite_or_none(row.get("measured_value")) is not None
        and _finite_or_none(row.get("reference_value")) is not None
    ]
    anchor_errors = [
        abs(float(row["measured_value"]) - float(row["reference_value"]))
        for row in rows
        if str(row.get("anchor_role") or "")
    ]
    repeatability = _group_standard_deviation(
        rows,
        group_fields=(
            "temperature_c",
            "pressure_hpa",
            "flow_slpm",
            "target_value",
            "sequence_direction",
            "session",
        ),
        span=span,
    )
    hysteresis = _paired_difference(
        rows,
        pair_field="sequence_direction",
        left_value="ascending",
        right_value="descending",
        group_fields=(
            "temperature_c",
            "pressure_hpa",
            "flow_slpm",
            "target_value",
            "session",
        ),
        span=span,
    )
    drift = _paired_difference(
        rows,
        pair_field="session",
        left_value="start",
        right_value="end",
        group_fields=(
            "temperature_c",
            "pressure_hpa",
            "flow_slpm",
            "target_value",
            "sequence_direction",
        ),
        span=span,
    )
    nominal_rows = [
        row
        for row in rows
        if _close(row.get("temperature_c"), 25.0)
        and _close(row.get("pressure_hpa"), 1000.0)
        and _close(row.get("flow_slpm"), 12.0)
    ]
    linearity = _linearity_residual(nominal_rows, span=span)
    pressure_sensitivity = _max_axis_sensitivity(
        rows,
        axis="pressure_hpa",
        group_fields=(
            "temperature_c",
            "flow_slpm",
            "target_value",
            "sequence_direction",
            "session",
            "replicate",
        ),
        scale=100.0,
        span=span,
    )
    temperature_sensitivity = _max_axis_sensitivity(
        rows,
        axis="temperature_c",
        group_fields=(
            "pressure_hpa",
            "flow_slpm",
            "target_value",
            "sequence_direction",
            "session",
            "replicate",
        ),
        scale=10.0,
        span=span,
    )
    flow_sensitivity = _max_axis_sensitivity(
        rows,
        axis="flow_slpm",
        group_fields=(
            "temperature_c",
            "pressure_hpa",
            "target_value",
            "sequence_direction",
            "session",
            "replicate",
        ),
        scale=1.0,
        span=span,
    )
    interference = _interference_effect(interference_rows, span=span)
    return {
        "max_anchor_absolute_error": _max_or_none(anchor_errors),
        "max_span_normalized_error": _divide(_max_or_none(errors), span),
        "max_repeatability_sigma_span_fraction": repeatability,
        "max_hysteresis_span_fraction": hysteresis,
        "max_drift_span_fraction": drift,
        "max_linearity_residual_span_fraction": linearity,
        "max_pressure_sensitivity_span_fraction_per_100_hpa": pressure_sensitivity,
        "max_temperature_sensitivity_span_fraction_per_10_c": temperature_sensitivity,
        "max_flow_sensitivity_span_fraction_per_slpm": flow_sensitivity,
        "max_interference_effect_span_fraction": interference,
        "usable_measurement_row_count": len(rows),
        "interference_row_count": len(interference_rows),
        "environment_axis_count": len(list(grid.get("temperatures_c") or []))
        * len(list(grid.get("pressures_hpa") or []))
        * len(list(grid.get("flows_slpm") or [])),
    }


def _environment_cells(
    rows: list[dict[str, Any]],
    *,
    span: float,
    anchor_role: str,
    anchor_limit: float,
    max_span_error: float,
    grid: dict[str, Any],
) -> list[dict[str, Any]]:
    grouped = _group_rows(
        rows,
        ("temperature_c", "pressure_hpa", "flow_slpm"),
    )
    cells: list[dict[str, Any]] = []
    for temperature, pressure, flow in product(
        list(grid.get("temperatures_c") or []),
        list(grid.get("pressures_hpa") or []),
        list(grid.get("flows_slpm") or []),
    ):
        key = (float(temperature), float(pressure), float(flow))
        cell_rows = grouped.get(key, [])
        errors = [
            abs(float(row["measured_value"]) - float(row["reference_value"]))
            for row in cell_rows
        ]
        anchor_errors = [
            abs(float(row["measured_value"]) - float(row["reference_value"]))
            for row in cell_rows
            if str(row.get("anchor_role") or "") == anchor_role
        ]
        max_error_fraction = _divide(_max_or_none(errors), span)
        max_anchor_error = _max_or_none(anchor_errors)
        qualified = (
            bool(cell_rows)
            and max_error_fraction is not None
            and max_error_fraction <= max_span_error
            and max_anchor_error is not None
            and max_anchor_error <= anchor_limit
            and all(
                str(row.get("reference_quality") or "") == "healthy"
                for row in cell_rows
            )
            and all(row.get("frame_usable") is True for row in cell_rows)
        )
        cells.append(
            {
                "temperature_c": float(temperature),
                "pressure_hpa": float(pressure),
                "flow_slpm": float(flow),
                "row_count": len(cell_rows),
                "max_span_normalized_error": max_error_fraction,
                "max_anchor_absolute_error": max_anchor_error,
                "status": "qualified" if qualified else "unqualified",
            }
        )
    return cells


def _expected_measurement_keys(
    gas: str,
    gas_contract: Mapping[str, Any],
    grid: Mapping[str, Any],
) -> set[tuple[Any, ...]]:
    return {
        (
            gas,
            float(temperature),
            float(pressure),
            float(flow),
            float(target),
            str(direction),
            str(session),
            int(replicate),
        )
        for temperature, pressure, flow, target, direction, session, replicate in product(
            list(grid.get("temperatures_c") or []),
            list(grid.get("pressures_hpa") or []),
            list(grid.get("flows_slpm") or []),
            list(gas_contract.get("target_levels") or []),
            list(grid.get("sequence_directions") or []),
            list(grid.get("sessions") or []),
            list(grid.get("replicates") or []),
        )
    }


def _measurement_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        str(row.get("gas") or "").lower(),
        _finite_or_none(row.get("temperature_c")),
        _finite_or_none(row.get("pressure_hpa")),
        _finite_or_none(row.get("flow_slpm")),
        _finite_or_none(row.get("target_value")),
        str(row.get("sequence_direction") or ""),
        str(row.get("session") or ""),
        _integer_or_none(row.get("replicate")),
    )


def _expected_interference_keys(
    gas: str,
    interference_contract: Mapping[str, Any],
) -> set[tuple[Any, ...]]:
    return {
        (
            gas,
            str(interference_contract.get("interferent_name") or "").lower(),
            float(interferent_value),
            float(interference_contract.get("target_value")),
            int(replicate),
        )
        for interferent_value, replicate in product(
            list(interference_contract.get("interferent_values") or []),
            list(interference_contract.get("replicates") or []),
        )
    }


def _interference_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        str(row.get("gas") or "").lower(),
        str(row.get("interferent_name") or "").lower(),
        _finite_or_none(row.get("interferent_value")),
        _finite_or_none(row.get("target_value")),
        _integer_or_none(row.get("replicate")),
    )


def _group_standard_deviation(
    rows: list[dict[str, Any]],
    *,
    group_fields: Sequence[str],
    span: float,
) -> float | None:
    grouped = _group_rows(rows, group_fields)
    values = []
    for group in grouped.values():
        measurements = [
            float(row["measured_value"])
            for row in group
            if _finite_or_none(row.get("measured_value")) is not None
        ]
        if len(measurements) >= 2:
            values.append(float(np.std(measurements, ddof=1)) / span)
    return _max_or_none(values)


def _paired_difference(
    rows: list[dict[str, Any]],
    *,
    pair_field: str,
    left_value: str,
    right_value: str,
    group_fields: Sequence[str],
    span: float,
) -> float | None:
    grouped = _group_rows(rows, group_fields)
    differences = []
    for group in grouped.values():
        left = [
            float(row["measured_value"])
            for row in group
            if str(row.get(pair_field) or "") == left_value
        ]
        right = [
            float(row["measured_value"])
            for row in group
            if str(row.get(pair_field) or "") == right_value
        ]
        if left and right:
            differences.append(abs(float(np.mean(left)) - float(np.mean(right))) / span)
    return _max_or_none(differences)


def _linearity_residual(rows: list[dict[str, Any]], *, span: float) -> float | None:
    grouped = _group_rows(rows, ("target_value",))
    targets: list[float] = []
    means: list[float] = []
    for key, group in sorted(grouped.items()):
        targets.append(float(key[0]))
        means.append(float(np.mean([float(row["measured_value"]) for row in group])))
    if len(targets) < 3:
        return None
    coefficients = np.polyfit(np.asarray(targets), np.asarray(means), 1)
    fitted = np.polyval(coefficients, np.asarray(targets))
    return round(
        float(np.max(np.abs(np.asarray(means) - fitted))) / span,
        12,
    )


def _max_axis_sensitivity(
    rows: list[dict[str, Any]],
    *,
    axis: str,
    group_fields: Sequence[str],
    scale: float,
    span: float,
) -> float | None:
    grouped = _group_rows(rows, group_fields)
    slopes = []
    for group in grouped.values():
        axis_values = np.asarray(
            [float(row[axis]) for row in group],
            dtype=float,
        )
        residuals = np.asarray(
            [
                float(row["measured_value"]) - float(row["reference_value"])
                for row in group
            ],
            dtype=float,
        )
        if len(np.unique(axis_values)) < 2:
            continue
        slope = float(np.polyfit(axis_values, residuals, 1)[0])
        slopes.append(abs(slope) * scale / span)
    return _max_or_none(slopes)


def _interference_effect(
    rows: list[dict[str, Any]],
    *,
    span: float,
) -> float | None:
    grouped = _group_rows(rows, ("interferent_name",))
    effects = []
    for group in grouped.values():
        by_level = _group_rows(group, ("interferent_value",))
        means = [
            float(np.mean([float(row["measured_value"]) for row in level_rows]))
            for level_rows in by_level.values()
        ]
        if len(means) >= 2:
            effects.append((max(means) - min(means)) / span)
    return _max_or_none(effects)


def _group_rows(
    rows: Iterable[Mapping[str, Any]],
    fields: Sequence[str],
) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for source in rows:
        row = dict(source)
        key = tuple(_group_value(row.get(field)) for field in fields)
        grouped[key].append(row)
    return grouped


def _group_value(value: Any) -> Any:
    numeric = _finite_or_none(value)
    return numeric if numeric is not None else str(value or "")


def _range(values: Iterable[Any]) -> dict[str, float | None]:
    finite = [
        item
        for item in (_finite_or_none(value) for value in values)
        if item is not None
    ]
    return {
        "minimum": None if not finite else min(finite),
        "maximum": None if not finite else max(finite),
    }


def _divide(value: Any, denominator: float) -> float | None:
    numeric = _finite_or_none(value)
    if numeric is None or denominator <= _EPSILON:
        return None
    return round(numeric / denominator, 12)


def _close(value: Any, reference: Any, tolerance: float = 1e-9) -> bool:
    left = _finite_or_none(value)
    right = _finite_or_none(reference)
    return left is not None and right is not None and abs(left - right) <= tolerance


def _positive(value: Any) -> float:
    numeric = _finite_or_none(value)
    if numeric is None or numeric <= 0.0:
        return 1.0
    return numeric


def _max_or_none(values: Iterable[Any]) -> float | None:
    finite = [
        item
        for item in (_finite_or_none(value) for value in values)
        if item is not None
    ]
    return None if not finite else round(max(finite), 12)


def _integer_or_none(value: Any) -> int | None:
    numeric = _finite_or_none(value)
    if numeric is None or not float(numeric).is_integer():
        return None
    return int(numeric)


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


__all__ = [
    "analyze_gas_analyzer_operating_envelope",
    "build_gas_analyzer_operating_envelope_acceptance",
]
