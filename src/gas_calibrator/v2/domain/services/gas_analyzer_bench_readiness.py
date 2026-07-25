"""Offline real-bench protocol readiness analysis for gas analyzers."""

from __future__ import annotations

from collections import Counter
from datetime import date
from itertools import product
from math import sqrt
from random import Random
from typing import Any, Iterable, Mapping

from gas_calibrator.utils.converters import finite_float as _finite_or_none


_EPSILON = 1e-12


def analyze_gas_analyzer_bench_readiness(
    protocol: Mapping[str, Any],
    asset_records: Iterable[Mapping[str, Any]],
    uncertainty_budgets: Iterable[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    protocol_row = dict(protocol)
    assets = [dict(item) for item in asset_records]
    budgets = [dict(item) for item in uncertainty_budgets]
    gas_contracts = dict(contract.get("gas_protocols") or {})
    interference_contracts = dict(contract.get("interference_protocols") or {})
    grid = dict(contract.get("environment_grid") or {})
    gas_plans = [dict(item) for item in list(protocol_row.get("gas_plans") or [])]
    interference_plans = [
        dict(item) for item in list(protocol_row.get("interference_plans") or [])
    ]

    traceability = _analyze_traceability(
        assets,
        contract=contract,
        protocol_date=str(protocol_row.get("protocol_date") or ""),
    )
    channels = [
        _analyze_channel_plan(
            gas,
            [item for item in gas_plans if str(item.get("gas") or "").lower() == gas],
            [
                item
                for item in interference_plans
                if str(item.get("gas") or "").lower() == gas
            ],
            [item for item in budgets if str(item.get("gas") or "").lower() == gas],
            gas_contract=dict(gas_contracts.get(gas) or {}),
            interference_contract=dict(interference_contracts.get(gas) or {}),
            grid=grid,
            contract=contract,
        )
        for gas in ("co2", "h2o")
    ]
    gas_plan_counts = Counter(str(item.get("gas") or "").lower() for item in gas_plans)
    interference_plan_counts = Counter(
        str(item.get("gas") or "").lower() for item in interference_plans
    )
    budget_counts = Counter(str(item.get("gas") or "").lower() for item in budgets)
    supported = {"co2", "h2o"}
    unexpected_gases = sorted(
        (set(gas_plan_counts) | set(interference_plan_counts) | set(budget_counts))
        - supported
    )
    protocol_scope = {
        "execution_mode": protocol_row.get("execution_mode"),
        "gas_analyzer_only": protocol_row.get("gas_analyzer_only"),
        "ec_flux_in_scope": protocol_row.get("ec_flux_in_scope"),
        "device_io_requested": protocol_row.get("device_io_requested"),
        "automatic_execution_requested": protocol_row.get(
            "automatic_execution_requested"
        ),
        "coefficient_fit_requested": protocol_row.get("coefficient_fit_requested"),
        "coefficient_write_requested": protocol_row.get("coefficient_write_requested"),
        "database_write_requested": protocol_row.get("database_write_requested"),
        "real_primary_latest_refresh_requested": protocol_row.get(
            "real_primary_latest_refresh_requested"
        ),
    }
    required_dut_identity_fields = {
        str(item) for item in contract.get("required_dut_identity_fields") or []
    }
    dut_identity = {
        field: protocol_row.get(field) for field in sorted(required_dut_identity_fields)
    }
    missing_dut_identity_fields = sorted(
        field
        for field, value in dut_identity.items()
        if value is None or not str(value).strip()
    )
    bench_controls = _analyze_bench_controls(
        protocol_row.get("bench_controls"),
        limits=dict(contract.get("bench_control_limits") or {}),
    )
    required_stages = [
        str(item) for item in contract.get("required_protocol_stages") or []
    ]
    observed_stages = [str(item) for item in protocol_row.get("stages") or []]
    evidence_plan = dict(protocol_row.get("evidence_plan") or {})
    required_artifact_roles = {
        str(item) for item in contract.get("required_artifact_roles") or []
    }
    observed_artifact_roles = {
        str(item) for item in evidence_plan.get("required_artifact_roles") or []
    }
    return {
        "artifact_type": "gas_analyzer_bench_readiness",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "gas_analyzer_bench_readiness_v1",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_protocol_readiness",
        "promotion_state": "blocked",
        "ready_for_real_execution": False,
        "protocol_id": str(protocol_row.get("protocol_id") or ""),
        "protocol_schema_version": str(protocol_row.get("schema_version") or ""),
        "protocol_date": str(protocol_row.get("protocol_date") or ""),
        "analyzer_id": str(protocol_row.get("analyzer_id") or ""),
        "dut_identity": dut_identity,
        "missing_dut_identity_fields": missing_dut_identity_fields,
        "protocol_scope": protocol_scope,
        "bench_controls": bench_controls,
        "traceability": traceability,
        "channels": channels,
        "unexpected_gases": unexpected_gases,
        "gas_plan_counts": dict(gas_plan_counts),
        "interference_plan_counts": dict(interference_plan_counts),
        "uncertainty_budget_counts": dict(budget_counts),
        "stage_plan": {
            "required": required_stages,
            "observed": observed_stages,
            "exact_order_match": observed_stages == required_stages,
        },
        "evidence_plan": {
            "required_artifact_roles": sorted(required_artifact_roles),
            "observed_artifact_roles": sorted(observed_artifact_roles),
            "roles_complete": observed_artifact_roles == required_artifact_roles,
            "raw_data_immutable": evidence_plan.get("raw_data_immutable"),
            "lineage_required": evidence_plan.get("lineage_required"),
            "formal_analysis_state": evidence_plan.get("formal_analysis_state"),
        },
        "status": "ok" if protocol_row and assets and budgets else "incomplete",
    }


def build_gas_analyzer_bench_readiness_acceptance(
    readiness: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    scope = dict(readiness.get("protocol_scope") or {})
    traceability = dict(readiness.get("traceability") or {})
    stage_plan = dict(readiness.get("stage_plan") or {})
    evidence_plan = dict(readiness.get("evidence_plan") or {})
    protocol_date = str(readiness.get("protocol_date") or "")
    expected_protocol_date = str(contract.get("protocol_date") or "")
    gates: list[dict[str, Any]] = [
        _gate(
            "analysis_complete",
            readiness.get("status") == "ok",
            readiness.get("status"),
            "ok",
        ),
        _gate(
            "protocol_schema_version",
            readiness.get("protocol_schema_version")
            == "gas_analyzer_bench_protocol_plan_v1",
            readiness.get("protocol_schema_version"),
            "gas_analyzer_bench_protocol_plan_v1",
        ),
        _gate(
            "protocol_date_locked",
            protocol_date == expected_protocol_date,
            protocol_date,
            expected_protocol_date,
        ),
        _gate(
            "analyzer_identity_present",
            bool(str(readiness.get("analyzer_id") or "")),
            readiness.get("analyzer_id"),
            "non_empty",
        ),
        _gate(
            "dut_identity_complete",
            not list(readiness.get("missing_dut_identity_fields") or []),
            readiness.get("missing_dut_identity_fields"),
            [],
        ),
        _gate(
            "offline_no_io_no_write_scope",
            scope
            == {
                "execution_mode": "offline_protocol_design",
                "gas_analyzer_only": True,
                "ec_flux_in_scope": False,
                "device_io_requested": False,
                "automatic_execution_requested": False,
                "coefficient_fit_requested": False,
                "coefficient_write_requested": False,
                "database_write_requested": False,
                "real_primary_latest_refresh_requested": False,
            },
            scope,
            {
                "execution_mode": "offline_protocol_design",
                "device_io_requested": False,
                "all_writes_requested": False,
            },
        ),
        _gate(
            "supported_gases_only",
            not list(readiness.get("unexpected_gases") or []),
            readiness.get("unexpected_gases"),
            [],
        ),
        _gate(
            "one_plan_per_gas",
            _exact_gas_counts(readiness.get("gas_plan_counts")),
            readiness.get("gas_plan_counts"),
            {"co2": 1, "h2o": 1},
        ),
        _gate(
            "one_interference_plan_per_gas",
            _exact_gas_counts(readiness.get("interference_plan_counts")),
            readiness.get("interference_plan_counts"),
            {"co2": 1, "h2o": 1},
        ),
        _gate(
            "one_uncertainty_budget_per_gas",
            _exact_gas_counts(readiness.get("uncertainty_budget_counts")),
            readiness.get("uncertainty_budget_counts"),
            {"co2": 1, "h2o": 1},
        ),
        _gate(
            "traceability_roles_complete",
            traceability.get("roles_complete") is True,
            {
                "missing": traceability.get("missing_roles"),
                "duplicates": traceability.get("duplicate_roles"),
                "unexpected": traceability.get("unexpected_roles"),
            },
            {"missing": [], "duplicates": [], "unexpected": []},
        ),
        _gate(
            "traceability_asset_identity_unique",
            not list(traceability.get("duplicate_asset_ids") or [])
            and not list(traceability.get("duplicate_serial_numbers") or []),
            {
                "asset_ids": traceability.get("duplicate_asset_ids"),
                "serial_numbers": traceability.get("duplicate_serial_numbers"),
            },
            {"asset_ids": [], "serial_numbers": []},
        ),
        _gate(
            "traceability_fields_complete",
            not list(traceability.get("missing_fields") or []),
            traceability.get("missing_fields"),
            [],
        ),
        _gate(
            "traceability_certificates_valid",
            int(traceability.get("invalid_certificate_row_count") or 0) == 0,
            traceability.get("invalid_certificate_row_count"),
            0,
        ),
        _gate(
            "traceability_uncertainties_valid",
            int(traceability.get("invalid_uncertainty_row_count") or 0) == 0,
            traceability.get("invalid_uncertainty_row_count"),
            0,
        ),
        _gate(
            "bench_physical_controls_ready",
            dict(readiness.get("bench_controls") or {}).get("valid") is True,
            readiness.get("bench_controls"),
            {"valid": True},
        ),
        _gate(
            "protocol_stage_order_complete",
            stage_plan.get("exact_order_match") is True,
            stage_plan.get("observed"),
            stage_plan.get("required"),
        ),
        _gate(
            "evidence_roles_complete",
            evidence_plan.get("roles_complete") is True,
            evidence_plan.get("observed_artifact_roles"),
            evidence_plan.get("required_artifact_roles"),
        ),
        _gate(
            "evidence_immutability_and_lineage",
            evidence_plan.get("raw_data_immutable") is True
            and evidence_plan.get("lineage_required") is True,
            {
                "raw_data_immutable": evidence_plan.get("raw_data_immutable"),
                "lineage_required": evidence_plan.get("lineage_required"),
            },
            {"raw_data_immutable": True, "lineage_required": True},
        ),
        _gate(
            "formal_analysis_remains_blocked",
            evidence_plan.get("formal_analysis_state")
            == "blocked_until_real_acceptance",
            evidence_plan.get("formal_analysis_state"),
            "blocked_until_real_acceptance",
        ),
    ]
    channel_results: list[dict[str, Any]] = []
    for channel in list(readiness.get("channels") or []):
        gas = str(channel.get("gas") or "")
        channel_gates = [
            _gate(
                "channel_plan_present",
                channel.get("status") == "ok",
                channel.get("status"),
                "ok",
            ),
            _gate(
                "anchor_role_and_target",
                channel.get("anchor_contract_valid") is True,
                {
                    "role": channel.get("anchor_role"),
                    "target": channel.get("anchor_target"),
                },
                channel.get("expected_anchor"),
            ),
            _gate(
                "complete_rectangular_environment_order",
                dict(channel.get("environment_order") or {}).get("complete") is True,
                channel.get("environment_order"),
                {"missing": 0, "duplicates": 0, "unexpected": 0},
            ),
            _gate(
                "environment_order_randomized_and_reproducible",
                dict(channel.get("environment_order") or {}).get("randomization_valid")
                is True,
                {
                    "method": dict(channel.get("environment_order") or {}).get(
                        "method"
                    ),
                    "seed": dict(channel.get("environment_order") or {}).get("seed"),
                    "differs_from_canonical": dict(
                        channel.get("environment_order") or {}
                    ).get("differs_from_canonical"),
                },
                {
                    "method": "seeded_shuffle",
                    "seed": "integer",
                    "differs_from_canonical": True,
                },
            ),
            _gate(
                "planned_measurement_count",
                channel.get("planned_measurement_count_valid") is True,
                channel.get("planned_measurement_row_count"),
                channel.get("expected_measurement_row_count"),
            ),
            _gate(
                "sampling_axes_match_contract",
                channel.get("sampling_axes_valid") is True,
                channel.get("sampling_axes"),
                channel.get("expected_sampling_axes"),
            ),
            _gate(
                "conditioning_criteria",
                dict(channel.get("conditioning") or {}).get("valid") is True,
                channel.get("conditioning"),
                {"valid": True},
            ),
            _gate(
                "post_run_anchor_check",
                channel.get("post_run_anchor_check") is True,
                channel.get("post_run_anchor_check"),
                True,
            ),
            _gate(
                "route_semantics",
                channel.get("route_semantics_valid") is True,
                channel.get("route_mode"),
                channel.get("expected_route_mode"),
            ),
            _gate(
                "interference_sweep_complete",
                dict(channel.get("interference_plan") or {}).get("valid") is True,
                channel.get("interference_plan"),
                {"valid": True},
            ),
            _gate(
                "uncertainty_components_complete",
                dict(channel.get("uncertainty_budget") or {}).get("components_complete")
                is True,
                {
                    "missing": dict(channel.get("uncertainty_budget") or {}).get(
                        "missing_components"
                    ),
                    "duplicates": dict(channel.get("uncertainty_budget") or {}).get(
                        "duplicate_components"
                    ),
                    "unexpected": dict(channel.get("uncertainty_budget") or {}).get(
                        "unexpected_components"
                    ),
                },
                {"missing": [], "duplicates": [], "unexpected": []},
            ),
            _gate(
                "uncertainty_values_valid",
                dict(channel.get("uncertainty_budget") or {}).get("values_valid")
                is True,
                dict(channel.get("uncertainty_budget") or {}).get(
                    "invalid_value_count"
                ),
                0,
            ),
            _gate(
                "expanded_uncertainty_within_limit",
                dict(channel.get("uncertainty_budget") or {}).get("within_limit")
                is True,
                dict(channel.get("uncertainty_budget") or {}).get(
                    "expanded_uncertainty_span_fraction"
                ),
                dict(channel.get("uncertainty_budget") or {}).get("maximum_allowed"),
            ),
        ]
        for gate in channel_gates:
            gate["gas"] = gas
            gate["analyzer_id"] = readiness.get("analyzer_id")
        gates.extend(channel_gates)
        channel_results.append(
            {
                "gas": gas,
                "status": (
                    "pass"
                    if all(bool(gate.get("passed")) for gate in channel_gates)
                    else "fail"
                ),
                "gates": channel_gates,
            }
        )
    passed = len(channel_results) == 2 and all(
        bool(gate.get("passed")) for gate in gates
    )
    return {
        "artifact_type": "gas_analyzer_bench_readiness_acceptance",
        "artifact_role": "diagnostic_analysis",
        "schema_version": str(contract.get("schema_version") or ""),
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_protocol_readiness",
        "promotion_state": "blocked",
        "protocol_design_ready": passed,
        "ready_for_real_execution": False,
        "execution_authorization_status": "not_requested",
        "real_acceptance_status": "blocked",
        "device_io_status": "not_attempted",
        "write_status": "not_attempted",
        "all_readiness_gates_passed": passed,
        "failed_gate_names": [
            f"{gate.get('gas', 'global')}:{gate.get('analyzer_id', 'all')}:{gate.get('name')}"
            for gate in gates
            if not bool(gate.get("passed"))
        ],
        "required_gates": gates,
        "channels": channel_results,
        "boundary_note": (
            "本工件只证明离线台架协议、可溯源资产和不确定度预算的准备度；"
            "它不授权真实执行，不连接设备，不写入系数或数据库，也不构成真实验收。"
        ),
    }


def _analyze_traceability(
    assets: list[dict[str, Any]],
    *,
    contract: Mapping[str, Any],
    protocol_date: str,
) -> dict[str, Any]:
    required_roles = {str(item) for item in contract.get("required_asset_roles") or []}
    required_fields = {
        str(item) for item in contract.get("required_asset_fields") or []
    }
    role_requirements = dict(contract.get("asset_role_requirements") or {})
    role_counts = Counter(str(item.get("role") or "") for item in assets)
    asset_id_counts = Counter(str(item.get("asset_id") or "") for item in assets)
    serial_counts = Counter(str(item.get("serial_number") or "") for item in assets)
    missing_roles = sorted(required_roles - set(role_counts))
    duplicate_roles = sorted(role for role, count in role_counts.items() if count > 1)
    unexpected_roles = sorted(set(role_counts) - required_roles)
    missing_fields = sorted(
        {
            field
            for asset in assets
            for field in required_fields
            if field not in asset or asset.get(field) in {None, ""}
        }
    )
    protocol_day = _parse_date(protocol_date)
    invalid_certificate_rows = []
    invalid_uncertainty_rows = []
    for asset in assets:
        calibration_day = _parse_date(asset.get("calibration_date"))
        due_day = _parse_date(asset.get("due_date"))
        certificate_valid = (
            protocol_day is not None
            and calibration_day is not None
            and due_day is not None
            and calibration_day <= protocol_day < due_day
            and str(asset.get("status") or "") == "active"
            and bool(str(asset.get("certificate_id") or ""))
            and bool(str(asset.get("traceability_chain") or ""))
        )
        if not certificate_valid:
            invalid_certificate_rows.append(str(asset.get("role") or ""))
        uncertainty = _finite_or_none(asset.get("standard_uncertainty"))
        requirement = dict(role_requirements.get(str(asset.get("role") or "")) or {})
        maximum = _finite_or_none(requirement.get("max_standard_uncertainty"))
        unit = str(asset.get("uncertainty_unit") or "")
        expected_unit = str(requirement.get("uncertainty_unit") or "")
        if (
            uncertainty is None
            or uncertainty <= 0.0
            or maximum is None
            or uncertainty > maximum
            or not unit
            or unit != expected_unit
        ):
            invalid_uncertainty_rows.append(str(asset.get("role") or ""))
    return {
        "asset_count": len(assets),
        "required_roles": sorted(required_roles),
        "missing_roles": missing_roles,
        "duplicate_roles": duplicate_roles,
        "unexpected_roles": unexpected_roles,
        "duplicate_asset_ids": sorted(
            asset_id for asset_id, count in asset_id_counts.items() if count > 1
        ),
        "duplicate_serial_numbers": sorted(
            serial for serial, count in serial_counts.items() if count > 1
        ),
        "roles_complete": (
            not missing_roles and not duplicate_roles and not unexpected_roles
        ),
        "missing_fields": missing_fields,
        "invalid_certificate_row_count": len(invalid_certificate_rows),
        "invalid_certificate_roles": sorted(invalid_certificate_rows),
        "invalid_uncertainty_row_count": len(invalid_uncertainty_rows),
        "invalid_uncertainty_roles": sorted(invalid_uncertainty_rows),
    }


def _analyze_bench_controls(
    source: Any,
    *,
    limits: Mapping[str, Any],
) -> dict[str, Any]:
    controls = dict(source or {})
    leak_hold = _finite_or_none(controls.get("leak_hold_s"))
    pressure_decay = _finite_or_none(controls.get("pressure_decay_hpa_per_min"))
    purge_exchanges = _finite_or_none(controls.get("purge_volume_exchanges"))
    material = str(controls.get("h2o_wetted_path_material") or "")
    allowed_materials = {
        str(item) for item in limits.get("allowed_wetted_path_materials") or []
    }
    h2o_temperature_controlled = controls.get("h2o_wetted_path_temperature_controlled")
    shared_timebase = controls.get("shared_timebase")
    safe_exhaust = controls.get("safe_exhaust")
    checks = {
        "leak_hold": (
            leak_hold is not None
            and leak_hold >= float(limits.get("min_leak_hold_s") or 0.0)
        ),
        "pressure_decay": (
            pressure_decay is not None
            and 0.0
            <= pressure_decay
            <= float(limits.get("max_pressure_decay_hpa_per_min") or 0.0)
        ),
        "purge_volume": (
            purge_exchanges is not None
            and purge_exchanges
            >= float(limits.get("min_purge_volume_exchanges") or 0.0)
        ),
        "h2o_wetted_path_material": material in allowed_materials,
        "h2o_temperature_control": (
            h2o_temperature_controlled is True
            if limits.get("h2o_wetted_path_temperature_control_required") is True
            else True
        ),
        "shared_timebase": (
            shared_timebase is True
            if limits.get("shared_timebase_required") is True
            else True
        ),
        "safe_exhaust": (
            safe_exhaust is True
            if limits.get("safe_exhaust_required") is True
            else True
        ),
    }
    return {
        "leak_hold_s": leak_hold,
        "pressure_decay_hpa_per_min": pressure_decay,
        "purge_volume_exchanges": purge_exchanges,
        "h2o_wetted_path_material": material,
        "h2o_wetted_path_temperature_controlled": h2o_temperature_controlled,
        "shared_timebase": shared_timebase,
        "safe_exhaust": safe_exhaust,
        "checks": checks,
        "valid": all(checks.values()),
    }


def _analyze_channel_plan(
    gas: str,
    plans: list[dict[str, Any]],
    interference_plans: list[dict[str, Any]],
    budgets: list[dict[str, Any]],
    *,
    gas_contract: dict[str, Any],
    interference_contract: dict[str, Any],
    grid: dict[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    plan = plans[0] if len(plans) == 1 else {}
    interference_plan = interference_plans[0] if len(interference_plans) == 1 else {}
    budget = budgets[0] if len(budgets) == 1 else {}
    expected_anchor = {
        "role": str(gas_contract.get("anchor_role") or ""),
        "target": _finite_or_none(gas_contract.get("anchor_target")),
    }
    anchor_role = str(plan.get("anchor_role") or "")
    anchor_target = _finite_or_none(plan.get("anchor_target"))
    expected_cells = [
        (float(temperature), float(pressure), float(flow))
        for temperature, pressure, flow in product(
            list(grid.get("temperatures_c") or []),
            list(grid.get("pressures_hpa") or []),
            list(grid.get("flows_slpm") or []),
        )
    ]
    observed_cells = [
        (
            _finite_or_none(item.get("temperature_c")),
            _finite_or_none(item.get("pressure_hpa")),
            _finite_or_none(item.get("flow_slpm")),
        )
        for item in list(plan.get("environment_cell_order") or [])
    ]
    observed_counts = Counter(observed_cells)
    missing_cells = sorted(
        set(expected_cells) - set(observed_counts),
        key=repr,
    )
    duplicate_count = sum(max(0, count - 1) for count in observed_counts.values())
    unexpected_cells = sorted(
        set(observed_counts) - set(expected_cells),
        key=repr,
    )
    method = str(plan.get("randomization_method") or "")
    seed = _integer_or_none(plan.get("randomization_seed"))
    expected_randomized_order = list(expected_cells)
    if seed is not None:
        Random(seed).shuffle(expected_randomized_order)
    reproducible_order = (
        seed is not None and observed_cells == expected_randomized_order
    )
    environment_order = {
        "expected_cell_count": len(expected_cells),
        "observed_cell_count": len(observed_cells),
        "missing_cell_count": len(missing_cells),
        "duplicate_cell_count": duplicate_count,
        "unexpected_cell_count": len(unexpected_cells),
        "missing_cell_preview": [list(item) for item in missing_cells[:10]],
        "complete": (
            not missing_cells
            and duplicate_count == 0
            and not unexpected_cells
            and len(observed_cells) == len(expected_cells)
        ),
        "method": method,
        "seed": seed,
        "differs_from_canonical": observed_cells != expected_cells,
        "reproducible_from_seed": reproducible_order,
        "randomization_valid": (
            method == "seeded_shuffle"
            and seed is not None
            and observed_cells != expected_cells
            and reproducible_order
        ),
    }
    expected_measurement_count = (
        len(expected_cells)
        * len(list(gas_contract.get("target_levels") or []))
        * len(list(grid.get("sequence_directions") or []))
        * len(list(grid.get("sessions") or []))
        * len(list(grid.get("replicates") or []))
    )
    planned_measurement_count = _integer_or_none(
        plan.get("planned_measurement_row_count")
    )
    sampling_axes = {
        "target_levels": [
            _finite_or_none(item) for item in plan.get("target_levels") or []
        ],
        "sequence_directions": [
            str(item) for item in plan.get("sequence_directions") or []
        ],
        "sessions": [str(item) for item in plan.get("sessions") or []],
        "replicates": [_integer_or_none(item) for item in plan.get("replicates") or []],
    }
    expected_sampling_axes = {
        "target_levels": [
            float(item) for item in gas_contract.get("target_levels") or []
        ],
        "sequence_directions": [
            str(item) for item in grid.get("sequence_directions") or []
        ],
        "sessions": [str(item) for item in grid.get("sessions") or []],
        "replicates": [int(item) for item in grid.get("replicates") or []],
    }
    conditioning = _analyze_conditioning(plan, gas_contract=gas_contract, gas=gas)
    expected_route = (
        "co2_zero_span_dry_path" if gas == "co2" else "h2o_dry_wet_conditioned_path"
    )
    interference_summary = _analyze_interference_plan(
        interference_plan,
        interference_contract=interference_contract,
    )
    uncertainty_summary = _analyze_uncertainty_budget(
        budget,
        gas=gas,
        gas_contract=gas_contract,
        contract=contract,
    )
    return {
        "gas": gas,
        "status": (
            "ok"
            if len(plans) == 1 and len(interference_plans) == 1 and len(budgets) == 1
            else "incomplete"
        ),
        "anchor_role": anchor_role,
        "anchor_target": anchor_target,
        "expected_anchor": expected_anchor,
        "anchor_contract_valid": (
            anchor_role == expected_anchor["role"]
            and _close(anchor_target, expected_anchor["target"])
            and anchor_role
            in {
                "co2_zero_gas",
                "h2o_dry_gas",
            }
        ),
        "environment_order": environment_order,
        "planned_measurement_row_count": planned_measurement_count,
        "expected_measurement_row_count": expected_measurement_count,
        "planned_measurement_count_valid": (
            planned_measurement_count == expected_measurement_count
        ),
        "sampling_axes": sampling_axes,
        "expected_sampling_axes": expected_sampling_axes,
        "sampling_axes_valid": sampling_axes == expected_sampling_axes,
        "conditioning": conditioning,
        "post_run_anchor_check": plan.get("post_run_anchor_check"),
        "route_mode": plan.get("route_mode"),
        "expected_route_mode": expected_route,
        "route_semantics_valid": plan.get("route_mode") == expected_route,
        "interference_plan": interference_summary,
        "uncertainty_budget": uncertainty_summary,
    }


def _analyze_conditioning(
    plan: Mapping[str, Any],
    *,
    gas_contract: Mapping[str, Any],
    gas: str,
) -> dict[str, Any]:
    source = dict(plan.get("conditioning") or {})
    window = _finite_or_none(source.get("stability_window_s"))
    slope = _finite_or_none(source.get("max_slope_span_fraction_per_min"))
    sigma = _finite_or_none(source.get("max_sigma_span_fraction"))
    max_wait = _finite_or_none(source.get("max_wait_s"))
    base_valid = (
        window is not None
        and window >= float(gas_contract.get("min_stability_window_s") or 0.0)
        and slope is not None
        and 0.0
        <= slope
        <= float(gas_contract.get("max_stability_slope_span_fraction_per_min") or 0.0)
        and sigma is not None
        and 0.0
        <= sigma
        <= float(gas_contract.get("max_stability_sigma_span_fraction") or 0.0)
        and max_wait is not None
        and max_wait >= float(gas_contract.get("min_max_wait_s") or 0.0)
    )
    recovery_fraction = _finite_or_none(source.get("wet_to_dry_recovery_fraction"))
    recovery_hold = _finite_or_none(source.get("recovery_hold_s"))
    recovery_valid = True
    if gas == "h2o":
        recovery_valid = (
            recovery_fraction is not None
            and 0.0
            <= recovery_fraction
            <= float(gas_contract.get("max_wet_to_dry_recovery_fraction") or 0.0)
            and recovery_hold is not None
            and recovery_hold >= float(gas_contract.get("min_recovery_hold_s") or 0.0)
        )
    return {
        "stability_window_s": window,
        "max_slope_span_fraction_per_min": slope,
        "max_sigma_span_fraction": sigma,
        "max_wait_s": max_wait,
        "wet_to_dry_recovery_fraction": recovery_fraction,
        "recovery_hold_s": recovery_hold,
        "base_valid": base_valid,
        "recovery_valid": recovery_valid,
        "valid": base_valid and recovery_valid,
    }


def _analyze_interference_plan(
    plan: Mapping[str, Any],
    *,
    interference_contract: Mapping[str, Any],
) -> dict[str, Any]:
    observed_levels = [
        _finite_or_none(item) for item in plan.get("interferent_values") or []
    ]
    expected_levels = [
        float(item) for item in interference_contract.get("interferent_values") or []
    ]
    observed_replicates = [
        _integer_or_none(item) for item in plan.get("replicates") or []
    ]
    expected_replicates = [
        int(item) for item in interference_contract.get("replicates") or []
    ]
    valid = (
        str(plan.get("interferent_name") or "").lower()
        == str(interference_contract.get("interferent_name") or "").lower()
        and _close(
            plan.get("target_value"),
            interference_contract.get("target_value"),
        )
        and observed_levels == expected_levels
        and observed_replicates == expected_replicates
    )
    return {
        "interferent_name": plan.get("interferent_name"),
        "target_value": _finite_or_none(plan.get("target_value")),
        "interferent_values": observed_levels,
        "replicates": observed_replicates,
        "valid": valid,
    }


def _analyze_uncertainty_budget(
    budget: Mapping[str, Any],
    *,
    gas: str,
    gas_contract: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    required_by_gas = dict(contract.get("required_uncertainty_components") or {})
    required = {str(item) for item in required_by_gas.get(gas) or []}
    components = [dict(item) for item in budget.get("components") or []]
    names = [str(item.get("name") or "") for item in components]
    counts = Counter(names)
    missing = sorted(required - set(names))
    duplicates = sorted(name for name, count in counts.items() if count > 1)
    unexpected = sorted(set(names) - required)
    values = [
        _finite_or_none(item.get("standard_uncertainty_span_fraction"))
        for item in components
    ]
    invalid_count = sum(1 for value in values if value is None or value <= 0.0)
    valid_values = [value for value in values if value is not None and value > 0.0]
    combined = sqrt(sum(value**2 for value in valid_values)) if valid_values else None
    coverage_factor = _finite_or_none(budget.get("coverage_factor"))
    required_coverage_factor = float(contract.get("uncertainty_coverage_factor") or 0.0)
    expanded = (
        combined * coverage_factor
        if combined is not None and coverage_factor is not None
        else None
    )
    maximum = float(gas_contract.get("max_expanded_uncertainty_span_fraction") or 0.0)
    return {
        "required_components": sorted(required),
        "observed_components": sorted(names),
        "missing_components": missing,
        "duplicate_components": duplicates,
        "unexpected_components": unexpected,
        "components_complete": not missing and not duplicates and not unexpected,
        "invalid_value_count": invalid_count,
        "coverage_factor": coverage_factor,
        "required_coverage_factor": required_coverage_factor,
        "values_valid": (
            invalid_count == 0
            and coverage_factor is not None
            and abs(coverage_factor - required_coverage_factor) <= _EPSILON
        ),
        "combined_standard_uncertainty_span_fraction": (
            None if combined is None else round(combined, 12)
        ),
        "expanded_uncertainty_span_fraction": (
            None if expanded is None else round(expanded, 12)
        ),
        "maximum_allowed": maximum,
        "within_limit": (
            expanded is not None and maximum > 0.0 and expanded <= maximum
        ),
    }


def _exact_gas_counts(value: Any) -> bool:
    source = {str(key): int(count) for key, count in dict(value or {}).items()}
    return source == {"co2": 1, "h2o": 1}


def _gate(name: str, passed: bool, observed: Any, required: Any) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "observed": observed,
        "required": required,
    }


def _parse_date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _integer_or_none(value: Any) -> int | None:
    numeric = _finite_or_none(value)
    if numeric is None or not numeric.is_integer():
        return None
    return int(numeric)


def _close(left: Any, right: Any, tolerance: float = 1e-12) -> bool:
    left_value = _finite_or_none(left)
    right_value = _finite_or_none(right)
    return (
        left_value is not None
        and right_value is not None
        and abs(left_value - right_value) <= tolerance
    )


__all__ = [
    "analyze_gas_analyzer_bench_readiness",
    "build_gas_analyzer_bench_readiness_acceptance",
]
