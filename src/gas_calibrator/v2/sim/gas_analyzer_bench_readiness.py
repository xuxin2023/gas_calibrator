"""Simulation-only GA-D4 real-bench protocol readiness fixtures."""

from __future__ import annotations

from datetime import date, datetime
from itertools import product
import json
from pathlib import Path
from random import Random
from typing import Any, Mapping

from gas_calibrator.utils.file_io import write_json as _write_json

from ..domain.services.gas_analyzer_bench_readiness import (
    analyze_gas_analyzer_bench_readiness,
    build_gas_analyzer_bench_readiness_acceptance,
)


DEFAULT_GAS_ANALYZER_BENCH_READINESS_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "metrology"
    / "gas_analyzer_bench_readiness_contract_v1.json"
)


def load_gas_analyzer_bench_readiness_contract(
    path: str | Path = DEFAULT_GAS_ANALYZER_BENCH_READINESS_CONTRACT_PATH,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if (
        str(payload.get("schema_version") or "")
        != "gas_analyzer_bench_readiness_contract_v1"
    ):
        raise ValueError("unexpected GA-D4 bench-readiness contract schema")
    boundary = dict(payload.get("evidence_boundary") or {})
    required_boundary = {
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_protocol_readiness",
        "promotion_state": "blocked",
        "real_primary_latest_refresh_allowed": False,
        "device_io_allowed": False,
        "coefficient_fit_allowed": False,
        "coefficient_write_allowed": False,
        "database_write_allowed": False,
        "automatic_execution_allowed": False,
    }
    for key, value in required_boundary.items():
        if boundary.get(key) != value:
            raise ValueError(f"GA-D4 contract must set {key}={str(value).lower()}")
    interpretation = dict(payload.get("interpretation") or {})
    required_interpretation = {
        "gas_analyzer_only": True,
        "ec_flux_or_cospectral_correction_in_scope": False,
        "co2_zero_gas_and_h2o_dry_gas_are_distinct": True,
        "protocol_readiness_is_execution_authorization": False,
        "protocol_readiness_is_real_acceptance": False,
        "qualified_bounds_require_complete_rectangular_grid": True,
        "randomized_environment_order_required": True,
        "formal_analysis_requires_separate_real_acceptance": True,
    }
    for key, value in required_interpretation.items():
        if interpretation.get(key) != value:
            raise ValueError(f"GA-D4 contract must set {key}={str(value).lower()}")
    try:
        date.fromisoformat(str(payload.get("protocol_date") or ""))
    except ValueError as exc:
        raise ValueError("GA-D4 protocol_date must be ISO-8601") from exc
    grid = dict(payload.get("environment_grid") or {})
    if (
        list(grid.get("temperatures_c") or []) != [5.0, 25.0, 45.0]
        or list(grid.get("pressures_hpa") or []) != [850.0, 1000.0, 1100.0]
        or list(grid.get("flows_slpm") or []) != [8.0, 12.0, 16.0]
    ):
        raise ValueError("GA-D4 must retain the GA-D3 27-cell environment grid")
    if list(grid.get("sequence_directions") or []) != [
        "ascending",
        "descending",
    ]:
        raise ValueError("GA-D4 must retain ascending and descending sequences")
    if list(grid.get("sessions") or []) != ["start", "end"]:
        raise ValueError("GA-D4 must retain start and end sessions")
    if list(grid.get("replicates") or []) != [1, 2, 3]:
        raise ValueError("GA-D4 must retain three replicates")
    gas_protocols = dict(payload.get("gas_protocols") or {})
    expected_anchors = {
        "co2": ("co2_zero_gas", 0.0),
        "h2o": ("h2o_dry_gas", 0.2),
    }
    expected_targets = {
        "co2": [0.0, 400.0, 800.0, 1200.0],
        "h2o": [0.2, 5.0, 15.0, 25.0],
    }
    expected_channel_limits = {
        "co2": {
            "unit": "ppm",
            "span_value": 1200.0,
            "min_stability_window_s": 120.0,
            "max_stability_slope_span_fraction_per_min": 0.0002,
            "max_stability_sigma_span_fraction": 0.0002,
            "min_max_wait_s": 600.0,
            "max_expanded_uncertainty_span_fraction": 0.004,
        },
        "h2o": {
            "unit": "mmol_mol",
            "span_value": 25.0,
            "min_stability_window_s": 300.0,
            "max_stability_slope_span_fraction_per_min": 0.0004,
            "max_stability_sigma_span_fraction": 0.0004,
            "min_max_wait_s": 1200.0,
            "max_wet_to_dry_recovery_fraction": 0.01,
            "min_recovery_hold_s": 300.0,
            "max_expanded_uncertainty_span_fraction": 0.006,
        },
    }
    for gas, (role, target) in expected_anchors.items():
        channel = dict(gas_protocols.get(gas) or {})
        if channel.get("anchor_role") != role:
            raise ValueError(f"GA-D4 {gas} anchor role must be {role}")
        if float(channel.get("anchor_target")) != target:
            raise ValueError(f"GA-D4 {gas} anchor target must be {target}")
        if [
            float(item) for item in channel.get("target_levels") or []
        ] != expected_targets[gas]:
            raise ValueError(f"GA-D4 {gas} target levels must retain GA-D3")
        for key, expected in expected_channel_limits[gas].items():
            if channel.get(key) != expected:
                raise ValueError(f"GA-D4 {gas} must retain controlled limit {key}")
    interference_protocols = dict(payload.get("interference_protocols") or {})
    expected_interference = {
        "co2": {
            "interferent_name": "h2o",
            "target_value": 800.0,
            "interferent_values": [0.2, 10.0, 20.0],
            "replicates": [1, 2, 3],
        },
        "h2o": {
            "interferent_name": "co2",
            "target_value": 15.0,
            "interferent_values": [0.0, 400.0, 1000.0],
            "replicates": [1, 2, 3],
        },
    }
    for gas, expected in expected_interference.items():
        if dict(interference_protocols.get(gas) or {}) != expected:
            raise ValueError(f"GA-D4 {gas} interference protocol must retain GA-D3")
    required_roles = [str(item) for item in payload.get("required_asset_roles") or []]
    expected_roles = {
        "co2_zero_gas_standard",
        "co2_span_gas_standard",
        "h2o_dry_reference",
        "h2o_humidity_reference",
        "pressure_reference",
        "temperature_reference",
        "flow_reference",
        "timebase_reference",
    }
    if set(required_roles) != expected_roles or len(required_roles) != len(
        expected_roles
    ):
        raise ValueError("GA-D4 must retain all traceability asset roles")
    asset_requirements = dict(payload.get("asset_role_requirements") or {})
    if set(asset_requirements) != set(required_roles):
        raise ValueError("GA-D4 asset requirements must match all asset roles")
    expected_asset_requirements = {
        "co2_zero_gas_standard": ("ppm", 0.5),
        "co2_span_gas_standard": ("ppm", 1.0),
        "h2o_dry_reference": ("mmol_mol", 0.02),
        "h2o_humidity_reference": ("mmol_mol", 0.05),
        "pressure_reference": ("hpa", 0.1),
        "temperature_reference": ("deg_c", 0.05),
        "flow_reference": ("slpm", 0.02),
        "timebase_reference": ("s", 0.001),
    }
    for role, (expected_unit, expected_maximum) in expected_asset_requirements.items():
        requirement = dict(asset_requirements.get(role) or {})
        if (
            requirement.get("uncertainty_unit") != expected_unit
            or requirement.get("max_standard_uncertainty") != expected_maximum
        ):
            raise ValueError(f"GA-D4 {role} must retain its uncertainty limit")
    required_dut_fields = {
        str(item) for item in payload.get("required_dut_identity_fields") or []
    }
    if required_dut_fields != {
        "analyzer_id",
        "analyzer_model",
        "analyzer_serial_number",
        "analyzer_firmware_version",
        "software_build_id",
    }:
        raise ValueError("GA-D4 must retain the complete DUT identity contract")
    required_asset_fields = {
        str(item) for item in payload.get("required_asset_fields") or []
    }
    if required_asset_fields != {
        "asset_id",
        "role",
        "serial_number",
        "certificate_id",
        "calibration_date",
        "due_date",
        "status",
        "traceability_chain",
        "standard_uncertainty",
        "uncertainty_unit",
    }:
        raise ValueError("GA-D4 must retain all traceability asset fields")
    bench_limits = dict(payload.get("bench_control_limits") or {})
    expected_bench_limits = {
        "min_leak_hold_s": 300.0,
        "max_pressure_decay_hpa_per_min": 0.1,
        "min_purge_volume_exchanges": 5.0,
    }
    for key, expected in expected_bench_limits.items():
        if bench_limits.get(key) != expected:
            raise ValueError(f"GA-D4 must retain controlled bench limit {key}")
    if set(bench_limits.get("allowed_wetted_path_materials") or []) != {
        "PFA",
        "PTFE",
        "electropolished_stainless_steel",
    }:
        raise ValueError("GA-D4 must retain controlled wetted path materials")
    for key in (
        "h2o_wetted_path_temperature_control_required",
        "shared_timebase_required",
        "safe_exhaust_required",
    ):
        if bench_limits.get(key) is not True:
            raise ValueError(f"GA-D4 must set {key}=true")
    required_stages = [
        str(item) for item in payload.get("required_protocol_stages") or []
    ]
    expected_stages = [
        "offline_preflight",
        "traceability_lock",
        "leak_and_route_integrity_plan",
        "conditioning_plan",
        "static_environment_matrix_plan",
        "interference_plan",
        "dynamic_dependency_plan",
        "h2o_recovery_memory_plan",
        "post_run_anchor_check_plan",
        "evidence_closure_plan",
    ]
    if required_stages != expected_stages:
        raise ValueError("GA-D4 must retain the controlled protocol stage order")
    required_components = dict(payload.get("required_uncertainty_components") or {})
    expected_components = {
        "co2": {
            "certified_gas_standard",
            "gas_delivery_and_blending",
            "pressure_reference",
            "temperature_reference",
            "flow_reference",
            "analyzer_repeatability",
            "drift_and_hysteresis",
            "cross_interference",
            "digital_resolution",
        },
        "h2o": {
            "humidity_reference",
            "dry_gas_residual",
            "gas_delivery_and_blending",
            "pressure_reference",
            "temperature_reference",
            "flow_reference",
            "analyzer_repeatability",
            "drift_and_hysteresis",
            "sorption_and_memory",
            "cross_interference",
            "digital_resolution",
        },
    }
    for gas, expected in expected_components.items():
        observed = [str(item) for item in required_components.get(gas) or []]
        if set(observed) != expected or len(observed) != len(expected):
            raise ValueError(f"GA-D4 {gas} must retain all uncertainty components")
    if payload.get("uncertainty_coverage_factor") != 2.0:
        raise ValueError("GA-D4 uncertainty coverage factor must remain k=2")
    required_artifact_roles = list(payload.get("required_artifact_roles") or [])
    if (
        set(required_artifact_roles)
        != {
            "execution_rows",
            "execution_summary",
            "diagnostic_analysis",
            "formal_analysis",
        }
        or len(required_artifact_roles) != 4
    ):
        raise ValueError("GA-D4 must retain all evidence artifact roles")
    return payload


def generate_gas_analyzer_bench_readiness_fixture(
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    grid = dict(contract.get("environment_grid") or {})
    gas_contracts = dict(contract.get("gas_protocols") or {})
    interference_contracts = dict(contract.get("interference_protocols") or {})
    gas_plans = []
    for index, gas in enumerate(("co2", "h2o")):
        channel = dict(gas_contracts.get(gas) or {})
        seed = 20260724 + index
        environment_cells = [
            {
                "temperature_c": float(temperature),
                "pressure_hpa": float(pressure),
                "flow_slpm": float(flow),
            }
            for temperature, pressure, flow in product(
                list(grid.get("temperatures_c") or []),
                list(grid.get("pressures_hpa") or []),
                list(grid.get("flows_slpm") or []),
            )
        ]
        Random(seed).shuffle(environment_cells)
        expected_count = (
            len(environment_cells)
            * len(list(channel.get("target_levels") or []))
            * len(list(grid.get("sequence_directions") or []))
            * len(list(grid.get("sessions") or []))
            * len(list(grid.get("replicates") or []))
        )
        conditioning = {
            "stability_window_s": 180.0 if gas == "co2" else 360.0,
            "max_slope_span_fraction_per_min": (0.0001 if gas == "co2" else 0.0002),
            "max_sigma_span_fraction": (0.0001 if gas == "co2" else 0.0002),
            "max_wait_s": 900.0 if gas == "co2" else 1800.0,
            "wet_to_dry_recovery_fraction": None if gas == "co2" else 0.005,
            "recovery_hold_s": None if gas == "co2" else 600.0,
        }
        gas_plans.append(
            {
                "gas": gas,
                "anchor_role": channel.get("anchor_role"),
                "anchor_target": channel.get("anchor_target"),
                "route_mode": (
                    "co2_zero_span_dry_path"
                    if gas == "co2"
                    else "h2o_dry_wet_conditioned_path"
                ),
                "target_levels": list(channel.get("target_levels") or []),
                "sequence_directions": list(grid.get("sequence_directions") or []),
                "sessions": list(grid.get("sessions") or []),
                "replicates": list(grid.get("replicates") or []),
                "randomization_method": "seeded_shuffle",
                "randomization_seed": seed,
                "environment_cell_order": environment_cells,
                "planned_measurement_row_count": expected_count,
                "conditioning": conditioning,
                "post_run_anchor_check": True,
            }
        )
    interference_plans = [
        {
            "gas": gas,
            "interferent_name": row.get("interferent_name"),
            "target_value": row.get("target_value"),
            "interferent_values": list(row.get("interferent_values") or []),
            "replicates": list(row.get("replicates") or []),
        }
        for gas, row in (
            (gas, dict(interference_contracts.get(gas) or {})) for gas in ("co2", "h2o")
        )
    ]
    protocol = {
        "schema_version": "gas_analyzer_bench_protocol_plan_v1",
        "protocol_id": "ga_d4_clean_offline_bench_readiness",
        "protocol_date": contract.get("protocol_date"),
        "execution_mode": "offline_protocol_design",
        "analyzer_id": "GA01",
        "analyzer_model": "GA-SIM-01",
        "analyzer_serial_number": "GA01-SIM-2026",
        "analyzer_firmware_version": "sim-fw-1.0",
        "software_build_id": "ga-d4-simulated-fixture",
        "gas_analyzer_only": True,
        "ec_flux_in_scope": False,
        "device_io_requested": False,
        "automatic_execution_requested": False,
        "coefficient_fit_requested": False,
        "coefficient_write_requested": False,
        "database_write_requested": False,
        "real_primary_latest_refresh_requested": False,
        "bench_controls": {
            "leak_hold_s": 600.0,
            "pressure_decay_hpa_per_min": 0.02,
            "purge_volume_exchanges": 8.0,
            "h2o_wetted_path_material": "PFA",
            "h2o_wetted_path_temperature_controlled": True,
            "shared_timebase": True,
            "safe_exhaust": True,
        },
        "gas_plans": gas_plans,
        "interference_plans": interference_plans,
        "stages": list(contract.get("required_protocol_stages") or []),
        "evidence_plan": {
            "required_artifact_roles": list(
                contract.get("required_artifact_roles") or []
            ),
            "raw_data_immutable": True,
            "lineage_required": True,
            "formal_analysis_state": "blocked_until_real_acceptance",
        },
    }
    return protocol, _fixture_assets(), _fixture_uncertainty_budgets(contract)


def build_gas_analyzer_bench_readiness_offline_report(
    *,
    report_root: Path,
    run_name: str = "gas_analyzer_bench_readiness_contract",
    contract_path: str | Path = DEFAULT_GAS_ANALYZER_BENCH_READINESS_CONTRACT_PATH,
) -> dict[str, Any]:
    contract = load_gas_analyzer_bench_readiness_contract(contract_path)
    report_dir = Path(report_root) / str(run_name)
    report_dir.mkdir(parents=True, exist_ok=True)
    protocol, assets, budgets = generate_gas_analyzer_bench_readiness_fixture(contract)
    readiness = analyze_gas_analyzer_bench_readiness(
        protocol,
        assets,
        budgets,
        contract=contract,
    )
    acceptance = build_gas_analyzer_bench_readiness_acceptance(
        readiness,
        contract=contract,
    )
    status = "MATCH" if acceptance["all_readiness_gates_passed"] else "MISMATCH"
    inputs_path = _write_json(
        report_dir / "gas_analyzer_bench_readiness_inputs.json",
        {
            "artifact_type": "gas_analyzer_bench_readiness_input_bundle",
            "artifact_role": "execution_summary",
            "schema_version": "gas_analyzer_bench_readiness_input_bundle_v1",
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
            "promotion_state": "blocked",
            "protocol": protocol,
            "traceability_assets": assets,
            "uncertainty_budgets": budgets,
        },
    )
    report = {
        "artifact_type": "gas_analyzer_bench_readiness_report",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "gas_analyzer_bench_readiness_report_v1",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "compare_status": status,
        "evidence_source": "simulated",
        "evidence_state": "simulated_protocol",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_protocol_readiness",
        "promotion_state": "blocked",
        "contract_id": contract.get("contract_id"),
        "contract_path": str(Path(contract_path).resolve()),
        "protocol_design_ready": acceptance["protocol_design_ready"],
        "ready_for_real_execution": False,
        "execution_authorization_status": "not_requested",
        "real_acceptance_status": "blocked",
        "device_io_status": "not_attempted",
        "coefficient_fit_status": "not_attempted",
        "coefficient_writeback_status": "not_attempted",
        "database_write_status": "not_attempted",
        "real_primary_latest_refresh_status": "not_attempted",
        "ec_flux_status": "not_in_scope",
        "readiness": readiness,
        "acceptance": acceptance,
        "artifacts": {"execution_summary": str(inputs_path)},
        "boundary_note": acceptance["boundary_note"],
    }
    report_json = _write_json(
        report_dir / "gas_analyzer_bench_readiness_report.json",
        report,
    )
    report_markdown = report_dir / "gas_analyzer_bench_readiness_report.md"
    report_markdown.write_text(_format_markdown(report), encoding="utf-8")
    report["artifacts"].update(
        {
            "report_json": str(report_json),
            "report_markdown": str(report_markdown),
        }
    )
    _write_json(report_json, report)
    return {
        "status": status,
        "compare_status": status,
        "report_dir": str(report_dir),
        "report_json": str(report_json),
        "report_markdown": str(report_markdown),
        "execution_summary": str(inputs_path),
        "report": report,
    }


def _fixture_assets() -> list[dict[str, Any]]:
    definitions = (
        ("STD-CO2-ZERO", "co2_zero_gas_standard", "CZ-001", "CERT-CZ-2026", 0.3, "ppm"),
        ("STD-CO2-SPAN", "co2_span_gas_standard", "CS-001", "CERT-CS-2026", 0.5, "ppm"),
        (
            "REF-H2O-DRY",
            "h2o_dry_reference",
            "HD-001",
            "CERT-HD-2026",
            0.01,
            "mmol_mol",
        ),
        (
            "REF-H2O-WET",
            "h2o_humidity_reference",
            "HW-001",
            "CERT-HW-2026",
            0.02,
            "mmol_mol",
        ),
        ("REF-PRESSURE", "pressure_reference", "PR-001", "CERT-PR-2026", 0.05, "hpa"),
        (
            "REF-TEMPERATURE",
            "temperature_reference",
            "TR-001",
            "CERT-TR-2026",
            0.02,
            "deg_c",
        ),
        ("REF-FLOW", "flow_reference", "FR-001", "CERT-FR-2026", 0.01, "slpm"),
        ("REF-TIMEBASE", "timebase_reference", "TB-001", "CERT-TB-2026", 0.0001, "s"),
    )
    return [
        {
            "asset_id": asset_id,
            "role": role,
            "serial_number": serial,
            "certificate_id": certificate,
            "calibration_date": "2026-01-15",
            "due_date": "2027-01-15",
            "status": "active",
            "traceability_chain": "national_metrology_institute_chain",
            "standard_uncertainty": uncertainty,
            "uncertainty_unit": unit,
        }
        for asset_id, role, serial, certificate, uncertainty, unit in definitions
    ]


def _fixture_uncertainty_budgets(
    contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    values = {
        "co2": {
            "certified_gas_standard": 0.0005,
            "gas_delivery_and_blending": 0.0004,
            "pressure_reference": 0.0002,
            "temperature_reference": 0.0002,
            "flow_reference": 0.0001,
            "analyzer_repeatability": 0.0003,
            "drift_and_hysteresis": 0.0004,
            "cross_interference": 0.0003,
            "digital_resolution": 0.0001,
        },
        "h2o": {
            "humidity_reference": 0.0008,
            "dry_gas_residual": 0.0005,
            "gas_delivery_and_blending": 0.0005,
            "pressure_reference": 0.0003,
            "temperature_reference": 0.0003,
            "flow_reference": 0.0002,
            "analyzer_repeatability": 0.0004,
            "drift_and_hysteresis": 0.0005,
            "sorption_and_memory": 0.0010,
            "cross_interference": 0.0005,
            "digital_resolution": 0.0002,
        },
    }
    return [
        {
            "gas": gas,
            "coverage_factor": contract.get("uncertainty_coverage_factor"),
            "components": [
                {
                    "name": name,
                    "standard_uncertainty_span_fraction": value,
                }
                for name, value in channel.items()
            ],
        }
        for gas, channel in values.items()
    ]


def _format_markdown(report: Mapping[str, Any]) -> str:
    readiness = dict(report.get("readiness") or {})
    traceability = dict(readiness.get("traceability") or {})
    lines = [
        "# GA-D4 气体分析仪真实台架协议准备度报告",
        "",
        f"- 状态：{report.get('status')}",
        f"- 协议设计就绪：{report.get('protocol_design_ready')}",
        f"- 真实执行就绪：{report.get('ready_for_real_execution')}",
        f"- 执行授权：{report.get('execution_authorization_status')}",
        f"- 真实验收：{report.get('real_acceptance_status')}",
        f"- 设备 I/O：{report.get('device_io_status')}",
        f"- 系数写入：{report.get('coefficient_writeback_status')}",
        f"- 数据库写入：{report.get('database_write_status')}",
        "",
        "## 可溯源资产",
        "",
        f"- 资产数量：{traceability.get('asset_count')}",
        f"- 缺失角色：{traceability.get('missing_roles')}",
        f"- 无效证书：{traceability.get('invalid_certificate_roles')}",
        "",
        "## 气体通道",
        "",
        "| 气体 | 锚点 | 环境格点 | 计划测量行 | 扩展不确定度 | 限值 |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for channel in list(readiness.get("channels") or []):
        uncertainty = dict(channel.get("uncertainty_budget") or {})
        environment = dict(channel.get("environment_order") or {})
        lines.append(
            "| {gas} | {anchor} | {cells} | {rows} | {expanded} | {limit} |".format(
                gas=channel.get("gas"),
                anchor=channel.get("anchor_role"),
                cells=environment.get("observed_cell_count"),
                rows=channel.get("planned_measurement_row_count"),
                expanded=uncertainty.get("expanded_uncertainty_span_fraction"),
                limit=uncertainty.get("maximum_allowed"),
            )
        )
    failed = list(dict(report.get("acceptance") or {}).get("failed_gate_names") or [])
    lines.extend(["", "## 失败门禁", ""])
    lines.extend(f"- {item}" for item in failed)
    if not failed:
        lines.append("- 无")
    lines.extend(
        [
            "",
            "## 边界",
            "",
            f"- {report.get('boundary_note')}",
            "- CO2 零气与 H2O 干气点保持独立，不能合并为一个低端锚点。",
            "- 本报告不启动真实台架、不连接 COM、不产生执行许可。",
            "- 所有 formal_analysis 仍须等待独立真实验收授权和真实证据。",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "DEFAULT_GAS_ANALYZER_BENCH_READINESS_CONTRACT_PATH",
    "build_gas_analyzer_bench_readiness_offline_report",
    "generate_gas_analyzer_bench_readiness_fixture",
    "load_gas_analyzer_bench_readiness_contract",
]
