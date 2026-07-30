"""Simulation-only integrated gas-analyzer operating-envelope fixtures for GA-D3."""

from __future__ import annotations

from datetime import datetime
from itertools import product
import json
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.utils.file_io import write_json as _write_json

from gas_calibrator.validation.metrology.gas_analyzer_operating_envelope import (
    analyze_gas_analyzer_operating_envelope,
    build_gas_analyzer_operating_envelope_acceptance,
)
from .gas_analyzer_dynamic_uncertainty import (
    build_gas_analyzer_dynamic_uncertainty_offline_report,
)


DEFAULT_GAS_ANALYZER_OPERATING_ENVELOPE_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "metrology"
    / "gas_analyzer_operating_envelope_contract_v1.json"
)


def load_gas_analyzer_operating_envelope_contract(
    path: str | Path = DEFAULT_GAS_ANALYZER_OPERATING_ENVELOPE_CONTRACT_PATH,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if (
        str(payload.get("schema_version") or "")
        != "gas_analyzer_operating_envelope_contract_v1"
    ):
        raise ValueError("unexpected GA-D3 operating-envelope contract schema")
    boundary = dict(payload.get("evidence_boundary") or {})
    required_boundary = {
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "promotion_state": "blocked",
        "real_primary_latest_refresh_allowed": False,
        "device_io_allowed": False,
        "coefficient_write_allowed": False,
        "automatic_dynamic_correction_allowed": False,
    }
    for key, value in required_boundary.items():
        if boundary.get(key) != value:
            raise ValueError(f"GA-D3 contract must set {key}={str(value).lower()}")
    interpretation = dict(payload.get("interpretation") or {})
    if interpretation.get("gas_analyzer_only") is not True:
        raise ValueError("GA-D3 contract must remain gas_analyzer_only=true")
    if interpretation.get("ec_flux_or_cospectral_correction_in_scope") is not False:
        raise ValueError(
            "GA-D3 contract must set ec_flux_or_cospectral_correction_in_scope=false"
        )
    if interpretation.get("coefficient_fitting_or_writeback_in_scope") is not False:
        raise ValueError(
            "GA-D3 contract must set coefficient_fitting_or_writeback_in_scope=false"
        )
    if interpretation.get("co2_zero_gas_and_h2o_dry_gas_are_distinct") is not True:
        raise ValueError(
            "GA-D3 contract must keep CO2 zero and H2O dry anchors distinct"
        )
    if (
        interpretation.get("qualified_bounds_require_complete_rectangular_grid")
        is not True
    ):
        raise ValueError("GA-D3 contract must require a complete rectangular grid")
    if interpretation.get("limits_are_product_specifications") is not False:
        raise ValueError(
            "GA-D3 fixture limits must not be labeled product specifications"
        )
    gases = dict(payload.get("gas_contracts") or {})
    expected_anchors = {
        "co2": ("co2_zero_gas", 0.0),
        "h2o": ("h2o_dry_gas", 0.2),
    }
    for gas, (role, target) in expected_anchors.items():
        channel = dict(gases.get(gas) or {})
        if str(channel.get("anchor_role") or "") != role:
            raise ValueError(f"GA-D3 {gas} anchor role must be {role}")
        if abs(float(channel.get("anchor_target")) - target) > 1e-12:
            raise ValueError(f"GA-D3 {gas} anchor target must be {target}")
        targets = [float(item) for item in list(channel.get("target_levels") or [])]
        if target not in targets or len(targets) < 3:
            raise ValueError(f"GA-D3 {gas} target grid is invalid")
        for key in (
            "span_value",
            "max_anchor_absolute_error",
            "min_ten_percent_bandwidth_hz",
            "max_low_frequency_effective_phase_delay_s",
        ):
            if float(channel.get(key) or 0.0) <= 0.0:
                raise ValueError(f"GA-D3 {gas} must define positive {key}")
    grid = dict(payload.get("environment_grid") or {})
    for key in ("temperatures_c", "pressures_hpa", "flows_slpm"):
        if len(list(grid.get(key) or [])) < 2:
            raise ValueError(f"GA-D3 environment grid must define at least two {key}")
    for key in ("sequence_directions", "sessions", "replicates"):
        if not list(grid.get(key) or []):
            raise ValueError(f"GA-D3 environment grid must define {key}")
    if set(grid.get("sequence_directions") or []) != {"ascending", "descending"}:
        raise ValueError("GA-D3 environment grid must retain both sequence directions")
    if set(grid.get("sessions") or []) != {"start", "end"}:
        raise ValueError("GA-D3 environment grid must retain start and end sessions")
    if {int(item) for item in list(grid.get("replicates") or [])} != {1, 2, 3}:
        raise ValueError("GA-D3 environment grid must retain three replicates")
    for key, value in dict(payload.get("common_fixture_limits") or {}).items():
        if float(value or 0.0) <= 0.0:
            raise ValueError(f"GA-D3 common fixture limit must be positive: {key}")
    interference_contracts = dict(payload.get("interference_contracts") or {})
    expected_interferents = {"co2": "h2o", "h2o": "co2"}
    for gas, interferent in expected_interferents.items():
        sweep = dict(interference_contracts.get(gas) or {})
        if str(sweep.get("interferent_name") or "").lower() != interferent:
            raise ValueError(f"GA-D3 {gas} interference sweep must use {interferent}")
        if len(list(sweep.get("interferent_values") or [])) < 2:
            raise ValueError(
                f"GA-D3 {gas} interference sweep needs at least two levels"
            )
        if not list(sweep.get("replicates") or []):
            raise ValueError(f"GA-D3 {gas} interference sweep needs replicates")
    expected_measurement_fields = {
        "analyzer_id",
        "gas",
        "temperature_c",
        "pressure_hpa",
        "flow_slpm",
        "target_value",
        "anchor_role",
        "sequence_direction",
        "session",
        "replicate",
        "reference_value",
        "measured_value",
        "reference_quality",
        "frame_usable",
    }
    if not expected_measurement_fields.issubset(
        set(payload.get("required_measurement_fields") or [])
    ):
        raise ValueError("GA-D3 contract is missing required measurement fields")
    expected_interference_fields = {
        "analyzer_id",
        "gas",
        "interferent_name",
        "interferent_value",
        "target_value",
        "reference_value",
        "measured_value",
        "replicate",
        "reference_quality",
        "frame_usable",
    }
    if not expected_interference_fields.issubset(
        set(payload.get("required_interference_fields") or [])
    ):
        raise ValueError("GA-D3 contract is missing required interference fields")
    return payload


def generate_gas_analyzer_static_envelope_fixture(
    contract: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grid = dict(contract.get("environment_grid") or {})
    gases = dict(contract.get("gas_contracts") or {})
    rows: list[dict[str, Any]] = []
    for gas in ("co2", "h2o"):
        channel = dict(gases.get(gas) or {})
        for (
            temperature,
            pressure,
            flow,
            target,
            direction,
            session,
            replicate,
        ) in product(
            list(grid.get("temperatures_c") or []),
            list(grid.get("pressures_hpa") or []),
            list(grid.get("flows_slpm") or []),
            list(channel.get("target_levels") or []),
            list(grid.get("sequence_directions") or []),
            list(grid.get("sessions") or []),
            list(grid.get("replicates") or []),
        ):
            target_value = float(target)
            rows.append(
                {
                    "analyzer_id": "GA01",
                    "gas": gas,
                    "unit": str(channel.get("unit") or ""),
                    "temperature_c": float(temperature),
                    "pressure_hpa": float(pressure),
                    "flow_slpm": float(flow),
                    "target_value": target_value,
                    "anchor_role": (
                        str(channel.get("anchor_role") or "")
                        if abs(target_value - float(channel["anchor_target"])) <= 1e-12
                        else ""
                    ),
                    "sequence_direction": str(direction),
                    "session": str(session),
                    "replicate": int(replicate),
                    "reference_value": target_value,
                    "measured_value": round(
                        target_value
                        + _simulated_static_error(
                            gas=gas,
                            target=target_value,
                            temperature=float(temperature),
                            pressure=float(pressure),
                            flow=float(flow),
                            direction=str(direction),
                            session=str(session),
                            replicate=int(replicate),
                        ),
                        9,
                    ),
                    "reference_quality": "healthy",
                    "frame_usable": True,
                }
            )
    return rows, _generate_interference_fixture(contract)


def build_gas_analyzer_operating_envelope_offline_report(
    *,
    report_root: Path,
    run_name: str = "gas_analyzer_operating_envelope_contract",
    contract_path: str | Path = DEFAULT_GAS_ANALYZER_OPERATING_ENVELOPE_CONTRACT_PATH,
) -> dict[str, Any]:
    contract = load_gas_analyzer_operating_envelope_contract(contract_path)
    report_dir = Path(report_root) / str(run_name)
    report_dir.mkdir(parents=True, exist_ok=True)
    measurement_rows, interference_rows = generate_gas_analyzer_static_envelope_fixture(
        contract
    )
    dynamic_result = build_gas_analyzer_dynamic_uncertainty_offline_report(
        report_root=report_dir,
        run_name="ga_d2_dynamic_dependency",
    )
    dynamic_report = dict(dynamic_result.get("report") or {})
    performances = list(dynamic_report.get("performances") or [])
    envelope = analyze_gas_analyzer_operating_envelope(
        measurement_rows,
        interference_rows,
        performances,
        contract=contract,
    )
    acceptance = build_gas_analyzer_operating_envelope_acceptance(
        envelope,
        contract=contract,
        protocol_id="ga_d3_clean_integrated_operating_envelope",
    )
    status = "MATCH" if acceptance["all_fixture_gates_passed"] else "MISMATCH"
    inputs_path = _write_json(
        report_dir / "gas_analyzer_operating_envelope_inputs.json",
        {
            "artifact_type": "gas_analyzer_operating_envelope_input_bundle",
            "artifact_role": "execution_rows",
            "schema_version": "gas_analyzer_operating_envelope_input_bundle_v1",
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
            "promotion_state": "blocked",
            "measurement_rows": measurement_rows,
            "interference_rows": interference_rows,
            "dynamic_dependency": {
                "artifact_role": "diagnostic_analysis",
                "report_json": dynamic_result.get("report_json"),
                "status": dynamic_result.get("status"),
            },
        },
    )
    report = {
        "artifact_type": "gas_analyzer_operating_envelope_report",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "gas_analyzer_operating_envelope_report_v1",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "compare_status": status,
        "evidence_source": "simulated",
        "evidence_state": "simulated_protocol",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "acceptance_scope": "gas_analyzer_integrated_operating_envelope_fixture",
        "promotion_state": "blocked",
        "ready_for_promotion": False,
        "contract_id": contract.get("contract_id"),
        "contract_path": str(Path(contract_path).resolve()),
        "static_calibration_status": acceptance["static_calibration_status"],
        "gas_analyzer_dynamic_status": acceptance["gas_analyzer_dynamic_status"],
        "operating_envelope_status": acceptance["operating_envelope_status"],
        "ec_flux_status": "not_in_scope",
        "real_acceptance_status": "blocked",
        "coefficient_fit_status": "not_applied",
        "coefficient_writeback_status": "not_applied",
        "dynamic_correction_status": "not_applied",
        "measurement_row_count": len(measurement_rows),
        "interference_row_count": len(interference_rows),
        "envelope": envelope,
        "acceptance": acceptance,
        "artifacts": {
            "execution_rows": str(inputs_path),
            "ga_d2_dynamic_dependency_report": dynamic_result.get("report_json"),
        },
        "boundary_note": acceptance["boundary_note"],
    }
    report_json = _write_json(
        report_dir / "gas_analyzer_operating_envelope_report.json",
        report,
    )
    report_markdown = report_dir / "gas_analyzer_operating_envelope_report.md"
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
        "execution_rows": str(inputs_path),
        "ga_d2_dynamic_dependency_report": dynamic_result.get("report_json"),
        "report": report,
    }


def _simulated_static_error(
    *,
    gas: str,
    target: float,
    temperature: float,
    pressure: float,
    flow: float,
    direction: str,
    session: str,
    replicate: int,
) -> float:
    replicate_pattern = {1: -1.0, 2: 0.0, 3: 1.0}.get(replicate, 0.0)
    if gas == "co2":
        return (
            0.4
            + 0.0010 * target
            + 0.010 * (temperature - 25.0)
            + 0.001 * (pressure - 1000.0)
            + 0.020 * (flow - 12.0)
            + (0.12 if direction == "ascending" else -0.12)
            + (0.20 if session == "end" else 0.0)
            + 0.10 * replicate_pattern
        )
    return (
        0.010
        + 0.0020 * target
        + 0.0005 * (temperature - 25.0)
        + 0.00002 * (pressure - 1000.0)
        + 0.001 * (flow - 12.0)
        + (0.006 if direction == "ascending" else -0.006)
        + (0.006 if session == "end" else 0.0)
        + 0.004 * replicate_pattern
    )


def _generate_interference_fixture(
    contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    sweeps = dict(contract.get("interference_contracts") or {})
    sensitivities = {
        "co2": (0.020, 0.2),
        "h2o": (0.000005, 0.0),
    }
    rows: list[dict[str, Any]] = []
    for gas in ("co2", "h2o"):
        sweep = dict(sweeps.get(gas) or {})
        interferent = str(sweep.get("interferent_name") or "")
        target = float(sweep.get("target_value"))
        sensitivity, baseline = sensitivities[gas]
        for level, replicate in product(
            list(sweep.get("interferent_values") or []),
            list(sweep.get("replicates") or []),
        ):
            noise_scale = 0.03 if gas == "co2" else 0.001
            noise = noise_scale * {1: -1.0, 2: 0.0, 3: 1.0}[replicate]
            rows.append(
                {
                    "analyzer_id": "GA01",
                    "gas": gas,
                    "interferent_name": interferent,
                    "interferent_value": float(level),
                    "target_value": target,
                    "reference_value": target,
                    "measured_value": round(
                        target + sensitivity * (float(level) - baseline) + noise,
                        9,
                    ),
                    "replicate": replicate,
                    "reference_quality": "healthy",
                    "frame_usable": True,
                }
            )
    return rows


def _format_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# GA-D3 气体分析仪综合工作包络报告",
        "",
        f"- 状态：{report.get('status')}",
        f"- 静态校准状态：{report.get('static_calibration_status')}",
        f"- 分析仪动态状态：{report.get('gas_analyzer_dynamic_status')}",
        f"- 综合工作包络状态：{report.get('operating_envelope_status')}",
        f"- EC 通量状态：{report.get('ec_flux_status')}",
        f"- 真实验收状态：{report.get('real_acceptance_status')}",
        f"- 证据来源：{report.get('evidence_source')}",
        "",
        "## 通道包络",
        "",
        "| 气体 | 锚点角色 | 温度/°C | 压力/hPa | 流量/slpm | 最大量程归一化误差 | 10% 带宽/Hz | 等效延迟/s | 失败格点 |",
        "|---|---|---|---|---|---:|---:|---:|---:|",
    ]
    envelope = dict(report.get("envelope") or {})
    for channel in list(envelope.get("channels") or []):
        qualified = dict(channel.get("qualified_operating_envelope") or {})
        metrics = dict(channel.get("metrics") or {})
        dynamic = dict(channel.get("dynamic_performance") or {})
        lines.append(
            "| {gas} | {anchor} | {temperature} | {pressure} | {flow} | {error} | {bandwidth} | {delay} | {failed} |".format(
                gas=channel.get("gas"),
                anchor=channel.get("anchor_role"),
                temperature=_format_range(qualified.get("temperature_range_c")),
                pressure=_format_range(qualified.get("pressure_range_hpa")),
                flow=_format_range(qualified.get("flow_range_slpm")),
                error=metrics.get("max_span_normalized_error"),
                bandwidth=dynamic.get("usable_bandwidth_hz"),
                delay=dynamic.get("low_frequency_effective_phase_delay_s"),
                failed=qualified.get("failed_cell_count"),
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
            "## 科学边界",
            "",
            "- CO2 零气锚点与 H2O 干气点分别评价，不能互相替代。",
            "- 完整矩形环境网格是包络成立的前提；缺角、重复或越界点都会拒绝。",
            "- 本报告只评价气体分析仪，不包含涡动协方差、协谱或通量闭合。",
            "- 不拟合、不写入校准系数，不自动应用动态反卷积修正。",
            "- 仿真限值用于回归门禁，不是实际产品规格或真实验收证据。",
            "- 不连接 COM、不刷新 real_primary_latest。",
            "",
        ]
    )
    return "\n".join(lines)


def _format_range(value: Any) -> str:
    bounds = dict(value or {})
    return f"{bounds.get('minimum')}–{bounds.get('maximum')}"


__all__ = [
    "DEFAULT_GAS_ANALYZER_OPERATING_ENVELOPE_CONTRACT_PATH",
    "build_gas_analyzer_operating_envelope_offline_report",
    "generate_gas_analyzer_static_envelope_fixture",
    "load_gas_analyzer_operating_envelope_contract",
]
