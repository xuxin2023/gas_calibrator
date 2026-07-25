"""Simulation-only gas-analyzer dynamic uncertainty fixtures for GA-D2."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.utils.file_io import write_json as _write_json

from ..domain.services.ec_system_identification import identify_empirical_transfer
from ..domain.services.gas_analyzer_dynamic_uncertainty import (
    analyze_gas_analyzer_dynamic_performance,
    build_gas_analyzer_dynamic_uncertainty_acceptance,
)
from .ec_system_identification import (
    default_system_identification_fixtures,
    simulate_system_identification,
)


DEFAULT_GAS_ANALYZER_DYNAMIC_UNCERTAINTY_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "metrology"
    / "gas_analyzer_dynamic_uncertainty_contract_v1.json"
)


def load_gas_analyzer_dynamic_uncertainty_contract(
    path: str | Path = DEFAULT_GAS_ANALYZER_DYNAMIC_UNCERTAINTY_CONTRACT_PATH,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if (
        str(payload.get("schema_version") or "")
        != "gas_analyzer_dynamic_uncertainty_contract_v1"
    ):
        raise ValueError("unexpected GA-D2 dynamic uncertainty contract schema")
    boundary = dict(payload.get("evidence_boundary") or {})
    required = {
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "promotion_state": "blocked",
        "real_primary_latest_refresh_allowed": False,
        "device_io_allowed": False,
        "coefficient_write_allowed": False,
        "automatic_dynamic_correction_allowed": False,
    }
    for key, value in required.items():
        if boundary.get(key) != value:
            raise ValueError(f"GA-D2 contract must set {key}={str(value).lower()}")
    interpretation = dict(payload.get("interpretation") or {})
    if interpretation.get("gas_analyzer_only") is not True:
        raise ValueError("GA-D2 contract must remain gas_analyzer_only=true")
    if interpretation.get("ec_flux_or_cospectral_correction_in_scope") is not False:
        raise ValueError(
            "GA-D2 contract must set ec_flux_or_cospectral_correction_in_scope=false"
        )
    return payload


def build_gas_analyzer_dynamic_uncertainty_offline_report(
    *,
    report_root: Path,
    run_name: str = "gas_analyzer_dynamic_uncertainty_contract",
    contract_path: str | Path = DEFAULT_GAS_ANALYZER_DYNAMIC_UNCERTAINTY_CONTRACT_PATH,
) -> dict[str, Any]:
    contract = load_gas_analyzer_dynamic_uncertainty_contract(contract_path)
    grid = dict(contract.get("evaluation_grid") or {})
    sample_rate_hz = float(grid.get("sample_rate_hz") or 20.0)
    segment_size = int(grid.get("segment_size") or 512)
    first_bin = int(grid.get("first_positive_bin") or 1)
    last_bin = int(grid.get("last_positive_bin") or first_bin)
    if first_bin < 1 or last_bin < first_bin:
        raise ValueError("GA-D2 evaluation bins are invalid")
    frequencies = [
        index * sample_rate_hz / segment_size
        for index in range(first_bin, last_bin + 1)
    ]
    report_dir = Path(report_root) / str(run_name)
    report_dir.mkdir(parents=True, exist_ok=True)
    identifications: list[dict[str, Any]] = []
    performances: list[dict[str, Any]] = []
    fixture_protocols: list[dict[str, Any]] = []
    for protocol, path in default_system_identification_fixtures():
        if abs(float(protocol.sample_rate_hz) - sample_rate_hz) > 1e-9:
            raise ValueError("GA-D2 contract sample rate must match fixture sample rate")
        series = simulate_system_identification(
            protocol,
            path,
            target_frequencies_hz=frequencies,
        )
        identification = identify_empirical_transfer(
            series,
            target_frequencies_hz=frequencies,
            warmup_s=float(grid.get("warmup_s") or 10.0),
            segment_size=segment_size,
        )
        performance = analyze_gas_analyzer_dynamic_performance(
            identification,
            contract=contract,
        )
        identifications.append(identification)
        performances.append(performance)
        fixture_protocols.append(
            {
                "protocol": protocol.to_dict(),
                "path": path.to_dict(),
            }
        )
    acceptance = build_gas_analyzer_dynamic_uncertainty_acceptance(
        performances,
        contract=contract,
        protocol_id="ga_d2_clean_co2_h2o_dynamic_uncertainty",
    )
    status = "MATCH" if acceptance["all_fixture_gates_passed"] else "MISMATCH"
    inputs_path = _write_json(
        report_dir / "gas_analyzer_dynamic_uncertainty_inputs.json",
        {
            "artifact_type": "gas_analyzer_dynamic_uncertainty_input_bundle",
            "artifact_role": "execution_summary",
            "schema_version": "gas_analyzer_dynamic_uncertainty_input_bundle_v1",
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
            "promotion_state": "blocked",
            "evaluation_frequencies_hz": frequencies,
            "fixtures": fixture_protocols,
            "system_identifications": identifications,
        },
    )
    report = {
        "artifact_type": "gas_analyzer_dynamic_uncertainty_report",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "gas_analyzer_dynamic_uncertainty_report_v1",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "compare_status": status,
        "evidence_source": "simulated",
        "evidence_state": "simulated_protocol",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "acceptance_scope": "gas_analyzer_dynamic_uncertainty_fixture",
        "promotion_state": "blocked",
        "ready_for_promotion": False,
        "contract_id": contract.get("contract_id"),
        "contract_path": str(Path(contract_path).resolve()),
        "static_calibration_status": "not_evaluated",
        "gas_analyzer_dynamic_status": acceptance["gas_analyzer_dynamic_status"],
        "ec_flux_status": "not_in_scope",
        "real_acceptance_status": "blocked",
        "dynamic_correction_status": "not_applied",
        "performance_count": len(performances),
        "performances": performances,
        "acceptance": acceptance,
        "artifacts": {"system_identification_inputs": str(inputs_path)},
        "boundary_note": acceptance["boundary_note"],
    }
    report_json = _write_json(
        report_dir / "gas_analyzer_dynamic_uncertainty_report.json",
        report,
    )
    report_markdown = report_dir / "gas_analyzer_dynamic_uncertainty_report.md"
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
        "system_identification_inputs": str(inputs_path),
        "report": report,
    }


def _format_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# GA-D2 气体分析仪动态性能与工程不确定度报告",
        "",
        f"- 状态：{report.get('status')}",
        f"- 分析仪动态状态：{report.get('gas_analyzer_dynamic_status')}",
        f"- EC 通量状态：{report.get('ec_flux_status')}",
        f"- 真实验收状态：{report.get('real_acceptance_status')}",
        f"- 动态修正状态：{report.get('dynamic_correction_status')}",
        f"- 证据来源：{report.get('evidence_source')}",
        f"- promotion_state：{report.get('promotion_state')}",
        "",
        "## 动态性能摘要",
        "",
        "| 气体 | 分析仪 | 5% 带宽/Hz | 10% 带宽/Hz | -3 dB 带宽/Hz | 低频等效相位延迟/s | 最大扩展幅值不确定度 | 最大扩展相位不确定度/° |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for performance in list(report.get("performances") or []):
        bandwidths = dict(performance.get("bandwidths") or {})
        budget = dict(performance.get("uncertainty_budget") or {})
        lines.append(
            "| {gas} | {analyzer} | {five} | {ten} | {db3} | {delay} | {amp_u} | {phase_u} |".format(
                gas=performance.get("gas"),
                analyzer=performance.get("analyzer_id"),
                five=dict(bandwidths.get("five_percent_attenuation") or {}).get(
                    "frequency_hz"
                ),
                ten=dict(bandwidths.get("ten_percent_attenuation") or {}).get(
                    "frequency_hz"
                ),
                db3=dict(bandwidths.get("minus_3db") or {}).get("frequency_hz"),
                delay=performance.get("low_frequency_effective_phase_delay_s"),
                amp_u=budget.get("max_expanded_amplitude_relative_uncertainty"),
                phase_u=budget.get("max_expanded_phase_uncertainty_deg"),
            )
        )
    failed = list(dict(report.get("acceptance") or {}).get("failed_gate_names") or [])
    lines.extend(["", "## 失败门禁", ""])
    if failed:
        lines.extend(f"- {item}" for item in failed)
    else:
        lines.append("- 无")
    lines.extend(
        [
            "",
            "## 科学边界",
            "",
            f"- {report.get('boundary_note')}",
            "- 动态衰减作为系统性偏差单独报告，不并入不确定度掩盖。",
            "- 当前预算为离线工程预算，不是正式计量不确定度声明。",
            "- 不输出反卷积系数，不自动修正生产数据。",
            "- 不连接 COM、不写系数、不刷新 real_primary_latest。",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "DEFAULT_GAS_ANALYZER_DYNAMIC_UNCERTAINTY_CONTRACT_PATH",
    "build_gas_analyzer_dynamic_uncertainty_offline_report",
    "load_gas_analyzer_dynamic_uncertainty_contract",
]
