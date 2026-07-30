"""Simulation-only PRBS system-identification fixtures for EC-D1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from gas_calibrator.utils.file_io import write_json as _write_json
from gas_calibrator.validation.metrology.ec_dynamic_metrology import DynamicPathMetadata
from gas_calibrator.validation.metrology.ec_system_identification import (
    build_system_identification_acceptance,
    identify_empirical_transfer,
)



DEFAULT_SYSTEM_ID_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "metrology"
    / "ec_dynamic_system_identification_contract_v1.json"
)


@dataclass(frozen=True)
class SystemIdentificationProtocol:
    protocol_id: str
    gas: str
    sample_rate_hz: float
    duration_s: float
    chip_rate_hz: float
    baseline_value: float
    amplitude: float
    source_delay_s: float
    source_tau_s: float
    reference_delay_s: float
    reference_tau_s: float
    reference_noise_std: float = 0.001
    dut_noise_std: float = 0.001
    timestamp_jitter_std_s: float = 0.0
    prbs_order: int = 9
    random_seed: int = 20260724
    include_upstream_reference: bool = True

    def validate(self) -> None:
        if str(self.gas or "").lower() not in {"co2", "h2o"}:
            raise ValueError(f"unsupported gas: {self.gas}")
        if not str(self.protocol_id or "").strip():
            raise ValueError("protocol_id is required")
        if float(self.sample_rate_hz) <= 0.0 or float(self.duration_s) <= 0.0:
            raise ValueError("sample_rate_hz and duration_s must be positive")
        if not 0.0 < float(self.chip_rate_hz) < 0.5 * float(self.sample_rate_hz):
            raise ValueError("chip_rate_hz must be positive and below Nyquist")
        if float(self.amplitude) <= 0.0:
            raise ValueError("amplitude must be positive")
        for name in ("source_tau_s", "reference_tau_s"):
            if float(getattr(self, name)) <= 0.0:
                raise ValueError(f"{name} must be positive")
        for name in (
            "source_delay_s",
            "reference_delay_s",
            "reference_noise_std",
            "dut_noise_std",
            "timestamp_jitter_std_s",
        ):
            if float(getattr(self, name)) < 0.0:
                raise ValueError(f"{name} must be non-negative")
        if int(self.prbs_order) != 9:
            raise ValueError("EC-D1 currently fixes prbs_order=9 for a verified 511-chip period")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        payload = asdict(self)
        payload["gas"] = str(self.gas).lower()
        return payload


def generate_prbs(*, length: int, order: int = 9) -> np.ndarray:
    """Generate the verified x^9 + x^5 + 1 maximal-length binary sequence."""

    if int(order) != 9:
        raise ValueError("only verified order=9 is supported")
    if int(length) <= 0:
        raise ValueError("length must be positive")
    state = [1] * 9
    period: list[int] = []
    for _ in range(2**9 - 1):
        period.append(int(state[-1]))
        feedback = int(state[-1] ^ state[-5])
        state = [feedback, *state[:-1]]
    repeats = int(np.ceil(int(length) / len(period)))
    return np.asarray((period * repeats)[: int(length)], dtype=float)


def load_system_identification_contract(
    path: str | Path = DEFAULT_SYSTEM_ID_CONTRACT_PATH,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if str(payload.get("schema_version") or "") != "ec_dynamic_system_identification_contract_v1":
        raise ValueError("unexpected EC-D1 system-identification contract schema")
    boundary = dict(payload.get("evidence_boundary") or {})
    required = {
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "promotion_state": "blocked",
        "real_primary_latest_refresh_allowed": False,
        "device_io_allowed": False,
        "coefficient_write_allowed": False,
    }
    for key, value in required.items():
        if boundary.get(key) != value:
            raise ValueError(f"EC-D1 contract must set {key}={str(value).lower()}")
    return payload


def simulate_system_identification(
    protocol: SystemIdentificationProtocol,
    path: DynamicPathMetadata,
    *,
    target_frequencies_hz: Sequence[float],
) -> dict[str, Any]:
    protocol.validate()
    path.validate()
    if str(protocol.gas).lower() != str(path.gas).lower():
        raise ValueError("protocol gas and path gas must match")
    if abs(float(protocol.sample_rate_hz) - float(path.sample_rate_hz)) > 1e-9:
        raise ValueError("protocol and path sample rates must match")
    if abs(float(path.fast_rise_tau_s) - float(path.fast_fall_tau_s)) > 1e-9:
        raise ValueError("EC-D1 LTI fixture requires equal fast rise/fall time constants")
    if abs(float(path.memory_rise_tau_s) - float(path.memory_fall_tau_s)) > 1e-9:
        raise ValueError("EC-D1 LTI fixture requires equal memory rise/fall time constants")

    sample_rate = float(protocol.sample_rate_hz)
    interval = 1.0 / sample_rate
    sample_count = int(np.floor(float(protocol.duration_s) * sample_rate))
    chip_samples = max(1, int(round(sample_rate / float(protocol.chip_rate_hz))))
    chip_count = int(np.ceil(sample_count / chip_samples))
    chips = generate_prbs(length=chip_count, order=int(protocol.prbs_order))
    normalized_command = np.repeat(2.0 * chips - 1.0, chip_samples)[:sample_count]
    command = float(protocol.baseline_value) + float(protocol.amplitude) * normalized_command
    source_input = _delay_values(
        command,
        delay_s=float(protocol.source_delay_s),
        sample_rate_hz=sample_rate,
        fill_value=float(protocol.baseline_value),
    )
    source_truth = _first_order(
        source_input,
        sample_interval_s=interval,
        tau_s=float(protocol.source_tau_s),
        initial_value=float(protocol.baseline_value),
    )
    reference_input = _delay_values(
        source_truth,
        delay_s=float(protocol.reference_delay_s),
        sample_rate_hz=sample_rate,
        fill_value=float(protocol.baseline_value),
    )
    reference = _first_order(
        reference_input,
        sample_interval_s=interval,
        tau_s=float(protocol.reference_tau_s),
        initial_value=float(protocol.baseline_value),
    )
    dut_input = _delay_values(
        source_truth,
        delay_s=float(path.transport_delay_s),
        sample_rate_hz=sample_rate,
        fill_value=float(protocol.baseline_value),
    )
    fast = _first_order(
        dut_input,
        sample_interval_s=interval,
        tau_s=float(path.fast_rise_tau_s),
        initial_value=float(protocol.baseline_value),
    )
    slow = _first_order(
        dut_input,
        sample_interval_s=interval,
        tau_s=float(path.memory_rise_tau_s),
        initial_value=float(protocol.baseline_value),
    )
    memory_fraction = float(path.memory_fraction)
    dut = (1.0 - memory_fraction) * fast + memory_fraction * slow
    rng = np.random.default_rng(int(protocol.random_seed))
    reference = reference + rng.normal(0.0, float(protocol.reference_noise_std), sample_count)
    dut = dut + rng.normal(0.0, float(protocol.dut_noise_std), sample_count)
    intervals = np.full(max(sample_count - 1, 0), interval, dtype=float)
    if float(protocol.timestamp_jitter_std_s) > 0.0 and len(intervals):
        intervals += rng.normal(0.0, float(protocol.timestamp_jitter_std_s), len(intervals))
        intervals = np.maximum(intervals, 0.05 * interval)
    timestamps = np.concatenate((np.asarray([0.0]), np.cumsum(intervals)))
    truth_points = _relative_truth_points(
        path=path,
        protocol=protocol,
        target_frequencies_hz=target_frequencies_hz,
    )
    return {
        "artifact_type": "ec_dynamic_system_identification_series",
        "artifact_role": "execution_rows",
        "schema_version": "ec_dynamic_system_identification_series_v1",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "protocol": protocol.to_dict(),
        "metadata": path.to_dict(),
        "synchronization": {
            "clock_domain": "shared_simulated_sample_clock",
            "reference_and_dut_time_aligned": True,
        },
        "timestamps_s": np.round(timestamps, 12).tolist(),
        "command_values": np.round(command, 12).tolist(),
        "source_truth_values": np.round(source_truth, 12).tolist(),
        "upstream_reference_values": (
            np.round(reference, 12).tolist()
            if protocol.include_upstream_reference
            else []
        ),
        "dut_values": np.round(dut, 12).tolist(),
        "synthetic_truth": {
            "source_delay_s": float(protocol.source_delay_s),
            "source_tau_s": float(protocol.source_tau_s),
            "reference_delay_s": float(protocol.reference_delay_s),
            "reference_tau_s": float(protocol.reference_tau_s),
            "relative_transfer_points": truth_points,
        },
    }


def build_ec_system_identification_offline_report(
    *,
    report_root: Path,
    run_name: str = "ec_dynamic_system_identification_contract",
    contract_path: str | Path = DEFAULT_SYSTEM_ID_CONTRACT_PATH,
) -> dict[str, Any]:
    contract = load_system_identification_contract(contract_path)
    targets = [float(item) for item in list(contract.get("target_frequencies_hz") or [])]
    report_dir = Path(report_root) / str(run_name)
    report_dir.mkdir(parents=True, exist_ok=True)
    series_rows: list[dict[str, Any]] = []
    analyses: list[dict[str, Any]] = []
    for protocol, path in default_system_identification_fixtures():
        series = simulate_system_identification(
            protocol,
            path,
            target_frequencies_hz=targets,
        )
        series_rows.append(series)
        analyses.append(
            identify_empirical_transfer(
                series,
                target_frequencies_hz=targets,
                warmup_s=float(contract.get("warmup_s") or 10.0),
                segment_size=int(contract.get("segment_size") or 512),
            )
        )
    acceptance = build_system_identification_acceptance(
        analyses,
        contract=contract,
        protocol_id="ec_d1_clean_co2_h2o_prbs",
    )
    status = "MATCH" if acceptance["all_fixture_gates_passed"] else "MISMATCH"
    series_path = _write_json(
        report_dir / "ec_dynamic_system_identification_series.json",
        {
            "artifact_type": "ec_dynamic_system_identification_series_bundle",
            "artifact_role": "execution_rows",
            "schema_version": "ec_dynamic_system_identification_series_bundle_v1",
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
            "promotion_state": "blocked",
            "series": series_rows,
        },
    )
    report = {
        "artifact_type": "ec_dynamic_system_identification_report",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "ec_dynamic_system_identification_report_v1",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "compare_status": status,
        "evidence_source": "simulated",
        "evidence_state": "simulated_protocol",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "acceptance_scope": "ec_dynamic_system_identification_fixture",
        "promotion_state": "blocked",
        "ready_for_promotion": False,
        "contract_id": contract.get("contract_id"),
        "contract_path": str(Path(contract_path).resolve()),
        "static_calibration_status": "not_evaluated",
        "ec_dynamic_status": acceptance["ec_dynamic_status"],
        "real_acceptance_status": "blocked",
        "analysis_count": len(analyses),
        "analyses": analyses,
        "acceptance": acceptance,
        "artifacts": {"simulated_series": str(series_path)},
        "boundary_note": acceptance["boundary_note"],
    }
    report_json = _write_json(report_dir / "ec_dynamic_system_identification_report.json", report)
    report_markdown = report_dir / "ec_dynamic_system_identification_report.md"
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
        "simulated_series": str(series_path),
        "report": report,
    }


def default_system_identification_fixtures() -> list[
    tuple[SystemIdentificationProtocol, DynamicPathMetadata]
]:
    co2_protocol = SystemIdentificationProtocol(
        protocol_id="ec_d1_co2_prbs",
        gas="co2",
        sample_rate_hz=20.0,
        duration_s=280.0,
        chip_rate_hz=2.0,
        baseline_value=600.0,
        amplitude=200.0,
        source_delay_s=0.10,
        source_tau_s=0.30,
        reference_delay_s=0.05,
        reference_tau_s=0.04,
        reference_noise_std=0.01,
        dut_noise_std=0.01,
        random_seed=20260724,
    )
    co2_path = DynamicPathMetadata(
        analyzer_id="GA01",
        gas="co2",
        serial_position=1,
        sample_rate_hz=20.0,
        tube_length_m=1.5,
        tube_inner_diameter_mm=4.0,
        tube_material="PTFE",
        flow_slpm=12.0,
        cell_pressure_hpa=1000.0,
        cell_temperature_c=25.0,
        relative_humidity_pct=30.0,
        heated_tube=False,
        filter_id="SIM-FILTER-D1-C",
        transport_delay_s=0.30,
        fast_rise_tau_s=0.25,
        fast_fall_tau_s=0.25,
    )
    h2o_protocol = SystemIdentificationProtocol(
        protocol_id="ec_d1_h2o_prbs",
        gas="h2o",
        sample_rate_hz=20.0,
        duration_s=280.0,
        chip_rate_hz=2.0,
        baseline_value=11.0,
        amplitude=9.0,
        source_delay_s=0.10,
        source_tau_s=0.30,
        reference_delay_s=0.05,
        reference_tau_s=0.04,
        reference_noise_std=0.001,
        dut_noise_std=0.001,
        random_seed=20260725,
    )
    h2o_path = DynamicPathMetadata(
        analyzer_id="GA01",
        gas="h2o",
        serial_position=1,
        sample_rate_hz=20.0,
        tube_length_m=1.5,
        tube_inner_diameter_mm=4.0,
        tube_material="PTFE",
        flow_slpm=12.0,
        cell_pressure_hpa=1000.0,
        cell_temperature_c=30.0,
        relative_humidity_pct=65.0,
        heated_tube=True,
        filter_id="SIM-FILTER-D1-H",
        transport_delay_s=0.50,
        fast_rise_tau_s=0.40,
        fast_fall_tau_s=0.40,
        memory_fraction=0.15,
        memory_rise_tau_s=1.50,
        memory_fall_tau_s=1.50,
    )
    return [(co2_protocol, co2_path), (h2o_protocol, h2o_path)]


def _relative_truth_points(
    *,
    path: DynamicPathMetadata,
    protocol: SystemIdentificationProtocol,
    target_frequencies_hz: Sequence[float],
) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    for frequency in target_frequencies_hz:
        ref = _digital_first_order(
            frequency_hz=float(frequency),
            sample_rate_hz=float(protocol.sample_rate_hz),
            tau_s=float(protocol.reference_tau_s),
        ) * np.exp(-1j * 2.0 * np.pi * float(frequency) * float(protocol.reference_delay_s))
        fast = _digital_first_order(
            frequency_hz=float(frequency),
            sample_rate_hz=float(protocol.sample_rate_hz),
            tau_s=float(path.fast_rise_tau_s),
        )
        slow = _digital_first_order(
            frequency_hz=float(frequency),
            sample_rate_hz=float(protocol.sample_rate_hz),
            tau_s=float(path.memory_rise_tau_s),
        )
        dut = (
            (1.0 - float(path.memory_fraction)) * fast
            + float(path.memory_fraction) * slow
        ) * np.exp(-1j * 2.0 * np.pi * float(frequency) * float(path.transport_delay_s))
        relative = dut / ref
        rows.append(
            {
                "frequency_hz": round(float(frequency), 9),
                "amplitude_ratio": round(abs(relative), 12),
                "phase_deg": round(float(np.angle(relative, deg=True)), 12),
            }
        )
    return rows


def _digital_first_order(*, frequency_hz: float, sample_rate_hz: float, tau_s: float) -> complex:
    alpha = float(np.exp(-1.0 / (float(sample_rate_hz) * float(tau_s))))
    z_inverse = np.exp(-1j * 2.0 * np.pi * float(frequency_hz) / float(sample_rate_hz))
    return complex((1.0 - alpha) / (1.0 - alpha * z_inverse))


def _delay_values(
    values: np.ndarray,
    *,
    delay_s: float,
    sample_rate_hz: float,
    fill_value: float,
) -> np.ndarray:
    delay_samples = float(delay_s) * float(sample_rate_hz)
    positions = np.arange(len(values), dtype=float) - delay_samples
    return np.interp(
        positions,
        np.arange(len(values), dtype=float),
        values,
        left=float(fill_value),
        right=float(values[-1]),
    )


def _first_order(
    targets: np.ndarray,
    *,
    sample_interval_s: float,
    tau_s: float,
    initial_value: float,
) -> np.ndarray:
    output = np.empty_like(targets, dtype=float)
    state = float(initial_value)
    alpha = 1.0 - np.exp(-float(sample_interval_s) / float(tau_s))
    for index, target in enumerate(targets):
        state += alpha * (float(target) - state)
        output[index] = state
    return output


def _format_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# EC-D1 离线系统辨识报告",
        "",
        f"- 状态：{report.get('status')}",
        f"- EC 动态状态：{report.get('ec_dynamic_status')}",
        f"- 真实验收状态：{report.get('real_acceptance_status')}",
        f"- 证据来源：{report.get('evidence_source')}",
        f"- promotion_state：{report.get('promotion_state')}",
        "",
        "## 传递函数点",
        "",
        "| 气体 | 分析仪 | 频率/Hz | 幅值比 | 相位/° | 相干性 | 幅值误差 | 相位误差/° |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for analysis in list(report.get("analyses") or []):
        for point in list(analysis.get("relative_transfer_points") or []):
            lines.append(
                "| {gas} | {analyzer} | {frequency} | {amplitude} | {phase} | {coherence} | {amp_error} | {phase_error} |".format(
                    gas=analysis.get("gas"),
                    analyzer=analysis.get("analyzer_id"),
                    frequency=point.get("frequency_hz"),
                    amplitude=point.get("amplitude_ratio"),
                    phase=point.get("phase_deg"),
                    coherence=point.get("coherence"),
                    amp_error=point.get("amplitude_relative_error"),
                    phase_error=point.get("phase_absolute_error_deg"),
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
            "## 证据边界",
            "",
            f"- {report.get('boundary_note')}",
            "- 上游参考是系统辨识的强制输入；命令信号只用于诊断气源传递链。",
            "- 区间为 Welch 分段离散度，不是正式计量置信区间。",
            "- 不连接 COM、不写系数、不刷新 real_primary_latest。",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "DEFAULT_SYSTEM_ID_CONTRACT_PATH",
    "SystemIdentificationProtocol",
    "build_ec_system_identification_offline_report",
    "default_system_identification_fixtures",
    "generate_prbs",
    "load_system_identification_contract",
    "simulate_system_identification",
]
