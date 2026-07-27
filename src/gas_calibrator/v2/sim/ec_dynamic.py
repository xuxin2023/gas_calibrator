"""Simulation-only EC dynamic response fixtures and suite artifact builder."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np

from gas_calibrator.utils.file_io import write_json as _write_json

from ..domain.services.ec_dynamic_metrology import (
    DynamicPathMetadata,
    analyze_dynamic_channel,
    build_dynamic_acceptance,
)


DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "metrology"
    / "ec_dynamic_acceptance_contract_v1.json"
)


@dataclass(frozen=True)
class DynamicProtocolDefinition:
    protocol_id: str
    gas: str
    duration_s: float
    step_up_s: float
    step_down_s: float
    baseline_value: float
    step_value: float
    timestamp_jitter_std_s: float = 0.0005
    dropout_fraction: float = 0.0
    noise_std: float = 0.0
    random_seed: int = 20260724

    def validate(self) -> None:
        gas = str(self.gas or "").strip().lower()
        if gas not in {"co2", "h2o"}:
            raise ValueError(f"unsupported gas: {self.gas}")
        if not str(self.protocol_id or "").strip():
            raise ValueError("protocol_id is required")
        if float(self.duration_s) <= 0.0:
            raise ValueError("duration_s must be positive")
        if not 0.0 < float(self.step_up_s) < float(self.step_down_s) < float(self.duration_s):
            raise ValueError("step times must satisfy 0 < step_up < step_down < duration")
        if float(self.step_value) == float(self.baseline_value):
            raise ValueError("step_value must differ from baseline_value")
        if float(self.timestamp_jitter_std_s) < 0.0:
            raise ValueError("timestamp_jitter_std_s must be non-negative")
        if not 0.0 <= float(self.dropout_fraction) < 1.0:
            raise ValueError("dropout_fraction must be within [0, 1)")
        if float(self.noise_std) < 0.0:
            raise ValueError("noise_std must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        payload = asdict(self)
        payload["gas"] = str(self.gas).lower()
        return payload


def load_ec_dynamic_contract(path: str | Path = DEFAULT_CONTRACT_PATH) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if str(payload.get("schema_version") or "") != "ec_dynamic_acceptance_contract_v1":
        raise ValueError("unexpected EC dynamic acceptance contract schema")
    boundary = dict(payload.get("evidence_boundary") or {})
    if boundary.get("evidence_source") != "simulated":
        raise ValueError("EC-D0 contract must require evidence_source=simulated")
    if boundary.get("not_real_acceptance_evidence") is not True:
        raise ValueError("EC-D0 contract must block real acceptance")
    if boundary.get("promotion_state") != "blocked":
        raise ValueError("EC-D0 contract must block promotion")
    for forbidden_flag in (
        "real_primary_latest_refresh_allowed",
        "device_io_allowed",
        "coefficient_write_allowed",
    ):
        if boundary.get(forbidden_flag) is not False:
            raise ValueError(f"EC-D0 contract must set {forbidden_flag}=false")
    return payload


def simulate_dynamic_protocol(
    protocol: DynamicProtocolDefinition,
    paths: Iterable[DynamicPathMetadata],
) -> dict[str, Any]:
    """Generate bidirectional step responses with per-path delay and H2O memory."""

    protocol.validate()
    path_rows = list(paths)
    if not path_rows:
        raise ValueError("at least one dynamic path is required")
    for path in path_rows:
        path.validate()
        if str(path.gas).lower() != str(protocol.gas).lower():
            raise ValueError("protocol gas and path gas must match")

    channels: list[dict[str, Any]] = []
    for path_index, path in enumerate(path_rows):
        channels.append(
            _simulate_path(
                protocol=protocol,
                path=path,
                seed=int(protocol.random_seed) + 1009 * path_index,
            )
        )
    return {
        "artifact_type": "ec_dynamic_simulated_series",
        "artifact_role": "execution_rows",
        "schema_version": "ec_dynamic_simulated_series_v1",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "protocol": protocol.to_dict(),
        "channels": channels,
    }


def build_ec_dynamic_offline_report(
    *,
    report_root: Path,
    run_name: str = "ec_dynamic_offline_contract",
    contract_path: str | Path = DEFAULT_CONTRACT_PATH,
) -> dict[str, Any]:
    """Build the clean EC-D0 CO2/H2O fixture report used by simulation suites."""

    contract = load_ec_dynamic_contract(contract_path)
    report_dir = Path(report_root) / str(run_name)
    report_dir.mkdir(parents=True, exist_ok=True)
    fixture_definitions = _default_fixture_definitions()
    series_payloads: list[dict[str, Any]] = []
    analyses: list[dict[str, Any]] = []
    for protocol, paths in fixture_definitions:
        series = simulate_dynamic_protocol(protocol, paths)
        series_payloads.append(series)
        analyses.extend(
            analyze_dynamic_channel(channel, protocol=series["protocol"])
            for channel in list(series.get("channels") or [])
        )

    acceptance = build_dynamic_acceptance(
        analyses,
        contract=contract,
        protocol_id="ec_d0_clean_co2_h2o_chain",
    )
    status = "MATCH" if acceptance["all_fixture_gates_passed"] else "MISMATCH"
    raw_path = _write_json(
        report_dir / "ec_dynamic_simulated_series.json",
        {
            "artifact_type": "ec_dynamic_simulated_series_bundle",
            "artifact_role": "execution_rows",
            "schema_version": "ec_dynamic_simulated_series_bundle_v1",
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
            "promotion_state": "blocked",
            "series": series_payloads,
        },
    )
    report = {
        "artifact_type": "ec_dynamic_offline_report",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "ec_dynamic_offline_report_v1",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "compare_status": status,
        "evidence_source": "simulated",
        "evidence_state": "simulated_protocol",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "acceptance_scope": "ec_dynamic_simulation_contract",
        "promotion_state": "blocked",
        "ready_for_promotion": False,
        "contract_id": contract.get("contract_id"),
        "contract_schema_version": contract.get("schema_version"),
        "contract_path": str(Path(contract_path).resolve()),
        "static_calibration_status": "not_evaluated",
        "ec_dynamic_status": acceptance["ec_dynamic_status"],
        "real_acceptance_status": "blocked",
        "protocol_count": len(series_payloads),
        "channel_count": len(analyses),
        "analyses": analyses,
        "acceptance": acceptance,
        "artifacts": {
            "simulated_series": str(raw_path),
        },
        "boundary_note": acceptance["boundary_note"],
    }
    report_json = _write_json(report_dir / "ec_dynamic_offline_report.json", report)
    report_markdown = report_dir / "ec_dynamic_offline_report.md"
    report_markdown.write_text(_format_report_markdown(report), encoding="utf-8")
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
        "simulated_series": str(raw_path),
        "report": report,
    }


def _simulate_path(
    *,
    protocol: DynamicProtocolDefinition,
    path: DynamicPathMetadata,
    seed: int,
) -> dict[str, Any]:
    sample_rate_hz = float(path.sample_rate_hz)
    sample_interval_s = 1.0 / sample_rate_hz
    sample_count = int(np.floor(float(protocol.duration_s) * sample_rate_hz)) + 1
    ideal_times = np.arange(sample_count, dtype=float) * sample_interval_s
    input_values = _source_values(ideal_times, protocol)
    delayed_values = _source_values(ideal_times - float(path.transport_delay_s), protocol)
    fast_values = _first_order_response(
        delayed_values,
        sample_interval_s=sample_interval_s,
        rise_tau_s=float(path.fast_rise_tau_s),
        fall_tau_s=float(path.fast_fall_tau_s),
        initial_value=float(protocol.baseline_value),
    )
    slow_values = _first_order_response(
        delayed_values,
        sample_interval_s=sample_interval_s,
        rise_tau_s=float(path.memory_rise_tau_s),
        fall_tau_s=float(path.memory_fall_tau_s),
        initial_value=float(protocol.baseline_value),
    )
    memory_fraction = float(path.memory_fraction)
    response_values = (1.0 - memory_fraction) * fast_values + memory_fraction * slow_values
    rng = np.random.default_rng(int(seed))
    if float(protocol.noise_std) > 0.0:
        response_values = response_values + rng.normal(0.0, float(protocol.noise_std), size=sample_count)

    clock_intervals = np.full(max(sample_count - 1, 0), sample_interval_s, dtype=float)
    if float(protocol.timestamp_jitter_std_s) > 0.0 and clock_intervals.size:
        clock_intervals += rng.normal(
            0.0,
            float(protocol.timestamp_jitter_std_s),
            size=clock_intervals.size,
        )
        clock_intervals = np.maximum(clock_intervals, 0.05 * sample_interval_s)
    observed_times = np.concatenate((np.asarray([0.0]), np.cumsum(clock_intervals)))
    keep_mask = np.ones(sample_count, dtype=bool)
    if float(protocol.dropout_fraction) > 0.0:
        candidate_indices = np.arange(1, sample_count - 1)
        dropout_count = min(
            len(candidate_indices),
            int(round(float(protocol.dropout_fraction) * sample_count)),
        )
        if dropout_count:
            dropped = rng.choice(candidate_indices, size=dropout_count, replace=False)
            keep_mask[dropped] = False

    sample_indices = np.arange(sample_count, dtype=int)
    return {
        "analyzer_id": path.analyzer_id,
        "gas": str(path.gas).lower(),
        "metadata": path.to_dict(),
        "expected_sample_count": int(sample_count),
        "sample_indices": sample_indices[keep_mask].tolist(),
        "timestamps_s": np.round(observed_times[keep_mask], 12).tolist(),
        "values": np.round(response_values[keep_mask], 12).tolist(),
        "input_reference": {
            "timestamps_s": np.round(ideal_times, 12).tolist(),
            "values": np.round(input_values, 12).tolist(),
        },
        "synthetic_truth": {
            "transport_delay_s": float(path.transport_delay_s),
            "fast_rise_tau_s": float(path.fast_rise_tau_s),
            "fast_fall_tau_s": float(path.fast_fall_tau_s),
            "memory_fraction": memory_fraction,
            "memory_rise_tau_s": float(path.memory_rise_tau_s),
            "memory_fall_tau_s": float(path.memory_fall_tau_s),
        },
    }


def _source_values(times: np.ndarray, protocol: DynamicProtocolDefinition) -> np.ndarray:
    baseline = float(protocol.baseline_value)
    high = float(protocol.step_value)
    return np.where(
        (times >= float(protocol.step_up_s)) & (times < float(protocol.step_down_s)),
        high,
        baseline,
    ).astype(float)


def _first_order_response(
    targets: np.ndarray,
    *,
    sample_interval_s: float,
    rise_tau_s: float,
    fall_tau_s: float,
    initial_value: float,
) -> np.ndarray:
    output = np.empty_like(targets, dtype=float)
    state = float(initial_value)
    for index, target in enumerate(targets):
        target_value = float(target)
        tau = rise_tau_s if target_value >= state else fall_tau_s
        alpha = 1.0 - np.exp(-sample_interval_s / max(float(tau), 1e-12))
        state += alpha * (target_value - state)
        output[index] = state
    return output


def _default_fixture_definitions() -> list[tuple[DynamicProtocolDefinition, list[DynamicPathMetadata]]]:
    co2_protocol = DynamicProtocolDefinition(
        protocol_id="ec_d0_co2_bidirectional_step",
        gas="co2",
        duration_s=45.0,
        step_up_s=5.0,
        step_down_s=25.0,
        baseline_value=400.0,
        step_value=800.0,
        timestamp_jitter_std_s=0.0005,
        noise_std=0.01,
        random_seed=20260724,
    )
    co2_paths = [
        DynamicPathMetadata(
            analyzer_id="GA01",
            gas="co2",
            serial_position=1,
            sample_rate_hz=20.0,
            tube_length_m=1.0,
            tube_inner_diameter_mm=4.0,
            tube_material="PTFE",
            flow_slpm=12.0,
            cell_pressure_hpa=1000.0,
            cell_temperature_c=25.0,
            relative_humidity_pct=30.0,
            heated_tube=False,
            filter_id="SIM-FILTER-A",
            transport_delay_s=0.20,
            fast_rise_tau_s=0.18,
            fast_fall_tau_s=0.20,
        ),
        DynamicPathMetadata(
            analyzer_id="GA02",
            gas="co2",
            serial_position=2,
            sample_rate_hz=20.0,
            tube_length_m=2.5,
            tube_inner_diameter_mm=4.0,
            tube_material="PTFE",
            flow_slpm=12.0,
            cell_pressure_hpa=998.0,
            cell_temperature_c=25.0,
            relative_humidity_pct=30.0,
            heated_tube=False,
            filter_id="SIM-FILTER-A",
            transport_delay_s=0.45,
            fast_rise_tau_s=0.22,
            fast_fall_tau_s=0.24,
        ),
    ]
    h2o_protocol = DynamicProtocolDefinition(
        protocol_id="ec_d0_h2o_bidirectional_step",
        gas="h2o",
        duration_s=70.0,
        step_up_s=5.0,
        step_down_s=35.0,
        baseline_value=2.0,
        step_value=20.0,
        timestamp_jitter_std_s=0.0005,
        noise_std=0.001,
        random_seed=20260725,
    )
    h2o_paths = [
        DynamicPathMetadata(
            analyzer_id="GA01",
            gas="h2o",
            serial_position=1,
            sample_rate_hz=20.0,
            tube_length_m=1.0,
            tube_inner_diameter_mm=4.0,
            tube_material="PTFE",
            flow_slpm=12.0,
            cell_pressure_hpa=1000.0,
            cell_temperature_c=30.0,
            relative_humidity_pct=65.0,
            heated_tube=True,
            filter_id="SIM-FILTER-H",
            transport_delay_s=0.30,
            fast_rise_tau_s=0.30,
            fast_fall_tau_s=0.42,
            memory_fraction=0.10,
            memory_rise_tau_s=1.1,
            memory_fall_tau_s=1.5,
        ),
        DynamicPathMetadata(
            analyzer_id="GA02",
            gas="h2o",
            serial_position=2,
            sample_rate_hz=20.0,
            tube_length_m=2.5,
            tube_inner_diameter_mm=4.0,
            tube_material="PTFE",
            flow_slpm=12.0,
            cell_pressure_hpa=998.0,
            cell_temperature_c=30.0,
            relative_humidity_pct=65.0,
            heated_tube=True,
            filter_id="SIM-FILTER-H",
            transport_delay_s=0.65,
            fast_rise_tau_s=0.34,
            fast_fall_tau_s=0.48,
            memory_fraction=0.12,
            memory_rise_tau_s=1.3,
            memory_fall_tau_s=1.8,
        ),
    ]
    return [(co2_protocol, co2_paths), (h2o_protocol, h2o_paths)]


def _format_report_markdown(report: Mapping[str, Any]) -> str:
    acceptance = dict(report.get("acceptance") or {})
    lines = [
        "# EC-D0 离线动态计量报告",
        "",
        f"- 状态：{report.get('status')}",
        f"- 静态校准状态：{report.get('static_calibration_status')}",
        f"- EC 动态状态：{report.get('ec_dynamic_status')}",
        f"- 真实验收状态：{report.get('real_acceptance_status')}",
        f"- 证据来源：{report.get('evidence_source')}",
        f"- promotion_state：{report.get('promotion_state')}",
        f"- 通道数：{report.get('channel_count')}",
        "",
        "## 通道摘要",
        "",
        "| 分析仪 | 气体 | 串联位置 | 增益 | t90/s | 延迟/s | tau/s | 抖动 | 丢帧 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in list(report.get("analyses") or []):
        timing = dict(item.get("timing") or {})
        rise = dict(item.get("rise") or {})
        lines.append(
            "| {analyzer} | {gas} | {position} | {gain} | {t90} | {delay} | {tau} | {jitter} | {dropout} |".format(
                analyzer=item.get("analyzer_id"),
                gas=item.get("gas"),
                position=item.get("serial_position"),
                gain=item.get("gain"),
                t90=rise.get("t90_s"),
                delay=item.get("effective_rise_delay_s"),
                tau=item.get("effective_rise_tau_s"),
                jitter=timing.get("interval_jitter_ratio"),
                dropout=timing.get("dropout_fraction"),
            )
        )
    lines.extend(
        [
            "",
            "## 失败门禁",
            "",
        ]
    )
    failed = list(acceptance.get("failed_gate_names") or [])
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
            "- static_calibration_status 与 ec_dynamic_status 必须独立解释。",
            "- 该报告不连接 COM、不写系数、不刷新 real_primary_latest。",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "DEFAULT_CONTRACT_PATH",
    "DynamicProtocolDefinition",
    "build_ec_dynamic_offline_report",
    "load_ec_dynamic_contract",
    "simulate_dynamic_protocol",
]
