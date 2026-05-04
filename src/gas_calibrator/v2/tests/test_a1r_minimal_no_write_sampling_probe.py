from __future__ import annotations

from collections import deque
import json
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.v2.core.run001_a1r_minimal_no_write_sampling_probe import (
    A1R_ENV_VAR,
    evaluate_a1r_minimal_no_write_sampling_gate,
    write_a1r_minimal_no_write_sampling_probe_artifacts,
)


def _write_prereq(tmp_path: Path, name: str, source: str, extra: dict[str, Any] | None = None) -> Path:
    run_dir = tmp_path / name
    run_dir.mkdir()
    payload = {
        "final_decision": "PASS",
        "evidence_source": source,
        "not_real_acceptance_evidence": True,
        "attempted_write_count": 0,
        "any_write_command_sent": False,
    }
    payload.update(extra or {})
    (run_dir / "summary.json").write_text(json.dumps(payload), encoding="utf-8")
    return run_dir


def _base_config(tmp_path: Path) -> dict[str, Any]:
    r0_1_dir = _write_prereq(tmp_path, "r0_1", "real_probe_r0_1_reference_read_only")
    r0_dir = _write_prereq(tmp_path, "r0_full", "real_probe_query_only")
    r1_dir = _write_prereq(
        tmp_path,
        "r1",
        "real_probe_r1_conditioning_only",
        {"r1_conditioning_only_executed": True},
    )
    return {
        "scope": "a1r_minimal_no_write_sampling",
        "r0_1_reference_readonly_output_dir": str(r0_1_dir),
        "r0_full_query_only_output_dir": str(r0_dir),
        "r1_conditioning_only_output_dir": str(r1_dir),
        "co2_only": True,
        "skip0": True,
        "single_route": True,
        "single_temperature": True,
        "one_nonzero_point": True,
        "no_write": True,
        "h2o_enabled": False,
        "full_group_enabled": False,
        "a2_enabled": False,
        "a3_enabled": False,
        "pressure_setpoint_enabled": False,
        "mode_switch_enabled": False,
        "analyzer_id_write_enabled": False,
        "senco_write_enabled": False,
        "calibration_write_enabled": False,
        "chamber_set_temperature_enabled": False,
        "chamber_start_enabled": False,
        "chamber_stop_enabled": False,
        "real_primary_latest_refresh": False,
        "a1r_minimal_no_write_sampling": {
            "scope": "a1r_minimal_no_write_sampling",
            "r0_1_reference_readonly_output_dir": str(r0_1_dir),
            "r0_full_query_only_output_dir": str(r0_dir),
            "r1_conditioning_only_output_dir": str(r1_dir),
            "co2_only": True,
            "skip0": True,
            "single_route": True,
            "single_temperature": True,
            "one_nonzero_point": True,
            "no_write": True,
            "h2o_enabled": False,
            "full_group_enabled": False,
            "a2_enabled": False,
            "a3_enabled": False,
            "pressure_setpoint_enabled": False,
            "mode_switch_enabled": False,
            "analyzer_id_write_enabled": False,
            "senco_write_enabled": False,
            "calibration_write_enabled": False,
            "chamber_set_temperature_enabled": False,
            "chamber_start_enabled": False,
            "chamber_stop_enabled": False,
            "real_primary_latest_refresh": False,
            "point": {"target_co2_ppm": 1000},
            "sampling": {
                "target_co2_ppm": 1000,
                "pressure_overlimit_hpa": 1150,
                "pressure_cache_max_age_ms": 8000,
                "heartbeat_max_age_ms_before_sample": 8000,
                "sample_timeout_s": 0.01,
            },
        },
        "devices": {
            "pressure_controller": {"enabled": True, "port": "COM31", "baud": 9600},
            "pressure_gauge": {"enabled": True, "port": "COM30", "baud": 9600, "response_timeout_s": 0.01},
            "temperature_chamber": {"enabled": True, "port": "COM27", "baud": 9600, "addr": 1},
            "thermometer": {"enabled": True, "port": "COM26", "baud": 2400},
            "relay": {"enabled": True, "port": "COM28", "baud": 38400, "addr": 1},
            "relay_8": {"enabled": True, "port": "COM29", "baud": 38400, "addr": 1},
            "gas_analyzers": [{"name": "ga01", "enabled": True, "port": "COM35", "baud": 115200, "device_id": "001"}],
        },
        "valves": {
            "gas_main": 11,
            "co2_path": 7,
            "relay_map": {
                "6": {"device": "relay", "channel": 12},
                "7": {"device": "relay", "channel": 15},
                "11": {"device": "relay_8", "channel": 3},
            },
            "co2_map": {"1000": 6},
        },
        "workflow": {
            "route_mode": "co2_only",
            "selected_temps_c": [20.0],
            "skip_co2_ppm": [0],
        },
    }


def _operator_confirmation(tmp_path: Path, config_path: Path, config: Mapping[str, Any]) -> Path:
    payload = {
        "operator_name": "pytest",
        "timestamp": "2026-04-27T00:00:00+08:00",
        "branch": "codex/run001-a1-no-write-dry-run",
        "HEAD": "42ba0ad29af16c48b7ff579042a05068f0734cb8",
        "config_path": str(config_path),
        "r0_1_reference_readonly_output_dir": config["r0_1_reference_readonly_output_dir"],
        "r0_full_query_only_output_dir": config["r0_full_query_only_output_dir"],
        "r1_conditioning_only_output_dir": config["r1_conditioning_only_output_dir"],
        "port_manifest": {
            "pressure_controller": "COM31",
            "pressure_gauge": "COM30",
            "temperature_chamber": "COM27",
            "thermometer": "COM26",
            "relay": "COM28",
            "relay_8": "COM29",
            "gas_analyzers": ["COM35"],
        },
        "explicit_acknowledgement": {
            "only_a1r_minimal_no_write_sampling": True,
            "co2_only": True,
            "skip0": True,
            "single_route": True,
            "single_temperature": True,
            "one_nonzero_point": True,
            "no_write": True,
            "no_id_write": True,
            "no_senco_write": True,
            "no_calibration_write": True,
            "no_chamber_sv_write": True,
            "no_chamber_set_temperature": True,
            "no_chamber_start": True,
            "no_chamber_stop": True,
            "no_mode_switch": True,
            "no_pressure_setpoint": True,
            "v1_fallback_required": True,
            "not_real_acceptance": True,
            "engineering_probe_only": True,
            "do_not_refresh_real_primary_latest": True,
            "real_primary_latest_refresh": False,
        },
    }
    path = tmp_path / "operator_confirmation.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _config_and_operator(tmp_path: Path) -> tuple[dict[str, Any], Path, Path]:
    config = _base_config(tmp_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    op_path = _operator_confirmation(tmp_path, config_path, config)
    return config, config_path, op_path


class FakeModbusResponse:
    def __init__(self, registers: list[int] | None = None) -> None:
        self.registers = list(registers or [])

    def isError(self) -> bool:
        return False


class ReadOnlyChamberClient:
    def connect(self) -> bool:
        return True

    def close(self) -> None:
        return None

    def read_input_registers(self, address: int, count: int = 1, **_kwargs):
        return FakeModbusResponse([201] if int(address) == 7991 else [0])

    def read_holding_registers(self, address: int, count: int = 1, **_kwargs):
        return FakeModbusResponse([200] if int(address) == 8100 else [0])

    def write_register(self, *_args, **_kwargs):
        raise AssertionError("A1R must not write chamber registers")


class FakePressureGauge:
    def __init__(self, values: list[float]) -> None:
        self.values = deque(values)

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def read_pressure_fast(self, **_kwargs) -> float:
        if self.values:
            return float(self.values.popleft())
        return 1013.25


class FakePace:
    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def get_device_identity(self) -> str:
        return "PACE5000,READONLY"

    def get_output_state(self) -> int:
        return 0

    def get_isolation_state(self) -> int:
        return 1

    def get_vent_status(self) -> int:
        return 2

    def vent(self, on: bool = True) -> None:
        if not on:
            raise AssertionError("A1R must not send vent-off")


class FakeRelay:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[tuple[int, bool]] = []

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def set_valve(self, channel: int, open_: bool) -> None:
        self.calls.append((int(channel), bool(open_)))


class FakeThermometer:
    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def read_temp_c(self) -> float:
        return 20.2


class FakeAnalyzerSerial:
    def __init__(self, line: bytes = b"YGAS,001,1000.0,0.0,400.0,0.0,1.0,1.0,0.0,0.0,1,2,3,20.0,21.0,101.3") -> None:
        self.line = line

    def readline(self) -> bytes:
        return self.line

    def close(self) -> None:
        return None


def test_a1r_gate_requires_triple_unlock(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    missing_cli = evaluate_a1r_minimal_no_write_sampling_gate(
        config,
        cli_allow=False,
        env={A1R_ENV_VAR: "1"},
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="42ba0ad29af16c48b7ff579042a05068f0734cb8",
        config_path=str(config_path),
    )
    assert missing_cli.approved is False
    assert "missing_cli_flag_allow_v2_a1r_minimal_no_write_real_com" in missing_cli.reasons

    missing_env = evaluate_a1r_minimal_no_write_sampling_gate(
        config,
        cli_allow=True,
        env={},
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="42ba0ad29af16c48b7ff579042a05068f0734cb8",
        config_path=str(config_path),
    )
    assert missing_env.approved is False
    assert "missing_env_gas_cal_v2_a1r_minimal_no_write_real_com" in missing_env.reasons


def test_a1r_gate_requires_r1_pass(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    (Path(config["r1_conditioning_only_output_dir"]) / "summary.json").write_text(
        json.dumps(
            {
                "final_decision": "FAIL_CLOSED",
                "evidence_source": "real_probe_r1_conditioning_only",
                "not_real_acceptance_evidence": True,
                "attempted_write_count": 0,
                "any_write_command_sent": False,
            }
        ),
        encoding="utf-8",
    )

    admission = evaluate_a1r_minimal_no_write_sampling_gate(
        config,
        cli_allow=True,
        env={A1R_ENV_VAR: "1"},
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="42ba0ad29af16c48b7ff579042a05068f0734cb8",
        config_path=str(config_path),
    )

    assert admission.approved is False
    assert any(reason.startswith("r1_prereq_final_decision_not_pass") for reason in admission.reasons)


def test_a1r_sampling_writes_required_artifacts_and_no_forbidden_writes(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    relay_handles: dict[str, FakeRelay] = {}

    def relay_factory(device: Mapping[str, Any]) -> FakeRelay:
        relay = FakeRelay(str(device.get("device_name")))
        relay_handles[str(device.get("device_name"))] = relay
        return relay

    summary = write_a1r_minimal_no_write_sampling_probe_artifacts(
        config,
        output_dir=tmp_path / "a1r",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="42ba0ad29af16c48b7ff579042a05068f0734cb8",
        cli_allow=True,
        env={A1R_ENV_VAR: "1"},
        execute_sampling=True,
        pressure_gauge_factory=lambda _device: FakePressureGauge([1013.2, 1013.3]),
        pace_factory=lambda _device: FakePace(),
        relay_factory=relay_factory,
        thermometer_factory=lambda _device: FakeThermometer(),
        analyzer_serial_factory=lambda _device: FakeAnalyzerSerial(),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
    )

    assert summary["final_decision"] == "PASS"
    assert summary["r0_1_reference_readonly_prereq_pass"] is True
    assert summary["r0_full_query_only_prereq_pass"] is True
    assert summary["r1_conditioning_only_prereq_pass"] is True
    assert summary["a1r_minimal_sampling_executed"] is True
    assert summary["co2_only"] is True
    assert summary["skip0"] is True
    assert summary["single_route"] is True
    assert summary["single_temperature"] is True
    assert summary["one_nonzero_point"] is True
    assert summary["sample_count"] > 0
    assert summary["points_completed"] > 0
    assert summary["pressure_gauge_freshness_ok_before_sample"] is True
    assert summary["route_conditioning_ready_before_sample"] is True
    assert summary["heartbeat_ready_before_sample"] is True
    assert summary["a2_allowed"] is False
    assert summary["a3_allowed"] is False

    assert summary["attempted_write_count"] == 0
    assert summary["any_write_command_sent"] is False
    assert summary["pressure_setpoint_command_sent"] is False
    assert summary["mode_switch_command_sent"] is False
    assert summary["identity_write_command_sent"] is False
    assert summary["senco_write_command_sent"] is False
    assert summary["calibration_write_command_sent"] is False
    assert summary["chamber_set_temperature_command_sent"] is False
    assert summary["chamber_start_command_sent"] is False
    assert summary["chamber_stop_command_sent"] is False
    assert summary["real_primary_latest_refresh"] is False

    assert relay_handles["relay_a"].calls == [(12, True), (15, True), (12, False), (15, False)]
    assert relay_handles["relay_b"].calls == [(3, True), (3, False)]
    for path in summary["artifact_paths"].values():
        assert Path(path).exists()
    sample_rows = Path(summary["artifact_paths"]["analyzer_sampling_rows"]).read_text(encoding="utf-8")
    assert "a1r_minimal_no_write_one_nonzero_point" in sample_rows
    safety = json.loads(Path(summary["artifact_paths"]["safety_assertions"]).read_text(encoding="utf-8"))
    assert safety["attempted_write_count"] == 0
    assert safety["mode_switch_command_sent"] is False


def test_a1r_pressure_overlimit_fails_closed_without_sampling(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    summary = write_a1r_minimal_no_write_sampling_probe_artifacts(
        config,
        output_dir=tmp_path / "a1r_overlimit",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="42ba0ad29af16c48b7ff579042a05068f0734cb8",
        cli_allow=True,
        env={A1R_ENV_VAR: "1"},
        execute_sampling=True,
        pressure_gauge_factory=lambda _device: FakePressureGauge([1160.0]),
        pace_factory=lambda _device: FakePace(),
        relay_factory=lambda device: FakeRelay(str(device.get("device_name"))),
        thermometer_factory=lambda _device: FakeThermometer(),
        analyzer_serial_factory=lambda _device: FakeAnalyzerSerial(),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
    )

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["pressure_overlimit_seen"] is True
    assert summary["sample_count"] == 0
    assert summary["points_completed"] == 0
    assert summary["a2_allowed"] is False
    assert summary["pressure_setpoint_command_sent"] is False
    assert summary["mode_switch_command_sent"] is False
