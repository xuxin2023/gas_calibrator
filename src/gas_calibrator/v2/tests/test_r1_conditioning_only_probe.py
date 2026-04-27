from __future__ import annotations

from collections import deque
import json
from pathlib import Path
import time
from typing import Any, Mapping

from gas_calibrator.v2.core.run001_r1_conditioning_only_probe import (
    R1_ENV_VAR,
    _build_timing_breakdown,
    evaluate_r1_conditioning_only_gate,
    write_r1_conditioning_only_probe_artifacts,
)


def _write_r0_pass(tmp_path: Path) -> Path:
    run_dir = tmp_path / "r0_full"
    run_dir.mkdir()
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "final_decision": "PASS",
                "not_real_acceptance_evidence": True,
                "attempted_write_count": 0,
                "any_write_command_sent": False,
            }
        ),
        encoding="utf-8",
    )
    return run_dir


def _base_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "scope": "r1_conditioning_only",
        "r0_full_query_only_output_dir": str(_write_r0_pass(tmp_path)),
        "co2_only": True,
        "skip0": True,
        "single_route": True,
        "single_temperature": True,
        "no_write": True,
        "h2o_enabled": False,
        "full_group_enabled": False,
        "a1r_enabled": False,
        "a2_enabled": False,
        "a3_enabled": False,
        "pressure_setpoint_enabled": False,
        "vent_off_enabled": False,
        "seal_enabled": False,
        "high_pressure_enabled": False,
        "sample_enabled": False,
        "mode_switch_enabled": False,
        "analyzer_id_write_enabled": False,
        "senco_write_enabled": False,
        "calibration_write_enabled": False,
        "chamber_set_temperature_enabled": False,
        "chamber_start_enabled": False,
        "chamber_stop_enabled": False,
        "real_primary_latest_refresh": False,
        "r1_conditioning_only": {
            "scope": "r1_conditioning_only",
            "co2_only": True,
            "skip0": True,
            "single_route": True,
            "single_temperature": True,
            "no_write": True,
            "full_group_enabled": False,
            "a1r_enabled": False,
            "a2_enabled": False,
            "a3_enabled": False,
            "pressure_setpoint_enabled": False,
            "vent_off_enabled": False,
            "seal_enabled": False,
            "high_pressure_enabled": False,
            "sample_enabled": False,
            "mode_switch_enabled": False,
            "analyzer_id_write_enabled": False,
            "senco_write_enabled": False,
            "calibration_write_enabled": False,
            "chamber_set_temperature_enabled": False,
            "chamber_start_enabled": False,
            "chamber_stop_enabled": False,
            "real_primary_latest_refresh": False,
            "r0_full_query_only_output_dir": str(tmp_path / "r0_full"),
            "conditioning": {
                "target_co2_ppm": 1000,
                "route_open_valves": [11, 7, 6],
                "conditioning_duration_s": 0.0,
                "vent_heartbeat_interval_s": 0.01,
                "max_vent_heartbeat_gap_ms": 3000,
                "route_open_to_first_vent_max_ms": 1000,
                "pressure_freshness_max_age_ms": 1000,
                "pressure_overlimit_hpa": 1150,
            },
        },
        "devices": {
            "pressure_controller": {"enabled": True, "port": "COM31", "baud": 9600},
            "pressure_gauge": {"enabled": True, "port": "COM30", "baud": 9600, "response_timeout_s": 0.01},
            "temperature_chamber": {"enabled": True, "port": "COM27", "baud": 9600, "addr": 1},
            "thermometer": {"enabled": True, "port": "COM26", "baud": 2400},
            "relay": {"enabled": True, "port": "COM28", "baud": 38400, "addr": 1},
            "relay_8": {"enabled": True, "port": "COM29", "baud": 38400, "addr": 1},
            "dewpoint_meter": {"enabled": False, "port": "COM25"},
            "humidity_generator": {"enabled": False, "port": "COM24"},
            "gas_analyzers": [{"name": "ga01", "enabled": True, "port": "COM35", "baud": 115200}],
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
            "pressure": {"vent_hold_interval_s": 0.01},
        },
    }


def _operator_confirmation(tmp_path: Path, config_path: Path, r0_dir: Path) -> Path:
    payload = {
        "operator_name": "pytest",
        "timestamp": "2026-04-27T00:00:00+08:00",
        "branch": "codex/run001-a1-no-write-dry-run",
        "HEAD": "9ff62135cb0fd7f3280218cd9ce1b45eb70c31c5",
        "config_path": str(config_path),
        "r0_full_query_only_output_dir": str(r0_dir),
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
            "only_r1_conditioning_only": True,
            "co2_only": True,
            "skip0": True,
            "single_route": True,
            "single_temperature": True,
            "no_write": True,
            "no_pressure_setpoint": True,
            "no_vent_off": True,
            "no_seal": True,
            "no_high_pressure": True,
            "no_sample": True,
            "no_id_write": True,
            "no_senco_write": True,
            "no_calibration_write": True,
            "no_chamber_sv_write": True,
            "no_chamber_set_temperature": True,
            "no_chamber_start": True,
            "no_chamber_stop": True,
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
    op_path = _operator_confirmation(tmp_path, config_path, tmp_path / "r0_full")
    return config, config_path, op_path


class FakeModbusResponse:
    def __init__(self, registers: list[int] | None = None, error: str = "") -> None:
        self.registers = list(registers or [])
        self._error = error

    def isError(self) -> bool:
        return bool(self._error)

    def __str__(self) -> str:
        return self._error or "OK"


class ReadOnlyChamberClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def connect(self) -> bool:
        self.calls.append("connect")
        return True

    def close(self) -> None:
        self.calls.append("close")

    def read_input_registers(self, address: int, count: int = 1, **_kwargs):
        self.calls.append(f"read_input_registers:{address}:{count}")
        if int(address) == 7991:
            return FakeModbusResponse([201])
        return FakeModbusResponse([0])

    def read_holding_registers(self, address: int, count: int = 1, **_kwargs):
        self.calls.append(f"read_holding_registers:{address}:{count}")
        if int(address) == 8100:
            return FakeModbusResponse([200])
        return FakeModbusResponse([0])

    def write_register(self, *_args, **_kwargs):
        raise AssertionError("R1 must not write chamber registers")

    def write_coil(self, *_args, **_kwargs):
        raise AssertionError("R1 must not write chamber coils")


class FakePressureGauge:
    def __init__(self, values: list[float]) -> None:
        self.values = deque(values)
        self.opened = False

    def open(self) -> None:
        self.opened = True

    def close(self) -> None:
        self.opened = False

    def read_pressure_fast(self, **_kwargs) -> float:
        if self.values:
            return float(self.values.popleft())
        return 1013.25


class FakePace:
    def __init__(self) -> None:
        self.vent_calls: list[bool] = []
        self.opened = False

    def open(self) -> None:
        self.opened = True

    def close(self) -> None:
        self.opened = False

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
            raise AssertionError("R1 must not send vent-off")
        self.vent_calls.append(bool(on))


class SlowVentPace(FakePace):
    def vent(self, on: bool = True) -> None:
        time.sleep(1.05)
        super().vent(on)


class FakeRelay:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[tuple[int, bool]] = []
        self.opened = False

    def open(self) -> None:
        self.opened = True

    def close(self) -> None:
        self.opened = False

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
    def readline(self) -> bytes:
        return b""

    def close(self) -> None:
        return None


def test_r1_gate_requires_triple_unlock(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    missing_cli = evaluate_r1_conditioning_only_gate(
        config,
        cli_allow=False,
        env={R1_ENV_VAR: "1"},
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="9ff62135cb0fd7f3280218cd9ce1b45eb70c31c5",
        config_path=str(config_path),
    )
    assert missing_cli.approved is False
    assert "missing_cli_flag_allow_v2_r1_conditioning_only_real_com" in missing_cli.reasons

    missing_env = evaluate_r1_conditioning_only_gate(
        config,
        cli_allow=True,
        env={},
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="9ff62135cb0fd7f3280218cd9ce1b45eb70c31c5",
        config_path=str(config_path),
    )
    assert missing_env.approved is False
    assert "missing_env_gas_cal_v2_r1_conditioning_only_real_com" in missing_env.reasons


def test_r1_gate_approves_only_after_r0_pass_and_operator_confirmation(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    admission = evaluate_r1_conditioning_only_gate(
        config,
        cli_allow=True,
        env={R1_ENV_VAR: "1"},
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="9ff62135cb0fd7f3280218cd9ce1b45eb70c31c5",
        config_path=str(config_path),
    )

    assert admission.approved is True
    assert admission.evidence["r0_full_query_only_prereq_pass"] is True
    assert admission.evidence["a1r_allowed"] is False
    assert admission.evidence["a2_allowed"] is False
    assert admission.evidence["a3_allowed"] is False


def test_r1_conditioning_writes_required_artifacts_and_no_forbidden_writes(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    relay_handles: dict[str, FakeRelay] = {}

    def relay_factory(device: Mapping[str, Any]) -> FakeRelay:
        name = str(device.get("device_name"))
        relay = FakeRelay(name)
        relay_handles[name] = relay
        return relay

    summary = write_r1_conditioning_only_probe_artifacts(
        config,
        output_dir=tmp_path / "r1",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="9ff62135cb0fd7f3280218cd9ce1b45eb70c31c5",
        cli_allow=True,
        env={R1_ENV_VAR: "1"},
        execute_conditioning_only=True,
        pressure_gauge_factory=lambda _device: FakePressureGauge([1013.2, 1013.3]),
        pace_factory=lambda _device: FakePace(),
        relay_factory=relay_factory,
        thermometer_factory=lambda _device: FakeThermometer(),
        analyzer_serial_factory=lambda _device: FakeAnalyzerSerial(),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
        sleep_fn=lambda _seconds: None,
    )

    assert summary["final_decision"] == "PASS"
    assert summary["r1_conditioning_only_executed"] is True
    assert summary["evidence_source"] == "real_probe_r1_conditioning_only"
    assert summary["acceptance_level"] == "engineering_probe_only"
    assert summary["not_real_acceptance_evidence"] is True
    assert summary["promotion_state"] == "blocked"
    assert summary["real_primary_latest_refresh"] is False
    assert summary["a1r_allowed"] is False
    assert summary["a2_allowed"] is False
    assert summary["a3_allowed"] is False
    assert summary["co2_only"] is True
    assert summary["skip0"] is True
    assert summary["single_route"] is True
    assert summary["single_temperature"] is True

    assert summary["pressure_gauge_freshness_ok"] is True
    assert summary["vent_heartbeat_count"] >= 2
    assert summary["max_vent_heartbeat_gap_ms"] <= 3000
    assert summary["route_open_to_first_vent_ms"] <= 1000
    assert summary["legacy_route_open_to_first_vent_ms"] == summary["route_open_to_first_vent_ms"]
    assert summary["legacy_route_open_to_first_vent_exceeded"] is False
    assert summary["legacy_metric_superseded_by_emit_start_gate"] is False
    assert summary["route_open_completed_to_first_vent_ms"] is not None
    assert summary["route_open_completed_to_first_vent_emit_start_ms"] == summary["route_open_completed_to_first_vent_ms"]
    assert summary["last_route_action_end_to_first_vent_ms"] is not None
    assert summary["last_route_action_end_to_first_vent_emit_start_ms"] == summary["last_route_action_end_to_first_vent_ms"]
    assert summary["route_action_sequence_duration_ms"] is not None
    assert summary["max_relay_action_duration_ms"] is not None
    assert summary["vent_command_roundtrip_ms"] == summary["first_vent_emit_duration_ms"]
    assert summary["max_vent_heartbeat_emit_start_gap_ms"] <= 3000
    assert summary["pressure_read_latency_ms"] is not None
    assert summary["max_pressure_read_latency_ms"] is not None
    assert summary["pressure_read_deferred_for_heartbeat_count"] >= 0
    assert summary["heartbeat_due_before_pressure_read_count"] >= 0
    assert summary["heartbeat_sent_before_pressure_read_count"] >= 0
    assert summary["diagnostic_decision"] == "NOT_APPLICABLE"
    assert summary["critical_path_suspect"] == "unknown"
    assert summary["pressure_overlimit_seen"] is False
    assert summary["pressure_overlimit_fail_closed"] is False

    assert summary["attempted_write_count"] == 0
    assert summary["any_write_command_sent"] is False
    assert summary["pressure_setpoint_command_sent"] is False
    assert summary["vent_off_command_sent"] is False
    assert summary["seal_command_sent"] is False
    assert summary["high_pressure_command_sent"] is False
    assert summary["sample_started"] is False
    assert summary["sample_count"] == 0
    assert summary["points_completed"] == 0
    assert summary["identity_write_command_sent"] is False
    assert summary["senco_write_command_sent"] is False
    assert summary["calibration_write_command_sent"] is False
    assert summary["chamber_set_temperature_command_sent"] is False
    assert summary["chamber_start_command_sent"] is False
    assert summary["chamber_stop_command_sent"] is False

    assert summary["relay_output_command_sent"] is True
    assert summary["relay_output_command_scope"] == "authorized_r1_route_conditioning_only"
    assert summary["relay_route_action_count"] == 6
    assert relay_handles["relay_a"].calls == [(12, True), (15, True), (12, False), (15, False)]
    assert relay_handles["relay_b"].calls == [(3, True), (3, False)]

    for path in summary["artifact_paths"].values():
        assert Path(path).exists()
    safety = json.loads(Path(summary["artifact_paths"]["safety_assertions"]).read_text(encoding="utf-8"))
    assert safety["attempted_write_count"] == 0
    assert safety["pressure_setpoint_command_sent"] is False
    assert safety["non_authorized_relay_output_command_sent"] is False
    timing = json.loads(Path(summary["artifact_paths"]["r1_timing_breakdown"]).read_text(encoding="utf-8"))
    assert timing["route_open_completed_to_first_vent_emit_start_ms"] == summary["route_open_completed_to_first_vent_emit_start_ms"]
    events_text = Path(summary["artifact_paths"]["r1_timing_events"]).read_text(encoding="utf-8")
    assert "vent_heartbeat_scheduler_started" in events_text
    assert "each_relay_action_start" in events_text
    assert "pressure_gauge_read_start" in events_text
    latency_csv = Path(summary["artifact_paths"]["r1_latency_breakdown"]).read_text(encoding="utf-8")
    assert "relay_action" in latency_csv
    assert "vent_heartbeat_emit" in latency_csv
    assert "pressure_gauge_read" in latency_csv


def test_r1_legacy_route_to_vent_roundtrip_is_diagnostic_only(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    summary = write_r1_conditioning_only_probe_artifacts(
        config,
        output_dir=tmp_path / "r1_slow_vent",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="9ff62135cb0fd7f3280218cd9ce1b45eb70c31c5",
        cli_allow=True,
        env={R1_ENV_VAR: "1"},
        execute_conditioning_only=True,
        pressure_gauge_factory=lambda _device: FakePressureGauge([1013.2]),
        pace_factory=lambda _device: SlowVentPace(),
        relay_factory=lambda device: FakeRelay(str(device.get("device_name"))),
        thermometer_factory=lambda _device: FakeThermometer(),
        analyzer_serial_factory=lambda _device: FakeAnalyzerSerial(),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
        sleep_fn=lambda _seconds: None,
    )

    assert summary["final_decision"] == "PASS"
    assert summary["legacy_route_open_to_first_vent_exceeded"] is True
    assert summary["legacy_metric_superseded_by_emit_start_gate"] is True
    assert summary["diagnostic_decision"] == "LEGACY_ANCHOR_SUPERSEDED"
    assert "VENT_COMMAND_ROUNDTRIP_SLOW" in summary["secondary_diagnostic_decisions"]
    assert summary["route_open_completed_to_first_vent_emit_start_ms"] <= 300
    assert summary["last_route_action_end_to_first_vent_emit_start_ms"] <= 300
    assert summary["vent_command_roundtrip_slow"] is True
    assert "route_open_to_first_vent_exceeded" not in summary["rejection_reasons"]


def test_r1_pressure_overlimit_fails_closed_without_downstream_actions(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    relay_calls: list[tuple[int, bool]] = []

    class RecordingRelay(FakeRelay):
        def set_valve(self, channel: int, open_: bool) -> None:
            super().set_valve(channel, open_)
            relay_calls.append((int(channel), bool(open_)))

    summary = write_r1_conditioning_only_probe_artifacts(
        config,
        output_dir=tmp_path / "r1_overlimit",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch="codex/run001-a1-no-write-dry-run",
        head="9ff62135cb0fd7f3280218cd9ce1b45eb70c31c5",
        cli_allow=True,
        env={R1_ENV_VAR: "1"},
        execute_conditioning_only=True,
        pressure_gauge_factory=lambda _device: FakePressureGauge([1160.0]),
        pace_factory=lambda _device: FakePace(),
        relay_factory=lambda device: RecordingRelay(str(device.get("device_name"))),
        thermometer_factory=lambda _device: FakeThermometer(),
        analyzer_serial_factory=lambda _device: FakeAnalyzerSerial(),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
        sleep_fn=lambda _seconds: None,
    )

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["pressure_overlimit_seen"] is True
    assert summary["relay_route_action_count"] == 0
    assert relay_calls == []
    assert summary["pressure_setpoint_command_sent"] is False
    assert summary["vent_off_command_sent"] is False
    assert summary["seal_command_sent"] is False
    assert summary["high_pressure_command_sent"] is False
    assert summary["sample_started"] is False
    assert summary["sample_count"] == 0
    assert summary["a1r_allowed"] is False
    assert summary["a2_allowed"] is False
    assert summary["a3_allowed"] is False


def test_r1_timing_breakdown_supersedes_legacy_anchor_without_promoting() -> None:
    ns = 1_000_000
    rows = [
        {"event_name": "route_conditioning_start", "perf_counter_ns": 0},
        {"event_name": "first_route_action_start", "perf_counter_ns": 10 * ns},
        {"event_name": "each_relay_action_start", "perf_counter_ns": 10 * ns, "sequence_id": 1},
        {"event_name": "each_relay_action_end", "perf_counter_ns": 30 * ns, "sequence_id": 1},
        {"event_name": "first_route_action_end", "perf_counter_ns": 30 * ns},
        {"event_name": "last_route_action_start", "perf_counter_ns": 700 * ns},
        {"event_name": "each_relay_action_start", "perf_counter_ns": 700 * ns, "sequence_id": 2},
        {"event_name": "each_relay_action_end", "perf_counter_ns": 820 * ns, "sequence_id": 2},
        {"event_name": "last_route_action_end", "perf_counter_ns": 820 * ns},
        {"event_name": "route_open_completed", "perf_counter_ns": 850 * ns},
        {"event_name": "vent_heartbeat_scheduler_started", "perf_counter_ns": 851 * ns},
        {"event_name": "first_vent_heartbeat_emit_start", "perf_counter_ns": 910 * ns},
        {"event_name": "each_vent_heartbeat_emit_start", "perf_counter_ns": 910 * ns, "sequence_id": 2},
        {"event_name": "each_vent_heartbeat_emit_end", "perf_counter_ns": 1_020 * ns, "sequence_id": 2},
        {"event_name": "first_vent_heartbeat_emit_end", "perf_counter_ns": 1_020 * ns},
        {"event_name": "pressure_gauge_read_start", "perf_counter_ns": 1_100 * ns, "sequence_id": 1},
        {"event_name": "pressure_gauge_read_end", "perf_counter_ns": 1_150 * ns, "sequence_id": 1},
    ]

    breakdown = _build_timing_breakdown(
        rows,
        route_open_to_first_vent_ms=1089.685,
        max_vent_heartbeat_gap_ms=1202.694,
        max_vent_heartbeat_emit_start_gap_ms=1202.694,
        vent_heartbeat_count=2,
        relay_route_action_count=2,
        route_open_to_first_vent_threshold_ms=1000.0,
        route_to_first_vent_emit_start_threshold_ms=300.0,
    )

    assert breakdown["diagnostic_decision"] == "LEGACY_ANCHOR_SUPERSEDED"
    assert breakdown["suspected_root_cause"] == "threshold_anchor_too_early"
    assert breakdown["critical_path_suspect"] == "threshold_anchor_too_early"
    assert breakdown["legacy_route_open_to_first_vent_exceeded"] is True
    assert breakdown["legacy_metric_superseded_by_emit_start_gate"] is True
    assert breakdown["route_open_completed_to_first_vent_emit_start_ms"] == 60.0
    assert breakdown["last_route_action_end_to_first_vent_emit_start_ms"] == 90.0
