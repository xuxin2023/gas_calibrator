from __future__ import annotations

from collections import deque
import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from gas_calibrator.v2.core.run001_query_only_real_com_probe import (
    QUERY_ONLY_EVIDENCE_MARKERS,
    QUERY_ONLY_REAL_COM_ENV_VAR,
    evaluate_query_only_real_com_gate,
    write_query_only_real_com_probe_artifacts,
)


def _base_config() -> dict:
    return {
        "scope": "query_only",
        "query_only": True,
        "no_write": True,
        "h2o_enabled": False,
        "full_group_enabled": False,
        "route_open_enabled": False,
        "sample_enabled": False,
        "relay_output_enabled": False,
        "valve_command_enabled": False,
        "pressure_setpoint_enabled": False,
        "vent_off_enabled": False,
        "seal_enabled": False,
        "high_pressure_enabled": False,
        "a1r_enabled": False,
        "a2_enabled": False,
        "a3_enabled": False,
        "analyzer_id_write_enabled": False,
        "mode_switch_enabled": False,
        "senco_write_enabled": False,
        "calibration_write_enabled": False,
        "real_primary_latest_refresh": False,
        "devices": {
            "pressure_controller": {"enabled": True, "port": "COM31", "baud": 9600},
            "pressure_gauge": {
                "enabled": True,
                "port": "COM30",
                "baud": 9600,
                "timeout": 0.01,
                "response_timeout_s": 0.02,
                "dest_id": "01",
            },
            "temperature_chamber": {
                "enabled": True,
                "port": "COM27",
                "baud": 9600,
                "parity": "N",
                "stopbits": 1,
                "bytesize": 8,
                "timeout": 1.0,
                "addr": 1,
            },
            "thermometer": {"enabled": True, "port": "COM26", "baud": 2400},
            "relay": {"enabled": True, "port": "COM28", "baud": 38400},
            "relay_8": {"enabled": True, "port": "COM29", "baud": 38400},
            "dewpoint_meter": {"enabled": False, "port": "COM25", "baud": 9600},
            "humidity_generator": {"enabled": False, "port": "COM24", "baud": 9600},
            "gas_analyzers": [
                {"name": "ga01", "enabled": True, "port": "COM35", "baud": 115200, "device_id": "001"},
                {"name": "ga02", "enabled": True, "port": "COM37", "baud": 115200, "device_id": "029"},
                {"name": "ga03", "enabled": True, "port": "COM41", "baud": 115200, "device_id": "003"},
                {"name": "ga04", "enabled": True, "port": "COM42", "baud": 115200, "device_id": "004"},
            ],
        },
    }


def _operator_confirmation(tmp_path: Path) -> Path:
    payload = {
        "operator_name": "test-operator",
        "timestamp": "2026-04-27T19:00:00+08:00",
        "branch": "codex/run001-a1-no-write-dry-run",
        "HEAD": "4c5facec951ce168bb4564f19361aa82644049a0",
        "config_path": str(tmp_path / "r0_config.json"),
        "port_manifest": {
            "pressure_controller": "COM31",
            "pressure_gauge": "COM30",
            "temperature_chamber": "COM27",
            "thermometer": "COM26",
            "relay": "COM28",
            "relay_8": "COM29",
            "gas_analyzers": ["COM35", "COM37", "COM41", "COM42"],
            "h2o_disabled": ["COM25", "COM24"],
        },
        "explicit_acknowledgement": {
            "query_only": True,
            "no_write": True,
            "no_route_open": True,
            "no_relay_output": True,
            "no_valve_command": True,
            "no_pressure_setpoint": True,
            "no_seal": True,
            "no_vent_off": True,
            "no_high_pressure": True,
            "no_sample": True,
            "no_mode_switch": True,
            "no_id_write": True,
            "no_senco_write": True,
            "no_calibration_write": True,
            "no_chamber_write_register": True,
            "no_chamber_set_temperature": True,
            "no_chamber_start": True,
            "no_chamber_stop": True,
            "not_real_acceptance": True,
            "engineering_probe_only": True,
            "real_primary_latest_refresh": False,
            "v1_fallback_required": True,
        },
    }
    path = tmp_path / "operator_confirmation.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _evaluate(config: dict, tmp_path: Path, *, cli: bool = True, env: bool = True):
    return evaluate_query_only_real_com_gate(
        config,
        cli_allow=cli,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"} if env else {},
        operator_confirmation_path=_operator_confirmation(tmp_path),
    )


class FakeModbusResponse:
    def __init__(self, registers: list[int] | None = None, error: str = "") -> None:
        self.registers = list(registers or [])
        self._error = error

    def isError(self) -> bool:
        return bool(self._error)

    def __str__(self) -> str:
        return self._error or "OK"


class ReadOnlyChamberClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[str] = []
        self.fail = fail

    def connect(self) -> bool:
        self.calls.append("connect")
        return True

    def close(self) -> None:
        self.calls.append("close")

    def read_input_registers(self, address: int, count: int = 1, **_kwargs):
        self.calls.append(f"read_input_registers:{address}:{count}")
        if self.fail:
            return FakeModbusResponse(error="NO_RESPONSE")
        if int(address) == 7991:
            return FakeModbusResponse([199])
        return FakeModbusResponse([0])

    def read_holding_registers(self, address: int, count: int = 1, **_kwargs):
        self.calls.append(f"read_holding_registers:{address}:{count}")
        if self.fail:
            return FakeModbusResponse(error="NO_RESPONSE")
        if int(address) == 8100:
            return FakeModbusResponse([200])
        return FakeModbusResponse([0])

    def write_register(self, *_args, **_kwargs):
        raise AssertionError("full R0 query-only must not write chamber registers")

    def write_coil(self, *_args, **_kwargs):
        raise AssertionError("full R0 query-only must not write chamber coils")


class FakePressureSerial:
    def __init__(self, *, generic_line: bytes = b"", p3_response: bytes = b"", read_frame: bytes = b"") -> None:
        self.timeout = 0.01
        self.is_open = True
        self.writes: list[bytes] = []
        self._generic_line = generic_line
        self._read_frame = read_frame
        self._line_queue: deque[bytes] = deque()
        self._p3_response = p3_response

    def write(self, payload: bytes) -> int:
        self.writes.append(payload)
        if b"P3" in payload and self._p3_response:
            self._line_queue.append(self._p3_response)
        return len(payload)

    def flush(self) -> None:
        return None

    def readline(self) -> bytes:
        if self._generic_line:
            line = self._generic_line
            self._generic_line = b""
            return line
        if self._line_queue:
            return self._line_queue.popleft()
        if self._read_frame:
            line = self._read_frame
            self._read_frame = b""
            return line
        return b""

    def read(self, _count: int) -> bytes:
        return b""

    @property
    def in_waiting(self) -> int:
        return 0

    def reset_input_buffer(self) -> None:
        self._line_queue.clear()

    def close(self) -> None:
        self.is_open = False


class PressureSerialFactory:
    def __init__(self, *handles: FakePressureSerial) -> None:
        self.handles = deque(handles)

    def __call__(self, _device: Mapping[str, Any]) -> FakePressureSerial:
        if self.handles:
            return self.handles.popleft()
        return FakePressureSerial()


def _config_only(*enabled_devices: str) -> dict:
    config = _base_config()
    enabled = set(enabled_devices)
    for name in ("pressure_controller", "pressure_gauge", "temperature_chamber", "thermometer", "relay", "relay_8"):
        config["devices"][name]["enabled"] = name in enabled
    if "gas_analyzers" not in enabled:
        config["devices"]["gas_analyzers"] = []
    return config


def test_query_only_gate_rejects_without_cli_flag(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path, cli=False, env=True)
    assert admission.approved is False
    assert "missing_cli_flag_allow_v2_query_only_real_com" in admission.reasons


def test_query_only_gate_rejects_cli_without_env(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path, cli=True, env=False)
    assert admission.approved is False
    assert "missing_env_gas_cal_v2_query_only_real_com" in admission.reasons


def test_query_only_gate_rejects_env_without_cli(tmp_path: Path) -> None:
    admission = _evaluate(_base_config(), tmp_path, cli=False, env=True)
    assert admission.approved is False
    assert "missing_cli_flag_allow_v2_query_only_real_com" in admission.reasons


def test_query_only_gate_rejects_missing_operator_confirmation_json() -> None:
    admission = evaluate_query_only_real_com_gate(
        _base_config(),
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=None,
    )
    assert admission.approved is False
    assert "missing_operator_confirmation_json" in admission.reasons


def test_query_only_gate_rejects_h2o_enabled(tmp_path: Path) -> None:
    config = _base_config()
    config["h2o_enabled"] = True
    admission = _evaluate(config, tmp_path)
    assert admission.approved is False
    assert "config_h2o_not_disabled" in admission.reasons


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("sample_enabled", "config_sample_enabled_not_disabled"),
        ("route_open_enabled", "config_route_open_enabled_not_disabled"),
        ("relay_output_enabled", "config_relay_output_enabled_not_disabled"),
        ("valve_command_enabled", "config_valve_command_enabled_not_disabled"),
        ("pressure_setpoint_enabled", "config_pressure_setpoint_enabled_not_disabled"),
    ],
)
def test_query_only_gate_rejects_control_capabilities(tmp_path: Path, field: str, reason: str) -> None:
    config = _base_config()
    config[field] = True
    admission = _evaluate(config, tmp_path)
    assert admission.approved is False
    assert reason in admission.reasons


def test_query_only_approved_dry_admission_does_not_open_com(tmp_path: Path) -> None:
    opened: list[str] = []

    def forbidden_open(device):
        opened.append(str(device.get("port")))
        raise AssertionError("dry admission must not open COM")

    summary = write_query_only_real_com_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        execute_query_only=False,
        serial_factory=forbidden_open,
    )

    assert opened == []
    assert summary["admission_approved"] is True
    assert summary["execute_query_only"] is False
    assert summary["real_com_opened"] is False
    assert summary["real_probe_executed"] is False
    assert summary["final_decision"] == "ADMISSION_APPROVED"


def test_query_only_evidence_markers_and_no_write_counts(tmp_path: Path) -> None:
    summary = write_query_only_real_com_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        execute_query_only=False,
    )

    assert summary["evidence_source"] == "real_probe_query_only"
    assert summary["not_real_acceptance_evidence"] is True
    assert summary["acceptance_level"] == "engineering_probe_only"
    assert summary["promotion_state"] == "blocked"
    assert summary["real_primary_latest_refresh"] is False
    for key, expected in QUERY_ONLY_EVIDENCE_MARKERS.items():
        assert summary[key] == expected
    assert summary["attempted_write_count"] == 0
    assert summary["identity_write_command_sent"] is False
    assert summary["calibration_write_command_sent"] is False
    assert summary["senco_write_command_sent"] is False
    assert summary["route_open_command_sent"] is False
    assert summary["relay_output_command_sent"] is False
    assert summary["valve_command_sent"] is False
    assert summary["pressure_setpoint_command_sent"] is False
    assert summary["vent_off_sent"] is False
    assert summary["seal_command_sent"] is False
    assert summary["sample_count"] == 0
    assert summary["points_completed"] == 0

    artifact_paths = summary["artifact_paths"]
    assert Path(artifact_paths["summary"]).exists()
    assert Path(artifact_paths["device_inventory"]).exists()
    assert Path(artifact_paths["query_results"]).exists()
    assert Path(artifact_paths["port_open_close_trace"]).exists()
    assert Path(artifact_paths["operator_confirmation_record"]).exists()
    assert Path(artifact_paths["safety_assertions"]).exists()


def test_query_only_execute_fails_closed_on_unavailable_query(tmp_path: Path) -> None:
    class SilentSerial:
        timeout = 0.01
        is_open = True

        def write(self, _payload: bytes) -> None:
            return None

        def flush(self) -> None:
            return None

        def readline(self) -> bytes:
            return b""

        def read(self, _count: int) -> bytes:
            return b""

        @property
        def in_waiting(self) -> int:
            return 0

        def reset_input_buffer(self) -> None:
            return None

        def close(self) -> None:
            return None

    summary = write_query_only_real_com_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        execute_query_only=True,
        serial_factory=lambda _device: SilentSerial(),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(fail=True),
    )

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["query_failure_seen"] is True
    assert summary["attempted_write_count"] == 0
    assert summary["route_open_command_sent"] is False
    assert summary["relay_output_command_sent"] is False
    assert summary["valve_command_sent"] is False
    assert summary["pressure_setpoint_command_sent"] is False
    assert summary["sample_count"] == 0
    assert summary["command_profile_mismatch"] is True
    assert summary["command_profile_mismatch_reason"] != "no_response"
    assert "pressure_controller.identity_query.command=*IDN?" in summary["command_profile_mismatch_reason"]
    assert "pressure_controller.v1_aligned_readonly_ping.command=:OUTP:STAT?" in summary["command_profile_mismatch_reason"]
    assert "pressure_meter.p3.command=" in summary["command_profile_mismatch_reason"]
    assert ".dest_id=01" in summary["command_profile_mismatch_reason"]
    assert ".raw_response_empty" in summary["command_profile_mismatch_reason"]


def test_full_r0_pressure_gauge_uses_paroscientific_p3_before_unavailable(tmp_path: Path) -> None:
    summary = write_query_only_real_com_probe_artifacts(
        _config_only("pressure_gauge"),
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        execute_query_only=True,
        serial_factory=PressureSerialFactory(
            FakePressureSerial(),
            FakePressureSerial(p3_response=b"*01001014.555\r\n"),
        ),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
    )

    assert summary["final_decision"] == "PASS"
    assert summary["pressure_gauge_protocol_profile"] == "paroscientific_p3_readonly"
    assert summary["parsed_pressure_hpa"] == 1014.555
    assert summary["pressure_gauge_blocks_r1"] is False

    results = json.loads(Path(summary["artifact_paths"]["query_results"]).read_text(encoding="utf-8"))
    pressure = next(item for item in results if item["device_type"] == "pressure_gauge")
    assert pressure["generic_read_frame_failed"] is True
    assert pressure["generic_frame_mode_unsupported_or_not_continuous"] is True
    assert pressure["known_v1_driver_readonly_attempted"] is True
    assert pressure["paroscientific_p3_read_attempted"] is True
    assert pressure["paroscientific_p3_read_succeeded"] is True
    assert pressure["paroscientific_fast_read_attempted"] is False
    assert pressure["pressure_gauge_unavailable"] is False


def test_full_r0_pressure_controller_idn_no_response_uses_v1_aligned_ping(tmp_path: Path) -> None:
    class PressureControllerStatusSerial(FakePressureSerial):
        def __init__(self) -> None:
            super().__init__()
            self._last_write = b""

        def write(self, payload: bytes) -> int:
            self.writes.append(payload)
            self._last_write = payload
            if b":OUTP:STAT?" in payload:
                self._line_queue.append(b"0\n")
            elif b":SENS:PRES?" in payload:
                self._line_queue.append(b":SENS:PRES 1014.25\n")
            elif b":SOUR:PRES:LEV:IMM:AMPL:VENT?" in payload:
                self._line_queue.append(b"0\n")
            elif b":SYST:ERR?" in payload:
                self._line_queue.append(b'0,"No error"\n')
            return len(payload)

    handle = PressureControllerStatusSerial()
    summary = write_query_only_real_com_probe_artifacts(
        _config_only("pressure_controller"),
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        execute_query_only=True,
        serial_factory=lambda _device: handle,
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
    )

    assert summary["final_decision"] == "PASS"
    assert summary["query_failure_seen"] is False
    assert summary["pressure_controller_identity_query_command"] == "*IDN?"
    assert summary["pressure_controller_identity_query_result"] == "unsupported_identity_query"
    assert summary["pressure_controller_identity_query_error"] == "unsupported_identity_query"
    assert summary["pressure_controller_v1_aligned_ping_command"] == ":OUTP:STAT?"
    assert summary["pressure_controller_v1_aligned_ping_result"] == "available"
    assert summary["pressure_controller_offline_decision_source"] == "v1_aligned_readonly_ping"

    results = json.loads(Path(summary["artifact_paths"]["query_results"]).read_text(encoding="utf-8"))
    identity = next(item for item in results if item.get("command") == "*IDN?")
    status = next(item for item in results if item.get("command") == ":OUTP:STAT?")
    pressure = next(item for item in results if item.get("command") == ":SENS:PRES?")
    assert identity["result"] == "unsupported_identity_query"
    assert identity["offline_decision_blocked_by_identity_query_only"] is True
    assert status["result"] == "available"
    assert status["pressure_controller_query_role"] == "v1_aligned_readonly_ping"
    assert pressure["result"] == "available"
    assert pressure["pressure_controller_query_role"] == "v1_aligned_readonly_ping"
    assert all(b"OUTP:STAT 0" not in payload for payload in handle.writes)


def test_full_r0_temperature_chamber_uses_modbus_readonly_driver(tmp_path: Path) -> None:
    chamber_client = ReadOnlyChamberClient()
    summary = write_query_only_real_com_probe_artifacts(
        _config_only("temperature_chamber"),
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        execute_query_only=True,
        serial_factory=lambda _device: FakePressureSerial(),
        chamber_client_factory=lambda _device: chamber_client,
    )

    assert summary["final_decision"] == "PASS"
    assert summary["temperature_chamber_unavailable"] is False
    assert summary["pv_temperature_c"] == 19.9
    assert summary["sv_temperature_c"] == 20.0
    assert summary["status_value"] == 0

    results = json.loads(Path(summary["artifact_paths"]["query_results"]).read_text(encoding="utf-8"))
    chamber = next(item for item in results if item["device_type"] == "temperature_chamber")
    assert chamber["protocol_candidate"] == "modbus_rtu"
    assert chamber["generic_ascii_query_failed"] is True
    assert chamber["ascii_query_unsupported"] is True
    assert chamber["known_driver_readonly_attempted"] is True
    assert chamber["known_driver_readonly_succeeded"] is True
    assert chamber["temperature_chamber_unavailable"] is False
    assert chamber["chamber_write_register_command_sent"] is False
    assert chamber["chamber_set_temperature_command_sent"] is False
    assert chamber["chamber_start_command_sent"] is False
    assert chamber["chamber_stop_command_sent"] is False
    assert not any(call.startswith("write_") for call in chamber_client.calls)


def test_relay_open_close_only_is_actuator_not_query_failure(tmp_path: Path) -> None:
    class SilentSerial:
        def write(self, _payload: bytes) -> None:
            raise AssertionError("relay R0 must not send output/control bytes")

        def readline(self) -> bytes:
            return b""

        def close(self) -> None:
            return None

    config = _base_config()
    config["devices"]["pressure_gauge"]["enabled"] = False
    config["devices"]["temperature_chamber"]["enabled"] = False
    config["devices"]["thermometer"]["enabled"] = False
    config["devices"]["pressure_controller"]["enabled"] = False
    config["devices"]["gas_analyzers"] = []

    summary = write_query_only_real_com_probe_artifacts(
        config,
        output_dir=tmp_path / "r0",
        config_path=tmp_path / "r0_config.json",
        cli_allow=True,
        env={QUERY_ONLY_REAL_COM_ENV_VAR: "1"},
        operator_confirmation_path=_operator_confirmation(tmp_path),
        execute_query_only=True,
        serial_factory=lambda _device: SilentSerial(),
    )

    assert summary["final_decision"] == "PASS"
    assert summary["query_failure_seen"] is False
    assert summary["attempted_write_count"] == 0
    assert summary["relay_output_command_sent"] is False

    inventory = json.loads(Path(summary["artifact_paths"]["device_inventory"]).read_text(encoding="utf-8"))
    relay_entries = [item for item in inventory if item["device_name"] in {"relay", "relay_8"}]
    assert {item["device_type"] for item in relay_entries} == {"actuator_only"}
    assert {item["query_capability"] for item in relay_entries} == {"not_applicable"}

    results = json.loads(Path(summary["artifact_paths"]["query_results"]).read_text(encoding="utf-8"))
    relay_results = [item for item in results if item["device_name"] in {"relay", "relay_8"}]
    assert {item["result"] for item in relay_results} == {"not_applicable"}
    assert all(item["port_open_close_ok"] is True for item in relay_results)
    assert all(item["control_command_sent"] is False for item in relay_results)
