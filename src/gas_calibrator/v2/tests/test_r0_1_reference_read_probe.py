from __future__ import annotations

from collections import deque
import json
from pathlib import Path
from typing import Any

from gas_calibrator.v2.core.run001_r0_1_reference_read_probe import (
    R0_1_ENV_VAR,
    R0_1_EVIDENCE_MARKERS,
    write_r0_1_reference_read_probe_artifacts,
)


def _base_config() -> dict:
    return {
        "devices": {
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
        },
        "r0_1": {
            "pressure_gauge": {
                "read_window_s": 0.0,
                "poll_interval_s": 0.0,
                "max_reads": 1,
            }
        },
    }


def _operator_confirmation(tmp_path: Path, config_path: Path, *, branch: str = "", head: str = "") -> Path:
    payload = {
        "operator_name": "pytest",
        "timestamp": "2026-04-27T00:00:00+08:00",
        "branch": branch,
        "HEAD": head,
        "config_path": str(config_path),
        "port_manifest": {
            "allowed_ports": ["COM30", "COM27"],
            "pressure_gauge": "COM30",
            "temperature_chamber": "COM27",
            "h2o_devices": "disabled",
        },
        "explicit_acknowledgement": {
            "query_only": True,
            "read_only": True,
            "no_write": True,
            "no_route_open": True,
            "no_relay_output": True,
            "no_valve_command": True,
            "no_pressure_setpoint": True,
            "no_vent_off": True,
            "no_seal": True,
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
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


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
        self.closed = False

    def connect(self) -> bool:
        self.calls.append("connect")
        return True

    def close(self) -> None:
        self.calls.append("close")
        self.closed = True

    def read_input_registers(self, address: int, count: int = 1, **_kwargs):
        self.calls.append(f"read_input_registers:{address}:{count}")
        if int(address) == 7991:
            return FakeModbusResponse([235])
        return FakeModbusResponse([0])

    def read_holding_registers(self, address: int, count: int = 1, **_kwargs):
        self.calls.append(f"read_holding_registers:{address}:{count}")
        if int(address) == 8100:
            return FakeModbusResponse([250])
        return FakeModbusResponse([0])

    def write_register(self, *_args, **_kwargs):
        raise AssertionError("R0.1 must not write chamber registers")

    def write_coil(self, *_args, **_kwargs):
        raise AssertionError("R0.1 must not write chamber coils")


class FakePressureSerial:
    def __init__(
        self,
        *,
        generic_line: bytes = b"",
        p3_response: bytes = b"",
        drain_line: bytes = b"",
    ) -> None:
        self.writes: list[bytes] = []
        self.closed = False
        self.is_open = True
        self.timeout = 0.01
        self._generic_line = generic_line
        self._line_queue: deque[bytes] = deque()
        self._p3_response = p3_response
        if drain_line:
            self._line_queue.append(drain_line)

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
        return b""

    def read(self, count: int) -> bytes:
        return b""

    @property
    def in_waiting(self) -> int:
        return 0

    def reset_input_buffer(self) -> None:
        self._line_queue.clear()

    def close(self) -> None:
        self.closed = True


class PressureSerialFactory:
    def __init__(self, *handles: FakePressureSerial) -> None:
        self.handles = deque(handles)

    def __call__(self, _device: dict[str, Any]) -> FakePressureSerial:
        if self.handles:
            return self.handles.popleft()
        return FakePressureSerial()


def _successful_pressure_factory() -> PressureSerialFactory:
    return PressureSerialFactory(
        FakePressureSerial(),
        FakePressureSerial(p3_response=b"*01001012.345\r\n"),
    )


def test_com30_unavailable_blocks_r1_and_preserves_no_write(tmp_path: Path) -> None:
    pressure_factory = PressureSerialFactory(FakePressureSerial(), FakePressureSerial(), FakePressureSerial())
    chamber_client = ReadOnlyChamberClient()

    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        operator_confirmation_path=_operator_confirmation(tmp_path, tmp_path / "r0_1_config.json"),
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=pressure_factory,
        chamber_client_factory=lambda _device: chamber_client,
    )

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["r1_conditioning_allowed"] is False
    assert summary["r1_blocked"] is True
    assert "pressure_gauge_reference_unavailable" in summary["r1_block_reasons"]
    for key, expected in R0_1_EVIDENCE_MARKERS.items():
        assert summary[key] == expected


def test_com30_raw_capture_records_bytes_without_write_or_control(tmp_path: Path) -> None:
    pressure_factory = PressureSerialFactory(
        FakePressureSerial(generic_line=b"*01001012.345\r\n"),
        FakePressureSerial(p3_response=b"*01001012.500\r\n"),
    )
    chamber_client = ReadOnlyChamberClient()

    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        operator_confirmation_path=_operator_confirmation(tmp_path, tmp_path / "r0_1_config.json"),
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=pressure_factory,
        chamber_client_factory=lambda _device: chamber_client,
    )

    raw_path = Path(summary["artifact_paths"]["raw_capture_COM30_pressure_gauge"])
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    assert raw["raw_bytes_len"] > 0
    assert raw["parser_status"] == "parse_ok"
    assert raw["stream_mode_assessment"] == "continuous_output"
    assert raw["known_v1_driver_readonly_attempted"] is True
    assert raw["paroscientific_p3_read_attempted"] is True
    assert raw["paroscientific_p3_read_succeeded"] is True
    assert raw["pressure_gauge_probe_status"] == "readonly_available"
    assert raw["pressure_gauge_unavailable"] is False
    assert raw["write_command_sent"] is False
    assert raw["any_write_command_sent"] is False
    assert raw["persistent_config_write_sent"] is False
    assert raw["pressure_gauge_setting_write_sent"] is False
    assert raw["control_command_sent"] is False


def test_com30_generic_frame_failure_uses_v1_p3_driver_before_unavailable(tmp_path: Path) -> None:
    pressure_factory = PressureSerialFactory(
        FakePressureSerial(),
        FakePressureSerial(p3_response=b"*01001013.250\r\n"),
    )

    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        operator_confirmation_path=_operator_confirmation(tmp_path, tmp_path / "r0_1_config.json"),
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=pressure_factory,
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
    )

    raw = json.loads(Path(summary["artifact_paths"]["raw_capture_COM30_pressure_gauge"]).read_text(encoding="utf-8"))
    assert raw["generic_read_frame_failed"] is True
    assert raw["generic_frame_mode_unsupported_or_not_continuous"] is True
    assert raw["known_v1_driver_readonly_attempted"] is True
    assert raw["paroscientific_command_profile"] == "P3_single_read_query"
    assert raw["p3_command_preview"] == "*0100P3\r\n"
    assert raw["paroscientific_p3_read_succeeded"] is True
    assert raw["parsed_pressure_hpa"] == 1013.25
    assert raw["pressure_gauge_probe_status"] == "readonly_available"
    assert raw["pressure_gauge_unavailable"] is False
    assert raw["pressure_gauge_blocks_r1"] is False
    assert summary["pressure_gauge_blocks_r1"] is False


def test_com30_fast_read_fallback_can_make_reference_available(tmp_path: Path) -> None:
    pressure_factory = PressureSerialFactory(
        FakePressureSerial(),
        FakePressureSerial(),
        FakePressureSerial(drain_line=b"*01001014.750\r\n"),
    )

    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        operator_confirmation_path=_operator_confirmation(tmp_path, tmp_path / "r0_1_config.json"),
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=pressure_factory,
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
    )

    raw = json.loads(Path(summary["artifact_paths"]["raw_capture_COM30_pressure_gauge"]).read_text(encoding="utf-8"))
    assert raw["paroscientific_p3_read_attempted"] is True
    assert raw["paroscientific_p3_read_failed"] is True
    assert raw["paroscientific_fast_read_attempted"] is True
    assert raw["paroscientific_fast_read_succeeded"] is True
    assert raw["parsed_pressure_hpa"] == 1014.75
    assert raw["pressure_gauge_unavailable"] is False
    assert raw["pressure_gauge_blocks_r1"] is False
    assert raw["continuous_mode_supported_by_v1"] is True
    assert raw["continuous_mode_not_used_in_r0_1"] is True


def test_com27_uses_modbus_read_only_driver_fallback_instead_of_pv_sv(tmp_path: Path) -> None:
    pressure_factory = _successful_pressure_factory()
    chamber_client = ReadOnlyChamberClient()

    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        operator_confirmation_path=_operator_confirmation(tmp_path, tmp_path / "r0_1_config.json"),
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=pressure_factory,
        chamber_client_factory=lambda _device: chamber_client,
    )

    chamber_path = Path(summary["artifact_paths"]["chamber_read_diagnostics_COM27"])
    chamber = json.loads(chamber_path.read_text(encoding="utf-8"))
    assert chamber["protocol_candidate"] == "modbus_rtu"
    assert chamber["protocol_status"] == "readonly_available"
    assert chamber["generic_ascii_query_failed"] is True
    assert chamber["ascii_query_unsupported"] is True
    assert chamber["legacy_ascii_query_status"] == "unsupported_for_configured_modbus_driver"
    assert chamber["chamber_driver_available"] is True
    assert chamber["chamber_readonly_driver_probe_status"] == "known_driver_readonly_succeeded"
    assert chamber["known_driver_readonly_succeeded"] is True
    assert chamber["known_driver_readonly_failed"] is False
    assert chamber["chamber_unavailable"] is False
    assert chamber["temperature_chamber_unavailable"] is False
    assert chamber["pv_current_temperature_c"] == 23.5
    assert chamber["sv_set_temperature_c"] == 25.0
    assert chamber["run_state"] == 0
    assert chamber["pv_current_temperature_register"] == 7991
    assert chamber["sv_set_temperature_read_register"] == 8100
    assert chamber["sv_set_temperature_write_register_identified_not_called"] == 8100
    assert chamber["slave_id"] == 1
    assert chamber["unit_id"] == 1
    assert chamber["write_register_sent"] is False
    assert chamber["write_coil_sent"] is False
    assert chamber["set_temperature_called"] is False
    assert chamber["control_command_sent"] is False
    assert not any(call.startswith("write_") for call in chamber_client.calls)


def test_com27_unavailable_is_protocol_unresolved_not_chamber_broken(tmp_path: Path) -> None:
    class UnavailableChamberClient(ReadOnlyChamberClient):
        def read_input_registers(self, address: int, count: int = 1, **_kwargs):
            self.calls.append(f"read_input_registers:{address}:{count}")
            return FakeModbusResponse(error="NO_RESPONSE")

    pressure_factory = _successful_pressure_factory()
    chamber_client = UnavailableChamberClient()

    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        operator_confirmation_path=_operator_confirmation(tmp_path, tmp_path / "r0_1_config.json"),
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=pressure_factory,
        chamber_client_factory=lambda _device: chamber_client,
    )

    chamber = json.loads(Path(summary["artifact_paths"]["chamber_read_diagnostics_COM27"]).read_text(encoding="utf-8"))
    assert summary["final_decision"] == "FAIL_CLOSED"
    assert chamber["protocol_status"] == "temperature_chamber_protocol_unresolved"
    assert chamber["generic_ascii_query_failed"] is True
    assert chamber["ascii_query_unsupported"] is True
    assert chamber["chamber_readonly_driver_probe_status"] == "known_driver_readonly_failed"
    assert chamber["known_driver_readonly_failed"] is True
    assert chamber["chamber_unavailable"] is True
    assert chamber["temperature_chamber_unavailable"] is True
    assert chamber["legacy_ascii_query_status"] == "unsupported_for_configured_modbus_driver"
    assert chamber["control_command_sent"] is False
    assert not any(call.startswith("write_") for call in chamber_client.calls)


def test_pv_sv_ascii_failure_alone_does_not_mark_chamber_unavailable(tmp_path: Path) -> None:
    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        operator_confirmation_path=_operator_confirmation(tmp_path, tmp_path / "r0_1_config.json"),
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=_successful_pressure_factory(),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
    )

    chamber = json.loads(Path(summary["artifact_paths"]["chamber_read_diagnostics_COM27"]).read_text(encoding="utf-8"))
    assert chamber["generic_ascii_query_failed"] is True
    assert chamber["ascii_query_unsupported"] is True
    assert chamber["known_driver_readonly_succeeded"] is True
    assert chamber["chamber_unavailable"] is False
    assert summary["temperature_chamber_unavailable"] is False


def test_r0_1_writes_required_artifacts(tmp_path: Path) -> None:
    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        operator_confirmation_path=_operator_confirmation(tmp_path, tmp_path / "r0_1_config.json"),
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=_successful_pressure_factory(),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
    )

    assert Path(summary["artifact_paths"]["summary"]).exists()
    assert Path(summary["artifact_paths"]["reference_read_diagnostics"]).exists()
    assert Path(summary["artifact_paths"]["r0_1_reference_read_diagnostics"]).exists()
    assert Path(summary["artifact_paths"]["raw_capture_COM30"]).exists()
    assert Path(summary["artifact_paths"]["raw_capture_COM27"]).exists()
    assert Path(summary["artifact_paths"]["raw_capture_COM30_pressure_gauge"]).exists()
    assert Path(summary["artifact_paths"]["chamber_read_diagnostics_COM27"]).exists()
    assert Path(summary["artifact_paths"]["port_open_close_trace"]).exists()
    assert Path(summary["artifact_paths"]["operator_confirmation_record"]).exists()
    assert Path(summary["artifact_paths"]["safety_assertions"]).exists()
    assert summary["operator_confirmation_valid"] is True
    assert summary["opened_only_com30_com27"] is True
    assert summary["attempted_write_count"] == 0
    assert summary["route_open_command_sent"] is False
    assert summary["relay_output_command_sent"] is False
    assert summary["valve_command_sent"] is False
    assert summary["pressure_setpoint_command_sent"] is False


def test_execute_read_only_requires_operator_confirmation(tmp_path: Path) -> None:
    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=_successful_pressure_factory(),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
    )

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["real_probe_executed"] is False
    assert summary["real_com_opened"] is False
    assert summary["operator_confirmation_valid"] is False
    assert "operator_confirmation_not_provided" in summary["r1_block_reasons"]
