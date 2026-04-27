from __future__ import annotations

import json
from pathlib import Path

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
                "timeout": 1.0,
                "response_timeout_s": 2.2,
                "dest_id": "01",
            },
            "temperature_chamber": {
                "enabled": True,
                "port": "COM27",
                "baud": 9600,
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


class SilentPressureSerial:
    def __init__(self) -> None:
        self.writes: list[bytes] = []
        self.closed = False
        self.in_waiting = 0

    def write(self, payload: bytes) -> int:
        self.writes.append(payload)
        raise AssertionError("raw capture must not write pressure gauge commands")

    def readline(self) -> bytes:
        return b""

    def close(self) -> None:
        self.closed = True


class StreamingPressureSerial(SilentPressureSerial):
    def __init__(self, line: bytes) -> None:
        super().__init__()
        self._line = line

    def readline(self) -> bytes:
        line = self._line
        self._line = b""
        return line


def test_com30_unavailable_blocks_r1_and_preserves_no_write(tmp_path: Path) -> None:
    pressure_handle = SilentPressureSerial()
    chamber_client = ReadOnlyChamberClient()

    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=lambda _device: pressure_handle,
        chamber_client_factory=lambda _device: chamber_client,
    )

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["r1_conditioning_allowed"] is False
    assert summary["r1_blocked"] is True
    assert "pressure_gauge_reference_unavailable" in summary["r1_block_reasons"]
    assert pressure_handle.writes == []
    for key, expected in R0_1_EVIDENCE_MARKERS.items():
        assert summary[key] == expected


def test_com30_raw_capture_records_bytes_without_write_or_control(tmp_path: Path) -> None:
    pressure_handle = StreamingPressureSerial(b"*01001012.345\r\n")
    chamber_client = ReadOnlyChamberClient()

    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=lambda _device: pressure_handle,
        chamber_client_factory=lambda _device: chamber_client,
    )

    raw_path = Path(summary["artifact_paths"]["raw_capture_COM30_pressure_gauge"])
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    assert raw["raw_bytes_len"] > 0
    assert raw["parser_status"] == "parse_ok"
    assert raw["stream_mode_assessment"] == "continuous_output"
    assert raw["write_command_sent"] is False
    assert raw["control_command_sent"] is False
    assert pressure_handle.writes == []


def test_com27_uses_modbus_read_only_driver_fallback_instead_of_pv_sv(tmp_path: Path) -> None:
    pressure_handle = StreamingPressureSerial(b"*01001012.345\r\n")
    chamber_client = ReadOnlyChamberClient()

    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=lambda _device: pressure_handle,
        chamber_client_factory=lambda _device: chamber_client,
    )

    chamber_path = Path(summary["artifact_paths"]["chamber_read_diagnostics_COM27"])
    chamber = json.loads(chamber_path.read_text(encoding="utf-8"))
    assert chamber["protocol_candidate"] == "modbus_rtu"
    assert chamber["protocol_status"] == "read_only_driver_fallback_used"
    assert chamber["legacy_ascii_query_status"] == "unsupported_for_configured_modbus_driver"
    assert chamber["pv_current_temperature_c"] == 23.5
    assert chamber["sv_set_temperature_c"] == 25.0
    assert chamber["write_register_sent"] is False
    assert chamber["write_coil_sent"] is False
    assert chamber["control_command_sent"] is False
    assert not any(call.startswith("write_") for call in chamber_client.calls)


def test_com27_unavailable_is_protocol_unresolved_not_chamber_broken(tmp_path: Path) -> None:
    class UnavailableChamberClient(ReadOnlyChamberClient):
        def read_input_registers(self, address: int, count: int = 1, **_kwargs):
            self.calls.append(f"read_input_registers:{address}:{count}")
            return FakeModbusResponse(error="NO_RESPONSE")

    pressure_handle = StreamingPressureSerial(b"*01001012.345\r\n")
    chamber_client = UnavailableChamberClient()

    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=lambda _device: pressure_handle,
        chamber_client_factory=lambda _device: chamber_client,
    )

    chamber = json.loads(Path(summary["artifact_paths"]["chamber_read_diagnostics_COM27"]).read_text(encoding="utf-8"))
    assert summary["final_decision"] == "FAIL_CLOSED"
    assert chamber["protocol_status"] == "temperature_chamber_protocol_unresolved"
    assert chamber["legacy_ascii_query_status"] == "unsupported_for_configured_modbus_driver"
    assert chamber["control_command_sent"] is False
    assert not any(call.startswith("write_") for call in chamber_client.calls)


def test_r0_1_writes_required_artifacts(tmp_path: Path) -> None:
    summary = write_r0_1_reference_read_probe_artifacts(
        _base_config(),
        output_dir=tmp_path / "r0_1",
        config_path=tmp_path / "r0_1_config.json",
        cli_allow=True,
        env={R0_1_ENV_VAR: "1"},
        execute_read_only=True,
        pressure_serial_factory=lambda _device: StreamingPressureSerial(b"*01001012.345\r\n"),
        chamber_client_factory=lambda _device: ReadOnlyChamberClient(),
    )

    assert Path(summary["artifact_paths"]["summary"]).exists()
    assert Path(summary["artifact_paths"]["r0_1_reference_read_diagnostics"]).exists()
    assert Path(summary["artifact_paths"]["raw_capture_COM30_pressure_gauge"]).exists()
    assert Path(summary["artifact_paths"]["chamber_read_diagnostics_COM27"]).exists()
    assert Path(summary["artifact_paths"]["port_open_close_trace"]).exists()
    assert summary["attempted_write_count"] == 0
    assert summary["route_open_command_sent"] is False
    assert summary["relay_output_command_sent"] is False
    assert summary["valve_command_sent"] is False
    assert summary["pressure_setpoint_command_sent"] is False
