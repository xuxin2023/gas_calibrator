from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools import run_v1_command_channel_matrix as module


class _FakeMatrixSerial:
    def __init__(self, *, responses: dict[bytes, bytes], **kwargs) -> None:
        self.port = kwargs.get("port", "COMX")
        self.baudrate = kwargs.get("baudrate", 115200)
        self.timeout = kwargs.get("timeout", 0.35)
        self.parity = kwargs.get("parity", "N")
        self.stopbits = kwargs.get("stopbits", 1)
        self.bytesize = kwargs.get("bytesize", 8)
        self._responses = {bytes(key): bytes(value) for key, value in responses.items()}
        self._buffer = bytearray()
        self._in_waiting = 0
        self.dtr = True
        self.rts = True
        self.is_open = True
        self.writes: list[bytes] = []

    @property
    def in_waiting(self) -> int:
        return len(self._buffer)

    def write(self, data: bytes) -> int:
        payload = bytes(data)
        self.writes.append(payload)
        self._buffer.extend(self._responses.get(payload, b""))
        return len(payload)

    def flush(self) -> None:
        return None

    def read(self, n: int) -> bytes:
        count = max(0, int(n))
        chunk = bytes(self._buffer[:count])
        del self._buffer[:count]
        return chunk

    def close(self) -> None:
        self.is_open = False


def test_command_channel_matrix_records_command_bytes_hex_and_statuses(tmp_path: Path) -> None:
    responses = {
        b"GETCO1,YGAS,079\r\n": b"<C0:1,C1:2,C2:3,C3:4>\r\n",
        b"GETCO,YGAS,FFF,7\n": b"YGAS,012,T\r\n",
        b"READDATA,YGAS,000\r": b"ALT,012,READY\r\n",
        b"GETCO7,YGAS,079": b"YGAS,079,0782.713,00.000,0.99,0.99,031.94,104.24,0001,2769\r\n",
    }
    created: list[_FakeMatrixSerial] = []

    def _factory(**kwargs):
        serial = _FakeMatrixSerial(responses=responses, **kwargs)
        created.append(serial)
        return serial

    result = module.run_command_channel_matrix(
        output_dir=tmp_path / "matrix",
        cases=[{"port": "COM39", "device_id": "079"}],
        capture_seconds=0.01,
        pre_drain_s=0.0,
        serial_factory=_factory,
    )

    rows = result["rows"]
    assert any(row["CommandBytesHex"] == b"GETCO1,YGAS,079\r\n".hex() for row in rows)
    assert any(row["Status"] == "explicit_c0" for row in rows)
    assert any(row["Status"] == "ack" for row in rows)
    assert any(row["Status"] == "non_ygas_response" for row in rows)
    assert any(row["Status"] == "only_legacy_ygas_stream" for row in rows)
    summary = json.loads(Path(result["summary_json_path"]).read_text(encoding="utf-8"))
    assert summary["per_case"]["COM39/079"]["explicit_c0_found"] is True
    assert summary["per_case"]["COM39/079"]["ack_found"] is True
    assert summary["per_case"]["COM39/079"]["non_ygas_response_found"] is True


def test_command_channel_matrix_compares_two_cases(tmp_path: Path) -> None:
    def _factory(**kwargs):
        port = str(kwargs.get("port"))
        if port == "COM39":
            responses = {
                b"GETCO1,YGAS,079\r\n": b"YGAS,079,0782.713,00.000,0.99,0.99,031.94,104.24,0001,2769\r\n"
            }
        else:
            responses = {
                b"GETCO1,YGAS,012\r\n": b"<C0:10,C1:20,C2:30,C3:40>\r\n"
            }
        return _FakeMatrixSerial(responses=responses, **kwargs)

    result = module.run_command_channel_matrix(
        output_dir=tmp_path / "matrix_compare",
        cases=[{"port": "COM39", "device_id": "079"}, {"port": "COM35", "device_id": "012"}],
        capture_seconds=0.01,
        pre_drain_s=0.0,
        serial_factory=_factory,
    )

    summary = result["summary"]
    assert summary["per_case"]["COM39/079"]["explicit_c0_found"] is False
    assert summary["per_case"]["COM35/012"]["explicit_c0_found"] is True


def test_command_channel_matrix_never_sends_write_commands(tmp_path: Path) -> None:
    created: list[_FakeMatrixSerial] = []

    def _factory(**kwargs):
        serial = _FakeMatrixSerial(responses={}, **kwargs)
        created.append(serial)
        return serial

    module.run_command_channel_matrix(
        output_dir=tmp_path / "matrix_no_write",
        cases=[{"port": "COM39", "device_id": "079"}],
        capture_seconds=0.0,
        pre_drain_s=0.0,
        serial_factory=_factory,
    )

    writes = [payload.decode("ascii", errors="ignore") for serial in created for payload in serial.writes]
    assert writes
    assert all(text.startswith(("READDATA", "GETCO")) for text in writes)
