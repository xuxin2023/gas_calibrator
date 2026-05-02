from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools.run_v1_getco_probe import classify_probe_capture, run_getco_probe


def test_classify_probe_capture_accepts_mixed_stream_and_coefficient_line() -> None:
    raw = (
        b"YGAS,079,0782.713,00.000,0.99,0.99,031.94,104.24,0001,2769\r\n"
        b"<C0:1.1,C1:2.2,C2:3.3,C3:4.4>\r\n"
    )

    result = classify_probe_capture(raw, raw.decode("ascii", errors="ignore"))

    assert result["status"] == "parsed_success"
    assert result["failure_reason"] == ""
    assert result["parsed_coefficients"] == {
        "C0": 1.1,
        "C1": 2.2,
        "C2": 3.3,
        "C3": 4.4,
    }


def test_classify_probe_capture_does_not_misread_plain_legacy_stream() -> None:
    raw = b"YGAS,079,0782.713,00.000,0.99,0.99,031.94,104.24,0001,2769\r\n"

    result = classify_probe_capture(raw, raw.decode("ascii", errors="ignore"))

    assert result["status"] == "raw_received_but_unparsed"
    assert result["failure_reason"] == "active_stream_only"
    assert result["parsed_coefficients"] == {}


class _FakeProbeSerial:
    def __init__(self, *, responses: dict[str, bytes], **_kwargs) -> None:
        self._responses = {str(key): bytes(value) for key, value in responses.items()}
        self._buffer = bytearray()
        self.writes: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    @property
    def in_waiting(self) -> int:
        return len(self._buffer)

    def write(self, data: bytes) -> int:
        decoded = data.decode("ascii", errors="ignore")
        self.writes.append(decoded)
        self._buffer.extend(self._responses.get(decoded, b""))
        return len(data)

    def flush(self) -> None:
        return None

    def read(self, n: int) -> bytes:
        count = max(0, int(n))
        chunk = bytes(self._buffer[:count])
        del self._buffer[:count]
        return chunk


def test_run_getco_probe_summary_distinguishes_probe_statuses(tmp_path: Path) -> None:
    out_dir = tmp_path / "probe"
    responses = {
        "GETCO1,YGAS,079\r\n": b"<C0:1.0,C1:2.0,C2:3.0,C3:4.0>\r\n",
        "GETCO3,YGAS,079\r\n": b"YGAS,079,0782.713,00.000,0.99,0.99,031.94,104.24,0001,2769\r\n",
    }

    result = run_getco_probe(
        port="COM39",
        device_id="079",
        output_dir=out_dir,
        repeat=1,
        capture_seconds=0.05,
        drain_window_s=0.0,
        quiet_window_s=0.0,
        serial_factory=lambda **kwargs: _FakeProbeSerial(responses=responses, **kwargs),
    )

    statuses = {
        record["command"]: record["status"]
        for record in result["records"]
        if record["strategy"] == "direct"
    }
    assert statuses["GETCO1,YGAS,079"] == "parsed_success"
    assert statuses["GETCO3,YGAS,079"] == "raw_received_but_unparsed"
    assert statuses["GETCO7,YGAS,079"] == "no_response"

    summary_payload = json.loads((out_dir / "getco_probe_summary.json").read_text(encoding="utf-8"))
    matrix_csv = (out_dir / "getco_probe_matrix.csv").read_text(encoding="utf-8-sig")
    raw_log = (out_dir / "getco_probe_raw.log").read_text(encoding="utf-8")

    assert any(record["status"] == "parsed_success" for record in summary_payload["records"])
    assert any(record["status"] == "raw_received_but_unparsed" for record in summary_payload["records"])
    assert any(record["status"] == "no_response" for record in summary_payload["records"])
    assert "parsed_success" in matrix_csv
    assert "raw_received_but_unparsed" in matrix_csv
    assert "no_response" in matrix_csv
    assert "GETCO1,YGAS,079" in raw_log
