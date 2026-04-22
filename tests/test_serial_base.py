import csv
from pathlib import Path

from gas_calibrator.devices import serial_base
from gas_calibrator.logging_utils import RunLogger


class FakeSerial:
    def __init__(
        self,
        port,
        baudrate,
        timeout,
        parity,
        stopbits,
        bytesize,
    ):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.parity = parity
        self.stopbits = stopbits
        self.bytesize = bytesize
        self.is_open = True

        self._line_queue = [b"OK\r\n"]
        self._read_buf = b"TAIL\r\n"
        self.in_waiting = len(self._read_buf)

    def write(self, data: bytes) -> None:
        self._last_write = data

    def flush(self) -> None:
        return None

    def readline(self) -> bytes:
        if self._line_queue:
            return self._line_queue.pop(0)
        return b""

    def read(self, n: int) -> bytes:
        if self._line_queue:
            self._read_buf = self._line_queue.pop(0) + self._read_buf
            self.in_waiting = len(self._read_buf)
        chunk = self._read_buf[:n]
        self._read_buf = self._read_buf[n:]
        self.in_waiting = len(self._read_buf)
        return chunk

    def reset_input_buffer(self) -> None:
        self._read_buf = b""
        self.in_waiting = 0

    def close(self) -> None:
        self.is_open = False


class ExplodingLogger:
    def log_io(self, **kwargs) -> None:
        raise RuntimeError("logger failed")


class BrokenStrError(Exception):
    def __str__(self) -> str:
        raise RuntimeError("bad __str__")


class WriteFailSerial(FakeSerial):
    def write(self, data: bytes) -> None:
        raise BrokenStrError()


class FlakyOpenFactory:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, **kwargs):
        self.calls += 1
        if self.calls == 1:
            raise PermissionError(13, "拒绝访问。")
        return FakeSerial(**kwargs)


class ReadPermissionDeniedSerial(FakeSerial):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.close_calls = 0

    def read(self, n: int) -> bytes:
        raise PermissionError(13, "拒绝访问。")

    def readline(self) -> bytes:
        raise PermissionError(13, "拒绝访问。")

    def close(self) -> None:
        self.close_calls += 1
        super().close()


class RecoveringReadFactory:
    def __init__(self) -> None:
        self.calls = 0
        self.first = None

    def __call__(self, **kwargs):
        self.calls += 1
        if self.calls == 1:
            self.first = ReadPermissionDeniedSerial(**kwargs)
            return self.first
        return FakeSerial(**kwargs)


class CrOnlySerial(FakeSerial):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._line_queue = [b"PACE 1\r"]
        self._read_buf = b""
        self.in_waiting = 0


def test_serial_device_logs_tx_rx(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(serial_base.serial, "Serial", FakeSerial)

    logger = RunLogger(tmp_path)
    dev = serial_base.SerialDevice(
        "COM7",
        9600,
        device_name="serial_under_test",
        io_logger=logger,
    )

    dev.open()
    dev.write("PING\\r\\n")
    assert dev.readline() == "OK"
    assert dev.read_available() == "TAIL"
    dev.close()
    logger.close()

    with logger.io_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    directions = [r["direction"] for r in rows]
    assert "TX" in directions
    assert "RX" in directions
    assert "OPEN" in directions
    assert "CLOSE" in directions

    tx_rows = [r for r in rows if r["direction"] == "TX"]
    assert any("PING" in r["command"] for r in tx_rows)
    rx_rows = [r for r in rows if r["direction"] == "RX"]
    assert any(r["duration_ms"] for r in rx_rows)


def test_serial_device_readline_handles_cr_only_response(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(serial_base.serial, "Serial", CrOnlySerial)

    logger = RunLogger(tmp_path)
    dev = serial_base.SerialDevice(
        "COM8",
        9600,
        device_name="pace_like",
        io_logger=logger,
    )

    dev.open()
    response = dev.query(":OUTP:STAT?\\n", delay_s=0.0)
    dev.close()
    logger.close()

    assert response == "PACE 1"
    with logger.io_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    query_rows = [r for r in rows if r["direction"] == "QUERY"]
    assert query_rows
    assert query_rows[-1]["response"] == "PACE 1"
    assert float(query_rows[-1]["duration_ms"]) >= 0.0


def test_serial_device_ignores_logger_failures(monkeypatch) -> None:
    monkeypatch.setattr(serial_base.serial, "Serial", FakeSerial)

    dev = serial_base.SerialDevice(
        "COM7",
        9600,
        device_name="serial_under_test",
        io_logger=ExplodingLogger(),
    )

    dev.open()
    dev.write("PING\\r\\n")
    assert dev.readline() == "OK"
    assert dev.read_available() == "TAIL"
    dev.close()


def test_serial_device_logs_error_rows_with_unprintable_exception(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(serial_base.serial, "Serial", WriteFailSerial)

    logger = RunLogger(tmp_path)
    dev = serial_base.SerialDevice(
        "COM9",
        9600,
        device_name="serial_under_test",
        io_logger=logger,
    )

    dev.open()
    try:
        dev.write("PING\\r\\n")
    except BrokenStrError:
        pass
    else:
        raise AssertionError("expected BrokenStrError")
    finally:
        dev.close()
        logger.close()

    with logger.io_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    error_rows = [r for r in rows if r["direction"] == "ERROR"]
    assert error_rows
    assert error_rows[-1]["port"] == "COM9"
    assert error_rows[-1]["device"] == "serial_under_test"
    assert "PING" in error_rows[-1]["command"]
    assert "BrokenStrError" in error_rows[-1]["error"]


def test_serial_device_retries_open_after_permission_error(monkeypatch, tmp_path: Path) -> None:
    factory = FlakyOpenFactory()

    logger = RunLogger(tmp_path)
    dev = serial_base.SerialDevice(
        "COM11",
        9600,
        device_name="serial_under_test",
        io_logger=logger,
        serial_factory=factory,
    )

    dev.open()
    dev.close()
    logger.close()

    assert factory.calls == 2
    with logger.io_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    directions = [r["direction"] for r in rows]
    assert "WARN" in directions
    assert directions.count("OPEN") == 1
    warn_rows = [r for r in rows if r["direction"] == "WARN"]
    assert any("OPEN_RETRY" in str(r["response"]) for r in warn_rows)


def test_serial_device_reopens_after_runtime_permission_error(monkeypatch, tmp_path: Path) -> None:
    factory = RecoveringReadFactory()

    logger = RunLogger(tmp_path)
    dev = serial_base.SerialDevice(
        "COM12",
        9600,
        device_name="serial_under_test",
        io_logger=logger,
        serial_factory=factory,
    )

    dev.open()
    assert dev.readline() == "OK"
    dev.close()
    logger.close()

    assert factory.calls == 2
    assert factory.first is not None
    assert factory.first.close_calls >= 1

    with logger.io_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    directions = [r["direction"] for r in rows]
    assert directions.count("OPEN") == 2
    assert "ERROR" in directions
    assert "WARN" in directions
    warn_rows = [r for r in rows if r["direction"] == "WARN"]
    assert any("IO_RECOVER_REOPEN" in str(r["response"]) for r in warn_rows)


def test_serial_device_exchange_readlines_restores_timeout_and_collects_lines(monkeypatch) -> None:
    monkeypatch.setattr(serial_base.serial, "Serial", FakeSerial)

    dev = serial_base.SerialDevice(
        "COM13",
        9600,
        timeout=1.0,
        device_name="serial_under_test",
    )
    dev.open()
    try:
        lines = dev.exchange_readlines("PING\r\n", response_timeout_s=0.03, read_timeout_s=0.01, clear_input=True)
        assert "OK" in lines
        assert dev._ser is not None
        assert dev._ser.timeout == 1.0
    finally:
        dev.close()
