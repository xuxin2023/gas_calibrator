from gas_calibrator.devices import paroscientific
from gas_calibrator.devices.serial_base import ReplaySerial


def test_read_pressure_ignores_echo_and_parses_payload(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self._lines = ["*0000P3", "*00011022.344"]

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def readline(self) -> str:
            if self._lines:
                return self._lines.pop(0)
            return ""

    monkeypatch.setattr(paroscientific, "SerialDevice", FakeSerialDevice)
    dev = paroscientific.ParoscientificGauge("COM1", 9600, dest_id="00", timeout=0.2)

    assert dev.read_pressure() == 1022.344


def test_paroscientific_supports_replay_status() -> None:
    replay = ReplaySerial(
        on_write=lambda data, transport: (
            transport.queue_line("*0000P3"),
            transport.queue_line("*00011015.500"),
        )
    )
    dev = paroscientific.ParoscientificGauge(
        "COM1",
        9600,
        dest_id="00",
        timeout=0.2,
        serial_factory=lambda **_: replay,
    )

    dev.open()
    status = dev.status()
    dev.close()

    assert status["pressure_hpa"] == 1015.5


def test_paroscientific_raises_after_no_response(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            pass

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(paroscientific, "SerialDevice", FakeSerialDevice)
    dev = paroscientific.ParoscientificGauge("COM1", 9600, dest_id="00", timeout=0.01)

    try:
        dev.read_pressure()
    except RuntimeError as exc:
        assert "NO_RESPONSE" in str(exc)
    else:
        raise AssertionError("expected NO_RESPONSE")


def test_paroscientific_fast_read_supports_single_attempt_and_buffer_reset(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.reset_calls = 0
            self.write_calls = 0
            self._lines = ["*0000P3", "*00011011.250"]

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.write_calls += 1
            self.last_write = data

        def readline(self) -> str:
            if self._lines:
                return self._lines.pop(0)
            return ""

        def reset_input_buffer(self) -> None:
            self.reset_calls += 1

    monkeypatch.setattr(paroscientific, "SerialDevice", FakeSerialDevice)
    dev = paroscientific.ParoscientificGauge("COM1", 9600, dest_id="00", timeout=0.2)

    assert dev.read_pressure(response_timeout_s=0.05, retries=1, retry_sleep_s=0.0, clear_buffer=True) == 1011.25
    assert dev.ser.reset_calls == 1
    assert dev.ser.write_calls == 1


def test_paroscientific_prefers_exchange_readlines_when_available(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.exchange_calls = []

        def open(self):
            return None

        def close(self):
            return None

        def exchange_readlines(
            self,
            data: str,
            *,
            response_timeout_s: float,
            read_timeout_s: float = 0.1,
            clear_input: bool = False,
        ):
            self.exchange_calls.append(
                {
                    "data": data,
                    "response_timeout_s": response_timeout_s,
                    "read_timeout_s": read_timeout_s,
                    "clear_input": clear_input,
                }
            )
            return ["*0000P3", "*00011015.750"]

    monkeypatch.setattr(paroscientific, "SerialDevice", FakeSerialDevice)
    dev = paroscientific.ParoscientificGauge("COM1", 9600, dest_id="00", timeout=0.2)

    assert dev.read_pressure(response_timeout_s=0.6, retries=1, retry_sleep_s=0.0, clear_buffer=False) == 1015.75
    assert len(dev.ser.exchange_calls) == 1
    assert dev.ser.exchange_calls[0]["clear_input"] is False


def test_paroscientific_fast_read_prefers_buffered_numeric_frame(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.exchange_calls = []

        def open(self):
            return None

        def close(self):
            return None

        def drain_input_nonblock(self, drain_s: float = 0.0, read_timeout_s: float = 0.05):
            self.drain_args = {"drain_s": drain_s, "read_timeout_s": read_timeout_s}
            return ["*00011012.500"]

        def exchange_readlines(self, *args, **kwargs):
            self.exchange_calls.append((args, kwargs))
            return []

    monkeypatch.setattr(paroscientific, "SerialDevice", FakeSerialDevice)
    dev = paroscientific.ParoscientificGauge("COM1", 9600, dest_id="00", timeout=0.2)

    assert dev.read_pressure_fast(response_timeout_s=0.3) == 1012.5
    assert dev.ser.exchange_calls == []


def test_paroscientific_continuous_pressure_mode_start_read_stop(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.reset_calls = 0
            self.writes = []
            self._lines = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)
            if data.endswith("P3\r\n"):
                self._lines = ["*0000P3", "*00011011.125"]

        def readline(self) -> str:
            if self._lines:
                return self._lines.pop(0)
            return ""

        def reset_input_buffer(self) -> None:
            self.reset_calls += 1

        def drain_input_nonblock(self, drain_s: float = 0.0, read_timeout_s: float = 0.05):
            self.drain_args = {"drain_s": drain_s, "read_timeout_s": read_timeout_s}
            return ["*00011011.500", "*00011011.750"]

    monkeypatch.setattr(paroscientific, "SerialDevice", FakeSerialDevice)
    dev = paroscientific.ParoscientificGauge("COM1", 9600, dest_id="00", timeout=0.2)

    assert dev.start_pressure_continuous(mode="P4", clear_buffer=True) is True
    assert dev.pressure_continuous_active() is True
    assert dev.read_pressure_continuous_latest(drain_s=0.12, read_timeout_s=0.02) == 1011.75
    assert dev.stop_pressure_continuous(response_timeout_s=0.05) is True
    assert dev.pressure_continuous_active() is False
    assert dev.ser.reset_calls == 1
    assert dev.ser.writes[0].endswith("P4\r\n")
    assert dev.ser.writes[-1].endswith("P3\r\n")


def test_paroscientific_continuous_stop_accepts_valid_cancel_command_without_numeric_reply(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.exchange_calls = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def exchange_readlines(self, *args, **kwargs):
            self.exchange_calls.append((args, kwargs))
            return ["*0000P4"]

        def drain_input_nonblock(self, drain_s: float = 0.0, read_timeout_s: float = 0.05):
            return []

    monkeypatch.setattr(paroscientific, "SerialDevice", FakeSerialDevice)
    dev = paroscientific.ParoscientificGauge("COM1", 9600, dest_id="00", timeout=0.2)

    assert dev.start_pressure_continuous(mode="P4", clear_buffer=False) is True
    assert dev.stop_pressure_continuous(response_timeout_s=0.05) is True
    assert dev.pressure_continuous_active() is False
    assert dev.ser.exchange_calls
    assert dev.ser.exchange_calls[-1][0][0].endswith("P3\r\n")
