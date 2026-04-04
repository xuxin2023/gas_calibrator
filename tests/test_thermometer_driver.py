from gas_calibrator.devices import thermometer
from gas_calibrator.devices.serial_base import ReplaySerial


def test_read_current_uses_buffer_fallback(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            pass

        def open(self):
            return None

        def close(self):
            return None

        def flush_input(self):
            return None

        def drain_input_nonblock(self, drain_s: float = 0.0, read_timeout_s: float = 0.0):
            return []

        def readline(self) -> str:
            return ""

        def read_available(self) -> str:
            return "T=23.45\r\n"

    monkeypatch.setattr(thermometer, "SerialDevice", FakeSerialDevice)
    dev = thermometer.Thermometer("COM1", 2400)

    row = dev.read_current()
    assert row["ok"] is True
    assert row["temp_c"] == 23.45


def test_read_current_prefers_latest_drained_frame(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            pass

        def open(self):
            return None

        def close(self):
            return None

        def flush_input(self):
            return None

        def drain_input_nonblock(self, drain_s: float = 0.0, read_timeout_s: float = 0.0):
            return ["+018.09C", "+000.25C"]

        def readline(self) -> str:
            raise AssertionError("readline should not be used when drained frames are available")

        def read_available(self) -> str:
            return ""

    monkeypatch.setattr(thermometer, "SerialDevice", FakeSerialDevice)
    dev = thermometer.Thermometer("COM1", 2400)

    row = dev.read_current()
    assert row["ok"] is True
    assert row["temp_c"] == 0.25


def test_thermometer_returns_not_ok_for_invalid_frame() -> None:
    replay = ReplaySerial(read_lines=["NO_TEMP"])
    dev = thermometer.Thermometer("COM1", 2400, serial_factory=lambda **_: replay)

    dev.open()
    row = dev.read()
    dev.close()

    assert row["ok"] is False
    assert row["temp_c"] is None


def test_thermometer_status_uses_replay_transport() -> None:
    replay = ReplaySerial(read_lines=["T=21.50"])
    dev = thermometer.Thermometer("COM1", 2400, serial_factory=lambda **_: replay)

    dev.open()
    status = dev.status()
    dev.close()

    assert status["ok"] is True
    assert status["temp_c"] == 21.5
