import time

from gas_calibrator.devices import pace5000
from gas_calibrator.devices.serial_base import ReplaySerial


def test_read_pressure_handles_echo_line(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self._reads = ["1013.25"]

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            # Legacy mode may echo the command first.
            return data.strip()

        def readline(self) -> str:
            if self._reads:
                return self._reads.pop(0)
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.read_pressure() == 1013.25


def test_read_pressure_falls_back_to_meas_query(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            pass

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd.startswith(":SENS:PRES?"):
                return ""
            if cmd.startswith(":MEAS:PRES?"):
                return "1009.5"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.read_pressure() == 1009.5


def test_read_pressure_prefers_in_limits_query_over_selected_sensor(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            pass

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd.startswith(":SENS:PRES:INL?"):
                return ":SENS:PRES:INL 1000.5, 1"
            if cmd.startswith(":SENS:PRES?"):
                return ":SENS:PRES 0.08"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600, pressure_queries=[":SENS:PRES?", ":MEAS:PRES?"])

    assert dev.read_pressure() == 1000.5


def test_vent_command_requires_explicit_on_off(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.vent(True)
    dev.vent(False)

    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in w for w in dev.ser.writes)


def test_get_vent_status_queries_scpi_status(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            pass

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            return ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_vent_status() == pace5000.Pace5000.VENT_STATUS_IN_PROGRESS


def test_legacy_ge_druck_identity_accepts_completed_vent_status_for_control(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.queries = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            self.queries.append(cmd)
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 2"
            raise AssertionError(f"unexpected query: {data}")

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.vent_status_allows_control(dev.get_vent_status()) is True
    assert dev.supports_vent_after_valve_open() is False
    assert not any(":SOUR:PRES:LEV:IMM:AMPL:VENT:AFT:VVAL:STAT?" in cmd for cmd in dev.ser.queries)


def test_legacy_ge_druck_identity_accepts_trapped_vent_status_for_control(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.queries = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            self.queries.append(cmd)
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 3"
            raise AssertionError(f"unexpected query: {data}")

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.vent_status_allows_control(dev.get_vent_status()) is True
    assert dev.vent_terminal_statuses() == [
        pace5000.Pace5000.VENT_STATUS_IDLE,
        pace5000.Pace5000.VENT_STATUS_COMPLETED,
        pace5000.Pace5000.VENT_STATUS_TRAPPED_PRESSURE,
    ]


def test_set_vent_after_valve_open_writes_scpi(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.set_vent_after_valve_open(True)
    dev.set_vent_after_valve_open(False)

    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT:AFT:VVAL:STAT OPEN" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT:AFT:VVAL:STAT CLOSED" in w for w in dev.ser.writes)


def test_get_vent_after_valve_open_parses_query(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self._responses = iter(["OPEN", "CLOSED"])

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            return next(self._responses)

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_vent_after_valve_open() is True
    assert dev.get_vent_after_valve_open() is False


def test_set_vent_popup_ack_enabled_writes_scpi(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.set_vent_popup_ack_enabled(True)
    dev.set_vent_popup_ack_enabled(False)

    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT:APOP:STAT ENABled" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT:APOP:STAT DISabled" in w for w in dev.ser.writes)


def test_get_vent_popup_ack_enabled_parses_query(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self._responses = iter(["ENABLED", "DISABLED"])

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            return next(self._responses)

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_vent_popup_ack_enabled() is True
    assert dev.get_vent_popup_ack_enabled() is False


def test_get_isolation_state_queries_scpi_status(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            pass

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            return ":OUTP:ISOL:STAT 1"

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_isolation_state() == 1


def test_enter_atmosphere_mode_opens_isolation_and_tolerates_terminal_vent_timeout(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self._vent_status = iter([
                ":SOUR:PRES:LEV:IMM:AMPL:VENT 1",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
            ])

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
                return next(self._vent_status)
            if cmd.startswith(":OUTP:STAT?"):
                return ":OUTP:STAT 0"
            if cmd.startswith(":OUTP:ISOL:STAT?"):
                return ":OUTP:ISOL:STAT 1"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    status = dev.enter_atmosphere_mode(timeout_s=1.0, poll_s=0.0)

    assert status == pace5000.Pace5000.VENT_STATUS_TIMED_OUT
    assert any(":OUTP 0" in w for w in dev.ser.writes)
    assert any(":OUTP:ISOL:STAT 1" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in w for w in dev.ser.writes)


def test_exit_atmosphere_mode_keeps_output_path_open_without_enabling_output(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 3"
            if cmd.startswith(":OUTP:STAT?"):
                return ":OUTP:STAT 0"
            if cmd.startswith(":OUTP:ISOL:STAT?"):
                return ":OUTP:ISOL:STAT 1"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    status = dev.exit_atmosphere_mode(timeout_s=1.0, poll_s=0.0)

    assert status == pace5000.Pace5000.VENT_STATUS_TRAPPED_PRESSURE
    assert any(":OUTP 0" in w for w in dev.ser.writes)
    assert any(":OUTP:ISOL:STAT 1" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in w for w in dev.ser.writes)


def test_enter_atmosphere_mode_hold_open_reissues_vent(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.port = "COM1"

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"
            if cmd.startswith(":OUTP:STAT?"):
                return ":OUTP:STAT 0"
            if cmd.startswith(":OUTP:ISOL:STAT?"):
                return ":OUTP:ISOL:STAT 1"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    status = dev.enter_atmosphere_mode(timeout_s=1.0, poll_s=0.0, hold_open=True, hold_interval_s=0.02)
    time.sleep(0.08)
    assert dev.is_atmosphere_hold_active() is True
    assert dev.stop_atmosphere_hold() is True

    assert status == pace5000.Pace5000.VENT_STATUS_IN_PROGRESS
    assert dev.is_atmosphere_hold_active() is False
    assert sum(1 for w in dev.ser.writes if ":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in w) >= 2


def test_enter_atmosphere_mode_with_open_vent_valve_uses_single_vent(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.port = "COM1"

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT:AFT:VVAL:STAT?"):
                return "OPEN"
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT:APOP:STAT?"):
                return "DISABLED"
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
            if cmd.startswith(":OUTP:STAT?"):
                return ":OUTP:STAT 0"
            if cmd.startswith(":OUTP:ISOL:STAT?"):
                return ":OUTP:ISOL:STAT 1"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    status = dev.enter_atmosphere_mode_with_open_vent_valve(timeout_s=1.0, poll_s=0.0, popup_ack_enabled=False)

    assert status == pace5000.Pace5000.VENT_STATUS_IDLE
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT:AFT:VVAL:STAT OPEN" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT:APOP:STAT DISabled" in w for w in dev.ser.writes)
    assert sum(1 for w in dev.ser.writes if ":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in w) == 1
    assert dev.is_atmosphere_hold_active() is False


def test_begin_atmosphere_handoff_disables_output_and_starts_vent_without_waiting(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    status = dev.begin_atmosphere_handoff()

    assert status == pace5000.Pace5000.VENT_STATUS_IN_PROGRESS
    assert any(":OUTP 0" in w for w in dev.ser.writes)
    assert any(":OUTP:ISOL:STAT 1" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in w for w in dev.ser.writes)


def test_enable_control_output_sets_active_mode_and_output_on(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.enable_control_output()

    assert any(":OUTP:MODE ACT" in w for w in dev.ser.writes)
    assert any(":OUTP 1" in w for w in dev.ser.writes)


def test_set_setpoint_uses_level_amplitude_command(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.set_setpoint(1100.0)

    assert any(":SOUR:PRES:LEV:IMM:AMPL 1100.0" in w for w in dev.ser.writes)


def test_set_output_mode_active(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.set_output_mode_active()

    assert any(":OUTP:MODE ACT" in w for w in dev.ser.writes)


def test_soft_control_scpi_helpers(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.set_output_mode_passive()
    dev.set_slew_mode_linear()
    dev.set_slew_mode_max()
    dev.set_slew_rate(3.0)
    dev.set_overshoot_allowed(False)

    assert any(":OUTP:MODE PASS" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:SLEW:MODE LIN" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:SLEW:MODE MAX" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:SLEW 3.0" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:SLEW:OVER 0" in w for w in dev.ser.writes)


def test_pace5000_supports_replay_status() -> None:
    def _on_write(data: bytes, transport: ReplaySerial) -> None:
        text = data.decode("ascii", errors="ignore").strip().upper()
        mapping = {
            ":SENS:PRES:INL?": ":SENS:PRES:INL 1000.5, 1",
            ":OUTP:STAT?": ":OUTP:STAT 0",
            ":OUTP:ISOL:STAT?": ":OUTP:ISOL:STAT 1",
            ":SOUR:PRES:LEV:IMM:AMPL:VENT?": ":SOUR:PRES:LEV:IMM:AMPL:VENT 0",
        }
        reply = mapping.get(text)
        if reply:
            transport.queue_line(reply)

    replay = ReplaySerial(on_write=_on_write)
    dev = pace5000.Pace5000("COM1", 9600, serial_factory=lambda **_: replay)

    dev.open()
    status = dev.status()
    dev.close()

    assert status["pressure_hpa"] == 1000.5
    assert status["output_state"] == 0
    assert status["isolation_state"] == 1
    assert status["vent_status"] == 0


def test_pace5000_query_reads_second_line_after_echo_with_serial_device() -> None:
    def _on_write(data: bytes, transport: ReplaySerial) -> None:
        text = data.decode("ascii", errors="ignore")
        transport.queue_line(text.strip())
        transport.queue_line(":OUTP:STAT 1")

    replay = ReplaySerial(on_write=_on_write)
    dev = pace5000.Pace5000("COM1", 9600, serial_factory=lambda **_: replay)

    dev.open()
    value = dev.get_output_state()
    dev.close()

    assert value == 1


def test_parse_bool_state_rejects_error_strings_with_numeric_tokens() -> None:
    for text in ("ERROR 1", "BAD 1", "COMMAND 1 FAILED", "STATE_1_ERROR"):
        assert pace5000.Pace5000._parse_bool_state(text) is None
