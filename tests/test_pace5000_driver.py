import time

import pytest

from gas_calibrator.devices import pace5000
from gas_calibrator.devices.serial_base import ReplaySerial


@pytest.fixture(autouse=True)
def _default_blank_system_error_is_no_error(monkeypatch) -> None:
    original = pace5000.Pace5000.get_system_error

    def _patched(self):
        value = original(self)
        return value or ':SYST:ERR 0,"No error"'

    monkeypatch.setattr(pace5000.Pace5000, "get_system_error", _patched)


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

    assert dev.get_in_limits() == (1000.5, 1)
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


def test_legacy_ge_druck_identity_treats_completed_vent_status_as_control_ready_baseline(monkeypatch) -> None:
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
            if cmd == ":INST:VERS?":
                return ':INST:VERS "02.00.07"'
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


def test_legacy_ge_druck_identity_keeps_vent_status_3_as_watchlist_only(monkeypatch) -> None:
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
            if cmd == ":INST:VERS?":
                return ':INST:VERS "02.00.07"'
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 3"
            raise AssertionError(f"unexpected query: {data}")

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.vent_status_allows_control(dev.get_vent_status()) is False
    assert dev.vent_terminal_statuses() == [pace5000.Pace5000.VENT_STATUS_IDLE]


def test_legacy_ge_druck_identity_rejects_trapped_vent_status_without_known_compatibility_version(monkeypatch) -> None:
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
                return "GE Druck,Pace5000 User Interface,3213201,02.00.08"
            if cmd == ":INST:VERS?":
                return "02.00.08"
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 3"
            raise AssertionError(f"unexpected query: {data}")

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.vent_status_allows_control(dev.get_vent_status()) is False
    assert dev.vent_terminal_statuses() == [pace5000.Pace5000.VENT_STATUS_IDLE]


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


def test_enter_atmosphere_mode_old_profile_accepts_completed_status_without_vent0(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self._vent_status = iter([
                ":SOUR:PRES:LEV:IMM:AMPL:VENT 1",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT 0",
            ])

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
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

    assert status == pace5000.Pace5000.VENT_STATUS_COMPLETED
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in w for w in dev.ser.writes)
    assert not any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in w for w in dev.ser.writes)


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
            if cmd == "*IDN?":
                return "PACE5000,Controller,123456,03.01.00"
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

    with pytest.raises(RuntimeError, match="VENT_STATUS_3"):
        dev.exit_atmosphere_mode(timeout_s=1.0, poll_s=0.0)

    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in w for w in dev.ser.writes)


def test_enter_atmosphere_mode_hold_open_argument_does_not_start_refresh_loop(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.port = "COM1"
            self._vent_status = iter([
                ":SOUR:PRES:LEV:IMM:AMPL:VENT 1",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT 0",
            ])

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
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

    status = dev.enter_atmosphere_mode(timeout_s=1.0, poll_s=0.0, hold_open=True, hold_interval_s=0.02)

    assert status == pace5000.Pace5000.VENT_STATUS_COMPLETED
    assert dev.is_atmosphere_hold_active() is False
    assert dev.stop_atmosphere_hold() is True
    assert sum(1 for w in dev.ser.writes if ":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in w) == 1
    assert sum(1 for w in dev.ser.writes if ":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in w) == 0


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
    assert any(":OUTP:STAT 0" in w for w in dev.ser.writes)
    assert any(":OUTP:ISOL:STAT 1" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in w for w in dev.ser.writes)


def test_enable_control_output_sets_active_mode_and_output_on(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.output_state = 0
            self.isolation_state = 0
            self.output_mode = "PASS"

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)
            text = data.strip()
            upper = text.upper()
            if upper.startswith(":OUTP:STAT "):
                self.output_state = int(float(text.split()[-1]))
            elif upper.startswith(":OUTP:ISOL:STAT "):
                self.isolation_state = int(float(text.split()[-1]))
            elif upper == ":OUTP:MODE ACT":
                self.output_mode = "ACT"

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
            if cmd == ":OUTP:MODE?":
                return f":OUTP:MODE {self.output_mode}"
            if cmd == ":OUTP:STAT?":
                return f":OUTP:STAT {self.output_state}"
            if cmd == ":OUTP:ISOL:STAT?":
                return f":OUTP:ISOL:STAT {self.isolation_state}"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.enable_control_output()

    assert any(":OUTP:MODE ACT" in w for w in dev.ser.writes)
    assert any(":OUTP:STAT 1" in w for w in dev.ser.writes)


def test_enable_control_output_rejects_legacy_watchlist_status_3(monkeypatch) -> None:
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
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":INST:VERS?":
                return "02.00.07"
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 3"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    with pytest.raises(RuntimeError, match="VENT_"):
        dev.enable_control_output(timeout_s=0.5, poll_s=0.0)

    assert not any(":OUTP:STAT 1" in w for w in dev.ser.writes)


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
            cmd = data.strip().upper()
            if cmd == ":SOUR:PRES?":
                return ":SOUR:PRES 1100.0"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.set_setpoint(1100.0)

    assert any(":SOUR:PRES 1100.0" in w for w in dev.ser.writes)


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
            ":SENS:PRES?": ":SENS:PRES 1000.5",
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


def test_set_output_enabled_verified_waits_for_output_off(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.output_state = 1

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)
            text = data.strip()
            if text.upper().startswith(":OUTP:STAT "):
                self.output_state = int(float(text.split()[-1]))

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == ":OUTP:STAT?":
                return f":OUTP:STAT {self.output_state}"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.set_output_enabled_verified(False) == 0
    assert any(":OUTP:STAT 0" in write for write in dev.ser.writes)


def test_set_output_isolated_verified_maps_isolated_true_to_closed_path(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.isolation_state = 1

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)
            text = data.strip()
            if text.upper().startswith(":OUTP:ISOL:STAT "):
                self.isolation_state = int(float(text.split()[-1]))

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == ":OUTP:ISOL:STAT?":
                return f":OUTP:ISOL:STAT {self.isolation_state}"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.set_output_isolated_verified(True) == 0
    assert any(":OUTP:ISOL:STAT 0" in write for write in dev.ser.writes)


def test_get_vent_after_valve_state_parses_open_and_closed(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.responses = iter(["OPEN", "CLOSED"])

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            return next(self.responses)

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_vent_after_valve_state() == "OPEN"
    assert dev.get_vent_after_valve_state() == "CLOSED"


def test_get_vent_popup_state_parses_enabled_and_disabled(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.responses = iter(["ENABLED", "DISABLED"])

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            return next(self.responses)

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_vent_popup_state() == "ENABLED"
    assert dev.get_vent_popup_state() == "DISABLED"


def test_get_oper_condition_queries_scpi_status(monkeypatch) -> None:
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
            if cmd == ":STAT:OPER:COND?":
                return ":STAT:OPER:COND 17"
            if cmd == ":STAT:OPER:PRES:COND?":
                return ":STAT:OPER:PRES:COND 9"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_oper_condition() == 17
    assert dev.get_oper_pressure_condition() == 9


def test_clear_completed_vent_latch_if_present_sends_vent_zero_and_waits_for_idle(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.responses = iter(
                [
                    ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
                    ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
                    ":SOUR:PRES:LEV:IMM:AMPL:VENT 0",
                ]
            )

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return next(self.responses)
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    result = dev.clear_completed_vent_latch_if_present(timeout_s=0.2, poll_s=0.0)

    assert result["before_status"] == 2
    assert result["clear_attempted"] is True
    assert result["after_status"] == 0
    assert result["cleared"] is True
    assert result["command"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
    assert result["vent3_watchlist_observed"] is False
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in write for write in dev.ser.writes)


def test_clear_completed_vent_latch_if_present_blocks_legacy_auto_clear_when_completed_latched(monkeypatch) -> None:
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
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":INST:VERS?":
                return "02.00.07"
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 2"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    result = dev.clear_completed_vent_latch_if_present(timeout_s=0.2, poll_s=0.0)

    assert result["before_status"] == 2
    assert result["clear_attempted"] is False
    assert result["after_status"] == 2
    assert result["cleared"] is False
    assert result["command"] == ""
    assert result["vent3_watchlist_observed"] is False
    assert result["skipped"] is True
    assert result["blocked"] is True
    assert result["reason"] == "legacy_completed_latch_auto_clear_blocked"
    assert result["manual_intervention_required"] is True
    assert result["vent_command_sent"] is False
    assert not any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in write for write in dev.ser.writes)


def test_wait_for_vent_idle_blocks_legacy_auto_clear_when_completed_latched(monkeypatch) -> None:
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
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":INST:VERS?":
                return ':INST:VERS "02.00.07"'
            if cmd == ":OUTP:STAT?":
                return ":OUTP:STAT 0"
            if cmd == ":OUTP:ISOL:STAT?":
                return ":OUTP:ISOL:STAT 1"
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 2"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    with pytest.raises(RuntimeError, match="VENT_COMPLETED_LATCH_AUTO_CLEAR_BLOCKED"):
        dev.wait_for_vent_idle(timeout_s=0.2, poll_s=0.0)

    assert not any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in write for write in dev.ser.writes)


def test_exit_atmosphere_mode_treats_legacy_completed_status_as_observed_baseline(monkeypatch) -> None:
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
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":INST:VERS?":
                return ':INST:VERS "02.00.07"'
            if cmd == ":OUTP:STAT?":
                return ":OUTP:STAT 0"
            if cmd == ":OUTP:ISOL:STAT?":
                return ":OUTP:ISOL:STAT 1"
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 2"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    status = dev.exit_atmosphere_mode(timeout_s=0.2, poll_s=0.0)

    assert status == pace5000.Pace5000.VENT_STATUS_COMPLETED
    assert not any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in write for write in dev.ser.writes)


def test_vent_status_allows_control_accepts_completed_status_for_old_profile(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            pass

        def open(self):
            return None

        def close(self):
            return None

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":INST:VERS?":
                return ':INST:VERS "02.00.07"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_OLD_PACE5000
    assert dev.vent_status_allows_control(2) is True
    assert dev.vent_status_allows_control(1) is False


def test_exit_atmosphere_mode_blocks_legacy_auto_abort_for_watchlist_status_3(monkeypatch) -> None:
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
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 3"
            if cmd == ":OUTP:STAT?":
                return ":OUTP:STAT 0"
            if cmd == ":OUTP:ISOL:STAT?":
                return ":OUTP:ISOL:STAT 1"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    with pytest.raises(RuntimeError, match="LEGACY_AUTO_ABORT_VENT_BLOCKED"):
        dev.exit_atmosphere_mode(timeout_s=0.2, poll_s=0.0)

    assert not any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in write for write in dev.ser.writes)


def test_exit_atmosphere_mode_legacy_override_allows_explicit_auto_abort(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.responses = iter(
                [
                    ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
                    ":SOUR:PRES:LEV:IMM:AMPL:VENT 0",
                ]
            )

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":INST:VERS?":
                return ':INST:VERS "02.00.07"'
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return next(self.responses)
            if cmd == ":OUTP:STAT?":
                return ":OUTP:STAT 0"
            if cmd == ":OUTP:ISOL:STAT?":
                return ":OUTP:ISOL:STAT 1"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    monkeypatch.setenv(pace5000.Pace5000.LEGACY_AUTO_ABORT_VENT_OVERRIDE_ENV, "1")
    dev = pace5000.Pace5000("COM1", 9600)

    status = dev.exit_atmosphere_mode(timeout_s=0.2, poll_s=0.0)

    assert status == pace5000.Pace5000.VENT_STATUS_IDLE
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in write for write in dev.ser.writes)


def test_diagnostic_status_collects_best_effort_aux_fields(monkeypatch) -> None:
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
            mapping = {
                ":SENS:PRES:INL?": ":SENS:PRES:INL 1000.5, 1",
                ":OUTP:STAT?": ":OUTP:STAT 0",
                ":OUTP:ISOL:STAT?": ":OUTP:ISOL:STAT 0",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT?": ":SOUR:PRES:LEV:IMM:AMPL:VENT 0",
                ":OUTP:MODE?": "ACT",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT:AFT:VVAL:STAT?": "OPEN",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT:APOP:STAT?": "ENABLED",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT:ETIM?": "7.5",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT:ORPV:STAT?": "ENABLED",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT:PUPV:STAT?": "DISABLED",
                ":STAT:OPER:COND?": ":STAT:OPER:COND 3",
                ":STAT:OPER:PRES:COND?": ":STAT:OPER:PRES:COND 5",
                ":STAT:OPER:PRES:EVEN?": ":STAT:OPER:PRES:EVEN 1",
                ":SOUR:PRES:EFF?": ":SOUR:PRES:EFF 0.02",
                ":SOUR:PRES:COMP1?": ":SOUR:PRES:COMP1 0.18",
                ":SOUR:PRES:COMP2?": ":SOUR:PRES:COMP2 -0.01",
                ":SENS:PRES:CONT?": ":SENS:PRES:CONT 1000.7",
                ":SENS:PRES:BAR?": ":SENS:PRES:BAR 1013.2",
                ":SENS:PRES:INL:TIME?": ":SENS:PRES:INL:TIME 12.0",
                ":SENS:PRES:SLEW?": ":SENS:PRES:SLEW 0.003",
            }
            return mapping.get(cmd, "")

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    status = dev.diagnostic_status()

    assert status["pressure_hpa"] == 1000.5
    assert status["output_state"] == 0
    assert status["isolation_state"] == 0
    assert status["vent_status"] == 0
    assert status["output_mode"] == "ACT"
    assert status["vent_after_valve_state"] == "OPEN"
    assert status["vent_popup_state"] == "ENABLED"
    assert status["vent_elapsed_time_s"] == 7.5
    assert status["vent_orpv_state"] == "ENABLED"
    assert status["vent_pupv_state"] == "DISABLED"
    assert status["oper_condition"] == 3
    assert status["oper_pressure_condition"] == 5
    assert status["oper_pressure_event"] == 1
    assert status["oper_pressure_vent_complete_bit"] is True
    assert status["oper_pressure_in_limits_bit"] is True
    assert status["effort"] == 0.02
    assert status["comp1"] == 0.18
    assert status["comp2"] == -0.01
    assert status["control_pressure_hpa"] == 1000.5
    assert ":SENS:PRES:CONT?" not in dev.ser.queries
    assert status["barometric_pressure_hpa"] == 1013.2
    assert status["in_limits_pressure_hpa"] == 1000.5
    assert status["in_limits_state"] == 1
    assert status["in_limits_time_s"] == 12.0
    assert status["measured_slew_hpa_s"] == 0.003
    assert status["vent_completed_latched"] is False


def test_clear_status_and_drain_system_errors_use_standard_scpi(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.responses = iter(
                [
                    '-222,"Data out of range"',
                    '0,"No error"',
                ]
            )

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == ":SYST:ERR?":
                return next(self.responses)
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.clear_status()
    drained = dev.drain_system_errors()

    assert any("*CLS" in write for write in dev.ser.writes)
    assert drained == ['-222,"Data out of range"']


def test_parse_system_error_accepts_bare_zero_no_error() -> None:
    code, message = pace5000.Pace5000._parse_system_error('0,"No error"')

    assert code == 0
    assert message == "No error"
    assert pace5000.Pace5000._is_zero_system_error('0,"No error"') is True


def test_parse_system_error_accepts_bare_zero_no_error_with_space() -> None:
    code, message = pace5000.Pace5000._parse_system_error('0, "No error"')

    assert code == 0
    assert message == "No error"


def test_parse_system_error_accepts_bare_syntax_error() -> None:
    code, message = pace5000.Pace5000._parse_system_error('-102,"Syntax error"')

    assert code == -102
    assert message == "Syntax error"


def test_parse_system_error_accepts_bare_undefined_header() -> None:
    code, message = pace5000.Pace5000._parse_system_error('-113,"Undefined header"')

    assert code == -113
    assert message == "Undefined header"


def test_parse_system_error_accepts_headered_syntax_error() -> None:
    code, message = pace5000.Pace5000._parse_system_error(':SYST:ERR -102,"Syntax error"')

    assert code == -102
    assert message == "Syntax error"


def test_response_payload_does_not_strip_bare_error_payload() -> None:
    assert pace5000.Pace5000._response_payload('0,"No error"') == '0,"No error"'
    assert pace5000.Pace5000._response_payload('-102,"Syntax error"') == '-102,"Syntax error"'


def test_response_payload_still_strips_scpi_header() -> None:
    assert pace5000.Pace5000._response_payload(":SOUR:PRES 1000.0") == "1000.0"
    assert pace5000.Pace5000._response_payload(':SYST:ERR 0,"No error"') == '0,"No error"'
    assert pace5000.Pace5000._response_payload(":OUTP:STAT 1") == "1"
    assert pace5000.Pace5000._response_payload(":UNIT:PRES HPA") == "HPA"
    assert pace5000.Pace5000._response_payload(":SOUR:PRES:LEV:IMM:AMPL:VENT 2") == "2"
    assert pace5000.Pace5000._response_payload(":SENS:PRES:INL 1013.25,1") == "1013.25,1"


def test_parse_range_upper_hpa_accepts_hpa_suffixes() -> None:
    assert pace5000.Pace5000._parse_range_upper_hpa("1000HPA") == 1000.0
    assert pace5000.Pace5000._parse_range_upper_hpa("1000HPAA") == 1000.0
    assert pace5000.Pace5000._parse_range_upper_hpa("1000HPAG") == 1000.0


def test_parse_range_upper_hpa_accepts_headered_hpa_suffixes() -> None:
    assert pace5000.Pace5000._parse_range_upper_hpa(":SOUR:PRES:RANG 1000HPAA") == 1000.0
    assert pace5000.Pace5000._parse_range_upper_hpa(':SENS:PRES:RANG "1000HPAG"') == 1000.0


def test_parse_range_upper_hpa_keeps_existing_units() -> None:
    assert pace5000.Pace5000._parse_range_upper_hpa("1BARA") == 1000.0
    assert pace5000.Pace5000._parse_range_upper_hpa("1BARG") == 1000.0
    assert pace5000.Pace5000._parse_range_upper_hpa("1000MBARA") == 1000.0
    assert pace5000.Pace5000._parse_range_upper_hpa("1000MBARG") == 1000.0
    assert pace5000.Pace5000._parse_range_upper_hpa("100KPAA") == 1000.0
    assert pace5000.Pace5000._parse_range_upper_hpa("100KPAG") == 1000.0
    assert pace5000.Pace5000._parse_range_upper_hpa("2.00bara") == 2000.0
    assert pace5000.Pace5000._parse_range_upper_hpa("1.00barg") == 1000.0


def test_parse_range_upper_hpa_ignores_reference_suffix_for_scale() -> None:
    assert pace5000.Pace5000._parse_range_upper_hpa("1000HPAA") == pace5000.Pace5000._parse_range_upper_hpa("1000HPAG")
    assert pace5000.Pace5000._parse_range_upper_hpa("100KPAA") == pace5000.Pace5000._parse_range_upper_hpa("100KPAG")
    assert pace5000.Pace5000._parse_range_upper_hpa("1BARA") == pace5000.Pace5000._parse_range_upper_hpa("1BARG")


def test_parse_range_upper_hpa_barometer_returns_none() -> None:
    assert pace5000.Pace5000._parse_range_upper_hpa("BAROMETER") is None


def test_drain_system_errors_stops_on_bare_no_error(monkeypatch) -> None:
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
            if cmd == ":SYST:ERR?":
                return '0,"No error"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.drain_system_errors() == []
    assert dev.ser.queries == [":SYST:ERR?"]


def test_get_vent_elapsed_time_and_protective_states_parse_queries(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.responses = iter(["7.25", "ENABLED", "DISABLED"])

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            return next(self.responses)

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_vent_elapsed_time_s() == 7.25
    assert dev.get_vent_over_range_protect_state() == "ENABLED"
    assert dev.get_vent_power_up_protect_state() == "DISABLED"


def test_detect_profile_old_from_ge_druck_identity(monkeypatch) -> None:
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
            if cmd == ":INST:MOD?":
                return '-113,"Undefined header"'
            if cmd == ":SYST:ECHO?":
                return '-113,"Undefined header"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_OLD_PACE5000
    assert dev.supports_sens_pres_cont() is False
    assert dev.ser.queries == ["*IDN?"]


def test_detect_profile_pace5000e_from_model(monkeypatch) -> None:
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
            if cmd == "*IDN?":
                return "PACE5000E,Controller,123456,03.01.00"
            if cmd == ":INST:MOD?":
                return "PACE5000E"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_PACE5000E
    assert dev.vent_terminal_statuses() == [
        pace5000.Pace5000.VENT_STATUS_IDLE,
        pace5000.Pace5000.VENT_STATUS_ABORTED,
    ]


def test_detect_profile_retries_after_initial_unknown_then_old_pace5000(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.queries = []
            self.responses = {
                "*IDN?": ["", "GE Druck,Pace5000 User Interface,3213201,02.00.07"],
                ":INST:MOD?": ["", ""],
                ":SYST:ECHO?": ["", ""],
            }

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            self.queries.append(cmd)
            values = self.responses.get(cmd, [""])
            if len(values) > 1:
                return values.pop(0)
            return values[0] if values else ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_UNKNOWN
    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_OLD_PACE5000


def test_detect_profile_retries_after_initial_unknown_then_pace5000e(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.queries = []
            self.responses = {
                "*IDN?": ["", "PACE5000E,Controller,123456,03.01.00"],
                ":INST:MOD?": ["", "PACE5000E"],
                ":SYST:ECHO?": ["", ""],
            }

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            self.queries.append(cmd)
            values = self.responses.get(cmd, [""])
            if len(values) > 1:
                return values.pop(0)
            return values[0] if values else ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_UNKNOWN
    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_PACE5000E


def test_detect_profile_unknown_does_not_permanently_cache_unknown(monkeypatch) -> None:
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
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_UNKNOWN
    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_UNKNOWN
    assert dev.ser.queries.count("*IDN?") == 2
    assert dev.ser.queries.count(":INST:MOD?") == 2
    assert dev.ser.queries.count(":SYST:ECHO?") == 2


def test_detect_profile_confirmed_old_pace5000_is_cached(monkeypatch) -> None:
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
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_OLD_PACE5000
    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_OLD_PACE5000
    assert dev.ser.queries == ["*IDN?"]


def test_detect_profile_confirmed_pace5000e_is_cached(monkeypatch) -> None:
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
                return "PACE5000E,Controller,123456,03.01.00"
            if cmd == ":INST:MOD?":
                return "PACE5000E"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_PACE5000E
    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_PACE5000E
    assert dev.ser.queries == ["*IDN?", ":INST:MOD?"]


def test_detect_profile_refresh_forces_reprobe(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.queries = []
            self.responses = {
                "*IDN?": [
                    "GE Druck,Pace5000 User Interface,3213201,02.00.07",
                    "PACE5000E,Controller,123456,03.01.00",
                ],
                ":INST:MOD?": ["PACE5000E"],
                ":SYST:ECHO?": ["", ""],
            }

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            self.queries.append(cmd)
            values = self.responses.get(cmd, [""])
            if len(values) > 1:
                return values.pop(0)
            return values[0] if values else ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_OLD_PACE5000
    assert dev.detectProfile(refresh=True) == pace5000.Pace5000.PROFILE_PACE5000E
    assert dev.ser.queries == ["*IDN?", "*IDN?", ":INST:MOD?"]


def test_unknown_profile_keeps_sens_pres_cont_conservative(monkeypatch) -> None:
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
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_UNKNOWN
    assert dev.supports_sens_pres_cont() is False
    assert dev._supports_sens_pres_cont is None


def test_vent_status_description_retries_unknown_profile(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.queries = []
            self.responses = {
                "*IDN?": ["", "GE Druck,Pace5000 User Interface,3213201,02.00.07"],
                ":INST:MOD?": ["", ""],
                ":SYST:ECHO?": ["", ""],
            }

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            self.queries.append(cmd)
            values = self.responses.get(cmd, [""])
            if len(values) > 1:
                return values.pop(0)
            return values[0] if values else ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    description = dev.describe_vent_status(2)

    assert description["profile"] == pace5000.Pace5000.PROFILE_OLD_PACE5000
    assert description["classification"] == "completed_latched"
    assert description["text"] == "completed"


def test_old_profile_optional_probe_errors_are_drained_before_formal_write(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.err_responses = iter(
                [
                    ':SYST:ERR 0,"No error"',
                    ':SYST:ERR 0,"No error"',
                    ':SYST:ERR 0,"No error"',
                ]
            )

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.writes.append(data)

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":INST:MOD?":
                return ""
            if cmd == ":SYST:ERR?":
                return next(self.err_responses)
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_OLD_PACE5000
    dev.get_device_model()
    assert dev.sendAndCheckError(":OUTP:STAT 1") == ':SYST:ERR 0,"No error"'


def test_read_pressure_old_profile_uses_cached_in_limits_without_cont(monkeypatch) -> None:
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
            if cmd == ":INST:MOD?":
                return '-113,"Undefined header"'
            if cmd == ":SENS:PRES:INL?":
                return ":SENS:PRES:INL 1000.5, 1"
            if cmd == ":SYST:ERR?":
                return ':SYST:ERR 0,"No error"'
            if cmd == ":SENS:PRES:CONT?":
                raise AssertionError("OLD profile must not query :SENS:PRES:CONT?")
            if cmd == ":SENS:PRES?":
                raise AssertionError("cached in-limits pressure should be used first")
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_OLD_PACE5000
    assert dev.get_in_limits() == (1000.5, 1)
    assert dev.read_pressure() == 1000.5
    assert ":SENS:PRES:CONT?" not in dev.ser.queries


class _CacheInvalidationSerialDevice:
    def __init__(self, *args, **kwargs):
        self.queries = []
        self.writes = []
        self.output_state = 0
        self.isolation_state = 1
        self.vent_state = 0
        self.output_mode = "ACT"
        self.unit = "HPA"
        self.range_name = "1600HPAG"
        self.setpoint = 1000.0
        self.inl_responses = [(1000.5, 1)]

    def open(self):
        return None

    def close(self):
        return None

    def write(self, data: str):
        text = data.strip()
        upper = text.upper()
        self.writes.append(text)
        if upper.startswith(":OUTP:STAT "):
            self.output_state = int(float(text.split()[-1]))
        elif upper.startswith(":OUTP:ISOL:STAT "):
            self.isolation_state = int(float(text.split()[-1]))
        elif upper.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT "):
            self.vent_state = int(float(text.split()[-1]))
        elif upper.startswith(":OUTP:MODE "):
            self.output_mode = text.split()[-1].upper()
        elif upper.startswith(":UNIT:PRES "):
            self.unit = text.split()[-1].upper()
        elif upper.startswith(':SOUR:PRES:RANG "'):
            self.range_name = text.split('"')[1]
        elif upper.startswith(":SOUR:PRES ") and not upper.endswith("?"):
            self.setpoint = float(text.split()[-1])

    def query(self, data: str) -> str:
        cmd = data.strip().upper()
        self.queries.append(cmd)
        if cmd == ":SYST:ERR?":
            return ':SYST:ERR 0,"No error"'
        if cmd == ":UNIT:PRES?":
            return f":UNIT:PRES {self.unit}"
        if cmd == ":OUTP:STAT?":
            return f":OUTP:STAT {self.output_state}"
        if cmd == ":OUTP:ISOL:STAT?":
            return f":OUTP:ISOL:STAT {self.isolation_state}"
        if cmd == ":OUTP:MODE?":
            return f":OUTP:MODE {self.output_mode}"
        if cmd == ":SOUR:PRES?":
            return f":SOUR:PRES {self.setpoint}"
        if cmd == ":SOUR:PRES:RANG?":
            return f':SOUR:PRES:RANG "{self.range_name}"'
        if cmd == ":INST:CAT:ALL?":
            return ':INST:CAT:ALL "1600HPAG","BAROMETER"'
        if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
            return f":SOUR:PRES:LEV:IMM:AMPL:VENT {self.vent_state}"
        if cmd == ":SENS:PRES?":
            return ""
        if cmd == ":MEAS:PRES?":
            return ""
        if cmd == ":SENS:PRES:INL?":
            pressure, flag = self.inl_responses.pop(0) if self.inl_responses else (self.setpoint, 1)
            return f":SENS:PRES:INL {pressure}, {flag}"
        return ""

    def readline(self) -> str:
        return ""


def test_set_pressure_invalidates_in_limits_cache(monkeypatch) -> None:
    monkeypatch.setattr(pace5000, "SerialDevice", _CacheInvalidationSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.ser.inl_responses = [(1000.5, 1), (500.0, 1)]
    assert dev.get_in_limits() == (1000.5, 1)

    dev.set_setpoint(500.0)

    assert dev._last_in_limits_pressure_hpa is None
    assert dev._last_in_limits_flag is None
    assert dev._last_in_limits_invalidation_reason == "set_setpoint:500.0"
    assert dev.read_pressure() == 500.0
    assert dev.ser.queries.count(":SENS:PRES:INL?") == 2


def test_output_state_change_invalidates_in_limits_cache(monkeypatch) -> None:
    monkeypatch.setattr(pace5000, "SerialDevice", _CacheInvalidationSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_in_limits() == (1000.5, 1)

    dev.set_output(True)

    assert dev._last_in_limits_pressure_hpa is None
    assert dev._last_in_limits_invalidation_reason == "set_output:1"
    assert dev._in_limits_cache_is_current() is False


def test_vent_command_invalidates_in_limits_cache(monkeypatch) -> None:
    monkeypatch.setattr(pace5000, "SerialDevice", _CacheInvalidationSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_in_limits() == (1000.5, 1)

    dev.vent(True)

    assert dev._last_in_limits_pressure_hpa is None
    assert dev._last_in_limits_invalidation_reason == "vent:1"
    assert dev._in_limits_cache_is_current() is False


def test_pressure_read_after_setpoint_does_not_use_stale_in_limits(monkeypatch) -> None:
    monkeypatch.setattr(pace5000, "SerialDevice", _CacheInvalidationSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.ser.inl_responses = [(1000.5, 1), (499.2, 0)]
    assert dev.get_in_limits() == (1000.5, 1)

    dev.set_setpoint(500.0)

    assert dev.read_pressure() == 499.2
    assert dev.ser.queries.count(":SENS:PRES:INL?") == 2


def test_in_limits_cache_is_not_reused_across_pressure_targets(monkeypatch) -> None:
    monkeypatch.setattr(pace5000, "SerialDevice", _CacheInvalidationSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.set_setpoint(1000.0)
    dev.ser.inl_responses = [(1000.0, 1)]
    assert dev.get_in_limits() == (1000.0, 1)

    dev.set_setpoint(500.0)
    dev.ser.inl_responses = [(495.0, 0)]

    assert dev.get_in_limits() == (495.0, 0)
    assert dev._last_in_limits_flag == 0
    assert dev._last_in_limits_setpoint_hpa == 500.0


def test_in_limits_cache_invalidation_records_reason(monkeypatch) -> None:
    monkeypatch.setattr(pace5000, "SerialDevice", _CacheInvalidationSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.get_in_limits() == (1000.5, 1)
    dev.vent(False)
    dev.ser.inl_responses = [(1000.0, 1)]

    status = dev.status()

    assert status["in_limits_cache_invalidation_reason"] == "vent:0"
    assert status["in_limits_cache_generation"] >= 1
    assert status["in_limits_cache_invalidation_count"] >= 1


def test_read_pressure_old_profile_falls_back_to_sens_pres_without_cont(monkeypatch) -> None:
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
            if cmd == ":INST:MOD?":
                return '-113,"Undefined header"'
            if cmd == ":SENS:PRES?":
                return ":SENS:PRES 1009.5"
            if cmd == ":SYST:ERR?":
                return ':SYST:ERR 0,"No error"'
            if cmd == ":SENS:PRES:CONT?":
                raise AssertionError("OLD profile must not query :SENS:PRES:CONT?")
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.detectProfile() == pace5000.Pace5000.PROFILE_OLD_PACE5000
    assert dev.read_pressure() == 1009.5
    assert ":SENS:PRES:CONT?" not in dev.ser.queries


def test_send_and_check_error_raises_on_nonzero_system_error(monkeypatch) -> None:
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
            if cmd == ":SYST:ERR?":
                return ':SYST:ERR -222,"Data out of range"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    with pytest.raises(RuntimeError, match="PACE_COMMAND_ERROR\\(command=:OUTP:STAT 1,code=-222"):
        dev.sendAndCheckError(":OUTP:STAT 1")


def test_send_and_check_error_accepts_bare_no_error(monkeypatch) -> None:
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
            if cmd == ":SYST:ERR?":
                return '0,"No error"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.sendAndCheckError(":OUTP:STAT 1") == '0,"No error"'
    assert any(":OUTP:STAT 1" in write for write in dev.ser.writes)


def test_send_and_check_error_raises_on_bare_syntax_error(monkeypatch) -> None:
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
            if cmd == ":SYST:ERR?":
                return '-102,"Syntax error"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    with pytest.raises(RuntimeError, match="PACE_COMMAND_ERROR\\(command=:OUTP:STAT 1,code=-102"):
        dev.sendAndCheckError(":OUTP:STAT 1")


def test_consume_optional_query_error_accepts_bare_optional_errors(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.responses = iter(
                [
                    '-102,"Syntax error"',
                    '-113,"Undefined header"',
                    '0,"No error"',
                ]
            )

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            self.last_write = data

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == ":SYST:ERR?":
                return next(self.responses)
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev._consume_optional_query_error(":INST:MOD?") == '0,"No error"'


def test_select_control_range_requires_exact_catalog_token(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.range_name = "1200HPAG"

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            text = data.strip()
            if text.upper().startswith(':SOUR:PRES:RANG "'):
                self.range_name = text.split('"', 2)[1]

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == ":INST:CAT:ALL?":
                return ':INST:CAT:ALL "1200HPAG","1600HPAG","BAROMETER"'
            if cmd == ":SOUR:PRES:RANG?":
                return f':SOUR:PRES:RANG "{self.range_name}"'
            if cmd == ":SYST:ERR?":
                return ':SYST:ERR 0,"No error"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.selectControlRange("1600HPAG") == "1600HPAG"
    with pytest.raises(RuntimeError, match="CONTROL_RANGE_NOT_IN_CATALOG"):
        dev.selectControlRange("1600hpag_extra")


def test_select_control_range_skips_write_when_current_range_already_matches(monkeypatch) -> None:
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
            if cmd == ":INST:CAT:ALL?":
                return ':INST:CAT:ALL "2.00bara","BAROMETER"'
            if cmd == ":SOUR:PRES:RANG?":
                return ':SOUR:PRES:RANG "2.00bara"'
            if cmd == ":SYST:ERR?":
                return ':SYST:ERR 0,"No error"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.selectControlRange("2.00bara") == "2.00bara"
    assert not any(':SOUR:PRES:RANG "2.00bara"' in write for write in dev.ser.writes)


def test_set_unit_skips_write_when_current_unit_already_matches(monkeypatch) -> None:
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
            if cmd == ":UNIT:PRES?":
                return ":UNIT:PRES HPA"
            if cmd == ":SYST:ERR?":
                return ':SYST:ERR 0,"No error"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.setUnit("HPA") == "HPA"
    assert not any(":UNIT:PRES HPA" in write for write in dev.ser.writes)


def test_set_in_limits_skips_writes_when_current_settings_already_match(monkeypatch) -> None:
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
            if cmd == ":SOUR:PRES:INL?":
                return ":SOUR:PRES:INL 0.0200000"
            if cmd == ":SOUR:PRES:INL:TIME?":
                return ":SOUR:PRES:INL:TIME 10"
            if cmd == ":SYST:ERR?":
                return ':SYST:ERR 0,"No error"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.set_in_limits(0.02, 10.0)

    assert not any(":SOUR:PRES:INL 0.02" in write for write in dev.ser.writes)
    assert not any(":SOUR:PRES:INL:TIME 10.0" in write for write in dev.ser.writes)


def test_set_output_and_isolation_skip_redundant_writes(monkeypatch) -> None:
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
            if cmd == ":OUTP:STAT?":
                return ":OUTP:STAT 0"
            if cmd == ":OUTP:ISOL:STAT?":
                return ":OUTP:ISOL:STAT 1"
            if cmd == ":OUTP:MODE?":
                return ":OUTP:MODE ACT"
            if cmd == ":SYST:ERR?":
                return ':SYST:ERR 0,"No error"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.set_output(False)
    dev.set_isolation_open(True)
    dev.set_output_mode_active()

    assert not any(":OUTP:STAT 0" in write for write in dev.ser.writes)
    assert not any(":OUTP:ISOL:STAT 1" in write for write in dev.ser.writes)
    assert not any(":OUTP:MODE ACT" in write for write in dev.ser.writes)


def test_set_point_and_wait_stable_records_stable_state(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.output_state = 0
            self.setpoint = 1000.0
            self.inl_reads = 0
            self.queries = []

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            text = data.strip()
            upper = text.upper()
            if upper.startswith(":OUTP:STAT "):
                self.output_state = int(float(text.split()[-1]))
            if upper.startswith(":SOUR:PRES ") and not upper.endswith("?"):
                self.setpoint = float(text.split()[-1])

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            self.queries.append(cmd)
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":UNIT:PRES?":
                return ":UNIT:PRES HPA"
            if cmd == ":OUTP:STAT?":
                return f":OUTP:STAT {self.output_state}"
            if cmd == ":OUTP:MODE?":
                return ":OUTP:MODE ACT"
            if cmd == ":SOUR:PRES?":
                return f":SOUR:PRES {self.setpoint}"
            if cmd == ":SOUR:PRES:RANG?":
                return ':SOUR:PRES:RANG "1600HPAG"'
            if cmd == ":INST:CAT:ALL?":
                return ':INST:CAT:ALL "1600HPAG","BAROMETER"'
            if cmd == ":SENS:PRES:INL?":
                self.inl_reads += 1
                flag = 1 if self.inl_reads >= 5 else 0
                value = self.setpoint if flag else self.setpoint - 0.4
                return f":SENS:PRES:INL {value}, {flag}"
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
            if cmd == ":SYST:ERR?":
                return ':SYST:ERR 0,"No error"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    result = dev.setPointAndWaitStable(
        1000.0,
        tolerance_hpa=0.5,
        timeout_s=1.0,
        poll_s=0.0,
        stable_count_required=5,
        stable_hold_s=99.0,
        control_range="1600HPAG",
    )

    assert result["ok"] is True
    assert result["control_range"] == "1600HPAG"
    assert result["setpoint_readback_hpa"] == 1000.0
    assert result["control_pressure_hpa"] == 1000.0
    assert result["stable_count"] >= 5
    assert dev.ser.queries.count(":SENS:PRES:INL?") >= 5
    assert dev.ser.queries.count(":SENS:PRES:CONT?") == 0
    assert dev.ser.queries.count(":OUTP:STAT?") <= 4
    assert dev.ser.queries.count(":SOUR:PRES?") <= 3


def test_set_point_and_wait_stable_timeout_calls_safe_stop(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.output_state = 1
            self.setpoint = 1000.0

        def open(self):
            return None

        def close(self):
            return None

        def write(self, data: str):
            text = data.strip()
            upper = text.upper()
            if upper.startswith(":SOUR:PRES ") and not upper.endswith("?"):
                self.setpoint = float(text.split()[-1])

        def query(self, data: str) -> str:
            cmd = data.strip().upper()
            if cmd == "*IDN?":
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":UNIT:PRES?":
                return ":UNIT:PRES HPA"
            if cmd == ":OUTP:STAT?":
                return f":OUTP:STAT {self.output_state}"
            if cmd == ":SOUR:PRES?":
                return f":SOUR:PRES {self.setpoint}"
            if cmd == ":SENS:PRES:INL?":
                return f":SENS:PRES:INL {self.setpoint - 5.0}, 0"
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
            if cmd == ":SYST:ERR?":
                return ':SYST:ERR 0,"No error"'
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)
    safe_stop_calls = []

    def _fake_safe_stop(**kwargs):
        safe_stop_calls.append(dict(kwargs))
        return {"profile": pace5000.Pace5000.PROFILE_OLD_PACE5000, "vent_status": 0}

    monkeypatch.setattr(dev, "safe_stop", _fake_safe_stop)

    with pytest.raises(RuntimeError, match="SETPOINT_STABILITY_TIMEOUT"):
        dev.setPointAndWaitStable(
            1000.0,
            tolerance_hpa=0.5,
            timeout_s=0.2,
            poll_s=0.0,
            stable_count_required=2,
            stable_hold_s=0.0,
            select_range=False,
        )

    assert safe_stop_calls and safe_stop_calls[0]["vent_on"] is True
