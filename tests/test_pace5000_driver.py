import time

import pytest

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


def test_legacy_ge_druck_identity_treats_completed_vent_status_as_latched_not_ready(monkeypatch) -> None:
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

    assert dev.vent_status_allows_control(dev.get_vent_status()) is False
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
                return "02.00.07"
            if cmd.startswith(":SOUR:PRES:LEV:IMM:AMPL:VENT?"):
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 3"
            raise AssertionError(f"unexpected query: {data}")

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.vent_status_allows_control(dev.get_vent_status()) is False
    assert dev.vent_terminal_statuses() == [
        pace5000.Pace5000.VENT_STATUS_IDLE,
        pace5000.Pace5000.VENT_STATUS_ABORTED,
    ]


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
    assert dev.vent_terminal_statuses() == [
        pace5000.Pace5000.VENT_STATUS_IDLE,
        pace5000.Pace5000.VENT_STATUS_ABORTED,
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


def test_enter_atmosphere_mode_opens_isolation_and_clears_completed_vent_latch(monkeypatch) -> None:
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

    assert status == pace5000.Pace5000.VENT_STATUS_IDLE
    assert any(":OUTP 0" in w for w in dev.ser.writes)
    assert any(":OUTP:ISOL:STAT 1" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in w for w in dev.ser.writes)
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in w for w in dev.ser.writes)


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
                return "GE Druck,Pace5000 User Interface,3213201,02.00.07"
            if cmd == ":INST:VERS?":
                return "02.00.07"
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

    assert any(":OUTP 0" in w for w in dev.ser.writes)
    assert any(":OUTP:ISOL:STAT 1" in w for w in dev.ser.writes)
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

    assert status == pace5000.Pace5000.VENT_STATUS_IDLE
    assert dev.is_atmosphere_hold_active() is False
    assert dev.stop_atmosphere_hold() is True
    assert sum(1 for w in dev.ser.writes if ":SOUR:PRES:LEV:IMM:AMPL:VENT 1" in w) == 1
    assert sum(1 for w in dev.ser.writes if ":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in w) == 1


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
            cmd = data.strip().upper()
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
            if cmd == ":OUTP:MODE?":
                return ":OUTP:MODE ACT"
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    dev.enable_control_output()

    assert any(":OUTP:MODE ACT" in w for w in dev.ser.writes)
    assert any(":OUTP 1" in w for w in dev.ser.writes)


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

    assert not any(":OUTP 1" in w for w in dev.ser.writes)


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


def test_set_output_enabled_verified_waits_for_output_off(monkeypatch) -> None:
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
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)

    assert dev.set_output_enabled_verified(False) == 0
    assert any(":OUTP 0" in write for write in dev.ser.writes)


def test_set_output_isolated_verified_maps_isolated_true_to_closed_path(monkeypatch) -> None:
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
            if cmd == ":OUTP:ISOL:STAT?":
                return ":OUTP:ISOL:STAT 0"
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


def test_clear_completed_vent_latch_if_present_keeps_legacy_watchlist_status_3_as_not_cleared(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.writes = []
            self.responses = iter(
                [
                    ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
                    ":SOUR:PRES:LEV:IMM:AMPL:VENT 3",
                    ":SOUR:PRES:LEV:IMM:AMPL:VENT 3",
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
            if cmd == ":SOUR:PRES:LEV:IMM:AMPL:VENT?":
                return next(self.responses)
            return ""

        def readline(self) -> str:
            return ""

    monkeypatch.setattr(pace5000, "SerialDevice", FakeSerialDevice)
    dev = pace5000.Pace5000("COM1", 9600)
    dev._device_identity_probed = True
    dev._legacy_vent_status_model = True
    dev._instrument_version_probed = True
    dev._instrument_version = "02.00.07"

    result = dev.clear_completed_vent_latch_if_present(timeout_s=0.2, poll_s=0.0)

    assert result["before_status"] == 2
    assert result["clear_attempted"] is True
    assert result["after_status"] == 3
    assert result["cleared"] is False
    assert result["command"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
    assert result["vent3_watchlist_observed"] is True
    assert any(":SOUR:PRES:LEV:IMM:AMPL:VENT 0" in write for write in dev.ser.writes)


def test_diagnostic_status_collects_best_effort_aux_fields(monkeypatch) -> None:
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
    assert status["control_pressure_hpa"] == 1000.7
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
