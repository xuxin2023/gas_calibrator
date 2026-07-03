import csv
import json
from pathlib import Path

from gas_calibrator.tools import pace_output_prearm_no_outp as prearm


def _base_config() -> dict:
    return {
        "devices": {
            "pressure_controller": {
                "enabled": True,
                "port": "COM23",
                "baud": 9600,
                "timeout": 1.0,
                "line_ending": "LF",
            },
            "relay": {"enabled": True, "port": "COM20", "baud": 38400, "addr": 1},
            "relay_8": {"enabled": True, "port": "COM21", "baud": 38400, "addr": 1},
        },
        "workflow": {
            "collect_only": True,
            "pressure": {"no_outp_transition_mode": True},
            "startup_pressure_sensor_calibration": {"enabled": False, "apply_write": False},
            "postrun_corrected_delivery": {
                "enabled": False,
                "write_devices": False,
                "write_pressure_coefficients": False,
            },
        },
        "coefficients": {"enabled": False, "sencos": {}},
        "paths": {"output_dir": "logs"},
    }


def _write_config(tmp_path: Path, cfg: dict) -> Path:
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    path = config_dir / "prearm.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    return path


class FakePace:
    instances = []

    def __init__(self, *args, io_logger=None, output_after=1, output_poll_values=None, **kwargs):
        self.io_logger = io_logger
        self.output_state = 0
        self.output_after = output_after
        self.output_poll_values = list(output_poll_values or [])
        self.outp1_sent = False
        self.closed = False
        self.setpoint_calls = 0
        self.vent_calls = []
        self.enable_control_output_called = False
        self.output_mode_active_calls = 0
        self.isolation_open_calls = 0
        self.wait_for_vent_idle_calls = 0
        FakePace.instances.append(self)

    def _log(self, direction, command=None, response=None):
        self.io_logger.log_io(
            port="COM23",
            device="pace5000",
            direction=direction,
            command=command,
            response=response,
        )

    def open(self):
        self._log("OPEN", "open")

    def close(self):
        self.closed = True
        self._log("CLOSE", "close")

    def read_pressure(self):
        self._log("TX", ":SENS:PRES:INL?")
        self._log("RX", response=":SENS:PRES:INL 1012.5, 0")
        return 1012.5

    def get_output_state(self):
        if self.outp1_sent and self.output_poll_values:
            self.output_state = int(self.output_poll_values.pop(0))
        self._log("TX", ":OUTP:STAT?")
        self._log("RX", response=f":OUTP:STAT {self.output_state}")
        return self.output_state

    def get_isolation_state(self):
        self._log("TX", ":OUTP:ISOL:STAT?")
        self._log("RX", response=":OUTP:ISOL:STAT 1")
        return 1

    def get_vent_status(self):
        self._log("TX", ":SOUR:PRES:LEV:IMM:AMPL:VENT?")
        self._log("RX", response=":SOUR:PRES:LEV:IMM:AMPL:VENT 2")
        return 2

    def set_isolation_open(self, is_open):
        self.isolation_open_calls += 1
        self._log("TX", f":OUTP:ISOL:STAT {1 if is_open else 0}")

    def wait_for_vent_idle(self, *args, **kwargs):
        self.wait_for_vent_idle_calls += 1
        self.get_vent_status()

    def set_output_mode_active(self):
        self.output_mode_active_calls += 1
        self._log("TX", ":OUTP:MODE ACT")

    def get_output_mode(self):
        self._log("TX", ":OUTP:MODE?")
        self._log("RX", response=":OUTP:MODE ACT")
        return "ACT"

    def set_output(self, on):
        self._log("TX", f":OUTP {1 if on else 0}")
        if on:
            self.outp1_sent = True
            if not self.output_poll_values:
                self.output_state = self.output_after
        else:
            self.output_state = 0

    def enable_control_output(self):
        self.enable_control_output_called = True
        self.set_isolation_open(True)
        self.wait_for_vent_idle()
        self.set_output_mode_active()
        self.get_output_mode()
        self.set_output(True)

    def set_setpoint(self, value):
        self.setpoint_calls += 1
        self._log("TX", f":SOUR:PRES:LEV:IMM:AMPL {value}")

    def vent(self, on=True):
        self.vent_calls.append(bool(on))
        self._log("TX", f":SOUR:PRES:LEV:IMM:AMPL:VENT {1 if on else 0}")


class FakePaceFallback(FakePace):
    enable_control_output = None


class FakePaceBadOutp0(FakePace):
    def enable_control_output(self):
        self.enable_control_output_called = True
        self.set_output(False)
        self.set_output(True)


class FakePaceBadVent(FakePace):
    def enable_control_output(self):
        self.enable_control_output_called = True
        self.vent(True)
        self.vent(False)
        self.set_output(True)


class FakePaceBadSetpoint(FakePace):
    def enable_control_output(self):
        self.enable_control_output_called = True
        self.set_setpoint(1100)
        self.set_output(True)


class FakeRelay:
    states_by_port = {"COM20": [False] * 16, "COM21": [False] * 8}
    instances = []

    def __init__(self, port, baudrate=38400, addr=1, io_logger=None):
        self.port = port
        self.io_logger = io_logger
        self.states = list(self.states_by_port[port])
        self.closed = False
        FakeRelay.instances.append(self)

    def _log(self, direction, command=None, response=None):
        self.io_logger.log_io(
            port=self.port,
            device="relay_controller",
            direction=direction,
            command=command,
            response=response,
        )

    def open(self):
        self._log("TX", "connect")
        self._log("RX", response="connected")

    def close(self):
        self.closed = True
        self._log("TX", "close")
        self._log("RX", response="closed")

    def read_coils(self, start=0, count=1):
        self._log("TX", f"read_coils({start},{count},addr=1)")
        out = self.states[start : start + count]
        self._log("RX", response=out)
        return out

    def set_valve(self, channel, open_):
        self.states[channel - 1] = bool(open_)
        self._log("TX", f"write_coil({channel - 1},{bool(open_)},addr=1)")
        self._log("RX", response="ok")


def _run(
    tmp_path: Path,
    cfg: dict,
    *,
    pace_factory=FakePace,
    relay_factory=FakeRelay,
    output_confirm_timeout_s=0.05,
    output_confirm_poll_s=0.001,
):
    FakePace.instances = []
    FakeRelay.instances = []
    FakeRelay.states_by_port = {"COM20": [False] * 16, "COM21": [False] * 8}
    config_path = _write_config(tmp_path, cfg)
    return prearm.run_prearm(
        config_path=config_path,
        confirm_route_closed=True,
        confirm_no_calibration_running=True,
        output_root=tmp_path / "logs" / "pace_output_prearm",
        pace_factory=pace_factory,
        relay_factory=relay_factory,
        cwd=Path(__file__).resolve().parents[1],
        output_confirm_timeout_s=output_confirm_timeout_s,
        output_confirm_poll_s=output_confirm_poll_s,
    )


def test_prearm_requires_no_outp_config(tmp_path):
    cfg = _base_config()
    cfg["workflow"]["pressure"]["no_outp_transition_mode"] = False

    code, summary = _run(tmp_path, cfg)

    assert code == 1
    assert summary["final_decision"] == prearm.BLOCKED_CONFIG


def test_prearm_requires_no_write(tmp_path):
    cfg = _base_config()
    cfg["coefficients"]["enabled"] = True

    code, summary = _run(tmp_path, cfg)

    assert code == 1
    assert summary["final_decision"] == prearm.BLOCKED_CONFIG
    assert "coefficients.enabled" in summary["no_write_issues"]


def test_prearm_refuses_or_baselines_when_route_not_closed(tmp_path):
    cfg = _base_config()
    config_path = _write_config(tmp_path, cfg)
    FakePace.instances = []
    FakeRelay.instances = []
    FakeRelay.states_by_port = {"COM20": [True] + [False] * 15, "COM21": [False] * 8}

    code, summary = prearm.run_prearm(
        config_path=config_path,
        confirm_route_closed=True,
        confirm_no_calibration_running=True,
        output_root=tmp_path / "logs" / "pace_output_prearm",
        pace_factory=FakePace,
        relay_factory=FakeRelay,
        cwd=Path(__file__).resolve().parents[1],
    )

    assert code == 1
    assert summary["final_decision"] == prearm.BLOCKED_ROUTE
    assert not FakePace.instances


def test_prearm_uses_enable_control_output_when_available(tmp_path):
    cfg = _base_config()

    code, summary = _run(tmp_path, cfg)

    assert code == 0
    assert summary["final_decision"] == prearm.FINAL_PASS
    assert summary["enable_control_output_used"] is True
    assert summary["fallback_output_sequence_used"] is False
    assert FakePace.instances[0].enable_control_output_called is True


def test_prearm_enable_control_output_sequence_allows_mode_active_and_isolation(tmp_path):
    cfg = _base_config()

    code, summary = _run(tmp_path, cfg)

    assert code == 0
    assert summary["outp1_sent_count"] == 1
    assert summary["outp0_sent_count"] == 0
    assert summary["vent0_sent_count"] == 0
    assert summary["vent1_sent_count"] == 0
    assert summary["setpoint_sent_count"] == 0
    assert summary["output_mode_active_sent_count"] == 1
    assert summary["isolation_open_sent_count"] == 1


def test_prearm_still_blocks_outp0(tmp_path):
    cfg = _base_config()

    code, summary = _run(tmp_path, cfg, pace_factory=FakePaceBadOutp0)

    assert code == 1
    assert summary["final_decision"] == prearm.BLOCKED_COMMAND_VIOLATION
    assert summary["outp0_sent_count"] == 1


def test_prearm_still_blocks_vent0_vent1(tmp_path):
    cfg = _base_config()

    code, summary = _run(tmp_path, cfg, pace_factory=FakePaceBadVent)

    assert code == 1
    assert summary["final_decision"] == prearm.BLOCKED_COMMAND_VIOLATION
    assert summary["vent0_sent_count"] == 1
    assert summary["vent1_sent_count"] == 1


def test_prearm_still_blocks_setpoint(tmp_path):
    cfg = _base_config()

    code, summary = _run(tmp_path, cfg, pace_factory=FakePaceBadSetpoint)

    assert code == 1
    assert summary["final_decision"] == prearm.BLOCKED_COMMAND_VIOLATION
    assert summary["setpoint_sent_count"] == 1


def test_prearm_fallback_sets_mode_active_then_output(tmp_path):
    cfg = _base_config()

    code, summary = _run(tmp_path, cfg, pace_factory=FakePaceFallback)

    assert code == 0
    assert summary["enable_control_output_used"] is False
    assert summary["fallback_output_sequence_used"] is True
    assert summary["output_mode_active_sent_count"] == 1
    assert summary["isolation_open_sent_count"] == 1
    assert summary["outp1_sent_count"] == 1
    assert FakePace.instances[0].output_mode_active_calls == 1


def test_prearm_polls_output_state_until_one(tmp_path):
    cfg = _base_config()

    def pace_factory(*args, **kwargs):
        return FakePace(*args, output_poll_values=[0, 0, 1], **kwargs)

    code, summary = _run(tmp_path, cfg, pace_factory=pace_factory)

    assert code == 0
    assert summary["final_decision"] == prearm.FINAL_PASS
    assert summary["output_confirmed_after_poll"] is True
    assert summary["output_state_poll_values"] == [0, 0, 1]
    assert summary["output_confirm_poll_count"] == 3


def test_prearm_does_not_fail_on_initial_output_zero(tmp_path):
    cfg = _base_config()

    def pace_factory(*args, **kwargs):
        return FakePace(*args, output_poll_values=[0, 1], **kwargs)

    code, summary = _run(tmp_path, cfg, pace_factory=pace_factory)

    assert code == 0
    assert summary["output_state_poll_values"][0] == 0
    assert summary["pace_output_after"] == 1


def test_prearm_passes_when_output_state_after_is_one(tmp_path):
    cfg = _base_config()

    code, summary = _run(tmp_path, cfg)

    assert code == 0
    assert summary["pace_output_before"] == 0
    assert summary["pace_output_after"] == 1


def test_prearm_fails_when_output_never_becomes_one(tmp_path):
    cfg = _base_config()

    def pace_factory(*args, **kwargs):
        return FakePace(*args, output_after=0, **kwargs)

    code, summary = _run(
        tmp_path,
        cfg,
        pace_factory=pace_factory,
        output_confirm_timeout_s=0.01,
        output_confirm_poll_s=0.001,
    )

    assert code == 1
    assert summary["final_decision"] == prearm.BLOCKED_OUTPUT
    assert summary["outp1_sent_count"] == 1
    assert summary["pace_output_after"] == 0
    assert summary["output_confirmed_after_poll"] is False


def test_prearm_records_output_poll_values(tmp_path):
    cfg = _base_config()

    def pace_factory(*args, **kwargs):
        return FakePace(*args, output_poll_values=[0, 1], **kwargs)

    code, summary = _run(tmp_path, cfg, pace_factory=pace_factory)

    assert code == 0
    assert summary["output_state_poll_values"] == [0, 1]
    assert summary["output_confirm_poll_count"] == 2
    assert summary["output_state_first_one_elapsed_s"] is not None
    assert summary["output_confirm_elapsed_s"] >= summary["output_state_first_one_elapsed_s"]


def test_prearm_does_not_send_second_outp1_by_default(tmp_path):
    cfg = _base_config()

    def pace_factory(*args, **kwargs):
        return FakePace(*args, output_poll_values=[0, 1], **kwargs)

    code, summary = _run(tmp_path, cfg, pace_factory=pace_factory)

    assert code == 0
    assert summary["outp1_sent_count"] == 1
    run_dir = Path(summary["run_dir"])
    with (run_dir / "prearm_io_log.csv").open(newline="", encoding="utf-8") as handle:
        outp1_commands = [
            row for row in csv.DictReader(handle)
            if row["direction"] == "TX" and row["command"].strip().upper().startswith(":OUTP 1")
        ]
    assert len(outp1_commands) == 1


def test_prearm_writes_summary_and_io_log(tmp_path):
    cfg = _base_config()

    code, summary = _run(tmp_path, cfg)

    run_dir = Path(summary["run_dir"])
    summary_path = run_dir / "prearm_summary.json"
    io_path = run_dir / "prearm_io_log.csv"
    assert code == 0
    assert summary_path.exists()
    assert io_path.exists()
    stored = json.loads(summary_path.read_text(encoding="utf-8"))
    assert stored["final_decision"] == prearm.FINAL_PASS
    with io_path.open(newline="", encoding="utf-8") as handle:
        commands = [row["command"] for row in csv.DictReader(handle)]
    assert ":OUTP 1" in commands


def test_prearm_does_not_start_calibration(tmp_path):
    cfg = _base_config()

    code, summary = _run(tmp_path, cfg)

    assert code == 0
    assert summary["calibration_path_started"] is False
    assert FakePace.instances[0].setpoint_calls == 0
    assert FakePace.instances[0].vent_calls == []


def test_prearm_records_not_real_acceptance(tmp_path):
    cfg = _base_config()

    code, summary = _run(tmp_path, cfg)

    assert code == 0
    assert summary["not_real_acceptance"] is True
    assert summary["real_primary_latest_refresh"] is False
    assert summary["allowed_prearm_only"] is True
