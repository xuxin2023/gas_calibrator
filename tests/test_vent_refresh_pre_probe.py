import importlib.util
import json
from pathlib import Path


def _load_module():
    path = Path("scripts/vent_refresh_pre_probe.py").resolve()
    spec = importlib.util.spec_from_file_location("vent_refresh_pre_probe", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakePace:
    def __init__(self, *, syst_err=':SYST:ERR -102,"Syntax error"', fail_step: str = "", event_log=None):
        self.calls = []
        self.writes = []
        self.syst_err = syst_err
        self.fail_step = fail_step
        self.event_log = event_log if event_log is not None else []

    def _record(self, entry):
        self.calls.append(entry)
        self.event_log.append(("pace",) + entry)

    def open(self):
        self._record(("open",))

    def close(self):
        self._record(("close",))

    def write(self, command):
        self.writes.append(command)
        self._record(("write", command))

    def vent(self, on=True):
        self._record(("vent", bool(on)))

    def get_vent_status(self):
        self._record(("get_vent_status",))
        if self.fail_step == "pace_vent_status":
            raise RuntimeError("NO_RESPONSE")
        return 0

    def get_output_state(self):
        self._record(("get_output_state",))
        if self.fail_step == "pace_outp_state":
            raise RuntimeError("NO_RESPONSE")
        return 0

    def get_isolation_state(self):
        self._record(("get_isolation_state",))
        if self.fail_step == "pace_isol_state":
            raise RuntimeError("NO_RESPONSE")
        return 1

    def read_pressure(self):
        self._record(("read_pressure",))
        if self.fail_step == "pace_read_pressure":
            raise RuntimeError("NO_RESPONSE_OR_PARSE")
        return 1018.0155

    def query(self, command):
        self._record(("query", command))
        if self.fail_step == "pace_device_idn" and command == "*IDN?":
            raise RuntimeError("NO_RESPONSE")
        if self.fail_step == "pace_syst_err" and command == ":SYST:ERR?":
            raise RuntimeError("NO_RESPONSE")
        responses = {
            "*IDN?": "*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07",
            ":SYST:ERR?": self.syst_err,
        }
        return responses[command]


class _FakeGauge:
    def __init__(self, *, fail_step: str = "", event_log=None):
        self.calls = []
        self.fail_step = fail_step
        self.event_log = event_log if event_log is not None else []

    def _record(self, entry):
        self.calls.append(entry)
        self.event_log.append(("gauge",) + entry)

    def open(self):
        self._record(("open",))

    def close(self):
        self._record(("close",))

    def read_pressure_fast(self):
        self._record(("read_pressure_fast",))
        if self.fail_step == "gauge_read_pressure_fast":
            raise RuntimeError("NO_RESPONSE")
        return 1017.512


def _read_json(path_text: str):
    return json.loads(Path(path_text).read_text(encoding="utf-8"))


def test_preprobe_uses_new_sequence_and_delay(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()
    event_log = []
    pace = _FakePace(syst_err=':SYST:ERR 0,"No error"', event_log=event_log)
    gauge = _FakeGauge(event_log=event_log)
    sleep_calls = []
    monkeypatch.setattr(module.time, "sleep", lambda value: sleep_calls.append(value))

    summary = module.run_preprobe(
        config_path=Path("configs/default_config.json").resolve(),
        output_root=tmp_path,
        delay_s=0.5,
        pace_factory=lambda settings: pace,
        gauge_factory=lambda settings: gauge,
    )

    trace_rows = _read_json(summary["probe_trace_rows_json"])
    assert [row["step"] for row in trace_rows] == [
        "open_pace",
        "delay_after_open_pace",
        "open_gauge",
        "delay_after_open_gauge",
        "pace_vent_status",
        "pace_outp_state",
        "pace_isol_state",
        "pace_read_pressure",
        "gauge_read_pressure_fast",
        "pace_device_idn",
        "pace_syst_err",
        "close_gauge",
        "close_pace",
    ]
    assert [row["delay_s"] for row in trace_rows if row["step"].startswith("delay_after_")] == [0.5, 0.5]
    assert sleep_calls == [0.5, 0.5]
    assert summary["vent_status"] == 0
    assert summary["outp_state"] == 0
    assert summary["isol_state"] == 1
    assert pace.calls[:6] == [
        ("open",),
        ("get_vent_status",),
        ("get_output_state",),
        ("get_isolation_state",),
        ("read_pressure",),
        ("query", "*IDN?"),
    ]
    assert gauge.calls == [("open",), ("read_pressure_fast",), ("close",)]


def test_syst_err_minus_102_is_warning_only(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()
    monkeypatch.setattr(module.time, "sleep", lambda value: None)

    summary = module.run_preprobe(
        config_path=Path("configs/default_config.json").resolve(),
        output_root=tmp_path,
        delay_s=0.5,
        pace_factory=lambda settings: _FakePace(),
        gauge_factory=lambda settings: _FakeGauge(),
    )

    assert summary["probe_pass"] is True
    assert summary["probe_fail_due_to_comm"] is False
    assert summary["probe_warning_only"] is True
    assert any("syst_err_warning" in item for item in summary["warnings"])


def test_open_close_order_is_fixed(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()
    event_log = []
    monkeypatch.setattr(module.time, "sleep", lambda value: None)

    module.run_preprobe(
        config_path=Path("configs/default_config.json").resolve(),
        output_root=tmp_path,
        delay_s=0.5,
        pace_factory=lambda settings: _FakePace(syst_err=':SYST:ERR 0,"No error"', event_log=event_log),
        gauge_factory=lambda settings: _FakeGauge(event_log=event_log),
    )

    assert event_log[0] == ("pace", "open")
    assert event_log[1] == ("gauge", "open")
    assert event_log[-2] == ("gauge", "close")
    assert event_log[-1] == ("pace", "close")


def test_failure_exposes_specific_failing_step(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()
    pace = _FakePace(syst_err=':SYST:ERR 0,"No error"', fail_step="pace_outp_state")
    gauge = _FakeGauge()
    monkeypatch.setattr(module.time, "sleep", lambda value: None)

    summary = module.run_preprobe(
        config_path=Path("configs/default_config.json").resolve(),
        output_root=tmp_path,
        delay_s=0.5,
        pace_factory=lambda settings: pace,
        gauge_factory=lambda settings: gauge,
    )

    assert summary["probe_pass"] is False
    assert summary["probe_fail_due_to_comm"] is True
    assert summary["probe_warning_only"] is False
    assert summary["first_failing_step"] == "pace_outp_state"
    assert ("read_pressure_fast",) not in gauge.calls


def test_preprobe_never_sends_raw_vent_zero_or_runtime_writes(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()
    pace = _FakePace(syst_err=':SYST:ERR 0,"No error"')
    gauge = _FakeGauge()
    monkeypatch.setattr(module.time, "sleep", lambda value: None)

    summary = module.run_preprobe(
        config_path=Path("configs/default_config.json").resolve(),
        output_root=tmp_path,
        delay_s=0.5,
        pace_factory=lambda settings: pace,
        gauge_factory=lambda settings: gauge,
    )

    assert summary["probe_pass"] is True
    assert pace.writes == []
    assert not any(call[0] == "vent" for call in pace.calls)
