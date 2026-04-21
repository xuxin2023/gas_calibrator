from pathlib import Path

from gas_calibrator.tools import safe_stop
from gas_calibrator.tools.safe_stop import perform_safe_stop, perform_safe_stop_with_retries, validate_safe_stop_result


class _FakePace:
    PROFILE_OLD_PACE5000 = "OLD_PACE5000"
    PROFILE_PACE5000E = "PACE5000E"
    PROFILE_UNKNOWN = "UNKNOWN"

    def __init__(self, *, profile: str = PROFILE_OLD_PACE5000, helper_vent_command_sent: bool = True) -> None:
        self.calls = []
        self.profile = profile
        self.helper_vent_command_sent = helper_vent_command_sent
        self.classify_calls = []

    def safe_stop(self):
        self.calls.append("safe_stop")
        return {
            "profile": self.profile,
            "output_state": 0,
            "isolation_state": 1,
            "vent_status": 2,
            "vent_command_sent": self.helper_vent_command_sent,
            "system_error": ':SYST:ERR 0,"No error"',
        }

    def enter_atmosphere_mode(self):
        self.calls.append("enter_atmosphere_mode")

    def read_pressure(self):
        return 1010.5

    def query(self, cmd: str):
        mapping = {
            ":SOUR:PRES:LEV:IMM:AMPL:VENT?": ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
            ":OUTP:STAT?": ":OUTP:STAT 0",
            ":OUTP:ISOL:STAT?": ":OUTP:ISOL:STAT 1",
        }
        return mapping[cmd]

    def detect_profile(self):
        return self.profile

    @staticmethod
    def parse_vent_status_value(response):
        return int(str(response).strip().split()[-1])

    def classify_vent_status(self, status):
        value = self.parse_vent_status_value(status)
        self.classify_calls.append(value)
        if value == 0:
            return "idle"
        if value == 1:
            return "in_progress"
        if value == 2 and self.profile == self.PROFILE_OLD_PACE5000:
            return "completed_latched"
        if value == 2 and self.profile == self.PROFILE_PACE5000E:
            return "timed_out"
        if value == 3 and self.profile == self.PROFILE_PACE5000E:
            return "trapped_pressure"
        if value == 4 and self.profile == self.PROFILE_PACE5000E:
            return "aborted"
        return "unknown"

    def vent_status_text(self, status):
        value = self.parse_vent_status_value(status)
        if value == 2 and self.profile == self.PROFILE_OLD_PACE5000:
            return "completed"
        if value == 2 and self.profile == self.PROFILE_PACE5000E:
            return "timeout"
        if value == 0:
            return "idle"
        if value == 1:
            return "in_progress"
        return "unknown"


class _FakeDiagnosticBlockedPace(_FakePace):
    def enter_legacy_diagnostic_safe_vent_mode(self, action: str = "safe_stop"):
        self.calls.append(("diagnostic_safe_vent", action))
        return {
            "action": action,
            "legacy_identity": True,
            "ok": False,
            "recoverable": True,
            "reason": "legacy_safe_vent_blocked_for_test",
            "vent_command_sent": False,
            "profile": self.profile,
        }


class _FakeSuppressedSafeStopPace(_FakePace):
    def safe_stop(self):
        self.calls.append("safe_stop")
        return {
            "profile": self.profile,
            "output_state": 0,
            "isolation_state": 1,
            "vent_status": 2,
            "vent_command_sent": False,
            "reason": "adapter_safe_stop_did_not_send_vent_for_test",
            "system_error": ':SYST:ERR 0,"No error"',
        }


class _FakeRelay:
    def __init__(self) -> None:
        self.calls = []
        self.bits = []

    def set_valve(self, channel: int, open_: bool) -> None:
        self.calls.append((int(channel), bool(open_)))
        while len(self.bits) < channel:
            self.bits.append(False)
        self.bits[channel - 1] = bool(open_)

    def read_coils(self, start: int, count: int):
        return self.bits[start : start + count]


class _FakeChamber:
    def stop(self):
        return None

    def read_temp_c(self):
        return 20.0

    def read_rh_pct(self):
        return 99.0

    def read_run_state(self):
        return 0


class _FakeHgen:
    def __init__(self) -> None:
        self.wait_calls = []

    def safe_stop(self):
        return {
            "flow_off": "ok",
            "ctrl_off": "ok",
            "cool_off": "ok",
            "heat_off": "ok",
        }

    def wait_stopped(self, **kwargs):
        self.wait_calls.append(kwargs)
        return {"ok": True, "flow_lpm": 0.0, "max_flow_lpm": 0.05}

    def fetch_all(self):
        return {"raw": "demo", "data": {"Tc": 20.1, "Uw": 30.0}}


class _FakeGauge:
    def read_pressure(self):
        return 1009.9


def test_perform_safe_stop_uses_set_valve_and_verifies_states() -> None:
    relay = _FakeRelay()
    relay8 = _FakeRelay()
    hgen = _FakeHgen()
    result = perform_safe_stop(
        {
            "pace": _FakePace(),
            "relay": relay,
            "relay_8": relay8,
            "temp_chamber": _FakeChamber(),
            "humidity_gen": hgen,
            "pressure_gauge": _FakeGauge(),
        },
        log_fn=lambda *_: None,
    )

    assert len(relay.calls) == 16
    assert relay.calls[0] == (1, False)
    assert relay.calls[-1] == (16, False)
    assert relay8.calls == [
        (1, False),
        (2, False),
        (3, False),
        (4, False),
        (5, False),
        (6, False),
        (7, False),
        (8, False),
    ]
    assert result["relay_states"] == [False] * 16
    assert result["relay8_states"] == [False] * 8
    assert result["pace_pressure_hpa"] == 1010.5
    assert result["pace_vent_command_sent"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"
    assert result["pace_vent_command_suppressed"] is False
    assert result["pace_vent_status_returned"] == 2
    assert result["pace_vent_status_text"] == "completed"
    assert result["pace_vent_status_classification"] == "completed_latched"
    assert result["pace_profile"] == "OLD_PACE5000"
    assert result["pace_vent_status_query_raw"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 2"
    assert result["gauge_pressure_hpa"] == 1009.9
    assert result["chamber"]["run_state"] == 0
    assert result["hgen_stop_check"] == {"ok": True, "flow_lpm": 0.0, "max_flow_lpm": 0.05}
    assert hgen.wait_calls == [{"max_flow_lpm": 0.05, "timeout_s": 5.0, "poll_s": 0.5}]


def test_never_sends_vent_status_2_as_command() -> None:
    pace = _FakePace()

    result = perform_safe_stop({"pace": pace}, log_fn=lambda *_: None)

    assert ":SOUR:PRES:LEV:IMM:AMPL:VENT 2" not in pace.calls
    assert result["pace_vent_command_sent"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"
    assert result["pace_vent_status_returned"] == 2
    assert result["pace_vent_status_classification"] == "completed_latched"


def test_cleanup_summary_separates_vent_command_and_vent_status() -> None:
    result = perform_safe_stop({"pace": _FakePace()}, log_fn=lambda *_: None)

    assert "pace_vent" not in result
    assert result["pace_vent_command_sent"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"
    assert result["pace_vent_status_returned"] == 2
    assert result["pace_vent_status_text"] == "completed"
    assert result["pace_vent_status_classification"] == "completed_latched"


def test_safe_stop_old_pace5000_vent_status_2_completed() -> None:
    result = perform_safe_stop({"pace": _FakePace(profile=_FakePace.PROFILE_OLD_PACE5000)}, log_fn=lambda *_: None)

    assert result["pace_profile"] == "OLD_PACE5000"
    assert result["pace_vent_status_returned"] == 2
    assert result["pace_vent_status_text"] == "completed"
    assert result["pace_vent_status_classification"] == "completed_latched"


def test_safe_stop_pace5000e_vent_status_2_timeout_or_profile_defined() -> None:
    result = perform_safe_stop({"pace": _FakePace(profile=_FakePace.PROFILE_PACE5000E)}, log_fn=lambda *_: None)

    assert result["pace_profile"] == "PACE5000E"
    assert result["pace_vent_status_returned"] == 2
    assert result["pace_vent_status_text"] == "timeout"
    assert result["pace_vent_status_classification"] == "timed_out"


def test_safe_stop_uses_profile_vent_status_classifier() -> None:
    pace = _FakePace(profile=_FakePace.PROFILE_PACE5000E)

    result = perform_safe_stop({"pace": pace}, log_fn=lambda *_: None)

    assert pace.classify_calls
    assert pace.classify_calls[-1] == 2
    assert result["pace_vent_status_classification"] == "timed_out"


def test_safe_stop_records_vent_command_only_when_write_happens() -> None:
    result = perform_safe_stop({"pace": _FakePace(helper_vent_command_sent=True)}, log_fn=lambda *_: None)

    assert result["pace_vent_command_sent"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"
    assert result["pace_vent_command_suppressed"] is False
    assert result["pace_vent_command_suppressed_reason"] == ""


def test_safe_stop_does_not_record_vent_command_when_helper_blocks() -> None:
    result = perform_safe_stop(
        {"pace": _FakeDiagnosticBlockedPace()},
        log_fn=lambda *_: None,
        pace_mode="diagnostic_safe_vent",
    )

    assert result["pace_vent_command_sent"] is None
    assert result["pace_vent_command_suppressed"] is True
    assert result["pace_diagnostic_safe_vent"]["ok"] is False


def test_safe_stop_records_suppressed_reason_when_vent_not_sent() -> None:
    result = perform_safe_stop({"pace": _FakeSuppressedSafeStopPace()}, log_fn=lambda *_: None)

    assert result["pace_vent_command_sent"] is None
    assert result["pace_vent_command_suppressed"] is True
    assert result["pace_vent_command_suppressed_reason"] == "adapter_safe_stop_did_not_send_vent_for_test"


def test_perform_safe_stop_uses_cfg_baseline_when_available() -> None:
    relay = _FakeRelay()
    relay8 = _FakeRelay()
    cfg = {
        "valves": {
            "gas_main": 11,
            "h2o_path": 8,
            "hold": 9,
            "flow_switch": 10,
            "relay_map": {
                "8": {"device": "relay_8", "channel": 8},
                "9": {"device": "relay_8", "channel": 1},
                "10": {"device": "relay_8", "channel": 2},
                "11": {"device": "relay_8", "channel": 3},
            },
        }
    }

    result = perform_safe_stop(
        {
            "relay": relay,
            "relay_8": relay8,
        },
        log_fn=lambda *_: None,
        cfg=cfg,
    )

    assert result["relay_states"] == [False] * 16
    assert result["relay8_states"] == [False] * 8


def test_perform_safe_stop_uses_cfg_hgen_stop_wait_settings() -> None:
    hgen = _FakeHgen()

    perform_safe_stop(
        {
            "humidity_gen": hgen,
        },
        log_fn=lambda *_: None,
        cfg={
            "workflow": {
                "humidity_generator": {
                    "safe_stop_verify_flow": True,
                    "safe_stop_max_flow_lpm": 0.2,
                    "safe_stop_timeout_s": 25.0,
                    "safe_stop_poll_s": 0.75,
                }
            }
        },
    )

    assert hgen.wait_calls == [{"max_flow_lpm": 0.2, "timeout_s": 25.0, "poll_s": 0.75}]


def test_perform_safe_stop_logs_hgen_command_failures_truthfully() -> None:
    class _FailingHgen:
        def safe_stop(self):
            return {
                "flow_off": "ok",
                "ctrl_off": "failed",
                "ctrl_off_error": "ctrl broken",
                "cool_off": "ok",
                "heat_off": "failed",
                "heat_off_error": "heat broken",
            }

        def fetch_all(self):
            return {"raw": "demo", "data": {"Tc": 20.0}}

    logs = []
    perform_safe_stop({"humidity_gen": _FailingHgen()}, log_fn=logs.append)

    assert any("hgen ctrl_off failed: ctrl broken" in msg for msg in logs)
    assert any("hgen heat_off failed: heat broken" in msg for msg in logs)
    assert any("hgen safe_stop summary:" in msg and "ctrl_off=failed" in msg and "heat_off=failed" in msg for msg in logs)


def test_validate_safe_stop_result_reports_verification_issues() -> None:
    issues = validate_safe_stop_result(
        {
            "relay_states": [True] * 16,
            "relay8_states": [False] * 8,
            "chamber": {"run_state": 1},
            "hgen_stop_check": {"ok": False},
            "pace_outp": ":OUTP:STAT 1",
            "pace_isol": ":OUTP:ISOL:STAT 0",
        },
        cfg={
            "valves": {},
            "workflow": {"humidity_generator": {"safe_stop_enforce_flow_check": True}},
        },
    )

    assert "chamber run_state not stopped: 1" in issues
    assert "humidity generator stop check failed" in issues
    assert "pace output not off: 1" in issues
    assert "pace isolation not open: 0" in issues


def test_validate_safe_stop_result_enforces_hgen_flow_failure_by_default() -> None:
    issues = validate_safe_stop_result(
        {
            "hgen_stop_check": {"ok": False},
            "pace_outp": ":OUTP:STAT 0",
            "pace_isol": ":OUTP:ISOL:STAT 1",
        },
        cfg={"valves": {}},
    )

    assert "humidity generator stop check failed" in issues


def test_validate_safe_stop_result_reports_hgen_command_failures() -> None:
    issues = validate_safe_stop_result(
        {
            "hgen_safe_stop": {
                "flow_off": "ok",
                "ctrl_off": "failed",
                "cool_off": "ok",
                "heat_off": "failed",
            },
            "hgen_stop_check": {"ok": True, "flow_lpm": 0.0},
            "pace_outp": ":OUTP:STAT 0",
            "pace_isol": ":OUTP:ISOL:STAT 1",
        },
        cfg={"valves": {}},
    )

    assert "humidity generator ctrl_off failed" in issues
    assert "humidity generator heat_off failed" in issues


def test_validate_safe_stop_result_reports_hgen_current_snapshot_error() -> None:
    issues = validate_safe_stop_result(
        {
            "hgen_stop_check": {"ok": True, "flow_lpm": 0.0},
            "hgen_current": {"raw": "Error!", "data": {}},
            "pace_outp": ":OUTP:STAT 0",
            "pace_isol": ":OUTP:ISOL:STAT 1",
        },
        cfg={"valves": {}},
    )

    assert "humidity generator current snapshot invalid" in issues


def test_validate_safe_stop_result_reports_hgen_current_flow_when_snapshot_disagrees() -> None:
    issues = validate_safe_stop_result(
        {
            "hgen_stop_check": {"ok": True, "flow_lpm": 0.0},
            "hgen_current": {"raw": "Flux= 1.2", "data": {"Fl": 1.2}},
            "pace_outp": ":OUTP:STAT 0",
            "pace_isol": ":OUTP:ISOL:STAT 1",
        },
        cfg={"valves": {}},
    )

    assert "humidity generator flow still high: 1.2" in issues


def test_perform_safe_stop_with_retries_retries_until_verified(monkeypatch) -> None:
    calls = {"count": 0}

    def _fake_perform(devices, log_fn=None, cfg=None):
        calls["count"] += 1
        if calls["count"] == 1:
            return {"pace_outp": ":OUTP:STAT 1"}
        return {"pace_outp": ":OUTP:STAT 0", "pace_isol": ":OUTP:ISOL:STAT 1"}

    monkeypatch.setattr(safe_stop, "perform_safe_stop", _fake_perform)

    result = perform_safe_stop_with_retries({}, log_fn=lambda *_: None, attempts=3, retry_delay_s=0.0)

    assert calls["count"] == 2
    assert result["safe_stop_verified"] is True
    assert result["safe_stop_attempt"] == 2


def test_safe_stop_cfg_disables_nonessential_devices_only() -> None:
    cfg = {
        "devices": {
            "pressure_controller": {"enabled": True},
            "pressure_gauge": {"enabled": True},
            "humidity_generator": {"enabled": True},
            "temperature_chamber": {"enabled": True},
            "relay": {"enabled": True},
            "relay_8": {"enabled": False},
            "gas_analyzer": {"enabled": True},
            "dewpoint_meter": {"enabled": True},
            "thermometer": {"enabled": True},
            "gas_analyzers": [
                {"name": "ga01", "enabled": True},
                {"name": "ga02", "enabled": False},
            ],
        }
    }

    reduced = safe_stop._safe_stop_cfg(cfg)

    assert reduced["devices"]["pressure_controller"]["enabled"] is True
    assert reduced["devices"]["pressure_gauge"]["enabled"] is True
    assert reduced["devices"]["humidity_generator"]["enabled"] is True
    assert reduced["devices"]["temperature_chamber"]["enabled"] is True
    assert reduced["devices"]["relay"]["enabled"] is True
    assert reduced["devices"]["relay_8"]["enabled"] is False
    assert reduced["devices"]["gas_analyzer"]["enabled"] is False
    assert reduced["devices"]["dewpoint_meter"]["enabled"] is False
    assert reduced["devices"]["thermometer"]["enabled"] is False
    assert [item["enabled"] for item in reduced["devices"]["gas_analyzers"]] == [False, False]
    assert cfg["devices"]["gas_analyzer"]["enabled"] is True
    assert cfg["devices"]["dewpoint_meter"]["enabled"] is True


def test_main_builds_only_safe_stop_devices(monkeypatch, tmp_path: Path) -> None:
    cfg = {
        "paths": {"output_dir": str(tmp_path)},
        "devices": {
            "pressure_controller": {"enabled": True},
            "pressure_gauge": {"enabled": True},
            "humidity_generator": {"enabled": True},
            "temperature_chamber": {"enabled": True},
            "relay": {"enabled": True},
            "relay_8": {"enabled": True},
            "gas_analyzer": {"enabled": True},
            "dewpoint_meter": {"enabled": True},
            "thermometer": {"enabled": True},
            "gas_analyzers": [{"name": "ga01", "enabled": True}],
        },
    }
    seen = {}

    class _FakeLogger:
        def __init__(self, out_dir, run_id=None):
            self.run_dir = str(Path(out_dir) / (run_id or "run"))

        def close(self) -> None:
            return None

    monkeypatch.setattr(safe_stop, "load_config", lambda _path: cfg)
    monkeypatch.setattr(safe_stop, "RunLogger", _FakeLogger)
    monkeypatch.setattr(safe_stop, "_close_devices", lambda _devices: None)
    monkeypatch.setattr(
        safe_stop,
        "perform_safe_stop",
        lambda devices, log_fn=None, cfg=None: seen.setdefault("devices", devices),
    )

    def _fake_build_devices(build_cfg, io_logger=None):
        seen["cfg"] = build_cfg
        return {"relay": object()}

    monkeypatch.setattr(safe_stop, "_build_devices", _fake_build_devices)

    code = safe_stop.main(["--config", "configs/default_config.json"])

    assert code == 0
    assert seen["cfg"]["devices"]["pressure_controller"]["enabled"] is True
    assert seen["cfg"]["devices"]["pressure_gauge"]["enabled"] is True
    assert seen["cfg"]["devices"]["humidity_generator"]["enabled"] is True
    assert seen["cfg"]["devices"]["temperature_chamber"]["enabled"] is True
    assert seen["cfg"]["devices"]["relay"]["enabled"] is True
    assert seen["cfg"]["devices"]["relay_8"]["enabled"] is True
    assert seen["cfg"]["devices"]["gas_analyzer"]["enabled"] is False
    assert seen["cfg"]["devices"]["dewpoint_meter"]["enabled"] is False
    assert seen["cfg"]["devices"]["thermometer"]["enabled"] is False
    assert seen["cfg"]["devices"]["gas_analyzers"][0]["enabled"] is False
