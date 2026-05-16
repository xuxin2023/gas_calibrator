"""Tests for V1.5 no-OUTP preseal physical probe tool.

All tests are OFFLINE — no real hardware is touched.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROBE_PATH = "gas_calibrator.tools.run_v1_5_no_outp_preseal_probe"


class FakePace:
    VENT_STATUS_TRAPPED_PRESSURE = 3
    VENT_STATUS_IDLE = 0

    def __init__(self):
        self._pressure = 1013.0
        self.vent_calls: list[bool] = []
        self.output_calls: list[bool] = []

    def open(self): ...
    def close(self): ...

    def read_pressure(self):
        return self._pressure

    def vent(self, on: bool = True):
        self.vent_calls.append(on)

    def set_output(self, on: bool):
        self.output_calls.append(on)

    def stop_atmosphere_hold(self): ...
    def start_atmosphere_hold(self, interval_s=2.0): ...

    def set_isolation_open(self, is_open: bool): ...


class FakeGauge:
    def __init__(self, pressures=None):
        self._pressures = pressures or [1013.0]
        self._idx = 0

    def open(self): ...
    def close(self): ...

    def read_pressure(self):
        v = self._pressures[min(self._idx, len(self._pressures) - 1)]
        self._idx += 1
        return v


class FakeDewpoint:
    def open(self): ...
    def close(self): ...
    def read_dewpoint(self):
        return -25.0


class FakeRelay:
    """Fake relay that matches real RelayController API (set_valve, set_valves_bulk)."""

    def __init__(self, *a, **kw):
        self.valve_states: dict[int, bool] = {}
        self.bulk_calls: list[list] = []
        self.single_calls: list[tuple] = []
        self._fail: Optional[Exception] = None

    def open(self): ...
    def close(self): ...

    def set_fail(self, exc: Exception) -> None:
        self._fail = exc

    def set_valve(self, channel: int, open_: bool) -> None:
        if self._fail is not None:
            raise self._fail
        self.single_calls.append((int(channel), bool(open_)))
        self.valve_states[int(channel)] = bool(open_)

    def set_valves_bulk(self, updates) -> None:
        if self._fail is not None:
            raise self._fail
        normalized = list(updates)
        self.bulk_calls.append(normalized)
        for channel, state in normalized:
            self.valve_states[int(channel)] = bool(state)


def _sample_config(co2_map_1000_valve=6):
    return {
        "paths": {"output_dir": "logs"},
        "devices": {
            "pressure_controller": {"enabled": True, "port": "COM99", "baud": 115200},
            "pressure_gauge": {"enabled": True, "port": "COM22", "baud": 115200, "dest_id": 1},
            "dewpoint_meter": {"enabled": True, "port": "COM33", "baud": 115200, "station": 1},
            "relay": {"enabled": True, "port": "COM88", "baud": 38400, "addr": 1},
        },
        "valves": {
            "h2o_path": 10,
            "gas_main": 11,
            "co2_path": 7,
            "co2_map": {"1000": co2_map_1000_valve},
        },
        "workflow": {
            "pressure": {},
            "startup_pressure_precheck": {"enabled": False},
        },
        "coefficients": {"enabled": False},
        "postrun_corrected_delivery": {"enabled": False, "write_devices": False, "write_pressure_coefficients": False},
    }


def _sample_group2_only_config():
    cfg = _sample_config()
    cfg["valves"]["co2_map"] = {}
    cfg["valves"]["co2_map_group2"] = {"1000": 16}
    cfg["valves"]["co2_path_group2"] = 17
    return cfg


def _sample_config_with_relay8():
    cfg = _sample_config()
    cfg["devices"]["relay_8"] = {"enabled": True, "port": "COM28", "baud": 38400, "addr": 1}
    cfg["valves"]["relay_map"] = {
        "8": {"device": "relay_8", "channel": 8},
        "9": {"device": "relay_8", "channel": 1},
        "10": {"device": "relay_8", "channel": 2},
        "11": {"device": "relay_8", "channel": 3},
    }
    return cfg


def _args(**over):
    ns = MagicMock()
    ns.config = "fake_config.json"
    ns.co2_ppm = 1000.0
    ns.temp = 20.0
    ns.observe_s = 0.2
    ns.min_rise_hpa = 3.0
    ns.no_write = True
    ns.safe_stop_before = False
    ns.safe_stop_after = False
    for k, v in over.items():
        setattr(ns, k, v)
    return ns


def _setup_probe(cfg, monkeypatch, **args_over):
    from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe, _now_ts
    monkeypatch.setattr(f"{PROBE_PATH}._now_ts", lambda: "20260101_000000")
    with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
        return NoOutpProbe(_args(**args_over))


def _inject_devices(probe, pace, gauge, dewpoint, relay=None, relay_8=None, monkeypatch=None):
    probe.devices = {"pace": pace, "pressure_gauge": gauge, "dewpoint": dewpoint}
    if relay is not None:
        probe.devices["relay"] = relay
    if relay_8 is not None:
        probe.devices["relay_8"] = relay_8
    probe.output_dir = Path("logs/no_outp_preseal_probe/test")
    probe.output_dir.mkdir(parents=True, exist_ok=True)
    probe.trace_path = probe.output_dir / "probe_trace.csv"
    probe.summary_path = probe.output_dir / "probe_summary.json"
    probe.io_log_path = probe.output_dir / "io_log.csv"
    from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import ProbeIoLogger
    probe.io_logger = ProbeIoLogger(probe.io_log_path)
    if monkeypatch is not None:
        monkeypatch.setattr("time.sleep", lambda _: None)


# ═══════════════════════════════════════════════════════════════

class TestPreflight:
    def test_refuses_when_write_risk_true(self):
        cfg = _sample_config()
        cfg["coefficients"]["enabled"] = True

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args())
        err = probe._no_write_preflight()
        assert err is not None
        assert "NO_WRITE_PREFLIGHT_FAIL" in err

    def test_refuses_when_coefficients_sencos_non_empty(self):
        cfg = _sample_config()
        cfg["coefficients"]["sencos"] = {"001": [1.0, 2.0]}

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args())
        err = probe._no_write_preflight()
        assert err is not None
        assert "coefficients.sencos_non_empty" in err

    def test_refuses_when_workflow_coefficients_sencos_non_empty(self):
        cfg = _sample_config()
        cfg.setdefault("workflow", {}).setdefault("coefficients", {})["sencos"] = {"001": [1.0, 2.0]}

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args())
        err = probe._no_write_preflight()
        assert err is not None
        assert "workflow.coefficients.sencos_non_empty" in err

    def test_refuses_when_workflow_postrun_corrected_delivery_write_devices_true(self):
        cfg = _sample_config()
        cfg.setdefault("workflow", {}).setdefault("postrun_corrected_delivery", {})["write_devices"] = True

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args())
        err = probe._no_write_preflight()
        assert err is not None
        assert "NO_WRITE_PREFLIGHT_FAIL" in err
        assert "workflow.postrun_corrected_delivery" in err

    def test_refuses_when_workflow_startup_pressure_sensor_calibration_apply_write_true(self):
        cfg = _sample_config()
        cfg.setdefault("workflow", {}).setdefault("startup_pressure_sensor_calibration", {})["apply_write"] = True

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args())
        err = probe._no_write_preflight()
        assert err is not None
        assert "NO_WRITE_PREFLIGHT_FAIL" in err
        assert "workflow.startup_pressure_sensor_calibration" in err

    def test_refuses_when_startup_pressure_hold_enabled(self):
        cfg = _sample_config()
        cfg["workflow"]["startup_pressure_precheck"]["enabled"] = True

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args())
        err = probe._no_write_preflight()
        assert err is not None
        assert "startup_pressure_precheck" in err or "FAIL" in err

    def test_passes_clean_config(self):
        cfg = _sample_config()
        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args())
        err = probe._no_write_preflight()
        assert err is None


# ═══════════════════════════════════════════════════════════════

class TestBuildDevices:
    def test_build_devices_creates_relay_when_config_has_relay(self, monkeypatch):
        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        cfg = _sample_config()
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            with patch(f"{PROBE_PATH}.RelayController") as MockRelay:
                instance = MagicMock()
                MockRelay.return_value = instance
                # Prevent PACE/gauge/dewpoint from opening real COM
                with patch(f"{PROBE_PATH}.Pace5000") as MockPace:
                    MockPace.return_value = MagicMock()
                    with patch(f"{PROBE_PATH}.ParoscientificGauge") as MockGauge:
                        MockGauge.return_value = MagicMock()
                        with patch(f"{PROBE_PATH}.DewpointMeter") as MockDew:
                            MockDew.return_value = MagicMock()
                            probe = NoOutpProbe(_args())
                            probe._build_devices()
        assert "relay" in probe.devices
        assert probe.devices["relay"] is not None

    def test_build_devices_creates_relay8_when_config_has_relay8(self, monkeypatch):
        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        cfg = _sample_config_with_relay8()
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            with patch(f"{PROBE_PATH}.RelayController") as MockRelay:
                instance = MagicMock()
                MockRelay.return_value = instance
                with patch(f"{PROBE_PATH}.Pace5000") as MockPace:
                    MockPace.return_value = MagicMock()
                    with patch(f"{PROBE_PATH}.ParoscientificGauge") as MockGauge:
                        MockGauge.return_value = MagicMock()
                        with patch(f"{PROBE_PATH}.DewpointMeter") as MockDew:
                            MockDew.return_value = MagicMock()
                            probe = NoOutpProbe(_args())
                            probe._build_devices()
        assert "relay" in probe.devices
        assert "relay_8" in probe.devices

    def test_probe_blocks_if_relay_missing_for_real_mode(self, monkeypatch):
        cfg = _sample_config()
        cfg["devices"]["relay"]["enabled"] = False
        probe = _setup_probe(cfg, monkeypatch)

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import ProbeIoLogger
        # Mock all device constructors so _build_devices doesn't open real COM
        with patch(f"{PROBE_PATH}.Pace5000") as MockPace:
            MockPace.return_value = MagicMock()
            with patch(f"{PROBE_PATH}.ParoscientificGauge") as MockGauge:
                MockGauge.return_value = MagicMock()
                with patch(f"{PROBE_PATH}.DewpointMeter") as MockDew:
                    MockDew.return_value = MagicMock()
                    code = probe.run()
        assert code == 4
        assert "BLOCKED_RELAY_MISSING" in probe._final_decision


# ═══════════════════════════════════════════════════════════════

class TestProbeUsesRealRelayApi:
    def test_probe_uses_set_valve_not_activate(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch)
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1020.0, 1020.0])
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        code = probe._probe()

        # Verify relay methods were called (set_valve or set_valves_bulk, NOT activate)
        total_calls = len(relay.single_calls) + sum(len(b) for b in relay.bulk_calls)
        assert total_calls > 0, "no relay calls made"
        # Verify no activate method was used
        assert not hasattr(relay, "activated") or len(getattr(relay, "activated", [])) == 0

    def test_probe_opens_co2_route_valves(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch)
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1020.0, 1020.0])
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        probe._probe()

        # CO2 source valve (6), co2_path (7), gas_main (11), h2o_path (10)
        open_channels = set()
        for ch, st in relay.single_calls:
            if st is True:
                open_channels.add(ch)
        for bulk in relay.bulk_calls:
            for ch, st in bulk:
                if st is True:
                    open_channels.add(ch)
        assert 6 in open_channels, "CO2 source valve 1000ppm not opened"
        assert 7 in open_channels, "CO2 path valve not opened"
        assert 10 in open_channels, "h2o_path (total valve) not opened"
        assert 11 in open_channels, "gas_main not opened"

    def test_probe_uses_group_a_co2_path_for_co2_map_match(self, monkeypatch):
        cfg = _sample_config()
        cfg["valves"]["co2_path_group2"] = 17
        probe = _setup_probe(cfg, monkeypatch)
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1020.0, 1020.0])
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        probe._probe()

        open_channels = set()
        for ch, st in relay.single_calls:
            if st is True:
                open_channels.add(ch)
        for bulk in relay.bulk_calls:
            for ch, st in bulk:
                if st is True:
                    open_channels.add(ch)
        assert 7 in open_channels
        assert 17 not in open_channels
        probe._write_summary()
        summary = json.loads(probe.summary_path.read_text())
        assert summary["co2_group"] == "A"
        assert summary["group2_supported"] is False

    def test_probe_blocks_group2_when_only_co2_map_group2_matches(self, monkeypatch):
        cfg = _sample_group2_only_config()
        probe = _setup_probe(cfg, monkeypatch)
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1020.0, 1020.0])
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        code = probe._probe()

        assert code == 7
        assert probe._final_decision == "BLOCKED_GROUP2_UNSUPPORTED"
        assert not relay.single_calls
        assert not relay.bulk_calls

    def test_probe_fails_if_route_open_failed(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch)
        pace = FakePace()
        gauge = FakeGauge()
        relay = FakeRelay()
        relay.set_fail(RuntimeError("SIMULATED_RELAY_FAILURE"))
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        code = probe._probe()
        assert code == 5
        assert "BLOCKED_ROUTE_OPEN_FAIL" in probe._final_decision


# ═══════════════════════════════════════════════════════════════

class TestOpenflowVent:
    def test_vent1_without_outp0(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch, observe_s=0.1)
        pace = FakePace()
        gauge = FakeGauge()
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        code = probe._probe()

        outp_set = [c for c in pace.output_calls if c is True or c is False]
        assert len(outp_set) == 0, f"OUTP calls: {pace.output_calls}"
        assert pace.vent_calls.count(True) >= 1
        assert pace.vent_calls.count(False) == 1
        assert code == 0 if (probe.com22_max or 0) - (probe.com22_baseline or 0) >= 3.0 else 1


class TestCloseAtmosphere:
    def test_vent0_without_outp0(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch, observe_s=0.1)
        pace = FakePace()
        gauge = FakeGauge()
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        probe._probe()
        outp_set = [c for c in pace.output_calls if c is True or c is False]
        assert len(outp_set) == 0, f"OUTP0/OUTP1 in probe: {pace.output_calls}"


class TestPressureRiseDecide:
    def test_passes_when_com22_rises(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch, observe_s=0.2, min_rise_hpa=3.0)
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1013.0, 1013.0, 1015.0, 1020.0, 1025.0, 1025.0])
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        code = probe._probe()
        assert code == 0, f"Expected PASS but got code={code}, com22_baseline={probe.com22_baseline}, com22_max={probe.com22_max}"

    def test_fails_when_com22_does_not_rise(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch, observe_s=0.2, min_rise_hpa=3.0)
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1013.0, 1013.0, 1013.0])
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        code = probe._probe()
        assert code == 1, f"Expected FAIL but got code={code}"


# ═══════════════════════════════════════════════════════════════

class TestIoLog:
    def test_probe_writes_io_log(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch, observe_s=0.1)
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1020.0, 1020.0])
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        probe._probe()
        probe.io_logger.close()

        assert probe.io_log_path.exists()
        with probe.io_log_path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) > 0
        # Should have PROBE phase entries
        probe_rows = [r for r in rows if r.get("port") == "PROBE"]
        assert len(probe_rows) >= 3  # open_valves, VENT1, VENT0

    def test_probe_counts_outp_from_io_log(self, monkeypatch, tmp_path):
        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import ProbeIoLogger

        io_path = tmp_path / "io_log.csv"
        logger = ProbeIoLogger(io_path)
        logger.log_io(port="PROBE", device="pace", direction="TX", command=":OUTP 0")
        logger.log_io(port="PROBE", device="pace", direction="TX", command=":OUTP 0")
        logger.log_io(port="PROBE", device="pace", direction="TX", command=":OUTP 1")
        logger.close()

        cfg = _sample_config()
        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args())
        probe.io_log_path = io_path

        outp0, outp1, vent0, vent1 = probe._count_outp_from_io_log()
        assert outp0 == 2
        assert outp1 == 1
        assert vent0 == 0
        assert vent1 == 0

    def test_probe_counts_vent_from_io_log(self, monkeypatch, tmp_path):
        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import ProbeIoLogger

        io_path = tmp_path / "io_log.csv"
        logger = ProbeIoLogger(io_path)
        logger.log_io(port="PROBE", device="pace", direction="TX", command="VENT1 open-flow start")
        logger.log_io(port="PROBE", device="pace", direction="TX", command="VENT0 close")
        logger.log_io(port="PROBE", device="pace", direction="TX", command="VENT1 restore atmosphere")
        logger.close()

        cfg = _sample_config()
        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args())
        probe.io_log_path = io_path

        outp0, outp1, vent0, vent1 = probe._count_outp_from_io_log()
        assert vent0 == 1
        assert vent1 == 2

    def test_probe_summary_marks_blocked_when_io_log_missing(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch, observe_s=0.1)
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1020.0, 1020.0])
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        # Delete io_log to simulate missing
        probe.io_log_path = Path("logs/no_outp_preseal_probe/nonexistent/io_log.csv")

        code = probe._probe()
        # Should be BLOCKED because no io_log
        assert code == 6
        assert "BLOCKED_IO_LOG_MISSING" in probe._final_decision


# ═══════════════════════════════════════════════════════════════

class TestOutpCounting:
    def test_counts_outp_from_io_log_as_fail(self, monkeypatch, tmp_path):
        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import ProbeIoLogger

        io_path = tmp_path / "io_log.csv"
        logger = ProbeIoLogger(io_path)
        logger.log_io(port="PROBE", device="pace", direction="TX", command=":OUTP 0")
        logger.log_io(port="PROBE", device="pace", direction="TX", command="VENT0 close")
        logger.close()

        cfg = _sample_config()
        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe
        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args(observe_s=0.1, min_rise_hpa=3.0))
        probe.io_log_path = io_path
        probe.com22_baseline = 1013.0
        probe.com22_max = 1025.0

        code = probe._decide()
        assert code == 1
        assert "OUTP0_from_io_log=1" in probe._final_decision

    def test_separates_probe_and_cleanup_phase(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch, observe_s=0.1)
        pace = FakePace()
        gauge = FakeGauge()
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        probe._probe()
        assert probe._vent0_count == 1


class TestSummaryOutput:
    def test_writes_summary_and_trace(self, monkeypatch):
        cfg = _sample_config()
        probe = _setup_probe(cfg, monkeypatch, observe_s=0.1, min_rise_hpa=3.0)
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1020.0, 1020.0])
        relay = FakeRelay()
        _inject_devices(probe, pace, gauge, FakeDewpoint(), relay=relay, monkeypatch=monkeypatch)

        probe._probe()
        probe._write_summary()

        assert probe.summary_path.exists()
        summary = json.loads(probe.summary_path.read_text())
        assert "final_decision" in summary
        assert "com22_pressure_rise_hpa" in summary
        assert "io_log_exists" in summary
        assert "outp_counting_source" in summary
        assert summary["outp_counting_source"] == "io_log.csv"

        assert probe.trace_path.exists()
        assert probe.trace_path.stat().st_size > 0
