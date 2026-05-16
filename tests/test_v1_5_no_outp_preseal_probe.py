"""Tests for V1.5 no-OUTP preseal physical probe tool.

All tests are OFFLINE — no real hardware is touched.
"""

from __future__ import annotations

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
    def __init__(self, *a, **kw):
        self.activated: list = []

    def open(self): ...
    def close(self): ...

    def activate(self, valves):
        self.activated = list(valves)


def _sample_config(co2_map_1000_valve=6):
    return {
        "paths": {"output_dir": "logs"},
        "devices": {
            "pressure_controller": {"enabled": True, "port": "COM99", "baud": 115200},
            "pressure_gauge": {"enabled": True, "port": "COM22", "baud": 115200, "dest_id": 1},
            "dewpoint_meter": {"enabled": True, "port": "COM33", "baud": 115200, "station": 1},
            "relay": {"port": "COM88", "baud": 115200},
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

class TestOpenflowVent:
    def test_vent1_without_outp0(self, monkeypatch):
        cfg = _sample_config()
        pace = FakePace()
        gauge = FakeGauge()
        dew = FakeDewpoint()

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe, _now_ts
        monkeypatch.setattr(f"{PROBE_PATH}._now_ts", lambda: "20260101_000000")

        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args(observe_s=0.1))
        probe.devices = {"pace": pace, "pressure_gauge": gauge, "dewpoint": dew}
        probe.output_dir = Path("logs/no_outp_preseal_probe/test")
        probe.trace_path = probe.output_dir / "probe_trace.csv"
        probe.summary_path = probe.output_dir / "probe_summary.json"
        probe.output_dir.mkdir(parents=True, exist_ok=True)

        # Bypass relay
        pace_relay = MagicMock()
        pace_relay.open = MagicMock()
        pace_relay.activate = MagicMock()
        probe.devices["relay"] = pace_relay

        code = probe._probe()

        outp_set = [c for c in pace.output_calls if c is True or c is False]
        assert len(outp_set) == 0, f"OUTP calls: {pace.output_calls}"
        assert pace.vent_calls.count(True) >= 1
        assert pace.vent_calls.count(False) == 1
        assert code == 0 if (probe.com22_max or 0) - (probe.com22_baseline or 0) >= 3.0 else 1


class TestCloseAtmosphere:
    def test_vent0_without_outp0(self, monkeypatch):
        cfg = _sample_config()
        pace = FakePace()
        gauge = FakeGauge()

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe, _now_ts
        monkeypatch.setattr(f"{PROBE_PATH}._now_ts", lambda: "20260101_000001")

        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args(observe_s=0.1))
        probe.devices = {"pace": pace, "pressure_gauge": gauge, "dewpoint": FakeDewpoint()}
        probe.output_dir = Path("logs/no_outp_preseal_probe/test2")
        probe.trace_path = probe.output_dir / "probe_trace.csv"
        probe.summary_path = probe.output_dir / "probe_summary.json"
        probe.output_dir.mkdir(parents=True, exist_ok=True)

        pace_relay = MagicMock()
        pace_relay.open = MagicMock()
        pace_relay.activate = MagicMock()
        probe.devices["relay"] = pace_relay

        probe._probe()
        outp_set = [c for c in pace.output_calls if c is True or c is False]
        assert len(outp_set) == 0, f"OUTP0/OUTP1 in probe: {pace.output_calls}"


class TestPressureRiseDecide:
    def test_passes_when_com22_rises(self, monkeypatch):
        cfg = _sample_config()
        pace = FakePace()
        # Gauge rises from 1013 → 1025
        gauge = FakeGauge([1013.0, 1013.0, 1013.0, 1015.0, 1020.0, 1025.0, 1025.0])

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe, _now_ts
        monkeypatch.setattr(f"{PROBE_PATH}._now_ts", lambda: "20260101_000002")

        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args(observe_s=0.2, min_rise_hpa=3.0))
        probe.devices = {"pace": pace, "pressure_gauge": gauge, "dewpoint": FakeDewpoint()}
        probe.output_dir = Path("logs/no_outp_preseal_probe/test3")
        probe.trace_path = probe.output_dir / "probe_trace.csv"
        probe.summary_path = probe.output_dir / "probe_summary.json"
        probe.output_dir.mkdir(parents=True, exist_ok=True)

        pace_relay = MagicMock()
        pace_relay.open = MagicMock()
        pace_relay.activate = MagicMock()
        probe.devices["relay"] = pace_relay

        code = probe._probe()
        assert code == 0, f"Expected PASS but got code={code}, com22_baseline={probe.com22_baseline}, com22_max={probe.com22_max}"

    def test_fails_when_com22_does_not_rise(self, monkeypatch):
        cfg = _sample_config()
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1013.0, 1013.0, 1013.0])

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe, _now_ts
        monkeypatch.setattr(f"{PROBE_PATH}._now_ts", lambda: "20260101_000003")

        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args(observe_s=0.2, min_rise_hpa=3.0))
        probe.devices = {"pace": pace, "pressure_gauge": gauge, "dewpoint": FakeDewpoint()}
        probe.output_dir = Path("logs/no_outp_preseal_probe/test4")
        probe.trace_path = probe.output_dir / "probe_trace.csv"
        probe.summary_path = probe.output_dir / "probe_summary.json"
        probe.output_dir.mkdir(parents=True, exist_ok=True)

        pace_relay = MagicMock()
        pace_relay.open = MagicMock()
        pace_relay.activate = MagicMock()
        probe.devices["relay"] = pace_relay

        code = probe._probe()
        assert code == 1, f"Expected FAIL but got code={code}"


class TestOutpCounting:
    def test_counts_outp_as_fail(self, monkeypatch):
        cfg = _sample_config()
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1025.0, 1025.0])

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe, _now_ts
        monkeypatch.setattr(f"{PROBE_PATH}._now_ts", lambda: "20260101_000004")

        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args(observe_s=0.1, min_rise_hpa=3.0))
        probe.devices = {"pace": pace, "pressure_gauge": gauge, "dewpoint": FakeDewpoint()}
        probe.output_dir = Path("logs/no_outp_preseal_probe/test5")
        probe.trace_path = probe.output_dir / "probe_trace.csv"
        probe.summary_path = probe.output_dir / "probe_summary.json"
        probe.output_dir.mkdir(parents=True, exist_ok=True)

        pace_relay = MagicMock()
        pace_relay.open = MagicMock()
        pace_relay.activate = MagicMock()
        probe.devices["relay"] = pace_relay

        # Inject a "bad" OUTP0 call (simulate if something sent OUTP0)
        probe._outp0_count = 1
        code = probe._decide()
        assert code == 1

    def test_separates_probe_and_cleanup_phase(self, monkeypatch):
        cfg = _sample_config()
        pace = FakePace()
        gauge = FakeGauge()

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe, _now_ts
        monkeypatch.setattr(f"{PROBE_PATH}._now_ts", lambda: "20260101_000005")

        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args(observe_s=0.1))
        probe.devices = {"pace": pace, "pressure_gauge": gauge, "dewpoint": FakeDewpoint()}
        probe.output_dir = Path("logs/no_outp_preseal_probe/test6")
        probe.trace_path = probe.output_dir / "probe_trace.csv"
        probe.summary_path = probe.output_dir / "probe_summary.json"
        probe.output_dir.mkdir(parents=True, exist_ok=True)

        pace_relay = MagicMock()
        pace_relay.open = MagicMock()
        pace_relay.activate = MagicMock()
        probe.devices["relay"] = pace_relay

        probe._probe()
        # _outp0_count tracks probe phase only — cleanup vent counts are separate
        assert probe._outp0_count == 0
        # vent0 should be exactly 1
        assert probe._vent0_count == 1


class TestSummaryOutput:
    def test_writes_summary_and_trace(self, monkeypatch):
        cfg = _sample_config()
        pace = FakePace()
        gauge = FakeGauge([1013.0, 1020.0])

        from gas_calibrator.tools.run_v1_5_no_outp_preseal_probe import NoOutpProbe, _now_ts
        monkeypatch.setattr(f"{PROBE_PATH}._now_ts", lambda: "20260101_000006")

        with patch(f"{PROBE_PATH}.load_config", return_value=cfg):
            probe = NoOutpProbe(_args(observe_s=0.1, min_rise_hpa=3.0))
        probe.devices = {"pace": pace, "pressure_gauge": gauge, "dewpoint": FakeDewpoint()}
        probe.output_dir = Path("logs/no_outp_preseal_probe/test7")
        probe.trace_path = probe.output_dir / "probe_trace.csv"
        probe.summary_path = probe.output_dir / "probe_summary.json"
        probe.output_dir.mkdir(parents=True, exist_ok=True)

        pace_relay = MagicMock()
        pace_relay.open = MagicMock()
        pace_relay.activate = MagicMock()
        probe.devices["relay"] = pace_relay

        code = probe._probe()
        probe._write_summary()

        assert probe.summary_path.exists()
        summary = json.loads(probe.summary_path.read_text())
        assert "final_decision" in summary
        assert "com22_pressure_rise_hpa" in summary
        assert summary["outp0_count_probe_phase"] == 0

        assert probe.trace_path.exists()
        assert probe.trace_path.stat().st_size > 0
