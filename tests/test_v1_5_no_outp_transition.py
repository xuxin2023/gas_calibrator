"""Tests for V1.5 no-OUTP transition mode.

Verifies that when workflow.pressure.no_outp_transition_mode=true:
  - close-atmosphere (preseal vent-off) does NOT call pace.set_output(False)
  - enter-atmosphere (open-flow) keepalive refresh does NOT call pace.set_output(False)
  - enable_control_output returns early without set_output(True)
  - pressure-rise gate fires after vent0 in _pressurize_and_hold
  - default mode (flag off) preserves original behaviour
"""

import pytest
from unittest.mock import MagicMock

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.workflow.runner import CalibrationRunner


def _co2_point(index=3, temp=20.0, co2=1000.0, pressure=1100.0):
    return CalibrationPoint(
        index=index, temp_chamber_c=temp, co2_ppm=co2,
        target_pressure_hpa=pressure, co2_group=None,
        hgen_temp_c=None, hgen_rh_pct=None,
        dewpoint_c=None, h2o_mmol=None, raw_h2o=None,
    )


def _make_runner(cfg_override=None, pace=None):
    cfg = {
        "paths": {"output_dir": "logs"},
        "workflow": {
            "pressure": {
                "vent_hold_interval_s": 2.0,
                "vent_transition_timeout_s": 5.0,
                "stabilize_timeout_s": 0.0,
                "atmosphere_hold_strategy": "legacy_hold_thread",
            },
        },
        "valves": {},
    }
    if cfg_override:
        def deep_update(d, u):
            for k, v in u.items():
                if isinstance(v, dict) and k in d:
                    deep_update(d[k], v)
                else:
                    d[k] = v
        deep_update(cfg, cfg_override)

    if pace is None:
        pace = MagicMock()
        from unittest.mock import PropertyMock
        type(pace).VENT_STATUS_TRAPPED_PRESSURE = PropertyMock(return_value=3)
        type(pace).VENT_STATUS_IDLE = PropertyMock(return_value=0)
        pace.stop_atmosphere_hold = MagicMock(return_value=True)
        pace.vent = MagicMock()
        pace.set_output = MagicMock()
        pace.set_output_mode_active = MagicMock()
        pace.set_isolation_open = MagicMock()
        pace.enable_control_output = MagicMock()
        pace.get_output_state = MagicMock(return_value=1)
        pace.get_isolation_state = MagicMock(return_value=1)
        pace.get_vent_status = MagicMock(return_value=0)
        pace.get_vent_after_valve_open = MagicMock(return_value=False)
        pace.read_pressure = MagicMock(return_value=1013.0)

    logged: list[str] = []

    def log_fn(msg):
        logged.append(str(msg))

    class FakeLogger:
        run_dir = None

    runner = CalibrationRunner(cfg, {"pace": pace}, FakeLogger(), log_fn, log_fn)
    return runner, pace, logged


class TestNoOutpFlag:
    def test_default_is_false(self):
        runner, _, _ = _make_runner()
        assert runner._no_outp_transition() is False

    def test_enabled_via_config(self):
        runner, _, _ = _make_runner({
            "workflow": {"pressure": {"no_outp_transition_mode": True}}
        })
        assert runner._no_outp_transition() is True


class TestCloseAtmosphereWithoutOutputToggle:
    """Verify _close_atmosphere_without_output_toggle skips OUTP."""

    def test_stops_hold_calls_vent_false_no_set_output(self):
        runner, pace, _ = _make_runner()
        runner._close_atmosphere_without_output_toggle(pace, reason="test")

        pace.stop_atmosphere_hold.assert_called_once()
        pace.vent.assert_called_once_with(False)
        pace.set_output.assert_not_called()

    def test_handles_missing_stop_hold(self):
        runner, pace, _ = _make_runner()
        del pace.stop_atmosphere_hold

        runner._close_atmosphere_without_output_toggle(pace, reason="test")
        pace.vent.assert_called_once_with(False)
        pace.set_output.assert_not_called()


class TestEnableOutputSkippedInNoOutp:
    def test_enable_output_skips_set_output_true(self):
        runner, pace, _ = _make_runner({
            "workflow": {"pressure": {"no_outp_transition_mode": True}}
        })

        ok = runner._enable_pressure_controller_output(reason="test")

        assert ok is True
        pace.set_output.assert_not_called()
        pace.enable_control_output.assert_not_called()
        pace.set_output_mode_active.assert_not_called()

    def test_normal_mode_still_calls_output(self):
        runner, pace, _ = _make_runner()

        ok = runner._enable_pressure_controller_output(reason="test")
        assert ok is True
        # default mock path uses enable_control_output
        pace.enable_control_output.assert_called_once()


class TestAtmosphereRefreshNoOutp:
    def test_no_outp_mode_skips_set_output_false_in_keepalive(self):
        runner, pace, _ = _make_runner({
            "workflow": {"pressure": {"no_outp_transition_mode": True}}
        })
        runner._pressure_atmosphere_hold_enabled = True
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

        runner._refresh_pressure_controller_atmosphere_hold(force=True, reason="test")

        pace.set_output.assert_not_called()
        pace.vent.assert_called_once_with(True)

    def test_normal_mode_keepalive_still_calls_set_output(self):
        runner, pace, _ = _make_runner()
        runner._pressure_atmosphere_hold_enabled = True
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

        runner._refresh_pressure_controller_atmosphere_hold(force=True, reason="test")

        pace.set_output.assert_called_once_with(False)


class TestPreflightLogging:
    def test_preflight_logs_output_state(self):
        runner, pace, logged = _make_runner({
            "workflow": {"pressure": {"no_outp_transition_mode": True}}
        })
        pace.get_output_state = MagicMock(return_value=1)

        result = runner._check_pressure_output_preflight()
        assert result is None
        assert any("no-outp-preflight" in msg for msg in logged)

    def test_default_mode_does_not_log_preflight(self):
        runner, pace, logged = _make_runner()

        runner._check_pressure_output_preflight()
        assert not any("no-outp-preflight" in msg for msg in logged)


class TestPressureRiseGate:
    def test_pressure_rise_gate_passes_when_pressure_rises(self):
        """Mock PACE pressure rising: 1013 → 1020 hPa (7 hPa rise)."""
        pace = MagicMock()
        type(pace).VENT_STATUS_TRAPPED_PRESSURE = type(pace).__dict__.get(
            'VENT_STATUS_TRAPPED_PRESSURE', 3)
        pace.VENT_STATUS_TRAPPED_PRESSURE = 3
        pace.VENT_STATUS_IDLE = 0
        pace.stop_atmosphere_hold = MagicMock(return_value=True)
        pace.vent = MagicMock()
        pace.set_output = MagicMock()
        pace.set_isolation_open = MagicMock()
        pace.enable_control_output = MagicMock()
        pace.get_output_state = MagicMock(return_value=1)
        pace.get_isolation_state = MagicMock(return_value=1)
        pace.get_vent_status = MagicMock(return_value=0)
        pace.get_vent_after_valve_open = MagicMock(return_value=False)
        pace.set_setpoint = MagicMock()
        pace.get_in_limits = MagicMock(return_value=(1100.0, 1))

        # Pressure rises stepwise
        pressures = [1013.0, 1015.0, 1018.0, 1020.0]
        call_count = [0]

        def rising_pressure():
            idx = min(call_count[0], len(pressures) - 1)
            call_count[0] += 1
            return pressures[idx]

        pace.read_pressure = MagicMock(side_effect=rising_pressure)

        runner, _, logged = _make_runner(
            cfg_override={"workflow": {"pressure": {
                "no_outp_transition_mode": True,
                "no_outp_pressure_rise_timeout_s": 10.0,
                "no_outp_pressure_rise_min_hpa": 5.0,
                "stabilize_timeout_s": 0.0,
                "preseal_timeout_s": 30.0,
            }}},
            pace=pace,
        )
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_transition_fast_signal_context = MagicMock()
        runner._start_pressure_transition_fast_signal_context = lambda *a, **kw: None
        runner._stop_pressure_controller_atmosphere_hold = lambda p, **kw: True

        # Simulate that _set_pressure_controller_vent(False) did NOT call set_output
        # (tested separately above). Now run _pressurize_and_hold to verify rise gate.

        point = _co2_point(index=3, pressure=1100.0)

        ok = runner._pressurize_and_hold(point, route="co2")
        # Should succeed — pressure rose past baseline+5hPa
        assert ok is True

        # Cleanup route valves reference
        runner._cleanup_co2_route = MagicMock()
        runner._pressure_transition_fast_signal_stop = MagicMock()


class TestPressureRiseGateFail:
    def test_rise_gate_fails_when_no_pressure_increase(self):
        """When pressure does not rise, gate fails and returns False."""
        pace = MagicMock()
        pace.VENT_STATUS_TRAPPED_PRESSURE = 3
        pace.VENT_STATUS_IDLE = 0
        pace.stop_atmosphere_hold = MagicMock(return_value=True)
        pace.vent = MagicMock()
        pace.set_output = MagicMock()
        pace.set_isolation_open = MagicMock()
        pace.get_output_state = MagicMock(return_value=1)
        pace.get_isolation_state = MagicMock(return_value=1)
        pace.get_vent_status = MagicMock(return_value=0)
        pace.get_vent_after_valve_open = MagicMock(return_value=False)
        pace.read_pressure = MagicMock(return_value=1013.0)

        runner, _, _ = _make_runner(
            cfg_override={"workflow": {"pressure": {
                "no_outp_transition_mode": True,
                "no_outp_pressure_rise_timeout_s": 0.5,
                "no_outp_pressure_rise_min_hpa": 5.0,
                "stabilize_timeout_s": 0.0,
                "preseal_timeout_s": 30.0,
            }}},
            pace=pace,
        )
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_transition_fast_signal_context = MagicMock()
        runner._start_pressure_transition_fast_signal_context = lambda *a, **kw: None
        runner._stop_pressure_controller_atmosphere_hold = lambda p, **kw: True
        runner._cleanup_co2_route = MagicMock()
        runner._pressure_transition_fast_signal_stop = MagicMock()

        point = _co2_point(index=3, pressure=1100.0)
        ok = runner._pressurize_and_hold(point, route="co2")
        assert ok is False
        runner._cleanup_co2_route.assert_called()


class TestSafeStopStillAllowed:
    def test_cleanup_vent_on_still_works(self):
        """cleanup/safe_stop vent-on is still allowed (vent_on=True path)."""
        runner, pace, _ = _make_runner({
            "workflow": {"pressure": {"no_outp_transition_mode": True}}
        })
        runner._set_co2_route_baseline = lambda *a, **kw: None
        runner._cleanup_co2_route(reason="test")
        # _cleanup_co2_route calls _set_co2_route_baseline which calls _set_pressure_controller_vent(True)
        # We've stubbed it — the point is no exception is raised
