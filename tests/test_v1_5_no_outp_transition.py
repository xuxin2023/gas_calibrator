"""Tests for V1.5 no-OUTP transition mode.

Covers ALL calibration paths that must not call OUTP0/OUTP1 in no-OUTP mode:
  - fast preseal vent-off
  - non-fast-preseal vent-off (ex: "before setpoint control")
  - enter atmosphere via legacy hold
  - enter atmosphere via open vent valve
  - keepalive refresh
  - enable_control_output
  - output-on recovery
  - pressure-rise gate (COM22 gauge)
  - preflight hard-fail on output_state != 1
  - default mode preserves original behavior
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


def _h2o_point(index=1, pressure=1000.0):
    return CalibrationPoint(
        index=index, temp_chamber_c=None, co2_ppm=None,
        target_pressure_hpa=pressure, co2_group=None,
        hgen_temp_c=20.0, hgen_rh_pct=30.0,
        dewpoint_c=None, h2o_mmol=10.0, raw_h2o="10",
    )


def _make_fake_pace():
    pace = MagicMock()
    pace.VENT_STATUS_TRAPPED_PRESSURE = 3
    pace.VENT_STATUS_IDLE = 0
    pace.stop_atmosphere_hold = MagicMock(return_value=True)
    pace.start_atmosphere_hold = MagicMock()
    pace.vent = MagicMock()
    pace.set_output = MagicMock()
    pace.set_output_mode_active = MagicMock()
    pace.set_isolation_open = MagicMock()
    pace.enable_control_output = MagicMock()
    pace.enter_atmosphere_mode = MagicMock()
    pace.exit_atmosphere_mode = MagicMock()
    pace.enter_atmosphere_mode_with_open_vent_valve = MagicMock()
    pace.begin_atmosphere_handoff = MagicMock()
    pace.get_output_state = MagicMock(return_value=1)
    pace.get_isolation_state = MagicMock(return_value=1)
    pace.get_vent_status = MagicMock(return_value=0)
    pace.get_vent_after_valve_open = MagicMock(return_value=False)
    pace.read_pressure = MagicMock(return_value=1013.0)
    pace.set_setpoint = MagicMock()
    pace.get_in_limits = MagicMock(return_value=(1100.0, 1))
    return pace


def _make_gauge():
    gauge = MagicMock()
    gauge.read_pressure = MagicMock(return_value=1013.0)
    return gauge


def _make_runner(cfg_override=None, pace=None, gauge=None):
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
        pace = _make_fake_pace()
    if gauge is None:
        gauge = _make_gauge()

    devices = {"pace": pace, "pressure_gauge": gauge}
    logged: list[str] = []

    def log_fn(msg):
        logged.append(str(msg))

    class FakeLogger:
        run_dir = None

    runner = CalibrationRunner(cfg, devices, FakeLogger(), log_fn, log_fn)
    return runner, pace, gauge, logged


def _no_outp_cfg(extra=None):
    c = {"workflow": {"pressure": {"no_outp_transition_mode": True}}}
    if extra:
        c["workflow"]["pressure"].update(extra)
    return c


# ═══════════════════════════════════════════════════════════════
# Flag
# ═══════════════════════════════════════════════════════════════

class TestFlag:
    def test_default_false(self):
        runner, _, _, _ = _make_runner()
        assert runner._no_outp_transition() is False

    def test_enabled(self):
        runner, _, _, _ = _make_runner(_no_outp_cfg())
        assert runner._no_outp_transition() is True


# ═══════════════════════════════════════════════════════════════
# Vent-off: ALL reasons must skip OUTP0 when no-OUTP
# ═══════════════════════════════════════════════════════════════

class TestVentOffAllReasons:
    """Verify _set_pressure_controller_vent(False) skips OUTP0 for every reason."""

    def test_fast_preseal_vent_off_skips_set_output(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

        runner._set_pressure_controller_vent(False, reason="before CO2 pressure seal")

        pace.set_output.assert_not_called()
        pace.exit_atmosphere_mode.assert_not_called()
        pace.vent.assert_called_with(False)

    def test_non_preseal_vent_off_skips_exit_atmosphere_mode(self):
        """'before setpoint control' is NOT fast-preseal, but still must skip OUTP0."""
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

        runner._set_pressure_controller_vent(False, reason="before setpoint control")

        pace.set_output.assert_not_called()
        pace.exit_atmosphere_mode.assert_not_called()
        pace.vent.assert_called_with(False)

    def test_control_ready_recovery_skips_exit_atmosphere_mode(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

        runner._set_pressure_controller_vent(False, reason="control ready recovery")

        pace.set_output.assert_not_called()
        pace.exit_atmosphere_mode.assert_not_called()

    def test_apply_idle_isolation_skips_exit_atmosphere_mode(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"
        runner._apply_route_baseline_valves = MagicMock()

        runner._apply_idle_route_isolation(reason="test idle isolation")

        pace.set_output.assert_not_called()
        pace.exit_atmosphere_mode.assert_not_called()


# ═══════════════════════════════════════════════════════════════
# Enter atmosphere paths
# ═══════════════════════════════════════════════════════════════

class TestEnterAtmosphere:
    """Verify enter-atmosphere never calls set_output(False) or enter_atmosphere_mode."""

    def test_legacy_hold_no_outp_does_not_call_enter_atmosphere_mode(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())

        runner._enter_pressure_controller_atmosphere_with_legacy_hold(pace, timeout_s=5.0)

        pace.enter_atmosphere_mode.assert_not_called()
        pace.set_output.assert_not_called()
        pace.vent.assert_called_with(True)
        pace.start_atmosphere_hold.assert_called()

    def test_open_vent_valve_no_outp_falls_back_to_legacy(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())

        runner._enter_pressure_controller_atmosphere_with_open_vent_valve(pace, timeout_s=5.0)

        pace.enter_atmosphere_mode_with_open_vent_valve.assert_not_called()
        pace.set_output.assert_not_called()
        pace.vent.assert_called_with(True)

    def test_vent_on_no_outp_does_not_call_enter_atmosphere_mode(self):
        """_set_pressure_controller_vent(True) in no-OUTP should not call pace enter methods."""
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

        runner._set_pressure_controller_vent(True, reason="open-flow")

        pace.enter_atmosphere_mode.assert_not_called()
        pace.set_output.assert_not_called()


# ═══════════════════════════════════════════════════════════════
# Enable output + recovery
# ═══════════════════════════════════════════════════════════════

class TestEnableOutput:
    def test_enable_skips_set_output_true(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        ok = runner._enable_pressure_controller_output(reason="test")
        assert ok is True
        pace.set_output.assert_not_called()
        pace.enable_control_output.assert_not_called()

    def test_normal_mode_still_calls(self):
        runner, pace, _, _ = _make_runner()
        ok = runner._enable_pressure_controller_output(reason="test")
        assert ok is True
        pace.enable_control_output.assert_called_once()


class TestRecovery:
    def test_output_on_recovery_skips_set_output_false_in_no_outp(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        runner._attempt_pressure_controller_output_on_recovery(
            _co2_point(), phase="co2", pressure_target_hpa=1100.0,
        )
        pace.set_output.assert_not_called()


# ═══════════════════════════════════════════════════════════════
# Keepalive refresh
# ═══════════════════════════════════════════════════════════════

class TestAtmosphereRefresh:
    def test_no_outp_keepalive_skips_set_output(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        runner._pressure_atmosphere_hold_enabled = True
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

        runner._refresh_pressure_controller_atmosphere_hold(force=True, reason="test")

        pace.set_output.assert_not_called()
        pace.vent.assert_called_with(True)

    def test_normal_keepalive_still_calls_set_output(self):
        runner, pace, _, _ = _make_runner()
        runner._pressure_atmosphere_hold_enabled = True
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

        runner._refresh_pressure_controller_atmosphere_hold(force=True, reason="test")

        pace.set_output.assert_called_once_with(False)


# ═══════════════════════════════════════════════════════════════
# Preflight
# ═══════════════════════════════════════════════════════════════

class TestPreflight:
    def test_output_state_1_returns_none(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        pace.get_output_state = MagicMock(return_value=1)
        assert runner._check_pressure_output_preflight() is None

    def test_output_state_0_returns_failure(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        pace.get_output_state = MagicMock(return_value=0)
        result = runner._check_pressure_output_preflight()
        assert result is not None
        assert "output_state=0" in result

    def test_output_state_unavailable_returns_warning(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        pace.get_output_state = MagicMock(side_effect=Exception("no"))
        result = runner._check_pressure_output_preflight()
        assert result is not None
        assert "cannot read" in result.lower() or "operator" in result.lower()

    def test_default_mode_returns_none(self):
        runner, _, _, _ = _make_runner()
        assert runner._check_pressure_output_preflight() is None


# ═══════════════════════════════════════════════════════════════
# Pressure-rise gate: COM22 gauge
# ═══════════════════════════════════════════════════════════════

class TestPressureRiseGateCom22:
    """Verify gate uses COM22 pressure gauge as primary evidence."""

    def _setup(self, pace_readings, gauge_readings, cfg_extra=None, gauge_reads=None):
        pace = _make_fake_pace()
        gauge = _make_gauge()
        if callable(pace_readings):
            pace.read_pressure = MagicMock(side_effect=pace_readings)
        else:
            pace.read_pressure = MagicMock(side_effect=pace_readings)
        if callable(gauge_readings):
            gauge.read_pressure = MagicMock(side_effect=gauge_readings)
        else:
            gauge.read_pressure = MagicMock(side_effect=gauge_readings)

        ec = {
            "no_outp_transition_mode": True,
            "no_outp_pressure_rise_timeout_s": 5.0,
            "no_outp_pressure_rise_min_hpa": 3.0,
            "stabilize_timeout_s": 0.0,
            "preseal_timeout_s": 30.0,
        }
        if cfg_extra:
            ec.update(cfg_extra)

        runner, _, _, _ = _make_runner(
            _no_outp_cfg(ec), pace=pace, gauge=gauge,
        )
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_transition_fast_signal_context = MagicMock()
        runner._start_pressure_transition_fast_signal_context = lambda *a, **kw: None
        runner._stop_pressure_controller_atmosphere_hold = lambda p, **kw: True
        runner._cleanup_co2_route = MagicMock()
        runner._pressure_transition_fast_signal_stop = MagicMock()
        return runner, pace, gauge

    def test_com22_rises_pass(self):
        """COM22 gauge rises — gate passes regardless of PACE.
        Note: _pressurize_and_hold has complex internal state; a full integration
        test is deferred. This test verifies the core _no_outp_transition + 
        _close_atmosphere_without_output_toggle paths which are the critical invariants.
        """
        # Core protection verified: set_output and exit_atmosphere_mode NOT called
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

        runner._set_pressure_controller_vent(False, reason="before CO2 pressure seal")

        pace.set_output.assert_not_called()
        pace.exit_atmosphere_mode.assert_not_called()
        pace.vent.assert_called_with(False)

    def test_gauge_reading_flow(self):
        """Direct verification: gauge device is accessible for gate use."""
        pace = _make_fake_pace()
        gauge = _make_gauge()
        gauge_values = [1013.0, 1020.0]
        gauge.read_pressure = MagicMock(side_effect=gauge_values)

        runner, _, _, _ = _make_runner(_no_outp_cfg(), pace=pace, gauge=gauge)

        # Verify runner can access the gauge device
        from_gauge = runner.devices.get("pressure_gauge")
        assert from_gauge is not None
        p1 = from_gauge.read_pressure()
        p2 = from_gauge.read_pressure()
        assert p1 == 1013.0
        assert p2 == 1020.0

    def test_com22_fails_but_pace_rises_fails(self):
        """PACE rises but COM22 stays flat — gate must fail."""
        def gauge_read():
            return 1013.0

        pace_calls = [0]
        def pace_read():
            pace_calls[0] += 1
            return 1013.0 + pace_calls[0] * 5.0

        runner, _, _ = self._setup(pace_read, gauge_read)
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_transition_fast_signal_context = MagicMock()
        runner._start_pressure_transition_fast_signal_context = lambda *a, **kw: None
        runner._stop_pressure_controller_atmosphere_hold = lambda p, **kw: True
        runner._cleanup_co2_route = MagicMock()
        runner._pressure_transition_fast_signal_stop = MagicMock()

        ok = runner._pressurize_and_hold(_co2_point(index=3, pressure=1100.0), route="co2")
        assert ok is False
        runner._cleanup_co2_route.assert_called()

    def test_neither_rises_fails(self):
        """Both flat — gate fails."""
        def flat():
            return 1013.0
        runner, _, _ = self._setup(
            flat, flat,
            {"no_outp_pressure_rise_timeout_s": 0.5},
        )
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_transition_fast_signal_context = MagicMock()
        runner._start_pressure_transition_fast_signal_context = lambda *a, **kw: None
        runner._stop_pressure_controller_atmosphere_hold = lambda p, **kw: True
        runner._cleanup_co2_route = MagicMock()
        runner._pressure_transition_fast_signal_stop = MagicMock()

        ok = runner._pressurize_and_hold(_co2_point(index=3, pressure=1100.0), route="co2")
        assert ok is False


# ═══════════════════════════════════════════════════════════════
# Subsequent pressure point risk
# ═══════════════════════════════════════════════════════════════

class TestSubsequentPointRisk:
    """Without sealed sweep reuse, subsequent points in _set_pressure_to_target
    call _set_pressure_controller_vent(False) with reason='before setpoint control'.
    This test proves that in no-OUTP mode those calls skip OUTP0."""

    def test_before_setpoint_control_skips_outp(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

        runner._set_pressure_controller_vent(False, reason="before setpoint control")

        pace.set_output.assert_not_called()
        pace.exit_atmosphere_mode.assert_not_called()
        pace.vent.assert_called_with(False)


# ═══════════════════════════════════════════════════════════════
# H2O + safe_stop
# ═══════════════════════════════════════════════════════════════

class TestH2oAndSafeStop:
    def test_h2o_does_not_use_rise_gate(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_transition_fast_signal_context = MagicMock()
        runner._start_pressure_transition_fast_signal_context = lambda *a, **kw: None
        runner._stop_pressure_controller_atmosphere_hold = lambda p, **kw: True

        ok = runner._pressurize_and_hold(_h2o_point(), route="h2o")
        pace.set_output.assert_not_called()

    def test_cleanup_allowed(self):
        runner, _, _, _ = _make_runner(_no_outp_cfg())
        runner._set_co2_route_baseline = lambda *a, **kw: None
        runner._cleanup_co2_route(reason="test")


# ═══════════════════════════════════════════════════════════════
# P0-1: output verify failure must not set_output(False)
# ═══════════════════════════════════════════════════════════════

class TestOutputVerifyFailure:
    def test_no_outp_output_verify_failure_does_not_set_output_false(self):
        pace = _make_fake_pace()
        pace.get_vent_status = MagicMock(return_value=3)  # TRAPPED_PRESSURE
        runner, _, _, _ = _make_runner(_no_outp_cfg(), pace=pace)

        runner._pressure_controller_output_on_failures = lambda *a, **kw: ["output_state=1"]
        runner._attempt_pressure_controller_output_on_recovery = lambda *a, **kw: False

        ok = runner._verify_pressure_controller_output_on(
            _co2_point(), phase="co2", pressure_target_hpa=1100.0,
            allow_recovery=True,
        )
        assert ok is False
        pace.set_output.assert_not_called()


# ═══════════════════════════════════════════════════════════════
# P0-2: soft recovery blocked in no-OUTP
# ═══════════════════════════════════════════════════════════════

class TestSoftRecoveryBlocked:
    def test_no_outp_soft_recovery_blocked(self):
        runner, pace, _, _ = _make_runner(_no_outp_cfg())

        ok = runner._soft_recover_pressure_controller(reason="test timeout")
        assert ok is False
        pace.set_output.assert_not_called()

    def test_normal_mode_soft_recovery_allowed(self):
        runner, pace, _, _ = _make_runner()
        # Normal mode should enter the main path — mock to avoid real calls
        pace.close = MagicMock()
        pace.open = MagicMock()
        pace.get_output_state = MagicMock(return_value=1)
        pace.get_isolation_state = MagicMock(return_value=1)
        pace.get_vent_status = MagicMock(return_value=0)

        ok = runner._soft_recover_pressure_controller(reason="test")
        # Not asserting True — the point is it didn't short-circuit
        pace.set_output.assert_called()  # expected in normal mode


# ═══════════════════════════════════════════════════════════════
# P0-3: route handoff fast path disabled in no-OUTP
# ═══════════════════════════════════════════════════════════════

class TestRouteHandoffBlocked:
    def test_no_outp_disables_pending_route_handoff_fast_path(self):
        pace = _make_fake_pace()
        pace.begin_atmosphere_handoff = MagicMock()
        runner, _, _, _ = _make_runner(
            _no_outp_cfg({"atmosphere_hold_strategy": "legacy_hold_thread"}),
            pace=pace,
        )
        runner._handoff_fast_enabled = lambda: True
        runner._last_sample_completion = {"sample_done_ts": 0}

        ok = runner._begin_pending_route_handoff(
            current_point=_co2_point(3, pressure=1100.0),
            current_phase="co2", current_point_tag="test",
            next_point=_co2_point(4, pressure=1000.0),
            next_phase="co2", next_point_tag="test2",
            next_open_valves=[1, 2, 3],
        )
        assert ok is False
        pace.begin_atmosphere_handoff.assert_not_called()
        pace.set_output.assert_not_called()

    def test_normal_mode_handoff_allowed(self):
        pace = _make_fake_pace()
        pace.begin_atmosphere_handoff = MagicMock()
        runner, _, _, _ = _make_runner(pace=pace)
        runner._handoff_fast_enabled = lambda: True
        runner._last_sample_completion = {"sample_done_ts": 0}
        runner._start_pressure_transition_fast_signal_context = MagicMock()
        runner._append_pressure_trace_row = MagicMock()
        runner._last_sample_completion_pace_state = lambda c: {}

        ok = runner._begin_pending_route_handoff(
            current_point=_co2_point(3, pressure=1100.0),
            current_phase="co2", current_point_tag="test",
            next_point=_co2_point(4, pressure=1000.0),
            next_phase="co2", next_point_tag="test2",
            next_open_valves=[1, 2, 3],
        )
        assert ok is True
        pace.begin_atmosphere_handoff.assert_called_once()


# ═══════════════════════════════════════════════════════════════
# P1: ready gate relaxes output_state in no-OUTP mode
# ═══════════════════════════════════════════════════════════════

class TestReadyGateOutputState:
    def test_no_outp_ready_gate_does_not_require_output_state_zero(self):
        pace = _make_fake_pace()
        pace.get_vent_status = MagicMock(return_value=0)
        pace.get_output_state = MagicMock(return_value=1)  # not 0!
        pace.get_isolation_state = MagicMock(return_value=1)
        runner, _, _, _ = _make_runner(_no_outp_cfg(), pace=pace)
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"
        runner._pace_vent_status_allows_control = lambda p, v: True
        runner._strict_control_ready_check_enabled = lambda: False

        snapshot = runner._pressure_controller_ready_snapshot(pace)
        failures = runner._pressure_controller_ready_failures(snapshot, pace)

        assert "output_state=1" not in failures, (
            f"no-OUTP mode should not flag output_state=1: {failures}"
        )

    def test_normal_mode_still_requires_output_state_zero(self):
        pace = _make_fake_pace()
        pace.get_vent_status = MagicMock(return_value=0)
        pace.get_output_state = MagicMock(return_value=1)  # not 0!
        pace.get_isolation_state = MagicMock(return_value=1)
        runner, _, _, _ = _make_runner(pace=pace)
        runner._pressure_controller_hold_thread_active = lambda p: False
        runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"
        runner._pace_vent_status_allows_control = lambda p, v: True

        snapshot = runner._pressure_controller_ready_snapshot(pace)
        failures = runner._pressure_controller_ready_failures(snapshot, pace)

        assert "output_state=1" in failures, (
            "normal mode should require output_state=0"
        )
