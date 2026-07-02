"""Offline tests for V1.5 no-OUTP + CO2 sealed sweep reuse research."""

from __future__ import annotations

from unittest.mock import MagicMock

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.workflow.runner import CalibrationRunner


def _co2_point(index=1, pressure=1100.0, ppm=1000.0):
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=ppm,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=None,
    )


def _h2o_point(index=1, pressure=1000.0):
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=pressure,
        dewpoint_c=None,
        h2o_mmol=10.0,
        raw_h2o="10",
        co2_group=None,
    )


def _fake_pace():
    pace = MagicMock()
    pace.VENT_STATUS_IDLE = 0
    pace.VENT_STATUS_TRAPPED_PRESSURE = 3
    pace.stop_atmosphere_hold.return_value = True
    pace.get_output_state.return_value = 1
    pace.get_isolation_state.return_value = 1
    pace.get_vent_status.return_value = 0
    pace.get_vent_after_valve_open.return_value = False
    pace.read_pressure.return_value = 1015.0
    pace.get_in_limits.return_value = (1000.0, 1)
    return pace


def _fake_gauge(values=None):
    gauge = MagicMock()
    seq = list(values or [1010.0, 1020.0, 1020.0])

    def read_pressure():
        if seq:
            return seq.pop(0)
        return 1020.0

    gauge.read_pressure.side_effect = read_pressure
    return gauge


def _runner(pace=None, gauge=None, cfg_extra=None):
    cfg = {
        "paths": {"output_dir": "logs"},
        "workflow": {
            "pressure": {
                "no_outp_transition_mode": True,
                "stabilize_timeout_s": 1.0,
                "restabilize_retries": 0,
                "pressure_trace_poll_s": 0.01,
                "co2_no_topoff_vent_off_open_wait_s": 0.0,
                "no_outp_pressure_rise_min_hpa": 5.0,
            }
        },
        "valves": {
            "h2o_path": 8,
            "gas_main": 11,
            "co2_path": 7,
            "co2_map": {"1000": 6},
        },
    }
    if cfg_extra:
        for key, value in cfg_extra.items():
            cfg.setdefault(key, {}).update(value)
    devices = {
        "pace": pace or _fake_pace(),
        "pressure_gauge": gauge or _fake_gauge(),
    }

    class FakeLogger:
        run_dir = None

    logs = []
    runner = CalibrationRunner(cfg, devices, FakeLogger(), lambda msg: logs.append(str(msg)), lambda msg: None)
    runner._append_pressure_trace_row = MagicMock()
    runner._emit_stage_event = MagicMock()
    runner._check_pause = MagicMock()
    runner._stop_pressure_controller_atmosphere_hold = MagicMock(return_value=True)
    runner._update_atmosphere_reference_hpa = MagicMock()
    runner._refresh_pressure_controller_aux_state = MagicMock()
    runner._record_preseal_pressure_control_ready_state = MagicMock()
    runner._cached_ready_check_trace_values = MagicMock(return_value={})
    runner._capture_preseal_dewpoint_snapshot = MagicMock()
    runner._start_pressure_transition_fast_signal_context = MagicMock()
    runner._stop_pressure_transition_fast_signal_context = MagicMock()
    runner._pressure_transition_fast_signal_context_active = MagicMock(return_value=None)
    runner._pressure_trace_poll_s = MagicMock(return_value=0.01)
    runner._pressure_control_wait_aux_interval_s = MagicMock(return_value=999.0)
    runner._wait_after_pressure_stable_before_sampling = MagicMock(return_value=True)
    return runner, devices["pace"], devices["pressure_gauge"], logs


def test_co2_ambient_to_sealed_order_no_outp(monkeypatch):
    runner, pace, _, _ = _runner(gauge=_fake_gauge([1010.0, 1020.0]))
    point = _co2_point(pressure=900.0)
    order = []
    sleeps = []
    runner._apply_valve_states = MagicMock(side_effect=lambda valves: order.append(("close_valves", list(valves))))
    original_activate = runner._activate_co2_sealed_no_vent_guard
    runner._activate_co2_sealed_no_vent_guard = MagicMock(
        side_effect=lambda *a, **kw: (order.append(("guard", None)), original_activate(*a, **kw))
    )
    monkeypatch.setattr("time.sleep", lambda seconds: (sleeps.append(seconds), order.append(("sleep", seconds))))

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    pace.vent.assert_any_call(False)
    pace.set_output.assert_not_called()
    pace.enable_control_output.assert_not_called()
    assert ("close_valves", []) in order
    assert any(item[0] == "sleep" and item[1] >= 1.49 for item in order)
    assert order.index(("close_valves", [])) < order.index(("guard", None))
    assert runner._co2_sealed_no_vent_guard_active is True


def test_route_valves_stay_open_during_vent0_wait(monkeypatch):
    runner, _, _, _ = _runner(gauge=_fake_gauge([1010.0, 1020.0]))
    point = _co2_point(pressure=900.0)
    events = []
    runner._apply_valve_states = MagicMock(side_effect=lambda valves: events.append("close_valves"))
    monkeypatch.setattr("time.sleep", lambda seconds: events.append(f"sleep:{seconds:.1f}"))

    runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point])

    assert events[0].startswith("sleep:")
    assert "close_valves" in events[1:]


def test_sealed_no_vent_guard_active_after_route_close(monkeypatch):
    runner, _, _, _ = _runner(gauge=_fake_gauge([1010.0, 1020.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda seconds: None)

    runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point])

    runner._apply_valve_states.assert_called_with([])
    assert runner._co2_sealed_no_vent_guard_active is True


def test_no_vent_after_sealed_guard():
    runner, pace, _, _ = _runner()
    runner._activate_co2_sealed_no_vent_guard(_co2_point(), reason="test")
    pace.vent.reset_mock()

    assert runner._set_pressure_controller_vent(True, reason="should block") is False
    assert runner._set_pressure_controller_vent(False, reason="should block") is False
    pace.vent.assert_not_called()


def test_first_co2_point_not_setpoint_only_before_sealed_context():
    runner, _, _, _ = _runner()
    source = _co2_point(index=1, pressure=1100.0)
    p1 = _co2_point(index=1, pressure=1100.0)
    p2 = _co2_point(index=1, pressure=900.0)
    runner._apply_idle_route_isolation = MagicMock()
    runner._set_temperature_for_point = MagicMock(return_value=True)
    runner._capture_temperature_calibration_snapshot = MagicMock()
    runner._split_pressure_execution_points = MagicMock(return_value=([], [p1, p2]))
    runner._open_co2_route_for_conditioning = MagicMock()
    runner._wait_co2_route_soak_before_seal = MagicMock(return_value=True)
    runner._gas_route_dewpoint_gate_enabled = MagicMock(return_value=False)
    runner._wait_co2_preseal_primary_sensor_gate = MagicMock(return_value=True)
    runner._wait_cold_co2_quality_gate = MagicMock(return_value=True)
    runner._pressurize_route_for_sealed_points = MagicMock(return_value=True)
    runner._set_pressure_to_target = MagicMock(return_value=True)
    runner._set_pressure_to_target_in_active_co2_sealed_sweep = MagicMock(return_value=True)
    runner._sample_and_log = MagicMock()
    runner._cleanup_co2_route = MagicMock()

    runner._run_co2_point(source, pressure_points=[p1, p2])

    runner._set_pressure_to_target.assert_called_once()
    runner._set_pressure_to_target_in_active_co2_sealed_sweep.assert_called_once()


def test_context_created_only_after_first_sealed_sample_success():
    runner, _, _, _ = _runner()
    p1 = _co2_point(index=1, pressure=1100.0)
    p2 = _co2_point(index=1, pressure=900.0)
    runner._apply_idle_route_isolation = MagicMock()
    runner._set_temperature_for_point = MagicMock(return_value=True)
    runner._capture_temperature_calibration_snapshot = MagicMock()
    runner._split_pressure_execution_points = MagicMock(return_value=([], [p1, p2]))
    runner._open_co2_route_for_conditioning = MagicMock()
    runner._wait_co2_route_soak_before_seal = MagicMock(return_value=True)
    runner._gas_route_dewpoint_gate_enabled = MagicMock(return_value=False)
    runner._wait_co2_preseal_primary_sensor_gate = MagicMock(return_value=True)
    runner._wait_cold_co2_quality_gate = MagicMock(return_value=True)
    runner._pressurize_route_for_sealed_points = MagicMock(return_value=True)
    runner._set_pressure_to_target = MagicMock(return_value=True)
    runner._set_pressure_to_target_in_active_co2_sealed_sweep = MagicMock(return_value=True)
    runner._cleanup_co2_route = MagicMock()

    def sample_side_effect(*args, **kwargs):
        if runner._sample_and_log.call_count == 1:
            assert runner._active_co2_sealed_sweep_context is None

    runner._sample_and_log = MagicMock(side_effect=sample_side_effect)

    runner._run_co2_point(p1, pressure_points=[p1, p2])

    assert runner._active_co2_sealed_sweep_context is not None
    assert runner._sample_and_log.call_count == 2


def test_subsequent_co2_points_setpoint_only():
    runner, pace, _, _ = _runner()
    p1 = _co2_point(index=1, pressure=1100.0)
    p2 = _co2_point(index=1, pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(p1, reason="test")
    runner._begin_active_co2_sealed_sweep_context(p1)
    pace.vent.reset_mock()
    pace.set_output.reset_mock()
    pace.enable_control_output.reset_mock()

    assert runner._set_pressure_to_target_in_active_co2_sealed_sweep(p2) is True

    pace.set_setpoint.assert_called_once_with(900.0)
    pace.vent.assert_not_called()
    pace.set_output.assert_not_called()
    pace.enable_control_output.assert_not_called()


def test_context_cleared_on_pressure_failure():
    runner, _, _, _ = _runner()
    p1 = _co2_point(index=1, pressure=1100.0)
    p2 = _co2_point(index=1, pressure=900.0)
    runner._apply_idle_route_isolation = MagicMock()
    runner._set_temperature_for_point = MagicMock(return_value=True)
    runner._capture_temperature_calibration_snapshot = MagicMock()
    runner._split_pressure_execution_points = MagicMock(return_value=([], [p1, p2]))
    runner._open_co2_route_for_conditioning = MagicMock()
    runner._wait_co2_route_soak_before_seal = MagicMock(return_value=True)
    runner._gas_route_dewpoint_gate_enabled = MagicMock(return_value=False)
    runner._wait_co2_preseal_primary_sensor_gate = MagicMock(return_value=True)
    runner._wait_cold_co2_quality_gate = MagicMock(return_value=True)
    runner._pressurize_route_for_sealed_points = MagicMock(return_value=True)
    runner._set_pressure_to_target = MagicMock(return_value=True)
    runner._set_pressure_to_target_in_active_co2_sealed_sweep = MagicMock(return_value=False)
    runner._retry_co2_pressure_point_after_timeout = MagicMock()
    runner._sample_and_log = MagicMock()
    runner._cleanup_co2_route = MagicMock()

    runner._run_co2_point(p1, pressure_points=[p1, p2])

    assert runner._active_co2_sealed_sweep_context is None
    runner._retry_co2_pressure_point_after_timeout.assert_not_called()
    runner._cleanup_co2_route.assert_called()


def test_h2o_path_unchanged():
    runner, _, _, _ = _runner()
    h2o = _h2o_point()
    runner._set_pressure_controller_vent = MagicMock(return_value=True)
    runner._prepare_pressure_for_h2o = MagicMock()
    runner._prepare_humidity_generator = MagicMock()
    runner._set_temperature_for_point = MagicMock(return_value=False)
    runner._cleanup_h2o_route = MagicMock()
    runner._route_entry_context_for_h2o_group = MagicMock(return_value={})
    runner._discard_pending_route_handoff = MagicMock()

    runner._run_h2o_group([h2o], pressure_points=[h2o])

    assert runner._active_co2_sealed_sweep_context is None
    assert runner._set_pressure_to_target_in_active_co2_sealed_sweep.__name__ == "_set_pressure_to_target_in_active_co2_sealed_sweep"
