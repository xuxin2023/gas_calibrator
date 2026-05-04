import time
from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _PressureReader:
    def __init__(self, values):
        self.values = list(values)
        self.calls = 0

    def read_pressure(self):
        self.calls += 1
        if self.values:
            return float(self.values.pop(0))
        return 1000.0


class _FakePaceForConfigure:
    def __init__(self):
        self.calls = []

    def set_units_hpa(self):
        self.calls.append(("units_hpa",))

    def set_output_mode_active(self):
        self.calls.append(("mode_active",))

    def set_output_mode_passive(self):
        self.calls.append(("mode_passive",))

    def set_slew_mode_linear(self):
        self.calls.append(("slew_linear",))

    def set_slew_rate(self, value):
        self.calls.append(("slew_rate", float(value)))

    def set_overshoot_allowed(self, enabled):
        self.calls.append(("overshoot", bool(enabled)))

    def set_in_limits(self, pct, time_s):
        self.calls.append(("in_limits", float(pct), float(time_s)))


class _FakePaceUnsupportedSoft:
    def __init__(self):
        self.calls = []

    def set_units_hpa(self):
        self.calls.append(("units_hpa",))

    def set_output_mode_active(self):
        self.calls.append(("mode_active",))

    def set_in_limits(self, pct, time_s):
        self.calls.append(("in_limits", float(pct), float(time_s)))


def _co2_point() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=400.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=800.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def _prime_sealed_runtime(runner: CalibrationRunner, point: CalibrationPoint, *, phase: str = "co2") -> None:
    now = time.time()
    runner._set_point_runtime_fields(
        point,
        phase=phase,
        timing_stages={
            "route_sealed": now - 2.0,
            "pressure_in_limits": now - 1.0,
        },
    )


def test_adaptive_pressure_sampling_flag_off_keeps_old_wait_path(tmp_path: Path) -> None:
    logs = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"adaptive_pressure_sampling_enabled": False, "co2_post_stable_sample_delay_s": 0.0}}},
        {},
        logger,
        logs.append,
        lambda *_: None,
    )
    point = _co2_point()
    _prime_sealed_runtime(runner, point)
    runner._set_pressure_controller_sampling_isolation = lambda _point, **_kwargs: True
    runner._wait_post_isolation_leak_test = lambda _point, **_kwargs: True
    runner._wait_sampling_pressure_gate = lambda _point, **_kwargs: True
    runner._wait_postseal_dewpoint_gate = lambda _point, **_kwargs: True
    runner._wait_co2_presample_long_guard = lambda _point, **_kwargs: True

    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()


def test_wait_after_pressure_stable_uses_adaptive_gate_when_enabled(tmp_path: Path) -> None:
    logs = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "adaptive_pressure_sampling_enabled": True,
                    "skip_fixed_post_stable_delay_when_adaptive": True,
                    "co2_post_stable_sample_delay_s": 60.0,
                }
            }
        },
        {},
        logger,
        logs.append,
        lambda *_: None,
    )
    calls = {"count": 0}
    point = _co2_point()
    _prime_sealed_runtime(runner, point)
    runner._set_pressure_controller_sampling_isolation = lambda _point, **_kwargs: True
    runner._wait_post_isolation_leak_test = lambda _point, **_kwargs: True
    runner._wait_sampling_pressure_gate = lambda _point, **_kwargs: calls.__setitem__("count", calls["count"] + 1) or True
    runner._wait_postseal_dewpoint_gate = lambda _point, **_kwargs: True
    runner._wait_co2_presample_long_guard = lambda _point, **_kwargs: True
    runner._set_pressure_to_target = lambda point, recovery_attempted=False: (_ for _ in ()).throw(
        AssertionError("adaptive wait path should not re-capture pressure here")
    )

    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    assert calls["count"] == 1
    assert any("engineering-only" in msg.lower() and "non-default" in msg.lower() for msg in logs)
    assert any("adaptive pressure-sampling config enabled" in msg.lower() for msg in logs)


def test_adaptive_pressure_gate_success_uses_joint_window_config(tmp_path: Path) -> None:
    logs = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "adaptive_pressure_sampling_enabled": True,
                    "use_pressure_gauge_for_sampling_gate": True,
                    "sampling_gate_poll_s": 0.25,
                    "co2_sampling_gate_window_s": 8.0,
                    "co2_sampling_gate_pressure_span_hpa": 0.2,
                    "co2_sampling_gate_pressure_fill_s": 5.0,
                    "co2_sampling_gate_min_samples": 6,
                }
            }
        },
        {},
        logger,
        logs.append,
        lambda *_: None,
    )
    captured = {}

    def fake_wait(point, **kwargs):
        captured.update(kwargs)
        return True

    runner._wait_primary_sensor_stable = fake_wait

    assert runner._wait_pressure_and_primary_sensor_ready(_co2_point()) is True
    logger.close()

    assert captured["value_key"] == "co2_ratio_f"
    assert captured["require_pressure_in_limits"] is True
    assert captured["window_override"] == 8.0
    assert captured["min_samples_override"] == 6
    assert captured["read_interval_override"] == 0.25
    assert captured["pressure_fill_override"] == 5.0
    assert captured["pressure_window_cfg"]["pressure_span_hpa"] == 0.2


def test_adaptive_pressure_gate_failure_returns_false(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"adaptive_pressure_sampling_enabled": True}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._wait_primary_sensor_stable = lambda point, **kwargs: False

    assert runner._wait_pressure_and_primary_sensor_ready(_co2_point()) is False
    logger.close()


def test_sampling_gate_pressure_reader_falls_back_to_pace(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _PressureReader([812.3])
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"adaptive_pressure_sampling_enabled": True}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    value, source = runner._read_best_pressure_for_sampling_gate(True)
    logger.close()

    assert value == 812.3
    assert source == "pace"
    assert pace.calls == 1


def test_adaptive_mode_does_not_reapply_setpoint_in_wait_path(tmp_path: Path) -> None:
    logs = []
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "adaptive_pressure_sampling_enabled": True,
                    "skip_fixed_post_stable_delay_when_adaptive": True,
                }
            }
        },
        {},
        logger,
        logs.append,
        lambda *_: None,
    )
    recaptures = {"count": 0}
    point = _co2_point()
    _prime_sealed_runtime(runner, point)
    runner._set_pressure_to_target = lambda point, recovery_attempted=False: recaptures.__setitem__(
        "count", recaptures["count"] + 1
    ) or True
    runner._set_pressure_controller_sampling_isolation = lambda _point, **_kwargs: True
    runner._wait_post_isolation_leak_test = lambda _point, **_kwargs: True
    runner._wait_sampling_pressure_gate = lambda _point, **_kwargs: True
    runner._wait_postseal_dewpoint_gate = lambda _point, **_kwargs: True
    runner._wait_co2_presample_long_guard = lambda _point, **_kwargs: True

    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    assert recaptures["count"] == 0


def test_fast5s_capture_can_sample_immediately_when_override_allows_early_sample(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "adaptive_pressure_sampling_enabled": True,
                    "skip_fixed_post_stable_delay_when_adaptive": True,
                    "capture_then_hold_enabled": True,
                    "co2_post_isolation_diagnostic_enabled": True,
                    "post_isolation_fast_capture_enabled": True,
                    "post_isolation_fast_capture_allow_early_sample": True,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    _prime_sealed_runtime(runner, point)
    runner._set_pressure_controller_sampling_isolation = lambda _point, **_kwargs: True

    def _fast_pass(_point, **_kwargs):
        runner._set_point_runtime_fields(
            point,
            phase="co2",
            post_isolation_capture_mode="fast5s",
            post_isolation_fast_capture_status="pass",
            post_isolation_fast_capture_reason="clean_window",
            post_isolation_fast_capture_fallback=False,
        )
        return True

    runner._wait_post_isolation_leak_test = _fast_pass
    runner._wait_sampling_pressure_gate = lambda _point, **_kwargs: (_ for _ in ()).throw(
        AssertionError("fast 5s early sample should skip extended gate chain")
    )
    runner._wait_postseal_dewpoint_gate = runner._wait_sampling_pressure_gate
    runner._wait_co2_presample_long_guard = runner._wait_sampling_pressure_gate

    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["post_isolation_capture_mode"] == "fast5s"
    assert state["post_isolation_fast_capture_status"] == "pass"
    assert state["timing_stages"]["sampling_begin"] is not None
    logger.close()


def test_pressure_point_order_unchanged_with_adaptive_flag(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"adaptive_pressure_sampling_enabled": True}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    points = [
        CalibrationPoint(index=1, temp_chamber_c=20.0, co2_ppm=200.0, hgen_temp_c=None, hgen_rh_pct=None, target_pressure_hpa=500.0, dewpoint_c=None, h2o_mmol=None, raw_h2o=None),
        CalibrationPoint(index=2, temp_chamber_c=20.0, co2_ppm=200.0, hgen_temp_c=None, hgen_rh_pct=None, target_pressure_hpa=1100.0, dewpoint_c=None, h2o_mmol=None, raw_h2o=None),
        CalibrationPoint(index=3, temp_chamber_c=20.0, co2_ppm=200.0, hgen_temp_c=None, hgen_rh_pct=None, target_pressure_hpa=800.0, dewpoint_c=None, h2o_mmol=None, raw_h2o=None),
    ]

    ordered = runner._co2_pressure_points_for_temperature(points)
    logger.close()

    assert [int(point.target_pressure_hpa or 0) for point in ordered] == [1100, 800, 500]


def test_soft_control_flag_off_keeps_default_pace_setup(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceForConfigure()
    runner = CalibrationRunner(
        {"devices": {"pressure_controller": {"in_limits_pct": 0.02, "in_limits_time_s": 10}}, "workflow": {"pressure": {"soft_control_enabled": False}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    runner._configure_devices()
    logger.close()

    assert pace.calls == [
        ("mode_active",),
        ("in_limits", 0.02, 10.0),
    ]


def test_soft_control_unsupported_commands_warn_only(tmp_path: Path) -> None:
    logs = []
    logger = RunLogger(tmp_path)
    pace = _FakePaceUnsupportedSoft()
    runner = CalibrationRunner(
        {
            "devices": {"pressure_controller": {"in_limits_pct": 0.02, "in_limits_time_s": 10}},
            "workflow": {
                "pressure": {
                    "soft_control_enabled": True,
                    "soft_control_use_active_mode": True,
                    "soft_control_linear_slew_hpa_per_s": 3.0,
                    "soft_control_disallow_overshoot": True,
                }
            },
        },
        {"pace": pace},
        logger,
        logs.append,
        lambda *_: None,
    )

    runner._configure_devices()
    logger.close()

    assert ("mode_active",) in pace.calls
    assert ("in_limits", 0.02, 10.0) in pace.calls
    assert any("engineering-only" in msg.lower() and "non-default" in msg.lower() for msg in logs)
    assert any("pressure soft-control config enabled" in msg.lower() for msg in logs)
    assert any("unsupported" in msg.lower() for msg in logs)


def test_same_gas_low_pressure_standard_control_uses_linear_slew_and_disables_overshoot(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceForConfigure()
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "same_gas_low_pressure_standard_control_enabled": True,
                    "same_gas_low_pressure_standard_control_slew_hpa_per_s": 5.0,
                    "low_pressure_same_gas_use_linear_slew": True,
                    "low_pressure_same_gas_overshoot_allowed": False,
                }
            }
        },
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    runner._route_signature_for_point = lambda *_args, **_kwargs: (7, 11)
    runner._last_sealed_pressure_route_context = {"phase": "co2", "route_signature": (7, 11)}

    runner._configure_same_gas_low_pressure_standard_control(pace, point, phase="co2")
    logger.close()

    assert ("slew_linear",) in pace.calls
    assert ("slew_rate", 5.0) in pace.calls
    assert ("overshoot", False) in pace.calls


def test_pressure_control_ready_fails_legacy_watchlist_status_3_with_explicit_reason(tmp_path: Path) -> None:
    class _FakeLegacyPace:
        VENT_STATUS_TRAPPED_PRESSURE = 3

        def has_legacy_vent_state_3_compatibility(self):
            return True

        def read_pressure(self):
            return 1001.2

    logs = []
    logger = RunLogger(tmp_path)
    pace = _FakeLegacyPace()
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"control_ready_wait_timeout_s": 0.0}}},
        {"pace": pace},
        logger,
        logs.append,
        lambda *_: None,
    )
    point = _co2_point()
    runner._pressure_controller_ready_snapshot = lambda _pace, **_kwargs: {
        "pace_vent_status": 3,
        "pace_output_state": 0,
        "pace_isolation_state": 1,
        "hold_thread_active": False,
    }
    snapshot = runner._pressure_controller_ready_snapshot(pace)
    failures = runner._pressure_controller_ready_failures(snapshot, pace)

    assert runner._ensure_pressure_controller_ready_for_control(
        point,
        phase="co2",
        pressure_target_hpa=800.0,
        attempt_recovery=False,
        note="hotfix probe",
    ) is False
    logger.close()

    assert failures == ["vent_status=3(watchlist_only)"]
    assert any("vent_status=3(watchlist_only)" in msg for msg in logs)
