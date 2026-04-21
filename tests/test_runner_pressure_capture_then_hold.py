import time
from pathlib import Path

import pytest

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


class _FakePaceForSamplingIsolation:
    def __init__(self):
        self.calls = []
        self.output_state = 1
        self.isolation_state = 1
        self.vent_status = 0

    def set_output_enabled_verified(self, enabled):
        self.calls.append(("output_verified", bool(enabled)))
        self.output_state = 1 if enabled else 0

    def set_output_isolated_verified(self, isolated):
        self.calls.append(("isolated_verified", bool(isolated)))
        self.isolation_state = 0 if isolated else 1

    def get_output_state(self):
        return self.output_state

    def get_isolation_state(self):
        return self.isolation_state

    def get_vent_status(self):
        return self.vent_status

    def vent_status_allows_control(self, status):
        return int(status) == 0


class _FakeLegacyPaceForSamplingIsolation(_FakePaceForSamplingIsolation):
    def __init__(self):
        super().__init__()
        self.vent_status = 3

    def has_legacy_vent_state_3_compatibility(self):
        return True

    def vent_status_allows_control(self, status):
        return int(status) == 0


def _co2_point(pressure_hpa: float = 800.0, *, index: int = 1) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=400.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure_hpa,
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


@pytest.mark.parametrize(
    "handoff_mode",
    [
        "same_gas_pressure_step_handoff",
        "same_gas_superambient_precharge_handoff",
    ],
)
def test_sampling_isolation_requires_output_off_and_isol_closed(
    tmp_path: Path,
    handoff_mode: str,
) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceForSamplingIsolation()
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": True, "co2_output_off_hold_s": 0.0}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    _prime_sealed_runtime(runner, point)
    runner._observe_pressure_hold_after_output_off = lambda _point: (
        True,
        {
            "source": "pace",
            "span_hpa": 0.02,
            "max_abs_drift_hpa": 0.02,
            "limit_hpa": 0.25,
            "samples": 2,
        },
    )

    assert runner._set_pressure_controller_sampling_isolation(
        point,
        phase="co2",
        context=None,
        handoff_mode=handoff_mode,
    ) is True
    logger.close()

    assert pace.calls == [
        ("output_verified", False),
        ("isolated_verified", True),
    ]
    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["capture_hold_status"] == "pass"
    assert runtime_state["pace_output_state"] == 0
    assert runtime_state["pace_isolation_state"] == 0


@pytest.mark.parametrize(
    "handoff_mode",
    [
        "same_gas_pressure_step_handoff",
        "same_gas_superambient_precharge_handoff",
    ],
)
def test_sampling_isolation_blocks_legacy_watchlist_status_3(
    tmp_path: Path,
    handoff_mode: str,
) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakeLegacyPaceForSamplingIsolation()
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": True, "co2_output_off_hold_s": 0.0}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    _prime_sealed_runtime(runner, point)
    runner._observe_pressure_hold_after_output_off = lambda _point: (
        True,
        {
            "source": "pace",
            "span_hpa": 0.02,
            "max_abs_drift_hpa": 0.02,
            "limit_hpa": 0.25,
            "samples": 2,
        },
    )

    assert runner._set_pressure_controller_sampling_isolation(
        point,
        phase="co2",
        context=None,
        handoff_mode=handoff_mode,
    ) is False
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["capture_hold_status"] == "fail"
    assert runtime_state["capture_hold_reason"] == "pace_vent_status_not_terminal:3"
    assert runtime_state["pace_vent_status_query"] == 3
    assert runtime_state["pace_atmosphere_connected_latched_state_suspect"] is True
    assert runtime_state["vent3_hard_blocked"] is True
    assert runtime_state["vent3_watchlist_only"] is True
    assert runtime_state["vent3_control_ready_attempted"] is True
    assert runtime_state["vent3_control_ready_prevented"] is True
    assert runtime_state["vent3_block_scope"] == "sampling_capture"


def test_sampling_isolation_skips_when_capture_then_hold_disabled(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceForSamplingIsolation()
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": False}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    _prime_sealed_runtime(runner, point)
    runner._observe_pressure_hold_after_output_off = lambda _point: pytest.fail("capture hold should be skipped when disabled")

    assert runner._set_pressure_controller_sampling_isolation(
        point,
        phase="co2",
        context=None,
        handoff_mode="same_gas_pressure_step_handoff",
    ) is True
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert pace.calls == []
    assert runtime_state["capture_hold_status"] == "skipped"
    assert runtime_state["capture_hold_reason"] == "capture_then_hold_disabled"


@pytest.mark.parametrize(
    "handoff_mode",
    [
        "same_gas_pressure_step_handoff",
        "same_gas_superambient_precharge_handoff",
    ],
)
def test_low_pressure_sampling_rejects_when_atmosphere_refresh_detected(
    tmp_path: Path,
    handoff_mode: str,
) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceForSamplingIsolation()
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": True, "co2_output_off_hold_s": 0.0}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point(pressure_hpa=800.0)
    _prime_sealed_runtime(runner, point)
    runner._atmosphere_reference_hpa = 1013.25
    runner._last_pressure_atmosphere_refresh_ts = time.time()
    runner._observe_pressure_hold_after_output_off = lambda _point: (
        True,
        {
            "source": "pace",
            "span_hpa": 0.01,
            "max_abs_drift_hpa": 0.01,
            "limit_hpa": 0.25,
            "samples": 2,
        },
    )

    assert runner._set_pressure_controller_sampling_isolation(
        point,
        phase="co2",
        context=None,
        handoff_mode=handoff_mode,
    ) is False
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["capture_hold_status"] == "fail"
    assert runtime_state["root_cause_reject_reason"] == "ambient_ingress_suspect"


def test_same_gas_follow_on_point_reuses_route_sealed_evidence(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceForSamplingIsolation()
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": True, "co2_output_off_hold_s": 0.0}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    sealed_point = _co2_point(pressure_hpa=1000.0, index=1)
    follow_on_point = _co2_point(pressure_hpa=800.0, index=2)
    _prime_sealed_runtime(runner, sealed_point)
    runner._set_point_runtime_fields(
        sealed_point,
        phase="co2",
        timing_stages={
            "route_open": time.time() - 10.0,
            "soak_begin": time.time() - 9.0,
            "soak_end": time.time() - 5.0,
            "preseal_vent_off_begin": time.time() - 4.0,
            "preseal_trigger_reached": time.time() - 3.0,
            "route_sealed": time.time() - 2.0,
            "pressure_in_limits": time.time() - 1.0,
        },
    )
    runner._set_point_runtime_fields(
        follow_on_point,
        phase="co2",
        timing_stages={"pressure_in_limits": time.time() - 0.5},
    )
    runner._last_sealed_pressure_route_context = {
        "phase": "co2",
        "route_signature": runner._route_signature_for_point(sealed_point, phase="co2"),
        "point_row": sealed_point.index,
    }
    runner._observe_pressure_hold_after_output_off = lambda _point: (
        True,
        {
            "source": "pace",
            "span_hpa": 0.02,
            "max_abs_drift_hpa": 0.02,
            "limit_hpa": 0.25,
            "samples": 2,
        },
    )

    handoff_mode = runner._prepare_sampling_handoff_mode(follow_on_point, phase="co2")
    assert handoff_mode == "same_gas_pressure_step_handoff"
    assert runner._set_pressure_controller_sampling_isolation(
        follow_on_point,
        phase="co2",
        context=None,
        handoff_mode=handoff_mode,
    ) is True
    logger.close()

    follow_on_state = runner._point_runtime_state(follow_on_point, phase="co2") or {}
    timing_stages = dict(follow_on_state.get("timing_stages") or {})
    assert timing_stages["route_sealed"] is not None
    assert timing_stages["route_open"] is not None
    assert follow_on_state["capture_hold_status"] == "pass"


@pytest.mark.parametrize(
    "handoff_mode",
    [
        "same_gas_pressure_step_handoff",
        "same_gas_superambient_precharge_handoff",
    ],
)
def test_wait_after_pressure_stable_runs_capture_hold_then_pressure_and_dewpoint_gates(
    tmp_path: Path,
    handoff_mode: str,
) -> None:
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
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    _prime_sealed_runtime(runner, point)
    runner._set_point_runtime_fields(point, phase="co2", handoff_mode=handoff_mode)
    order = []
    runner._set_pressure_controller_sampling_isolation = (
        lambda _point, **_kwargs: order.append("capture_hold") or True
    )
    runner._wait_post_isolation_leak_test = lambda _point, **_kwargs: order.append("post_isolation_test") or True
    runner._wait_sampling_pressure_gate = lambda _point, **_kwargs: order.append("pressure_gate") or True
    runner._wait_postseal_dewpoint_gate = lambda _point, **_kwargs: order.append("dewpoint_gate") or True
    runner._wait_co2_presample_long_guard = lambda _point, **_kwargs: order.append("presample_guard") or True

    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    assert order == ["capture_hold", "post_isolation_test", "pressure_gate", "dewpoint_gate", "presample_guard"]


def test_wait_after_pressure_stable_uses_remaining_fixed_delay_when_adaptive_skip_disabled(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "adaptive_pressure_sampling_enabled": True,
                    "skip_fixed_post_stable_delay_when_adaptive": False,
                    "co2_post_stable_sample_delay_s": 10.0,
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
    runtime_state = runner._point_runtime_state(point, phase="co2")
    runtime_state["timing_stages"]["pressure_in_limits"] = time.time() - 3.0
    waits = []
    runner._set_pressure_controller_sampling_isolation = lambda _point, **_kwargs: True
    runner._wait_post_isolation_leak_test = lambda _point, **_kwargs: True
    runner._wait_sampling_pressure_gate = lambda _point, **_kwargs: True
    runner._wait_postseal_dewpoint_gate = lambda _point, **_kwargs: True
    runner._wait_co2_presample_long_guard = lambda _point, **_kwargs: True
    runner._sampling_window_wait = lambda seconds, stop_event=None: waits.append(float(seconds)) or True

    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    assert len(waits) == 1
    assert 6.0 <= waits[0] <= 8.5


def test_wait_after_pressure_stable_rejects_when_capture_hold_fails(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"adaptive_pressure_sampling_enabled": True}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    _prime_sealed_runtime(runner, point)

    def _fail_capture(_point, **_kwargs):
        runner._set_point_runtime_fields(
            point,
            phase="co2",
            capture_hold_status="fail",
            capture_hold_reason="hold_drift_exceeded",
            root_cause_reject_reason="controller_hunting_suspect",
        )
        return False

    runner._set_pressure_controller_sampling_isolation = _fail_capture
    runner._wait_sampling_pressure_gate = lambda _point, **_kwargs: (_ for _ in ()).throw(
        AssertionError("pressure gate must not run after capture_hold failure")
    )

    assert runner._wait_after_pressure_stable_before_sampling(point) is False
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["root_cause_reject_reason"] == "controller_hunting_suspect"


def test_wait_after_pressure_stable_rejects_rebound_veto_as_adsorption_tail(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"adaptive_pressure_sampling_enabled": True}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    _prime_sealed_runtime(runner, point)
    runner._set_pressure_controller_sampling_isolation = lambda _point, **_kwargs: True
    runner._wait_post_isolation_leak_test = lambda _point, **_kwargs: True
    runner._wait_sampling_pressure_gate = lambda _point, **_kwargs: True

    def _fail_dewpoint(_point, **_kwargs):
        runner._set_point_runtime_fields(
            point,
            phase="co2",
            dewpoint_gate_result="rebound_veto",
            pressure_dew_sync_status="independent",
        )
        return False

    runner._wait_postseal_dewpoint_gate = _fail_dewpoint
    runner._wait_co2_presample_long_guard = lambda _point, **_kwargs: True

    assert runner._wait_after_pressure_stable_before_sampling(point) is False
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["root_cause_reject_reason"] == "dead_volume_wet_release_suspect"


def test_wait_after_pressure_stable_rejects_hunting_before_sampling(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"adaptive_pressure_sampling_enabled": True}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    _prime_sealed_runtime(runner, point)
    runner._set_pressure_controller_sampling_isolation = lambda _point, **_kwargs: True
    runner._wait_post_isolation_leak_test = lambda _point, **_kwargs: True

    def _fail_pressure_gate(_point, **_kwargs):
        runner._set_point_runtime_fields(
            point,
            phase="co2",
            pressure_gate_status="fail",
            pressure_gate_reason="span_exceeded",
            pressure_dew_sync_status="synchronous",
            root_cause_reject_reason="controller_hunting_suspect",
        )
        return False

    runner._wait_sampling_pressure_gate = _fail_pressure_gate
    runner._wait_postseal_dewpoint_gate = lambda _point, **_kwargs: True
    runner._wait_co2_presample_long_guard = lambda _point, **_kwargs: True

    assert runner._wait_after_pressure_stable_before_sampling(point) is False
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["root_cause_reject_reason"] == "controller_hunting_suspect"
    assert runner._presample_lock_state is None


@pytest.mark.parametrize(
    "handoff_mode",
    [
        "same_gas_pressure_step_handoff",
        "same_gas_superambient_precharge_handoff",
    ],
)
def test_presample_lock_blocks_vent_on_before_sampling_begin(tmp_path: Path, handoff_mode: str) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()

    runner._arm_presample_sampling_lock(
        point,
        phase="co2",
        handoff_mode=handoff_mode,
    )

    with pytest.raises(RuntimeError, match="presample_lock_violation:vent_on"):
        runner._set_pressure_controller_vent(True, reason="forbidden test vent")
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["root_cause_reject_reason"] == "ambient_ingress_suspect"
    assert runner._presample_lock_state is None


@pytest.mark.parametrize(
    "handoff_mode",
    [
        "same_gas_pressure_step_handoff",
        "same_gas_superambient_precharge_handoff",
    ],
)
def test_presample_lock_blocks_output_enable_before_sampling_begin(tmp_path: Path, handoff_mode: str) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()

    runner._arm_presample_sampling_lock(
        point,
        phase="co2",
        handoff_mode=handoff_mode,
    )

    with pytest.raises(RuntimeError, match="presample_lock_violation:output_enable"):
        runner._enable_pressure_controller_output(reason="forbidden test output")
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["root_cause_reject_reason"] == "controller_hunting_suspect"
    assert runner._presample_lock_state is None


@pytest.mark.parametrize(
    "handoff_mode",
    [
        "same_gas_pressure_step_handoff",
        "same_gas_superambient_precharge_handoff",
    ],
)
def test_presample_lock_blocks_route_reopen_before_sampling_begin(tmp_path: Path, handoff_mode: str) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()

    runner._arm_presample_sampling_lock(
        point,
        phase="co2",
        handoff_mode=handoff_mode,
    )

    with pytest.raises(RuntimeError, match="presample_lock_violation:route_reopen"):
        runner._apply_valve_states([1, 2, 3])
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["root_cause_reject_reason"] == "ambient_ingress_suspect"
    assert runner._presample_lock_state is None


def test_wait_after_pressure_stable_clears_presample_lock_on_sampling_begin(tmp_path: Path) -> None:
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
        lambda *_: None,
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

    assert runner._presample_lock_state is None


def test_sampling_begin_blocks_vent_during_sampling_under_pressure(tmp_path: Path) -> None:
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
        lambda *_: None,
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

    with pytest.raises(RuntimeError, match="sealed_no_vent_guard_violation:vent_on"):
        runner._set_pressure_controller_vent(True, reason="forbidden during sampling under pressure")
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["sealed_no_vent_guard_active"] is True
    assert state["sealed_no_vent_guard_phase"] == "SamplingUnderPressure"


def test_output_off_hold_falls_back_to_pace_when_gauge_missing(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _PressureReader([805.5])
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": True}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    value, source = runner._read_best_pressure_for_output_off_hold(True)
    logger.close()

    assert value == 805.5
    assert source == "pace"
    assert pace.calls == 1


def test_pressure_point_order_remains_high_to_low_with_capture_hold_enabled(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"capture_then_hold_enabled": True}}},
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
