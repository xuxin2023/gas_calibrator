import json
import types
from collections import deque
from pathlib import Path

import gas_calibrator.workflow.runner as runner_module
from gas_calibrator.config import load_config
from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


ROOT = Path(__file__).resolve().parents[1]


def _point_co2() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def test_default_and_fasttrace_pressure_flags_are_conservative() -> None:
    default_cfg = load_config(ROOT / "configs" / "default_config.json")
    co2_cfg = load_config(ROOT / "configs" / "headless_real_smoke_co2_fasttrace.json")
    co2_short_cfg = load_config(ROOT / "configs" / "headless_real_smoke_co2_fasttrace_short.json")
    co2_short_noanalyzers_cfg = load_config(
        ROOT / "configs" / "headless_real_smoke_co2_fasttrace_short_noanalyzers.json"
    )
    h2o_cfg = load_config(ROOT / "configs" / "headless_real_smoke_h2o_fasttrace.json")

    for cfg in (default_cfg, co2_cfg, co2_short_cfg, co2_short_noanalyzers_cfg, h2o_cfg):
        pressure_cfg = cfg["workflow"]["pressure"]
        sampling_cfg = cfg["workflow"]["sampling"]
        assert pressure_cfg["soft_control_enabled"] is False
        assert pressure_cfg["handoff_fast_enabled"] is False
        assert pressure_cfg["soft_control_linear_slew_hpa_per_s"] == 10.0
        assert pressure_cfg["co2_postseal_dewpoint_timeout_s"] <= 6.0
        assert pressure_cfg["h2o_postseal_dewpoint_timeout_s"] <= 6.0
        assert sampling_cfg["pre_sample_freshness_timeout_s"] <= 1.5

    for cfg in (co2_short_cfg, co2_short_noanalyzers_cfg):
        sampling_cfg = cfg["workflow"]["sampling"]
        assert sampling_cfg["count"] == 10
        assert sampling_cfg["interval_s"] == 1.0
        assert sampling_cfg["co2_interval_s"] == 1.0
        assert sampling_cfg["h2o_interval_s"] == 1.0


def test_h2o_fasttrace_keeps_humidity_and_dewpoint_stability_guards() -> None:
    cfg = load_config(ROOT / "configs" / "headless_real_smoke_h2o_fasttrace.json")
    hgen_cfg = cfg["workflow"]["stability"]["humidity_generator"]
    dew_cfg = cfg["workflow"]["stability"]["dewpoint"]

    assert cfg["workflow"]["sampling"]["slow_aux_cache_enabled"] is True
    assert hgen_cfg["enabled"] is True
    assert dew_cfg["temp_match_tol_c"] < 5.0
    assert dew_cfg["rh_match_tol_pct"] < 50.0
    assert dew_cfg["window_s"] >= 40
    assert dew_cfg["stability_tol_c"] < 1.0
    assert dew_cfg["timeout_s"] >= 1800
    assert cfg["workflow"]["stability"]["h2o_route"]["preseal_soak_s"] >= 240


def test_user_tuning_does_not_reenable_aggressive_pressure_flags() -> None:
    tuning = json.loads((ROOT / "configs" / "user_tuning.json").read_text(encoding="utf-8-sig"))
    pressure_cfg = tuning["workflow"]["pressure"]

    assert pressure_cfg["soft_control_enabled"] is False
    assert pressure_cfg["handoff_fast_enabled"] is False
    assert pressure_cfg["soft_control_linear_slew_hpa_per_s"] == 10.0
    assert pressure_cfg.get("co2_postseal_dewpoint_timeout_s", 6.0) <= 6.0
    assert pressure_cfg.get("h2o_postseal_dewpoint_timeout_s", 6.0) <= 6.0


def test_preseal_ready_state_requires_target_age_and_invalidation_match(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2()
    runner._preseal_pressure_control_ready_state = {
        "phase": "co2",
        "point_row": point.index,
        "target_pressure_hpa": point.target_pressure_hpa,
        "recorded_wall_ts": runner_module.time.time(),
        "route_sealed": True,
        "atmosphere_hold_stopped": True,
        "failures": [],
    }

    state, reason = runner._matching_preseal_pressure_control_ready_state(point, phase="co2")
    assert state is not None
    assert reason == ""

    mismatch_point = CalibrationPoint(
        index=point.index,
        temp_chamber_c=point.temp_chamber_c,
        co2_ppm=point.co2_ppm,
        hgen_temp_c=point.hgen_temp_c,
        hgen_rh_pct=point.hgen_rh_pct,
        target_pressure_hpa=float(point.target_pressure_hpa or 0.0) + 0.8,
        dewpoint_c=point.dewpoint_c,
        h2o_mmol=point.h2o_mmol,
        raw_h2o=point.raw_h2o,
    )
    state, reason = runner._matching_preseal_pressure_control_ready_state(mismatch_point, phase="co2")
    assert state is None
    assert reason.startswith("target_pressure_mismatch:")

    runner._preseal_pressure_control_ready_state["recorded_wall_ts"] = runner_module.time.time() - 12.0
    state, reason = runner._matching_preseal_pressure_control_ready_state(point, phase="co2")
    assert state is None
    assert reason.startswith("snapshot_age_exceeded:")

    runner._preseal_pressure_control_ready_state = {
        "phase": "co2",
        "point_row": point.index,
        "target_pressure_hpa": point.target_pressure_hpa,
        "recorded_wall_ts": runner_module.time.time(),
        "route_sealed": True,
        "atmosphere_hold_stopped": True,
        "failures": [],
    }
    runner._clear_preseal_pressure_control_ready_state(reason="vent_on:test", point=point, phase="co2")
    state, reason = runner._matching_preseal_pressure_control_ready_state(point, phase="co2")
    logger.close()

    assert state is None
    assert reason == "snapshot_invalidated:vent_on:test"


def test_pressure_controller_ready_snapshot_skips_aux_refresh_in_legacy_hold(tmp_path: Path) -> None:
    class _FakePace:
        def __init__(self) -> None:
            self.output_calls = 0
            self.isolation_calls = 0
            self.vent_calls = 0
            self.aux_calls = 0

        def get_output_state(self):
            self.output_calls += 1
            return 0

        def get_isolation_state(self):
            self.isolation_calls += 1
            return 1

        def get_vent_status(self):
            self.vent_calls += 1
            return 0

        def supports_vent_after_valve_open(self):
            self.aux_calls += 1
            return True

        def get_vent_after_valve_open(self):
            self.aux_calls += 1
            return False

        def supports_vent_popup_ack(self):
            self.aux_calls += 1
            return True

        def get_vent_popup_ack_enabled(self):
            self.aux_calls += 1
            return True

    logger = RunLogger(tmp_path)
    pace = _FakePace()
    runner = CalibrationRunner({}, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    snapshot = runner._pressure_controller_ready_snapshot(pace)
    logger.close()

    assert snapshot["pace_output_state"] == 0
    assert snapshot["pace_isolation_state"] == 1
    assert snapshot["pace_vent_status"] == 0
    assert pace.output_calls == 1
    assert pace.isolation_calls == 1
    assert pace.vent_calls == 1
    assert pace.aux_calls == 0


def test_pressure_controller_ready_snapshot_refreshes_aux_for_open_vent_strategy(tmp_path: Path) -> None:
    class _FakePace:
        def __init__(self) -> None:
            self.aux_calls = 0

        def get_output_state(self):
            return 0

        def get_isolation_state(self):
            return 1

        def get_vent_status(self):
            return 0

        def supports_vent_after_valve_open(self):
            self.aux_calls += 1
            return True

        def get_vent_after_valve_open(self):
            self.aux_calls += 1
            return False

        def supports_vent_popup_ack(self):
            self.aux_calls += 1
            return True

        def get_vent_popup_ack_enabled(self):
            self.aux_calls += 1
            return True

    logger = RunLogger(tmp_path)
    pace = _FakePace()
    runner = CalibrationRunner({}, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._pressure_atmosphere_hold_strategy = "vent_valve_open_after_vent"

    snapshot = runner._pressure_controller_ready_snapshot(pace)
    logger.close()

    assert snapshot["vent_after_valve_open"] is False
    assert snapshot["vent_popup_ack_enabled"] is True
    assert pace.aux_calls >= 4


def test_real_co2_fasttrace_keeps_long_route_guards() -> None:
    cfg = load_config(ROOT / "configs" / "headless_real_smoke_co2_fasttrace.json")
    sampling_cfg = cfg["workflow"]["sampling"]
    stability_cfg = cfg["workflow"]["stability"]["co2_route"]

    assert sampling_cfg["slow_aux_cache_enabled"] is True
    assert stability_cfg["preseal_soak_s"] >= 180


def test_short_fasttrace_configs_enable_gauge_continuous_and_keep_single_reseal_retry() -> None:
    short_cfg = load_config(ROOT / "configs" / "headless_real_smoke_co2_fasttrace_short.json")
    short_noanalyzers_cfg = load_config(ROOT / "configs" / "headless_real_smoke_co2_fasttrace_short_noanalyzers.json")

    for cfg in (short_cfg, short_noanalyzers_cfg):
        pressure_cfg = cfg["workflow"]["pressure"]
        sampling_cfg = cfg["workflow"]["sampling"]
        route_cfg = cfg["workflow"]["stability"]["co2_route"]
        pace_match_cfg = sampling_cfg["fast_signal_match"]["pace"]
        assert pressure_cfg["co2_reseal_retry_count"] == 1
        assert sampling_cfg["pressure_gauge_continuous_enabled"] is True
        assert sampling_cfg["pressure_gauge_continuous_mode"] == "P4"
        assert sampling_cfg["count"] == 10
        assert sampling_cfg["interval_s"] == 1.0
        assert route_cfg["preseal_soak_s"] >= 180
        assert pace_match_cfg["left_match_max_ms"] >= 1200.0
        assert pace_match_cfg["stale_ms"] >= 1200.0

    assert short_cfg["workflow"]["pressure"]["stabilize_timeout_s"] >= 80
    assert short_cfg["workflow"]["sampling"]["slow_aux_cache_enabled"] is True
    assert "first_point_preseal_soak_s" not in short_cfg["workflow"]["stability"]["co2_route"]
    assert "preseal_soak_cap_s" not in short_cfg["workflow"]["stability"]["co2_route"]
    assert "early_seal_if_stable" not in short_cfg["workflow"]["stability"]["co2_route"]
    assert short_noanalyzers_cfg["workflow"]["stability"]["co2_route"]["first_point_preseal_soak_s"] == 0
    assert "preseal_soak_cap_s" not in short_noanalyzers_cfg["workflow"]["stability"]["co2_route"]


def test_slow_fast_signal_match_accepts_recent_gauge_and_dew_frames(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {},
                "pressure": {"fast_gauge_response_timeout_s": 1.0},
                "analyzer_live_snapshot": {},
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    context = {
        "lock": None,
        "fast_signal_buffers": {
            "pressure_gauge": deque(
                [
                    {
                        "recv_wall_ts": "2026-03-31T12:00:00.000",
                        "recv_mono_s": 98.8,
                        "values": {"pressure_gauge_hpa": 1001.2},
                    }
                ]
            ),
            "dewpoint": deque(
                [
                    {
                        "recv_wall_ts": "2026-03-31T12:00:00.100",
                        "recv_mono_s": 98.6,
                        "values": {
                            "dewpoint_live_c": -16.2,
                            "dew_temp_live_c": 21.1,
                            "dew_rh_live_pct": 4.2,
                        },
                    }
                ]
            ),
        },
        "fast_signal_errors": {},
    }
    data = {}

    runner._merge_fast_signal_cache_into_sample(
        data,
        context,
        sample_anchor_mono=100.0,
        row_time_s=100.0,
    )
    logger.close()

    assert data["pressure_gauge_hpa"] == 1001.2
    assert data["pressure_gauge_anchor_delta_ms"] == 1200.0
    assert data["dewpoint_live_c"] == -16.2
    assert data["dewpoint_live_anchor_delta_ms"] == 1400.0


def test_pressure_transition_fast_signal_refresh_populates_pace_gauge_and_dewpoint(tmp_path: Path) -> None:
    class FakePace:
        def read_pressure(self):
            return 1004.2

    class FakeGauge:
        def read_pressure(self, *args, **kwargs):
            return 1003.7

    class FakeDew:
        def get_current_fast(self, timeout_s=0.35):
            return {"dewpoint_c": -15.4, "temp_c": 21.2, "rh_pct": 11.0}

    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "fast_signal_worker_interval_s": 0.2,
                    "fast_signal_match": {},
                },
                "pressure": {"fast_gauge_response_timeout_s": 1.0},
                "analyzer_live_snapshot": {},
            }
        },
        {"pace": FakePace(), "pressure_gauge": FakeGauge(), "dewpoint": FakeDew()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2()
    context = runner._new_sampling_window_context(point=point, phase="co2", point_tag="transition_demo")

    runner._refresh_pressure_transition_fast_signal_once(context, reason="unit-test")
    ready_values = runner._cached_ready_check_trace_values(context=context, point=point)
    logger.close()

    assert runner._latest_fast_signal_frame("pace", context=context) is not None
    assert runner._latest_fast_signal_frame("pressure_gauge", context=context) is not None
    assert runner._latest_fast_signal_frame("dewpoint", context=context) is not None
    assert ready_values["pace_pressure_hpa"] == 1004.2
    assert ready_values["pressure_gauge_hpa"] == 1003.7
    assert ready_values["dewpoint_live_c"] == -15.4
    assert ready_values["dewpoint_c"] == -15.4


def test_sampling_pressure_gauge_uses_continuous_reader_when_active(tmp_path: Path) -> None:
    class FakeGauge:
        def __init__(self) -> None:
            self.calls = []

        def pressure_continuous_active(self) -> bool:
            return True

        def read_pressure_continuous_latest(self, *, drain_s: float, read_timeout_s: float):
            self.calls.append((drain_s, read_timeout_s))
            return 1008.25

    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "pressure_gauge_continuous_enabled": True,
                    "pressure_gauge_continuous_mode": "P4",
                    "pressure_gauge_continuous_drain_s": 0.12,
                    "pressure_gauge_continuous_read_timeout_s": 0.02,
                },
                "analyzer_live_snapshot": {},
            }
        },
        {"pressure_gauge": FakeGauge()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    context = runner._new_sampling_window_context(point=_point_co2(), phase="co2", point_tag="gauge_continuous")

    runner._refresh_fast_signal_entry(context, "pressure_gauge", reason="unit-test")
    frames = runner._sampling_window_fast_signal_frames(context, "pressure_gauge")
    logger.close()

    assert len(frames) == 1
    assert frames[0]["values"]["pressure_gauge_hpa"] == 1008.25
    assert runner.devices["pressure_gauge"].calls == [(0.12, 0.02)]


def test_collect_samples_keeps_ten_samples_at_one_hz(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "count": 10,
                    "interval_s": 1.0,
                    "fixed_rate_enabled": True,
                    "slow_aux_cache_enabled": False,
                    "fast_signal_worker_enabled": False,
                },
                "analyzer_live_snapshot": {
                    "sampling_worker_enabled": False,
                    "passive_round_robin_enabled": False,
                },
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._all_gas_analyzers = types.MethodType(lambda self: [], runner)
    runner._prime_sampling_window_context = types.MethodType(
        lambda self, context, worker_plan=None, reason="": None,
        runner,
    )
    runner._stop_sampling_window_context = types.MethodType(lambda self, context: None, runner)
    runner._merge_fast_signal_cache_into_sample = types.MethodType(
        lambda self, data, context, sample_anchor_mono=None, row_time_s=None: None,
        runner,
    )
    runner._merge_analyzer_cache_into_sample = types.MethodType(
        lambda self, data, gas_analyzers, context=None, sample_anchor_mono=None, row_time_s=None: {},
        runner,
    )
    runner._merge_slow_aux_cache_into_sample = types.MethodType(
        lambda self, data, context, row_time_s=None: None,
        runner,
    )

    point = _point_co2()
    clock = {"mono": 0.0}
    sleeps: list[float] = []

    monkeypatch.setattr(runner_module.time, "monotonic", lambda: clock["mono"])
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["mono"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["mono"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    rows = runner._collect_samples(point, 10, 1.0, phase="co2", point_tag="cadence_guard")
    logger.close()

    assert rows is not None
    assert len(rows) == 10
    assert sleeps == [1.0] * 9
    assert [row["sample_index"] for row in rows] == list(range(1, 11))


def test_v1_workflow_and_headless_do_not_call_set_device_id() -> None:
    targets = [
        ROOT / "src" / "gas_calibrator" / "workflow" / "runner.py",
        ROOT / "src" / "gas_calibrator" / "tools" / "run_headless.py",
    ]
    for path in targets:
        text = path.read_text(encoding="utf-8")
        assert "set_device_id(" not in text
        assert "set_device_id_with_ack(" not in text


def test_pressure_transition_context_starts_per_device_workers(tmp_path: Path) -> None:
    class _FakePace:
        def read_pressure(self):
            return 1000.0

    class _FakeGauge:
        def read_pressure(self):
            return 999.9

    class _FakeDew:
        def get_current_fast(self, timeout_s=0.35):
            return {"dewpoint_c": -15.0, "temp_c": 20.0, "rh_pct": 5.0}

    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"fast_signal_worker_enabled": True}}},
        {"pace": _FakePace(), "pressure_gauge": _FakeGauge(), "dewpoint": _FakeDew()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    context = runner._start_pressure_transition_fast_signal_context(
        point=_point_co2(),
        phase="co2",
        point_tag="transition_workers",
        reason="unit-test",
    )
    worker_keys = {str(entry.get("key") or "") for entry in list(context.get("workers") or [])}
    runner._stop_pressure_transition_fast_signal_context(reason="unit-test done")
    logger.close()

    assert "pressure_transition_fast_signal:pace" in worker_keys
    assert "pressure_transition_fast_signal:pressure_gauge" in worker_keys
    assert "pressure_transition_fast_signal:dewpoint" in worker_keys


def test_wait_after_pressure_stable_before_sampling_uses_cached_transition_frames(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {},
        {"pace": object(), "pressure_gauge": object(), "dewpoint": object()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2()
    now = runner_module.time.monotonic()
    context = runner._new_sampling_window_context(point=point, phase="co2", point_tag="ready_check")
    context["worker_states"] = {
        "pressure_transition_fast_signal:pace": {"exited": False},
        "pressure_transition_fast_signal:pressure_gauge": {"exited": False},
        "pressure_transition_fast_signal:dewpoint": {"exited": False},
    }
    context["fast_signal_buffers"]["pace"] = deque(
        [{"recv_mono_s": now - 0.10, "values": {"pressure_hpa": 1100.0}}],
        maxlen=runner._sampling_fast_signal_ring_buffer_size(),
    )
    context["fast_signal_buffers"]["pressure_gauge"] = deque(
        [{"recv_mono_s": now - 0.08, "values": {"pressure_gauge_hpa": 1099.8}}],
        maxlen=runner._sampling_fast_signal_ring_buffer_size(),
    )
    context["fast_signal_buffers"]["dewpoint"] = deque(
        [
            {"recv_mono_s": now - 0.20, "values": {"dewpoint_live_c": -15.02, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 5.0}},
            {"recv_mono_s": now - 0.05, "values": {"dewpoint_live_c": -15.01, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 5.0}},
        ],
        maxlen=runner._sampling_fast_signal_ring_buffer_size(),
    )
    runner._pressure_transition_fast_signal_context = context
    runner._wait_postseal_dewpoint_gate = types.MethodType(lambda self, point, phase="", context=None: True, runner)
    runner._refresh_pressure_transition_fast_signal_once = types.MethodType(
        lambda self, *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected sync refresh")),
        runner,
    )

    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()


def test_wait_postseal_dewpoint_gate_uses_live_cache_without_sync_refresh(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {},
        {"pace": object(), "pressure_gauge": object(), "dewpoint": object()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2()
    now = runner_module.time.monotonic()
    context = runner._new_sampling_window_context(point=point, phase="co2", point_tag="dew_gate")
    context["worker_states"] = {
        "pressure_transition_fast_signal:pace": {"exited": False},
        "pressure_transition_fast_signal:pressure_gauge": {"exited": False},
        "pressure_transition_fast_signal:dewpoint": {"exited": False},
    }
    context["fast_signal_buffers"]["pace"] = deque(
        [{"recv_mono_s": now - 0.10, "values": {"pressure_hpa": 1100.0}}],
        maxlen=runner._sampling_fast_signal_ring_buffer_size(),
    )
    context["fast_signal_buffers"]["pressure_gauge"] = deque(
        [{"recv_mono_s": now - 0.09, "values": {"pressure_gauge_hpa": 1099.9}}],
        maxlen=runner._sampling_fast_signal_ring_buffer_size(),
    )
    context["fast_signal_buffers"]["dewpoint"] = deque(
        [
            {"recv_mono_s": now - 0.40, "values": {"dewpoint_live_c": -15.03, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 5.0}},
            {"recv_mono_s": now - 0.30, "values": {"dewpoint_live_c": -15.02, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 5.0}},
            {"recv_mono_s": now - 0.20, "values": {"dewpoint_live_c": -15.01, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 5.0}},
            {"recv_mono_s": now - 0.10, "values": {"dewpoint_live_c": -15.02, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 5.0}},
        ],
        maxlen=runner._sampling_fast_signal_ring_buffer_size(),
    )
    runner._refresh_pressure_transition_fast_signal_once = types.MethodType(
        lambda self, *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected sync refresh")),
        runner,
    )

    assert runner._wait_postseal_dewpoint_gate(point, phase="co2", context=context) is True
    logger.close()
    assert runner._point_runtime_summary[("co2", point.index)]["dewpoint_gate_result"] == "stable"


def test_wait_after_pressure_stable_warns_when_sampling_gate_prefers_gauge_without_continuous_mode(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "use_pressure_gauge_for_sampling_gate": True,
                    "post_stable_sample_delay_s": 0.0,
                    "co2_post_stable_sample_delay_s": 0.0,
                },
                "sampling": {
                    "pressure_gauge_continuous_enabled": False,
                },
            }
        },
        {},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_co2()
    runner._wait_postseal_dewpoint_gate = types.MethodType(lambda self, point, phase="", context=None: True, runner)

    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    assert any("pressure_gauge_continuous_enabled=false" in message for message in messages)
