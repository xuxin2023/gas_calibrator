import csv
import threading
import types
from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow import runner as runner_module
from gas_calibrator.workflow.runner import CalibrationRunner


def _co2_point(index: int, ppm: float, pressure_hpa: float) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=ppm,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure_hpa,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def _load_pressure_trace_rows(logger: RunLogger):
    path = logger.run_dir / "pressure_transition_trace.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_sample_and_log_writes_heavy_exports_immediately_without_handoff(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {"stable_count": 1, "interval_s": 0.0, "quality": {"enabled": False}},
                "reporting": {"defer_heavy_exports_during_handoff": True},
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point(1, 400.0, 1000.0)
    runner._collect_samples = types.MethodType(
        lambda self, *_args, **_kwargs: [
            {
                "point_row": point.index,
                "point_title": "demo",
                "co2_ppm": 401.2,
                "pressure_hpa": 1000.1,
                "pressure_gauge_hpa": 999.9,
            }
        ],
        runner,
    )
    called = {"summary": 0, "workbook": 0, "point": 0}
    runner.logger.log_analyzer_summary = lambda *_args, **_kwargs: called.__setitem__("summary", called["summary"] + 1)  # type: ignore[method-assign]
    runner.logger.log_analyzer_workbook = lambda *_args, **_kwargs: called.__setitem__("workbook", called["workbook"] + 1)  # type: ignore[method-assign]
    runner.logger.log_point = lambda *_args, **_kwargs: called.__setitem__("point", called["point"] + 1)  # type: ignore[method-assign]

    runner._sample_and_log(point, phase="co2", point_tag="demo")
    logger.close()

    assert called == {"summary": 1, "workbook": 1, "point": 1}
    assert runner._deferred_point_exports == []
    assert (logger.run_dir / "point_0001_co2_demo_samples.csv").exists()
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "handoff_last_sample_done" for row in trace_rows)


def test_sample_and_log_arms_route_handoff_before_sample_exports(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {"stable_count": 1, "interval_s": 0.0, "quality": {"enabled": False}},
                "reporting": {"defer_heavy_exports_during_handoff": True},
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point(1, 400.0, 1000.0)
    next_point = _co2_point(2, 800.0, 1000.0)
    runner._collect_samples = types.MethodType(
        lambda self, *_args, **_kwargs: [
            {
                "point_row": point.index,
                "point_title": "demo",
                "co2_ppm": 401.2,
                "pressure_hpa": 1000.1,
                "pressure_gauge_hpa": 999.9,
                "sample_end_ts": "2026-03-30T21:06:12.636",
            }
        ],
        runner,
    )

    order = []

    def _mark_handoff(self, **_kwargs):
        order.append("handoff")
        return True

    runner._begin_pending_route_handoff = types.MethodType(_mark_handoff, runner)
    runner.logger.log_sample = lambda *_args, **_kwargs: order.append("log_sample")  # type: ignore[method-assign]
    runner.logger.log_point_samples = lambda *_args, **_kwargs: (order.append("point_samples"), logger.run_dir / "demo.csv")[1]  # type: ignore[method-assign]
    called = {"summary": 0, "workbook": 0, "point": 0}
    runner.logger.log_analyzer_summary = lambda *_args, **_kwargs: called.__setitem__("summary", called["summary"] + 1)  # type: ignore[method-assign]
    runner.logger.log_analyzer_workbook = lambda *_args, **_kwargs: called.__setitem__("workbook", called["workbook"] + 1)  # type: ignore[method-assign]
    runner.logger.log_point = lambda *_args, **_kwargs: called.__setitem__("point", called["point"] + 1)  # type: ignore[method-assign]
    runner._sample_handoff_request = {
        "current_phase": "co2",
        "current_point_tag": "demo",
        "next_point": next_point,
        "next_phase": "co2",
        "next_point_tag": "next",
        "next_open_valves": [7, 8],
        "armed": False,
    }

    runner._sample_and_log(point, phase="co2", point_tag="demo")
    logger.close()

    assert order[0] == "handoff"
    assert "log_sample" not in order
    assert "point_samples" not in order
    assert runner._sample_handoff_request["armed"] is True
    assert len(runner._deferred_sample_exports) == 1
    assert len(runner._deferred_point_exports) == 1
    assert called == {"summary": 0, "workbook": 0, "point": 0}
    trace_rows = _load_pressure_trace_rows(logger)
    done_rows = [row for row in trace_rows if row["trace_stage"] == "handoff_last_sample_done"]
    assert len(done_rows) == 1
    assert done_rows[0]["ts"] == "2026-03-30T21:06:12.636"


def test_sample_and_log_defers_exports_when_route_seal_request_matches(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {"stable_count": 1, "interval_s": 0.0, "quality": {"enabled": False}},
                "reporting": {"defer_heavy_exports_during_handoff": True},
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point(1, 400.0, 1000.0)
    runner._collect_samples = types.MethodType(
        lambda self, *_args, **_kwargs: [
            {
                "point_row": point.index,
                "point_title": "demo",
                "co2_ppm": 401.2,
                "pressure_hpa": 1000.1,
                "pressure_gauge_hpa": 999.9,
                "sample_end_ts": "2026-03-30T21:06:12.636",
            }
        ],
        runner,
    )

    order = []
    runner.logger.log_sample = lambda *_args, **_kwargs: order.append("log_sample")  # type: ignore[method-assign]
    runner.logger.log_point_samples = lambda *_args, **_kwargs: (order.append("point_samples"), logger.run_dir / "demo.csv")[1]  # type: ignore[method-assign]
    called = {"summary": 0, "workbook": 0, "point": 0}
    runner.logger.log_analyzer_summary = lambda *_args, **_kwargs: called.__setitem__("summary", called["summary"] + 1)  # type: ignore[method-assign]
    runner.logger.log_analyzer_workbook = lambda *_args, **_kwargs: called.__setitem__("workbook", called["workbook"] + 1)  # type: ignore[method-assign]
    runner.logger.log_point = lambda *_args, **_kwargs: called.__setitem__("point", called["point"] + 1)  # type: ignore[method-assign]
    assert runner._request_sample_export_deferral(point, phase="co2", point_tag="demo", mode="route_seal") is True

    runner._sample_and_log(point, phase="co2", point_tag="demo")
    logger.close()

    assert order == []
    assert runner._sample_export_deferral_request is None
    assert len(runner._deferred_sample_exports) == 1
    assert len(runner._deferred_point_exports) == 1
    assert called == {"summary": 0, "workbook": 0, "point": 0}


def test_sample_and_log_uses_cached_pace_state_for_end_trace_rows(tmp_path: Path) -> None:
    class _FailIfPaceStateRead:
        def get_output_state(self):
            raise AssertionError("sampling end trace should reuse cached pace output state")

        def get_isolation_state(self):
            raise AssertionError("sampling end trace should reuse cached pace isolation state")

        def get_vent_status(self):
            raise AssertionError("sampling end trace should reuse cached pace vent state")

    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"stable_count": 1, "interval_s": 0.0, "quality": {"enabled": False}}}},
        {"pace": _FailIfPaceStateRead()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point(1, 400.0, 1000.0)
    runner._collect_samples = types.MethodType(
        lambda self, *_args, **_kwargs: [
            {
                "point_row": point.index,
                "point_title": "demo",
                "co2_ppm": 401.2,
                "pressure_hpa": 1000.1,
                "pressure_gauge_hpa": 999.9,
                "pace_output_state": 1,
                "pace_isolation_state": 1,
                "pace_vent_status": 3,
                "sample_end_ts": "2026-03-30T21:06:12.636",
            }
        ],
        runner,
    )

    runner._sample_and_log(point, phase="co2", point_tag="demo")
    logger.close()

    trace_rows = _load_pressure_trace_rows(logger)
    end_rows = [row for row in trace_rows if row["trace_stage"] in {"sampling_end", "handoff_last_sample_done"}]
    assert len(end_rows) == 2
    assert all(row["pace_output_state"] == "1" for row in end_rows)
    assert all(row["pace_isolation_state"] == "1" for row in end_rows)
    assert all(row["pace_vent_status"] == "3" for row in end_rows)


def test_route_handoff_fast_path_uses_safe_open_threshold_and_flushes_deferred_exports(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class _FakePace:
        def __init__(self) -> None:
            self.calls = []
            self.wait_for_vent_idle_calls = 0

        def begin_atmosphere_handoff(self):
            self.calls.append("begin_handoff")
            return 1

        def enter_atmosphere_mode(self, timeout_s=0.0, **kwargs):
            self.calls.append(("vent_on", float(timeout_s), bool(kwargs.get("hold_open", False))))

        def stop_atmosphere_hold(self):
            self.calls.append("stop_hold")
            return True

        def read_pressure(self):
            return 1001.0

        def get_output_state(self):
            return 0

        def get_isolation_state(self):
            return 1

        def get_vent_status(self):
            return 0

    class _FakeGauge:
        def __init__(self) -> None:
            self.values = iter([1008.0, 1002.0])

        def read_pressure(self):
            return next(self.values)

    logger = RunLogger(tmp_path)
    pace = _FakePace()
    point_from = _co2_point(1, 400.0, 1100.0)
    point_next = _co2_point(2, 800.0, 900.0)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "handoff_fast_enabled": True,
                    "handoff_safe_open_delta_hpa": 3.0,
                    "handoff_use_pressure_gauge": True,
                    "handoff_require_vent_completed": False,
                    "transition_trace_poll_s": 0.5,
                },
                "reporting": {
                    "defer_heavy_exports_during_handoff": True,
                    "flush_deferred_exports_on_next_route_soak": True,
                },
            }
        },
        {"pace": pace, "pressure_gauge": _FakeGauge()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    clock = {"now": 10.0}
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(runner_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))

    opened_routes: list[list[int]] = []
    flushed: list[str] = []
    runner._apply_valve_states = types.MethodType(lambda self, open_valves: opened_routes.append(list(open_valves)), runner)
    runner._perform_light_point_exports = types.MethodType(
        lambda self, point, samples, **kwargs: flushed.append(f"sample:{point.index}:{kwargs.get('phase')}"),
        runner,
    )
    runner._perform_heavy_point_exports = types.MethodType(
        lambda self, point, samples, **kwargs: flushed.append(f"heavy:{point.index}:{kwargs.get('phase')}"),
        runner,
    )
    runner._atmosphere_reference_hpa = 1000.0
    runner._last_sample_completion = {
        "sample_done_ts": 10.0,
        "pace_pressure_hpa": 1100.0,
        "pressure_gauge_hpa": 1005.5,
        "dewpoint_c": None,
        "dew_temp_c": None,
        "dew_rh_pct": None,
    }
    runner._deferred_sample_exports = [
        {
            "point": point_from,
            "samples": [{"point_row": point_from.index, "co2_ppm": 401.0}],
            "phase": "co2",
            "point_tag": "from",
        }
    ]
    runner._deferred_point_exports = [
        {
            "point": point_from,
            "samples": [{"point_row": point_from.index, "co2_ppm": 401.0}],
            "phase": "co2",
            "point_tag": "from",
            "analyzer_labels": [],
            "integrity_summary": {},
        }
    ]

    assert runner._begin_pending_route_handoff(
        current_point=point_from,
        current_phase="co2",
        current_point_tag="from",
        next_point=point_next,
        next_phase="co2",
        next_point_tag="to",
        next_open_valves=[7, 8],
    ) is True
    transition_context = runner._pressure_transition_fast_signal_context
    assert isinstance(transition_context, dict)
    runner._append_fast_signal_frame(
        transition_context,
        "pressure_gauge",
        values={"pressure_gauge_hpa": 1002.0, "pressure_gauge_raw": 1002.0},
        source="pressure_gauge_read",
    )
    assert runner._complete_pending_route_handoff(
        point_next,
        phase="co2",
        point_tag="to",
        open_valves=[7, 8],
    ) is True
    logger.close()

    assert "begin_handoff" in pace.calls
    assert any(call[0] == "vent_on" for call in pace.calls if isinstance(call, tuple))
    assert opened_routes == [[7, 8]]
    assert flushed == ["sample:1:co2", "heavy:1:co2"]
    assert runner._deferred_sample_exports == []
    assert runner._deferred_point_exports == []
    trace_rows = _load_pressure_trace_rows(logger)
    trace_stages = [row["trace_stage"] for row in trace_rows]
    assert "handoff_vent_command_sent" in trace_stages
    assert "handoff_safe_to_open_reached" in trace_stages
    assert "handoff_next_route_open_done" in trace_stages
    assert "handoff_deferred_exports_begin" in trace_stages
    assert "handoff_deferred_exports_end" in trace_stages


def test_begin_pending_route_handoff_starts_transition_worker_without_sync_prime(tmp_path: Path) -> None:
    class _FakePace:
        def begin_atmosphere_handoff(self):
            return 1

    logger = RunLogger(tmp_path)
    point_from = _co2_point(1, 400.0, 1100.0)
    point_next = _co2_point(2, 800.0, 900.0)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"handoff_fast_enabled": True}}},
        {"pace": _FakePace(), "pressure_gauge": object()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._last_sample_completion = {"sample_done_ts": 10.0}
    start_calls = []

    def _fake_start(self, **kwargs):
        start_calls.append(dict(kwargs))
        return {}

    runner._start_pressure_transition_fast_signal_context = types.MethodType(_fake_start, runner)

    assert runner._begin_pending_route_handoff(
        current_point=point_from,
        current_phase="co2",
        current_point_tag="from",
        next_point=point_next,
        next_phase="co2",
        next_point_tag="to",
        next_open_valves=[7, 8],
    ) is True
    logger.close()

    assert len(start_calls) == 1
    assert start_calls[0]["reason"] == "after route handoff vent"
    assert start_calls[0]["prime_immediately"] is False


def test_route_handoff_fast_path_uses_transition_gauge_cache_without_blocking_reads(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class _FakePace:
        def __init__(self) -> None:
            self.calls = []

        def begin_atmosphere_handoff(self):
            self.calls.append("begin_handoff")
            return 1

        def enter_atmosphere_mode(self, timeout_s=0.0, **kwargs):
            self.calls.append(("vent_on", float(timeout_s), bool(kwargs.get("hold_open", False))))

        def stop_atmosphere_hold(self):
            self.calls.append("stop_hold")
            return True

        def read_pressure(self):
            raise AssertionError("handoff safe-open path should not do blocking PACE pressure reads")

        def get_output_state(self):
            raise AssertionError("handoff trace rows should use cached PACE output state")

        def get_isolation_state(self):
            raise AssertionError("handoff trace rows should use cached PACE isolation state")

        def get_vent_status(self):
            raise AssertionError("handoff trace rows should use cached PACE vent state")

    class _FailIfGaugeRead:
        def read_pressure(self):
            raise AssertionError("handoff safe-open path should use transition gauge cache")

    logger = RunLogger(tmp_path)
    pace = _FakePace()
    point_from = _co2_point(1, 400.0, 1100.0)
    point_next = _co2_point(2, 800.0, 900.0)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "handoff_fast_enabled": True,
                    "handoff_safe_open_delta_hpa": 3.0,
                    "handoff_use_pressure_gauge": True,
                    "handoff_require_vent_completed": False,
                    "transition_trace_poll_s": 0.5,
                },
                "sampling": {
                    "fast_signal_worker_enabled": False,
                },
            }
        },
        {"pace": pace, "pressure_gauge": _FailIfGaugeRead()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    clock = {"now": 10.0}
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(runner_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))

    opened_routes: list[list[int]] = []
    runner._apply_valve_states = types.MethodType(lambda self, open_valves: opened_routes.append(list(open_valves)), runner)
    runner._atmosphere_reference_hpa = 1000.0
    runner._last_sample_completion = {
        "sample_done_ts": 10.0,
        "pace_pressure_hpa": 1100.0,
        "pressure_gauge_hpa": 1005.5,
        "pace_output_state": 0,
        "pace_isolation_state": 1,
        "pace_vent_status": 1,
    }
    transition_context = runner._new_sampling_window_context(point=point_from, phase="co2", point_tag="from")
    runner._append_fast_signal_frame(
        transition_context,
        "pressure_gauge",
        values={"pressure_gauge_raw": 1002.0, "pressure_gauge_hpa": 1002.0},
        source="pressure_gauge_read",
    )
    runner._pressure_transition_fast_signal_context = transition_context

    assert runner._begin_pending_route_handoff(
        current_point=point_from,
        current_phase="co2",
        current_point_tag="from",
        next_point=point_next,
        next_phase="co2",
        next_point_tag="to",
        next_open_valves=[7, 8],
    ) is True
    assert runner._complete_pending_route_handoff(
        point_next,
        phase="co2",
        point_tag="to",
        open_valves=[7, 8],
    ) is True
    logger.close()

    assert "begin_handoff" in pace.calls
    assert any(call[0] == "vent_on" for call in pace.calls if isinstance(call, tuple))
    assert opened_routes == [[7, 8]]
    trace_rows = _load_pressure_trace_rows(logger)
    safe_rows = [row for row in trace_rows if row["trace_stage"] == "handoff_safe_to_open_reached"]
    assert len(safe_rows) == 1
    assert float(safe_rows[0]["pressure_gauge_hpa"]) == 1002.0


def test_route_handoff_fast_path_uses_pressure_gauge_delta_from_last_sample_not_atmosphere(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class _FakePace:
        def __init__(self) -> None:
            self.calls = []

        def begin_atmosphere_handoff(self):
            self.calls.append("begin_handoff")
            return 1

        def enter_atmosphere_mode(self, timeout_s=0.0, **kwargs):
            self.calls.append(("vent_on", float(timeout_s), bool(kwargs.get("hold_open", False))))

        def stop_atmosphere_hold(self):
            self.calls.append("stop_hold")
            return True

        def read_pressure(self):
            raise AssertionError("handoff safe-open path should not do blocking PACE pressure reads")

        def get_output_state(self):
            raise AssertionError("handoff trace rows should use cached PACE output state")

        def get_isolation_state(self):
            raise AssertionError("handoff trace rows should use cached PACE isolation state")

        def get_vent_status(self):
            raise AssertionError("handoff trace rows should use cached PACE vent state")

    class _FakeGauge:
        def __init__(self) -> None:
            self.values = iter([1097.0])

        def read_pressure(self):
            return next(self.values)

    logger = RunLogger(tmp_path)
    pace = _FakePace()
    point_from = _co2_point(1, 400.0, 1100.0)
    point_next = _co2_point(2, 800.0, 900.0)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "handoff_fast_enabled": True,
                    "handoff_safe_open_delta_hpa": 3.0,
                    "handoff_use_pressure_gauge": True,
                    "handoff_require_vent_completed": False,
                    "transition_trace_poll_s": 0.5,
                }
            }
        },
        {"pace": pace, "pressure_gauge": _FakeGauge()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    clock = {"now": 10.0}
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(runner_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))

    opened_routes: list[list[int]] = []
    runner._apply_valve_states = types.MethodType(lambda self, open_valves: opened_routes.append(list(open_valves)), runner)
    runner._atmosphere_reference_hpa = 1000.0
    runner._last_sample_completion = {
        "sample_done_ts": 10.0,
        "pace_pressure_hpa": 1100.0,
        "pressure_gauge_hpa": 1101.0,
        "pace_output_state": 0,
        "pace_isolation_state": 1,
        "pace_vent_status": 1,
    }

    assert runner._begin_pending_route_handoff(
        current_point=point_from,
        current_phase="co2",
        current_point_tag="from",
        next_point=point_next,
        next_phase="co2",
        next_point_tag="to",
        next_open_valves=[7, 8],
    ) is True
    transition_context = runner._pressure_transition_fast_signal_context
    assert isinstance(transition_context, dict)
    runner._append_fast_signal_frame(
        transition_context,
        "pressure_gauge",
        values={"pressure_gauge_raw": 1097.0, "pressure_gauge_hpa": 1097.0},
        source="pressure_gauge_read",
    )
    assert runner._complete_pending_route_handoff(
        point_next,
        phase="co2",
        point_tag="to",
        open_valves=[7, 8],
    ) is True
    logger.close()

    assert opened_routes == [[7, 8]]
    trace_rows = _load_pressure_trace_rows(logger)
    safe_rows = [row for row in trace_rows if row["trace_stage"] == "handoff_safe_to_open_reached"]
    assert len(safe_rows) == 1
    assert float(safe_rows[0]["pressure_gauge_hpa"]) == 1097.0


def test_write_physical_valve_states_parallelizes_multi_relay_bulk_writes(tmp_path: Path) -> None:
    class _BlockingRelay:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.release = threading.Event()
            self.calls = []

        def set_valves_bulk(self, updates):
            self.calls.append(list(updates))
            self.started.set()
            if not self.release.wait(timeout=1.0):
                raise AssertionError("bulk relay write did not get released")

    logger = RunLogger(tmp_path)
    relay_a = _BlockingRelay()
    relay_b = _BlockingRelay()
    runner = CalibrationRunner(
        {},
        {"relay": relay_a, "relay_8": relay_b},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    worker = threading.Thread(
        target=runner._write_physical_valve_states,
        args=({("relay", 5): True, ("relay_8", 3): True},),
        daemon=True,
    )
    worker.start()

    assert relay_a.started.wait(timeout=0.5) is True
    assert relay_b.started.wait(timeout=0.5) is True
    relay_a.release.set()
    relay_b.release.set()
    worker.join(timeout=1.0)
    logger.close()

    assert worker.is_alive() is False
    assert relay_a.calls == [[(5, True)]]
    assert relay_b.calls == [[(3, True)]]


def test_run_flushes_deferred_exports_before_coefficients(monkeypatch, tmp_path: Path) -> None:
    point = _co2_point(1, 400.0, 1000.0)
    monkeypatch.setattr(runner_module, "load_points_from_excel", lambda *_args, **_kwargs: [point])
    monkeypatch.setattr(runner_module, "reorder_points", lambda points, *_args, **_kwargs: points)
    monkeypatch.setattr(runner_module, "validate_points", lambda *_args, **_kwargs: [])

    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "paths": {"points_excel": "demo.xlsx"},
            "workflow": {"collect_only": False, "missing_pressure_policy": "carry_forward"},
        },
        {"gas_analyzer": object()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    order: list[tuple[str, str]] = []
    runner._sensor_precheck = types.MethodType(lambda self: None, runner)
    runner._configure_devices = types.MethodType(lambda self: None, runner)
    runner._startup_preflight_reset = types.MethodType(lambda self: None, runner)
    runner._startup_pressure_precheck = types.MethodType(lambda self, _points: None, runner)
    runner._run_points = types.MethodType(
        lambda self, _points: (
            self._deferred_sample_exports.append(
                {
                    "point": point,
                    "samples": [{"point_row": point.index}],
                    "phase": "co2",
                    "point_tag": "demo",
                }
            ),
            self._deferred_point_exports.append(
                {
                    "point": point,
                    "samples": [{"point_row": point.index}],
                    "phase": "co2",
                    "point_tag": "demo",
                    "analyzer_labels": [],
                    "integrity_summary": {},
                }
            ),
        ),
        runner,
    )
    runner._flush_deferred_sample_exports = types.MethodType(
        lambda self, reason="": order.append(("sample_flush", reason)),
        runner,
    )
    runner._flush_deferred_point_exports = types.MethodType(
        lambda self, reason="": order.append(("heavy_flush", reason)),
        runner,
    )
    runner._maybe_write_coefficients = types.MethodType(lambda self: order.append(("coeff", "")), runner)

    runner.run()
    logger.close()

    assert order[:2] == [
        ("sample_flush", "before coefficient fitting"),
        ("heavy_flush", "before coefficient fitting"),
    ]
    assert order[2] == ("coeff", "")


def test_same_gas_pressure_step_handoff_does_not_emit_atmosphere_enter_or_route_reopen(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    current_point = _co2_point(1, 400.0, 1000.0)
    next_point = _co2_point(2, 400.0, 800.0)
    runner._last_sealed_pressure_route_context = {
        "phase": "co2",
        "route_signature": runner._route_signature_for_point(current_point, phase="co2"),
        "point_row": current_point.index,
    }

    mode = runner._prepare_sampling_handoff_mode(next_point, phase="co2")
    logger.close()

    assert mode == "same_gas_pressure_step_handoff"
    trace_rows = _load_pressure_trace_rows(logger)
    selected_rows = [row for row in trace_rows if row["trace_stage"] == "handoff_mode_selected"]
    assert len(selected_rows) == 1
    assert selected_rows[0]["handoff_mode"] == "same_gas_pressure_step_handoff"
    assert not any(row["trace_stage"] == "atmosphere_enter_begin" for row in trace_rows)
    assert not any(row["trace_stage"] == "route_open" for row in trace_rows)


def test_route_seal_context_is_remembered_before_sampling_for_follow_on_same_gas_point(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    sealed_point = _co2_point(1, 800.0, 1000.0)
    follow_on_point = _co2_point(2, 800.0, 800.0)
    runner._set_point_runtime_fields(
        sealed_point,
        phase="co2",
        timing_stages={
            "route_open": 100.0,
            "soak_begin": 101.0,
            "soak_end": 104.0,
            "preseal_vent_off_begin": 105.0,
            "preseal_trigger_reached": 106.0,
            "route_sealed": 107.0,
        },
    )

    remembered = runner._remember_last_sealed_pressure_route_context(
        sealed_point,
        phase="co2",
        reason="route_sealed_for_pressure_control",
    )
    mode = runner._prepare_sampling_handoff_mode(follow_on_point, phase="co2")
    logger.close()

    assert remembered["phase"] == "co2"
    assert remembered["point_row"] == sealed_point.index
    assert remembered["timing_stages"]["route_sealed"] == 107.0
    assert mode == "same_gas_pressure_step_handoff"
    follow_on_state = runner._point_runtime_state(follow_on_point, phase="co2") or {}
    inherited_stages = dict(follow_on_state.get("timing_stages") or {})
    assert inherited_stages["route_open"] == 100.0
    assert inherited_stages["route_sealed"] == 107.0
