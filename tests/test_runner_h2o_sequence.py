import types
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

import gas_calibrator.workflow.runner as runner_module
from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


def _point_h2o() -> CalibrationPoint:
    return CalibrationPoint(
        index=2,
        temp_chamber_c=40.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=-10.0,
        h2o_mmol=2.0,
        raw_h2o="demo",
    )


def _point_co2() -> CalibrationPoint:
    return CalibrationPoint(
        index=3,
        temp_chamber_c=40.0,
        co2_ppm=200.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
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


def test_run_h2o_point_wait_order(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []

    runner._set_h2o_path = types.MethodType(
        lambda self, is_open, point=None: calls.append(f"h2o_path_{bool(is_open)}"),
        runner,
    )
    runner._prepare_humidity_generator = types.MethodType(
        lambda self, point: calls.append("prepare_humidity"),
        runner,
    )
    runner._prepare_pressure_for_h2o = types.MethodType(
        lambda self, point: calls.append("prepare_pressure"),
        runner,
    )
    runner._set_temperature = types.MethodType(
        lambda self, target: calls.append("set_temperature") or True,
        runner,
    )
    runner._wait_humidity_generator_stable = types.MethodType(
        lambda self, point: calls.append("wait_hgen_setpoint") or True,
        runner,
    )
    runner._open_h2o_route_and_wait_ready = types.MethodType(
        lambda self, point, point_tag="": calls.append("open_route_ready") or True,
        runner,
    )
    runner._wait_h2o_route_soak_before_seal = types.MethodType(
        lambda self, point: calls.append("h2o_preseal_soak") or True,
        runner,
    )
    runner._pressurize_and_hold = types.MethodType(
        lambda self, point, route="h2o": calls.append(f"pressurize_{route}") or True,
        runner,
    )
    runner._set_pressure_to_target = types.MethodType(
        lambda self, point: calls.append("set_pressure") or True,
        runner,
    )
    runner._wait_primary_sensor_stable = types.MethodType(
        lambda self, point, **kwargs: calls.append(f"wait_sensor_ratio_{kwargs.get('require_pressure_in_limits', False)}") or True,
        runner,
    )
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append("wait_pressure_delay") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="": calls.append(f"sample_{phase}"),
        runner,
    )
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}"),
        runner,
    )
    runner._apply_route_baseline_valves = types.MethodType(
        lambda self: calls.append("baseline_route"),
        runner,
    )

    runner._run_h2o_point(_point_h2o())
    logger.close()

    assert calls == [
        "h2o_path_False",
        "prepare_pressure",
        "prepare_humidity",
        "set_temperature",
        "wait_hgen_setpoint",
        "open_route_ready",
        "h2o_preseal_soak",
        "pressurize_h2o",
        "set_pressure",
        "wait_pressure_delay",
        "sample_h2o",
        "vent_True",
        "baseline_route",
    ]


def test_run_h2o_point_wait_order_when_prepared(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []

    runner._set_h2o_path = types.MethodType(
        lambda self, is_open, point=None: calls.append(f"h2o_path_{bool(is_open)}"),
        runner,
    )
    runner._prepare_humidity_generator = types.MethodType(
        lambda self, point: calls.append("prepare_humidity"),
        runner,
    )
    runner._prepare_pressure_for_h2o = types.MethodType(
        lambda self, point: calls.append("prepare_pressure"),
        runner,
    )
    runner._set_temperature = types.MethodType(
        lambda self, target: calls.append("set_temperature") or True,
        runner,
    )
    runner._wait_humidity_generator_stable = types.MethodType(
        lambda self, point: calls.append("wait_hgen_setpoint") or True,
        runner,
    )
    runner._open_h2o_route_and_wait_ready = types.MethodType(
        lambda self, point, point_tag="": calls.append("open_route_ready") or True,
        runner,
    )
    runner._wait_h2o_route_soak_before_seal = types.MethodType(
        lambda self, point: calls.append("h2o_preseal_soak") or True,
        runner,
    )
    runner._pressurize_and_hold = types.MethodType(
        lambda self, point, route="h2o": calls.append(f"pressurize_{route}") or True,
        runner,
    )
    runner._set_pressure_to_target = types.MethodType(
        lambda self, point: calls.append("set_pressure") or True,
        runner,
    )
    runner._wait_primary_sensor_stable = types.MethodType(
        lambda self, point, **kwargs: calls.append(f"wait_sensor_ratio_{kwargs.get('require_pressure_in_limits', False)}") or True,
        runner,
    )
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append("wait_pressure_delay") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="": calls.append(f"sample_{phase}"),
        runner,
    )
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}"),
        runner,
    )
    runner._apply_route_baseline_valves = types.MethodType(
        lambda self: calls.append("baseline_route"),
        runner,
    )

    runner._run_h2o_point(_point_h2o(), prepared=True)
    logger.close()

    assert calls == [
        "h2o_path_False",
        "set_temperature",
        "wait_hgen_setpoint",
        "open_route_ready",
        "h2o_preseal_soak",
        "pressurize_h2o",
        "set_pressure",
        "wait_pressure_delay",
        "sample_h2o",
        "vent_True",
        "baseline_route",
    ]


def test_run_h2o_point_restores_vent_after_completion(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []

    runner._set_h2o_path = types.MethodType(lambda self, is_open, point=None: None, runner)
    runner._prepare_humidity_generator = types.MethodType(lambda self, point: None, runner)
    runner._prepare_pressure_for_h2o = types.MethodType(lambda self, point: None, runner)
    runner._set_temperature = types.MethodType(lambda self, target: True, runner)
    runner._wait_humidity_generator_stable = types.MethodType(lambda self, point: True, runner)
    runner._open_h2o_route_and_wait_ready = types.MethodType(lambda self, point, point_tag="": True, runner)
    runner._wait_h2o_route_soak_before_seal = types.MethodType(lambda self, point: True, runner)
    runner._pressurize_and_hold = types.MethodType(lambda self, point, route="h2o": True, runner)
    runner._set_pressure_to_target = types.MethodType(lambda self, point: True, runner)
    runner._wait_primary_sensor_stable = types.MethodType(lambda self, point, **kwargs: True, runner)
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(lambda self, point: True, runner)
    runner._sample_and_log = types.MethodType(lambda self, point, phase="": None, runner)
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(bool(vent_on)),
        runner,
    )

    runner._run_h2o_point(_point_h2o())
    logger.close()

    assert calls == [True]


def test_run_h2o_group_ambient_only_samples_open_route_without_pressure_control(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    point = _point_h2o()
    ambient_ref = runner._ambient_pressure_reference_point(point)

    runner._set_h2o_path = types.MethodType(lambda self, is_open, point=None: calls.append(f"h2o_path_{bool(is_open)}"), runner)
    runner._prepare_humidity_generator = types.MethodType(lambda self, point: calls.append("prepare_humidity"), runner)
    runner._prepare_pressure_for_h2o = types.MethodType(lambda self, point: calls.append("prepare_pressure"), runner)
    runner._set_temperature = types.MethodType(lambda self, target: calls.append("set_temperature") or True, runner)
    runner._wait_humidity_generator_stable = types.MethodType(
        lambda self, point: calls.append("wait_hgen_setpoint") or True,
        runner,
    )
    runner._open_h2o_route_and_wait_ready = types.MethodType(
        lambda self, point, point_tag="": calls.append("open_route_ready") or True,
        runner,
    )
    runner._wait_h2o_route_soak_before_seal = types.MethodType(
        lambda self, point: calls.append("h2o_preseal_soak") or True,
        runner,
    )
    runner._pressurize_and_hold = types.MethodType(
        lambda self, point, route="h2o": calls.append(f"pressurize_{route}") or True,
        runner,
    )
    runner._set_pressure_to_target = types.MethodType(lambda self, point: calls.append("set_pressure") or True, runner)
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append("wait_pressure_delay") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}"),
        runner,
    )
    runner._apply_route_baseline_valves = types.MethodType(lambda self: calls.append("baseline_route"), runner)

    runner._run_h2o_group([point], pressure_points=[ambient_ref])
    logger.close()

    assert calls == [
        "h2o_path_False",
        "prepare_pressure",
        "prepare_humidity",
        "set_temperature",
        "wait_hgen_setpoint",
        "open_route_ready",
        "sample_h2o_h2o_20c_30rh_ambient",
        "vent_True",
        "baseline_route",
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    sampling_begin_rows = [row for row in trace_rows if row["trace_stage"] == "sampling_begin"]
    assert len(sampling_begin_rows) == 1
    assert sampling_begin_rows[0]["trigger_reason"] == "ambient_open_route"


def test_run_h2o_group_runs_ambient_before_sealed_pressure_control(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    point = _point_h2o()
    ambient_ref = runner._ambient_pressure_reference_point(point)
    pressure_ref = CalibrationPoint(
        index=6,
        temp_chamber_c=point.temp_chamber_c,
        co2_ppm=point.co2_ppm,
        hgen_temp_c=point.hgen_temp_c,
        hgen_rh_pct=point.hgen_rh_pct,
        target_pressure_hpa=1100.0,
        dewpoint_c=point.dewpoint_c,
        h2o_mmol=point.h2o_mmol,
        raw_h2o=point.raw_h2o,
    )

    runner._set_h2o_path = types.MethodType(lambda self, is_open, point=None: calls.append(f"h2o_path_{bool(is_open)}"), runner)
    runner._prepare_humidity_generator = types.MethodType(lambda self, point: calls.append("prepare_humidity"), runner)
    runner._prepare_pressure_for_h2o = types.MethodType(lambda self, point: calls.append("prepare_pressure"), runner)
    runner._set_temperature = types.MethodType(lambda self, target: calls.append("set_temperature") or True, runner)
    runner._wait_humidity_generator_stable = types.MethodType(
        lambda self, point: calls.append("wait_hgen_setpoint") or True,
        runner,
    )
    runner._open_h2o_route_and_wait_ready = types.MethodType(
        lambda self, point, point_tag="": calls.append("open_route_ready") or True,
        runner,
    )
    runner._wait_h2o_route_soak_before_seal = types.MethodType(
        lambda self, point: calls.append("h2o_preseal_soak") or True,
        runner,
    )
    runner._pressurize_and_hold = types.MethodType(
        lambda self, point, route="h2o": calls.append(f"pressurize_{route}") or True,
        runner,
    )
    runner._set_pressure_to_target = types.MethodType(lambda self, point: calls.append("set_pressure") or True, runner)
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append("wait_pressure_delay") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}"),
        runner,
    )
    runner._apply_route_baseline_valves = types.MethodType(lambda self: calls.append("baseline_route"), runner)

    runner._run_h2o_group([point], pressure_points=[ambient_ref, pressure_ref])
    logger.close()

    assert calls == [
        "h2o_path_False",
        "prepare_pressure",
        "prepare_humidity",
        "set_temperature",
        "wait_hgen_setpoint",
        "open_route_ready",
        "sample_h2o_h2o_20c_30rh_ambient",
        "h2o_preseal_soak",
        "pressurize_h2o",
        "set_pressure",
        "wait_pressure_delay",
        "sample_h2o_h2o_20c_30rh_1100hpa",
        "vent_True",
        "baseline_route",
    ]


def test_open_h2o_route_keeps_vent_on_until_seal(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []

    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}:{reason}"),
        runner,
    )
    runner._set_h2o_path = types.MethodType(
        lambda self, is_open, point=None: calls.append(f"h2o_path_{bool(is_open)}"),
        runner,
    )
    runner._ensure_dewpoint_meter_ready = types.MethodType(
        lambda self: calls.append("dewpoint_ready") or True,
        runner,
    )
    runner._wait_dewpoint_alignment_stable = types.MethodType(
        lambda self, point=None: calls.append(f"dewpoint_stable_{getattr(point, 'index', None)}") or True,
        runner,
    )

    assert runner._open_h2o_route_and_wait_ready(_point_h2o()) is True
    logger.close()

    assert calls == [
        "vent_True:during H2O route pre-seal preparation",
        "h2o_path_True",
        "dewpoint_ready",
        "dewpoint_stable_2",
    ]


def test_wait_dewpoint_alignment_stable_skips_when_disabled(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)

    class _FailIfRead:
        def get_current(self):
            raise AssertionError("dewpoint alignment should be skipped when disabled")

    runner = CalibrationRunner(
        {"workflow": {"stability": {"dewpoint": {"enabled": False}}}},
        {"dewpoint": _FailIfRead()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    assert runner._wait_dewpoint_alignment_stable(_point_h2o()) is True
    logger.close()


def test_sample_and_log_emits_progress_events(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"count": 2, "interval_s": 0.0}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2()
    point.target_pressure_hpa = 1100.0
    runner._collect_samples = types.MethodType(
        lambda self, *_args, **_kwargs: [
            {"co2_ppm": 401.0, "pressure_hpa": 1100.0},
            {"co2_ppm": 402.0, "pressure_hpa": 1100.0},
        ],
        runner,
    )
    runner.logger.log_analyzer_workbook = types.MethodType(lambda self, *_args, **_kwargs: Path("demo.xlsx"), runner.logger)  # type: ignore[method-assign]

    runner._sample_and_log(point, phase="co2", point_tag="co2_groupa_200ppm_1100hpa")
    logger.close()

    with logger.io_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    run_events = [row for row in rows if row["port"] == "RUN" and row["direction"] == "EVENT"]
    stage_events = [json.loads(row["response"]) for row in run_events if row["command"] == "stage" and row["response"]]
    sample_events = [json.loads(row["response"]) for row in run_events if row["command"] == "sample-progress" and row["response"]]
    assert any(event.get("point_row") == point.index and event.get("wait_reason") == "采样中" for event in stage_events)
    assert any(event.get("text", "").endswith("0/2") for event in sample_events)


def test_collect_samples_emits_incrementing_progress_counts(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"count": 3, "interval_s": 0.0}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2()
    point.target_pressure_hpa = 1100.0
    runner._all_gas_analyzers = types.MethodType(  # type: ignore[method-assign]
        lambda self: [("ga01", object(), {}), ("ga02", object(), {})],
        runner,
    )
    runner._read_sensor_parsed = types.MethodType(  # type: ignore[method-assign]
        lambda self, _ga: ("", {"co2_ppm": 401.0, "h2o_mmol": 1.2, "chamber_temp_c": 20.1}),
        runner,
    )

    rows = runner._collect_samples(point, 3, 0.0, phase="co2", point_tag="demo")
    logger.close()

    assert rows is not None
    assert len(rows) == 3
    with logger.io_path.open("r", encoding="utf-8", newline="") as f:
        io_rows = list(csv.DictReader(f))

    sample_events = [
        json.loads(row["response"])
        for row in io_rows
        if row["port"] == "RUN" and row["direction"] == "EVENT" and row["command"] == "sample-progress" and row["response"]
    ]
    assert [event.get("current") for event in sample_events] == [1, 2, 3]
    assert [event.get("text") for event in sample_events] == [
        "采样进度：1/3",
        "采样进度：2/3",
        "采样进度：3/3",
    ]


def test_prepare_humidity_generator_logs_target_readback(tmp_path: Path) -> None:
    class _FakeHumidityGen:
        def __init__(self) -> None:
            self.calls = []

        def set_target_temp(self, value: float) -> None:
            self.calls.append(("set_temp", value))

        def set_target_rh(self, value: float) -> None:
            self.calls.append(("set_rh", value))

        def enable_control(self, on: bool) -> None:
            self.calls.append(("ctrl", bool(on)))

        def heat_on(self) -> None:
            self.calls.append(("heat_on",))

        def cool_on(self) -> None:
            self.calls.append(("cool_on",))

        def verify_target_readback(self, *, target_temp_c=None, target_rh_pct=None):
            self.calls.append(("verify", target_temp_c, target_rh_pct))
            return {
                "ok": True,
                "target_temp_c": target_temp_c,
                "target_rh_pct": target_rh_pct,
                "read_temp_c": target_temp_c,
                "read_rh_pct": target_rh_pct,
            }

    logger = RunLogger(tmp_path)
    logs = []
    hgen = _FakeHumidityGen()
    runner = CalibrationRunner({}, {"humidity_gen": hgen}, logger, logs.append, lambda *_: None)
    try:
        runner._prepare_humidity_generator(_point_h2o())
    finally:
        logger.close()

    assert ("ctrl", True) in hgen.calls
    assert ("heat_on",) in hgen.calls
    assert ("cool_on",) in hgen.calls
    assert ("verify", 20.0, 30.0) in hgen.calls
    assert any("target readback" in msg.lower() for msg in logs)
    assert any("humidity generator prepared:" in msg.lower() for msg in logs)
    assert any("ctrl_on=ok" in msg.lower() for msg in logs)
    assert any("heat_on=ok" in msg.lower() for msg in logs)
    assert any("cool_on=ok" in msg.lower() for msg in logs)


def test_prepare_humidity_generator_reports_partial_failure_truthfully(tmp_path: Path) -> None:
    class _FakeHumidityGen:
        def __init__(self) -> None:
            self.calls = []

        def set_target_temp(self, value: float) -> None:
            self.calls.append(("set_temp", value))

        def set_target_rh(self, value: float) -> None:
            self.calls.append(("set_rh", value))

        def enable_control(self, on: bool) -> None:
            self.calls.append(("ctrl", bool(on)))

        def heat_on(self) -> None:
            self.calls.append(("heat_on",))
            raise RuntimeError("heat broken")

        def cool_on(self) -> None:
            self.calls.append(("cool_on",))

    logger = RunLogger(tmp_path)
    logs = []
    hgen = _FakeHumidityGen()
    runner = CalibrationRunner({}, {"humidity_gen": hgen}, logger, logs.append, lambda *_: None)
    try:
        runner._prepare_humidity_generator(_point_h2o())
    finally:
        logger.close()

    assert ("ctrl", True) in hgen.calls
    assert ("heat_on",) in hgen.calls
    assert ("cool_on",) in hgen.calls
    assert any("heat_on failed" in msg.lower() for msg in logs)
    assert any("humidity generator prepared:" in msg.lower() and "heat_on=failed" in msg.lower() for msg in logs)
    assert not any("humidity generator prepared:" in msg.lower() and "heat_on=ok" in msg.lower() for msg in logs)


def test_pressurize_h2o_captures_preseal_dewpoint_snapshot(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {"pressure": {"pressurize_wait_after_vent_off_s": 0}},
            "valves": {"co2_path": 7, "co2_map": {"200": 2}},
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    calls = []

    class _FakePace:
        def read_pressure(self):
            return 1200.0

    runner.devices["pace"] = _FakePace()
    runner._capture_preseal_dewpoint_snapshot = types.MethodType(
        lambda self: calls.append("capture_preseal_dewpoint"),
        runner,
    )
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}:{reason}"),
        runner,
    )
    runner._set_h2o_path = types.MethodType(
        lambda self, is_open, point=None: calls.append(f"h2o_path_{bool(is_open)}"),
        runner,
    )

    assert runner._pressurize_and_hold(_point_h2o(), route="h2o") is True
    logger.close()

    assert calls[:3] == [
        "capture_preseal_dewpoint",
        "vent_False:before H2O pressure seal",
        "h2o_path_False",
    ]


def test_capture_preseal_dewpoint_snapshot_reads_pressure_before_dewpoint(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    calls = []

    class _FakeGauge:
        def read_pressure(self):
            calls.append("read_pressure")
            return 1001.2

    class _FakeDew:
        def open(self):
            calls.append("dew_open")

        def get_current(self):
            calls.append("dew_get_current")
            return {"dewpoint_c": 1.2, "temp_c": 20.0, "rh_pct": 30.0}

    runner = CalibrationRunner(
        {},
        {"pressure_gauge": _FakeGauge(), "dewpoint": _FakeDew()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    runner._capture_preseal_dewpoint_snapshot()
    logger.close()

    assert calls == ["read_pressure", "dew_open", "dew_get_current"]
    assert runner._preseal_dewpoint_snapshot is not None
    assert runner._preseal_dewpoint_snapshot["pressure_hpa"] == 1001.2


def test_pressurize_co2_seals_directly_after_vent_off_settle(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 0,
                    "pressurize_high_hpa": 1100,
                    "pressurize_timeout_s": 2,
                }
            },
            "valves": {"co2_path": 7, "co2_map": {"200": 2}},
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    calls = []

    class _FakePace:
        def __init__(self) -> None:
            self.values = iter([1026.5, 1099.8, 1100.2])

        def read_pressure(self):
            return next(self.values)

    point = CalibrationPoint(
        index=3,
        temp_chamber_c=40.0,
        co2_ppm=200.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    runner.devices["pace"] = _FakePace()
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}:{reason}"),
        runner,
    )
    runner._apply_valve_states = types.MethodType(
        lambda self, states: calls.append(f"apply_{states}"),
        runner,
    )

    assert runner._pressurize_and_hold(point, route="co2") is True
    logger.close()

    assert calls == [
        "vent_False:before CO2 pressure seal",
        "apply_[]",
    ]


def test_pressurize_co2_logs_preseal_pressure_window(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 1.0,
                }
            },
            "valves": {"co2_path": 7, "co2_map": {"200": 2}},
        },
        {},
        logger,
        messages.append,
        lambda *_: None,
    )

    class _FakePace:
        def __init__(self, sealed: dict[str, bool]) -> None:
            self.sealed = sealed

        def read_pressure(self):
            if not self.sealed["done"]:
                raise AssertionError("preseal threshold path should not read PACE before route seal")
            return 1082.1

        def query(self, cmd: str):
            raise AssertionError(f"preseal trace should not query PACE aux state: {cmd}")

    class _FakeGauge:
        def __init__(self) -> None:
            self.values = iter([1098.2, 1101.4, 1103.6])

        def read_pressure(self):
            return next(self.values)

    clock = {"now": 0.0}
    calls: list[str] = []
    sealed = {"done": False}

    def fake_time() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "time", fake_time)
    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    runner.devices["pace"] = _FakePace(sealed)
    runner.devices["pressure_gauge"] = _FakeGauge()
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}:{reason}"),
        runner,
    )
    runner._apply_valve_states = types.MethodType(
        lambda self, states: sealed.__setitem__("done", True) or calls.append(f"apply_{states}"),
        runner,
    )

    assert runner._pressurize_and_hold(_point_co2(), route="co2") is True
    logger.close()

    assert calls == [
        "vent_False:before CO2 pressure seal",
        "apply_[]",
    ]
    assert any("pre-seal pressure peak=1103.600 hPa last=1103.600 hPa" in message for message in messages)
    assert any("CO2 route sealed for pressure control" in message and "sealed pressure=1082.1" in message for message in messages)


def test_pressurize_co2_seals_early_when_pressure_gauge_reaches_threshold(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 5.0,
                    "co2_preseal_pressure_gauge_trigger_hpa": 1110.0,
                },
                "sampling": {"fast_signal_worker_enabled": False},
            },
            "valves": {"co2_path": 7, "co2_map": {"200": 2}},
        },
        {},
        logger,
        messages.append,
        lambda *_: None,
    )

    class _FakePace:
        def __init__(self, sealed: dict[str, bool]) -> None:
            self.sealed = sealed

        def read_pressure(self):
            if not self.sealed["done"]:
                raise AssertionError("preseal threshold path should not read PACE before route seal")
            return 1082.1

    class _FakeGauge:
        def __init__(self) -> None:
            self.values = iter([1108.0, 1110.0, 1111.2])

        def read_pressure(self):
            return next(self.values)

    class _FakeDew:
        def get_current(self):
            return {"dewpoint_c": -12.3, "temp_c": 24.5, "rh_pct": 45.6}

    clock = {"now": 0.0}
    sleeps: list[float] = []
    calls: list[str] = []
    sealed = {"done": False}

    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    runner.devices["pace"] = _FakePace(sealed)
    runner.devices["pressure_gauge"] = _FakeGauge()
    runner.devices["dewpoint"] = _FakeDew()
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}:{reason}"),
        runner,
    )
    runner._apply_valve_states = types.MethodType(
        lambda self, states: sealed.__setitem__("done", True) or calls.append(f"apply_{states}"),
        runner,
    )

    assert runner._pressurize_and_hold(_point_co2(), route="co2") is True
    logger.close()

    assert calls == [
        "vent_False:before CO2 pressure seal",
        "apply_[]",
    ]
    assert sleeps == []
    assert any("pressure gauge trigger=1110.000 hPa >= 1110.000 hPa" in message for message in messages)
    assert any("CO2 route sealed for pressure control" in message and "sealed pressure=1082.1" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    assert [row["trace_stage"] for row in trace_rows].count("preseal_vent_off_begin") == 2
    assert any(row["trace_stage"] == "preseal_wait" for row in trace_rows)
    trigger_rows = [row for row in trace_rows if row["trace_stage"] == "preseal_trigger_reached"]
    assert len(trigger_rows) == 1
    trigger_row = trigger_rows[0]
    assert trigger_row["trigger_reason"] == "pressure_gauge_threshold"
    assert float(trigger_row["pressure_gauge_hpa"]) == 1110.0
    assert float(trigger_row["pace_pressure_hpa"]) == 1082.1
    assert float(trigger_row["dewpoint_c"]) == -12.3
    assert float(trigger_row["dew_temp_c"]) == 24.5
    assert float(trigger_row["dew_rh_pct"]) == 45.6
    assert any(row["trace_stage"] == "route_sealed" for row in trace_rows)


def test_pressurize_co2_uses_transition_gauge_cache_before_direct_read(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 5.0,
                    "co2_preseal_pressure_gauge_trigger_hpa": 1110.0,
                    "transition_trace_poll_s": 0.5,
                },
                "sampling": {
                    "fast_signal_worker_enabled": False,
                },
            },
            "valves": {"co2_path": 7, "co2_map": {"200": 2}},
        },
        {},
        logger,
        messages.append,
        lambda *_: None,
    )

    class _FakePace:
        def __init__(self, sealed: dict[str, bool]) -> None:
            self.sealed = sealed

        def read_pressure(self):
            if not self.sealed["done"]:
                raise AssertionError("PACE should still only be read after route seal")
            return 1082.1

    class _FailIfGaugeRead:
        def read_pressure(self):
            raise AssertionError("preseal threshold path should use transition gauge cache")

    class _FakeDew:
        def get_current(self):
            return {"dewpoint_c": -12.3, "temp_c": 24.5, "rh_pct": 45.6}

    runner.devices["pace"] = _FakePace({"done": False})
    runner.devices["pressure_gauge"] = _FailIfGaugeRead()
    runner.devices["dewpoint"] = _FakeDew()

    sealed = {"done": False}
    runner.devices["pace"].sealed = sealed
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": None,
        runner,
    )
    runner._apply_valve_states = types.MethodType(
        lambda self, states: sealed.__setitem__("done", True),
        runner,
    )

    transition_context = runner._new_sampling_window_context(point=_point_co2(), phase="co2", point_tag="preseal")
    runner._append_fast_signal_frame(
        transition_context,
        "pressure_gauge",
        values={"pressure_gauge_raw": 1110.0, "pressure_gauge_hpa": 1110.0},
        source="pressure_gauge_read",
    )
    runner._pressure_transition_fast_signal_context = transition_context

    assert runner._pressurize_and_hold(_point_co2(), route="co2") is True
    logger.close()

    assert any("pressure_gauge_threshold reached" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    trigger_rows = [row for row in trace_rows if row["trace_stage"] == "preseal_trigger_reached"]
    assert len(trigger_rows) == 1
    assert trigger_rows[0]["trigger_reason"] == "pressure_gauge_threshold"
    assert float(trigger_rows[0]["pressure_gauge_hpa"]) == 1110.0


def test_pressurize_co2_uses_cached_fast_trace_values_for_trigger_and_route_sealed(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 5.0,
                    "co2_preseal_pressure_gauge_trigger_hpa": 1110.0,
                },
                "sampling": {
                    "fast_signal_worker_enabled": False,
                },
            },
            "valves": {"co2_path": 7, "co2_map": {"200": 2}},
        },
        {},
        logger,
        messages.append,
        lambda *_: None,
    )

    class _FailIfPaceRead:
        def read_pressure(self):
            raise AssertionError("cached fast trace path should avoid direct PACE read")

    class _FailIfGaugeRead:
        def read_pressure(self):
            raise AssertionError("preseal threshold path should use transition gauge cache")

    class _FailIfDewRead:
        def get_current(self):
            raise AssertionError("cached fast trace path should avoid direct dewpoint read")

        def get_current_fast(self, timeout_s: float = 0.35, clear_buffer: bool = False):
            raise AssertionError("cached fast trace path should avoid direct dewpoint fast read")

    calls: list[str] = []
    runner.devices["pace"] = _FailIfPaceRead()
    runner.devices["pressure_gauge"] = _FailIfGaugeRead()
    runner.devices["dewpoint"] = _FailIfDewRead()
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}:{reason}"),
        runner,
    )
    runner._apply_valve_states = types.MethodType(
        lambda self, states: calls.append(f"apply_{states}"),
        runner,
    )

    transition_context = runner._new_sampling_window_context(point=_point_co2(), phase="co2", point_tag="preseal")
    runner._append_fast_signal_frame(
        transition_context,
        "pressure_gauge",
        values={"pressure_gauge_raw": 1110.0, "pressure_gauge_hpa": 1110.0},
        source="pressure_gauge_read",
    )
    runner._append_fast_signal_frame(
        transition_context,
        "pace",
        values={"pressure_hpa": 1082.1},
        source="pace_read",
    )
    runner._append_fast_signal_frame(
        transition_context,
        "dewpoint",
        values={"dewpoint_live_c": -12.3, "dew_temp_live_c": 24.5, "dew_rh_live_pct": 45.6},
        source="dewpoint_fast",
    )
    runner._pressure_transition_fast_signal_context = transition_context

    assert runner._pressurize_and_hold(_point_co2(), route="co2") is True
    logger.close()

    assert calls == [
        "vent_False:before CO2 pressure seal",
        "apply_[]",
    ]
    assert any("pressure_gauge_threshold reached" in message for message in messages)
    assert any("sealed pressure=1082.1" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    trigger_rows = [row for row in trace_rows if row["trace_stage"] == "preseal_trigger_reached"]
    assert len(trigger_rows) == 1
    assert float(trigger_rows[0]["pressure_gauge_hpa"]) == 1110.0
    assert float(trigger_rows[0]["pace_pressure_hpa"]) == 1082.1
    assert float(trigger_rows[0]["dewpoint_c"]) == -12.3
    route_rows = [row for row in trace_rows if row["trace_stage"] == "route_sealed"]
    assert len(route_rows) == 1
    assert float(route_rows[0]["pace_pressure_hpa"]) == 1082.1


def test_read_preseal_pressure_gauge_avoids_direct_read_while_transition_worker_active(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)

    class _FailIfGaugeRead:
        def read_pressure(self):
            raise AssertionError("transition worker active path should not direct-read pressure gauge")

    runner.devices["pressure_gauge"] = _FailIfGaugeRead()
    transition_context = runner._new_sampling_window_context(point=_point_co2(), phase="co2", point_tag="preseal")
    runner._record_fast_signal_error(transition_context, "pressure_gauge", "NO_RESPONSE")
    runner._pressure_transition_fast_signal_context = transition_context

    try:
        value, source = runner._read_preseal_pressure_gauge()
    finally:
        runner._pressure_transition_fast_signal_context = None
        logger.close()

    assert value is None
    assert source == "pressure_gauge_cache_wait"


def test_refresh_pressure_transition_fast_signal_uses_transition_timeout_path(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    class _FakeDew:
        pass

    runner.devices["pace"] = object()
    runner.devices["pressure_gauge"] = object()
    runner.devices["dewpoint"] = _FakeDew()

    called: dict[str, object] = {"dewpoint_fast": 0}

    def _fake_read_pace_pressure_value(self, *, fast: bool = False) -> float:
        called["pace_fast"] = fast
        return 1000.8

    def _fake_read_pressure_gauge_value(self, *, fast: bool = False, purpose: str = "sampling") -> float:
        called["gauge_fast"] = fast
        called["purpose"] = purpose
        return 1001.2

    def _fake_get_current_fast(timeout_s: float = 0.35):
        called["dewpoint_fast"] = called["dewpoint_fast"] + 1
        called["dewpoint_timeout_s"] = timeout_s
        return {"dewpoint_c": -12.4, "temp_c": 21.5, "rh_pct": 44.1}

    runner._read_pace_pressure_value = types.MethodType(_fake_read_pace_pressure_value, runner)
    runner._read_pressure_gauge_value = types.MethodType(_fake_read_pressure_gauge_value, runner)
    runner.devices["dewpoint"].get_current_fast = _fake_get_current_fast  # type: ignore[attr-defined]
    context = runner._new_sampling_window_context(point=_point_co2(), phase="co2", point_tag="preseal")

    try:
        runner._refresh_pressure_transition_fast_signal_once(context, reason="test")
    finally:
        logger.close()

    assert called["pace_fast"] is True
    assert called["gauge_fast"] is True
    assert called["purpose"] == "transition"
    assert called["dewpoint_fast"] == 1
    assert len(runner._sampling_window_fast_signal_frames(context, "pace")) == 1
    assert len(runner._sampling_window_fast_signal_frames(context, "pressure_gauge")) == 1
    assert len(runner._sampling_window_fast_signal_frames(context, "dewpoint")) == 1
    assert runner._sampling_window_fast_signal_frames(context, "pace")[0]["values"]["pressure_hpa"] == 1000.8
    assert runner._sampling_window_fast_signal_frames(context, "pressure_gauge")[0]["values"]["pressure_gauge_hpa"] == 1001.2
    assert runner._sampling_window_fast_signal_frames(context, "dewpoint")[0]["values"]["dewpoint_live_c"] == -12.4


def test_set_pressure_to_target_reuses_preseal_ready_state_without_repeating_vent_off(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)

    class _FakePace:
        def __init__(self) -> None:
            self.output_state = 0
            self.setpoints: list[float] = []

        def read_pressure(self):
            return 1000.0

        def get_output_state(self):
            return self.output_state

        def get_isolation_state(self):
            return 1

        def get_vent_status(self):
            return 0

        def set_setpoint(self, value: float):
            self.setpoints.append(float(value))

        def set_output(self, on: bool):
            self.output_state = 1 if on else 0

        def get_in_limits(self):
            return 1000.0, 1

    pace = _FakePace()
    runner = CalibrationRunner({}, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    vent_calls: list[str] = []
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": vent_calls.append(f"{vent_on}:{reason}") or True,
        runner,
    )

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

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert vent_calls == []
    assert pace.setpoints == [1000.0]


def test_run_co2_point_skips_preseal_analyzer_stability_wait(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []

    runner._set_temperature = types.MethodType(lambda self, target: True, runner)
    runner._set_co2_route_baseline = types.MethodType(
        lambda self, reason="": calls.append(f"co2_baseline:{reason}"),
        runner,
    )
    runner._set_valves_for_co2 = types.MethodType(
        lambda self, point: calls.append(f"co2_valves_{getattr(point, 'index', None)}"),
        runner,
    )
    runner._refresh_pressure_controller_atmosphere_hold = types.MethodType(
        lambda self, force=False, reason="": calls.append(f"refresh_{bool(force)}:{reason}"),
        runner,
    )
    runner._wait_co2_route_soak_before_seal = types.MethodType(
        lambda self, point: calls.append(f"co2_soak_{getattr(point, 'index', None)}") or True,
        runner,
    )
    runner._wait_primary_sensor_stable = types.MethodType(
        lambda self, point, **kwargs: calls.append("sensor_wait_called") or False,
        runner,
    )
    runner._pressurize_and_hold = types.MethodType(
        lambda self, point, route="co2": calls.append(f"pressurize_{route}") or False,
        runner,
    )

    runner._run_co2_point(_point_co2())
    logger.close()

    assert calls == [
        "co2_baseline:before CO2 route conditioning",
        "co2_valves_3",
        "co2_soak_3",
        "pressurize_co2",
        "co2_baseline:after CO2 pressure-seal failure",
    ]


def test_run_co2_point_waits_after_pressure_stable_before_sampling(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []

    runner._set_temperature = types.MethodType(lambda self, target: True, runner)
    runner._set_co2_route_baseline = types.MethodType(lambda self, reason="": calls.append(f"co2_baseline:{reason}"), runner)
    runner._set_valves_for_co2 = types.MethodType(lambda self, point: calls.append("co2_valves"), runner)
    runner._refresh_pressure_controller_atmosphere_hold = types.MethodType(
        lambda self, force=False, reason="": calls.append(f"refresh_{bool(force)}:{reason}"),
        runner,
    )
    runner._wait_co2_route_soak_before_seal = types.MethodType(
        lambda self, point: calls.append("co2_soak") or True,
        runner,
    )
    runner._wait_primary_sensor_stable = types.MethodType(
        lambda self, point, **kwargs: calls.append("sensor_wait_called") or True,
        runner,
    )
    runner._pressurize_and_hold = types.MethodType(lambda self, point, route="co2": calls.append("pressurize") or True, runner)
    runner._set_pressure_to_target = types.MethodType(lambda self, point: calls.append("set_pressure") or True, runner)
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append("wait_pressure_delay") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )

    runner._run_co2_point(_point_co2())
    logger.close()

    assert calls == [
        "co2_baseline:before CO2 route conditioning",
        "co2_valves",
        "co2_soak",
        "pressurize",
        "set_pressure",
        "wait_pressure_delay",
        "sample_co2_co2_groupa_200ppm_1000hpa",
        "co2_baseline:after CO2 source complete",
    ]


def test_run_co2_point_ambient_only_samples_open_route_without_pressure_control(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    point = _point_co2()
    ambient_ref = runner._ambient_pressure_reference_point(point)

    runner._set_temperature = types.MethodType(lambda self, target: True, runner)
    runner._set_co2_route_baseline = types.MethodType(lambda self, reason="": calls.append(f"co2_baseline:{reason}"), runner)
    runner._set_valves_for_co2 = types.MethodType(lambda self, point: calls.append("co2_valves"), runner)
    runner._wait_co2_route_soak_before_seal = types.MethodType(lambda self, point: calls.append("co2_soak") or True, runner)
    runner._pressurize_and_hold = types.MethodType(lambda self, point, route="co2": calls.append("pressurize") or True, runner)
    runner._set_pressure_to_target = types.MethodType(lambda self, point: calls.append("set_pressure") or True, runner)
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append("wait_pressure_delay") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )

    runner._run_co2_point(point, pressure_points=[ambient_ref])
    logger.close()

    assert calls == [
        "co2_baseline:before CO2 route conditioning",
        "co2_valves",
        "co2_soak",
        "sample_co2_co2_groupa_200ppm_ambient",
        "co2_baseline:after CO2 source complete",
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    sampling_begin_rows = [row for row in trace_rows if row["trace_stage"] == "sampling_begin"]
    assert len(sampling_begin_rows) == 1
    assert sampling_begin_rows[0]["trigger_reason"] == "ambient_open_route"
    assert "route_open=true pressure_control=skipped" in sampling_begin_rows[0]["note"]


def test_run_co2_point_runs_ambient_before_sealed_pressure_control(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    point = _point_co2()
    ambient_ref = runner._ambient_pressure_reference_point(point)
    pressure_ref = CalibrationPoint(
        index=5,
        temp_chamber_c=point.temp_chamber_c,
        co2_ppm=point.co2_ppm,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=900.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    runner._set_temperature = types.MethodType(lambda self, target: True, runner)
    runner._set_co2_route_baseline = types.MethodType(lambda self, reason="": calls.append(f"co2_baseline:{reason}"), runner)
    runner._set_valves_for_co2 = types.MethodType(lambda self, point: calls.append("co2_valves"), runner)
    runner._wait_co2_route_soak_before_seal = types.MethodType(lambda self, point: calls.append("co2_soak") or True, runner)
    runner._pressurize_and_hold = types.MethodType(lambda self, point, route="co2": calls.append("pressurize") or True, runner)
    runner._set_pressure_to_target = types.MethodType(lambda self, point: calls.append("set_pressure") or True, runner)
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append("wait_pressure_delay") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )

    runner._run_co2_point(point, pressure_points=[ambient_ref, pressure_ref])
    logger.close()

    assert calls == [
        "co2_baseline:before CO2 route conditioning",
        "co2_valves",
        "co2_soak",
        "sample_co2_co2_groupa_200ppm_ambient",
        "pressurize",
        "set_pressure",
        "wait_pressure_delay",
        "sample_co2_co2_groupa_200ppm_900hpa",
        "co2_baseline:after CO2 source complete",
    ]


def test_run_co2_point_skips_preseal_topoff_when_1100_not_selected(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    point = _point_co2()
    ambient_ref = runner._ambient_pressure_reference_point(point)
    pressure_ref = CalibrationPoint(
        index=5,
        temp_chamber_c=point.temp_chamber_c,
        co2_ppm=point.co2_ppm,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=500.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    runner._set_temperature = types.MethodType(lambda self, target: True, runner)
    runner._set_co2_route_baseline = types.MethodType(lambda self, reason="": calls.append(f"co2_baseline:{reason}"), runner)
    runner._set_valves_for_co2 = types.MethodType(lambda self, point: calls.append("co2_valves"), runner)
    runner._wait_co2_route_soak_before_seal = types.MethodType(lambda self, point: calls.append("co2_soak") or True, runner)

    def pressurize(self, point, route="co2"):
        calls.append(f"pressurize_{bool(self._active_route_requires_preseal_topoff)}_{int(point.target_pressure_hpa or 0)}")
        return True

    runner._pressurize_and_hold = types.MethodType(pressurize, runner)
    runner._set_pressure_to_target = types.MethodType(lambda self, point: calls.append("set_pressure") or True, runner)
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append("wait_pressure_delay") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )

    runner._run_co2_point(point, pressure_points=[ambient_ref, pressure_ref])
    logger.close()

    assert calls == [
        "co2_baseline:before CO2 route conditioning",
        "co2_valves",
        "co2_soak",
        "sample_co2_co2_groupa_200ppm_ambient",
        "pressurize_False_500",
        "set_pressure",
        "wait_pressure_delay",
        "sample_co2_co2_groupa_200ppm_500hpa",
        "co2_baseline:after CO2 source complete",
    ]


def test_run_co2_point_keeps_preseal_topoff_when_1100_selected(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    point = _point_co2()
    ambient_ref = runner._ambient_pressure_reference_point(point)
    pressure_ref = CalibrationPoint(
        index=5,
        temp_chamber_c=point.temp_chamber_c,
        co2_ppm=point.co2_ppm,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    runner._set_temperature = types.MethodType(lambda self, target: True, runner)
    runner._set_co2_route_baseline = types.MethodType(lambda self, reason="": calls.append(f"co2_baseline:{reason}"), runner)
    runner._set_valves_for_co2 = types.MethodType(lambda self, point: calls.append("co2_valves"), runner)
    runner._wait_co2_route_soak_before_seal = types.MethodType(lambda self, point: calls.append("co2_soak") or True, runner)

    def pressurize(self, point, route="co2"):
        calls.append(f"pressurize_{bool(self._active_route_requires_preseal_topoff)}_{int(point.target_pressure_hpa or 0)}")
        return True

    runner._pressurize_and_hold = types.MethodType(pressurize, runner)
    runner._set_pressure_to_target = types.MethodType(lambda self, point: calls.append("set_pressure") or True, runner)
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append("wait_pressure_delay") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )

    runner._run_co2_point(point, pressure_points=[ambient_ref, pressure_ref])
    logger.close()

    assert calls == [
        "co2_baseline:before CO2 route conditioning",
        "co2_valves",
        "co2_soak",
        "sample_co2_co2_groupa_200ppm_ambient",
        "pressurize_True_1100",
        "set_pressure",
        "wait_pressure_delay",
        "sample_co2_co2_groupa_200ppm_1100hpa",
        "co2_baseline:after CO2 source complete",
    ]


def test_run_co2_point_reseals_once_after_pressure_timeout(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"co2_reseal_retry_count": 1}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    calls = []
    pressure_attempts = {"count": 0}
    pressure_ref = CalibrationPoint(
        index=5,
        temp_chamber_c=20.0,
        co2_ppm=200.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=550.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    runner._set_temperature = types.MethodType(lambda self, target: True, runner)
    runner._set_co2_route_baseline = types.MethodType(lambda self, reason="": calls.append(f"co2_baseline:{reason}"), runner)
    runner._set_valves_for_co2 = types.MethodType(
        lambda self, point: calls.append(f"co2_valves_{int(point.co2_ppm or 0)}"),
        runner,
    )
    runner._wait_co2_route_soak_before_seal = types.MethodType(lambda self, point: calls.append("co2_soak") or True, runner)
    runner._pressurize_and_hold = types.MethodType(
        lambda self, point, route="co2": calls.append(f"pressurize_{route}_{int(point.co2_ppm or 0)}") or True,
        runner,
    )

    def set_pressure(self, point):
        pressure_attempts["count"] += 1
        calls.append(f"set_pressure_{int(point.target_pressure_hpa or 0)}_{pressure_attempts['count']}")
        return pressure_attempts["count"] >= 2

    runner._set_pressure_to_target = types.MethodType(set_pressure, runner)
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append("wait_pressure_delay") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )

    runner._run_co2_point(_point_co2(), pressure_points=[pressure_ref])
    logger.close()

    assert calls == [
        "co2_baseline:before CO2 route conditioning",
        "co2_valves_200",
        "co2_soak",
        "pressurize_co2_200",
        "set_pressure_550_1",
        "set_pressure_550_2",
        "wait_pressure_delay",
        "sample_co2_co2_groupa_200ppm_550hpa",
        "co2_baseline:after CO2 source complete",
    ]


def test_run_co2_point_falls_back_to_lower_pressure_when_highest_seal_fails(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    pressure_refs = [
        CalibrationPoint(
            index=13,
            temp_chamber_c=40.0,
            co2_ppm=200.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=15,
            temp_chamber_c=40.0,
            co2_ppm=200.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1000.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=17,
            temp_chamber_c=40.0,
            co2_ppm=200.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=900.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]

    runner._set_temperature = types.MethodType(lambda self, target: True, runner)
    runner._set_co2_route_baseline = types.MethodType(
        lambda self, reason="": calls.append(f"co2_baseline:{reason}"),
        runner,
    )
    runner._set_valves_for_co2 = types.MethodType(lambda self, point: calls.append(f"co2_valves_{int(point.co2_ppm or 0)}"), runner)
    runner._wait_co2_route_soak_before_seal = types.MethodType(lambda self, point: calls.append("co2_soak") or True, runner)

    def pressurize(self, point, route="co2"):
        calls.append(f"pressurize_{int(point.target_pressure_hpa or 0)}")
        return int(point.target_pressure_hpa or 0) <= 1000

    runner._pressurize_and_hold = types.MethodType(pressurize, runner)
    runner._set_pressure_to_target = types.MethodType(
        lambda self, point: calls.append(f"set_pressure_{int(point.target_pressure_hpa or 0)}") or True,
        runner,
    )
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append(f"wait_pressure_delay_{int(point.target_pressure_hpa or 0)}") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )

    runner._run_co2_point(_point_co2(), pressure_points=pressure_refs)
    logger.close()

    assert calls == [
        "co2_baseline:before CO2 route conditioning",
        "co2_valves_200",
        "co2_soak",
        "pressurize_1100",
        "pressurize_1000",
        "set_pressure_1000",
        "wait_pressure_delay_1000",
        "sample_co2_co2_groupa_200ppm_1000hpa",
        "set_pressure_900",
        "wait_pressure_delay_900",
        "sample_co2_co2_groupa_200ppm_900hpa",
        "co2_baseline:after CO2 source complete",
    ]


def test_wait_co2_route_soak_does_not_refresh_atmosphere_hold(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"stability": {"co2_route": {"preseal_soak_s": 3}}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    calls = []
    point = _point_co2()

    runner._refresh_pressure_controller_atmosphere_hold = types.MethodType(
        lambda self, force=False, reason="": calls.append(f"refresh_{bool(force)}:{reason}"),
        runner,
    )

    clock = {"now": 0.0}

    def fake_time() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "time", fake_time)
    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    assert runner._wait_co2_route_soak_before_seal(point) is True
    logger.close()

    assert calls == []


def test_wait_co2_route_soak_uses_post_h2o_zero_flush(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "co2_route": {
                        "preseal_soak_s": 180,
                        "post_h2o_zero_ppm_soak_s": 600,
                    }
                }
            }
        },
        {},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_co2()
    point.co2_ppm = 0.0
    runner._post_h2o_co2_zero_flush_pending = True

    clock = {"now": 0.0}

    def fake_time() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "time", fake_time)
    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    assert runner._wait_co2_route_soak_before_seal(point) is True
    logger.close()

    assert runner._post_h2o_co2_zero_flush_pending is False
    assert runner._active_post_h2o_co2_zero_flush is True
    assert any("wait 600s before pressure sealing" in message for message in messages)


def test_wait_co2_route_soak_uses_longer_duration_for_first_gas_point(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "co2_route": {
                        "preseal_soak_s": 180,
                        "first_point_preseal_soak_s": 420,
                    }
                }
            }
        },
        {},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_co2()
    point.co2_ppm = 200.0

    clock = {"now": 0.0}

    def fake_time() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "time", fake_time)
    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    assert runner._wait_co2_route_soak_before_seal(point) is True
    logger.close()

    assert runner._first_co2_route_soak_pending is False
    assert any("wait 420s before pressure sealing" in message for message in messages)


def test_wait_co2_route_soak_uses_initial_zero_flush_in_co2_only(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "route_mode": "co2_only",
                "stability": {
                    "co2_route": {
                        "preseal_soak_s": 180,
                        "post_h2o_zero_ppm_soak_s": 600,
                    }
                },
            }
        },
        {},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_co2()
    point.co2_ppm = 0.0

    clock = {"now": 0.0}

    def fake_time() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "time", fake_time)
    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    assert runner._wait_co2_route_soak_before_seal(point) is True
    logger.close()

    assert runner._initial_co2_zero_flush_pending is False
    assert runner._active_post_h2o_co2_zero_flush is True
    assert any("wait 600s before pressure sealing" in message for message in messages)


def test_wait_co2_route_soak_does_not_read_dewpoint_gate_when_disabled(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "co2_route": {"preseal_soak_s": 3},
                    "gas_route_dewpoint_gate_enabled": False,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2()
    calls: list[str] = []
    clock = {"now": 0.0}
    runner._first_co2_route_soak_pending = False

    runner._read_precondition_dewpoint_gate_snapshot = types.MethodType(
        lambda self: calls.append("gate_read") or {"dewpoint_c": -30.0, "temp_c": 22.0, "rh_pct": 5.0},
        runner,
    )
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(runner_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))

    assert runner._wait_co2_route_soak_before_seal(point) is True
    logger.close()

    assert calls == []


def test_wait_co2_route_soak_runs_dewpoint_gate_only_after_fixed_soak_and_waits_until_stable(
    monkeypatch,
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "co2_route": {"preseal_soak_s": 3},
                    "gas_route_dewpoint_gate_enabled": True,
                    "gas_route_dewpoint_gate_window_s": 5.0,
                    "gas_route_dewpoint_gate_max_total_wait_s": 12.0,
                    "gas_route_dewpoint_gate_poll_s": 1.0,
                    "gas_route_dewpoint_gate_tail_span_max_c": 0.02,
                    "gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s": 0.02,
                    "gas_route_dewpoint_gate_log_interval_s": 1.0,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2()
    clock = {"now": 0.0}
    gate_read_times: list[float] = []
    dewpoints = iter([-30.0, -29.9, -29.95, -29.95, -29.95, -29.95, -29.95, -29.95])
    runner._first_co2_route_soak_pending = False

    def fake_snapshot(self):
        gate_read_times.append(clock["now"])
        return {"dewpoint_c": next(dewpoints), "temp_c": 22.0, "rh_pct": 5.0}

    def fake_gate_row(self, *, total_elapsed_s, snapshot):
        base = datetime(2026, 4, 3, 9, 0, 0)
        return {
            "timestamp": (base + timedelta(seconds=float(total_elapsed_s))).isoformat(timespec="seconds"),
            "phase_elapsed_s": float(total_elapsed_s),
            "phase": "co2_route_precondition",
            "controller_vent_state": "VENT_ON",
            "dewpoint_c": snapshot.get("dewpoint_c"),
            "dewpoint_temp_c": snapshot.get("temp_c"),
            "dewpoint_rh_percent": snapshot.get("rh_pct"),
        }

    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(runner_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))
    runner._read_precondition_dewpoint_gate_snapshot = types.MethodType(fake_snapshot, runner)
    runner._build_co2_route_dewpoint_gate_row = types.MethodType(fake_gate_row, runner)

    assert runner._wait_co2_route_soak_before_seal(point) is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2")
    assert gate_read_times[0] >= 3.0
    assert state is not None
    assert state["flush_gate_status"] == "pass"
    assert float(state["dewpoint_time_to_gate"]) > 3.0


def test_wait_co2_route_soak_fails_when_dewpoint_gate_times_out(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "co2_route": {"preseal_soak_s": 2},
                    "gas_route_dewpoint_gate_enabled": True,
                    "gas_route_dewpoint_gate_window_s": 2.0,
                    "gas_route_dewpoint_gate_max_total_wait_s": 4.0,
                    "gas_route_dewpoint_gate_poll_s": 1.0,
                    "gas_route_dewpoint_gate_tail_span_max_c": 0.01,
                    "gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s": 0.005,
                    "gas_route_dewpoint_gate_log_interval_s": 1.0,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2()
    clock = {"now": 0.0}
    dewpoints = iter([-30.0, -29.8, -29.6, -29.4, -29.2, -29.0])
    runner._first_co2_route_soak_pending = False

    def fake_gate_row(self, *, total_elapsed_s, snapshot):
        base = datetime(2026, 4, 3, 9, 0, 0)
        return {
            "timestamp": (base + timedelta(seconds=float(total_elapsed_s))).isoformat(timespec="seconds"),
            "phase_elapsed_s": float(total_elapsed_s),
            "phase": "co2_route_precondition",
            "controller_vent_state": "VENT_ON",
            "dewpoint_c": snapshot.get("dewpoint_c"),
            "dewpoint_temp_c": snapshot.get("temp_c"),
            "dewpoint_rh_percent": snapshot.get("rh_pct"),
        }

    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(runner_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))
    runner._read_precondition_dewpoint_gate_snapshot = types.MethodType(
        lambda self: {"dewpoint_c": next(dewpoints), "temp_c": 22.0, "rh_pct": 5.0},
        runner,
    )
    runner._build_co2_route_dewpoint_gate_row = types.MethodType(fake_gate_row, runner)

    assert runner._wait_co2_route_soak_before_seal(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2")
    assert state is not None
    assert state["flush_gate_status"] == "timeout"
    assert float(state["dewpoint_time_to_gate"]) >= 6.0
    assert "max_total_wait_exceeded" in str(state["flush_gate_reason"])


def test_wait_first_co2_route_soak_uses_after_soak_timeout_budget(
    monkeypatch,
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "co2_route": {
                        "preseal_soak_s": 2,
                        "first_point_preseal_soak_s": 5,
                    },
                    "gas_route_dewpoint_gate_enabled": True,
                    "gas_route_dewpoint_gate_window_s": 4.0,
                    "gas_route_dewpoint_gate_max_total_wait_s": 3.0,
                    "gas_route_dewpoint_gate_poll_s": 1.0,
                    "gas_route_dewpoint_gate_tail_span_max_c": 0.01,
                    "gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s": 0.005,
                    "gas_route_dewpoint_gate_log_interval_s": 1.0,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2()
    clock = {"now": 0.0}
    dewpoints = iter([-30.0, -29.8, -29.6, -29.4, -29.2, -29.0])

    def fake_snapshot(self):
        return {"dewpoint_c": next(dewpoints), "temp_c": 22.0, "rh_pct": 5.0}

    def fake_gate_row(self, *, total_elapsed_s, snapshot):
        base = datetime(2026, 4, 3, 9, 0, 0)
        return {
            "timestamp": (base + timedelta(seconds=float(total_elapsed_s))).isoformat(timespec="seconds"),
            "phase_elapsed_s": float(total_elapsed_s),
            "phase": "co2_route_precondition",
            "controller_vent_state": "VENT_ON",
            "dewpoint_c": snapshot.get("dewpoint_c"),
            "dewpoint_temp_c": snapshot.get("temp_c"),
            "dewpoint_rh_percent": snapshot.get("rh_pct"),
        }

    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(
        runner_module.time,
        "sleep",
        lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)),
    )
    runner._read_precondition_dewpoint_gate_snapshot = types.MethodType(fake_snapshot, runner)
    runner._build_co2_route_dewpoint_gate_row = types.MethodType(fake_gate_row, runner)

    assert runner._wait_co2_route_soak_before_seal(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2")
    assert state is not None
    assert state["flush_gate_status"] == "timeout"
    assert 8.0 <= float(state["dewpoint_time_to_gate"]) < 9.5
    assert "max_total_wait_exceeded" in str(state["flush_gate_reason"])


def test_wait_co2_route_soak_warn_policy_allows_following_pressure_seal(
    monkeypatch,
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "co2_route": {"preseal_soak_s": 2},
                    "gas_route_dewpoint_gate_enabled": True,
                    "gas_route_dewpoint_gate_policy": "warn",
                    "gas_route_dewpoint_gate_window_s": 2.0,
                    "gas_route_dewpoint_gate_max_total_wait_s": 4.0,
                    "gas_route_dewpoint_gate_poll_s": 1.0,
                    "gas_route_dewpoint_gate_tail_span_max_c": 0.01,
                    "gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s": 0.005,
                    "gas_route_dewpoint_gate_log_interval_s": 1.0,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2()
    clock = {"now": 0.0}
    dewpoints = iter([-30.0, -29.8, -29.6, -29.4, -29.2, -29.0])
    trace_stages: list[str] = []

    def fake_gate_row(self, *, total_elapsed_s, snapshot):
        base = datetime(2026, 4, 3, 9, 0, 0)
        return {
            "timestamp": (base + timedelta(seconds=float(total_elapsed_s))).isoformat(timespec="seconds"),
            "phase_elapsed_s": float(total_elapsed_s),
            "phase": "co2_route_precondition",
            "controller_vent_state": "VENT_ON",
            "dewpoint_c": snapshot.get("dewpoint_c"),
            "dewpoint_temp_c": snapshot.get("temp_c"),
            "dewpoint_rh_percent": snapshot.get("rh_pct"),
        }

    def fake_trace(self, **kwargs):
        trace_stages.append(str(kwargs.get("trace_stage") or ""))

    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(runner_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))
    runner._read_precondition_dewpoint_gate_snapshot = types.MethodType(
        lambda self: {"dewpoint_c": next(dewpoints), "temp_c": 22.0, "rh_pct": 5.0},
        runner,
    )
    runner._build_co2_route_dewpoint_gate_row = types.MethodType(fake_gate_row, runner)
    runner._append_pressure_trace_row = types.MethodType(fake_trace, runner)

    assert runner._wait_co2_route_soak_before_seal(point) is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2")
    assert state is not None
    assert state["flush_gate_status"] == "timeout"
    assert "max_total_wait_exceeded" in str(state["flush_gate_reason"])
    assert "co2_precondition_dewpoint_gate_end" in trace_stages


def test_build_point_summary_row_includes_co2_precondition_dewpoint_gate_fields(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2()
    point_tag = runner._co2_point_tag(point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        dewpoint_time_to_gate=205.0,
        dewpoint_tail_span_60s=0.08,
        dewpoint_tail_slope_60s=0.001,
        dewpoint_rebound_detected=False,
        flush_gate_status="pass",
        flush_gate_reason="",
    )

    row = runner._build_point_summary_row(
        point,
        [],
        phase="co2",
        point_tag=point_tag,
        integrity_summary={},
    )
    logger.close()

    assert row["dewpoint_time_to_gate"] == 205.0
    assert row["dewpoint_tail_span_60s"] == 0.08
    assert row["dewpoint_tail_slope_60s"] == 0.001
    assert row["dewpoint_rebound_detected"] is False
    assert row["flush_gate_status"] == "pass"
    assert row["flush_gate_reason"] == ""


def test_pressurize_and_hold_uses_post_h2o_vent_off_wait(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 15,
                    "co2_post_h2o_vent_off_wait_s": 5,
                    "pressurize_high_hpa": 0,
                    "pressurize_timeout_s": 0,
                }
            }
        },
        {"pace": types.SimpleNamespace(read_pressure=lambda: 1000.0)},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    sleeps: list[float] = []
    point = _point_co2()
    clock = {"now": 0.0}

    runner._active_post_h2o_co2_zero_flush = True
    runner._set_pressure_controller_vent = types.MethodType(lambda self, vent_on, reason="": None, runner)
    runner._apply_valve_states = types.MethodType(lambda self, open_valves: None, runner)
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    assert runner._pressurize_and_hold(point, route="co2") is True
    logger.close()

    assert sum(sleeps) == 5.0
    assert all(value == 0.5 for value in sleeps)
    assert runner._active_post_h2o_co2_zero_flush is False


def test_pressurize_and_hold_waits_for_invalid_gauge_before_timeout_when_threshold_not_reached(
    monkeypatch,
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 1.0,
                    "co2_post_h2o_vent_off_wait_s": 1.0,
                    "co2_preseal_pressure_gauge_trigger_hpa": 1110.0,
                },
                "sampling": {"fast_signal_worker_enabled": False},
            }
        },
        {"pace": types.SimpleNamespace(read_pressure=lambda: 1082.1)},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_co2()
    sleeps: list[float] = []
    clock = {"now": 0.0}

    class _FakeGauge:
        def __init__(self) -> None:
            self.values = iter([1108.0, 1108.6, 1109.1, float("nan"), float("nan"), float("nan")])

        def read_pressure(self):
            return next(self.values)

    runner.devices["pressure_gauge"] = _FakeGauge()
    runner._active_post_h2o_co2_zero_flush = False
    runner._set_pressure_controller_vent = types.MethodType(lambda self, vent_on, reason="": None, runner)
    runner._apply_valve_states = types.MethodType(lambda self, open_valves: None, runner)
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    assert runner._pressurize_and_hold(point, route="co2") is True
    logger.close()

    assert sleeps == [0.5, 0.5, 0.5, 0.5]
    assert not any("fallback timeout=1.000s with pressure gauge" in message for message in messages)
    assert any("became continuously invalid" in message for message in messages)


def test_pressurize_and_hold_keeps_timeout_fallback_when_pressure_gauge_is_unavailable(
    monkeypatch,
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 1.0,
                    "co2_post_h2o_vent_off_wait_s": 1.0,
                    "co2_preseal_pressure_gauge_trigger_hpa": 1110.0,
                }
            }
        },
        {"pace": types.SimpleNamespace(read_pressure=lambda: 1082.1)},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_co2()
    sleeps: list[float] = []
    clock = {"now": 0.0}

    runner._active_post_h2o_co2_zero_flush = False
    runner._set_pressure_controller_vent = types.MethodType(lambda self, vent_on, reason="": None, runner)
    runner._apply_valve_states = types.MethodType(lambda self, open_valves: None, runner)
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    assert runner._pressurize_and_hold(point, route="co2") is True
    logger.close()

    assert sleeps == [0.5, 0.5]
    assert any("fallback timeout without valid pressure gauge after 1.000s" in message for message in messages)


def test_pressurize_and_hold_fails_when_valid_pressure_gauge_stalls_below_threshold(
    monkeypatch,
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 1.0,
                    "co2_preseal_pressure_gauge_trigger_hpa": 1110.0,
                    "preseal_valid_gauge_stall_window_s": 2.0,
                    "preseal_valid_gauge_min_rise_hpa": 0.5,
                },
                "sampling": {"fast_signal_worker_enabled": False},
            }
        },
        {"pace": types.SimpleNamespace(read_pressure=lambda: 1082.1)},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_co2()
    clock = {"now": 0.0}
    sleeps: list[float] = []

    class _FakeGauge:
        def __init__(self) -> None:
            self.values = iter([1108.0, 1108.1, 1108.1, 1108.2, 1108.2, 1108.2, 1108.2])

        def read_pressure(self):
            return next(self.values)

    runner.devices["pressure_gauge"] = _FakeGauge()
    runner._set_pressure_controller_vent = types.MethodType(lambda self, vent_on, reason="": None, runner)
    runner._apply_valve_states = types.MethodType(lambda self, open_valves: None, runner)
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    assert runner._pressurize_and_hold(point, route="co2") is False
    logger.close()

    assert sleeps == [0.5, 0.5, 0.5, 0.5]
    assert any("valid gauge stall detected" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    fail_rows = [row for row in trace_rows if row["trace_stage"] == "preseal_fail"]
    assert len(fail_rows) == 1
    assert fail_rows[0]["trigger_reason"] == "preseal_valid_gauge_stall"
    assert fail_rows[0]["pressure_gauge_hpa"] == "1108.2"


def test_pressurize_and_hold_h2o_seals_early_when_pressure_gauge_reaches_threshold(
    monkeypatch,
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 5.0,
                    "h2o_preseal_pressure_gauge_trigger_hpa": 1110.0,
                },
                "sampling": {"fast_signal_worker_enabled": False},
            }
        },
        {},
        logger,
        messages.append,
        lambda *_: None,
    )

    class _FakePace:
        def read_pressure(self):
            return 1088.4

    class _FakeGauge:
        def __init__(self) -> None:
            self.values = iter([1108.0, 1110.0])

        def read_pressure(self):
            return next(self.values)

    clock = {"now": 0.0}
    sleeps: list[float] = []
    calls: list[str] = []

    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    runner.devices["pace"] = _FakePace()
    runner.devices["pressure_gauge"] = _FakeGauge()
    runner._capture_preseal_dewpoint_snapshot = types.MethodType(
        lambda self: setattr(
            self,
            "_preseal_dewpoint_snapshot",
            {"dewpoint_c": -9.8, "temp_c": 24.0, "rh_pct": 52.3},
        ),
        runner,
    )
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}:{reason}"),
        runner,
    )
    runner._set_h2o_path = types.MethodType(
        lambda self, is_open, point=None: calls.append(f"h2o_path_{bool(is_open)}"),
        runner,
    )

    assert runner._pressurize_and_hold(_point_h2o(), route="h2o") is True
    logger.close()

    assert calls == [
        "vent_False:before H2O pressure seal",
        "h2o_path_False",
    ]
    assert sleeps == []
    assert any("pressure gauge trigger=1110.000 hPa >= 1110.000 hPa" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    trigger_rows = [row for row in trace_rows if row["trace_stage"] == "preseal_trigger_reached"]
    assert len(trigger_rows) == 1
    trigger_row = trigger_rows[0]
    assert trigger_row["trigger_reason"] == "pressure_gauge_threshold"
    assert float(trigger_row["pressure_gauge_hpa"]) == 1110.0
    assert float(trigger_row["dewpoint_c"]) == -9.8
    assert float(trigger_row["dew_temp_c"]) == 24.0
    assert float(trigger_row["dew_rh_pct"]) == 52.3


def test_pressurize_and_hold_h2o_waits_for_invalid_gauge_before_timeout_when_threshold_not_reached(
    monkeypatch,
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 1.0,
                    "h2o_preseal_pressure_gauge_trigger_hpa": 1110.0,
                },
                "sampling": {"fast_signal_worker_enabled": False},
            }
        },
        {"pace": types.SimpleNamespace(read_pressure=lambda: 1088.4)},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_h2o()
    sleeps: list[float] = []
    clock = {"now": 0.0}

    class _FakeGauge:
        def __init__(self) -> None:
            self.values = iter([1108.0, 1108.7, 1109.3, float("nan"), float("nan"), float("nan")])

        def read_pressure(self):
            return next(self.values)

    runner.devices["pressure_gauge"] = _FakeGauge()
    runner._capture_preseal_dewpoint_snapshot = types.MethodType(
        lambda self: setattr(
            self,
            "_preseal_dewpoint_snapshot",
            {"dewpoint_c": -9.8, "temp_c": 24.0, "rh_pct": 52.3},
        ),
        runner,
    )
    runner._set_pressure_controller_vent = types.MethodType(lambda self, vent_on, reason="": None, runner)
    runner._set_h2o_path = types.MethodType(lambda self, is_open, point=None: None, runner)
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    assert runner._pressurize_and_hold(point, route="h2o") is True
    logger.close()

    assert sleeps == [0.5, 0.5, 0.5, 0.5]
    assert not any("fallback timeout=1.000s with pressure gauge" in message for message in messages)
    assert any("became continuously invalid" in message for message in messages)


def test_pressurize_and_hold_seals_immediately_when_preseal_topoff_disabled(
    monkeypatch,
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "pressurize_wait_after_vent_off_s": 5.0,
                    "co2_preseal_pressure_gauge_trigger_hpa": 1110.0,
                }
            }
        },
        {"pace": types.SimpleNamespace(read_pressure=lambda: 1007.2)},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_co2()
    sleeps: list[float] = []
    clock = {"now": 0.0}
    calls: list[str] = []

    runner._active_route_requires_preseal_topoff = False
    runner._capture_preseal_dewpoint_snapshot = types.MethodType(
        lambda self, prefer_cached_pressure=False: None,
        runner,
    )
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}:{reason}"),
        runner,
    )
    runner._apply_valve_states = types.MethodType(
        lambda self, open_valves: calls.append(f"valves_{list(open_valves)}"),
        runner,
    )
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["now"] += float(seconds)

    monkeypatch.setattr(runner_module.time, "sleep", fake_sleep)

    assert runner._pressurize_and_hold(point, route="co2") is True
    logger.close()

    assert calls == [
        "vent_False:before CO2 pressure seal",
        "valves_[]",
    ]
    assert sleeps == []
    assert any("preseal top-off skipped" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    trigger_rows = [row for row in trace_rows if row["trace_stage"] == "preseal_trigger_reached"]
    assert len(trigger_rows) == 1
    assert trigger_rows[0]["trigger_reason"] == "no_wait"
    assert "selected sealed pressures do not include 1100hPa" in trigger_rows[0]["note"]


def test_run_h2o_group_skips_preseal_topoff_when_1100_not_selected(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    lead = _point_h2o()
    ambient_ref = runner._ambient_pressure_reference_point(lead)
    pressure_ref = CalibrationPoint(
        index=11,
        temp_chamber_c=lead.temp_chamber_c,
        co2_ppm=None,
        hgen_temp_c=lead.hgen_temp_c,
        hgen_rh_pct=lead.hgen_rh_pct,
        target_pressure_hpa=500.0,
        dewpoint_c=lead.dewpoint_c,
        h2o_mmol=lead.h2o_mmol,
        raw_h2o=lead.raw_h2o,
    )

    runner._set_h2o_path = types.MethodType(lambda self, is_open, point=None: calls.append(f"h2o_path_{bool(is_open)}"), runner)
    runner._prepare_humidity_generator = types.MethodType(lambda self, point: calls.append("prepare_humidity"), runner)
    runner._prepare_pressure_for_h2o = types.MethodType(lambda self, point: calls.append("prepare_pressure"), runner)
    runner._set_temperature_for_point = types.MethodType(lambda self, point, phase="": calls.append("set_temperature") or True, runner)
    runner._wait_humidity_generator_stable = types.MethodType(lambda self, point: calls.append("wait_hgen_setpoint") or True, runner)
    runner._open_h2o_route_and_wait_ready = types.MethodType(
        lambda self, point, point_tag="": calls.append("open_route_ready") or True,
        runner,
    )
    runner._wait_h2o_route_soak_before_seal = types.MethodType(
        lambda self, point: calls.append("h2o_preseal_soak") or True,
        runner,
    )

    def pressurize(self, point, route="h2o"):
        calls.append(f"pressurize_{bool(self._active_route_requires_preseal_topoff)}_{route}")
        return True

    runner._pressurize_and_hold = types.MethodType(pressurize, runner)
    runner._set_pressure_to_target = types.MethodType(lambda self, point: calls.append(f"set_pressure_{int(point.target_pressure_hpa or 0)}") or True, runner)
    runner._wait_after_pressure_stable_before_sampling = types.MethodType(
        lambda self, point: calls.append(f"wait_pressure_delay_{int(point.target_pressure_hpa or 0)}") or True,
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}"),
        runner,
    )
    runner._apply_route_baseline_valves = types.MethodType(lambda self: calls.append("baseline_route"), runner)

    runner._run_h2o_group([lead], pressure_points=[ambient_ref, pressure_ref])
    logger.close()

    assert calls == [
        "h2o_path_False",
        "prepare_pressure",
        "prepare_humidity",
        "set_temperature",
        "wait_hgen_setpoint",
        "open_route_ready",
        "sample_h2o_h2o_20c_30rh_ambient",
        "h2o_preseal_soak",
        "pressurize_False_h2o",
        "set_pressure_500",
        "wait_pressure_delay_500",
        "sample_h2o_h2o_20c_30rh_500hpa",
        "vent_True",
        "baseline_route",
    ]


def test_run_h2o_group_ambient_only_skips_preseal_soak(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    lead = _point_h2o()
    ambient_ref = runner._ambient_pressure_reference_point(lead)

    runner._set_h2o_path = types.MethodType(lambda self, is_open, point=None: calls.append(f"h2o_path_{bool(is_open)}"), runner)
    runner._prepare_humidity_generator = types.MethodType(lambda self, point: calls.append("prepare_humidity"), runner)
    runner._prepare_pressure_for_h2o = types.MethodType(lambda self, point: calls.append("prepare_pressure"), runner)
    runner._set_temperature_for_point = types.MethodType(lambda self, point, phase="": calls.append("set_temperature") or True, runner)
    runner._wait_humidity_generator_stable = types.MethodType(lambda self, point: calls.append("wait_hgen_setpoint") or True, runner)
    runner._open_h2o_route_and_wait_ready = types.MethodType(
        lambda self, point, point_tag="": calls.append("open_route_ready") or True,
        runner,
    )
    runner._wait_h2o_route_soak_before_seal = types.MethodType(
        lambda self, point: (_ for _ in ()).throw(AssertionError("ambient-only H2O should not run preseal soak")),
        runner,
    )
    runner._sample_and_log = types.MethodType(
        lambda self, point, phase="", point_tag="": calls.append(f"sample_{phase}_{point_tag}"),
        runner,
    )
    runner._set_pressure_controller_vent = types.MethodType(
        lambda self, vent_on, reason="": calls.append(f"vent_{bool(vent_on)}"),
        runner,
    )
    runner._apply_route_baseline_valves = types.MethodType(lambda self: calls.append("baseline_route"), runner)

    runner._run_h2o_group([lead], pressure_points=[ambient_ref])
    logger.close()

    assert calls == [
        "h2o_path_False",
        "prepare_pressure",
        "prepare_humidity",
        "set_temperature",
        "wait_hgen_setpoint",
        "open_route_ready",
        "sample_h2o_h2o_20c_30rh_ambient",
        "vent_True",
        "baseline_route",
    ]


def test_run_temperature_group_skip_h2o_only_runs_co2(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({"workflow": {"skip_h2o": True}}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []

    runner._run_h2o_group = types.MethodType(
        lambda self, group, pressure_points=None, next_route_context=None: calls.append("run_h2o_group"),
        runner,
    )
    runner._run_co2_point = types.MethodType(
        lambda self, point, pressure_points=None, next_route_context=None: calls.append(int(point.co2_ppm or 0)),
        runner,
    )

    points = [_point_h2o(), _point_co2()]
    runner._run_temperature_group(points)
    logger.close()

    assert calls == [200]


def test_run_temperature_group_h2o_only_skips_co2(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({"workflow": {"route_mode": "h2o_only"}}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []

    runner._run_h2o_group = types.MethodType(
        lambda self, group, pressure_points=None, next_route_context=None: calls.append("run_h2o_group"),
        runner,
    )
    runner._run_co2_point = types.MethodType(
        lambda self, point, pressure_points=None, next_route_context=None: calls.append(
            f"run_co2_{getattr(point, 'index', None)}"
        ),
        runner,
    )

    points = [_point_h2o(), _point_co2()]
    runner._run_temperature_group(points)
    logger.close()

    assert calls == ["run_h2o_group"]


def test_wait_after_pressure_stable_co2_starts_sampling_immediately_and_traces_begin(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"post_stable_sample_delay_s": 0.0, "co2_post_stable_sample_delay_s": 0.0}}},
        {},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_co2()
    runner._wait_postseal_dewpoint_gate = types.MethodType(lambda self, point, phase="", context=None: True, runner)
    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    assert any("CO2 pressure stable; post-seal dewpoint gate complete; start sampling immediately" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    sampling_begin_rows = [row for row in trace_rows if row["trace_stage"] == "sampling_begin"]
    assert len(sampling_begin_rows) == 1
    assert sampling_begin_rows[0]["trigger_reason"] == "co2_post_stable_immediate"


def test_wait_after_pressure_stable_co2_honors_configured_delay(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"post_stable_sample_delay_s": 0.0, "co2_post_stable_sample_delay_s": 10.0}}},
        {},
        logger,
        messages.append,
        lambda *_: None,
    )
    waits: list[float] = []
    runner._sampling_window_wait = types.MethodType(
        lambda self, duration_s, stop_event=None: waits.append(float(duration_s)) or True,
        runner,
    )
    runner._wait_postseal_dewpoint_gate = types.MethodType(lambda self, point, phase="", context=None: True, runner)

    point = _point_co2()
    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    assert waits == [10.0]
    assert any("waiting 10.0s before sampling" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    sampling_begin_rows = [row for row in trace_rows if row["trace_stage"] == "sampling_begin"]
    assert len(sampling_begin_rows) == 1
    assert sampling_begin_rows[0]["trigger_reason"] == "co2_post_stable_delay_elapsed"
    assert "configured_delay_s=10.0" in sampling_begin_rows[0]["note"]


def test_wait_after_pressure_stable_co2_tops_up_minimum_delay_from_pressure_in_limits(
    monkeypatch,
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"post_stable_sample_delay_s": 0.0, "co2_post_stable_sample_delay_s": 10.0}}},
        {},
        logger,
        messages.append,
        lambda *_: None,
    )
    waits: list[float] = []
    point = _point_co2()
    clock = {"now": 100.0}

    runner._sampling_window_wait = types.MethodType(
        lambda self, duration_s, stop_event=None: waits.append(float(duration_s)) or True,
        runner,
    )
    runner._wait_postseal_dewpoint_gate = types.MethodType(lambda self, point, phase="", context=None: True, runner)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        timing_stages={"pressure_in_limits": 94.0},
    )
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])

    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    assert waits == [4.0]
    assert any("elapsed since pressure_in_limits=6.0s" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    sampling_begin_rows = [row for row in trace_rows if row["trace_stage"] == "sampling_begin"]
    assert len(sampling_begin_rows) == 1
    assert sampling_begin_rows[0]["trigger_reason"] == "co2_post_stable_delay_elapsed"
    assert "waited_remaining_s=4.000" in sampling_begin_rows[0]["note"]


def test_wait_after_pressure_stable_ready_check_uses_cached_fast_values(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    now_mono = runner_module.time.monotonic()
    runner._pressure_transition_fast_signal_context = {
        "stop_event": None,
        "fast_signal_buffers": {
            "pace": [
                {
                    "recv_mono_s": now_mono - 0.2,
                    "values": {"pressure_hpa": 1100.12},
                }
            ],
            "pressure_gauge": [
                {
                    "recv_mono_s": now_mono - 0.15,
                    "values": {"pressure_gauge_hpa": 1099.31},
                }
            ],
            "dewpoint": [
                {
                    "recv_mono_s": now_mono - 2.8,
                    "values": {"dewpoint_live_c": -35.20, "dew_temp_live_c": 20.1, "dew_rh_live_pct": 52.0},
                },
                {
                    "recv_mono_s": now_mono - 1.4,
                    "values": {"dewpoint_live_c": -35.18, "dew_temp_live_c": 20.1, "dew_rh_live_pct": 52.1},
                },
                {
                    "recv_mono_s": now_mono - 0.3,
                    "values": {"dewpoint_live_c": -35.17, "dew_temp_live_c": 20.2, "dew_rh_live_pct": 52.2},
                },
            ],
        },
    }

    point = _point_co2()
    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    trace_rows = _load_pressure_trace_rows(logger)
    ready_rows = [row for row in trace_rows if row["trace_stage"] == "pressure_in_limits_ready_check"]
    assert len(ready_rows) == 1
    ready_row = ready_rows[0]
    assert ready_row["pace_pressure_hpa"] == "1100.12"
    assert ready_row["pressure_gauge_hpa"] == "1099.31"
    assert ready_row["dewpoint_c"] == "-35.17"
    assert "cached fast confirm before sampling" in ready_row["note"]
    assert "dewpoint_span_c=" in ready_row["note"]


def test_wait_after_pressure_stable_h2o_starts_sampling_immediately_and_traces_begin(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"post_stable_sample_delay_s": 0.0, "co2_post_stable_sample_delay_s": 0.0}}},
        {},
        logger,
        messages.append,
        lambda *_: None,
    )
    runner._wait_postseal_dewpoint_gate = types.MethodType(lambda self, point, phase="", context=None: True, runner)
    assert runner._wait_after_pressure_stable_before_sampling(_point_h2o()) is True
    logger.close()

    assert any("H2O pressure stable; post-seal dewpoint gate complete; start sampling immediately" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    sampling_begin_rows = [row for row in trace_rows if row["trace_stage"] == "sampling_begin"]
    assert len(sampling_begin_rows) == 1
    assert sampling_begin_rows[0]["trigger_reason"] == "h2o_post_stable_immediate"


def test_wait_after_pressure_stable_h2o_honors_configured_delay(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"post_stable_sample_delay_s": 10.0, "co2_post_stable_sample_delay_s": 0.0}}},
        {},
        logger,
        messages.append,
        lambda *_: None,
    )
    waits: list[float] = []
    runner._sampling_window_wait = types.MethodType(
        lambda self, duration_s, stop_event=None: waits.append(float(duration_s)) or True,
        runner,
    )
    runner._wait_postseal_dewpoint_gate = types.MethodType(lambda self, point, phase="", context=None: True, runner)

    point = _point_h2o()
    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    assert waits == [10.0]
    assert any("waiting 10.0s before sampling" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    sampling_begin_rows = [row for row in trace_rows if row["trace_stage"] == "sampling_begin"]
    assert len(sampling_begin_rows) == 1
    assert sampling_begin_rows[0]["trigger_reason"] == "h2o_post_stable_delay_elapsed"
    assert "configured_delay_s=10.0" in sampling_begin_rows[0]["note"]


def test_build_point_summary_row_includes_pressure_timing_fields(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2()

    runner._set_point_runtime_fields(
        point,
        phase="co2",
        timing_stages={
            "preseal_vent_off_begin": 10.0,
            "route_sealed": 12.5,
            "control_prepare_begin": 13.1,
            "pressure_in_limits": 20.0,
            "sampling_begin": 30.4,
        },
    )

    row = runner._build_point_summary_row(
        point,
        [],
        phase="co2",
        point_tag=runner._co2_point_tag(point),
        integrity_summary={},
    )
    logger.close()

    assert row["preseal_vent_off_begin_to_route_sealed_ms"] == 2500.0
    assert row["route_sealed_to_control_prepare_begin_ms"] == 600.0
    assert row["pressure_in_limits_to_sampling_begin_ms"] == 10400.0


def test_wait_after_pressure_stable_co2_runs_postseal_dewpoint_gate(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_postseal_dewpoint_window_s": 2.0,
                    "co2_postseal_dewpoint_timeout_s": 1.0,
                    "co2_postseal_dewpoint_span_c": 0.05,
                    "co2_postseal_dewpoint_slope_c_per_s": 0.05,
                    "co2_postseal_dewpoint_min_samples": 3,
                }
            }
        },
        {"dewpoint": types.SimpleNamespace()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    now_mono = runner_module.time.monotonic()
    runner._pressure_transition_fast_signal_context = {
        "stop_event": None,
        "fast_signal_buffers": {
            "pace": [{"recv_mono_s": now_mono - 0.2, "values": {"pressure_hpa": 1100.0}}],
            "pressure_gauge": [{"recv_mono_s": now_mono - 0.2, "values": {"pressure_gauge_hpa": 1099.9}}],
            "dewpoint": [
                {"recv_mono_s": now_mono - 1.6, "values": {"dewpoint_live_c": -20.12, "dew_temp_live_c": 20.1, "dew_rh_live_pct": 40.0}},
                {"recv_mono_s": now_mono - 0.9, "values": {"dewpoint_live_c": -20.10, "dew_temp_live_c": 20.2, "dew_rh_live_pct": 40.1}},
                {"recv_mono_s": now_mono - 0.2, "values": {"dewpoint_live_c": -20.09, "dew_temp_live_c": 20.2, "dew_rh_live_pct": 40.2}},
            ],
        },
    }
    runner._refresh_pressure_transition_fast_signal_once = types.MethodType(lambda self, context, reason="": None, runner)

    point = _point_co2()
    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    trace_rows = _load_pressure_trace_rows(logger)
    pass_rows = [row for row in trace_rows if row["trace_stage"] == "dewpoint_gate_pass"]
    assert len(pass_rows) == 1
    pass_row = pass_rows[0]
    assert pass_row["dewpoint_live_c"] == "-20.09"
    assert pass_row["dewpoint_gate_count"] == "3"
    state = runner._point_runtime_state(point, phase="co2")
    assert state["dewpoint_gate_result"] == "stable"
    assert state["dewpoint_gate_count"] == 3
    assert state["dewpoint_gate_span_c"] is not None
    assert state["dewpoint_gate_slope_c_per_s"] is not None


def test_wait_after_pressure_stable_h2o_runs_postseal_dewpoint_gate_with_live_values(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "h2o_postseal_dewpoint_window_s": 2.0,
                    "h2o_postseal_dewpoint_timeout_s": 1.0,
                    "h2o_postseal_dewpoint_span_c": 0.02,
                    "h2o_postseal_dewpoint_slope_c_per_s": 0.02,
                    "h2o_postseal_dewpoint_min_samples": 2,
                }
            }
        },
        {"dewpoint": types.SimpleNamespace()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._preseal_dewpoint_snapshot = {
        "sample_ts": "2026-03-30T12:00:00.000",
        "dewpoint_c": -9.5,
        "temp_c": 19.0,
        "rh_pct": 88.0,
        "pressure_hpa": 1000.0,
    }
    now_mono = runner_module.time.monotonic()
    runner._pressure_transition_fast_signal_context = {
        "stop_event": None,
        "fast_signal_buffers": {
            "pace": [{"recv_mono_s": now_mono - 0.2, "values": {"pressure_hpa": 1000.0}}],
            "pressure_gauge": [{"recv_mono_s": now_mono - 0.2, "values": {"pressure_gauge_hpa": 999.8}}],
            "dewpoint": [
                {"recv_mono_s": now_mono - 1.6, "values": {"dewpoint_live_c": -8.72, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 33.0}},
                {"recv_mono_s": now_mono - 1.0, "values": {"dewpoint_live_c": -8.71, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 33.1}},
                {"recv_mono_s": now_mono - 0.6, "values": {"dewpoint_live_c": -8.70, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 33.1}},
                {"recv_mono_s": now_mono - 0.2, "values": {"dewpoint_live_c": -8.70, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 33.0}},
            ],
        },
    }
    runner._refresh_pressure_transition_fast_signal_once = types.MethodType(lambda self, context, reason="": None, runner)

    point = _point_h2o()
    assert runner._wait_after_pressure_stable_before_sampling(point) is True
    logger.close()

    trace_rows = _load_pressure_trace_rows(logger)
    pass_rows = [row for row in trace_rows if row["trace_stage"] == "dewpoint_gate_pass"]
    assert len(pass_rows) == 1
    pass_row = pass_rows[0]
    assert pass_row["dewpoint_c"] == "-9.5"
    assert pass_row["dewpoint_live_c"] == "-8.7"
    assert pass_row["dewpoint_gate_count"] == "3"
    state = runner._point_runtime_state(point, phase="h2o")
    assert state["dewpoint_gate_result"] == "stable"
    assert state["dewpoint_gate_count"] == 3


def test_collect_samples_records_device_timestamps_and_sampling_trace(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)

    class _FakeChamber:
        def read_temp_c(self):
            return 40.2

        def read_rh_pct(self):
            return 55.4

    class _FakeThermometer:
        def read_temp_c(self):
            return 39.8

    class _FakePace:
        def read_pressure(self):
            return 999.4

        def get_output_state(self):
            return 1

        def get_isolation_state(self):
            return 1

        def get_vent_status(self):
            return 0

    class _FakeGauge:
        def read_pressure(self):
            return 1000.1

    class _FakeDew:
        def get_current(self, timeout_s=None, attempts=None):
            return {"dewpoint_c": -11.2, "temp_c": 23.4, "rh_pct": 44.5}

    runner = CalibrationRunner(
        {},
        {
            "temp_chamber": _FakeChamber(),
            "thermometer": _FakeThermometer(),
            "pace": _FakePace(),
            "pressure_gauge": _FakeGauge(),
            "dewpoint": _FakeDew(),
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    rows = runner._collect_samples(_point_co2(), 1, 0.0, phase="co2", point_tag="demo")
    logger.close()

    assert rows is not None
    assert len(rows) == 1
    row = rows[0]
    assert row["sample_ts"]
    assert row["sample_due_ts"]
    assert row["sample_start_ts"]
    assert row["sample_end_ts"]
    assert row["sample_elapsed_ms"] >= 0.0
    assert row["sample_lag_ms"] >= 0.0
    assert row["fast_group_anchor_ts"] == row["sample_ts"]
    assert row["fast_group_span_ms"] >= 0.0
    assert row["chamber_sample_ts"]
    assert row["chamber_cache_age_ms"] >= 0.0
    assert row["thermometer_sample_ts"]
    assert row["thermometer_cache_age_ms"] >= 0.0
    assert row["pace_sample_ts"]
    assert row["pressure_gauge_sample_ts"]
    assert row["dewpoint_sample_ts"]
    assert row["dewpoint_live_sample_ts"]
    assert row["dewpoint_live_c"] == -11.2
    assert row["dew_temp_live_c"] == 23.4
    assert row["dew_rh_live_pct"] == 44.5
    assert row["pace_output_state"] == 1
    assert row["pace_isolation_state"] == 1
    assert row["pace_vent_status"] == 0
    trace_rows = _load_pressure_trace_rows(logger)
    sampling_rows = [one for one in trace_rows if one["trace_stage"] == "sampling_row"]
    assert len(sampling_rows) == 1
    assert sampling_rows[0]["point_tag"] == "demo"
    assert float(sampling_rows[0]["pace_pressure_hpa"]) == 999.4
    assert float(sampling_rows[0]["pressure_gauge_hpa"]) == 1000.1
    assert float(sampling_rows[0]["dewpoint_live_c"]) == -11.2
    assert float(sampling_rows[0]["fast_group_span_ms"]) >= 0.0


def test_ambient_points_export_explicit_pressure_mode_and_label(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"count": 1, "interval_s": 0.0}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    source_point = _point_co2()
    ambient_ref = runner._ambient_pressure_reference_point(source_point)
    ambient_point = runner._build_co2_pressure_point(source_point, ambient_ref)
    point_tag = runner._co2_point_tag(ambient_point)

    rows = runner._collect_samples(ambient_point, 1, 0.0, phase="co2", point_tag=point_tag)
    assert rows is not None
    assert len(rows) == 1
    assert rows[0]["pressure_mode"] == "ambient_open"
    assert rows[0]["pressure_target_label"] == "当前大气压"

    summary_row = runner._build_point_summary_row(
        ambient_point,
        rows,
        phase="co2",
        point_tag=point_tag,
        integrity_summary={},
    )
    logger.close()

    assert summary_row["pressure_mode"] == "ambient_open"
    assert summary_row["pressure_target_label"] == "当前大气压"
    assert summary_row["pressure_target_hpa"] is None
