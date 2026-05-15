from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, List

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _FakePace:
    VENT_STATUS_TRAPPED_PRESSURE = 3

    def __init__(self) -> None:
        self.calls: List[tuple[Any, ...]] = []
        self.output_state = 1
        self.isolation_state = 1
        self.vent_status = 0
        self.hold_active = False
        self.in_limits = True

    def set_setpoint(self, value: float) -> None:
        self.calls.append(("setpoint", float(value)))

    def get_in_limits(self):
        target = next((call[1] for call in reversed(self.calls) if call[0] == "setpoint"), 1000.0)
        return float(target), 1 if self.in_limits else 0

    def set_output(self, on: bool) -> None:
        self.calls.append(("output", bool(on)))
        self.output_state = 1 if on else 0

    def enable_control_output(self) -> None:
        self.calls.append(("output_on",))
        self.output_state = 1

    def vent(self, on: bool = True) -> None:
        self.calls.append(("vent", bool(on)))
        self.vent_status = 1 if on else 0

    def enter_atmosphere_mode(self, *args, **kwargs) -> None:
        self.calls.append(("vent_on",))
        self.output_state = 0

    def exit_atmosphere_mode(self, *args, **kwargs) -> None:
        self.calls.append(("vent_off",))
        self.output_state = 0
        self.vent_status = 0

    def set_isolation_open(self, is_open: bool) -> None:
        self.calls.append(("isol", bool(is_open)))
        self.isolation_state = 1 if is_open else 0

    def get_output_state(self):
        return self.output_state

    def get_isolation_state(self):
        return self.isolation_state

    def get_vent_status(self):
        return self.vent_status

    def is_atmosphere_hold_active(self):
        return self.hold_active

    def vent_status_allows_control(self, status: Any) -> bool:
        return int(status) in {0, 2, 3}


def _cfg() -> dict:
    return {
        "workflow": {
            "pressure": {
                "transition_trace_enabled": True,
                "transition_trace_poll_s": 0.001,
                "stabilize_timeout_s": 0.05,
                "co2_reseal_retry_count": 1,
            },
        },
        "valves": {
            "h2o_path": 10,
            "gas_main": 11,
            "co2_path": 12,
            "co2_path_group2": 13,
            "co2_map": {"1000": 6, "500": 5},
            "co2_map_group2": {"1000": 16},
        },
    }


def _runner(tmp_path: Path, pace: _FakePace | None = None) -> CalibrationRunner:
    logger = RunLogger(tmp_path)
    return CalibrationRunner(_cfg(), {"pace": pace or _FakePace()}, logger, lambda *_: None, lambda *_: None)


def _co2_point(index: int, pressure_hpa: float, *, ppm: float = 1000.0, group: str | None = None) -> CalibrationPoint:
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
        co2_group=group,
    )


def _h2o_point(index: int, pressure_hpa: float) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=pressure_hpa,
        dewpoint_c=None,
        h2o_mmol=10.0,
        raw_h2o="10",
        co2_group=None,
    )


def _trace_rows(runner: CalibrationRunner) -> list[dict[str, str]]:
    run_dir = getattr(runner.logger, "run_dir", None)
    if run_dir is None:
        return []
    path = run_dir / "pressure_transition_trace.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _install_co2_loop_stubs(runner: CalibrationRunner):
    full_calls: list[float] = []
    seal_calls: list[float] = []
    samples: list[float] = []

    runner._apply_idle_route_isolation = lambda *args, **kwargs: None
    runner._set_temperature_for_point = lambda *args, **kwargs: True
    runner._capture_temperature_calibration_snapshot = lambda *args, **kwargs: None
    runner._open_co2_route_for_conditioning = lambda *args, **kwargs: None
    runner._wait_co2_route_soak_before_seal = lambda *args, **kwargs: True
    runner._gas_route_dewpoint_gate_enabled = lambda: False
    runner._wait_co2_preseal_primary_sensor_gate = lambda *args, **kwargs: True
    runner._wait_cold_co2_quality_gate = lambda *args, **kwargs: True
    runner._set_co2_route_baseline = lambda *args, **kwargs: None
    runner._pressurize_route_for_sealed_points = (
        lambda point, **kwargs: seal_calls.append(float(point.target_pressure_hpa)) or True
    )

    def full_pressure(point: CalibrationPoint, *args, **kwargs) -> bool:
        full_calls.append(float(point.target_pressure_hpa))
        return True

    runner._set_pressure_to_target = full_pressure
    runner._wait_after_pressure_stable_before_sampling = lambda *args, **kwargs: True
    runner._sample_and_log = lambda point, **kwargs: samples.append(float(point.target_pressure_hpa))
    return full_calls, seal_calls, samples


def _run_co2_sweep(tmp_path: Path, pressures: list[float]):
    pace = _FakePace()
    runner = _runner(tmp_path, pace)
    full_calls, seal_calls, samples = _install_co2_loop_stubs(runner)
    source = _co2_point(1, pressures[0])
    pressure_refs = [_co2_point(idx + 10, pressure) for idx, pressure in enumerate(pressures)]

    runner._run_co2_point(source, pressure_points=pressure_refs)

    return runner, pace, full_calls, seal_calls, samples


def test_single_selected_pressure_point_1100_uses_full_sequence_no_reuse(tmp_path: Path) -> None:
    runner, pace, full_calls, seal_calls, samples = _run_co2_sweep(tmp_path, [1100.0])

    assert seal_calls == [1100.0]
    assert full_calls == [1100.0]
    assert samples == [1100.0]
    assert [call for call in pace.calls if call[0] == "setpoint"] == []
    assert runner._active_co2_sealed_sweep_context is None
    assert "sealed_sweep_setpoint_update" not in [row["trace_stage"] for row in _trace_rows(runner)]


def test_single_selected_pressure_point_500_uses_full_sequence_no_reuse(tmp_path: Path) -> None:
    runner, pace, full_calls, seal_calls, samples = _run_co2_sweep(tmp_path, [500.0])

    assert seal_calls == [500.0]
    assert full_calls == [500.0]
    assert samples == [500.0]
    assert [call for call in pace.calls if call[0] == "setpoint"] == []
    assert runner._active_co2_sealed_sweep_context is None


def test_subset_1100_800_500_first_full_then_setpoint_only(tmp_path: Path) -> None:
    runner, pace, full_calls, seal_calls, samples = _run_co2_sweep(tmp_path, [1100.0, 800.0, 500.0])

    assert seal_calls == [1100.0]
    assert full_calls == [1100.0]
    assert samples == [1100.0, 800.0, 500.0]
    assert [call for call in pace.calls if call[0] == "setpoint"] == [
        ("setpoint", 800.0),
        ("setpoint", 500.0),
    ]
    assert not [call for call in pace.calls if call[0] in {"output", "output_on", "vent", "vent_off"}]
    assert runner._active_co2_sealed_sweep_context is None


def test_subset_900_700_first_full_then_setpoint_only(tmp_path: Path) -> None:
    runner, pace, full_calls, seal_calls, samples = _run_co2_sweep(tmp_path, [900.0, 700.0])

    assert seal_calls == [900.0]
    assert full_calls == [900.0]
    assert samples == [900.0, 700.0]
    assert [call for call in pace.calls if call[0] == "setpoint"] == [("setpoint", 700.0)]
    assert not [call for call in pace.calls if call[0] in {"output", "output_on", "vent", "vent_off"}]


def test_full_seven_pressure_sweep_only_first_cycles_output(tmp_path: Path) -> None:
    pressures = [1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0]
    runner, pace, full_calls, seal_calls, samples = _run_co2_sweep(tmp_path, pressures)

    assert seal_calls == [1100.0]
    assert full_calls == [1100.0]
    assert samples == pressures
    assert [call for call in pace.calls if call[0] == "setpoint"] == [
        ("setpoint", 1000.0),
        ("setpoint", 900.0),
        ("setpoint", 800.0),
        ("setpoint", 700.0),
        ("setpoint", 600.0),
        ("setpoint", 500.0),
    ]
    assert not [call for call in pace.calls if call[0] in {"output", "output_on", "vent", "vent_off"}]
    stages = [row["trace_stage"] for row in _trace_rows(runner)]
    assert stages.count("sealed_sweep_setpoint_update") == 6


def test_second_pressure_point_setpoint_only_does_not_call_vent_or_output_cycle(tmp_path: Path) -> None:
    pace = _FakePace()
    runner = _runner(tmp_path, pace)
    first = _co2_point(1, 1100.0)
    second = _co2_point(2, 800.0)
    runner._begin_active_co2_sealed_sweep_context(first)
    runner._set_pressure_controller_vent = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("vent called"))
    runner._enable_pressure_controller_output = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("output enable called")
    )

    assert runner._set_pressure_to_target_in_active_co2_sealed_sweep(second) is True

    assert pace.calls == [("setpoint", 800.0)]


def test_live_check_fail_does_not_attempt_output_recovery(tmp_path: Path) -> None:
    pace = _FakePace()
    pace.output_state = 0
    runner = _runner(tmp_path, pace)
    first = _co2_point(1, 1100.0)
    second = _co2_point(2, 900.0)
    runner._begin_active_co2_sealed_sweep_context(first)

    assert runner._set_pressure_to_target_in_active_co2_sealed_sweep(second) is False

    assert pace.calls == []
    assert any(row["trace_stage"] == "sealed_sweep_live_check_fail" for row in _trace_rows(runner))


def test_key_mismatch_disables_reuse(tmp_path: Path) -> None:
    pace = _FakePace()
    runner = _runner(tmp_path, pace)
    runner._begin_active_co2_sealed_sweep_context(_co2_point(1, 1100.0, ppm=1000.0, group=None))

    assert runner._set_pressure_to_target_in_active_co2_sealed_sweep(
        _co2_point(2, 900.0, ppm=1000.0, group="B")
    ) is False

    assert pace.calls == []
    assert runner._active_co2_sealed_sweep_context is None
    assert any(row["trace_stage"] == "sealed_sweep_key_mismatch" for row in _trace_rows(runner))


def test_cleanup_resets_sealed_sweep_context(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    runner._set_co2_route_baseline = lambda *args, **kwargs: None
    runner._begin_active_co2_sealed_sweep_context(_co2_point(1, 1100.0))

    runner._cleanup_co2_route(reason="test cleanup")

    assert runner._active_co2_sealed_sweep_context is None
    assert any(row["trace_stage"] == "sealed_sweep_context_end" for row in _trace_rows(runner))


def test_h2o_path_not_affected(tmp_path: Path) -> None:
    pace = _FakePace()
    pace.output_state = 0
    runner = _runner(tmp_path, pace)
    point = _h2o_point(1, 1000.0)

    assert runner._set_pressure_to_target(point) is True

    assert pace.calls == [
        ("vent_off",),
        ("setpoint", 1000.0),
        ("output_on",),
    ]


def test_safe_stop_or_cleanup_vent_not_counted_as_sealed_violation(tmp_path: Path) -> None:
    pace = _FakePace()
    runner = _runner(tmp_path, pace)
    runner._begin_active_co2_sealed_sweep_context(_co2_point(1, 1100.0))

    runner._cleanup_co2_route(reason="test cleanup may vent")

    assert runner._active_co2_sealed_sweep_context is None
    assert any(call[0] in {"vent_on", "vent"} for call in pace.calls)
    assert any(row["trace_stage"] == "sealed_sweep_context_end" for row in _trace_rows(runner))
