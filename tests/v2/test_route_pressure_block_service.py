from __future__ import annotations

from types import SimpleNamespace

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.event_bus import EventBus, EventType
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.services.route_pressure_block_service import (
    PressureBlockResult,
    RoutePressureBlockService,
)


class _RecordingRouteContext(RouteContext):
    def __init__(self) -> None:
        super().__init__()
        self.updates: list[dict[str, object]] = []

    def update(self, **kwargs) -> None:
        super().update(**kwargs)
        self.updates.append({
            "current_point_index": None if self.current_point is None else self.current_point.index,
            "point_tag": self.point_tag,
            "retry": self.retry,
        })


class _TraceStatusService:
    def __init__(self, calls: list[str], payloads: list[dict[str, object]]) -> None:
        self._calls = calls
        self._payloads = payloads

    def check_stop(self) -> None:
        self._calls.append("check_stop")

    def update_status(self, **kwargs) -> None:
        self._calls.append(f"update:{kwargs['phase'].value}")

    def begin_point_timing(self, point, *, phase="", point_tag="") -> None:
        self._calls.append(f"begin:{point_tag}")

    def clear_point_timing(self, point, *, phase="", point_tag="") -> None:
        self._calls.append(f"clear:{point_tag}")

    def mark_point_stable_for_sampling(self, point, *, phase="", point_tag="") -> None:
        self._calls.append(f"stable:{point_tag}")

    def log(self, message: str) -> None:
        self._calls.append(f"log:{message}")

    def record_route_trace(self, **kwargs) -> None:
        self._payloads.append(dict(kwargs))
        self._calls.append(f"trace:{kwargs.get('action')}:{kwargs.get('result', 'ok')}")


def _make_a2_hooks() -> SimpleNamespace:
    return SimpleNamespace(
        callbacks={},
        co2_route_conditioning_at_atmosphere_active=False,
        high_pressure_first_point_mode_enabled=False,
    )


def _make_service(overrides=None):
    overrides = dict(overrides or {})
    calls: list[str] = []
    payloads: list[dict[str, object]] = []
    event_bus = EventBus()
    context = _RecordingRouteContext()
    a2_hooks = _make_a2_hooks()

    service = SimpleNamespace(
        event_bus=event_bus,
        route_context=context,
        a2_hooks=a2_hooks,
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=_TraceStatusService(calls, payloads),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": calls.append("pressurize_and_hold") or SimpleNamespace(ok=True),
            set_pressure_to_target=lambda point: calls.append(f"set_pressure_to_target:{point.index}") or SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: calls.append(f"wait_after_stable:{point.index}") or SimpleNamespace(ok=True),
            set_pressure_controller_vent=lambda on, reason="", **kw: calls.append(f"vent:{on}:{reason}"),
            _current_pressure=lambda: overrides.get("_current_pressure_hpa", 1013.25),
            _coerce_float=lambda v: float(v) if v is not None else None,
        ),
        sampling_service=SimpleNamespace(
            sampling_params=lambda phase="": (4, 15),
            sample_point=lambda point, phase="", point_tag="": calls.append(f"sample:{point_tag}") or [SimpleNamespace(point=point, point_tag=point_tag)],
        ),
        qc_service=SimpleNamespace(
            run_point_qc=lambda point, phase="", point_tag="": calls.append(f"qc:{point_tag}"),
        ),
        valve_routing_service=SimpleNamespace(
            apply_valve_states=lambda open_valves: calls.append(f"apply_valves:{open_valves}") or {"relay_a": {}, "relay_b": {}},
        ),
        _cfg_get=lambda path, default=None: overrides.get(path, default),
        _record_workflow_timing=lambda *a, **kw: None,
    )
    return service, calls, payloads, context


def test_split_pressure_blocks_empty() -> None:
    service, *_ = _make_service()
    blocks = RoutePressureBlockService(service)
    ambient, sealed = blocks.split_pressure_blocks([])
    assert ambient == []
    assert sealed == []


def test_split_pressure_blocks_only_ambient() -> None:
    service, *_ = _make_service()
    blocks = RoutePressureBlockService(service)
    refs = [
        CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, pressure_mode="ambient_open", route="co2"),
    ]
    ambient, sealed = blocks.split_pressure_blocks(refs)
    assert len(ambient) == 1
    assert ambient[0].index == 1
    assert sealed == []


def test_split_pressure_blocks_only_sealed() -> None:
    service, *_ = _make_service()
    blocks = RoutePressureBlockService(service)
    refs = [
        CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=1100.0, pressure_mode="sealed_controlled", route="co2"),
        CalibrationPoint(index=2, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=1000.0, pressure_mode="sealed_controlled", route="co2"),
        CalibrationPoint(index=3, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2"),
    ]
    ambient, sealed = blocks.split_pressure_blocks(refs)
    assert ambient == []
    assert len(sealed) == 3
    assert sealed[0].target_pressure_hpa == 1100.0
    assert sealed[1].target_pressure_hpa == 1000.0
    assert sealed[2].target_pressure_hpa == 800.0


def test_split_pressure_blocks_mixed_ambient_plus_sealed() -> None:
    service, *_ = _make_service()
    blocks = RoutePressureBlockService(service)
    refs = [
        CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, pressure_mode="ambient_open", route="co2"),
        CalibrationPoint(index=2, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2"),
    ]
    ambient, sealed = blocks.split_pressure_blocks(refs)
    assert len(ambient) == 1
    assert ambient[0].index == 1
    assert len(sealed) == 1
    assert sealed[0].index == 2
    assert sealed[0].target_pressure_hpa == 800.0


def test_split_pressure_blocks_dedup_duplicate_pressures() -> None:
    service, *_ = _make_service()
    blocks = RoutePressureBlockService(service)
    refs = [
        CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2"),
        CalibrationPoint(index=2, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2"),
    ]
    ambient, sealed = blocks.split_pressure_blocks(refs)
    assert ambient == []
    assert len(sealed) == 1
    assert sealed[0].index == 1


def test_run_co2_ambient_block_vent_on_no_pressure_control() -> None:
    service, calls, payloads, context = _make_service()
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, route="co2")
    ambient = CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, pressure_mode="ambient_open", route="co2")

    result = blocks.run_co2_ambient_block(source, [ambient])

    assert result.completed_point_indices == [10]
    assert result.sampled_point_indices == [10]
    assert result.skipped_point_indices == []

    vent_calls = [c for c in calls if c.startswith("vent:")]
    assert len(vent_calls) >= 1
    assert vent_calls[0].startswith("vent:True:")

    assert "pressurize_and_hold" not in calls
    assert not any(c.startswith("set_pressure_to_target:") for c in calls)

    skip_traces = [p for p in payloads if p.get("action") == "pressure_skip" and p.get("result") == "skipped"]
    assert len(skip_traces) >= 1
    assert skip_traces[0]["target"]["vent_on"] is True

    assert any(c.startswith("sample:co2_") for c in calls)
    assert any(c.startswith("qc:co2_") for c in calls)


def test_run_co2_sealed_block_only_sealed_pressurize_and_hold_once() -> None:
    service, calls, payloads, context = _make_service()
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=1100.0, route="co2")
    sealed = [
        CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=1100.0, pressure_mode="sealed_controlled", route="co2"),
        CalibrationPoint(index=12, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=1000.0, pressure_mode="sealed_controlled", route="co2"),
        CalibrationPoint(index=13, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2"),
    ]

    result = blocks.run_co2_sealed_block(source, sealed, already_sealed=False)

    assert result.completed_point_indices == [11, 12, 13]
    assert result.sampled_point_indices == [11, 12, 13]
    assert result.skipped_point_indices == []

    pressurize_calls = [c for c in calls if c == "pressurize_and_hold"]
    assert len(pressurize_calls) == 1

    set_pressure_calls = [c for c in calls if c.startswith("set_pressure_to_target:")]
    assert len(set_pressure_calls) == 3

    vent_on_calls = [c for c in calls if c.startswith("vent:True:")]
    assert len(vent_on_calls) == 0

    sample_calls = [c for c in calls if c.startswith("sample:")]
    assert len(sample_calls) == 3

    qc_calls = [c for c in calls if c.startswith("qc:")]
    assert len(qc_calls) == 3


def test_run_co2_sealed_block_already_sealed_skips_pressurize_and_hold() -> None:
    service, calls, payloads, context = _make_service()
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, route="co2")
    sealed = [
        CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2"),
    ]

    result = blocks.run_co2_sealed_block(source, sealed, already_sealed=True)

    assert result.completed_point_indices == [11]

    assert "pressurize_and_hold" not in calls

    assert any(c.startswith("set_pressure_to_target:11") for c in calls)
    assert any(c.startswith("sample:co2_") for c in calls)


def test_mixed_ambient_plus_sealed_vent_off_transition() -> None:
    service, calls, payloads, context = _make_service()
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, route="co2")
    ambient = CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, pressure_mode="ambient_open", route="co2")
    sealed = [
        CalibrationPoint(index=12, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2"),
    ]

    ambient_result = blocks.run_co2_ambient_block(source, [ambient])
    assert ambient_result.completed_point_indices == [10]

    first_sealed_sample = service.route_planner.build_co2_pressure_point(source, sealed[0])
    transition = blocks.transition_co2_ambient_to_sealed(source, first_sealed_sample)
    assert transition.ok

    sealed_result = blocks.run_co2_sealed_block(source, sealed, already_sealed=True)
    assert sealed_result.completed_point_indices == [12]

    vent_off_calls = [c for c in calls if c.startswith("vent:False:")]
    assert len(vent_off_calls) >= 1

    apply_valves_calls = [c for c in calls if c.startswith("apply_valves:")]
    assert len(apply_valves_calls) >= 1

    transition_traces = [p for p in payloads if p.get("action") == "co2_ambient_to_sealed_transition"]
    assert len(transition_traces) == 1
    assert transition_traces[0]["actual"]["sealed_no_vent_guard_active_before_set_pressure"] is True
    assert transition_traces[0]["actual"]["target_pressure_hpa"] == 800.0

    assert service.a2_hooks.co2_sealed_route_no_vent_active is True


def test_mixed_ambient_plus_800_700_two_sealed_no_vent_between() -> None:
    service, calls, payloads, context = _make_service()
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, route="co2")
    ambient = CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, pressure_mode="ambient_open", route="co2")
    sealed = [
        CalibrationPoint(index=12, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2"),
        CalibrationPoint(index=13, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=700.0, pressure_mode="sealed_controlled", route="co2"),
    ]

    ambient_result = blocks.run_co2_ambient_block(source, [ambient])
    assert ambient_result.completed_point_indices == [10]

    first_sealed_sample = service.route_planner.build_co2_pressure_point(source, sealed[0])
    transition = blocks.transition_co2_ambient_to_sealed(source, first_sealed_sample)
    assert transition.ok

    sealed_result = blocks.run_co2_sealed_block(source, sealed, already_sealed=True)
    assert sealed_result.completed_point_indices == [12, 13]

    pressurize_calls = [c for c in calls if c == "pressurize_and_hold"]
    assert len(pressurize_calls) == 0

    set_pressure_calls = [c for c in calls if c.startswith("set_pressure_to_target:")]
    assert len(set_pressure_calls) == 2

    vent_on_after_close = [
        c for c in calls
        if c.startswith("vent:True:") and "apply_valves" in "".join(calls[:calls.index(c)])
    ]
    assert len(vent_on_after_close) == 0

    apply_valves_calls = [c for c in calls if c.startswith("apply_valves:")]
    assert len(apply_valves_calls) == 1

    transition_traces = [p for p in payloads if p.get("action") == "co2_ambient_to_sealed_transition"]
    assert len(transition_traces) == 1


def test_no_vent_guard_blocked_vent_during_sealed_block() -> None:
    service, calls, payloads, context = _make_service()
    vent_call_log: list[dict] = []

    def guarded_set_pressure_controller_vent(on, reason="", **kw):
        guard_active = getattr(service.a2_hooks, "co2_sealed_route_no_vent_active", False)
        entry = {
            "vent_on": on,
            "reason": reason,
            "guard_active": guard_active,
            "command_sent": False,
            "blocked": False,
        }
        if on and guard_active:
            entry["blocked"] = True
            vent_call_log.append(entry)
            payloads.append({
                "action": "sealed_route_vent_on_blocked",
                "target": {"vent_on": True},
                "actual": {
                    "vent_command_blocked": True,
                    "blocked_by": "co2_sealed_route_no_vent_guard",
                },
                "result": "blocked",
            })
            return {"vent_command_blocked": True}
        entry["command_sent"] = True
        vent_call_log.append(entry)
        return {}

    def guarded_set_pressure_to_target(point):
        calls.append(f"set_pressure_to_target:{point.index}")
        guarded_set_pressure_controller_vent(
            True, reason="redundant vent open during setpoint control"
        )
        return SimpleNamespace(ok=True)

    service.pressure_control_service.set_pressure_controller_vent = guarded_set_pressure_controller_vent
    service.pressure_control_service.set_pressure_to_target = guarded_set_pressure_to_target

    def activating_apply_valve_states(open_valves):
        return {"relay_a": {}, "relay_b": {}}

    service.valve_routing_service.apply_valve_states = activating_apply_valve_states
    service._cfg_get = lambda path, default=None: (
        0.0 if path == "workflow.pressure.co2_ambient_to_sealed_vent_off_settle_s" else default
    )

    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, route="co2")
    ambient = CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, pressure_mode="ambient_open", route="co2")
    sealed = [
        CalibrationPoint(index=12, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2"),
    ]

    blocks.run_co2_ambient_block(source, [ambient])

    first_sealed_sample = service.route_planner.build_co2_pressure_point(source, sealed[0])
    blocks.transition_co2_ambient_to_sealed(source, first_sealed_sample)

    blocks.run_co2_sealed_block(source, sealed, already_sealed=True)

    blocked_traces = [p for p in payloads if p.get("action") == "sealed_route_vent_on_blocked"]
    assert len(blocked_traces) >= 1
    assert blocked_traces[0]["result"] == "blocked"


def test_pressurize_and_hold_failure_returns_all_skipped() -> None:
    service, calls, payloads, context = _make_service()
    service.pressure_control_service.pressurize_and_hold = (
        lambda point, route="co2": calls.append("pressurize_and_hold_fail") or SimpleNamespace(ok=False)
    )
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=1100.0, route="co2")
    sealed = [
        CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=1100.0, pressure_mode="sealed_controlled", route="co2"),
    ]

    result = blocks.run_co2_sealed_block(source, sealed, already_sealed=False)

    assert result.completed_points == []
    assert result.completed_point_indices == []
    assert result.skipped_point_indices == [11]
    assert "pressurize_and_hold_fail" in calls


# ── A: ambient heartbeat ──

def test_ambient_block_vent_tick_count_gt_one():
    service, calls, payloads, context = _make_service({"_current_pressure_hpa": 1013.25})
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, route="co2")
    ambient = CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
                               pressure_mode="ambient_open", route="co2")

    result = blocks.run_co2_ambient_block(source, [ambient])

    assert result.skipped_point_indices == []
    vent_calls = [c for c in calls if c.startswith("vent:True:")]
    assert len(vent_calls) >= 2, f"vent tick count should be > 1, got {len(vent_calls)}"

    gate_traces = [p for p in payloads if p.get("action") == "ambient_atmosphere_gate"]
    assert len(gate_traces) >= 1
    assert gate_traces[0]["result"] == "PASS"
    actual = gate_traces[0]["actual"]
    assert actual["vent_tick_count"] >= 1
    assert actual["near_atmosphere"] is True


# ── B: ambient sampling heartbeat ──

def test_ambient_sampling_continues_vent_heartbeat():
    service, calls, payloads, context = _make_service({"_current_pressure_hpa": 1013.25})
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, route="co2")
    ambient = CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
                               pressure_mode="ambient_open", route="co2")

    result = blocks.run_co2_ambient_block(source, [ambient])
    assert result.skipped_point_indices == []

    sample_start_idx = next((i for i, c in enumerate(calls) if c.startswith("trace:sample_start")), -1)
    sample_end_idx = next((i for i, c in enumerate(calls) if c.startswith("trace:sample_end")), -1)
    assert sample_start_idx >= 0
    assert sample_end_idx > sample_start_idx

    sample_vent_calls = [
        c for c in calls[sample_start_idx:sample_end_idx]
        if c.startswith("vent:True:CO2 ambient sampling")
    ]
    assert len(sample_vent_calls) >= 1, "vent must be called during sampling phase"


# ── C: ambient pressure gate fail ──

def test_ambient_pressure_gate_fails_above_atmosphere():
    service, calls, payloads, context = _make_service({
        "_current_pressure_hpa": 1418.0,
        "workflow.pressure.ambient_atmosphere_wait_s": 0.1,
        "workflow.pressure.atmosphere_vent_heartbeat_interval_s": 0.05,
    })
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, route="co2")
    ambient = CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
                               pressure_mode="ambient_open", route="co2")

    result = blocks.run_co2_ambient_block(source, [ambient])

    assert result.completed_point_indices == []
    assert result.skipped_point_indices == [10]

    gate_traces = [p for p in payloads if p.get("action") == "ambient_atmosphere_gate"]
    assert len(gate_traces) >= 1
    assert gate_traces[0]["result"] == "FAIL"

    sample_calls = [c for c in calls if c.startswith("sample:")]
    assert len(sample_calls) == 0, "must not call sample_point when atmosphere gate fails"


# ── D: transition order ──

def test_transition_order_vent_off_wait_close_then_sealed():
    service, calls, payloads, context = _make_service({"_current_pressure_hpa": 1013.25})
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, route="co2")
    ambient = CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
                               pressure_mode="ambient_open", route="co2")
    sealed_point = CalibrationPoint(index=12, temperature_c=20.0, co2_ppm=1000.0,
                                     pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2")

    ambient_result = blocks.run_co2_ambient_block(source, [ambient])
    assert ambient_result.skipped_point_indices == []

    trans_result = blocks.transition_co2_ambient_to_sealed(source, sealed_point)
    assert trans_result.ok is True

    sample_end_idx = next((i for i, c in enumerate(calls) if c.startswith("trace:sample_end:")), -1)
    assert sample_end_idx >= 0

    vent_off_idx = next((i for i, c in enumerate(calls) if c.startswith("vent:False:CO2 ambient")), -1)
    assert vent_off_idx > sample_end_idx

    apply_valves_idx = next((i for i, c in enumerate(calls) if c.startswith("apply_valves")), -1)
    assert apply_valves_idx > vent_off_idx, f"no apply_valves after vent_off; calls near vent_off: {calls[max(0, vent_off_idx-2):vent_off_idx+10]}"

    vent_off_call = calls[vent_off_idx]
    assert "vent:False:" in vent_off_call

    # Guard arm verified via trace
    transition_traces = [p for p in payloads if p.get("action") == "co2_ambient_to_sealed_transition"]
    assert len(transition_traces) == 1
    assert transition_traces[0]["result"] == "ok"
    actual = transition_traces[0]["actual"]
    assert actual["sealed_no_vent_guard_active_before_set_pressure"] is True


# ── E: sealed-only regression ──

def test_sealed_only_no_vent_during_pressure_points():
    service, calls, payloads, context = _make_service()
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=1100.0, route="co2")
    sealed = [
        CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=1100.0,
                         pressure_mode="sealed_controlled", route="co2"),
        CalibrationPoint(index=12, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=1000.0,
                         pressure_mode="sealed_controlled", route="co2"),
    ]

    result = blocks.run_co2_sealed_block(source, sealed, already_sealed=False)

    assert result.skipped_point_indices == []
    assert len([c for c in calls if c == "pressurize_and_hold"]) == 1
    set_pressure_calls = [c for c in calls if c.startswith("set_pressure_to_target:")]
    assert len(set_pressure_calls) == 2

    sample_range_start = next((i for i, c in enumerate(calls) if c.startswith("trace:sample_start")), 0)
    sample_range_end = next((i for i, c in enumerate(calls) if c.startswith("trace:sample_end")
                              and i > sample_range_start), len(calls))
    vent_on_during_sealed = [
        c for c in calls[sample_range_start:sample_range_end]
        if c.startswith("vent:True:")
    ]
    assert len(vent_on_during_sealed) == 0, "no vent=ON during sealed sampling"


# ── F: cleanup guard lifecycle ──

def test_cleanup_guard_disarmed_before_cleanup():
    service, calls, payloads, context = _make_service({"_current_pressure_hpa": 1013.25})
    blocks = RoutePressureBlockService(service)

    source = CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None, route="co2")
    ambient = CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
                               pressure_mode="ambient_open", route="co2")
    sealed = CalibrationPoint(index=12, temperature_c=20.0, co2_ppm=1000.0,
                               pressure_hpa=800.0, pressure_mode="sealed_controlled", route="co2")

    ambient_result = blocks.run_co2_ambient_block(source, [ambient])
    assert ambient_result.skipped_point_indices == []

    ambient_end_idx = len(calls)
    sealed_result = blocks.run_co2_sealed_block(source, [sealed], already_sealed=False)
    assert sealed_result.skipped_point_indices == []

    sealed_calls = calls[ambient_end_idx:]
    sealed_vent_on = [c for c in sealed_calls if c.startswith("vent:True:")]
    assert len(sealed_vent_on) == 0, "no vent=ON during sealed block"
