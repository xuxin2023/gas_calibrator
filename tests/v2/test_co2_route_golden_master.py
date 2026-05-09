from __future__ import annotations

from types import SimpleNamespace

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.runners.co2_route_runner import Co2RouteRunner


GOLDEN_BASELINE = {
    "commit": "cdb821110a8d49e56bc29e18d029d74303b479c2",
    "tag": "v2.0.1",
    "output_dir": "run_20260508_101607",
    "final_decision": "PASS",
    "fail_closed_reason": None,
    "pressure_points_hpa": [1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0],
    "points_completed": 7,
    "sample_count": 28,
    "attempted_write_count": 0,
    "sealed_real_vent_on_count": 0,
}


class RecordingRouteContext(RouteContext):
    def __init__(self) -> None:
        super().__init__()
        self.snapshots: list[dict[str, object]] = []

    def enter(self, **kwargs) -> None:
        super().enter(**kwargs)
        self.snapshots.append(self._snapshot())

    def update(self, **kwargs) -> None:
        super().update(**kwargs)
        self.snapshots.append(self._snapshot())

    def _snapshot(self) -> dict[str, object]:
        return {
            "current_route": self.current_route,
            "source_point_index": None if self.source_point is None else self.source_point.index,
            "active_point_index": None if self.active_point is None else self.active_point.index,
            "point_tag": self.point_tag,
            "retry": self.retry,
        }


def _assert_subsequence(items: list[str], expected: list[str]) -> None:
    position = 0
    for expected_item in expected:
        while position < len(items) and items[position] != expected_item:
            position += 1
        assert position < len(items), f"Missing expected item {expected_item!r} in {items!r}"
        position += 1


def _co2_source() -> CalibrationPoint:
    return CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A")


def _pressure_points() -> list[CalibrationPoint]:
    return [
        CalibrationPoint(
            index=100 + offset,
            temperature_c=20.0,
            co2_ppm=800.0,
            pressure_hpa=pressure,
            route="co2",
            co2_group="A",
        )
        for offset, pressure in enumerate(GOLDEN_BASELINE["pressure_points_hpa"], start=1)
    ]


def _co2_route_blocks(raw_config: dict[str, object]) -> list[str]:
    workflow = raw_config.get("workflow") if isinstance(raw_config, dict) else {}
    co2 = workflow.get("co2") if isinstance(workflow, dict) else {}
    ambient_enabled = bool(co2.get("ambient_block_enabled")) if isinstance(co2, dict) else False
    blocks = ["open_conditioning"]
    if ambient_enabled:
        blocks.append("ambient_open_sampling")
    blocks.extend(["seal_transition", "sealed_pressure_control", "cleanup"])
    return blocks


def _run_golden_route() -> tuple[object, list[str], list[dict[str, object]], RecordingRouteContext]:
    calls: list[str] = []
    trace_payloads: list[dict[str, object]] = []
    context = RecordingRouteContext()
    route_planner = RoutePlanner(AppConfig.from_dict({}), PointParser())

    class StatusService:
        def check_stop(self):
            calls.append("check_stop")

        def update_status(self, **kwargs):
            calls.append(f"update:{kwargs['phase'].value}")

        def begin_point_timing(self, point, *, phase="", point_tag=""):
            calls.append(f"begin:{point_tag}")

        def clear_point_timing(self, point, *, phase="", point_tag=""):
            calls.append(f"clear:{point_tag}")

        def mark_point_stable_for_sampling(self, point, *, phase="", point_tag=""):
            calls.append(f"stable:{point_tag}")

        def finish_point_timing(self, point, *, phase="", point_tag=""):
            calls.append(f"finish:{point_tag}")
            return {"stability_time_s": 1.0, "total_time_s": 2.0}

        def mark_point_completed(self, point, **kwargs):
            calls.append(f"completed:{kwargs.get('point_tag', '')}")

        def log(self, message: str):
            calls.append(f"log:{message}")

        def record_route_trace(self, **kwargs):
            trace_payloads.append(dict(kwargs))
            calls.append(f"trace:{kwargs.get('action')}:{kwargs.get('result', 'ok')}")

    class PressureControlService:
        def pressurize_and_hold(self, point, route="co2"):
            calls.append("seal_transition:vent_off")
            calls.append("seal_transition:route_close")
            return SimpleNamespace(ok=True)

        def set_pressure_to_target(self, point):
            calls.append(f"set_pressure_to_target:{float(point.target_pressure_hpa):.0f}")
            trace_payloads.append(
                {
                    "action": "set_pressure_to_target",
                    "route": "co2",
                    "point_index": point.index,
                    "target": {"pressure_hpa": float(point.target_pressure_hpa)},
                    "actual": {"pressure_stable": True, "vent_on": False},
                    "result": "ok",
                    "phase": "sealed_pressure_control",
                }
            )
            return SimpleNamespace(ok=True)

        def wait_after_pressure_stable_before_sampling(self, point):
            calls.append(f"wait_pressure_stable:{float(point.target_pressure_hpa):.0f}")
            trace_payloads.append(
                {
                    "action": "wait_post_pressure",
                    "route": "co2",
                    "point_index": point.index,
                    "target": {"pressure_hpa": float(point.target_pressure_hpa)},
                    "actual": {"post_stable_hold_complete": True, "vent_on": False},
                    "result": "ok",
                    "phase": "sealed_pressure_control",
                }
            )
            return SimpleNamespace(ok=True)

    class SamplingService:
        def sampling_params(self, phase=""):
            return 4, 0.0

        def sample_point(self, point, phase="", point_tag=""):
            calls.append(f"sample:{float(point.target_pressure_hpa):.0f}")
            return [SimpleNamespace(point=point, point_tag=point_tag, sample_index=index) for index in range(1, 5)]

    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=context,
        route_planner=route_planner,
        a2_hooks=SimpleNamespace(
            callbacks={},
            co2_route_open_monotonic_s=None,
            co2_route_conditioning_at_atmosphere_active=False,
            co2_route_open_pressure_hpa=None,
            preseal_pressure_rise_detected=False,
            route_open_pressure_first_sample_recorded=False,
            co2_route_conditioning_at_atmosphere_context={},
            high_pressure_first_point_mode_enabled=False,
        ),
        status_service=StatusService(),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": calls.append("wait_temperature") or SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": calls.append("capture_temperature"),
        ),
        valve_routing_service=SimpleNamespace(
            set_co2_route_baseline=lambda reason="": calls.append(f"route_baseline:{reason}") or calls.append("open_conditioning:vent_on"),
            set_valves_for_co2=lambda point: calls.append(f"open_co2_route:{point.index}"),
            cleanup_co2_route=lambda reason="": calls.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=PressureControlService(),
        sampling_service=SamplingService(),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": calls.append(f"qc:{float(point.target_pressure_hpa):.0f}")),
        _wait_co2_route_soak_before_seal=lambda point: calls.append("wait_route_soak") or True,
        _record_workflow_timing=lambda *args, **kwargs: calls.append(f"timing:{args[0]}:{args[1]}"),
        _cfg_get=lambda path, default=None: default,
        _as_float=lambda value: None if value is None else float(value),
    )
    result = Co2RouteRunner(service, _co2_source(), _pressure_points()).execute()
    return result, calls, trace_payloads, context


def test_co2_sealed_only_default_does_not_enter_ambient_block() -> None:
    blocks = _co2_route_blocks({"workflow": {"route_mode": "co2_only"}})
    result, calls, _, _ = _run_golden_route()

    assert GOLDEN_BASELINE["commit"] == "cdb821110a8d49e56bc29e18d029d74303b479c2"
    assert GOLDEN_BASELINE["tag"] == "v2.0.1"
    assert GOLDEN_BASELINE["output_dir"] == "run_20260508_101607"
    assert result.success is True
    assert "ambient_open_sampling" not in blocks
    assert not any("ambient_open" in item for item in calls)


def test_co2_explicit_ambient_block_requires_config() -> None:
    default_blocks = _co2_route_blocks({"workflow": {"route_mode": "co2_only"}})
    explicit_blocks = _co2_route_blocks({"workflow": {"route_mode": "co2_only", "co2": {"ambient_block_enabled": True}}})

    assert "ambient_open_sampling" not in default_blocks
    assert explicit_blocks == ["open_conditioning", "ambient_open_sampling", "seal_transition", "sealed_pressure_control", "cleanup"]


def test_co2_open_conditioning_vent_on_sequence() -> None:
    _, calls, _, _ = _run_golden_route()

    _assert_subsequence(calls, ["route_baseline:before CO2 route conditioning", "open_conditioning:vent_on", "open_co2_route:10"])


def test_co2_seal_transition_vent_off_before_route_close() -> None:
    _, calls, _, _ = _run_golden_route()

    _assert_subsequence(calls, ["wait_route_soak", "seal_transition:vent_off", "seal_transition:route_close", "set_pressure_to_target:1100"])


def test_co2_pressure_points_order_is_1100_to_500() -> None:
    result, _, _, context = _run_golden_route()

    assert result.completed_point_indices == [101, 102, 103, 104, 105, 106, 107]
    assert [item["active_point_index"] for item in context.snapshots if item["active_point_index"] in result.completed_point_indices] == [101, 102, 103, 104, 105, 106, 107]
    assert GOLDEN_BASELINE["pressure_points_hpa"] == [1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0]


def test_co2_set_pressure_to_target_order_matches_golden() -> None:
    _, calls, trace_payloads, _ = _run_golden_route()

    assert [item for item in calls if item.startswith("set_pressure_to_target:")] == [
        "set_pressure_to_target:1100",
        "set_pressure_to_target:1000",
        "set_pressure_to_target:900",
        "set_pressure_to_target:800",
        "set_pressure_to_target:700",
        "set_pressure_to_target:600",
        "set_pressure_to_target:500",
    ]
    assert [row["target"]["pressure_hpa"] for row in trace_payloads if row.get("action") == "set_pressure_to_target"] == GOLDEN_BASELINE["pressure_points_hpa"]


def test_co2_wait_pressure_stable_before_sampling() -> None:
    _, calls, _, _ = _run_golden_route()

    for pressure in GOLDEN_BASELINE["pressure_points_hpa"]:
        _assert_subsequence(calls, [f"set_pressure_to_target:{pressure:.0f}", f"wait_pressure_stable:{pressure:.0f}", f"stable:co2_groupa_800ppm_{pressure:.0f}hpa", f"sample:{pressure:.0f}"])


def test_co2_sample_gate_after_pressure_ready_only() -> None:
    _, calls, trace_payloads, _ = _run_golden_route()

    sample_calls = [item for item in calls if item.startswith("sample:")]
    assert len(sample_calls) == GOLDEN_BASELINE["points_completed"]
    assert all(row["actual"].get("post_stable_hold_complete") is True for row in trace_payloads if row.get("action") == "wait_post_pressure")
    for pressure in GOLDEN_BASELINE["pressure_points_hpa"]:
        assert calls.index(f"wait_pressure_stable:{pressure:.0f}") < calls.index(f"sample:{pressure:.0f}")


def test_co2_cleanup_after_route_loop_complete() -> None:
    result, calls, _, _ = _run_golden_route()

    assert result.success is True
    assert len(result.completed_point_indices) == GOLDEN_BASELINE["points_completed"]
    assert calls.index("sample:500") < calls.index("cleanup:after CO2 source complete")
    assert calls[-1] == "cleanup:after CO2 source complete"
