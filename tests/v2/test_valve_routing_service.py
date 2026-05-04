from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import threading

import pytest

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import ValveRoutingService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager


class FakeRelay:
    def __init__(self, name: str) -> None:
        self.name = name
        self.actions: list[tuple[int, bool]] = []

    def set_valve(self, channel: int, state: bool) -> None:
        self.actions.append((int(channel), bool(state)))


class FakeTemperatureChamber:
    def __init__(self) -> None:
        self.stop_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1

    def read_temp_c(self) -> float:
        return 25.0

    def read_rh_pct(self) -> float:
        return 40.0

    def read_run_state(self) -> int:
        return 0


class FakeHumidityGenerator:
    def __init__(self) -> None:
        self.safe_stop_calls = 0
        self.wait_stopped_calls = 0

    def safe_stop(self) -> None:
        self.safe_stop_calls += 1

    def wait_stopped(self, *, max_flow_lpm: float, timeout_s: float, poll_s: float) -> dict[str, float | bool]:
        self.wait_stopped_calls += 1
        return {"ok": True, "flow_lpm": 0.0, "max_flow_lpm": max_flow_lpm, "timeout_s": timeout_s, "poll_s": poll_s}


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})


def _build_service(
    tmp_path: Path,
) -> tuple[
    ValveRoutingService,
    OrchestrationContext,
    RunState,
    SimpleNamespace,
    FakeRelay,
    FakeRelay,
    FakeTemperatureChamber,
    FakeHumidityGenerator,
]:
    config = _config(tmp_path)
    session = RunSession(config)
    event_bus = EventBus()
    state_manager = StateManager(event_bus)
    result_store = ResultStore(tmp_path, session.run_id)
    run_logger = RunLogger(str(tmp_path), session.run_id)
    device_manager = DeviceManager(config.devices)
    stability_checker = StabilityChecker(config.workflow.stability)
    stop_event = threading.Event()
    pause_event = threading.Event()
    pause_event.set()
    context = OrchestrationContext(
        config=config,
        session=session,
        state_manager=state_manager,
        event_bus=event_bus,
        result_store=result_store,
        run_logger=run_logger,
        device_manager=device_manager,
        stability_checker=stability_checker,
        stop_event=stop_event,
        pause_event=pause_event,
    )
    run_state = RunState()
    relay_a = FakeRelay("relay_a")
    relay_b = FakeRelay("relay_b")
    chamber = FakeTemperatureChamber()
    humidity_generator = FakeHumidityGenerator()
    logs: list[str] = []
    vent_calls: list[tuple[bool, str]] = []
    config_map = {
        "valves": {
            "gas_main": 1,
            "h2o_path": 2,
            "hold": 3,
            "flow_switch": 4,
            "co2_path": 5,
            "co2_path_group2": 7,
            "co2_map": {"400": 6},
            "co2_map_group2": {"400": 8},
            "relay_map": {
                "7": {"device": "relay_8", "channel": 2},
                "8": {"device": "relay_8", "channel": 3},
            },
        },
        "workflow": {
            "humidity_generator": {
                "safe_stop_max_flow_lpm": 0.05,
                "safe_stop_timeout_s": 15.0,
                "safe_stop_poll_s": 0.5,
            }
        },
    }

    class Host(SimpleNamespace):
        _preseal_dewpoint_snapshot = None
        _post_h2o_co2_zero_flush_pending = False

        def _cfg_get(self, path: str, default=None):
            node = config_map
            for part in str(path).split("."):
                if not part:
                    continue
                if isinstance(node, dict):
                    node = node.get(part)
                else:
                    node = None
                if node is None:
                    return default
            return node

        def _as_int(self, value):
            if value is None:
                return None
            return int(value)

        def _as_float(self, value):
            if value is None:
                return None
            return float(value)

        def _device(self, *names):
            if "relay_a" in names:
                return relay_a
            if "relay_b" in names:
                return relay_b
            if "temperature_chamber" in names:
                return chamber
            if "humidity_generator" in names:
                return humidity_generator
            return None

        def _call_first(self, device, method_names, *args):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    method(*args)
                    return True
            return False

        def _log(self, message: str):
            logs.append(message)

        def _set_pressure_controller_vent(self, vent_on: bool, reason: str = ""):
            vent_calls.append((bool(vent_on), str(reason)))

        def _route_mode(self) -> str:
            return "h2o_then_co2"

    host = Host(logs=logs, vent_calls=vent_calls)
    service = ValveRoutingService(context, run_state, host=host)
    return service, context, run_state, host, relay_a, relay_b, chamber, humidity_generator


def _relay_state_map(relay: FakeRelay) -> dict[int, bool]:
    state: dict[int, bool] = {}
    for channel, value in relay.actions:
        state[channel] = value
    return state


def test_valve_routing_service_manages_baseline_and_h2o_path(tmp_path: Path) -> None:
    service, context, run_state, host, relay_a, relay_b, _, _ = _build_service(tmp_path)

    assert service.managed_valves() == [1, 2, 3, 4, 5, 6, 7, 8]
    assert service.resolve_valve_target(7) == ("relay_b", 2)
    assert service.desired_valve_state(4, {2, 3}) is True

    service.set_h2o_path(True)
    h2o_state_a = _relay_state_map(relay_a)
    assert h2o_state_a[2] is True
    assert h2o_state_a[3] is True
    assert h2o_state_a[4] is True

    relay_a.actions.clear()
    relay_b.actions.clear()
    service.apply_route_baseline_valves()
    baseline_a = _relay_state_map(relay_a)
    baseline_b = _relay_state_map(relay_b)
    assert baseline_a[1] is False
    assert baseline_a[2] is False
    assert baseline_a[3] is False
    assert baseline_a[4] is False
    assert baseline_b[2] is False
    assert baseline_b[3] is False

    context.run_logger.finalize()


def test_valve_routing_service_handles_co2_path_cleanup_and_pending_mark(tmp_path: Path) -> None:
    service, context, run_state, host, relay_a, relay_b, _, _ = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, route="co2", co2_group="B")

    maps = service.co2_maps_for_point(point)
    path = service.co2_path_for_point(point)
    source = service.source_valve_for_point(point)
    open_valves = service.co2_open_valves(point, include_total_valve=True)
    service.set_valves_for_co2(point)

    assert maps[0]["400"] == 8
    assert path == 7
    assert source == 8
    assert open_valves == [2, 1, 7, 8]
    state_a = _relay_state_map(relay_a)
    state_b = _relay_state_map(relay_b)
    assert state_a[1] is True
    assert state_a[2] is True
    assert state_b[2] is True
    assert state_b[3] is True

    run_state.humidity.preseal_dewpoint_snapshot = {"dewpoint_c": 5.0}
    host._preseal_dewpoint_snapshot = {"dewpoint_c": 5.0}
    service.cleanup_h2o_route(point, reason="after H2O route")
    assert run_state.humidity.preseal_dewpoint_snapshot is None
    assert host._preseal_dewpoint_snapshot is None

    service.mark_post_h2o_co2_zero_flush_pending()
    assert run_state.humidity.post_h2o_co2_zero_flush_pending is True
    assert host._post_h2o_co2_zero_flush_pending is True

    service.cleanup_co2_route(reason="after CO2 route")
    assert host.vent_calls[0] == (True, "after H2O route")
    assert host.vent_calls[-1] == (True, "after CO2 route")
    assert any("CO2 route baseline applied" in message for message in host.logs)

    context.run_logger.finalize()


def test_co2_route_baseline_closes_valves_before_pressure_vent_gate_failure(tmp_path: Path) -> None:
    service, context, run_state, host, relay_a, relay_b, _, _ = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, route="co2", co2_group="B")
    service.set_valves_for_co2(point)
    relay_a.actions.clear()
    relay_b.actions.clear()

    def _fail_pressure_vent(_vent_on: bool, reason: str = "") -> None:
        raise RuntimeError("pressure atmosphere gate failed")

    host._set_pressure_controller_vent = _fail_pressure_vent

    with pytest.raises(RuntimeError, match="pressure atmosphere gate failed"):
        service.set_co2_route_baseline(reason="before CO2 route conditioning")

    baseline_a = _relay_state_map(relay_a)
    baseline_b = _relay_state_map(relay_b)
    assert baseline_a[1] is False
    assert baseline_a[2] is False
    assert baseline_a[3] is False
    assert baseline_a[4] is False
    assert baseline_a[5] is False
    assert baseline_a[6] is False
    assert baseline_b[2] is False
    assert baseline_b[3] is False

    context.run_logger.finalize()


def test_valve_routing_service_safe_stop_after_run_restores_baseline_and_stops_aux_devices(tmp_path: Path) -> None:
    service, context, run_state, host, relay_a, relay_b, chamber, humidity_generator = _build_service(tmp_path)
    run_state.humidity.preseal_dewpoint_snapshot = {"dewpoint_c": 5.0}
    run_state.humidity.post_h2o_co2_zero_flush_pending = True
    run_state.humidity.initial_co2_zero_flush_pending = True
    run_state.humidity.last_hgen_target = (25.0, 60.0)
    run_state.humidity.last_hgen_setpoint_ready = True
    host._preseal_dewpoint_snapshot = {"dewpoint_c": 5.0}
    host._post_h2o_co2_zero_flush_pending = True
    host._initial_co2_zero_flush_pending = True
    host._last_hgen_target = (25.0, 60.0)
    host._last_hgen_setpoint_ready = True

    restore_summary = service.restore_baseline_after_run(reason="restore baseline on finish")
    safe_summary = service.safe_stop_after_run(baseline_already_restored=True, reason="final safe stop")

    assert "relay_state" in restore_summary
    assert chamber.stop_calls == 1
    assert humidity_generator.safe_stop_calls == 1
    assert humidity_generator.wait_stopped_calls == 1
    assert run_state.humidity.preseal_dewpoint_snapshot is None
    assert run_state.humidity.post_h2o_co2_zero_flush_pending is False
    assert run_state.humidity.initial_co2_zero_flush_pending is False
    assert run_state.humidity.last_hgen_target == (None, None)
    assert run_state.humidity.last_hgen_setpoint_ready is False
    assert host._preseal_dewpoint_snapshot is None
    assert host._post_h2o_co2_zero_flush_pending is False
    assert host._initial_co2_zero_flush_pending is False
    assert host._last_hgen_target == (None, None)
    assert host._last_hgen_setpoint_ready is False
    assert safe_summary["chamber"]["run_state"] == 0
    assert safe_summary["hgen_stop_check"]["ok"] is True
    assert any("Final route baseline applied" in message for message in host.logs)
    assert any("Final route safe stop complete" in message for message in host.logs)

    context.run_logger.finalize()
