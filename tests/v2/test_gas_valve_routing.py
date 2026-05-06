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


def _relay_state_map(relay: FakeRelay) -> dict[int, bool]:
    state: dict[int, bool] = {}
    for channel, value in relay.actions:
        state[channel] = value
    return state


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
    logs: list[str] = []
    vent_calls: list[tuple[bool, str]] = []
    config_map = {
        "valves": {
            "gas_main": 11,
            "h2o_path": 8,
            "hold": 9,
            "flow_switch": 10,
            "co2_path": 7,
            "co2_path_group2": 16,
            "co2_map": {
                "0": 1,
                "200": 2,
                "400": 3,
                "600": 4,
                "800": 5,
                "1000": 6,
            },
            "co2_map_group2": {
                "0": 21,
                "100": 22,
                "300": 23,
                "500": 24,
                "700": 25,
                "900": 26,
            },
            "relay_map": {
                "1": {"device": "relay", "channel": 7},
                "2": {"device": "relay", "channel": 8},
                "3": {"device": "relay", "channel": 9},
                "4": {"device": "relay", "channel": 10},
                "5": {"device": "relay", "channel": 11},
                "6": {"device": "relay", "channel": 12},
                "7": {"device": "relay", "channel": 15},
                "8": {"device": "relay_8", "channel": 8},
                "9": {"device": "relay_8", "channel": 1},
                "10": {"device": "relay_8", "channel": 2},
                "11": {"device": "relay_8", "channel": 3},
                "16": {"device": "relay", "channel": 16},
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
            return "co2_only"

    host = Host(logs=logs, vent_calls=vent_calls)
    service = ValveRoutingService(context, run_state, host=host)
    return service, context, run_state, host, relay_a, relay_b


def test_co2_map_1000ppm_selects_channel_6(tmp_path: Path) -> None:
    service, context, _, _, _, _ = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=1000.0, route="co2", co2_group="A")

    source = service.source_valve_for_point(point)
    assert source == 6, f"co2_map 1000ppm should map to logical valve 6, got {source}"

    context.run_logger.finalize()


def test_co2_map_0ppm_selects_channel_1(tmp_path: Path) -> None:
    service, context, _, _, _, _ = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=0.0, route="co2", co2_group="A")

    source = service.source_valve_for_point(point)
    assert source is None, "0ppm always skipped; source_valve_for_point resolves to None (0 is falsy in or chain)"

    context.run_logger.finalize()


def test_co2_map_400ppm_selects_channel_3(tmp_path: Path) -> None:
    service, context, _, _, _, _ = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, route="co2", co2_group="A")

    source = service.source_valve_for_point(point)
    assert source == 3, f"co2_map 400ppm should map to logical valve 3, got {source}"

    context.run_logger.finalize()


def test_set_valves_for_co2_1000ppm_opens_correct_relay_channels(tmp_path: Path) -> None:
    service, context, _, _, relay_a, relay_b = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=1000.0, route="co2", co2_group="A")

    service.set_valves_for_co2(point)

    open_valves = service.co2_open_valves(point, include_total_valve=True)
    state_a = _relay_state_map(relay_a)
    state_b = _relay_state_map(relay_b)

    assert 11 in open_valves, "gas_main (11) should be open"
    assert 8 in open_valves, "h2o_path (8) should be open"
    assert 7 in open_valves, "co2_path (7) should be open"
    assert 6 in open_valves, "source valve (6) for 1000ppm should be open"

    assert state_b[3] is True, "relay_8 channel 3 (gas_main=11 via relay_map 11->relay_8:3) should be ON"
    assert state_b[8] is True, "relay_8 channel 8 (h2o_path=8 via relay_map 8->relay_8:8) should be ON"
    assert state_a[15] is True, "relay channel 15 (co2_path=7 via relay_map 7->relay:15) should be ON"
    assert state_a[12] is True, "relay channel 12 (source=6 via relay_map 6->relay:12) should be ON"

    context.run_logger.finalize()


def test_co2_map_group2_is_preferred_for_group_b(tmp_path: Path) -> None:
    service, context, _, _, _, _ = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=100.0, route="co2", co2_group="B")

    source = service.source_valve_for_point(point)
    path = service.co2_path_for_point(point)
    assert source == 22, f"co2_map_group2 100ppm should map to 22, got {source}"
    assert path == 16, f"co2_path_group2 should be 16, got {path}"

    context.run_logger.finalize()


def test_co2_open_valves_skips_source_when_ppm_not_in_map(tmp_path: Path) -> None:
    service, context, _, _, _, _ = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=1500.0, route="co2", co2_group="A")

    source = service.source_valve_for_point(point)
    assert source is None, "1500ppm not in co2_map, source should be None"

    open_valves = service.co2_open_valves(point, include_total_valve=True)
    assert 11 in open_valves
    assert 8 in open_valves
    assert 7 in open_valves
    assert len([v for v in open_valves if v not in (11, 8, 7)]) == 0, (
        "no extra source valve should be open for unmapped ppm"
    )

    context.run_logger.finalize()
