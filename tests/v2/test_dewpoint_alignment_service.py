from __future__ import annotations

from pathlib import Path
import threading

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import DewpointAlignmentService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager


class FakeDewpointMeter:
    def __init__(self, *, snapshots: list[object] | None = None) -> None:
        self.opened = False
        self.snapshots = list(snapshots or [])
        self.fetch_calls = 0

    def open(self) -> None:
        self.opened = True

    def fetch_all(self):
        self.fetch_calls += 1
        if self.snapshots:
            next_item = self.snapshots.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            return dict(next_item)
        return {"dewpoint_c": 1.2, "temp_c": 25.0, "rh_pct": 50.0}


def test_dewpoint_alignment_service_handles_ready_soak_alignment_and_snapshot(tmp_path: Path) -> None:
    config = AppConfig.from_dict(
        {
            "paths": {"output_dir": str(tmp_path)},
            "workflow": {
                "stability": {
                    "h2o_route": {"preseal_soak_s": 0.0},
                    "dewpoint": {
                        "window_s": 0.11,
                        "timeout_s": 0.5,
                        "poll_s": 0.01,
                        "temp_match_tol_c": 0.3,
                        "rh_match_tol_pct": 3.0,
                        "stability_tol_c": 1.0,
                    },
                }
            },
        }
    )
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
    dewpoint = FakeDewpointMeter()
    calls: list[tuple[str, object]] = []

    class Host:
        def _device(self, *names):
            return dewpoint if "dewpoint_meter" in names else None

        def _collect_only_fast_path_enabled(self):
            return False

        def _log(self, message: str):
            calls.append(("log", message))

        def _normalize_snapshot(self, snapshot):
            if isinstance(snapshot, dict) and "data" in snapshot:
                return dict(snapshot["data"])
            return dict(snapshot or {})

        def _first_method(self, device, method_names):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    return method
            return None

        def _read_device_snapshot(self, device):
            return device.fetch_all()

        def _cfg_get(self, path: str, default=None):
            mapping = {
                "workflow.sensor_read_retry.retries": 1,
                "workflow.sensor_read_retry.delay_s": 0.0,
                "workflow.stability.h2o_route.preseal_soak_s": 0.0,
                "workflow.stability.dewpoint.window_s": 0.11,
                "workflow.stability.dewpoint.timeout_s": 0.5,
                "workflow.stability.dewpoint.poll_s": 0.01,
                "workflow.stability.dewpoint.temp_match_tol_c": 0.3,
                "workflow.stability.dewpoint.rh_match_tol_pct": 3.0,
                "workflow.stability.dewpoint.stability_tol_c": 1.0,
            }
            return mapping.get(path, default)

        def _check_stop(self):
            return None

        def _as_float(self, value):
            return None if value is None else float(value)

        def _read_humidity_generator_temp_rh(self):
            return 25.0, 50.0

        def _make_pressure_reader(self):
            return lambda: 1000.0

        def _set_pressure_controller_vent(self, vent_on: bool, reason: str = ""):
            calls.append(("vent", vent_on, reason))

        def _set_h2o_path(self, is_open: bool, point=None):
            calls.append(("h2o_path", is_open, getattr(point, "index", None)))

    service = DewpointAlignmentService(context, run_state, host=Host())
    point = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, pressure_hpa=1000.0, route="h2o")

    assert service.ensure_dewpoint_meter_ready() is True
    assert service.wait_h2o_route_soak_before_seal(point) is True
    assert service.wait_dewpoint_alignment_stable(point) is True
    service.capture_preseal_dewpoint_snapshot()
    assert service.open_h2o_route_and_wait_ready(point) is True
    assert dewpoint.opened is True
    assert run_state.humidity.preseal_dewpoint_snapshot is not None
    assert any(item[0] == "vent" for item in calls)
    assert any(item[0] == "h2o_path" for item in calls)

    context.run_logger.finalize()


def test_dewpoint_alignment_service_retries_snapshot_read(tmp_path: Path) -> None:
    config = AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})
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
    dewpoint = FakeDewpointMeter(
        snapshots=[
            RuntimeError("transient dewpoint read"),
            {"dewpoint_c": 1.4, "temp_c": 25.1, "rh_pct": 49.8},
        ]
    )
    calls: list[tuple[str, object]] = []

    class Host:
        def _device(self, *names):
            return dewpoint if "dewpoint_meter" in names else None

        def _collect_only_fast_path_enabled(self):
            return False

        def _log(self, message: str):
            calls.append(("log", message))

        def _normalize_snapshot(self, snapshot):
            if isinstance(snapshot, dict) and "data" in snapshot:
                return dict(snapshot["data"])
            return dict(snapshot or {})

        def _first_method(self, device, method_names):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    return method
            return None

        def _cfg_get(self, path: str, default=None):
            mapping = {
                "workflow.sensor_read_retry.retries": 1,
                "workflow.sensor_read_retry.delay_s": 0.0,
            }
            return mapping.get(path, default)

        def _check_stop(self):
            return None

        def _as_float(self, value):
            return None if value is None else float(value)

        def _read_humidity_generator_temp_rh(self):
            return 25.0, 50.0

        def _make_pressure_reader(self):
            return lambda: 1000.0

        def _set_pressure_controller_vent(self, vent_on: bool, reason: str = ""):
            calls.append(("vent", vent_on, reason))

        def _set_h2o_path(self, is_open: bool, point=None):
            calls.append(("h2o_path", is_open, getattr(point, "index", None)))

    service = DewpointAlignmentService(context, run_state, host=Host())

    assert service.ensure_dewpoint_meter_ready() is True
    assert dewpoint.fetch_calls == 2
    assert any("Sensor read retry (dewpoint meter initial read) 1/1: error=transient dewpoint read" in item[1] for item in calls if item[0] == "log")

    context.run_logger.finalize()


def test_dewpoint_alignment_service_consumes_legacy_temp_and_rh_tolerances(tmp_path: Path) -> None:
    config = AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})
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
    dewpoint = FakeDewpointMeter(
        snapshots=[
            {"dewpoint_c": 1.0, "temp_c": 25.4, "rh_pct": 54.5},
            {"dewpoint_c": 1.0, "temp_c": 25.4, "rh_pct": 54.5},
            {"dewpoint_c": 1.0, "temp_c": 25.4, "rh_pct": 54.5},
        ]
    )
    calls: list[str] = []

    class Host:
        def _device(self, *names):
            return dewpoint if "dewpoint_meter" in names else None

        def _collect_only_fast_path_enabled(self):
            return False

        def _log(self, message: str):
            calls.append(message)

        def _normalize_snapshot(self, snapshot):
            return dict(snapshot or {})

        def _first_method(self, device, method_names):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    return method
            return None

        def _read_device_snapshot(self, device):
            return device.fetch_all()

        def _cfg_get(self, path: str, default=None):
            mapping = {
                "workflow.sensor_read_retry.retries": 0,
                "workflow.sensor_read_retry.delay_s": 0.0,
                "workflow.stability.dewpoint.window_s": 0.2,
                "workflow.stability.dewpoint.timeout_s": 0.35,
                "workflow.stability.dewpoint.poll_s": 0.1,
                "workflow.stability.dewpoint.temp_tol_c": 0.5,
                "workflow.stability.dewpoint.rh_tol_pct": 5.0,
                "workflow.stability.dewpoint.stability_tol_c": 0.01,
                "workflow.stability.dewpoint.min_samples": 2,
            }
            return mapping.get(path, default)

        def _check_stop(self):
            return None

        def _as_float(self, value):
            return None if value is None else float(value)

        def _read_humidity_generator_temp_rh(self):
            return 25.0, 50.0

        def _make_pressure_reader(self):
            return lambda: 1000.0

        def _set_pressure_controller_vent(self, vent_on: bool, reason: str = ""):
            return None

        def _set_h2o_path(self, is_open: bool, point=None):
            return None

    service = DewpointAlignmentService(context, run_state, host=Host())
    point = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, pressure_hpa=1000.0, route="h2o")

    assert service.wait_dewpoint_alignment_stable(point) is True
    assert any("matched humidity generator" in message for message in calls)

    context.run_logger.finalize()


def test_dewpoint_alignment_service_respects_min_samples(tmp_path: Path) -> None:
    config = AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})
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
    dewpoint = FakeDewpointMeter(
        snapshots=[
            {"dewpoint_c": 1.1, "temp_c": 25.0, "rh_pct": 50.0},
            {"dewpoint_c": 1.1, "temp_c": 25.0, "rh_pct": 50.0},
            {"dewpoint_c": 1.1, "temp_c": 25.0, "rh_pct": 50.0},
        ]
    )
    calls: list[str] = []

    class Host:
        def _device(self, *names):
            return dewpoint if "dewpoint_meter" in names else None

        def _collect_only_fast_path_enabled(self):
            return False

        def _log(self, message: str):
            calls.append(message)

        def _normalize_snapshot(self, snapshot):
            return dict(snapshot or {})

        def _first_method(self, device, method_names):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    return method
            return None

        def _read_device_snapshot(self, device):
            return device.fetch_all()

        def _cfg_get(self, path: str, default=None):
            mapping = {
                "workflow.sensor_read_retry.retries": 0,
                "workflow.sensor_read_retry.delay_s": 0.0,
                "workflow.stability.dewpoint.window_s": 0.11,
                "workflow.stability.dewpoint.timeout_s": 0.25,
                "workflow.stability.dewpoint.poll_s": 0.1,
                "workflow.stability.dewpoint.temp_match_tol_c": 0.3,
                "workflow.stability.dewpoint.rh_match_tol_pct": 3.0,
                "workflow.stability.dewpoint.stability_tol_c": 0.01,
                "workflow.stability.dewpoint.min_samples": 3,
            }
            return mapping.get(path, default)

        def _check_stop(self):
            return None

        def _as_float(self, value):
            return None if value is None else float(value)

        def _read_humidity_generator_temp_rh(self):
            return 25.0, 50.0

        def _make_pressure_reader(self):
            return lambda: 1000.0

        def _set_pressure_controller_vent(self, vent_on: bool, reason: str = ""):
            return None

        def _set_h2o_path(self, is_open: bool, point=None):
            return None

    service = DewpointAlignmentService(context, run_state, host=Host())
    point = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, pressure_hpa=1000.0, route="h2o")

    assert service.wait_dewpoint_alignment_stable(point) is False
    assert any("samples=" in message or "stability timeout" in message for message in calls)

    context.run_logger.finalize()
