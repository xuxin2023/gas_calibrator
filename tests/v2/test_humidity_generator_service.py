from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import threading

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import HumidityGeneratorService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager


class FakeHumidityGenerator:
    def __init__(self, snapshot: dict | None = None, *, snapshots: list[object] | None = None) -> None:
        self.snapshot = dict(snapshot or {"data": {"Tc": 25.0, "Rh": 50.0}})
        self.snapshots = list(snapshots or [])
        self.fetch_calls = 0
        self.temp_targets: list[float] = []
        self.rh_targets: list[float] = []
        self.flags: list[str] = []
        self.ensure_args: list[dict] = []

    def set_target_temp(self, value: float) -> None:
        self.temp_targets.append(float(value))

    def set_relative_humidity_pct(self, value: float) -> None:
        self.rh_targets.append(float(value))

    def enable_control(self, enabled: bool) -> None:
        self.flags.append(f"enable_control:{enabled}")

    def heat_on(self) -> None:
        self.flags.append("heat_on")

    def cool_on(self) -> None:
        self.flags.append("cool_on")

    def verify_target_readback(self, *, target_temp_c, target_rh_pct):
        return {
            "read_temp_c": target_temp_c,
            "target_temp_c": target_temp_c,
            "read_rh_pct": target_rh_pct,
            "target_rh_pct": target_rh_pct,
            "ok": True,
        }

    def ensure_run(self, **kwargs):
        self.ensure_args.append(dict(kwargs))
        return {"ok": True}

    def fetch_all(self):
        self.fetch_calls += 1
        if self.snapshots:
            next_item = self.snapshots.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            self.snapshot = dict(next_item)
        return dict(self.snapshot)


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})


def _build_service(
    tmp_path: Path,
    *,
    snapshot: dict | None = None,
    snapshots: list[object] | None = None,
    cfg_overrides: dict[str, float | bool] | None = None,
) -> tuple[HumidityGeneratorService, OrchestrationContext, RunState, SimpleNamespace, FakeHumidityGenerator]:
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
    generator = FakeHumidityGenerator(snapshot=snapshot, snapshots=snapshots)
    logs: list[str] = []
    overrides = dict(cfg_overrides or {})

    class Host(SimpleNamespace):
        _last_hgen_target = (None, None)
        _last_hgen_setpoint_ready = False

        def _device(self, *names):
            if "humidity_generator" in names:
                return generator
            return None

        def _as_float(self, value):
            if value is None:
                return None
            return float(value)

        def _call_first(self, device, method_names, *args):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    method(*args)
                    return True
            return False

        def _cfg_get(self, path: str, default=None):
            if path in overrides:
                return overrides[path]
            if path == "workflow.sensor_read_retry.retries":
                return 1
            if path == "workflow.sensor_read_retry.delay_s":
                return 0.0
            if path == "workflow.humidity_generator.min_flow_lpm":
                return 0.1
            if path == "workflow.humidity_generator.tries":
                return 2
            if path == "workflow.humidity_generator.wait_s":
                return 0.01
            if path == "workflow.humidity_generator.poll_s":
                return 0.01
            if path == "workflow.stability.humidity_generator.temp_tol_c":
                return 1.0
            if path == "workflow.stability.humidity_generator.rh_tol_pct":
                return 1.0
            if path == "workflow.stability.humidity_generator.rh_stable_window_s":
                return 0.0
            if path == "workflow.stability.humidity_generator.window_s":
                return 0.0
            if path == "workflow.stability.humidity_generator.rh_stable_span_pct":
                return 0.3
            if path == "workflow.stability.humidity_generator.timeout_s":
                return 0.2
            if path == "workflow.stability.humidity_generator.poll_s":
                return 0.05
            return default

        def _log(self, message: str):
            logs.append(message)

        def _collect_only_fast_path_enabled(self):
            return False

        def _normalize_snapshot(self, snapshot):
            return dict(snapshot or {})

        def _first_method(self, device, method_names):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    return method
            return None

        def _read_device_snapshot(self, device):
            return dict(device.snapshot)

        def _pick_numeric(self, snapshot, *keys):
            for key in keys:
                value = snapshot.get(key)
                if value is not None:
                    return float(value)
            return None

        def _check_stop(self):
            if context.stop_event.is_set():
                raise RuntimeError("stop requested")

    host = Host(logs=logs)
    return HumidityGeneratorService(context, run_state, host=host), context, run_state, host, generator


def test_humidity_generator_service_prepares_target_and_reads_snapshot(tmp_path: Path) -> None:
    service, context, run_state, host, generator = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, route="h2o")

    service.prepare_humidity_generator(point)
    temp_now, rh_now = service.read_humidity_generator_temp_rh()

    assert generator.temp_targets == [25.0]
    assert generator.rh_targets == [50.0]
    assert generator.flags == ["enable_control:True", "heat_on", "cool_on"]
    assert run_state.humidity.last_hgen_target == (25.0, 50.0)
    assert run_state.humidity.last_hgen_setpoint_ready is False
    assert host._last_hgen_target == (25.0, 50.0)
    assert host._last_hgen_setpoint_ready is False
    assert temp_now == 25.0
    assert rh_now == 50.0
    assert generator.ensure_args[0]["min_flow_lpm"] == 0.1

    context.run_logger.finalize()


def test_humidity_generator_service_wait_returns_structured_success(tmp_path: Path) -> None:
    service, context, run_state, host, generator = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, route="h2o")

    service.prepare_humidity_generator(point)
    result = service.wait_humidity_generator_stable(point)

    assert result.ok is True
    assert result.timed_out is False
    assert result.target_temp_c == 25.0
    assert result.target_rh_pct == 50.0
    assert host._last_hgen_setpoint_ready is True
    assert run_state.humidity.last_hgen_setpoint_ready is True
    assert any("reached setpoint" in message for message in host.logs)

    context.run_logger.finalize()


def test_humidity_generator_service_wait_times_out_without_stability(tmp_path: Path) -> None:
    service, context, run_state, host, generator = _build_service(
        tmp_path,
        snapshot={"data": {"Tc": 25.0, "Rh": 42.0}},
        cfg_overrides={"workflow.stability.humidity_generator.timeout_s": 0.1},
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, route="h2o")

    service.prepare_humidity_generator(point)
    result = service.wait_humidity_generator_stable(point)

    assert result.ok is False
    assert result.timed_out is True
    assert result.error == "Humidity generator reach-setpoint timeout"
    assert run_state.humidity.last_hgen_setpoint_ready is False
    assert host._last_hgen_setpoint_ready is False
    assert any("reach-setpoint timeout" in message for message in host.logs)

    context.run_logger.finalize()


def test_humidity_generator_service_wait_retries_snapshot_read(tmp_path: Path) -> None:
    service, context, run_state, host, generator = _build_service(
        tmp_path,
        snapshots=[
            RuntimeError("transient hgen read"),
            {"data": {"Tc": 25.0, "Rh": 50.0}},
        ],
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, route="h2o")

    service.prepare_humidity_generator(point)
    result = service.wait_humidity_generator_stable(point)

    assert result.ok is True
    assert generator.fetch_calls >= 2
    assert run_state.humidity.last_hgen_setpoint_ready is True

    context.run_logger.finalize()
