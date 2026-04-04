from __future__ import annotations

import json
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
from gas_calibrator.v2.core.services import TemperatureControlService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.state_manager import StateManager
from gas_calibrator.v2.exceptions import StabilityTimeoutError


class FakeTemperatureChamber:
    def __init__(
        self,
        temp_c: float = 25.0,
        *,
        readouts: list[object] | None = None,
        setpoint_readbacks: list[object] | None = None,
        start_error: Exception | None = None,
        run_state_readouts: list[object] | None = None,
        fallback_start_error: Exception | None = None,
    ) -> None:
        self.temp_c = temp_c
        self.readouts = list(readouts or [])
        self.setpoint_readbacks = list(setpoint_readbacks or [])
        self.start_error = start_error
        self.run_state_readouts = list(run_state_readouts or [])
        self.fallback_start_error = fallback_start_error
        self.read_calls = 0
        self.setpoint_read_calls = 0
        self.run_state_calls = 0
        self.start_called = 0
        self.stop_called = 0
        self.set_targets: list[float] = []
        self.fallback_calls: list[tuple[str, int, int]] = []
        self.commanded_setpoint_c = float(temp_c)

    def set_temp_c(self, value: float) -> None:
        self.temp_c = float(value)
        self.commanded_setpoint_c = float(value)
        self.set_targets.append(float(value))

    def start(self) -> None:
        self.start_called += 1
        if self.start_error is not None:
            raise self.start_error

    def stop(self) -> None:
        self.stop_called += 1

    def read_temp_c(self) -> float:
        self.read_calls += 1
        if self.readouts:
            next_item = self.readouts.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            self.temp_c = float(next_item)
        return self.temp_c

    def read_set_temp_c(self) -> float:
        self.setpoint_read_calls += 1
        if self.setpoint_readbacks:
            next_item = self.setpoint_readbacks.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            self.commanded_setpoint_c = float(next_item)
        return float(self.commanded_setpoint_c)

    def read_run_state(self) -> int:
        self.run_state_calls += 1
        if self.run_state_readouts:
            next_item = self.run_state_readouts.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            return int(next_item)
        return 1

    def _call_with_addr(self, method_name: str, address: int, value: int):
        self.fallback_calls.append((method_name, address, value))
        if self.fallback_start_error is not None:
            raise self.fallback_start_error
        return {"ok": True}

    def _raise_on_modbus_error(self, response) -> None:
        return None


class FakeTemperatureChamberWithoutSetpointReadback:
    def __init__(self, temp_c: float = 25.0) -> None:
        self.temp_c = float(temp_c)
        self.set_targets: list[float] = []
        self.start_called = 0

    def set_temp_c(self, value: float) -> None:
        self.temp_c = float(value)
        self.set_targets.append(float(value))

    def start(self) -> None:
        self.start_called += 1

    def read_temp_c(self) -> float:
        return float(self.temp_c)


class PassingStabilityChecker:
    def wait_for_stability(self, stability_type, read_func, stop_event, *, min_wait_s=0.0):
        return SimpleNamespace(elapsed_s=min_wait_s, last_value=read_func(), stable=True)


class TimeoutStabilityChecker:
    def wait_for_stability(self, stability_type, read_func, stop_event, *, min_wait_s=0.0):
        raise StabilityTimeoutError(parameter=stability_type.value, actual=read_func(), tolerance=0.2, timeout_s=1.0)


class MultiReadStabilityChecker:
    def __init__(self, reads: int) -> None:
        self.reads = reads

    def wait_for_stability(self, stability_type, read_func, stop_event, *, min_wait_s=0.0, max_wait_s=None):
        last_value = None
        for _ in range(self.reads):
            last_value = read_func()
        return SimpleNamespace(elapsed_s=min_wait_s, last_value=last_value, stable=True)


class LegacyStabilityChecker:
    def wait_for_stability(self, stability_type, read_func, stop_event):
        return SimpleNamespace(elapsed_s=0.0, last_value=read_func(), stable=True)


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})


def _build_service(
    tmp_path: Path,
    *,
    stability_checker,
    chamber: FakeTemperatureChamber | None = None,
    analyzers: list[tuple[str, object, object]] | None = None,
    cfg_overrides: dict[str, object] | None = None,
) -> tuple[TemperatureControlService, OrchestrationContext, RunState, SimpleNamespace]:
    config = _config(tmp_path)
    session = RunSession(config)
    event_bus = EventBus()
    state_manager = StateManager(event_bus)
    result_store = ResultStore(tmp_path, session.run_id)
    run_logger = RunLogger(str(tmp_path), session.run_id)
    device_manager = DeviceManager(config.devices)
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
    chamber_obj = chamber
    logs: list[str] = []
    cfg_map = dict(cfg_overrides or {})

    class Host:
        _temperature_ready_target_c = None

        def _device(self, *names):
            if "temperature_chamber" in names:
                return chamber_obj
            return None

        def _cfg_get(self, path: str, default=None):
            if path in cfg_map:
                return cfg_map[path]
            if path == "workflow.sensor_read_retry.retries":
                return 1
            if path == "workflow.sensor_read_retry.delay_s":
                return 0.0
            if path == "workflow.stability.temperature.tol":
                return 0.2
            if path == "workflow.stability.temperature.soak_after_reach_s":
                return 0.0
            return default

        def _update_status(self, **kwargs):
            return None

        def _call_first(self, device, method_names, *args):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    method(*args)
                    return True
            return False

        def _first_method(self, device, method_names):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    return method
            return None

        def _make_temperature_reader(self, chamber):
            return None if chamber is None else chamber.read_temp_c

        def _collect_only_fast_path_enabled(self):
            return False

        def _log(self, message: str):
            logs.append(message)

        def _check_stop(self):
            if context.stop_event.is_set():
                raise RuntimeError("stop requested")

        def _all_gas_analyzers(self):
            return list(analyzers or [])

        def _active_gas_analyzers(self):
            return list(analyzers or [])

        def _normalize_snapshot(self, snapshot):
            return dict(snapshot or {})

        def _read_device_snapshot(self, analyzer):
            if hasattr(analyzer, "fetch_all"):
                return analyzer.fetch_all()
            return {}

        def _pick_numeric(self, snapshot, *keys):
            for key in keys:
                value = snapshot.get(key)
                if value is not None:
                    return float(value)
            return None

        def _remember_output_file(self, path: str):
            run_state.artifacts.output_files.append(path)

        def _as_int(self, value):
            return None if value is None else int(value)

    host = Host()
    host.logs = logs
    return TemperatureControlService(context, run_state, host=host), context, run_state, host


def test_temperature_control_service_returns_structured_success_and_bool_compatible_snapshot(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(temp_c=25.0)
    analyzer = SimpleNamespace(fetch_all=lambda: {"chamber_temp_c": 25.1, "case_temp_c": 26.0})
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=PassingStabilityChecker(),
        chamber=chamber,
        analyzers=[("GA01", analyzer, None)],
        cfg_overrides={"workflow.stability.temperature.analyzer_chamber_temp_enabled": False},
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")
    captured = service.capture_temperature_calibration_snapshot(point, route_type="co2")
    service.export_temperature_snapshots()

    path = context.result_store.run_dir / "temperature_snapshots.json"
    assert wait.ok is True
    assert wait.reused_previous_stability is False
    assert wait.target_c == 25.0
    assert chamber.set_targets == [25.0]
    assert chamber.start_called == 1
    assert run_state.temperature.ready_target_c == 25.0
    assert captured is True
    assert len(run_state.temperature.snapshots) == 1
    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload[0]["analyzer_id"] == "GA01"
    assert str(path) in run_state.artifacts.output_files

    context.run_logger.finalize()


def test_temperature_control_service_rewrites_mismatched_setpoint_once(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(temp_c=25.0, setpoint_readbacks=[22.0, 25.0])
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=PassingStabilityChecker(),
        chamber=chamber,
        cfg_overrides={"workflow.stability.temperature.analyzer_chamber_temp_enabled": False},
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert chamber.set_targets == [25.0, 25.0]
    assert chamber.setpoint_read_calls == 2
    assert wait.diagnostics["setpoint_readback_supported"] is True
    assert wait.diagnostics["setpoint_rewrite_attempted"] is True
    assert wait.diagnostics["setpoint_readback_c"] == 22.0
    assert wait.diagnostics["setpoint_readback_after_rewrite_c"] == 25.0
    assert any("setpoint mismatch; rewrite target" in message for message in host.logs)

    context.run_logger.finalize()


def test_temperature_control_service_warns_when_setpoint_readback_is_unsupported(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamberWithoutSetpointReadback(temp_c=25.0)
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=PassingStabilityChecker(),
        chamber=chamber,
        cfg_overrides={"workflow.stability.temperature.analyzer_chamber_temp_enabled": False},
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert chamber.set_targets == [25.0]
    assert chamber.start_called == 1
    assert wait.diagnostics["setpoint_readback_supported"] is False
    assert wait.diagnostics["setpoint_readback_warning"] == "unsupported"
    assert any("setpoint readback unsupported" in message for message in host.logs)

    context.run_logger.finalize()


def test_temperature_control_service_allows_timeout_recovery_with_in_band_temperature(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(temp_c=25.0)
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=TimeoutStabilityChecker(),
        chamber=chamber,
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert wait.timed_out is True
    assert wait.final_temp_c == 25.0
    assert run_state.temperature.ready_target_c == 25.0
    assert any("stability timeout reached in-band value" in message for message in host.logs)

    context.run_logger.finalize()


def test_temperature_control_service_retries_chamber_read_during_wait(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(temp_c=25.0, readouts=[RuntimeError("transient chamber read"), 25.0])
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=PassingStabilityChecker(),
        chamber=chamber,
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert chamber.read_calls >= 2
    assert run_state.temperature.ready_target_c == 25.0

    context.run_logger.finalize()


def test_temperature_control_service_retries_run_state_read_during_fallback_start(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(
        temp_c=25.0,
        start_error=RuntimeError("START_STATE_MISMATCH"),
        run_state_readouts=[RuntimeError("transient run_state"), 0, 1],
    )
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=PassingStabilityChecker(),
        chamber=chamber,
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert chamber.run_state_calls >= 3
    assert chamber.fallback_calls == [("write_register", 8010, 1)]
    assert not any("Sensor read retry (temperature chamber run_state)" in message for message in host.logs)
    assert any("fallback start succeeded" in message for message in host.logs)

    context.run_logger.finalize()


def test_temperature_control_service_consumes_wait_skip_and_command_offset(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(temp_c=20.0)
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=PassingStabilityChecker(),
        chamber=chamber,
        cfg_overrides={
            "workflow.stability.temperature.wait_for_target_before_continue": False,
            "workflow.stability.temperature.command_offset_c": 1.5,
        },
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert wait.diagnostics["wait_skipped"] is True
    assert wait.diagnostics["command_target_c"] == 26.5
    assert chamber.set_targets == [26.5]
    assert chamber.start_called == 1
    assert run_state.temperature.last_target_c == 25.0
    assert run_state.temperature.last_soak_done is False

    context.run_logger.finalize()


def test_temperature_control_service_reuses_running_in_tol_without_restarting(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(temp_c=25.0)
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=PassingStabilityChecker(),
        chamber=chamber,
        cfg_overrides={
            "workflow.stability.temperature.soak_after_reach_s": 10.0,
            "workflow.stability.temperature.reuse_running_in_tol_without_soak": True,
        },
    )
    run_state.temperature.last_target_c = 20.0
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert chamber.set_targets == []
    assert chamber.start_called == 0
    assert run_state.temperature.last_target_c == 25.0
    assert run_state.temperature.last_soak_done is True
    assert any("reuse current thermal state for soak/stability wait" in message for message in host.logs)

    context.run_logger.finalize()


def test_temperature_control_service_restarts_on_target_change_when_configured(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(temp_c=20.0)
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=PassingStabilityChecker(),
        chamber=chamber,
        cfg_overrides={"workflow.stability.temperature.restart_on_target_change": True},
    )
    run_state.temperature.last_target_c = 20.0
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert chamber.stop_called == 1
    assert chamber.set_targets == [25.0]
    assert chamber.start_called == 1

    context.run_logger.finalize()


def test_temperature_control_service_fails_on_transition_stall(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    chamber = FakeTemperatureChamber(temp_c=10.0, readouts=[10.0, 10.05, 10.08])
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=MultiReadStabilityChecker(reads=3),
        chamber=chamber,
        cfg_overrides={
            "workflow.stability.temperature.transition_check_window_s": 1.0,
            "workflow.stability.temperature.transition_min_delta_c": 0.3,
        },
    )
    point = CalibrationPoint(index=1, temperature_c=20.0, route="co2")
    monotonic_values = iter([0.0, 0.4, 1.2, 1.2])

    monkeypatch.setattr(
        "gas_calibrator.v2.core.services.temperature_control_service.time.monotonic",
        lambda: next(monotonic_values),
    )

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is False
    assert wait.diagnostics["stage"] == "transition"
    assert "transition stalled" in wait.error
    assert run_state.temperature.last_soak_done is False

    context.run_logger.finalize()


def test_temperature_control_service_refreshes_live_snapshots_during_wait(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(temp_c=10.0, readouts=[10.0, 15.0, 20.0])
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=MultiReadStabilityChecker(reads=3),
        chamber=chamber,
        cfg_overrides={"workflow.stability.temperature.analyzer_chamber_temp_enabled": False},
    )
    refresh_calls: list[dict[str, object]] = []
    host.analyzer_fleet_service = SimpleNamespace(
        refresh_live_snapshots=lambda **kwargs: refresh_calls.append(dict(kwargs)) or True
    )
    point = CalibrationPoint(index=1, temperature_c=20.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert len(refresh_calls) == 3
    assert {call["reason"] for call in refresh_calls} == {"temperature_wait"}

    context.run_logger.finalize()


def test_temperature_control_service_waits_for_analyzer_chamber_temp_stability(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(temp_c=25.0)
    analyzer = SimpleNamespace(fetch_all=lambda: {"chamber_temp_c": 25.0, "case_temp_c": 26.0})
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=PassingStabilityChecker(),
        chamber=chamber,
        analyzers=[("GA01", analyzer, None)],
        cfg_overrides={
            "workflow.stability.temperature.soak_after_reach_s": 0.0,
            "workflow.stability.temperature.analyzer_chamber_temp_enabled": True,
            "workflow.stability.temperature.analyzer_chamber_temp_window_s": 0.1,
            "workflow.stability.temperature.analyzer_chamber_temp_span_c": 0.05,
            "workflow.stability.temperature.analyzer_chamber_temp_timeout_s": 0.3,
            "workflow.stability.temperature.analyzer_chamber_temp_first_valid_timeout_s": 0.1,
            "workflow.stability.temperature.analyzer_chamber_temp_poll_s": 0.1,
        },
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert run_state.temperature.last_soak_done is True
    assert any("Analyzer chamber temp stable" in message for message in host.logs)

    context.run_logger.finalize()


def test_temperature_control_service_supports_legacy_wait_for_stability_signature(tmp_path: Path) -> None:
    chamber = FakeTemperatureChamber(temp_c=25.0)
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=LegacyStabilityChecker(),
        chamber=chamber,
        cfg_overrides={"workflow.stability.temperature.analyzer_chamber_temp_enabled": False},
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is True
    assert run_state.temperature.ready_target_c == 25.0

    context.run_logger.finalize()


def test_temperature_control_service_fails_when_fallback_start_never_reaches_running(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chamber = FakeTemperatureChamber(
        temp_c=25.0,
        start_error=RuntimeError("START_STATE_MISMATCH"),
        run_state_readouts=[0, 0],
    )
    service, context, run_state, host = _build_service(
        tmp_path,
        stability_checker=PassingStabilityChecker(),
        chamber=chamber,
    )
    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")
    monotonic_values = iter([0.0, 0.0, 11.0])

    monkeypatch.setattr(
        "gas_calibrator.v2.core.services.temperature_control_service.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        "gas_calibrator.v2.core.services.temperature_control_service.time.sleep",
        lambda _: None,
    )

    wait = service.set_temperature_for_point(point, phase="co2")

    assert wait.ok is False
    assert wait.error == "START_STATE_MISMATCH"
    assert wait.diagnostics["stage"] == "start"
    assert wait.diagnostics["fallback_start"] is True
    assert wait.diagnostics["fallback_verified"] is False
    assert chamber.fallback_calls == [("write_register", 8010, 1)]
    assert run_state.temperature.ready_target_c is None
    assert any("Temperature chamber command failed: START_STATE_MISMATCH" in message for message in host.logs)

    context.run_logger.finalize()
