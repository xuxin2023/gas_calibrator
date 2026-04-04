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
from gas_calibrator.v2.core.services import PressureControlService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager
from gas_calibrator.v2.exceptions import WorkflowValidationError
import gas_calibrator.v2.core.services.pressure_control_service as pressure_control_service_module


class FakePressureController:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self.current_pressure_hpa = 1013.25
        self.target_pressure_hpa = 1013.25
        self.in_limits = True
        self.output_enabled = False
        self.vent_open = False

    def enter_atmosphere_mode(self, **kwargs) -> None:
        self.vent_open = True
        self.output_enabled = False
        self.calls.append(("enter_atmosphere_mode", tuple(), dict(kwargs)))

    def exit_atmosphere_mode(self, **kwargs) -> None:
        self.vent_open = False
        self.calls.append(("exit_atmosphere_mode", tuple(), dict(kwargs)))

    def enable_control_output(self) -> None:
        self.output_enabled = True
        self.calls.append(("enable_control_output", tuple(), {}))

    def set_output(self, value: bool) -> None:
        self.output_enabled = bool(value)
        self.calls.append(("set_output", (bool(value),), {}))

    def get_output(self) -> int:
        return 1 if self.output_enabled else 0

    def is_in_atmosphere_mode(self) -> bool:
        return self.vent_open

    def set_setpoint(self, value: float) -> None:
        self.target_pressure_hpa = float(value)
        self.current_pressure_hpa = float(value)
        self.in_limits = True
        self.calls.append(("set_setpoint", (float(value),), {}))

    def get_in_limits(self) -> tuple[float, int]:
        return self.current_pressure_hpa, 1 if self.in_limits else 0

    def close(self) -> None:
        self.calls.append(("close", tuple(), {}))

    def open(self) -> None:
        self.calls.append(("open", tuple(), {}))


class FakePressureGauge:
    def __init__(self, values: list[float]) -> None:
        self.values = [float(value) for value in values]
        self.calls = 0

    def read_pressure(self) -> float:
        if not self.values:
            raise RuntimeError("no gauge readings configured")
        index = min(self.calls, len(self.values) - 1)
        self.calls += 1
        return float(self.values[index])


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})


def _build_service(
    tmp_path: Path,
    *,
    gauge_values: list[float] | None = None,
    startup_precheck_cfg: dict[str, object] | None = None,
) -> tuple[PressureControlService, OrchestrationContext, RunState, SimpleNamespace, FakePressureController]:
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
    controller = FakePressureController()
    gauge = None if gauge_values is None else FakePressureGauge(gauge_values)
    logs: list[str] = []
    snapshots: list[str] = []
    valve_actions: list[tuple[str, object]] = []
    route_traces: list[dict[str, object]] = []
    counters = {"configure": 0}
    config_map = {
        "workflow.pressure.vent_transition_timeout_s": 0.0,
        "workflow.pressure.continuous_atmosphere_hold": True,
        "workflow.pressure.vent_hold_interval_s": 0.0,
        "workflow.pressure.vent_time_s": 0.0,
        "workflow.pressure.stabilize_timeout_s": 0.1,
        "workflow.pressure.restabilize_retries": 0,
        "workflow.pressure.restabilize_retry_interval_s": 0.0,
        "workflow.pressure.soft_recover_on_pressure_timeout": False,
        "workflow.pressure.soft_recover_reopen_delay_s": 0.0,
        "workflow.pressure.pressurize_wait_after_vent_off_s": 0.0,
        "workflow.pressure.co2_post_h2o_vent_off_wait_s": 0.0,
        "workflow.pressure.pressurize_high_hpa": 1100.0,
        "workflow.pressure.pressurize_timeout_s": 0.1,
        "workflow.pressure.post_stable_sample_delay_s": 0.0,
        "workflow.pressure.co2_post_stable_sample_delay_s": 0.0,
        "workflow.pressure_control.setpoint_tolerance_hpa": 0.5,
        "workflow.startup_pressure_precheck": dict(startup_precheck_cfg or {}),
    }

    class Host(SimpleNamespace):
        _h2o_pressure_prepared_target = "sentinel"
        _preseal_dewpoint_snapshot = None
        _active_post_h2o_co2_zero_flush = True

        def _device(self, *names):
            if "pressure_controller" in names:
                return controller
            if gauge is not None and ("pressure_meter" in names or "pressure_gauge" in names):
                return gauge
            return None

        def _cfg_get(self, path: str, default=None):
            return config_map.get(path, default)

        def _call_first(self, device, method_names, *args):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    method(*args)
                    return True
            return False

        def _log(self, message: str):
            logs.append(message)

        def _as_float(self, value):
            if value is None:
                return None
            return float(value)

        def _as_int(self, value):
            if value is None:
                return None
            return int(value)

        def _make_pressure_reader(self):
            return lambda: controller.current_pressure_hpa

        def _check_stop(self):
            if context.stop_event.is_set():
                raise RuntimeError("stop requested")

        def _configure_pressure_controller_in_limits(self):
            counters["configure"] += 1

        def _capture_preseal_dewpoint_snapshot(self):
            snapshots.append("captured")

        def _set_h2o_path(self, is_open, point=None):
            valve_actions.append(("h2o_path", bool(is_open)))

        def _apply_valve_states(self, open_valves):
            valve_actions.append(("apply", list(open_valves)))

        def _collect_only_fast_path_enabled(self):
            return False

        def _set_co2_route_baseline(self, reason=""):
            valve_actions.append(("co2_baseline", reason))

        def _set_valves_for_co2(self, point):
            valve_actions.append(("co2_route", int(point.index)))

        def _cleanup_co2_route(self, reason=""):
            valve_actions.append(("cleanup_co2", reason))

        def _cleanup_h2o_route(self, point, reason=""):
            valve_actions.append(("cleanup_h2o", int(point.index), reason))

        def _co2_source_points(self, points):
            return [point for point in points if point.co2_ppm is not None]

    host = Host(
        logs=logs,
        valve_actions=valve_actions,
        counters=counters,
        snapshots=snapshots,
        route_traces=route_traces,
        _startup_pressure_precheck_result=None,
        status_service=SimpleNamespace(record_route_trace=lambda **kwargs: route_traces.append(dict(kwargs))),
    )
    service = PressureControlService(context, run_state, host=host)
    host._set_pressure_controller_vent = service.set_pressure_controller_vent
    host._enable_pressure_controller_output = service.enable_pressure_controller_output
    return service, context, run_state, host, controller


def test_pressure_control_service_controls_vent_output_and_prepare(tmp_path: Path) -> None:
    service, context, run_state, host, controller = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, pressure_hpa=800.0, route="h2o")

    service.set_pressure_controller_vent(True, reason="baseline")
    service.set_pressure_controller_vent(False, reason="seal")
    service.enable_pressure_controller_output(reason="apply")
    service.prepare_pressure_for_h2o(point)

    assert [call[0] for call in controller.calls[:4]] == [
        "enter_atmosphere_mode",
        "exit_atmosphere_mode",
        "enable_control_output",
        "enter_atmosphere_mode",
    ]
    assert run_state.humidity.h2o_pressure_prepared_target is None
    assert host._h2o_pressure_prepared_target is None
    assert any("vent=ON (baseline)" in message for message in host.logs)
    assert any("output=ON (apply)" in message for message in host.logs)
    assert any("H2O route conditioning" in message for message in host.logs)

    context.run_logger.finalize()


def test_pressure_control_service_handles_in_limits_soft_recover_and_hold(tmp_path: Path) -> None:
    service, context, run_state, host, controller = _build_service(tmp_path)
    point = CalibrationPoint(index=2, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1100.0, route="co2")

    pressure_now, in_limits = service.pressure_reading_and_in_limits(1013.25)
    setpoint = service.set_pressure_to_target(point)
    recovered = service.soft_recover_pressure_controller(reason="unit test")
    hold = service.pressurize_and_hold(point, route="co2")
    after_hold = service.wait_after_pressure_stable_before_sampling(point)

    assert pressure_now == 1013.25
    assert in_limits is True
    assert setpoint.ok is True
    assert setpoint.in_limits is True
    assert setpoint.target_hpa == 1100.0
    assert recovered.ok is True
    assert hold.ok is True
    assert after_hold.ok is True
    assert host.counters["configure"] == 1
    assert host.snapshots == []
    assert host.valve_actions[-1] == ("apply", [])
    assert run_state.humidity.active_post_h2o_co2_zero_flush is False
    assert host._active_post_h2o_co2_zero_flush is False
    assert any("Pressure in-limits at target 1100.0 hPa" in message for message in host.logs)
    assert any("soft recovery complete" in message for message in host.logs)
    assert any("CO2 route sealed for pressure control" in message for message in host.logs)

    context.run_logger.finalize()


def test_pressure_control_service_co2_seal_does_not_require_recovery_to_pressurize_high(tmp_path: Path) -> None:
    service, context, run_state, host, controller = _build_service(tmp_path)
    point = CalibrationPoint(index=3, temperature_c=0.0, co2_ppm=400.0, pressure_hpa=800.0, route="co2")
    controller.current_pressure_hpa = 972.4
    run_state.humidity.active_post_h2o_co2_zero_flush = True
    host._active_post_h2o_co2_zero_flush = True

    hold = service.pressurize_and_hold(point, route="co2")

    assert hold.ok is True
    assert hold.timed_out is False
    assert hold.final_pressure_hpa == 972.4
    assert host.valve_actions[-1] == ("apply", [])
    assert run_state.humidity.active_post_h2o_co2_zero_flush is False
    assert host._active_post_h2o_co2_zero_flush is False
    assert any("vent OFF settle complete" in message for message in host.logs)
    assert any("seal route directly before pressure control" in message for message in host.logs)
    assert any("CO2 route sealed for pressure control" in message for message in host.logs)

    context.run_logger.finalize()


def test_pressure_control_service_co2_seal_triggers_early_on_pressure_gauge_threshold(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service, context, run_state, host, controller = _build_service(tmp_path, gauge_values=[1108.0, 1110.0, 1111.2])
    point = CalibrationPoint(index=30, temperature_c=0.0, co2_ppm=400.0, pressure_hpa=800.0, route="co2")
    original_cfg_get = host._cfg_get
    overrides = {
        "workflow.pressure.pressurize_wait_after_vent_off_s": 5.0,
        "workflow.pressure.co2_post_h2o_vent_off_wait_s": 5.0,
    }
    gauge = host._device("pressure_meter")
    sleeps: list[float] = []
    clock = {"now": 0.0}

    host._cfg_get = lambda path, default=None: overrides.get(path, original_cfg_get(path, default))
    run_state.humidity.active_post_h2o_co2_zero_flush = True
    host._active_post_h2o_co2_zero_flush = True

    monkeypatch.setattr(pressure_control_service_module.time, "time", lambda: clock["now"])

    def _fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["now"] += float(seconds)

    monkeypatch.setattr(pressure_control_service_module.time, "sleep", _fake_sleep)

    hold = service.pressurize_and_hold(point, route="co2")

    assert hold.ok is True
    assert hold.diagnostics["preseal_trigger"] == "pressure_gauge_threshold"
    assert hold.diagnostics["preseal_trigger_pressure_hpa"] == 1110.0
    assert gauge is not None and gauge.calls == 2
    assert sleeps == [0.5]
    assert any("pressure gauge trigger=1110.000 hPa >= 1110.000 hPa" in message for message in host.logs)

    context.run_logger.finalize()


def test_pressure_control_service_co2_seal_falls_back_to_timeout_when_threshold_not_met(
    monkeypatch,
    tmp_path: Path,
) -> None:
    service, context, run_state, host, _controller = _build_service(tmp_path, gauge_values=[1108.0, 1108.6, 1109.1])
    point = CalibrationPoint(index=31, temperature_c=0.0, co2_ppm=400.0, pressure_hpa=800.0, route="co2")
    original_cfg_get = host._cfg_get
    overrides = {
        "workflow.pressure.pressurize_wait_after_vent_off_s": 1.0,
        "workflow.pressure.co2_post_h2o_vent_off_wait_s": 1.0,
    }
    gauge = host._device("pressure_meter")
    sleeps: list[float] = []
    clock = {"now": 0.0}

    host._cfg_get = lambda path, default=None: overrides.get(path, original_cfg_get(path, default))
    run_state.humidity.active_post_h2o_co2_zero_flush = True
    host._active_post_h2o_co2_zero_flush = True

    monkeypatch.setattr(pressure_control_service_module.time, "time", lambda: clock["now"])

    def _fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock["now"] += float(seconds)

    monkeypatch.setattr(pressure_control_service_module.time, "sleep", _fake_sleep)

    hold = service.pressurize_and_hold(point, route="co2")

    assert hold.ok is True
    assert hold.diagnostics["preseal_trigger"] == "timeout"
    assert hold.diagnostics["preseal_trigger_pressure_hpa"] is None
    assert gauge is not None and gauge.calls == 3
    assert sleeps == [0.5, 0.5]
    assert any("trigger timeout=1.000s" in message for message in host.logs)

    context.run_logger.finalize()


def test_startup_pressure_precheck_passes_and_records_route_trace(tmp_path: Path) -> None:
    service, context, _, host, _ = _build_service(
        tmp_path,
        gauge_values=[1000.0, 1000.2, 1000.3],
        startup_precheck_cfg={
            "enabled": True,
            "route": "co2",
            "route_soak_s": 0.0,
            "hold_s": 0.1,
            "sample_interval_s": 0.05,
            "max_abs_drift_hpa": 0.5,
            "prefer_gauge": True,
            "strict": True,
        },
    )
    point = CalibrationPoint(index=4, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")

    result = service.run_startup_pressure_precheck([point])

    assert result.passed is True
    assert any(trace["action"] == "startup_pressure_hold" and trace["result"] == "ok" for trace in host.route_traces)
    assert any(trace["action"] == "startup_pressure_precheck" and trace["result"] == "ok" for trace in host.route_traces)
    assert ("co2_baseline", "before startup pressure precheck") in host.valve_actions
    assert ("co2_route", 4) in host.valve_actions
    assert ("cleanup_co2", "after startup pressure precheck") in host.valve_actions
    assert any("hold result: pass" in message for message in host.logs)
    assert host._startup_pressure_precheck_result is result

    context.run_logger.finalize()


def test_startup_pressure_precheck_strict_failure_raises(tmp_path: Path) -> None:
    service, context, _, host, _ = _build_service(
        tmp_path,
        gauge_values=[1000.0, 1000.0, 1002.0, 1002.5],
        startup_precheck_cfg={
            "enabled": True,
            "route": "co2",
            "route_soak_s": 0.0,
            "hold_s": 0.1,
            "sample_interval_s": 0.05,
            "max_abs_drift_hpa": 0.5,
            "prefer_gauge": True,
            "strict": True,
        },
    )
    point = CalibrationPoint(index=5, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")

    with pytest.raises(WorkflowValidationError):
        service.run_startup_pressure_precheck([point])

    assert any(trace["action"] == "startup_pressure_hold" and trace["result"] == "fail" for trace in host.route_traces)
    assert any(trace["action"] == "startup_pressure_precheck" and trace["result"] == "fail" for trace in host.route_traces)
    assert ("cleanup_co2", "after startup pressure precheck") in host.valve_actions
    assert any("hold result: fail" in message for message in host.logs)
    assert host._startup_pressure_precheck_result is not None
    assert host._startup_pressure_precheck_result.passed is False
    assert host._startup_pressure_precheck_result.error_count == 1

    context.run_logger.finalize()


def test_startup_pressure_precheck_non_strict_failure_warns_and_continues(tmp_path: Path) -> None:
    service, context, _, host, _ = _build_service(
        tmp_path,
        gauge_values=[1000.0, 1000.0, 1002.0, 1002.5],
        startup_precheck_cfg={
            "enabled": True,
            "route": "co2",
            "route_soak_s": 0.0,
            "hold_s": 0.1,
            "sample_interval_s": 0.05,
            "max_abs_drift_hpa": 0.5,
            "prefer_gauge": True,
            "strict": False,
        },
    )
    point = CalibrationPoint(index=6, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")

    service.run_startup_pressure_precheck([point])

    assert any(trace["action"] == "startup_pressure_precheck" and trace["result"] == "warn" for trace in host.route_traces)
    assert any("warning" in message.lower() for message in host.logs)
    assert ("cleanup_co2", "after startup pressure precheck") in host.valve_actions

    context.run_logger.finalize()


def test_pressure_control_service_safe_stop_after_run_returns_controller_to_safe_state(tmp_path: Path) -> None:
    service, context, run_state, host, controller = _build_service(tmp_path)
    controller.output_enabled = True
    controller.vent_open = False
    run_state.humidity.h2o_pressure_prepared_target = 900.0
    run_state.humidity.active_post_h2o_co2_zero_flush = True
    host._h2o_pressure_prepared_target = 900.0
    host._active_post_h2o_co2_zero_flush = True

    summary = service.safe_stop_after_run(reason="final safe stop")

    assert summary["vent_on"] is True
    assert summary["output_enabled"] is False
    assert run_state.humidity.h2o_pressure_prepared_target is None
    assert run_state.humidity.active_post_h2o_co2_zero_flush is False
    assert host._h2o_pressure_prepared_target is None
    assert host._active_post_h2o_co2_zero_flush is False
    assert ("set_output", (False,), {}) in controller.calls
    assert any(call[0] == "enter_atmosphere_mode" for call in controller.calls)
    assert any(trace["action"] == "final_safe_stop_pressure" for trace in host.route_traces)

    context.run_logger.finalize()
