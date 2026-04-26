from __future__ import annotations

from dataclasses import replace
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev
import time
from typing import Any, Callable, Dict, Iterable, Optional

from ..config import AppConfig
from ..export import export_ratio_poly_report
from ..exceptions import StabilityTimeoutError, WorkflowInterruptedError, WorkflowValidationError
from ..qc import QCPipeline
from ..utils import as_float, safe_get
from ...validation.dewpoint_flush_gate import evaluate_dewpoint_flush_gate
from .device_factory import DeviceType
from .device_manager import DeviceManager, DeviceStatus
from .event_bus import EventBus, EventType
from .models import CalibrationPhase, CalibrationPoint, CalibrationStatus, SamplingResult
from .orchestration_context import OrchestrationContext
from .point_parser import PointParser, TemperatureGroup
from .route_context import RouteContext
from .route_planner import RoutePlanner
from .result_store import ResultStore
from .run_state import RunState
from .run_logger import RunLogger
from .session import RunSession
from .services import (
    AIExplanationService,
    AnalyzerFleetService,
    ArtifactService,
    CoefficientService,
    DewpointAlignmentService,
    HumidityGeneratorService,
    PressureControlService,
    PressureWaitResult,
    StartupPressurePrecheckResult,
    QCService,
    SamplingService,
    StatusService,
    TemperatureControlService,
    TimingMonitorService,
    ValveRoutingService,
)
from .stability_checker import StabilityChecker, StabilityType
from .state_manager import StateManager


class WorkflowOrchestrator:
    """Executes workflow business logic for one calibration run."""

    def __init__(
        self,
        *,
        service: Any,
        device_manager: DeviceManager,
        stability_checker: StabilityChecker,
        session: RunSession,
        event_bus: EventBus,
        result_store: ResultStore,
        qc_pipeline: QCPipeline,
        run_logger: RunLogger,
        state_manager: StateManager,
        point_parser: PointParser,
        config: AppConfig,
        stop_event: Any,
        pause_event: Any,
    ) -> None:
        self.service = service
        self.device_manager = device_manager
        self.stability_checker = stability_checker
        self.session = session
        self.event_bus = event_bus
        self.result_store = result_store
        self.qc_pipeline = qc_pipeline
        self.run_logger = run_logger
        self.state_manager = state_manager
        self.point_parser = point_parser
        self.config = config
        self._stop_event = stop_event
        self._pause_event = pause_event
        self.data_writer = self.result_store.data_writer
        self.context = OrchestrationContext(
            config=self.config,
            session=self.session,
            state_manager=self.state_manager,
            event_bus=self.event_bus,
            result_store=self.result_store,
            run_logger=self.run_logger,
            device_manager=self.device_manager,
            stability_checker=self.stability_checker,
            stop_event=self._stop_event,
            pause_event=self._pause_event,
        )
        self.run_state = RunState()
        self.route_planner = RoutePlanner(self.config, self.point_parser)
        self.route_context = RouteContext()
        self._log_callback: Optional[Callable[[str], None]] = None
        self._bind_run_state_aliases()
        self.status_service = StatusService(self.context, self.run_state, host=self)
        self.sampling_service = SamplingService(self.context, self.run_state, host=self)
        self.qc_service = QCService(self.context, self.run_state, host=self)
        self.coefficient_service = CoefficientService(self.context, self.run_state, host=self)
        self.temperature_control_service = TemperatureControlService(self.context, self.run_state, host=self)
        self.analyzer_fleet_service = AnalyzerFleetService(self.context, self.run_state, host=self)
        self.humidity_generator_service = HumidityGeneratorService(self.context, self.run_state, host=self)
        self.pressure_control_service = PressureControlService(self.context, self.run_state, host=self)
        self.valve_routing_service = ValveRoutingService(self.context, self.run_state, host=self)
        self.dewpoint_alignment_service = DewpointAlignmentService(self.context, self.run_state, host=self)
        self.artifact_service = ArtifactService(self.context, self.run_state, host=self)
        self.ai_explanation_service = AIExplanationService(self.context, self.run_state, host=self)
        self.timing_monitor_service = TimingMonitorService(
            self.result_store.run_dir,
            run_id=self.session.run_id,
            enabled=False,
        )
        self._startup_pressure_precheck_result: Optional[StartupPressurePrecheckResult] = None
        self._last_co2_route_dewpoint_gate_summary: Dict[str, Any] = {}

    def set_log_callback(self, callback: Optional[Callable[[str], None]]) -> None:
        self._log_callback = callback

    def _workflow_timing_enabled(self) -> bool:
        raw_cfg = getattr(self.service, "_raw_cfg", None)
        if not isinstance(raw_cfg, dict):
            return False
        policy = raw_cfg.get("run001_a2")
        if not isinstance(policy, dict):
            return False
        scope = str(policy.get("scope") or "").strip()
        return scope == "run001_a2_co2_no_write_pressure_sweep"

    def _workflow_no_write_guard_active(self) -> bool:
        guard = getattr(self.service, "no_write_guard", None)
        if guard is not None:
            return True
        raw_cfg = getattr(self.service, "_raw_cfg", None)
        if not isinstance(raw_cfg, dict):
            return False
        policy = raw_cfg.get("run001_a2")
        if isinstance(policy, dict) and bool(policy.get("no_write")):
            return True
        policy = raw_cfg.get("run001_a1")
        return bool(isinstance(policy, dict) and policy.get("no_write"))

    def _record_workflow_timing(
        self,
        event_name: str,
        event_type: str = "info",
        *,
        stage: str = "",
        point: Optional[CalibrationPoint] = None,
        point_index: Any = None,
        target_pressure_hpa: Any = None,
        duration_s: Any = None,
        expected_max_s: Any = None,
        wait_reason: Any = None,
        blocking_condition: Any = None,
        decision: Any = None,
        pressure_hpa: Any = None,
        chamber_temperature_c: Any = None,
        dewpoint_c: Any = None,
        pace_output_state: Any = None,
        pace_isolation_state: Any = None,
        pace_vent_status: Any = None,
        sample_count: Any = None,
        warning_code: Any = None,
        error_code: Any = None,
    ) -> dict[str, Any]:
        monitor = getattr(self, "timing_monitor_service", None)
        recorder = getattr(monitor, "record_event", None)
        if not callable(recorder):
            return {}
        resolved_point = point or getattr(self.route_context, "active_point", None) or getattr(self.route_context, "current_point", None)
        resolved_point_index = point_index
        if resolved_point_index is None and resolved_point is not None:
            resolved_point_index = getattr(resolved_point, "index", None)
        if target_pressure_hpa is None and resolved_point is not None:
            target_pressure_hpa = getattr(resolved_point, "target_pressure_hpa", None)
        pressure_state = getattr(self.run_state, "pressure", None)
        route_state = {
            "current_route": getattr(self.route_context, "current_route", ""),
            "current_phase": str(getattr(getattr(self.route_context, "current_phase", None), "value", getattr(self.route_context, "current_phase", "")) or ""),
            "point_tag": getattr(self.route_context, "point_tag", ""),
            "retry": getattr(self.route_context, "retry", 0),
            "route_state": dict(getattr(self.route_context, "route_state", {}) or {}),
        }
        return recorder(
            event_name,
            event_type,
            stage=stage,
            point_index=resolved_point_index,
            target_pressure_hpa=target_pressure_hpa,
            duration_s=duration_s,
            expected_max_s=expected_max_s,
            wait_reason=wait_reason,
            blocking_condition=blocking_condition,
            decision=decision,
            route_state=route_state,
            pressure_hpa=pressure_hpa
            if pressure_hpa is not None
            else getattr(pressure_state, "sealed_route_last_controlled_pressure_hpa", None),
            chamber_temperature_c=chamber_temperature_c,
            dewpoint_c=dewpoint_c,
            pace_output_state=pace_output_state,
            pace_isolation_state=pace_isolation_state,
            pace_vent_status=pace_vent_status,
            sample_count=sample_count,
            warning_code=warning_code,
            error_code=error_code,
            no_write_guard_active=self._workflow_no_write_guard_active(),
        )

    def _bind_run_state_aliases(self) -> None:
        self._output_files = self.run_state.artifacts.output_files
        self._cleaned_point_samples = self.run_state.qc.cleaned_point_samples
        self._point_qc_inputs = self.run_state.qc.point_qc_inputs
        self._point_validations = self.run_state.qc.point_validations
        self._run_quality_score = self.run_state.qc.run_quality_score
        self._qc_report = self.run_state.qc.qc_report
        self._disabled_analyzers = self.run_state.analyzers.disabled
        self._disabled_analyzer_reasons = self.run_state.analyzers.disabled_reasons
        self._disabled_analyzer_last_reprobe_ts = self.run_state.analyzers.disabled_last_reprobe_ts
        self._last_live_analyzer_snapshot_ts = self.run_state.analyzers.last_live_snapshot_ts
        self._preseal_dewpoint_snapshot = self.run_state.humidity.preseal_dewpoint_snapshot
        self._h2o_pressure_prepared_target = self.run_state.humidity.h2o_pressure_prepared_target
        self._post_h2o_co2_zero_flush_pending = self.run_state.humidity.post_h2o_co2_zero_flush_pending
        self._initial_co2_zero_flush_pending = self.run_state.humidity.initial_co2_zero_flush_pending
        self._first_co2_route_soak_pending = self.run_state.humidity.first_co2_route_soak_pending
        self._active_post_h2o_co2_zero_flush = self.run_state.humidity.active_post_h2o_co2_zero_flush
        self._last_hgen_target = self.run_state.humidity.last_hgen_target
        self._last_hgen_setpoint_ready = self.run_state.humidity.last_hgen_setpoint_ready
        self._temperature_snapshot_keys = self.run_state.temperature.snapshot_keys
        self._temperature_snapshots = self.run_state.temperature.snapshots
        self._point_timing_contexts = self.run_state.timing.point_contexts
        self._temperature_ready_target_c = self.run_state.temperature.ready_target_c

    def reset_run_state(self) -> None:
        self.run_state.reset(initial_co2_zero_flush_pending=self._route_mode() == "co2_only")
        self.route_context.clear()
        self._bind_run_state_aliases()
        self.result_store._samples.clear()
        self.result_store._point_summaries.clear()
        self.timing_monitor_service = TimingMonitorService(
            self.result_store.run_dir,
            run_id=self.session.run_id,
            no_write_guard_active=self._workflow_no_write_guard_active(),
            enabled=self._workflow_timing_enabled(),
        )

    def get_results(self) -> list[SamplingResult]:
        return self.result_store.get_samples()

    def get_cleaned_results(self, point_index: Optional[int] = None) -> list[SamplingResult]:
        return self.qc_service.get_cleaned_results(point_index)

    def get_output_files(self) -> list[str]:
        return list(self._output_files)

    def run(
        self,
        points: list[CalibrationPoint],
        temperature_groups: Optional[list[TemperatureGroup]] = None,
    ) -> None:
        self._record_workflow_timing("run_start", "start", stage="run")
        self.service._run_initialization()
        self.service._run_precheck()
        self._run_startup_pressure_precheck(points)
        groups = temperature_groups or self.point_parser.group_by_temperature(points)
        for index, group in enumerate(groups):
            self._check_stop()
            next_group = groups[index + 1].points if index + 1 < len(groups) else None
            self.service._run_temperature_group(group.points, next_group=next_group)

    def _run_initialization_impl(self) -> None:
        self._check_stop()
        self._update_status(
            phase=CalibrationPhase.INITIALIZING,
            message="Creating and opening devices",
        )
        self._create_devices()
        results = self.device_manager.open_all()
        expected_disabled = [
            name
            for name, ok in results.items()
            if not ok and self.device_manager.get_status(name) is DeviceStatus.DISABLED
        ]
        if expected_disabled:
            info_message = f"Devices skipped by profile: {', '.join(sorted(expected_disabled))}"
            self._log(info_message)
        failed = [name for name, ok in results.items() if not ok and name not in expected_disabled]
        if failed:
            critical = [name for name in failed if self._is_critical_device(name)]
            self.event_bus.publish(EventType.DEVICE_ERROR, {"failed_devices": failed})
            if critical:
                raise WorkflowValidationError(
                    "Critical device initialization failed",
                    details={"failed_devices": critical},
                )
            warning = f"Device open warnings: {', '.join(failed)}"
            self.session.add_warning(warning)
            self.event_bus.publish(EventType.WARNING_RAISED, {"message": warning, "devices": failed})
            self._log(warning)
        self._update_status(
            phase=CalibrationPhase.INITIALIZING,
            message="Applying analyzer setup",
        )
        self._record_workflow_timing("analyzer_setup_start", "start", stage="analyzer_setup")
        self.analyzer_fleet_service.apply_analyzer_setup()
        self._record_workflow_timing("analyzer_setup_end", "end", stage="analyzer_setup")
        self._update_status(
            phase=CalibrationPhase.INITIALIZING,
            message="Running sensor precheck",
        )
        self._record_workflow_timing("analyzer_precheck_start", "start", stage="analyzer_precheck")
        self.analyzer_fleet_service.run_sensor_precheck()
        self._record_workflow_timing("analyzer_precheck_end", "end", stage="analyzer_precheck")
        self._configure_pressure_controller_in_limits()

    def _run_precheck_impl(self) -> None:
        self._check_stop()
        self._record_workflow_timing("preflight_start", "start", stage="preflight")
        self._update_status(
            phase=CalibrationPhase.PRECHECK,
            message="Running precheck",
        )

        precheck = self.config.workflow.precheck
        if not precheck.enabled:
            self._log("Precheck disabled by configuration")
            self._record_workflow_timing("preflight_end", "end", stage="preflight", decision="skipped")
            return

        if precheck.device_connection:
            health = self.device_manager.health_check()
            expected_disabled = [
                name
                for name, ok in health.items()
                if not ok and self.device_manager.get_status(name) is DeviceStatus.DISABLED
            ]
            if expected_disabled:
                self._log(f"Devices skipped by profile: {', '.join(sorted(expected_disabled))}")
            failing = [name for name, ok in health.items() if not ok and name not in expected_disabled]
            if failing:
                critical = [name for name in failing if self._is_critical_device(name)]
                self.event_bus.publish(EventType.DEVICE_ERROR, {"failed_devices": failing})
                if critical:
                    raise WorkflowValidationError(
                        "Device precheck failed",
                        details={"failed_devices": critical},
                    )
                warning = f"Device precheck warnings: {', '.join(failing)}"
                self.session.add_warning(warning)
                self.event_bus.publish(EventType.WARNING_RAISED, {"message": warning, "devices": failing})
                self._log(warning)

        if precheck.pressure_leak_test:
            self._run_pressure_leak_test()
        if precheck.sensor_check:
            self._run_sensor_check()
        self._record_workflow_timing("preflight_end", "end", stage="preflight", decision="ok")

    def _run_startup_pressure_precheck(self, points: list[CalibrationPoint]) -> None:
        self._check_stop()
        self._update_status(
            phase=CalibrationPhase.PRECHECK,
            message="Running startup pressure precheck",
        )
        self._startup_pressure_precheck_result = self.pressure_control_service.run_startup_pressure_precheck(points)

    def _run_finalization_impl(self) -> None:
        self._update_status(
            phase=CalibrationPhase.FINALIZING,
            current_point=None,
            message="Closing devices",
        )
        self.device_manager.close_all()
        self._export_all_artifacts()

    def _create_devices(self) -> None:
        self._ensure_device("pressure_controller", DeviceType.PRESSURE_CONTROLLER, self.config.devices.pressure_controller)
        self._ensure_device("pressure_meter", DeviceType.PRESSURE_METER, self.config.devices.pressure_meter)
        self._ensure_device("dewpoint_meter", DeviceType.DEWPOINT_METER, self.config.devices.dewpoint_meter)
        self._ensure_device("humidity_generator", DeviceType.HUMIDITY_GENERATOR, self.config.devices.humidity_generator)
        self._ensure_device("temperature_chamber", DeviceType.TEMPERATURE_CHAMBER, self.config.devices.temperature_chamber)
        self._ensure_device("relay_a", DeviceType.RELAY, self.config.devices.relay_a)
        self._ensure_device("relay_b", DeviceType.RELAY, self.config.devices.relay_b)
        for index, config in enumerate(self.config.devices.gas_analyzers):
            self._ensure_device(f"gas_analyzer_{index}", DeviceType.GAS_ANALYZER, config)

    def _run_pressure_leak_test(self) -> None:
        reader = self._make_pressure_reader()
        if reader is None:
            self._log("Pressure leak test skipped: no pressure device")
            return
        start_pressure = as_float(reader())
        time.sleep(0.05)
        end_pressure = as_float(reader())
        if start_pressure is None or end_pressure is None:
            raise WorkflowValidationError("Pressure leak test failed: unable to read pressure")
        tolerance = max(0.5, float(self.config.workflow.pressure_control.setpoint_tolerance_hpa))
        if abs(end_pressure - start_pressure) > tolerance:
            raise WorkflowValidationError(
                "Pressure leak test failed",
                details={
                    "start_pressure_hpa": start_pressure,
                    "end_pressure_hpa": end_pressure,
                    "tolerance_hpa": tolerance,
                },
            )

    def _run_sensor_check(self) -> None:
        analyzers = self._collect_analyzers()
        if not analyzers:
            self._log("Sensor check skipped: no gas analyzers")
            return
        for analyzer_id, analyzer in analyzers:
            snapshot = self._normalize_snapshot(self._read_device_snapshot(analyzer))
            if not snapshot:
                raise WorkflowValidationError(
                    "Sensor check failed",
                    details={"analyzer_id": analyzer_id},
                )

    def _ensure_device(self, name: str, device_type: DeviceType, config: Any) -> None:
        if config is None:
            return
        if self.device_manager.get_device(name) is not None:
            return
        self.device_manager.create_device(name, device_type, config)

    def _is_critical_device(self, name: str) -> bool:
        return name == "temperature_chamber" or name.startswith("gas_analyzer_")

    def _set_pressure_target(self, pressure_hpa: Optional[float]) -> None:
        if pressure_hpa is None:
            return
        controller = self.device_manager.get_device("pressure_controller")
        if controller is not None:
            self._call_first(controller, ("set_pressure_hpa", "set_pressure", "set_setpoint"), pressure_hpa)

    def _switch_co2_route(self, point: CalibrationPoint) -> None:
        for relay_name in ("relay_a", "relay_b"):
            relay = self.device_manager.get_device(relay_name)
            if relay is not None:
                route_value = point.co2_ppm if point.co2_ppm is not None else point.route
                self._call_first(relay, ("select_route", "set_route", "switch_route"), route_value)

    def _check_stop(self) -> None:
        self.status_service.check_stop()

    def _append_result(self, result: SamplingResult) -> None:
        self.result_store.save_sample(result)

    def _mark_point_completed(
        self,
        point: CalibrationPoint,
        *,
        point_tag: str = "",
        stability_time_s: Optional[float] = None,
        total_time_s: Optional[float] = None,
    ) -> None:
        self.status_service.mark_point_completed(
            point,
            point_tag=point_tag,
            stability_time_s=stability_time_s,
            total_time_s=total_time_s,
        )

    def _update_status(
        self,
        *,
        phase: Optional[CalibrationPhase] = None,
        current_point: Optional[CalibrationPoint] = None,
        message: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        self.status_service.update_status(
            phase=phase,
            current_point=current_point,
            message=message,
            error=error,
        )

    def _log(self, message: str) -> None:
        self.status_service.log(message)

    def _collect_analyzers(self) -> list[tuple[str, Any]]:
        return list(self.device_manager.get_devices_by_type(DeviceType.GAS_ANALYZER).items())

    def _collect_sampling_result(
        self,
        point: CalibrationPoint,
        analyzer_id: str,
        analyzer: Any,
        *,
        phase: str = "",
        point_tag: str = "",
    ) -> SamplingResult:
        return self.sampling_service.collect_sampling_result(
            point,
            analyzer_id,
            analyzer,
            phase=phase,
            point_tag=point_tag,
        )

    def _read_temperature_for_sampling(self, snapshot: Dict[str, Any]) -> Optional[float]:
        return self.sampling_service.read_temperature_for_sampling(snapshot)

    def _read_pressure_for_sampling(self, snapshot: Dict[str, Any]) -> Optional[float]:
        return self.sampling_service.read_pressure_for_sampling(snapshot)

    def _read_dew_point_for_sampling(self, snapshot: Dict[str, Any]) -> Optional[float]:
        return self.sampling_service.read_dew_point_for_sampling(snapshot)

    def _make_temperature_reader(self, chamber: Any) -> Optional[Callable[[], Optional[float]]]:
        return self.sampling_service.make_temperature_reader(chamber)

    def _make_humidity_reader(self, humidity_generator: Any) -> Optional[Callable[[], Optional[float]]]:
        return self.sampling_service.make_humidity_reader(humidity_generator)

    def _make_pressure_reader(self) -> Optional[Callable[[], Optional[float]]]:
        return self.sampling_service.make_pressure_reader()

    def _make_signal_reader(self, analyzer: Any) -> Optional[Callable[[], Optional[float]]]:
        return self.sampling_service.make_signal_reader(analyzer)

    @staticmethod
    def _first_method(device: Any, method_names: Iterable[str]) -> Optional[Callable[..., Any]]:
        for method_name in method_names:
            method = getattr(device, method_name, None)
            if callable(method):
                return method
        return None

    def _call_first(self, device: Any, method_names: Iterable[str], *args: Any) -> bool:
        method = self._first_method(device, method_names)
        if method is None:
            return False
        device_name = getattr(device, "__class__", type(device)).__name__
        self.run_logger.log_io(device_name, "TX", f"{method.__name__}({', '.join(str(arg) for arg in args)})")
        method(*args)
        self.run_logger.log_io(device_name, "RX", "ok")
        return True

    def _read_device_snapshot(self, device: Any) -> Any:
        return self.sampling_service.read_device_snapshot(device)

    @staticmethod
    def _normalize_snapshot(snapshot: Any) -> Dict[str, Any]:
        return SamplingService.normalize_snapshot(snapshot)

    @staticmethod
    def _pick_numeric(snapshot: Dict[str, Any], *keys: str) -> Optional[float]:
        return SamplingService.pick_numeric(snapshot, *keys)

    @classmethod
    def _pick_humidity_value(cls, snapshot: Dict[str, Any]) -> Optional[float]:
        return SamplingService.pick_humidity_value(snapshot)

    @staticmethod
    def _sanitize_humidity_value(value: Optional[float]) -> Optional[float]:
        return SamplingService.sanitize_humidity_value(value)

    @staticmethod
    def _sampling_result_to_row(result: SamplingResult) -> Dict[str, Any]:
        return SamplingService.sampling_result_to_row(result)

    def _remember_output_file(self, path: str) -> None:
        self.status_service.remember_output_file(path)

    def _samples_for_point(
        self,
        point: CalibrationPoint,
        *,
        phase: str = "",
        point_tag: str = "",
    ) -> list[SamplingResult]:
        return self.sampling_service.samples_for_point(point, phase=phase, point_tag=point_tag)

    def _run_point_qc(
        self,
        point: CalibrationPoint,
        *,
        phase: str = "",
        point_tag: str = "",
    ) -> None:
        self.qc_service.run_point_qc(point, phase=phase, point_tag=point_tag)

    def _export_qc_report(self) -> dict[str, str]:
        return self.qc_service.export_qc_report()

    def generate_ai_anomaly_report(self, advisor: Any) -> str:
        return self.ai_explanation_service.generate_ai_anomaly_report(advisor)

    def generate_ai_run_summary(self, summarizer: Any, *, anomaly_diagnosis: str = "") -> str:
        return self.ai_explanation_service.generate_ai_run_summary(
            summarizer,
            anomaly_diagnosis=anomaly_diagnosis,
        )

    def _generate_ai_outputs(self) -> None:
        self.ai_explanation_service.generate_ai_outputs()

    def _export_summary(
        self,
        session: RunSession,
        *,
        current_status: Optional[CalibrationStatus] = None,
        remember: bool = False,
    ) -> Path:
        return self.artifact_service.export_summary(
            session,
            current_status=current_status,
            remember=remember,
            startup_pressure_precheck=self._startup_pressure_precheck_payload(),
        )

    def _export_manifest(
        self,
        session: RunSession,
        *,
        source_points_file: Optional[str | Path] = None,
        remember: bool = True,
    ) -> Path:
        return self.artifact_service.export_manifest(
            session,
            source_points_file=source_points_file,
            remember=remember,
            startup_pressure_precheck=self._startup_pressure_precheck_payload(),
        )

    def _export_all_artifacts(self) -> None:
        self.artifact_service.export_all_artifacts()

    def _startup_pressure_precheck_payload(self) -> Optional[dict[str, Any]]:
        if self._startup_pressure_precheck_result is None:
            return None
        result = self._startup_pressure_precheck_result
        return {
            "passed": result.passed,
            "route": result.route,
            "point_index": result.point_index,
            "target_pressure_hpa": result.target_pressure_hpa,
            "warning_count": result.warning_count,
            "error_count": result.error_count,
            "details": dict(result.details),
            "error": result.error,
        }

    def _sync_results_to_storage(self) -> None:
        self.artifact_service.sync_results_to_storage()

    def _export_coefficient_report(self) -> dict[str, str]:
        return self.coefficient_service.export_coefficient_report()

    def _anomaly_alarm_payload(self) -> list[dict[str, Any]]:
        alarms: list[dict[str, Any]] = []
        for message in list(self.session.warnings):
            alarms.append({"severity": "warning", "category": self._alarm_category(message), "message": message})
        for message in list(self.session.errors):
            alarms.append({"severity": "error", "category": self._alarm_category(message), "message": message})
        return alarms

    def _anomaly_device_events(self) -> list[dict[str, Any]]:
        path = self.run_logger.io_log_path
        if not path.exists():
            return []
        events: list[dict[str, Any]] = []
        try:
            import csv

            with path.open("r", encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    events.append(
                        {
                            "timestamp": row.get("timestamp"),
                            "device": row.get("device"),
                            "direction": row.get("direction"),
                        }
                    )
        except Exception:
            return []
        return events

    @staticmethod
    def _alarm_category(message: str) -> str:
        text = str(message or "").lower()
        if "humidity" in text or "dew" in text or "h2o" in text:
            return "humidity"
        if "pressure" in text or "leak" in text:
            return "pressure"
        if "commun" in text or "serial" in text or "frame" in text:
            return "communication"
        if "qc" in text or "outlier" in text or "stability" in text:
            return "qc"
        return "general"

    def _cfg_root(self) -> Any:
        return getattr(self.service, "_raw_cfg", None) or self.config

    def _cfg_get(self, path: str, default: Any = None) -> Any:
        node: Any = self._cfg_root()
        for part in str(path or "").split("."):
            if not part:
                continue
            if node is None:
                return default
            if isinstance(node, dict):
                node = node.get(part)
            else:
                node = getattr(node, part, None)
        return default if node is None else node

    def _collect_only_fast_path_enabled(self) -> bool:
        if not self._collect_only_mode():
            return False
        return bool(self._cfg_get("workflow.collect_only_fast_path", True))

    def _timing_key(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> str:
        phase_text = str(phase or point.route or "").strip().lower()
        tag_text = str(point_tag or "").strip()
        if tag_text:
            return f"{phase_text}:{tag_text}"
        return (
            f"{phase_text}:{int(point.index)}:{self._as_float(point.co2_ppm)}:"
            f"{self._as_float(point.hgen_rh_pct)}:{self._as_float(point.target_pressure_hpa)}"
        )

    def _begin_point_timing(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> None:
        self.status_service.begin_point_timing(point, phase=phase, point_tag=point_tag)

    def _mark_point_stable_for_sampling(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> None:
        self.status_service.mark_point_stable_for_sampling(point, phase=phase, point_tag=point_tag)

    def _finish_point_timing(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> dict[str, Any]:
        return self.status_service.finish_point_timing(point, phase=phase, point_tag=point_tag)

    def _point_timing(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> dict[str, Any]:
        return dict(self._point_timing_contexts.get(self._timing_key(point, phase=phase, point_tag=point_tag)) or {})

    def _clear_point_timing(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> None:
        self.status_service.clear_point_timing(point, phase=phase, point_tag=point_tag)

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        return as_float(value)

    @staticmethod
    def _as_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            try:
                return int(float(value))
            except Exception:
                return None

    def _device(self, *names: str) -> Any:
        for name in names:
            device = self.device_manager.get_device(name)
            if device is not None:
                return device
        return None

    def _all_gas_analyzers(self) -> list[tuple[str, Any, Any]]:
        return self.analyzer_fleet_service.all_gas_analyzers()

    def _active_gas_analyzers(self) -> list[tuple[str, Any, Any]]:
        return self.analyzer_fleet_service.active_gas_analyzers()

    def _disable_analyzers(self, labels: list[str], reason: str) -> None:
        self.analyzer_fleet_service.disable_analyzers(labels, reason)

    def _analyzer_reprobe_cooldown_s(self) -> float:
        return self.analyzer_fleet_service.analyzer_reprobe_cooldown_s()

    def _gas_analyzer_runtime_settings(self, cfg: Any) -> dict[str, Any]:
        return self.analyzer_fleet_service.gas_analyzer_runtime_settings(cfg)

    def _configure_gas_analyzer(self, analyzer: Any, *, label: str, cfg: Any) -> None:
        self.analyzer_fleet_service.configure_gas_analyzer(analyzer, label=label, cfg=cfg)

    def _attempt_reenable_disabled_analyzers(self) -> None:
        self.analyzer_fleet_service.attempt_reenable_disabled_analyzers()

    def _configure_pressure_controller_in_limits(self) -> None:
        controller = self._device("pressure_controller")
        if controller is None:
            return
        try:
            pct = float(self._cfg_get("devices.pressure_controller.in_limits_pct", 0.02))
            time_s = float(self._cfg_get("devices.pressure_controller.in_limits_time_s", 10.0))
            self._call_first(controller, ("set_in_limits",), pct, time_s)
        except Exception as exc:
            self._log(f"Pressure controller in-limits setup failed: {exc}")

    def _route_mode(self) -> str:
        return self.route_planner.route_mode()

    def _collect_only_mode(self) -> bool:
        return bool(self._cfg_get("workflow.collect_only", False))

    def _co2_skip_ppm_set(self) -> set[int]:
        raw = self._cfg_get("workflow.skip_co2_ppm", [])
        if not isinstance(raw, list):
            raw = [raw]
        out: set[int] = set()
        for item in raw:
            value = self._as_int(item)
            if value is not None:
                out.add(value)
        return out

    def _group_h2o_points(self, points: list[CalibrationPoint]) -> list[list[CalibrationPoint]]:
        return self.route_planner.group_h2o_points(points)

    def _pressure_reference_points(self, points: list[CalibrationPoint]) -> list[CalibrationPoint]:
        out: list[CalibrationPoint] = []
        seen: set[float] = set()
        for point in points:
            pressure = self._as_float(point.target_pressure_hpa)
            if pressure is None:
                continue
            key = float(pressure)
            if key in seen:
                continue
            seen.add(key)
            out.append(point)
        out.sort(key=lambda item: float(self._as_float(item.target_pressure_hpa) or 0.0), reverse=True)
        return out

    def _h2o_pressure_points_for_temperature(self, points: list[CalibrationPoint]) -> list[CalibrationPoint]:
        return self.route_planner.h2o_pressure_points(points)

    def _co2_pressure_points_for_temperature(self, points: list[CalibrationPoint]) -> list[CalibrationPoint]:
        return self.route_planner.co2_pressure_points(None, points)

    def _co2_source_points(self, points: list[CalibrationPoint]) -> list[CalibrationPoint]:
        return self.route_planner.co2_sources(points)

    def _build_co2_pressure_point(self, source_point: CalibrationPoint, pressure_point: CalibrationPoint) -> CalibrationPoint:
        return self.route_planner.build_co2_pressure_point(source_point, pressure_point)

    def _build_h2o_pressure_point(self, source_point: CalibrationPoint, pressure_point: CalibrationPoint) -> CalibrationPoint:
        return self.route_planner.build_h2o_pressure_point(source_point, pressure_point)

    def _co2_point_tag(self, point: CalibrationPoint) -> str:
        return self.route_planner.co2_point_tag(point)

    def _h2o_point_tag(self, point: CalibrationPoint) -> str:
        return self.route_planner.h2o_point_tag(point)

    def _set_temperature_for_point(self, point: CalibrationPoint, *, phase: str) -> bool:
        return self.temperature_control_service.set_temperature_for_point(point, phase=phase).ok

    def _capture_temperature_calibration_snapshot(self, point: CalibrationPoint, *, route_type: str) -> bool:
        return self.temperature_control_service.capture_temperature_calibration_snapshot(point, route_type=route_type)

    def _export_temperature_snapshots(self) -> dict[str, str]:
        return self.temperature_control_service.export_temperature_snapshots()

    def _prepare_humidity_generator(self, point: CalibrationPoint) -> None:
        self.humidity_generator_service.prepare_humidity_generator(point)

    def _read_humidity_generator_temp_rh(self) -> tuple[Optional[float], Optional[float]]:
        return self.humidity_generator_service.read_humidity_generator_temp_rh()

    def _wait_humidity_generator_stable(self, point: CalibrationPoint) -> bool:
        return self.humidity_generator_service.wait_humidity_generator_stable(point).ok
        generator = self._device("humidity_generator")
        if generator is None:
            return True
        if self._collect_only_fast_path_enabled():
            self._log("Collect-only mode: humidity-generator wait skipped")
            return True
        target_temp = self._as_float(point.hgen_temp_c)
        target_rh = self._as_float(point.hgen_rh_pct)
        if target_temp is None and target_rh is None:
            return True
        if (
            self._last_hgen_target == (target_temp, target_rh)
            and self._last_hgen_setpoint_ready
        ):
            self._log("Humidity generator setpoint already ready for current target, skip wait")
            return True

        cfg = self._cfg_get("workflow.stability.humidity_generator", {})
        if isinstance(cfg, dict) and not cfg.get("enabled", True):
            return True

        temp_tol = float(self._cfg_get("workflow.stability.humidity_generator.temp_tol_c", 1.0))
        rh_tol = float(self._cfg_get("workflow.stability.humidity_generator.rh_tol_pct", 1.0))
        window_s = float(
            self._cfg_get(
                "workflow.stability.humidity_generator.rh_stable_window_s",
                self._cfg_get("workflow.stability.humidity_generator.window_s", 60.0),
            )
        )
        span_tol = float(self._cfg_get("workflow.stability.humidity_generator.rh_stable_span_pct", 0.3))
        timeout_raw = float(self._cfg_get("workflow.stability.humidity_generator.timeout_s", 1800.0))
        timeout_s: Optional[float] = timeout_raw if timeout_raw > 0 else None
        poll_s = max(0.1, float(self._cfg_get("workflow.stability.humidity_generator.poll_s", 1.0)))
        start = time.time()
        last_report = 0.0
        in_band_since: Optional[float] = None
        rh_samples: list[tuple[float, float]] = []
        if timeout_s is None:
            self._log("Humidity generator wait timeout disabled; waiting until RH stabilizes")
        while True:
            self._check_stop()
            if timeout_s is not None and (time.time() - start) >= timeout_s:
                break
            temp_now, rh_now = self._read_humidity_generator_temp_rh()
            temp_ok = target_temp is None or (temp_now is not None and abs(temp_now - target_temp) <= temp_tol)
            rh_ok = target_rh is None or (rh_now is not None and abs(rh_now - target_rh) <= rh_tol)
            if temp_ok and rh_ok:
                now = time.time()
                if in_band_since is None:
                    in_band_since = now
                    rh_samples = []
                if rh_now is not None:
                    rh_samples.append((now, float(rh_now)))
                    rh_samples = [(ts, value) for ts, value in rh_samples if now - ts <= window_s]
                if (
                    in_band_since is not None
                    and (now - in_band_since) >= window_s
                    and rh_samples
                    and self._span([value for _, value in rh_samples]) < span_tol
                ):
                    span = self._span([value for _, value in rh_samples])
                    self._log(
                        f"Humidity generator reached setpoint: temp={temp_now}C target={target_temp} "
                        f"rh={rh_now}% target={target_rh} span={span:.3f} window={int(window_s)}s"
                    )
                    self._last_hgen_setpoint_ready = True
                    return True
            else:
                if in_band_since is not None:
                    self._log(
                        f"Humidity left target band: temp={temp_now}C/{target_temp} tol=±{temp_tol} "
                        f"rh={rh_now}%/{target_rh} tol=±{rh_tol}; reset stability window"
                    )
                in_band_since = None
                rh_samples = []
            if time.time() - last_report >= 30.0:
                last_report = time.time()
                if in_band_since is None or not rh_samples:
                    self._log(
                        f"Humidity settling... temp={temp_now}C/{target_temp} rh={rh_now}%/{target_rh} "
                        f"window=0/{int(window_s)}s"
                    )
                else:
                    remain = max(0.0, window_s - (time.time() - in_band_since))
                    span = self._span([value for _, value in rh_samples])
                    self._log(
                        f"Humidity in target band, observing stability... temp={temp_now}C/{target_temp} "
                        f"rh={rh_now}%/{target_rh} span={span:.3f} remaining={int(remain)}s"
                    )
            time.sleep(poll_s)
        self._log("Humidity generator reach-setpoint timeout")
        return False

    def _ensure_dewpoint_meter_ready(self) -> bool:
        return self.dewpoint_alignment_service.ensure_dewpoint_meter_ready()

    def _wait_h2o_route_soak_before_seal(self, point: CalibrationPoint) -> bool:
        return self.dewpoint_alignment_service.wait_h2o_route_soak_before_seal(point)

    def _wait_dewpoint_alignment_stable(self, point: Optional[CalibrationPoint] = None) -> bool:
        return self.dewpoint_alignment_service.wait_dewpoint_alignment_stable(point)

    def _capture_preseal_dewpoint_snapshot(self) -> None:
        self.dewpoint_alignment_service.capture_preseal_dewpoint_snapshot()

    def _open_h2o_route_and_wait_ready(self, point: CalibrationPoint) -> bool:
        return self.dewpoint_alignment_service.open_h2o_route_and_wait_ready(point)

    def _has_special_co2_zero_flush_pending(self) -> bool:
        return bool(self._post_h2o_co2_zero_flush_pending or self._initial_co2_zero_flush_pending)

    def _is_zero_co2_point(self, point: CalibrationPoint) -> bool:
        ppm = self._as_int(point.co2_ppm)
        zero_values = self._cfg_get("workflow.stability.co2_route.post_h2o_zero_ppm_values", [0])
        if not isinstance(zero_values, list):
            zero_values = [zero_values]
        normalized = {self._as_int(value) for value in zero_values}
        return ppm is not None and ppm in normalized

    def _gas_route_dewpoint_gate_enabled(self) -> bool:
        return bool(self._cfg_get("workflow.stability.gas_route_dewpoint_gate_enabled", False))

    def _gas_route_dewpoint_gate_cfg(self) -> Dict[str, Any]:
        return {
            "enabled": self._gas_route_dewpoint_gate_enabled(),
            "window_s": max(5.0, float(self._cfg_get("workflow.stability.gas_route_dewpoint_gate_window_s", 60.0) or 60.0)),
            "max_total_wait_s": max(
                0.0,
                float(self._cfg_get("workflow.stability.gas_route_dewpoint_gate_max_total_wait_s", 300.0) or 300.0),
            ),
            "poll_s": max(0.2, float(self._cfg_get("workflow.stability.gas_route_dewpoint_gate_poll_s", 2.0) or 2.0)),
            "tail_span_max_c": max(
                0.0,
                float(self._cfg_get("workflow.stability.gas_route_dewpoint_gate_tail_span_max_c", 0.35) or 0.35),
            ),
            "tail_slope_abs_max_c_per_s": max(
                0.0,
                float(
                    self._cfg_get("workflow.stability.gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s", 0.003)
                    or 0.003
                ),
            ),
            "rebound_window_s": max(
                1.0,
                float(self._cfg_get("workflow.stability.gas_route_dewpoint_gate_rebound_window_s", 180.0) or 180.0),
            ),
            "rebound_min_rise_c": max(
                0.0,
                float(self._cfg_get("workflow.stability.gas_route_dewpoint_gate_rebound_min_rise_c", 1.0) or 1.0),
            ),
            "log_interval_s": max(
                1.0,
                float(self._cfg_get("workflow.stability.gas_route_dewpoint_gate_log_interval_s", 15.0) or 15.0),
            ),
        }

    def _read_precondition_dewpoint_gate_snapshot(self) -> Dict[str, Any]:
        dewpoint = self._device("dewpoint_meter")
        if dewpoint is None:
            raise RuntimeError("dewpoint_meter_unavailable")
        snapshot = self._normalize_snapshot(
            self.dewpoint_alignment_service._read_dewpoint_snapshot(  # type: ignore[attr-defined]
                dewpoint,
                context="co2 route dewpoint gate",
                log_failures=False,
            )
        )
        dewpoint_c = self._as_float(snapshot.get("dewpoint_c"))
        if dewpoint_c is None:
            raise RuntimeError("dewpoint_gate_read_missing")
        return {
            "dewpoint_c": dewpoint_c,
            "temp_c": self._as_float(snapshot.get("temp_c")),
            "rh_pct": self._as_float(snapshot.get("rh_pct")),
        }

    def _wait_co2_route_dewpoint_gate_before_seal(
        self,
        point: CalibrationPoint,
        *,
        base_soak_s: float,
        log_context: str,
    ) -> bool:
        self._last_co2_route_dewpoint_gate_summary = {}
        cfg = self._gas_route_dewpoint_gate_cfg()
        if not bool(cfg.get("enabled")):
            return True

        gate_begin_ts = time.time()
        gate_rows: list[dict[str, Any]] = []
        last_log_ts = 0.0
        while True:
            self._check_stop()
            try:
                snapshot = self._read_precondition_dewpoint_gate_snapshot()
            except Exception as exc:
                reason = str(exc) or "dewpoint_gate_read_failed"
                total_elapsed_s = float(base_soak_s) + max(0.0, time.time() - gate_begin_ts)
                self._last_co2_route_dewpoint_gate_summary = {
                    "dewpoint_time_to_gate": round(total_elapsed_s, 3),
                    "dewpoint_tail_span_60s": None,
                    "dewpoint_tail_slope_60s": None,
                    "dewpoint_rebound_detected": None,
                    "flush_gate_status": "fail",
                    "flush_gate_reason": reason,
                }
                self._log(
                    "CO2 route precondition dewpoint gate failed before seal: "
                    f"row={point.index} reason={reason}"
                )
                return False
            total_elapsed_s = float(base_soak_s) + max(0.0, time.time() - gate_begin_ts)
            gate_rows.append(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                    "phase_elapsed_s": max(0.0, float(total_elapsed_s)),
                    "phase": "co2_route_precondition",
                    "controller_vent_state": "VENT_ON",
                    "dewpoint_c": snapshot.get("dewpoint_c"),
                    "dewpoint_temp_c": snapshot.get("temp_c"),
                    "dewpoint_rh_percent": snapshot.get("rh_pct"),
                }
            )
            gate_eval = evaluate_dewpoint_flush_gate(
                gate_rows,
                min_flush_s=float(base_soak_s),
                gate_window_s=float(cfg["window_s"]),
                max_tail_span_c=float(cfg["tail_span_max_c"]),
                max_abs_tail_slope_c_per_s=float(cfg["tail_slope_abs_max_c_per_s"]),
                rebound_window_s=float(cfg["rebound_window_s"]),
                rebound_min_rise_c=float(cfg["rebound_min_rise_c"]),
                include_rebound_in_gate=True,
            )
            summary = {
                "dewpoint_time_to_gate": self._as_float(gate_eval.get("dewpoint_time_to_gate")),
                "dewpoint_tail_span_60s": self._as_float(gate_eval.get("dewpoint_tail_span_60s")),
                "dewpoint_tail_slope_60s": self._as_float(gate_eval.get("dewpoint_tail_slope_60s")),
                "dewpoint_rebound_detected": bool(gate_eval.get("dewpoint_rebound_detected")),
                "flush_gate_status": "pass" if bool(gate_eval.get("gate_pass")) else "waiting",
                "flush_gate_reason": str(gate_eval.get("gate_reason") or ""),
            }
            self._last_co2_route_dewpoint_gate_summary = summary
            if bool(gate_eval.get("gate_pass")):
                self._log(
                    "CO2 route dewpoint gate passed after fixed precondition: "
                    f"row={point.index} time_to_gate={summary['dewpoint_time_to_gate']} "
                    f"tail_span_60s={summary['dewpoint_tail_span_60s']} "
                    f"tail_slope_60s={summary['dewpoint_tail_slope_60s']} "
                    f"rebound={summary['dewpoint_rebound_detected']}"
                )
                return True
            max_total_wait_s = float(cfg["max_total_wait_s"])
            if max_total_wait_s > 0 and total_elapsed_s >= max_total_wait_s:
                reason = summary["flush_gate_reason"]
                if "max_total_wait_exceeded" not in reason:
                    reason = f"{reason};max_total_wait_exceeded" if reason else "max_total_wait_exceeded"
                summary["flush_gate_status"] = "timeout"
                summary["flush_gate_reason"] = reason
                summary["dewpoint_time_to_gate"] = round(total_elapsed_s, 3)
                self._last_co2_route_dewpoint_gate_summary = summary
                self._log(
                    "CO2 route precondition failed: dewpoint gate timeout after fixed purge; "
                    f"row={point.index} total_wait_s={total_elapsed_s:.1f} reason={reason}"
                )
                return False
            now_ts = time.time()
            if (now_ts - last_log_ts) >= float(cfg["log_interval_s"]):
                last_log_ts = now_ts
                self._log(
                    "CO2 route precondition dewpoint gate waiting: "
                    f"row={point.index} dewpoint={snapshot.get('dewpoint_c')} "
                    f"time_to_gate={total_elapsed_s:.1f}s "
                    f"tail_span_60s={summary['dewpoint_tail_span_60s']} "
                    f"tail_slope_60s={summary['dewpoint_tail_slope_60s']} "
                    f"reason={summary['flush_gate_reason'] or 'waiting'}"
                )
            time.sleep(float(cfg["poll_s"]))

    def _wait_co2_route_soak_before_seal(self, point: CalibrationPoint) -> bool:
        self._last_co2_route_dewpoint_gate_summary = {}
        if self._collect_only_fast_path_enabled():
            self._log("Collect-only mode: CO2 route pre-seal soak skipped")
            self._record_workflow_timing(
                "preseal_soak_end",
                "warning",
                stage="preseal_soak",
                point=point,
                decision="collect_only_skipped",
            )
            return True
        special_flush = self._has_special_co2_zero_flush_pending() and self._is_zero_co2_point(point)
        soak_key = "workflow.stability.co2_route.preseal_soak_s"
        soak_default = 180.0
        log_context = "CO2 route opened"
        if special_flush:
            soak_key = "workflow.stability.co2_route.post_h2o_zero_ppm_soak_s"
            soak_default = 600.0
            if self._post_h2o_co2_zero_flush_pending:
                log_context = "CO2 route opened after H2O; zero-gas flush"
            else:
                log_context = "CO2 route opened for first zero-gas flush"
            self._active_post_h2o_co2_zero_flush = True
        elif self._first_co2_route_soak_pending:
            soak_key = "workflow.stability.co2_route.first_point_preseal_soak_s"
            soak_default = 300.0
            log_context = "CO2 route opened for first gas-point flush"
        else:
            self._active_post_h2o_co2_zero_flush = False
        soak_s = float(self._cfg_get(soak_key, soak_default))
        if soak_s <= 0:
            self.run_state.humidity.first_co2_route_soak_pending = False
            self._first_co2_route_soak_pending = False
            self._record_workflow_timing(
                "preseal_soak_end",
                "warning",
                stage="preseal_soak",
                point=point,
                decision="disabled",
            )
            return True
        self._log(f"{log_context}; wait {int(soak_s)}s before pressure sealing (row {point.index})")
        self._record_workflow_timing(
            "preseal_soak_start",
            "start",
            stage="preseal_soak",
            point=point,
            expected_max_s=soak_s + max(5.0, soak_s * 0.1),
            wait_reason=soak_key,
        )
        start = time.time()
        continuous_atmosphere_hold = bool(self._cfg_get("workflow.pressure.continuous_atmosphere_hold", False))
        vent_hold_interval_s = max(0.1, float(self._cfg_get("workflow.pressure.vent_hold_interval_s", 2.0)))
        last_atmosphere_hold_ts = 0.0
        while time.time() - start < soak_s:
            self._check_stop()
            now = time.time()
            self._record_workflow_timing(
                "preseal_soak_tick",
                "tick",
                stage="preseal_soak",
                point=point,
                duration_s=now - start,
                expected_max_s=soak_s,
                wait_reason=soak_key,
            )
            if continuous_atmosphere_hold and (now - last_atmosphere_hold_ts) >= vent_hold_interval_s:
                self._set_pressure_controller_vent(
                    True,
                    reason="CO2 route pre-seal atmosphere hold",
                    wait_after_command=False,
                )
                self._record_workflow_timing(
                    "preseal_vent_hold_tick",
                    "tick",
                    stage="preseal_soak",
                    point=point,
                    duration_s=now - start,
                    expected_max_s=vent_hold_interval_s,
                    wait_reason="continuous_atmosphere_hold",
                )
                pressure_decision = self._verify_co2_preseal_atmosphere_hold_pressure(point)
                if pressure_decision == "positive_preseal_ready_handoff":
                    self._log(
                        "CO2 pre-seal atmosphere flush reached positive preseal ready pressure; "
                        f"handoff to pressurization/seal (row {point.index})"
                    )
                    break
                last_atmosphere_hold_ts = now
            self._refresh_live_analyzer_snapshots(reason="co2_route_preseal_soak")
            time.sleep(min(1.0, max(0.05, soak_s - (time.time() - start))))
        if special_flush:
            self._post_h2o_co2_zero_flush_pending = False
            self._initial_co2_zero_flush_pending = False
        self.run_state.humidity.first_co2_route_soak_pending = False
        self._first_co2_route_soak_pending = False
        self._record_workflow_timing(
            "preseal_soak_end",
            "end",
            stage="preseal_soak",
            point=point,
            expected_max_s=soak_s + max(5.0, soak_s * 0.1),
            decision="ok",
        )
        return self._wait_co2_route_dewpoint_gate_before_seal(
            point,
            base_soak_s=soak_s,
            log_context=log_context,
        )

    def _verify_co2_preseal_atmosphere_hold_pressure(self, point: CalibrationPoint) -> str:
        reader = getattr(self.pressure_control_service, "_current_pressure", None)
        if not callable(reader):
            return "unavailable"
        pressure_hpa = self._as_float(reader())
        if pressure_hpa is None:
            return "unavailable"
        limit_hpa = self._as_float(self._cfg_get("workflow.pressure.preseal_atmosphere_hold_max_hpa"))
        target_hpa = self._as_float(point.target_pressure_hpa)
        positive_preseal_enabled = bool(
            self._cfg_get("workflow.pressure.positive_preseal_pressurization_enabled", False)
        )
        ready_hpa = self._as_float(self._cfg_get("workflow.pressure.preseal_ready_pressure_hpa"))
        if ready_hpa is None and target_hpa is not None:
            ready_margin_hpa = self._as_float(self._cfg_get("workflow.pressure.preseal_ready_margin_hpa", 0.0))
            ready_hpa = target_hpa + float(0.0 if ready_margin_hpa is None else ready_margin_hpa)
        abort_hpa = self._as_float(self._cfg_get("workflow.pressure.preseal_atmosphere_flush_abort_pressure_hpa"))
        if abort_hpa is None:
            abort_hpa = self._as_float(self._cfg_get("workflow.pressure.preseal_abort_pressure_hpa"))
        if limit_hpa is None:
            default_limit_hpa = self._as_float(
                self._cfg_get("workflow.pressure.preseal_atmosphere_hold_default_max_hpa", 1110.0)
            )
            if target_hpa is None:
                return "unavailable"
            margin_hpa = self._as_float(self._cfg_get("workflow.pressure.preseal_atmosphere_hold_margin_hpa", 10.0))
            target_limit_hpa = target_hpa + abs(10.0 if margin_hpa is None else margin_hpa)
            limit_hpa = max(1110.0 if default_limit_hpa is None else default_limit_hpa, target_limit_hpa)
        effective_limit_hpa = float(abort_hpa if positive_preseal_enabled and abort_hpa is not None else limit_hpa)
        if (
            positive_preseal_enabled
            and ready_hpa is not None
            and pressure_hpa >= float(ready_hpa)
            and pressure_hpa <= effective_limit_hpa
        ):
            tagger = getattr(getattr(self, "route_planner", None), "co2_point_tag", None)
            point_tag = tagger(point) if callable(tagger) else ""
            details = {
                "pressure_hpa": pressure_hpa,
                "ready_pressure_hpa": float(ready_hpa),
                "abort_pressure_hpa": effective_limit_hpa,
                "point_index": point.index,
                "point_tag": point_tag,
                "reason": "positive_preseal_ready_handoff",
            }
            recorder = getattr(getattr(self, "status_service", None), "record_route_trace", None)
            if callable(recorder):
                recorder(
                    action="preseal_atmosphere_flush_ready_handoff",
                    route="co2",
                    point=point,
                    actual=details,
                    result="ok",
                    message="CO2 atmosphere flush reached positive preseal ready pressure",
                )
            self._record_workflow_timing(
                "preseal_pressure_check",
                "info",
                stage="preseal_atmosphere_flush_hold",
                point=point,
                pressure_hpa=pressure_hpa,
                target_pressure_hpa=target_hpa,
                decision="positive_preseal_ready_handoff",
            )
            return "positive_preseal_ready_handoff"
        if pressure_hpa <= effective_limit_hpa:
            self._record_workflow_timing(
                "preseal_pressure_check",
                "info",
                stage="preseal_atmosphere_flush_hold" if positive_preseal_enabled else "preseal_soak",
                point=point,
                pressure_hpa=pressure_hpa,
                target_pressure_hpa=target_hpa,
                decision="within_limit",
            )
            return "within_limit"
        tagger = getattr(getattr(self, "route_planner", None), "co2_point_tag", None)
        point_tag = tagger(point) if callable(tagger) else ""
        reason = (
            "co2_preseal_atmosphere_flush_abort_pressure_exceeded"
            if positive_preseal_enabled
            else "co2_preseal_atmosphere_hold_pressure_exceeded"
        )
        details = {
            "pressure_hpa": pressure_hpa,
            "limit_hpa": float(effective_limit_hpa),
            "ready_pressure_hpa": ready_hpa,
            "point_index": point.index,
            "point_tag": point_tag,
            "reason": reason,
        }
        recorder = getattr(getattr(self, "status_service", None), "record_route_trace", None)
        if callable(recorder):
            recorder(
                action="co2_preseal_atmosphere_hold_pressure_guard",
                route="co2",
                point=point,
                actual=details,
                result="fail",
                message="CO2 pre-seal atmosphere hold pressure exceeded limit",
            )
        self._record_workflow_timing(
            "preseal_pressure_check",
            "fail",
            stage="preseal_soak",
            point=point,
            pressure_hpa=pressure_hpa,
            target_pressure_hpa=target_hpa,
            decision="limit_exceeded",
            error_code=reason,
        )
        self._log(
            "CO2 pre-seal atmosphere hold pressure exceeded limit: "
            f"row={point.index} pressure={pressure_hpa:.3f}hPa limit={float(effective_limit_hpa):.3f}hPa"
        )
        raise WorkflowValidationError(
            "CO2 pre-seal atmosphere hold pressure exceeded limit",
            details=details,
        )

    def _refresh_live_analyzer_snapshots(self, *, force: bool = False, reason: str = "") -> bool:
        refresher = getattr(self.analyzer_fleet_service, "refresh_live_snapshots", None)
        if not callable(refresher):
            return False
        return bool(refresher(force=force, reason=reason))

    def _set_pressure_controller_vent(
        self,
        vent_on: bool,
        reason: str = "",
        *,
        wait_after_command: bool = True,
        capture_pressure: bool = True,
    ) -> None:
        self.pressure_control_service.set_pressure_controller_vent(
            vent_on,
            reason=reason,
            wait_after_command=wait_after_command,
            capture_pressure=capture_pressure,
        )

    def _enable_pressure_controller_output(self, reason: str = "") -> None:
        self.pressure_control_service.enable_pressure_controller_output(reason=reason)

    def _prepare_pressure_for_h2o(self, point: CalibrationPoint) -> None:
        self.pressure_control_service.prepare_pressure_for_h2o(point)

    def _pressure_reading_and_in_limits(self, target_hpa: float) -> tuple[Optional[float], bool]:
        return self.pressure_control_service.pressure_reading_and_in_limits(target_hpa)

    def _soft_recover_pressure_controller(self, *, reason: str = "") -> bool:
        return self.pressure_control_service.soft_recover_pressure_controller(reason=reason).ok

    def _set_pressure_to_target(self, point: CalibrationPoint, *, recovery_attempted: bool = False) -> bool:
        return self.pressure_control_service.set_pressure_to_target(point, recovery_attempted=recovery_attempted).ok

    def _pressurize_and_hold(self, point: CalibrationPoint, route: str = "co2") -> bool:
        return self.pressure_control_service.pressurize_and_hold(point, route=route).ok

    def _wait_after_pressure_stable_before_sampling(self, point: CalibrationPoint) -> bool:
        return self.pressure_control_service.wait_after_pressure_stable_before_sampling(point).ok

    def _managed_valves(self) -> list[int]:
        return self.valve_routing_service.managed_valves()

    def _resolve_valve_target(self, logical_valve: int) -> tuple[str, int]:
        return self.valve_routing_service.resolve_valve_target(logical_valve)

    def _desired_valve_state(self, valve: int, open_set: set[int]) -> bool:
        return self.valve_routing_service.desired_valve_state(valve, open_set)

    def _apply_valve_states(self, open_valves: Iterable[int]) -> dict[str, dict[str, bool]]:
        return self.valve_routing_service.apply_valve_states(open_valves)

    def _apply_route_baseline_valves(self) -> None:
        self.valve_routing_service.apply_route_baseline_valves()

    def _set_h2o_path(self, is_open: bool, point: Optional[CalibrationPoint] = None) -> None:
        self.valve_routing_service.set_h2o_path(is_open, point)

    def _co2_maps_for_point(self, point: CalibrationPoint) -> list[dict[str, Any]]:
        return self.valve_routing_service.co2_maps_for_point(point)

    def _co2_path_for_point(self, point: CalibrationPoint) -> Optional[int]:
        return self.valve_routing_service.co2_path_for_point(point)

    def _source_valve_for_point(self, point: CalibrationPoint) -> Optional[int]:
        return self.valve_routing_service.source_valve_for_point(point)

    def _co2_open_valves(self, point: CalibrationPoint, *, include_total_valve: bool) -> list[int]:
        return self.valve_routing_service.co2_open_valves(point, include_total_valve=include_total_valve)

    def _set_valves_for_co2(self, point: CalibrationPoint) -> None:
        self.valve_routing_service.set_valves_for_co2(point)

    def _set_co2_route_baseline(self, *, reason: str = "") -> None:
        self.valve_routing_service.set_co2_route_baseline(reason=reason)

    def _cleanup_co2_route(self, *, reason: str = "") -> None:
        self.valve_routing_service.cleanup_co2_route(reason=reason)

    def _cleanup_h2o_route(self, point: CalibrationPoint, *, reason: str = "") -> None:
        self.valve_routing_service.cleanup_h2o_route(point, reason=reason)

    def _mark_post_h2o_co2_zero_flush_pending(self) -> None:
        self.valve_routing_service.mark_post_h2o_co2_zero_flush_pending()

    def _precondition_next_temperature_chamber(self, next_points: Optional[list[CalibrationPoint]]) -> None:
        if not next_points or not bool(self._cfg_get("workflow.stability.temperature.precondition_next_group_enabled", False)):
            return
        chamber = self._device("temperature_chamber")
        if chamber is None:
            return
        target_c = float(next_points[0].temp_chamber_c)
        command_target = target_c + float(self._cfg_get("workflow.stability.temperature.command_offset_c", 0.0))
        try:
            self._call_first(chamber, ("set_temp_c", "set_temperature_c", "set_temperature"), command_target)
            self._call_first(chamber, ("start",))
            self._log(f"Preconditioning temperature chamber for next temperature group: target={target_c:.2f}C")
        except Exception as exc:
            self._log(f"Next-group chamber precondition failed: {exc}")

    def _precondition_next_temperature_humidity(self, next_points: Optional[list[CalibrationPoint]]) -> None:
        if not next_points:
            return
        if self._route_mode() != "h2o_then_co2":
            return
        if not bool(self._cfg_get("workflow.stability.humidity_generator.precondition_next_group_enabled", True)):
            return
        h2o_points = [point for point in next_points if point.is_h2o_point]
        if not h2o_points:
            return
        self._prepare_humidity_generator(h2o_points[0])
        self._log(
            "Preconditioning humidity generator for next temperature group: "
            f"chamber={h2o_points[0].temp_chamber_c}C hgen={h2o_points[0].hgen_temp_c}C/{h2o_points[0].hgen_rh_pct}%"
        )

    @staticmethod
    def _span(values: list[float]) -> float:
        if len(values) < 2:
            return 0.0
        return float(max(values) - min(values))

    def _evaluate_sample_quality(self, rows: list[dict[str, Any]]) -> tuple[bool, dict[str, float]]:
        return self.sampling_service.evaluate_sample_quality(rows)

    def _sampling_params(self, phase: str = "") -> tuple[int, float]:
        return self.sampling_service.sampling_params(phase=phase)

    def _summarize_analyzer_integrity(self, rows: list[dict[str, Any]], *, analyzer_labels: list[str]) -> dict[str, Any]:
        return self.sampling_service.summarize_analyzer_integrity(rows, analyzer_labels=analyzer_labels)

    def _collect_sample_batch(
        self,
        point: CalibrationPoint,
        *,
        count: int,
        interval_s: float,
        phase: str,
        point_tag: str,
    ) -> tuple[list[dict[str, Any]], list[SamplingResult]]:
        return self.sampling_service.collect_sample_batch(
            point,
            count=count,
            interval_s=interval_s,
            phase=phase,
            point_tag=point_tag,
        )

    def _sample_point(self, point: CalibrationPoint, *, phase: str, point_tag: str = "") -> list[SamplingResult]:
        return self.sampling_service.sample_point(point, phase=phase, point_tag=point_tag)
