from __future__ import annotations

from dataclasses import replace
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev
import time
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

from ..config import AppConfig
from ..export import export_ratio_poly_report
from ..exceptions import StabilityTimeoutError, WorkflowInterruptedError, WorkflowValidationError
from ..qc import QCPipeline
from ..utils import as_bool, as_float, safe_get
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
    ConditioningService,
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


from .a2_hooks import A2Hooks
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
        self.a2_hooks = A2Hooks()
        self.conditioning_service = ConditioningService(host=self)
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
        self._device_init_policy_evidence: dict[str, Any] = self._classify_device_failures(
            [],
            all_devices=[],
            stage="initialization",
        )
        self._populate_a2_hooks_callbacks()

    def _populate_a2_hooks_callbacks(self) -> None:
        self.a2_hooks.callbacks["mark_route_open_started"] = self._mark_a2_co2_route_open_command_write_started
        self.a2_hooks.callbacks["mark_route_open_completed"] = self._mark_a2_co2_route_open_command_write_completed
        self.a2_hooks.callbacks["refresh_after_route_open"] = self._refresh_a2_co2_conditioning_after_route_open
        self.a2_hooks.callbacks["fail_route_open_transition"] = self._fail_a2_route_open_transition_if_blocked
        self.a2_hooks.callbacks["wait_route_open_settle"] = self._wait_a2_co2_route_open_settle_before_conditioning
        self.a2_hooks.callbacks["complete_route_open_transition"] = self._complete_a2_co2_route_open_transition
        self.a2_hooks.callbacks["record_a2_conditioning_workflow_timing"] = self._record_a2_conditioning_workflow_timing

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
        route_state: Any = None,
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
        resolved_route_state = {
            "current_route": getattr(self.route_context, "current_route", ""),
            "current_phase": str(getattr(getattr(self.route_context, "current_phase", None), "value", getattr(self.route_context, "current_phase", "")) or ""),
            "point_tag": getattr(self.route_context, "point_tag", ""),
            "retry": getattr(self.route_context, "retry", 0),
            "route_state": dict(getattr(self.route_context, "route_state", {}) or {}),
        }
        if isinstance(route_state, dict):
            resolved_route_state["route_state"].update(route_state)
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
            route_state=resolved_route_state,
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

    def _precondition_next_temperature_humidity(self, next_group: Any) -> None:
        pass

    def _precondition_next_temperature_chamber(self, next_group: Any) -> None:
        pass

    def _bind_run_state_aliases(self) -> None:
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
            self._handle_device_failures(
                failed,
                all_devices=results.keys(),
                error_message="Critical device initialization failed",
                warning_prefix="Device open warnings",
                stage="initialization",
            )
        else:
            self._record_device_failure_policy(
                self._classify_device_failures([], all_devices=results.keys(), stage="initialization"),
                stage="initialization",
            )
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
                self._handle_device_failures(
                    failing,
                    all_devices=health.keys(),
                    error_message="Device precheck failed",
                    warning_prefix="Device precheck warnings",
                    stage="precheck",
                )

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

    def _handle_device_failures(
        self,
        failed_devices: Iterable[str],
        *,
        all_devices: Iterable[str],
        error_message: str,
        warning_prefix: str,
        stage: str,
    ) -> dict[str, Any]:
        failed = sorted({str(name) for name in failed_devices if str(name or "").strip()})
        policy = self._classify_device_failures(failed, all_devices=all_devices, stage=stage)
        self._record_device_failure_policy(policy, stage=stage)
        if not failed:
            return policy

        critical = list(policy.get("critical_devices_failed") or [])
        optional_failed = list(policy.get("optional_context_devices_failed") or [])
        self.event_bus.publish(
            EventType.DEVICE_ERROR,
            {
                "failed_devices": failed,
                "critical_devices_failed": critical,
                "optional_context_devices_failed": optional_failed,
                "device_policy_stage": stage,
            },
        )
        if critical:
            fault = self._resolve_fault_code(failed_devices=critical, stage=stage)
            raise WorkflowValidationError(
                error_message,
                details={
                    "failed_devices": critical,
                    "critical_devices_failed": critical,
                    "optional_context_devices_failed": optional_failed,
                    "critical_device_init_failure_blocks_probe": True,
                    "fault_code": fault,
                },
            )

        if optional_failed and set(optional_failed) == set(failed):
            warning = f"Optional context devices unavailable during {stage}: {', '.join(optional_failed)}"
        elif optional_failed:
            warning = f"{warning_prefix}: {', '.join(failed)}; optional_context_devices_failed={optional_failed}"
        else:
            warning = f"{warning_prefix}: {', '.join(failed)}"
        self.session.add_warning(warning)
        self.event_bus.publish(
            EventType.WARNING_RAISED,
            {
                "message": warning,
                "devices": failed,
                "optional_context_devices_failed": optional_failed,
                "device_policy_stage": stage,
            },
        )
        self._log(warning)
        return policy

    def _classify_device_failures(
        self,
        failed_devices: Iterable[str],
        *,
        all_devices: Iterable[str] | None = None,
        stage: str = "initialization",
    ) -> dict[str, Any]:
        failed = sorted({str(name) for name in failed_devices if str(name or "").strip()})
        known = sorted({str(name) for name in (all_devices or []) if str(name or "").strip()} | set(failed))
        gas_devices = sorted({name for name in known if name.startswith("gas_analyzer_")})
        a2_probe = self._a2_pressure_sweep_mode()
        skip_temp_probe = self._a2_skip_temp_wait_engineering_probe_mode()

        route_pressure_devices = ["pressure_controller", "pressure_meter", "relay_a", "relay_b"]
        h2o_only_precheck = (
            stage == "precheck"
            and str(self._cfg_get("workflow.route_mode", "") or "").strip().lower() == "h2o_only"
        )
        if a2_probe:
            critical_required = list(route_pressure_devices)
            critical_required.extend(gas_devices)
            optional_context_devices = ["temperature_chamber"] if skip_temp_probe else []
            if not skip_temp_probe:
                critical_required.append("temperature_chamber")
        elif h2o_only_precheck:
            healthy_gas = [d for d in gas_devices if d not in set(failed)]
            critical_required = ["temperature_chamber"] if healthy_gas else ["temperature_chamber", *gas_devices]
            optional_context_devices = list(gas_devices) if healthy_gas else []
        else:
            critical_required = ["temperature_chamber", *gas_devices]
            optional_context_devices = []
        critical_required = sorted(dict.fromkeys(critical_required))
        optional_context_devices = sorted(dict.fromkeys(optional_context_devices))

        critical_failed = [
            name for name in failed
            if name in critical_required
            or (name.startswith("gas_analyzer_") and not h2o_only_precheck)
        ]
        optional_failed = [name for name in failed if name in optional_context_devices and name not in critical_failed]
        temp_attempted = "temperature_chamber" in known or "temperature_chamber" in failed
        temp_failed_at_stage = "temperature_chamber" in failed
        temp_init_failed = bool(stage == "initialization" and temp_failed_at_stage)
        temp_blocks_a2 = bool("temperature_chamber" in critical_failed and a2_probe)
        temp_required_for_a2 = bool(a2_probe and "temperature_chamber" in critical_required)
        temp_context_available: Optional[bool]
        if temp_failed_at_stage and "temperature_chamber" in optional_context_devices:
            temp_context_available = False
        elif temp_attempted and "temperature_chamber" in optional_context_devices:
            temp_context_available = True
        elif temp_attempted and a2_probe and "temperature_chamber" in critical_required:
            temp_context_available = not temp_failed_at_stage
        else:
            temp_context_available = None

        unavailable_reason = ""
        if temp_failed_at_stage and "temperature_chamber" in optional_context_devices:
            unavailable_reason = (
                "temperature_chamber_init_failed"
                if stage == "initialization"
                else "temperature_chamber_precheck_failed"
            )
        readonly_probe_result = "not_applicable"
        if skip_temp_probe:
            if temp_context_available is False:
                readonly_probe_result = "unavailable"
            elif temp_context_available is True:
                readonly_probe_result = "available_pending_current_pv_read"

        return {
            "temperature_chamber_required_for_a2": temp_required_for_a2,
            "temperature_chamber_init_attempted": bool(temp_attempted if stage == "initialization" else temp_attempted),
            "temperature_chamber_init_ok": bool(temp_attempted and not temp_init_failed) if stage == "initialization" else None,
            "temperature_chamber_init_failed": temp_init_failed,
            "temperature_chamber_init_failure_blocks_a2": temp_blocks_a2,
            "temperature_chamber_optional_in_skip_temp_wait": bool(skip_temp_probe),
            "temperature_context_available": temp_context_available,
            "temperature_context_source": (
                "temperature_chamber_readonly_current_pv"
                if temp_context_available is True and skip_temp_probe
                else ("unavailable" if temp_context_available is False else "")
            ),
            "temperature_context_unavailable_reason": unavailable_reason,
            "temperature_chamber_readonly_probe_attempted": bool(skip_temp_probe and temp_attempted),
            "temperature_chamber_readonly_probe_result": readonly_probe_result,
            "temperature_not_part_of_acceptance": self._cfg_bool_any(
                (
                    "a2_co2_7_pressure_no_write_probe.temperature_not_part_of_acceptance",
                    "workflow.stability.temperature.temperature_not_part_of_acceptance",
                ),
                default=False,
            ),
            "temperature_stabilization_wait_skipped": self._a2_temperature_wait_skipped(),
            "temperature_gate_mode": self._temperature_gate_mode(),
            "critical_devices_required": critical_required,
            "critical_devices_failed": critical_failed,
            "optional_context_devices": optional_context_devices,
            "optional_context_devices_failed": optional_failed,
            "critical_device_init_failure_blocks_probe": bool(critical_failed),
            "optional_context_failure_blocks_probe": False,
        }

    def _record_device_failure_policy(self, policy: Mapping[str, Any], *, stage: str) -> None:
        current = dict(getattr(self, "_device_init_policy_evidence", {}) or {})
        if stage != "initialization" and current:
            preserved = {
                key: current.get(key)
                for key in (
                    "temperature_chamber_init_attempted",
                    "temperature_chamber_init_ok",
                    "temperature_chamber_init_failed",
                )
                if key in current
            }
            current.update(dict(policy))
            current.update({key: value for key, value in preserved.items() if value is not None})
        else:
            current.update(dict(policy))
        self._device_init_policy_evidence = current
        self._record_workflow_timing(
            "device_init_policy",
            "warning"
            if current.get("critical_devices_failed") or current.get("optional_context_devices_failed")
            else "info",
            stage=f"device_{stage}",
            decision="blocked"
            if current.get("critical_devices_failed")
            else (
                "optional_context_unavailable"
                if current.get("optional_context_devices_failed")
                else "ok"
            ),
            route_state=current,
        )

    @staticmethod
    def _resolve_fault_code(*, failed_devices: list[str], stage: str) -> str:
        from .models import FAULT_CODES

        if stage in ("precheck", "initialization"):
            analyzer_failed = any("gas_analyzer" in d for d in failed_devices)
            if analyzer_failed:
                return "H2O-001"
        return "H2O-009"

    def _device_init_policy_summary(self) -> dict[str, Any]:
        current = dict(getattr(self, "_device_init_policy_evidence", {}) or {})
        if not current:
            current = self._classify_device_failures(
                [],
                all_devices=self._known_device_names(),
                stage="initialization",
            )
        return dict(current)

    def _known_device_names(self) -> list[str]:
        manager = getattr(self, "device_manager", None)
        lister = getattr(manager, "list_device_info", None)
        if callable(lister):
            try:
                return sorted(str(name) for name in dict(lister()).keys())
            except Exception:
                return []
        return []

    def _is_critical_device(self, name: str) -> bool:
        policy = self._classify_device_failures([name], all_devices=[name], stage="classification")
        return str(name or "") in set(policy.get("critical_devices_failed") or [])

    def _a2_pressure_sweep_mode(self) -> bool:
        run001_scope = str(self._cfg_get("run001_a2.scope", "") or "").strip()
        probe_scope = str(self._cfg_get("a2_co2_7_pressure_no_write_probe.scope", "") or "").strip()
        return bool(
            run001_scope == "run001_a2_co2_no_write_pressure_sweep"
            or probe_scope == "a2_co2_7_pressure_no_write"
            or (
                self._cfg_bool_any(("run001_a2.no_write",), default=False)
                and self._cfg_bool_any(("run001_a2.co2_only",), default=False)
            )
        )

    def _a2_skip_temp_wait_engineering_probe_mode(self) -> bool:
        if not self._a2_pressure_sweep_mode():
            return False
        route_mode = str(self._cfg_get("workflow.route_mode", "") or "").strip().lower()
        co2_only = bool(
            route_mode == "co2_only"
            or self._cfg_bool_any(
                (
                    "a2_co2_7_pressure_no_write_probe.co2_only",
                    "run001_a2.co2_only",
                ),
                default=False,
            )
        )
        selected_temps = self._cfg_get("workflow.selected_temps_c", [])
        single_temp = self._cfg_bool_any(
            (
                "a2_co2_7_pressure_no_write_probe.single_temperature",
                "run001_a2.single_temperature_group",
            ),
            default=False,
        )
        if isinstance(selected_temps, list) and len(selected_temps) <= 1:
            single_temp = True
        skip_wait = self._a2_temperature_wait_skipped()
        current_pv_mode = self._temperature_gate_mode() == "current_pv_engineering_probe"
        not_acceptance = self._cfg_bool_any(
            (
                "a2_co2_7_pressure_no_write_probe.temperature_not_part_of_acceptance",
                "workflow.stability.temperature.temperature_not_part_of_acceptance",
            ),
            default=False,
        )
        no_chamber_writes = not any(
            self._cfg_bool_any((path,), default=False)
            for path in (
                "a2_co2_7_pressure_no_write_probe.chamber_set_temperature_enabled",
                "a2_co2_7_pressure_no_write_probe.chamber_start_enabled",
                "a2_co2_7_pressure_no_write_probe.chamber_stop_enabled",
                "run001_a2.chamber_set_temperature_enabled",
                "run001_a2.chamber_start_enabled",
                "run001_a2.chamber_stop_enabled",
            )
        )
        multi_temperature = self._cfg_bool_any(
            (
                "a2_co2_7_pressure_no_write_probe.multi_temperature_enabled",
                "run001_a2.multi_temperature_enabled",
            ),
            default=False,
        )
        return bool(
            co2_only
            and single_temp
            and skip_wait
            and current_pv_mode
            and not_acceptance
            and no_chamber_writes
            and not multi_temperature
        )

    def _a2_temperature_wait_skipped(self) -> bool:
        return bool(
            self._cfg_bool_any(
                (
                    "a2_co2_7_pressure_no_write_probe.temperature_stabilization_wait_skipped",
                    "workflow.stability.temperature.skip_temperature_stabilization_wait",
                    "workflow.stability.temperature.temperature_stabilization_wait_skipped",
                ),
                default=False,
            )
            or self._temperature_gate_mode() == "current_pv_engineering_probe"
        )

    def _temperature_gate_mode(self) -> str:
        return str(
            self._cfg_get("a2_co2_7_pressure_no_write_probe.temperature_gate_mode", "")
            or self._cfg_get("workflow.stability.temperature.temperature_gate_mode", "")
            or self._cfg_get("workflow.stability.temperature.gate_mode", "")
            or ""
        ).strip()

    def _cfg_bool_any(self, paths: Iterable[str], *, default: bool = False) -> bool:
        for path in paths:
            value = self._cfg_get(path, None)
            if value is not None:
                return as_bool(value, default=default)
        return default

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
            time_s = float(self._cfg_get("devices.pressure_controller.in_limits_time_s", 0.5))
            self._call_first(controller, ("set_in_limits",), pct, time_s)
        except Exception as exc:
            self._log(f"Pressure controller in-limits setup failed: {exc}")

    def _set_pressure_controller_vent(
        self,
        vent_on: bool,
        reason: str = "",
        **kwargs: Any,
    ) -> Any:
        return self.pressure_control_service.set_pressure_controller_vent(
            vent_on,
            reason=reason,
            **kwargs,
        )

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

    def _wait_co2_route_soak_before_seal(self, *args, **kwargs):
        return self.conditioning_service._wait_co2_route_soak_before_seal(*args, **kwargs)

    def _record_pressure_source_latency_events(
        self,
        sample: Mapping[str, Any],
        *,
        point: CalibrationPoint,
        stage: str,
    ) -> None:
        source_items = (
            ("digital_gauge_pressure_sample", "gauge"),
            ("pace_pressure_sample", "pace"),
        )
        latency_warn_s = self._as_float(self._cfg_get("workflow.pressure.pressure_read_latency_warn_s", 0.5))
        for key, prefix in source_items:
            item = sample.get(key)
            if not isinstance(item, Mapping):
                continue
            pressure_hpa = self._as_float(item.get("pressure_hpa"))
            latency_s = self._as_float(item.get("read_latency_s"))
            self._record_workflow_timing(
                f"{prefix}_pressure_read_start",
                "start",
                stage=stage,
                point=point,
                pressure_hpa=pressure_hpa,
                decision=str(item.get("source") or item.get("pressure_sample_source") or ""),
                route_state=item,
            )
            self._record_workflow_timing(
                f"{prefix}_pressure_read_end",
                "end",
                stage=stage,
                point=point,
                pressure_hpa=pressure_hpa,
                duration_s=latency_s,
                decision="ok" if item.get("parse_ok") else "unavailable",
                route_state=item,
            )
            if bool(item.get("is_stale", item.get("pressure_sample_is_stale"))):
                self._record_workflow_timing(
                    "pressure_sample_stale",
                    "warning",
                    stage=stage,
                    point=point,
                    pressure_hpa=pressure_hpa,
                    warning_code="pressure_sample_stale",
                    route_state=item,
                )
            if latency_warn_s is not None and latency_s is not None and float(latency_s) > float(latency_warn_s):
                self._record_workflow_timing(
                    "pressure_read_latency_warning",
                    "warning",
                    stage=stage,
                    point=point,
                    pressure_hpa=pressure_hpa,
                    duration_s=latency_s,
                    expected_max_s=latency_warn_s,
                    warning_code="pressure_read_latency_s_long",
                    route_state=item,
                )
        self._record_workflow_timing(
            "pressure_source_selected",
            "info",
            stage=stage,
            point=point,
            pressure_hpa=self._as_float(sample.get("pressure_hpa")),
            decision=str(sample.get("pressure_source_used_for_decision") or ""),
            route_state=sample,
        )
        if bool(sample.get("pressure_source_disagreement_warning")):
            self._record_workflow_timing(
                "pressure_source_disagreement",
                "warning",
                stage=stage,
                point=point,
                pressure_hpa=self._as_float(sample.get("pressure_hpa")),
                warning_code="pressure_source_disagreement_hpa_high",
                route_state=sample,
            )

    def _a2_high_pressure_pressure_values(
        self,
        point: CalibrationPoint,
        pressure_points: Optional[Iterable[CalibrationPoint]] = None,
    ) -> list[float]:
        candidates = list(pressure_points or [point])
        values: list[float] = []
        for item in candidates:
            value = self._as_float(getattr(item, "target_pressure_hpa", None))
            if value is not None:
                values.append(float(value))
        if not values:
            value = self._as_float(getattr(point, "target_pressure_hpa", None))
            if value is not None:
                values.append(float(value))
        return values

    def _a2_co2_route_conditioning_required(
        self,
        point: CalibrationPoint,
        pressure_points: Optional[Iterable[CalibrationPoint]] = None,
    ) -> bool:
        enabled = bool(self._cfg_get("workflow.pressure.co2_route_conditioning_atmosphere_required", True))
        pressure_values = self._a2_high_pressure_pressure_values(point, pressure_points)
        contains_1100 = any(abs(float(value) - 1100.0) <= 0.001 for value in pressure_values)
        return bool(
            enabled
            and contains_1100
            and self._workflow_timing_enabled()
            and self._workflow_no_write_guard_active()
        )

    def _co2_conditioning_soak_s(self, point: CalibrationPoint) -> float:
        if self._has_special_co2_zero_flush_pending() and self._is_zero_co2_point(point):
            return float(self._cfg_get("workflow.stability.co2_route.post_h2o_zero_ppm_soak_s", 600.0))
        if getattr(self, "_first_co2_route_soak_pending", False):
            return float(self._cfg_get("workflow.stability.co2_route.first_point_preseal_soak_s", 300.0))
        return float(self._cfg_get("workflow.stability.co2_route.preseal_soak_s", 180.0))

    def _begin_a2_co2_route_conditioning_at_atmosphere(
        self,
        point: CalibrationPoint,
        pressure_points: Optional[Iterable[CalibrationPoint]] = None,
    ) -> dict[str, Any]:
        return self.conditioning_service._begin_a2_co2_route_conditioning_at_atmosphere(point, pressure_points)

    def _a2_conditioning_stream_snapshot(
        self,
        *,
        point: Optional[CalibrationPoint] = None,
        phase: str = "",
        fast: bool = False,
        budget_ms: Any = None,
    ) -> dict[str, Any]:
        if fast:
            fast_snapshotter = getattr(
                self.pressure_control_service,
                "digital_gauge_continuous_latest_fast_snapshot",
                None,
            )
            if callable(fast_snapshotter):
                snapshot = fast_snapshotter(
                    stage="co2_route_conditioning_at_atmosphere",
                    point_index=None if point is None else point.index,
                    budget_ms=budget_ms,
                )
                return dict(snapshot) if isinstance(snapshot, Mapping) else {}
        snapshotter = getattr(self.pressure_control_service, "digital_gauge_continuous_stream_snapshot", None)
        if not callable(snapshotter):
            return {}
        snapshot = snapshotter()
        return dict(snapshot) if isinstance(snapshot, Mapping) else {}

    def _a2_conditioning_pressure_source_mode(self) -> str:
        return self.conditioning_service._a2_conditioning_pressure_source_mode()

    def _a2_conditioning_vent_heartbeat_interval_s(self) -> float:
        return self.conditioning_service._a2_conditioning_vent_heartbeat_interval_s()

    def _a2_conditioning_vent_max_gap_s(self) -> float:
        return self.conditioning_service._a2_conditioning_vent_max_gap_s()

    def _a2_conditioning_high_frequency_vent_max_gap_s(self) -> float:
        return self.conditioning_service._a2_conditioning_high_frequency_vent_max_gap_s()

    def _a2_conditioning_vent_maintenance_interval_s(self) -> float:
        return self.conditioning_service._a2_conditioning_vent_maintenance_interval_s()

    def _a2_conditioning_vent_maintenance_max_gap_s(self) -> float:
        return self.conditioning_service._a2_conditioning_vent_maintenance_max_gap_s()

    def _a2_conditioning_scheduler_sleep_step_s(self) -> float:
        return self.conditioning_service._a2_conditioning_scheduler_sleep_step_s()

    def _a2_conditioning_defer_reschedule_latency_budget_ms(self) -> float:
        return self.conditioning_service._a2_conditioning_defer_reschedule_latency_budget_ms()

    def _a2_conditioning_pressure_monitor_interval_s(self) -> float:
        return self.conditioning_service._a2_conditioning_pressure_monitor_interval_s()

    def _a2_conditioning_diagnostic_budget_ms(self) -> float:
        return self.conditioning_service._a2_conditioning_diagnostic_budget_ms()

    def _a2_conditioning_pressure_monitor_budget_ms(self) -> float:
        return self.conditioning_service._a2_conditioning_pressure_monitor_budget_ms()

    def _a2_conditioning_continuous_latest_fresh_budget_ms(self) -> float:
        return self.conditioning_service._a2_conditioning_continuous_latest_fresh_budget_ms()

    def _a2_conditioning_selected_pressure_sample_stale_budget_ms(self) -> float:
        return self.conditioning_service._a2_conditioning_selected_pressure_sample_stale_budget_ms()

    def _a2_conditioning_monitor_pressure_max_defer_ms(self) -> float:
        return self.conditioning_service._a2_conditioning_monitor_pressure_max_defer_ms()

    def _a2_conditioning_trace_write_budget_ms(self) -> float:
        return self.conditioning_service._a2_conditioning_trace_write_budget_ms()

    def _a2_conditioning_digital_gauge_max_age_s(self) -> float:
        return self.conditioning_service._a2_conditioning_digital_gauge_max_age_s()

    def _a2_conditioning_pressure_abort_hpa(self) -> float:
        return self.conditioning_service._a2_conditioning_pressure_abort_hpa()

    def _a2_cfg_bool(self, path: str, default: bool) -> bool:
        return self.conditioning_service._a2_cfg_bool(path, default)

    def _a2_route_conditioning_hard_abort_pressure_hpa(self) -> float:
        return self.conditioning_service._a2_route_conditioning_hard_abort_pressure_hpa()

    def _a2_route_open_transient_window_enabled(self) -> bool:
        return self.conditioning_service._a2_route_open_transient_window_enabled()

    def _a2_route_open_transient_recovery_timeout_s(self) -> float:
        return self.conditioning_service._a2_route_open_transient_recovery_timeout_s()

    def _a2_route_open_transient_recovery_band_hpa(self) -> float:
        return self.conditioning_service._a2_route_open_transient_recovery_band_hpa()

    def _a2_route_open_transient_stable_hold_s(self) -> float:
        return self.conditioning_service._a2_route_open_transient_stable_hold_s()

    def _a2_route_open_transient_stable_span_hpa(self) -> float:
        return self.conditioning_service._a2_route_open_transient_stable_span_hpa()

    def _a2_route_open_transient_stable_slope_hpa_per_s(self) -> float:
        return self.conditioning_service._a2_route_open_transient_stable_slope_hpa_per_s()

    def _a2_route_open_transient_sustained_rise_min_samples(self) -> int:
        return self.conditioning_service._a2_route_open_transient_sustained_rise_min_samples()

    def _a2_conditioning_high_frequency_vent_window_s(self) -> float:
        return self.conditioning_service._a2_conditioning_high_frequency_vent_window_s()

    def _a2_conditioning_high_frequency_vent_interval_s(self) -> float:
        return self.conditioning_service._a2_conditioning_high_frequency_vent_interval_s()

    def _a2_conditioning_fast_vent_max_duration_s(self) -> float:
        return self.conditioning_service._a2_conditioning_fast_vent_max_duration_s()

    def _a2_route_open_transition_block_threshold_s(self) -> float:
        return self.conditioning_service._a2_route_open_transition_block_threshold_s()

    def _a2_route_open_settle_wait_s(self) -> float:
        return self.conditioning_service._a2_route_open_settle_wait_s()

    def _a2_route_open_settle_wait_slice_s(self) -> float:
        return self.conditioning_service._a2_route_open_settle_wait_slice_s()

    def _a2_conditioning_pressure_rise_vent_trigger_hpa(self) -> float:
        return self.conditioning_service._a2_conditioning_pressure_rise_vent_trigger_hpa()

    def _a2_conditioning_pressure_rise_vent_min_interval_s(self) -> float:
        return self.conditioning_service._a2_conditioning_pressure_rise_vent_min_interval_s()

    def _a2_conditioning_vent_schedule(
        self,
        context: Mapping[str, Any],
        *,
        now_mono: float,
    ) -> dict[str, Any]:
        route_open_monotonic = self._as_float(
            context.get("route_open_completed_monotonic_s")
            or self.a2_hooks.co2_route_open_monotonic_s
        )
        high_frequency_window = False
        route_open_elapsed_s = None
        if route_open_monotonic is not None:
            route_open_elapsed_s = max(0.0, float(now_mono) - float(route_open_monotonic))
            high_frequency_window = route_open_elapsed_s <= self._a2_conditioning_high_frequency_vent_window_s()
        if route_open_monotonic is None:
            interval_s = self._a2_conditioning_high_frequency_vent_interval_s()
            max_gap_s = self._a2_conditioning_vent_maintenance_max_gap_s()
            phase = "pre_route_vent_phase"
        elif high_frequency_window:
            interval_s = self._a2_conditioning_high_frequency_vent_interval_s()
            max_gap_s = self._a2_conditioning_high_frequency_vent_max_gap_s()
            phase = "route_open_high_frequency_vent_phase"
        else:
            interval_s = self._a2_conditioning_vent_maintenance_interval_s()
            max_gap_s = self._a2_conditioning_vent_maintenance_max_gap_s()
            phase = "route_conditioning_flush_maintenance_phase"
        return {
            "route_open_elapsed_s": route_open_elapsed_s,
            "route_conditioning_high_frequency_window_active": high_frequency_window,
            "route_conditioning_effective_vent_interval_s": interval_s,
            "route_conditioning_effective_max_gap_s": max_gap_s,
            "max_vent_pulse_gap_limit_ms": round(max_gap_s * 1000.0, 3),
            "vent_phase": phase,
        }

    def _a2_conditioning_record_scheduler_loop(
        self,
        context: Mapping[str, Any],
        *,
        now_mono: float,
    ) -> dict[str, Any]:
        updated = dict(context)
        previous = self._as_float(updated.get("last_vent_scheduler_tick_monotonic_s"))
        if previous is not None:
            gap_ms = round(max(0.0, float(now_mono) - float(previous)) * 1000.0, 3)
            loop_gaps = list(updated.get("vent_scheduler_loop_gap_ms") or [])
            loop_gaps.append(gap_ms)
            updated["vent_scheduler_loop_gap_ms"] = loop_gaps
            previous_max = self._as_float(updated.get("max_vent_scheduler_loop_gap_ms"))
            updated["max_vent_scheduler_loop_gap_ms"] = (
                gap_ms if previous_max is None else max(float(previous_max), gap_ms)
            )
        defer_started = self._as_float(updated.get("last_diagnostic_defer_monotonic_s"))
        if defer_started is not None and not bool(updated.get("_last_diagnostic_defer_reschedule_recorded", False)):
            defer_loop_ms = round(max(0.0, float(now_mono) - float(defer_started)) * 1000.0, 3)
            schedule = self._a2_conditioning_vent_schedule(updated, now_mono=now_mono)
            defer_state = self._a2_conditioning_defer_reschedule_state(
                updated,
                now_mono=now_mono,
                max_gap_s=float(schedule["route_conditioning_effective_max_gap_s"]),
                defer_loop_ms=defer_loop_ms,
            )
            operation = str(
                updated.get("last_diagnostic_defer_operation")
                or updated.get("defer_operation")
                or updated.get("diagnostic_blocking_operation")
                or "deferred_diagnostic"
            )
            updated.update(schedule)
            updated.update(defer_state)
            vent_gap_exceeded = bool(defer_state.get("vent_gap_exceeded_after_defer"))
            updated["defer_returned_to_vent_loop"] = True
            updated["defer_reschedule_requested"] = True
            updated["defer_reschedule_completed"] = not vent_gap_exceeded
            updated["defer_reschedule_reason"] = str(
                updated.get("defer_reschedule_reason") or f"return_to_vent_loop_after_{operation}"
            )
            if vent_gap_exceeded:
                updated["terminal_gap_source"] = "defer_path_no_reschedule"
                updated["terminal_gap_operation"] = operation
                updated["terminal_gap_duration_ms"] = defer_state.get("vent_gap_after_defer_ms")
                updated["terminal_gap_detected_at"] = datetime.now(timezone.utc).isoformat()
                updated["terminal_gap_stack_marker"] = str(
                    updated.get("terminal_gap_stack_marker") or "defer_path_no_reschedule"
                )
            elif bool(defer_state.get("defer_reschedule_latency_warning")):
                updated = self._a2_route_open_transient_mark_continuing_after_defer_warning(updated)
            updated["_last_diagnostic_defer_reschedule_recorded"] = True
        updated["last_vent_scheduler_tick_monotonic_s"] = float(now_mono)
        updated["vent_scheduler_tick_count"] = int(updated.get("vent_scheduler_tick_count") or 0) + 1
        return updated

    def _a2_conditioning_last_vent_write_monotonic_s(self, context: Mapping[str, Any]) -> Optional[float]:
        return self._as_float(
            context.get("last_vent_command_write_sent_monotonic_s")
            or context.get("last_vent_tick_monotonic_s")
            or context.get("last_vent_heartbeat_started_monotonic_s")
        )

    def _a2_conditioning_defer_reschedule_state(
        self,
        context: Mapping[str, Any],
        *,
        now_mono: float,
        max_gap_s: float,
        defer_loop_ms: Optional[float] = None,
    ) -> dict[str, Any]:
        budget_ms = self._a2_conditioning_defer_reschedule_latency_budget_ms()
        if defer_loop_ms is None:
            defer_started = self._as_float(context.get("last_diagnostic_defer_monotonic_s"))
            if defer_started is not None:
                defer_loop_ms = round(max(0.0, float(now_mono) - float(defer_started)) * 1000.0, 3)
        latency_exceeded = bool(defer_loop_ms is not None and float(defer_loop_ms) > float(budget_ms))
        last_write = self._a2_conditioning_last_vent_write_monotonic_s(context)
        vent_gap_ms = None
        if last_write is not None:
            vent_gap_ms = round(max(0.0, float(now_mono) - float(last_write)) * 1000.0, 3)
        elif defer_loop_ms is not None:
            vent_gap_ms = round(float(defer_loop_ms), 3)
        threshold_ms = round(float(max_gap_s) * 1000.0, 3)
        vent_gap_exceeded = bool(vent_gap_ms is not None and float(vent_gap_ms) > threshold_ms)
        return {
            "defer_to_next_vent_loop_ms": defer_loop_ms,
            "vent_tick_after_defer_ms": defer_loop_ms,
            "defer_reschedule_latency_ms": defer_loop_ms,
            "defer_reschedule_latency_budget_ms": round(float(budget_ms), 3),
            "defer_reschedule_latency_exceeded": latency_exceeded,
            "defer_reschedule_latency_warning": latency_exceeded,
            "defer_reschedule_caused_vent_gap_exceeded": vent_gap_exceeded,
            "vent_gap_exceeded_after_defer": vent_gap_exceeded,
            "vent_gap_after_defer_ms": vent_gap_ms,
            "vent_gap_after_defer_threshold_ms": threshold_ms,
            "terminal_gap_after_defer": vent_gap_exceeded,
            "terminal_gap_after_defer_ms": vent_gap_ms if vent_gap_exceeded else None,
            "defer_path_no_reschedule": vent_gap_exceeded,
            "defer_path_no_reschedule_reason": (
                "actual_vent_gap_exceeded_after_defer" if vent_gap_exceeded else ""
            ),
        }

    def _a2_conditioning_scheduler_evidence(self, context: Mapping[str, Any]) -> dict[str, Any]:
        keys = (
            "vent_scheduler_priority_mode",
            "vent_scheduler_checked_before_diagnostic",
            "diagnostic_deferred_for_vent_priority",
            "diagnostic_deferred_count",
            "diagnostic_budget_ms",
            "diagnostic_budget_exceeded",
            "diagnostic_blocking_component",
            "diagnostic_blocking_operation",
            "diagnostic_blocking_duration_ms",
            "pressure_monitor_nonblocking",
            "pressure_monitor_deferred_for_vent_priority",
            "pressure_monitor_budget_ms",
            "pressure_monitor_duration_ms",
            "pressure_monitor_blocked_vent_scheduler",
            "conditioning_monitor_pressure_deferred",
            "conditioning_monitor_latest_frame_age_s",
            "conditioning_monitor_latest_frame_fresh",
            "conditioning_monitor_latest_frame_unavailable",
            "conditioning_monitor_pressure_deferred_count",
            "conditioning_monitor_pressure_deferred_elapsed_ms",
            "conditioning_monitor_max_defer_ms",
            "conditioning_monitor_pressure_stale_timeout",
            "conditioning_monitor_pressure_unavailable_fail_closed",
            "selected_pressure_sample_stale_deferred_for_vent_priority",
            "trace_write_budget_ms",
            "trace_write_duration_ms",
            "trace_write_blocked_vent_scheduler",
            "trace_write_deferred_for_vent_priority",
            "terminal_gap_source",
            "terminal_gap_operation",
            "terminal_gap_duration_ms",
            "terminal_gap_started_at",
            "terminal_gap_detected_at",
            "terminal_gap_stack_marker",
            "defer_source",
            "defer_operation",
            "defer_started_at",
            "defer_returned_to_vent_loop",
            "defer_to_next_vent_loop_ms",
            "defer_reschedule_latency_ms",
            "defer_reschedule_latency_budget_ms",
            "defer_reschedule_latency_exceeded",
            "defer_reschedule_latency_warning",
            "defer_reschedule_caused_vent_gap_exceeded",
            "defer_reschedule_requested",
            "defer_reschedule_completed",
            "defer_reschedule_reason",
            "vent_tick_after_defer_ms",
            "fast_vent_after_defer_sent",
            "fast_vent_after_defer_write_ms",
            "terminal_gap_after_defer",
            "terminal_gap_after_defer_ms",
            "vent_gap_exceeded_after_defer",
            "vent_gap_after_defer_ms",
            "vent_gap_after_defer_threshold_ms",
            "defer_path_no_reschedule",
            "defer_path_no_reschedule_reason",
            "fail_closed_path_started",
            "fail_closed_path_started_while_route_open",
            "fail_closed_path_vent_maintenance_required",
            "fail_closed_path_vent_maintenance_active",
            "fail_closed_path_duration_ms",
            "fail_closed_path_blocked_vent_scheduler",
        )
        return {key: context.get(key) for key in keys if key in context}

    def _a2_conditioning_diagnostic_source(self, context: Mapping[str, Any], fallback: str = "unknown") -> str:
        component = str(context.get("diagnostic_blocking_component") or "").strip()
        if component:
            return component
        operation = str(context.get("diagnostic_blocking_operation") or "").strip().lower()
        if "pressure" in operation:
            return "pressure_monitor"
        if "stream" in operation or "snapshot" in operation:
            return "stream_snapshot"
        if "p3" in operation:
            return "p3_fallback"
        if "heartbeat" in operation:
            return "heartbeat_diagnostic"
        if "trace" in operation:
            return "trace_write"
        if "route" in operation:
            return "route_diagnostic"
        return str(fallback or "unknown")

    def _a2_conditioning_update_diagnostic_budget(
        self,
        context: Mapping[str, Any],
        *,
        component: str,
        operation: str,
        duration_ms: Any,
        budget_ms: Any = None,
        blocked_scheduler: bool = False,
        deferred: bool = False,
    ) -> dict[str, Any]:
        updated = dict(context)
        duration_value = self._as_float(duration_ms)
        budget_value = self._as_float(budget_ms)
        if budget_value is None:
            budget_value = self._a2_conditioning_diagnostic_budget_ms()
        budget_exceeded = bool(duration_value is not None and float(duration_value) > float(budget_value))
        updated.update(
            {
                "vent_scheduler_priority_mode": True,
                "diagnostic_budget_ms": round(float(budget_value), 3),
                "diagnostic_budget_exceeded": bool(updated.get("diagnostic_budget_exceeded", False) or budget_exceeded),
                "diagnostic_blocking_component": str(component or "unknown"),
                "diagnostic_blocking_operation": str(operation or "unknown"),
                "diagnostic_blocking_duration_ms": None
                if duration_value is None
                else round(float(duration_value), 3),
                "diagnostic_deferred_for_vent_priority": bool(
                    updated.get("diagnostic_deferred_for_vent_priority", False) or deferred
                ),
                "route_conditioning_diagnostic_blocked_vent_scheduler": bool(
                    updated.get("route_conditioning_diagnostic_blocked_vent_scheduler", False)
                    or blocked_scheduler
                ),
            }
        )
        return updated

    def _a2_conditioning_defer_diagnostic_for_vent_priority(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_defer_diagnostic_for_vent_priority(*args, **kwargs)

    def _a2_conditioning_defer_if_diagnostic_budget_unsafe(
        self,
        point: CalibrationPoint,
        context: Mapping[str, Any],
        *,
        now_mono: float,
        max_gap_s: float,
        budget_ms: float,
        component: str,
        operation: str,
        pressure_monitor: bool = False,
    ) -> Optional[dict[str, Any]]:
        last_write = self._a2_conditioning_last_vent_write_monotonic_s(context)
        if last_write is None:
            return None
        age_ms = max(0.0, float(now_mono) - float(last_write)) * 1000.0
        remaining_ms = max(0.0, float(max_gap_s) * 1000.0 - age_ms)
        safety_margin_ms = min(100.0, max(25.0, float(budget_ms)))
        if remaining_ms > float(budget_ms) + safety_margin_ms:
            return None
        return self._a2_conditioning_defer_diagnostic_for_vent_priority(
            context,
            point=point,
            component=component,
            operation=operation,
            now_mono=now_mono,
            pressure_monitor=pressure_monitor,
        )

    def _a2_conditioning_pressure_sample_from_snapshot(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_pressure_sample_from_snapshot(*args, **kwargs)

    def _record_a2_conditioning_workflow_timing(
        self,
        context: Mapping[str, Any],
        event_name: str,
        event_type: str = "info",
        *,
        route_state: Optional[dict[str, Any]] = None,
        force: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        updated = dict(context)
        now_mono = time.monotonic()
        schedule = self._a2_conditioning_vent_schedule(updated, now_mono=now_mono)
        updated.update(schedule)
        trace_budget_ms = self._a2_conditioning_trace_write_budget_ms()
        high_frequency = bool(schedule.get("route_conditioning_high_frequency_window_active"))
        if high_frequency and not force and event_type not in {"fail", "abort"}:
            updated.update(
                {
                    "trace_write_budget_ms": trace_budget_ms,
                    "trace_write_duration_ms": 0.0,
                    "trace_write_deferred_for_vent_priority": True,
                    "trace_write_blocked_vent_scheduler": False,
                }
            )
            if isinstance(route_state, dict):
                route_state.update(self._a2_conditioning_scheduler_evidence(updated))
            self.a2_hooks.co2_route_conditioning_at_atmosphere_context = updated
            return {}
        started = time.monotonic()
        record = self._record_workflow_timing(
            event_name,
            event_type,
            route_state=route_state,
            **kwargs,
        )
        completed = time.monotonic()
        duration_ms = round(max(0.0, completed - started) * 1000.0, 3)
        blocked = bool(high_frequency and duration_ms > trace_budget_ms)
        updated.update(
            {
                "trace_write_budget_ms": trace_budget_ms,
                "trace_write_duration_ms": duration_ms,
                "trace_write_deferred_for_vent_priority": False,
                "trace_write_blocked_vent_scheduler": blocked,
            }
        )
        if blocked:
            updated = self._a2_conditioning_update_diagnostic_budget(
                updated,
                component="trace_write",
                operation=str(event_name or "workflow_timing_trace_write"),
                duration_ms=duration_ms,
                budget_ms=trace_budget_ms,
                blocked_scheduler=True,
            )
        if isinstance(route_state, dict):
            route_state.update(self._a2_conditioning_scheduler_evidence(updated))
        self.a2_hooks.co2_route_conditioning_at_atmosphere_context = updated
        return record

    def _a2_conditioning_unsafe_vent_reason(self, context: Mapping[str, Any]) -> str:
        phase = str(context.get("route_conditioning_phase") or "route_conditioning_flush_phase")
        if phase != "route_conditioning_flush_phase":
            return "route_conditioning_phase_not_flush"
        if bool(context.get("ready_to_seal_phase_started", False)):
            return "ready_to_seal_phase_started"
        if bool(context.get("route_valve_closing", False)) or bool(context.get("route_valve_closed", False)):
            return "route_valve_closing_or_closed"
        if bool(context.get("seal_command_sent", False)):
            return "seal_command_sent"
        if bool(context.get("pressure_setpoint_command_sent", False)):
            return "pressure_setpoint_command_sent"
        if bool(context.get("pressure_ready_started", False)) or bool(context.get("sampling_started", False)):
            return "pressure_ready_or_sampling_started"
        if int(context.get("sample_count") or 0) > 0 or int(context.get("points_completed") or 0) > 0:
            return "sampling_or_points_completed"
        return ""

    def _a2_conditioning_relief_mix_risk_reason(self, context: Mapping[str, Any]) -> str:
        if bool(context.get("seal_command_sent", False)) or bool(
            context.get("positive_preseal_seal_command_sent", False)
        ) or bool(context.get("sealed", False)):
            return "seal_command_sent"
        if bool(context.get("pressure_setpoint_command_sent", False)) or bool(
            context.get("positive_preseal_pressure_setpoint_command_sent", False)
        ):
            return "pressure_setpoint_command_sent"
        if bool(context.get("pressure_ready_started", False)) or bool(context.get("pressure_gate_reached", False)):
            return "pressure_ready_started"
        if bool(context.get("sampling_started", False)) or bool(context.get("sample_started", False)) or bool(
            context.get("positive_preseal_sample_started", False)
        ):
            return "sample_started"
        if int(context.get("sample_count") or 0) > 0 or int(context.get("points_completed") or 0) > 0:
            return "sampling_or_points_completed"
        if bool(
            context.get("any_write_command_sent")
            or context.get("identity_write_command_sent")
            or context.get("senco_write_command_sent")
            or context.get("calibration_write_command_sent")
        ):
            return "write_state_not_clean"
        return ""

    def _a2_conditioning_mark_vent_blocked(
        self,
        context: Mapping[str, Any],
        *,
        reason: str,
    ) -> dict[str, Any]:
        unsafe_after_control = reason in {
            "seal_command_sent",
            "pressure_setpoint_command_sent",
            "pressure_ready_or_sampling_started",
            "sampling_or_points_completed",
            "route_valve_closing_or_closed",
        }
        updated = dict(context)
        updated.update(
            {
                "vent_pulse_blocked_after_flush_phase": True,
                "vent_pulse_blocked_reason": reason,
                "normal_maintenance_vent_blocked_after_flush_phase": True,
                "cleanup_vent_classification": "normal_maintenance_vent",
                "cleanup_vent_requested": True,
                "cleanup_vent_phase": str(context.get("route_conditioning_phase") or "unknown"),
                "cleanup_vent_reason": reason,
                "cleanup_vent_allowed": False,
                "cleanup_vent_blocked_reason": reason,
                "cleanup_vent_is_normal_maintenance": True,
                "cleanup_vent_is_safe_stop_relief": False,
                "safe_stop_relief_required": False,
                "safe_stop_relief_allowed": False,
                "safe_stop_relief_command_sent": False,
                "safe_stop_relief_blocked_reason": reason,
                "vent_blocked_after_flush_phase_is_failure": True,
                "vent_blocked_after_flush_phase_context": {
                    "phase": str(context.get("route_conditioning_phase") or "unknown"),
                    "classification": "normal_maintenance_vent",
                    "blocked_reason": reason,
                },
                "attempted_unsafe_vent_after_seal_or_pressure_control": bool(unsafe_after_control),
                "unsafe_vent_after_seal_or_pressure_control_command_sent": False,
            }
        )
        if self._a2_conditioning_pressure_source_mode() == "v1_aligned":
            updated.setdefault("pressure_source_selected", "")
            updated.setdefault(
                "pressure_source_selection_reason",
                "v1_aligned_fallback_not_allowed_outside_atmosphere_conditioning",
            )
            updated.setdefault(
                "source_selection_reason",
                "v1_aligned_fallback_not_allowed_outside_atmosphere_conditioning",
            )
        self.a2_hooks.co2_route_conditioning_at_atmosphere_context = updated
        return updated

    def _record_positive_preseal_fail_closed_context(self, actual: Mapping[str, Any]) -> dict[str, Any]:
        context = self.a2_hooks.co2_route_conditioning_at_atmosphere_context
        actual_data = dict(actual or {})
        pressure_hpa = self._as_float(
            actual_data.get("emergency_abort_relief_pressure_hpa")
            or actual_data.get("positive_preseal_pressure_hpa")
            or actual_data.get("pressure_hpa")
            or actual_data.get("current_line_pressure_hpa")
        )
        overlimit = bool(
            actual_data.get("positive_preseal_pressure_overlimit")
            or actual_data.get("positive_preseal_overlimit_fail_closed")
            or str(actual_data.get("positive_preseal_abort_reason") or actual_data.get("abort_reason") or "")
            == "preseal_abort_pressure_exceeded"
        )
        updated = dict(context)
        for key in (
            "positive_preseal_phase_started",
            "positive_preseal_phase_started_at",
            "positive_preseal_pressure_guard_checked",
            "positive_preseal_pressure_hpa",
            "positive_preseal_pressure_source",
            "positive_preseal_pressure_sample_age_s",
            "positive_preseal_abort_pressure_hpa",
            "positive_preseal_pressure_overlimit",
            "positive_preseal_abort_reason",
            "positive_preseal_setpoint_sent",
            "positive_preseal_setpoint_hpa",
            "positive_preseal_output_enabled",
            "positive_preseal_route_open",
            "positive_preseal_seal_command_sent",
            "positive_preseal_pressure_setpoint_command_sent",
            "positive_preseal_sample_started",
            "positive_preseal_overlimit_fail_closed",
            "preseal_capture_started",
            "preseal_capture_not_pressure_control",
            "preseal_capture_pressure_rise_expected_after_vent_close",
            "preseal_capture_monitor_armed_before_vent_close_command",
            "preseal_capture_monitor_covers_abort_path",
            "preseal_capture_abort_reason",
            "preseal_capture_abort_pressure_hpa",
            "preseal_capture_abort_source",
            "preseal_capture_abort_sample_age_s",
            "preseal_capture_ready_window_min_hpa",
            "preseal_capture_ready_window_max_hpa",
            "preseal_capture_ready_window_action",
            "preseal_capture_over_abort_action",
            "preseal_capture_predictive_ready_to_seal",
            "preseal_capture_pressure_rise_rate_hpa_per_s",
            "preseal_capture_estimated_time_to_target_s",
            "preseal_capture_seal_completion_latency_s",
            "preseal_capture_predicted_seal_completion_pressure_hpa",
            "preseal_capture_predictive_trigger_reason",
            "preseal_abort_source_path",
            "positive_preseal_pressure_source_path",
            "positive_preseal_pressure_missing_reason",
            "first_over_1100_before_vent_close",
            "first_over_1100_not_actionable_reason",
            "high_pressure_first_point_abort_pressure_hpa",
            "high_pressure_first_point_abort_reason",
            "monitor_context_propagated_to_wrapper_summary",
            "preseal_guard_armed",
            "preseal_guard_armed_at",
            "preseal_guard_arm_source",
            "preseal_guard_armed_from_vent_close_command",
            "vent_close_to_preseal_guard_arm_latency_s",
            "vent_close_to_positive_preseal_start_latency_s",
            "vent_off_settle_wait_pressure_monitored",
            "vent_off_settle_wait_overlimit_seen",
            "vent_off_settle_wait_ready_to_seal_seen",
            "first_target_ready_to_seal_min_hpa",
            "first_target_ready_to_seal_max_hpa",
            "first_target_ready_to_seal_pressure_hpa",
            "first_target_ready_to_seal_elapsed_s",
            "first_target_ready_to_seal_before_abort",
            "first_target_ready_to_seal_missed",
            "first_target_ready_to_seal_missed_reason",
            "first_over_abort_pressure_hpa",
            "first_over_abort_elapsed_s",
            "first_over_abort_source",
            "first_over_abort_sample_age_s",
            "first_over_abort_to_abort_latency_s",
            "positive_preseal_guard_started_before_first_over_abort",
            "positive_preseal_guard_started_after_first_over_abort",
            "positive_preseal_guard_late_reason",
            "seal_command_allowed_after_atmosphere_vent_closed",
            "seal_command_blocked_reason",
            "pressure_control_started_after_seal_confirmed",
            "setpoint_command_blocked_before_seal",
            "output_enable_blocked_before_seal",
            "normal_atmosphere_vent_attempted_after_pressure_points_started",
            "normal_atmosphere_vent_blocked_after_pressure_points_started",
            "emergency_relief_after_pressure_control_is_abort_only",
            "resume_after_emergency_relief_allowed",
            "seal_command_sent",
            "pressure_setpoint_command_sent",
            "sampling_started",
        ):
            if key in actual_data:
                updated[key] = actual_data.get(key)
        updated["route_conditioning_phase"] = "positive_preseal_pressurization"
        if pressure_hpa is not None:
            updated["emergency_abort_relief_pressure_hpa"] = float(pressure_hpa)
        if overlimit:
            updated.update(
                {
                    "emergency_abort_relief_vent_required": True,
                    "emergency_abort_relief_reason": "positive_preseal_abort_pressure_exceeded",
                }
            )
        self.a2_hooks.co2_route_conditioning_at_atmosphere_context = updated
        return updated

    def _a2_conditioning_emergency_abort_relief_decision(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_emergency_abort_relief_decision(*args, **kwargs)

    def _a2_conditioning_cleanup_relief_decision(
        self,
        context: Mapping[str, Any],
        *,
        reason: str = "",
        vent_classification: str = "safe_stop_relief",
    ) -> dict[str, Any]:
        classification = str(vent_classification or "safe_stop_relief").strip() or "safe_stop_relief"
        phase = str(context.get("route_conditioning_phase") or "unknown")
        blocked_reason = self._a2_conditioning_relief_mix_risk_reason(context)
        allowed = not bool(blocked_reason)
        updated = dict(context)
        updated.update(
            {
                "cleanup_vent_requested": True,
                "cleanup_vent_classification": classification,
                "cleanup_vent_phase": phase,
                "cleanup_vent_reason": reason,
                "cleanup_vent_allowed": allowed,
                "cleanup_vent_blocked_reason": blocked_reason,
                "cleanup_vent_is_normal_maintenance": False,
                "cleanup_vent_is_safe_stop_relief": True,
                "safe_stop_relief_required": True,
                "safe_stop_relief_allowed": allowed,
                "safe_stop_relief_command_sent": allowed,
                "safe_stop_relief_blocked_reason": blocked_reason,
                "normal_maintenance_vent_blocked_after_flush_phase": False,
                "vent_blocked_after_flush_phase_is_failure": False,
                "vent_blocked_after_flush_phase_context": {
                    "phase": phase,
                    "classification": classification,
                    "blocked_reason": blocked_reason,
                },
                "safe_stop_pressure_relief_result": "command_sent"
                if allowed
                else f"blocked:{blocked_reason}",
            }
        )
        self.a2_hooks.co2_route_conditioning_at_atmosphere_context = updated
        payload = {
            "cleanup_vent_requested": True,
            "cleanup_vent_classification": classification,
            "cleanup_vent_phase": phase,
            "cleanup_vent_reason": reason,
            "cleanup_vent_allowed": allowed,
            "cleanup_vent_blocked_reason": blocked_reason,
            "cleanup_vent_is_normal_maintenance": False,
            "cleanup_vent_is_safe_stop_relief": True,
            "safe_stop_relief_required": True,
            "safe_stop_relief_allowed": allowed,
            "safe_stop_relief_command_sent": allowed,
            "safe_stop_relief_blocked_reason": blocked_reason,
            "normal_maintenance_vent_blocked_after_flush_phase": False,
            "vent_blocked_after_flush_phase_is_failure": False,
            "vent_blocked_after_flush_phase_context": updated["vent_blocked_after_flush_phase_context"],
            "safe_stop_pressure_relief_result": updated["safe_stop_pressure_relief_result"],
        }
        if not allowed:
            payload.update(
                {
                    "vent_command_blocked": True,
                    "reason": reason,
                }
            )
        return payload

    def _guard_a2_conditioning_vent_command(
        self,
        *,
        reason: str = "",
        vent_classification: str = "normal_maintenance_vent",
        emergency_abort_relief: bool = False,
        relief_context: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        vent_classification = str(vent_classification or "normal_maintenance_vent").strip()
        context = self.a2_hooks.co2_route_conditioning_at_atmosphere_context
        if not context:
            return {}
        if emergency_abort_relief or vent_classification == "emergency_abort_relief":
            return self._a2_conditioning_emergency_abort_relief_decision(
                context,
                reason=reason,
                relief_context=relief_context,
            )
        if vent_classification in {"cleanup_relief", "safe_stop_relief"}:
            return self._a2_conditioning_cleanup_relief_decision(
                context,
                reason=reason,
                vent_classification=vent_classification,
            )
        blocked_reason = self._a2_conditioning_unsafe_vent_reason(context)
        if not blocked_reason:
            return {}
        # A2.35: seal/phase transition is expected after flush — not a failure.
        if blocked_reason in {
            "seal_command_sent",
            "pressure_setpoint_command_sent",
            "route_conditioning_phase_not_flush",
            "ready_to_seal_phase_started",
        }:
            return {"vent_command_blocked": True, "vent_pulse_blocked_reason": blocked_reason}
        context = self._a2_conditioning_mark_vent_blocked(context, reason=blocked_reason)
        return {
            "vent_command_blocked": True,
            "vent_pulse_blocked_after_flush_phase": True,
            "vent_pulse_blocked_reason": blocked_reason,
            "attempted_unsafe_vent_after_seal_or_pressure_control": context.get(
                "attempted_unsafe_vent_after_seal_or_pressure_control",
                False,
            ),
            "unsafe_vent_after_seal_or_pressure_control_command_sent": False,
            "normal_maintenance_vent_blocked_after_flush_phase": True,
            "cleanup_vent_classification": "normal_maintenance_vent",
            "cleanup_vent_requested": True,
            "cleanup_vent_phase": str(context.get("route_conditioning_phase") or "unknown"),
            "cleanup_vent_reason": reason,
            "cleanup_vent_allowed": False,
            "cleanup_vent_blocked_reason": blocked_reason,
            "cleanup_vent_is_normal_maintenance": True,
            "cleanup_vent_is_safe_stop_relief": False,
            "safe_stop_relief_required": False,
            "safe_stop_relief_allowed": False,
            "safe_stop_relief_command_sent": False,
            "safe_stop_relief_blocked_reason": blocked_reason,
            "vent_blocked_after_flush_phase_is_failure": True,
            "vent_blocked_after_flush_phase_context": context.get("vent_blocked_after_flush_phase_context"),
            "reason": reason,
        }

    def _a2_conditioning_first_float(self, *values: Any) -> Optional[float]:
        for value in values:
            parsed = self._as_float(value)
            if parsed is not None:
                return float(parsed)
        return None

    def _a2_route_open_transient_evidence(self, context: Mapping[str, Any]) -> dict[str, Any]:
        keys = (
            "measured_atmospheric_pressure_hpa",
            "measured_atmospheric_pressure_source",
            "measured_atmospheric_pressure_sample_age_s",
            "route_conditioning_pressure_before_route_open_hpa",
            "route_open_transient_window_enabled",
            "route_open_transient_peak_pressure_hpa",
            "route_open_transient_peak_time_ms",
            "route_open_transient_recovery_required",
            "route_open_transient_recovered_to_atmosphere",
            "route_open_transient_recovery_time_ms",
            "route_open_transient_recovery_target_hpa",
            "route_open_transient_recovery_band_hpa",
            "route_open_transient_stable_hold_s",
            "route_open_transient_stable_pressure_mean_hpa",
            "route_open_transient_stable_pressure_span_hpa",
            "route_open_transient_stable_pressure_slope_hpa_per_s",
            "route_open_transient_accepted",
            "route_open_transient_rejection_reason",
            "route_open_transient_evaluation_state",
            "route_open_transient_interrupted_by_vent_gap",
            "route_open_transient_interrupted_reason",
            "route_open_transient_summary_source",
            "sustained_pressure_rise_after_route_open",
            "pressure_rise_despite_valid_vent_scheduler",
            "route_conditioning_hard_abort_pressure_hpa",
            "route_conditioning_hard_abort_exceeded",
        )
        return {key: context.get(key) for key in keys if key in context}

    def _a2_route_open_transient_mark_interrupted_by_vent_gap(
        self,
        context: Mapping[str, Any],
        *,
        reason: str = "vent_gap_exceeded_before_recovery_evaluation",
    ) -> dict[str, Any]:
        updated = dict(context)
        if not bool(updated.get("route_open_transient_window_enabled", True)):
            updated["route_open_transient_evaluation_state"] = "not_started"
            return updated
        route_open_monotonic = self._as_float(
            updated.get("route_open_completed_monotonic_s")
            or self.a2_hooks.co2_route_open_monotonic_s
        )
        if route_open_monotonic is None:
            return updated
        if bool(updated.get("route_open_transient_accepted", False)):
            return updated
        if bool(updated.get("route_conditioning_hard_abort_exceeded", False)):
            updated["route_open_transient_evaluation_state"] = "hard_abort"
            return updated
        reason_text = str(reason or "vent_gap_exceeded_before_recovery_evaluation")
        updated["route_open_transient_evaluation_state"] = "interrupted_by_vent_gap"
        updated["route_open_transient_interrupted_by_vent_gap"] = True
        updated["route_open_transient_interrupted_reason"] = reason_text
        updated["route_open_transient_rejection_reason"] = str(
            updated.get("route_open_transient_rejection_reason") or reason_text
        )
        updated["route_open_transient_summary_source"] = "route_conditioning_vent_gap"
        return updated

    def _a2_route_open_transient_mark_continuing_after_defer_warning(
        self,
        context: Mapping[str, Any],
    ) -> dict[str, Any]:
        updated = dict(context)
        if not bool(updated.get("route_open_transient_window_enabled", True)):
            updated["route_open_transient_evaluation_state"] = "not_started"
            return updated
        route_open_monotonic = self._as_float(
            updated.get("route_open_completed_monotonic_s")
            or self.a2_hooks.co2_route_open_monotonic_s
        )
        if route_open_monotonic is None:
            return updated
        if bool(updated.get("route_open_transient_accepted", False)):
            return updated
        if bool(updated.get("route_conditioning_hard_abort_exceeded", False)):
            updated["route_open_transient_evaluation_state"] = "hard_abort"
            return updated
        if bool(updated.get("vent_gap_exceeded_after_defer", False)):
            return updated
        updated["route_open_transient_evaluation_state"] = "continuing_after_defer_warning"
        updated["route_open_transient_interrupted_by_vent_gap"] = False
        updated["route_open_transient_interrupted_reason"] = ""
        if updated.get("route_open_transient_rejection_reason") == "vent_gap_exceeded_before_recovery_evaluation":
            updated["route_open_transient_rejection_reason"] = ""
        updated.setdefault("route_open_transient_rejection_reason", "")
        updated["route_open_transient_summary_source"] = "route_conditioning_defer_latency_warning"
        return updated

    def _a2_route_open_transient_target_hpa(self, context: Mapping[str, Any]) -> Optional[float]:
        return self._a2_conditioning_first_float(
            context.get("measured_atmospheric_pressure_hpa"),
            context.get("route_conditioning_pressure_before_route_open_hpa"),
            context.get("route_open_transient_recovery_target_hpa"),
            context.get("route_open_pressure_hpa"),
            self.a2_hooks.co2_route_open_pressure_hpa,
        )

    def _a2_route_open_transient_update(self, *args, **kwargs):
        return self.conditioning_service._a2_route_open_transient_update(*args, **kwargs)

    def _a2_conditioning_update_pressure_metrics(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_update_pressure_metrics(*args, **kwargs)

    def _a2_conditioning_context_with_counts(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_context_with_counts(*args, **kwargs)

    def _a2_conditioning_terminal_gap_details(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_terminal_gap_details(*args, **kwargs)

    def _a2_conditioning_terminal_gap_source(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_terminal_gap_source(*args, **kwargs)

    def _a2_conditioning_vent_gap_source(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_vent_gap_source(*args, **kwargs)

    def _a2_conditioning_fail_if_defer_not_rescheduled(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_fail_if_defer_not_rescheduled(*args, **kwargs)

    def _a2_conditioning_reschedule_after_defer(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_reschedule_after_defer(*args, **kwargs)

    def _a2_conditioning_heartbeat_gap_state(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_heartbeat_gap_state(*args, **kwargs)

    def _a2_conditioning_failure_context(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_failure_context(*args, **kwargs)

    def _fail_a2_co2_route_conditioning_closed(self, *args, **kwargs):
        return self.conditioning_service._fail_a2_co2_route_conditioning_closed(*args, **kwargs)

    def _a2_v1_aligned_pressure_fallback_allowed(self) -> bool:
        context = self.a2_hooks.co2_route_conditioning_at_atmosphere_context
        return bool(
            getattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", False)
            and context.get("atmosphere_vent_enabled", True)
            and not bool(context.get("seal_command_sent", False))
            and not str(context.get("vent_off_sent_at") or "").strip()
            and not bool(context.get("pressure_setpoint_command_sent", False))
        )

    def _a2_conditioning_pressure_sample(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_pressure_sample(*args, **kwargs)

    def _a2_conditioning_pressure_details(self, *args, **kwargs):
        return self.conditioning_service._a2_conditioning_pressure_details(*args, **kwargs)

    def _a2_conditioning_digital_gauge_evidence(self, details: Mapping[str, Any]) -> dict[str, Any]:
        keys = (
            "digital_gauge_continuous_mode",
            "digital_gauge_continuous_started",
            "digital_gauge_continuous_active",
            "digital_gauge_stream_first_frame_at",
            "digital_gauge_stream_last_frame_at",
            "digital_gauge_latest_sequence_id",
            "continuous_stream_stale",
            "continuous_stream_age_s",
            "digital_gauge_stream_stale",
            "digital_gauge_stream_stale_threshold_s",
            "digital_gauge_drain_empty_count",
            "digital_gauge_drain_nonempty_count",
            "last_pressure_command",
            "last_pressure_command_may_cancel_continuous",
            "continuous_interrupted_by_command",
            "continuous_restart_required_before_return_to_continuous",
            "continuous_restart_attempted",
            "continuous_restart_result",
            "pressure_source_selected",
            "pressure_source_selection_reason",
            "selected_pressure_source",
            "selected_pressure_sample_age_s",
            "selected_pressure_sample_is_stale",
            "selected_pressure_parse_ok",
            "selected_pressure_freshness_ok",
            "pressure_freshness_decision_source",
            "selected_pressure_fail_closed_reason",
            "selected_pressure_sample_stale_duration_ms",
            "selected_pressure_sample_stale_budget_ms",
            "selected_pressure_sample_stale_budget_exceeded",
            "selected_pressure_sample_stale_performed_io",
            "selected_pressure_sample_stale_triggered_source_selection",
            "selected_pressure_sample_stale_triggered_p3_fallback",
            "selected_pressure_sample_stale_deferred_for_vent_priority",
            "conditioning_monitor_latest_frame_age_s",
            "conditioning_monitor_latest_frame_fresh",
            "conditioning_monitor_latest_frame_unavailable",
            "conditioning_monitor_pressure_deferred_count",
            "conditioning_monitor_pressure_deferred_elapsed_ms",
            "conditioning_monitor_max_defer_ms",
            "conditioning_monitor_pressure_stale_timeout",
            "conditioning_monitor_pressure_unavailable_fail_closed",
            "continuous_latest_fresh_fast_path_used",
            "continuous_latest_fresh_duration_ms",
            "continuous_latest_fresh_lock_acquire_ms",
            "continuous_latest_fresh_lock_timeout",
            "continuous_latest_fresh_waited_for_frame",
            "continuous_latest_fresh_performed_io",
            "continuous_latest_fresh_triggered_stream_restart",
            "continuous_latest_fresh_triggered_drain",
            "continuous_latest_fresh_triggered_p3_fallback",
            "continuous_latest_fresh_budget_ms",
            "continuous_latest_fresh_budget_exceeded",
            "a2_3_pressure_source_strategy",
            "critical_window_uses_latest_frame",
            "critical_window_uses_query",
            "p3_fast_fallback_attempted",
            "p3_fast_fallback_result",
            "normal_p3_fallback_attempted",
            "normal_p3_fallback_result",
            "fail_closed_reason",
            "route_conditioning_hard_abort_pressure_hpa",
            "route_conditioning_hard_abort_exceeded",
        )
        return {key: details.get(key) for key in keys if key in details}

    def _record_a2_co2_conditioning_pressure_monitor(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
    ) -> dict[str, Any]:
        return self.conditioning_service._record_a2_co2_conditioning_pressure_monitor(point, phase=phase)


    def _begin_a2_co2_route_open_transition(self, point: CalibrationPoint) -> dict[str, Any]:
        if not self.a2_hooks.co2_route_conditioning_at_atmosphere_active:
            return {}
        context = self.a2_hooks.co2_route_conditioning_at_atmosphere_context
        if not context:
            return {}
        now_mono = time.monotonic()
        now_at = datetime.now(timezone.utc).isoformat()
        context.update(
            {
                "route_open_transition_started": True,
                "route_open_transition_completed": False,
                "route_open_transition_started_at": now_at,
                "route_open_transition_started_monotonic_s": now_mono,
                "route_open_settle_wait_sliced": False,
                "route_open_settle_wait_slice_count": 0,
                "route_open_settle_wait_total_ms": 0.0,
                "vent_ticks_during_route_open_transition": 0,
                "route_open_transition_max_vent_write_gap_ms": None,
                "route_open_transition_terminal_vent_write_age_ms": None,
                "route_open_transition_blocked_vent_scheduler": False,
                "route_open_settle_wait_blocked_vent_scheduler": False,
                "route_conditioning_vent_gap_exceeded_source": "",
                "_route_open_transition_last_vent_write_sent_monotonic_s": None,
            }
        )
        self.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
        self._record_a2_conditioning_workflow_timing(
            context,
            "co2_route_open_transition_start",
            "start",
            stage="co2_route_open_transition",
            point=point,
            route_state=context,
        )
        return context

    def _mark_a2_co2_route_open_command_write_started(self, point: CalibrationPoint) -> dict[str, Any]:
        context = self.a2_hooks.co2_route_conditioning_at_atmosphere_context
        if not context:
            return {}
        now_mono = time.monotonic()
        now_at = datetime.now(timezone.utc).isoformat()
        context.update(
            {
                "route_open_command_write_started_at": now_at,
                "route_open_command_write_started_monotonic_s": now_mono,
                "route_open_started_at": now_at,
            }
        )
        self.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
        self._record_a2_conditioning_workflow_timing(
            context,
            "co2_route_open_command_write_start",
            "start",
            stage="co2_route_open_transition",
            point=point,
            route_state=context,
        )
        return context

    def _mark_a2_co2_route_open_command_write_completed(self, point: CalibrationPoint) -> dict[str, Any]:
        context = self.a2_hooks.co2_route_conditioning_at_atmosphere_context
        if not context:
            return {}
        now_mono = time.monotonic()
        now_at = datetime.now(timezone.utc).isoformat()
        started = self._as_float(context.get("route_open_command_write_started_monotonic_s"))
        duration_ms = None if started is None else round(max(0.0, now_mono - float(started)) * 1000.0, 3)
        context.update(
            {
                "route_open_command_write_completed_at": now_at,
                "route_open_command_write_completed_monotonic_s": now_mono,
                "route_open_command_write_duration_ms": duration_ms,
                "route_open_completed_at": now_at,
                "route_open_completed_monotonic_s": now_mono,
            }
        )
        self.a2_hooks.co2_route_open_monotonic_s = now_mono
        self.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
        self._record_a2_conditioning_workflow_timing(
            context,
            "co2_route_open_command_write_end",
            "end",
            stage="co2_route_open_transition",
            point=point,
            duration_s=None if duration_ms is None else round(float(duration_ms) / 1000.0, 3),
            route_state=context,
        )
        return context

    def _fail_a2_route_open_transition_if_blocked(self, point: CalibrationPoint) -> None:
        context = self.a2_hooks.co2_route_conditioning_at_atmosphere_context
        if not context:
            return
        duration_ms = self._as_float(context.get("route_open_command_write_duration_ms"))
        threshold_s = self._a2_route_open_transition_block_threshold_s()
        if duration_ms is None or float(duration_ms) <= threshold_s * 1000.0:
            return
        now_mono = time.monotonic()
        details = {
            **context,
            **self._a2_conditioning_terminal_gap_details(
                context,
                now_mono=now_mono,
                max_gap_s=threshold_s,
                source="route_open_transition",
            ),
            "route_open_transition_blocked_vent_scheduler": True,
            "route_open_command_write_duration_ms": round(float(duration_ms), 3),
            "fail_closed_reason": "route_open_transition_blocked_vent_scheduler",
            "whether_safe_to_continue": False,
        }
        self._fail_a2_co2_route_conditioning_closed(
            point,
            reason="route_open_transition_blocked_vent_scheduler",
            details=details,
            event_name="co2_route_open_transition_blocked_vent_scheduler",
            route_trace_action="co2_route_open_transition_blocked_vent_scheduler",
        )

    def _wait_a2_co2_route_open_settle_before_conditioning(self, *args, **kwargs):
        return self.conditioning_service._wait_a2_co2_route_open_settle_before_conditioning(*args, **kwargs)

    def _complete_a2_co2_route_open_transition(self, point: CalibrationPoint) -> dict[str, Any]:
        context = self.a2_hooks.co2_route_conditioning_at_atmosphere_context
        if not context:
            return {}
        now_mono = time.monotonic()
        started = self._as_float(context.get("route_open_transition_started_monotonic_s"))
        if started is not None:
            context["route_open_transition_total_duration_ms"] = round(
                max(0.0, now_mono - float(started)) * 1000.0,
                3,
            )
        schedule = self._a2_conditioning_vent_schedule(context, now_mono=now_mono)
        terminal = self._a2_conditioning_terminal_gap_details(
            context,
            now_mono=now_mono,
            max_gap_s=float(schedule["route_conditioning_effective_max_gap_s"]),
            source="route_open_transition",
        )
        context["route_open_transition_terminal_vent_write_age_ms"] = terminal.get(
            "terminal_vent_write_age_ms_at_gap_gate"
        )
        context.update(
            {
                key: value
                for key, value in terminal.items()
                if key
                in {
                    "terminal_vent_write_age_ms_at_gap_gate",
                    "max_vent_pulse_write_gap_ms_including_terminal_gap",
                    "max_vent_scheduler_loop_gap_ms",
                }
            }
        )
        context["route_open_transition_completed"] = True
        context = self._a2_conditioning_context_with_counts(context)
        self.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
        self._record_a2_conditioning_workflow_timing(
            context,
            "co2_route_open_transition_end",
            "end",
            stage="co2_route_open_transition",
            point=point,
            duration_s=(
                None
                if context.get("route_open_transition_total_duration_ms") is None
                else round(float(context["route_open_transition_total_duration_ms"]) / 1000.0, 3)
            ),
            route_state=context,
        )
        return context

    def _record_a2_co2_conditioning_vent_tick(self, point: CalibrationPoint, *, phase: str) -> dict[str, Any]:
        return self.conditioning_service._record_a2_co2_conditioning_vent_tick(point, phase=phase)
    def _maybe_reassert_a2_conditioning_vent(self, *args, **kwargs):
        return self.conditioning_service._maybe_reassert_a2_conditioning_vent(*args, **kwargs)

    def _confirm_a2_co2_conditioning_before_route_open(self, point: CalibrationPoint) -> dict[str, Any]:
        if not self.a2_hooks.co2_route_conditioning_at_atmosphere_active:
            return {}
        return self._record_a2_co2_conditioning_vent_tick(point, phase="before_route_open_confirm")

    def _refresh_a2_co2_conditioning_after_route_open(self, point: CalibrationPoint) -> dict[str, Any]:
        if not self.a2_hooks.co2_route_conditioning_at_atmosphere_active:
            return {}
        context = self.a2_hooks.co2_route_conditioning_at_atmosphere_context
        route_open_monotonic = self._as_float(self.a2_hooks.co2_route_open_monotonic_s)
        if route_open_monotonic is not None:
            context["route_open_completed_monotonic_s"] = float(route_open_monotonic)
            context["route_open_completed_at"] = datetime.now(timezone.utc).isoformat()
            self.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
        return self._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")

    def _end_a2_co2_route_conditioning_at_atmosphere(self, *args, **kwargs):
        return self.conditioning_service._end_a2_co2_route_conditioning_at_atmosphere(*args, **kwargs)

    def _preseal_analyzer_gate_after_conditioning(
        self,
        point: CalibrationPoint,
        *,
        route_soak_ok: bool,
        route_soak_actual: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        self._record_workflow_timing(
            "preseal_analyzer_gate_start",
            "start",
            stage="preseal_analyzer_gate",
            point=point,
            route_state={"conditioning_completed": self.a2_hooks.co2_route_conditioning_completed},
        )
        passed = bool(route_soak_ok)
        if bool(getattr(self, "_gas_route_dewpoint_gate_enabled", lambda: False)()):
            self.status_service.log("CO2 preseal dewpoint gate passed")
        else:
            self.status_service.log("CO2 preseal analyzer stability check skipped")
        self._record_workflow_timing(
            "preseal_analyzer_gate_end",
            "end" if passed else "fail",
            stage="preseal_analyzer_gate",
            point=point,
            decision="PASS" if passed else "FAIL",
            route_state={
                "preseal_analyzer_gate_passed": passed,
                "conditioning_completed": self.a2_hooks.co2_route_conditioning_completed,
                "route_soak_actual": dict(route_soak_actual or {}),
            },
        )
        self.a2_hooks.preseal_analyzer_gate_passed = passed
        return passed

    def _prepare_a2_high_pressure_first_point_after_conditioning(
        self,
        point: CalibrationPoint,
        pressure_points: Optional[Iterable[CalibrationPoint]] = None,
    ) -> dict[str, Any]:
        if not self._a2_co2_route_conditioning_required(point, pressure_points):
            return {}
        if not self.a2_hooks.co2_route_conditioning_completed:
            details = {"conditioning_completed": False, "seal_preparation_blocked_reason": "conditioning_not_completed"}
            self._record_workflow_timing(
                "seal_preparation_after_conditioning_start",
                "fail",
                stage="seal_preparation_after_conditioning",
                point=point,
                decision="conditioning_not_completed",
                error_code="conditioning_not_completed",
                route_state=details,
            )
            raise WorkflowValidationError("A2 high-pressure first point requires completed CO2 route conditioning", details=details)
        conditioning_completed_at = self.a2_hooks.co2_route_conditioning_completed_at
        self._record_workflow_timing(
            "seal_preparation_after_conditioning_start",
            "start",
            stage="seal_preparation_after_conditioning",
            point=point,
            route_state={
                "conditioning_completed_before_high_pressure_mode": True,
                "conditioning_completed_at": conditioning_completed_at,
                "preseal_analyzer_gate_passed": self.a2_hooks.preseal_analyzer_gate_passed,
            },
        )
        context = self._prearm_a2_high_pressure_first_point_mode(point, pressure_points)
        if not bool(context.get("enabled")):
            return context
        context = dict(context)
        context.update(
            {
                "conditioning_completed_before_high_pressure_mode": True,
                "conditioning_completed_at": conditioning_completed_at,
                "preseal_analyzer_gate_passed": self.a2_hooks.preseal_analyzer_gate_passed,
                "sealed_after_conditioning": False,
            }
        )
        self.a2_hooks.high_pressure_first_point_context = context
        self._record_workflow_timing(
            "high_pressure_first_point_mode_start",
            "start",
            stage="high_pressure_first_point",
            point=point,
            target_pressure_hpa=context.get("first_target_pressure_hpa"),
            pressure_hpa=context.get("baseline_pressure_hpa"),
            decision="enabled_after_conditioning",
            route_state=context,
        )
        self._preclose_a2_high_pressure_first_point_vent(point)
        self._record_workflow_timing(
            "high_pressure_ready_wait_started_after_conditioning",
            "start",
            stage="high_pressure_first_point",
            point=point,
            target_pressure_hpa=context.get("first_target_pressure_hpa"),
            decision="wait_for_ready_pressure_after_conditioning",
            route_state=dict(self.a2_hooks.high_pressure_first_point_context or context),
        )
        self._request_a2_high_pressure_route_open_pressure_sample(point)
        return dict(self.a2_hooks.high_pressure_first_point_context or context)

    def _a2_prearm_route_conditioning_baseline_max_age_s(self) -> float:
        return self.conditioning_service._a2_prearm_route_conditioning_baseline_max_age_s()

    def _a2_prearm_baseline_freshness_max_s(self) -> float:
        return self.conditioning_service._a2_prearm_baseline_freshness_max_s()

    def _a2_prearm_baseline_atmosphere_band_hpa(self) -> float:
        return self.conditioning_service._a2_prearm_baseline_atmosphere_band_hpa()

    def _a2_prearm_baseline_sources(self, sample: Mapping[str, Any]) -> dict[str, Any]:
        pace_sample = sample.get("pace_pressure_sample")
        pace_sample = dict(pace_sample) if isinstance(pace_sample, Mapping) else {}
        sample_source = str(sample.get("pressure_sample_source") or sample.get("source") or "")
        selected_source = str(
            sample.get("pressure_source_selected")
            or sample.get("pressure_source_used_for_decision")
            or sample.get("pressure_source_used_for_abort")
            or ""
        )
        observed = selected_source or sample_source
        selection_reason = str(
            sample.get("pressure_source_selection_reason")
            or sample.get("source_selection_reason")
            or ""
        )
        disagreement = bool(
            sample.get("prearm_pressure_source_disagreement")
            or sample.get("pressure_source_disagreement")
            or "disagreement" in selection_reason.lower()
        )
        if disagreement and pace_sample.get("pressure_hpa") is not None:
            pace_source = str(pace_sample.get("pressure_sample_source") or pace_sample.get("source") or "pace_controller")
            observed = f"{sample_source or 'unknown'} vs {pace_source}"
        return {
            "observed": observed,
            "selected_source": selected_source,
            "sample_source": sample_source,
            "selection_reason": selection_reason,
            "disagreement": disagreement,
            "disagreement_reason": selection_reason if disagreement else "",
        }

    def _a2_latest_route_conditioning_prearm_baseline(self, *args, **kwargs):
        return self.conditioning_service._a2_latest_route_conditioning_prearm_baseline(*args, **kwargs)

    def _prearm_a2_high_pressure_first_point_mode(self, *args, **kwargs):
        return self.conditioning_service._prearm_a2_high_pressure_first_point_mode(*args, **kwargs)

    def _a2_preseal_capture_ready_window(
        self,
        point: CalibrationPoint,
        context: Mapping[str, Any],
    ) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
        first_target = self._as_float(context.get("first_target_pressure_hpa") or point.target_pressure_hpa)
        ready_pressure = self._as_float(
            self._cfg_get("workflow.pressure.preseal_ready_pressure_hpa", first_target)
        )
        abort_pressure = self._a2_preseal_capture_urgent_seal_threshold_hpa(ready_pressure)
        ready_window = getattr(self.pressure_control_service, "_first_target_ready_to_seal_window", None)
        if callable(ready_window):
            ready_min, ready_max = ready_window(
                target_pressure_hpa=first_target,
                ready_pressure_hpa=ready_pressure,
                abort_pressure_hpa=abort_pressure,
            )
        else:
            ready_min = first_target if first_target is not None else ready_pressure
            ready_max = None if ready_min is None else float(ready_min) + 12.0
            if abort_pressure is not None and ready_max is not None:
                ready_max = min(float(ready_max), float(abort_pressure) - 0.001)
        return first_target, ready_pressure, abort_pressure, ready_min, ready_max

    def _a2_preseal_capture_urgent_seal_threshold_hpa(
        self,
        ready_pressure_hpa: Optional[float] = None,
    ) -> Optional[float]:
        configured = self._as_float(
            self._cfg_get(
                "workflow.pressure.preseal_capture_urgent_seal_threshold_hpa",
                self._cfg_get(
                    "workflow.pressure.preseal_urgent_seal_threshold_hpa",
                    self._cfg_get("workflow.pressure.preseal_abort_pressure_hpa", None),
                ),
            )
        )
        if configured is not None:
            return float(configured)
        if ready_pressure_hpa is None:
            return None
        margin = self._as_float(
            self._cfg_get("workflow.pressure.preseal_abort_margin_hpa", 40.0)
        )
        return float(ready_pressure_hpa) + abs(float(40.0 if margin is None else margin))

    def _a2_preseal_capture_hard_abort_pressure_hpa(
        self,
        urgent_seal_threshold_hpa: Optional[float] = None,
    ) -> Optional[float]:
        configured = self._as_float(
            self._cfg_get(
                "workflow.pressure.preseal_capture_hard_abort_pressure_hpa",
                self._cfg_get("workflow.pressure.preseal_hard_abort_pressure_hpa", 1250.0),
            )
        )
        if configured is None:
            return None
        hard_abort = float(configured)
        if urgent_seal_threshold_hpa is not None:
            hard_abort = max(hard_abort, float(urgent_seal_threshold_hpa))
        return hard_abort

    def _a2_preseal_capture_seal_latency_s(self) -> float:
        return self.conditioning_service._a2_preseal_capture_seal_latency_s()

    def _a2_preseal_capture_arm_context(self, *args, **kwargs):
        return self.conditioning_service._a2_preseal_capture_arm_context(*args, **kwargs)

    def _a2_mark_preseal_capture_pressure(self, *args, **kwargs):
        return self.conditioning_service._a2_mark_preseal_capture_pressure(*args, **kwargs)

    def _get_latest_pressure_hpa(self) -> Optional[float]:
        return self.pressure_control_service._get_latest_pressure_hpa()

    def _apply_valve_states(self, open_valves):
        return self.valve_routing_service.apply_valve_states(open_valves or [])

    def _set_h2o_path(self, is_open: bool, point: Optional[CalibrationPoint] = None) -> None:
        self.valve_routing_service.set_h2o_path(is_open, point)

    def _enable_pressure_controller_output(self, *, reason: str = ""):
        return self.pressure_control_service.enable_pressure_controller_output(reason=reason)

    def _request_a2_high_pressure_route_open_pressure_sample(
        self,
        point: CalibrationPoint,
    ) -> Optional[float]:
        gauge = self._device("pressure_meter", "pressure_gauge")
        if gauge is None:
            self._log("A2 high-pressure first-point pressure sample skipped: no gauge device")
            return None
        pressure_hpa: Optional[float] = None
        read_method = "read_pressure"
        try:
            for method_name in ("read_pressure", "read_pressure_hpa", "get_pressure", "get_pressure_hpa"):
                method = getattr(gauge, method_name, None)
                if not callable(method):
                    continue
                read_method = method_name
                result = method()
                if isinstance(result, (int, float)):
                    pressure_hpa = float(result)
                elif isinstance(result, dict):
                    pressure_hpa = self._as_float(result.get("pressure_hpa"))
                if pressure_hpa is not None:
                    break
        except Exception as exc:
            self._log(f"A2 high-pressure first-point pressure read failed ({read_method}): {exc}")
        context = self.a2_hooks.high_pressure_first_point_context
        context["route_open_pressure_sample_hpa"] = pressure_hpa
        context["route_open_pressure_sample_read_method"] = read_method
        context["route_open_pressure_sample_attempted"] = True
        self.a2_hooks.high_pressure_first_point_context = context
        self._record_workflow_timing(
            "high_pressure_route_open_pressure_sample_read",
            "info",
            stage="high_pressure_first_point",
            point=point,
            pressure_hpa=pressure_hpa,
            decision="preseal_pressure_sample",
        )
        return pressure_hpa

    def _preclose_a2_high_pressure_first_point_vent(self, point: CalibrationPoint) -> dict[str, Any]:
        return self.pressure_control_service.preclose_vent_and_allow_seal(point)
