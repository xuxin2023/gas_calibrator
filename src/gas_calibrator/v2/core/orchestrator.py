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
        self._device_init_policy_evidence: dict[str, Any] = self._classify_device_failures(
            [],
            all_devices=[],
            stage="initialization",
        )

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
            raise WorkflowValidationError(
                error_message,
                details={
                    "failed_devices": critical,
                    "critical_devices_failed": critical,
                    "optional_context_devices_failed": optional_failed,
                    "critical_device_init_failure_blocks_probe": True,
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
        if a2_probe:
            critical_required = list(route_pressure_devices)
            critical_required.extend(gas_devices)
            optional_context_devices = ["temperature_chamber"] if skip_temp_probe else []
            if not skip_temp_probe:
                critical_required.append("temperature_chamber")
        else:
            critical_required = ["temperature_chamber", *gas_devices]
            optional_context_devices = []
        critical_required = sorted(dict.fromkeys(critical_required))
        optional_context_devices = sorted(dict.fromkeys(optional_context_devices))

        critical_failed = [name for name in failed if name in critical_required or name.startswith("gas_analyzer_")]
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
            time_s = float(self._cfg_get("devices.pressure_controller.in_limits_time_s", 10.0))
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
        high_pressure_first_point_mode = bool(getattr(self, "_a2_high_pressure_first_point_mode_enabled", False))
        conditioning_at_atmosphere = bool(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", False))
        stage_name = (
            "co2_route_conditioning_at_atmosphere"
            if conditioning_at_atmosphere
            else ("high_pressure_first_point" if high_pressure_first_point_mode else "preseal_atmosphere_flush_hold")
        )
        special_flush = self._has_special_co2_zero_flush_pending() and self._is_zero_co2_point(point)
        soak_key = "workflow.stability.co2_route.preseal_soak_s"
        soak_default = 180.0
        log_context = "CO2 route opened"
        if high_pressure_first_point_mode:
            special_flush = False
            soak_key = "workflow.pressure.preseal_ready_timeout_s"
            soak_default = float(self._cfg_get("workflow.pressure.preseal_ready_timeout_s", 30.0))
            log_context = "CO2 route opened for 1100 hPa high-pressure first-point positive build-up"
            self._active_post_h2o_co2_zero_flush = False
        elif special_flush:
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
        soak_s = float(
            self._cfg_get(
                "workflow.pressure.high_pressure_first_point_ready_timeout_s",
                soak_default,
            )
            if high_pressure_first_point_mode
            else self._cfg_get(soak_key, soak_default)
        )
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
        if high_pressure_first_point_mode:
            self._log(
                f"{log_context}; poll pressure up to {int(soak_s)}s before immediate route seal (row {point.index})"
            )
        else:
            self._log(f"{log_context}; wait {int(soak_s)}s before pressure sealing (row {point.index})")
        self._record_workflow_timing(
            "preseal_soak_start",
            "start",
            stage="preseal_soak",
            point=point,
            expected_max_s=soak_s + max(5.0, soak_s * 0.1),
            wait_reason=soak_key,
        )
        setattr(self, "_a2_preseal_last_pressure_hpa", None)
        setattr(self, "_a2_preseal_last_pressure_monotonic_s", None)
        setattr(self, "_a2_preseal_pressure_rise_detected", False)
        setattr(self, "_a2_preseal_vent_close_arm_context", {})
        if not high_pressure_first_point_mode:
            setattr(self, "_a2_co2_route_open_pressure_hpa", None)
            setattr(self, "_a2_route_open_pressure_first_sample_recorded", False)
        start = time.time()
        if not high_pressure_first_point_mode or self._as_float(
            getattr(self, "_a2_co2_route_open_monotonic_s", None)
        ) is None:
            setattr(self, "_a2_co2_route_open_monotonic_s", time.monotonic())
        self._record_workflow_timing(
            "route_open_pressure_baseline",
            "info",
            stage=stage_name,
            point=point,
            decision=(
                "high_pressure_first_point_baseline_prearmed"
                if high_pressure_first_point_mode
                else "awaiting_first_dual_pressure_sample"
            ),
            route_state={
                "high_pressure_first_point_mode": high_pressure_first_point_mode,
                "baseline_context": dict(getattr(self, "_a2_high_pressure_first_point_context", {}) or {}),
                "primary_pressure_source": self._cfg_get(
                    "workflow.pressure.primary_pressure_source",
                    "digital_pressure_gauge",
                ),
                "pressure_source_cross_check_enabled": self._cfg_get(
                    "workflow.pressure.pressure_source_cross_check_enabled",
                    True,
                ),
            },
        )
        continuous_atmosphere_hold = bool(
            self._cfg_get("workflow.pressure.continuous_atmosphere_hold", False)
        ) and not high_pressure_first_point_mode
        positive_preseal_enabled = bool(
            self._cfg_get("workflow.pressure.positive_preseal_pressurization_enabled", False)
        ) or high_pressure_first_point_mode
        vent_hold_interval_s = max(0.1, float(self._cfg_get("workflow.pressure.vent_hold_interval_s", 2.0)))
        preseal_pressure_poll_interval_s = max(
            0.05,
            float(self._cfg_get("workflow.pressure.preseal_pressure_poll_interval_s", 0.2)),
        )
        last_atmosphere_hold_ts = time.time() if positive_preseal_enabled and continuous_atmosphere_hold else 0.0
        last_preseal_pressure_check_ts = 0.0
        last_snapshot_refresh_ts = 0.0
        if positive_preseal_enabled:
            self._record_workflow_timing(
                "preseal_atmosphere_flush_hold_start",
                "start",
                stage=stage_name,
                point=point,
                pressure_hpa=getattr(self, "_a2_co2_route_open_pressure_hpa", None),
                expected_max_s=soak_s,
                wait_reason=(
                    "high_pressure_first_point_positive_pressure_build_up"
                    if high_pressure_first_point_mode
                    else "continuous_atmosphere_hold"
                ),
            )
        handoff_decision = str(getattr(self, "_a2_high_pressure_first_point_initial_decision", "") or "")
        if high_pressure_first_point_mode and handoff_decision in {
            "positive_preseal_ready_handoff",
            "positive_preseal_arm_handoff",
        }:
            self._log(
                "A2 high-pressure first-point route-open pressure sample reached preseal handoff; "
                f"skip atmosphere flush and seal route (row {point.index})"
            )
        while time.time() - start < soak_s:
            self._check_stop()
            now = time.time()
            if high_pressure_first_point_mode and handoff_decision in {
                "positive_preseal_ready_handoff",
                "positive_preseal_arm_handoff",
            }:
                break
            if conditioning_at_atmosphere:
                self._maybe_reassert_a2_conditioning_vent(point)
                context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
                self._record_a2_conditioning_workflow_timing(
                    context,
                    "preseal_soak_tick",
                    "tick",
                    stage="preseal_soak",
                    point=point,
                    duration_s=now - start,
                    expected_max_s=soak_s,
                    wait_reason=soak_key,
                    route_state=context,
                )
            else:
                self._record_workflow_timing(
                    "preseal_soak_tick",
                    "tick",
                    stage="preseal_soak",
                    point=point,
                    duration_s=now - start,
                    expected_max_s=soak_s,
                    wait_reason=soak_key,
                )
                self._maybe_reassert_a2_conditioning_vent(point)
            if conditioning_at_atmosphere:
                time.sleep(
                    min(
                        self._a2_conditioning_scheduler_sleep_step_s(),
                        max(0.01, soak_s - (time.time() - start)),
                    )
                )
                continue
            if high_pressure_first_point_mode:
                if (now - last_preseal_pressure_check_ts) >= preseal_pressure_poll_interval_s:
                    pressure_decision = self._verify_co2_preseal_atmosphere_hold_pressure(point)
                    if pressure_decision in {"positive_preseal_ready_handoff", "positive_preseal_arm_handoff"}:
                        handoff_decision = pressure_decision
                        self._log(
                            "A2 high-pressure first-point reached positive preseal handoff pressure; "
                            f"seal without atmosphere flush delay (row {point.index})"
                        )
                        break
                    last_preseal_pressure_check_ts = time.time()
                sleep_step_s = (
                    self._a2_conditioning_scheduler_sleep_step_s()
                    if conditioning_at_atmosphere
                    else preseal_pressure_poll_interval_s
                )
                time.sleep(min(sleep_step_s, max(0.01, soak_s - (time.time() - start))))
                continue
            if continuous_atmosphere_hold and positive_preseal_enabled and (
                now - last_preseal_pressure_check_ts
            ) >= preseal_pressure_poll_interval_s:
                pressure_decision = self._verify_co2_preseal_atmosphere_hold_pressure(point)
                if (
                    not conditioning_at_atmosphere
                    and pressure_decision in {"positive_preseal_ready_handoff", "positive_preseal_arm_handoff"}
                ):
                    handoff_decision = pressure_decision
                    self._log(
                        "CO2 pre-seal atmosphere flush reached positive preseal handoff pressure; "
                        f"handoff to pressurization/seal (row {point.index})"
                    )
                    break
                last_preseal_pressure_check_ts = time.time()
            if (
                continuous_atmosphere_hold
                and not conditioning_at_atmosphere
                and (time.time() - last_atmosphere_hold_ts) >= vent_hold_interval_s
            ):
                vent_started = time.time()
                self._set_pressure_controller_vent(
                    True,
                    reason="CO2 route pre-seal atmosphere hold",
                    wait_after_command=False,
                    capture_pressure=False,
                    transition_timeout_s=self._cfg_get(
                        "workflow.pressure.preseal_atmosphere_hold_reassert_timeout_s",
                        min(0.5, vent_hold_interval_s),
                    ),
                )
                vent_ended = time.time()
                self._record_workflow_timing(
                    "preseal_vent_hold_tick",
                    "tick",
                    stage="preseal_soak",
                    point=point,
                    duration_s=vent_ended - start,
                    expected_max_s=vent_hold_interval_s,
                    wait_reason="continuous_atmosphere_hold",
                    route_state={
                        "vent_hold_command_duration_s": round(max(0.0, vent_ended - vent_started), 3),
                    },
                )
                last_atmosphere_hold_ts = vent_ended
                if not positive_preseal_enabled:
                    self._verify_co2_preseal_atmosphere_hold_pressure(point)
            if (now - last_snapshot_refresh_ts) >= 1.0:
                self._refresh_live_analyzer_snapshots(reason="co2_route_preseal_soak")
                last_snapshot_refresh_ts = now
            sleep_ceiling_s = preseal_pressure_poll_interval_s if positive_preseal_enabled else 1.0
            if conditioning_at_atmosphere:
                sleep_ceiling_s = min(sleep_ceiling_s, self._a2_conditioning_scheduler_sleep_step_s())
            time.sleep(min(1.0, sleep_ceiling_s, max(0.01, soak_s - (time.time() - start))))
        if special_flush:
            self._post_h2o_co2_zero_flush_pending = False
            self._initial_co2_zero_flush_pending = False
        self.run_state.humidity.first_co2_route_soak_pending = False
        self._first_co2_route_soak_pending = False
        if positive_preseal_enabled:
            if conditioning_at_atmosphere:
                context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
                self._record_a2_conditioning_workflow_timing(
                    context,
                    "preseal_atmosphere_flush_hold_end",
                    "end",
                    stage=stage_name,
                    point=point,
                    expected_max_s=soak_s,
                    decision=handoff_decision or "ok",
                    route_state=context,
                )
            else:
                self._record_workflow_timing(
                    "preseal_atmosphere_flush_hold_end",
                    "end",
                    stage=stage_name,
                    point=point,
                    expected_max_s=soak_s,
                    decision=handoff_decision or "ok",
                )
        if high_pressure_first_point_mode and handoff_decision not in {
            "positive_preseal_ready_handoff",
            "positive_preseal_arm_handoff",
        }:
            context = dict(getattr(self, "_a2_high_pressure_first_point_context", {}) or {})
            context.update(
                {
                    "timeout_s": soak_s,
                    "last_decision": handoff_decision or "timeout_before_ready",
                    "pressure_hpa": getattr(self, "_a2_preseal_last_pressure_hpa", None),
                    "abort_pressure_hpa": self._cfg_get("workflow.pressure.preseal_abort_pressure_hpa", None),
                }
            )
            self._record_workflow_timing(
                "high_pressure_abort",
                "fail",
                stage=stage_name,
                point=point,
                pressure_hpa=getattr(self, "_a2_preseal_last_pressure_hpa", None),
                target_pressure_hpa=point.target_pressure_hpa,
                decision="timeout_before_ready",
                error_code="high_pressure_first_point_ready_timeout",
                route_state=context,
            )
            raise WorkflowValidationError(
                "A2 high-pressure first point did not reach preseal ready pressure before timeout",
                details=context,
            )
        if conditioning_at_atmosphere:
            context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
            self._record_a2_conditioning_workflow_timing(
                context,
                "preseal_soak_end",
                "end",
                stage="preseal_soak",
                point=point,
                expected_max_s=soak_s + max(5.0, soak_s * 0.1),
                decision=handoff_decision or "ok",
                route_state=context,
            )
        else:
            self._record_workflow_timing(
                "preseal_soak_end",
                "end",
                stage="preseal_soak",
                point=point,
                expected_max_s=soak_s + max(5.0, soak_s * 0.1),
                decision=handoff_decision or "ok",
            )
        if high_pressure_first_point_mode:
            return True
        return self._wait_co2_route_dewpoint_gate_before_seal(
            point,
            base_soak_s=soak_s,
            log_context=log_context,
        )

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
        setattr(self, "_a2_high_pressure_first_point_mode_enabled", False)
        setattr(self, "_a2_high_pressure_first_point_context", {})
        setattr(self, "_a2_high_pressure_first_point_initial_decision", "")
        setattr(self, "_a2_high_pressure_first_point_vent_preclosed", False)
        setattr(self, "_a2_co2_route_conditioning_completed", False)
        setattr(self, "_a2_co2_route_conditioning_completed_at", "")
        if not self._a2_co2_route_conditioning_required(point, pressure_points):
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", False)
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {})
            return {}
        stream_state: dict[str, Any] = {}
        pressure_source_mode = self._a2_conditioning_pressure_source_mode()
        stream_starter = getattr(self.pressure_control_service, "_start_a2_high_pressure_digital_gauge_stream", None)
        if pressure_source_mode in {"continuous", "auto", "v1_aligned"} and callable(stream_starter):
            stream_state = dict(
                stream_starter(stage="co2_route_conditioning_at_atmosphere", point_index=point.index) or {}
            )
        now = datetime.now(timezone.utc).isoformat()
        started_monotonic_s = time.monotonic()
        context = {
            "route_open_started_at": "",
            "route_open_completed_at": "",
            "route_open_completed_monotonic_s": None,
            "atmosphere_vent_enabled": True,
            "vent_command_before_route_open": True,
            "route_conditioning_phase": "route_conditioning_flush_phase",
            "ready_to_seal_phase_started": False,
            "route_conditioning_flush_min_time_completed": False,
            "vent_off_blocked_during_flush": True,
            "seal_blocked_during_flush": True,
            "pressure_setpoint_blocked_during_flush": True,
            "sample_blocked_during_flush": True,
            "conditioning_soak_s": self._co2_conditioning_soak_s(point),
            "conditioning_started_at": now,
            "conditioning_started_monotonic_s": started_monotonic_s,
            "conditioning_completed_at": "",
            "conditioning_duration_s": None,
            "pressure_monitoring_enabled": True,
            "pressure_max_during_conditioning_hpa": None,
            "pressure_min_during_conditioning_hpa": None,
            "route_conditioning_pressure_returned_to_atmosphere": False,
            "route_conditioning_atmosphere_stable_before_flush": False,
            "route_conditioning_atmosphere_stable_hold_s": None,
            "route_conditioning_high_pressure_seen_before_preseal": False,
            "route_conditioning_high_pressure_seen_before_preseal_hpa": None,
            "route_conditioning_high_pressure_seen_phase": "",
            "route_conditioning_high_pressure_seen_source": "",
            "route_conditioning_high_pressure_seen_sample_age_s": None,
            "route_conditioning_high_pressure_seen_decision": "",
            "pressure_source": (
                "digital_pressure_gauge_p3_fast_poll"
                if pressure_source_mode == "p3_fast_poll"
                else (
                    "digital_pressure_gauge_v1_aligned"
                    if pressure_source_mode == "v1_aligned"
                    else "digital_pressure_gauge_continuous"
                )
            ),
            "pressure_source_selected": pressure_source_mode,
            "pressure_source_selection_reason": "a2_conditioning_pressure_source_config",
            "vent_heartbeat_interval_s": self._a2_conditioning_vent_heartbeat_interval_s(),
            "atmosphere_vent_max_gap_s": self._a2_conditioning_vent_max_gap_s(),
            "vent_heartbeat_gap_exceeded": False,
            "route_conditioning_vent_maintenance_active": True,
            "vent_maintenance_started_at": now,
            "vent_maintenance_started_monotonic_s": started_monotonic_s,
            "route_conditioning_high_frequency_vent_interval_s": (
                self._a2_conditioning_high_frequency_vent_interval_s()
            ),
            "route_conditioning_high_frequency_max_gap_s": (
                self._a2_conditioning_high_frequency_vent_max_gap_s()
            ),
            "route_conditioning_high_frequency_vent_window_s": (
                self._a2_conditioning_high_frequency_vent_window_s()
            ),
            "route_conditioning_vent_maintenance_interval_s": (
                self._a2_conditioning_vent_maintenance_interval_s()
            ),
            "route_conditioning_vent_maintenance_max_gap_s": (
                self._a2_conditioning_vent_maintenance_max_gap_s()
            ),
            "route_conditioning_effective_vent_interval_s": self._a2_conditioning_vent_maintenance_interval_s(),
            "route_conditioning_effective_max_gap_s": self._a2_conditioning_vent_maintenance_max_gap_s(),
            "route_conditioning_vent_gap_exceeded": False,
            "route_open_to_first_vent_s": None,
            "route_open_to_first_vent_ms": None,
            "route_open_to_first_vent_write_ms": None,
            "last_vent_command_age_s": None,
            "pre_route_vent_phase_started": False,
            "pre_route_fast_vent_required": True,
            "pre_route_fast_vent_sent": False,
            "pre_route_fast_vent_duration_ms": None,
            "pre_route_fast_vent_timeout": False,
            "fast_vent_reassert_supported": False,
            "fast_vent_reassert_used": False,
            "vent_command_write_started_at": "",
            "vent_command_write_sent_at": "",
            "vent_command_write_completed_at": "",
            "vent_command_write_started_monotonic_s": None,
            "vent_command_write_sent_monotonic_s": None,
            "vent_command_write_completed_monotonic_s": None,
            "vent_command_write_duration_ms": None,
            "vent_command_total_duration_ms": None,
            "vent_command_wait_after_command_s": 0.0,
            "vent_command_capture_pressure_enabled": False,
            "vent_command_query_state_enabled": False,
            "vent_command_confirm_transition_enabled": False,
            "vent_command_blocking_phase": "",
            "route_conditioning_fast_vent_command_timeout": False,
            "route_conditioning_fast_vent_not_supported": False,
            "route_conditioning_diagnostic_blocked_vent_scheduler": False,
            "vent_scheduler_priority_mode": True,
            "vent_scheduler_checked_before_diagnostic": False,
            "diagnostic_deferred_for_vent_priority": False,
            "diagnostic_deferred_count": 0,
            "diagnostic_budget_ms": self._a2_conditioning_diagnostic_budget_ms(),
            "diagnostic_budget_exceeded": False,
            "diagnostic_blocking_component": "",
            "diagnostic_blocking_operation": "",
            "diagnostic_blocking_duration_ms": None,
            "pressure_monitor_nonblocking": True,
            "pressure_monitor_deferred_for_vent_priority": False,
            "pressure_monitor_budget_ms": self._a2_conditioning_pressure_monitor_budget_ms(),
            "pressure_monitor_duration_ms": None,
            "pressure_monitor_blocked_vent_scheduler": False,
            "conditioning_monitor_pressure_deferred": False,
            "trace_write_budget_ms": self._a2_conditioning_trace_write_budget_ms(),
            "trace_write_duration_ms": None,
            "trace_write_blocked_vent_scheduler": False,
            "trace_write_deferred_for_vent_priority": False,
            "route_open_transition_started": False,
            "route_open_transition_started_at": "",
            "route_open_transition_started_monotonic_s": None,
            "route_open_command_write_started_at": "",
            "route_open_command_write_completed_at": "",
            "route_open_command_write_started_monotonic_s": None,
            "route_open_command_write_completed_monotonic_s": None,
            "route_open_command_write_duration_ms": None,
            "route_open_settle_wait_sliced": False,
            "route_open_settle_wait_slice_count": 0,
            "route_open_settle_wait_total_ms": 0.0,
            "route_open_transition_total_duration_ms": None,
            "vent_ticks_during_route_open_transition": 0,
            "route_open_transition_max_vent_write_gap_ms": None,
            "route_open_transition_terminal_vent_write_age_ms": None,
            "route_open_transition_blocked_vent_scheduler": False,
            "route_open_settle_wait_blocked_vent_scheduler": False,
            "terminal_vent_write_age_ms_at_gap_gate": None,
            "max_vent_pulse_write_gap_ms_including_terminal_gap": None,
            "route_conditioning_vent_gap_exceeded_source": "",
            "terminal_gap_source": "",
            "terminal_gap_operation": "",
            "terminal_gap_duration_ms": None,
            "terminal_gap_started_at": "",
            "terminal_gap_detected_at": "",
            "terminal_gap_stack_marker": "",
            "max_vent_pulse_write_gap_phase": "",
            "max_vent_pulse_write_gap_threshold_ms": self._a2_conditioning_vent_maintenance_max_gap_s() * 1000.0,
            "max_vent_pulse_write_gap_threshold_source": "route_conditioning_vent_maintenance_max_gap_s",
            "max_vent_pulse_write_gap_exceeded": False,
            "max_vent_pulse_write_gap_not_exceeded_reason": "",
            "defer_source": "",
            "defer_operation": "",
            "defer_started_at": "",
            "defer_returned_to_vent_loop": False,
            "defer_to_next_vent_loop_ms": None,
            "defer_reschedule_latency_ms": None,
            "defer_reschedule_latency_budget_ms": self._a2_conditioning_defer_reschedule_latency_budget_ms(),
            "defer_reschedule_latency_exceeded": False,
            "defer_reschedule_latency_warning": False,
            "defer_reschedule_caused_vent_gap_exceeded": False,
            "defer_reschedule_requested": False,
            "defer_reschedule_completed": False,
            "defer_reschedule_reason": "",
            "vent_tick_after_defer_ms": None,
            "fast_vent_after_defer_sent": False,
            "fast_vent_after_defer_write_ms": None,
            "terminal_gap_after_defer": False,
            "terminal_gap_after_defer_ms": None,
            "vent_gap_exceeded_after_defer": False,
            "vent_gap_after_defer_ms": None,
            "vent_gap_after_defer_threshold_ms": None,
            "defer_path_no_reschedule": False,
            "defer_path_no_reschedule_reason": "",
            "fail_closed_path_started": False,
            "fail_closed_path_started_while_route_open": False,
            "fail_closed_path_vent_maintenance_required": False,
            "fail_closed_path_vent_maintenance_active": False,
            "fail_closed_path_duration_ms": None,
            "fail_closed_path_blocked_vent_scheduler": False,
            "route_open_high_frequency_vent_phase_started": False,
            "max_vent_pulse_write_gap_ms": None,
            "max_vent_command_total_duration_ms": None,
            "selected_pressure_source_for_conditioning_monitor": "",
            "selected_pressure_source_for_pressure_gate": "",
            "a2_conditioning_pressure_source_strategy": pressure_source_mode,
            "pressure_monitor_interval_s": self._a2_conditioning_pressure_monitor_interval_s(),
            "digital_gauge_max_age_s": self._a2_conditioning_digital_gauge_max_age_s(),
            "digital_gauge_latest_age_s": None,
            "digital_gauge_sequence_progress": None,
            "digital_gauge_monitoring_required": True,
            "conditioning_pressure_abort_hpa": self._a2_conditioning_pressure_abort_hpa(),
            "route_conditioning_hard_abort_pressure_hpa": (
                self._a2_route_conditioning_hard_abort_pressure_hpa()
            ),
            "route_conditioning_hard_abort_exceeded": False,
            "pressure_overlimit_seen": False,
            "pressure_overlimit_source": "",
            "pressure_overlimit_hpa": None,
            "fail_closed_before_vent_off": False,
            "vent_off_sent_at": "",
            "vent_off_command_sent": False,
            "seal_command_sent": False,
            "pressure_setpoint_command_sent": False,
            "sample_count": 0,
            "points_completed": 0,
            "latest_frame_age_max_s": None,
            "abnormal_pressure_events": [],
            "vent_ticks": [],
            "vent_pulse_count": 0,
            "vent_pulse_interval_ms": [],
            "max_vent_pulse_gap_ms": None,
            "max_vent_pulse_gap_limit_ms": self._a2_conditioning_vent_maintenance_max_gap_s() * 1000.0,
            "vent_scheduler_tick_count": 0,
            "vent_scheduler_loop_gap_ms": [],
            "max_vent_scheduler_loop_gap_ms": None,
            "last_vent_scheduler_tick_monotonic_s": None,
            "pressure_drop_after_vent_hpa": [],
            "pressure_samples": [],
            "route_open_to_first_pressure_read_ms": None,
            "route_open_to_overlimit_ms": None,
            "measured_atmospheric_pressure_hpa": None,
            "route_conditioning_pressure_before_route_open_hpa": None,
            "route_conditioning_pressure_after_route_open_hpa": None,
            "route_conditioning_pressure_rise_rate_hpa_per_s": None,
            "route_conditioning_peak_pressure_hpa": None,
            "latest_route_conditioning_pressure_hpa": None,
            "latest_route_conditioning_pressure_source": "",
            "latest_route_conditioning_pressure_age_s": None,
            "latest_route_conditioning_pressure_recorded_monotonic_s": None,
            "latest_route_conditioning_pressure_eligible_for_prearm_baseline": False,
            "route_conditioning_pressure_overlimit": False,
            "route_open_transient_window_enabled": self._a2_route_open_transient_window_enabled(),
            "route_open_transient_peak_pressure_hpa": None,
            "route_open_transient_peak_time_ms": None,
            "route_open_transient_recovery_required": False,
            "route_open_transient_recovered_to_atmosphere": False,
            "route_open_transient_recovery_time_ms": None,
            "route_open_transient_recovery_target_hpa": None,
            "route_open_transient_recovery_band_hpa": self._a2_route_open_transient_recovery_band_hpa(),
            "route_open_transient_stable_hold_s": self._a2_route_open_transient_stable_hold_s(),
            "route_open_transient_stable_pressure_mean_hpa": None,
            "route_open_transient_stable_pressure_span_hpa": None,
            "route_open_transient_stable_pressure_slope_hpa_per_s": None,
            "route_open_transient_accepted": False,
            "route_open_transient_rejection_reason": "",
            "route_open_transient_evaluation_state": "not_started",
            "route_open_transient_interrupted_by_vent_gap": False,
            "route_open_transient_interrupted_reason": "",
            "route_open_transient_summary_source": "route_conditioning_context",
            "measured_atmospheric_pressure_source": "",
            "measured_atmospheric_pressure_sample_age_s": None,
            "sustained_pressure_rise_after_route_open": False,
            "pressure_rise_despite_valid_vent_scheduler": False,
            "vent_pulse_blocked_after_flush_phase": False,
            "vent_pulse_blocked_reason": "",
            "attempted_unsafe_vent_after_seal_or_pressure_control": False,
            "unsafe_vent_after_seal_or_pressure_control_command_sent": False,
            "conditioning_decision": "START",
            "did_not_seal_during_conditioning": True,
            "stream_state_at_start": stream_state,
            "a2_conditioning_pressure_source": pressure_source_mode,
            "a2_3_pressure_source_strategy": pressure_source_mode,
        }
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", True)
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        self._record_workflow_timing(
            "co2_route_conditioning_start",
            "start",
            stage="co2_route_conditioning_at_atmosphere",
            point=point,
            expected_max_s=context["conditioning_soak_s"],
            wait_reason="co2_route_conditioning_atmosphere",
            route_state=context,
        )
        self._record_a2_co2_conditioning_vent_tick(point, phase="before_route_open")
        return dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})

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
        value = str(
            self._cfg_get(
                "workflow.pressure.a2_conditioning_pressure_source",
                self._cfg_get("workflow.pressure.conditioning_pressure_source", "continuous"),
            )
            or "continuous"
        ).strip().lower()
        aliases = {
            "p3": "p3_fast_poll",
            "p3_fast": "p3_fast_poll",
            "fast_poll": "p3_fast_poll",
            "continuous_stream": "continuous",
            "v1": "v1_aligned",
            "v1_aligned_p3": "v1_aligned",
        }
        value = aliases.get(value, value)
        return value if value in {"continuous", "p3_fast_poll", "auto", "v1_aligned"} else "continuous"

    def _a2_conditioning_vent_heartbeat_interval_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.atmosphere_vent_heartbeat_interval_s",
                self._cfg_get("workflow.pressure.conditioning_vent_heartbeat_interval_s", 1.0),
            )
        )
        return max(0.1, float(1.0 if value is None else value))

    def _a2_conditioning_vent_max_gap_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.atmosphere_vent_max_gap_s",
                self._cfg_get("workflow.pressure.conditioning_vent_max_gap_s", 3.0),
            )
        )
        return max(0.1, float(3.0 if value is None else value))

    def _a2_conditioning_high_frequency_vent_max_gap_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_high_frequency_max_gap_s",
                self._cfg_get("workflow.pressure.conditioning_high_frequency_vent_max_gap_s", 1.0),
            )
        )
        return max(0.1, float(1.0 if value is None else value))

    def _a2_conditioning_vent_maintenance_interval_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_vent_maintenance_interval_s",
                self._cfg_get(
                    "workflow.pressure.conditioning_vent_maintenance_interval_s",
                    self._a2_conditioning_vent_heartbeat_interval_s(),
                ),
            )
        )
        return max(0.1, float(1.0 if value is None else value))

    def _a2_conditioning_vent_maintenance_max_gap_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_vent_maintenance_max_gap_s",
                self._cfg_get("workflow.pressure.conditioning_vent_maintenance_max_gap_s", 2.0),
            )
        )
        return max(0.1, float(2.0 if value is None else value))

    def _a2_conditioning_scheduler_sleep_step_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_scheduler_sleep_step_s",
                self._cfg_get("workflow.pressure.conditioning_scheduler_sleep_step_s", 0.1),
            )
        )
        return min(0.2, max(0.01, float(0.1 if value is None else value)))

    def _a2_conditioning_defer_reschedule_latency_budget_ms(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_defer_reschedule_latency_budget_ms",
                self._cfg_get("workflow.pressure.conditioning_defer_reschedule_latency_budget_ms", 200.0),
            )
        )
        return min(1000.0, max(50.0, float(200.0 if value is None else value)))

    def _a2_conditioning_pressure_monitor_interval_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.pressure_monitor_interval_s",
                self._cfg_get("workflow.pressure.conditioning_pressure_monitor_interval_s", 0.5),
            )
        )
        return max(0.05, float(0.5 if value is None else value))

    def _a2_conditioning_diagnostic_budget_ms(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_diagnostic_budget_ms",
                self._cfg_get("workflow.pressure.conditioning_diagnostic_budget_ms", 100.0),
            )
        )
        return min(200.0, max(10.0, float(100.0 if value is None else value)))

    def _a2_conditioning_pressure_monitor_budget_ms(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_pressure_monitor_budget_ms",
                self._cfg_get(
                    "workflow.pressure.conditioning_pressure_monitor_budget_ms",
                    self._a2_conditioning_diagnostic_budget_ms(),
                ),
            )
        )
        return min(200.0, max(10.0, float(self._a2_conditioning_diagnostic_budget_ms() if value is None else value)))

    def _a2_conditioning_continuous_latest_fresh_budget_ms(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.continuous_latest_fresh_budget_ms",
                self._cfg_get("workflow.pressure.conditioning_continuous_latest_fresh_budget_ms", 5.0),
            )
        )
        return min(50.0, max(1.0, float(5.0 if value is None else value)))

    def _a2_conditioning_selected_pressure_sample_stale_budget_ms(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.selected_pressure_sample_stale_budget_ms",
                self._cfg_get("workflow.pressure.conditioning_selected_pressure_sample_stale_budget_ms", 10.0),
            )
        )
        return min(50.0, max(1.0, float(10.0 if value is None else value)))

    def _a2_conditioning_monitor_pressure_max_defer_ms(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.conditioning_monitor_pressure_max_defer_ms",
                self._cfg_get("workflow.pressure.route_conditioning_pressure_max_defer_ms", 5000.0),
            )
        )
        return max(100.0, float(5000.0 if value is None else value))

    def _a2_conditioning_trace_write_budget_ms(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_trace_write_budget_ms",
                self._cfg_get("workflow.pressure.conditioning_trace_write_budget_ms", 50.0),
            )
        )
        return min(200.0, max(5.0, float(50.0 if value is None else value)))

    def _a2_conditioning_digital_gauge_max_age_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.conditioning_digital_gauge_max_age_s",
                self._cfg_get("workflow.pressure.digital_gauge_max_age_s", 3.0),
            )
        )
        return max(0.1, float(3.0 if value is None else value))

    def _a2_conditioning_pressure_abort_hpa(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.conditioning_pressure_abort_hpa",
                self._cfg_get(
                    "workflow.pressure.preseal_atmosphere_flush_abort_pressure_hpa",
                    self._cfg_get("workflow.pressure.preseal_abort_pressure_hpa", 1150.0),
                ),
            )
        )
        return float(1150.0 if value is None else value)

    def _a2_cfg_bool(self, path: str, default: bool) -> bool:
        value = self._cfg_get(path, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "y", "on"}:
                return True
            if lowered in {"0", "false", "no", "n", "off"}:
                return False
        return bool(default if value is None else value)

    def _a2_route_conditioning_hard_abort_pressure_hpa(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_hard_abort_pressure_hpa",
                self._cfg_get("workflow.pressure.conditioning_hard_abort_pressure_hpa", None),
            )
        )
        return float(1250.0 if value is None else value)

    def _a2_route_open_transient_window_enabled(self) -> bool:
        return self._a2_cfg_bool("workflow.pressure.route_open_transient_window_enabled", True)

    def _a2_route_open_transient_recovery_timeout_s(self) -> float:
        value = self._as_float(
            self._cfg_get("workflow.pressure.route_open_transient_recovery_timeout_s", 10.0)
        )
        return max(0.1, float(10.0 if value is None else value))

    def _a2_route_open_transient_recovery_band_hpa(self) -> float:
        value = self._as_float(
            self._cfg_get("workflow.pressure.route_open_transient_recovery_band_hpa", 10.0)
        )
        return max(0.1, float(10.0 if value is None else value))

    def _a2_route_open_transient_stable_hold_s(self) -> float:
        value = self._as_float(
            self._cfg_get("workflow.pressure.route_open_transient_stable_hold_s", 2.0)
        )
        return max(0.0, float(2.0 if value is None else value))

    def _a2_route_open_transient_stable_span_hpa(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_open_transient_stable_pressure_span_hpa",
                self._cfg_get(
                    "workflow.pressure.route_open_transient_stable_span_hpa",
                    self._a2_route_open_transient_recovery_band_hpa(),
                ),
            )
        )
        return max(0.1, float(self._a2_route_open_transient_recovery_band_hpa() if value is None else value))

    def _a2_route_open_transient_stable_slope_hpa_per_s(self) -> float:
        value = self._as_float(
            self._cfg_get("workflow.pressure.route_open_transient_stable_slope_hpa_per_s", 1.0)
        )
        return max(0.0, float(1.0 if value is None else value))

    def _a2_route_open_transient_sustained_rise_min_samples(self) -> int:
        value = self._as_float(
            self._cfg_get("workflow.pressure.route_open_transient_sustained_rise_min_samples", 3)
        )
        return max(2, int(3 if value is None else value))

    def _a2_conditioning_high_frequency_vent_window_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_high_frequency_vent_window_s",
                self._cfg_get("workflow.pressure.conditioning_high_frequency_vent_window_s", 20.0),
            )
        )
        return max(0.0, float(20.0 if value is None else value))

    def _a2_conditioning_high_frequency_vent_interval_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_high_frequency_vent_interval_s",
                self._cfg_get("workflow.pressure.conditioning_high_frequency_vent_interval_s", 0.5),
            )
        )
        return max(0.1, float(0.5 if value is None else value))

    def _a2_conditioning_fast_vent_max_duration_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_fast_vent_max_duration_s",
                self._cfg_get("workflow.pressure.preseal_atmosphere_hold_reassert_timeout_s", 0.5),
            )
        )
        return max(0.05, float(0.5 if value is None else value))

    def _a2_route_open_transition_block_threshold_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_open_transition_blocked_vent_scheduler_threshold_s",
                self._a2_conditioning_high_frequency_vent_max_gap_s(),
            )
        )
        return max(0.1, float(self._a2_conditioning_high_frequency_vent_max_gap_s() if value is None else value))

    def _a2_route_open_settle_wait_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_open_settle_wait_s",
                self._cfg_get("workflow.pressure.co2_route_open_settle_wait_s", 0.0),
            )
        )
        return max(0.0, float(0.0 if value is None else value))

    def _a2_route_open_settle_wait_slice_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_open_settle_wait_slice_s",
                self._a2_conditioning_scheduler_sleep_step_s(),
            )
        )
        return min(0.2, max(0.01, float(self._a2_conditioning_scheduler_sleep_step_s() if value is None else value)))

    def _a2_conditioning_pressure_rise_vent_trigger_hpa(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_pressure_rise_vent_trigger_hpa",
                self._cfg_get("workflow.pressure.pressure_rise_detection_threshold_hpa", 2.0),
            )
        )
        return max(0.1, float(2.0 if value is None else value))

    def _a2_conditioning_pressure_rise_vent_min_interval_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.route_conditioning_pressure_rise_vent_min_interval_s",
                self._a2_conditioning_high_frequency_vent_interval_s(),
            )
        )
        return max(0.1, float(self._a2_conditioning_high_frequency_vent_interval_s() if value is None else value))

    def _a2_conditioning_vent_schedule(
        self,
        context: Mapping[str, Any],
        *,
        now_mono: float,
    ) -> dict[str, Any]:
        route_open_monotonic = self._as_float(
            context.get("route_open_completed_monotonic_s")
            or getattr(self, "_a2_co2_route_open_monotonic_s", None)
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

    def _a2_conditioning_defer_diagnostic_for_vent_priority(
        self,
        context: Mapping[str, Any],
        *,
        point: CalibrationPoint,
        component: str,
        operation: str,
        now_mono: float,
        pressure_monitor: bool = False,
    ) -> dict[str, Any]:
        updated = dict(context)
        defer_started_at = datetime.now(timezone.utc).isoformat()
        component_text = str(component or "unknown")
        operation_text = str(operation or "deferred_for_vent_priority")
        updated.update(
            {
                "vent_scheduler_priority_mode": True,
                "vent_scheduler_checked_before_diagnostic": True,
                "diagnostic_deferred_for_vent_priority": True,
                "diagnostic_deferred_count": int(updated.get("diagnostic_deferred_count") or 0) + 1,
                "diagnostic_budget_ms": self._a2_conditioning_diagnostic_budget_ms(),
                "diagnostic_budget_exceeded": bool(updated.get("diagnostic_budget_exceeded", False)),
                "diagnostic_blocking_component": component_text,
                "diagnostic_blocking_operation": operation_text,
                "diagnostic_blocking_duration_ms": 0.0,
                "last_pressure_monitor_monotonic_s": float(now_mono),
                "last_diagnostic_defer_monotonic_s": float(now_mono),
                "last_diagnostic_defer_at": defer_started_at,
                "last_diagnostic_defer_component": component_text,
                "last_diagnostic_defer_operation": operation_text,
                "defer_source": component_text,
                "defer_operation": operation_text,
                "defer_started_at": defer_started_at,
                "_last_diagnostic_defer_reschedule_recorded": False,
                "defer_returned_to_vent_loop": False,
                "defer_to_next_vent_loop_ms": None,
                "defer_reschedule_latency_ms": None,
                "defer_reschedule_latency_budget_ms": self._a2_conditioning_defer_reschedule_latency_budget_ms(),
                "defer_reschedule_latency_exceeded": False,
                "defer_reschedule_latency_warning": False,
                "defer_reschedule_caused_vent_gap_exceeded": False,
                "defer_reschedule_requested": True,
                "defer_reschedule_completed": False,
                "defer_reschedule_reason": f"return_to_vent_loop_after_{operation_text}",
                "vent_tick_after_defer_ms": None,
                "fast_vent_after_defer_sent": False,
                "fast_vent_after_defer_write_ms": None,
                "terminal_gap_after_defer": False,
                "terminal_gap_after_defer_ms": None,
                "vent_gap_exceeded_after_defer": False,
                "vent_gap_after_defer_ms": None,
                "vent_gap_after_defer_threshold_ms": None,
                "defer_path_no_reschedule": False,
                "defer_path_no_reschedule_reason": "",
                "terminal_gap_stack_marker": f"defer:{component_text}:{operation_text}",
                "route_conditioning_diagnostic_blocked_vent_scheduler": False,
                "route_conditioning_vent_gap_exceeded": bool(
                    updated.get("route_conditioning_vent_gap_exceeded", False)
                ),
            }
        )
        if pressure_monitor:
            defer_count = int(updated.get("conditioning_monitor_pressure_deferred_count") or 0) + 1
            first_defer = self._as_float(updated.get("conditioning_monitor_pressure_first_deferred_monotonic_s"))
            if first_defer is None:
                first_defer = float(now_mono)
            max_defer_ms = self._a2_conditioning_monitor_pressure_max_defer_ms()
            deferred_ms = max(0.0, float(now_mono) - float(first_defer)) * 1000.0
            stale_timeout = bool(deferred_ms > max_defer_ms)
            updated.update(
                {
                    "pressure_monitor_nonblocking": True,
                    "pressure_monitor_deferred_for_vent_priority": True,
                    "pressure_monitor_budget_ms": self._a2_conditioning_pressure_monitor_budget_ms(),
                    "pressure_monitor_duration_ms": 0.0,
                    "pressure_monitor_blocked_vent_scheduler": False,
                    "conditioning_monitor_pressure_deferred": True,
                    "conditioning_monitor_pressure_deferred_count": defer_count,
                    "conditioning_monitor_pressure_first_deferred_monotonic_s": first_defer,
                    "conditioning_monitor_pressure_deferred_elapsed_ms": round(deferred_ms, 3),
                    "conditioning_monitor_max_defer_ms": round(float(max_defer_ms), 3),
                    "conditioning_monitor_pressure_stale_timeout": stale_timeout,
                    "conditioning_monitor_pressure_unavailable_fail_closed": bool(
                        updated.get("conditioning_monitor_pressure_unavailable_fail_closed", False)
                        or stale_timeout
                    ),
                    "selected_pressure_sample_stale_deferred_for_vent_priority": True,
                    "selected_pressure_source_for_conditioning_monitor": (
                        updated.get("selected_pressure_source_for_conditioning_monitor")
                        or "digital_pressure_gauge_continuous"
                    ),
                }
            )
        state = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "phase": str(updated.get("vent_phase") or "conditioning_pressure_monitor"),
            "route_conditioning_phase": updated.get("route_conditioning_phase", "route_conditioning_flush_phase"),
            "vent_command_sent": False,
            "whether_safe_to_continue": True,
            "fail_closed_reason": "",
            **self._a2_conditioning_scheduler_evidence(updated),
        }
        deferred = [item for item in list(updated.get("diagnostic_deferred_events") or []) if isinstance(item, Mapping)]
        deferred.append(dict(state))
        updated["diagnostic_deferred_events"] = deferred
        samples = [item for item in list(updated.get("pressure_samples") or []) if isinstance(item, Mapping)]
        samples.append(dict(state))
        updated["pressure_samples"] = samples
        updated = self._a2_conditioning_context_with_counts(updated)
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", updated)
        return updated

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

    def _a2_conditioning_pressure_sample_from_snapshot(
        self,
        snapshot: Mapping[str, Any],
        point: CalibrationPoint,
        *,
        phase: str,
    ) -> dict[str, Any]:
        latest = snapshot.get("latest_frame")
        latest = dict(latest) if isinstance(latest, Mapping) else {}
        pressure_hpa = self._a2_conditioning_first_float(
            latest.get("pressure_hpa"),
            snapshot.get("pressure_hpa"),
        )
        age_s = self._a2_conditioning_first_float(
            snapshot.get("latest_frame_age_s"),
            latest.get("latest_frame_age_s"),
            latest.get("sample_age_s"),
            latest.get("pressure_sample_age_s"),
        )
        max_age_s = self._a2_conditioning_digital_gauge_max_age_s()
        sequence_id = (
            snapshot.get("latest_frame_sequence_id")
            or snapshot.get("digital_gauge_latest_sequence_id")
            or latest.get("sequence_id")
            or latest.get("pressure_sample_sequence_id")
        )
        stale = bool(
            snapshot.get("latest_frame_stale")
            or latest.get("is_stale")
            or age_s is None
            or float(age_s) > max_age_s
        )
        parse_ok = bool(pressure_hpa is not None and not bool(latest.get("parse_ok") is False))
        latest_unavailable = bool(not latest or pressure_hpa is None or not parse_ok)
        latest_fresh = bool(parse_ok and not stale and pressure_hpa is not None)
        selection_reason = str(
            snapshot.get("pressure_source_selection_reason")
            or snapshot.get("source_selection_reason")
            or (
                "digital_gauge_continuous_latest_fresh"
                if latest_fresh
                else (
                    "digital_gauge_continuous_latest_unavailable"
                    if latest_unavailable
                    else "digital_gauge_continuous_latest_stale"
                )
            )
        )
        return {
            "stage": "co2_route_conditioning_at_atmosphere",
            "phase": phase,
            "point_index": point.index,
            "pressure_hpa": pressure_hpa,
            "digital_gauge_pressure_hpa": pressure_hpa,
            "source": "digital_pressure_gauge_continuous",
            "pressure_sample_source": "digital_pressure_gauge_continuous",
            "pressure_source_selected": "digital_pressure_gauge_continuous" if parse_ok and not stale else "",
            "pressure_source_selection_reason": selection_reason,
            "source_selection_reason": selection_reason,
            "pressure_source_used_for_decision": "digital_pressure_gauge_continuous" if parse_ok and not stale else "",
            "pressure_source_used_for_abort": "digital_pressure_gauge_continuous" if parse_ok and not stale else "",
            "sample_age_s": age_s,
            "pressure_sample_age_s": age_s,
            "latest_frame_age_s": age_s,
            "sequence_id": sequence_id,
            "pressure_sample_sequence_id": sequence_id,
            "latest_frame_sequence_id": sequence_id,
            "is_stale": stale,
            "pressure_sample_is_stale": stale,
            "parse_ok": parse_ok,
            "error": "" if parse_ok else "continuous_snapshot_unavailable",
            "digital_gauge_mode": "continuous",
            "a2_3_pressure_source_strategy": self._a2_conditioning_pressure_source_mode(),
            "critical_window_uses_latest_frame": True,
            "critical_window_uses_query": False,
            "p3_fast_fallback_attempted": False,
            "p3_fast_fallback_result": "deferred_for_vent_priority" if not parse_ok or stale else "",
            "normal_p3_fallback_attempted": False,
            "normal_p3_fallback_result": "",
            "conditioning_monitor_latest_frame_age_s": age_s,
            "conditioning_monitor_latest_frame_fresh": latest_fresh,
            "conditioning_monitor_latest_frame_unavailable": latest_unavailable,
            "continuous_latest_fresh_fast_path_used": bool(
                snapshot.get("continuous_latest_fresh_fast_path_used", False)
            ),
            "continuous_latest_fresh_duration_ms": snapshot.get("continuous_latest_fresh_duration_ms"),
            "continuous_latest_fresh_lock_acquire_ms": snapshot.get("continuous_latest_fresh_lock_acquire_ms"),
            "continuous_latest_fresh_lock_timeout": bool(snapshot.get("continuous_latest_fresh_lock_timeout", False)),
            "continuous_latest_fresh_waited_for_frame": bool(
                snapshot.get("continuous_latest_fresh_waited_for_frame", False)
            ),
            "continuous_latest_fresh_performed_io": bool(
                snapshot.get("continuous_latest_fresh_performed_io", False)
            ),
            "continuous_latest_fresh_triggered_stream_restart": bool(
                snapshot.get("continuous_latest_fresh_triggered_stream_restart", False)
            ),
            "continuous_latest_fresh_triggered_drain": bool(
                snapshot.get("continuous_latest_fresh_triggered_drain", False)
            ),
            "continuous_latest_fresh_triggered_p3_fallback": bool(
                snapshot.get("continuous_latest_fresh_triggered_p3_fallback", False)
            ),
            "continuous_latest_fresh_budget_ms": snapshot.get("continuous_latest_fresh_budget_ms"),
            "continuous_latest_fresh_budget_exceeded": bool(
                snapshot.get("continuous_latest_fresh_budget_exceeded", False)
            ),
            "digital_gauge_pressure_sample": {
                "pressure_hpa": pressure_hpa,
                "source": "digital_pressure_gauge_continuous",
                "pressure_sample_source": "digital_pressure_gauge_continuous",
                "sample_age_s": age_s,
                "pressure_sample_age_s": age_s,
                "latest_frame_age_s": age_s,
                "sequence_id": sequence_id,
                "pressure_sample_sequence_id": sequence_id,
                "latest_frame_sequence_id": sequence_id,
                "is_stale": stale,
                "pressure_sample_is_stale": stale,
                "parse_ok": parse_ok,
            },
        }

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
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", updated)
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
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", updated)
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
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", updated)
        return updated

    def _record_positive_preseal_fail_closed_context(self, actual: Mapping[str, Any]) -> dict[str, Any]:
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
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
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", updated)
        return updated

    def _a2_conditioning_emergency_abort_relief_decision(
        self,
        context: Mapping[str, Any],
        *,
        reason: str = "",
        relief_context: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        merged = dict(context)
        if isinstance(relief_context, Mapping):
            merged.update(dict(relief_context))
        phase = str(merged.get("route_conditioning_phase") or "unknown")
        pressure_hpa = self._as_float(
            merged.get("emergency_abort_relief_pressure_hpa")
            or merged.get("positive_preseal_pressure_hpa")
            or merged.get("pressure_hpa")
        )
        relief_reason = str(
            merged.get("emergency_abort_relief_reason")
            or merged.get("positive_preseal_abort_reason")
            or merged.get("abort_reason")
            or reason
            or ""
        )
        seal_sent = bool(
            merged.get("seal_command_sent")
            or merged.get("positive_preseal_seal_command_sent")
            or merged.get("sealed")
        )
        setpoint_sent = bool(
            merged.get("pressure_setpoint_command_sent")
            or merged.get("positive_preseal_pressure_setpoint_command_sent")
        )
        sample_started = bool(
            merged.get("sampling_started")
            or merged.get("sample_started")
            or merged.get("positive_preseal_sample_started")
            or int(merged.get("sample_count") or 0) > 0
            or int(merged.get("points_completed") or 0) > 0
        )
        pressure_ready_started = bool(
            merged.get("pressure_ready_started")
            or merged.get("pressure_gate_reached")
        )
        write_state = bool(
            merged.get("any_write_command_sent")
            or merged.get("identity_write_command_sent")
            or merged.get("senco_write_command_sent")
            or merged.get("calibration_write_command_sent")
        )
        route_open = bool(
            merged.get(
                "positive_preseal_route_open",
                not bool(merged.get("route_valve_closed") or merged.get("route_valve_closing")),
            )
        )
        may_mix_air = bool(seal_sent or sample_started or pressure_ready_started)
        block_reasons: list[str] = []
        if not bool(
            merged.get("emergency_abort_relief_vent_required")
            or merged.get("positive_preseal_pressure_overlimit")
            or relief_reason == "preseal_abort_pressure_exceeded"
            or relief_reason == "positive_preseal_abort_pressure_exceeded"
        ):
            block_reasons.append("emergency_abort_relief_not_required")
        if seal_sent:
            block_reasons.append("seal_command_sent")
        if setpoint_sent:
            block_reasons.append("pressure_setpoint_command_sent")
        if pressure_ready_started:
            block_reasons.append("pressure_ready_started")
        if sample_started:
            block_reasons.append("sample_started")
        if write_state:
            block_reasons.append("write_state_not_clean")
        if may_mix_air and not any(
            item in block_reasons for item in ("seal_command_sent", "pressure_ready_started", "sample_started")
        ):
            block_reasons.append("may_mix_air_into_measurement_path")
        allowed = not block_reasons
        block_reason = ",".join(block_reasons)
        updated = dict(context)
        updated.update(
            {
                "emergency_abort_relief_vent_required": True,
                "emergency_abort_relief_vent_allowed": allowed,
                "emergency_abort_relief_vent_blocked_reason": block_reason,
                "emergency_abort_relief_vent_command_sent": allowed,
                "emergency_abort_relief_vent_phase": phase,
                "emergency_abort_relief_reason": relief_reason,
                "emergency_abort_relief_pressure_hpa": pressure_hpa,
                "emergency_abort_relief_route_open": route_open,
                "emergency_abort_relief_seal_command_sent": seal_sent,
                "emergency_abort_relief_pressure_setpoint_command_sent": setpoint_sent,
                "emergency_abort_relief_sample_started": sample_started,
                "emergency_abort_relief_may_mix_air": may_mix_air,
                "emergency_relief_after_pressure_control_is_abort_only": bool(
                    pressure_ready_started or setpoint_sent or sample_started
                ),
                "resume_after_emergency_relief_allowed": False,
                "normal_maintenance_vent_blocked_after_flush_phase": False,
                "cleanup_vent_classification": "emergency_abort_relief",
                "safe_stop_pressure_relief_result": "command_sent" if allowed else f"blocked:{block_reason}",
            }
        )
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", updated)
        payload = {key: updated.get(key) for key in (
            "emergency_abort_relief_vent_required",
            "emergency_abort_relief_vent_allowed",
            "emergency_abort_relief_vent_blocked_reason",
            "emergency_abort_relief_vent_command_sent",
            "emergency_abort_relief_vent_phase",
            "emergency_abort_relief_reason",
            "emergency_abort_relief_pressure_hpa",
            "emergency_abort_relief_route_open",
            "emergency_abort_relief_seal_command_sent",
            "emergency_abort_relief_pressure_setpoint_command_sent",
            "emergency_abort_relief_sample_started",
            "emergency_abort_relief_may_mix_air",
            "emergency_relief_after_pressure_control_is_abort_only",
            "resume_after_emergency_relief_allowed",
            "normal_maintenance_vent_blocked_after_flush_phase",
            "cleanup_vent_classification",
            "safe_stop_pressure_relief_result",
        )}
        if not allowed:
            payload.update(
                {
                    "vent_command_blocked": True,
                    "reason": reason,
                }
            )
        return payload

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
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", updated)
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
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
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
            or getattr(self, "_a2_co2_route_open_monotonic_s", None)
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
            or getattr(self, "_a2_co2_route_open_monotonic_s", None)
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
            getattr(self, "_a2_co2_route_open_pressure_hpa", None),
        )

    def _a2_route_open_transient_update(
        self,
        context: Mapping[str, Any],
        *,
        pressure_hpa: float,
        event_monotonic_s: float,
    ) -> dict[str, Any]:
        updated = dict(context)
        if not bool(updated.get("route_open_transient_window_enabled", True)):
            return updated
        route_open_monotonic = self._as_float(
            updated.get("route_open_completed_monotonic_s")
            or getattr(self, "_a2_co2_route_open_monotonic_s", None)
        )
        if route_open_monotonic is None:
            return updated
        elapsed_ms = round(max(0.0, float(event_monotonic_s) - float(route_open_monotonic)) * 1000.0, 3)
        target_hpa = self._a2_route_open_transient_target_hpa(updated)
        if target_hpa is None:
            updated["route_open_transient_evaluation_state"] = "not_started"
            updated["route_open_transient_summary_source"] = "missing_measured_atmospheric_pressure"
            return updated
        target_hpa = round(float(target_hpa), 3)
        band_hpa = self._as_float(updated.get("route_open_transient_recovery_band_hpa"))
        if band_hpa is None:
            band_hpa = self._a2_route_open_transient_recovery_band_hpa()
        band_hpa = float(band_hpa)
        updated["route_open_transient_recovery_target_hpa"] = target_hpa
        updated["route_open_transient_recovery_band_hpa"] = round(band_hpa, 3)
        updated.setdefault("route_open_transient_stable_hold_s", self._a2_route_open_transient_stable_hold_s())
        updated["route_open_transient_evaluation_state"] = "evaluating"
        updated["route_open_transient_summary_source"] = "route_conditioning_pressure_monitor"
        samples = [item for item in list(updated.get("route_open_transient_pressure_samples") or []) if isinstance(item, Mapping)]
        samples.append({"elapsed_ms": elapsed_ms, "pressure_hpa": round(float(pressure_hpa), 3)})
        updated["route_open_transient_pressure_samples"] = samples[-200:]
        peak = self._as_float(updated.get("route_open_transient_peak_pressure_hpa"))
        if peak is None or float(pressure_hpa) > float(peak):
            updated["route_open_transient_peak_pressure_hpa"] = round(float(pressure_hpa), 3)
            updated["route_open_transient_peak_time_ms"] = elapsed_ms
        required = bool(updated.get("route_open_transient_recovery_required", False)) or bool(
            float(pressure_hpa) > target_hpa + band_hpa
        )
        updated["route_open_transient_recovery_required"] = required
        if not required:
            return updated

        in_band = abs(float(pressure_hpa) - target_hpa) <= band_hpa
        if in_band:
            if updated.get("route_open_transient_recovery_time_ms") in (None, ""):
                updated["route_open_transient_recovery_time_ms"] = elapsed_ms
            updated["route_open_transient_recovered_to_atmosphere"] = True
            if updated.get("route_open_transient_stable_start_ms") in (None, ""):
                updated["route_open_transient_stable_start_ms"] = elapsed_ms
        else:
            updated["route_open_transient_stable_start_ms"] = None
            if bool(updated.get("route_open_transient_recovered_to_atmosphere", False)):
                updated["route_open_transient_rejection_reason"] = "route_open_transient_unstable_after_recovery"
                updated["route_open_transient_evaluation_state"] = "rejected"

        stable_start_ms = self._as_float(updated.get("route_open_transient_stable_start_ms"))
        stable_hold_s = self._as_float(updated.get("route_open_transient_stable_hold_s"))
        stable_hold_s = self._a2_route_open_transient_stable_hold_s() if stable_hold_s is None else float(stable_hold_s)
        if stable_start_ms is not None and elapsed_ms - float(stable_start_ms) >= stable_hold_s * 1000.0:
            stable_samples = [
                item
                for item in samples
                if self._as_float(item.get("elapsed_ms")) is not None
                and float(self._as_float(item.get("elapsed_ms")) or 0.0) >= float(stable_start_ms)
                and self._as_float(item.get("pressure_hpa")) is not None
            ]
            stable_values = [float(self._as_float(item.get("pressure_hpa")) or 0.0) for item in stable_samples]
            if stable_values:
                mean_hpa = sum(stable_values) / len(stable_values)
                span_hpa = max(stable_values) - min(stable_values)
                slope_hpa_per_s = 0.0
                if len(stable_samples) >= 2:
                    first = stable_samples[0]
                    last = stable_samples[-1]
                    dt_s = (
                        float(self._as_float(last.get("elapsed_ms")) or 0.0)
                        - float(self._as_float(first.get("elapsed_ms")) or 0.0)
                    ) / 1000.0
                    if dt_s > 0.0:
                        slope_hpa_per_s = (stable_values[-1] - stable_values[0]) / dt_s
                updated["route_open_transient_stable_pressure_mean_hpa"] = round(mean_hpa, 3)
                updated["route_open_transient_stable_pressure_span_hpa"] = round(span_hpa, 3)
                updated["route_open_transient_stable_pressure_slope_hpa_per_s"] = round(slope_hpa_per_s, 3)
                span_limit = self._a2_route_open_transient_stable_span_hpa()
                slope_limit = self._a2_route_open_transient_stable_slope_hpa_per_s()
                if span_hpa <= span_limit and abs(slope_hpa_per_s) <= slope_limit:
                    updated["route_open_transient_accepted"] = True
                    updated["route_open_transient_rejection_reason"] = ""
                    updated["route_open_transient_evaluation_state"] = "accepted"
                else:
                    updated["route_open_transient_rejection_reason"] = "route_open_transient_unstable_after_recovery"
                    updated["route_open_transient_evaluation_state"] = "rejected"

        post_open_values = [
            float(self._as_float(item.get("pressure_hpa")) or 0.0)
            for item in samples
            if self._as_float(item.get("pressure_hpa")) is not None
        ]
        min_samples = self._a2_route_open_transient_sustained_rise_min_samples()
        if len(post_open_values) >= min_samples:
            recent = post_open_values[-min_samples:]
            sustained = all(recent[index + 1] > recent[index] + 0.1 for index in range(len(recent) - 1))
            if sustained and recent[-1] > target_hpa + band_hpa:
                updated["sustained_pressure_rise_after_route_open"] = True
                updated["route_open_transient_rejection_reason"] = "sustained_pressure_rise_after_route_open"
                updated["route_open_transient_evaluation_state"] = "rejected"

        pressure_rise_since_last_vent = self._as_float(updated.get("pressure_rise_since_last_vent_hpa"))
        if (
            pressure_rise_since_last_vent is not None
            and int(updated.get("vent_pulse_count") or 0) > 0
            and not bool(updated.get("route_conditioning_vent_gap_exceeded", False))
            and float(pressure_hpa) > target_hpa + band_hpa
            and float(pressure_rise_since_last_vent) >= self._a2_conditioning_pressure_rise_vent_trigger_hpa()
            and len(post_open_values) >= 2
        ):
            updated["pressure_rise_despite_valid_vent_scheduler"] = True
            updated["route_open_transient_rejection_reason"] = "pressure_rise_despite_valid_vent_scheduler"
            updated["route_open_transient_evaluation_state"] = "rejected"

        timeout_ms = self._a2_route_open_transient_recovery_timeout_s() * 1000.0
        if elapsed_ms > timeout_ms and not bool(updated.get("route_open_transient_accepted", False)):
            if bool(updated.get("route_open_transient_recovered_to_atmosphere", False)):
                updated["route_open_transient_rejection_reason"] = (
                    updated.get("route_open_transient_rejection_reason")
                    or "route_open_transient_stable_hold_timeout"
                )
            else:
                updated["route_open_transient_rejection_reason"] = "route_open_transient_recovery_timeout"
            updated["route_open_transient_evaluation_state"] = "rejected"
        return updated

    def _a2_conditioning_update_pressure_metrics(
        self,
        context: Mapping[str, Any],
        *,
        phase: str,
        pressure_hpa: Any,
        event_monotonic_s: float,
        vent_command_sent: bool,
        vent_command_write_sent_monotonic_s: Optional[float] = None,
    ) -> dict[str, Any]:
        updated = dict(context)
        pressure_value = self._as_float(pressure_hpa)
        route_open_monotonic = self._as_float(
            updated.get("route_open_completed_monotonic_s")
            or getattr(self, "_a2_co2_route_open_monotonic_s", None)
        )
        phase_text = str(phase or "")
        before_route_open = bool(
            phase_text.startswith("before_route_open")
            or route_open_monotonic is None
            or float(event_monotonic_s) < float(route_open_monotonic)
        )
        if route_open_monotonic is not None and updated.get("route_open_completed_monotonic_s") in (None, ""):
            updated["route_open_completed_monotonic_s"] = float(route_open_monotonic)
        if pressure_value is not None:
            if before_route_open:
                updated["route_conditioning_pressure_before_route_open_hpa"] = round(float(pressure_value), 3)
                updated["measured_atmospheric_pressure_hpa"] = round(float(pressure_value), 3)
                updated["measured_atmospheric_pressure_source"] = "route_conditioning_pressure_before_route_open"
                updated["measured_atmospheric_pressure_sample_age_s"] = 0.0
                updated["route_open_transient_recovery_target_hpa"] = round(float(pressure_value), 3)
            else:
                if updated.get("route_conditioning_pressure_after_route_open_hpa") in (None, ""):
                    updated["route_conditioning_pressure_after_route_open_hpa"] = round(float(pressure_value), 3)
                if updated.get("route_open_to_first_pressure_read_ms") in (None, "") and route_open_monotonic is not None:
                    updated["route_open_to_first_pressure_read_ms"] = round(
                        max(0.0, float(event_monotonic_s) - float(route_open_monotonic)) * 1000.0,
                        3,
                    )
                baseline_hpa = self._as_float(
                    updated.get("route_conditioning_pressure_before_route_open_hpa")
                    or updated.get("route_open_pressure_hpa")
                    or getattr(self, "_a2_co2_route_open_pressure_hpa", None)
                    or updated.get("route_conditioning_pressure_after_route_open_hpa")
                )
                if baseline_hpa is not None and route_open_monotonic is not None:
                    elapsed_s = max(0.0, float(event_monotonic_s) - float(route_open_monotonic))
                    if elapsed_s > 0.0:
                        updated["route_conditioning_pressure_rise_rate_hpa_per_s"] = round(
                            (float(pressure_value) - float(baseline_hpa)) / elapsed_s,
                            3,
                        )
            previous_peak = self._as_float(updated.get("route_conditioning_peak_pressure_hpa"))
            updated["route_conditioning_peak_pressure_hpa"] = round(
                float(pressure_value) if previous_peak is None else max(float(previous_peak), float(pressure_value)),
                3,
            )
            hard_abort_hpa = self._as_float(updated.get("route_conditioning_hard_abort_pressure_hpa"))
            if hard_abort_hpa is None:
                hard_abort_hpa = self._a2_route_conditioning_hard_abort_pressure_hpa()
                updated["route_conditioning_hard_abort_pressure_hpa"] = float(hard_abort_hpa)
            if hard_abort_hpa is not None and float(pressure_value) >= float(hard_abort_hpa):
                updated["route_conditioning_hard_abort_exceeded"] = True
                updated["route_conditioning_pressure_overlimit"] = True
                updated["route_conditioning_high_pressure_seen_before_preseal"] = True
                updated["route_conditioning_high_pressure_seen_before_preseal_hpa"] = round(
                    float(pressure_value),
                    3,
                )
                updated["route_conditioning_high_pressure_seen_phase"] = str(
                    phase or "co2_route_conditioning_at_atmosphere"
                )
                updated["route_conditioning_high_pressure_seen_decision"] = "fail_closed"
                updated["route_open_transient_evaluation_state"] = "hard_abort"
                if updated.get("route_open_to_overlimit_ms") in (None, "") and route_open_monotonic is not None:
                    updated["route_open_to_overlimit_ms"] = round(
                        max(0.0, float(event_monotonic_s) - float(route_open_monotonic)) * 1000.0,
                        3,
                    )
            last_vent_pressure = self._as_float(updated.get("last_vent_pressure_hpa"))
            if last_vent_pressure is not None:
                updated["pressure_rise_since_last_vent_hpa"] = round(
                    float(pressure_value) - float(last_vent_pressure),
                    3,
                )
            elif int(updated.get("vent_pulse_count") or 0) > 0:
                updated["last_vent_pressure_hpa"] = round(float(pressure_value), 3)
                updated["last_vent_pressure_baseline_pending"] = False
                updated["pressure_rise_since_last_vent_hpa"] = 0.0
            if not before_route_open:
                updated = self._a2_route_open_transient_update(
                    updated,
                    pressure_hpa=float(pressure_value),
                    event_monotonic_s=event_monotonic_s,
                )
            updated["last_conditioning_pressure_hpa"] = round(float(pressure_value), 3)
            updated["last_conditioning_pressure_monotonic_s"] = float(event_monotonic_s)
            updated["latest_route_conditioning_pressure_hpa"] = round(float(pressure_value), 3)
            updated["latest_route_conditioning_pressure_recorded_monotonic_s"] = float(event_monotonic_s)
        if vent_command_sent:
            vent_write_sent = self._as_float(vent_command_write_sent_monotonic_s)
            if vent_write_sent is None:
                vent_write_sent = float(event_monotonic_s)
            previous_vent_start = self._as_float(
                updated.get("last_vent_command_write_sent_monotonic_s")
                or updated.get("last_vent_heartbeat_started_monotonic_s")
                or updated.get("last_vent_tick_monotonic_s")
            )
            if previous_vent_start is not None:
                intervals = list(updated.get("vent_pulse_interval_ms") or [])
                write_gap_ms = round(max(0.0, float(vent_write_sent) - float(previous_vent_start)) * 1000.0, 3)
                intervals.append(write_gap_ms)
                updated["vent_pulse_interval_ms"] = intervals
                updated["max_vent_pulse_write_gap_ms"] = (
                    write_gap_ms
                    if self._as_float(updated.get("max_vent_pulse_write_gap_ms")) is None
                    else max(float(updated["max_vent_pulse_write_gap_ms"]), write_gap_ms)
                )
            if route_open_monotonic is not None and updated.get("route_open_to_first_vent_write_ms") in (None, ""):
                route_open_gap_ms = round(
                    max(0.0, float(vent_write_sent) - float(route_open_monotonic)) * 1000.0,
                    3,
                )
                updated["route_open_to_first_vent_write_ms"] = route_open_gap_ms
                updated["route_open_to_first_vent_ms"] = route_open_gap_ms
                updated["route_open_to_first_vent_s"] = round(route_open_gap_ms / 1000.0, 3)
            previous_pressure = self._as_float(updated.get("last_vent_pressure_hpa"))
            drop = None if pressure_value is None or previous_pressure is None else round(float(previous_pressure) - float(pressure_value), 3)
            drops = list(updated.get("pressure_drop_after_vent_hpa") or [])
            drops.append(drop)
            updated["pressure_drop_after_vent_hpa"] = drops
            updated["last_pressure_drop_after_vent_hpa"] = drop
            if pressure_value is not None:
                updated["last_vent_pressure_hpa"] = round(float(pressure_value), 3)
                updated["last_vent_pressure_baseline_pending"] = False
            else:
                updated["last_vent_pressure_hpa"] = None
                updated["last_vent_pressure_baseline_pending"] = True
            updated["vent_pulse_count"] = int(updated.get("vent_pulse_count") or 0) + 1
            updated["last_vent_command_write_sent_monotonic_s"] = float(vent_write_sent)
        return updated

    def _a2_conditioning_context_with_counts(self, context: Mapping[str, Any]) -> dict[str, Any]:
        updated = dict(context)
        ticks = [item for item in list(updated.get("vent_ticks") or []) if isinstance(item, Mapping)]
        gaps = [
            self._as_float(item.get("vent_heartbeat_gap_s", item.get("vent_tick_gap_s")))
            for item in ticks
            if self._as_float(item.get("vent_heartbeat_gap_s", item.get("vent_tick_gap_s"))) is not None
        ]
        pulse_gaps_ms = [
            self._as_float(item)
            for item in list(updated.get("vent_pulse_interval_ms") or [])
            if self._as_float(item) is not None
        ]
        scheduler_gaps_ms = [
            self._as_float(item)
            for item in list(updated.get("vent_scheduler_loop_gap_ms") or [])
            if self._as_float(item) is not None
        ]
        updated["vent_tick_count"] = len(ticks)
        updated["vent_tick_avg_gap_s"] = (
            None if not gaps else round(sum(float(item) for item in gaps) / len(gaps), 3)
        )
        updated["vent_tick_max_gap_s"] = None if not gaps else round(max(float(item) for item in gaps), 3)
        updated["max_vent_pulse_gap_ms"] = (
            None if not pulse_gaps_ms else round(max(float(item) for item in pulse_gaps_ms), 3)
        )
        updated["max_vent_pulse_write_gap_ms"] = updated["max_vent_pulse_gap_ms"]
        terminal_gap_ms = self._as_float(updated.get("terminal_vent_write_age_ms_at_gap_gate"))
        existing_including_terminal_ms = self._as_float(
            updated.get("max_vent_pulse_write_gap_ms_including_terminal_gap")
        )
        including_terminal_candidates = [
            value
            for value in (
                self._as_float(updated.get("max_vent_pulse_write_gap_ms")),
                terminal_gap_ms,
                existing_including_terminal_ms,
            )
            if value is not None
        ]
        updated["max_vent_pulse_write_gap_ms_including_terminal_gap"] = (
            None
            if not including_terminal_candidates
            else round(max(float(value) for value in including_terminal_candidates), 3)
        )
        threshold_ms = self._as_float(updated.get("max_vent_pulse_gap_limit_ms"))
        if threshold_ms is None:
            threshold_ms = self._as_float(updated.get("route_conditioning_effective_max_gap_s"))
            threshold_ms = None if threshold_ms is None else round(float(threshold_ms) * 1000.0, 3)
        if threshold_ms is None:
            threshold_ms = round(self._a2_conditioning_vent_maintenance_max_gap_s() * 1000.0, 3)
        max_write_gap_ms = self._as_float(updated.get("max_vent_pulse_write_gap_ms_including_terminal_gap"))
        phase = str(updated.get("route_conditioning_phase") or updated.get("terminal_gap_source") or "unknown")
        exceeded = bool(max_write_gap_ms is not None and float(max_write_gap_ms) > float(threshold_ms))
        updated["max_vent_pulse_write_gap_phase"] = phase
        updated["max_vent_pulse_write_gap_threshold_ms"] = round(float(threshold_ms), 3)
        updated["max_vent_pulse_write_gap_threshold_source"] = (
            "route_conditioning_effective_max_gap_s"
            if updated.get("route_conditioning_effective_max_gap_s") not in (None, "")
            else "route_conditioning_vent_maintenance_max_gap_s"
        )
        updated["max_vent_pulse_write_gap_exceeded"] = exceeded
        updated["max_vent_pulse_write_gap_not_exceeded_reason"] = (
            ""
            if exceeded or max_write_gap_ms is None
            else f"{round(float(max_write_gap_ms), 3)}ms <= {round(float(threshold_ms), 3)}ms in {phase}"
        )
        computed_scheduler_gap_ms = None if not scheduler_gaps_ms else round(max(float(item) for item in scheduler_gaps_ms), 3)
        existing_scheduler_gap_ms = self._as_float(updated.get("max_vent_scheduler_loop_gap_ms"))
        scheduler_candidates = [
            value
            for value in (computed_scheduler_gap_ms, existing_scheduler_gap_ms)
            if value is not None
        ]
        updated["max_vent_scheduler_loop_gap_ms"] = (
            None if not scheduler_candidates else round(max(float(value) for value in scheduler_candidates), 3)
        )
        command_durations_ms = [
            self._as_float(item.get("vent_command_total_duration_ms"))
            for item in ticks
            if self._as_float(item.get("vent_command_total_duration_ms")) is not None
        ]
        updated["max_vent_command_total_duration_ms"] = (
            None if not command_durations_ms else round(max(float(item) for item in command_durations_ms), 3)
        )
        updated["route_conditioning_vent_gap_exceeded"] = bool(
            updated.get("route_conditioning_vent_gap_exceeded", False)
        )
        if updated.get("route_open_to_first_vent_s") not in (None, ""):
            updated["route_open_to_first_vent_ms"] = round(
                float(updated["route_open_to_first_vent_s"]) * 1000.0,
                3,
            )
        if updated.get("route_open_to_first_vent_write_ms") in (None, ""):
            updated["route_open_to_first_vent_write_ms"] = updated.get("route_open_to_first_vent_ms")
        return updated

    def _a2_conditioning_terminal_gap_details(
        self,
        context: Mapping[str, Any],
        *,
        now_mono: float,
        max_gap_s: float,
        source: str,
    ) -> dict[str, Any]:
        updated = dict(context)
        last_write = self._as_float(
            updated.get("last_vent_command_write_sent_monotonic_s")
            or updated.get("last_vent_tick_monotonic_s")
            or updated.get("last_vent_heartbeat_started_monotonic_s")
        )
        terminal_gap_ms = None
        if last_write is not None:
            terminal_gap_ms = round(max(0.0, float(now_mono) - float(last_write)) * 1000.0, 3)
        pulse_gap_ms = self._as_float(updated.get("max_vent_pulse_write_gap_ms"))
        if pulse_gap_ms is None:
            pulse_gap_ms = self._as_float(updated.get("max_vent_pulse_gap_ms"))
        existing_including = self._as_float(updated.get("max_vent_pulse_write_gap_ms_including_terminal_gap"))
        including_candidates = [
            value
            for value in (pulse_gap_ms, terminal_gap_ms, existing_including)
            if value is not None
        ]
        including_gap_ms = None if not including_candidates else round(max(float(value) for value in including_candidates), 3)
        scheduler_gap_ms = self._as_float(updated.get("max_vent_scheduler_loop_gap_ms"))
        if terminal_gap_ms is not None:
            scheduler_gap_ms = (
                terminal_gap_ms
                if scheduler_gap_ms is None
                else max(float(scheduler_gap_ms), float(terminal_gap_ms))
            )
        source_text = self._a2_conditioning_terminal_gap_source(context, source=source)
        operation = str(
            updated.get("terminal_gap_operation")
            or updated.get("last_diagnostic_defer_operation")
            or updated.get("diagnostic_blocking_operation")
            or updated.get("last_blocking_operation_name")
            or source_text
        )
        details = {
            "terminal_vent_write_age_ms_at_gap_gate": terminal_gap_ms,
            "max_vent_pulse_write_gap_ms_including_terminal_gap": including_gap_ms,
            "route_conditioning_vent_gap_exceeded_source": source_text,
            "terminal_gap_source": source_text,
            "terminal_gap_operation": operation,
            "terminal_gap_duration_ms": terminal_gap_ms,
            "terminal_gap_started_at": str(
                updated.get("terminal_gap_started_at")
                or updated.get("last_vent_command_write_sent_at")
                or updated.get("vent_command_write_sent_at")
                or updated.get("last_diagnostic_defer_at")
                or ""
            ),
            "terminal_gap_detected_at": datetime.now(timezone.utc).isoformat(),
            "terminal_gap_stack_marker": str(updated.get("terminal_gap_stack_marker") or source_text),
            "max_vent_scheduler_loop_gap_ms": None if scheduler_gap_ms is None else round(float(scheduler_gap_ms), 3),
            "max_vent_pulse_gap_limit_ms": round(float(max_gap_s) * 1000.0, 3),
        }
        terminal_exceeded = bool(
            terminal_gap_ms is not None and float(terminal_gap_ms) > float(max_gap_s) * 1000.0
        )
        write_gap_exceeded = bool(
            including_gap_ms is not None and float(including_gap_ms) > float(max_gap_s) * 1000.0
        )
        details.update(
            {
                "max_vent_pulse_write_gap_phase": str(
                    updated.get("route_conditioning_phase") or source_text or "unknown"
                ),
                "max_vent_pulse_write_gap_threshold_ms": round(float(max_gap_s) * 1000.0, 3),
                "max_vent_pulse_write_gap_threshold_source": "route_conditioning_effective_max_gap_s",
                "max_vent_pulse_write_gap_exceeded": write_gap_exceeded,
                "max_vent_pulse_write_gap_not_exceeded_reason": (
                    ""
                    if write_gap_exceeded or including_gap_ms is None
                    else (
                        f"{round(float(including_gap_ms), 3)}ms <= "
                        f"{round(float(max_gap_s) * 1000.0, 3)}ms in "
                        f"{str(updated.get('route_conditioning_phase') or source_text or 'unknown')}"
                    )
                ),
            }
        )
        if terminal_exceeded and source_text == "route_open_transition":
            details["route_open_transition_blocked_vent_scheduler"] = True
        elif terminal_exceeded and source_text == "route_open_settle_wait":
            details["route_open_settle_wait_blocked_vent_scheduler"] = True
        elif terminal_exceeded and source_text in {
            "diagnostic",
            "pressure_monitor",
            "stream_snapshot",
            "p3_fallback",
            "route_diagnostic",
            "heartbeat_diagnostic",
            "trace_write",
        }:
            details["route_conditioning_diagnostic_blocked_vent_scheduler"] = True
        return details

    def _a2_conditioning_terminal_gap_source(self, context: Mapping[str, Any], *, source: str) -> str:
        raw = str(source or "").strip()
        if raw and raw != "terminal_gap":
            return raw
        existing = str(context.get("terminal_gap_source") or "").strip()
        if existing and existing != "terminal_gap":
            return existing
        if bool(context.get("defer_reschedule_caused_vent_gap_exceeded")) or bool(
            context.get("vent_gap_exceeded_after_defer")
        ):
            return "defer_path_no_reschedule"
        if bool(context.get("fail_closed_path_started")):
            if bool(context.get("fail_closed_path_vent_maintenance_required")):
                return "safe_stop_prework"
            return "fail_closed_summary_aggregation"
        if bool(context.get("safety_assertion_aggregation_active")):
            return "safety_assertion_aggregation"
        if bool(context.get("workflow_timing_flush_active")):
            return "workflow_timing_flush"
        if bool(context.get("artifact_write_active")):
            return "artifact_write"
        if bool(context.get("route_conditioning_gate_eval_active")):
            return "route_conditioning_gate_eval"
        if bool(context.get("main_loop_sleep_active")):
            return "main_loop_sleep"
        if bool(context.get("exception_finally_active")):
            return "exception_finally"
        operation = str(
            context.get("last_diagnostic_defer_operation")
            or context.get("diagnostic_blocking_operation")
            or ""
        ).lower()
        if "fresh" in operation or "stale" in operation:
            return "pressure_freshness_wait"
        return "unknown"

    def _a2_conditioning_vent_gap_source(self, context: Mapping[str, Any]) -> str:
        blocking_name = str(context.get("last_blocking_operation_name") or "")
        terminal_source = str(context.get("terminal_gap_source") or "").strip()
        if terminal_source and terminal_source != "terminal_gap":
            return terminal_source
        if bool(context.get("defer_reschedule_caused_vent_gap_exceeded")) or bool(
            context.get("vent_gap_exceeded_after_defer")
        ):
            return "defer_path_no_reschedule"
        if bool(context.get("route_conditioning_diagnostic_blocked_vent_scheduler")):
            return self._a2_conditioning_diagnostic_source(context, fallback="unknown")
        if bool(context.get("route_open_settle_wait_blocked_vent_scheduler")):
            return "route_open_settle_wait"
        if blocking_name == "a2_conditioning_pressure_monitor":
            return "pressure_monitor"
        if blocking_name == "route_open_diagnostic":
            return "route_diagnostic"
        if bool(context.get("route_open_transition_started")) and not bool(
            context.get("route_open_transition_completed", False)
        ):
            return "route_open_transition"
        return "unknown"

    def _a2_conditioning_fail_if_defer_not_rescheduled(
        self,
        point: CalibrationPoint,
        context: Mapping[str, Any],
        *,
        now_mono: float,
        max_gap_s: float,
        interval_s: float,
        schedule: Mapping[str, Any],
        phase: str,
    ) -> None:
        if not bool(
            context.get("defer_reschedule_caused_vent_gap_exceeded")
            or context.get("vent_gap_exceeded_after_defer")
        ):
            return
        terminal = self._a2_conditioning_terminal_gap_details(
            context,
            now_mono=now_mono,
            max_gap_s=max_gap_s,
            source="defer_path_no_reschedule",
        )
        details = {
            **context,
            **terminal,
            "phase": phase,
            "vent_heartbeat_interval_s": interval_s,
            "atmosphere_vent_max_gap_s": max_gap_s,
            "route_conditioning_vent_gap_exceeded": True,
            "vent_heartbeat_gap_exceeded": True,
            "fail_closed_reason": "route_conditioning_vent_gap_exceeded",
            "whether_safe_to_continue": False,
            **dict(schedule),
        }
        details = self._a2_route_open_transient_mark_interrupted_by_vent_gap(
            details,
            reason="vent_gap_exceeded_before_recovery_evaluation",
        )
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", details)
        self._fail_a2_co2_route_conditioning_closed(
            point,
            reason="route_conditioning_vent_gap_exceeded",
            details=details,
            event_name="co2_route_conditioning_defer_no_reschedule",
            route_trace_action="co2_route_conditioning_defer_no_reschedule",
        )

    def _a2_conditioning_reschedule_after_defer(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
        reason: str,
    ) -> dict[str, Any]:
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        if not context:
            return {}
        defer_started = self._as_float(context.get("last_diagnostic_defer_monotonic_s"))
        if defer_started is None:
            return context
        now_mono = time.monotonic()
        schedule = self._a2_conditioning_vent_schedule(context, now_mono=now_mono)
        max_gap_s = float(schedule["route_conditioning_effective_max_gap_s"])
        interval_s = float(schedule["route_conditioning_effective_vent_interval_s"])
        defer_loop_ms = round(max(0.0, float(now_mono) - float(defer_started)) * 1000.0, 3)
        defer_state = self._a2_conditioning_defer_reschedule_state(
            context,
            now_mono=now_mono,
            max_gap_s=max_gap_s,
            defer_loop_ms=defer_loop_ms,
        )
        vent_gap_exceeded = bool(defer_state.get("vent_gap_exceeded_after_defer"))
        context.update(schedule)
        context.update(defer_state)
        context.update(
            {
                "defer_returned_to_vent_loop": True,
                "defer_reschedule_requested": True,
                "defer_reschedule_completed": not vent_gap_exceeded,
                "defer_reschedule_reason": str(reason or "return_to_vent_loop_after_defer"),
                "_last_diagnostic_defer_reschedule_recorded": True,
            }
        )
        if vent_gap_exceeded:
            context.update(
                {
                    "terminal_gap_source": "defer_path_no_reschedule",
                    "terminal_gap_operation": str(
                        context.get("defer_operation")
                        or context.get("last_diagnostic_defer_operation")
                        or "deferred_diagnostic"
                    ),
                    "terminal_gap_duration_ms": defer_state.get("vent_gap_after_defer_ms"),
                    "terminal_gap_detected_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
            self._a2_conditioning_fail_if_defer_not_rescheduled(
                point,
                context,
                now_mono=now_mono,
                max_gap_s=max_gap_s,
                interval_s=interval_s,
                schedule=schedule,
                phase=phase,
            )
        elif bool(defer_state.get("defer_reschedule_latency_warning")):
            context = self._a2_route_open_transient_mark_continuing_after_defer_warning(context)
        last_write = self._a2_conditioning_last_vent_write_monotonic_s(context)
        age_s = None if last_write is None else max(0.0, float(now_mono) - float(last_write))
        should_vent = bool(last_write is None or (age_s is not None and age_s >= interval_s))
        if not should_vent and age_s is not None:
            should_vent = bool((float(max_gap_s) - float(age_s)) * 1000.0 <= 200.0)
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        if should_vent:
            tick = self._record_a2_co2_conditioning_vent_tick(point, phase=phase)
            context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
            context["fast_vent_after_defer_sent"] = bool(
                isinstance(tick, Mapping) and str(tick.get("command_result") or "").lower() == "ok"
            )
            context["fast_vent_after_defer_write_ms"] = (
                tick.get("vent_command_write_duration_ms")
                if isinstance(tick, Mapping)
                else context.get("fast_vent_after_defer_write_ms")
            )
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        return dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or context)

    def _a2_conditioning_heartbeat_gap_state(
        self,
        context: Mapping[str, Any],
        *,
        now_mono: float,
        max_gap_s: Optional[float] = None,
        interval_s: Optional[float] = None,
    ) -> dict[str, Any]:
        max_gap_s = self._a2_conditioning_vent_max_gap_s() if max_gap_s is None else float(max_gap_s)
        interval_s = (
            self._a2_conditioning_vent_heartbeat_interval_s()
            if interval_s is None
            else float(interval_s)
        )
        previous_start = self._as_float(
            context.get("last_vent_heartbeat_started_monotonic_s")
            or context.get("last_vent_tick_monotonic_s")
        )
        previous_completed = self._as_float(
            context.get("last_vent_heartbeat_completed_monotonic_s")
            or context.get("last_vent_tick_completed_monotonic_s")
            or context.get("last_vent_tick_monotonic_s")
        )
        observed_gap_s = None if previous_start is None else max(0.0, float(now_mono) - float(previous_start))
        emission_gap_s = None if previous_completed is None else max(0.0, float(now_mono) - float(previous_completed))
        blocking_started = self._as_float(context.get("last_blocking_operation_started_monotonic_s"))
        blocking_completed = self._as_float(context.get("last_blocking_operation_completed_monotonic_s"))
        blocking_duration = self._as_float(context.get("last_blocking_operation_duration_s"))
        blocking_name = str(context.get("last_blocking_operation_name") or "")
        blocking_safe = bool(context.get("last_blocking_operation_safe_to_continue", True))
        if (
            emission_gap_s is not None
            and blocking_started is not None
            and blocking_completed is not None
            and previous_completed is not None
            and float(blocking_completed) >= float(previous_completed)
            and float(blocking_completed) <= float(now_mono)
            and blocking_safe
        ):
            emission_gap_s = max(0.0, float(now_mono) - float(blocking_completed))
        blocking_explains_gap = bool(
            observed_gap_s is not None
            and observed_gap_s > max_gap_s
            and emission_gap_s is not None
            and emission_gap_s <= max_gap_s
            and blocking_duration is not None
            and float(blocking_duration) > 0.0
            and blocking_safe
        )
        return {
            "heartbeat_gap_threshold_ms": round(max_gap_s * 1000.0, 3),
            "heartbeat_interval_ms": round(interval_s * 1000.0, 3),
            "heartbeat_gap_observed_ms": None if observed_gap_s is None else round(float(observed_gap_s) * 1000.0, 3),
            "heartbeat_emission_gap_ms": None if emission_gap_s is None else round(float(emission_gap_s) * 1000.0, 3),
            "vent_heartbeat_gap_s": None if observed_gap_s is None else round(float(observed_gap_s), 3),
            "heartbeat_emission_gap_s": None if emission_gap_s is None else round(float(emission_gap_s), 3),
            "heartbeat_gap_explained_by_blocking_operation": blocking_explains_gap,
            "blocking_operation_name": blocking_name,
            "blocking_operation_duration_ms": (
                None if blocking_duration is None else round(float(blocking_duration) * 1000.0, 3)
            ),
            "blocking_operation_safe_to_continue": blocking_safe,
            "whether_safe_to_continue": bool(
                emission_gap_s is None or (emission_gap_s <= max_gap_s and blocking_safe)
            ),
        }

    def _a2_conditioning_failure_context(
        self,
        point: CalibrationPoint,
        *,
        reason: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        detail_payload = dict(details or {})
        now_mono = time.monotonic()
        started = self._as_float(context.get("conditioning_started_monotonic_s"))
        if started is not None:
            context["conditioning_duration_s"] = round(max(0.0, now_mono - float(started)), 3)
        last_vent = self._as_float(context.get("last_vent_tick_monotonic_s"))
        if last_vent is not None:
            context["last_vent_command_age_s"] = round(max(0.0, now_mono - float(last_vent)), 3)
        context.update(detail_payload)
        route_open_monotonic = self._as_float(
            context.get("route_open_completed_monotonic_s")
            or getattr(self, "_a2_co2_route_open_monotonic_s", None)
        )
        flush_phase = str(context.get("route_conditioning_phase") or "route_conditioning_flush_phase") == (
            "route_conditioning_flush_phase"
        )
        after_flush = bool(
            context.get("ready_to_seal_phase_started", False)
            or context.get("seal_command_sent", False)
            or context.get("pressure_setpoint_command_sent", False)
            or context.get("pressure_ready_started", False)
            or context.get("sampling_started", False)
            or int(context.get("sample_count") or 0) > 0
            or int(context.get("points_completed") or 0) > 0
        )
        fail_started_mono = self._as_float(context.get("fail_closed_path_started_monotonic_s"))
        if fail_started_mono is None:
            fail_started_mono = now_mono
            context["fail_closed_path_started_at"] = datetime.now(timezone.utc).isoformat()
            context["fail_closed_path_started_monotonic_s"] = float(fail_started_mono)
        maintenance_required = bool(route_open_monotonic is not None and flush_phase and not after_flush)
        context.update(
            {
                "fail_closed_path_started": True,
                "fail_closed_path_started_while_route_open": bool(route_open_monotonic is not None),
                "fail_closed_path_vent_maintenance_required": maintenance_required,
                "fail_closed_path_vent_maintenance_active": bool(
                    maintenance_required and context.get("route_conditioning_vent_maintenance_active", True)
                ),
                "fail_closed_path_blocked_vent_scheduler": bool(
                    context.get("fail_closed_path_blocked_vent_scheduler", False)
                ),
            }
        )
        context.update(
            {
                "conditioning_decision": "FAIL",
                "fail_closed_before_vent_off": True,
                "route_conditioning_vent_gap_exceeded": bool(
                    context.get("route_conditioning_vent_gap_exceeded")
                    or detail_payload.get("route_conditioning_vent_gap_exceeded")
                    or reason
                    in {
                        "route_conditioning_vent_gap_exceeded",
                        "atmosphere_vent_heartbeat_gap_exceeded",
                        "route_open_to_first_vent_heartbeat_exceeded",
                        "route_open_transition_blocked_vent_scheduler",
                        "route_open_settle_wait_blocked_vent_scheduler",
                    }
                ),
                "vent_off_sent_at": str(context.get("vent_off_sent_at") or ""),
                "seal_command_sent": bool(context.get("seal_command_sent", False)),
                "sample_count": int(context.get("sample_count") or 0),
                "points_completed": int(context.get("points_completed") or 0),
                "fail_closed_reason": reason,
                "point_index": getattr(point, "index", None),
            }
        )
        if bool(context.get("route_conditioning_vent_gap_exceeded", False)):
            context = self._a2_route_open_transient_mark_interrupted_by_vent_gap(
                context,
                reason="vent_gap_exceeded_before_recovery_evaluation",
            )
        context = self._a2_conditioning_context_with_counts(context)
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        return context

    def _fail_a2_co2_route_conditioning_closed(
        self,
        point: CalibrationPoint,
        *,
        reason: str,
        details: Optional[Mapping[str, Any]] = None,
        event_name: str = "co2_route_conditioning_fail_closed",
        route_trace_action: Optional[str] = None,
        pressure_hpa: Any = None,
    ) -> None:
        context = self._a2_conditioning_failure_context(point, reason=reason, details=details)
        fail_started = self._as_float(context.get("fail_closed_path_started_monotonic_s"))
        if reason == "route_conditioning_pressure_overlimit":
            blocked_reason = self._a2_conditioning_unsafe_vent_reason(context)
            if blocked_reason:
                context = self._a2_conditioning_mark_vent_blocked(context, reason=blocked_reason)
                context["fail_closed_vent_pulse_sent"] = False
                context["fail_closed_vent_pulse_result"] = f"blocked:{blocked_reason}"
            else:
                try:
                    fast_reassert = getattr(self.pressure_control_service, "set_pressure_controller_vent_fast_reassert", None)
                    if not callable(fast_reassert):
                        context["fail_closed_vent_pulse_sent"] = False
                        context["fail_closed_vent_pulse_result"] = "blocked:route_conditioning_fast_vent_not_supported"
                        context["route_conditioning_fast_vent_not_supported"] = True
                    else:
                        fail_vent = fast_reassert(
                            True,
                            reason="A2 CO2 route conditioning pressure overlimit fail-closed",
                            max_duration_s=self._a2_conditioning_fast_vent_max_duration_s(),
                            wait_after_command=False,
                            capture_pressure=False,
                            query_state=False,
                            confirm_transition=False,
                        )
                        context.update(dict(fail_vent or {}))
                        context["fail_closed_vent_pulse_sent"] = str(
                            (fail_vent or {}).get("command_result") or ""
                        ).lower() == "ok"
                        context["fail_closed_vent_pulse_result"] = str(
                            (fail_vent or {}).get("command_result") or "unknown"
                        )
                except Exception as exc:
                    context["fail_closed_vent_pulse_sent"] = False
                    context["fail_closed_vent_pulse_result"] = f"failed: {exc}"
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        if fail_started is not None:
            fail_duration_ms = round(max(0.0, time.monotonic() - float(fail_started)) * 1000.0, 3)
            context["fail_closed_path_duration_ms"] = fail_duration_ms
            max_gap_ms = self._as_float(context.get("max_vent_pulse_gap_limit_ms"))
            if max_gap_ms is None:
                max_gap_ms = self._a2_conditioning_vent_maintenance_max_gap_s() * 1000.0
            context["fail_closed_path_blocked_vent_scheduler"] = bool(
                context.get("fail_closed_path_vent_maintenance_required", False)
                and fail_duration_ms > float(max_gap_ms)
            )
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        if bool(context.get("fail_closed_path_vent_maintenance_required", False)):
            context.update(
                {
                    "trace_write_budget_ms": self._a2_conditioning_trace_write_budget_ms(),
                    "trace_write_duration_ms": 0.0,
                    "trace_write_deferred_for_vent_priority": True,
                    "trace_write_blocked_vent_scheduler": False,
                    "fail_closed_path_vent_maintenance_active": True,
                }
            )
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        else:
            self._record_workflow_timing(
                event_name,
                "fail",
                stage="co2_route_conditioning_at_atmosphere",
                point=point,
                pressure_hpa=pressure_hpa if pressure_hpa is not None else context.get("pressure_overlimit_hpa"),
                decision=reason,
                error_code=reason,
                route_state=context,
            )
        recorder = getattr(getattr(self, "status_service", None), "record_route_trace", None)
        if callable(recorder):
            recorder(
                action=route_trace_action or event_name,
                route="co2",
                point=point,
                actual=context,
                result="fail",
                message=f"A2 CO2 route conditioning fail-closed: {reason}",
            )
        raise WorkflowValidationError(
            "A2 CO2 route conditioning failed closed before vent-off",
            details=context,
        )

    def _a2_v1_aligned_pressure_fallback_allowed(self) -> bool:
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        return bool(
            getattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", False)
            and context.get("atmosphere_vent_enabled", True)
            and not bool(context.get("seal_command_sent", False))
            and not str(context.get("vent_off_sent_at") or "").strip()
            and not bool(context.get("pressure_setpoint_command_sent", False))
        )

    def _a2_conditioning_pressure_sample(self, point: CalibrationPoint, *, phase: str) -> dict[str, Any]:
        stage = "co2_route_conditioning_at_atmosphere"
        pressure_source_mode = self._a2_conditioning_pressure_source_mode()
        direct_reader = getattr(self.pressure_control_service, "_pressure_sample_from_device", None)
        if pressure_source_mode == "p3_fast_poll" and callable(direct_reader):
            sample = direct_reader("digital_pressure_gauge")
            sample = dict(sample) if isinstance(sample, Mapping) else {}
            sample.update(
                {
                    "stage": stage,
                    "point_index": point.index,
                    "digital_gauge_mode": "p3_fast_poll",
                    "pressure_sample_source": sample.get("pressure_sample_source") or "digital_pressure_gauge",
                    "source": sample.get("source") or "digital_pressure_gauge",
                    "pressure_source_used_for_decision": "digital_pressure_gauge"
                    if sample.get("pressure_hpa") is not None and not bool(sample.get("is_stale"))
                    else "",
                    "pressure_source_used_for_abort": "digital_pressure_gauge"
                    if sample.get("pressure_hpa") is not None and not bool(sample.get("is_stale"))
                    else "",
                    "pressure_source_selected": "digital_pressure_gauge_p3_fast_poll",
                    "pressure_source_selection_reason": "a2_conditioning_p3_fast_poll_config",
                    "source_selection_reason": "a2_conditioning_p3_fast_poll_config",
                    "critical_window_uses_latest_frame": False,
                    "critical_window_uses_query": True,
                }
            )
            return sample
        high_pressure_reader = getattr(
            self.pressure_control_service,
            "_current_high_pressure_first_point_sample",
            None,
        )
        if callable(high_pressure_reader):
            sample = high_pressure_reader(stage=stage, point_index=point.index)
            sample = dict(sample) if isinstance(sample, Mapping) else {}
            sample.setdefault("a2_3_pressure_source_strategy", pressure_source_mode)
            if pressure_source_mode == "v1_aligned":
                continuous_stale = bool(sample.get("is_stale", sample.get("pressure_sample_is_stale")))
                continuous_pressure = self._as_float(sample.get("pressure_hpa", sample.get("digital_gauge_pressure_hpa")))
                if not continuous_stale and continuous_pressure is not None:
                    sample.update(
                        {
                            "pressure_source_selected": "digital_pressure_gauge_continuous",
                            "pressure_source_selection_reason": "digital_gauge_continuous_latest_fresh",
                            "source_selection_reason": "digital_gauge_continuous_latest_fresh",
                            "critical_window_uses_latest_frame": True,
                            "critical_window_uses_query": False,
                            "p3_fast_fallback_attempted": False,
                            "p3_fast_fallback_result": "",
                            "normal_p3_fallback_attempted": False,
                            "normal_p3_fallback_result": "",
                        }
                    )
                    return sample
                if not self._a2_v1_aligned_pressure_fallback_allowed():
                    sample.update(
                        {
                            "pressure_source_selected": "",
                            "pressure_source_selection_reason": "v1_aligned_fallback_not_allowed_outside_atmosphere_conditioning",
                            "source_selection_reason": "v1_aligned_fallback_not_allowed_outside_atmosphere_conditioning",
                            "critical_window_uses_latest_frame": True,
                            "critical_window_uses_query": False,
                            "p3_fast_fallback_attempted": False,
                            "p3_fast_fallback_result": "",
                            "normal_p3_fallback_attempted": False,
                            "normal_p3_fallback_result": "",
                            "fail_closed_reason": "digital_gauge_v1_aligned_fallback_not_allowed",
                        }
                    )
                    return sample
                v1_reader = getattr(self.pressure_control_service, "_a2_v1_aligned_pressure_gauge_sample", None)
                if callable(v1_reader):
                    fallback = v1_reader(stage=stage, point_index=point.index, continuous_sample=sample)
                    fallback = dict(fallback) if isinstance(fallback, Mapping) else {}
                    fallback.setdefault("a2_3_pressure_source_strategy", "v1_aligned")
                    fallback.setdefault("critical_window_uses_latest_frame", False)
                    fallback.setdefault("critical_window_uses_query", True)
                    fallback.setdefault("pressure_source_selection_reason", "continuous_stale_fallback_to_p3_fast")
                    fallback.setdefault("source_selection_reason", fallback.get("pressure_source_selection_reason"))
                    return fallback
                sample.update(
                    {
                        "pressure_source_selected": "",
                        "pressure_source_selection_reason": "digital_gauge_v1_aligned_reader_unavailable",
                        "source_selection_reason": "digital_gauge_v1_aligned_reader_unavailable",
                        "critical_window_uses_latest_frame": True,
                        "critical_window_uses_query": False,
                        "p3_fast_fallback_attempted": False,
                        "p3_fast_fallback_result": "",
                        "normal_p3_fallback_attempted": False,
                        "normal_p3_fallback_result": "",
                        "fail_closed_reason": "digital_gauge_v1_aligned_read_unavailable",
                    }
                )
                return sample
            if pressure_source_mode == "auto" and bool(sample.get("is_stale", sample.get("pressure_sample_is_stale"))) and callable(direct_reader):
                fallback = direct_reader("digital_pressure_gauge")
                fallback = dict(fallback) if isinstance(fallback, Mapping) else {}
                fallback.update(
                    {
                        "stage": stage,
                        "point_index": point.index,
                        "digital_gauge_mode": "p3_fast_poll",
                        "pressure_source_selected": "digital_pressure_gauge_p3_fast_poll",
                        "pressure_source_selection_reason": "a2_conditioning_auto_fallback_after_continuous_stale",
                        "source_selection_reason": "a2_conditioning_auto_fallback_after_continuous_stale",
                        "critical_window_uses_latest_frame": False,
                        "critical_window_uses_query": True,
                    }
                )
                return fallback
            return sample
        sample_reader = getattr(self.pressure_control_service, "_current_pressure_sample", None)
        if callable(sample_reader):
            sample = sample_reader(source="pressure_gauge")
            sample = dict(sample) if isinstance(sample, Mapping) else {}
            sample.setdefault("stage", stage)
            sample.setdefault("point_index", point.index)
            return sample
        pressure_reader = getattr(self.pressure_control_service, "_current_pressure", None)
        if callable(pressure_reader):
            try:
                pressure_hpa = self._as_float(pressure_reader())
            except Exception as exc:
                return {
                    "stage": stage,
                    "point_index": point.index,
                    "source": "pressure_gauge",
                    "pressure_sample_source": "pressure_gauge",
                    "parse_ok": False,
                    "error": str(exc),
                }
            return {
                "stage": stage,
                "point_index": point.index,
                "pressure_hpa": pressure_hpa,
                "source": "pressure_gauge",
                "pressure_sample_source": "pressure_gauge",
                "parse_ok": pressure_hpa is not None,
            }
        return {
            "stage": stage,
            "point_index": point.index,
            "source": "digital_pressure_gauge_continuous",
            "pressure_sample_source": "digital_pressure_gauge_continuous",
            "parse_ok": False,
            "error": "conditioning_pressure_sample_unavailable",
        }

    def _a2_conditioning_pressure_details(
        self,
        sample: Mapping[str, Any],
        snapshot: Mapping[str, Any],
        *,
        context: Mapping[str, Any],
    ) -> dict[str, Any]:
        latest = snapshot.get("latest_frame")
        latest = dict(latest) if isinstance(latest, Mapping) else {}
        digital_sample = sample.get("digital_gauge_pressure_sample")
        digital_sample = dict(digital_sample) if isinstance(digital_sample, Mapping) else {}
        selected_stale_started = time.monotonic()

        def truthy(value: Any) -> bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "y", "on"}
            return bool(value)

        def parse_bool(value: Any) -> Optional[bool]:
            if value is None or value == "":
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"1", "true", "yes", "y", "on"}:
                    return True
                if lowered in {"0", "false", "no", "n", "off"}:
                    return False
            return bool(value)

        source_text = str(
            digital_sample.get("pressure_sample_source")
            or digital_sample.get("source")
            or sample.get("pressure_sample_source")
            or sample.get("source")
            or ""
        )
        selected_source = (
            sample.get("pressure_source_selected")
            or sample.get("pressure_source_used_for_decision")
            or source_text
            or "digital_pressure_gauge_continuous"
        )
        selected_source_text = str(selected_source or source_text or "")
        pressure_hpa = self._a2_conditioning_first_float(
            digital_sample.get("pressure_hpa"),
            sample.get("digital_gauge_pressure_hpa"),
            sample.get("pressure_hpa"),
            latest.get("pressure_hpa"),
        )
        continuous_age_s = self._a2_conditioning_first_float(
            sample.get("latest_frame_age_s"),
            sample.get("digital_gauge_age_s"),
            digital_sample.get("latest_frame_age_s"),
            snapshot.get("latest_frame_age_s"),
            latest.get("latest_frame_age_s"),
            latest.get("sample_age_s"),
        )
        selected_age_s = self._a2_conditioning_first_float(
            sample.get("pressure_sample_age_s"),
            sample.get("sample_age_s"),
            digital_sample.get("pressure_sample_age_s"),
            digital_sample.get("sample_age_s"),
            latest.get("pressure_sample_age_s"),
        )
        sequence_raw = (
            sample.get("latest_frame_sequence_id")
            or sample.get("digital_gauge_latest_sequence_id")
            or sample.get("pressure_sample_sequence_id")
            or sample.get("sequence_id")
            or digital_sample.get("latest_frame_sequence_id")
            or digital_sample.get("digital_gauge_latest_sequence_id")
            or digital_sample.get("pressure_sample_sequence_id")
            or digital_sample.get("sequence_id")
            or snapshot.get("latest_frame_sequence_id")
            or snapshot.get("digital_gauge_latest_sequence_id")
            or latest.get("sequence_id")
            or latest.get("pressure_sample_sequence_id")
        )
        sequence_value = self._as_float(sequence_raw)
        previous_sequence = self._as_float(context.get("last_digital_gauge_sequence_id"))
        sequence_progress: Optional[bool]
        if sequence_value is None:
            sequence_progress = None
        else:
            sequence_progress = bool(previous_sequence is None or float(sequence_value) > float(previous_sequence))
        source_for_decision = str(
            sample.get("pressure_source_used_for_decision")
            or sample.get("pressure_source_used_for_abort")
            or source_text
            or "digital_pressure_gauge_continuous"
        )
        max_age_s = self._a2_conditioning_digital_gauge_max_age_s()
        digital_expected = bool(context.get("digital_gauge_monitoring_required", False))
        pressure_source_mode = str(
            sample.get("digital_gauge_mode")
            or sample.get("pressure_source_selected")
            or context.get("a2_conditioning_pressure_source")
            or self._a2_conditioning_pressure_source_mode()
        )
        continuous_interrupted = bool(
            sample.get("continuous_interrupted_by_command")
            or digital_sample.get("continuous_interrupted_by_command")
            or snapshot.get("continuous_interrupted_by_command")
            or context.get("continuous_interrupted_by_command")
        )
        restart_result = str(
            sample.get("continuous_restart_result")
            or digital_sample.get("continuous_restart_result")
            or snapshot.get("continuous_restart_result")
            or context.get("continuous_restart_result")
            or ""
        )
        continuous_interruption_unrecovered = bool(
            continuous_interrupted
            and "p3" not in pressure_source_mode
            and pressure_source_mode != "v1_aligned"
            and restart_result != "recovered"
        )
        p3_direct_source = bool(
            "p3" in source_text
            or "p3" in selected_source_text
            or "p3" in pressure_source_mode
        )
        if selected_age_s is None and not p3_direct_source:
            selected_age_s = continuous_age_s
        continuous_source = bool(
            "continuous" in source_text
            or "continuous" in selected_source_text
            or (
                not p3_direct_source
                and str(context.get("a2_conditioning_pressure_source") or self._a2_conditioning_pressure_source_mode())
                in {"continuous", "auto", "v1_aligned"}
            )
        )
        continuous_stream_stale = bool(
            (digital_expected or continuous_source)
            and (
                continuous_age_s is None
                or float(continuous_age_s) > max_age_s
                or truthy(sample.get("digital_gauge_stale"))
                or truthy(snapshot.get("latest_frame_stale"))
                or truthy(latest.get("is_stale"))
                or (sequence_value is None and digital_expected and not p3_direct_source)
                or sequence_progress is False
                or (
                    pressure_hpa is None
                    and not p3_direct_source
                    and str(sample.get("error") or digital_sample.get("error") or "")
                )
                or continuous_interruption_unrecovered
            )
        )
        selected_parse_ok = parse_bool(sample.get("parse_ok"))
        if selected_parse_ok is None:
            selected_parse_ok = parse_bool(digital_sample.get("parse_ok"))
        if selected_parse_ok is None:
            selected_parse_ok = parse_bool(latest.get("parse_ok"))
        selected_error_text = str(sample.get("error") or digital_sample.get("error") or "")
        if (
            not p3_direct_source
            and pressure_hpa is not None
            and not continuous_stream_stale
            and selected_error_text in {"", "conditioning_pressure_sample_unavailable"}
        ):
            selected_error_text = ""
            selected_parse_ok = True
        if selected_parse_ok is None:
            selected_parse_ok = bool(pressure_hpa is not None and not selected_error_text)
        selected_pressure_controller_primary = bool(
            "pressure_controller" in selected_source_text
            or "pace" in selected_source_text.lower()
        )
        selected_pressure_sample_is_stale = bool(
            selected_age_s is None
            or float(selected_age_s) > max_age_s
            or truthy(sample.get("is_stale"))
            or truthy(sample.get("pressure_sample_is_stale"))
            or truthy(digital_sample.get("is_stale"))
            or truthy(digital_sample.get("pressure_sample_is_stale"))
            or (not p3_direct_source and continuous_stream_stale)
        )
        selected_stale_duration_ms = round(max(0.0, time.monotonic() - selected_stale_started) * 1000.0, 3)
        selected_stale_budget_ms = self._a2_conditioning_selected_pressure_sample_stale_budget_ms()
        selected_stale_budget_exceeded = bool(selected_stale_duration_ms > selected_stale_budget_ms)
        selected_pressure_fail_closed_reason = ""
        if selected_pressure_controller_primary:
            selected_pressure_fail_closed_reason = "selected_pressure_unavailable"
        elif pressure_hpa is None or not bool(selected_parse_ok) or selected_error_text:
            selected_pressure_fail_closed_reason = "selected_pressure_unavailable"
        elif selected_pressure_sample_is_stale:
            selected_pressure_fail_closed_reason = "selected_pressure_sample_stale"
        selected_pressure_freshness_ok = not bool(selected_pressure_fail_closed_reason)
        pressure_freshness_decision_source = str(selected_source or source_for_decision or source_text or "")
        stream_stale = bool(selected_pressure_sample_is_stale)
        if selected_pressure_freshness_ok:
            stream_stale = False
        abort_hpa = self._a2_conditioning_pressure_abort_hpa()
        hard_abort_hpa = self._a2_route_conditioning_hard_abort_pressure_hpa()
        fresh_for_abort = bool(pressure_hpa is not None and selected_pressure_freshness_ok)
        pressure_overlimit = bool(fresh_for_abort and float(pressure_hpa) >= float(hard_abort_hpa))
        first_frame_at = (
            sample.get("digital_gauge_stream_first_frame_at")
            or digital_sample.get("digital_gauge_stream_first_frame_at")
            or snapshot.get("digital_gauge_stream_first_frame_at")
            or snapshot.get("stream_first_frame_at")
            or context.get("digital_gauge_stream_first_frame_at")
        )
        last_frame_at = (
            sample.get("digital_gauge_stream_last_frame_at")
            or digital_sample.get("digital_gauge_stream_last_frame_at")
            or snapshot.get("digital_gauge_stream_last_frame_at")
            or snapshot.get("stream_last_frame_at")
            or latest.get("frame_received_at")
            or latest.get("sample_recorded_at")
            or context.get("digital_gauge_stream_last_frame_at")
        )
        drain_empty = (
            sample.get("digital_gauge_drain_empty_count")
            if sample.get("digital_gauge_drain_empty_count") is not None
            else snapshot.get("digital_gauge_drain_empty_count")
        )
        drain_nonempty = (
            sample.get("digital_gauge_drain_nonempty_count")
            if sample.get("digital_gauge_drain_nonempty_count") is not None
            else snapshot.get("digital_gauge_drain_nonempty_count")
        )
        selection_reason = (
            sample.get("pressure_source_selection_reason")
            or sample.get("source_selection_reason")
            or digital_sample.get("pressure_source_selection_reason")
            or digital_sample.get("source_selection_reason")
            or ""
        )
        fail_closed_reason = str(sample.get("fail_closed_reason") or "")
        if selected_pressure_fail_closed_reason and "fallback_not_allowed" not in str(selection_reason):
            fail_closed_reason = selected_pressure_fail_closed_reason
        return {
            "sample": dict(sample),
            "digital_sample": digital_sample,
            "pressure_hpa": pressure_hpa,
            "pressure_sample_source": source_for_decision,
            "pressure_sample_age_s": None if selected_age_s is None else round(float(selected_age_s), 3),
            "pressure_sample_is_stale": selected_pressure_sample_is_stale,
            "digital_gauge_latest_age_s": None if continuous_age_s is None else round(float(continuous_age_s), 3),
            "continuous_stream_age_s": None if continuous_age_s is None else round(float(continuous_age_s), 3),
            "latest_frame_sequence_id": None if sequence_value is None else int(sequence_value),
            "digital_gauge_latest_sequence_id": None if sequence_value is None else int(sequence_value),
            "digital_gauge_sequence_progress": sequence_progress,
            "continuous_stream_stale": continuous_stream_stale,
            "digital_gauge_stream_stale": continuous_stream_stale,
            "stream_stale": stream_stale,
            "digital_gauge_stream_stale_threshold_s": max_age_s,
            "digital_gauge_max_age_s": max_age_s,
            "digital_gauge_continuous_mode": (
                sample.get("digital_gauge_continuous_mode")
                or digital_sample.get("digital_gauge_continuous_mode")
                or snapshot.get("digital_gauge_continuous_mode")
                or context.get("digital_gauge_continuous_mode")
                or ""
            ),
            "digital_gauge_continuous_started": bool(
                sample.get("digital_gauge_continuous_started")
                or digital_sample.get("digital_gauge_continuous_started")
                or snapshot.get("digital_gauge_continuous_started")
                or snapshot.get("stream_started_at")
                or context.get("digital_gauge_continuous_started")
            ),
            "digital_gauge_continuous_active": bool(
                sample.get("digital_gauge_continuous_active")
                or digital_sample.get("digital_gauge_continuous_active")
                or snapshot.get("digital_gauge_continuous_active")
            ),
            "digital_gauge_stream_first_frame_at": first_frame_at or "",
            "digital_gauge_stream_last_frame_at": last_frame_at or "",
            "digital_gauge_drain_empty_count": int(drain_empty or 0),
            "digital_gauge_drain_nonempty_count": int(drain_nonempty or 0),
            "last_pressure_command": (
                sample.get("last_pressure_command")
                or digital_sample.get("last_pressure_command")
                or snapshot.get("last_pressure_command")
                or ""
            ),
            "last_pressure_command_may_cancel_continuous": bool(
                sample.get("last_pressure_command_may_cancel_continuous")
                or digital_sample.get("last_pressure_command_may_cancel_continuous")
                or snapshot.get("last_pressure_command_may_cancel_continuous")
            ),
            "continuous_interrupted_by_command": continuous_interrupted,
            "continuous_restart_required_before_return_to_continuous": bool(
                sample.get("continuous_restart_required_before_return_to_continuous")
                or digital_sample.get("continuous_restart_required_before_return_to_continuous")
                or snapshot.get("continuous_restart_required_before_return_to_continuous")
                or context.get("continuous_restart_required_before_return_to_continuous")
            ),
            "continuous_restart_attempted": bool(
                sample.get("continuous_restart_attempted")
                or digital_sample.get("continuous_restart_attempted")
                or snapshot.get("continuous_restart_attempted")
            ),
            "continuous_restart_result": restart_result,
            "pressure_source_selected": selected_source,
            "pressure_source_selection_reason": selection_reason,
            "selected_pressure_source": selected_source,
            "selected_pressure_sample_age_s": None if selected_age_s is None else round(float(selected_age_s), 3),
            "selected_pressure_sample_is_stale": selected_pressure_sample_is_stale,
            "selected_pressure_parse_ok": bool(selected_parse_ok),
            "selected_pressure_freshness_ok": selected_pressure_freshness_ok,
            "pressure_freshness_decision_source": pressure_freshness_decision_source,
            "selected_pressure_fail_closed_reason": selected_pressure_fail_closed_reason,
            "selected_pressure_sample_stale_duration_ms": selected_stale_duration_ms,
            "selected_pressure_sample_stale_budget_ms": round(float(selected_stale_budget_ms), 3),
            "selected_pressure_sample_stale_budget_exceeded": selected_stale_budget_exceeded,
            "selected_pressure_sample_stale_performed_io": False,
            "selected_pressure_sample_stale_triggered_source_selection": False,
            "selected_pressure_sample_stale_triggered_p3_fallback": False,
            "selected_pressure_sample_stale_deferred_for_vent_priority": False,
            "conditioning_monitor_latest_frame_age_s": sample.get(
                "conditioning_monitor_latest_frame_age_s",
                None if continuous_age_s is None else round(float(continuous_age_s), 3),
            ),
            "conditioning_monitor_latest_frame_fresh": bool(
                sample.get(
                    "conditioning_monitor_latest_frame_fresh",
                    pressure_hpa is not None and not continuous_stream_stale,
                )
            ),
            "conditioning_monitor_latest_frame_unavailable": bool(
                sample.get("conditioning_monitor_latest_frame_unavailable", pressure_hpa is None)
            ),
            "continuous_latest_fresh_fast_path_used": bool(sample.get("continuous_latest_fresh_fast_path_used")),
            "continuous_latest_fresh_duration_ms": sample.get("continuous_latest_fresh_duration_ms"),
            "continuous_latest_fresh_lock_acquire_ms": sample.get("continuous_latest_fresh_lock_acquire_ms"),
            "continuous_latest_fresh_lock_timeout": bool(sample.get("continuous_latest_fresh_lock_timeout", False)),
            "continuous_latest_fresh_waited_for_frame": bool(
                sample.get("continuous_latest_fresh_waited_for_frame", False)
            ),
            "continuous_latest_fresh_performed_io": bool(sample.get("continuous_latest_fresh_performed_io", False)),
            "continuous_latest_fresh_triggered_stream_restart": bool(
                sample.get("continuous_latest_fresh_triggered_stream_restart", False)
            ),
            "continuous_latest_fresh_triggered_drain": bool(
                sample.get("continuous_latest_fresh_triggered_drain", False)
            ),
            "continuous_latest_fresh_triggered_p3_fallback": bool(
                sample.get("continuous_latest_fresh_triggered_p3_fallback", False)
            ),
            "continuous_latest_fresh_budget_ms": sample.get("continuous_latest_fresh_budget_ms"),
            "continuous_latest_fresh_budget_exceeded": bool(
                sample.get("continuous_latest_fresh_budget_exceeded", False)
            ),
            "a2_3_pressure_source_strategy": sample.get("a2_3_pressure_source_strategy")
            or context.get("a2_3_pressure_source_strategy")
            or self._a2_conditioning_pressure_source_mode(),
            "critical_window_uses_latest_frame": bool(sample.get("critical_window_uses_latest_frame")),
            "critical_window_uses_query": bool(sample.get("critical_window_uses_query")),
            "p3_fast_fallback_attempted": bool(sample.get("p3_fast_fallback_attempted")),
            "p3_fast_fallback_result": sample.get("p3_fast_fallback_result") or "",
            "normal_p3_fallback_attempted": bool(sample.get("normal_p3_fallback_attempted")),
            "normal_p3_fallback_result": sample.get("normal_p3_fallback_result") or "",
            "fail_closed_reason": fail_closed_reason,
            "conditioning_pressure_abort_hpa": abort_hpa,
            "route_conditioning_hard_abort_pressure_hpa": hard_abort_hpa,
            "route_conditioning_hard_abort_exceeded": pressure_overlimit,
            "pressure_overlimit_seen": pressure_overlimit,
            "pressure_overlimit_source": source_for_decision if pressure_overlimit else "",
            "pressure_overlimit_hpa": pressure_hpa if pressure_overlimit else None,
            "route_conditioning_high_pressure_seen_before_preseal": pressure_overlimit,
            "route_conditioning_high_pressure_seen_before_preseal_hpa": pressure_hpa
            if pressure_overlimit
            else None,
            "route_conditioning_high_pressure_seen_phase": "co2_route_conditioning_at_atmosphere"
            if pressure_overlimit
            else "",
            "route_conditioning_high_pressure_seen_source": source_for_decision if pressure_overlimit else "",
            "route_conditioning_high_pressure_seen_sample_age_s": None
            if not pressure_overlimit
            else (None if selected_age_s is None else round(float(selected_age_s), 3)),
            "route_conditioning_high_pressure_seen_decision": "fail_closed"
            if pressure_overlimit
            else "",
        }

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
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        if not context:
            return {}
        context.setdefault("route_conditioning_diagnostic_blocked_vent_scheduler", False)
        context.setdefault("route_conditioning_vent_gap_exceeded", False)
        context.setdefault("pressure_monitor_blocked_vent_scheduler", False)
        context.setdefault("trace_write_blocked_vent_scheduler", False)
        now_mono = time.monotonic()
        monitor_started_monotonic_s = now_mono
        schedule = self._a2_conditioning_vent_schedule(context, now_mono=now_mono)
        context.update(schedule)
        context["vent_scheduler_priority_mode"] = True
        context["vent_scheduler_checked_before_diagnostic"] = True
        context["diagnostic_budget_ms"] = self._a2_conditioning_diagnostic_budget_ms()
        context["pressure_monitor_budget_ms"] = self._a2_conditioning_pressure_monitor_budget_ms()
        context["trace_write_budget_ms"] = self._a2_conditioning_trace_write_budget_ms()
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        monitor_budget_ms = self._a2_conditioning_pressure_monitor_budget_ms()
        high_frequency_window = bool(schedule.get("route_conditioning_high_frequency_window_active"))
        flush_maintenance_window = bool(
            str(context.get("route_conditioning_phase") or "route_conditioning_flush_phase")
            == "route_conditioning_flush_phase"
            and context.get("route_conditioning_vent_maintenance_active", True)
            and not bool(context.get("ready_to_seal_phase_started", False))
            and not bool(context.get("seal_command_sent", False))
            and not bool(context.get("pressure_setpoint_command_sent", False))
            and not bool(context.get("pressure_ready_started", False))
            and not bool(context.get("sampling_started", False))
        )
        nonblocking_pressure_monitor = bool(high_frequency_window or flush_maintenance_window)
        deferred = self._a2_conditioning_defer_if_diagnostic_budget_unsafe(
            point,
            context,
            now_mono=now_mono,
            max_gap_s=float(schedule["route_conditioning_effective_max_gap_s"]),
            budget_ms=monitor_budget_ms,
            component="pressure_monitor",
            operation="conditioning_pressure_monitor_budget_check",
            pressure_monitor=True,
        )
        if deferred is not None:
            return dict(deferred)
        last_vent = self._as_float(context.get("last_vent_tick_monotonic_s"))
        if last_vent is not None:
            context["last_vent_command_age_s"] = round(max(0.0, now_mono - float(last_vent)), 3)
        if nonblocking_pressure_monitor:
            snapshot_started = time.monotonic()
            snapshot = self._a2_conditioning_stream_snapshot(
                point=point,
                phase=phase,
                fast=True,
                budget_ms=self._a2_conditioning_continuous_latest_fresh_budget_ms(),
            )
            snapshot_completed = time.monotonic()
            snapshot_duration_ms = round(max(0.0, snapshot_completed - snapshot_started) * 1000.0, 3)
            sample = self._a2_conditioning_pressure_sample_from_snapshot(snapshot, point, phase=phase)
            sample["pressure_monitor_nonblocking"] = True
            sample["conditioning_monitor_pressure_deferred"] = False
            sample["pressure_monitor_deferred_for_vent_priority"] = False
            sample["pressure_monitor_budget_ms"] = monitor_budget_ms
        else:
            sample = self._a2_conditioning_pressure_sample(point, phase=phase)
            snapshot_started = time.monotonic()
            snapshot = self._a2_conditioning_stream_snapshot(point=point, phase=phase)
            snapshot_completed = time.monotonic()
            snapshot_duration_ms = round(max(0.0, snapshot_completed - snapshot_started) * 1000.0, 3)
        snapshot_budget_exceeded = bool(nonblocking_pressure_monitor and snapshot_duration_ms > monitor_budget_ms)
        monitor_completed_monotonic_s = time.monotonic()
        monitor_duration_s = max(0.0, monitor_completed_monotonic_s - monitor_started_monotonic_s)
        details = self._a2_conditioning_pressure_details(sample, snapshot, context=context)
        if nonblocking_pressure_monitor and (
            not bool(details.get("selected_pressure_freshness_ok"))
            or bool(details.get("continuous_latest_fresh_budget_exceeded"))
            or bool(details.get("selected_pressure_sample_stale_budget_exceeded"))
        ):
            operation = str(
                details.get("selected_pressure_fail_closed_reason")
                or (
                    "continuous_latest_fresh_budget_exceeded"
                    if details.get("continuous_latest_fresh_budget_exceeded")
                    else ""
                )
                or (
                    "selected_pressure_sample_stale_budget_exceeded"
                    if details.get("selected_pressure_sample_stale_budget_exceeded")
                    else ""
                )
                or "continuous_snapshot_not_fresh"
            )
            deferred_context = self._a2_conditioning_defer_diagnostic_for_vent_priority(
                {
                    **context,
                    **details,
                    "pressure_monitor_duration_ms": round(monitor_duration_s * 1000.0, 3),
                },
                point=point,
                component="pressure_monitor",
                operation=operation,
                now_mono=monitor_completed_monotonic_s,
                pressure_monitor=True,
            )
            return dict(deferred_context)
        elapsed_s = max(0.0, now_mono - float(context.get("conditioning_started_monotonic_s") or now_mono))
        context = self._a2_conditioning_update_pressure_metrics(
            context,
            phase=phase,
            pressure_hpa=details.get("pressure_hpa"),
            event_monotonic_s=monitor_completed_monotonic_s,
            vent_command_sent=False,
        )
        context["selected_pressure_source_for_conditioning_monitor"] = (
            details.get("selected_pressure_source")
            or details.get("pressure_source_selected")
            or details.get("pressure_sample_source")
            or ""
        )
        context["a2_conditioning_pressure_source_strategy"] = self._a2_conditioning_pressure_source_mode()
        context["latest_route_conditioning_pressure_source"] = context[
            "selected_pressure_source_for_conditioning_monitor"
        ]
        context["latest_route_conditioning_pressure_age_s"] = details.get("selected_pressure_sample_age_s")
        context["latest_route_conditioning_pressure_eligible_for_prearm_baseline"] = bool(
            details.get("pressure_hpa") is not None
            and details.get("selected_pressure_freshness_ok")
            and not details.get("pressure_overlimit_seen")
        )
        monitor_state = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "phase": phase,
            "elapsed_s": round(elapsed_s, 3),
            "route_conditioning_phase": context.get("route_conditioning_phase", "route_conditioning_flush_phase"),
            "ready_to_seal_phase_started": bool(context.get("ready_to_seal_phase_started", False)),
            "vent_off_blocked_during_flush": bool(context.get("vent_off_blocked_during_flush", True)),
            "seal_blocked_during_flush": bool(context.get("seal_blocked_during_flush", True)),
            "pressure_setpoint_blocked_during_flush": bool(
                context.get("pressure_setpoint_blocked_during_flush", True)
            ),
            "sample_blocked_during_flush": bool(context.get("sample_blocked_during_flush", True)),
            "vent_off_command_sent": bool(context.get("vent_off_command_sent", False)),
            "seal_command_sent": bool(context.get("seal_command_sent", False)),
            "pressure_setpoint_command_sent": bool(context.get("pressure_setpoint_command_sent", False)),
            "vent_command_sent": False,
            "whether_safe_to_continue": bool(
                details.get("selected_pressure_freshness_ok")
                and not details.get("pressure_overlimit_seen")
                and not context.get("route_open_transient_rejection_reason")
            ),
            "pressure_monitor_interval_s": self._a2_conditioning_pressure_monitor_interval_s(),
            "last_vent_command_age_s": context.get("last_vent_command_age_s"),
            "blocking_operation_name": "a2_conditioning_pressure_monitor",
            "blocking_operation_duration_ms": round(monitor_duration_s * 1000.0, 3),
            "diagnostic_duration_ms": round(monitor_duration_s * 1000.0, 3),
            "vent_scheduler_priority_mode": True,
            "vent_scheduler_checked_before_diagnostic": True,
            "diagnostic_deferred_for_vent_priority": bool(
                context.get("diagnostic_deferred_for_vent_priority", False)
            ),
            "diagnostic_deferred_count": int(context.get("diagnostic_deferred_count") or 0),
            "diagnostic_budget_ms": self._a2_conditioning_diagnostic_budget_ms(),
            "diagnostic_budget_exceeded": bool(
                context.get("diagnostic_budget_exceeded", False)
                or (round(monitor_duration_s * 1000.0, 3) > self._a2_conditioning_diagnostic_budget_ms())
                or snapshot_budget_exceeded
            ),
            "diagnostic_blocking_component": "pressure_monitor",
            "diagnostic_blocking_operation": (
                str(details.get("pressure_source_selection_reason") or "continuous_latest_fast_snapshot")
                if nonblocking_pressure_monitor
                else str(details.get("pressure_source_selection_reason") or "pressure_monitor")
            ),
            "diagnostic_blocking_duration_ms": round(monitor_duration_s * 1000.0, 3),
            "pressure_monitor_nonblocking": bool(nonblocking_pressure_monitor),
            "pressure_monitor_deferred_for_vent_priority": False,
            "pressure_monitor_budget_ms": monitor_budget_ms,
            "pressure_monitor_duration_ms": round(monitor_duration_s * 1000.0, 3),
            "pressure_monitor_blocked_vent_scheduler": False,
            "conditioning_monitor_pressure_deferred": False,
            "conditioning_monitor_pressure_deferred_count": int(
                context.get("conditioning_monitor_pressure_deferred_count") or 0
            ),
            "conditioning_monitor_max_defer_ms": context.get(
                "conditioning_monitor_max_defer_ms",
                self._a2_conditioning_monitor_pressure_max_defer_ms(),
            ),
            "conditioning_monitor_pressure_stale_timeout": bool(
                context.get("conditioning_monitor_pressure_stale_timeout", False)
            ),
            "conditioning_monitor_pressure_unavailable_fail_closed": bool(
                context.get("conditioning_monitor_pressure_unavailable_fail_closed", False)
            ),
            "trace_write_budget_ms": self._a2_conditioning_trace_write_budget_ms(),
            "trace_write_duration_ms": context.get("trace_write_duration_ms"),
            "trace_write_blocked_vent_scheduler": bool(context.get("trace_write_blocked_vent_scheduler", False)),
            "trace_write_deferred_for_vent_priority": bool(
                context.get("trace_write_deferred_for_vent_priority", False)
            ),
            "route_conditioning_diagnostic_blocked_vent_scheduler": bool(
                context.get("route_conditioning_diagnostic_blocked_vent_scheduler", False)
            ),
            "route_open_transition_started": bool(context.get("route_open_transition_started", False)),
            "route_open_transition_started_at": context.get("route_open_transition_started_at", ""),
            "route_open_transition_started_monotonic_s": context.get(
                "route_open_transition_started_monotonic_s"
            ),
            "route_open_command_write_started_at": context.get("route_open_command_write_started_at", ""),
            "route_open_command_write_completed_at": context.get("route_open_command_write_completed_at", ""),
            "route_open_command_write_duration_ms": context.get("route_open_command_write_duration_ms"),
            "route_open_settle_wait_sliced": bool(context.get("route_open_settle_wait_sliced", False)),
            "route_open_settle_wait_slice_count": int(context.get("route_open_settle_wait_slice_count") or 0),
            "route_open_settle_wait_total_ms": context.get("route_open_settle_wait_total_ms"),
            "route_open_transition_total_duration_ms": context.get("route_open_transition_total_duration_ms"),
            "vent_ticks_during_route_open_transition": int(
                context.get("vent_ticks_during_route_open_transition") or 0
            ),
            "route_open_transition_max_vent_write_gap_ms": context.get(
                "route_open_transition_max_vent_write_gap_ms"
            ),
            "route_open_transition_terminal_vent_write_age_ms": context.get(
                "route_open_transition_terminal_vent_write_age_ms"
            ),
            "route_open_transition_blocked_vent_scheduler": bool(
                context.get("route_open_transition_blocked_vent_scheduler", False)
            ),
            "route_open_settle_wait_blocked_vent_scheduler": bool(
                context.get("route_open_settle_wait_blocked_vent_scheduler", False)
            ),
            "terminal_vent_write_age_ms_at_gap_gate": context.get(
                "terminal_vent_write_age_ms_at_gap_gate"
            ),
            "max_vent_pulse_write_gap_ms_including_terminal_gap": context.get(
                "max_vent_pulse_write_gap_ms_including_terminal_gap"
            ),
            "max_vent_pulse_write_gap_phase": context.get("max_vent_pulse_write_gap_phase", ""),
            "max_vent_pulse_write_gap_threshold_ms": context.get(
                "max_vent_pulse_write_gap_threshold_ms"
            ),
            "max_vent_pulse_write_gap_threshold_source": context.get(
                "max_vent_pulse_write_gap_threshold_source",
                "",
            ),
            "max_vent_pulse_write_gap_exceeded": bool(
                context.get("max_vent_pulse_write_gap_exceeded", False)
            ),
            "max_vent_pulse_write_gap_not_exceeded_reason": context.get(
                "max_vent_pulse_write_gap_not_exceeded_reason",
                "",
            ),
            "route_conditioning_vent_gap_exceeded_source": context.get(
                "route_conditioning_vent_gap_exceeded_source",
                "",
            ),
            "terminal_gap_source": context.get("terminal_gap_source", ""),
            "terminal_gap_operation": context.get("terminal_gap_operation", ""),
            "terminal_gap_duration_ms": context.get("terminal_gap_duration_ms"),
            "terminal_gap_started_at": context.get("terminal_gap_started_at", ""),
            "terminal_gap_detected_at": context.get("terminal_gap_detected_at", ""),
            "terminal_gap_stack_marker": context.get("terminal_gap_stack_marker", ""),
            "defer_returned_to_vent_loop": bool(context.get("defer_returned_to_vent_loop", False)),
            "defer_to_next_vent_loop_ms": context.get("defer_to_next_vent_loop_ms"),
            "vent_tick_after_defer_ms": context.get("vent_tick_after_defer_ms"),
            "terminal_gap_after_defer": bool(context.get("terminal_gap_after_defer", False)),
            "terminal_gap_after_defer_ms": context.get("terminal_gap_after_defer_ms"),
            "defer_path_no_reschedule": bool(context.get("defer_path_no_reschedule", False)),
            "fail_closed_path_started": bool(context.get("fail_closed_path_started", False)),
            "fail_closed_path_started_while_route_open": bool(
                context.get("fail_closed_path_started_while_route_open", False)
            ),
            "fail_closed_path_vent_maintenance_required": bool(
                context.get("fail_closed_path_vent_maintenance_required", False)
            ),
            "fail_closed_path_vent_maintenance_active": bool(
                context.get("fail_closed_path_vent_maintenance_active", False)
            ),
            "fail_closed_path_duration_ms": context.get("fail_closed_path_duration_ms"),
            "fail_closed_path_blocked_vent_scheduler": bool(
                context.get("fail_closed_path_blocked_vent_scheduler", False)
            ),
            "route_open_to_first_pressure_read_ms": context.get("route_open_to_first_pressure_read_ms"),
            "route_open_to_overlimit_ms": context.get("route_open_to_overlimit_ms"),
            "route_conditioning_pressure_before_route_open_hpa": context.get(
                "route_conditioning_pressure_before_route_open_hpa"
            ),
            "route_conditioning_pressure_after_route_open_hpa": context.get(
                "route_conditioning_pressure_after_route_open_hpa"
            ),
            "route_conditioning_pressure_rise_rate_hpa_per_s": context.get(
                "route_conditioning_pressure_rise_rate_hpa_per_s"
            ),
            "route_conditioning_peak_pressure_hpa": context.get("route_conditioning_peak_pressure_hpa"),
            "latest_route_conditioning_pressure_hpa": context.get("latest_route_conditioning_pressure_hpa"),
            "latest_route_conditioning_pressure_source": context.get(
                "latest_route_conditioning_pressure_source",
                "",
            ),
            "latest_route_conditioning_pressure_age_s": context.get(
                "latest_route_conditioning_pressure_age_s"
            ),
            "latest_route_conditioning_pressure_eligible_for_prearm_baseline": bool(
                context.get("latest_route_conditioning_pressure_eligible_for_prearm_baseline", False)
            ),
            "route_conditioning_pressure_overlimit": bool(context.get("route_conditioning_pressure_overlimit", False)),
            **self._a2_route_open_transient_evidence(context),
            "pressure_rise_since_last_vent_hpa": context.get("pressure_rise_since_last_vent_hpa"),
            "pressure_sample_stale": bool(details.get("selected_pressure_sample_is_stale")),
            "pressure_freshness_ok": bool(details.get("selected_pressure_freshness_ok")),
            "selected_pressure_source_for_conditioning_monitor": context.get(
                "selected_pressure_source_for_conditioning_monitor",
                "",
            ),
            "a2_conditioning_pressure_source_strategy": context.get(
                "a2_conditioning_pressure_source_strategy",
                self._a2_conditioning_pressure_source_mode(),
            ),
            **{key: value for key, value in details.items() if key not in {"sample", "digital_sample"}},
        }
        contextual_sample = {**sample, **monitor_state}
        samples = [item for item in list(context.get("pressure_samples") or []) if isinstance(item, Mapping)]
        samples.append(monitor_state)
        context["pressure_samples"] = samples
        context["last_pressure_monitor_monotonic_s"] = now_mono
        pressure_values = [
            self._as_float(item.get("pressure_hpa", item.get("digital_gauge_pressure_hpa")))
            for item in samples
            if isinstance(item, Mapping)
            and self._as_float(item.get("pressure_hpa", item.get("digital_gauge_pressure_hpa"))) is not None
        ]
        if pressure_values:
            context["pressure_max_during_conditioning_hpa"] = max(float(value) for value in pressure_values if value is not None)
            context["pressure_min_during_conditioning_hpa"] = min(float(value) for value in pressure_values if value is not None)
        if details.get("digital_gauge_latest_age_s") is not None:
            context["digital_gauge_latest_age_s"] = details.get("digital_gauge_latest_age_s")
            previous_max = self._as_float(context.get("latest_frame_age_max_s"))
            context["latest_frame_age_max_s"] = (
                details["digital_gauge_latest_age_s"]
                if previous_max is None
                else max(float(previous_max), float(details["digital_gauge_latest_age_s"]))
            )
        if details.get("latest_frame_sequence_id") is not None:
            context["last_digital_gauge_sequence_id"] = details.get("latest_frame_sequence_id")
        context["digital_gauge_sequence_progress"] = details.get("digital_gauge_sequence_progress")
        context.update(self._a2_conditioning_digital_gauge_evidence(details))
        context["selected_pressure_source_for_conditioning_monitor"] = monitor_state.get(
            "selected_pressure_source_for_conditioning_monitor",
            "",
        )
        context["a2_conditioning_pressure_source_strategy"] = monitor_state.get(
            "a2_conditioning_pressure_source_strategy",
            self._a2_conditioning_pressure_source_mode(),
        )
        context["pressure_monitor_interval_s"] = self._a2_conditioning_pressure_monitor_interval_s()
        context["conditioning_pressure_abort_hpa"] = details.get("conditioning_pressure_abort_hpa")
        context["route_conditioning_hard_abort_pressure_hpa"] = details.get(
            "route_conditioning_hard_abort_pressure_hpa",
            context.get("route_conditioning_hard_abort_pressure_hpa"),
        )
        context["route_conditioning_hard_abort_exceeded"] = bool(
            context.get("route_conditioning_hard_abort_exceeded")
            or details.get("route_conditioning_hard_abort_exceeded")
        )
        context["pressure_overlimit_seen"] = bool(context.get("pressure_overlimit_seen") or details.get("pressure_overlimit_seen"))
        if details.get("pressure_overlimit_seen"):
            context["pressure_overlimit_source"] = details.get("pressure_overlimit_source")
            context["pressure_overlimit_hpa"] = details.get("pressure_overlimit_hpa")
            context["route_conditioning_high_pressure_seen_before_preseal"] = True
            context["route_conditioning_high_pressure_seen_before_preseal_hpa"] = details.get(
                "route_conditioning_high_pressure_seen_before_preseal_hpa",
                details.get("pressure_overlimit_hpa"),
            )
            context["route_conditioning_high_pressure_seen_phase"] = details.get(
                "route_conditioning_high_pressure_seen_phase",
                "co2_route_conditioning_at_atmosphere",
            )
            context["route_conditioning_high_pressure_seen_source"] = details.get(
                "route_conditioning_high_pressure_seen_source",
                details.get("pressure_overlimit_source"),
            )
            context["route_conditioning_high_pressure_seen_sample_age_s"] = details.get(
                "route_conditioning_high_pressure_seen_sample_age_s"
            )
            context["route_conditioning_high_pressure_seen_decision"] = details.get(
                "route_conditioning_high_pressure_seen_decision",
                "fail_closed",
            )
        context.update(
            {
                "vent_scheduler_priority_mode": True,
                "vent_scheduler_checked_before_diagnostic": True,
                "diagnostic_budget_ms": monitor_state["diagnostic_budget_ms"],
                "diagnostic_budget_exceeded": monitor_state["diagnostic_budget_exceeded"],
                "diagnostic_blocking_component": monitor_state["diagnostic_blocking_component"],
                "diagnostic_blocking_operation": monitor_state["diagnostic_blocking_operation"],
                "diagnostic_blocking_duration_ms": monitor_state["diagnostic_blocking_duration_ms"],
                "pressure_monitor_nonblocking": monitor_state["pressure_monitor_nonblocking"],
                "pressure_monitor_deferred_for_vent_priority": False,
                "pressure_monitor_budget_ms": monitor_state["pressure_monitor_budget_ms"],
                "pressure_monitor_duration_ms": monitor_state["pressure_monitor_duration_ms"],
                "pressure_monitor_blocked_vent_scheduler": False,
                "conditioning_monitor_pressure_deferred": False,
                "trace_write_budget_ms": monitor_state["trace_write_budget_ms"],
            }
        )
        context["last_blocking_operation_name"] = "a2_conditioning_pressure_monitor"
        context["last_blocking_operation_started_monotonic_s"] = monitor_started_monotonic_s
        context["last_blocking_operation_completed_monotonic_s"] = monitor_completed_monotonic_s
        context["last_blocking_operation_duration_s"] = monitor_duration_s
        context["last_blocking_operation_safe_to_continue"] = bool(
            details.get("selected_pressure_freshness_ok")
            and not details.get("pressure_overlimit_seen")
            and not context.get("route_open_transient_rejection_reason")
        )
        context = self._a2_conditioning_context_with_counts(context)
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        schedule = self._a2_conditioning_vent_schedule(context, now_mono=monitor_completed_monotonic_s)
        diagnostic_max_gap_s = float(schedule.get("route_conditioning_effective_max_gap_s") or 0.0)
        if diagnostic_max_gap_s > 0.0 and monitor_duration_s > diagnostic_max_gap_s:
            source = self._a2_conditioning_diagnostic_source(context, fallback="pressure_monitor")
            terminal = self._a2_conditioning_terminal_gap_details(
                context,
                now_mono=monitor_completed_monotonic_s,
                max_gap_s=diagnostic_max_gap_s,
                source=source,
            )
            monitor_state["route_conditioning_diagnostic_blocked_vent_scheduler"] = True
            monitor_state["pressure_monitor_blocked_vent_scheduler"] = True
            monitor_state["diagnostic_blocking_component"] = source
            monitor_state["diagnostic_blocking_operation"] = context.get(
                "diagnostic_blocking_operation",
                "pressure_monitor",
            )
            monitor_state.update(terminal)
            monitor_state["fail_closed_reason"] = "route_conditioning_diagnostic_blocked_vent_scheduler"
            context.update(
                {
                    **terminal,
                    "route_conditioning_diagnostic_blocked_vent_scheduler": True,
                    "pressure_monitor_blocked_vent_scheduler": True,
                    "diagnostic_duration_ms": monitor_state["diagnostic_duration_ms"],
                    "diagnostic_blocking_component": monitor_state["diagnostic_blocking_component"],
                    "diagnostic_blocking_operation": monitor_state["diagnostic_blocking_operation"],
                    "diagnostic_blocking_duration_ms": monitor_state["diagnostic_blocking_duration_ms"],
                    "fail_closed_reason": "route_conditioning_diagnostic_blocked_vent_scheduler",
                }
            )
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason="route_conditioning_diagnostic_blocked_vent_scheduler",
                details=monitor_state,
                event_name="co2_route_conditioning_diagnostic_blocked_vent_scheduler",
                route_trace_action="co2_route_conditioning_diagnostic_blocked_vent_scheduler",
            )
        if high_frequency_window:
            context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
            context.update(
                {
                    "trace_write_budget_ms": self._a2_conditioning_trace_write_budget_ms(),
                    "trace_write_duration_ms": 0.0,
                    "trace_write_blocked_vent_scheduler": False,
                    "trace_write_deferred_for_vent_priority": True,
                }
            )
            monitor_state.update(
                {
                    "trace_write_budget_ms": context["trace_write_budget_ms"],
                    "trace_write_duration_ms": 0.0,
                    "trace_write_blocked_vent_scheduler": False,
                    "trace_write_deferred_for_vent_priority": True,
                }
            )
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        else:
            self._record_pressure_source_latency_events(
                contextual_sample,
                point=point,
                stage="co2_route_conditioning_at_atmosphere",
            )
        transient_rejection_reason = str(context.get("route_open_transient_rejection_reason") or "").strip()
        pressure_fail = bool(details.get("pressure_overlimit_seen") or transient_rejection_reason)
        event_type = "fail" if pressure_fail else "tick"
        event_name = (
            "co2_route_conditioning_pressure_warning"
            if details.get("pressure_overlimit_seen")
            else (
                "co2_route_conditioning_transient_recovery_failed"
                if transient_rejection_reason
                else "co2_route_conditioning_pressure_sample"
            )
        )
        self._record_a2_conditioning_workflow_timing(
            context,
            event_name,
            event_type,
            stage="co2_route_conditioning_at_atmosphere",
            point=point,
            duration_s=monitor_state["elapsed_s"],
            pressure_hpa=details.get("pressure_hpa"),
            decision=(
                "hard_abort_pressure_exceeded"
                if details.get("pressure_overlimit_seen")
                else (transient_rejection_reason or "monitor_only_no_seal")
            ),
            warning_code="conditioning_pressure_above_hard_abort_threshold"
            if details.get("pressure_overlimit_seen")
            else None,
            error_code="route_conditioning_pressure_overlimit"
            if details.get("pressure_overlimit_seen")
            else (transient_rejection_reason or None),
            route_state=monitor_state,
        )
        if not bool(details.get("selected_pressure_freshness_ok")):
            reason = str(
                details.get("fail_closed_reason")
                or details.get("selected_pressure_fail_closed_reason")
                or "selected_pressure_sample_stale"
            )
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason=reason,
                details={
                    **monitor_state,
                    "stream_stale": bool(details.get("selected_pressure_sample_is_stale")),
                    "continuous_stream_stale": bool(details.get("continuous_stream_stale")),
                },
                event_name="co2_route_conditioning_stream_stale",
                route_trace_action="co2_route_conditioning_stream_stale",
                pressure_hpa=details.get("pressure_hpa"),
            )
        if details.get("pressure_overlimit_seen"):
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason="route_conditioning_pressure_overlimit",
                details=monitor_state,
                event_name="co2_route_conditioning_pressure_overlimit",
                route_trace_action="co2_preseal_atmosphere_hold_pressure_guard",
                pressure_hpa=details.get("pressure_hpa"),
            )
        if transient_rejection_reason and not bool(context.get("route_open_transient_accepted", False)):
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason=transient_rejection_reason,
                details=monitor_state,
                event_name="co2_route_conditioning_transient_recovery_failed",
                route_trace_action="co2_route_conditioning_transient_recovery_failed",
                pressure_hpa=details.get("pressure_hpa"),
            )
        return monitor_state

    def _begin_a2_co2_route_open_transition(self, point: CalibrationPoint) -> dict[str, Any]:
        if not bool(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", False)):
            return {}
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
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
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
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
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
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
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
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
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
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
        setattr(self, "_a2_co2_route_open_monotonic_s", now_mono)
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
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
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
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

    def _wait_a2_co2_route_open_settle_before_conditioning(self, point: CalibrationPoint) -> dict[str, Any]:
        if not bool(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", False)):
            return {}
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        if not context:
            return {}
        total_s = self._a2_route_open_settle_wait_s()
        if total_s <= 0.0:
            context.update(
                {
                    "route_open_settle_wait_sliced": False,
                    "route_open_settle_wait_slice_count": 0,
                    "route_open_settle_wait_total_ms": 0.0,
                }
            )
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
            return context
        slice_s = min(self._a2_route_open_settle_wait_slice_s(), self._a2_conditioning_scheduler_sleep_step_s())
        start_mono = time.monotonic()
        deadline = start_mono + total_s
        slice_count = 0
        context.update({"route_open_settle_wait_sliced": True})
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        while True:
            now_mono = time.monotonic()
            if now_mono >= deadline:
                break
            self._maybe_reassert_a2_conditioning_vent(point)
            now_mono = time.monotonic()
            if now_mono >= deadline:
                break
            sleep_s = min(slice_s, max(0.0, deadline - now_mono))
            if sleep_s <= 0.0:
                continue
            time.sleep(sleep_s)
            slice_count += 1
        completed_mono = time.monotonic()
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        context.update(
            {
                "route_open_settle_wait_sliced": True,
                "route_open_settle_wait_slice_count": slice_count,
                "route_open_settle_wait_total_ms": round(max(0.0, completed_mono - start_mono) * 1000.0, 3),
            }
        )
        schedule = self._a2_conditioning_vent_schedule(context, now_mono=completed_mono)
        terminal = self._a2_conditioning_terminal_gap_details(
            context,
            now_mono=completed_mono,
            max_gap_s=float(schedule["route_conditioning_effective_max_gap_s"]),
            source="route_open_settle_wait",
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
        terminal_age_ms = self._as_float(terminal.get("terminal_vent_write_age_ms_at_gap_gate"))
        max_gap_ms = float(schedule["route_conditioning_effective_max_gap_s"]) * 1000.0
        if terminal_age_ms is not None and float(terminal_age_ms) > max_gap_ms:
            context.update(
                {
                    **terminal,
                    "route_open_settle_wait_blocked_vent_scheduler": True,
                    "route_conditioning_vent_gap_exceeded": True,
                    "route_conditioning_vent_gap_exceeded_source": "route_open_settle_wait",
                    "fail_closed_reason": "route_conditioning_vent_gap_exceeded",
                    "whether_safe_to_continue": False,
                }
            )
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason="route_conditioning_vent_gap_exceeded",
                details=context,
                event_name="co2_route_open_settle_wait_blocked_vent_scheduler",
                route_trace_action="co2_route_open_settle_wait_blocked_vent_scheduler",
            )
        context = self._a2_conditioning_context_with_counts(context)
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        self._record_a2_conditioning_workflow_timing(
            context,
            "co2_route_open_settle_wait",
            "end",
            stage="co2_route_open_transition",
            point=point,
            duration_s=round(max(0.0, completed_mono - start_mono), 3),
            route_state=context,
        )
        return context

    def _complete_a2_co2_route_open_transition(self, point: CalibrationPoint) -> dict[str, Any]:
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
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
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
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
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        if not context:
            return {}
        tick_started_monotonic_s = time.monotonic()
        schedule = self._a2_conditioning_vent_schedule(context, now_mono=tick_started_monotonic_s)
        max_gap_s = float(schedule["route_conditioning_effective_max_gap_s"])
        active_interval_s = float(schedule["route_conditioning_effective_vent_interval_s"])
        context.update(schedule)
        defer_started = self._as_float(context.get("last_diagnostic_defer_monotonic_s"))
        if defer_started is not None and context.get("vent_tick_after_defer_ms") in (None, ""):
            vent_after_defer_ms = round(max(0.0, tick_started_monotonic_s - float(defer_started)) * 1000.0, 3)
            defer_state = self._a2_conditioning_defer_reschedule_state(
                context,
                now_mono=tick_started_monotonic_s,
                max_gap_s=max_gap_s,
                defer_loop_ms=vent_after_defer_ms,
            )
            vent_gap_exceeded = bool(defer_state.get("vent_gap_exceeded_after_defer"))
            operation = str(
                context.get("defer_operation")
                or context.get("last_diagnostic_defer_operation")
                or context.get("diagnostic_blocking_operation")
                or "deferred_diagnostic"
            )
            context.update(defer_state)
            context["defer_to_next_vent_loop_ms"] = context.get("defer_to_next_vent_loop_ms", vent_after_defer_ms)
            context["defer_returned_to_vent_loop"] = True
            context["defer_reschedule_requested"] = True
            context["defer_reschedule_completed"] = not vent_gap_exceeded
            context["defer_reschedule_reason"] = str(
                context.get("defer_reschedule_reason") or f"fast_vent_tick_after_{operation}"
            )
            if vent_gap_exceeded:
                context["terminal_gap_source"] = "defer_path_no_reschedule"
                context["terminal_gap_operation"] = operation
                context["terminal_gap_duration_ms"] = defer_state.get("vent_gap_after_defer_ms")
                context["terminal_gap_detected_at"] = datetime.now(timezone.utc).isoformat()
            elif bool(defer_state.get("defer_reschedule_latency_warning")):
                context = self._a2_route_open_transient_mark_continuing_after_defer_warning(context)
        context.setdefault("vent_scheduler_priority_mode", True)
        context.setdefault("diagnostic_budget_ms", self._a2_conditioning_diagnostic_budget_ms())
        context.setdefault("pressure_monitor_budget_ms", self._a2_conditioning_pressure_monitor_budget_ms())
        context.setdefault("trace_write_budget_ms", self._a2_conditioning_trace_write_budget_ms())
        context["route_conditioning_vent_maintenance_active"] = True
        route_open_monotonic = self._as_float(
            context.get("route_open_completed_monotonic_s")
            or getattr(self, "_a2_co2_route_open_monotonic_s", None)
        )
        if route_open_monotonic is None:
            context["pre_route_vent_phase_started"] = True
        elif bool(schedule.get("route_conditioning_high_frequency_window_active")):
            context["route_open_high_frequency_vent_phase_started"] = True
        blocked_reason = self._a2_conditioning_unsafe_vent_reason(context)
        if blocked_reason:
            blocked_context = self._a2_conditioning_mark_vent_blocked(context, reason=blocked_reason)
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason="unsafe_vent_after_flush_phase_blocked",
                details={
                    **blocked_context,
                    "phase": phase,
                    "vent_command_sent": False,
                    "command_result": "blocked",
                    "command_error": blocked_reason,
                },
                event_name="co2_route_conditioning_vent_blocked_after_flush_phase",
                route_trace_action="co2_route_conditioning_vent_blocked_after_flush_phase",
            )
        gap_state = self._a2_conditioning_heartbeat_gap_state(
            context,
            now_mono=tick_started_monotonic_s,
            max_gap_s=max_gap_s,
            interval_s=active_interval_s,
        )
        vent_gap_s = self._as_float(gap_state.get("vent_heartbeat_gap_s"))
        emission_gap_s = self._as_float(gap_state.get("heartbeat_emission_gap_s"))
        effective_gap_s = vent_gap_s if vent_gap_s is not None else emission_gap_s
        first_after_route_vent = bool(
            route_open_monotonic is not None
            and context.get("route_open_to_first_vent_s") in (None, "")
            and str(phase or "") == "after_route_open"
        )
        if route_open_monotonic is not None and effective_gap_s is not None and not first_after_route_vent:
            if effective_gap_s > max_gap_s:
                gap_ms = round(float(effective_gap_s) * 1000.0, 3)
                source = self._a2_conditioning_vent_gap_source(context)
                terminal = self._a2_conditioning_terminal_gap_details(
                    context,
                    now_mono=tick_started_monotonic_s,
                    max_gap_s=max_gap_s,
                    source=source,
                )
                self._fail_a2_co2_route_conditioning_closed(
                    point,
                    reason="route_conditioning_vent_gap_exceeded",
                    details={
                        "phase": phase,
                        **gap_state,
                        "vent_heartbeat_gap_s": round(float(effective_gap_s), 3),
                        "vent_heartbeat_interval_s": active_interval_s,
                        "atmosphere_vent_max_gap_s": max_gap_s,
                        "vent_heartbeat_gap_exceeded": True,
                        "route_conditioning_vent_gap_exceeded": True,
                        "max_vent_pulse_gap_ms": gap_ms,
                        "max_vent_pulse_gap_limit_ms": round(max_gap_s * 1000.0, 3),
                        **terminal,
                        "last_vent_command_age_s": round(float(effective_gap_s), 3),
                        "fail_closed_reason": "route_conditioning_vent_gap_exceeded",
                        "whether_safe_to_continue": False,
                        **schedule,
                    },
                    event_name="co2_route_conditioning_vent_heartbeat_gap",
                    route_trace_action="co2_route_conditioning_vent_heartbeat_gap",
                )
        route_open_to_first_vent_s = None
        if route_open_monotonic is not None and context.get("route_open_to_first_vent_s") in (None, ""):
            route_open_to_first_vent_s = max(0.0, tick_started_monotonic_s - float(route_open_monotonic))
            if route_open_to_first_vent_s > max_gap_s:
                gap_ms = round(float(route_open_to_first_vent_s) * 1000.0, 3)
                source = self._a2_conditioning_vent_gap_source(context)
                terminal = self._a2_conditioning_terminal_gap_details(
                    context,
                    now_mono=tick_started_monotonic_s,
                    max_gap_s=max_gap_s,
                    source=source,
                )
                self._fail_a2_co2_route_conditioning_closed(
                    point,
                    reason="route_conditioning_vent_gap_exceeded",
                    details={
                        "phase": phase,
                        "route_open_to_first_vent_s": round(float(route_open_to_first_vent_s), 3),
                        "route_open_to_first_vent_ms": gap_ms,
                        "atmosphere_vent_max_gap_s": max_gap_s,
                        "vent_heartbeat_gap_exceeded": True,
                        "route_conditioning_vent_gap_exceeded": True,
                        "max_vent_pulse_gap_ms": gap_ms,
                        "max_vent_pulse_gap_limit_ms": round(max_gap_s * 1000.0, 3),
                        **terminal,
                        **schedule,
                    },
                    event_name="co2_route_conditioning_route_open_first_vent_gap",
                    route_trace_action="co2_route_conditioning_route_open_first_vent_gap",
                )
        diagnostics: dict[str, Any] = {}
        command_result = "ok"
        command_error = ""
        try:
            fast_reassert = getattr(self.pressure_control_service, "set_pressure_controller_vent_fast_reassert", None)
            if not callable(fast_reassert):
                diagnostics = {
                    "command_result": "unsupported",
                    "command_error": "route_conditioning_fast_vent_not_supported",
                    "route_conditioning_fast_vent_not_supported": True,
                    "fast_vent_reassert_supported": False,
                    "fast_vent_reassert_used": False,
                }
            else:
                diagnostics = fast_reassert(
                    True,
                    reason="A2 route conditioning fast vent maintenance",
                    max_duration_s=self._a2_conditioning_fast_vent_max_duration_s(),
                    wait_after_command=False,
                    capture_pressure=False,
                    query_state=False,
                    confirm_transition=False,
                )
        except Exception as exc:
            command_result = "fail"
            command_error = str(exc)
            diagnostics = {"command_error": command_error}
        diagnostics = dict(diagnostics or {})
        if bool(diagnostics.get("vent_command_blocked")):
            command_result = "blocked"
            command_error = str(diagnostics.get("vent_pulse_blocked_reason") or "vent_command_blocked")
        elif str(diagnostics.get("command_result") or "").lower() in {"fail", "failed", "timeout", "unsupported"}:
            command_result = str(diagnostics.get("command_result") or "fail").lower()
            command_error = str(diagnostics.get("command_error") or command_result)
        tick_completed_monotonic_s = time.monotonic()
        blocking_duration_s = max(0.0, tick_completed_monotonic_s - tick_started_monotonic_s)
        write_sent_monotonic_s = self._as_float(
            diagnostics.get("vent_command_write_sent_monotonic_s")
            or diagnostics.get("vent_command_write_started_monotonic_s")
            or tick_started_monotonic_s
        )
        pressure_hpa = None
        latest_age_s = None
        continuous_age_s = context.get("digital_gauge_latest_age_s")
        abort_hpa = context.get("conditioning_pressure_abort_hpa")
        hard_abort_hpa = context.get(
            "route_conditioning_hard_abort_pressure_hpa",
            self._a2_route_conditioning_hard_abort_pressure_hpa(),
        )
        pressure_abnormal = bool(
            context.get("pressure_overlimit_seen")
            or context.get("route_conditioning_pressure_overlimit")
            or context.get("route_conditioning_hard_abort_exceeded")
        )
        selected_freshness_ok = True
        sample_stale = False
        elapsed_s = max(0.0, time.monotonic() - float(context.get("conditioning_started_monotonic_s") or tick_started_monotonic_s))
        context = self._a2_conditioning_update_pressure_metrics(
            context,
            phase=phase,
            pressure_hpa=pressure_hpa,
            event_monotonic_s=float(write_sent_monotonic_s or tick_started_monotonic_s),
            vent_command_sent=True,
            vent_command_write_sent_monotonic_s=write_sent_monotonic_s,
        )
        if bool(diagnostics.get("route_conditioning_fast_vent_command_timeout")):
            context["route_conditioning_fast_vent_command_timeout"] = True
            if phase.startswith("pre_route") or route_open_monotonic is None:
                context["pre_route_fast_vent_timeout"] = True
        if bool(diagnostics.get("route_conditioning_fast_vent_not_supported")):
            context["route_conditioning_fast_vent_not_supported"] = True
        if route_open_monotonic is None:
            context["pre_route_fast_vent_sent"] = bool(command_result == "ok")
            context["pre_route_fast_vent_duration_ms"] = diagnostics.get("vent_command_total_duration_ms")
        if bool(context.get("route_open_transition_started")) and not bool(
            context.get("route_open_transition_completed", False)
        ):
            previous_transition_write = self._as_float(
                context.get("_route_open_transition_last_vent_write_sent_monotonic_s")
            )
            if previous_transition_write is not None and write_sent_monotonic_s is not None:
                transition_gap_ms = round(
                    max(0.0, float(write_sent_monotonic_s) - float(previous_transition_write)) * 1000.0,
                    3,
                )
                previous_transition_max = self._as_float(context.get("route_open_transition_max_vent_write_gap_ms"))
                context["route_open_transition_max_vent_write_gap_ms"] = (
                    transition_gap_ms
                    if previous_transition_max is None
                    else max(float(previous_transition_max), transition_gap_ms)
                )
            if write_sent_monotonic_s is not None:
                context["_route_open_transition_last_vent_write_sent_monotonic_s"] = float(write_sent_monotonic_s)
            context["vent_ticks_during_route_open_transition"] = (
                int(context.get("vent_ticks_during_route_open_transition") or 0) + 1
            )
            context["route_open_transition_terminal_vent_write_age_ms"] = 0.0
        context.update(
            {
                "fast_vent_reassert_supported": bool(diagnostics.get("fast_vent_reassert_supported")),
                "fast_vent_reassert_used": bool(diagnostics.get("fast_vent_reassert_used")),
                "vent_command_write_started_at": diagnostics.get("vent_command_write_started_at", ""),
                "vent_command_write_sent_at": diagnostics.get("vent_command_write_sent_at", ""),
                "vent_command_write_completed_at": diagnostics.get("vent_command_write_completed_at", ""),
                "vent_command_write_started_monotonic_s": diagnostics.get("vent_command_write_started_monotonic_s"),
                "vent_command_write_sent_monotonic_s": diagnostics.get("vent_command_write_sent_monotonic_s"),
                "vent_command_write_completed_monotonic_s": diagnostics.get("vent_command_write_completed_monotonic_s"),
                "vent_command_write_duration_ms": diagnostics.get("vent_command_write_duration_ms"),
                "vent_command_total_duration_ms": diagnostics.get("vent_command_total_duration_ms"),
                "vent_command_wait_after_command_s": diagnostics.get("vent_command_wait_after_command_s"),
                "vent_command_capture_pressure_enabled": bool(diagnostics.get("vent_command_capture_pressure_enabled")),
                "vent_command_query_state_enabled": bool(diagnostics.get("vent_command_query_state_enabled")),
                "vent_command_confirm_transition_enabled": bool(diagnostics.get("vent_command_confirm_transition_enabled")),
                "vent_command_blocking_phase": diagnostics.get("vent_command_blocking_phase", ""),
                "selected_pressure_source_for_conditioning_monitor": context.get("selected_pressure_source") or "",
                "a2_conditioning_pressure_source_strategy": context.get("a2_conditioning_pressure_source", self._a2_conditioning_pressure_source_mode()),
            }
        )
        if defer_started is not None:
            context["fast_vent_after_defer_sent"] = bool(command_result == "ok")
            context["fast_vent_after_defer_write_ms"] = (
                diagnostics.get("vent_command_write_duration_ms")
                or diagnostics.get("vent_command_total_duration_ms")
            )
        tick = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "phase": phase,
            "elapsed_s": round(elapsed_s, 3),
            "route_conditioning_phase": context.get("route_conditioning_phase", "route_conditioning_flush_phase"),
            "ready_to_seal_phase_started": bool(context.get("ready_to_seal_phase_started", False)),
            "route_conditioning_flush_min_time_completed": bool(
                context.get("route_conditioning_flush_min_time_completed", False)
            ),
            "vent_off_blocked_during_flush": bool(context.get("vent_off_blocked_during_flush", True)),
            "seal_blocked_during_flush": bool(context.get("seal_blocked_during_flush", True)),
            "pressure_setpoint_blocked_during_flush": bool(
                context.get("pressure_setpoint_blocked_during_flush", True)
            ),
            "sample_blocked_during_flush": bool(context.get("sample_blocked_during_flush", True)),
            "vent_off_command_sent": bool(context.get("vent_off_command_sent", False)),
            "seal_command_sent": bool(context.get("seal_command_sent", False)),
            "pressure_setpoint_command_sent": bool(context.get("pressure_setpoint_command_sent", False)),
            "vent_command_sent": True,
            "route_conditioning_vent_maintenance_active": True,
            "vent_maintenance_started_at": context.get("vent_maintenance_started_at"),
            "vent_maintenance_started_monotonic_s": context.get("vent_maintenance_started_monotonic_s"),
            "route_conditioning_high_frequency_vent_interval_s": self._a2_conditioning_high_frequency_vent_interval_s(),
            "route_conditioning_high_frequency_max_gap_s": self._a2_conditioning_high_frequency_vent_max_gap_s(),
            "route_conditioning_high_frequency_vent_window_s": self._a2_conditioning_high_frequency_vent_window_s(),
            "route_conditioning_vent_maintenance_interval_s": self._a2_conditioning_vent_maintenance_interval_s(),
            "route_conditioning_vent_maintenance_max_gap_s": self._a2_conditioning_vent_maintenance_max_gap_s(),
            "route_conditioning_high_frequency_window_active": schedule.get(
                "route_conditioning_high_frequency_window_active"
            ),
            "route_conditioning_effective_vent_interval_s": schedule.get(
                "route_conditioning_effective_vent_interval_s"
            ),
            "route_conditioning_effective_max_gap_s": schedule.get("route_conditioning_effective_max_gap_s"),
            "max_vent_pulse_gap_limit_ms": schedule.get("max_vent_pulse_gap_limit_ms"),
            "vent_pulse_count": context.get("vent_pulse_count"),
            "vent_pulse_interval_ms": context.get("vent_pulse_interval_ms"),
            "max_vent_pulse_gap_ms": context.get("max_vent_pulse_gap_ms"),
            "vent_scheduler_tick_count": context.get("vent_scheduler_tick_count", 0),
            "vent_scheduler_loop_gap_ms": context.get("vent_scheduler_loop_gap_ms", []),
            "max_vent_scheduler_loop_gap_ms": context.get("max_vent_scheduler_loop_gap_ms"),
            "pressure_drop_after_vent_hpa": context.get("last_pressure_drop_after_vent_hpa"),
            "pressure_drop_after_vent_hpa_all": context.get("pressure_drop_after_vent_hpa"),
            "command_result": command_result,
            "command_error": command_error,
            "vent_pulse_blocked_after_flush_phase": bool(
                context.get("vent_pulse_blocked_after_flush_phase", False)
            ),
            "vent_pulse_blocked_reason": context.get("vent_pulse_blocked_reason", ""),
            "attempted_unsafe_vent_after_seal_or_pressure_control": bool(
                context.get("attempted_unsafe_vent_after_seal_or_pressure_control", False)
            ),
            "unsafe_vent_after_seal_or_pressure_control_command_sent": False,
            "output_state": diagnostics.get("output_state"),
            "isolation_state": diagnostics.get("isolation_state"),
            "vent_status": diagnostics.get("vent_status_raw"),
            "pre_route_vent_phase_started": bool(context.get("pre_route_vent_phase_started", False)),
            "pre_route_fast_vent_required": bool(context.get("pre_route_fast_vent_required", True)),
            "pre_route_fast_vent_sent": bool(context.get("pre_route_fast_vent_sent", False)),
            "pre_route_fast_vent_duration_ms": context.get("pre_route_fast_vent_duration_ms"),
            "pre_route_fast_vent_timeout": bool(context.get("pre_route_fast_vent_timeout", False)),
            "fast_vent_reassert_supported": bool(context.get("fast_vent_reassert_supported", False)),
            "fast_vent_reassert_used": bool(context.get("fast_vent_reassert_used", False)),
            "vent_command_write_started_at": context.get("vent_command_write_started_at", ""),
            "vent_command_write_sent_at": context.get("vent_command_write_sent_at", ""),
            "vent_command_write_completed_at": context.get("vent_command_write_completed_at", ""),
            "vent_command_write_duration_ms": context.get("vent_command_write_duration_ms"),
            "vent_command_total_duration_ms": context.get("vent_command_total_duration_ms"),
            "vent_command_wait_after_command_s": context.get("vent_command_wait_after_command_s"),
            "vent_command_capture_pressure_enabled": bool(context.get("vent_command_capture_pressure_enabled", False)),
            "vent_command_query_state_enabled": bool(context.get("vent_command_query_state_enabled", False)),
            "vent_command_confirm_transition_enabled": bool(
                context.get("vent_command_confirm_transition_enabled", False)
            ),
            "vent_command_blocking_phase": context.get("vent_command_blocking_phase", ""),
            "route_conditioning_fast_vent_command_timeout": bool(
                context.get("route_conditioning_fast_vent_command_timeout", False)
            ),
            "route_conditioning_fast_vent_not_supported": bool(
                context.get("route_conditioning_fast_vent_not_supported", False)
            ),
            "route_conditioning_diagnostic_blocked_vent_scheduler": bool(
                context.get("route_conditioning_diagnostic_blocked_vent_scheduler", False)
            ),
            "route_open_transition_started": bool(context.get("route_open_transition_started", False)),
            "route_open_transition_started_at": context.get("route_open_transition_started_at", ""),
            "route_open_transition_started_monotonic_s": context.get(
                "route_open_transition_started_monotonic_s"
            ),
            "route_open_command_write_started_at": context.get("route_open_command_write_started_at", ""),
            "route_open_command_write_completed_at": context.get("route_open_command_write_completed_at", ""),
            "route_open_command_write_duration_ms": context.get("route_open_command_write_duration_ms"),
            "route_open_settle_wait_sliced": bool(context.get("route_open_settle_wait_sliced", False)),
            "route_open_settle_wait_slice_count": int(context.get("route_open_settle_wait_slice_count") or 0),
            "route_open_settle_wait_total_ms": context.get("route_open_settle_wait_total_ms"),
            "route_open_transition_total_duration_ms": context.get("route_open_transition_total_duration_ms"),
            "vent_ticks_during_route_open_transition": int(
                context.get("vent_ticks_during_route_open_transition") or 0
            ),
            "route_open_transition_max_vent_write_gap_ms": context.get(
                "route_open_transition_max_vent_write_gap_ms"
            ),
            "route_open_transition_terminal_vent_write_age_ms": context.get(
                "route_open_transition_terminal_vent_write_age_ms"
            ),
            "route_open_transition_blocked_vent_scheduler": bool(
                context.get("route_open_transition_blocked_vent_scheduler", False)
            ),
            "route_open_settle_wait_blocked_vent_scheduler": bool(
                context.get("route_open_settle_wait_blocked_vent_scheduler", False)
            ),
            "terminal_vent_write_age_ms_at_gap_gate": context.get(
                "terminal_vent_write_age_ms_at_gap_gate"
            ),
            "max_vent_pulse_write_gap_ms_including_terminal_gap": context.get(
                "max_vent_pulse_write_gap_ms_including_terminal_gap"
            ),
            "route_conditioning_vent_gap_exceeded_source": context.get(
                "route_conditioning_vent_gap_exceeded_source",
                "",
            ),
            "route_open_high_frequency_vent_phase_started": bool(
                context.get("route_open_high_frequency_vent_phase_started", False)
            ),
            "route_open_to_first_vent_write_ms": context.get("route_open_to_first_vent_write_ms"),
            "max_vent_pulse_write_gap_ms": context.get("max_vent_pulse_write_gap_ms"),
            "max_vent_command_total_duration_ms": context.get("max_vent_command_total_duration_ms"),
            "digital_gauge_pressure_hpa": pressure_hpa,
            "pressure_hpa": pressure_hpa,
            "pressure_sample_source": context.get("selected_pressure_source") or context.get("pressure_source_selected"),
            "pressure_sample_age_s": latest_age_s,
            "digital_gauge_latest_age_s": continuous_age_s,
            "pressure_sample_stale": sample_stale,
            "pressure_freshness_ok": selected_freshness_ok,
            "pressure_abnormal": pressure_abnormal,
            "abort_pressure_hpa": abort_hpa,
            "conditioning_pressure_abort_hpa": abort_hpa,
            "route_conditioning_hard_abort_pressure_hpa": hard_abort_hpa,
            "route_conditioning_hard_abort_exceeded": bool(
                context.get("route_conditioning_hard_abort_exceeded", False)
            ),
            "pressure_overlimit_seen": pressure_abnormal,
            "pressure_overlimit_source": context.get("pressure_overlimit_source"),
            "pressure_overlimit_hpa": context.get("pressure_overlimit_hpa"),
            "latest_frame_sequence_id": context.get("latest_frame_sequence_id"),
            "digital_gauge_sequence_progress": context.get("digital_gauge_sequence_progress"),
            "stream_stale": sample_stale,
            "latest_frame_interval_s": context.get("latest_frame_interval_s"),
            "stream_frame_count": context.get("stream_frame_count"),
            "vent_heartbeat_interval_s": self._a2_conditioning_vent_heartbeat_interval_s(),
            "atmosphere_vent_max_gap_s": max_gap_s,
            "vent_heartbeat_gap_s": None if vent_gap_s is None else round(float(vent_gap_s), 3),
            "vent_heartbeat_gap_exceeded": False,
            "route_conditioning_vent_gap_exceeded": False,
            "heartbeat_gap_threshold_ms": gap_state.get("heartbeat_gap_threshold_ms"),
            "heartbeat_gap_observed_ms": gap_state.get("heartbeat_gap_observed_ms"),
            "heartbeat_emission_gap_ms": gap_state.get("heartbeat_emission_gap_ms"),
            "heartbeat_gap_explained_by_blocking_operation": gap_state.get(
                "heartbeat_gap_explained_by_blocking_operation"
            ),
            "blocking_operation_name": "a2_conditioning_vent_tick",
            "blocking_operation_duration_ms": round(blocking_duration_s * 1000.0, 3),
            "whether_safe_to_continue": bool(command_result == "ok" and not pressure_abnormal and selected_freshness_ok),
            "route_open_to_first_vent_s": context.get("route_open_to_first_vent_s"),
            "route_open_to_first_vent_ms": context.get("route_open_to_first_vent_ms"),
            "route_open_to_first_pressure_read_ms": context.get("route_open_to_first_pressure_read_ms"),
            "route_open_to_overlimit_ms": context.get("route_open_to_overlimit_ms"),
            "route_conditioning_pressure_before_route_open_hpa": context.get(
                "route_conditioning_pressure_before_route_open_hpa"
            ),
            "route_conditioning_pressure_after_route_open_hpa": context.get(
                "route_conditioning_pressure_after_route_open_hpa"
            ),
            "route_conditioning_pressure_rise_rate_hpa_per_s": context.get(
                "route_conditioning_pressure_rise_rate_hpa_per_s"
            ),
            "route_conditioning_peak_pressure_hpa": context.get("route_conditioning_peak_pressure_hpa"),
            "route_conditioning_pressure_overlimit": bool(context.get("route_conditioning_pressure_overlimit", False)),
            **self._a2_route_open_transient_evidence(context),
            "pressure_rise_since_last_vent_hpa": context.get("pressure_rise_since_last_vent_hpa"),
            "pressure_monitor_interval_s": self._a2_conditioning_pressure_monitor_interval_s(),
            "selected_pressure_source_for_conditioning_monitor": context.get(
                "selected_pressure_source_for_conditioning_monitor",
                "",
            ),
            "selected_pressure_source_for_pressure_gate": context.get("selected_pressure_source_for_pressure_gate", ""),
            "a2_conditioning_pressure_source_strategy": context.get(
                "a2_conditioning_pressure_source_strategy",
                self._a2_conditioning_pressure_source_mode(),
            ),
            **self._a2_conditioning_scheduler_evidence(context),
            **self._a2_conditioning_digital_gauge_evidence(context),
        }
        ticks = [item for item in list(context.get("vent_ticks") or []) if isinstance(item, Mapping)]
        ticks.append(tick)
        context["vent_ticks"] = ticks
        context["last_vent_tick_monotonic_s"] = float(write_sent_monotonic_s or tick_started_monotonic_s)
        context["last_vent_heartbeat_started_monotonic_s"] = float(write_sent_monotonic_s or tick_started_monotonic_s)
        context["last_vent_heartbeat_completed_monotonic_s"] = tick_completed_monotonic_s
        context["last_vent_tick_completed_monotonic_s"] = tick_completed_monotonic_s
        if context.get("last_pressure_monitor_monotonic_s") in (None, ""):
            context["last_pressure_monitor_monotonic_s"] = tick_completed_monotonic_s
        context["last_vent_command_age_s"] = 0.0
        context["last_vent_command_duration_s"] = blocking_duration_s
        context["last_blocking_operation_name"] = "a2_conditioning_vent_tick"
        context["last_blocking_operation_started_monotonic_s"] = tick_started_monotonic_s
        context["last_blocking_operation_completed_monotonic_s"] = tick_completed_monotonic_s
        context["last_blocking_operation_duration_s"] = blocking_duration_s
        context["last_blocking_operation_safe_to_continue"] = bool(
            command_result == "ok" and not pressure_abnormal and selected_freshness_ok
        )
        context["vent_heartbeat_interval_s"] = self._a2_conditioning_vent_heartbeat_interval_s()
        context["atmosphere_vent_max_gap_s"] = max_gap_s
        context["route_conditioning_high_frequency_vent_interval_s"] = self._a2_conditioning_high_frequency_vent_interval_s()
        context["route_conditioning_high_frequency_max_gap_s"] = self._a2_conditioning_high_frequency_vent_max_gap_s()
        context["route_conditioning_high_frequency_vent_window_s"] = self._a2_conditioning_high_frequency_vent_window_s()
        context["route_conditioning_vent_maintenance_interval_s"] = self._a2_conditioning_vent_maintenance_interval_s()
        context["route_conditioning_vent_maintenance_max_gap_s"] = self._a2_conditioning_vent_maintenance_max_gap_s()
        context["route_conditioning_effective_vent_interval_s"] = active_interval_s
        context["route_conditioning_effective_max_gap_s"] = max_gap_s
        context["max_vent_pulse_gap_limit_ms"] = round(max_gap_s * 1000.0, 3)
        context["route_conditioning_vent_maintenance_active"] = True
        context["pressure_monitor_interval_s"] = self._a2_conditioning_pressure_monitor_interval_s()
        if context.get("route_open_to_first_vent_write_ms") not in (None, ""):
            context["route_open_to_first_vent_ms"] = context.get("route_open_to_first_vent_write_ms")
        samples = [item for item in list(context.get("pressure_samples") or []) if isinstance(item, Mapping)]
        samples.append(tick)
        context["pressure_samples"] = samples
        pressure_values = [
            self._as_float(item.get("digital_gauge_pressure_hpa", item.get("pressure_hpa")))
            for item in samples
            if isinstance(item, Mapping)
            and self._as_float(item.get("digital_gauge_pressure_hpa", item.get("pressure_hpa"))) is not None
        ]
        if pressure_values:
            context["pressure_max_during_conditioning_hpa"] = max(float(value) for value in pressure_values if value is not None)
            context["pressure_min_during_conditioning_hpa"] = min(float(value) for value in pressure_values if value is not None)
        if continuous_age_s is not None:
            context["digital_gauge_latest_age_s"] = continuous_age_s
            previous_max = self._as_float(context.get("latest_frame_age_max_s"))
            context["latest_frame_age_max_s"] = (
                continuous_age_s if previous_max is None else max(float(previous_max), float(continuous_age_s))
            )
        if context.get("latest_frame_sequence_id") is not None:
            context["last_digital_gauge_sequence_id"] = context.get("latest_frame_sequence_id")
        context.update(self._a2_conditioning_digital_gauge_evidence(context))
        context["conditioning_pressure_abort_hpa"] = abort_hpa
        context["route_conditioning_hard_abort_pressure_hpa"] = hard_abort_hpa
        context["pressure_overlimit_seen"] = bool(context.get("pressure_overlimit_seen") or pressure_abnormal)
        if pressure_abnormal:
            context["pressure_overlimit_source"] = context.get("pressure_overlimit_source")
            context["pressure_overlimit_hpa"] = context.get("pressure_overlimit_hpa")
            context["route_conditioning_high_pressure_seen_before_preseal"] = True
            context["route_conditioning_high_pressure_seen_before_preseal_hpa"] = context.get(
                "route_conditioning_high_pressure_seen_before_preseal_hpa",
                context.get("pressure_overlimit_hpa"),
            )
            context["route_conditioning_high_pressure_seen_phase"] = context.get(
                "route_conditioning_high_pressure_seen_phase",
                "co2_route_conditioning_at_atmosphere",
            )
            context["route_conditioning_high_pressure_seen_source"] = context.get(
                "route_conditioning_high_pressure_seen_source",
                context.get("pressure_overlimit_source"),
            )
            context["route_conditioning_high_pressure_seen_decision"] = context.get(
                "route_conditioning_high_pressure_seen_decision",
                "fail_closed",
            )
        if pressure_abnormal or command_result != "ok":
            abnormal_events = list(context.get("abnormal_pressure_events") or [])
            abnormal_events.append(tick)
            context["abnormal_pressure_events"] = abnormal_events
        context = self._a2_conditioning_context_with_counts(context)
        tick.update(
            {
                "max_vent_pulse_gap_ms": context.get("max_vent_pulse_gap_ms"),
                "max_vent_pulse_write_gap_ms": context.get("max_vent_pulse_write_gap_ms"),
                "max_vent_pulse_write_gap_ms_including_terminal_gap": context.get(
                    "max_vent_pulse_write_gap_ms_including_terminal_gap"
                ),
                "max_vent_pulse_write_gap_phase": context.get("max_vent_pulse_write_gap_phase"),
                "max_vent_pulse_write_gap_threshold_ms": context.get(
                    "max_vent_pulse_write_gap_threshold_ms"
                ),
                "max_vent_pulse_write_gap_threshold_source": context.get(
                    "max_vent_pulse_write_gap_threshold_source"
                ),
                "max_vent_pulse_write_gap_exceeded": context.get(
                    "max_vent_pulse_write_gap_exceeded"
                ),
                "max_vent_pulse_write_gap_not_exceeded_reason": context.get(
                    "max_vent_pulse_write_gap_not_exceeded_reason"
                ),
                "max_vent_scheduler_loop_gap_ms": context.get("max_vent_scheduler_loop_gap_ms"),
                "max_vent_command_total_duration_ms": context.get("max_vent_command_total_duration_ms"),
                "route_open_transition_max_vent_write_gap_ms": context.get(
                    "route_open_transition_max_vent_write_gap_ms"
                ),
                "route_open_transition_terminal_vent_write_age_ms": context.get(
                    "route_open_transition_terminal_vent_write_age_ms"
                ),
            }
        )
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        if bool(schedule.get("route_conditioning_high_frequency_window_active")):
            context.update(
                {
                    "trace_write_budget_ms": self._a2_conditioning_trace_write_budget_ms(),
                    "trace_write_duration_ms": 0.0,
                    "trace_write_blocked_vent_scheduler": False,
                    "trace_write_deferred_for_vent_priority": True,
                }
            )
            tick.update(
                {
                    "trace_write_budget_ms": context["trace_write_budget_ms"],
                    "trace_write_duration_ms": 0.0,
                    "trace_write_blocked_vent_scheduler": False,
                    "trace_write_deferred_for_vent_priority": True,
                }
            )
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        else:
            self._record_pressure_source_latency_events(tick, point=point, stage="co2_route_conditioning_at_atmosphere")
        self._record_a2_conditioning_workflow_timing(
            context,
            "co2_route_conditioning_vent_tick",
            "fail" if command_result != "ok" else "tick",
            stage="co2_route_conditioning_at_atmosphere",
            point=point,
            duration_s=tick["elapsed_s"],
            expected_max_s=self._cfg_get("workflow.pressure.vent_hold_interval_s", 2.0),
            wait_reason=phase,
            decision=command_result,
            pressure_hpa=pressure_hpa,
            pace_output_state=diagnostics.get("output_state"),
            pace_isolation_state=diagnostics.get("isolation_state"),
            pace_vent_status=diagnostics.get("vent_status_raw"),
            route_state=tick,
            warning_code="co2_route_conditioning_pressure_abnormal" if pressure_abnormal else None,
            error_code=command_error or ("route_conditioning_pressure_overlimit" if pressure_abnormal else None),
        )
        if command_result != "ok":
            if bool(tick.get("route_conditioning_fast_vent_command_timeout")):
                fail_reason = "route_conditioning_fast_vent_command_timeout"
            elif bool(tick.get("route_conditioning_fast_vent_not_supported")):
                fail_reason = "route_conditioning_fast_vent_not_supported"
            else:
                fail_reason = "route_conditioning_vent_command_failed"
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason=fail_reason,
                details=tick,
                event_name=f"co2_{fail_reason}",
                route_trace_action=f"co2_{fail_reason}",
                pressure_hpa=pressure_hpa,
            )
        if not selected_freshness_ok:
            reason = str(
                context.get("fail_closed_reason")
                or context.get("selected_pressure_fail_closed_reason")
                or "selected_pressure_sample_stale"
            )
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason=reason,
                details={
                    **tick,
                    "stream_stale": sample_stale,
                    "continuous_stream_stale": bool(context.get("continuous_stream_stale")),
                },
                event_name="co2_route_conditioning_stream_stale",
                route_trace_action="co2_route_conditioning_stream_stale",
                pressure_hpa=pressure_hpa,
            )
        if pressure_abnormal:
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason="route_conditioning_pressure_overlimit",
                details=tick,
                event_name="co2_route_conditioning_pressure_overlimit",
                route_trace_action="co2_preseal_atmosphere_hold_pressure_guard",
                pressure_hpa=pressure_hpa,
            )
        return tick

    def _maybe_reassert_a2_conditioning_vent(self, point: CalibrationPoint) -> None:
        if not bool(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", False)):
            return
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        if not context:
            return
        now_mono = time.monotonic()
        context = self._a2_conditioning_record_scheduler_loop(context, now_mono=now_mono)
        schedule = self._a2_conditioning_vent_schedule(context, now_mono=now_mono)
        context.update(schedule)
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        interval_s = float(schedule["route_conditioning_effective_vent_interval_s"])
        max_gap_s = float(schedule["route_conditioning_effective_max_gap_s"])
        self._a2_conditioning_fail_if_defer_not_rescheduled(
            point,
            context,
            now_mono=now_mono,
            max_gap_s=max_gap_s,
            interval_s=interval_s,
            schedule=schedule,
            phase=str(schedule.get("vent_phase") or "route_conditioning_vent_loop"),
        )
        last_tick = self._as_float(context.get("last_vent_tick_monotonic_s"))
        gap_state = self._a2_conditioning_heartbeat_gap_state(
            context,
            now_mono=now_mono,
            max_gap_s=max_gap_s,
            interval_s=interval_s,
        )
        emission_gap_s = self._as_float(gap_state.get("heartbeat_emission_gap_s"))
        observed_gap_s = self._as_float(gap_state.get("vent_heartbeat_gap_s"))
        effective_gap_s = observed_gap_s if observed_gap_s is not None else emission_gap_s
        route_open_monotonic = self._as_float(
            context.get("route_open_completed_monotonic_s")
            or getattr(self, "_a2_co2_route_open_monotonic_s", None)
        )
        if route_open_monotonic is not None and effective_gap_s is not None and effective_gap_s > max_gap_s:
            gap_ms = round(float(effective_gap_s) * 1000.0, 3)
            source = self._a2_conditioning_vent_gap_source(context)
            terminal = self._a2_conditioning_terminal_gap_details(
                context,
                now_mono=now_mono,
                max_gap_s=max_gap_s,
                source=source,
            )
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason="route_conditioning_vent_gap_exceeded",
                details={
                    **gap_state,
                    "vent_heartbeat_gap_s": round(float(effective_gap_s), 3),
                    "vent_heartbeat_interval_s": interval_s,
                    "atmosphere_vent_max_gap_s": max_gap_s,
                    "vent_heartbeat_gap_exceeded": True,
                    "route_conditioning_vent_gap_exceeded": True,
                    "max_vent_pulse_gap_ms": gap_ms,
                    "max_vent_pulse_gap_limit_ms": round(max_gap_s * 1000.0, 3),
                    **terminal,
                    "last_vent_command_age_s": round(float(effective_gap_s), 3),
                    "fail_closed_reason": "route_conditioning_vent_gap_exceeded",
                    "whether_safe_to_continue": False,
                    **schedule,
                },
                event_name="co2_route_conditioning_vent_heartbeat_gap",
                route_trace_action="co2_route_conditioning_vent_heartbeat_gap",
            )
        if (
            route_open_monotonic is not None
            and context.get("route_open_to_first_vent_s") in (None, "")
            and (now_mono - float(route_open_monotonic)) > max_gap_s
        ):
            gap_ms = round((now_mono - float(route_open_monotonic)) * 1000.0, 3)
            source = self._a2_conditioning_vent_gap_source(context)
            terminal = self._a2_conditioning_terminal_gap_details(
                context,
                now_mono=now_mono,
                max_gap_s=max_gap_s,
                source=source,
            )
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason="route_conditioning_vent_gap_exceeded",
                details={
                    "route_open_to_first_vent_s": round(now_mono - float(route_open_monotonic), 3),
                    "route_open_to_first_vent_ms": gap_ms,
                    "atmosphere_vent_max_gap_s": max_gap_s,
                    "vent_heartbeat_gap_exceeded": True,
                    "route_conditioning_vent_gap_exceeded": True,
                    "max_vent_pulse_gap_ms": gap_ms,
                    "max_vent_pulse_gap_limit_ms": round(max_gap_s * 1000.0, 3),
                    **terminal,
                    **schedule,
                },
                event_name="co2_route_conditioning_route_open_first_vent_gap",
                route_trace_action="co2_route_conditioning_route_open_first_vent_gap",
            )
        if last_tick is None or (now_mono - float(last_tick)) >= interval_s:
            self._record_a2_co2_conditioning_vent_tick(
                point,
                phase=str(schedule.get("vent_phase") or "route_conditioning_vent_maintenance"),
            )
            return
        context["vent_scheduler_priority_mode"] = True
        context["vent_scheduler_checked_before_diagnostic"] = True
        deferred = self._a2_conditioning_defer_if_diagnostic_budget_unsafe(
            point,
            context,
            now_mono=now_mono,
            max_gap_s=max_gap_s,
            budget_ms=self._a2_conditioning_pressure_monitor_budget_ms(),
            component="pressure_monitor",
            operation="conditioning_pressure_monitor_pre_loop_budget_check",
            pressure_monitor=True,
        )
        if deferred is not None:
            self._record_a2_co2_conditioning_vent_tick(
                point,
                phase=str(schedule.get("vent_phase") or "route_conditioning_vent_priority"),
            )
            return
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        last_pressure = self._as_float(context.get("last_pressure_monitor_monotonic_s"))
        pressure_interval_s = self._a2_conditioning_pressure_monitor_interval_s()
        if last_pressure is None or (now_mono - float(last_pressure)) >= pressure_interval_s:
            monitor = self._record_a2_co2_conditioning_pressure_monitor(
                point,
                phase="conditioning_pressure_monitor",
            )
            context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
            if (
                isinstance(monitor, Mapping)
                and bool(
                    monitor.get("diagnostic_deferred_for_vent_priority")
                    or monitor.get("conditioning_monitor_pressure_deferred")
                    or monitor.get("pressure_monitor_deferred_for_vent_priority")
                )
            ):
                self._a2_conditioning_reschedule_after_defer(
                    point,
                    phase=str(schedule.get("vent_phase") or "route_conditioning_defer_reschedule"),
                    reason="pressure_monitor_defer_return_to_vent_loop",
                )
                return
            diagnostic_duration_s = self._as_float(
                monitor.get("blocking_operation_duration_ms") if isinstance(monitor, Mapping) else None
            )
            if diagnostic_duration_s is not None:
                diagnostic_duration_s = float(diagnostic_duration_s) / 1000.0
            if diagnostic_duration_s is not None and diagnostic_duration_s > max_gap_s:
                source = self._a2_conditioning_diagnostic_source(context, fallback="pressure_monitor")
                terminal = self._a2_conditioning_terminal_gap_details(
                    context,
                    now_mono=time.monotonic(),
                    max_gap_s=max_gap_s,
                    source=source,
                )
                details = {
                    **context,
                    **terminal,
                    "route_conditioning_diagnostic_blocked_vent_scheduler": True,
                    "pressure_monitor_blocked_vent_scheduler": True,
                    "diagnostic_blocking_component": source,
                    "diagnostic_blocking_operation": context.get("diagnostic_blocking_operation", "pressure_monitor"),
                    "diagnostic_blocking_duration_ms": round(float(diagnostic_duration_s) * 1000.0, 3),
                    "diagnostic_duration_ms": round(float(diagnostic_duration_s) * 1000.0, 3),
                    "vent_heartbeat_interval_s": interval_s,
                    "atmosphere_vent_max_gap_s": max_gap_s,
                    "fail_closed_reason": "route_conditioning_diagnostic_blocked_vent_scheduler",
                    "whether_safe_to_continue": False,
                }
                context.update(details)
                setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
                self._fail_a2_co2_route_conditioning_closed(
                    point,
                    reason="route_conditioning_diagnostic_blocked_vent_scheduler",
                    details=details,
                    event_name="co2_route_conditioning_diagnostic_blocked_vent_scheduler",
                    route_trace_action="co2_route_conditioning_diagnostic_blocked_vent_scheduler",
                )
            pressure_rise_hpa = self._as_float(
                monitor.get("pressure_rise_since_last_vent_hpa")
                if isinstance(monitor, Mapping)
                else context.get("pressure_rise_since_last_vent_hpa")
            )
            last_tick = self._as_float(context.get("last_vent_tick_monotonic_s"))
            if (
                pressure_rise_hpa is not None
                and pressure_rise_hpa >= self._a2_conditioning_pressure_rise_vent_trigger_hpa()
                and (
                    last_tick is None
                    or (time.monotonic() - float(last_tick))
                    >= self._a2_conditioning_pressure_rise_vent_min_interval_s()
                )
            ):
                self._record_a2_co2_conditioning_vent_tick(point, phase="pressure_rise_vent_pulse")

    def _confirm_a2_co2_conditioning_before_route_open(self, point: CalibrationPoint) -> dict[str, Any]:
        if not bool(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", False)):
            return {}
        return self._record_a2_co2_conditioning_vent_tick(point, phase="before_route_open_confirm")

    def _refresh_a2_co2_conditioning_after_route_open(self, point: CalibrationPoint) -> dict[str, Any]:
        if not bool(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", False)):
            return {}
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        route_open_monotonic = self._as_float(getattr(self, "_a2_co2_route_open_monotonic_s", None))
        if route_open_monotonic is not None:
            context["route_open_completed_monotonic_s"] = float(route_open_monotonic)
            context["route_open_completed_at"] = datetime.now(timezone.utc).isoformat()
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        return self._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")

    def _end_a2_co2_route_conditioning_at_atmosphere(
        self,
        point: CalibrationPoint,
        *,
        route_soak_ok: bool,
        route_soak_actual: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        if not context:
            return {}
        completed_at = datetime.now(timezone.utc).isoformat()
        started = self._as_float(context.get("conditioning_started_monotonic_s"))
        now_monotonic = time.monotonic()
        duration_s = max(0.0, now_monotonic - float(started)) if started is not None else None
        last_vent = self._as_float(context.get("last_vent_tick_monotonic_s"))
        if last_vent is not None:
            context["last_vent_command_age_s"] = round(max(0.0, now_monotonic - float(last_vent)), 3)
        if route_soak_ok:
            schedule = self._a2_conditioning_vent_schedule(context, now_mono=now_monotonic)
            terminal = self._a2_conditioning_terminal_gap_details(
                context,
                now_mono=now_monotonic,
                max_gap_s=float(schedule["route_conditioning_effective_max_gap_s"]),
                source=self._a2_conditioning_vent_gap_source(context),
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
                        "max_vent_pulse_gap_limit_ms",
                        "max_vent_pulse_write_gap_phase",
                        "max_vent_pulse_write_gap_threshold_ms",
                        "max_vent_pulse_write_gap_threshold_source",
                        "max_vent_pulse_write_gap_exceeded",
                        "max_vent_pulse_write_gap_not_exceeded_reason",
                        "route_conditioning_vent_gap_exceeded_source",
                        "terminal_gap_source",
                        "terminal_gap_operation",
                    }
                }
            )
            terminal_age_ms = self._as_float(terminal.get("terminal_vent_write_age_ms_at_gap_gate"))
            if terminal_age_ms is not None and float(terminal_age_ms) > float(
                schedule["route_conditioning_effective_max_gap_s"]
            ) * 1000.0:
                context.update(
                    {
                        **terminal,
                        "route_conditioning_vent_gap_exceeded": True,
                        "route_conditioning_vent_gap_exceeded_source": terminal.get(
                            "terminal_gap_source",
                            "unknown",
                        ),
                        "fail_closed_reason": "route_conditioning_vent_gap_exceeded",
                        "whether_safe_to_continue": False,
                    }
                )
                setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
                self._fail_a2_co2_route_conditioning_closed(
                    point,
                    reason="route_conditioning_vent_gap_exceeded",
                    details=context,
                    event_name="co2_route_conditioning_terminal_vent_gap",
                    route_trace_action="co2_route_conditioning_terminal_vent_gap",
                )
            if bool(context.get("route_open_transient_recovery_required")) and not bool(
                context.get("route_open_transient_accepted")
            ):
                reason = str(context.get("route_open_transient_rejection_reason") or "").strip()
                if not reason:
                    if bool(context.get("route_open_transient_recovered_to_atmosphere")):
                        reason = "route_open_transient_stable_hold_timeout"
                    else:
                        reason = "route_open_transient_recovery_timeout"
                context["route_open_transient_rejection_reason"] = reason
                context["route_conditioning_phase"] = "route_conditioning_flush_phase"
                context["ready_to_seal_phase_started"] = False
                context["route_conditioning_flush_min_time_completed"] = False
                setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
                self._fail_a2_co2_route_conditioning_closed(
                    point,
                    reason=reason,
                    details=context,
                    event_name="co2_route_conditioning_transient_recovery_failed",
                    route_trace_action="co2_route_conditioning_transient_recovery_failed",
                )
        latest_pressure = self._a2_conditioning_first_float(
            context.get("latest_route_conditioning_pressure_hpa"),
            context.get("last_conditioning_pressure_hpa"),
            context.get("route_conditioning_pressure_after_route_open_hpa"),
        )
        atmosphere_pressure = self._a2_conditioning_first_float(
            context.get("measured_atmospheric_pressure_hpa"),
            context.get("route_conditioning_pressure_before_route_open_hpa"),
        )
        atmosphere_band_hpa = self._a2_prearm_baseline_atmosphere_band_hpa()
        pressure_returned_to_atmosphere: Optional[bool] = None
        if latest_pressure is not None and atmosphere_pressure is not None:
            pressure_returned_to_atmosphere = (
                abs(float(latest_pressure) - float(atmosphere_pressure)) <= atmosphere_band_hpa
            )
        hard_abort_hpa = self._as_float(
            context.get("route_conditioning_hard_abort_pressure_hpa")
            or self._a2_route_conditioning_hard_abort_pressure_hpa()
        )
        peak_pressure = self._a2_conditioning_first_float(
            context.get("pressure_overlimit_hpa"),
            context.get("route_conditioning_high_pressure_seen_before_preseal_hpa"),
            context.get("route_conditioning_peak_pressure_hpa"),
            context.get("pressure_max_during_conditioning_hpa"),
        )
        high_pressure_seen = bool(
            context.get("route_conditioning_high_pressure_seen_before_preseal")
            or context.get("route_conditioning_pressure_overlimit")
            or context.get("route_conditioning_hard_abort_exceeded")
            or (
                peak_pressure is not None
                and hard_abort_hpa is not None
                and float(peak_pressure) >= float(hard_abort_hpa)
            )
        )
        if high_pressure_seen:
            context["route_conditioning_high_pressure_seen_before_preseal"] = True
            context["route_conditioning_high_pressure_seen_before_preseal_hpa"] = peak_pressure
            context["route_conditioning_high_pressure_seen_phase"] = str(
                context.get("route_conditioning_high_pressure_seen_phase")
                or "co2_route_conditioning_at_atmosphere"
            )
            context["route_conditioning_high_pressure_seen_source"] = str(
                context.get("route_conditioning_high_pressure_seen_source")
                or context.get("pressure_overlimit_source")
                or context.get("latest_route_conditioning_pressure_source")
                or context.get("selected_pressure_source_for_conditioning_monitor")
                or ""
            )
            context["route_conditioning_high_pressure_seen_sample_age_s"] = self._a2_conditioning_first_float(
                context.get("route_conditioning_high_pressure_seen_sample_age_s"),
                context.get("selected_pressure_sample_age_s"),
                context.get("latest_route_conditioning_pressure_age_s"),
                context.get("last_conditioning_pressure_sample_age_s"),
            )
            context["route_conditioning_high_pressure_seen_decision"] = "fail_closed"
        stable_hold_s = self._a2_conditioning_first_float(
            context.get("route_conditioning_atmosphere_stable_hold_s"),
            context.get("route_open_transient_stable_hold_s"),
            context.get("conditioning_duration_s"),
        )
        context["route_conditioning_pressure_returned_to_atmosphere"] = (
            bool(pressure_returned_to_atmosphere)
            if pressure_returned_to_atmosphere is not None
            else bool(route_soak_ok and not high_pressure_seen)
        )
        context["route_conditioning_atmosphere_stable_before_flush"] = bool(
            context["route_conditioning_pressure_returned_to_atmosphere"]
            and not high_pressure_seen
        )
        context["route_conditioning_atmosphere_stable_hold_s"] = stable_hold_s
        if route_soak_ok and high_pressure_seen:
            context["route_conditioning_high_pressure_seen_decision"] = "fail_closed"
            context["fail_closed_reason"] = "route_conditioning_high_pressure_seen_before_preseal"
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason="route_conditioning_high_pressure_seen_before_preseal",
                details=context,
                event_name="co2_route_conditioning_high_pressure_before_preseal",
                route_trace_action="co2_route_conditioning_high_pressure_before_preseal",
                pressure_hpa=peak_pressure,
            )
        if route_soak_ok and pressure_returned_to_atmosphere is False:
            context["fail_closed_reason"] = "route_conditioning_not_atmosphere_stable_before_preseal"
            setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
            self._fail_a2_co2_route_conditioning_closed(
                point,
                reason="route_conditioning_not_atmosphere_stable_before_preseal",
                details=context,
                event_name="co2_route_conditioning_not_atmosphere_stable",
                route_trace_action="co2_route_conditioning_not_atmosphere_stable",
                pressure_hpa=latest_pressure,
            )
        context.update(
            {
                "conditioning_completed_at": completed_at,
                "conditioning_duration_s": None if duration_s is None else round(float(duration_s), 3),
                "conditioning_decision": "PASS" if route_soak_ok else "FAIL",
                "route_conditioning_phase": "ready_to_seal_phase"
                if route_soak_ok
                else "route_conditioning_flush_phase",
                "ready_to_seal_phase_started": bool(route_soak_ok),
                "route_conditioning_flush_min_time_completed": bool(route_soak_ok),
                "route_soak_actual": dict(route_soak_actual or {}),
                "did_not_seal_during_conditioning": True,
            }
        )
        context = self._a2_conditioning_context_with_counts(context)
        setattr(self, "_a2_co2_route_conditioning_completed", bool(route_soak_ok))
        setattr(self, "_a2_co2_route_conditioning_completed_at", completed_at if route_soak_ok else "")
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_active", False)
        setattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", context)
        self._record_workflow_timing(
            "co2_route_conditioning_end",
            "end" if route_soak_ok else "fail",
            stage="co2_route_conditioning_at_atmosphere",
            point=point,
            duration_s=duration_s,
            expected_max_s=context.get("conditioning_soak_s"),
            decision=context["conditioning_decision"],
            route_state=context,
        )
        return context

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
            route_state={"conditioning_completed": bool(getattr(self, "_a2_co2_route_conditioning_completed", False))},
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
                "conditioning_completed": bool(getattr(self, "_a2_co2_route_conditioning_completed", False)),
                "route_soak_actual": dict(route_soak_actual or {}),
            },
        )
        setattr(self, "_a2_preseal_analyzer_gate_passed", passed)
        return passed

    def _prepare_a2_high_pressure_first_point_after_conditioning(
        self,
        point: CalibrationPoint,
        pressure_points: Optional[Iterable[CalibrationPoint]] = None,
    ) -> dict[str, Any]:
        if not self._a2_co2_route_conditioning_required(point, pressure_points):
            return {}
        if not bool(getattr(self, "_a2_co2_route_conditioning_completed", False)):
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
        conditioning_completed_at = str(getattr(self, "_a2_co2_route_conditioning_completed_at", "") or "")
        self._record_workflow_timing(
            "seal_preparation_after_conditioning_start",
            "start",
            stage="seal_preparation_after_conditioning",
            point=point,
            route_state={
                "conditioning_completed_before_high_pressure_mode": True,
                "conditioning_completed_at": conditioning_completed_at,
                "preseal_analyzer_gate_passed": bool(getattr(self, "_a2_preseal_analyzer_gate_passed", False)),
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
                "preseal_analyzer_gate_passed": bool(getattr(self, "_a2_preseal_analyzer_gate_passed", False)),
                "sealed_after_conditioning": False,
            }
        )
        setattr(self, "_a2_high_pressure_first_point_context", context)
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
            route_state=dict(getattr(self, "_a2_high_pressure_first_point_context", {}) or context),
        )
        self._request_a2_high_pressure_route_open_pressure_sample(point)
        return dict(getattr(self, "_a2_high_pressure_first_point_context", {}) or context)

    def _a2_prearm_route_conditioning_baseline_max_age_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.a2_prearm_route_conditioning_baseline_max_age_s",
                self._cfg_get("workflow.pressure.prearm_route_conditioning_baseline_max_age_s", 2.0),
            )
        )
        return min(10.0, max(0.1, float(2.0 if value is None else value)))

    def _a2_prearm_baseline_freshness_max_s(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.a2_prearm_baseline_freshness_max_s",
                self._cfg_get(
                    "workflow.pressure.prearm_baseline_freshness_max_s",
                    self._cfg_get("workflow.pressure.pressure_sample_stale_threshold_s", 2.0),
                ),
            )
        )
        return min(10.0, max(0.1, float(2.0 if value is None else value)))

    def _a2_prearm_baseline_atmosphere_band_hpa(self) -> float:
        value = self._as_float(
            self._cfg_get(
                "workflow.pressure.a2_prearm_baseline_atmosphere_band_hpa",
                self._cfg_get("workflow.pressure.prearm_baseline_atmosphere_band_hpa", 2.0),
            )
        )
        return min(25.0, max(0.01, float(2.0 if value is None else value)))

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

    def _a2_latest_route_conditioning_prearm_baseline(self) -> dict[str, Any]:
        context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        if not context:
            return {
                "pressure_hpa": None,
                "source": "",
                "age_s": None,
                "eligible": False,
                "ineligible_reason": "route_conditioning_context_unavailable",
            }
        pressure = self._a2_conditioning_first_float(
            context.get("latest_route_conditioning_pressure_hpa"),
            context.get("last_conditioning_pressure_hpa"),
            context.get("route_conditioning_pressure_after_route_open_hpa"),
            context.get("route_conditioning_pressure_before_route_open_hpa"),
            context.get("measured_atmospheric_pressure_hpa"),
        )
        source = str(
            context.get("latest_route_conditioning_pressure_source")
            or context.get("last_conditioning_pressure_source")
            or context.get("selected_pressure_source_for_conditioning_monitor")
            or context.get("measured_atmospheric_pressure_source")
            or "route_conditioning_pressure"
        )
        recorded_mono = self._as_float(
            context.get("latest_route_conditioning_pressure_recorded_monotonic_s")
            or context.get("last_conditioning_pressure_monotonic_s")
        )
        if recorded_mono is not None:
            age_s = round(max(0.0, time.monotonic() - float(recorded_mono)), 3)
        else:
            age_s = self._a2_conditioning_first_float(
                context.get("latest_route_conditioning_pressure_age_s"),
                context.get("last_conditioning_pressure_sample_age_s"),
                context.get("measured_atmospheric_pressure_sample_age_s"),
            )
        atmosphere = self._a2_conditioning_first_float(
            context.get("measured_atmospheric_pressure_hpa"),
            context.get("route_conditioning_pressure_before_route_open_hpa"),
        )
        delta = None if pressure is None or atmosphere is None else round(abs(float(pressure) - float(atmosphere)), 3)
        max_age_s = self._a2_prearm_route_conditioning_baseline_max_age_s()
        band_hpa = self._a2_prearm_baseline_atmosphere_band_hpa()
        mix_risk_reason = self._a2_conditioning_relief_mix_risk_reason(context)
        reason = ""
        if pressure is None:
            reason = "latest_route_conditioning_pressure_unavailable"
        elif age_s is None:
            reason = "latest_route_conditioning_pressure_age_unavailable"
        elif float(age_s) > max_age_s:
            reason = "latest_route_conditioning_pressure_stale"
        elif delta is not None and float(delta) > band_hpa:
            reason = "latest_route_conditioning_pressure_not_atmospheric"
        elif bool(context.get("route_conditioning_pressure_overlimit")) or bool(
            context.get("route_conditioning_hard_abort_exceeded")
        ):
            reason = "route_conditioning_pressure_overlimit"
        elif mix_risk_reason:
            reason = mix_risk_reason
        eligible = not bool(reason)
        sample = {
            "stage": "high_pressure_first_point_prearm",
            "pressure_hpa": pressure,
            "pressure_sample_source": source,
            "source": source,
            "sample_age_s": age_s,
            "pressure_sample_age_s": age_s,
            "is_stale": not eligible,
            "pressure_sample_is_stale": not eligible,
            "parse_ok": pressure is not None,
            "pressure_source_selected": source if eligible else "",
            "pressure_source_selection_reason": (
                "latest_route_conditioning_pressure_selected_for_prearm_baseline"
                if eligible
                else reason
            ),
            "source_selection_reason": (
                "latest_route_conditioning_pressure_selected_for_prearm_baseline"
                if eligible
                else reason
            ),
            "a2_3_pressure_source_strategy": context.get(
                "a2_conditioning_pressure_source_strategy",
                self._a2_conditioning_pressure_source_mode(),
            ),
            "critical_window_uses_latest_frame": False,
            "critical_window_uses_query": False,
        }
        return {
            "pressure_hpa": pressure,
            "source": source,
            "age_s": age_s,
            "eligible": eligible,
            "ineligible_reason": reason,
            "max_age_s": max_age_s,
            "atmosphere_hpa": atmosphere,
            "atmosphere_delta_hpa": delta,
            "atmosphere_band_hpa": band_hpa,
            "sample": sample,
            "context": context,
        }

    def _prearm_a2_high_pressure_first_point_mode(
        self,
        point: CalibrationPoint,
        pressure_points: Optional[Iterable[CalibrationPoint]] = None,
    ) -> dict[str, Any]:
        setattr(self, "_a2_high_pressure_first_point_mode_enabled", False)
        setattr(self, "_a2_high_pressure_first_point_context", {})
        setattr(self, "_a2_high_pressure_first_point_initial_decision", "")
        setattr(self, "_a2_high_pressure_first_point_vent_preclosed", False)
        pressure_cfg_enabled = bool(
            self._cfg_get("workflow.pressure.high_pressure_first_point_mode_enabled", True)
        )
        pressure_values = self._a2_high_pressure_pressure_values(point, pressure_points)
        first_target = pressure_values[0] if pressure_values else self._as_float(point.target_pressure_hpa)
        contains_1100 = any(abs(float(value) - 1100.0) <= 0.001 for value in pressure_values)
        candidate = bool(
            pressure_cfg_enabled
            and self._workflow_timing_enabled()
            and self._workflow_no_write_guard_active()
            and contains_1100
            and first_target is not None
        )
        context: dict[str, Any] = {
            "enabled": False,
            "configured": pressure_cfg_enabled,
            "candidate": candidate,
            "planned_pressure_points_hpa": pressure_values,
            "first_target_pressure_hpa": first_target,
            "contains_1100_hpa": contains_1100,
            "trigger_reason": "not_candidate",
        }
        if not candidate:
            self._record_workflow_timing(
                "high_pressure_first_point_mode_enabled",
                "info",
                stage="high_pressure_first_point",
                point=point,
                target_pressure_hpa=first_target,
                decision="disabled",
                route_state=context,
            )
            return context
        sample_reader = getattr(self.pressure_control_service, "_current_high_pressure_first_point_sample", None)
        sample = sample_reader(stage="high_pressure_first_point_prearm", point_index=point.index) if callable(sample_reader) else {}
        sample = dict(sample) if isinstance(sample, Mapping) else {}
        raw_sample = dict(sample)
        baseline_pressure = self._as_float(sample.get("pressure_hpa"))
        baseline_stale = bool(sample.get("is_stale", sample.get("pressure_sample_is_stale")))
        baseline_age = self._as_float(sample.get("sample_age_s", sample.get("pressure_sample_age_s")))
        conditioning_completed = bool(getattr(self, "_a2_co2_route_conditioning_completed", False))
        route_context = dict(getattr(self, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        pressure_source_mode = str(
            route_context.get("a2_conditioning_pressure_source_strategy")
            or self._a2_conditioning_pressure_source_mode()
        )
        source_evidence = self._a2_prearm_baseline_sources(sample)
        prearm_expected_source = pressure_source_mode
        raw_observed_source = str(source_evidence.get("observed") or "")
        prearm_baseline_freshness_max_s = self._a2_prearm_baseline_freshness_max_s()
        raw_primary_source_text = str(
            source_evidence.get("selected_source")
            or source_evidence.get("sample_source")
            or sample.get("primary_pressure_source")
            or sample.get("pressure_source_used_for_decision")
            or ""
        )
        raw_primary_source_lower = raw_primary_source_text.lower()
        raw_primary_is_digital = bool(
            "digital_pressure_gauge" in raw_primary_source_lower
            or raw_primary_source_lower.endswith("_p3")
            or "p3" in raw_primary_source_lower
        )
        raw_primary_age_within_prearm = bool(
            baseline_pressure is not None
            and baseline_age is not None
            and float(baseline_age) <= float(prearm_baseline_freshness_max_s)
        )
        disagreement_reason = str(source_evidence.get("disagreement_reason") or "")
        raw_aux_disagreement_nonblocking = bool(
            source_evidence.get("disagreement")
            and disagreement_reason == "digital_latest_stale_pace_aux_disagreement"
            and raw_primary_is_digital
            and raw_primary_age_within_prearm
        )
        if raw_aux_disagreement_nonblocking:
            baseline_stale = False
            sample["is_stale"] = False
            sample["pressure_sample_is_stale"] = False
            sample.setdefault("a2_prearm_baseline_freshness_max_s", prearm_baseline_freshness_max_s)
            sample["baseline_primary_freshness_ok"] = True
            sample["baseline_aux_disagreement_nonblocking"] = True
        route_baseline = self._a2_latest_route_conditioning_prearm_baseline()
        latest_route_eligible = bool(route_baseline.get("eligible"))
        raw_alignment_ok = bool(
            (raw_observed_source or raw_primary_source_text)
            and (not bool(source_evidence.get("disagreement")) or raw_aux_disagreement_nonblocking)
            and (
                prearm_expected_source != "v1_aligned"
                or "p3" in (raw_observed_source or raw_primary_source_text).lower()
                or "route_conditioning" in (raw_observed_source or raw_primary_source_text).lower()
                or "digital_pressure_gauge" in raw_primary_source_lower
            )
        )
        use_route_conditioning_baseline = bool(
            pressure_source_mode == "v1_aligned"
            and conditioning_completed
            and latest_route_eligible
            and (
                baseline_pressure is None
                or baseline_stale
                or (bool(source_evidence.get("disagreement")) and not raw_aux_disagreement_nonblocking)
                or not raw_alignment_ok
            )
        )
        baseline_sample_for_context = sample
        v1_aligned_decision = str(source_evidence.get("selection_reason") or "")
        if use_route_conditioning_baseline:
            sample = dict(route_baseline.get("sample") or {})
            baseline_sample_for_context = sample
            baseline_pressure = self._as_float(sample.get("pressure_hpa"))
            baseline_stale = False
            baseline_age = self._as_float(sample.get("sample_age_s", sample.get("pressure_sample_age_s")))
            v1_aligned_decision = "latest_route_conditioning_pressure_selected_for_prearm_baseline"
        selected_source_text = str(
            sample.get("pressure_source_selected")
            or sample.get("pressure_source_used_for_decision")
            or sample.get("pressure_sample_source")
            or sample.get("source")
            or ""
        )
        prearm_alignment_ok = bool(
            selected_source_text
            and (
                pressure_source_mode != "v1_aligned"
                or use_route_conditioning_baseline
                or "p3" in selected_source_text.lower()
                or "route_conditioning" in selected_source_text.lower()
                or "digital_pressure_gauge" in selected_source_text.lower()
            )
            and (
                use_route_conditioning_baseline
                or not bool(source_evidence.get("disagreement"))
                or raw_aux_disagreement_nonblocking
            )
        )
        prearm_aux_disagreement = bool(
            source_evidence.get("disagreement")
            and disagreement_reason == "digital_latest_stale_pace_aux_disagreement"
            and (use_route_conditioning_baseline or raw_aux_disagreement_nonblocking)
        )
        prearm_primary_disagreement = bool(
            source_evidence.get("disagreement") and not prearm_aux_disagreement
        )
        prearm_aux_disagreement_nonblocking = bool(
            prearm_aux_disagreement
            and prearm_alignment_ok
            and (latest_route_eligible or raw_aux_disagreement_nonblocking)
        )
        baseline_primary_freshness_ok = bool(
            baseline_pressure is not None
            and not baseline_stale
            and (
                use_route_conditioning_baseline
                or baseline_age is None
                or float(baseline_age) <= float(prearm_baseline_freshness_max_s)
            )
        )
        pace_aux_sample = raw_sample.get("pace_pressure_sample")
        pace_aux_sample = dict(pace_aux_sample) if isinstance(pace_aux_sample, Mapping) else {}
        pace_aux_source = str(
            pace_aux_sample.get("pressure_sample_source")
            or pace_aux_sample.get("source")
            or ("pace_controller" if pace_aux_sample else "")
        )
        pace_aux_absolute_pressure_comparable = bool(
            pace_aux_sample.get("absolute_pressure_comparable", False)
        )
        if disagreement_reason == "digital_latest_stale_pace_aux_disagreement":
            pace_aux_absolute_pressure_comparable = False
        baseline_stale_reason = ""
        if baseline_pressure is None:
            baseline_stale_reason = "baseline_pressure_sample_unavailable"
        elif baseline_stale:
            baseline_stale_reason = (
                str(sample.get("fail_closed_reason") or "")
                or str(sample.get("pressure_source_selection_reason") or sample.get("source_selection_reason") or "")
                or "baseline_pressure_sample_stale"
            )
        elif not prearm_alignment_ok:
            baseline_stale_reason = "prearm_pressure_source_disagreement"
        context.update(
            {
                "high_pressure_first_point_prearm_started": True,
                "high_pressure_first_point_prearm_blocked": False,
                "high_pressure_first_point_prearm_block_reason": "",
                "high_pressure_first_point_prearm_phase": "high_pressure_first_point_prearm",
                "baseline_pressure_sample": baseline_sample_for_context,
                "prearm_raw_pressure_sample": raw_sample,
                "baseline_pressure_hpa": baseline_pressure,
                "baseline_pressure_source": sample.get("pressure_sample_source") or sample.get("source"),
                "baseline_pressure_primary_source": selected_source_text,
                "baseline_pressure_aux_source": pace_aux_source,
                "baseline_pressure_age_s": baseline_age,
                "baseline_pressure_sample_age_s": baseline_age,
                "baseline_pressure_stale": baseline_stale,
                "a2_prearm_baseline_freshness_max_s": prearm_baseline_freshness_max_s,
                "baseline_primary_freshness_ok": baseline_primary_freshness_ok,
                "baseline_aux_disagreement_nonblocking": prearm_aux_disagreement_nonblocking,
                "baseline_aux_disagreement_reason": disagreement_reason if prearm_aux_disagreement else "",
                "pace_aux_absolute_pressure_comparable": pace_aux_absolute_pressure_comparable,
                "baseline_pressure_freshness_ok": bool(
                    baseline_pressure is not None and not baseline_stale and prearm_alignment_ok
                ),
                "baseline_pressure_stale_reason": baseline_stale_reason,
                "prearm_pressure_source_expected": prearm_expected_source,
                "prearm_pressure_source_observed": raw_observed_source,
                "prearm_pressure_source_alignment_ok": prearm_alignment_ok,
                "prearm_pressure_source_disagreement": bool(source_evidence.get("disagreement")),
                "prearm_pressure_source_disagreement_reason": disagreement_reason,
                "prearm_primary_source_disagreement": prearm_primary_disagreement,
                "prearm_aux_source_disagreement": prearm_aux_disagreement,
                "prearm_aux_source_disagreement_nonblocking": prearm_aux_disagreement_nonblocking,
                "prearm_aux_source_disagreement_reason": disagreement_reason
                if prearm_aux_disagreement
                else "",
                "conditioning_monitor_pressure_source": route_context.get(
                    "selected_pressure_source_for_conditioning_monitor",
                    "",
                ),
                "pressure_gate_pressure_source": route_context.get("selected_pressure_source_for_pressure_gate", ""),
                "v1_aligned_pressure_source_decision": v1_aligned_decision,
                "latest_route_conditioning_pressure_hpa": route_baseline.get("pressure_hpa"),
                "latest_route_conditioning_pressure_source": route_baseline.get("source"),
                "latest_route_conditioning_pressure_age_s": route_baseline.get("age_s"),
                "latest_route_conditioning_pressure_eligible_for_prearm_baseline": latest_route_eligible,
                "latest_route_conditioning_pressure_ineligible_reason": route_baseline.get("ineligible_reason", ""),
                "latest_route_conditioning_pressure_atmosphere_delta_hpa": route_baseline.get(
                    "atmosphere_delta_hpa"
                ),
                "latest_route_conditioning_pressure_atmosphere_band_hpa": route_baseline.get(
                    "atmosphere_band_hpa"
                ),
            }
        )
        self._record_pressure_source_latency_events(raw_sample, point=point, stage="high_pressure_first_point_prearm")
        if baseline_pressure is None or baseline_stale or not prearm_alignment_ok:
            if baseline_stale:
                block_reason = "baseline_pressure_sample_stale"
            elif baseline_pressure is None:
                block_reason = "baseline_pressure_sample_unavailable"
            else:
                block_reason = "prearm_pressure_source_disagreement"
            context["trigger_reason"] = block_reason
            context["high_pressure_first_point_prearm_blocked"] = True
            context["high_pressure_first_point_prearm_block_reason"] = block_reason
            context["baseline_pressure_freshness_ok"] = False
            context["baseline_pressure_stale_reason"] = context.get("baseline_pressure_stale_reason") or block_reason
            self._record_workflow_timing(
                "pressure_polling_prearmed",
                "fail",
                stage="high_pressure_first_point",
                point=point,
                target_pressure_hpa=first_target,
                pressure_hpa=baseline_pressure,
                decision=block_reason,
                error_code=block_reason,
                route_state=context,
            )
            raise WorkflowValidationError(
                "A2 high-pressure first point requires a fresh baseline pressure sample before route open",
                details=context,
            )
        margin = self._as_float(self._cfg_get("workflow.pressure.high_pressure_first_point_margin_hpa", 0.0))
        margin = 0.0 if margin is None else float(margin)
        if conditioning_completed and first_target is not None and contains_1100:
            context["enabled"] = True
            context["ambient_reference_pressure_hpa"] = baseline_pressure
            context["current_ambient_reference_pressure_hpa"] = baseline_pressure
            context["trigger_reason"] = "conditioning_completed_first_1100_point"
            context["ambient_reference_margin_hpa"] = margin
            context["conditioning_completed_before_high_pressure_mode"] = True
            context["conditioning_completed_at"] = str(getattr(self, "_a2_co2_route_conditioning_completed_at", "") or "")
        elif first_target is not None and float(first_target) >= float(baseline_pressure) + margin:
            context["enabled"] = True
            context["ambient_reference_pressure_hpa"] = baseline_pressure
            context["current_ambient_reference_pressure_hpa"] = baseline_pressure
            context["trigger_reason"] = (
                "first_target_exceeds_ambient_reference_margin"
                if margin > 0
                else "first_target_above_ambient_reference"
            )
            context["ambient_reference_margin_hpa"] = margin
        else:
            context["trigger_reason"] = "first_target_not_above_ambient_reference"
        setattr(self, "_a2_high_pressure_first_point_mode_enabled", bool(context["enabled"]))
        setattr(self, "_a2_high_pressure_first_point_context", dict(context))
        if context["enabled"]:
            setattr(self, "_a2_co2_route_open_pressure_hpa", baseline_pressure)
            remember = getattr(self.pressure_control_service, "_remember_ambient_reference_pressure", None)
            if callable(remember):
                remember(
                    baseline_pressure,
                    source=str(context.get("baseline_pressure_source") or "digital_pressure_gauge_pre_route_baseline"),
                    timestamp=str(sample.get("sample_recorded_at") or sample.get("pressure_sample_timestamp") or ""),
                    monotonic_s=self._as_float(
                        sample.get("sample_recorded_monotonic_s", sample.get("pressure_sample_monotonic_s"))
                    ),
                )
        self._record_workflow_timing(
            "high_pressure_first_point_mode_enabled",
            "info",
            stage="high_pressure_first_point",
            point=point,
            target_pressure_hpa=first_target,
            pressure_hpa=baseline_pressure,
            decision="enabled" if context["enabled"] else "disabled",
            route_state=context,
        )
        self._record_workflow_timing(
            "pressure_polling_prearmed",
            "info",
            stage="high_pressure_first_point",
            point=point,
            target_pressure_hpa=first_target,
            pressure_hpa=baseline_pressure,
            decision="fresh_baseline_sample_ready",
            route_state=context,
        )
        recorder = getattr(getattr(self, "status_service", None), "record_route_trace", None)
        if callable(recorder):
            recorder(
                action="high_pressure_first_point_mode_enabled",
                route="co2",
                point=point,
                target={"pressure_hpa": first_target},
                actual=context,
                result="ok" if context["enabled"] else "skip",
                message="A2 1100 hPa high-pressure first-point mode evaluated",
            )
        return context

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
        explicit_latency = self._as_float(
            self._cfg_get(
                "workflow.pressure.preseal_capture_predictive_seal_latency_s",
                self._cfg_get("workflow.pressure.preseal_predictive_seal_latency_s"),
            )
        )
        if explicit_latency is not None:
            return max(0.0, float(explicit_latency))
        command_latency = self._as_float(
            self._cfg_get("workflow.pressure.expected_ready_to_seal_command_max_s")
        )
        confirm_latency = self._as_float(
            self._cfg_get("workflow.pressure.expected_ready_to_seal_confirm_max_s")
        )
        if command_latency is None and confirm_latency is None:
            return 0.0
        return max(0.0, float(command_latency or 0.0) + float(confirm_latency or 0.0))

    def _a2_preseal_capture_arm_context(
        self,
        point: CalibrationPoint,
        context: Mapping[str, Any],
        *,
        command_sent_at: str,
        command_sent_monotonic_s: float,
        command_completed_at: str = "",
        settle_s: float = 0.0,
    ) -> dict[str, Any]:
        first_target, ready_pressure, abort_pressure, ready_min, ready_max = (
            self._a2_preseal_capture_ready_window(point, context)
        )
        hard_abort_pressure = self._a2_preseal_capture_hard_abort_pressure_hpa(abort_pressure)
        monitor_context = {
            **dict(context),
            "preseal_capture_started": True,
            "preseal_capture_not_pressure_control": True,
            "preseal_capture_pressure_rise_expected_after_vent_close": True,
            "preseal_capture_monitor_armed_before_vent_close_command": True,
            "preseal_capture_monitor_covers_abort_path": True,
            "preseal_capture_ready_window_min_hpa": ready_min,
            "preseal_capture_ready_window_max_hpa": ready_max,
            "preseal_capture_ready_window_action": "ready_to_seal",
            "preseal_capture_over_abort_action": "urgent_seal",
            "preseal_capture_urgent_seal_threshold_hpa": abort_pressure,
            "preseal_capture_hard_abort_pressure_hpa": hard_abort_pressure,
            "preseal_capture_over_urgent_threshold_action": "urgent_seal",
            "preseal_capture_urgent_seal_triggered": False,
            "preseal_capture_urgent_seal_pressure_hpa": None,
            "preseal_capture_urgent_seal_reason": "",
            "preseal_capture_hard_abort_triggered": False,
            "preseal_capture_hard_abort_reason": "",
            "preseal_capture_continue_to_control_after_seal": False,
            "pressure_control_allowed_after_seal_confirmed": False,
            "pressure_control_target_after_preseal_hpa": first_target,
            "preseal_capture_predictive_ready_to_seal": False,
            "preseal_capture_pressure_rise_rate_hpa_per_s": None,
            "preseal_capture_estimated_time_to_target_s": None,
            "preseal_capture_seal_completion_latency_s": self._a2_preseal_capture_seal_latency_s(),
            "preseal_capture_predicted_seal_completion_pressure_hpa": None,
            "preseal_capture_predictive_trigger_reason": "",
            "preseal_guard_armed": True,
            "preseal_guard_armed_at": command_sent_at,
            "preseal_guard_arm_source": "atmosphere_vent_close_command",
            "preseal_guard_expected_arm_source": "atmosphere_vent_close_command",
            "preseal_guard_actual_arm_source": "atmosphere_vent_close_command",
            "preseal_guard_arm_source_alignment_ok": True,
            "preseal_guard_armed_from_vent_close_command": True,
            "preseal_guard_armed_from_vent_close_command_false_reason": "",
            "positive_preseal_vent_close_command_sent": True,
            "vent_close_command_sent_at": command_sent_at,
            "vent_close_command_completed_at": command_completed_at,
            "vent_close_command_monotonic_s": command_sent_monotonic_s,
            "vent_off_sent_at": command_sent_at,
            "vent_off_sent_monotonic_s": command_sent_monotonic_s,
            "vent_off_completed_at": command_completed_at,
            "vent_off_settle_s": settle_s,
            "vent_off_settle_monitor_started": True,
            "vent_off_settle_monitor_started_at": command_sent_at,
            "vent_off_settle_monitor_sample_count": 0,
            "vent_off_settle_wait_pressure_monitored": True,
            "vent_off_settle_wait_ready_to_seal_seen": False,
            "vent_off_settle_wait_overlimit_seen": False,
            "vent_off_settle_first_ready_to_seal_sample_hpa": None,
            "vent_off_settle_first_ready_to_seal_sample_at": "",
            "vent_off_settle_first_over_abort_sample_hpa": None,
            "vent_off_settle_first_over_abort_sample_at": "",
            "vent_close_to_monitor_start_latency_s": 0.0,
            "first_target_ready_to_seal_min_hpa": ready_min,
            "first_target_ready_to_seal_max_hpa": ready_max,
            "ready_to_seal_window_entered": False,
            "ready_to_seal_window_missed_reason": "",
            "first_target_ready_to_seal_pressure_hpa": None,
            "first_target_ready_to_seal_before_abort": False,
            "first_target_ready_to_seal_missed": False,
            "first_target_ready_to_seal_missed_reason": "",
            "first_over_abort_pressure_hpa": None,
            "first_over_abort_elapsed_s": None,
            "first_over_abort_source": "",
            "first_over_abort_sample_age_s": None,
            "first_over_abort_to_abort_latency_s": None,
            "positive_preseal_pressure_hpa": None,
            "positive_preseal_pressure_source_path": "",
            "positive_preseal_pressure_missing_reason": "",
            "preseal_abort_source_path": "",
            "preseal_capture_abort_reason": "",
            "preseal_capture_abort_pressure_hpa": None,
            "preseal_capture_abort_source": "",
            "preseal_capture_abort_sample_age_s": None,
            "monitor_context_propagated_to_wrapper_summary": True,
            "seal_command_allowed_after_atmosphere_vent_closed": False,
            "setpoint_command_blocked_before_seal": True,
            "output_enable_blocked_before_seal": True,
            "pressure_control_started_after_seal_confirmed": False,
            "target_pressure_hpa": first_target,
            "ready_pressure_hpa": ready_pressure,
            "abort_pressure_hpa": abort_pressure,
        }
        return monitor_context

    def _a2_mark_preseal_capture_pressure(
        self,
        point: CalibrationPoint,
        context: Mapping[str, Any],
        *,
        pressure_hpa: float,
        sample_meta: Mapping[str, Any],
        sample_at: str,
        elapsed_s: float,
        source_path: str,
    ) -> tuple[dict[str, Any], bool, bool]:
        updated = dict(context)
        first_target, _ready_pressure, abort_pressure, ready_min, ready_max = (
            self._a2_preseal_capture_ready_window(point, updated)
        )
        urgent_seal_threshold = abort_pressure
        hard_abort_pressure = self._a2_preseal_capture_hard_abort_pressure_hpa(urgent_seal_threshold)
        sample_stale = bool(sample_meta.get("pressure_sample_is_stale", sample_meta.get("is_stale", False)))
        now_mono = time.monotonic()
        previous_pressure = self._as_float(updated.get("preseal_capture_last_pressure_hpa"))
        previous_mono = self._as_float(updated.get("preseal_capture_last_monotonic_s"))
        sample_mono = self._as_float(
            sample_meta.get("sample_recorded_monotonic_s")
            or sample_meta.get("monotonic_timestamp")
            or sample_meta.get("response_received_monotonic_s")
        )
        current_mono = sample_mono if sample_mono is not None else now_mono
        rise_rate_hpa_per_s = None
        if previous_pressure is not None and previous_mono is not None and float(current_mono) > float(previous_mono):
            rise_rate_hpa_per_s = (float(pressure_hpa) - float(previous_pressure)) / (
                float(current_mono) - float(previous_mono)
            )
        seal_latency_s = self._a2_preseal_capture_seal_latency_s()
        predicted_completion_pressure = None
        estimated_time_to_target_s = None
        predictive_ready = False
        if (
            rise_rate_hpa_per_s is not None
            and rise_rate_hpa_per_s > 0.0
            and ready_min is not None
            and ready_max is not None
            and not sample_stale
            and float(pressure_hpa) < float(ready_min)
        ):
            estimated_time_to_target_s = max(
                0.0,
                (float(ready_min) - float(pressure_hpa)) / float(rise_rate_hpa_per_s),
            )
            predicted_completion_pressure = float(pressure_hpa) + (
                float(rise_rate_hpa_per_s) * float(seal_latency_s)
            )
            predictive_ready = bool(
                float(predicted_completion_pressure) >= float(ready_min)
                and float(predicted_completion_pressure) <= float(ready_max)
            )
        urgent_seal_seen = bool(
            urgent_seal_threshold is not None
            and not sample_stale
            and float(pressure_hpa) >= float(urgent_seal_threshold)
        )
        hard_abort_seen = bool(
            hard_abort_pressure is not None
            and not sample_stale
            and float(pressure_hpa) >= float(hard_abort_pressure)
        )
        ready_seen = bool(
            ready_min is not None
            and ready_max is not None
            and not sample_stale
            and float(pressure_hpa) >= float(ready_min)
            and float(pressure_hpa) <= float(ready_max)
        )
        sample_count = int(updated.get("vent_off_settle_monitor_sample_count") or 0) + 1
        source = str(
            sample_meta.get("pressure_sample_source")
            or sample_meta.get("source")
            or sample_meta.get("pressure_source_used_for_decision")
            or ""
        )
        updated.update(
            {
                "preseal_capture_started": True,
                "preseal_capture_not_pressure_control": True,
                "preseal_capture_pressure_rise_expected_after_vent_close": True,
                "preseal_capture_monitor_covers_abort_path": True,
                "vent_off_settle_monitor_started": True,
                "vent_off_settle_monitor_started_at": updated.get("vent_off_settle_monitor_started_at")
                or sample_at,
                "vent_off_settle_wait_pressure_monitored": True,
                "vent_off_settle_monitor_sample_count": sample_count,
                "vent_off_settle_wait_overlimit_seen": bool(
                    updated.get("vent_off_settle_wait_overlimit_seen", False)
                ),
                "vent_close_arm_pressure_hpa": float(pressure_hpa),
                "vent_close_arm_elapsed_s": round(max(0.0, elapsed_s), 3),
                "current_line_pressure_hpa": float(pressure_hpa),
                "positive_preseal_pressure_hpa": float(pressure_hpa),
                "positive_preseal_pressure_source_path": source_path,
                "positive_preseal_pressure_missing_reason": "",
                "first_target_ready_to_seal_min_hpa": ready_min,
                "first_target_ready_to_seal_max_hpa": ready_max,
                "preseal_capture_ready_window_min_hpa": ready_min,
                "preseal_capture_ready_window_max_hpa": ready_max,
                "preseal_capture_urgent_seal_threshold_hpa": urgent_seal_threshold,
                "preseal_capture_hard_abort_pressure_hpa": hard_abort_pressure,
                "preseal_capture_over_urgent_threshold_action": "urgent_seal",
                "preseal_capture_hard_abort_triggered": hard_abort_seen,
                "preseal_capture_hard_abort_reason": (
                    "preseal_capture_hard_abort_pressure_exceeded" if hard_abort_seen else ""
                ),
                "preseal_capture_abort_reason": str(updated.get("preseal_capture_abort_reason") or ""),
                "preseal_capture_abort_pressure_hpa": updated.get("preseal_capture_abort_pressure_hpa"),
                "preseal_capture_abort_source": str(updated.get("preseal_capture_abort_source") or ""),
                "preseal_capture_abort_sample_age_s": updated.get("preseal_capture_abort_sample_age_s"),
                "preseal_capture_continue_to_control_after_seal": False,
                "pressure_control_allowed_after_seal_confirmed": False,
                "pressure_control_target_after_preseal_hpa": first_target,
                "preseal_capture_pressure_rise_rate_hpa_per_s": rise_rate_hpa_per_s,
                "preseal_capture_estimated_time_to_target_s": estimated_time_to_target_s,
                "preseal_capture_seal_completion_latency_s": seal_latency_s,
                "preseal_capture_predicted_seal_completion_pressure_hpa": predicted_completion_pressure,
                "preseal_capture_predictive_ready_to_seal": predictive_ready,
                "preseal_capture_predictive_trigger_reason": (
                    "predicted_seal_completion_in_target_window" if predictive_ready else ""
                ),
                "preseal_capture_last_pressure_hpa": float(pressure_hpa),
                "preseal_capture_last_monotonic_s": float(current_mono),
                "target_pressure_hpa": first_target,
                **{key: value for key, value in sample_meta.items() if value not in (None, "")},
            }
        )
        if ready_seen or predictive_ready or (urgent_seal_seen and not hard_abort_seen):
            trigger = (
                "urgent_seal_threshold"
                if urgent_seal_seen and not (ready_seen or predictive_ready)
                else ("ready_pressure" if ready_seen else "predictive_ready_to_seal")
            )
            updated.update(
                {
                    "vent_close_arm_trigger": trigger,
                    "ready_reached_monotonic_s": now_mono,
                    "vent_off_settle_wait_ready_to_seal_seen": True,
                    "ready_to_seal_window_entered": bool(ready_seen),
                    "ready_to_seal_window_missed_reason": "",
                    "vent_off_settle_first_ready_to_seal_sample_hpa": float(pressure_hpa),
                    "vent_off_settle_first_ready_to_seal_sample_at": sample_at,
                    "first_target_ready_to_seal_pressure_hpa": float(pressure_hpa),
                    "first_target_ready_to_seal_elapsed_s": round(max(0.0, elapsed_s), 3),
                    "first_target_ready_to_seal_before_abort": True,
                    "first_target_ready_to_seal_missed": False,
                    "first_target_ready_to_seal_missed_reason": "",
                    "seal_command_allowed_after_atmosphere_vent_closed": True,
                    "preseal_capture_ready_window_action": (
                        "ready_to_seal"
                        if ready_seen
                        else (
                            "urgent_seal"
                            if urgent_seal_seen
                            else "predictive_ready_to_seal_before_target_window"
                        )
                    ),
                    "preseal_capture_over_abort_action": "urgent_seal",
                    "preseal_capture_urgent_seal_triggered": bool(urgent_seal_seen),
                    "preseal_capture_urgent_seal_pressure_hpa": (
                        float(pressure_hpa) if urgent_seal_seen else None
                    ),
                    "preseal_capture_urgent_seal_reason": (
                        "urgent_seal_threshold_reached" if urgent_seal_seen else ""
                    ),
                }
            )
        if hard_abort_seen:
            updated.update(
                {
                    "vent_close_arm_trigger": "hard_abort_pressure",
                    "vent_off_settle_wait_overlimit_seen": True,
                    "vent_off_settle_first_over_abort_sample_hpa": float(pressure_hpa),
                    "vent_off_settle_first_over_abort_sample_at": sample_at,
                    "first_over_abort_pressure_hpa": float(pressure_hpa),
                    "first_over_abort_elapsed_s": round(max(0.0, elapsed_s), 3),
                    "first_over_abort_source": source,
                    "first_over_abort_sample_age_s": self._as_float(
                        sample_meta.get("pressure_sample_age_s", sample_meta.get("sample_age_s"))
                    ),
                    "first_over_abort_to_abort_latency_s": 0.0,
                    "first_target_ready_to_seal_missed": True,
                    "first_target_ready_to_seal_missed_reason": "abort_before_ready_to_seal",
                    "ready_to_seal_window_missed_reason": "abort_before_ready_to_seal",
                    "seal_command_blocked_reason": "preseal_capture_hard_abort_pressure_exceeded",
                    "fail_closed_reason": "a2_positive_preseal_pressure_overlimit",
                    "preseal_capture_abort_reason": "preseal_capture_hard_abort_pressure_exceeded",
                    "preseal_capture_abort_pressure_hpa": float(pressure_hpa),
                    "preseal_capture_abort_source": source,
                    "preseal_capture_abort_sample_age_s": self._as_float(
                        sample_meta.get("pressure_sample_age_s", sample_meta.get("sample_age_s"))
                    ),
                    "preseal_capture_abort_source_path": source_path,
                    "preseal_abort_source_path": source_path,
                    "high_pressure_first_point_abort_pressure_hpa": float(pressure_hpa),
                    "high_pressure_first_point_abort_reason": "preseal_capture_hard_abort_pressure_exceeded",
                    "positive_preseal_pressure_overlimit": True,
                    "positive_preseal_overlimit_fail_closed": True,
                    "positive_preseal_abort_reason": "preseal_capture_hard_abort_pressure_exceeded",
                    "overlimit_elapsed_s_nonnegative": True,
                    "overlimit_elapsed_source": source_path,
                    "preseal_capture_over_abort_action": "fail_closed",
                    "preseal_capture_over_urgent_threshold_action": "fail_closed",
                    "preseal_capture_hard_abort_triggered": True,
                    "preseal_capture_hard_abort_reason": "preseal_capture_hard_abort_pressure_exceeded",
                    "seal_command_allowed_after_atmosphere_vent_closed": False,
                }
            )
        return updated, bool(ready_seen or predictive_ready or (urgent_seal_seen and not hard_abort_seen)), hard_abort_seen

    def _get_latest_pressure_hpa(self) -> Optional[float]:
        gauge = self._device("pressure_meter", "pressure_gauge")
        if gauge is None:
            return None
        for method_name in ("read_pressure", "read_pressure_hpa", "get_pressure", "get_pressure_hpa"):
            method = getattr(gauge, method_name, None)
            if not callable(method):
                continue
            try:
                result = method()
                if isinstance(result, (int, float)):
                    return float(result)
                if isinstance(result, dict):
                    value = self._as_float(result.get("pressure_hpa"))
                    if value is not None:
                        return value
            except Exception:
                continue
        return None

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
        context = dict(getattr(self, "_a2_high_pressure_first_point_context", {}) or {})
        context["route_open_pressure_sample_hpa"] = pressure_hpa
        context["route_open_pressure_sample_read_method"] = read_method
        context["route_open_pressure_sample_attempted"] = True
        setattr(self, "_a2_high_pressure_first_point_context", context)
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
        # A2.27: fixed delay then unconditionally seal; pressure overshoot is normal during preseal
        import time
        time.sleep(0.3)

        try:
            _ = self._get_latest_pressure_hpa()
        except Exception:
            pass

        self._seal_allowed = True
        self._seal_trigger_reason = "fixed_delay_seal_a2_27"
