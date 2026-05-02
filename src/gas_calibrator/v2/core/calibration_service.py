from __future__ import annotations

from pathlib import Path
import threading
from typing import Any, Callable, Optional

from ...data.points import reorder_points as legacy_reorder_points
from ..config import AppConfig
from ..exceptions import WorkflowInterruptedError, WorkflowValidationError
from ..qc import QCPipeline
from .device_factory import DeviceFactory
from .device_manager import DeviceManager
from .event_bus import EventBus, EventType
from .models import CalibrationPhase, CalibrationPoint, CalibrationStatus, SamplingResult
from .orchestrator import WorkflowOrchestrator
from .point_parser import LegacyExcelPointLoader, PointFilter, PointParser, TemperatureGroup
from .result_store import ResultStore
from .route_planner import RoutePlanner
from .runners.finalization_runner import FinalizationRunner
from .run_logger import RunLogger
from .session import RunSession
from .stability_checker import StabilityChecker
from .state_manager import StateManager
from .workflow_steps import FinalizeStep, PrecheckStep, SamplingStep, StartupStep, TemperatureGroupStep


def normalize_negative_temperature_route(point: CalibrationPoint) -> CalibrationPoint:
    if float(point.temp_chamber_c) >= 0.0 or not point.is_h2o_point:
        return point
    return CalibrationPoint(
        index=point.index,
        temperature_c=point.temperature_c,
        co2_ppm=point.co2_ppm,
        humidity_pct=None,
        pressure_hpa=point.pressure_hpa,
        route="co2",
        humidity_generator_temp_c=None,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=point.co2_group,
        cylinder_nominal_ppm=point.cylinder_nominal_ppm,
    )


def filter_selected_temperatures(
    points: list[CalibrationPoint],
    *,
    selected_temps_c: Any,
    log: Optional[Callable[[str], None]] = None,
) -> list[CalibrationPoint]:
    raw = selected_temps_c
    if raw in (None, "", []):
        return list(points)
    if not isinstance(raw, list):
        raw = [raw]

    selected: list[float] = []
    for item in raw:
        try:
            selected.append(float(item))
        except Exception:
            continue
    if not selected:
        if log is not None:
            log("Temperature filter requested but no valid selected_temps_c values were parsed; keep all points")
        return list(points)

    filtered = [
        point
        for point in points
        if any(abs(float(point.temp_chamber_c) - target) < 1e-9 for target in selected)
    ]
    if log is not None:
        selected_text = ",".join(f"{value:g}" for value in selected)
        log(f"Temperature filter: temps=[{selected_text}]C -> {len(filtered)}/{len(points)} points")
    return filtered


def reorder_points_for_execution(
    points: list[CalibrationPoint],
    *,
    route_planner: RoutePlanner,
    temperature_descending: bool,
) -> list[CalibrationPoint]:
    normalized = [normalize_negative_temperature_route(point) for point in points]
    return legacy_reorder_points(
        normalized,
        route_planner.water_first_temp_threshold(),
        descending_temperatures=temperature_descending,
    )


def prepare_points_for_execution(
    points: list[CalibrationPoint],
    *,
    selected_temps_c: Any,
    temperature_descending: bool,
    route_planner: RoutePlanner,
    point_parser: Optional[PointParser] = None,
    point_filter: Optional[PointFilter] = None,
    log: Optional[Callable[[str], None]] = None,
) -> list[CalibrationPoint]:
    prepared = filter_selected_temperatures(
        points,
        selected_temps_c=selected_temps_c,
        log=log,
    )
    prepared = reorder_points_for_execution(
        prepared,
        route_planner=route_planner,
        temperature_descending=temperature_descending,
    )
    if point_filter is not None and point_parser is not None:
        prepared = point_parser.filter(prepared, point_filter)
    return list(prepared)


def parse_points_for_execution(
    path: Path,
    *,
    point_parser: PointParser,
    selected_temps_c: Any,
    temperature_descending: bool,
    route_planner: RoutePlanner,
    point_filter: Optional[PointFilter] = None,
    log: Optional[Callable[[str], None]] = None,
) -> list[CalibrationPoint]:
    points = point_parser.parse(path)
    return prepare_points_for_execution(
        points,
        selected_temps_c=selected_temps_c,
        temperature_descending=temperature_descending,
        route_planner=route_planner,
        point_parser=point_parser,
        point_filter=point_filter,
        log=log,
    )


class CalibrationService:
    """Thin lifecycle facade coordinating session, state, and orchestration."""

    def __init__(
        self,
        config: AppConfig,
        device_manager: Optional[DeviceManager] = None,
        stability_checker: Optional[StabilityChecker] = None,
        device_factory: Optional[DeviceFactory] = None,
        point_parser: Optional[PointParser] = None,
        output_dir: Optional[str] = None,
    ) -> None:
        self.config = config
        self._raw_cfg: Optional[dict[str, Any]] = None
        self.device_factory = device_factory or DeviceFactory(
            simulation_mode=bool(config.features.simulation_mode)
        )
        self.device_manager = device_manager or DeviceManager(config.devices, device_factory=self.device_factory)
        self.stability_checker = stability_checker or StabilityChecker(config.workflow.stability)
        self.point_parser = point_parser or PointParser(
            legacy_excel_loader=LegacyExcelPointLoader(
                missing_pressure_policy=str(getattr(config.workflow, "missing_pressure_policy", "require") or "require"),
                carry_forward_h2o=bool(getattr(config.workflow, "h2o_carry_forward", False)),
            )
        )

        self.session = RunSession(config)
        self.output_dir = str(output_dir or config.paths.output_dir)
        if output_dir is not None:
            self.session.output_dir = Path(self.output_dir) / self.session.run_id
        self.run_id = self.session.run_id
        from ..intelligence import AIRuntime

        self.ai_runtime = AIRuntime.from_config(getattr(config, "ai", None))

        self.event_bus = EventBus()
        self.result_store = ResultStore(Path(self.output_dir), self.run_id)
        self.qc_pipeline = QCPipeline(
            config.qc,
            run_id=self.run_id,
            qc_explainer=self.ai_runtime.qc_explainer,
            ai_config=getattr(config, "ai", None),
        )
        self.run_logger = RunLogger(self.output_dir, self.run_id)
        self.state_manager = StateManager(self.event_bus)

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._done_event = threading.Event()
        self._points: list[CalibrationPoint] = []
        self._temperature_groups: list[TemperatureGroup] = []
        self._points_path: Optional[Path] = None
        self._progress_point_keys: list[str] = []
        self.runtime_hooks: Any = None

        self.orchestrator = WorkflowOrchestrator(
            service=self,
            device_manager=self.device_manager,
            stability_checker=self.stability_checker,
            session=self.session,
            event_bus=self.event_bus,
            result_store=self.result_store,
            qc_pipeline=self.qc_pipeline,
            run_logger=self.run_logger,
            state_manager=self.state_manager,
            point_parser=self.point_parser,
            config=self.config,
            stop_event=self._stop_event,
            pause_event=self._pause_event,
        )
        self.finalization_runner = FinalizationRunner(self)

    def __getattr__(self, name: str):
        orchestrator = self.__dict__.get("orchestrator")
        if orchestrator is not None and hasattr(orchestrator, name):
            return getattr(orchestrator, name)
        raise AttributeError(f"{type(self).__name__!s} object has no attribute {name!r}")

    def set_progress_callback(self, callback) -> None:
        self.state_manager.set_progress_callback(callback)

    def set_log_callback(self, callback) -> None:
        self.orchestrator.set_log_callback(callback)

    def set_runtime_hooks(self, hooks: Any) -> None:
        self.runtime_hooks = hooks

    def _invoke_runtime_hook(self, name: str, *args: Any, **kwargs: Any) -> None:
        hooks = getattr(self, "runtime_hooks", None)
        method = getattr(hooks, name, None)
        if callable(method):
            method(*args, **kwargs)

    def _filter_selected_temperatures(self, points: list[CalibrationPoint]) -> list[CalibrationPoint]:
        return filter_selected_temperatures(
            points,
            selected_temps_c=getattr(self.config.workflow, "selected_temps_c", None),
            log=self.orchestrator._log,
        )

    def _reorder_points(self, points: list[CalibrationPoint]) -> list[CalibrationPoint]:
        return reorder_points_for_execution(
            points,
            route_planner=self.orchestrator.route_planner,
            temperature_descending=bool(getattr(self.config.workflow, "temperature_descending", True)),
        )

    @staticmethod
    def _normalize_negative_temperature_route(point: CalibrationPoint) -> CalibrationPoint:
        return normalize_negative_temperature_route(point)

    def load_points(self, points_path: Optional[str] = None, point_filter: Optional[PointFilter] = None) -> int:
        path = Path(points_path or self.config.paths.points_excel)
        points = parse_points_for_execution(
            path,
            point_parser=self.point_parser,
            selected_temps_c=getattr(self.config.workflow, "selected_temps_c", None),
            temperature_descending=bool(getattr(self.config.workflow, "temperature_descending", True)),
            route_planner=self.orchestrator.route_planner,
            point_filter=point_filter,
            log=self.orchestrator._log,
        )
        self._points = list(points)
        self._temperature_groups = list(self.point_parser.group_by_temperature(points))
        self._progress_point_keys = list(self.orchestrator.route_planner.progress_point_keys(self._points))
        self._points_path = path
        self.state_manager.load_points(
            len(self._points),
            f"Loaded {len(self._points)} calibration points",
            point_keys=self._progress_point_keys,
        )
        status = self.state_manager.status
        self.session.total_points = int(status.total_points)
        self.session.completed_points = int(status.completed_points)
        self.session.progress = float(status.progress)
        self.orchestrator._log(
            f"Loaded {len(self._points)} calibration points from {path}; "
            f"planned logical points={len(self._progress_point_keys)}"
        )
        return len(self._points)

    def start(self, points_path: Optional[str] = None) -> None:
        if self._thread and self._thread.is_alive():
            raise WorkflowValidationError("Calibration service is already running")
        if points_path is not None:
            self.load_points(points_path)
        elif not self._points:
            self.load_points()
        if not self._points:
            raise WorkflowValidationError("No calibration points loaded")
        self.session.start()
        self.orchestrator.reset_run_state()
        self._stop_event.clear()
        self._pause_event.set()
        self._done_event.clear()
        self.state_manager.prepare_run(len(self._points), point_keys=self._progress_point_keys)
        status = self.state_manager.status
        self.session.total_points = int(status.total_points)
        self.session.completed_points = int(status.completed_points)
        self.session.progress = float(status.progress)
        self._thread = threading.Thread(target=self._run, name="CalibrationService", daemon=True)
        self._thread.start()

    def stop(self, wait: bool = True, timeout: float = 10.0) -> None:
        self._stop_event.set()
        self._pause_event.set()
        self.orchestrator._log("Stop requested")
        if wait and self._thread is not None:
            self._thread.join(timeout=timeout)

    def pause(self) -> None:
        self._pause_event.clear()
        self.state_manager.pause()

    def resume(self) -> None:
        self._pause_event.set()
        self.state_manager.resume()

    def wait(self, timeout: Optional[float] = None) -> bool:
        return self._done_event.wait(timeout=timeout)

    def run(self, points_path: Optional[str] = None, timeout: float = 900.0) -> None:
        self.start(points_path=points_path)
        done = self.wait(timeout=timeout)
        if not done:
            self.stop(wait=False)

    def get_status(self) -> CalibrationStatus:
        return self.state_manager.status

    def get_results(self) -> list[SamplingResult]:
        results = self.result_store.get_samples()
        if results:
            return results
        return self.orchestrator.get_results()

    def get_cleaned_results(self, point_index: Optional[int] = None) -> list[SamplingResult]:
        return self.orchestrator.get_cleaned_results(point_index)

    def get_output_files(self) -> list[str]:
        return self.orchestrator.get_output_files()

    @property
    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    @property
    def status(self) -> CalibrationStatus:
        return self.get_status()

    def _run(self) -> None:
        final_phase = CalibrationPhase.COMPLETED
        final_message = "Calibration completed"
        final_error: Optional[str] = None
        self.state_manager.start()
        try:
            self.orchestrator.run(self._points, self._temperature_groups)
        except WorkflowInterruptedError as exc:
            final_phase = CalibrationPhase.STOPPED
            final_message = str(exc) or "Calibration stopped"
            self.orchestrator._record_workflow_timing(
                "run_abort",
                "abort",
                stage="run",
                decision="STOPPED",
                error_code=str(exc) or "workflow_interrupted",
            )
        except Exception as exc:
            final_phase = CalibrationPhase.ERROR
            final_message = f"Calibration failed: {exc}"
            final_error = str(exc)
            self.session.add_error(final_error)
            self.orchestrator._log(final_message)
            self.event_bus.publish(EventType.DEVICE_ERROR, {"error": final_error})
            self.orchestrator._record_workflow_timing(
                "run_fail",
                "fail",
                stage="run",
                decision="ERROR",
                error_code=final_error,
            )
        finally:
            self.finalization_runner.run(
                final_phase=final_phase,
                final_message=final_message,
                final_error=final_error,
            )

    def _run_initialization(self) -> None:
        StartupStep(self.session, self.event_bus, self).execute()
        self._invoke_runtime_hook("after_initialization")

    def _run_precheck(self) -> None:
        self._invoke_runtime_hook("before_precheck")
        PrecheckStep(self.session, self.event_bus, self).execute()

    def _run_temperature_group(self, points, next_group=None) -> None:
        TemperatureGroupStep(self.session, self.event_bus, self, points, next_group=next_group).execute()

    def _run_h2o_route(self, point: CalibrationPoint) -> None:
        from .workflow_steps import H2oRouteStep

        H2oRouteStep(self.session, self.event_bus, self, point).execute()

    def _run_co2_route(self, point: CalibrationPoint) -> None:
        from .workflow_steps import Co2RouteStep

        Co2RouteStep(self.session, self.event_bus, self, point).execute()

    def _run_sampling(self, point: CalibrationPoint, phase: str = "", point_tag: str = "") -> None:
        SamplingStep(self.session, self.event_bus, self, point, phase=phase, point_tag=point_tag).execute()

    def _run_finalization(self) -> None:
        self._invoke_runtime_hook("before_finalization")
        FinalizeStep(self.session, self.event_bus, self).execute()

    def _sync_results_to_storage(self) -> None:
        self.orchestrator._sync_results_to_storage()

    def _sync_results_to_storage_impl(self) -> None:
        from ..storage import ArtifactImporter, DatabaseManager

        database = DatabaseManager.from_config(self.config.storage)
        try:
            database.initialize()
            importer = ArtifactImporter(database)
            importer.import_run_directory(self.session.output_dir)
        finally:
            database.dispose()

    def _generate_ai_outputs(self) -> None:
        self.orchestrator._generate_ai_outputs()
