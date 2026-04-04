from __future__ import annotations

from importlib import import_module
from typing import Any


__all__ = [
    "CalibrationPhase",
    "CalibrationPoint",
    "CalibrationService",
    "CalibrationStatus",
    "DataWriter",
    "DeviceFactory",
    "DeviceInfo",
    "DeviceManager",
    "DeviceStatus",
    "DeviceType",
    "Event",
    "EventBus",
    "EventType",
    "CompiledPlan",
    "PlanCompiler",
    "PointFilter",
    "PointParser",
    "RefitFilteringResult",
    "ResultStore",
    "RunLogger",
    "RunSession",
    "SamplingResult",
    "StateManager",
    "StabilityChecker",
    "StabilityResult",
    "StabilityType",
    "TemperatureGroup",
    "WorkflowOrchestrator",
    "Co2RouteStep",
    "FinalizeStep",
    "H2oRouteStep",
    "PrecheckStep",
    "SamplingStep",
    "StartupStep",
    "TemperatureGroupStep",
    "export_refit_filtering_result",
    "resolve_column_mapping",
    "run_refit_filtering",
]

_EXPORT_MAP = {
    "CalibrationPhase": (".calibration_service", "CalibrationPhase"),
    "CalibrationPoint": (".models", "CalibrationPoint"),
    "CalibrationService": (".calibration_service", "CalibrationService"),
    "CalibrationStatus": (".models", "CalibrationStatus"),
    "DataWriter": (".data_writer", "DataWriter"),
    "DeviceFactory": (".device_factory", "DeviceFactory"),
    "DeviceInfo": (".device_manager", "DeviceInfo"),
    "DeviceManager": (".device_manager", "DeviceManager"),
    "DeviceStatus": (".device_manager", "DeviceStatus"),
    "DeviceType": (".device_factory", "DeviceType"),
    "Event": (".event_bus", "Event"),
    "EventBus": (".event_bus", "EventBus"),
    "EventType": (".event_bus", "EventType"),
    "CompiledPlan": (".plan_compiler", "CompiledPlan"),
    "PlanCompiler": (".plan_compiler", "PlanCompiler"),
    "PointFilter": (".point_parser", "PointFilter"),
    "PointParser": (".point_parser", "PointParser"),
    "RefitFilteringResult": (".refit_filtering", "RefitFilteringResult"),
    "ResultStore": (".result_store", "ResultStore"),
    "RunLogger": (".run_logger", "RunLogger"),
    "RunSession": (".session", "RunSession"),
    "SamplingResult": (".models", "SamplingResult"),
    "StateManager": (".state_manager", "StateManager"),
    "StabilityChecker": (".stability_checker", "StabilityChecker"),
    "StabilityResult": (".stability_checker", "StabilityResult"),
    "StabilityType": (".stability_checker", "StabilityType"),
    "TemperatureGroup": (".point_parser", "TemperatureGroup"),
    "WorkflowOrchestrator": (".orchestrator", "WorkflowOrchestrator"),
    "Co2RouteStep": (".workflow_steps", "Co2RouteStep"),
    "FinalizeStep": (".workflow_steps", "FinalizeStep"),
    "H2oRouteStep": (".workflow_steps", "H2oRouteStep"),
    "PrecheckStep": (".workflow_steps", "PrecheckStep"),
    "SamplingStep": (".workflow_steps", "SamplingStep"),
    "StartupStep": (".workflow_steps", "StartupStep"),
    "TemperatureGroupStep": (".workflow_steps", "TemperatureGroupStep"),
    "export_refit_filtering_result": (".refit_filtering", "export_refit_filtering_result"),
    "resolve_column_mapping": (".refit_filtering", "resolve_column_mapping"),
    "run_refit_filtering": (".refit_filtering", "run_refit_filtering"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORT_MAP.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attr_name = target
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
