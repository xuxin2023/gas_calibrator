"""
V2 校准入口函数。
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from collections.abc import Mapping
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from .config import AIConfig, AppConfig, StorageConfig, summarize_step2_config_safety
from .core.calibration_service import CalibrationService, SamplingResult
from .core.device_factory import DeviceFactory
from .core.no_write_guard import (
    NoWriteConfigurationError,
    NoWriteDeviceFactory,
    build_no_write_guard_from_raw_config,
)
from .core.point_parser import PointFilter, PointParser
from .core.run001_a1_dry_run import evaluate_run001_a1_readiness, load_point_rows


RuntimeHooksFactory = Callable[[CalibrationService, Optional[dict[str, Any]]], Any]
STEP2_UNSAFE_CONFIG_UNLOCK_FLAG = "--allow-unsafe-step2-config"
STEP2_UNSAFE_CONFIG_UNLOCK_ENV = "GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG"
RUN001_A1_SAFE_GATE_ID = "run001_a1_no_write_real_machine_dry_run"


class Step2UnsafeConfigError(RuntimeError):
    """Raised when a non-default Step 2 config is loaded without the required dual unlock."""

    def __init__(self, config_path: str, config_safety: dict[str, Any]) -> None:
        self.config_path = str(config_path)
        self.config_safety = copy.deepcopy(dict(config_safety or {}))
        gate = dict(self.config_safety.get("execution_gate") or {})
        message = str(
            gate.get("summary")
            or self.config_safety.get("summary")
            or "Step 2 默认工作流已拦截当前配置。"
        )
        super().__init__(message)


class Run001A1SafetyGateError(Step2UnsafeConfigError):
    """Raised when the narrow Run-001/A1 no-write real-machine dry-run gate rejects a config."""

    def __init__(self, config_path: str, config_safety: dict[str, Any], reasons: list[str]) -> None:
        self.reasons = list(reasons)
        enriched = copy.deepcopy(dict(config_safety or {}))
        enriched["run001_a1_safety_gate"] = {
            "gate_id": RUN001_A1_SAFE_GATE_ID,
            "status": "blocked",
            "reasons": list(self.reasons),
            "unsafe_step2_bypass_used": False,
        }
        enriched["execution_gate"] = {
            **dict(enriched.get("execution_gate") or {}),
            "status": "blocked",
            "summary": "Run-001/A1 no-write real-machine dry-run safety gate blocked current config.",
            "blocked_reasons": list(self.reasons),
        }
        super().__init__(config_path, enriched)


def _truthy_env(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _as_int_list(values: Any) -> list[int]:
    if values is None:
        return []
    raw_values = values if isinstance(values, list) else [values]
    out: list[int] = []
    for item in raw_values:
        try:
            out.append(int(float(item)))
        except Exception:
            continue
    return out


def _mapping_section(raw_cfg: Mapping[str, Any], section: str) -> dict[str, Any]:
    candidate = raw_cfg.get(section)
    return dict(candidate) if isinstance(candidate, Mapping) else {}


def _run001_policy(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    policy = _mapping_section(raw_cfg, "run001_a1")
    if policy:
        return policy
    return _mapping_section(raw_cfg, "run001")


def _cli_value(cli_args: Any, name: str, default: Any = None) -> Any:
    if isinstance(cli_args, Mapping):
        return cli_args.get(name, default)
    return getattr(cli_args, name, default)


def _append_reason(reasons: list[str], reason: str) -> None:
    if reason not in reasons:
        reasons.append(reason)


def _is_run001_a1_marker(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    compact = text.replace("_", "").replace("-", "").replace("/", "").replace("\\", "").replace(" ", "")
    return "run001" in compact and "a1" in compact


def _device_enabled(devices: Mapping[str, Any], key: str) -> bool:
    candidate = devices.get(key)
    if isinstance(candidate, Mapping):
        return _coerce_bool(candidate.get("enabled", False))
    if isinstance(candidate, list):
        return any(isinstance(item, Mapping) and _coerce_bool(item.get("enabled", False)) for item in candidate)
    return False


def _run001_a1_authorization_failure_reasons(
    config: AppConfig,
    raw_cfg: Mapping[str, Any],
    cli_args: Any,
    *,
    config_path: str,
    allow_unsafe_step2_config: bool,
) -> list[str]:
    policy = _run001_policy(raw_cfg)
    workflow = _mapping_section(raw_cfg, "workflow")
    devices = _mapping_section(raw_cfg, "devices")
    coefficients = _mapping_section(raw_cfg, "coefficients")
    modeling = _mapping_section(raw_cfg, "modeling")
    reasons: list[str] = []

    if not any(
        _is_run001_a1_marker(value)
        for value in (
            policy.get("run_id"),
            policy.get("scenario"),
            raw_cfg.get("run_id"),
            raw_cfg.get("scenario"),
        )
    ):
        _append_reason(reasons, "run001_a1_marker_missing")

    mode = str(policy.get("mode", raw_cfg.get("mode", "")) or "").strip().lower()
    if mode != "real_machine_dry_run":
        _append_reason(reasons, "mode_not_real_machine_dry_run")
    if _coerce_bool(getattr(getattr(config, "features", None), "simulation_mode", False)):
        _append_reason(reasons, "simulation_mode_enabled_for_real_machine_dry_run")

    required_true_flags = (
        "no_write",
        "co2_only",
        "single_route",
        "single_temperature_group",
        "allow_real_route",
        "allow_real_pressure",
        "allow_real_wait",
        "allow_real_sample",
        "allow_artifact",
    )
    for key in required_true_flags:
        if key not in policy:
            _append_reason(reasons, f"{key}_missing")
        elif not _coerce_bool(policy.get(key)):
            _append_reason(reasons, f"{key}_not_true")

    required_false_flags = (
        "allow_write_coefficients",
        "allow_write_zero",
        "allow_write_span",
        "allow_write_calibration_parameters",
        "default_cutover_to_v2",
        "disable_v1",
    )
    for key in required_false_flags:
        if key not in policy:
            _append_reason(reasons, f"{key}_missing")
        elif _coerce_bool(policy.get(key)):
            _append_reason(reasons, f"{key}_true")

    if _as_int_list(policy.get("skip_co2_ppm")) != [0]:
        _append_reason(reasons, "policy_skip_co2_ppm_not_locked_to_0")
    if _as_int_list(workflow.get("skip_co2_ppm")) != [0]:
        _append_reason(reasons, "workflow_skip_co2_ppm_not_locked_to_0")

    if str(workflow.get("route_mode", "") or "").strip().lower() != "co2_only":
        _append_reason(reasons, "route_mode_not_co2_only")
    if _device_enabled(devices, "dewpoint_meter"):
        _append_reason(reasons, "dewpoint_meter_enabled")
    if _device_enabled(devices, "humidity_generator"):
        _append_reason(reasons, "humidity_generator_enabled")
    if _coerce_bool(coefficients.get("fit_h2o")):
        _append_reason(reasons, "fit_h2o_true")
    if _coerce_bool(modeling.get("fit_h2o")) or _coerce_bool(modeling.get("h2o_enabled")):
        _append_reason(reasons, "modeling_h2o_enabled")
    if _coerce_bool(policy.get("h2o_enabled")) or _coerce_bool(policy.get("include_h2o")):
        _append_reason(reasons, "h2o_scope_requested")
    if _coerce_bool(policy.get("h2o_single_route")):
        _append_reason(reasons, "h2o_single_route_requested")
    if _coerce_bool(policy.get("full_h2o_co2_group")) or _coerce_bool(policy.get("full_single_temperature_h2o_co2_group")):
        _append_reason(reasons, "full_h2o_co2_group_requested")

    if not _coerce_bool(_cli_value(cli_args, "execute", False)):
        _append_reason(reasons, "execute_flag_missing")
    if not _coerce_bool(_cli_value(cli_args, "confirm_real_machine_no_write", False)):
        _append_reason(reasons, "confirm_real_machine_no_write_missing")
    if _coerce_bool(_cli_value(cli_args, "allow_unsafe_step2_config", False)) or bool(allow_unsafe_step2_config):
        _append_reason(reasons, "unsafe_step2_bypass_not_allowed_for_run001_a1")

    try:
        point_rows = load_point_rows(config_path, raw_cfg)
    except Exception:
        point_rows = []
    readiness = evaluate_run001_a1_readiness(raw_cfg, config_path=config_path, point_rows=point_rows)
    for reason in list(readiness.get("hard_stop_reasons") or []):
        _append_reason(reasons, str(reason))

    try:
        guard = build_no_write_guard_from_raw_config(raw_cfg)
    except Exception:
        guard = None
        _append_reason(reasons, "no_write_guard_configuration_failed")
    if guard is None:
        _append_reason(reasons, "no_write_guard_not_installed")

    return reasons


def is_run001_a1_authorized_no_write_real_machine_dry_run(
    config: AppConfig,
    raw_cfg: Mapping[str, Any],
    cli_args: Any,
    *,
    config_path: str = "",
    allow_unsafe_step2_config: bool = False,
) -> bool:
    """Return true only for the narrow Run-001/A1 no-write real-machine dry-run path."""

    return not _run001_a1_authorization_failure_reasons(
        config,
        raw_cfg,
        cli_args,
        config_path=config_path,
        allow_unsafe_step2_config=allow_unsafe_step2_config,
    )


def authorize_run001_a1_no_write_real_machine_dry_run(
    config: AppConfig,
    raw_cfg: Mapping[str, Any],
    cli_args: Any,
    *,
    config_path: str,
    config_safety: dict[str, Any],
    allow_unsafe_step2_config: bool = False,
) -> dict[str, Any]:
    reasons = _run001_a1_authorization_failure_reasons(
        config,
        raw_cfg,
        cli_args,
        config_path=config_path,
        allow_unsafe_step2_config=allow_unsafe_step2_config,
    )
    if reasons:
        raise Run001A1SafetyGateError(config_path, config_safety, reasons)
    gate = {
        "gate_id": RUN001_A1_SAFE_GATE_ID,
        "status": "authorized",
        "scope": "Run-001/A1 CO2-only skip0 single-route single-temperature no-write real-machine dry-run",
        "unsafe_step2_bypass_used": False,
        "requires_execute_flag": True,
        "requires_confirm_real_machine_no_write": True,
        "requires_no_write_guard_before_service": True,
    }
    return gate


def _resolve_relative_path(base_dir: Path, raw_path: str) -> str:
    text = str(raw_path or "").strip()
    if not text:
        return text
    candidate = Path(text).expanduser()
    if candidate.is_absolute() or text == ":memory:":
        return str(candidate)
    return str((base_dir / candidate).resolve())


def _read_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _merge_optional_section(
    config_path: str,
    raw_cfg: dict[str, Any],
    *,
    filename: str,
    section: str,
    should_skip: Callable[[dict[str, Any]], bool],
) -> dict[str, Any]:
    if should_skip(raw_cfg):
        return raw_cfg
    candidate = Path(config_path).with_name(filename)
    if not candidate.exists():
        return raw_cfg
    try:
        payload = _read_json_file(candidate)
    except Exception:
        return raw_cfg
    merged = copy.deepcopy(raw_cfg)
    merged[section] = payload.get(section) if isinstance(payload.get(section), dict) else payload
    return merged


def _resolve_raw_config_paths(raw_cfg: dict[str, Any], config_path: str) -> dict[str, Any]:
    base_dir = Path(config_path).expanduser().resolve().parent
    resolved = copy.deepcopy(raw_cfg)
    paths = resolved.setdefault("paths", {})
    for key in ("points_excel", "output_dir", "logs_dir"):
        value = paths.get(key)
        if isinstance(value, str) and value.strip():
            paths[key] = _resolve_relative_path(base_dir, value)

    storage = resolved.get("storage")
    if isinstance(storage, dict):
        backend = str(storage.get("backend", "") or "").strip().lower()
        dsn = str(storage.get("dsn", "") or "").strip()
        database = storage.get("database")
        if backend in {"sqlite", "sqlite3"} and not dsn and isinstance(database, str) and database.strip():
            storage["database"] = _resolve_relative_path(base_dir, database)

    modeling = resolved.get("modeling")
    if isinstance(modeling, dict):
        data_source = modeling.get("data_source")
        if isinstance(data_source, dict):
            source_path = data_source.get("path")
            if isinstance(source_path, str) and source_path.strip():
                data_source["path"] = _resolve_relative_path(base_dir, source_path)
        export = modeling.get("export")
        if isinstance(export, dict):
            export_dir = export.get("output_dir")
            if isinstance(export_dir, str) and export_dir.strip():
                export["output_dir"] = _resolve_relative_path(base_dir, export_dir)
    return resolved


def load_config_bundle(
    config_path: str,
    *,
    simulation_mode: bool = False,
    allow_unsafe_step2_config: bool = False,
    enforce_step2_execution_gate: bool = False,
    unsafe_config_cli_flag: str = STEP2_UNSAFE_CONFIG_UNLOCK_FLAG,
    unsafe_config_env_var: str = STEP2_UNSAFE_CONFIG_UNLOCK_ENV,
) -> Tuple[str, dict[str, Any], AppConfig]:
    resolved_config_path = str(Path(config_path).expanduser().resolve())
    raw_cfg = _read_json_file(Path(resolved_config_path))
    raw_cfg = _merge_optional_section(
        resolved_config_path,
        raw_cfg,
        filename="storage_config.json",
        section="storage",
        should_skip=lambda payload: StorageConfig.from_dict(payload.get("storage")).database_enabled,
    )
    raw_cfg = _merge_optional_section(
        resolved_config_path,
        raw_cfg,
        filename="ai_config.json",
        section="ai",
        should_skip=lambda payload: AIConfig.from_dict(payload.get("ai")).enabled,
    )
    raw_cfg = _resolve_raw_config_paths(raw_cfg, resolved_config_path)
    config = AppConfig.from_dict(raw_cfg)
    if simulation_mode:
        config.features.simulation_mode = True
        raw_cfg.setdefault("features", {})["simulation_mode"] = True
    config_safety = summarize_step2_config_safety(
        config,
        allow_unsafe_step2_config=allow_unsafe_step2_config,
        unsafe_config_env_enabled=_truthy_env(os.environ.get(unsafe_config_env_var)),
        unsafe_config_cli_flag=unsafe_config_cli_flag,
        unsafe_config_env_var=unsafe_config_env_var,
    )
    raw_cfg["_config_safety"] = copy.deepcopy(config_safety)
    raw_cfg["_step2_execution_gate"] = copy.deepcopy(dict(config_safety.get("execution_gate") or {}))
    raw_cfg["_resolved_config_path"] = resolved_config_path
    setattr(config, "_config_safety", copy.deepcopy(config_safety))
    setattr(config, "_step2_execution_gate", copy.deepcopy(dict(config_safety.get("execution_gate") or {})))
    if enforce_step2_execution_gate and not bool(config_safety.get("step2_default_workflow_allowed", True)):
        raise Step2UnsafeConfigError(resolved_config_path, config_safety)
    return resolved_config_path, raw_cfg, config


def _resolve_config_paths(config: AppConfig, config_path: str) -> AppConfig:
    base_dir = Path(config_path).expanduser().resolve().parent
    config.paths.points_excel = _resolve_relative_path(base_dir, config.paths.points_excel)
    config.paths.output_dir = _resolve_relative_path(base_dir, config.paths.output_dir)
    config.paths.logs_dir = _resolve_relative_path(base_dir, config.paths.logs_dir)

    backend = str(config.storage.backend or "").strip().lower()
    if backend in {"sqlite", "sqlite3"} and not str(config.storage.dsn or "").strip():
        config.storage.database = _resolve_relative_path(base_dir, config.storage.database)
    return config


def _load_storage_config(config_path: str, current: StorageConfig) -> StorageConfig:
    if current.database_enabled:
        return current
    candidate = Path(config_path).with_name("storage_config.json")
    if not candidate.exists():
        return current
    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except Exception:
        return current
    loaded = StorageConfig.from_dict(payload)
    return loaded if loaded.database_enabled else current


def _load_ai_config(config_path: str, current: AIConfig) -> AIConfig:
    if current.enabled:
        return current
    candidate = Path(config_path).with_name("ai_config.json")
    if not candidate.exists():
        return current
    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except Exception:
        return current
    loaded = AIConfig.from_dict(payload)
    return loaded if loaded.enabled else current


def create_calibration_service(
    config_path: str,
    simulation_mode: bool = False,
    point_filter: Optional[PointFilter] = None,
    runtime_hooks: Any = None,
    runtime_hooks_factory: Optional[RuntimeHooksFactory] = None,
    allow_unsafe_step2_config: bool = False,
    unsafe_config_cli_flag: str = STEP2_UNSAFE_CONFIG_UNLOCK_FLAG,
    unsafe_config_env_var: str = STEP2_UNSAFE_CONFIG_UNLOCK_ENV,
    run001_a1_no_write_dry_run_cli_args: Any = None,
) -> CalibrationService:
    """
    创建校准服务并加载点位。
    """
    resolved_config_path, raw_cfg, config = load_config_bundle(
        config_path,
        simulation_mode=simulation_mode,
        allow_unsafe_step2_config=allow_unsafe_step2_config,
        enforce_step2_execution_gate=False,
        unsafe_config_cli_flag=unsafe_config_cli_flag,
        unsafe_config_env_var=unsafe_config_env_var,
    )
    config_safety = dict(raw_cfg.get("_config_safety") or {})
    require_no_write_guard = False
    if run001_a1_no_write_dry_run_cli_args is not None:
        gate = authorize_run001_a1_no_write_real_machine_dry_run(
            config,
            raw_cfg,
            run001_a1_no_write_dry_run_cli_args,
            config_path=resolved_config_path,
            config_safety=config_safety,
            allow_unsafe_step2_config=allow_unsafe_step2_config,
        )
        raw_cfg["_run001_a1_safety_gate"] = copy.deepcopy(gate)
        setattr(config, "_run001_a1_safety_gate", copy.deepcopy(gate))
        require_no_write_guard = True
    elif not bool(config_safety.get("step2_default_workflow_allowed", True)):
        raise Step2UnsafeConfigError(resolved_config_path, config_safety)
    return create_calibration_service_from_config(
        config,
        raw_cfg=raw_cfg,
        point_filter=point_filter,
        preload_points=True,
        runtime_hooks=runtime_hooks,
        runtime_hooks_factory=runtime_hooks_factory,
        require_no_write_guard=require_no_write_guard,
    )


def create_calibration_service_from_config(
    config: AppConfig,
    *,
    raw_cfg: Optional[dict[str, Any]] = None,
    point_filter: Optional[PointFilter] = None,
    preload_points: bool = False,
    service_cls: Type[CalibrationService] = CalibrationService,
    service_init_kwargs: Optional[dict[str, Any]] = None,
    runtime_hooks: Any = None,
    runtime_hooks_factory: Optional[RuntimeHooksFactory] = None,
    require_no_write_guard: bool = False,
) -> CalibrationService:
    simulation_context: Optional[dict[str, Any]] = None
    if isinstance(raw_cfg, dict):
        for key in ("simulation_context", "simulation"):
            candidate = raw_cfg.get(key)
            if isinstance(candidate, dict):
                simulation_context = copy.deepcopy(candidate)
                break
    device_factory = DeviceFactory(
        simulation_mode=bool(config.features.simulation_mode),
        simulation_context=simulation_context,
    )
    no_write_guard = build_no_write_guard_from_raw_config(raw_cfg)
    if require_no_write_guard and no_write_guard is None:
        raise NoWriteConfigurationError("Run-001/A1 authorization requires no-write guard before service creation")
    if no_write_guard is not None:
        device_factory = NoWriteDeviceFactory(device_factory, no_write_guard)
    point_parser = PointParser()
    service = service_cls(
        config=config,
        device_factory=device_factory,
        point_parser=point_parser,
        **(service_init_kwargs or {}),
    )
    service._raw_cfg = copy.deepcopy(raw_cfg) if raw_cfg is not None else None
    service._config_path = None
    if isinstance(raw_cfg, dict):
        service._config_path = raw_cfg.get("_resolved_config_path")
    service.no_write_guard = no_write_guard
    resolved_runtime_hooks = runtime_hooks
    if runtime_hooks_factory is not None:
        resolved_runtime_hooks = runtime_hooks_factory(service, service._raw_cfg)
    if resolved_runtime_hooks is not None:
        service.set_runtime_hooks(resolved_runtime_hooks)
    if preload_points:
        service.load_points(config.paths.points_excel, point_filter=point_filter)
    return service


def run_calibration(
    config_path: str,
    simulation_mode: bool = False,
    point_filter: Optional[PointFilter] = None,
    on_progress: Optional[Callable] = None,
    on_log: Optional[Callable[[str], None]] = None,
) -> List[SamplingResult]:
    """
    创建服务、运行校准并返回结果。
    """
    service = create_calibration_service(
        config_path=config_path,
        simulation_mode=simulation_mode,
        point_filter=point_filter,
    )
    if on_progress is not None:
        service.set_progress_callback(on_progress)
    if on_log is not None:
        service.set_log_callback(on_log)

    service.start()
    service.wait()
    return service.get_results()
