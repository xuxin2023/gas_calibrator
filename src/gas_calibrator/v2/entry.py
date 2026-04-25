"""
V2 校准入口函数。
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from .config import AIConfig, AppConfig, StorageConfig, summarize_step2_config_safety
from .core.calibration_service import CalibrationService, SamplingResult
from .core.device_factory import DeviceFactory
from .core.no_write_guard import NoWriteDeviceFactory, build_no_write_guard_from_raw_config
from .core.point_parser import PointFilter, PointParser


RuntimeHooksFactory = Callable[[CalibrationService, Optional[dict[str, Any]]], Any]
STEP2_UNSAFE_CONFIG_UNLOCK_FLAG = "--allow-unsafe-step2-config"
STEP2_UNSAFE_CONFIG_UNLOCK_ENV = "GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG"


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


def _truthy_env(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


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
) -> CalibrationService:
    """
    创建校准服务并加载点位。
    """
    _, raw_cfg, config = load_config_bundle(
        config_path,
        simulation_mode=simulation_mode,
        allow_unsafe_step2_config=allow_unsafe_step2_config,
        enforce_step2_execution_gate=True,
        unsafe_config_cli_flag=unsafe_config_cli_flag,
        unsafe_config_env_var=unsafe_config_env_var,
    )
    return create_calibration_service_from_config(
        config,
        raw_cfg=raw_cfg,
        point_filter=point_filter,
        preload_points=True,
        runtime_hooks=runtime_hooks,
        runtime_hooks_factory=runtime_hooks_factory,
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
