from __future__ import annotations

import json
import os
import sys
import time
import traceback
import copy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, Iterable, List, Optional

V2_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = Path(__file__).resolve().parents[3]

__test__ = False

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from gas_calibrator.v2.core.calibration_service import CalibrationService
from gas_calibrator.v2.core.acceptance_model import build_user_visible_evidence_boundary
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.point_parser import PointFilter
from gas_calibrator.v2.config import AppConfig, build_step2_config_safety_review
from gas_calibrator.v2.entry import create_calibration_service_from_config, load_config_bundle
from gas_calibrator.v2.scripts._cli_safety import build_step2_cli_safety_lines


CONFIG_PATH = V2_ROOT / "configs" / "test_v2_config.json"
OUTPUT_ROOT = V2_ROOT / "output" / "test_v2"
MAINLINE_RUNTIME_PROFILE = "mainline"
BENCH_RUNTIME_PROFILE = "bench"
REAL_BENCH_UNLOCK_FLAG = "--allow-real-bench"
REAL_BENCH_UNLOCK_ENV = "GAS_CALIBRATOR_V2_ALLOW_REAL_BENCH"
USAGE = (
    "Usage: python -m gas_calibrator.v2.scripts.test_v2_device "
    "[connection|single|full] [--bench] [--allow-real-bench]"
)


@dataclass(frozen=True)
class BenchRuntimePolicy:
    warmup_retries: int = 3
    warmup_delay_s: float = 0.2
    prepare_pressure_controller: bool = True
    configure_opened_analyzers: bool = True
    restore_valve_baseline: bool = True


class RealBenchLockedError(RuntimeError):
    """Raised when a caller attempts to enter the future real bench path without the dual unlock."""


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _print(message: str) -> None:
    print(message, flush=True)


def _truthy_env(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _real_bench_env_enabled() -> bool:
    return _truthy_env(os.environ.get(REAL_BENCH_UNLOCK_ENV))


def _assert_real_bench_unlocked(*, allow_real_bench: bool) -> None:
    if allow_real_bench and _real_bench_env_enabled():
        return

    missing: list[str] = []
    if not allow_real_bench:
        missing.append(f"CLI flag {REAL_BENCH_UNLOCK_FLAG}")
    if not _real_bench_env_enabled():
        missing.append(f"environment {REAL_BENCH_UNLOCK_ENV}=1")

    raise RealBenchLockedError(
        "Real bench mode is locked during Step 2. "
        "Default test_v2_device execution is simulation-only. "
        "A future real bench path requires both "
        f"{REAL_BENCH_UNLOCK_FLAG} and {REAL_BENCH_UNLOCK_ENV}=1. "
        f"Missing: {', '.join(missing)}."
    )


def _resolve_path(path_value: str) -> Path:
    candidate = Path(path_value)
    if candidate.is_absolute():
        return candidate
    return V2_ROOT / candidate


def _current_repo_root() -> Path:
    return V2_ROOT.parents[2]


def _relocate_repo_path(path_value: Any, *, source_base: Any) -> Any:
    if not isinstance(path_value, str) or not path_value.strip():
        return path_value
    candidate = Path(path_value).expanduser()
    if not candidate.is_absolute():
        return path_value
    source_text = str(source_base or "").strip()
    if not source_text:
        return path_value
    try:
        old_base = Path(source_text).expanduser()
        relative = candidate.relative_to(old_base)
    except Exception:
        return path_value
    return str((_current_repo_root() / relative).resolve())


def _normalize_portable_raw_config(raw_cfg: dict[str, Any]) -> dict[str, Any]:
    normalized = copy.deepcopy(raw_cfg)
    source_base = normalized.get("_base_dir")
    normalized["_base_dir"] = str(_current_repo_root())

    paths = normalized.get("paths")
    if isinstance(paths, dict):
        for key in ("points_excel", "output_dir", "logs_dir"):
            paths[key] = _relocate_repo_path(paths.get(key), source_base=source_base)

    storage = normalized.get("storage")
    if isinstance(storage, dict):
        storage["database"] = _relocate_repo_path(storage.get("database"), source_base=source_base)

    modeling = normalized.get("modeling")
    if isinstance(modeling, dict):
        data_source = modeling.get("data_source")
        if isinstance(data_source, dict):
            data_source["path"] = _relocate_repo_path(data_source.get("path"), source_base=source_base)
        export = modeling.get("export")
        if isinstance(export, dict):
            export["output_dir"] = _relocate_repo_path(export.get("output_dir"), source_base=source_base)

    if "_user_tuning_path" in normalized:
        normalized["_user_tuning_path"] = _relocate_repo_path(
            normalized.get("_user_tuning_path"),
            source_base=source_base,
        )
    return normalized


def _effective_runtime_paths(runtime_cfg: AppConfig) -> dict[str, str]:
    return {
        "points_excel": str(Path(runtime_cfg.paths.points_excel).resolve()),
        "output_dir": str(Path(runtime_cfg.paths.output_dir).resolve()),
        "logs_dir": str(Path(runtime_cfg.paths.logs_dir).resolve()),
    }


def _print_effective_paths(runtime_cfg: AppConfig) -> dict[str, str]:
    paths = _effective_runtime_paths(runtime_cfg)
    _print("Effective paths:")
    _print(f"  points_excel: {paths['points_excel']}")
    _print(f"  output_dir: {paths['output_dir']}")
    _print(f"  logs_dir: {paths['logs_dir']}")
    return paths


def _to_namespace(value: Any) -> Any:
    if isinstance(value, dict):
        return SimpleNamespace(**{key: _to_namespace(item) for key, item in value.items()})
    if isinstance(value, list):
        return [_to_namespace(item) for item in value]
    return value


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def _device_namespace(raw: Optional[dict[str, Any]]) -> Optional[SimpleNamespace]:
    if not isinstance(raw, dict):
        return None
    payload = dict(raw)
    payload.setdefault("enabled", True)
    payload.setdefault("timeout", 1.0)
    return _to_namespace(payload)


def _build_runtime_config(raw_cfg: dict[str, Any]) -> AppConfig:
    return AppConfig.from_dict(_normalize_portable_raw_config(raw_cfg))


def _create_mainline_service(
    raw_cfg: dict[str, Any],
    runtime_cfg: AppConfig,
    *,
    point_filter: Optional[PointFilter] = None,
    preload_points: bool = False,
) -> CalibrationService:
    """Build the default device-test service through the formal V2 entrypoint."""

    return _create_service_for_runtime_profile(
        raw_cfg,
        runtime_cfg,
        runtime_profile=MAINLINE_RUNTIME_PROFILE,
        point_filter=point_filter,
        preload_points=preload_points,
    )


def _create_v2_service(
    raw_cfg: dict[str, Any],
    runtime_cfg: AppConfig,
    *,
    point_filter: Optional[PointFilter] = None,
    preload_points: bool = False,
) -> CalibrationService:
    """Backward-compatible alias for existing tests and scripts."""

    return _create_mainline_service(
        raw_cfg,
        runtime_cfg,
        point_filter=point_filter,
        preload_points=preload_points,
    )


class BenchRuntimeAdapter:
    """Thin bench-only compatibility hooks layered on top of the formal V2 service."""

    def __init__(self, service: CalibrationService, policy: Optional[BenchRuntimePolicy] = None) -> None:
        self.service = service
        self.policy = policy or BenchRuntimePolicy()

    def after_initialization(self) -> None:
        if self.policy.prepare_pressure_controller:
            _prepare_pressure_controller(self.service.device_manager, self.service._log)
        if self.policy.configure_opened_analyzers:
            _configure_opened_gas_analyzers(self.service, self.service._log)

    def before_precheck(self) -> None:
        warmup = _warmup_gas_analyzers(
            self.service.device_manager,
            self.service._log,
            retries=self.policy.warmup_retries,
            delay_s=self.policy.warmup_delay_s,
        )
        failed = [name for name, ok in warmup.items() if not ok]
        if failed:
            self.service._log(f"Analyzer warm-up still failing before core precheck: {failed}")

    def before_finalization(self) -> None:
        if not self.policy.restore_valve_baseline:
            return
        try:
            self.service.orchestrator.valve_routing_service.apply_route_baseline_valves()
            self.service._log("Bench final valve baseline restored via ValveRoutingService")
        except Exception as exc:
            self.service._log(f"Valve reset before finalization failed: {exc}")


def _bench_runtime_policy_from_raw_cfg(raw_cfg: Optional[dict[str, Any]]) -> BenchRuntimePolicy:
    workflow = raw_cfg.get("workflow", {}) if isinstance(raw_cfg, dict) else {}
    retry_cfg = workflow.get("sensor_read_retry", {}) if isinstance(workflow, dict) else {}
    retries = max(3, int(retry_cfg.get("retries", 1)) + 1)
    delay_s = max(0.2, float(retry_cfg.get("delay_s", 0.05)))
    return BenchRuntimePolicy(
        warmup_retries=retries,
        warmup_delay_s=delay_s,
        prepare_pressure_controller=True,
        configure_opened_analyzers=True,
        restore_valve_baseline=True,
    )


def _build_runtime_hooks_for_profile(
    runtime_profile: str,
    *,
    service: CalibrationService,
    policy: Optional[BenchRuntimePolicy] = None,
) -> Any:
    if runtime_profile == MAINLINE_RUNTIME_PROFILE:
        return None
    if runtime_profile == BENCH_RUNTIME_PROFILE:
        service.stability_checker.set_debug_callback(service._log)
        return BenchRuntimeAdapter(service, policy=policy or BenchRuntimePolicy())
    raise ValueError(f"Unsupported runtime profile: {runtime_profile}")


def _runtime_hooks_factory_for_profile(
    runtime_profile: str,
    *,
    policy: Optional[BenchRuntimePolicy] = None,
) -> Optional[Callable[[CalibrationService, Optional[dict[str, Any]]], Any]]:
    if runtime_profile == MAINLINE_RUNTIME_PROFILE:
        return None
    if runtime_profile == BENCH_RUNTIME_PROFILE:
        resolved_policy = policy or BenchRuntimePolicy()
        return lambda service, profile_raw_cfg: _build_runtime_hooks_for_profile(
            runtime_profile,
            service=service,
            policy=resolved_policy,
        )
    raise ValueError(f"Unsupported runtime profile: {runtime_profile}")


def _create_service_for_runtime_profile(
    raw_cfg: dict[str, Any],
    runtime_cfg: AppConfig,
    *,
    runtime_profile: str,
    point_filter: Optional[PointFilter] = None,
    preload_points: bool = False,
    allow_real_bench: bool = False,
) -> CalibrationService:
    policy = None
    if runtime_profile == BENCH_RUNTIME_PROFILE:
        _assert_real_bench_unlocked(allow_real_bench=allow_real_bench)
        policy = _bench_runtime_policy_from_raw_cfg(raw_cfg)
    return create_calibration_service_from_config(
        runtime_cfg,
        raw_cfg=raw_cfg,
        point_filter=point_filter,
        preload_points=preload_points,
        runtime_hooks_factory=_runtime_hooks_factory_for_profile(runtime_profile, policy=policy),
    )


def _prepare_pressure_controller(device_manager: DeviceManager, log: Callable[[str], None]) -> None:
    controller = device_manager.get_device("pressure_controller")
    if controller is None:
        return
    for method_name, args in (
        ("set_units_hpa", ()),
        ("set_output_mode_active", ()),
        ("set_output", (True,)),
    ):
        method = getattr(controller, method_name, None)
        if not callable(method):
            continue
        try:
            method(*args)
            log(f"Pressure controller prepared via {method_name}")
        except Exception as exc:
            log(f"Pressure controller preparation failed in {method_name}: {exc}")


def _configure_opened_gas_analyzers(
    service: CalibrationService,
    log: Callable[[str], None],
) -> None:
    analyzer_service = getattr(service, "analyzer_fleet_service", None)
    orchestrator = getattr(service, "orchestrator", None)
    if analyzer_service is None and orchestrator is not None:
        analyzer_service = getattr(orchestrator, "analyzer_fleet_service", None)

    all_analyzers = getattr(analyzer_service, "all_gas_analyzers", None)
    configure = getattr(analyzer_service, "configure_gas_analyzer", None)
    if not callable(all_analyzers) or not callable(configure):
        all_analyzers = getattr(orchestrator, "_all_gas_analyzers", None)
        configure = getattr(orchestrator, "_configure_gas_analyzer", None)
    if not callable(all_analyzers) or not callable(configure):
        return

    for label, analyzer, cfg in all_analyzers():
        try:
            configure(analyzer, label=label, cfg=cfg)
            log(f"Analyzer {label} configured via AnalyzerFleetService")
        except Exception as exc:
            log(f"Analyzer {label} formal configuration failed: {exc}")


def _probe_analyzer(analyzer: Any) -> dict[str, Any]:
    started = time.time()
    try:
        status = analyzer.status()
    except Exception as exc:
        return {
            "ok": False,
            "elapsed_s": round(time.time() - started, 3),
            "error": str(exc),
        }

    payload = dict(status) if isinstance(status, dict) else {"raw_status": status}
    payload["ok"] = bool(payload.get("ok", bool(status)))
    payload["elapsed_s"] = round(time.time() - started, 3)
    return payload


def _warmup_gas_analyzers(
    device_manager: DeviceManager,
    log: Callable[[str], None],
    *,
    retries: int = 3,
    delay_s: float = 1.0,
) -> dict[str, bool]:
    results: dict[str, bool] = {}
    for index in range(8):
        name = f"gas_analyzer_{index}"
        analyzer = device_manager.get_device(name)
        if analyzer is None:
            continue

        ok = False
        for attempt in range(1, max(1, retries) + 1):
            snapshot = _probe_analyzer(analyzer)
            ok = bool(snapshot.get("ok"))
            log(
                f"Analyzer precheck probe {index} attempt {attempt}/{max(1, retries)}: "
                f"ok={ok} elapsed={snapshot.get('elapsed_s')}s "
                f"mode={snapshot.get('mode')} status={snapshot.get('status')} "
                f"co2={snapshot.get('co2_ppm')} h2o={snapshot.get('h2o_mmol')} "
                f"error={snapshot.get('error')}"
            )
            if ok:
                break
            if attempt < max(1, retries):
                time.sleep(max(0.0, delay_s))
        results[name] = ok
    return results


def _attach_bench_runtime_adapter(
    service: CalibrationService,
    raw_cfg: Optional[dict[str, Any]] = None,
    *,
    policy: Optional[BenchRuntimePolicy] = None,
) -> CalibrationService:
    """Backward-compatible alias for callers still expecting post-build bench hook attachment."""

    resolved_policy = policy
    if resolved_policy is None:
        resolved_policy = _bench_runtime_policy_from_raw_cfg(raw_cfg or getattr(service, "_raw_cfg", None))
    hooks = _build_runtime_hooks_for_profile(
        BENCH_RUNTIME_PROFILE,
        service=service,
        policy=resolved_policy,
    )
    setter = getattr(service, "set_runtime_hooks", None)
    if callable(setter):
        setter(hooks)
    else:
        setattr(service, "runtime_hooks", hooks)
    return service


def _create_bench_service(
    raw_cfg: dict[str, Any],
    runtime_cfg: AppConfig,
    *,
    point_filter: Optional[PointFilter] = None,
    preload_points: bool = False,
    allow_real_bench: bool = False,
) -> CalibrationService:
    return _create_service_for_runtime_profile(
        raw_cfg,
        runtime_cfg,
        runtime_profile=BENCH_RUNTIME_PROFILE,
        point_filter=point_filter,
        preload_points=preload_points,
        allow_real_bench=allow_real_bench,
    )


def _build_service_for_runtime_profile(
    raw_cfg: dict[str, Any],
    runtime_cfg: AppConfig,
    *,
    runtime_profile: str,
    point_filter: Optional[PointFilter] = None,
    preload_points: bool = False,
    allow_real_bench: bool = False,
) -> CalibrationService:
    if runtime_profile == MAINLINE_RUNTIME_PROFILE:
        return _create_mainline_service(
            raw_cfg,
            runtime_cfg,
            point_filter=point_filter,
            preload_points=preload_points,
        )
    if runtime_profile == BENCH_RUNTIME_PROFILE:
        return _create_bench_service(
            raw_cfg,
            runtime_cfg,
            point_filter=point_filter,
            preload_points=preload_points,
            allow_real_bench=allow_real_bench,
        )
    raise ValueError(f"Unsupported runtime profile: {runtime_profile}")


def _status_to_dict(status: Any) -> dict[str, Any]:
    phase = getattr(getattr(status, "phase", None), "value", getattr(status, "phase", None))
    current_point = getattr(status, "current_point", None)
    return {
        "phase": phase,
        "total_points": getattr(status, "total_points", 0),
        "completed_points": getattr(status, "completed_points", 0),
        "progress": getattr(status, "progress", 0.0),
        "message": getattr(status, "message", ""),
        "elapsed_s": getattr(status, "elapsed_s", 0.0),
        "error": getattr(status, "error", None),
        "current_point": {
            "index": getattr(current_point, "index", None),
            "temperature_c": getattr(current_point, "temperature_c", None),
            "route": getattr(current_point, "route", None),
            "co2_ppm": getattr(current_point, "co2_ppm", None),
            "humidity_pct": getattr(current_point, "humidity_pct", None),
            "pressure_hpa": getattr(current_point, "pressure_hpa", None),
        }
        if current_point is not None
        else None,
    }


def _step2_config_report_sections(raw_cfg: dict[str, Any]) -> dict[str, Any]:
    config_safety = dict(raw_cfg.get("_config_safety") or {})
    if not config_safety:
        return {}
    config_safety_review = build_step2_config_safety_review(config_safety)
    return {
        "config_safety": config_safety,
        "config_safety_review": config_safety_review,
        "cli_safety_lines": build_step2_cli_safety_lines(config_safety_review or config_safety),
    }


def _write_report(name: str, payload: dict[str, Any]) -> Path:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_ROOT / f"{name}_report_{_timestamp()}.json"
    merged_payload = dict(payload)
    config_safety = dict(merged_payload.get("config_safety") or {})
    if config_safety:
        merged_payload["config_safety"] = config_safety
        merged_payload["config_safety_review"] = dict(
            merged_payload.get("config_safety_review") or build_step2_config_safety_review(config_safety)
        )
    merged_payload.update(
        build_user_visible_evidence_boundary(
            evidence_source=merged_payload.get("evidence_source"),
            simulation_mode=merged_payload.get("simulation_mode", True),
            not_real_acceptance_evidence=merged_payload.get("not_real_acceptance_evidence"),
            acceptance_level=merged_payload.get("acceptance_level"),
            promotion_state=merged_payload.get("promotion_state"),
        )
    )
    report_path.write_text(json.dumps(merged_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report_path


def _sync_summary_status(output_files: Iterable[str], status: Any) -> None:
    summary_paths = [Path(path) for path in output_files if str(path).lower().endswith("summary.json")]
    if not summary_paths:
        return

    status_payload = _status_to_dict(status)
    for path in summary_paths:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload.update(
                build_user_visible_evidence_boundary(
                    evidence_source=payload.get("evidence_source"),
                    simulation_mode=True,
                    not_real_acceptance_evidence=payload.get("not_real_acceptance_evidence"),
                    acceptance_level=payload.get("acceptance_level"),
                    promotion_state=payload.get("promotion_state"),
                )
            )
            payload["status"] = {
                "phase": status_payload["phase"],
                "total_points": status_payload["total_points"],
                "completed_points": status_payload["completed_points"],
                "progress": status_payload["progress"],
                "message": status_payload["message"],
                "elapsed_s": status_payload["elapsed_s"],
                "error": status_payload["error"],
            }
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        except Exception:
            continue


def _connection_rows(manager: DeviceManager, open_results: dict[str, bool], health_results: dict[str, bool]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    names = []
    internal = getattr(manager, "_device_info", {})
    if isinstance(internal, dict):
        names.extend(internal.keys())
    for name in names:
        info = manager.get_info(name)
        rows.append(
            {
                "name": name,
                "device_type": getattr(info, "device_type", ""),
                "port": getattr(info, "port", ""),
                "enabled": bool(getattr(info, "enabled", False)),
                "open_ok": bool(open_results.get(name, False)),
                "health_ok": bool(health_results.get(name, False)),
                "status": getattr(getattr(info, "status", None), "value", ""),
                "error_message": getattr(info, "error_message", None),
            }
        )
    return rows


def _load_runtime(*, allow_real_bench: bool = False) -> tuple[dict[str, Any], AppConfig]:
    _, raw_cfg, _runtime_cfg = load_config_bundle(
        str(CONFIG_PATH),
        simulation_mode=not allow_real_bench,
    )
    portable_raw_cfg = _normalize_portable_raw_config(raw_cfg)
    runtime_cfg = _build_runtime_config(portable_raw_cfg)
    if not allow_real_bench and not bool(runtime_cfg.features.simulation_mode):
        raise RuntimeError(
            "test_v2_device default workflow must stay simulation-only during Step 2."
        )
    return portable_raw_cfg, runtime_cfg


def test_device_connection(
    *,
    runtime_profile: str = MAINLINE_RUNTIME_PROFILE,
    allow_real_bench: bool = False,
) -> bool:
    use_real_bench = runtime_profile == BENCH_RUNTIME_PROFILE and allow_real_bench
    raw_cfg, runtime_cfg = _load_runtime(allow_real_bench=use_real_bench)
    config_sections = _step2_config_report_sections(raw_cfg)
    effective_paths = _print_effective_paths(runtime_cfg)
    for line in list(config_sections.get("cli_safety_lines") or []):
        text = str(line or "").strip()
        if text:
            _print(text)
    service = _build_service_for_runtime_profile(
        raw_cfg,
        runtime_cfg,
        runtime_profile=runtime_profile,
        allow_real_bench=allow_real_bench,
    )
    create_devices = getattr(getattr(service, "orchestrator", None), "_create_devices", None)
    if callable(create_devices):
        create_devices()
    manager = service.device_manager
    open_results: dict[str, bool] = {}
    health_results: dict[str, bool] = {}
    success = True

    try:
        open_results = manager.open_all()
        _configure_opened_gas_analyzers(service, _print)
        health_results = manager.health_check()
        rows = _connection_rows(manager, open_results, health_results)
        success = all((not row["enabled"]) or (row["open_ok"] and row["health_ok"]) for row in rows)

        for row in rows:
            _print(
                f"{row['name']}: open={row['open_ok']} health={row['health_ok']} "
                f"status={row['status']} port={row['port']} error={row['error_message']}"
            )

        report_path = _write_report(
            "connection",
            {
                "test_type": "connection",
                "config_path": str(CONFIG_PATH),
                "points_path": runtime_cfg.paths.points_excel,
                "output_dir": runtime_cfg.paths.output_dir,
                "effective_paths": effective_paths,
                "simulation_mode": bool(runtime_cfg.features.simulation_mode),
                "success": success,
                "devices": rows,
                **config_sections,
            },
        )
        _print(f"Connection report: {report_path}")
        return success
    except Exception:
        report_path = _write_report(
            "connection",
            {
                "test_type": "connection",
                "config_path": str(CONFIG_PATH),
                "simulation_mode": bool(runtime_cfg.features.simulation_mode),
                "success": False,
                "error": traceback.format_exc(),
                **config_sections,
            },
        )
        _print(f"Connection test failed, report: {report_path}")
        raise
    finally:
        manager.close_all()


def _run_calibration_test(
    name: str,
    point_filter: Optional[PointFilter],
    *,
    runtime_profile: str = MAINLINE_RUNTIME_PROFILE,
    allow_real_bench: bool = False,
) -> bool:
    use_real_bench = runtime_profile == BENCH_RUNTIME_PROFILE and allow_real_bench
    raw_cfg, runtime_cfg = _load_runtime(allow_real_bench=use_real_bench)
    config_sections = _step2_config_report_sections(raw_cfg)
    effective_paths = _print_effective_paths(runtime_cfg)
    for line in list(config_sections.get("cli_safety_lines") or []):
        text = str(line or "").strip()
        if text:
            _print(text)
    service = _build_service_for_runtime_profile(
        raw_cfg,
        runtime_cfg,
        runtime_profile=runtime_profile,
        point_filter=point_filter,
        preload_points=True,
        allow_real_bench=allow_real_bench,
    )
    logs: list[str] = []

    def on_log(message: str) -> None:
        logs.append(message)
        _print(f"[log] {message}")

    service.set_log_callback(on_log)
    report_path: Optional[Path] = None

    try:
        loaded = service.get_status().total_points
        _print(f"Loaded points: {loaded}")
        service.start()
        service.wait()
        status = service.get_status()
        outputs = service.get_output_files()
        results = service.get_results()
        _sync_summary_status(outputs, status)
        success = getattr(status.phase, "value", "") == "completed" and not status.error

        report_path = _write_report(
            name,
            {
                "test_type": name,
                "config_path": str(CONFIG_PATH),
                "points_path": runtime_cfg.paths.points_excel,
                "output_dir": runtime_cfg.paths.output_dir,
                "effective_paths": effective_paths,
                "simulation_mode": bool(runtime_cfg.features.simulation_mode),
                "success": success,
                "status": _status_to_dict(status),
                "result_count": len(results),
                "output_files": outputs,
                "log_tail": logs[-50:],
                **config_sections,
            },
        )

        _print(f"Final phase: {status.phase.value}")
        _print(f"Result count: {len(results)}")
        if outputs:
            _print("Output files:")
            for item in outputs:
                _print(f"  {item}")
        _print(f"Test report: {report_path}")
        return success
    except Exception:
        report_path = _write_report(
            name,
            {
                "test_type": name,
                "config_path": str(CONFIG_PATH),
                "points_path": runtime_cfg.paths.points_excel,
                "output_dir": runtime_cfg.paths.output_dir,
                "effective_paths": effective_paths,
                "simulation_mode": bool(runtime_cfg.features.simulation_mode),
                "success": False,
                "error": traceback.format_exc(),
                "log_tail": logs[-50:],
                **config_sections,
            },
        )
        _print(f"{name} failed, report: {report_path}")
        raise
    finally:
        if service.is_running:
            try:
                service.stop(wait=True)
            except Exception:
                pass
        else:
            try:
                service.device_manager.close_all()
            except Exception:
                pass


def test_single_point(
    *,
    runtime_profile: str = MAINLINE_RUNTIME_PROFILE,
    allow_real_bench: bool = False,
) -> bool:
    return _run_calibration_test(
        "single",
        PointFilter(max_points=1),
        runtime_profile=runtime_profile,
        allow_real_bench=allow_real_bench,
    )


def test_full_calibration(
    *,
    runtime_profile: str = MAINLINE_RUNTIME_PROFILE,
    allow_real_bench: bool = False,
) -> bool:
    return _run_calibration_test(
        "full",
        None,
        runtime_profile=runtime_profile,
        allow_real_bench=allow_real_bench,
    )


def _main(argv: list[str]) -> int:
    runtime_profile = MAINLINE_RUNTIME_PROFILE
    allow_real_bench = False
    test_type: Optional[str] = None

    for arg in argv[1:]:
        if arg == "--bench":
            runtime_profile = BENCH_RUNTIME_PROFILE
            continue
        if arg == REAL_BENCH_UNLOCK_FLAG:
            allow_real_bench = True
            continue
        if arg in {"connection", "single", "full"} and test_type is None:
            test_type = arg
            continue
        _print(USAGE)
        return 1

    if runtime_profile == BENCH_RUNTIME_PROFILE:
        try:
            _assert_real_bench_unlocked(allow_real_bench=allow_real_bench)
        except RealBenchLockedError as exc:
            _print(str(exc))
            return 2

    if test_type == "connection":
        return 0 if test_device_connection(runtime_profile=runtime_profile, allow_real_bench=allow_real_bench) else 1
    if test_type == "single":
        return 0 if test_single_point(runtime_profile=runtime_profile, allow_real_bench=allow_real_bench) else 1
    if test_type == "full":
        return 0 if test_full_calibration(runtime_profile=runtime_profile, allow_real_bench=allow_real_bench) else 1

    _print(USAGE)
    return 1


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))
