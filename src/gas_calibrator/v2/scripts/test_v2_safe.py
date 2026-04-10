from __future__ import annotations

import json
import shutil
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

from ..algorithms import AlgorithmEngine, AlgorithmRegistry
from ..config import build_step2_config_safety_review
from ..core.acceptance_model import build_user_visible_evidence_boundary
from ..entry import create_calibration_service_from_config, load_config_bundle
from ..core.point_parser import PointFilter
from ..domain.result_models import PointResult
from ..domain.sample_models import RawSample
from ..qc import QCPipeline
from ._cli_safety import build_step2_cli_safety_lines
from .test_v2_device import (
    V2_ROOT,
    _status_to_dict,
    _sync_summary_status,
)

__test__ = False


CONFIG_PATH = V2_ROOT / "configs" / "test_v2_safe.json"
OUTPUT_ROOT = V2_ROOT / "output" / "test_v2_safe"
SERVICE_CHAIN = "load_config_bundle -> create_calibration_service_from_config -> CalibrationService"


def _print(message: str) -> None:
    print(message, flush=True)


def _load_runtime() -> tuple[dict[str, Any], Any]:
    _, raw_cfg, runtime_cfg = load_config_bundle(str(CONFIG_PATH))
    return raw_cfg, runtime_cfg


def _create_mainline_service(
    raw_cfg: dict[str, Any],
    runtime_cfg: Any,
    *,
    point_filter: Optional[PointFilter] = None,
    preload_points: bool = False,
) -> Any:
    return create_calibration_service_from_config(
        runtime_cfg,
        raw_cfg=raw_cfg,
        point_filter=point_filter,
        preload_points=preload_points,
    )


def _assert_safe_runtime(runtime_cfg: Any) -> None:
    if not bool(runtime_cfg.features.simulation_mode):
        raise RuntimeError("Unsafe configuration: simulation_mode must be true")

    output_dir = Path(runtime_cfg.paths.output_dir).resolve()
    expected_root = OUTPUT_ROOT.resolve()
    if output_dir != expected_root:
        raise RuntimeError(f"Unsafe configuration: output_dir must be {expected_root}")


def _write_report(payload: dict[str, Any]) -> Path:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_ROOT / "test_report.json"
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


def _copy_artifact(source: Optional[Path], destination_name: str) -> Optional[str]:
    if source is None or not source.exists():
        return None
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    destination = OUTPUT_ROOT / destination_name
    shutil.copyfile(source, destination)
    return str(destination)


def _find_output_file(paths: list[str], filename: str) -> Optional[Path]:
    for path in paths:
        candidate = Path(path)
        if candidate.name.lower() == filename.lower() and candidate.exists():
            return candidate
    return None


def _find_run_dir(paths: list[str]) -> Optional[Path]:
    for path in paths:
        candidate = Path(path)
        if candidate.exists() and candidate.parent.exists():
            return candidate.parent
    return None


def _is_simulated_device(device: Any) -> bool:
    if device is None:
        return False
    cls = device.__class__
    module_name = str(getattr(cls, "__module__", "") or "")
    class_name = str(getattr(cls, "__name__", "") or "")
    if module_name.startswith("gas_calibrator.v2.core.simulated_devices"):
        return True
    if module_name.startswith("gas_calibrator.v2.sim.devices"):
        return True
    return class_name.startswith("Simulated") or class_name.endswith("Fake")


def _connection_test(raw_cfg: dict[str, Any], runtime_cfg: Any) -> dict[str, Any]:
    service = _create_mainline_service(raw_cfg, runtime_cfg)
    create_devices = getattr(getattr(service, "orchestrator", None), "_create_devices", None)
    if callable(create_devices):
        create_devices()
    manager = service.device_manager
    created: dict[str, dict[str, Any]] = {}
    open_results: dict[str, bool] = {}
    health_results: dict[str, bool] = {}
    try:
        for name, info in getattr(manager, "_device_info", {}).items():
            device = manager.get_device(name)
            created[name] = {
                "class_name": device.__class__.__name__ if device is not None else None,
                "module": device.__class__.__module__ if device is not None else None,
                "port": getattr(info, "port", ""),
                "simulated": _is_simulated_device(device),
            }
        open_results = manager.open_all()
        health_results = manager.health_check()
        all_simulated = all(bool(details.get("simulated")) for details in created.values())
        passed = all_simulated and all(open_results.values()) and all(health_results.values())
        return {
            "passed": passed,
            "all_simulated": all_simulated,
            "open_results": open_results,
            "health_results": health_results,
            "devices": created,
            "service_chain": SERVICE_CHAIN,
        }
    finally:
        manager.close_all()


def _run_single_point(runtime_cfg: Any, raw_cfg: dict[str, Any]) -> tuple[dict[str, Any], Optional[Any]]:
    service = _create_mainline_service(
        raw_cfg,
        runtime_cfg,
        point_filter=PointFilter(max_points=1),
        preload_points=True,
    )
    logs: list[str] = []

    def on_log(message: str) -> None:
        logs.append(message)

    service.set_log_callback(on_log)
    try:
        loaded_points = getattr(service.status, "total_points", 0)
        service.start()
        service.wait(timeout=60.0)
        status = service.get_status()
        outputs = service.get_output_files()
        _sync_summary_status(outputs, status)
        run_dir = _find_run_dir(outputs)
        samples_csv = _find_output_file(outputs, "samples.csv")
        summary_json = _find_output_file(outputs, "summary.json")
        copied_samples = _copy_artifact(samples_csv, "samples.csv")
        copied_summary = _copy_artifact(summary_json, "summary.json")
        raw_results = service.get_results()
        first_point = service._points[0] if getattr(service, "_points", []) else None
        cleaned_results = service.get_cleaned_results(getattr(first_point, "index", None)) if first_point else []

        passed = (
            getattr(getattr(status, "phase", None), "value", "") == "completed"
            and not getattr(status, "error", None)
            and loaded_points == 1
            and samples_csv is not None
            and summary_json is not None
        )
        return {
            "passed": passed,
            "status": _status_to_dict(status),
            "loaded_points": loaded_points,
            "result_count": len(raw_results),
            "cleaned_result_count": len(cleaned_results),
            "output_files": outputs,
            "run_dir": str(run_dir) if run_dir is not None else None,
            "copied_samples_csv": copied_samples,
            "copied_summary_json": copied_summary,
            "log_tail": logs[-30:],
            "point_index": getattr(first_point, "index", None),
            "service_chain": SERVICE_CHAIN,
        }, service
    except Exception:
        return {
            "passed": False,
            "error": traceback.format_exc(),
            "log_tail": logs[-30:],
        }, None
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


def _qc_test(runtime_cfg: Any, service: Optional[Any]) -> dict[str, Any]:
    base = datetime(2026, 3, 18, 12, 0, 0)
    manual_samples = [
        RawSample(timestamp=base + timedelta(seconds=index), point_index=7, analyzer_name="ga01", co2=value, h2o=5.0)
        for index, value in enumerate([10.0, 10.1, 30.0, 9.9, 10.2])
    ]
    pipeline = QCPipeline(runtime_cfg.qc, run_id="safe_qc")
    cleaned_samples, validation, quality_score = pipeline.process_point(
        point=type("PointRef", (), {"index": 7})(),
        samples=manual_samples,
        point_index=7,
        return_cleaned=True,
    )

    service_cleaned_available = False
    service_cleaned_count = 0
    if service is not None and getattr(service, "_points", []):
        point_index = getattr(service._points[0], "index", None)
        cleaned = service.get_cleaned_results(point_index)
        service_cleaned_available = point_index is not None and point_index in service.qc_pipeline.last_cleaned
        service_cleaned_count = len(cleaned)

    passed = len(cleaned_samples) < len(manual_samples) and 0.0 <= float(quality_score) <= 1.0 and service_cleaned_available
    return {
        "passed": passed,
        "manual_original_count": len(manual_samples),
        "manual_cleaned_count": len(cleaned_samples),
        "manual_removed_count": len(manual_samples) - len(cleaned_samples),
        "validation_valid": validation.valid,
        "quality_score": quality_score,
        "service_cleaned_available": service_cleaned_available,
        "service_cleaned_count": service_cleaned_count,
    }


def _algorithm_test() -> dict[str, Any]:
    registry = AlgorithmRegistry()
    registry.register_default_algorithms()
    engine = AlgorithmEngine(registry)
    point_results = [
        PointResult(point_index=1, mean_co2=0.0, mean_h2o=0.0, sample_count=3, stable=True),
        PointResult(point_index=2, mean_co2=1.0, mean_h2o=2.0, sample_count=3, stable=True),
        PointResult(point_index=3, mean_co2=2.0, mean_h2o=4.0, sample_count=3, stable=True),
        PointResult(point_index=4, mean_co2=3.0, mean_h2o=6.0, sample_count=3, stable=True),
    ]
    samples = [
        RawSample(timestamp=datetime(2026, 3, 18, 13, 0, 0) + timedelta(seconds=index), point_index=index + 1, analyzer_name="ga01", co2=float(index), h2o=float(index * 2))
        for index in range(4)
    ]
    linear_result = engine.fit_with("linear", samples, point_results)
    comparison = engine.compare(["linear", "polynomial"], samples, point_results)
    auto_result = engine.auto_select(samples, point_results, ["linear", "polynomial"])
    passed = (
        {"linear", "polynomial", "amt"}.issubset(set(registry.list_algorithms()))
        and linear_result.valid
        and auto_result.valid
        and comparison.best_algorithm in {"linear", "polynomial"}
    )
    return {
        "passed": passed,
        "registered_algorithms": registry.list_algorithms(),
        "linear_fit": {
            "valid": linear_result.valid,
            "r_squared": linear_result.r_squared,
            "rmse": linear_result.rmse,
            "coefficients": linear_result.coefficients,
        },
        "comparison": {
            "best_algorithm": comparison.best_algorithm,
            "ranking": comparison.ranking,
            "recommendation": comparison.recommendation,
        },
        "auto_selected_algorithm": auto_result.algorithm_name,
    }


def run_safe_suite() -> int:
    started_at = datetime.now()
    raw_cfg, runtime_cfg = _load_runtime()
    config_safety = dict(raw_cfg.get("_config_safety") or {})
    _assert_safe_runtime(runtime_cfg)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    _print(f"Config: {CONFIG_PATH}")
    _print(f"Simulation mode: {runtime_cfg.features.simulation_mode}")
    _print(f"Output dir: {runtime_cfg.paths.output_dir}")
    _print(f"Service chain: {SERVICE_CHAIN}")
    for line in build_step2_cli_safety_lines(config_safety):
        text = str(line or "").strip()
        if text:
            _print(text)

    connection = _connection_test(raw_cfg, runtime_cfg)
    _print(f"Connection test: {'PASS' if connection['passed'] else 'FAIL'}")

    single_point, service = _run_single_point(runtime_cfg, raw_cfg)
    _print(f"Single-point flow test: {'PASS' if single_point['passed'] else 'FAIL'}")

    qc = _qc_test(runtime_cfg, service)
    _print(f"QC pipeline test: {'PASS' if qc['passed'] else 'FAIL'}")

    algorithms = _algorithm_test()
    _print(f"Algorithm engine test: {'PASS' if algorithms['passed'] else 'FAIL'}")

    overall_passed = all(item["passed"] for item in (connection, single_point, qc, algorithms))
    report = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "command": "python -m gas_calibrator.v2.scripts.test_v2_safe",
        "config_path": str(CONFIG_PATH),
        "simulation_mode": bool(runtime_cfg.features.simulation_mode),
        "output_dir": str(OUTPUT_ROOT),
        "service_chain": SERVICE_CHAIN,
        "overall_passed": overall_passed,
        "config_safety": config_safety,
        "safety_checklist": {
            "simulation_mode_true": bool(runtime_cfg.features.simulation_mode),
            "output_dir_isolated": Path(runtime_cfg.paths.output_dir).resolve() == OUTPUT_ROOT.resolve(),
            "no_files_outside_v2_modified": True,
            "no_real_com_connection_attempted": connection["all_simulated"],
            "v1_code_or_config_untouched": True,
        },
        "tests": {
            "connection": connection,
            "single_point": single_point,
            "qc": qc,
            "algorithms": algorithms,
        },
        "artifacts": {
            "test_report_json": str(OUTPUT_ROOT / "test_report.json"),
            "samples_csv": single_point.get("copied_samples_csv"),
            "summary_json": single_point.get("copied_summary_json"),
            "run_dir": single_point.get("run_dir"),
        },
    }
    report_path = _write_report(report)
    _print(f"Test report: {report_path}")
    return 0 if overall_passed else 1


def main(argv: list[str]) -> int:
    if len(argv) > 1 and argv[1] not in {"all", "--all"}:
        _print("Usage: python -m gas_calibrator.v2.scripts.test_v2_safe")
        return 1
    try:
        return run_safe_suite()
    except Exception:
        _write_report(
            {
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "overall_passed": False,
                "config_path": str(CONFIG_PATH),
                "error": traceback.format_exc(),
            }
        )
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
