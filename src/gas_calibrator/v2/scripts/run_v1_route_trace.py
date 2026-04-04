from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

from ...config import load_config
from ...logging_utils import RunLogger
from ...workflow import runner as runner_mod
from ..adapters.v1_route_trace import TracedCalibrationRunner


PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_V1_CONFIG = PROJECT_ROOT / "configs" / "default_config.json"


def _log(message: str) -> None:
    print(message, flush=True)


def run_self_test(*args, **kwargs):
    from ...diagnostics import run_self_test as _run_self_test

    return _run_self_test(*args, **kwargs)


def _build_devices(*args, **kwargs):
    from ...tools.run_headless import _build_devices as _runtime_build_devices

    return _runtime_build_devices(*args, **kwargs)


def _close_devices(*args, **kwargs):
    from ...tools.run_headless import _close_devices as _runtime_close_devices

    return _runtime_close_devices(*args, **kwargs)


def _enabled_failures(*args, **kwargs):
    from ...tools.run_headless import _enabled_failures as _runtime_enabled_failures

    return _runtime_enabled_failures(*args, **kwargs)


def _classify_v1_failure(message: Any) -> str:
    text = str(message or "").strip().lower()
    if not text:
        return "startup.failure"
    if "permission denied" in text or "access denied" in text or "拒绝访问" in text:
        return "startup.device_connection.port_busy"
    if "could not open port" in text or "port is busy" in text:
        return "startup.device_connection.port_busy"
    if "self-test failed" in text or "device connection" in text:
        return "startup.device_connection"
    if "point filter" in text or "no calibration points" in text:
        return "input_validation.points_filter"
    if "stream verify not full mode2" in text or "mode2" in text and "verify" in text:
        return "startup.sensor_precheck.mode2_verify"
    if "sensor precheck" in text or "传感器预检查" in text:
        return "startup.sensor_precheck"
    return "startup.failure"


def _latest_io_log_path(run_dir: Path) -> Optional[Path]:
    candidates = sorted(run_dir.glob("io_*.csv"), key=lambda item: item.stat().st_mtime)
    if not candidates:
        return None
    return candidates[-1]


def _runner_payload_text(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        loaded = json.loads(text)
    except Exception:
        return text
    if isinstance(loaded, dict):
        current = str(loaded.get("current") or "").strip()
        wait_reason = str(loaded.get("wait_reason") or "").strip()
        parts = [part for part in (current, wait_reason) if part]
        return " / ".join(parts) if parts else text
    return text


def _derive_runner_failure(run_dir: Path) -> dict[str, Any]:
    io_log_path = _latest_io_log_path(run_dir)
    if io_log_path is None or not io_log_path.exists():
        return {}

    last_stage = ""
    last_event = ""
    abort_message = ""
    trace_expected_but_missing = False
    saw_run_event = False
    try:
        with io_log_path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if str(row.get("port") or "").strip() != "RUN":
                    continue
                if str(row.get("device") or "").strip() != "runner":
                    continue
                saw_run_event = True
                event_name = str(row.get("command") or "").strip()
                response_text = _runner_payload_text(str(row.get("response") or ""))
                error_text = str(row.get("error") or "").strip()
                if event_name == "stage" and response_text:
                    last_stage = response_text
                if event_name:
                    last_event = event_name
                if event_name == "run-aborted":
                    abort_message = error_text or response_text
                    trace_expected_but_missing = True
    except Exception:
        return {}

    derived_failure_phase = ""
    abort_text = str(abort_message or "").strip()
    stage_text = str(last_stage or "").strip()
    if abort_text:
        derived_failure_phase = _classify_v1_failure(abort_text)
    elif "传感器预检查" in stage_text or "sensor precheck" in stage_text.lower():
        derived_failure_phase = "startup.sensor_precheck"
    elif saw_run_event:
        derived_failure_phase = "startup.failure"

    return {
        "io_log_path": str(io_log_path),
        "last_runner_stage": last_stage or None,
        "last_runner_event": last_event or None,
        "abort_message": abort_text or None,
        "derived_failure_phase": derived_failure_phase or None,
        "trace_expected_but_missing": bool(trace_expected_but_missing),
    }


def _write_status(
    logger: RunLogger,
    *,
    ok: bool,
    status_phase: str,
    status_error: str = "",
    error_category: Optional[str] = None,
    trace_path: Optional[Path] = None,
    last_runner_stage: Optional[str] = None,
    last_runner_event: Optional[str] = None,
    abort_message: Optional[str] = None,
    derived_failure_phase: Optional[str] = None,
    trace_expected_but_missing: Optional[bool] = None,
    details: Optional[dict[str, Any]] = None,
) -> None:
    payload = {
        "ok": bool(ok),
        "status_phase": str(status_phase or "").strip() or None,
        "status_error": str(status_error or "").strip() or None,
        "error_category": str(error_category or "").strip() or None,
        "trace_path": str(trace_path) if trace_path is not None else "",
        "last_runner_stage": str(last_runner_stage or "").strip() or None,
        "last_runner_event": str(last_runner_event or "").strip() or None,
        "abort_message": str(abort_message or "").strip() or None,
        "derived_failure_phase": str(derived_failure_phase or "").strip() or None,
        "trace_expected_but_missing": trace_expected_but_missing if trace_expected_but_missing is not None else None,
        "details": dict(details or {}),
    }
    path = logger.run_dir / "route_trace_status.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run V1 workflow with external route trace harness.")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_V1_CONFIG),
        help="Path to V1 config json.",
    )
    parser.add_argument("--temp", type=float, default=None, help="Only run points matching this chamber temperature.")
    parser.add_argument("--run-id", default=None, help="Optional fixed run_id folder name under logs/")
    parser.add_argument(
        "--skip-connect-check",
        action="store_true",
        help="Skip startup connectivity self-test gate.",
    )
    parser.add_argument(
        "--skip-h2o",
        action="store_true",
        help="Skip H2O route and start directly from CO2 route for the selected temperature group.",
    )
    parser.add_argument(
        "--h2o-only",
        action="store_true",
        help="Keep only H2O points for the selected temperature group and skip gas routes.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.config)
    logger = RunLogger(Path(cfg["paths"]["output_dir"]), run_id=args.run_id)
    _log(f"Run folder: {logger.run_dir}")

    patched_loader = False
    original_loader = runner_mod.load_points_from_excel
    devices = {}

    try:
        if args.skip_h2o:
            cfg.setdefault("workflow", {})["skip_h2o"] = True
            _log("Workflow override: skip_h2o=True")

        if args.temp is not None or args.h2o_only:
            target = None if args.temp is None else float(args.temp)

            def _filtered_loader(path: str, missing_pressure_policy: str = "require", **kwargs):
                points = original_loader(path, missing_pressure_policy=missing_pressure_policy, **kwargs)
                filtered = list(points)
                if target is not None:
                    filtered = [
                        point
                        for point in filtered
                        if point.temp_chamber_c is not None and abs(float(point.temp_chamber_c) - target) < 1e-9
                    ]
                    _log(f"Point filter: temp={target:g}C -> {len(filtered)}/{len(points)} points")
                if args.h2o_only:
                    before_h2o_only = len(filtered)
                    filtered = [point for point in filtered if bool(getattr(point, "is_h2o_point", False))]
                    _log(f"Point filter: h2o_only -> {len(filtered)}/{before_h2o_only} points")
                return filtered

            runner_mod.load_points_from_excel = _filtered_loader
            patched_loader = True

        if not args.skip_connect_check:
            self_test = run_self_test(cfg, io_logger=logger)
            failures = _enabled_failures(cfg, self_test)
            if failures:
                for name, err in failures:
                    _log(f"Self-test failed: {name}: {err}")
                _write_status(
                    logger,
                    ok=False,
                    status_phase="startup.device_connection",
                    status_error="; ".join(f"{name}: {err}" for name, err in failures),
                    error_category=_classify_v1_failure("; ".join(f"{name}: {err}" for name, err in failures)),
                    details={"failures": [{"name": name, "error": str(err)} for name, err in failures]},
                )
                return 2

        devices = _build_devices(cfg, io_logger=logger)
        runner = TracedCalibrationRunner(cfg, devices, logger, _log, _log)
        runner.run()
        _log(f"Route trace: {runner.route_trace_path}")
        trace_exists = Path(runner.route_trace_path).exists()
        derived = _derive_runner_failure(logger.run_dir)
        derived_failure_phase = str(derived.get("derived_failure_phase") or "").strip()
        _write_status(
            logger,
            ok=trace_exists,
            status_phase="completed" if trace_exists else "output.route_trace_missing",
            status_error="" if trace_exists else "route trace file was not produced",
            error_category=None if trace_exists else (derived_failure_phase or "output.route_trace_missing"),
            trace_path=Path(runner.route_trace_path),
            last_runner_stage=derived.get("last_runner_stage"),
            last_runner_event=derived.get("last_runner_event"),
            abort_message=derived.get("abort_message"),
            derived_failure_phase=derived_failure_phase,
            trace_expected_but_missing=bool(derived.get("trace_expected_but_missing", False)) if not trace_exists else False,
            details={"io_log_path": derived.get("io_log_path")} if derived.get("io_log_path") else None,
        )
        return 0 if trace_exists else 1
    except Exception as exc:
        _log(f"ERROR: {exc}")
        derived = _derive_runner_failure(logger.run_dir)
        derived_failure_phase = str(derived.get("derived_failure_phase") or "").strip()
        _write_status(
            logger,
            ok=False,
            status_phase=derived_failure_phase or _classify_v1_failure(exc),
            status_error=str(exc),
            error_category=derived_failure_phase or _classify_v1_failure(exc),
            last_runner_stage=derived.get("last_runner_stage"),
            last_runner_event=derived.get("last_runner_event"),
            abort_message=derived.get("abort_message"),
            derived_failure_phase=derived_failure_phase,
            trace_expected_but_missing=derived.get("trace_expected_but_missing"),
            details={"io_log_path": derived.get("io_log_path")} if derived.get("io_log_path") else None,
        )
        return 1
    finally:
        if patched_loader:
            runner_mod.load_points_from_excel = original_loader
        _close_devices(devices)
        try:
            logger.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
