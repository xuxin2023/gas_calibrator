"""Offline sidecar watcher for completed V1/V2 run directories.

This module is intentionally sidecar-only. It scans existing ``run_*``
directories, waits until they become stable, and then invokes the offline
V2 postprocess runner. It never touches live device control and never enables
coefficient download automatically.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import importlib
import json
from pathlib import Path
import time
from typing import Any, Dict, Iterable

SIDECAR_SCHEMA_VERSION = "1.0"
SIDECAR_STATUS_FILENAME = "sidecar_status.json"
TERMINAL_PHASES = frozenset({"completed", "failed", "aborted"})
SOURCE_IGNORED_FILES = frozenset(
    {
        SIDECAR_STATUS_FILENAME,
        "manifest.json",
        "qc_report.json",
        "qc_report.csv",
        "ai_run_summary.md",
        "ai_anomaly_note.md",
        "calibration_coefficients_postprocess_summary.json",
        "calibration_coefficients.xlsx",
    }
)
SOURCE_IGNORED_DIRS = frozenset({"analytics", "measurement_analytics", "offline_refit", "__pycache__"})


def _log(message: str) -> None:
    print(message, flush=True)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _status_path(run_dir: Path) -> Path:
    return run_dir / SIDECAR_STATUS_FILENAME


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _iter_source_files(run_dir: Path) -> list[Path]:
    files: list[Path] = []
    for path in run_dir.rglob("*"):
        if not path.is_file():
            continue
        relative_parts = path.relative_to(run_dir).parts
        if any(part in SOURCE_IGNORED_DIRS for part in relative_parts[:-1]):
            continue
        if relative_parts[-1] in SOURCE_IGNORED_FILES:
            continue
        files.append(path)
    return sorted(files)


def compute_artifact_fingerprint(run_dir: Path) -> dict[str, Any]:
    source_files = _iter_source_files(run_dir)
    entries: list[dict[str, Any]] = []
    latest_mtime_ns = 0
    total_size = 0
    for path in source_files:
        stat = path.stat()
        latest_mtime_ns = max(latest_mtime_ns, int(stat.st_mtime_ns))
        total_size += int(stat.st_size)
        entries.append(
            {
                "path": path.relative_to(run_dir).as_posix(),
                "size": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
            }
        )
    return {
        "source_file_count": len(entries),
        "total_size": total_size,
        "latest_source_mtime_ns": latest_mtime_ns,
        "files": entries,
    }


def _is_terminal_phase(summary_payload: dict[str, Any]) -> tuple[bool, str]:
    status = summary_payload.get("status") or {}
    phase = str(status.get("phase") or "").strip().lower()
    progress = status.get("progress")
    error = status.get("error")

    if phase in TERMINAL_PHASES:
        return True, phase
    if phase == "finalizing" and (error not in (None, "", "null", "None") or progress == 1 or progress == 1.0):
        return True, phase
    if not phase:
        return True, phase
    return False, phase


def _evaluate_run_directory(run_dir: Path, *, stable_seconds: float, now_ts: float | None = None) -> dict[str, Any]:
    summary_path = run_dir / "summary.json"
    if not summary_path.exists():
        return {"ready": False, "reason": "summary.json missing"}

    summary_payload = _load_json(summary_path)
    if not summary_payload:
        return {"ready": False, "reason": "summary.json unreadable"}

    terminal, phase = _is_terminal_phase(summary_payload)
    if not terminal:
        return {"ready": False, "reason": f"summary phase not terminal: {phase or 'unknown'}"}

    fingerprint = compute_artifact_fingerprint(run_dir)
    latest_source_mtime_ns = int(fingerprint.get("latest_source_mtime_ns") or 0)
    if latest_source_mtime_ns <= 0:
        return {"ready": False, "reason": "no source files found"}

    current_ts = float(now_ts if now_ts is not None else time.time())
    stable_age_s = current_ts - (latest_source_mtime_ns / 1_000_000_000)
    if stable_age_s < float(stable_seconds):
        return {
            "ready": False,
            "reason": f"run directory still changing ({stable_age_s:.1f}s < {float(stable_seconds):.1f}s)",
            "stable_age_s": stable_age_s,
        }

    return {
        "ready": True,
        "summary": summary_payload,
        "phase": phase,
        "fingerprint": fingerprint,
        "stable_age_s": stable_age_s,
    }


def _load_sidecar_status(run_dir: Path) -> dict[str, Any]:
    return _load_json(_status_path(run_dir))


def _write_sidecar_status(run_dir: Path, payload: dict[str, Any]) -> None:
    _write_json(_status_path(run_dir), payload)


def _should_skip_processed(
    *,
    run_dir: Path,
    fingerprint: dict[str, Any],
    force: bool,
) -> tuple[bool, str]:
    if force:
        return False, ""
    status_payload = _load_sidecar_status(run_dir)
    previous_status = str(status_payload.get("status") or "").strip().lower()
    if previous_status not in {"completed", "failed", "running"}:
        return False, ""
    if status_payload.get("artifact_fingerprint") != fingerprint:
        return False, ""
    return True, f"already processed with status={previous_status}"


def _runner_options(*, import_db: bool, skip_ai: bool) -> dict[str, Any]:
    return {
        "import_db": bool(import_db),
        "skip_ai": bool(skip_ai),
        "download": False,
    }


def _invoke_postprocess(*, run_dir: Path, import_db: bool, skip_ai: bool) -> dict[str, Any]:
    module = importlib.import_module("gas_calibrator.v2.adapters.v1_postprocess_runner")
    return module.run_from_cli(
        run_dir=str(run_dir),
        import_db=bool(import_db),
        skip_ai=bool(skip_ai),
        download=False,
    )


def process_run_directory(
    run_dir: Path,
    *,
    stable_seconds: float,
    import_db: bool = False,
    skip_ai: bool = False,
    force: bool = False,
    now_ts: float | None = None,
) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    evaluation = _evaluate_run_directory(run_dir, stable_seconds=stable_seconds, now_ts=now_ts)
    if not evaluation.get("ready"):
        return {
            "run_dir": str(run_dir),
            "status": "skipped",
            "reason": evaluation.get("reason", "run not ready"),
        }

    fingerprint = evaluation["fingerprint"]
    should_skip, reason = _should_skip_processed(run_dir=run_dir, fingerprint=fingerprint, force=force)
    if should_skip:
        return {
            "run_dir": str(run_dir),
            "status": "skipped",
            "reason": reason,
            "artifact_fingerprint": fingerprint,
        }

    summary_payload = evaluation["summary"]
    run_id = str(summary_payload.get("run_id") or run_dir.name)
    running_status = {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "status": "running",
        "started_at": _now_iso(),
        "finished_at": None,
        "phase": evaluation.get("phase"),
        "stable_seconds": float(stable_seconds),
        "stable_age_s": float(evaluation.get("stable_age_s") or 0.0),
        "artifact_fingerprint": fingerprint,
        "runner_options": _runner_options(import_db=import_db, skip_ai=skip_ai),
        "postprocess_summary": None,
        "result": None,
        "error": None,
    }
    _write_sidecar_status(run_dir, running_status)

    try:
        result_payload = _invoke_postprocess(run_dir=run_dir, import_db=import_db, skip_ai=skip_ai)
        completed_status = dict(running_status)
        completed_status.update(
            {
                "status": "completed",
                "finished_at": _now_iso(),
                "postprocess_summary": result_payload.get("summary"),
                "result": result_payload,
            }
        )
        _write_sidecar_status(run_dir, completed_status)
        return {
            "run_dir": str(run_dir),
            "run_id": run_id,
            "status": "completed",
            "postprocess_summary": result_payload.get("summary"),
            "result": result_payload,
            "artifact_fingerprint": fingerprint,
        }
    except Exception as exc:
        failed_status = dict(running_status)
        failed_status.update(
            {
                "status": "failed",
                "finished_at": _now_iso(),
                "error": str(exc),
            }
        )
        _write_sidecar_status(run_dir, failed_status)
        return {
            "run_dir": str(run_dir),
            "run_id": run_id,
            "status": "failed",
            "error": str(exc),
            "artifact_fingerprint": fingerprint,
        }


def iter_run_directories(root: Path) -> list[Path]:
    root = root.resolve()
    candidates = [path for path in root.rglob("run_*") if path.is_dir()]
    return sorted(candidates, key=lambda path: path.stat().st_mtime)


def run_sidecar_once(
    *,
    root: str | Path,
    stable_seconds: float,
    import_db: bool = False,
    skip_ai: bool = False,
    force: bool = False,
    now_ts: float | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    if not root_path.exists():
        raise FileNotFoundError(f"sidecar root does not exist: {root_path}")
    if not root_path.is_dir():
        raise NotADirectoryError(f"sidecar root must be a directory: {root_path}")

    results: list[dict[str, Any]] = []
    for run_dir in iter_run_directories(root_path):
        result = process_run_directory(
            run_dir,
            stable_seconds=stable_seconds,
            import_db=import_db,
            skip_ai=skip_ai,
            force=force,
            now_ts=now_ts,
        )
        results.append(result)

    return {
        "root": str(root_path),
        "stable_seconds": float(stable_seconds),
        "import_db": bool(import_db),
        "skip_ai": bool(skip_ai),
        "force": bool(force),
        "processed": [item for item in results if item.get("status") == "completed"],
        "failed": [item for item in results if item.get("status") == "failed"],
        "skipped": [item for item in results if item.get("status") == "skipped"],
        "results": results,
    }


def watch_sidecar(
    *,
    root: str | Path,
    stable_seconds: float,
    import_db: bool = False,
    skip_ai: bool = False,
    force: bool = False,
    poll_s: float = 30.0,
) -> None:
    while True:
        summary = run_sidecar_once(
            root=root,
            stable_seconds=stable_seconds,
            import_db=import_db,
            skip_ai=skip_ai,
            force=force,
        )
        _log(
            "sidecar scan finished: "
            f"processed={len(summary['processed'])}, failed={len(summary['failed'])}, skipped={len(summary['skipped'])}"
        )
        time.sleep(float(poll_s))


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safe sidecar watcher for offline V2 postprocess")
    parser.add_argument("--root", required=True, help="Root directory containing run_* outputs")
    parser.add_argument("--stable-seconds", type=float, default=30.0, help="Minimum quiet time before processing a run")
    parser.add_argument("--import-db", action="store_true", help="Enable offline database import during postprocess")
    parser.add_argument("--skip-ai", action="store_true", help="Disable AI explanation and summary steps")
    parser.add_argument("--watch", action="store_true", help="Continuously watch for new stable run directories")
    parser.add_argument("--poll-s", type=float, default=30.0, help="Polling interval when --watch is enabled")
    parser.add_argument("--force", action="store_true", help="Reprocess runs even if sidecar_status.json already matches")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.watch:
        watch_sidecar(
            root=args.root,
            stable_seconds=args.stable_seconds,
            import_db=bool(args.import_db),
            skip_ai=bool(args.skip_ai),
            force=bool(args.force),
            poll_s=args.poll_s,
        )
        return 0

    summary = run_sidecar_once(
        root=args.root,
        stable_seconds=args.stable_seconds,
        import_db=bool(args.import_db),
        skip_ai=bool(args.skip_ai),
        force=bool(args.force),
    )
    _log(
        "sidecar scan finished: "
        f"processed={len(summary['processed'])}, failed={len(summary['failed'])}, skipped={len(summary['skipped'])}"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
