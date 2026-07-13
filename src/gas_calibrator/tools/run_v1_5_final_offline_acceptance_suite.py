"""Run the allowlisted V1.5 final offline acceptance suite."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from ..validation.v1_5_final_offline_acceptance_suite import (
    PASS_STATUS,
    SUITE_TEST_FILES,
    write_v1_5_final_offline_acceptance_suite,
)


SubprocessRunner = Callable[..., subprocess.CompletedProcess[str]]


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the no-COM/no-write/no-database V1.5 final offline acceptance suite."
    )
    parser.add_argument("--repository-root", default=str(Path.cwd()))
    parser.add_argument("--source-origin-main-commit", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timeout-s", type=float, default=1800.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def run_v1_5_final_offline_acceptance_suite(
    *,
    repository_root: str | Path,
    source_origin_main_commit: str,
    output_dir: str | Path,
    timeout_s: float = 1800.0,
    subprocess_runner: SubprocessRunner = subprocess.run,
) -> dict[str, Any]:
    repo_root = Path(repository_root).resolve()
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    missing = [relative for relative in SUITE_TEST_FILES if not (repo_root / relative).is_file()]
    if missing:
        raise ValueError(f"Allowlisted tests missing: {', '.join(missing)}")
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-p",
        "no:cacheprovider",
        *SUITE_TEST_FILES,
    ]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(repo_root / "src")
    environment["PYTHONIOENCODING"] = "utf-8"
    started = time.monotonic()
    result = subprocess_runner(
        command,
        cwd=str(repo_root),
        env=environment,
        capture_output=True,
        text=True,
        timeout=max(1.0, float(timeout_s)),
        check=False,
    )
    duration = round(time.monotonic() - started, 3)
    stdout = str(result.stdout or "")
    stderr = str(result.stderr or "")
    stdout_path = destination / "pytest_stdout.txt"
    stderr_path = destination / "pytest_stderr.txt"
    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")
    execution = {
        "executed": True,
        "returncode": int(result.returncode),
        "duration_seconds": duration,
        "command_test_files": list(SUITE_TEST_FILES),
        "command": command,
        "cwd": str(repo_root),
        "stdout_path": str(stdout_path),
        "stdout_sha256": _sha256_text(stdout),
        "stderr_path": str(stderr_path),
        "stderr_sha256": _sha256_text(stderr),
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "not_real_acceptance_evidence": True,
    }
    paths = write_v1_5_final_offline_acceptance_suite(
        output_dir=destination,
        repository_root=repo_root,
        source_origin_main_commit=source_origin_main_commit,
        test_execution=execution,
    )
    model = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    return {"model": model, "paths": {key: str(value) for key, value in paths.items()}}


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        result = run_v1_5_final_offline_acceptance_suite(
            repository_root=args.repository_root,
            source_origin_main_commit=args.source_origin_main_commit,
            output_dir=args.output_dir,
            timeout_s=args.timeout_s,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result["paths"], ensure_ascii=False, indent=2))
    return 0 if result["model"].get("overall_status") == PASS_STATUS else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
