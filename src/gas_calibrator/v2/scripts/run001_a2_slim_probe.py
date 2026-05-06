from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Optional


def _bootstrap_src_path_for_direct_script() -> None:
    src_root = Path(__file__).resolve().parents[3]
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)


_bootstrap_src_path_for_direct_script()


def _load_json_mapping(path: str) -> dict[str, Any]:
    raw = Path(path).read_text(encoding="utf-8")
    return json.loads(raw)


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_branch_and_head(branch: str, head: str) -> tuple[str, str]:
    repo_root = Path(__file__).resolve().parents[4]
    resolved_branch = branch
    resolved_head = head
    if not resolved_branch or not resolved_head:
        try:
            import subprocess
            if not resolved_branch:
                result = subprocess.run(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                    cwd=str(repo_root), text=True, capture_output=True, check=False,
                )
                resolved_branch = result.stdout.strip() if result.returncode == 0 else ""
            if not resolved_head:
                result = subprocess.run(
                    ["git", "rev-parse", "HEAD"],
                    cwd=str(repo_root), text=True, capture_output=True, check=False,
                )
                resolved_head = result.stdout.strip()[:8] if result.returncode == 0 else ""
        except Exception:
            pass
    return resolved_branch, resolved_head


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="A2 Slim Probe — minimal gate-free multi-temp/multi-ppm/multi-pressure verification"
    )
    parser.add_argument("--config", required=True, help="Slim probe config JSON")
    parser.add_argument("--branch", default="", help="Expected branch (optional)")
    parser.add_argument("--head", default="", help="Expected HEAD (optional)")
    parser.add_argument(
        "--execute-probe",
        action="store_true",
        help="Execute the slim probe",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)

    if not args.execute_probe:
        print(json.dumps({"final_decision": "FAIL_CLOSED", "fail_reason": "missing_execute_probe_flag"}, ensure_ascii=False, indent=2))
        return 2

    config_path = str(Path(args.config).resolve())
    branch, head = _resolve_branch_and_head(args.branch, args.head)

    print(f"[A2 Slim] config={config_path}", flush=True)
    if branch:
        print(f"[A2 Slim] branch={branch}", flush=True)
    if head:
        print(f"[A2 Slim] head={head}", flush=True)

    from gas_calibrator.v2.entry import create_calibration_service_from_config, load_config_bundle

    resolved_config_path, raw_cfg, config = load_config_bundle(
        config_path,
        simulation_mode=False,
        allow_unsafe_step2_config=True,
        enforce_step2_execution_gate=False,
    )

    service = create_calibration_service_from_config(
        config,
        raw_cfg=raw_cfg,
        preload_points=True,
        require_no_write_guard=True,
    )

    timeout_s = float(raw_cfg.get("max_runtime_s", 3600.0))

    _original_sigint = signal.getsignal(signal.SIGINT)

    def _on_interrupt(signum, frame):
        print("\n[A2 Slim] Interrupted, stopping...", flush=True)
    signal.signal(signal.SIGINT, _on_interrupt)

    started = time.time()
    last_status = ""
    last_report = 0.0
    try:
        service.start()
        while True:
            elapsed = time.time() - started
            if service._done_event.wait(timeout=30.0):
                break
            if elapsed >= timeout_s:
                print(f"\n[A2 Slim] Timeout {timeout_s:.0f}s, stopping...", flush=True)
                service.stop(wait=True, timeout=30.0)
                break
            status = service.get_status()
            if status is not None:
                phase_val = getattr(status.phase, "value", str(status.phase or "unknown"))
                phase_text = str(phase_val or "unknown")
                msg = str(status.message or "")[:150]
                status_text = f"phase={phase_text}"
                if msg:
                    status_text += f" msg={msg}"
                if status_text != last_status:
                    print(f"[A2 Slim] {status_text}", flush=True)
                    last_status = status_text
                    last_report = elapsed
                elif elapsed - last_report >= 120.0:
                    print(f"[A2 Slim] still running {elapsed:.0f}s: {status_text}", flush=True)
                    last_report = elapsed
    except KeyboardInterrupt:
        print("\n[A2 Slim] KeyboardInterrupt, waiting for safe shutdown...", flush=True)
    finally:
        signal.signal(signal.SIGINT, _original_sigint)
        try:
            service.stop(wait=True, timeout=30.0)
        except Exception:
            pass

    run_dir = service.session.output_dir
    summary = _load_json_dict(run_dir / "summary.json")

    final_decision = str(summary.get("final_decision") or "FAIL_CLOSED")
    fail_reason = str(summary.get("fail_reason") or summary.get("failure_reason") or "")
    points_completed = summary.get("points_completed", 0)
    sample_count = summary.get("sample_count", 0)
    route_completed = summary.get("route_completed", False)
    pressure_completed = summary.get("pressure_completed", False)
    sample_completed = summary.get("sample_completed", False)
    pressure_points = summary.get("pressure_points", [])

    skipped_indices_raw = summary.get("skipped_point_indices") or summary.get("skipped_points") or []
    skipped_indices = list(skipped_indices_raw) if isinstance(skipped_indices_raw, list) else []

    result = {
        "final_decision": final_decision,
        "fail_reason": fail_reason,
        "points_completed": points_completed,
        "sample_count_total": sample_count,
        "route_completed": route_completed,
        "pressure_completed": pressure_completed,
        "sample_completed": sample_completed,
        "pressure_points": pressure_points,
        "skipped_point_indices": skipped_indices,
        "run_dir": str(run_dir),
        "branch": branch,
        "head": head,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    return 0 if final_decision == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
