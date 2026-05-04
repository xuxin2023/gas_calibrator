from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Optional


def _bootstrap_src_path_for_direct_script() -> None:
    src_root = Path(__file__).resolve().parents[3]
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)


_bootstrap_src_path_for_direct_script()

from gas_calibrator.v2.core.no_write_guard import build_no_write_guard_from_raw_config  # noqa: E402
from gas_calibrator.v2.core.run001_a2_no_write import (  # noqa: E402
    RUN001_FAIL,
    RUN001_PASS,
    authorize_run001_a2_no_write_pressure_sweep,
    build_run001_a2_evidence_payload,
    load_point_rows,
    write_run001_a2_artifacts,
)
from gas_calibrator.v2.entry import (  # noqa: E402
    create_calibration_service_from_config,
    load_config_bundle,
)


DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "validation"
    / "run001_a2_co2_only_7_pressure_no_write_real_machine.json"
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run-001/A2 CO2-only seven-pressure no-write entry")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG), help="Run-001/A2 JSON config")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="Preflight evidence output directory. Defaults to config output_dir/run001_a2_preflight.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the V2 real-machine A2 no-write pressure sweep after preflight.",
    )
    parser.add_argument(
        "--confirm-real-machine-no-write",
        action="store_true",
        help="Required with --execute to confirm a human operator accepted no-write real-machine scope.",
    )
    parser.add_argument(
        "--confirm-a2-no-write-pressure-sweep",
        action="store_true",
        help="Required with --execute to confirm A2 CO2-only seven-pressure no-write scope.",
    )
    return parser


def _default_output_dir(raw_cfg: dict, config_path: str) -> Path:
    paths = raw_cfg.get("paths") if isinstance(raw_cfg.get("paths"), dict) else {}
    output = str(paths.get("output_dir", "") or "").strip()
    if output:
        candidate = Path(output)
        if not candidate.is_absolute():
            candidate = Path(config_path).resolve().parent / candidate
        return candidate.resolve() / "run001_a2_preflight"
    return Path(config_path).resolve().parent / "output" / "run001_a2_preflight"


def _write_preflight(config_path: str, output_dir: Optional[str]) -> tuple[dict, dict[str, str]]:
    resolved_config_path, raw_cfg, _config = load_config_bundle(
        config_path,
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    target_dir = Path(output_dir).expanduser().resolve() if output_dir else _default_output_dir(raw_cfg, resolved_config_path)
    point_rows = load_point_rows(resolved_config_path, raw_cfg)
    guard = build_no_write_guard_from_raw_config(raw_cfg)
    payload = build_run001_a2_evidence_payload(
        raw_cfg,
        config_path=resolved_config_path,
        run_dir=target_dir,
        point_rows=point_rows,
        guard=guard,
        require_runtime_artifacts=False,
    )
    written = write_run001_a2_artifacts(target_dir, payload)
    return payload, written


def _execute(config_path: str, args: argparse.Namespace) -> tuple[int, str]:
    resolved_config_path, raw_cfg, config = load_config_bundle(
        config_path,
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    gate = authorize_run001_a2_no_write_pressure_sweep(
        config,
        raw_cfg,
        args,
        config_path=resolved_config_path,
    )
    raw_cfg["_run001_a2_safety_gate"] = gate
    setattr(config, "_run001_a2_safety_gate", gate)
    service = create_calibration_service_from_config(
        config,
        raw_cfg=raw_cfg,
        preload_points=True,
        require_no_write_guard=True,
    )
    service.run()
    run_dir = str(service.session.output_dir)
    summary_path = Path(run_dir) / "summary.json"
    final_decision = ""
    if summary_path.exists():
        try:
            import json

            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            final_decision = str(payload.get("a2_final_decision") or payload.get("final_decision") or "")
        except Exception:
            final_decision = ""
    status = service.get_status()
    phase = str(getattr(getattr(status, "phase", ""), "value", getattr(status, "phase", "")) or "").strip().lower()
    ok = phase == "completed" and final_decision == RUN001_PASS
    return (0 if ok else 2), run_dir


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    payload, written = _write_preflight(args.config, args.output_dir or None)
    print(f"[Run-001/A2] preflight final_decision={payload.get('final_decision')}", flush=True)
    print(f"[Run-001/A2] preflight a2_final_decision={payload.get('a2_final_decision')}", flush=True)
    for key, path in sorted(written.items()):
        print(f"[Run-001/A2] artifact {key}={path}", flush=True)
    if payload.get("final_decision") == RUN001_FAIL:
        return 2
    if not args.execute:
        return 0
    if not args.confirm_real_machine_no_write:
        print("[Run-001/A2] --confirm-real-machine-no-write is required with --execute", flush=True)
        return 2
    if not args.confirm_a2_no_write_pressure_sweep:
        print("[Run-001/A2] --confirm-a2-no-write-pressure-sweep is required with --execute", flush=True)
        return 2
    code, run_dir = _execute(args.config, args)
    print(f"[Run-001/A2] execute run_dir={run_dir}", flush=True)
    return code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
