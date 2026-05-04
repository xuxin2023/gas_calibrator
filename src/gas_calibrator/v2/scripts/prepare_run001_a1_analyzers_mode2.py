from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Optional


def _bootstrap_src_path_for_direct_script() -> None:
    src_root = Path(__file__).resolve().parents[3]
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)


_bootstrap_src_path_for_direct_script()

from gas_calibrator.v2.core.run001_a1_analyzer_mode2_setup import (  # noqa: E402
    run_analyzer_mode2_setup,
)
from gas_calibrator.v2.entry import load_config_bundle  # noqa: E402


DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "validation"
    / "run001_a1_co2_only_skip0_no_write_real_machine_dry_run.json"
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run-001/A1 analyzer MODE2 active-send communication setup")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG), help="Run-001/A1 JSON config")
    parser.add_argument(
        "--analyzers",
        nargs="*",
        default=[],
        help="Logical analyzer ids or labels, for example gas_analyzer_0 gas_analyzer_1 GA03",
    )
    parser.add_argument(
        "--ports",
        nargs="*",
        default=[],
        help="Explicit COM ports for setup target selection, for example COM35 COM41 COM42",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print/write the command plan without opening ports")
    parser.add_argument(
        "--set-mode2-active-send",
        action="store_true",
        help="Request sending the whitelisted MODE2 + active-send communication commands",
    )
    parser.add_argument(
        "--confirm-mode2-communication-setup",
        action="store_true",
        help="Required together with --set-mode2-active-send before any MODE2 setup command is sent",
    )
    parser.add_argument("--timeout-s", type=float, default=20.0, help="Read-only diagnostics timeout in seconds")
    parser.add_argument("--command-timeout-s", type=float, default=5.0, help="Per-command setup timeout in seconds")
    parser.add_argument("--device-timeout-s", type=float, default=30.0, help="Per-device setup timeout in seconds")
    parser.add_argument("--total-timeout-s", type=float, default=0.0, help="Overall setup timeout in seconds")
    parser.add_argument("--output-dir", type=str, default="", help="Output directory for setup artifacts")
    return parser


def _default_output_dir(raw_cfg: dict, config_path: str) -> Path:
    paths = raw_cfg.get("paths") if isinstance(raw_cfg, dict) else {}
    output = str(paths.get("output_dir", "") if isinstance(paths, dict) else "").strip()
    if output:
        candidate = Path(output)
        if not candidate.is_absolute():
            candidate = Path(config_path).resolve().parent / candidate
        base = candidate.resolve()
    else:
        base = Path(config_path).resolve().parent / "output" / "run001_a1"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return base / f"analyzer_mode2_setup_{stamp}"


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    resolved_config_path, raw_cfg, _config = load_config_bundle(
        args.config,
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else _default_output_dir(raw_cfg, resolved_config_path)
    dry_run = bool(args.dry_run or not (args.set_mode2_active_send and args.confirm_mode2_communication_setup))
    if args.ports and args.analyzers:
        print("[Run-001/A1] refused: use either --ports or --analyzers, not both.", flush=True)
        return 2
    payload, written = run_analyzer_mode2_setup(
        raw_cfg,
        output_dir=output_dir,
        analyzers=list(args.analyzers or []),
        ports=list(args.ports or []),
        dry_run=dry_run,
        set_mode2_active_send=bool(args.set_mode2_active_send),
        confirm_mode2_communication_setup=bool(args.confirm_mode2_communication_setup),
        timeout_s=float(args.timeout_s),
        command_timeout_s=float(args.command_timeout_s),
        device_timeout_s=float(args.device_timeout_s),
        total_timeout_s=float(args.total_timeout_s) if float(args.total_timeout_s or 0.0) > 0 else None,
        config_path=str(resolved_config_path),
    )
    print(f"[Run-001/A1] analyzer MODE2 setup output={output_dir}", flush=True)
    print(f"[Run-001/A1] analyzer MODE2 setup json={written['analyzer_mode2_setup_json']}", flush=True)
    print(f"[Run-001/A1] analyzer MODE2 setup report={written['analyzer_mode2_setup_report']}", flush=True)
    print(
        "[Run-001/A1] analyzer diagnostics json="
        f"{written['analyzer_precheck_diagnostics_json']}",
        flush=True,
    )
    print(
        "[Run-001/A1] analyzer diagnostics report="
        f"{written['analyzer_precheck_diagnostics_report']}",
        flush=True,
    )
    summary = dict(payload.get("summary") or {})
    print(f"[Run-001/A1] dry_run={payload.get('dry_run')}", flush=True)
    print(f"[Run-001/A1] commands_sent={payload.get('commands_sent')}", flush=True)
    print(f"[Run-001/A1] persistent_write_command_sent={payload.get('persistent_write_command_sent')}", flush=True)
    print(f"[Run-001/A1] calibration_write_command_sent={payload.get('calibration_write_command_sent')}", flush=True)
    print(f"[Run-001/A1] a1_no_write_rerun_allowed={summary.get('a1_no_write_rerun_allowed')}", flush=True)

    if args.set_mode2_active_send and not args.confirm_mode2_communication_setup:
        print("[Run-001/A1] refused: --confirm-mode2-communication-setup is required.", flush=True)
        return 2
    if payload.get("forbidden_command_plan_error"):
        return 2
    if not dry_run and not summary.get("a1_no_write_rerun_allowed"):
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
