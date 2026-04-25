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

from gas_calibrator.v2.core.run001_a1_analyzer_diagnostics import (  # noqa: E402
    build_analyzer_precheck_diagnostics,
    write_analyzer_precheck_diagnostics,
)
from gas_calibrator.v2.entry import load_config_bundle  # noqa: E402


DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "validation"
    / "run001_a1_co2_only_skip0_no_write_real_machine_dry_run.json"
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Run-001/A1 gas analyzer precheck diagnostics")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG), help="Run-001/A1 JSON config")
    parser.add_argument(
        "--only-failed",
        nargs="*",
        default=[],
        help="Logical analyzer ids to diagnose, for example gas_analyzer_1 gas_analyzer_2 gas_analyzer_3",
    )
    parser.add_argument(
        "--analyzers",
        nargs="*",
        default=[],
        help="Logical analyzer ids or labels to diagnose, for example gas_analyzer_0 gas_analyzer_1 GA03",
    )
    parser.add_argument(
        "--ports",
        nargs="*",
        default=[],
        help="Explicit COM ports for read-only discovery, for example COM35 COM36 COM37",
    )
    parser.add_argument("--read-only", action="store_true", help="Required marker; diagnostics are read-only only")
    parser.add_argument(
        "--allow-read-query",
        action="store_true",
        help="Permit the non-persistent READDATA query if passive active-send listening receives no frame.",
    )
    parser.add_argument("--timeout-s", type=float, default=20.0, help="Active-send listening timeout in seconds")
    parser.add_argument("--output-dir", type=str, default="", help="Output directory for diagnostics artifacts")
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
    return base / f"analyzer_diagnostics_{stamp}"


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    if not args.read_only:
        print("[Run-001/A1] --read-only is required; this diagnostic has no write mode.", flush=True)
        return 2
    resolved_config_path, raw_cfg, _config = load_config_bundle(
        args.config,
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else _default_output_dir(raw_cfg, resolved_config_path)
    payload = build_analyzer_precheck_diagnostics(
        raw_cfg,
        only_failed=list(args.only_failed or []),
        analyzers=list(args.analyzers or []) or None,
        ports=list(args.ports or []) or None,
        read_only=True,
        allow_read_query=bool(args.allow_read_query),
        timeout_s=float(args.timeout_s),
    )
    written = write_analyzer_precheck_diagnostics(output_dir, payload)
    print(f"[Run-001/A1] analyzer diagnostics json={written['json']}", flush=True)
    print(f"[Run-001/A1] analyzer diagnostics report={written['report']}", flush=True)
    print(f"[Run-001/A1] persistent_write_command_sent={payload.get('summary', {}).get('persistent_write_command_sent')}", flush=True)
    return 1 if payload.get("summary", {}).get("failed") else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
