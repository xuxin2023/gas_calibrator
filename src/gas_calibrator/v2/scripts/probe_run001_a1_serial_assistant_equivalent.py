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

from gas_calibrator.v2.core.run001_a1_serial_assistant_probe import (  # noqa: E402
    SERIAL_ASSISTANT_BASELINE_PORTS,
    build_serial_assistant_baseline_payload,
    build_serial_assistant_equivalent_probe_payload,
    write_serial_assistant_artifacts,
)
from gas_calibrator.v2.entry import load_config_bundle  # noqa: E402


DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "validation"
    / "run001_a1_co2_only_skip0_no_write_real_machine_dry_run.json"
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run-001/A1 serial assistant equivalent gas analyzer probe")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG), help="Run-001/A1 JSON config")
    parser.add_argument(
        "--ports",
        nargs="*",
        default=list(SERIAL_ASSISTANT_BASELINE_PORTS),
        help="Explicit COM ports to probe, for example COM35 COM37 COM41 COM42",
    )
    parser.add_argument("--read-only", action="store_true", help="Required unless explicit communication setup is requested")
    parser.add_argument(
        "--allow-read-query",
        action="store_true",
        help="Permit READDATA,YGAS,FFF if passive active-send listening receives no frame.",
    )
    parser.add_argument(
        "--send-mode2-active-send",
        action="store_true",
        help="Optionally send only MODE,YGAS,FFF,2 and SETCOMWAY,YGAS,FFF,1 after explicit confirmation.",
    )
    parser.add_argument(
        "--confirm-communication-setup",
        action="store_true",
        help="Required with --send-mode2-active-send before any MODE/SETCOMWAY communication command is sent.",
    )
    parser.add_argument("--timeout-s", type=float, default=30.0, help="Per-port active-send listening timeout")
    parser.add_argument("--command-timeout-s", type=float, default=5.0, help="Per-command ACK scan timeout")
    parser.add_argument("--output-dir", type=str, default="", help="Output directory for baseline/probe artifacts")
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
    return base / f"serial_assistant_baseline_{stamp}"


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    if args.send_mode2_active_send and not args.confirm_communication_setup:
        print("[Run-001/A1] refused: --confirm-communication-setup is required before MODE/SETCOMWAY.", flush=True)
        return 2
    if not args.read_only and not args.send_mode2_active_send:
        print("[Run-001/A1] refused: --read-only is required for a non-setup serial assistant probe.", flush=True)
        return 2

    resolved_config_path, raw_cfg, _config = load_config_bundle(
        args.config,
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else _default_output_dir(raw_cfg, resolved_config_path)
    ports = list(args.ports or [])
    baseline = build_serial_assistant_baseline_payload(raw_cfg, ports=ports)
    probe = build_serial_assistant_equivalent_probe_payload(
        raw_cfg,
        ports=ports,
        read_only=bool(args.read_only),
        allow_read_query=bool(args.allow_read_query),
        send_mode2_active_send=bool(args.send_mode2_active_send),
        confirm_communication_setup=bool(args.confirm_communication_setup),
        timeout_s=float(args.timeout_s),
        command_timeout_s=float(args.command_timeout_s),
    )
    written = write_serial_assistant_artifacts(output_dir, baseline, probe)
    print(f"[Run-001/A1] serial assistant output={output_dir}", flush=True)
    print(f"[Run-001/A1] baseline json={written['serial_assistant_baseline_json']}", flush=True)
    print(f"[Run-001/A1] baseline report={written['serial_assistant_baseline_md']}", flush=True)
    print(f"[Run-001/A1] probe json={written['serial_assistant_equivalent_probe_json']}", flush=True)
    print(f"[Run-001/A1] probe report={written['serial_assistant_equivalent_probe_md']}", flush=True)
    print(f"[Run-001/A1] commands_sent={probe.get('commands_sent')}", flush=True)
    print(f"[Run-001/A1] persistent_write_command_sent={probe.get('persistent_write_command_sent')}", flush=True)
    print(f"[Run-001/A1] calibration_write_command_sent={probe.get('calibration_write_command_sent')}", flush=True)
    print(
        "[Run-001/A1] serial_assistant_success_reproduced_by_v2="
        f"{probe.get('summary', {}).get('serial_assistant_success_reproduced_by_v2')}",
        flush=True,
    )
    return 0 if probe.get("summary", {}).get("serial_assistant_success_reproduced_by_v2") else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
