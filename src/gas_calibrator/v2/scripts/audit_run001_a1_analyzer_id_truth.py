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

from gas_calibrator.v2.core.run001_a1_analyzer_id_truth import (  # noqa: E402
    build_analyzer_id_truth_audit_payload,
    write_analyzer_id_truth_audit_artifacts,
)
from gas_calibrator.v2.entry import load_config_bundle  # noqa: E402


DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "validation"
    / "run001_a1_co2_only_skip0_no_write_detected_4_analyzers.json"
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run-001/A1 read-only analyzer MODE2 frame ID truth audit")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG), help="Run-001/A1 JSON config")
    parser.add_argument(
        "--ports",
        nargs="*",
        default=["COM35", "COM37", "COM41", "COM42"],
        help="COM ports to passively capture, for example COM35 COM37 COM41 COM42",
    )
    parser.add_argument("--read-only", action="store_true", help="Required; this audit never sends commands")
    parser.add_argument(
        "--capture-raw-frames",
        action="store_true",
        help="Required for operator clarity; raw frame samples are always saved",
    )
    parser.add_argument("--timeout-s", type=float, default=30.0, help="Per-port passive listening timeout")
    parser.add_argument("--output-dir", type=str, default="", help="Output directory for truth audit artifacts")
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
    return base / f"analyzer_id_truth_audit_{stamp}"


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    if not args.read_only:
        print("[Run-001/A1] refused: --read-only is required for analyzer ID truth audit.", flush=True)
        return 2
    if not args.capture_raw_frames:
        print("[Run-001/A1] refused: --capture-raw-frames is required for analyzer ID truth audit.", flush=True)
        return 2

    resolved_config_path, raw_cfg, _config = load_config_bundle(
        args.config,
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else _default_output_dir(raw_cfg, resolved_config_path)
    payload = build_analyzer_id_truth_audit_payload(
        raw_cfg,
        ports=list(args.ports or []),
        read_only=True,
        timeout_s=float(args.timeout_s),
    )
    written = write_analyzer_id_truth_audit_artifacts(output_dir, payload)

    print(f"[Run-001/A1] analyzer_id_truth_audit output={output_dir}", flush=True)
    print(f"[Run-001/A1] analyzer_id_truth_audit json={written['analyzer_id_truth_audit_json']}", flush=True)
    print(f"[Run-001/A1] analyzer_id_truth_audit report={written['analyzer_id_truth_audit_md']}", flush=True)
    print(f"[Run-001/A1] commands_sent={payload.get('commands_sent')}", flush=True)
    print(f"[Run-001/A1] duplicate_device_id_detected={payload.get('duplicate_device_id_detected')}", flush=True)
    print(f"[Run-001/A1] duplicate_device_id_status={payload.get('duplicate_device_id_status')}", flush=True)
    for item in list(payload.get("analyzers") or []):
        print(
            "[Run-001/A1] "
            f"{item.get('port')} stable_device_id={item.get('stable_device_id') or '-'} "
            f"mode2_frame_count={item.get('mode2_frame_count')} "
            f"parse_error_count={item.get('parse_error_count')}",
            flush=True,
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
