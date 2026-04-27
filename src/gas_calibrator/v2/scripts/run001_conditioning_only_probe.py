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

from gas_calibrator.v2.core.run001_conditioning_only_probe import (  # noqa: E402
    write_conditioning_only_probe_artifacts,
)


DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "validation"
    / "run001_a2_co2_only_7_pressure_no_write_real_machine.json"
)


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run-001 conditioning-only CO2 skip0 no-write no-COM probe artifact generator"
    )
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG), help="JSON config used for port inventory")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="Output directory. Defaults to output/run001_conditioning_only/.../run_<timestamp>.",
    )
    parser.add_argument(
        "--run-timestamp",
        type=str,
        default="",
        help="Optional run timestamp suffix, e.g. 20260427_170000.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    summary = write_conditioning_only_probe_artifacts(
        args.config,
        output_dir=args.output_dir or None,
        run_timestamp=args.run_timestamp or None,
    )
    print("[Run-001/conditioning-only] real_probe_executed=false", flush=True)
    print("[Run-001/conditioning-only] real_com_opened=false", flush=True)
    print(f"[Run-001/conditioning-only] final_decision={summary.get('final_decision')}", flush=True)
    print(f"[Run-001/conditioning-only] output_dir={summary.get('output_dir')}", flush=True)
    artifact_paths = summary.get("artifact_paths") if isinstance(summary.get("artifact_paths"), dict) else {}
    for key in (
        "summary",
        "route_trace",
        "co2_route_conditioning_evidence",
        "pressure_read_latency_samples",
    ):
        print(f"[Run-001/conditioning-only] artifact {key}={artifact_paths.get(key, '')}", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
