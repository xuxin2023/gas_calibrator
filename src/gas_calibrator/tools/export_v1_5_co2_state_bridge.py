"""CLI wrapper for V1.5 CO2 state-normalization bridge review."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_state_bridge import write_co2_state_bridge_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a no-COM V1.5 CO2 state-normalization bridge review."
    )
    parser.add_argument("--old-run-dir", required=True)
    parser.add_argument("--current-summary-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-device-id", default="100")
    parser.add_argument("--bridge-rel-limit-pct", type=float, default=1.5)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_state_bridge_report(
        old_run_dir=args.old_run_dir,
        current_summary_csv=args.current_summary_csv,
        output_dir=args.output_dir,
        target_device_id=args.target_device_id,
        bridge_rel_limit_pct=args.bridge_rel_limit_pct,
    )
    print(f"CO2 state bridge review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
