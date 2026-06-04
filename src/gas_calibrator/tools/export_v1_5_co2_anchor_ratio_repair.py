"""CLI wrapper for V1.5 CO2 anchored ratio-repair review."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_anchor_ratio_repair import write_co2_anchor_ratio_repair_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a no-COM V1.5 CO2 anchored ratio-repair review."
    )
    parser.add_argument("--old-run-dir", required=True)
    parser.add_argument("--current-sample-file", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-device-id", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_anchor_ratio_repair_report(
        old_run_dir=args.old_run_dir,
        current_sample_files=args.current_sample_file,
        output_dir=args.output_dir,
        target_device_id=args.target_device_id,
    )
    print(f"CO2 anchored ratio-repair review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
