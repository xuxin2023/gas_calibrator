"""Export V1.5 analyzer pressure-channel validation from existing artifacts.

This is an offline sidecar tool. It reads sample or quick-check CSV rows and
writes validation artifacts; it does not open COM ports or control devices.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.pressure_channel import write_pressure_channel_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export V1.5 ambient pressure-channel validation evidence."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run directory.")
    parser.add_argument(
        "--pressure-reference-json",
        default=None,
        help="COM22 pressure-reference certificate snapshot JSON.",
    )
    parser.add_argument(
        "--samples-csv",
        default=None,
        help="Optional explicit pressure quick-check or samples CSV.",
    )
    parser.add_argument("--output-dir", default=None, help="Optional output directory.")
    parser.add_argument(
        "--analyzer-prefix",
        default="ga01",
        help="Analyzer prefix containing pressure_kpa, e.g. ga01; use 'all' to validate every detected analyzer.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    run_dir = Path(args.run_dir).resolve()
    if not run_dir.exists():
        print(f"Run directory not found: {run_dir}", flush=True)
        return 2
    try:
        outputs = write_pressure_channel_report(
            run_dir=run_dir,
            output_dir=args.output_dir,
            pressure_reference_path=args.pressure_reference_json,
            samples_csv=args.samples_csv,
            analyzer_prefix=args.analyzer_prefix,
        )
    except Exception as exc:
        print(f"Pressure-channel validation export failed: {exc}", flush=True)
        return 1
    print(f"Pressure-channel validation saved: {outputs['workbook']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
