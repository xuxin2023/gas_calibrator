"""Export formal V1.5 open-flow calibration evidence from an existing run.

This is an offline sidecar tool. It reads run artifacts and writes report files;
it does not open real COM ports and does not write calibration coefficients.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_open_flow_artifacts import write_formal_open_flow_sidecar_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export V1.5 formal open-flow QC evidence from historical sample artifacts."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run directory.")
    parser.add_argument("--plan-json", default=None, help="Formal calibration plan snapshot JSON.")
    parser.add_argument(
        "--pressure-reference-json",
        default=None,
        help="COM22 pressure-reference certificate snapshot JSON.",
    )
    parser.add_argument(
        "--pressure-check-csv",
        default=None,
        help="Optional pressure quick-check CSV or directory to bind by analyzer device ID.",
    )
    parser.add_argument("--output-dir", default=None, help="Optional output directory.")
    parser.add_argument(
        "--component",
        choices=("co2", "h2o", "both"),
        default="both",
        help="Component rows to classify.",
    )
    parser.add_argument(
        "--analyzer-prefix",
        default="ga01",
        help="Analyzer sample prefix to use for MODE2 fields, e.g. ga01. Use 'all' for detected analyzers or a comma list.",
    )
    parser.add_argument(
        "--all-analyzers",
        action="store_true",
        help="Classify every detected analyzer prefix independently.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    run_dir = Path(args.run_dir).resolve()
    if not run_dir.exists():
        print(f"Run directory not found: {run_dir}", flush=True)
        return 2
    try:
        outputs = write_formal_open_flow_sidecar_report(
            run_dir=run_dir,
            output_dir=args.output_dir,
            plan_path=args.plan_json,
            pressure_reference_path=args.pressure_reference_json,
            pressure_check_path=args.pressure_check_csv,
            component=args.component,
            analyzer_prefix="all" if args.all_analyzers else args.analyzer_prefix,
        )
    except Exception as exc:
        print(f"Formal open-flow report export failed: {exc}", flush=True)
        return 1
    print(f"Formal open-flow report saved: {outputs['workbook']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
