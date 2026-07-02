"""Export V1.5 controlled SENCO9 write-review artifacts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.pressure_senco9_write_review import write_pressure_senco9_write_review_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export an offline V1.5 controlled SENCO9 write-review package."
    )
    parser.add_argument(
        "--fit-dir",
        required=True,
        help="Directory containing pressure_fit_summary.csv from the no-write SENCO9 evaluation.",
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for review artifacts.")
    parser.add_argument(
        "--selected-analyzer-device-id",
        default="",
        help="Analyzer MODE2 device ID selected for a possible single-device controlled write.",
    )
    parser.add_argument(
        "--selected-analyzer-prefix",
        default="",
        help="Acquisition channel fallback such as ga01; device ID is preferred.",
    )
    parser.add_argument(
        "--old-getco-json",
        default=None,
        help="Optional old GETCO9/SENCO9 snapshot JSON used to prove rollback is possible.",
    )
    parser.add_argument("--reviewer", default="", help="Reviewer name or identifier.")
    parser.add_argument("--approver", default="", help="Approver name or identifier.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    fit_dir = Path(args.fit_dir).resolve()
    if not fit_dir.exists():
        print(f"Fit directory not found: {fit_dir}", flush=True)
        return 2
    try:
        outputs = write_pressure_senco9_write_review_report(
            fit_dir=fit_dir,
            output_dir=args.output_dir,
            selected_analyzer_device_id=args.selected_analyzer_device_id,
            selected_analyzer_prefix=args.selected_analyzer_prefix,
            old_getco_snapshot_path=args.old_getco_json,
            reviewer=args.reviewer,
            approver=args.approver,
        )
    except Exception as exc:
        print(f"Pressure/SENCO9 write-review export failed: {exc}", flush=True)
        return 1
    print(f"Pressure/SENCO9 write-review saved: {outputs['workbook']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
