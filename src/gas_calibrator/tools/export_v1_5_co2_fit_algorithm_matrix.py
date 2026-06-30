"""CLI wrapper for the offline V1.5 CO2 fitting-algorithm matrix."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_fit_algorithm_matrix import write_co2_fit_algorithm_matrix_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export offline V1.5 CO2 fitting-algorithm matrix artifacts.")
    parser.add_argument("--fit-residuals-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--old-snapshot-json", default="")
    parser.add_argument("--exclude-device-id", action="append", default=[])
    parser.add_argument("--fit-point-treatment-plan-csv", default="")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_fit_algorithm_matrix_report(
        fit_residuals_csv=args.fit_residuals_csv,
        output_dir=args.output_dir,
        old_snapshot_json=args.old_snapshot_json or None,
        exclude_device_ids=args.exclude_device_id,
        fit_point_treatment_plan_csv=args.fit_point_treatment_plan_csv or None,
    )
    print(f"CO2 fitting algorithm matrix saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
