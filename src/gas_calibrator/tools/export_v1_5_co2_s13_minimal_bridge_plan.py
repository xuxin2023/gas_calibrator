"""Export an offline V1.5 CO2 S1/S3 minimal bridge or resampling plan."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_minimal_bridge_plan import (
    DEFAULT_ACCEPTANCE_PERCENT,
    DEFAULT_COMMON_DEVICE_COUNT,
    DEFAULT_MIN_RELATIVE_TARGET_PPM,
    write_co2_s13_minimal_bridge_plan,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--closure-summary-csv", required=True)
    parser.add_argument("--corrected-residuals-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--acceptance-percent", type=float, default=DEFAULT_ACCEPTANCE_PERCENT)
    parser.add_argument("--min-relative-target-ppm", type=float, default=DEFAULT_MIN_RELATIVE_TARGET_PPM)
    parser.add_argument("--common-device-count", type=int, default=DEFAULT_COMMON_DEVICE_COUNT)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_s13_minimal_bridge_plan(
        closure_summary_csv=args.closure_summary_csv,
        corrected_residuals_csv=args.corrected_residuals_csv,
        output_dir=args.output_dir,
        acceptance_percent=float(args.acceptance_percent),
        min_relative_target_ppm=float(args.min_relative_target_ppm),
        common_device_count=int(args.common_device_count),
    )
    print(f"CO2 minimal bridge plan saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
