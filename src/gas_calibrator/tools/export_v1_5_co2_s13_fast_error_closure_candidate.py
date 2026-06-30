"""Export offline V1.5 CO2 S1/S3 + S5 fast error-closure artifacts."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_fast_error_closure_candidate import (
    DEFAULT_ACCEPTANCE_PERCENT,
    DEFAULT_MIN_RELATIVE_TARGET_PPM,
    DEFAULT_S5_C0_DECIMALS,
    DEFAULT_S5_C1_DECIMALS,
    DEFAULT_S5_C1_MAX,
    DEFAULT_S5_C1_MIN,
    write_co2_s13_fast_error_closure_candidate,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--best-by-device-csv", required=True)
    parser.add_argument("--residuals-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--acceptance-percent", type=float, default=DEFAULT_ACCEPTANCE_PERCENT)
    parser.add_argument("--min-relative-target-ppm", type=float, default=DEFAULT_MIN_RELATIVE_TARGET_PPM)
    parser.add_argument("--s5-c0-decimals", type=int, default=DEFAULT_S5_C0_DECIMALS)
    parser.add_argument("--s5-c1-decimals", type=int, default=DEFAULT_S5_C1_DECIMALS)
    parser.add_argument("--s5-c1-min", type=float, default=DEFAULT_S5_C1_MIN)
    parser.add_argument("--s5-c1-max", type=float, default=DEFAULT_S5_C1_MAX)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_s13_fast_error_closure_candidate(
        best_by_device_csv=args.best_by_device_csv,
        residuals_csv=args.residuals_csv,
        output_dir=args.output_dir,
        acceptance_percent=float(args.acceptance_percent),
        min_relative_target_ppm=float(args.min_relative_target_ppm),
        s5_c0_decimals=int(args.s5_c0_decimals),
        s5_c1_decimals=int(args.s5_c1_decimals),
        s5_c1_min=float(args.s5_c1_min),
        s5_c1_max=float(args.s5_c1_max),
    )
    print(f"CO2 fast error-closure review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
