"""Export offline V1.5 CO2 S1/S3 multi-strategy fit review artifacts."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_multistrategy_fit_review import (
    DEFAULT_LOW_END_TARGET_PPM,
    DEFAULT_MIN_RELATIVE_TARGET_PPM,
    DEFAULT_S5_ACCEPTANCE_PERCENT,
    DEFAULT_S5_C0_DECIMALS,
    DEFAULT_S5_C1_DECIMALS,
    DEFAULT_S5_C1_MAX,
    DEFAULT_S5_C1_MIN,
    DEFAULT_TOP_N,
    write_co2_s13_multistrategy_fit_review,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fit-points-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fit-point-treatment-plan-csv", default="")
    parser.add_argument("--exclude-device-id", action="append", default=[])
    parser.add_argument("--min-relative-target-ppm", type=float, default=DEFAULT_MIN_RELATIVE_TARGET_PPM)
    parser.add_argument("--low-end-target-ppm", type=float, default=DEFAULT_LOW_END_TARGET_PPM)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--s5-acceptance-percent", type=float, default=DEFAULT_S5_ACCEPTANCE_PERCENT)
    parser.add_argument("--s5-c0-decimals", type=int, default=DEFAULT_S5_C0_DECIMALS)
    parser.add_argument("--s5-c1-decimals", type=int, default=DEFAULT_S5_C1_DECIMALS)
    parser.add_argument("--s5-c1-min", type=float, default=DEFAULT_S5_C1_MIN)
    parser.add_argument("--s5-c1-max", type=float, default=DEFAULT_S5_C1_MAX)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_s13_multistrategy_fit_review(
        fit_points_csv=args.fit_points_csv,
        output_dir=args.output_dir,
        fit_point_treatment_plan_csv=args.fit_point_treatment_plan_csv or None,
        exclude_device_ids=args.exclude_device_id,
        min_relative_target_ppm=float(args.min_relative_target_ppm),
        low_end_target_ppm=float(args.low_end_target_ppm),
        top_n=int(args.top_n),
        s5_acceptance_percent=float(args.s5_acceptance_percent),
        s5_c0_decimals=int(args.s5_c0_decimals),
        s5_c1_decimals=int(args.s5_c1_decimals),
        s5_c1_min=float(args.s5_c1_min),
        s5_c1_max=float(args.s5_c1_max),
    )
    print(f"CO2 S1/S3 multi-strategy fit review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
