"""Export offline V1.5 CO2 S1/S3 error root-cause resolution review."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_error_root_cause_resolution_review import (
    DEFAULT_COMMON_MODE_MIN_ABS_REL_PERCENT,
    DEFAULT_DEEP_DRY_DEWPOINT_C,
    DEFAULT_RATIO_A_THRESHOLD,
    write_co2_s13_error_root_cause_resolution_review,
)
from ..validation.co2_s13_multistrategy_fit_review import (
    DEFAULT_LOW_END_TARGET_PPM,
    DEFAULT_MIN_RELATIVE_TARGET_PPM,
    DEFAULT_S5_ACCEPTANCE_PERCENT,
    DEFAULT_S5_C0_DECIMALS,
    DEFAULT_S5_C1_DECIMALS,
    DEFAULT_S5_C1_MAX,
    DEFAULT_S5_C1_MIN,
    DEFAULT_TOP_N,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fit-points-csv", required=True)
    parser.add_argument("--baseline-review-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--skip-sensitivity", action="store_true")
    parser.add_argument("--max-top-common-variants", type=int, default=3)
    parser.add_argument("--ratio-a-threshold", type=float, default=DEFAULT_RATIO_A_THRESHOLD)
    parser.add_argument("--deep-dry-dewpoint-c", type=float, default=DEFAULT_DEEP_DRY_DEWPOINT_C)
    parser.add_argument("--common-mode-min-abs-rel-percent", type=float, default=DEFAULT_COMMON_MODE_MIN_ABS_REL_PERCENT)
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
    outputs = write_co2_s13_error_root_cause_resolution_review(
        fit_points_csv=args.fit_points_csv,
        baseline_review_dir=args.baseline_review_dir,
        output_dir=args.output_dir,
        run_sensitivity=not bool(args.skip_sensitivity),
        ratio_a_threshold=float(args.ratio_a_threshold),
        deep_dry_dewpoint_c=float(args.deep_dry_dewpoint_c),
        common_mode_min_abs_rel_percent=float(args.common_mode_min_abs_rel_percent),
        max_top_common_variants=int(args.max_top_common_variants),
        min_relative_target_ppm=float(args.min_relative_target_ppm),
        low_end_target_ppm=float(args.low_end_target_ppm),
        top_n=int(args.top_n),
        s5_acceptance_percent=float(args.s5_acceptance_percent),
        s5_c0_decimals=int(args.s5_c0_decimals),
        s5_c1_decimals=int(args.s5_c1_decimals),
        s5_c1_min=float(args.s5_c1_min),
        s5_c1_max=float(args.s5_c1_max),
    )
    print(f"CO2 S1/S3 error root-cause review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
