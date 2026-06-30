"""Export offline V1.5 CO2 S1/S3 low-end correction strategy review."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_low_end_correction_strategy_review import (
    DEFAULT_DIAGNOSTIC_HOLDOUT_POINTS,
    DEFAULT_LOW_END_MULTIPLIERS,
    DEFAULT_ZERO_OFFSETS_PPM,
    write_co2_s13_low_end_correction_strategy_review,
)


def _float_list(value: str) -> tuple[float, ...]:
    values = []
    for item in str(value or "").split(","):
        item = item.strip()
        if item:
            values.append(float(item))
    return tuple(values)


def _str_list(value: str) -> tuple[str, ...]:
    values = []
    for item in str(value or "").split(","):
        item = item.strip()
        if item:
            values.append(item)
    return tuple(values)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fit-points-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fit-point-treatment-plan-csv", default="")
    parser.add_argument("--exclude-device-id", action="append", default=[])
    parser.add_argument(
        "--zero-offsets-ppm",
        default=",".join(f"{float(value):g}" for value in DEFAULT_ZERO_OFFSETS_PPM),
    )
    parser.add_argument(
        "--low-end-multipliers",
        default=",".join(f"{float(value):g}" for value in DEFAULT_LOW_END_MULTIPLIERS),
    )
    parser.add_argument(
        "--diagnostic-holdout-points",
        default=",".join(DEFAULT_DIAGNOSTIC_HOLDOUT_POINTS),
    )
    parser.add_argument("--min-relative-target-ppm", type=float, default=50.0)
    parser.add_argument("--low-end-target-ppm", type=float, default=300.0)
    parser.add_argument("--irls-iterations", type=int, default=5)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_s13_low_end_correction_strategy_review(
        fit_points_csv=args.fit_points_csv,
        output_dir=args.output_dir,
        fit_point_treatment_plan_csv=args.fit_point_treatment_plan_csv or None,
        exclude_device_ids=args.exclude_device_id,
        zero_offsets_ppm=_float_list(args.zero_offsets_ppm),
        low_end_multipliers=_float_list(args.low_end_multipliers),
        diagnostic_holdout_points=_str_list(args.diagnostic_holdout_points),
        min_relative_target_ppm=float(args.min_relative_target_ppm),
        low_end_target_ppm=float(args.low_end_target_ppm),
        irls_iterations=int(args.irls_iterations),
    )
    print(f"CO2 S1/S3 low-end correction strategy review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
