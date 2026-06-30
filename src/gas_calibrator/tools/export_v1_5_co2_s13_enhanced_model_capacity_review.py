"""CLI for the V1.5 CO2 S1/S3 enhanced model-capacity review."""

from __future__ import annotations

import argparse
from pathlib import Path

from gas_calibrator.validation.co2_s13_enhanced_model_capacity_review import (
    DEFAULT_LOW_END_MULTIPLIER,
    DEFAULT_LOW_END_TARGET_PPM,
    DEFAULT_MIN_RELATIVE_TARGET_PPM,
    DEFAULT_OBJECTIVES,
    DEFAULT_ZERO_OFFSETS_PPM,
    write_co2_s13_enhanced_model_capacity_review,
)


def _parse_float_list(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in str(value).split(",") if item.strip())


def _parse_str_list(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in str(value).split(",") if item.strip())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export a no-write V1.5 CO2 S1/S3 enhanced model-capacity review.",
    )
    parser.add_argument("--fit-points-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--exclude-device-ids", default="")
    parser.add_argument("--zero-offsets-ppm", default=",".join(str(item) for item in DEFAULT_ZERO_OFFSETS_PPM))
    parser.add_argument("--objectives", default=",".join(DEFAULT_OBJECTIVES))
    parser.add_argument("--min-relative-target-ppm", type=float, default=DEFAULT_MIN_RELATIVE_TARGET_PPM)
    parser.add_argument("--low-end-target-ppm", type=float, default=DEFAULT_LOW_END_TARGET_PPM)
    parser.add_argument("--low-end-multiplier", type=float, default=DEFAULT_LOW_END_MULTIPLIER)
    parser.add_argument("--acceptance-percent", type=float, default=1.0)
    parser.add_argument(
        "--skip-s5-review",
        action="store_true",
        help="Skip slow three-decimal SENCO5 trim search and review S1/S3 model capacity only.",
    )
    args = parser.parse_args()

    paths = write_co2_s13_enhanced_model_capacity_review(
        fit_points_csv=Path(args.fit_points_csv),
        output_dir=Path(args.output_dir),
        exclude_device_ids=_parse_str_list(args.exclude_device_ids),
        zero_offsets_ppm=_parse_float_list(args.zero_offsets_ppm),
        objectives=_parse_str_list(args.objectives),
        min_relative_target_ppm=float(args.min_relative_target_ppm),
        low_end_target_ppm=float(args.low_end_target_ppm),
        low_end_multiplier=float(args.low_end_multiplier),
        acceptance_percent=float(args.acceptance_percent),
        include_s5_review=not bool(args.skip_s5_review),
    )
    print(f"CO2 S1/S3 enhanced model-capacity review saved: {paths['markdown']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
