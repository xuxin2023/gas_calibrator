"""Export an offline V1.5 CO2 S1/S3 minimal resampling run list."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_minimal_resampling_runlist import (
    DEFAULT_ACCEPTANCE_PERCENT,
    DEFAULT_EXCLUDED_TEMPERATURES_C,
    DEFAULT_MAX_POINTS,
    write_co2_s13_minimal_resampling_runlist,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--point-summary-csv", required=True)
    parser.add_argument(
        "--template-queue-csv",
        default=None,
        help=(
            "Optional canonical co2_runner_queue.csv to inherit valve group and queue defaults. "
            "Recommended for real no-write resampling preparation."
        ),
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-points", type=int, default=DEFAULT_MAX_POINTS)
    parser.add_argument("--acceptance-percent", type=float, default=DEFAULT_ACCEPTANCE_PERCENT)
    parser.add_argument(
        "--exclude-temperature-c",
        type=float,
        action="append",
        default=None,
        help="Temperature group to exclude from this minimal resampling list. Repeatable.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    excluded_temperatures = (
        tuple(float(value) for value in args.exclude_temperature_c)
        if args.exclude_temperature_c is not None
        else DEFAULT_EXCLUDED_TEMPERATURES_C
    )
    outputs = write_co2_s13_minimal_resampling_runlist(
        point_summary_csv=args.point_summary_csv,
        template_queue_csv=args.template_queue_csv,
        output_dir=args.output_dir,
        max_points=int(args.max_points),
        acceptance_percent=float(args.acceptance_percent),
        excluded_temperatures_c=excluded_temperatures,
    )
    print(f"CO2 minimal resampling run list saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
