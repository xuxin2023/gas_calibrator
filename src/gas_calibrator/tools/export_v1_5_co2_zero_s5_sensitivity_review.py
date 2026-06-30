"""CLI wrapper for V1.5 CO2 zero-anchor/SENCO5 sensitivity review."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_zero_s5_sensitivity_review import (
    DEFAULT_ZERO_OFFSETS_PPM,
    write_co2_zero_s5_sensitivity_review,
)


def _float_list(value: str) -> tuple[float, ...]:
    out = []
    for item in str(value or "").split(","):
        item = item.strip()
        if not item:
            continue
        out.append(float(item))
    return tuple(out)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export offline V1.5 CO2 zero-anchor and SENCO5 output-layer sensitivity review."
    )
    parser.add_argument("--fit-residuals-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fit-point-treatment-plan-csv", default="")
    parser.add_argument("--exclude-device-id", action="append", default=[])
    parser.add_argument(
        "--zero-offsets-ppm",
        default=",".join(f"{value:g}" for value in DEFAULT_ZERO_OFFSETS_PPM),
        help="Comma-separated estimated zero-gas CO2 offsets for no-write sensitivity, e.g. 0,2,5,8,10.",
    )
    parser.add_argument("--min-relative-target-ppm", type=float, default=50.0)
    parser.add_argument("--s5-c0-decimals", type=int, default=3)
    parser.add_argument("--s5-c1-decimals", type=int, default=3)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_zero_s5_sensitivity_review(
        fit_residuals_csv=args.fit_residuals_csv,
        output_dir=args.output_dir,
        fit_point_treatment_plan_csv=args.fit_point_treatment_plan_csv or None,
        exclude_device_ids=args.exclude_device_id,
        zero_offsets_ppm=_float_list(args.zero_offsets_ppm),
        min_relative_target_ppm=float(args.min_relative_target_ppm),
        s5_c0_decimals=int(args.s5_c0_decimals),
        s5_c1_decimals=int(args.s5_c1_decimals),
    )
    print(f"CO2 zero/SENCO5 sensitivity review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
