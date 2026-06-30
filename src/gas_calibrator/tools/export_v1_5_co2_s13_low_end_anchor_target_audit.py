"""Export V1.5 CO2 S1/S3 low-end anchor and target-state audit artifacts."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_low_end_anchor_target_audit import (
    write_co2_s13_low_end_anchor_target_audit,
)
from ..validation.co2_s13_model_structure_review import (
    DEFAULT_STRUCTURE_OBJECTIVES,
    DEFAULT_STRUCTURES,
)
from ..validation.co2_zero_s5_sensitivity_review import DEFAULT_ZERO_OFFSETS_PPM


def _float_list(value: str) -> tuple[float, ...]:
    out = []
    for item in str(value or "").split(","):
        item = item.strip()
        if item:
            out.append(float(item))
    return tuple(out)


def _str_list(value: str) -> tuple[str, ...]:
    out = []
    for item in str(value or "").split(","):
        item = item.strip()
        if item:
            out.append(item)
    return tuple(out)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fit-points-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--structures",
        default=",".join(DEFAULT_STRUCTURES),
        help="Comma-separated S1/S3 model structure ids.",
    )
    parser.add_argument(
        "--objectives",
        default=",".join(DEFAULT_STRUCTURE_OBJECTIVES),
        help="Comma-separated objective ids.",
    )
    parser.add_argument(
        "--zero-offsets-ppm",
        default=",".join(f"{float(value):g}" for value in DEFAULT_ZERO_OFFSETS_PPM),
        help="Comma-separated estimated zero-gas CO2 offsets.",
    )
    parser.add_argument("--min-relative-target-ppm", type=float, default=50.0)
    parser.add_argument("--low-end-target-ppm", type=float, default=300.0)
    parser.add_argument("--low-end-multiplier", type=float, default=3.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_s13_low_end_anchor_target_audit(
        fit_points_csv=args.fit_points_csv,
        output_dir=args.output_dir,
        structures=_str_list(args.structures),
        objectives=_str_list(args.objectives),
        zero_offsets_ppm=_float_list(args.zero_offsets_ppm),
        min_relative_target_ppm=float(args.min_relative_target_ppm),
        low_end_target_ppm=float(args.low_end_target_ppm),
        low_end_multiplier=float(args.low_end_multiplier),
    )
    print(f"CO2 S1/S3 low-end anchor target audit saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
