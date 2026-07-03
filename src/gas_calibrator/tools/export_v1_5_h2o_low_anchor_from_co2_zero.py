"""CLI for offline H2O low-anchor extraction from V1.5 CO2 zero-gas points."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from gas_calibrator.validation.h2o_low_anchor_from_co2_zero import (
    H2OLowAnchorFromCO2ZeroConfig,
    write_h2o_low_anchor_from_co2_zero_review,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export no-write H2O low-end anchors from completed V1.5 CO2 0 ppm "
            "gas-route evidence."
        )
    )
    parser.add_argument(
        "--co2-zero-run-dir",
        action="append",
        required=True,
        help="CO2 route run directory containing p*_0ppm_* point folders. May repeat.",
    )
    parser.add_argument("--output-dir", required=True, help="Directory for review artifacts.")
    parser.add_argument("--max-residual-h2o-mmol", type=float, default=0.5)
    parser.add_argument("--max-dewpoint-c", type=float, default=-30.0)
    parser.add_argument("--min-distinct-temperatures", type=int, default=3)
    parser.add_argument("--preferred-distinct-temperatures", type=int, default=5)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = H2OLowAnchorFromCO2ZeroConfig(
        max_residual_h2o_mmol=float(args.max_residual_h2o_mmol),
        max_dewpoint_c=float(args.max_dewpoint_c),
        min_distinct_temperatures=int(args.min_distinct_temperatures),
        preferred_distinct_temperatures=int(args.preferred_distinct_temperatures),
    )
    paths = write_h2o_low_anchor_from_co2_zero_review(
        co2_zero_run_dirs=tuple(Path(path) for path in args.co2_zero_run_dir),
        output_dir=Path(args.output_dir),
        cfg=cfg,
    )
    print("H2O low-anchor extraction complete:")
    for key, path in paths.items():
        print(f"- {key}: {path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
