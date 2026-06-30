"""Export V1.5 CO2 S1/S3 residual root-cause review artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

from gas_calibrator.validation.co2_s13_residual_root_cause_review import (
    write_co2_s13_residual_root_cause_review,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fit-points-csv", required=True)
    parser.add_argument("--objective-residuals-csv", required=True)
    parser.add_argument("--objective-summary-csv", required=True)
    parser.add_argument("--selected-candidates-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--worst-point-limit", type=int, default=15)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    outputs = write_co2_s13_residual_root_cause_review(
        fit_points_csv=Path(args.fit_points_csv),
        objective_residuals_csv=Path(args.objective_residuals_csv),
        objective_summary_csv=Path(args.objective_summary_csv),
        selected_candidates_csv=Path(args.selected_candidates_csv),
        output_dir=Path(args.output_dir),
        worst_point_limit=args.worst_point_limit,
    )
    print(f"CO2 S1/S3 residual root-cause review saved: {outputs['markdown']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
