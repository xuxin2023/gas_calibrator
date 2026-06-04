"""Export no-write V1.5 pressure/SENCO9 fit evaluation from existing artifacts.

This sidecar reads pressure quick-check or diagnostic CSV rows and evaluates
whether the analyzer pressure channel behaves like an offset-only SENCO9
candidate. It does not open COM ports, control pressure, switch routes, or
write coefficients.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.pressure_channel import (
    PressureSenco9FitConfig,
    write_pressure_senco9_fit_report,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export no-write V1.5 pressure/SENCO9 fit evaluation evidence."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run directory.")
    parser.add_argument(
        "--pressure-reference-json",
        default=None,
        help="COM22 pressure-reference certificate snapshot JSON.",
    )
    parser.add_argument(
        "--samples-csv",
        default=None,
        help="Optional explicit pressure quick-check, samples, or diagnostic CSV.",
    )
    parser.add_argument("--output-dir", default=None, help="Optional output directory.")
    parser.add_argument(
        "--analyzer-prefix",
        default="all",
        help="Analyzer prefix such as ga01; use 'all' to evaluate every detected analyzer.",
    )
    parser.add_argument("--min-pairs", type=int, default=10)
    parser.add_argument("--min-distinct-pressure-points", type=int, default=3)
    parser.add_argument("--min-pressure-span-hpa", type=float, default=300.0)
    parser.add_argument(
        "--discard-initial-samples-per-pressure-point",
        type=int,
        default=0,
        help="Reject the first N rows in each non-ambient pressure plateau as pressure-transition/cache settling evidence.",
    )
    parser.add_argument("--max-point-reference-span-hpa", type=float, default=1.0)
    parser.add_argument("--max-offset-residual-mean-abs-hpa", type=float, default=1.0)
    parser.add_argument("--max-offset-residual-max-abs-hpa", type=float, default=2.0)
    parser.add_argument("--max-slope-bias-for-offset-only", type=float, default=0.02)
    parser.add_argument(
        "--allow-engineering-reference",
        action="store_true",
        help="Do not fail fit eligibility when the pressure reference certificate snapshot is missing.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    run_dir = Path(args.run_dir).resolve()
    if not run_dir.exists():
        print(f"Run directory not found: {run_dir}", flush=True)
        return 2
    cfg = PressureSenco9FitConfig(
        min_pairs=int(args.min_pairs),
        min_distinct_pressure_points=int(args.min_distinct_pressure_points),
        min_pressure_span_hpa=float(args.min_pressure_span_hpa),
        discard_initial_samples_per_pressure_point=int(args.discard_initial_samples_per_pressure_point),
        max_point_reference_span_hpa=float(args.max_point_reference_span_hpa),
        max_offset_residual_mean_abs_hpa=float(args.max_offset_residual_mean_abs_hpa),
        max_offset_residual_max_abs_hpa=float(args.max_offset_residual_max_abs_hpa),
        max_slope_bias_for_offset_only=float(args.max_slope_bias_for_offset_only),
        require_traceable_reference=not bool(args.allow_engineering_reference),
    )
    try:
        outputs = write_pressure_senco9_fit_report(
            run_dir=run_dir,
            output_dir=args.output_dir,
            pressure_reference_path=args.pressure_reference_json,
            samples_csv=args.samples_csv,
            analyzer_prefix=args.analyzer_prefix,
            cfg=cfg,
        )
    except Exception as exc:
        print(f"Pressure/SENCO9 no-write evaluation export failed: {exc}", flush=True)
        return 1
    print(f"Pressure/SENCO9 no-write evaluation saved: {outputs['workbook']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
