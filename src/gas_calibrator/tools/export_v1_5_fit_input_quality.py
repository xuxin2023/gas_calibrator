"""CLI wrapper for the V1.5 fit-input quality audit."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.v1_5_fit_input_quality import FitInputQualityConfig, write_fit_input_quality_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export offline V1.5 CO2/H2O fit-input quality artifacts.")
    parser.add_argument("--co2-policy-csv", required=True)
    parser.add_argument("--co2-residuals-csv", required=True)
    parser.add_argument("--h2o-policy-csv", required=True)
    parser.add_argument("--h2o-residuals-csv", required=True)
    parser.add_argument("--h2o-point-inputs-csv", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-device-id", action="append", default=[])
    parser.add_argument("--exclude-device-id", action="append", default=[])
    parser.add_argument("--co2-min-fit-samples", type=int, default=10)
    parser.add_argument("--h2o-min-complete-points", type=int, default=8)
    parser.add_argument("--h2o-min-wet-points", type=int, default=3)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = FitInputQualityConfig(
        target_device_ids=tuple(args.target_device_id or ("022", "030", "033", "051")),
        excluded_device_ids=tuple(args.exclude_device_id or ("023", "100")),
        co2_min_fit_samples=int(args.co2_min_fit_samples),
        h2o_min_complete_points=int(args.h2o_min_complete_points),
        h2o_min_wet_points=int(args.h2o_min_wet_points),
    )
    try:
        outputs = write_fit_input_quality_report(
            co2_policy_csv=args.co2_policy_csv,
            co2_residuals_csv=args.co2_residuals_csv,
            h2o_policy_csv=args.h2o_policy_csv,
            h2o_residuals_csv=args.h2o_residuals_csv,
            h2o_point_inputs_csv=args.h2o_point_inputs_csv or None,
            output_dir=args.output_dir,
            cfg=cfg,
        )
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"V1.5 fit-input quality export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(f"V1.5 fit-input quality audit saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

