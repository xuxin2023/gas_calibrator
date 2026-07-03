"""CLI for the V1.5 CO2 S1/S3 source-state discontinuity audit."""

from __future__ import annotations

import argparse
from pathlib import Path

from gas_calibrator.validation.co2_s13_source_state_discontinuity_audit import (
    Co2S13SourceStateAuditConfig,
    write_co2_s13_source_state_discontinuity_audit,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export an offline/no-write V1.5 CO2 S1/S3 source-state discontinuity audit.",
    )
    parser.add_argument("--fit-points-csv", required=True)
    parser.add_argument("--enhanced-summary-csv", required=True)
    parser.add_argument("--enhanced-residuals-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--acceptance-percent", type=float, default=1.0)
    parser.add_argument("--dry-dewpoint-gate-c", type=float, default=-28.0)
    parser.add_argument("--ratio-std-a-gate", type=float, default=0.0005)
    parser.add_argument("--pressure-span-warn-hpa", type=float, default=5.0)
    parser.add_argument("--pressure-point-outlier-hpa", type=float, default=4.0)
    args = parser.parse_args()

    paths = write_co2_s13_source_state_discontinuity_audit(
        fit_points_csv=Path(args.fit_points_csv),
        enhanced_summary_csv=Path(args.enhanced_summary_csv),
        enhanced_residuals_csv=Path(args.enhanced_residuals_csv),
        output_dir=Path(args.output_dir),
        cfg=Co2S13SourceStateAuditConfig(
            acceptance_pct=float(args.acceptance_percent),
            dry_dewpoint_gate_c=float(args.dry_dewpoint_gate_c),
            ratio_std_a_gate=float(args.ratio_std_a_gate),
            pressure_span_warn_hpa=float(args.pressure_span_warn_hpa),
            pressure_point_outlier_hpa=float(args.pressure_point_outlier_hpa),
        ),
    )
    print(f"CO2 S1/S3 source-state discontinuity audit saved: {paths['markdown']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
