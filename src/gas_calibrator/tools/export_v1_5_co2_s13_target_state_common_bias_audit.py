"""Export V1.5 CO2 target-state/common-bias audit from offline S1/S3 evidence."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_target_state_common_bias_audit import (
    Co2TargetStateCommonBiasAuditConfig,
    write_co2_s13_target_state_common_bias_audit,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fit-points-csv", required=True)
    parser.add_argument("--selected-residuals-csv", required=True)
    parser.add_argument("--best-by-device-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--acceptance-pct", type=float, default=1.0)
    parser.add_argument("--min-relative-target-ppm", type=float, default=50.0)
    parser.add_argument("--dry-dewpoint-gate-c", type=float, default=-28.0)
    parser.add_argument("--ratio-std-a-gate", type=float, default=0.0005)
    parser.add_argument("--command-c0-decimals", type=int, default=3)
    parser.add_argument("--command-c1-decimals", type=int, default=3)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = Co2TargetStateCommonBiasAuditConfig(
        acceptance_pct=float(args.acceptance_pct),
        min_relative_target_ppm=float(args.min_relative_target_ppm),
        dry_dewpoint_gate_c=float(args.dry_dewpoint_gate_c),
        ratio_std_a_gate=float(args.ratio_std_a_gate),
        command_c0_decimals=int(args.command_c0_decimals),
        command_c1_decimals=int(args.command_c1_decimals),
    )
    outputs = write_co2_s13_target_state_common_bias_audit(
        fit_points_csv=args.fit_points_csv,
        selected_residuals_csv=args.selected_residuals_csv,
        best_by_device_csv=args.best_by_device_csv,
        output_dir=args.output_dir,
        cfg=cfg,
    )
    print(f"CO2 target-state/common-bias audit saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
