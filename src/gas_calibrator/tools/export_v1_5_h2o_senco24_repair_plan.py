"""CLI wrapper for the offline V1.5 H2O SENCO2/SENCO4 repair plan."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.h2o_senco24_repair_plan import (
    H2OSenco24RepairInputs,
    write_h2o_senco24_repair_plan_report,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a no-COM H2O SENCO2/SENCO4/SENCO6 repair plan.")
    parser.add_argument("--original-getco-snapshot-csv", required=True)
    parser.add_argument("--current-getco-snapshot-csv", required=True)
    parser.add_argument("--candidate-device-policy-csv", required=True)
    parser.add_argument("--candidate-payload-preview-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--candidate-residuals-csv", default="")
    parser.add_argument("--target-senco6-c0", type=float, default=0.0)
    parser.add_argument("--target-senco6-c1", type=float, default=1.0)
    parser.add_argument("--target-device-id", action="append", default=[])
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    inputs = H2OSenco24RepairInputs(
        original_getco_snapshot_csv=Path(args.original_getco_snapshot_csv),
        current_getco_snapshot_csv=Path(args.current_getco_snapshot_csv),
        candidate_device_policy_csv=Path(args.candidate_device_policy_csv),
        candidate_payload_preview_csv=Path(args.candidate_payload_preview_csv),
        candidate_residuals_csv=Path(args.candidate_residuals_csv) if args.candidate_residuals_csv else None,
        target_senco6=(float(args.target_senco6_c0), float(args.target_senco6_c1)),
        target_device_ids=tuple(args.target_device_id) if args.target_device_id else ("022", "030", "033", "051"),
    )
    outputs = write_h2o_senco24_repair_plan_report(inputs=inputs, output_dir=args.output_dir)
    print(f"H2O SENCO24 repair plan saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
