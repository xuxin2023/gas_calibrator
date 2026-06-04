"""CLI wrapper for the offline V1.5 CO2 SENCO1/SENCO3 repair plan."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.co2_senco13_repair_plan import (
    Senco13RepairInputs,
    write_co2_senco13_repair_plan_report,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a no-COM CO2 SENCO1/SENCO3 mixed-state repair plan.")
    parser.add_argument("--original-getco-snapshot-csv", required=True)
    parser.add_argument("--first-pair-write-summary-csv", required=True)
    parser.add_argument("--latest-s1-write-summary-csv", required=True)
    parser.add_argument("--integrated-recalc-summary-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--preclear-senco5-snapshot-csv", default="")
    parser.add_argument("--postclear-senco5-snapshot-csv", default="")
    parser.add_argument("--target-scenario", default="force_neutral_senco5")
    parser.add_argument("--target-senco5-c0", type=float, default=0.0)
    parser.add_argument("--target-senco5-c1", type=float, default=1.0)
    parser.add_argument("--target-device-id", action="append", default=[])
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    inputs = Senco13RepairInputs(
        original_getco_snapshot_csv=Path(args.original_getco_snapshot_csv),
        first_pair_write_summary_csv=Path(args.first_pair_write_summary_csv),
        latest_s1_write_summary_csv=Path(args.latest_s1_write_summary_csv),
        integrated_recalc_summary_csv=Path(args.integrated_recalc_summary_csv),
        preclear_senco5_snapshot_csv=Path(args.preclear_senco5_snapshot_csv) if args.preclear_senco5_snapshot_csv else None,
        postclear_senco5_snapshot_csv=Path(args.postclear_senco5_snapshot_csv) if args.postclear_senco5_snapshot_csv else None,
        target_scenario=args.target_scenario,
        target_senco5=(float(args.target_senco5_c0), float(args.target_senco5_c1)),
        target_device_ids=tuple(args.target_device_id) if args.target_device_id else ("022", "030", "033", "051"),
    )
    outputs = write_co2_senco13_repair_plan_report(inputs=inputs, output_dir=args.output_dir)
    print(f"CO2 SENCO13 repair plan saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
