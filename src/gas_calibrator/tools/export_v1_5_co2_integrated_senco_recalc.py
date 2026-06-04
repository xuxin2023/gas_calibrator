"""CLI wrapper for V1.5 offline integrated CO2 SENCO recalculation."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_integrated_senco_recalc import write_co2_integrated_senco_recalc_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export offline V1.5 CO2 SENCO1/SENCO3/SENCO5 integrated recalculation artifacts."
    )
    parser.add_argument("--fit-residuals-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--sampling-senco5-snapshot-csv", default="")
    parser.add_argument("--preclear-senco5-snapshot-csv", default="")
    parser.add_argument("--current-senco5-snapshot-csv", default="")
    parser.add_argument("--target-device-id", action="append", default=[])
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_integrated_senco_recalc_report(
        fit_residuals_csv=args.fit_residuals_csv,
        output_dir=args.output_dir,
        sampling_senco5_snapshot_csv=args.sampling_senco5_snapshot_csv or None,
        preclear_senco5_snapshot_csv=args.preclear_senco5_snapshot_csv or None,
        current_senco5_snapshot_csv=args.current_senco5_snapshot_csv or None,
        target_device_ids=tuple(args.target_device_id) if args.target_device_id else ("022", "030", "033", "051"),
    )
    print(f"CO2 integrated SENCO recalculation saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
