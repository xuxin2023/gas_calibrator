"""CLI wrapper for the offline V1.5 CO2 SENCO algorithm audit."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_senco_algorithm_audit import write_co2_senco_algorithm_audit


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export offline V1.5 CO2 SENCO algorithm audit artifacts.")
    parser.add_argument("--candidate-dir", required=True)
    parser.add_argument("--verification-summary-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--write-dir", default="")
    parser.add_argument(
        "--getco-snapshot-csv",
        default="",
        help="Optional read-only GETCO1-9 snapshot rows CSV for output-chain isolation.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_senco_algorithm_audit(
        candidate_dir=args.candidate_dir,
        verification_summary_csv=args.verification_summary_csv,
        output_dir=args.output_dir,
        write_dir=args.write_dir or None,
        getco_snapshot_csv=args.getco_snapshot_csv or None,
    )
    print(f"CO2 SENCO algorithm audit saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
