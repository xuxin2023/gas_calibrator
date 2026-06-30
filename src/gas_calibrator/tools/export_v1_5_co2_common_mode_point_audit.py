"""CLI wrapper for the offline V1.5 CO2 common-mode point audit."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_common_mode_point_audit import (
    AuditInputs,
    write_co2_common_mode_point_audit_report,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export offline V1.5 CO2 common-mode point audit artifacts.")
    parser.add_argument("--fit-points-csv", required=True)
    parser.add_argument("--predictions-csv", required=True)
    parser.add_argument("--recommendation-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_common_mode_point_audit_report(
        inputs=AuditInputs(
            fit_points_csv=args.fit_points_csv,
            predictions_csv=args.predictions_csv,
            recommendation_csv=args.recommendation_csv,
        ),
        output_dir=args.output_dir,
    )
    print(f"CO2 common-mode point audit saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
