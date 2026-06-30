"""Export V1.5 CO2 S1/S3 source-state repair fit review artifacts."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from gas_calibrator.validation.co2_s13_source_state_repair_fit_review import (
    write_co2_s13_source_state_repair_fit_review,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-state-audit-dir", required=True)
    parser.add_argument(
        "--strategy-dir",
        action="append",
        default=[],
        help="Strategy review directory. May be passed as label=path.",
    )
    parser.add_argument("--enhanced-dir", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--acceptance-percent", type=float, default=1.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_s13_source_state_repair_fit_review(
        source_state_audit_dir=args.source_state_audit_dir,
        strategy_dirs=args.strategy_dir,
        enhanced_dir=args.enhanced_dir or None,
        output_dir=args.output_dir,
        acceptance_percent=float(args.acceptance_percent),
    )
    print(f"CO2 S1/S3 source-state repair fit review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
