"""Export V1.5 CO2 S1/S3 ratio-target mapping audit artifacts."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_ratio_target_mapping_audit import (
    write_co2_s13_ratio_target_mapping_audit,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selected-residual-state-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_s13_ratio_target_mapping_audit(
        selected_residual_state_csv=args.selected_residual_state_csv,
        output_dir=args.output_dir,
    )
    print(f"CO2 S1/S3 ratio-target mapping audit saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
