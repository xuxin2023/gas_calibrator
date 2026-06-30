"""Export an offline V1.5 CO2 point-state bridge closure review."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_state_bridge_closure import (
    DEFAULT_ACCEPTANCE_PERCENT,
    DEFAULT_MIN_BRIDGE_SUPPORT,
    DEFAULT_MIN_RELATIVE_TARGET_PPM,
    write_co2_s13_state_bridge_closure,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corrected-residuals-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--acceptance-percent", type=float, default=DEFAULT_ACCEPTANCE_PERCENT)
    parser.add_argument("--min-relative-target-ppm", type=float, default=DEFAULT_MIN_RELATIVE_TARGET_PPM)
    parser.add_argument("--min-bridge-support", type=int, default=DEFAULT_MIN_BRIDGE_SUPPORT)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_s13_state_bridge_closure(
        corrected_residuals_csv=args.corrected_residuals_csv,
        output_dir=args.output_dir,
        acceptance_percent=float(args.acceptance_percent),
        min_relative_target_ppm=float(args.min_relative_target_ppm),
        min_bridge_support=int(args.min_bridge_support),
    )
    print(f"CO2 state bridge closure review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
