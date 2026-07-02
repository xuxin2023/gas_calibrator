"""CLI wrapper for offline V1.5 CO2 training-scope review."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_training_scope_review import write_co2_training_scope_review


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export offline V1.5 CO2 training-scope review artifacts.")
    parser.add_argument("--points-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-device-id", required=True)
    parser.add_argument("--old-source-set", default="old_fulltemp_prewrite")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_training_scope_review(
        points_csv=args.points_csv,
        output_dir=args.output_dir,
        target_device_id=args.target_device_id,
        old_source_set=args.old_source_set,
    )
    print(f"CO2 training-scope review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
