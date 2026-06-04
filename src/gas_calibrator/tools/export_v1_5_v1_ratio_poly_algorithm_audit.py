"""CLI wrapper for the offline V1 ratio-polynomial algorithm audit."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.v1_ratio_poly_algorithm_audit import write_v1_ratio_poly_algorithm_audit


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export offline V1 ratio-polynomial algorithm audit artifacts.")
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_v1_ratio_poly_algorithm_audit(output_dir=args.output_dir)
    print(f"V1 ratio-poly algorithm audit saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
