"""Export V1.5 CO2 S1/S3 low-end model correction review artifacts."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Optional

from ..validation.co2_s13_low_end_model_correction_review import (
    write_co2_s13_low_end_model_correction_review,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-structure-dir", required=True)
    parser.add_argument("--anchor-target-audit-dir", required=True)
    parser.add_argument("--residual-root-cause-dir", default="")
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    outputs = write_co2_s13_low_end_model_correction_review(
        model_structure_dir=args.model_structure_dir,
        anchor_target_audit_dir=args.anchor_target_audit_dir,
        residual_root_cause_dir=args.residual_root_cause_dir or None,
        output_dir=args.output_dir,
    )
    print(f"CO2 S1/S3 low-end model correction review saved: {outputs['markdown']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
