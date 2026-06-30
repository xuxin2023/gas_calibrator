"""Export the offline V1.5 SENCOA/SENCOB writer design review."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_sencoa_sencob_writer_design import (
    write_v1_5_sencoa_sencob_writer_design_review,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export offline/no-write V1.5 SENCOA/SENCOB R0(T) writer design artifacts. "
            "This does not open COM ports or write coefficients."
        )
    )
    parser.add_argument("--profile-path", required=True, help="Path to v1_5_algorithm_route_profiles.json.")
    parser.add_argument("--output-dir", required=True, help="Output directory for design artifacts.")
    parser.add_argument(
        "--profile-id",
        default="absorption_ratio_shadow",
        help="Algorithm profile id to export.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_sencoa_sencob_writer_design_review(
            profile_path=args.profile_path,
            output_dir=args.output_dir,
            profile_id=args.profile_id,
        )
    except Exception as exc:
        print(f"V1.5 SENCOA/SENCOB writer design export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(
        json.dumps(
            {key: str(Path(value).resolve()) for key, value in outputs.items()},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
