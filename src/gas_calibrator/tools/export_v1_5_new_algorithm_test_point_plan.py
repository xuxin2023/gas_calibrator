"""Export the offline V1.5 new-algorithm candidate test point plan."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_algorithm_route_profiles import (
    write_v1_5_new_algorithm_test_point_plan,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export the V1.5 new-algorithm candidate point plan from the route profile JSON. "
            "This is offline/no-write and does not run any route."
        )
    )
    parser.add_argument(
        "--profile-path",
        required=True,
        help="Path to configs/v1_5_algorithm_route_profiles.json.",
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for plan artifacts.")
    parser.add_argument(
        "--profile-id",
        default="absorption_ratio_shadow",
        help="Algorithm profile id to export.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_new_algorithm_test_point_plan(
            profile_path=args.profile_path,
            output_dir=args.output_dir,
            profile_id=args.profile_id,
        )
    except Exception as exc:
        print(f"V1.5 new-algorithm test point plan export failed: {exc}", file=sys.stderr, flush=True)
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
