"""Write the canonical simulated V1.5 formal evidence package."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_canonical_evidence import write_canonical_v1_5_evidence_package


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a simulated V1.5 canonical evidence package without device I/O."
    )
    parser.add_argument("--output-dir", required=True, help="Directory for the canonical package.")
    parser.add_argument("--run-id", default="v1_5_canonical_900ppm_open_flow")
    parser.add_argument("--today", default="2026-05-24", help="Contract date, YYYY-MM-DD.")
    parser.add_argument("--skip-reports", action="store_true", help="Only generate evidence bundle, not reports.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_canonical_v1_5_evidence_package(
            args.output_dir,
            run_id=args.run_id,
            today=args.today,
            include_reports=not bool(args.skip_reports),
        )
    except Exception as exc:
        print(f"Prepare V1.5 canonical evidence package failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
