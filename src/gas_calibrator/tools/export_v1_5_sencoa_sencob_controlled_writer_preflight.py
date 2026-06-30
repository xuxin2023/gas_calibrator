"""Export no-write V1.5 SENCOA/SENCOB controlled-writer preflight artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_sencoa_sencob_controlled_writer_preflight import (
    MIN_COMMAND_GAP_S,
    write_v1_5_sencoa_sencob_controlled_writer_preflight,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export offline/no-write V1.5 SENCOA/SENCOB controlled-writer preflight artifacts. "
            "This does not open COM ports or write coefficients."
        )
    )
    parser.add_argument("--profile-path", required=True, help="V1.5 algorithm route profile JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for preflight artifacts.")
    parser.add_argument("--profile-id", default="absorption_ratio_shadow")
    parser.add_argument("--payload-review", default="", help="Optional reviewed SENCOA/SENCOB payload CSV.")
    parser.add_argument("--old-snapshot-json", default="", help="Optional old GETCOA/GETCOB snapshot JSON.")
    parser.add_argument("--future-command-gap-s", type=float, default=MIN_COMMAND_GAP_S)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_sencoa_sencob_controlled_writer_preflight(
            Path(args.profile_path),
            Path(args.output_dir),
            payload_review_path=Path(args.payload_review) if args.payload_review else None,
            old_snapshot_json=Path(args.old_snapshot_json) if args.old_snapshot_json else None,
            profile_id=str(args.profile_id),
            future_command_gap_s=float(args.future_command_gap_s),
        )
    except Exception as exc:
        print(f"V1.5 SENCOA/SENCOB controlled-writer preflight export failed: {exc}", file=sys.stderr, flush=True)
        return 2
    print(json.dumps(outputs, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
