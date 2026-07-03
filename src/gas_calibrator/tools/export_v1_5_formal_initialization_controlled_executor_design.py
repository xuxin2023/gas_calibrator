"""Export the offline V1.5 initialization controlled-executor design."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_formal_initialization_controlled_executor_design import (
    write_v1_5_formal_initialization_controlled_executor_design,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export offline/no-COM V1.5 controlled initialization executor design artifacts. "
            "This does not open COM ports, write SN/device IDs, write SENCO, or connect PostgreSQL."
        )
    )
    parser.add_argument(
        "--formal-initialization-blocked-executor-json",
        default=None,
        help="Blocked initialization executor stub JSON that this design builds upon.",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_formal_initialization_controlled_executor_design(
            args.output_dir,
            formal_initialization_blocked_executor_json=args.formal_initialization_blocked_executor_json,
        )
        manifest_path = Path(outputs["manifest"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        print(f"V1.5 initialization controlled executor design export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(
        json.dumps(
            {
                "overall_status": manifest.get("overall_status"),
                "production_state": manifest.get("production_state"),
                "execution_supported": manifest.get("execution_supported"),
                "live_execution_allowed": manifest.get("live_execution_allowed"),
                "opens_com_ports": manifest.get("opens_com_ports"),
                "writes_sn": manifest.get("writes_sn"),
                "writes_coefficients": manifest.get("writes_coefficients"),
                "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    if args.fail_on_review_required and int(manifest.get("review_required_count") or 0):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
