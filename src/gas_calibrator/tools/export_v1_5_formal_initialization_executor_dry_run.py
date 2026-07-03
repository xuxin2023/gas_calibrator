"""Export the offline V1.5 formal initialization executor dry-run review."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_formal_initialization_executor_dry_run import (
    build_v1_5_formal_initialization_executor_dry_run,
    write_v1_5_formal_initialization_executor_dry_run_outputs,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export a no-COM/no-write dry-run review for the V1.5 formal initialization executor. "
            "This does not execute initialization plan commands."
        )
    )
    parser.add_argument("--formal-initialization-plan-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_formal_initialization_executor_dry_run(
            formal_initialization_plan_json=args.formal_initialization_plan_json,
        )
        outputs = write_v1_5_formal_initialization_executor_dry_run_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 formal initialization executor dry-run export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "dry_run_review_allowed": model.get("dry_run_review_allowed"),
                "live_execution_allowed": model.get("live_execution_allowed"),
                "read_only_real_com_execution_allowed": model.get("read_only_real_com_execution_allowed"),
                "controlled_write_execution_allowed": model.get("controlled_write_execution_allowed"),
                "opens_com_ports": model.get("opens_com_ports"),
                "connects_postgresql": model.get("connects_postgresql"),
                "writes_coefficients": model.get("writes_coefficients"),
                "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    if args.fail_on_review_required and int(model.get("review_required_count") or 0):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
