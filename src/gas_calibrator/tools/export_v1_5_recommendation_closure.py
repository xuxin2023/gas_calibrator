"""Export the V1.5 recommendation-closure audit."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_recommendation_closure import (
    build_v1_5_recommendation_closure,
    write_v1_5_recommendation_closure,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an offline V1.5 recommendation closure table without touching devices."
    )
    parser.add_argument("--repo-root", default=".", help="V1.5 repository/worktree root.")
    parser.add_argument("--run-dir", default="", help="Optional V1.5 run directory for traceability context.")
    parser.add_argument("--output-dir", required=True, help="Directory for JSON/CSV/Markdown closure artifacts.")
    parser.add_argument(
        "--fail-on-open",
        action="store_true",
        help="Return exit code 2 when any recommendation is still open.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        paths = write_v1_5_recommendation_closure(
            repo_root=args.repo_root,
            run_dir=args.run_dir or None,
            output_dir=args.output_dir,
        )
        model = build_v1_5_recommendation_closure(
            repo_root=args.repo_root,
            run_dir=args.run_dir or None,
        )
        result = {
            "status": model.get("overall_status"),
            "summary_counts": model.get("summary_counts"),
            "json_path": str(Path(paths["json"]).resolve()),
            "markdown_path": str(Path(paths["markdown"]).resolve()),
            "csv_path": str(Path(paths["csv"]).resolve()),
            "physical_boundaries": model.get("physical_boundaries"),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
        if args.fail_on_open and (model.get("summary_counts") or {}).get("open", 0):
            return 2
        return 0
    except Exception as exc:
        print(f"V1.5 recommendation closure export failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
