"""Export the V1.5 dirty-zone audit.

This tool is offline and read-only except for its output directory. It does not
stage, unstage, delete, move, or rewrite repository files.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_dirty_zone_audit import build_dirty_zone_audit, write_dirty_zone_audit


def _read_optional_status_file(path: str | None) -> str | None:
    if not path:
        return None
    return Path(path).read_text(encoding="utf-8-sig")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export a V1.5 clean/root dirty-zone audit.")
    parser.add_argument("--clean-worktree", default=".", help="Official V1.5 clean worktree root.")
    parser.add_argument("--root-workspace", default=None, help="Optional polluted/draft root workspace to classify.")
    parser.add_argument("--output-dir", required=True, help="Output directory for audit artifacts.")
    parser.add_argument("--clean-status-file", default=None, help="Test/replay input for clean git status --short.")
    parser.add_argument("--root-status-file", default=None, help="Test/replay input for root git status --short.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return non-zero if blocker entries exist.")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        audit = build_dirty_zone_audit(
            clean_worktree=args.clean_worktree,
            root_workspace=args.root_workspace,
            clean_status_text=_read_optional_status_file(args.clean_status_file),
            root_status_text=_read_optional_status_file(args.root_status_file),
        )
        outputs = write_dirty_zone_audit(audit, args.output_dir)
    except Exception as exc:
        print(f"V1.5 dirty-zone audit failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {key: str(value.resolve()) for key, value in outputs.items()}
    payload["status"] = audit.status
    payload["blocker_count"] = audit.summary["blocker_count"]
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if args.fail_on_blocker and audit.summary["blocker_count"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
