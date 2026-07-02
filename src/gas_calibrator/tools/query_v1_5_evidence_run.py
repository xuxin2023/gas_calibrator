"""Query an imported V1.5 evidence-registry run summary."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Iterable, Optional

from ..storage.v1_5_evidence.repository import (
    query_artifacts_by_sha256,
    query_run_summary,
    query_run_traceability,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query V1.5 evidence-registry run summary.")
    parser.add_argument("--run-id", default="", help="Run id or evidence-registry run database id.")
    parser.add_argument(
        "--traceability",
        action="store_true",
        help="Return full run traceability instead of compact run summary.",
    )
    parser.add_argument(
        "--artifact-sha256",
        default="",
        help="Query artifact index rows by SHA256. Does not require --run-id.",
    )
    parser.add_argument("--dsn", default=None, help="PostgreSQL DSN. Defaults to GAS_CAL_DB_DSN.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    dsn = args.dsn or os.environ.get("GAS_CAL_DB_DSN", "")
    if not dsn:
        print("Missing DSN. Pass --dsn or set GAS_CAL_DB_DSN.", file=sys.stderr, flush=True)
        return 2
    if not args.run_id and not args.artifact_sha256:
        print("Missing query key. Pass --run-id or --artifact-sha256.", file=sys.stderr, flush=True)
        return 2
    try:
        if args.artifact_sha256:
            payload = {
                "artifact_sha256": args.artifact_sha256,
                "rows": query_artifacts_by_sha256(dsn, args.artifact_sha256),
            }
        elif args.traceability:
            payload = query_run_traceability(dsn, args.run_id)
        else:
            payload = {"run_id": args.run_id, "rows": query_run_summary(dsn, args.run_id)}
    except Exception as exc:
        print(f"V1.5 evidence query failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
