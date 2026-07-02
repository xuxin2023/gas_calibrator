"""Create/update the PostgreSQL schema for the V1.5 evidence registry."""

from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable, Optional

from ..storage.v1_5_evidence.repository import apply_migrations, mask_dsn
from ..storage.v1_5_evidence.schema import load_migrations


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply V1.5 evidence-registry PostgreSQL migrations without touching devices."
    )
    parser.add_argument("--dsn", default=None, help="PostgreSQL DSN. Defaults to GAS_CAL_DB_DSN.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate that migrations are present, but do not connect to PostgreSQL.",
    )
    parser.add_argument(
        "--print-sql",
        action="store_true",
        help="Print migration SQL for DBA review; does not require a DSN when used with --dry-run.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    migrations = load_migrations()
    if args.print_sql:
        for migration in migrations:
            print(f"-- migration: {migration.version}")
            print(migration.sql)
    if args.dry_run:
        print(
            f"V1.5 evidence migrations available: {', '.join(item.version for item in migrations)}",
            flush=True,
        )
        return 0

    dsn = args.dsn or os.environ.get("GAS_CAL_DB_DSN", "")
    if not dsn:
        print("Missing DSN. Pass --dsn or set GAS_CAL_DB_DSN.", file=sys.stderr, flush=True)
        return 2
    try:
        applied = apply_migrations(dsn)
    except Exception as exc:
        print(f"V1.5 evidence migration failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(
        f"V1.5 evidence migrations applied: {', '.join(applied)} ({mask_dsn(dsn)})",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

