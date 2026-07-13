"""Export the no-connect V1.5 PostgreSQL 18 DBA migration packet."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from ..validation.v1_5_formal_database_migration_dba_readiness import (
    build_v1_5_formal_database_migration_dba_readiness,
    write_v1_5_formal_database_migration_dba_readiness_outputs,
)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a no-connect DBA packet for V1.5 PostgreSQL 18 migration 002."
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    forbidden = parser.add_argument_group("forbidden execution options")
    forbidden.add_argument("--dsn", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--dsn-env", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--execute", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--apply-migrations", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--production-import", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if any(
        (
            args.dsn,
            args.dsn_env,
            args.execute,
            args.apply_migrations,
            args.production_import,
        )
    ):
        print(
            "DSN, database connection, migration execution, and production import are forbidden.",
            file=sys.stderr,
        )
        return 2
    model = build_v1_5_formal_database_migration_dba_readiness()
    outputs = write_v1_5_formal_database_migration_dba_readiness_outputs(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "blocker_count": model.get("blocker_count"),
                "dba_packet_ready": model.get("dba_packet_ready"),
                "connects_postgresql": model.get("connects_postgresql"),
                "applies_migrations": model.get("applies_migrations"),
                "migration_execution_allowed": model.get(
                    "migration_execution_allowed"
                ),
                "outputs": {key: str(path) for key, path in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    return 3 if args.fail_on_blocker and model.get("blocker_count") else 0


if __name__ == "__main__":
    raise SystemExit(main())
