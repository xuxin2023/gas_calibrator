"""Run the deliberately blocked PostgreSQL 18 transaction executor surface."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_formal_database_import_transaction_blocked_executor import (
    build_v1_5_formal_database_import_transaction_blocked_executor,
    write_v1_5_formal_database_import_transaction_blocked_executor_outputs,
)


FORBIDDEN_FLAGS = {
    "--execute",
    "--execute-controlled-import",
    "--dsn",
    "--dsn-env-value",
    "--authorization-json",
    "--archive-closure-json",
    "--evidence-bundle-json",
    "--planned-devices-json",
    "--operator",
    "--reviewer",
    "--approver",
    "--apply-migrations",
}


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Emit blocked transaction-executor evidence only.")
    parser.add_argument("--transaction-plan-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocked", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    raw = list(argv) if argv is not None else sys.argv[1:]
    if any(item.split("=", 1)[0] in FORBIDDEN_FLAGS for item in raw):
        print("Real PostgreSQL import inputs are forbidden by this blocked executor.", file=sys.stderr)
        return 2
    args = _parse_args(raw)
    try:
        model = build_v1_5_formal_database_import_transaction_blocked_executor(
            transaction_plan_json=args.transaction_plan_json
        )
        outputs = write_v1_5_formal_database_import_transaction_blocked_executor_outputs(
            model, args.output_dir
        )
    except Exception as exc:
        print(f"V1.5 database transaction blocked executor failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "blocked_executor_ready": model.get("blocked_executor_ready"),
                "connects_postgresql": model.get("connects_postgresql"),
                "database_written": model.get("database_written"),
                "blocked_executor_json": str(outputs["json"].resolve()),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if args.fail_on_blocked else 0


if __name__ == "__main__":
    raise SystemExit(main())
