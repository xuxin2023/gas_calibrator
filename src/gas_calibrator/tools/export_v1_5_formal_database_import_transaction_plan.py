"""Export the offline V1.5 PostgreSQL 18 import transaction plan."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_formal_database_import_transaction_plan import (
    build_v1_5_formal_database_import_transaction_plan,
    write_v1_5_formal_database_import_transaction_plan_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a no-connect, no-SQL PostgreSQL 18 transaction plan.")
    parser.add_argument("--formal-database-dry-run-json", required=True)
    parser.add_argument("--formal-database-import-controlled-executor-design-json", required=True)
    parser.add_argument("--formal-database-import-command-contract-json", default="")
    parser.add_argument("--formal-database-import-authorization-json", default="")
    parser.add_argument("--formal-database-import-preflight-json", default="")
    parser.add_argument("--archive-closure-json", default="")
    parser.add_argument("--evidence-bundle-json", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_formal_database_import_transaction_plan(
            formal_database_dry_run_json=args.formal_database_dry_run_json,
            formal_database_import_controlled_executor_design_json=(
                args.formal_database_import_controlled_executor_design_json
            ),
            formal_database_import_command_contract_json=args.formal_database_import_command_contract_json or None,
            formal_database_import_authorization_json=args.formal_database_import_authorization_json or None,
            formal_database_import_preflight_json=args.formal_database_import_preflight_json or None,
            archive_closure_json=args.archive_closure_json or None,
            evidence_bundle_json=args.evidence_bundle_json or None,
        )
        outputs = write_v1_5_formal_database_import_transaction_plan_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 database transaction plan export failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "transaction_plan_contract_ready": model.get("transaction_plan_contract_ready"),
                "production_transaction_package_ready": model.get("production_transaction_package_ready"),
                "connects_postgresql": model.get("connects_postgresql"),
                "database_written": model.get("database_written"),
                "transaction_plan_json": str(outputs["json"].resolve()),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if args.fail_on_blocker and int(model.get("blocker_count") or 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())
