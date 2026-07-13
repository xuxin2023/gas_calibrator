"""Export the offline V1.5 PostgreSQL 18 production promotion preflight."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from ..validation.v1_5_formal_database_import_production_promotion_preflight import (
    DEFAULT_PRODUCTION_DSN_ENV,
    build_v1_5_formal_database_import_production_promotion_preflight,
    write_v1_5_formal_database_import_production_promotion_preflight_outputs,
)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Review staging proof for a future V1.5 PostgreSQL 18 production import executor."
    )
    parser.add_argument("--staging-import-json", required=True)
    parser.add_argument("--transaction-plan-json", required=True)
    parser.add_argument("--evidence-bundle-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--production-dsn-env", default=DEFAULT_PRODUCTION_DSN_ENV)
    parser.add_argument("--fail-on-blocker", action="store_true")
    parser.add_argument("--dsn", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--execute-production-import", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--apply-migrations", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--formal-release", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.dsn or args.execute_production_import or args.apply_migrations or args.formal_release:
        print("Production connection, migration, import, and release options are forbidden.", file=sys.stderr)
        return 2
    model = build_v1_5_formal_database_import_production_promotion_preflight(
        staging_import_json=args.staging_import_json,
        transaction_plan_json=args.transaction_plan_json,
        evidence_bundle_json=args.evidence_bundle_json,
        production_dsn_env=args.production_dsn_env,
    )
    outputs = write_v1_5_formal_database_import_production_promotion_preflight_outputs(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model.get("overall_status"),
                "blocker_count": model.get("blocker_count"),
                "promotion_preflight_ready": model.get("promotion_preflight_ready"),
                "production_import_execution_allowed": model.get(
                    "production_import_execution_allowed"
                ),
                "connects_postgresql": model.get("connects_postgresql"),
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
