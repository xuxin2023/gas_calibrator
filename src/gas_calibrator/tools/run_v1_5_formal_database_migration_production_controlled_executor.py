"""Preview or explicitly execute V1.5 PostgreSQL 18 migration 002."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Sequence

from ..storage.v1_5_evidence.production_migration import ProductionMigrationError
from ..validation.v1_5_formal_database_migration_dba_readiness import (
    PRODUCTION_DSN_ENV,
)
from ..validation.v1_5_formal_database_migration_production_controlled_executor import (
    authorization_blocked_model,
    build_migration_execution_preview,
    execution_preconnect_hold_model,
    execute_reviewed_production_migration,
    validate_migration_execution_authorization,
    write_migration_execution_outputs,
)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preview or explicitly execute fixed PostgreSQL 18 migration 002. "
            "Default invocation never reads a DSN or connects."
        )
    )
    parser.add_argument("--dba-readiness-json", required=True)
    parser.add_argument("--precheck-sql", required=True)
    parser.add_argument("--apply-sql", required=True)
    parser.add_argument("--postcheck-sql", required=True)
    parser.add_argument("--execution-authorization-json", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--execute-postgresql18-migration", action="store_true")
    parser.add_argument("--fail-on-blocker", action="store_true")

    forbidden = parser.add_argument_group("forbidden target and scope overrides")
    forbidden.add_argument("--dsn", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--dsn-env", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--database-name", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--schema", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--migration-version", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--import-evidence", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--formal-release", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def _read_production_dsn() -> str:
    return os.environ.get(PRODUCTION_DSN_ENV, "")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if any(
        (
            args.dsn,
            args.dsn_env,
            args.database_name,
            args.schema,
            args.migration_version,
            args.import_evidence,
            args.formal_release,
        )
    ):
        print(
            "Target overrides, evidence import, and formal release are forbidden.",
            file=sys.stderr,
        )
        return 2

    try:
        source_paths = {
            "dba_readiness": args.dba_readiness_json,
            "precheck_sql": args.precheck_sql,
            "apply_sql": args.apply_sql,
            "postcheck_sql": args.postcheck_sql,
        }
        preview = build_migration_execution_preview(
            dba_readiness_json=args.dba_readiness_json,
            precheck_sql=args.precheck_sql,
            apply_sql=args.apply_sql,
            postcheck_sql=args.postcheck_sql,
        )
        model = preview
        if args.execute_postgresql18_migration:
            if not args.execution_authorization_json:
                model = authorization_blocked_model(
                    preview, "migration_execution_authorization_json_required"
                )
            else:
                try:
                    validate_migration_execution_authorization(
                        execution_authorization_json=args.execution_authorization_json,
                        preview=preview,
                        source_paths=source_paths,
                    )
                except ProductionMigrationError as exc:
                    model = authorization_blocked_model(preview, str(exc))
                else:
                    dsn = _read_production_dsn()
                    if not dsn:
                        model = {
                            **preview,
                            "overall_status": "migration_execution_dsn_missing",
                            "export_status": "error",
                            "blocker_count": 1,
                            "reasons": [
                                f"production_dsn_env_not_configured:{PRODUCTION_DSN_ENV}"
                            ],
                            "dsn_value_read": True,
                        }
                    else:
                        try:
                            model = execute_reviewed_production_migration(
                                dba_readiness_json=args.dba_readiness_json,
                                precheck_sql=args.precheck_sql,
                                apply_sql=args.apply_sql,
                                postcheck_sql=args.postcheck_sql,
                                execution_authorization_json=(
                                    args.execution_authorization_json
                                ),
                                dsn=dsn,
                            )
                        except ProductionMigrationError as exc:
                            model = execution_preconnect_hold_model(
                                preview, str(exc)
                            )

        outputs = write_migration_execution_outputs(model, args.output_dir)
        print(
            json.dumps(
                {
                    "overall_status": model.get("overall_status"),
                    "execution_attempted": model.get("execution_attempted"),
                    "dsn_value_read": model.get("dsn_value_read"),
                    "connects_postgresql": model.get("connects_postgresql"),
                    "transaction_committed": model.get("transaction_committed"),
                    "commit_uncertain": model.get("commit_uncertain"),
                    "migration_execution_confirmed": model.get(
                        "migration_execution_confirmed"
                    ),
                    "database_import_allowed": model.get("database_import_allowed"),
                    "outputs": {key: str(path) for key, path in outputs.items()},
                },
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )
        if (
            args.execute_postgresql18_migration
            and model.get("migration_execution_confirmed") is not True
        ):
            return 2
        if model.get("blocker_count") and args.fail_on_blocker:
            return 3
        return 0
    except Exception as exc:
        print(
            f"V1.5 PostgreSQL 18 controlled migration failed: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
