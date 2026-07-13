"""Preview or explicitly execute the V1.5 PostgreSQL 18 production import."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Sequence

from ..validation.v1_5_formal_database_import_production_controlled_executor import (
    DEFAULT_PRODUCTION_DSN_ENV,
    ProductionImportError,
    authorization_blocked_model,
    build_production_import_preview,
    execute_reviewed_production_import,
    validate_execution_authorization,
    write_production_import_outputs,
)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preview or explicitly execute one atomic V1.5 PostgreSQL 18 production "
            "evidence import. Default invocation never reads a DSN or connects."
        )
    )
    parser.add_argument("--promotion-preflight-json", required=True)
    parser.add_argument("--transaction-plan-json", required=True)
    parser.add_argument("--evidence-bundle-json", required=True)
    parser.add_argument("--migration-execution-json", required=True)
    parser.add_argument("--execution-authorization-json", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--execute-production-import", action="store_true")
    parser.add_argument("--fail-on-blocker", action="store_true")

    forbidden = parser.add_argument_group("forbidden target override options")
    forbidden.add_argument("--dsn", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--dsn-env", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--database-name", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--core-schema", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--evidence-schema", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--apply-migrations", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--initialize-schema", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--formal-release", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def _read_production_dsn() -> str:
    return os.environ.get(DEFAULT_PRODUCTION_DSN_ENV, "")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if any(
        (
            args.dsn,
            args.dsn_env,
            args.database_name,
            args.core_schema,
            args.evidence_schema,
            args.apply_migrations,
            args.initialize_schema,
            args.formal_release,
        )
    ):
        print(
            "Production target overrides, migrations, and formal release are forbidden.",
            file=sys.stderr,
        )
        return 2

    try:
        preview = build_production_import_preview(
            promotion_preflight_json=args.promotion_preflight_json,
            transaction_plan_json=args.transaction_plan_json,
            evidence_bundle_json=args.evidence_bundle_json,
            migration_execution_json=args.migration_execution_json,
        )
        model = preview
        if args.execute_production_import:
            if not args.execution_authorization_json:
                model = authorization_blocked_model(
                    preview, "execution_authorization_json_required"
                )
            else:
                try:
                    validate_execution_authorization(
                        execution_authorization_json=args.execution_authorization_json,
                        preview=preview,
                        promotion_preflight_json=args.promotion_preflight_json,
                        transaction_plan_json=args.transaction_plan_json,
                        evidence_bundle_json=args.evidence_bundle_json,
                        migration_execution_json=args.migration_execution_json,
                    )
                except ProductionImportError as exc:
                    model = authorization_blocked_model(preview, str(exc))
                else:
                    dsn = _read_production_dsn()
                    if not dsn:
                        model = {
                            **preview,
                            "overall_status": "production_import_dsn_missing",
                            "export_status": "error",
                            "blocker_count": 1,
                            "reasons": [
                                f"production_dsn_env_not_configured:{DEFAULT_PRODUCTION_DSN_ENV}"
                            ],
                            "dsn_value_read": True,
                        }
                    else:
                        model = execute_reviewed_production_import(
                            promotion_preflight_json=args.promotion_preflight_json,
                            transaction_plan_json=args.transaction_plan_json,
                            evidence_bundle_json=args.evidence_bundle_json,
                            migration_execution_json=args.migration_execution_json,
                            execution_authorization_json=args.execution_authorization_json,
                            dsn=dsn,
                        )

        outputs = write_production_import_outputs(model, args.output_dir)
        print(
            json.dumps(
                {
                    "overall_status": model.get("overall_status"),
                    "planned_device_count": model.get("planned_device_count"),
                    "execution_attempted": model.get("execution_attempted"),
                    "transaction_committed": model.get("transaction_committed"),
                    "idempotent": model.get("idempotent"),
                    "dsn_value_read": model.get("dsn_value_read"),
                    "connects_postgresql": model.get("connects_postgresql"),
                    "production_database_written": model.get(
                        "production_database_written"
                    ),
                    "formal_release_allowed": model.get("formal_release_allowed"),
                    "outputs": {key: str(path) for key, path in outputs.items()},
                },
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )
        if args.execute_production_import and model.get("transaction_committed") is not True:
            return 2
        if model.get("blocker_count") and args.fail_on_blocker:
            return 3
        return 0
    except Exception as exc:
        print(
            f"V1.5 PostgreSQL 18 production controlled import failed: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
