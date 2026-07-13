"""Preview or execute the isolated V1.5 PostgreSQL 18 staging import."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Sequence

from ..validation.v1_5_formal_database_import_staging_executor import (
    CONFIRMATION_TEXT,
    DEFAULT_DSN_ENV,
    build_staging_import_preview,
    execute_reviewed_staging_import,
    write_staging_import_outputs,
)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preview or execute one atomic V1.5 PostgreSQL 18 staging-only evidence import. "
            "Production database import remains locked."
        )
    )
    parser.add_argument("--transaction-plan-json", required=True)
    parser.add_argument("--evidence-bundle-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--core-schema", default="v1_5_core_staging")
    parser.add_argument("--evidence-schema", default="v1_5_evidence_staging")
    parser.add_argument("--dsn-env", default=DEFAULT_DSN_ENV)
    parser.add_argument("--execute-staging-import", action="store_true")
    parser.add_argument("--initialize-staging-schemas", action="store_true")
    parser.add_argument("--authorization-id", default="")
    parser.add_argument("--operator", default="")
    parser.add_argument("--reviewer", default="")
    parser.add_argument("--approver", default="")
    parser.add_argument("--operator-confirmation-text", default="")
    parser.add_argument("--fail-on-blocker", action="store_true")
    forbidden = parser.add_argument_group("forbidden production options")
    forbidden.add_argument("--dsn", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--production-schema", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--allow-production-import", action="store_true", help=argparse.SUPPRESS)
    forbidden.add_argument("--formal-release", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.dsn or args.production_schema or args.allow_production_import or args.formal_release:
        print("Production database and release options are forbidden in the staging executor.", file=sys.stderr)
        return 2
    try:
        preview = build_staging_import_preview(
            transaction_plan_json=args.transaction_plan_json,
            evidence_bundle_json=args.evidence_bundle_json,
            core_schema=args.core_schema,
            evidence_schema=args.evidence_schema,
            dsn_env=args.dsn_env,
        )
        model = preview
        if args.execute_staging_import:
            dsn = os.environ.get(args.dsn_env, "")
            if not dsn:
                model = {
                    **preview,
                    "overall_status": "staging_import_dsn_missing",
                    "export_status": "error",
                    "blocker_count": 1,
                    "reasons": list(preview.get("reasons") or [])
                    + [f"staging_dsn_env_not_configured:{args.dsn_env}"],
                }
            else:
                model = execute_reviewed_staging_import(
                    preview=preview,
                    transaction_plan_json=args.transaction_plan_json,
                    evidence_bundle_json=args.evidence_bundle_json,
                    dsn=dsn,
                    authorization_id=args.authorization_id,
                    operator=args.operator,
                    reviewer=args.reviewer,
                    approver=args.approver,
                    operator_confirmation_text=args.operator_confirmation_text,
                    initialize_staging_schemas=args.initialize_staging_schemas,
                )
        outputs = write_staging_import_outputs(model, args.output_dir)
        print(
            json.dumps(
                {
                    "overall_status": model.get("overall_status"),
                    "planned_device_count": model.get("planned_device_count"),
                    "transaction_committed": model.get("transaction_committed"),
                    "idempotent": model.get("idempotent"),
                    "connects_postgresql": model.get("connects_postgresql"),
                    "staging_database_written": model.get("staging_database_written"),
                    "production_database_written": model.get("production_database_written"),
                    "formal_release_allowed": model.get("formal_release_allowed"),
                    "outputs": {key: str(path) for key, path in outputs.items()},
                },
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )
        if model.get("blocker_count") and args.fail_on_blocker:
            return 3
        if args.execute_staging_import and model.get("transaction_committed") is not True:
            return 2
        return 0
    except Exception as exc:
        print(f"V1.5 PostgreSQL 18 staging import failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
