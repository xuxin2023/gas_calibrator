"""Query the isolated V1.5 PostgreSQL 18 staging identity index."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Sequence

from ..storage.v1_5_evidence.staging_import import query_staging_identity


DEFAULT_DSN_ENV = "V1_5_POSTGRES_STAGING_DSN"


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read V1.5 staging rows by SN, device_code, protocol device ID, or run ID."
    )
    parser.add_argument(
        "--query-kind",
        required=True,
        choices=("sn_code", "device_code", "protocol_device_id", "run_id"),
    )
    parser.add_argument("--query-value", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--core-schema", default="v1_5_core_staging")
    parser.add_argument("--evidence-schema", default="v1_5_evidence_staging")
    parser.add_argument("--dsn-env", default=DEFAULT_DSN_ENV)
    parser.add_argument("--execute-staging-query", action="store_true")
    parser.add_argument("--dsn", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--production-schema", default=None, help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.dsn or args.production_schema:
        print("Production DSN/schema options are forbidden in the staging query tool.", file=sys.stderr)
        return 2
    if not args.execute_staging_query:
        print("Staging query is locked; pass --execute-staging-query explicitly.", file=sys.stderr)
        return 2
    if "STAGING" not in args.dsn_env.upper() and "TEST" not in args.dsn_env.upper():
        print("Staging query DSN environment name must be staging/test scoped.", file=sys.stderr)
        return 2
    dsn = os.environ.get(args.dsn_env, "")
    if not dsn:
        print(f"Staging query DSN environment is not configured: {args.dsn_env}", file=sys.stderr)
        return 2
    try:
        result = query_staging_identity(
            dsn=dsn,
            core_schema=args.core_schema,
            evidence_schema=args.evidence_schema,
            query_kind=args.query_kind,
            query_value=args.query_value,
        )
        payload = {
            "schema": "v1_5_formal_database_import_staging_query_v1",
            **result,
            "connects_postgresql": True,
            "query_only": True,
            "database_written": False,
            "production_database_written": False,
            "database_import_allowed": False,
            "formal_release_allowed": False,
            "opens_com_ports": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        }
        target = Path(args.output_json)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8-sig"
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), flush=True)
        return 0
    except Exception as exc:
        print(f"V1.5 staging query failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
