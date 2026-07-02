"""Export the offline V1.5 PostgreSQL 18 database import command contract."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_formal_database_import_command_contract import (
    DEFAULT_DSN_ENV,
    DEFAULT_REQUESTED_COMMAND_MODULE,
    build_v1_5_formal_database_import_command_contract,
    write_v1_5_formal_database_import_command_contract_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an offline V1.5 PostgreSQL 18 database import command contract. "
            "This never connects to PostgreSQL, applies migrations, or imports data."
        )
    )
    parser.add_argument(
        "--formal-database-import-authorization-json",
        required=True,
        help="Existing formal database import authorization JSON.",
    )
    parser.add_argument(
        "--formal-database-import-preflight-json",
        required=True,
        help="Existing formal database import preflight JSON.",
    )
    parser.add_argument(
        "--archive-closure-json",
        default="",
        help="Optional formal archive closure index JSON.",
    )
    parser.add_argument(
        "--evidence-bundle-json",
        default="",
        help="Optional formal evidence bundle JSON.",
    )
    parser.add_argument(
        "--dsn-env",
        default=DEFAULT_DSN_ENV,
        help="Environment variable name that the future import command must use for PostgreSQL DSN.",
    )
    parser.add_argument(
        "--requested-command-module",
        default=DEFAULT_REQUESTED_COMMAND_MODULE,
        help="Future controlled import command module reviewed by this offline contract.",
    )
    parser.add_argument("--output-dir", required=True, help="Directory for command-contract outputs.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    parser.add_argument(
        "--fail-on-review-required",
        action="store_true",
        help="Return exit code 3 when non-blocking review items remain.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_formal_database_import_command_contract(
            formal_database_import_authorization_json=args.formal_database_import_authorization_json,
            formal_database_import_preflight_json=args.formal_database_import_preflight_json,
            archive_closure_json=args.archive_closure_json or None,
            evidence_bundle_json=args.evidence_bundle_json or None,
            dsn_env=args.dsn_env,
            requested_command_module=args.requested_command_module,
        )
        outputs = write_v1_5_formal_database_import_command_contract_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 formal database import command contract export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "overall_status": model.get("overall_status"),
        "blocker_count": model.get("blocker_count"),
        "review_required_count": model.get("review_required_count"),
        "command_contract_ready": model.get("command_contract_ready"),
        "real_import_execution_allowed": model.get("real_import_execution_allowed"),
        "database_import_allowed": model.get("database_import_allowed"),
        "command_contract_json": str(outputs["json"].resolve()),
        "command_contract_markdown": str(outputs["markdown"].resolve()),
        "checks_csv": str(outputs["checks_csv"].resolve()),
        "physical_boundaries": {
            "connects_postgresql": model.get("connects_postgresql"),
            "opens_com_ports": model.get("opens_com_ports"),
            "controls_water_or_gas_routes": model.get("controls_water_or_gas_routes"),
            "writes_sn": model.get("writes_sn"),
            "writes_device_id": model.get("writes_device_id"),
            "writes_coefficients": model.get("writes_coefficients"),
            "applies_migrations": model.get("applies_migrations"),
            "database_import_attempted": model.get("database_import_attempted"),
            "database_written": model.get("database_written"),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    if args.fail_on_blocker and int(model.get("blocker_count") or 0) > 0:
        return 2
    if args.fail_on_review_required and int(model.get("review_required_count") or 0) > 0:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
