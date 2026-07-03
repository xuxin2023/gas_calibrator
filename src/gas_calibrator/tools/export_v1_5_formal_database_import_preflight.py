"""Export the offline V1.5 PostgreSQL 18 database import preflight."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_formal_database_import_preflight import (
    DEFAULT_DSN_ENV,
    build_v1_5_formal_database_import_preflight,
    write_v1_5_formal_database_import_preflight_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an offline V1.5 PostgreSQL 18 database import preflight. "
            "This never connects to PostgreSQL, applies migrations, or imports data."
        )
    )
    parser.add_argument(
        "--formal-database-dry-run-json",
        required=True,
        help="Existing formal database dry-run JSON used as the schema/insert contract input.",
    )
    parser.add_argument(
        "--dsn",
        default="",
        help="Optional DSN value for presence/fingerprint review only. It is never used to connect.",
    )
    parser.add_argument(
        "--dsn-env",
        default=DEFAULT_DSN_ENV,
        help="Environment variable name that would hold the production DSN. Defaults to V1_5_POSTGRES_DSN.",
    )
    parser.add_argument("--output-dir", required=True, help="Directory for preflight outputs.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    parser.add_argument(
        "--fail-on-review-required",
        action="store_true",
        help="Return exit code 3 when non-blocking review items remain, such as missing DSN configuration.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_formal_database_import_preflight(
            formal_database_dry_run_json=args.formal_database_dry_run_json,
            dsn=args.dsn or None,
            dsn_env=args.dsn_env,
        )
        outputs = write_v1_5_formal_database_import_preflight_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 formal database import preflight export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "overall_status": model.get("overall_status"),
        "blocker_count": model.get("blocker_count"),
        "review_required_count": model.get("review_required_count"),
        "production_backend": model.get("production_backend"),
        "production_postgresql_major": model.get("production_postgresql_major"),
        "dsn_configured": model.get("dsn_configured"),
        "database_import_allowed": model.get("database_import_allowed"),
        "preflight_json": str(outputs["json"].resolve()),
        "preflight_markdown": str(outputs["markdown"].resolve()),
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
