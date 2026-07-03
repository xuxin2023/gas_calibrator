"""Export the offline V1.5 PostgreSQL 18 database import authorization guard."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_formal_database_import_authorization import (
    build_v1_5_formal_database_import_authorization,
    write_v1_5_formal_database_import_authorization_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an offline V1.5 PostgreSQL 18 database import authorization guard. "
            "This never connects to PostgreSQL, applies migrations, or imports data."
        )
    )
    parser.add_argument(
        "--formal-database-import-preflight-json",
        required=True,
        help="Existing formal database import preflight JSON.",
    )
    parser.add_argument(
        "--archive-closure-json",
        default="",
        help="Optional formal archive closure index JSON required before real import authorization.",
    )
    parser.add_argument("--operator", default="", help="Operator label for manual import authorization.")
    parser.add_argument("--reviewer", default="", help="Reviewer label for manual import authorization.")
    parser.add_argument("--approver", default="", help="Approver label for manual import authorization.")
    parser.add_argument("--authorization-id", default="", help="Manual database import authorization identifier.")
    parser.add_argument("--output-dir", required=True, help="Directory for authorization guard outputs.")
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
        model = build_v1_5_formal_database_import_authorization(
            formal_database_import_preflight_json=args.formal_database_import_preflight_json,
            archive_closure_json=args.archive_closure_json or None,
            operator=args.operator,
            reviewer=args.reviewer,
            approver=args.approver,
            authorization_id=args.authorization_id,
        )
        outputs = write_v1_5_formal_database_import_authorization_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 formal database import authorization export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "overall_status": model.get("overall_status"),
        "blocker_count": model.get("blocker_count"),
        "review_required_count": model.get("review_required_count"),
        "preflight_ready": model.get("preflight_ready"),
        "archive_release_ready": model.get("archive_release_ready"),
        "manual_authorization_ready": model.get("manual_authorization_ready"),
        "database_import_allowed": model.get("database_import_allowed"),
        "authorization_json": str(outputs["json"].resolve()),
        "authorization_markdown": str(outputs["markdown"].resolve()),
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
