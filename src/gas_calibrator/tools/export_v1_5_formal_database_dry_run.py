"""Export the offline V1.5 PostgreSQL 18 database dry-run contract."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_formal_database_dry_run import (
    PRODUCTION_POSTGRESQL_MAJOR,
    build_v1_5_formal_database_dry_run_contract,
    write_v1_5_formal_database_dry_run_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an offline V1.5 PostgreSQL 18 schema/insert-preview contract. "
            "This never connects to PostgreSQL and never imports data."
        )
    )
    parser.add_argument(
        "--planned-device",
        action="append",
        default=[],
        help="Optional planned analyzer identity, e.g. 01260601=047 or 01260601,047. May be repeated.",
    )
    parser.add_argument(
        "--require-postgresql-major",
        type=int,
        default=PRODUCTION_POSTGRESQL_MAJOR,
        help="Required production PostgreSQL major version. Defaults to 18.",
    )
    parser.add_argument("--output-dir", required=True, help="Directory for dry-run outputs.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_formal_database_dry_run_contract(
            planned_devices=args.planned_device,
            required_postgresql_major=args.require_postgresql_major,
        )
        outputs = write_v1_5_formal_database_dry_run_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 formal database dry-run export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "overall_status": model.get("overall_status"),
        "blocker_count": model.get("blocker_count"),
        "production_backend": model.get("production_backend"),
        "production_postgresql_major": model.get("production_postgresql_major"),
        "primary_identity": model.get("primary_identity"),
        "database_import_allowed": model.get("database_import_allowed"),
        "formal_release_allowed": model.get("formal_release_allowed"),
        "dry_run_json": str(outputs["json"].resolve()),
        "dry_run_markdown": str(outputs["markdown"].resolve()),
        "checks_csv": str(outputs["checks_csv"].resolve()),
        "physical_boundaries": {
            "connects_postgresql": model.get("connects_postgresql"),
            "opens_com_ports": model.get("opens_com_ports"),
            "controls_water_or_gas_routes": model.get("controls_water_or_gas_routes"),
            "writes_sn": model.get("writes_sn"),
            "writes_device_id": model.get("writes_device_id"),
            "writes_coefficients": model.get("writes_coefficients"),
            "database_written": model.get("database_written"),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    if args.fail_on_blocker and int(model.get("blocker_count") or 0) > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
