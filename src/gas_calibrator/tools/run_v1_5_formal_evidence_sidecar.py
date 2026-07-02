"""Run the V1.5 formal evidence sidecar workflow."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Iterable, Optional

from ..validation.formal_evidence_run import run_formal_evidence_sidecar


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run V1.5 formal evidence preflight/package/bundle/import without touching devices."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run directory.")
    parser.add_argument("--plan-json", required=True, help="Formal plan snapshot JSON.")
    parser.add_argument("--pressure-reference-json", required=True, help="COM22 pressure-reference JSON.")
    parser.add_argument("--config", default=None, help="Optional runtime config JSON.")
    parser.add_argument("--output-dir", default=None, help="Optional sidecar output directory.")
    parser.add_argument("--stage", choices=("preflight", "package", "all"), default="all")
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument("--analyzer-prefix", default="ga01")
    parser.add_argument("--today", default=None, help="Optional YYYY-MM-DD date for contract checks.")
    parser.add_argument(
        "--allow-pressure-fallback",
        action="store_true",
        help="Do not require a dedicated pressure_channel_quick_check*.csv artifact.",
    )
    parser.add_argument("--import-db", action="store_true", help="Import evidence bundle into PostgreSQL.")
    parser.add_argument("--apply-migrations", action="store_true", help="Apply DB migrations before import.")
    parser.add_argument("--dsn", default=None, help="PostgreSQL DSN. Defaults to GAS_CAL_DB_DSN.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        payload = run_formal_evidence_sidecar(
            run_dir=args.run_dir,
            plan_path=args.plan_json,
            pressure_reference_path=args.pressure_reference_json,
            config_path=args.config,
            output_dir=args.output_dir,
            stage=args.stage,
            component=args.component,
            analyzer_prefix=args.analyzer_prefix,
            require_quick_check_artifact=not bool(args.allow_pressure_fallback),
            today=args.today,
            dsn=args.dsn or os.environ.get("GAS_CAL_DB_DSN", ""),
            apply_db_migrations=bool(args.apply_migrations),
            import_db=bool(args.import_db),
        )
    except Exception as exc:
        print(f"V1.5 formal evidence sidecar failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

