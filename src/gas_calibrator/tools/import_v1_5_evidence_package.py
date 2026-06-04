"""Import an offline V1.5 formal evidence package into PostgreSQL."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..storage.v1_5_evidence.bundle import build_evidence_bundle, bundle_summary, write_bundle_json
from ..storage.v1_5_evidence.repository import apply_migrations, import_bundle


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build/import a V1.5 evidence-registry bundle from existing artifacts."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run directory.")
    parser.add_argument("--plan-json", required=True, help="Formal calibration plan snapshot JSON.")
    parser.add_argument(
        "--pressure-reference-json",
        required=True,
        help="COM22 pressure-reference certificate snapshot JSON.",
    )
    parser.add_argument(
        "--pressure-check-csv",
        default=None,
        help="Optional pressure quick-check CSV or directory to bind by analyzer device ID.",
    )
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument("--analyzer-prefix", default="ga01")
    parser.add_argument("--today", default=None, help="Optional YYYY-MM-DD date for contract checks.")
    parser.add_argument(
        "--allow-pressure-fallback",
        action="store_true",
        help="Allow import bundle generation when the dedicated pressure quick-check CSV is missing.",
    )
    parser.add_argument("--output-json", default=None, help="Optional path for the evidence bundle JSON.")
    parser.add_argument("--summary-json", default=None, help="Optional path for a compact import summary JSON.")
    parser.add_argument("--dsn", default=None, help="PostgreSQL DSN. Defaults to GAS_CAL_DB_DSN.")
    parser.add_argument("--apply-migrations", action="store_true", help="Apply registry migrations before import.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the bundle and write JSON/summary only; do not connect to PostgreSQL.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _write_summary(path: str | Path, payload: dict[str, object]) -> None:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        bundle = build_evidence_bundle(
            run_dir=args.run_dir,
            plan_path=args.plan_json,
            pressure_reference_path=args.pressure_reference_json,
            component=args.component,
            analyzer_prefix=args.analyzer_prefix,
            require_quick_check_artifact=not bool(args.allow_pressure_fallback),
            pressure_check_path=args.pressure_check_csv,
            today=args.today,
        )
        summary = bundle_summary(bundle)
        if args.output_json:
            write_bundle_json(bundle, args.output_json)
            summary["bundle_json"] = str(Path(args.output_json).resolve())
        if args.dry_run:
            summary["database_imported"] = False
            if args.summary_json:
                _write_summary(args.summary_json, summary)
            print(json.dumps(summary, ensure_ascii=False, indent=2, default=str), flush=True)
            return 0

        dsn = args.dsn or os.environ.get("GAS_CAL_DB_DSN", "")
        if not dsn:
            print("Missing DSN. Pass --dsn, set GAS_CAL_DB_DSN, or use --dry-run.", file=sys.stderr, flush=True)
            return 2
        if args.apply_migrations:
            summary["migrations_applied"] = apply_migrations(dsn)
        summary["database_import"] = import_bundle(dsn, bundle)
        summary["database_imported"] = True
        if args.summary_json:
            _write_summary(args.summary_json, summary)
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str), flush=True)
        return 0
    except Exception as exc:
        print(f"V1.5 evidence import failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
