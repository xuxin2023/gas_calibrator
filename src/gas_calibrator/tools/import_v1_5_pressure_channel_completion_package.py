"""Import a V1.5 pressure-channel completion package into PostgreSQL."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..storage.v1_5_evidence.bundle import bundle_summary, write_bundle_json
from ..storage.v1_5_evidence.pressure_completion_bundle import (
    build_pressure_channel_completion_evidence_bundle,
)
from ..storage.v1_5_evidence.repository import apply_migrations, import_bundle


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build/import a V1.5 pressure-channel completion evidence bundle from existing artifacts."
    )
    parser.add_argument("--completion-dir", required=True, help="Existing pressure_channel_completion output directory.")
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
        bundle = build_pressure_channel_completion_evidence_bundle(
            completion_dir=args.completion_dir,
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
        print(f"V1.5 pressure-channel completion import failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
