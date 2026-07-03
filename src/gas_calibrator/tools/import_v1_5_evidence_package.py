"""Build a V1.5 evidence bundle dry-run or record a blocked import stub.

The production PostgreSQL import path is deliberately locked in V1.5. This
module keeps the old evidence-bundle dry-run available for offline review, and
adds a command-contract mode that writes a no-connect/no-write blocked executor
artifact. It never connects to PostgreSQL, applies migrations, or imports rows.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..storage.v1_5_evidence.bundle import build_evidence_bundle, bundle_summary, write_bundle_json
from ..validation.v1_5_formal_database_import_blocked_executor import (
    build_v1_5_formal_database_import_blocked_executor,
    write_v1_5_formal_database_import_blocked_executor_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Offline V1.5 evidence-bundle dry-run or blocked PostgreSQL 18 import executor stub. "
            "No mode connects to PostgreSQL."
        )
    )
    contract = parser.add_argument_group("blocked import executor stub")
    contract.add_argument(
        "--formal-database-import-command-contract-json",
        default=None,
        help="Reviewed V1.5 formal database import command contract JSON.",
    )
    contract.add_argument(
        "--formal-database-import-authorization-json",
        default=None,
        help="Reviewed manual import authorization JSON referenced by the command contract.",
    )
    contract.add_argument(
        "--formal-database-import-preflight-json",
        default=None,
        help="Reviewed import preflight JSON referenced by the command contract.",
    )
    contract.add_argument(
        "--archive-closure-json",
        default=None,
        help="Formal archive closure index referenced by the command contract.",
    )
    contract.add_argument(
        "--evidence-bundle-json",
        default=None,
        help="Frozen evidence bundle referenced by the command contract.",
    )
    contract.add_argument("--dsn-env", default="V1_5_POSTGRES_DSN")
    contract.add_argument(
        "--output-dir",
        default=None,
        help="Directory for blocked executor JSON/Markdown/CSV outputs.",
    )
    contract.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Return exit code 2 after writing the blocked executor artifact.",
    )
    contract.add_argument(
        "--fail-on-review-required",
        action="store_true",
        help="Return exit code 3 when contract/input review is required.",
    )

    legacy = parser.add_argument_group("legacy offline evidence-bundle dry-run")
    legacy.add_argument("--run-dir", default=None, help="Existing V1.5 run directory.")
    legacy.add_argument("--plan-json", default=None, help="Formal calibration plan snapshot JSON.")
    legacy.add_argument(
        "--pressure-reference-json",
        default=None,
        help="COM22 pressure-reference certificate snapshot JSON.",
    )
    legacy.add_argument(
        "--pressure-check-csv",
        default=None,
        help="Optional pressure quick-check CSV or directory to bind by analyzer device ID.",
    )
    legacy.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    legacy.add_argument("--analyzer-prefix", default="ga01")
    legacy.add_argument("--today", default=None, help="Optional YYYY-MM-DD date for contract checks.")
    legacy.add_argument(
        "--allow-pressure-fallback",
        action="store_true",
        help="Allow bundle generation when the dedicated pressure quick-check CSV is missing.",
    )
    legacy.add_argument("--output-json", default=None, help="Optional path for the evidence bundle JSON.")
    legacy.add_argument("--summary-json", default=None, help="Optional path for a compact import summary JSON.")
    legacy.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the legacy evidence bundle only; this is the only legacy mode still allowed.",
    )

    forbidden = parser.add_argument_group("locked real-import options")
    forbidden.add_argument("--dsn", default=None, help=argparse.SUPPRESS)
    forbidden.add_argument("--apply-migrations", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(list(argv) if argv is not None else None)


def _write_summary(path: str | Path, payload: dict[str, object]) -> None:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _contract_mode_requested(args: argparse.Namespace) -> bool:
    return bool(args.formal_database_import_command_contract_json or args.output_dir)


def _run_blocked_executor(args: argparse.Namespace) -> int:
    if not args.output_dir:
        print("Blocked import executor requires --output-dir.", file=sys.stderr, flush=True)
        return 2
    model = build_v1_5_formal_database_import_blocked_executor(
        formal_database_import_command_contract_json=args.formal_database_import_command_contract_json,
        formal_database_import_authorization_json=args.formal_database_import_authorization_json,
        formal_database_import_preflight_json=args.formal_database_import_preflight_json,
        archive_closure_json=args.archive_closure_json,
        evidence_bundle_json=args.evidence_bundle_json,
        dsn_env=args.dsn_env,
    )
    outputs = write_v1_5_formal_database_import_blocked_executor_outputs(model, args.output_dir)
    result = {
        "overall_status": model.get("overall_status"),
        "blocked_executor_ready": model.get("blocked_executor_ready"),
        "execution_supported": model.get("execution_supported"),
        "real_import_execution_allowed": model.get("real_import_execution_allowed"),
        "connects_postgresql": model.get("connects_postgresql"),
        "database_import_attempted": model.get("database_import_attempted"),
        "database_written": model.get("database_written"),
        "database_import_allowed": model.get("database_import_allowed"),
        "outputs": outputs,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
    if args.fail_on_review_required and model.get("review_required_count"):
        return 3
    if args.fail_on_blocked:
        return 2
    return 0


def _run_legacy_bundle_dry_run(args: argparse.Namespace) -> int:
    missing = [
        flag
        for flag, value in (
            ("--run-dir", args.run_dir),
            ("--plan-json", args.plan_json),
            ("--pressure-reference-json", args.pressure_reference_json),
        )
        if not value
    ]
    if missing:
        print("Legacy evidence bundle dry-run requires " + ", ".join(missing), file=sys.stderr, flush=True)
        return 2
    if not args.dry_run:
        print(
            "Real V1.5 PostgreSQL import is locked. Use --dry-run for bundle preview or "
            "--formal-database-import-command-contract-json with --output-dir for blocked executor evidence.",
            file=sys.stderr,
            flush=True,
        )
        return 2
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
    summary["database_imported"] = False
    summary["connects_postgresql"] = False
    summary["applies_migrations"] = False
    summary["database_written"] = False
    summary["real_import_locked"] = True
    if args.summary_json:
        _write_summary(args.summary_json, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str), flush=True)
    return 0


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if args.dsn or args.apply_migrations:
        print(
            "V1.5 real PostgreSQL import and migrations are locked in this command. "
            "Use the blocked executor contract path first.",
            file=sys.stderr,
            flush=True,
        )
        return 2
    try:
        if _contract_mode_requested(args):
            return _run_blocked_executor(args)
        return _run_legacy_bundle_dry_run(args)
    except Exception as exc:
        print(f"V1.5 evidence import command failed in no-connect mode: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
