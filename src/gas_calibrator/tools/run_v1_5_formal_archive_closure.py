"""Run the offline V1.5 formal archive closure."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_formal_archive_closure import build_v1_5_formal_archive_closure


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the V1.5 offline archive closure: evidence bundle, reports, "
            "optional DB import, traceability summary, and closure index."
        )
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run directory.")
    parser.add_argument("--plan-json", required=True, help="Formal calibration plan snapshot JSON.")
    parser.add_argument("--pressure-reference-json", required=True, help="COM22 pressure-reference snapshot JSON.")
    parser.add_argument(
        "--standard-gases-json",
        default=None,
        help=(
            "Optional reviewed standard-gases JSON. When provided, archive closure writes an "
            "archive-local plan snapshot with these gas/reference rows for traceability."
        ),
    )
    parser.add_argument("--contract-json", default=None, help="Optional v1_5_formal_flow_contract.json.")
    parser.add_argument("--output-dir", default=None, help="Closure output directory. Must be inside run-dir.")
    parser.add_argument("--pressure-check-csv", default=None, help="Optional pressure quick-check CSV or directory.")
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument("--analyzer-prefix", default="ga01")
    parser.add_argument("--today", default=None)
    parser.add_argument("--allow-pressure-fallback", action="store_true")
    parser.add_argument("--report-no", default="")
    parser.add_argument("--reviewer", default="")
    parser.add_argument("--approver", default="")
    parser.add_argument("--location", default="")
    parser.add_argument("--calibration-date", default="")
    parser.add_argument("--uncertainty-json", default=None)
    parser.add_argument(
        "--senco-artifact-authorization-json",
        default=None,
        help=(
            "Optional exact main SENCO artifact authorization JSON. When controlled-write evidence exists, "
            "archive closure binds its authorization ID, writer scope, device set, and verified readback rows."
        ),
    )
    parser.add_argument("--db-mode", choices=("skip", "dry-run", "import"), default="dry-run")
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--apply-migrations", action="store_true")
    parser.add_argument(
        "--capability-verification-csv",
        action="append",
        default=[],
        help="Optional verification summary CSV for the archive capability assessment. Can be repeated.",
    )
    parser.add_argument(
        "--capability-candidate-csv",
        action="append",
        default=[],
        help="Optional candidate summary CSV for the archive capability assessment. Can be repeated.",
    )
    parser.add_argument("--co2-limit-pct", type=float, default=1.5)
    parser.add_argument("--h2o-limit-pct", type=float, default=2.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        result = build_v1_5_formal_archive_closure(
            run_dir=args.run_dir,
            plan_json=args.plan_json,
            pressure_reference_json=args.pressure_reference_json,
            standard_gases_json=args.standard_gases_json,
            contract_json=args.contract_json,
            output_dir=args.output_dir,
            pressure_check_csv=args.pressure_check_csv,
            component=args.component,
            analyzer_prefix=args.analyzer_prefix,
            today=args.today,
            allow_pressure_fallback=args.allow_pressure_fallback,
            report_no=args.report_no,
            reviewer=args.reviewer,
            approver=args.approver,
            location=args.location,
            calibration_date=args.calibration_date,
            uncertainty_json=args.uncertainty_json,
            senco_artifact_authorization_json=args.senco_artifact_authorization_json,
            db_mode=args.db_mode,
            dsn=args.dsn,
            apply_db_migrations=args.apply_migrations,
            capability_verification_csvs=args.capability_verification_csv,
            capability_candidate_csvs=args.capability_candidate_csv,
            co2_limit_pct=args.co2_limit_pct,
            h2o_limit_pct=args.h2o_limit_pct,
        )
    except Exception as exc:
        print(f"V1.5 formal archive closure failed: {exc}", file=sys.stderr, flush=True)
        return 1
    paths = {key: str(Path(value).resolve()) for key, value in result["paths"].items()}
    print(json.dumps({"index": result["index"], "paths": paths}, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
