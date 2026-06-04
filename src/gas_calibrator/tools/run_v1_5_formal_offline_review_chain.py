"""Run the V1.5 formal offline review chain."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_offline_review_chain import run_formal_offline_review_chain


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run V1.5 readiness, sidecar evidence, reports, workbench, and review surface without touching devices."
    )
    parser.add_argument("--run-dir", required=True, help="Planned or existing V1.5 run directory.")
    parser.add_argument("--plan-json", required=True, help="Formal plan snapshot JSON.")
    parser.add_argument("--pressure-reference-json", required=True, help="COM22 pressure-reference JSON.")
    parser.add_argument("--config", default=None, help="Optional no-write runtime config JSON.")
    parser.add_argument("--output-dir", default=None, help="Optional chain output directory.")
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument("--analyzer-prefix", default="ga01")
    parser.add_argument("--reviewer", default="")
    parser.add_argument("--approver", default="")
    parser.add_argument("--location", default="")
    parser.add_argument("--calibration-date", default="")
    parser.add_argument("--uncertainty-json", default=None)
    parser.add_argument("--role", choices=("operator", "engineer", "reviewer", "admin"), default="operator")
    parser.add_argument("--today", default=None, help="Optional YYYY-MM-DD date for certificate checks.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        summary = run_formal_offline_review_chain(
            run_dir=args.run_dir,
            plan_path=args.plan_json,
            pressure_reference_path=args.pressure_reference_json,
            config_path=args.config,
            output_dir=args.output_dir,
            component=args.component,
            analyzer_prefix=args.analyzer_prefix,
            reviewer=args.reviewer,
            approver=args.approver,
            location=args.location,
            calibration_date=args.calibration_date,
            uncertainty_json=args.uncertainty_json,
            role=args.role,
            today=args.today,
        )
    except Exception as exc:
        print(f"V1.5 formal offline review chain failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
