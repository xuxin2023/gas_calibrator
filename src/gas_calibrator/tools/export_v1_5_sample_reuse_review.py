"""Export V1.5 per-device sample reuse review artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_candidate_coefficients import CandidateCoefficientPolicyConfig
from ..validation.formal_sample_reuse import write_sample_reuse_review


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export V1.5 per-analyzer sample reuse decisions without touching devices."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run directory.")
    parser.add_argument("--plan-json", required=True, help="Formal calibration plan snapshot JSON.")
    parser.add_argument("--pressure-reference-json", required=True, help="COM22 pressure-reference snapshot JSON.")
    parser.add_argument(
        "--pressure-check-csv",
        default=None,
        help="Optional pressure quick-check CSV or directory to bind by analyzer device ID.",
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for reuse review artifacts.")
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument(
        "--analyzer-prefix",
        default="all",
        help="Analyzer prefix, e.g. ga01. Use 'all' for detected analyzers or a comma list.",
    )
    parser.add_argument(
        "--allow-pressure-fallback",
        action="store_true",
        help="Allow review from sample-row pressure evidence when no quick-check artifact exists.",
    )
    parser.add_argument("--today", default=None, help="Optional YYYY-MM-DD date for certificate checks.")
    parser.add_argument("--min-fit-samples", type=int, default=10)
    parser.add_argument("--min-distinct-targets", type=int, default=2)
    parser.add_argument("--min-verification-samples", type=int, default=1)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        cfg = CandidateCoefficientPolicyConfig(
            min_fit_samples=int(args.min_fit_samples),
            min_distinct_targets=int(args.min_distinct_targets),
            min_verification_samples=int(args.min_verification_samples),
        )
        outputs = write_sample_reuse_review(
            run_dir=args.run_dir,
            output_dir=args.output_dir,
            plan_path=args.plan_json,
            pressure_reference_path=args.pressure_reference_json,
            pressure_check_path=args.pressure_check_csv,
            component=args.component,
            analyzer_prefix=args.analyzer_prefix,
            require_quick_check_artifact=not bool(args.allow_pressure_fallback),
            cfg=cfg,
            today=args.today,
        )
    except Exception as exc:
        print(f"V1.5 sample reuse review export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
