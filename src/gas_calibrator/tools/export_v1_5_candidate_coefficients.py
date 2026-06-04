"""Export V1.5 no-write candidate coefficient review artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_candidate_coefficients import (
    CandidateCoefficientPolicyConfig,
    write_candidate_coefficient_report,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export V1.5 current-atmosphere open-flow candidate coefficients without touching devices."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run directory.")
    parser.add_argument("--plan-json", required=True, help="Formal calibration plan snapshot JSON.")
    parser.add_argument("--pressure-reference-json", required=True, help="COM22 pressure-reference snapshot JSON.")
    parser.add_argument(
        "--pressure-check-csv",
        default=None,
        help="Optional pressure quick-check CSV or directory to bind by analyzer device ID.",
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for candidate coefficient artifacts.")
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument(
        "--analyzer-prefix",
        default="ga01",
        help="Analyzer prefix, e.g. ga01. Use 'all' for detected analyzers or a comma list.",
    )
    parser.add_argument(
        "--all-analyzers",
        action="store_true",
        help="Build candidate tables for every detected analyzer prefix.",
    )
    parser.add_argument(
        "--allow-pressure-fallback",
        action="store_true",
        help="Allow package review from sample-row pressure evidence when no quick-check artifact exists.",
    )
    parser.add_argument("--today", default=None, help="Optional YYYY-MM-DD date for certificate checks.")
    parser.add_argument("--min-fit-samples", type=int, default=10)
    parser.add_argument("--min-distinct-targets", type=int, default=2)
    parser.add_argument("--min-verification-samples", type=int, default=1)
    parser.add_argument("--allow-pressure-terms", action="store_true")
    parser.add_argument("--allow-temperature-terms", action="store_true")
    parser.add_argument(
        "--fit-all-eligible-samples",
        action="store_true",
        help=(
            "Use every complete A-grade non-diagnostic sample for fitting, including rows originally marked "
            "verification. The export remains no-write and requires a new independent verification run."
        ),
    )
    parser.add_argument(
        "--allow-uncertified-zero-co2-anchor",
        action="store_true",
        help=(
            "Diagnostic compatibility switch only: allow CO2=0 rows without an explicit CO2-zero certificate "
            "to enter the CO2 fit. Formal V1.5 exports should leave this disabled."
        ),
    )
    parser.add_argument(
        "--exclude-device-id",
        action="append",
        default=[],
        help="Analyzer device ID to exclude from candidate generation, e.g. 023. Can be repeated.",
    )
    parser.add_argument(
        "--preserved-secondary-coefficients-json",
        default=None,
        help=(
            "Optional GETCO backup JSON. For CO2 current-temperature SENCO1-only candidates, "
            "the export preserves existing SENCO3 and fits SENCO1 to "
            "certificate target minus the preserved SENCO3 contribution."
        ),
    )
    parser.add_argument(
        "--co2-dry-correction-h2o-source",
        default="reference_first",
        choices=("reference_first", "reference", "analyzer_first", "analyzer"),
        help=(
            "H2O evidence used to invert/replay the CO2 firmware dry-basis correction. "
            "reference_first prevents old analyzer H2O coefficients from biasing CO2 fitting."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        preserved_secondary = {}
        preserved_secondary_source = ""
        if args.preserved_secondary_coefficients_json:
            preserved_secondary_path = Path(args.preserved_secondary_coefficients_json).resolve()
            preserved_secondary = json.loads(preserved_secondary_path.read_text(encoding="utf-8"))
            preserved_secondary_source = str(preserved_secondary_path)
        cfg = CandidateCoefficientPolicyConfig(
            min_fit_samples=int(args.min_fit_samples),
            min_distinct_targets=int(args.min_distinct_targets),
            min_verification_samples=int(args.min_verification_samples),
            allow_pressure_terms=bool(args.allow_pressure_terms),
            allow_temperature_terms=bool(args.allow_temperature_terms),
            fit_all_eligible_samples=bool(args.fit_all_eligible_samples),
            allow_uncertified_zero_co2_anchor=bool(args.allow_uncertified_zero_co2_anchor),
            exclude_device_ids=tuple(args.exclude_device_id or ()),
            preserved_secondary_coefficients=preserved_secondary,
            preserved_secondary_coefficients_source=preserved_secondary_source,
            co2_dry_correction_h2o_source=args.co2_dry_correction_h2o_source,
        )
        outputs = write_candidate_coefficient_report(
            run_dir=args.run_dir,
            output_dir=args.output_dir,
            plan_path=args.plan_json,
            pressure_reference_path=args.pressure_reference_json,
            pressure_check_path=args.pressure_check_csv,
            component=args.component,
            analyzer_prefix="all" if args.all_analyzers else args.analyzer_prefix,
            require_quick_check_artifact=not bool(args.allow_pressure_fallback),
            cfg=cfg,
            today=args.today,
        )
    except Exception as exc:
        print(f"V1.5 candidate coefficient export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
