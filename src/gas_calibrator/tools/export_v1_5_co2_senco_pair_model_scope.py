"""Export a no-write CO2 SENCO1/SENCO3/SENCO5 model-scope review."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.co2_senco_pair_model_scope import write_co2_senco_pair_model_scope_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export a V1.5 CO2 SENCO1/SENCO3/SENCO5 no-write model-scope review "
            "against the original point table."
        )
    )
    parser.add_argument("--original-points-xlsx", required=True, help="Original/reference V1.5 points workbook.")
    parser.add_argument("--candidate-dir", required=True, help="Directory containing candidate coefficient artifacts.")
    parser.add_argument("--output-dir", required=True, help="Output directory for model-scope artifacts.")
    parser.add_argument(
        "--pair-review-dir",
        default=None,
        help="Optional directory containing co2_senco_pair_review_summary.csv from the previous review.",
    )
    parser.add_argument(
        "--database-sidecar-json",
        default=None,
        help="Optional JSON path for database-sidecar metadata. Defaults under output-dir.",
    )
    parser.add_argument("--min-temp-span-c-for-secondary", type=float, default=20.0)
    parser.add_argument("--min-pressure-span-hpa-for-pressure-terms", type=float, default=300.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_co2_senco_pair_model_scope_report(
            original_points_xlsx=args.original_points_xlsx,
            candidate_dir=args.candidate_dir,
            pair_review_dir=args.pair_review_dir,
            output_dir=args.output_dir,
            database_sidecar_json=args.database_sidecar_json,
            min_temp_span_c_for_secondary=float(args.min_temp_span_c_for_secondary),
            min_pressure_span_hpa_for_pressure_terms=float(args.min_pressure_span_hpa_for_pressure_terms),
        )
    except Exception as exc:
        print(f"V1.5 CO2 SENCO pair model-scope export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
