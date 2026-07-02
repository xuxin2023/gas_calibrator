"""Export a no-write V1.5 CO2 SENCO1/SENCO3 paired review package."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.co2_senco_pair_review import write_co2_senco_pair_review_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a no-write CO2 SENCO1/SENCO3 paired review after SENCO1-only verification."
    )
    parser.add_argument("--candidate-dir", required=True, help="Directory containing candidate coefficient artifacts.")
    parser.add_argument("--mapping-review-dir", required=True, help="Directory containing candidate write-review artifacts.")
    parser.add_argument(
        "--post-write-verification-dir",
        required=True,
        help="Directory containing the 900 ppm post-SENCO1 verification summary.",
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for paired-review artifacts.")
    parser.add_argument(
        "--database-sidecar-json",
        default=None,
        help="Optional JSON path for database-sidecar metadata. Defaults under output-dir.",
    )
    parser.add_argument("--device-output-abs-error-limit-ppm", type=float, default=20.0)
    parser.add_argument("--primary-model-abs-error-limit-ppm", type=float, default=10.0)
    parser.add_argument("--device-output-relative-error-limit-pct", type=float, default=1.0)
    parser.add_argument("--primary-model-relative-error-limit-pct", type=float, default=1.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_co2_senco_pair_review_report(
            candidate_dir=args.candidate_dir,
            mapping_review_dir=args.mapping_review_dir,
            post_write_verification_dir=args.post_write_verification_dir,
            output_dir=args.output_dir,
            database_sidecar_json=args.database_sidecar_json,
            device_output_abs_error_limit_ppm=float(args.device_output_abs_error_limit_ppm),
            primary_model_abs_error_limit_ppm=float(args.primary_model_abs_error_limit_ppm),
            device_output_relative_error_limit_pct=float(args.device_output_relative_error_limit_pct),
            primary_model_relative_error_limit_pct=float(args.primary_model_relative_error_limit_pct),
        )
    except Exception as exc:
        print(f"V1.5 CO2 SENCO pair review export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
