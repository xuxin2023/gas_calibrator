"""Export an offline V1.5 H2O formal closure review package."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.h2o_formal_closure_review import (
    H2OFormalClosureConfig,
    write_h2o_formal_closure_review,
)


def _split_ids(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in str(value or "").split(",") if part.strip())


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export offline V1.5 H2O formal closure review from existing evidence artifacts."
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--senco24-candidate-csv", required=True)
    parser.add_argument("--senco24-residuals-csv", default=None)
    parser.add_argument("--senco24-point-inputs-csv", default=None)
    parser.add_argument("--senco6-candidate-csv", default=None)
    parser.add_argument("--senco6-residuals-csv", default=None)
    parser.add_argument("--senco24-write-events-csv", default=None)
    parser.add_argument("--senco6-write-events-csv", default=None)
    parser.add_argument("--target-device-ids", default="")
    parser.add_argument("--dry-anchor-required", action="store_true", default=False)
    parser.add_argument("--no-senco6-review-required", action="store_true", default=False)
    parser.add_argument("--require-verified-writes", action="store_true", default=False)
    parser.add_argument(
        "--fit-temperature-source",
        choices=("digital_thermometer", "analyzer_chamber"),
        default="digital_thermometer",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        cfg = H2OFormalClosureConfig(
            target_device_ids=_split_ids(args.target_device_ids),
            dry_anchor_required=bool(args.dry_anchor_required),
            require_senco6_review=not bool(args.no_senco6_review_required),
            require_verified_writes=bool(args.require_verified_writes),
            fit_temperature_source=str(args.fit_temperature_source),
        )
        outputs = write_h2o_formal_closure_review(
            output_dir=args.output_dir,
            senco24_candidate_csv=args.senco24_candidate_csv,
            senco24_residuals_csv=args.senco24_residuals_csv,
            senco24_point_inputs_csv=args.senco24_point_inputs_csv,
            senco6_candidate_csv=args.senco6_candidate_csv,
            senco6_residuals_csv=args.senco6_residuals_csv,
            senco24_write_events_csv=args.senco24_write_events_csv,
            senco6_write_events_csv=args.senco6_write_events_csv,
            config=cfg,
        )
    except Exception as exc:
        print(f"V1.5 H2O formal closure review failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
