"""Export V1.5 post-write reverification review artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_post_write_reverification import (
    VerificationLimits,
    build_post_write_reverification_review,
    write_post_write_reverification_outputs,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export V1.5 post-write reverification review.")
    parser.add_argument(
        "--verification-csv",
        action="append",
        required=True,
        help="CSV produced from an already-completed post-write open-flow verification point or queue.",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--write-event-json", action="append", default=[])
    parser.add_argument("--coefficient-snapshot-json", action="append", default=[])
    parser.add_argument("--coefficient-epoch", default="")
    parser.add_argument("--co2-relative-pct", type=float, default=1.5)
    parser.add_argument("--h2o-relative-pct", type=float, default=2.0)
    parser.add_argument("--co2-zero-abs-ppm", type=float, default=5.0)
    parser.add_argument("--h2o-dry-abs-mmol-mol", type=float, default=0.5)
    parser.add_argument("--fail-on-review-fail", action="store_true")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    review = build_post_write_reverification_review(
        verification_csvs=[Path(item) for item in args.verification_csv],
        limits=VerificationLimits(
            co2_relative_pct=float(args.co2_relative_pct),
            h2o_relative_pct=float(args.h2o_relative_pct),
            co2_zero_abs_ppm=float(args.co2_zero_abs_ppm),
            h2o_dry_abs_mmol_mol=float(args.h2o_dry_abs_mmol_mol),
        ),
        write_event_files=[Path(item) for item in args.write_event_json],
        coefficient_snapshot_files=[Path(item) for item in args.coefficient_snapshot_json],
        coefficient_epoch=str(args.coefficient_epoch or ""),
    )
    outputs = write_post_write_reverification_outputs(review, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": review.overall_status,
                "point_count": len(review.point_results),
                "device_component_count": len(review.device_component_summary),
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    if args.fail_on_review_fail and review.overall_status == "fail":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
