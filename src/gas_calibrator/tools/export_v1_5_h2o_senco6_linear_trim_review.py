"""Export a no-write V1.5 H2O SENCO6 final linear-trim review."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.h2o_senco6_linear_trim_review import (
    H2oSenco6LinearTrimConfig,
    write_h2o_senco6_linear_trim_review,
)


def _split_ids(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in str(value or "").split(",") if part.strip())


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="No-write V1.5 H2O SENCO6 linear-trim review.")
    parser.add_argument("--verification-summary-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-device-ids", default="022,030,033,051")
    parser.add_argument("--exclude-device-ids", default="100")
    parser.add_argument("--acceptance-pct", type=float, default=2.0)
    parser.add_argument("--min-points", type=int, default=2)
    parser.add_argument("--command-c0-decimals", type=int, default=1)
    parser.add_argument("--command-c1-decimals", type=int, default=1)
    parser.add_argument("--command-c1-min", type=float, default=0.0)
    parser.add_argument("--command-c1-max", type=float, default=2.0)
    parser.add_argument("--max-abs-c0-mmol", type=float, default=2.0)
    parser.add_argument("--max-abs-c1-delta", type=float, default=0.15)
    parser.add_argument("--require-state-comparability-evidence", action="store_true")
    parser.add_argument("--max-abs-h2o-ratio-delta-vs-fit", type=float, default=0.005)
    parser.add_argument("--max-abs-temperature-delta-c-vs-fit", type=float, default=3.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        cfg = H2oSenco6LinearTrimConfig(
            acceptance_pct=float(args.acceptance_pct),
            min_points=int(args.min_points),
            target_device_ids=_split_ids(args.target_device_ids),
            exclude_device_ids=_split_ids(args.exclude_device_ids),
            command_c0_decimals=int(args.command_c0_decimals),
            command_c1_decimals=int(args.command_c1_decimals),
            command_c1_min=float(args.command_c1_min),
            command_c1_max=float(args.command_c1_max),
            max_abs_c0_mmol=float(args.max_abs_c0_mmol),
            max_abs_c1_delta=float(args.max_abs_c1_delta),
            require_state_comparability_evidence=bool(args.require_state_comparability_evidence),
            max_abs_h2o_ratio_delta_vs_fit=float(args.max_abs_h2o_ratio_delta_vs_fit),
            max_abs_temperature_delta_c_vs_fit=float(args.max_abs_temperature_delta_c_vs_fit),
        )
        outputs = write_h2o_senco6_linear_trim_review(
            verification_summary_csv=args.verification_summary_csv,
            output_dir=args.output_dir,
            cfg=cfg,
        )
    except Exception as exc:
        print(f"V1.5 H2O SENCO6 linear-trim review failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
