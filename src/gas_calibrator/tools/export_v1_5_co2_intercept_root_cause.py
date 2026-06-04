"""Export a no-write V1.5 CO2 intercept/root-cause diagnostic."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.co2_intercept_root_cause import (
    Co2InterceptRootCauseConfig,
    write_co2_intercept_root_cause_report,
)


def _split_ids(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in str(value or "").split(",") if part.strip())


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="No-write V1.5 CO2 intercept/root-cause diagnostic.")
    parser.add_argument("--point-errors-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--candidate-dir", default="")
    parser.add_argument("--current-getco-json", default="")
    parser.add_argument("--target-device-ids", default="022,030,033,051")
    parser.add_argument("--exclude-device-ids", default="100")
    parser.add_argument("--acceptance-pct", type=float, default=1.0)
    parser.add_argument("--h2o-low-mmol-mol", type=float, default=2.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        cfg = Co2InterceptRootCauseConfig(
            target_device_ids=_split_ids(args.target_device_ids),
            exclude_device_ids=_split_ids(args.exclude_device_ids),
            acceptance_pct=float(args.acceptance_pct),
            h2o_low_mmol_mol=float(args.h2o_low_mmol_mol),
        )
        outputs = write_co2_intercept_root_cause_report(
            point_errors_csv=args.point_errors_csv,
            candidate_dir=args.candidate_dir or None,
            current_getco_json=args.current_getco_json or None,
            output_dir=args.output_dir,
            cfg=cfg,
        )
    except Exception as exc:
        print(f"V1.5 CO2 intercept/root-cause diagnostic failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
