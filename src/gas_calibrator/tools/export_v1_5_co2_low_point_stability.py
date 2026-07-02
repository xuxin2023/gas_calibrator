"""Export V1.5 CO2 low-point stability diagnostics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, Optional

from ..validation.co2_low_point_stability import (
    Co2LowPointStabilityConfig,
    write_co2_low_point_stability_report,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-csv", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-device-id", action="append", default=None)
    parser.add_argument("--acceptance-pct", type=float, default=1.0)
    parser.add_argument("--low-point-max-ppm", type=float, default=150.0)
    parser.add_argument("--full-window-ratio-span-limit", type=float, default=0.0015)
    parser.add_argument("--tail-ratio-span-limit", type=float, default=0.0005)
    parser.add_argument("--full-window-co2-span-ppm-limit", type=float, default=3.0)
    parser.add_argument("--tail-co2-span-ppm-limit", type=float, default=1.0)
    parser.add_argument("--tail-fraction", type=float, default=0.25)
    parser.add_argument("--min-samples", type=int, default=20)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = Co2LowPointStabilityConfig(
        target_device_ids=tuple(args.target_device_id or ("030", "022", "033", "051")),
        low_point_max_ppm=float(args.low_point_max_ppm),
        acceptance_pct=float(args.acceptance_pct),
        full_window_ratio_span_limit=float(args.full_window_ratio_span_limit),
        tail_ratio_span_limit=float(args.tail_ratio_span_limit),
        full_window_co2_span_ppm_limit=float(args.full_window_co2_span_ppm_limit),
        tail_co2_span_ppm_limit=float(args.tail_co2_span_ppm_limit),
        tail_fraction=float(args.tail_fraction),
        min_samples=int(args.min_samples),
    )
    paths = write_co2_low_point_stability_report(
        sample_csv_paths=[Path(value) for value in args.sample_csv],
        output_dir=args.output_dir,
        cfg=cfg,
    )
    print(json.dumps({key: str(value) for key, value in paths.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
