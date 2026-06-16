"""Export a no-write V1.5 factory signal health review."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.factory_signal_health_review import (
    FactorySignalHealthConfig,
    write_factory_signal_health_report,
)


def _split_ids(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in str(value or "").split(",") if part.strip())


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="No-write V1.5 factory signal health review.")
    parser.add_argument("--point-means-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--residuals-csv", default="")
    parser.add_argument("--target-device-ids", default="")
    parser.add_argument("--ref-full-scale-hint", type=float, default=4000.0)
    parser.add_argument("--relative-error-pct-warn", type=float, default=5.0)
    parser.add_argument("--absolute-error-warn", type=float, default=25.0)
    parser.add_argument("--ratio-span-warn", type=float, default=0.001)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        cfg = FactorySignalHealthConfig(
            target_device_ids=_split_ids(args.target_device_ids),
            ref_full_scale_hint=float(args.ref_full_scale_hint),
            relative_error_pct_warn=float(args.relative_error_pct_warn),
            absolute_error_warn=float(args.absolute_error_warn),
            ratio_span_warn=float(args.ratio_span_warn),
        )
        outputs = write_factory_signal_health_report(
            point_means_csv=args.point_means_csv,
            residuals_csv=args.residuals_csv or None,
            output_dir=args.output_dir,
            cfg=cfg,
        )
    except Exception as exc:
        print(f"V1.5 factory signal health review failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
