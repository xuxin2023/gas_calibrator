"""Export V1.5 pressure/SENCO9 no-write collection preflight artifacts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..config import load_config
from ..validation.pressure_senco9_no_write_plan import (
    DEFAULT_PRESSURE_POINTS,
    write_pressure_senco9_no_write_preflight_report,
)


def _default_pressure_points() -> str:
    return ",".join("ambient" if str(item) == "ambient" else f"{float(item):g}" for item in DEFAULT_PRESSURE_POINTS)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a no-write V1.5 pressure/SENCO9 multi-point preflight runbook."
    )
    parser.add_argument("--config", required=True, help="V1.5 runtime config JSON.")
    parser.add_argument(
        "--pressure-reference-json",
        default=None,
        help="COM22 pressure-reference certificate snapshot JSON.",
    )
    parser.add_argument(
        "--pressure-points",
        default=_default_pressure_points(),
        help="Pressure points such as ambient,1100,1000,900,800,700,600,500.",
    )
    parser.add_argument("--count", type=int, default=12, help="Samples per pressure point.")
    parser.add_argument("--interval-s", type=float, default=1.0, help="Sampling interval in seconds.")
    parser.add_argument("--output-dir", required=True, help="Output directory for preflight artifacts.")
    parser.add_argument(
        "--allow-engineering-reference",
        action="store_true",
        help="Allow missing/expired pressure-reference certificate for engineering planning only.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    config_path = Path(args.config).resolve()
    if not config_path.exists():
        print(f"Config not found: {config_path}", flush=True)
        return 2
    try:
        cfg = load_config(config_path)
        outputs = write_pressure_senco9_no_write_preflight_report(
            config=cfg,
            config_path=config_path,
            pressure_reference_path=args.pressure_reference_json,
            output_dir=args.output_dir,
            pressure_points=args.pressure_points,
            sample_count=int(args.count),
            interval_s=float(args.interval_s),
            require_traceable_pressure_reference=not bool(args.allow_engineering_reference),
        )
    except Exception as exc:
        print(f"Pressure/SENCO9 no-write preflight export failed: {exc}", flush=True)
        return 1
    print(f"Pressure/SENCO9 no-write preflight saved: {outputs['workbook']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
