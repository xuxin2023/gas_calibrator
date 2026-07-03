"""Run offline V1.5 formal calibration preflight checks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_preflight import write_formal_preflight_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run V1.5 formal calibration preflight checks without touching devices."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run directory.")
    parser.add_argument("--plan-json", required=True, help="Formal plan snapshot JSON.")
    parser.add_argument("--pressure-reference-json", required=True, help="COM22 pressure-reference JSON.")
    parser.add_argument("--config", default=None, help="Optional runtime config JSON. Defaults to run-dir snapshot.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory.")
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument("--analyzer-prefix", default="ga01")
    parser.add_argument(
        "--allow-pressure-fallback",
        action="store_true",
        help="Do not require a dedicated pressure_channel_quick_check*.csv artifact.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    run_dir = Path(args.run_dir).resolve()
    try:
        outputs = write_formal_preflight_report(
            run_dir=run_dir,
            plan_path=args.plan_json,
            pressure_reference_path=args.pressure_reference_json,
            config_path=args.config,
            output_dir=args.output_dir,
            component=args.component,
            analyzer_prefix=args.analyzer_prefix,
            require_quick_check_artifact=not bool(args.allow_pressure_fallback),
        )
    except Exception as exc:
        print(f"Formal preflight failed: {exc}", flush=True)
        return 1
    print(f"Formal preflight saved: {outputs['workbook']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
