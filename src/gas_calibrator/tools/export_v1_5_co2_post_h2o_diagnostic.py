"""Export offline V1.5 post-H2O CO2 failure diagnostics."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.co2_post_h2o_diagnostic import write_co2_post_h2o_diagnostic


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export no-write V1.5 CO2 post-H2O failure diagnostics.")
    parser.add_argument("--verification-summary-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fit-residuals-csv", default=None)
    parser.add_argument("--firmware-replay-csv", default=None)
    parser.add_argument("--yesterday-today-csv", default=None)
    parser.add_argument("--target-devices", default="022,030,033,051")
    parser.add_argument("--acceptance-pct", type=float, default=1.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_co2_post_h2o_diagnostic(
            verification_summary_csv=Path(args.verification_summary_csv),
            output_dir=Path(args.output_dir),
            fit_residuals_csv=Path(args.fit_residuals_csv) if args.fit_residuals_csv else None,
            firmware_replay_csv=Path(args.firmware_replay_csv) if args.firmware_replay_csv else None,
            yesterday_today_csv=Path(args.yesterday_today_csv) if args.yesterday_today_csv else None,
            target_device_ids=[item.strip() for item in str(args.target_devices).split(",") if item.strip()],
            acceptance_pct=float(args.acceptance_pct),
        )
    except Exception as exc:
        print(f"V1.5 CO2 post-H2O diagnostic failed: {exc}", file=sys.stderr, flush=True)
        return 2
    print(json.dumps({key: str(value) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
