"""Export V1.5 initialization readiness from existing evidence only."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_initialization_readiness import write_initialization_readiness_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assess V1.5 initialization readiness without opening COM or controlling routes."
    )
    parser.add_argument("--run-dir", required=True, help="V1.5 run or planned run evidence directory.")
    parser.add_argument("--config", default=None, help="Optional runtime config JSON.")
    parser.add_argument("--getco-snapshot-dir", default=None, help="Optional coefficient_epoch_0_getco_snapshot dir.")
    parser.add_argument("--aux-neutralization-dir", default=None, help="Optional auxiliary_senco56789_neutralization dir.")
    parser.add_argument("--output-dir", required=True, help="Output directory for readiness report.")
    parser.add_argument(
        "--continuation-recovery",
        action="store_true",
        help="Treat missing auxiliary neutralization evidence as review-required warning for continuation runs.",
    )
    parser.add_argument(
        "--pressure-hardware-missing",
        action="store_true",
        help="Mark PACE/COM22 pressure hardware unavailable; stop automatic flow after initialization evidence.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_initialization_readiness_report(
            run_dir=args.run_dir,
            config_path=args.config,
            getco_snapshot_dir=args.getco_snapshot_dir,
            aux_neutralization_dir=args.aux_neutralization_dir,
            output_dir=args.output_dir,
            continuation_recovery=bool(args.continuation_recovery),
            pressure_hardware_missing=bool(args.pressure_hardware_missing),
        )
    except Exception as exc:
        print(f"V1.5 initialization readiness export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
