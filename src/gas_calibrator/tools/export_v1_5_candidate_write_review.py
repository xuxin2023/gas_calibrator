"""Export V1.5 offline component candidate write-review artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_candidate_write_review import (
    FORMULA_CONTRACT_MANUAL_SENCO13_RT_PRESSURE_SEPARATE,
    FORMULA_CONTRACT_UNCONFIRMED,
    SENCO5_POLICY_BLOCKED,
    SENCO5_POLICY_PRESERVE_EXISTING,
    SENCO5_POLICY_PRESERVE_EXISTING_LEGACY,
    write_candidate_write_review_report,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a V1.5 no-write component candidate write-review package."
    )
    parser.add_argument("--candidate-dir", required=True, help="Directory containing candidate coefficient CSV artifacts.")
    parser.add_argument("--plan-json", required=True, help="Formal calibration plan snapshot JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for write-review artifacts.")
    parser.add_argument("--component", choices=("co2", "h2o"), default="co2")
    parser.add_argument("--min-fit-points", type=int, default=5)
    parser.add_argument(
        "--old-coefficients-json",
        default=None,
        help="Optional old GETCO/SENCO component snapshot JSON. Without it, actual writes remain blocked.",
    )
    parser.add_argument(
        "--formula-contract",
        choices=(FORMULA_CONTRACT_UNCONFIRMED, FORMULA_CONTRACT_MANUAL_SENCO13_RT_PRESSURE_SEPARATE),
        default=FORMULA_CONTRACT_UNCONFIRMED,
        help=(
            "Explicit firmware/manual formula contract. Default keeps writes blocked. "
            f"Use {FORMULA_CONTRACT_MANUAL_SENCO13_RT_PRESSURE_SEPARATE!r} only after manual review."
        ),
    )
    parser.add_argument(
        "--senco5-policy",
        choices=(SENCO5_POLICY_BLOCKED, SENCO5_POLICY_PRESERVE_EXISTING, SENCO5_POLICY_PRESERVE_EXISTING_LEGACY),
        default=SENCO5_POLICY_BLOCKED,
        help=(
            "CO2 SENCO5 scope decision. Default blocks writes. "
            f"Use {SENCO5_POLICY_PRESERVE_EXISTING!r} to preserve existing SENCO5/SENCO6 linear correction layers "
            "and scope the write to SENCO1/SENCO3 optical/temperature coefficients."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        old_snapshot = None
        if args.old_coefficients_json:
            old_snapshot = json.loads(Path(args.old_coefficients_json).read_text(encoding="utf-8-sig"))
        outputs = write_candidate_write_review_report(
            candidate_dir=args.candidate_dir,
            plan_path=args.plan_json,
            output_dir=args.output_dir,
            component=args.component,
            min_fit_points=int(args.min_fit_points),
            old_coefficients_snapshot=old_snapshot,
            formula_contract=args.formula_contract,
            senco5_policy=args.senco5_policy,
        )
    except Exception as exc:
        print(f"V1.5 candidate write-review export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
