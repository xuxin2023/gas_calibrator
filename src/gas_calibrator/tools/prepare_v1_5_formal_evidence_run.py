"""Prepare V1.5 formal evidence run snapshots without touching devices."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_evidence_run import prepare_formal_evidence_run


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create V1.5 formal plan/reference/manifest evidence files without device I/O."
    )
    parser.add_argument("--output-dir", required=True, help="Directory for formal evidence snapshots.")
    parser.add_argument("--operator", required=True, help="Operator name for the formal plan snapshot.")
    parser.add_argument("--analyzer-id", required=True, help="Analyzer ID under test.")
    parser.add_argument("--run-id", default=None, help="Optional formal run id.")
    parser.add_argument("--plan-id", default=None, help="Optional plan id. Defaults to run id.")
    parser.add_argument("--plan-version", default=None, help="Optional plan version. Defaults to today.")
    parser.add_argument("--config", default=None, help="Optional runtime config JSON used to compute config_hash.")
    parser.add_argument(
        "--standard-gases-json",
        default=None,
        help="Optional JSON list or object with standard_gases for CO2/H2O traceability.",
    )
    parser.add_argument(
        "--pressure-reference-json",
        default=None,
        help="Optional COM22 pressure-reference JSON. If omitted, a fill-in template is written.",
    )
    parser.add_argument("--lab", default="", help="Optional lab name.")
    parser.add_argument("--ambient-temperature-c", default=None, help="Optional ambient temperature.")
    parser.add_argument("--ambient-rh-pct", default=None, help="Optional ambient RH percentage.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = prepare_formal_evidence_run(
            output_dir=args.output_dir,
            operator=args.operator,
            analyzer_id=args.analyzer_id,
            run_id=args.run_id,
            plan_id=args.plan_id,
            plan_version=args.plan_version,
            config_path=args.config,
            standard_gases_json=args.standard_gases_json,
            pressure_reference_json=args.pressure_reference_json,
            lab=args.lab,
            ambient_temperature_c=args.ambient_temperature_c,
            ambient_rh_pct=args.ambient_rh_pct,
        )
    except Exception as exc:
        print(f"Prepare V1.5 formal evidence run failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

