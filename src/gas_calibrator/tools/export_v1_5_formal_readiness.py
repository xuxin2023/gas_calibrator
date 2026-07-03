"""Export V1.5 formal no-write readiness report without touching devices."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_readiness import write_formal_readiness_report


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assess V1.5 formal no-write readiness from existing evidence files only."
    )
    parser.add_argument("--run-dir", required=True, help="Planned or existing V1.5 run directory.")
    parser.add_argument("--plan-json", required=True, help="Formal plan snapshot JSON.")
    parser.add_argument("--pressure-reference-json", required=True, help="COM22 pressure-reference JSON.")
    parser.add_argument("--config", default=None, help="Optional runtime config JSON.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory.")
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument("--analyzer-prefix", default="ga01")
    parser.add_argument("--today", default=None, help="Optional YYYY-MM-DD date for certificate checks.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_formal_readiness_report(
            run_dir=args.run_dir,
            plan_path=args.plan_json,
            pressure_reference_path=args.pressure_reference_json,
            config_path=args.config,
            output_dir=args.output_dir,
            component=args.component,
            analyzer_prefix=args.analyzer_prefix,
            today=args.today,
        )
    except Exception as exc:
        print(f"V1.5 formal readiness export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
