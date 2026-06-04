"""Export the offline V1.5 formal calibration evidence workbench."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_workbench import write_formal_workbench


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write a static V1.5 formal evidence workbench without touching devices."
    )
    parser.add_argument("--output-dir", required=True, help="Workbench output directory.")
    parser.add_argument("--run-dir", default=None, help="Existing V1.5 run directory.")
    parser.add_argument("--plan-json", default=None, help="Formal plan snapshot JSON.")
    parser.add_argument("--pressure-reference-json", default=None, help="COM22 pressure-reference JSON.")
    parser.add_argument("--config", default=None, help="Optional no-write runtime config JSON.")
    parser.add_argument("--evidence-bundle-json", default=None, help="Optional evidence_bundle.json.")
    parser.add_argument("--report-model-json", default=None, help="Optional report_model.json.")
    parser.add_argument("--uncertainty-json", default=None, help="Optional uncertainty inputs JSON.")
    parser.add_argument("--sidecar-summary-json", default=None, help="Optional formal_evidence_sidecar_summary.json.")
    parser.add_argument("--package-dir", default=None, help="Optional formal run package directory.")
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument("--analyzer-prefix", default="ga01")
    parser.add_argument("--reviewer", default="")
    parser.add_argument("--approver", default="")
    parser.add_argument("--today", default=None, help="Optional YYYY-MM-DD date for contract checks.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_formal_workbench(
            output_dir=args.output_dir,
            run_dir=args.run_dir,
            plan_path=args.plan_json,
            pressure_reference_path=args.pressure_reference_json,
            config_path=args.config,
            evidence_bundle_path=args.evidence_bundle_json,
            report_model_path=args.report_model_json,
            uncertainty_json=args.uncertainty_json,
            sidecar_summary_path=args.sidecar_summary_json,
            package_dir=args.package_dir,
            component=args.component,
            analyzer_prefix=args.analyzer_prefix,
            reviewer=args.reviewer,
            approver=args.approver,
            today=args.today,
        )
    except Exception as exc:
        print(f"V1.5 formal workbench export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
