"""Export V1.5 Run/Technical/Formal calibration reports from evidence bundle."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.formal_reports import write_v1_5_calibration_reports


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate V1.5 calibration reports from an evidence bundle without touching devices."
    )
    parser.add_argument("--evidence-bundle-json", required=True, help="Path to evidence_bundle.json.")
    parser.add_argument("--output-dir", required=True, help="Report output directory.")
    parser.add_argument("--report-no", default="", help="Optional formal report number.")
    parser.add_argument("--reviewer", default="", help="Reviewer name.")
    parser.add_argument("--approver", default="", help="Approver name.")
    parser.add_argument("--location", default="", help="Calibration location.")
    parser.add_argument("--calibration-date", default="", help="Calibration date.")
    parser.add_argument("--analyzer-prefix", default="ga01", help="Analyzer prefix, e.g. ga01.")
    parser.add_argument(
        "--uncertainty-json",
        default=None,
        help="Optional released uncertainty input JSON used for RSS and k=2 calculation.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_calibration_reports(
            evidence_bundle_path=args.evidence_bundle_json,
            output_dir=args.output_dir,
            report_no=args.report_no,
            reviewer=args.reviewer,
            approver=args.approver,
            location=args.location,
            calibration_date=args.calibration_date,
            analyzer_prefix=args.analyzer_prefix,
            uncertainty_json=args.uncertainty_json,
        )
    except Exception as exc:
        print(f"V1.5 calibration report export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
