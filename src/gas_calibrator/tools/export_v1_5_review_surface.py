"""Export the unified V1.5 formal calibration review surface."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..v1_5.review_surface import load_json_object, write_review_surface


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write the unified V1.5 review surface without touching devices."
    )
    parser.add_argument("--output-dir", required=True, help="Review-surface output directory.")
    parser.add_argument("--formal-workbench-json", default=None, help="Optional v1_5_formal_workbench.json.")
    parser.add_argument("--operation-console-json", default=None, help="Optional v1_5_operation_console.json.")
    parser.add_argument("--parameter-surface-json", default=None, help="Optional parameter surface JSON.")
    parser.add_argument("--advanced-qc-json", default=None, help="Optional advanced QC JSON.")
    parser.add_argument("--role", choices=("operator", "engineer", "reviewer", "admin"), default="operator")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_review_surface(
            output_dir=args.output_dir,
            formal_workbench=load_json_object(args.formal_workbench_json),
            operation_console=load_json_object(args.operation_console_json),
            parameter_surface=load_json_object(args.parameter_surface_json),
            advanced_qc=load_json_object(args.advanced_qc_json),
            role=args.role,
        )
    except Exception as exc:
        print(f"V1.5 review surface export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
