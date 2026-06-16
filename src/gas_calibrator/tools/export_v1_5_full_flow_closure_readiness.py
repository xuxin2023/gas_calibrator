"""Export the offline V1.5 full-flow closure readiness review."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_full_flow_closure_readiness import (
    build_v1_5_full_flow_closure_readiness,
    write_v1_5_full_flow_closure_readiness_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an offline V1.5 full-flow closure-readiness review without touching devices."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run or full-flow output directory.")
    parser.add_argument("--output-dir", required=True, help="Directory for closure-readiness outputs.")
    parser.add_argument("--full-flow-plan-json", default="", help="Optional v1_5_full_flow_plan.json path.")
    parser.add_argument("--run-evidence-status-json", default="", help="Optional v1_5_run_evidence_status.json path.")
    parser.add_argument("--post-run-executor-json", default="", help="Optional executor_manifest.json path.")
    parser.add_argument("--archive-closure-json", default="", help="Optional formal archive closure index JSON.")
    parser.add_argument("--controlled-write-package-csv", default="", help="Optional controlled_write_package.csv path.")
    parser.add_argument(
        "--post-write-reverification-plan-csv",
        default="",
        help="Optional post_write_reverification_plan.csv path.",
    )
    parser.add_argument("--archive-gap-list-csv", default="", help="Optional archive_gap_list.csv path.")
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Return exit code 2 when the closure readiness is blocked.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_full_flow_closure_readiness(
            run_dir=args.run_dir,
            full_flow_plan_json=args.full_flow_plan_json or None,
            run_evidence_status_json=args.run_evidence_status_json or None,
            post_run_executor_json=args.post_run_executor_json or None,
            archive_closure_json=args.archive_closure_json or None,
            controlled_write_package_csv=args.controlled_write_package_csv or None,
            post_write_reverification_plan_csv=args.post_write_reverification_plan_csv or None,
            archive_gap_list_csv=args.archive_gap_list_csv or None,
        )
        paths = write_v1_5_full_flow_closure_readiness_outputs(model, Path(args.output_dir))
        result = {
            "status": model.get("overall_status"),
            "run_dir": model.get("run_dir"),
            "device_count": len(model.get("devices") or []),
            "gap_count": len(model.get("gaps") or []),
            "readiness_json": str(paths["readiness_json"]),
            "readiness_markdown": str(paths["readiness_markdown"]),
            "physical_boundaries": model.get("physical_boundaries"),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        if args.fail_on_blocked and model.get("overall_status") == "blocked":
            return 2
        return 0
    except Exception as exc:
        print(f"V1.5 full-flow closure readiness export failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
