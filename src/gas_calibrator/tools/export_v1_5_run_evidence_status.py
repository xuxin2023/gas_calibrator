"""Export an offline V1.5 run evidence-status tree."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_run_evidence_status import (
    build_v1_5_run_evidence_status,
    render_v1_5_run_evidence_status_markdown,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a V1.5 run evidence status index without touching devices."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run/evidence directory.")
    parser.add_argument("--output-dir", required=True, help="Directory for status JSON and Markdown.")
    parser.add_argument("--full-flow-plan-json", default="", help="Optional v1_5_full_flow_plan.json path.")
    parser.add_argument(
        "--full-flow-stage-manifest-json",
        default="",
        help="Optional v1_5_full_flow_stage_manifest.json path.",
    )
    parser.add_argument("--contract-json", default="", help="Optional v1_5_formal_flow_contract.json path.")
    parser.add_argument("--evidence-bundle-json", default="", help="Optional evidence_bundle.json path.")
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Return exit code 2 when the evidence status is blocked.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        status = build_v1_5_run_evidence_status(
            run_dir=args.run_dir,
            full_flow_plan_json=args.full_flow_plan_json or None,
            full_flow_stage_manifest_json=args.full_flow_stage_manifest_json or None,
            contract_json=args.contract_json or None,
            evidence_bundle_json=args.evidence_bundle_json or None,
            component=args.component,
        )
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / "v1_5_run_evidence_status.json"
        md_path = output_dir / "v1_5_run_evidence_status.md"
        json_path.write_text(
            json.dumps(status, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        md_path.write_text(render_v1_5_run_evidence_status_markdown(status), encoding="utf-8")
        result = {
            "status": status.get("overall_status"),
            "current_stage": status.get("current_stage"),
            "artifact_count": status.get("artifact_count"),
            "json_path": str(json_path.resolve()),
            "markdown_path": str(md_path.resolve()),
            "physical_boundaries": status.get("physical_boundaries"),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
        if args.fail_on_blocked and status.get("overall_status") == "blocked":
            return 2
        return 0
    except Exception as exc:
        print(f"V1.5 run evidence status export failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
