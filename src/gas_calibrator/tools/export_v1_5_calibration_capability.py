"""Export an offline V1.5 calibration capability assessment."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_calibration_capability import (
    build_v1_5_calibration_capability,
    render_v1_5_calibration_capability_markdown,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assess V1.5 calibratability from existing evidence without touching devices."
    )
    parser.add_argument("--run-status-json", required=True, help="Existing v1_5_run_evidence_status.json path.")
    parser.add_argument("--output-dir", required=True, help="Directory for capability JSON and Markdown.")
    parser.add_argument(
        "--verification-csv",
        action="append",
        default=[],
        help="Optional post-write/reverification summary CSV. Can be repeated.",
    )
    parser.add_argument(
        "--candidate-csv",
        action="append",
        default=[],
        help="Optional candidate summary CSV. Can be repeated.",
    )
    parser.add_argument("--component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument("--co2-limit-pct", type=float, default=1.5)
    parser.add_argument("--h2o-limit-pct", type=float, default=2.0)
    parser.add_argument(
        "--fail-on-p0",
        action="store_true",
        help="Return exit code 2 when P0 blockers are present.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        assessment = build_v1_5_calibration_capability(
            run_status_json=args.run_status_json,
            verification_csvs=args.verification_csv,
            candidate_csvs=args.candidate_csv,
            component=args.component,
            co2_limit_pct=args.co2_limit_pct,
            h2o_limit_pct=args.h2o_limit_pct,
        )
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / "v1_5_calibration_capability.json"
        md_path = output_dir / "v1_5_calibration_capability.md"
        json_path.write_text(
            json.dumps(assessment, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        md_path.write_text(render_v1_5_calibration_capability_markdown(assessment), encoding="utf-8-sig")
        result = {
            "capability_status": assessment.get("capability_status"),
            "method_backbone_ready": assessment.get("method_backbone_ready"),
            "formal_release_ready": assessment.get("formal_release_ready"),
            "json_path": str(json_path.resolve()),
            "markdown_path": str(md_path.resolve()),
            "physical_boundaries": assessment.get("physical_boundaries"),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
        if args.fail_on_p0 and any(row.get("severity") == "P0" for row in assessment.get("issues") or []):
            return 2
        return 0
    except Exception as exc:
        print(f"V1.5 calibration capability export failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
