"""Export the offline V1.5 post-run coefficient execution plan."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_post_run_coefficient_executor import (
    build_post_run_coefficient_executor_model,
    write_post_run_coefficient_executor_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a no-write V1.5 post-run coefficient closure plan from "
            "existing pressure, temperature, CO2, H2O, write, verification, "
            "and archive evidence."
        )
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run or evidence directory.")
    parser.add_argument("--output-dir", required=True, help="Directory for executor outputs.")
    parser.add_argument("--plan-json", default="", help="Optional formal plan snapshot JSON.")
    parser.add_argument("--pressure-reference-json", default="", help="Optional COM22/PACE pressure reference JSON.")
    parser.add_argument("--initialization-readiness-json", default="", help="Optional initialization readiness JSON.")
    parser.add_argument("--run-evidence-status-json", default="", help="Optional run evidence status JSON.")
    parser.add_argument("--pressure-review-json", default="", help="Optional pressure/SENCO9 review JSON.")
    parser.add_argument(
        "--pressure-completion-summary-csv",
        default="",
        help="Optional pressure-channel completion summary CSV from the offline completion audit.",
    )
    parser.add_argument(
        "--pressure-device-readiness-csv",
        default="",
        help="Optional per-device pressure-channel readiness CSV from the offline completion audit.",
    )
    parser.add_argument("--temperature-review-csv", default="", help="Optional temperature/SENCO7/8 review CSV.")
    parser.add_argument("--device-quality-review-csv", default="", help="Optional device root-cause/quality review CSV.")
    parser.add_argument(
        "--fit-input-quality-summary-csv",
        default="",
        help="Optional v1_5_fit_input_quality_summary.csv from the mature-route continuity consumer gate.",
    )
    parser.add_argument(
        "--fit-input-quality-devices-csv",
        default="",
        help="Optional v1_5_fit_input_quality_devices.csv with per-device A/REJECT fit-input grades.",
    )
    parser.add_argument("--main-precheck-meta-json", default="", help="Optional main SENCO write precheck metadata JSON.")
    parser.add_argument("--post-write-reverification-json", default="", help="Optional post-write reverification JSON.")
    parser.add_argument("--archive-closure-json", default="", help="Optional formal archive closure index JSON.")
    parser.add_argument(
        "--co2-source-state-gate",
        default="",
        help=(
            "Optional CO2 source-state write-gate directory or "
            "co2_s13_source_state_run_summary.csv/root-cause CSV."
        ),
    )
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Return exit code 2 if the generated executor plan is blocked.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_post_run_coefficient_executor_model(
            run_dir=args.run_dir,
            plan_json=args.plan_json or None,
            pressure_reference_json=args.pressure_reference_json or None,
            initialization_readiness_json=args.initialization_readiness_json or None,
            run_evidence_status_json=args.run_evidence_status_json or None,
            pressure_review_json=args.pressure_review_json or None,
            pressure_completion_summary_csv=args.pressure_completion_summary_csv or None,
            pressure_device_readiness_csv=args.pressure_device_readiness_csv or None,
            temperature_review_csv=args.temperature_review_csv or None,
            device_quality_review_csv=args.device_quality_review_csv or None,
            fit_input_quality_summary_csv=args.fit_input_quality_summary_csv or None,
            fit_input_quality_devices_csv=args.fit_input_quality_devices_csv or None,
            main_precheck_meta_json=args.main_precheck_meta_json or None,
            post_write_reverification_json=args.post_write_reverification_json or None,
            archive_closure_json=args.archive_closure_json or None,
            co2_source_state_gate=args.co2_source_state_gate or None,
        )
        output_paths = write_post_run_coefficient_executor_outputs(model, Path(args.output_dir))
        result = {
            "status": model.get("overall_status"),
            "run_dir": model.get("run_dir"),
            "device_count": len(model.get("devices") or []),
            "stage_count": len(model.get("stages") or []),
            "manifest_path": str(output_paths["manifest"]),
            "summary_path": str(output_paths["summary"]),
            "physical_boundaries": model.get("physical_boundaries"),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        if args.fail_on_blocked and model.get("overall_status") == "blocked":
            return 2
        return 0
    except Exception as exc:
        print(f"V1.5 post-run coefficient executor export failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
