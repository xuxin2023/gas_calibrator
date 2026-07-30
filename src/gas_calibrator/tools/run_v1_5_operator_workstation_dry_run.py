"""Run the final-product V1.5 workstation through the mature 45/13 dry-run path."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable

from ..v1_5.orchestration.operator_workstation import (
    build_v1_5_operator_workstation_plan,
    run_v1_5_operator_workstation_application,
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", required=True, help="Reviewed V1.5 runtime config JSON.")
    parser.add_argument("--co2-queue-csv", required=True, help="Mature 45-point CO2 queue CSV.")
    parser.add_argument("--h2o-queue-csv", required=True, help="Mature 13-point H2O queue CSV.")
    parser.add_argument("--output-dir", required=True, help="Dry-run evidence output directory.")
    parser.add_argument("--run-id", required=True, help="Immutable dry-run identifier.")
    parser.add_argument(
        "--certificate-registry-json",
        default=None,
        help="Optional advisory certificate metadata. Missing/corrupt data never blocks dry-run.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    plan = build_v1_5_operator_workstation_plan(
        config_path=args.config,
        co2_queue_csv=args.co2_queue_csv,
        h2o_queue_csv=args.h2o_queue_csv,
        output_dir=args.output_dir,
        run_id=args.run_id,
        certificate_registry_json=args.certificate_registry_json,
    )
    result, outputs = run_v1_5_operator_workstation_application(
        plan,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "overall_status": result.get("overall_status"),
                "product_name": result.get("product_name"),
                "calibration_kernel": result.get("calibration_kernel"),
                "profile_id": result.get("profile_id"),
                "point_counts": result.get("point_counts"),
                "certificate_start_gate": result.get("certificate_start_gate"),
                "not_real_acceptance_evidence": result.get("not_real_acceptance_evidence"),
                "output_json": str(outputs["json"]),
                "output_markdown": str(outputs["markdown"]),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    if result.get("overall_status") == "pass":
        return 0
    if result.get("overall_status") == "blocked":
        return 2
    return 1


if __name__ == "__main__":
    sys.exit(main())
