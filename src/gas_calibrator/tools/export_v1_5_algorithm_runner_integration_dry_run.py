"""Export the offline V1.5 new-algorithm runner integration dry-run plan."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_algorithm_runner_integration_dry_run import (
    build_v1_5_algorithm_runner_integration_dry_run,
    write_v1_5_algorithm_runner_integration_dry_run_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an offline V1.5 new-algorithm runner integration dry-run plan. "
            "This emits review artifacts only and does not invoke formal queues."
        )
    )
    parser.add_argument("--readiness-dir", required=True, help="Directory containing algorithm runlist readiness JSON.")
    parser.add_argument("--runlist-dir", required=True, help="Directory containing formal runlist preview CSVs.")
    parser.add_argument("--readiness-json", default=None, help="Optional readiness JSON path.")
    parser.add_argument("--co2-runlist-csv", default=None, help="Optional CO2 runlist preview CSV.")
    parser.add_argument("--h2o-runlist-csv", default=None, help="Optional H2O runlist preview CSV.")
    parser.add_argument("--output-dir", required=True, help="Directory for dry-run outputs.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_algorithm_runner_integration_dry_run(
            readiness_dir=args.readiness_dir,
            runlist_dir=args.runlist_dir,
            readiness_json=args.readiness_json,
            co2_runlist_csv=args.co2_runlist_csv,
            h2o_runlist_csv=args.h2o_runlist_csv,
        )
        outputs = write_v1_5_algorithm_runner_integration_dry_run_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 algorithm runner integration dry-run export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "overall_status": model.get("overall_status"),
        "blocker_count": model.get("blocker_count"),
        "profile_id": model.get("profile_id"),
        "co2_runlist_count": model.get("co2_runlist_count"),
        "h2o_runlist_count": model.get("h2o_runlist_count"),
        "runner_integration_status": model.get("runner_integration_status"),
        "dry_run_json": str(outputs["json"].resolve()),
        "dry_run_markdown": str(outputs["markdown"].resolve()),
        "plan_csv": str(outputs["plan_csv"].resolve()),
        "checks_csv": str(outputs["checks_csv"].resolve()),
        "physical_boundaries": {
            "opens_com_ports": model.get("opens_com_ports"),
            "connects_postgresql": model.get("connects_postgresql"),
            "controls_water_or_gas_routes": model.get("controls_water_or_gas_routes"),
            "writes_coefficients": model.get("writes_coefficients"),
            "writes_device_id": model.get("writes_device_id"),
            "does_not_execute_commands": model.get("does_not_execute_commands"),
            "does_not_modify_runners": model.get("does_not_modify_runners"),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    if args.fail_on_blocker and int(model.get("blocker_count") or 0) > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
