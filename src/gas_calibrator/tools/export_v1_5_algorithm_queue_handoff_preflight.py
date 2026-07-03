"""Export the offline V1.5 new-algorithm queue handoff preflight."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_algorithm_queue_handoff_preflight import (
    build_v1_5_algorithm_queue_handoff_preflight,
    write_v1_5_algorithm_queue_handoff_preflight_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an offline V1.5 new-algorithm queue handoff preflight. "
            "This checks dry-run/no-prompt evidence and never executes mature queues."
        )
    )
    parser.add_argument(
        "--profile-runner-dry-run-json",
        required=True,
        help="Path to v1_5_algorithm_profile_runner_dry_run.json.",
    )
    parser.add_argument(
        "--runner-integration-dry-run-json",
        default=None,
        help="Optional path to v1_5_algorithm_runner_integration_dry_run.json.",
    )
    parser.add_argument("--output-dir", required=True, help="Directory for preflight outputs.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_algorithm_queue_handoff_preflight(
            profile_runner_dry_run_json=args.profile_runner_dry_run_json,
            runner_integration_dry_run_json=args.runner_integration_dry_run_json,
        )
        outputs = write_v1_5_algorithm_queue_handoff_preflight_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 algorithm queue handoff preflight export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "overall_status": model.get("overall_status"),
        "blocker_count": model.get("blocker_count"),
        "profile_id": model.get("profile_id"),
        "co2_runlist_count": model.get("co2_runlist_count"),
        "h2o_runlist_count": model.get("h2o_runlist_count"),
        "dry_run_handoff_review_allowed": model.get("dry_run_handoff_review_allowed"),
        "live_queue_execution_allowed": model.get("live_queue_execution_allowed"),
        "preflight_json": str(outputs["json"].resolve()),
        "preflight_markdown": str(outputs["markdown"].resolve()),
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
