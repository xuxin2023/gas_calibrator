"""Export the offline V1.5 profile-driven new-algorithm runner dry-run bundle."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_algorithm_profile_runner_dry_run import (
    build_v1_5_algorithm_profile_runner_dry_run,
    write_v1_5_algorithm_profile_runner_dry_run_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an offline V1.5 profile-driven new-algorithm runner dry-run bundle. "
            "This emits review artifacts only and does not invoke formal queues."
        )
    )
    parser.add_argument("--profile-path", required=True, help="V1.5 algorithm route profile JSON.")
    parser.add_argument("--profile-id", default="absorption_ratio_shadow", help="Profile id to preview.")
    parser.add_argument("--output-dir", required=True, help="Directory for dry-run bundle outputs.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_algorithm_profile_runner_dry_run(
            profile_path=args.profile_path,
            output_dir=args.output_dir,
            profile_id=args.profile_id,
        )
        outputs = write_v1_5_algorithm_profile_runner_dry_run_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 algorithm profile runner dry-run export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "overall_status": model.get("overall_status"),
        "blocker_count": model.get("blocker_count"),
        "profile_id": model.get("profile_id"),
        "co2_runlist_count": model.get("co2_runlist_count"),
        "h2o_runlist_count": model.get("h2o_runlist_count"),
        "runner_integration_status": model.get("runner_integration_status"),
        "bundle_json": str(outputs["json"].resolve()),
        "bundle_markdown": str(outputs["markdown"].resolve()),
        "checks_csv": str(outputs["checks_csv"].resolve()),
        "output_directories": model.get("output_directories"),
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
