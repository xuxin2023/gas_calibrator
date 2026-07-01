"""Export the offline V1.5 new-algorithm runlist readiness gate."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_algorithm_runlist_readiness import (
    build_v1_5_algorithm_runlist_readiness,
    write_v1_5_algorithm_runlist_readiness_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an offline V1.5 new-algorithm runlist readiness sidecar. "
            "This checks the 47/14 preview CSVs without opening COM or modifying runners."
        )
    )
    parser.add_argument("--runlist-dir", required=True, help="Directory containing formal runlist preview artifacts.")
    parser.add_argument("--manifest", default=None, help="Optional runlist preview manifest JSON.")
    parser.add_argument("--co2-runlist-csv", default=None, help="Optional CO2 runlist preview CSV.")
    parser.add_argument("--h2o-runlist-csv", default=None, help="Optional H2O runlist preview CSV.")
    parser.add_argument("--output-dir", required=True, help="Directory for readiness outputs.")
    parser.add_argument(
        "--fail-on-blocker",
        action="store_true",
        help="Return exit code 2 when the readiness sidecar has blockers.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_algorithm_runlist_readiness(
            runlist_dir=args.runlist_dir,
            manifest_path=args.manifest,
            co2_runlist_csv=args.co2_runlist_csv,
            h2o_runlist_csv=args.h2o_runlist_csv,
        )
        outputs = write_v1_5_algorithm_runlist_readiness_outputs(model, args.output_dir)
    except Exception as exc:
        print(f"V1.5 algorithm runlist readiness export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "overall_status": model.get("overall_status"),
        "blocker_count": model.get("blocker_count"),
        "profile_id": model.get("profile_id"),
        "co2_runlist_count": model.get("co2_runlist_count"),
        "h2o_runlist_count": model.get("h2o_runlist_count"),
        "runner_integration_status": model.get("runner_integration_status"),
        "readiness_json": str(outputs["json"].resolve()),
        "readiness_markdown": str(outputs["markdown"].resolve()),
        "checks_csv": str(outputs["checks_csv"].resolve()),
        "physical_boundaries": {
            "opens_com_ports": model.get("opens_com_ports"),
            "connects_postgresql": model.get("connects_postgresql"),
            "controls_water_or_gas_routes": model.get("controls_water_or_gas_routes"),
            "writes_coefficients": model.get("writes_coefficients"),
            "writes_device_id": model.get("writes_device_id"),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    if args.fail_on_blocker and int(model.get("blocker_count") or 0) > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
