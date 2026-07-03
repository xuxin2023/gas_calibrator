"""Export the offline V1.5 mature route contract guard."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_mature_route_contract import (
    build_v1_5_mature_route_contract,
    write_v1_5_mature_route_contract,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate that V1.5 mature CO2/H2O route runners, point counts, "
            "algorithm split, and worker guardrails remain frozen. Offline/no-write only."
        )
    )
    parser.add_argument("--profile-path", required=True, help="Path to configs/v1_5_algorithm_route_profiles.json.")
    parser.add_argument("--output-dir", required=True, help="Output directory for contract artifacts.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_mature_route_contract(
            profile_path=args.profile_path,
            output_dir=args.output_dir,
        )
        model = build_v1_5_mature_route_contract(profile_path=args.profile_path)
    except Exception as exc:
        print(f"V1.5 mature route contract export failed: {exc}", file=sys.stderr, flush=True)
        return 1

    payload = {
        "status": model["manifest"]["status"],
        "blocker_count": model["manifest"]["blocker_count"],
        "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
        "physical_boundaries": {
            key: model["manifest"][key]
            for key in (
                "opens_com_ports",
                "connects_postgresql",
                "controls_pressure",
                "controls_water_or_gas_routes",
                "writes_coefficients",
                "writes_device_id",
                "not_real_acceptance_evidence",
            )
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    if args.fail_on_blocker and model["manifest"]["blocker_count"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
