"""Export the offline V1.5 algorithm formal runlist preview."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_algorithm_route_profiles import (
    write_v1_5_algorithm_formal_runlist_preview,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export a V1.5 algorithm formal runlist preview from the route profile JSON. "
            "This is offline/no-write and does not run CO2/H2O routes or modify runners."
        )
    )
    parser.add_argument("--profile-path", required=True, help="Path to configs/v1_5_algorithm_route_profiles.json.")
    parser.add_argument("--output-dir", required=True, help="Output directory for runlist preview artifacts.")
    parser.add_argument(
        "--profile-id",
        default="absorption_ratio_shadow",
        help="Algorithm profile id to export.",
    )
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_algorithm_formal_runlist_preview(
            profile_path=args.profile_path,
            output_dir=args.output_dir,
            profile_id=args.profile_id,
        )
        manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8-sig"))
    except Exception as exc:
        print(f"V1.5 algorithm formal runlist preview export failed: {exc}", file=sys.stderr, flush=True)
        return 1

    payload = {
        "status": manifest["status"],
        "blocker_count": manifest["blocker_count"],
        "review_required_count": manifest["review_required_count"],
        "profile_id": manifest["profile_id"],
        "legacy_co2_formal_point_count": manifest["legacy_co2_formal_point_count"],
        "legacy_h2o_formal_point_count": manifest["legacy_h2o_formal_point_count"],
        "co2_runlist_count": manifest["co2_runlist_count"],
        "h2o_runlist_count": manifest["h2o_runlist_count"],
        "runner_integration_status": manifest["runner_integration_status"],
        "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
        "physical_boundaries": {
            key: manifest[key]
            for key in (
                "opens_com_ports",
                "connects_postgresql",
                "controls_water_or_gas_routes",
                "writes_coefficients",
                "writes_device_id",
            )
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    if args.fail_on_blocker and manifest["blocker_count"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
