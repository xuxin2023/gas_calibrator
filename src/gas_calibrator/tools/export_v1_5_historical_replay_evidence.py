"""Export read-only V1.5 historical replay evidence binding artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_historical_replay_evidence import (
    build_v1_5_historical_replay_evidence,
    write_v1_5_historical_replay_evidence,
)


def _parse_evidence_root(value: str) -> dict[str, str]:
    """Parse FAMILY:ROUTE=PATH evidence-root CLI entries."""

    try:
        head, root_path = value.split("=", 1)
        family_id, route_kind = head.split(":", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "evidence root must use FAMILY_ID:ROUTE_KIND=PATH, for example "
            "mature_0620_legacy_ratio:co2=D:\\path\\co2"
        ) from exc
    family_id = family_id.strip()
    route_kind = route_kind.strip().lower()
    if not family_id or route_kind not in {"co2", "h2o"}:
        raise argparse.ArgumentTypeError("route kind must be co2 or h2o and family id must be non-empty")
    return {
        "family_id": family_id,
        "route_kind": route_kind,
        "root_path": root_path,
        "algorithm_profile_id": "absorption_ratio_shadow"
        if family_id == "new_algorithm_shadow_run"
        else "legacy_ratio_production",
        "label": f"{family_id}_{route_kind}",
    }


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Bind existing V1.5 historical CO2/H2O evidence directories into an offline replay status. "
            "Reads CSV/JSON only; never opens COM, controls routes, connects PostgreSQL, or writes coefficients."
        )
    )
    parser.add_argument("--profile-path", required=True, help="Path to configs/v1_5_algorithm_route_profiles.json.")
    parser.add_argument("--output-dir", required=True, help="Output directory for replay evidence artifacts.")
    parser.add_argument(
        "--evidence-root",
        action="append",
        type=_parse_evidence_root,
        default=[],
        help="Evidence root in FAMILY_ID:ROUTE_KIND=PATH form. Repeat for multiple route roots.",
    )
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if not args.evidence_root:
        print("At least one --evidence-root is required.", file=sys.stderr, flush=True)
        return 1
    try:
        outputs = write_v1_5_historical_replay_evidence(
            profile_path=args.profile_path,
            evidence_roots=args.evidence_root,
            output_dir=args.output_dir,
        )
        model = build_v1_5_historical_replay_evidence(
            profile_path=args.profile_path,
            evidence_roots=args.evidence_root,
        )
    except Exception as exc:
        print(f"V1.5 historical replay evidence export failed: {exc}", file=sys.stderr, flush=True)
        return 1

    payload = {
        "status": model["manifest"]["status"],
        "blocker_count": model["manifest"]["blocker_count"],
        "review_required_count": model["manifest"]["review_required_count"],
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
