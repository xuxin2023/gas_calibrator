"""Export the offline V1.5 mature route continuity gate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_mature_route_continuity_gate import (
    build_v1_5_mature_route_continuity_gate,
    write_v1_5_mature_route_continuity_gate,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Review whether one mature V1.5 CO2/H2O queue manifest is eligible as fresh continuous "
            "route evidence. Offline/no-COM/no-route/no-write only."
        )
    )
    parser.add_argument("--route-kind", required=True, choices=("co2", "h2o"), help="Mature route kind.")
    parser.add_argument("--queue-manifest-path", required=True, help="queue_manifest.csv to review.")
    parser.add_argument("--root-cause-audit-path", default="", help="Optional route root-cause audit JSON.")
    parser.add_argument("--expected-point-count", type=int, default=0, help="Override expected point count.")
    parser.add_argument("--output-dir", required=True, help="Output directory for gate artifacts.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_mature_route_continuity_gate(
            route_kind=args.route_kind,
            queue_manifest_path=args.queue_manifest_path,
            root_cause_audit_path=args.root_cause_audit_path or None,
            expected_point_count=args.expected_point_count or None,
            output_dir=args.output_dir,
        )
        model = build_v1_5_mature_route_continuity_gate(
            route_kind=args.route_kind,
            queue_manifest_path=args.queue_manifest_path,
            root_cause_audit_path=args.root_cause_audit_path or None,
            expected_point_count=args.expected_point_count or None,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 mature route continuity gate export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "status": model["manifest"]["status"],
        "route_kind": model["manifest"]["route_kind"],
        "blocker_count": model["manifest"]["blocker_count"],
        "review_required_count": model["manifest"]["review_required_count"],
        "continuous_route_run_fit_eligible": model["manifest"]["continuous_route_run_fit_eligible"],
        "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
        "physical_boundaries": {
            key: model["manifest"][key]
            for key in (
                "opens_com_ports",
                "controls_pressure",
                "controls_water_or_gas_routes",
                "connects_postgresql",
                "writes_coefficients",
                "writes_sn_or_device_code",
                "formal_release_allowed",
                "database_import_allowed",
                "not_real_acceptance_evidence",
            )
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    if args.fail_on_blocker and model["manifest"]["blocker_count"]:
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
