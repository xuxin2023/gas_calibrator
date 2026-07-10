"""Export an offline V1.5 route physical recovery evidence binder."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_route_physical_recovery_evidence_binder import (
    build_v1_5_route_physical_recovery_evidence_binder,
    write_v1_5_route_physical_recovery_evidence_binder,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Bind reviewed dry-gas dewpoint, PACE vent, and COM22/INL pressure trace files "
            "into a V1.5 route physical recovery evidence packet. Offline/no-COM/no-write only."
        )
    )
    parser.add_argument("--dewpoint-trace-path", required=True, help="Reviewed dry-gas dewpoint trace CSV.")
    parser.add_argument("--pace-vent-trace-path", required=True, help="Reviewed PACE vent roundtrip trace CSV.")
    parser.add_argument("--pressure-gauge-trace-path", required=True, help="Reviewed COM22/INL pressure trace CSV.")
    parser.add_argument("--route-or-dryer-check-path", help="Optional route/dryer check evidence path.")
    parser.add_argument("--route-or-dryer-check-note", default="", help="Optional route/dryer check note.")
    parser.add_argument("--accepted-manifest-path", help="Optional accepted manifest path for segmented evidence.")
    parser.add_argument("--supersedence-review-id", default="", help="Optional accepted-manifest review id.")
    parser.add_argument("--pressure-source", default="inl", help="Fallback pressure source when trace lacks a source column.")
    parser.add_argument("--tail-count", type=int, default=5, help="Number of final dewpoint rows to use for tail stability.")
    parser.add_argument("--output-dir", required=True, help="Output directory for binder artifacts.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_route_physical_recovery_evidence_binder(
            output_dir=args.output_dir,
            dewpoint_trace_path=args.dewpoint_trace_path,
            pace_vent_trace_path=args.pace_vent_trace_path,
            pressure_gauge_trace_path=args.pressure_gauge_trace_path,
            route_or_dryer_check_path=args.route_or_dryer_check_path,
            route_or_dryer_check_note=args.route_or_dryer_check_note,
            accepted_manifest_path=args.accepted_manifest_path,
            supersedence_review_id=args.supersedence_review_id,
            pressure_source=args.pressure_source,
            tail_count=args.tail_count,
        )
        model = build_v1_5_route_physical_recovery_evidence_binder(
            dewpoint_trace_path=args.dewpoint_trace_path,
            pace_vent_trace_path=args.pace_vent_trace_path,
            pressure_gauge_trace_path=args.pressure_gauge_trace_path,
            route_or_dryer_check_path=args.route_or_dryer_check_path,
            route_or_dryer_check_note=args.route_or_dryer_check_note,
            accepted_manifest_path=args.accepted_manifest_path,
            supersedence_review_id=args.supersedence_review_id,
            pressure_source=args.pressure_source,
            tail_count=args.tail_count,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 route physical recovery evidence binder export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    manifest = model["manifest"]
    payload = {
        "status": manifest["status"],
        "blocker_count": manifest["blocker_count"],
        "ready_for_validator": manifest["ready_for_validator"],
        "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
        "physical_boundaries": {
            key: manifest[key]
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
    if args.fail_on_blocker and manifest["blocker_count"]:
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
