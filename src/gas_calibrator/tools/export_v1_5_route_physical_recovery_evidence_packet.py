"""Export the offline V1.5 route physical recovery evidence packet review."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_route_physical_recovery_evidence_packet import (
    build_v1_5_route_physical_recovery_evidence_packet,
    write_v1_5_route_physical_recovery_evidence_packet,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate a reviewed V1.5 route physical recovery evidence packet before "
            "feeding it to the route physical recovery readiness gate. Offline/no-COM/no-write only."
        )
    )
    parser.add_argument("--recovery-evidence-packet-path", required=True, help="Reviewed recovery evidence JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for packet review artifacts.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_route_physical_recovery_evidence_packet(
            recovery_evidence_packet_path=args.recovery_evidence_packet_path,
            output_dir=args.output_dir,
        )
        model = build_v1_5_route_physical_recovery_evidence_packet(
            recovery_evidence_packet_path=args.recovery_evidence_packet_path,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 route physical recovery evidence packet export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    manifest = model["manifest"]
    payload = {
        "status": manifest["status"],
        "blocker_count": manifest["blocker_count"],
        "review_required_count": manifest["review_required_count"],
        "readiness_input_ready": manifest["readiness_input_ready"],
        "segmented_evidence_review_ready": manifest["segmented_evidence_review_ready"],
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
