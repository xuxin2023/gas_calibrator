"""Export an offline V1.5 route physical recovery evidence packet template."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_route_physical_recovery_evidence_packet_template import (
    build_v1_5_route_physical_recovery_evidence_packet_template,
    write_v1_5_route_physical_recovery_evidence_packet_template,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate an offline template and collection checklist for the V1.5 route physical "
            "recovery evidence packet. This command does not open COM, control pressure/routes, "
            "connect PostgreSQL, or write analyzer state."
        )
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for template artifacts.")
    parser.add_argument(
        "--root-cause-audit-path",
        help="Optional v1_5_route_run_failure_root_cause_audit.json used to echo blocker categories.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_route_physical_recovery_evidence_packet_template(
            output_dir=args.output_dir,
            root_cause_audit_path=args.root_cause_audit_path,
        )
        model = build_v1_5_route_physical_recovery_evidence_packet_template(
            root_cause_audit_path=args.root_cause_audit_path,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 route physical recovery packet template export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    manifest = model["manifest"]
    payload = {
        "status": manifest["status"],
        "collection_step_count": manifest["collection_step_count"],
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
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
