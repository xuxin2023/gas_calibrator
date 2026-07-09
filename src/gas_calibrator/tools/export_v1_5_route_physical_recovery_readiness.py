"""Export the offline V1.5 route physical recovery readiness gate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_route_physical_recovery_readiness import (
    build_v1_5_route_physical_recovery_readiness,
    write_v1_5_route_physical_recovery_readiness,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Review whether V1.5 route physical blockers such as PACE vent NO_RESPONSE, "
            "pressure-gauge NO_RESPONSE, and dry-gas dewpoint rebound have recovery evidence "
            "before the next continuous formal queue. Offline/no-COM/no-write only."
        )
    )
    parser.add_argument("--root-cause-audit-path", required=True, help="v1_5_route_run_failure_root_cause_audit.json")
    parser.add_argument("--recovery-evidence-path", help="Reviewed physical recovery evidence JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for readiness artifacts.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_route_physical_recovery_readiness(
            root_cause_audit_path=args.root_cause_audit_path,
            recovery_evidence_path=args.recovery_evidence_path,
            output_dir=args.output_dir,
        )
        model = build_v1_5_route_physical_recovery_readiness(
            root_cause_audit_path=args.root_cause_audit_path,
            recovery_evidence_path=args.recovery_evidence_path,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 route physical recovery readiness export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "status": model["manifest"]["status"],
        "blocker_count": model["manifest"]["blocker_count"],
        "review_required_count": model["manifest"]["review_required_count"],
        "next_continuous_run_allowed": model["manifest"]["next_continuous_run_allowed"],
        "segmented_evidence_fit_use_allowed": model["manifest"]["segmented_evidence_fit_use_allowed"],
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
