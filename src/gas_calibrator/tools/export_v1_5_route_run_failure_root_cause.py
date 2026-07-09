"""Export the offline V1.5 route run failure root-cause audit."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_route_run_failure_root_cause import (
    audit_v1_5_route_run_failure_root_causes,
    write_v1_5_route_run_failure_root_cause_audit,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Classify why V1.5 formal CO2/H2O route runs split, stopped, or became unsafe for direct fitting. "
            "Offline/no-COM/no-write only."
        )
    )
    parser.add_argument("--run-dir", action="append", required=True, help="Route run directory to audit. Repeatable.")
    parser.add_argument("--output-dir", required=True, help="Output directory for audit artifacts.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_route_run_failure_root_cause_audit(
            run_dirs=args.run_dir,
            output_dir=args.output_dir,
        )
        model = audit_v1_5_route_run_failure_root_causes(run_dirs=args.run_dir)
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 route run failure root-cause audit failed: {exc}", file=sys.stderr, flush=True)
        return 1
    payload = {
        "status": model["manifest"]["status"],
        "blocker_count": model["manifest"]["blocker_count"],
        "review_required_count": model["manifest"]["review_required_count"],
        "category_counts": model["manifest"]["category_counts"],
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
