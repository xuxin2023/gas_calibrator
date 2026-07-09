"""Export the offline V1.5 production entrypoint map."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_production_entrypoint_map import (
    build_v1_5_production_entrypoint_map,
    write_v1_5_production_entrypoint_map,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export the V1.5 production entrypoint map. Offline/no-COM/no-write only; "
            "does not execute route runners, controlled writes, or database imports."
        )
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for map artifacts.")
    parser.add_argument("--fail-on-blocker", action="store_true", help="Return exit code 2 when blockers exist.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = write_v1_5_production_entrypoint_map(output_dir=args.output_dir)
        model = build_v1_5_production_entrypoint_map()
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"V1.5 production entrypoint map export failed: {exc}", file=sys.stderr, flush=True)
        return 1

    payload = {
        "status": model["manifest"]["status"],
        "blocker_count": model["manifest"]["blocker_count"],
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
