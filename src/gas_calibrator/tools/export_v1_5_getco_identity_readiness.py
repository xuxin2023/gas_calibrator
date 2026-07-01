"""Export the offline V1.5 identity/GETCO readiness sidecar."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_getco_identity_readiness import (
    build_getco_identity_readiness_model,
    write_getco_identity_readiness_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate V1.5 read-only identity/GETCO evidence without touching hardware."
    )
    parser.add_argument(
        "--run-dir",
        default=None,
        help="Full-flow output directory. Defaults --getco-dir to coefficient_epoch_0_getco_snapshot.",
    )
    parser.add_argument("--getco-dir", default=None, help="Directory produced by probe_v1_5_getco_component_snapshot.")
    parser.add_argument("--output-dir", required=True, help="Directory for identity/GETCO readiness outputs.")
    parser.add_argument(
        "--fail-on-not-ready",
        action="store_true",
        help="Return exit code 2 unless all identity/GETCO evidence checks are ready.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        if args.getco_dir:
            getco_dir = Path(args.getco_dir)
        elif args.run_dir:
            getco_dir = Path(args.run_dir) / "coefficient_epoch_0_getco_snapshot"
        else:
            raise ValueError("either --getco-dir or --run-dir is required")
        model = build_getco_identity_readiness_model(getco_dir=getco_dir)
        outputs = write_getco_identity_readiness_outputs(model, args.output_dir)
        result = {
            "overall_status": model.get("overall_status"),
            "active_analyzer_count": model.get("active_analyzer_count"),
            "next_controlled_gate": model.get("next_controlled_gate"),
            "readiness_json": str(outputs["json"]),
            "readiness_markdown": str(outputs["markdown"]),
            "checks_csv": str(outputs["checks_csv"]),
            "opens_com_ports": model.get("opens_com_ports"),
            "writes_coefficients": model.get("writes_coefficients"),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        if args.fail_on_not_ready and model.get("overall_status") != "identity_getco_ready_for_auxiliary_neutralization":
            return 2
        return 0
    except Exception as exc:
        print(f"V1.5 identity/GETCO readiness export failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
