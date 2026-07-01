"""Export the offline V1.5 pre-gas readiness sidecar."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_pre_gas_readiness import (
    build_pre_gas_readiness_model,
    write_pre_gas_readiness_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an offline V1.5 pre-gas readiness review without touching hardware."
    )
    parser.add_argument("--run-dir", required=True, help="Full-flow output or V1.5 run directory.")
    parser.add_argument("--initialization-dir", default=None, help="Formal initialization evidence directory.")
    parser.add_argument("--config", default=None, help="Optional runtime config JSON.")
    parser.add_argument("--initialization-readiness-json", default=None)
    parser.add_argument("--initialization-contract-json", default=None)
    parser.add_argument("--database-sidecar-json", default=None)
    parser.add_argument("--output-dir", required=True, help="Directory for pre-gas readiness outputs.")
    parser.add_argument(
        "--fail-on-review-required",
        action="store_true",
        help="Return exit code 2 when the sidecar status requires review.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_pre_gas_readiness_model(
            run_dir=args.run_dir,
            initialization_dir=args.initialization_dir,
            config_path=args.config,
            initialization_readiness_json=args.initialization_readiness_json,
            initialization_contract_json=args.initialization_contract_json,
            database_sidecar_json=args.database_sidecar_json,
        )
        outputs = write_pre_gas_readiness_outputs(model, args.output_dir)
        result = {
            "overall_status": model.get("overall_status"),
            "next_live_gate": model.get("next_live_gate"),
            "readiness_json": str(outputs["json"]),
            "readiness_markdown": str(outputs["markdown"]),
            "checks_csv": str(outputs["checks_csv"]),
            "opens_com_ports": model.get("opens_com_ports"),
            "writes_coefficients": model.get("writes_coefficients"),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        if args.fail_on_review_required and str(model.get("overall_status") or "").startswith("review_required"):
            return 2
        return 0
    except Exception as exc:
        print(f"V1.5 pre-gas readiness export failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
