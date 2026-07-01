"""Export an offline V1.5 formal run status rollup."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_formal_run_status import (
    build_v1_5_formal_run_status,
    write_v1_5_formal_run_status_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a V1.5 formal run status dashboard from existing sidecars only."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run/evidence directory.")
    parser.add_argument("--output-dir", required=True, help="Directory for status JSON/Markdown/CSV outputs.")
    parser.add_argument("--initialization-readiness-json", default="", help="Optional explicit readiness JSON.")
    parser.add_argument("--pre-gas-readiness-json", default="", help="Optional explicit pre-gas readiness JSON.")
    parser.add_argument("--getco-readiness-json", default="", help="Optional explicit identity/GETCO readiness JSON.")
    parser.add_argument("--run-evidence-status-json", default="", help="Optional explicit run evidence status JSON.")
    parser.add_argument(
        "--full-flow-closure-readiness-json",
        default="",
        help="Optional explicit full-flow closure readiness JSON.",
    )
    parser.add_argument("--archive-closure-json", default="", help="Optional explicit formal archive closure JSON.")
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Return exit code 2 when the rollup is blocked.",
    )
    parser.add_argument(
        "--fail-on-not-release-ready",
        action="store_true",
        help="Return exit code 3 unless formal_release_allowed is true.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_formal_run_status(
            run_dir=args.run_dir,
            initialization_readiness_json=args.initialization_readiness_json or None,
            pre_gas_readiness_json=args.pre_gas_readiness_json or None,
            getco_readiness_json=args.getco_readiness_json or None,
            run_evidence_status_json=args.run_evidence_status_json or None,
            full_flow_closure_readiness_json=args.full_flow_closure_readiness_json or None,
            archive_closure_json=args.archive_closure_json or None,
        )
        outputs = write_v1_5_formal_run_status_outputs(model, Path(args.output_dir))
        result = {
            "overall_status": model.get("overall_status"),
            "current_stage": model.get("current_stage"),
            "next_action": model.get("next_action"),
            "formal_release_allowed": model.get("formal_release_allowed"),
            "database_import_allowed": model.get("database_import_allowed"),
            "can_continue_physical_flow": model.get("can_continue_physical_flow"),
            "outputs": outputs,
            "physical_boundaries": model.get("physical_boundaries"),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
        if args.fail_on_blocked and model.get("overall_status") == "blocked":
            return 2
        if args.fail_on_not_release_ready and not model.get("formal_release_allowed"):
            return 3
        return 0
    except Exception as exc:
        print(f"V1.5 formal run status export failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
