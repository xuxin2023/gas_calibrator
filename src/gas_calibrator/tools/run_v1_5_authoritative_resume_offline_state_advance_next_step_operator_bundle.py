"""Prepare or execute one complete, hash-bound V1.5 next-step operator bundle."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle import (
    run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--controlled-executor-design-json", required=True)
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--operator", required=True)
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--approver", required=True)
    parser.add_argument("--authorization-ttl-s", type=float, default=900.0)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--execute-next-step", action="store_true")
    parser.add_argument("--expected-attempt-id", default="")
    parser.add_argument("--operator-confirmation-text", default="")
    parser.add_argument("--timeout-s", type=float, default=86400.0)
    args = parser.parse_args(list(argv) if argv is not None else None)
    output = Path(args.output_dir).absolute()
    if output.exists() and any(output.iterdir()):
        parser.error(
            "--output-dir must be absent or empty for a fresh authorization bundle"
        )
    model = (
        run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle(
            controlled_executor_design_json=args.controlled_executor_design_json,
            authorization_id=args.authorization_id,
            operator=args.operator,
            reviewer=args.reviewer,
            approver=args.approver,
            output_dir=output,
            ttl_s=args.authorization_ttl_s,
            execute_next_step=args.execute_next_step,
            expected_attempt_id=args.expected_attempt_id,
            operator_confirmation_text=args.operator_confirmation_text,
            timeout_s=args.timeout_s,
        )
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "operator_bundle_prepared": model["operator_bundle_prepared"],
                "execution_requested": model["execution_requested"],
                "execution_attempted": model["execution_attempted"],
                "next_step_process_completed": model["next_step_process_completed"],
                "hold_count": model["hold_count"],
                "manifest_json": model["manifest_json"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return (
        3 if args.execute_next_step and not model["next_step_process_completed"] else 0
    )


if __name__ == "__main__":
    raise SystemExit(main())
