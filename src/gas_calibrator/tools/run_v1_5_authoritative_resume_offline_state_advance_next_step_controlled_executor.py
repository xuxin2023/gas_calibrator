"""Run at most one explicitly authorized, hash-bound V1.5 next step."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor import (
    run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor,
    write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--next-step-execution-preflight-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--execute-next-step", action="store_true")
    parser.add_argument("--expected-attempt-id", default="")
    parser.add_argument("--operator-confirmation-text", default="")
    parser.add_argument("--timeout-s", type=float, default=86400.0)
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
        next_step_execution_preflight_json=args.next_step_execution_preflight_json,
        execute_next_step=args.execute_next_step,
        expected_attempt_id=args.expected_attempt_id,
        operator_confirmation_text=args.operator_confirmation_text,
        timeout_s=args.timeout_s,
    )
    outputs = write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "execution_attempted": model["execution_attempted"],
                "next_step_process_completed": model["next_step_process_completed"],
                "authoritative_state_advanced": model["authoritative_state_advanced"],
                "outputs": {
                    key: str(Path(value).resolve()) for key, value in outputs.items()
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if args.execute_next_step and not model["next_step_process_completed"]:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
