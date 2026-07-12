"""Run one explicitly authorized canonical V1.5 offline resume step."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_executor import (
    run_v1_5_authoritative_resume_offline_executor,
    write_v1_5_authoritative_resume_offline_executor,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--offline-candidate-gate-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--execute-offline-step", action="store_true")
    parser.add_argument("--expected-attempt-id", default="")
    parser.add_argument("--operator-confirmation-text", default="")
    parser.add_argument("--timeout-s", type=float, default=300.0)
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = run_v1_5_authoritative_resume_offline_executor(
        offline_candidate_gate_json=args.offline_candidate_gate_json,
        execute_offline_step=args.execute_offline_step,
        expected_attempt_id=args.expected_attempt_id,
        operator_confirmation_text=args.operator_confirmation_text,
        timeout_s=args.timeout_s,
    )
    outputs = write_v1_5_authoritative_resume_offline_executor(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "offline_step_executed": model["offline_step_executed"],
                "process_return_code": model["process_return_code"],
                "authoritative_state_advanced": model["authoritative_state_advanced"],
                "outputs": {
                    key: str(Path(value).resolve()) for key, value in outputs.items()
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if args.execute_offline_step and not model["offline_step_executed"]:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
