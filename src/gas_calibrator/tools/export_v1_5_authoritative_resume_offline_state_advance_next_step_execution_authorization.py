"""Validate a short-lived authorization for one exact V1.5 next step."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization import (
    build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization,
    write_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--controlled-executor-design-json", required=True)
    parser.add_argument("--execution-authorization-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization(
        controlled_executor_design_json=args.controlled_executor_design_json,
        execution_authorization_json=args.execution_authorization_json,
    )
    outputs = write_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "execution_authorization_validated": model[
                    "execution_authorization_validated"
                ],
                "next_step_execution_allowed": model["next_step_execution_allowed"],
                "outputs": {
                    key: str(Path(value).resolve()) for key, value in outputs.items()
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 3 if args.fail_on_review_required and model["review_required_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
