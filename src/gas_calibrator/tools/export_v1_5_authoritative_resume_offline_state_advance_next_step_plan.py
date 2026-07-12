"""Export a no-execute next-step plan from verified offline state-advance evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_next_step_plan import (
    build_v1_5_authoritative_resume_offline_state_advance_next_step_plan,
    write_v1_5_authoritative_resume_offline_state_advance_next_step_plan,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--consumer-readiness-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
        consumer_readiness_json=args.consumer_readiness_json
    )
    output = write_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "next_step_plan_review_ready": model[
                    "next_step_plan_review_ready"
                ],
                "next_step_id": model["next_step_id"],
                "next_step_execution_allowed": model[
                    "next_step_execution_allowed"
                ],
                "output": str(Path(output).resolve()),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 3 if args.fail_on_blocker and model["blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
