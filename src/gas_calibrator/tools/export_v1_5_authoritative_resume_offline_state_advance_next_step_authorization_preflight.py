"""Validate next-step review authorization without executing the V1.5 plan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight import (
    build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight,
    write_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--next-step-plan-json", required=True)
    parser.add_argument("--authorization-packet-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight(
        next_step_plan_json=args.next_step_plan_json,
        authorization_packet_json=args.authorization_packet_json,
    )
    outputs = write_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "plan_review_allowed": model["plan_review_allowed"],
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
