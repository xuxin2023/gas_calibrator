"""Export the offline controlled-executor design for one V1.5 next step."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design import (
    build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design,
    write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--next-step-blocked-executor-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design(
        next_step_blocked_executor_json=args.next_step_blocked_executor_json
    )
    outputs = write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design(
        model, args.output_dir
    )
    manifest = model["manifest"]
    print(
        json.dumps(
            {
                "overall_status": manifest["overall_status"],
                "controlled_next_step_executor_design_ready": manifest[
                    "controlled_next_step_executor_design_ready"
                ],
                "execution_supported": manifest["execution_supported"],
                "next_step_execution_allowed": manifest[
                    "next_step_execution_allowed"
                ],
                "outputs": {
                    key: str(Path(value).resolve()) for key, value in outputs.items()
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 3 if args.fail_on_review_required and manifest["review_required_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
