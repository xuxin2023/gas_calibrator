"""Verify one completed V1.5 offline resume step without advancing state."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_post_execution_verifier import (
    build_v1_5_authoritative_resume_offline_post_execution_verifier,
    write_v1_5_authoritative_resume_offline_post_execution_verifier,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--offline-executor-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_offline_post_execution_verifier(
        offline_executor_json=args.offline_executor_json
    )
    outputs = write_v1_5_authoritative_resume_offline_post_execution_verifier(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "offline_post_execution_verification_ready": model[
                    "offline_post_execution_verification_ready"
                ],
                "authoritative_state_advance_allowed": model[
                    "authoritative_state_advance_allowed"
                ],
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
