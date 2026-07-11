"""Emit fail-closed evidence for the future V1.5 resume executor."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_executor_blocked import (
    build_v1_5_authoritative_resume_executor_blocked,
    write_v1_5_authoritative_resume_executor_blocked,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resume-executor-plan-preview-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    model = build_v1_5_authoritative_resume_executor_blocked(
        resume_executor_plan_preview_json=args.resume_executor_plan_preview_json
    )
    outputs = write_v1_5_authoritative_resume_executor_blocked(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "blocked_executor_ready": model["blocked_executor_ready"],
                "execution_supported": model["execution_supported"],
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
