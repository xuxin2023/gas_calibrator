"""Export the offline V1.5 controlled resume executor design."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_executor_controlled_design import (
    build_v1_5_authoritative_resume_executor_controlled_design,
    write_v1_5_authoritative_resume_executor_controlled_design,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authoritative-resume-executor-blocked-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_executor_controlled_design(
        authoritative_resume_executor_blocked_json=(
            args.authoritative_resume_executor_blocked_json
        )
    )
    outputs = write_v1_5_authoritative_resume_executor_controlled_design(
        model, args.output_dir
    )
    manifest = model["manifest"]
    print(
        json.dumps(
            {
                "overall_status": manifest["overall_status"],
                "controlled_resume_executor_design_ready": manifest[
                    "controlled_resume_executor_design_ready"
                ],
                "execution_supported": manifest["execution_supported"],
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
