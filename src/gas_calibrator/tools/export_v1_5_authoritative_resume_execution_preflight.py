"""Export the last-moment offline V1.5 resume execution preflight."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_execution_preflight import (
    build_v1_5_authoritative_resume_execution_preflight,
    write_v1_5_authoritative_resume_execution_preflight,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authorization-validation-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_execution_preflight(
        authorization_validation_json=args.authorization_validation_json
    )
    outputs = write_v1_5_authoritative_resume_execution_preflight(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "resume_execution_preflight_ready": model[
                    "resume_execution_preflight_ready"
                ],
                "attempt_id": model["attempt_id"],
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
