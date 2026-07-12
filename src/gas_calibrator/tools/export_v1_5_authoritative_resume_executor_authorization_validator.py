"""Validate a V1.5 resume executor authorization packet offline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_executor_authorization_validator import (
    build_v1_5_authoritative_resume_executor_authorization_validator,
    write_v1_5_authoritative_resume_executor_authorization_validator,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--controlled-design-json", required=True)
    parser.add_argument("--authorization-packet-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_executor_authorization_validator(
        controlled_design_json=args.controlled_design_json,
        authorization_packet_json=args.authorization_packet_json,
    )
    outputs = write_v1_5_authoritative_resume_executor_authorization_validator(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "resume_executor_authorization_validated_offline": model[
                    "resume_executor_authorization_validated_offline"
                ],
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
