"""Validate a future state-advance authorization without writing V1.5 state."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_authorization import (
    build_v1_5_authoritative_resume_offline_state_advance_authorization,
    write_v1_5_authoritative_resume_offline_state_advance_authorization,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--offline-state-advance-preflight-json", required=True)
    parser.add_argument("--authorization-packet-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_offline_state_advance_authorization(
        offline_state_advance_preflight_json=args.offline_state_advance_preflight_json,
        authorization_packet_json=args.authorization_packet_json,
    )
    outputs = write_v1_5_authoritative_resume_offline_state_advance_authorization(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "offline_state_advance_authorization_validated": model[
                    "offline_state_advance_authorization_validated"
                ],
                "state_write_execution_allowed": model[
                    "state_write_execution_allowed"
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
