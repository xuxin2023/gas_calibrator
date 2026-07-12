"""Gate read-only consumption of a verified offline-advanced V1.5 resume state."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_consumer_readiness import (
    build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness,
    write_v1_5_authoritative_resume_offline_state_advance_consumer_readiness,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--post-write-verification-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
        post_write_verification_json=args.post_write_verification_json
    )
    output = write_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "resume_state_consumer_readiness_ready": model[
                    "resume_state_consumer_readiness_ready"
                ],
                "state_consumption_allowed": model["state_consumption_allowed"],
                "resume_execution_allowed": model["resume_execution_allowed"],
                "output": str(Path(output).resolve()),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 3 if args.fail_on_blocker and model["blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
