"""Export the no-write V1.5 offline-resume state-advance CAS preflight."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_preflight import (
    build_v1_5_authoritative_resume_offline_state_advance_preflight,
    write_v1_5_authoritative_resume_offline_state_advance_preflight,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--offline-post-execution-verifier-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_offline_state_advance_preflight(
        offline_post_execution_verifier_json=(
            args.offline_post_execution_verifier_json
        )
    )
    outputs = write_v1_5_authoritative_resume_offline_state_advance_preflight(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "offline_state_advance_preflight_ready": model[
                    "offline_state_advance_preflight_ready"
                ],
                "verified_step_id": model["verified_step_id"],
                "candidate_state_sha256": model["candidate_state_sha256"],
                "authoritative_state_write_allowed": model[
                    "authoritative_state_write_allowed"
                ],
                "outputs": {
                    key: str(Path(value).resolve()) for key, value in outputs.items()
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 3 if args.fail_on_blocker and model["blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
