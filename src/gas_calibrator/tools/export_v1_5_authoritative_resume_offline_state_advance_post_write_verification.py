"""Verify a committed V1.5 offline resume-state advance without writing state."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    build_v1_5_authoritative_resume_offline_state_advance_post_write_verification,
    write_v1_5_authoritative_resume_offline_state_advance_post_write_verification,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--atomic-write-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        atomic_write_json=args.atomic_write_json
    )
    outputs = write_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "post_write_verification_ready": model[
                    "post_write_verification_ready"
                ],
                "state_consumption_allowed": model["state_consumption_allowed"],
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
