"""Export offline V1.5 authoritative resume-state post-write verification."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_authoritative_resume_state_post_write_verification import (
    build_v1_5_authoritative_resume_state_post_write_verification,
    write_v1_5_authoritative_resume_state_post_write_verification_outputs,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--atomic-write-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_state_post_write_verification(
        atomic_write_json=args.atomic_write_json
    )
    outputs = write_v1_5_authoritative_resume_state_post_write_verification_outputs(
        model, args.output_dir
    )
    print(json.dumps({"overall_status": model["overall_status"], "outputs": {key: str(value) for key, value in outputs.items()}}, ensure_ascii=False, indent=2))
    return 2 if args.fail_on_blocker and model["blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
