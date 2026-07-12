"""Export the offline-only V1.5 resume candidate gate."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..validation.v1_5_authoritative_resume_offline_candidate_gate import (
    build_v1_5_authoritative_resume_offline_candidate_gate,
    write_v1_5_authoritative_resume_offline_candidate_gate,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execution-preflight-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-review-required", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_offline_candidate_gate(
        execution_preflight_json=args.execution_preflight_json
    )
    outputs = write_v1_5_authoritative_resume_offline_candidate_gate(
        model, args.output_dir
    )
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "offline_resume_candidate_ready": model[
                    "offline_resume_candidate_ready"
                ],
                "next_step_id": model["next_step_id"],
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
