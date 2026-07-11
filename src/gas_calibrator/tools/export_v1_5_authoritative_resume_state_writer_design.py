"""Export the offline V1.5 authoritative resume-state writer design review."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Optional

from ..validation.v1_5_authoritative_resume_state_writer_design import (
    write_v1_5_authoritative_resume_state_writer_design,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Review the future V1.5 authoritative resume-state writer contract without writing state or executing commands."
        )
    )
    parser.add_argument("--full-flow-plan-json", required=True)
    parser.add_argument("--resume-prefix-application-review-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocked", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        paths = write_v1_5_authoritative_resume_state_writer_design(
            output_dir=args.output_dir,
            full_flow_plan_json=args.full_flow_plan_json,
            resume_prefix_application_review_json=args.resume_prefix_application_review_json,
        )
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(
            f"V1.5 authoritative resume-state writer design export failed: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1
    print(json.dumps({key: str(path) for key, path in paths.items()}, ensure_ascii=False, indent=2))
    if args.fail_on_blocked:
        payload = json.loads(paths["manifest"].read_text(encoding="utf-8"))
        if payload.get("authoritative_resume_state_writer_design_ready") is not True:
            return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
