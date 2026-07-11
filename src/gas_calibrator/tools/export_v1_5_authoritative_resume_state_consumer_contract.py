"""Export the offline, default-locked V1.5 resume-state consumer contract."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_authoritative_resume_state_consumer_contract import (
    build_v1_5_authoritative_resume_state_consumer_contract,
    write_v1_5_authoritative_resume_state_consumer_contract,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full-flow-plan-json", required=True)
    parser.add_argument("--post-write-verification-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_state_consumer_contract(
        full_flow_plan_json=args.full_flow_plan_json,
        post_write_verification_json=args.post_write_verification_json,
    )
    output = write_v1_5_authoritative_resume_state_consumer_contract(model, args.output_dir)
    print(json.dumps({"overall_status": model["overall_status"], "output": str(output)}, ensure_ascii=False, indent=2))
    return 2 if args.fail_on_blocker and model["blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
