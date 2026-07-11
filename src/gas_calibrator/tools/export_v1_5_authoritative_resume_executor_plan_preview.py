"""Export a plan-only V1.5 authoritative resume executor preview."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_authoritative_resume_executor_plan_preview import (
    build_v1_5_authoritative_resume_executor_plan_preview,
    write_v1_5_authoritative_resume_executor_plan_preview,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumer-contract-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_authoritative_resume_executor_plan_preview(
        consumer_contract_json=args.consumer_contract_json
    )
    output = write_v1_5_authoritative_resume_executor_plan_preview(model, args.output_dir)
    print(json.dumps({"overall_status": model["overall_status"], "output": str(output)}, ensure_ascii=False, indent=2))
    return 2 if args.fail_on_blocker and model["blocker_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
