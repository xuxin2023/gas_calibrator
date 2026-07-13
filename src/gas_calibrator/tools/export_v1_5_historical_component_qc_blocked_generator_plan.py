"""Export a blocked, no-evaluation historical V1.5 component-QC plan."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_historical_component_qc_blocked_generator_plan import (
    build_v1_5_historical_component_qc_blocked_generator_plan,
    write_v1_5_historical_component_qc_blocked_generator_plan,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--preflight-json-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        model = build_v1_5_historical_component_qc_blocked_generator_plan(
            preflight_json_path=args.preflight_json_path
        )
        outputs = write_v1_5_historical_component_qc_blocked_generator_plan(
            model, args.output_dir
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"overall_status": "blocked", "error": str(exc)}, ensure_ascii=False))
        return 2
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "candidate_plan_ready_count": model["candidate_plan_ready_count"],
                "candidate_blocked_count": model["candidate_blocked_count"],
                "operation_plan_count": len(model["operation_plan"]),
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if args.fail_on_blocker and (
        model["global_blocker_codes"] or model["candidate_blocked_count"]
    ) else 0


if __name__ == "__main__":
    raise SystemExit(main())
