"""Export the offline V1.5 historical component-QC controlled-writer design."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_historical_component_qc_controlled_writer_design import (
    build_v1_5_historical_component_qc_controlled_writer_design,
    write_v1_5_historical_component_qc_controlled_writer_design,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--blocked-generator-plan-json-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-blocker", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        model = build_v1_5_historical_component_qc_controlled_writer_design(
            blocked_generator_plan_json_path=args.blocked_generator_plan_json_path
        )
        outputs = write_v1_5_historical_component_qc_controlled_writer_design(
            model, args.output_dir
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"overall_status": "blocked", "error": str(exc)}, ensure_ascii=False))
        return 2
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "candidate_binding_count": model["candidate_binding_count"],
                "candidate_binding_blocked_count": model["candidate_binding_blocked_count"],
                "writer_execution_supported": model["locks"]["writer_execution_supported"],
                "historical_component_qc_write_allowed": model["locks"][
                    "historical_component_qc_write_allowed"
                ],
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if args.fail_on_blocker and model["review_required_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
