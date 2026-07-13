"""Export an offline design review for legacy V1.5 component-QC derivation."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_p2_qc_derivation_design import (
    build_v1_5_p2_qc_derivation_design,
    write_v1_5_p2_qc_derivation_design,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--task-plan-json-path", required=True)
    parser.add_argument("--p1-audit-json-path", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_p2_qc_derivation_design(
        task_plan_json_path=args.task_plan_json_path,
        p1_audit_json_path=args.p1_audit_json_path,
    )
    outputs = write_v1_5_p2_qc_derivation_design(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "candidate_count": model["candidate_count"],
                "input_complete_count": model.get("input_complete_count", 0),
                "input_incomplete_count": model.get("input_incomplete_count", 0),
                "manual_gate_review_count": model.get("manual_gate_review_count", 0),
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 2 if model["overall_status"] == "blocked_invalid_upstream_evidence" else 0


if __name__ == "__main__":
    raise SystemExit(main())
