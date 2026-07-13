"""Export a bounded offline lineage audit for V1.5 P1 evidence gaps."""

from __future__ import annotations

import argparse
import json
from typing import Iterable

from ..validation.v1_5_p1_evidence_lineage_audit import (
    build_v1_5_p1_evidence_lineage_audit,
    write_v1_5_p1_evidence_lineage_audit,
)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--task-plan-json-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fail-on-unrecoverable", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    model = build_v1_5_p1_evidence_lineage_audit(
        task_plan_json_path=args.task_plan_json_path
    )
    outputs = write_v1_5_p1_evidence_lineage_audit(model, args.output_dir)
    print(
        json.dumps(
            {
                "overall_status": model["overall_status"],
                "point_count": model["point_count"],
                "recoverable_reference_count": model.get("recoverable_reference_count", 0),
                "unrecoverable_count": model.get("unrecoverable_count", 0),
                "outputs": {key: str(value) for key, value in outputs.items()},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if model["overall_status"] == "blocked_invalid_task_plan":
        return 2
    if args.fail_on_unrecoverable and model.get("unrecoverable_count"):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
